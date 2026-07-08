package public

import (
	"decentralized-api/apiconfig"
	"decentralized-api/broker"
	"decentralized-api/chainphase"
	"decentralized-api/cosmosclient"
	"decentralized-api/internal"
	"decentralized-api/internal/authzcache"
	"decentralized-api/internal/server/middleware"
	"decentralized-api/observability"
	"decentralized-api/payloadstorage"
	"decentralized-api/poc/artifacts"
	"decentralized-api/statsstorage"
	"devshard"
	"fmt"
	"net/http"
	"time"

	echoMiddleware "github.com/labstack/echo/v4/middleware"

	"github.com/labstack/echo/v4"
	echomw "github.com/labstack/echo/v4/middleware"
)

const httpClientTimeout = 30 * time.Minute

type Server struct {
	e                   *echo.Echo
	nodeBroker          *broker.Broker
	configManager       *apiconfig.ConfigManager
	recorder            cosmosclient.CosmosMessageClient
	blockQueue          *BridgeQueue
	bandwidthLimiter    *internal.BandwidthLimiter
	identityCache       *identityCache
	payloadStorage      payloadstorage.PayloadStorage
	phaseTracker        *chainphase.ChainPhaseTracker
	epochGroupDataCache *internal.EpochGroupDataCache
	artifactStore       *artifacts.ManagedArtifactStore
	authzCache          *authzcache.AuthzCache
	httpClient          *http.Client
	statsStorage        statsstorage.StatsStorage
}

// ServerOption configures optional Server dependencies.
type ServerOption func(*Server)

// WithArtifactStore enables local artifact storage for off-chain PoC proofs.
func WithArtifactStore(store *artifacts.ManagedArtifactStore) ServerOption {
	return func(s *Server) {
		s.artifactStore = store
	}
}

func WithStatsStorage(store statsstorage.StatsStorage) ServerOption {
	return func(s *Server) {
		s.statsStorage = store
	}
}

func NewServer(
	nodeBroker *broker.Broker,
	configManager *apiconfig.ConfigManager,
	recorder cosmosclient.CosmosMessageClient,
	blockQueue *BridgeQueue,
	phaseTracker *chainphase.ChainPhaseTracker,
	payloadStorage payloadstorage.PayloadStorage,
	opts ...ServerOption) *Server {
	e := echo.New()
	e.HTTPErrorHandler = middleware.TransparentErrorHandler

	// Set the package-level configManagerRef
	configManagerRef = configManager

	s := &Server{
		e:                   e,
		nodeBroker:          nodeBroker,
		configManager:       configManager,
		recorder:            recorder,
		blockQueue:          blockQueue,
		identityCache:       newIdentityCache(),
		payloadStorage:      payloadStorage,
		phaseTracker:        phaseTracker,
		epochGroupDataCache: internal.NewEpochGroupDataCache(recorder),
		authzCache:          authzcache.NewAuthzCache(recorder),
		httpClient:          NewNoRedirectClient(httpClientTimeout),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.bandwidthLimiter = internal.NewBandwidthLimiterFromConfig(configManager, recorder, phaseTracker)

	e.Use(middleware.LoggingMiddleware)
	e.Use(echoMiddleware.BodyLimit(MaxRequestBodyLimit))
	g := e.Group("/v1/")

	g.GET("status", s.getStatus)
	g.GET("identity", s.getIdentity)

	g.POST("chat/completions", s.postChat)
	g.POST("completions", s.postCompletions)
	g.GET("chat/completions", s.getChatById)
	g.GET("inference/payloads", s.getInferencePayloads)

	g.GET("participants/:address", s.getAccountByAddress)
	g.GET("participants", s.getAllParticipants)
	g.POST("participants", s.submitNewParticipantHandler)

	g.POST("verify-proof", s.postVerifyProof)
	g.POST("verify-block", s.postVerifyBlock)

	g.GET("pricing", s.getPricing)
	g.GET("models", s.getModels)
	g.GET("governance/pricing", s.getGovernancePricing)
	g.GET("governance/models", s.getGovernanceModels)
	//TODO: Remove later - response format used by old dashboard
	g.GET("governance/models-legacy", s.getGovernanceModelsLegacy)
	g.GET("stats/models", s.getStatsModels)
	g.GET("stats/developers/:developer/inferences", s.getStatsDeveloperInferences)
	g.GET("stats/developers/:developer/summary/epochs", s.getStatsDeveloperSummaryEpochs)
	g.GET("stats/summary/epochs", s.getStatsSummaryEpochs)
	g.GET("stats/summary/time", s.getStatsSummaryTime)
	g.GET("stats/debug/developers", s.getStatsDebugDevelopers)
	g.GET("poc-batches/:epoch", s.getPoCBatches)

	g.GET("debug/pubkey-to-addr/:pubkey", s.debugPubKeyToAddr)
	g.GET("debug/verify/:height", s.debugVerify)

	g.GET("versions", s.getVersions)

	g.GET("bridge/status", s.getBridgeStatus)
	g.GET("bridge/addresses", s.getBridgeAddresses)

	g.GET("epochs/:epoch", s.getEpochById)
	g.GET("epochs/:epoch/participants", s.getParticipantsByEpoch)

	// BLS Query Endpoints
	blsGroup := g.Group("bls/")
	blsGroup.GET("epoch/:id", s.getBLSEpochByID)
	blsGroup.GET("epochs/:id", s.getBLSEpochByID)
	blsGroup.GET("signatures/:request_id", s.getBLSSignatureByRequestID)

	// Restrictions public API (query-only)
	g.GET("restrictions/status", s.getRestrictionsStatus)
	g.GET("restrictions/exemptions", s.getRestrictionsExemptions)
	g.GET("restrictions/exemptions/:id/usage/:account", s.getRestrictionsExemptionUsage)

	// PoC proofs endpoint with IP rate limiting (100 req/min per IP)
	pocProofsRateLimiter := echomw.RateLimiter(echomw.NewRateLimiterMemoryStoreWithConfig(
		echomw.RateLimiterMemoryStoreConfig{
			Rate:      300.0 / 60.0, // 100 requests per minute
			Burst:     30,
			ExpiresIn: 3 * time.Minute,
		},
	))
	g.POST("poc/proofs", s.postPocProofs, pocProofsRateLimiter)

	// PoC artifact state endpoint (for testermint/validators to get real count and root_hash)
	g.GET("poc/artifacts/state", s.getPocArtifactsState)

	// Public mlnode metrics federation: one endpoint that merges the Prometheus
	// metrics of every mlnode this participant runs, each series labelled with
	// its mlnode id (plus mlnode_up per node). The mlnodes stay private — only
	// this api reaches them. Cached (10s) so public scrapes don't hammer the
	// mlnodes, and IP rate limited. Reachable publicly at /v1/mlnodes/metrics
	// via nginx's existing /v1/ proxy.
	mlnodeMetricsRateLimiter := echomw.RateLimiter(echomw.NewRateLimiterMemoryStoreWithConfig(
		echomw.RateLimiterMemoryStoreConfig{
			Rate:      60.0 / 60.0, // 60 requests per minute per IP
			Burst:     10,
			ExpiresIn: 3 * time.Minute,
		},
	))
	g.GET("mlnodes/metrics",
		echo.WrapHandler(observability.MLNodeMetricsHandler(s.mlNodeMetricsTargets, observability.MLNodeMetricsConfig{
			CacheTTL:      10 * time.Second,
			ScrapeTimeout: 3 * time.Second,
		})),
		mlnodeMetricsRateLimiter,
	)

	v2 := e.Group("/v2/")
	v2.GET("participants/:address", s.getParticipantByAddress)
	v2.GET("accounts/:address", s.getAccountByAddress)
	return s
}

// DevshardGroup returns an echo group for mounting devshard routes.
// Mounted under /v1/devshard so nginx's existing /v1/ location proxies it.
func (s *Server) DevshardGroup() *echo.Group {
	return s.e.Group(devshard.LegacyRoutePrefix)
}

func (s *Server) Start(addr string) {
	go s.e.Start(addr)
}

func (s *Server) getStatus(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

// mlNodeMetricsTargets snapshots the participant's current mlnodes and where to
// scrape each one's Prometheus metrics. vLLM-style mlnodes expose /metrics on
// their inference port; adjust here if a deployment serves it elsewhere.
func (s *Server) mlNodeMetricsTargets() ([]observability.MLNodeTarget, error) {
	nodes, err := s.nodeBroker.GetNodes()
	if err != nil {
		return nil, err
	}
	targets := make([]observability.MLNodeTarget, 0, len(nodes))
	for _, n := range nodes {
		targets = append(targets, observability.MLNodeTarget{
			ID:  n.Node.Id,
			URL: fmt.Sprintf("http://%s:%d/metrics", n.Node.Host, n.Node.InferencePort),
		})
	}
	return targets, nil
}
