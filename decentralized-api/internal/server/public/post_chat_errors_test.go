package public

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"decentralized-api/apiconfig"
	"decentralized-api/chainphase"
	"decentralized-api/cosmosclient"
	"decentralized-api/internal"
	"decentralized-api/utils"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/knadh/koanf/providers/file"
	"github.com/labstack/echo/v4"
	"github.com/productscience/inference/x/inference/calculations"
	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
)

func newTestConfigManager(t *testing.T) *apiconfig.ConfigManager {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	_, err = tmpFile.Write([]byte("nodes: []"))
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	configManager := &apiconfig.ConfigManager{
		KoanProvider:   file.Provider(tmpFile.Name()),
		WriterProvider: apiconfig.NewFileWriteCloserProvider(tmpFile.Name()),
	}
	require.NoError(t, configManager.Load())
	return configManager
}

func textMessageContent(text string) MessageContent {
	return MessageContent{Text: &text}
}

type fakePricingQueryServer struct {
	types.UnimplementedQueryServer
	price          uint64
	found          bool
	pubkey         string
	balance        int64
	epochGroupData *types.EpochGroupData
	epochGroupErr  error
}

func (f *fakePricingQueryServer) GetModelPerTokenPrice(ctx context.Context, req *types.QueryGetModelPerTokenPriceRequest) (*types.QueryGetModelPerTokenPriceResponse, error) {
	return &types.QueryGetModelPerTokenPriceResponse{
		Price: f.price,
		Found: f.found,
	}, nil
}

func (f *fakePricingQueryServer) AccountByAddress(ctx context.Context, req *types.QueryAccountByAddressRequest) (*types.QueryAccountByAddressResponse, error) {
	return &types.QueryAccountByAddressResponse{
		Pubkey:  f.pubkey,
		Balance: f.balance,
	}, nil
}

func (f *fakePricingQueryServer) CurrentEpochGroupData(ctx context.Context, req *types.QueryCurrentEpochGroupDataRequest) (*types.QueryCurrentEpochGroupDataResponse, error) {
	if f.epochGroupErr != nil {
		return nil, f.epochGroupErr
	}
	if f.epochGroupData == nil {
		return &types.QueryCurrentEpochGroupDataResponse{}, nil
	}
	return &types.QueryCurrentEpochGroupDataResponse{
		EpochGroupData: *f.epochGroupData,
	}, nil
}

func TestValidateRequester_ParticipantNotFound(t *testing.T) {
	s := &Server{}
	req := &ChatRequest{}

	err := s.validateRequester(context.Background(), req, nil, 1)
	require.Error(t, err)

	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Code)
}

func TestValidateRequester_InsufficientBalance(t *testing.T) {
	devKey := newTestKey()
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	timestamp := time.Now().UnixNano()
	transferAddress := "ta1"

	components := calculations.SignatureComponents{
		Payload:         utils.GenerateSHA256Hash(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
	}
	signature, err := calculations.Sign(devKey, components, calculations.Developer)
	require.NoError(t, err)

	request := &ChatRequest{
		Body:            []byte(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
		AuthKey:         signature,
		SignBodyHash:    utils.GenerateSHA256Hash(body),
		OpenAiRequest: OpenAiRequest{
			Model:     "test-model",
			MaxTokens: 1,
		},
	}

	queryServer := &fakePricingQueryServer{price: 10, found: true}
	conn, cleanup := startTestGRPCServer(t, queryServer)
	t.Cleanup(cleanup)

	mockCosmos := &cosmosclient.MockCosmosMessageClient{}
	mockCosmos.On("NewInferenceQueryClient").Return(types.NewQueryClient(conn))

	s := &Server{recorder: mockCosmos}
	requester := &types.QueryAccountByAddressResponse{
		Pubkey:  devKey.GetPubKeyBase64(),
		Balance: 0,
	}

	err = s.validateRequester(context.Background(), request, requester, 1)
	require.Error(t, err)

	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusPaymentRequired, httpErr.Code)

	mockCosmos.AssertExpectations(t)
}

func TestValidateRequester_UnsupportedModel(t *testing.T) {
	devKey := newTestKey()
	body := `{"model":"unsupported-model","messages":[{"role":"user","content":"hello"}]}`
	timestamp := time.Now().UnixNano()
	transferAddress := "ta1"

	components := calculations.SignatureComponents{
		Payload:         utils.GenerateSHA256Hash(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
	}
	signature, err := calculations.Sign(devKey, components, calculations.Developer)
	require.NoError(t, err)

	request := &ChatRequest{
		Body:            []byte(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
		AuthKey:         signature,
		SignBodyHash:    utils.GenerateSHA256Hash(body),
		OpenAiRequest: OpenAiRequest{
			Model:     "unsupported-model",
			MaxTokens: 1,
		},
	}

	queryServer := &fakePricingQueryServer{
		price:          1,
		found:          true,
		pubkey:         devKey.GetPubKeyBase64(),
		balance:        100,
		epochGroupData: &types.EpochGroupData{SubGroupModels: []string{"supported-model"}},
	}
	conn, cleanup := startTestGRPCServer(t, queryServer)
	t.Cleanup(cleanup)

	mockCosmos := &cosmosclient.MockCosmosMessageClient{}
	mockCosmos.On("NewInferenceQueryClient").Return(types.NewQueryClient(conn))

	phaseTracker := &chainphase.ChainPhaseTracker{}
	epoch := &types.Epoch{Index: 1, PocStartBlockHeight: 1}
	params := &types.EpochParams{EpochLength: 1}
	phaseTracker.Update(chainphase.BlockInfo{Height: 1, Hash: "hash-1"}, epoch, params, true, nil)

	s := &Server{
		recorder:            mockCosmos,
		phaseTracker:        phaseTracker,
		epochGroupDataCache: internal.NewEpochGroupDataCache(mockCosmos),
	}
	requester := &types.QueryAccountByAddressResponse{
		Pubkey:  devKey.GetPubKeyBase64(),
		Balance: 100,
	}

	err = s.validateRequester(context.Background(), request, requester, 1)
	require.Error(t, err)

	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Code)

	mockCosmos.AssertExpectations(t)
}

func TestValidateRequester_ModelValidationUnavailable(t *testing.T) {
	devKey := newTestKey()
	body := `{"model":"supported-model","messages":[{"role":"user","content":"hello"}]}`
	timestamp := time.Now().UnixNano()
	transferAddress := "ta1"

	components := calculations.SignatureComponents{
		Payload:         utils.GenerateSHA256Hash(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
	}
	signature, err := calculations.Sign(devKey, components, calculations.Developer)
	require.NoError(t, err)

	request := &ChatRequest{
		Body:            []byte(body),
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
		AuthKey:         signature,
		SignBodyHash:    utils.GenerateSHA256Hash(body),
		OpenAiRequest: OpenAiRequest{
			Model:     "supported-model",
			MaxTokens: 1,
		},
	}

	queryServer := &fakePricingQueryServer{
		price:         1,
		found:         true,
		pubkey:        devKey.GetPubKeyBase64(),
		balance:       100,
		epochGroupErr: fmt.Errorf("epoch group query failed"),
	}
	conn, cleanup := startTestGRPCServer(t, queryServer)
	t.Cleanup(cleanup)

	mockCosmos := &cosmosclient.MockCosmosMessageClient{}
	mockCosmos.On("NewInferenceQueryClient").Return(types.NewQueryClient(conn))

	phaseTracker := &chainphase.ChainPhaseTracker{}
	epoch := &types.Epoch{Index: 1, PocStartBlockHeight: 1}
	params := &types.EpochParams{EpochLength: 1}
	phaseTracker.Update(chainphase.BlockInfo{Height: 1, Hash: "hash-1"}, epoch, params, true, nil)

	s := &Server{
		recorder:            mockCosmos,
		phaseTracker:        phaseTracker,
		epochGroupDataCache: internal.NewEpochGroupDataCache(mockCosmos),
	}
	requester := &types.QueryAccountByAddressResponse{
		Pubkey:  devKey.GetPubKeyBase64(),
		Balance: 100,
	}

	err = s.validateRequester(context.Background(), request, requester, 1)
	require.Error(t, err)

	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusServiceUnavailable, httpErr.Code)
	require.Equal(t, "unable to fetch current epoch group data", httpErr.Message)

	mockCosmos.AssertExpectations(t)
}

func TestValidateRequest_InvalidTimestamp(t *testing.T) {
	configManager := newTestConfigManager(t)
	status := &coretypes.ResultStatus{
		SyncInfo: coretypes.SyncInfo{
			LatestBlockHeight: 1,
			LatestBlockTime:   time.Now(),
		},
	}

	req := &ChatRequest{
		AuthKey:   fmt.Sprintf("authkey-invalid-ts-%d", time.Now().UnixNano()),
		Timestamp: time.Now().Add(-20 * time.Second).UnixNano(),
	}

	err := validateRequest(req, status, configManager)
	require.Error(t, err)

	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)
}
