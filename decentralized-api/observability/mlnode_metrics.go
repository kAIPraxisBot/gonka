package observability

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// MLNodeTarget identifies one mlnode and the URL of its Prometheus /metrics.
type MLNodeTarget struct {
	ID  string
	URL string
}

// MLNodeMetricsConfig tunes the aggregating handler.
type MLNodeMetricsConfig struct {
	// CacheTTL is how long a merged snapshot is served before it is rebuilt.
	// It protects the mlnodes: a burst of public scrapes collapses onto at
	// most one fan-out per TTL.
	CacheTTL time.Duration
	// ScrapeTimeout bounds each per-mlnode fetch so one hung node cannot stall
	// the whole response.
	ScrapeTimeout time.Duration
}

func (c MLNodeMetricsConfig) withDefaults() MLNodeMetricsConfig {
	if c.CacheTTL <= 0 {
		c.CacheTTL = 10 * time.Second
	}
	if c.ScrapeTimeout <= 0 {
		c.ScrapeTimeout = 3 * time.Second
	}
	return c
}

// MLNodeMetricsHandler returns an http.Handler that federates the Prometheus
// metrics of every mlnode currently known to this participant into a single
// exposition, safe to expose publicly.
//
// On each uncached request it enumerates the live mlnodes via list(), scrapes
// each one's /metrics concurrently (bounded by ScrapeTimeout), and injects an
// mlnode="<id>" label onto every series so nothing collides once merged. The
// naive alternative — concatenating N exposition texts — breaks Prometheus on
// duplicate `# HELP`/`# TYPE` lines and on identical series from different
// nodes; parsing into MetricFamilies and merging by name avoids both. An
// mlnode_up{mlnode="<id>"} gauge (1 reachable, 0 not) is added so unavailable
// nodes stay visible instead of silently vanishing.
//
// Results are cached for CacheTTL and rebuilds are single-flighted, so many
// concurrent public scrapes cost the mlnodes at most one fan-out per TTL. The
// mlnodes themselves are never exposed — only this process reaches them. Rate
// limiting and TLS are applied at the edge where this handler is mounted (the
// public /v1 group behind nginx).
func MLNodeMetricsHandler(list func() ([]MLNodeTarget, error), cfg MLNodeMetricsConfig) http.Handler {
	cfg = cfg.withDefaults()
	a := &mlnodeAggregator{
		list:   list,
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.ScrapeTimeout},
	}
	return http.HandlerFunc(a.serve)
}

type mlnodeAggregator struct {
	list   func() ([]MLNodeTarget, error)
	cfg    MLNodeMetricsConfig
	client *http.Client

	buildMu  sync.Mutex // single-flights rebuilds so scrapes coalesce
	mu       sync.Mutex // guards the cache fields below
	cached   []byte
	cachedAt time.Time
}

func (a *mlnodeAggregator) serve(w http.ResponseWriter, r *http.Request) {
	body, err := a.render(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	_, _ = w.Write(body)
}

func (a *mlnodeAggregator) render(ctx context.Context) ([]byte, error) {
	if b := a.fresh(); b != nil {
		return b, nil
	}
	// Single-flight the rebuild: concurrent scrapers block here and then read
	// the freshly-cached snapshot instead of each fanning out to the mlnodes.
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	if b := a.fresh(); b != nil {
		return b, nil
	}
	body, err := a.build(ctx)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.cached, a.cachedAt = body, time.Now()
	a.mu.Unlock()
	return body, nil
}

func (a *mlnodeAggregator) fresh() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cached != nil && time.Since(a.cachedAt) < a.cfg.CacheTTL {
		return a.cached
	}
	return nil
}

func (a *mlnodeAggregator) build(ctx context.Context) ([]byte, error) {
	targets, err := a.list()
	if err != nil {
		return nil, fmt.Errorf("enumerate mlnodes: %w", err)
	}

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		merged = map[string]*dto.MetricFamily{}
		up     = make([]*dto.Metric, 0, len(targets))
	)
	for _, t := range targets {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			fams, scrapeErr := a.scrapeOne(ctx, t)
			mu.Lock()
			defer mu.Unlock()
			up = append(up, upMetric(t.ID, scrapeErr == nil))
			if scrapeErr != nil {
				return
			}
			for name, fam := range fams {
				if existing, ok := merged[name]; ok {
					// Same metric family from another node: append its series.
					// HELP/TYPE stay from the first node and are written once.
					existing.Metric = append(existing.Metric, fam.Metric...)
				} else {
					merged[name] = fam
				}
			}
		}()
	}
	wg.Wait()

	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic output so scrape diffs are stable

	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	sort.Slice(up, func(i, j int) bool { return up[i].Label[0].GetValue() < up[j].Label[0].GetValue() })
	if err := enc.Encode(upFamily(up)); err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := enc.Encode(merged[name]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (a *mlnodeAggregator) scrapeOne(ctx context.Context, t MLNodeTarget) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mlnode %s: status %d", t.ID, resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("mlnode %s: parse metrics: %w", t.ID, err)
	}
	for _, fam := range fams {
		for _, m := range fam.Metric {
			m.Label = append(m.Label, &dto.LabelPair{Name: strp("mlnode"), Value: strp(t.ID)})
		}
	}
	return fams, nil
}

func upFamily(metrics []*dto.Metric) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name:   strp("mlnode_up"),
		Help:   strp("Whether the mlnode's metrics endpoint was reachable on the last scrape (1 = up)."),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: metrics,
	}
}

func upMetric(id string, ok bool) *dto.Metric {
	v := 0.0
	if ok {
		v = 1.0
	}
	return &dto.Metric{
		Label: []*dto.LabelPair{{Name: strp("mlnode"), Value: strp(id)}},
		Gauge: &dto.Gauge{Value: &v},
	}
}

func strp(s string) *string { return &s }
