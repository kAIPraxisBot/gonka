package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// TestMLNodeMetricsHandler_MergesLabelsAndUp proves the aggregator produces a
// single VALID Prometheus exposition from multiple mlnodes exposing the SAME
// metric name — the case a naive text concatenation breaks (duplicate
// # HELP/# TYPE lines and colliding series). It also checks the mlnode label is
// injected and mlnode_up reflects reachability.
func TestMLNodeMetricsHandler_MergesLabelsAndUp(t *testing.T) {
	body := func(val string) string {
		return "# HELP vllm_num_requests_running Running requests.\n" +
			"# TYPE vllm_num_requests_running gauge\n" +
			"vllm_num_requests_running " + val + "\n"
	}
	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body("3")))
	}))
	defer nodeA.Close()
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body("7")))
	}))
	defer nodeB.Close()

	// A node that is unreachable: start then immediately close so the connect
	// is refused fast.
	down := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	downURL := down.URL
	down.Close()

	list := func() ([]MLNodeTarget, error) {
		return []MLNodeTarget{
			{ID: "node-a", URL: nodeA.URL},
			{ID: "node-b", URL: nodeB.URL},
			{ID: "node-down", URL: downURL},
		}, nil
	}

	h := MLNodeMetricsHandler(list, MLNodeMetricsConfig{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/mlnodes/metrics", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	// The merged output MUST re-parse cleanly — this is the whole point.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(rr.Body.String()))
	if err != nil {
		t.Fatalf("merged output is not valid Prometheus exposition: %v\n---\n%s", err, rr.Body.String())
	}

	// Same metric name from two nodes → 2 series, distinguished by mlnode label.
	fam, ok := fams["vllm_num_requests_running"]
	if !ok {
		t.Fatalf("merged output missing vllm_num_requests_running; got families: %v keys", len(fams))
	}
	if len(fam.Metric) != 2 {
		t.Fatalf("vllm_num_requests_running series = %d, want 2 (one per reachable node)", len(fam.Metric))
	}
	byNode := map[string]float64{}
	for _, m := range fam.Metric {
		var mlnode string
		for _, l := range m.Label {
			if l.GetName() == "mlnode" {
				mlnode = l.GetValue()
			}
		}
		if mlnode == "" {
			t.Fatalf("series without mlnode label: %v", m)
		}
		byNode[mlnode] = m.GetGauge().GetValue()
	}
	if byNode["node-a"] != 3 || byNode["node-b"] != 7 {
		t.Fatalf("values by node = %v, want node-a=3 node-b=7", byNode)
	}

	// mlnode_up must mark the unreachable node down and the others up.
	up, ok := fams["mlnode_up"]
	if !ok {
		t.Fatalf("merged output missing mlnode_up")
	}
	upByNode := map[string]float64{}
	for _, m := range up.Metric {
		for _, l := range m.Label {
			if l.GetName() == "mlnode" {
				upByNode[l.GetValue()] = m.GetGauge().GetValue()
			}
		}
	}
	if upByNode["node-a"] != 1 || upByNode["node-b"] != 1 || upByNode["node-down"] != 0 {
		t.Fatalf("mlnode_up = %v, want node-a=1 node-b=1 node-down=0", upByNode)
	}
}
