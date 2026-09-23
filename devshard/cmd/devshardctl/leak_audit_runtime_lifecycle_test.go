package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// TestLeakAudit_runtime_lifecycle drives the gateway devshard-lifecycle admin
// surface — add (build runtime via the faked builder) -> deactivate -> clean
// (close runtime + delete state) — repeatedly and measures net goroutine growth
// per full cycle. The runtime builder is faked (gatewayRuntimeBuilder) so no
// chain/session is needed; this isolates the gateway bookkeeping machinery
// (runtimes map, runtimeOrder, capacity membership, phaseGate wiring, rt.close).
func TestLeakAudit_runtime_lifecycle(t *testing.T) {
	store, err := NewGatewayStore(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Initialize(GatewaySettings{
		ChainREST:               "http://node:1317",
		PublicAPI:               "http://api:9000",
		DefaultModel:            "Qwen/Test",
		DefaultRequestMaxTokens: 1000,
		MaxConcurrentRequests:   2,
		MaxInputTokensInFlight:  200,
	}, nil); err != nil {
		t.Fatalf("init store: %v", err)
	}

	prevBuilder := gatewayRuntimeBuilder
	t.Cleanup(func() { gatewayRuntimeBuilder = prevBuilder })
	// Realistic-shape fake: returns a runtime carrying a Proxy and a participant
	// slot map (so attachRuntimeSharedState / capacity membership do real work),
	// but no user.Session (session-build/close is exercised separately / is
	// goroutine-free at construction — see the report). close() is a no-op here.
	gatewayRuntimeBuilder = func(cfg RuntimeConfig, chainREST, defaultModel string, perf *PerfTracker) (*devshardRuntime, error) {
		rt := &devshardRuntime{
			id:                    cfg.ID,
			model:                 firstNonEmpty(cfg.Model, defaultModel),
			proxy:                 &Proxy{},
			participantSlotCounts: map[string]int{"host-a": 1, "host-b": 2},
			participantKeys:       []string{"host-a", "host-b"},
		}
		rt.active.Store(true)
		rt.activeConfigured = true
		return rt, nil
	}

	g := NewGateway(nil, NewGatewayLimiter(2, 200), "Qwen/Test")
	g.store = store
	g.baseStorageDir = t.TempDir()
	g.phaseGate = &ChainPhaseGate{}

	const id = "12"

	addBody := `{"id":"` + id + `","private_key_env":"DEVSHARD_12_PRIVATE_KEY","model":"Qwen/Test"}`

	cycle := func() {
		// add (build)
		rec := httptest.NewRecorder()
		g.handleAdminAddDevshard(rec,
			httptest.NewRequest(http.MethodPost, "/v1/admin/devshards", strings.NewReader(addBody)))
		if rec.Code != http.StatusOK {
			t.Fatalf("add: code=%d body=%s", rec.Code, rec.Body.String())
		}
		// deactivate
		rec = httptest.NewRecorder()
		g.handleAdminDeactivateDevshard(rec,
			httptest.NewRequest(http.MethodPost, "/v1/admin/devshards/"+id+"/deactivate", nil), id)
		if rec.Code != http.StatusOK {
			t.Fatalf("deactivate: code=%d body=%s", rec.Code, rec.Body.String())
		}
		// clean (close + delete)
		rec = httptest.NewRecorder()
		g.handleAdminCleanDevshard(rec,
			httptest.NewRequest(http.MethodDelete, "/v1/admin/devshards/"+id, nil), id)
		if rec.Code != http.StatusOK {
			t.Fatalf("clean: code=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	rep := detectGoroutineLeak(2 /*warmup*/, 8 /*iters*/, cycle)
	t.Logf("runtime add/deactivate/clean lifecycle (fake builder):\n%s", renderReport(rep))
	fmt.Print("") // keep fmt import if renderReport signature changes
}
