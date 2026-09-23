package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// --- minimal goroutine/heap leak detector harness ---
//
// The task described an existing leakcheck_test.go with detectGoroutineLeak /
// renderReport / AssertNoGoroutineLeak, but no such file exists in this tree.
// This is a self-contained equivalent scoped to this audit file.

type leakSite struct {
	site  string
	count int
}

type leakReport struct {
	delta          int
	growth         []leakSite
	heapDeltaBytes int64
	topSample      string
}

// captureStacks returns, per creation-site signature, the number of live
// goroutines, plus a sample full stack for each site.
func captureStacks() (map[string]int, map[string]string) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	counts := map[string]int{}
	samples := map[string]string{}
	blocks := strings.Split(string(buf), "\n\n")
	for _, blk := range blocks {
		blk = strings.TrimSpace(blk)
		if blk == "" {
			continue
		}
		lines := strings.Split(blk, "\n")
		site := ""
		// Prefer the "created by X" frame; else the top user frame.
		for i, ln := range lines {
			if strings.HasPrefix(ln, "created by ") {
				site = strings.TrimPrefix(ln, "created by ")
				_ = i
				break
			}
		}
		if site == "" && len(lines) >= 2 {
			// lines[0] is the "goroutine N [state]:" header; lines[1] is top frame.
			site = strings.TrimSpace(lines[1])
		}
		if site == "" {
			continue
		}
		counts[site]++
		if _, ok := samples[site]; !ok {
			samples[site] = blk
		}
	}
	return counts, samples
}

// settle waits for background goroutines/finalizers to drain, up to ~8s.
func settle() {
	prev := -1
	stable := 0
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(150 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			stable++
			if stable >= 4 {
				return
			}
		} else {
			stable = 0
			prev = n
		}
	}
}

func detectGoroutineLeak(warmup, iters int, fn func()) leakReport {
	for i := 0; i < warmup; i++ {
		fn()
	}
	settle()

	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	baseCounts, _ := captureStacks()
	base := runtime.NumGoroutine()

	for i := 0; i < iters; i++ {
		fn()
	}
	settle()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	afterCounts, afterSamples := captureStacks()
	after := runtime.NumGoroutine()

	var growth []leakSite
	for site, n := range afterCounts {
		if d := n - baseCounts[site]; d > 0 {
			growth = append(growth, leakSite{site: site, count: d})
		}
	}
	sort.Slice(growth, func(i, j int) bool { return growth[i].count > growth[j].count })

	topSample := ""
	if len(growth) > 0 {
		topSample = afterSamples[growth[0].site]
	}

	return leakReport{
		delta:          after - base,
		growth:         growth,
		heapDeltaBytes: int64(msAfter.HeapAlloc) - int64(msBefore.HeapAlloc),
		topSample:      topSample,
	}
}

func renderReport(rep leakReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "net goroutine delta: %d\n", rep.delta)
	fmt.Fprintf(&b, "heap delta bytes: %d\n", rep.heapDeltaBytes)
	if len(rep.growth) == 0 {
		fmt.Fprintf(&b, "per-site growth: none\n")
	} else {
		fmt.Fprintf(&b, "per-site growth:\n")
		for _, g := range rep.growth {
			fmt.Fprintf(&b, "  +%d  %s\n", g.count, g.site)
		}
		fmt.Fprintf(&b, "top offender sample stack:\n%s\n", rep.topSample)
	}
	return b.String()
}

// --- the audit ---

// buildAdminAuditGateway constructs a multi-runtime Gateway backed by a real
// sqlite store, matching production wiring for the read-only admin/status
// endpoints (LoadState / snapshot / limiter+capacity), without starting any
// background loops.
func buildAdminAuditGateway(t *testing.T) *Gateway {
	t.Helper()

	store, err := NewGatewayStore(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("NewGatewayStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	settings := GatewaySettings{
		ChainREST:               "http://node:1317",
		PublicAPI:               "http://api:9000",
		DefaultModel:            "Qwen/Test",
		DefaultRequestMaxTokens: 1024,
		MaxConcurrentRequests:   5,
		MaxInputTokensInFlight:  999,
		ModelLimits: []GatewayModelLimitSettings{
			{ModelID: "Qwen/Test", MaxConcurrentRequests: 7, MaxInputTokensInFlight: 700, AccessMode: "open"},
			{ModelID: "Kimi/Test", MaxConcurrentRequests: 3, MaxInputTokensInFlight: 300, AccessMode: "open"},
		},
		EscrowRotation: EscrowRotationSettings{
			Enabled:           true,
			SettlementEnabled: true,
			Models: []EscrowRotationModelSettings{{
				ModelID:       "Qwen/Test",
				TempCount:     1,
				TargetCount:   2,
				Amount:        1000,
				PrivateKeyEnv: "DEVSHARD_PRIVATE_KEY",
			}},
		},
	}.WithTuningDefaults()

	devshards := []GatewayDevshardState{
		{RuntimeConfig: RuntimeConfig{ID: "12", PrivateKeyHex: "secret", Model: "Qwen/Test"}, Active: true, RotationRole: rotationRoleRegular, RotationEpoch: 1},
		{RuntimeConfig: RuntimeConfig{ID: "13", PrivateKeyHex: "secret", Model: "Kimi/Test"}, Active: true, RotationRole: rotationRoleRegular, RotationEpoch: 1},
	}
	if err := store.Initialize(settings, devshards); err != nil {
		t.Fatalf("store.Initialize: %v", err)
	}

	// Two runtimes so the pooled/admin handlers take the multi-runtime
	// snapshot path (not the single-runtime delegate path).
	rtA := &devshardRuntime{id: "12", model: "Qwen/Test"}
	rtB := &devshardRuntime{id: "13", model: "Kimi/Test"}
	g := NewGateway([]*devshardRuntime{rtA, rtB}, NewGatewayLimiter(10, 1000), "Qwen/Test")
	g.store = store
	g.settings = settings
	g.phaseGate = &ChainPhaseGate{}
	return g
}

func TestLeakAudit_admin_and_store(t *testing.T) {
	g := buildAdminAuditGateway(t)
	handler := g.Handler()

	// Rotate across the read-only admin + status + store-backed endpoints.
	endpoints := []string{
		"/v1/admin/state",   // handleAdminState -> store.LoadState + snapshots + limiter/capacity
		"/v1/status",        // handlePooledStatus -> snapshots + limiter/capacity
		"/v1/admin/settings", // handleAdminSettings GET
		"/v1/debug/rotation", // handleDebugRotation -> store.LoadRotationStatuses
	}

	i := 0
	drive := func() {
		path := endpoints[i%len(endpoints)]
		i++
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("endpoint %s: unexpected status %d body=%s", path, rec.Code, rec.Body.String())
		}
	}

	// warmup 8, then 48 measured requests (12 per endpoint).
	rep := detectGoroutineLeak(8, 48, drive)
	t.Logf("admin/status/store leak report (48 requests across %d endpoints):\n%s", len(endpoints), renderReport(rep))

	if rep.delta > 4 {
		t.Errorf("net goroutine growth after 48 admin/store requests: delta=%d (per-request ~%.2f)",
			rep.delta, float64(rep.delta)/48.0)
	}
}
