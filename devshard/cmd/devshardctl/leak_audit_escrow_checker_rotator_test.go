package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- self-contained goroutine leak detector (harness file absent in this worktree) ---

type auditSite struct {
	site  string
	count int
}

type auditReport struct {
	delta  int
	growth []auditSite
	sample string
}

func auditGoroutineSites() map[string]int {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	stacks := strings.Split(string(buf[:n]), "\n\n")
	sites := make(map[string]int)
	for _, s := range stacks {
		lines := strings.Split(s, "\n")
		// find the creation site "created by ..." if present, else the deepest frame
		created := ""
		for i, ln := range lines {
			if strings.HasPrefix(ln, "created by ") {
				loc := ""
				if i+1 < len(lines) {
					loc = strings.TrimSpace(lines[i+1])
				}
				created = strings.TrimPrefix(ln, "created by ") + " @ " + loc
				break
			}
		}
		if created == "" {
			continue
		}
		sites[created]++
	}
	return sites
}

func auditSampleStack(sitePrefix string) string {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	stacks := strings.Split(string(buf[:n]), "\n\n")
	for _, s := range stacks {
		if strings.Contains(s, sitePrefix) {
			return s
		}
	}
	return ""
}

// detectAuditLeak runs fn warmup+iters times, settles, and reports net goroutine
// growth by creation site.
func detectAuditLeak(warmup, iters int, fn func()) auditReport {
	for i := 0; i < warmup; i++ {
		fn()
	}
	// settle before baseline
	settle()
	base := auditGoroutineSites()
	baseCount := runtime.NumGoroutine()

	for i := 0; i < iters; i++ {
		fn()
	}
	settle()
	after := auditGoroutineSites()
	afterCount := runtime.NumGoroutine()

	var growth []auditSite
	for site, c := range after {
		delta := c - base[site]
		if delta > 0 {
			growth = append(growth, auditSite{site: site, count: delta})
		}
	}
	sort.Slice(growth, func(i, j int) bool { return growth[i].count > growth[j].count })
	rep := auditReport{delta: afterCount - baseCount, growth: growth}
	if len(growth) > 0 {
		rep.sample = auditSampleStack(strings.Split(growth[0].site, " @ ")[0])
	}
	return rep
}

func settle() {
	// Drain async background finalizers / connection close loops.
	for i := 0; i < 8; i++ {
		runtime.GC()
		time.Sleep(250 * time.Millisecond)
	}
}

func renderAuditReport(rep auditReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "net goroutine delta = %d\n", rep.delta)
	fmt.Fprintf(&b, "per-site growth (%d sites):\n", len(rep.growth))
	for _, g := range rep.growth {
		fmt.Fprintf(&b, "  +%d  %s\n", g.count, g.site)
	}
	if rep.sample != "" {
		fmt.Fprintf(&b, "top offender sample stack:\n%s\n", rep.sample)
	}
	return b.String()
}

// --- the actual audit ---

func TestLeakAudit_escrow_checker_rotator(t *testing.T) {
	const warmup, iters = 3, 8

	// Case A: escrow checker against an UNREACHABLE chain REST (connection refused
	// immediately). This is the "broken/unreachable escrow" case. TriggerCheck is
	// launched as `go ...` in production (attachEscrowChecker), so model that: each
	// op spawns a goroutine that does one bounded GET.
	t.Run("checker_unreachable", func(t *testing.T) {
		// grab a port then close the listener so connects are refused fast.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		refusedURL := "http://" + ln.Addr().String()
		ln.Close()

		checker := NewEscrowChecker(func() string { return refusedURL })
		var deact atomic.Int64

		rep := detectAuditLeak(warmup, iters, func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// unique escrow id each op so the inflight dedup never short-circuits
				checker.TriggerCheck(fmt.Sprintf("refused-%d", time.Now().UnixNano()),
					func() { deact.Add(1) })
			}()
			wg.Wait()
		})
		t.Logf("checker_unreachable:\n%s", renderAuditReport(rep))
	})

	// Case B: escrow checker against a healthy httptest server that returns
	// found:false (host reported false escrow-not-found -> deactivate path). Each
	// TriggerCheck builds a NEW RESTBridge => NEW http.Client/Transport. A fresh
	// transport that is never CloseIdleConnections'd can leave a keep-alive
	// readLoop/writeLoop goroutine pair per op against a keep-alive server.
	t.Run("checker_found_false_keepalive", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"found": false}`)
		}))
		defer srv.Close()

		checker := NewEscrowChecker(func() string { return srv.URL })
		var deact atomic.Int64

		rep := detectAuditLeak(warmup, iters, func() {
			checker.TriggerCheck(fmt.Sprintf("nf-%d", time.Now().UnixNano()),
				func() { deact.Add(1) })
		})
		t.Logf("checker_found_false_keepalive (deactivated=%d):\n%s", deact.Load(), renderAuditReport(rep))
	})

	// Case C: escrow checker against a server that ACCEPTS but hangs (never
	// responds). The RESTBridge client has a 10s Timeout, so each GET is bounded.
	// We run the op synchronously; detector settles ~2s after, but the 10s bound
	// means any inflight goroutine drains well within the settle window on the
	// NEXT op boundary. This probes whether an unresponsive chain wedges the
	// checker goroutine permanently.
	t.Run("checker_hanging_bounded", func(t *testing.T) {
		block := make(chan struct{})
		defer close(block)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-block:
			case <-r.Context().Done():
			case <-time.After(15 * time.Second):
			}
		}))
		defer srv.Close()

		checker := NewEscrowChecker(func() string { return srv.URL })
		var deact atomic.Int64

		// fewer iters: each op blocks up to 10s (client Timeout).
		rep := detectAuditLeak(1, 3, func() {
			checker.TriggerCheck(fmt.Sprintf("hang-%d", time.Now().UnixNano()),
				func() { deact.Add(1) })
		})
		t.Logf("checker_hanging_bounded:\n%s", renderAuditReport(rep))
	})
}
