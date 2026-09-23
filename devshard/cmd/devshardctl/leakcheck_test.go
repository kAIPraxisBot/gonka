package main

// Goroutine/heap leak-hunting harness for the gateway.
//
// Answers a concrete need: a test that measures HOW MANY goroutines (and how
// much heap) an operation leaves behind, and WHERE those goroutines were
// created, so we can hunt leaks in the gateway request path and PROVE a fix in
// CI instead of eyeballing a live pprof dump.
//
// Usage — wrap any operation you suspect leaks (an admin call, a request
// through a runtime handler, a build/close cycle):
//
//	AssertNoGoroutineLeak(t, 1 /*warmup*/, 200 /*iters*/, 2 /*tolerance*/, func() {
//		// drive one gateway operation here
//	})
//
// On failure it prints the per-creation-site growth (e.g. "+200  <func> at
// session.go:1884") plus one sample stack for the top offender, so you see
// both the count and the exact spawn site — the same information a
// /debug/pprof/goroutine?debug=2 dump gives, but as a repeatable assertion.

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// goroutineSites returns creation-site -> count for every live goroutine, plus
// one sample full stack per site. The "creation site" is Go's
// "created by <func> in goroutine N" frame — i.e. exactly WHERE the goroutine
// was spawned. The varying "in goroutine N" suffix is stripped so the same
// spawn site aggregates across calls.
func goroutineSites() (counts map[string]int, sample map[string]string) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	counts = map[string]int{}
	sample = map[string]string{}
	for _, block := range strings.Split(string(buf), "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		site := "«no creator (top-level/runtime)»"
		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "created by ") {
				s := strings.TrimPrefix(line, "created by ")
				if i := strings.Index(s, " in goroutine "); i >= 0 {
					s = s[:i]
				}
				site = s
				break
			}
		}
		counts[site]++
		if _, ok := sample[site]; !ok {
			sample[site] = block
		}
	}
	return counts, sample
}

func sumCounts(m map[string]int) int {
	total := 0
	for _, c := range m {
		total += c
	}
	return total
}

// settle gives transient (correctly-exiting) goroutines time to finish, so we
// don't mistake in-flight work for a leak. It returns once the live goroutine
// count reaches target, or stops shrinking for a few reads, or the timeout
// expires.
func settle(target int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	prev, stable := -1, 0
	for time.Now().Before(deadline) {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= target {
			return
		}
		if n == prev {
			if stable++; stable >= 4 {
				return
			}
		} else {
			stable = 0
		}
		prev = n
		time.Sleep(20 * time.Millisecond)
	}
}

type siteDelta struct {
	site  string
	count int
}

type leakReport struct {
	baseTotal, afterTotal, delta int
	growth                       []siteDelta
	sample                       map[string]string
	heapDeltaBytes               int64
}

// detectGoroutineLeak runs fn `iters` times and reports the net goroutine
// growth attributed per creation site, plus heap growth. `warmup` runs happen
// first (and are allowed to settle) so one-time singletons/pools spun up on the
// first call are not counted as a leak.
func detectGoroutineLeak(warmup, iters int, fn func()) leakReport {
	for i := 0; i < warmup; i++ {
		fn()
	}
	settle(0, 2*time.Second)

	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	baseCounts, _ := goroutineSites()
	base := sumCounts(baseCounts)

	for i := 0; i < iters; i++ {
		fn()
	}
	settle(base, 8*time.Second) // allow async background finalizers to drain

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	afterCounts, sample := goroutineSites()
	after := sumCounts(afterCounts)

	var growth []siteDelta
	for site, c := range afterCounts {
		if d := c - baseCounts[site]; d > 0 {
			growth = append(growth, siteDelta{site, d})
		}
	}
	sort.Slice(growth, func(i, j int) bool { return growth[i].count > growth[j].count })

	return leakReport{base, after, after - base, growth, sample, int64(m1.HeapInuse) - int64(m0.HeapInuse)}
}

func renderReport(rep leakReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "goroutines: %d -> %d (Δ%+d) | heapInuse Δ%+d KiB\n",
		rep.baseTotal, rep.afterTotal, rep.delta, rep.heapDeltaBytes/1024)
	if len(rep.growth) == 0 {
		b.WriteString("  (no per-site growth)\n")
		return b.String()
	}
	b.WriteString("  growth by creation site:\n")
	for i, g := range rep.growth {
		fmt.Fprintf(&b, "    +%-5d %s\n", g.count, g.site)
		if i == 0 {
			if s, ok := rep.sample[g.site]; ok {
				fmt.Fprintf(&b, "      ── sample stack of the top offender ──\n%s\n",
					"        "+strings.ReplaceAll(s, "\n", "\n        "))
			}
		}
		if i >= 9 {
			fmt.Fprintf(&b, "    … and %d more sites\n", len(rep.growth)-10)
			break
		}
	}
	return b.String()
}

// AssertNoGoroutineLeak fails the test if fn leaves more than `tolerance` net
// goroutines behind after `iters` runs, printing where they were created.
func AssertNoGoroutineLeak(t *testing.T, warmup, iters, tolerance int, fn func()) {
	t.Helper()
	rep := detectGoroutineLeak(warmup, iters, fn)
	if rep.delta > tolerance {
		t.Errorf("goroutine leak: %d net new goroutines after %d iterations (tolerance %d)\n%s",
			rep.delta, iters, tolerance, renderReport(rep))
	}
}

// TestLeakDetectorReportsCountAndSite is the fail-safe control: a KNOWN leaker
// (each call parks a goroutine forever) must be detected, counted, and
// attributed to the exact spawn site. Proves the harness actually finds leaks
// and says where — so a green result on a real op is trustworthy.
func TestLeakDetectorReportsCountAndSite(t *testing.T) {
	block := make(chan struct{}) // never closed -> spawned goroutines park
	t.Cleanup(func() { close(block) })
	leak := func() { go func() { <-block }() }

	const iters = 20
	rep := detectGoroutineLeak(1, iters, leak)

	if rep.delta < iters-1 { // tiny scheduler slack
		t.Fatalf("detector MISSED the leak: Δ=%d, want ~%d\n%s", rep.delta, iters, renderReport(rep))
	}
	if len(rep.growth) == 0 || !strings.Contains(rep.growth[0].site, "TestLeakDetectorReportsCountAndSite") {
		got := "(none)"
		if len(rep.growth) > 0 {
			got = rep.growth[0].site
		}
		t.Fatalf("top leak site = %q, want the injected leaker's site\n%s", got, renderReport(rep))
	}
	t.Logf("control OK — detector found the leak:\n%s", renderReport(rep))
}

// TestLeakDetectorNoFalsePositiveOnCleanOp guards against crying wolf: a
// well-behaved op whose goroutine exits promptly must not be flagged.
func TestLeakDetectorNoFalsePositiveOnCleanOp(t *testing.T) {
	clean := func() {
		done := make(chan struct{})
		go func() { close(done) }()
		<-done
	}
	rep := detectGoroutineLeak(2, 50, clean)
	if rep.delta > 2 { // small slack for runtime bookkeeping
		t.Fatalf("false positive on a clean op: Δ=%d\n%s", rep.delta, renderReport(rep))
	}
}
