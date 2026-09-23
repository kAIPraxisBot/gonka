package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// leakcheck_test.go provides a small goroutine/heap leak detector used by the
// leak_audit_*_test.go surface audits. It captures a goroutine profile before
// and after driving an operation N times, settling long enough that async
// background finalizers (e.g. metaDrain / speculative-loser finalizers) drain,
// then reports the net per-creation-site growth.

type siteCount struct {
	site  string
	count int
}

type leakReport struct {
	delta          int
	growth         []siteCount
	heapDeltaBytes int64
	topStack       string
}

// captureProfile returns (total goroutines, count-per-creation-site, one
// sample stack per creation-site). The creation site is the "created by ..."
// frame, which is stable per spawn location and thus a good leak fingerprint.
func captureProfile() (int, map[string]int, map[string]string) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	blocks := strings.Split(string(buf), "\n\n")
	counts := map[string]int{}
	samples := map[string]string{}
	total := 0
	for _, blk := range blocks {
		blk = strings.TrimSpace(blk)
		if blk == "" || !strings.HasPrefix(blk, "goroutine ") {
			continue
		}
		total++
		site := "<no-creator>"
		for _, line := range strings.Split(blk, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "created by ") {
				site = strings.TrimPrefix(line, "created by ")
				// drop trailing " in goroutine N"
				if idx := strings.Index(site, " in goroutine "); idx >= 0 {
					site = site[:idx]
				}
				break
			}
		}
		counts[site]++
		if _, ok := samples[site]; !ok {
			samples[site] = blk
		}
	}
	return total, counts, samples
}

func settle() {
	// Poll until the goroutine count stabilizes or ~8s elapses, forcing GC so
	// finalizers and timer goroutines get a chance to unwind.
	deadline := time.Now().Add(8 * time.Second)
	prev := runtime.NumGoroutine()
	stable := 0
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(150 * time.Millisecond)
		cur := runtime.NumGoroutine()
		if cur <= prev {
			stable++
			if stable >= 6 {
				break
			}
		} else {
			stable = 0
		}
		prev = cur
	}
	runtime.GC()
}

func detectGoroutineLeak(warmup, iters int, fn func()) leakReport {
	for i := 0; i < warmup; i++ {
		fn()
	}
	settle()

	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	base, baseCounts, _ := captureProfile()

	for i := 0; i < iters; i++ {
		fn()
	}
	settle()

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	final, finalCounts, finalSamples := captureProfile()

	growth := []siteCount{}
	for site, c := range finalCounts {
		if d := c - baseCounts[site]; d > 0 {
			growth = append(growth, siteCount{site: site, count: d})
		}
	}
	sort.Slice(growth, func(i, j int) bool { return growth[i].count > growth[j].count })

	topStack := ""
	if len(growth) > 0 {
		topStack = finalSamples[growth[0].site]
	}

	return leakReport{
		delta:          final - base,
		growth:         growth,
		heapDeltaBytes: int64(m1.HeapAlloc) - int64(m0.HeapAlloc),
		topStack:       topStack,
	}
}

func renderReport(rep leakReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== goroutine leak report ===\n")
	fmt.Fprintf(&b, "net goroutine delta: %d\n", rep.delta)
	fmt.Fprintf(&b, "heap delta: %d bytes\n", rep.heapDeltaBytes)
	if len(rep.growth) == 0 {
		fmt.Fprintf(&b, "per-site growth: <none>\n")
	} else {
		fmt.Fprintf(&b, "per-site growth (desc):\n")
		for _, g := range rep.growth {
			fmt.Fprintf(&b, "  +%d  %s\n", g.count, g.site)
		}
		fmt.Fprintf(&b, "top offender sample stack:\n%s\n", rep.topStack)
	}
	fmt.Fprintf(&b, "=============================\n")
	return b.String()
}

func AssertNoGoroutineLeak(t *testing.T, warmup, iters, tolerance int, fn func()) {
	t.Helper()
	rep := detectGoroutineLeak(warmup, iters, fn)
	t.Logf("%s", renderReport(rep))
	if rep.delta > tolerance {
		t.Fatalf("goroutine leak: net delta %d exceeds tolerance %d", rep.delta, tolerance)
	}
}
