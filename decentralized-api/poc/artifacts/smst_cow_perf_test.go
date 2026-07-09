package artifacts

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func cowPerfN(def int) int {
	if v := os.Getenv("SMST_PERF_N"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func cowSeqNonces(n int) []int32 {
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		out[i] = int32(i)
	}
	return out
}

// TestCOWSnapshotProofPerf reports the per-request cost of a historical
// snapshot proof both ways: the current path rebuilds the whole tree from the
// log (O(N)) before serving; the COW path serves from a retained snapshot
// (O(depth)). Set SMST_PERF_N to measure at 1_000_000 / 10_000_000.
func TestCOWSnapshotProofPerf(t *testing.T) {
	n := cowPerfN(100000)
	nonces := cowSeqNonces(n)
	leaves := make([][]byte, n)
	for i, nonce := range nonces {
		leaves[i] = cowLeaf(nonce)
	}

	var m0, m1 runtime.MemStats

	// Current path: build (== rebuild) cost with the in-place mutating insert.
	runtime.GC()
	runtime.ReadMemStats(&m0)
	t0 := time.Now()
	mut := NewSMST(0)
	for i, nonce := range nonces {
		if _, err := mut.Insert(nonce, leaves[i]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	buildMut := time.Since(t0)
	runtime.ReadMemStats(&m1)
	heapMut := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	// COW build cost + a retained snapshot (the proposed steady state).
	runtime.GC()
	runtime.ReadMemStats(&m0)
	t0 = time.Now()
	cow := NewSMST(0)
	for i, nonce := range nonces {
		if _, err := cow.insertCOW(nonce, leaves[i]); err != nil {
			t.Fatalf("insertCOW: %v", err)
		}
	}
	buildCow := time.Since(t0)
	_ = cow.snapshot()
	runtime.ReadMemStats(&m1)
	heapCow := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	mid := uint32(n / 2)

	// Current per-request: rebuild a fresh tree from the first `mid` log entries,
	// then serve one proof.
	t0 = time.Now()
	rebuilt := NewSMST(0)
	for i := uint32(0); i < mid; i++ {
		if _, err := rebuilt.Insert(nonces[i], leaves[i]); err != nil {
			t.Fatalf("rebuild insert: %v", err)
		}
	}
	if _, _, err := rebuilt.GetLeafByDenseIndex(mid / 2); err != nil {
		t.Fatalf("rebuild serve: %v", err)
	}
	serveRebuild := time.Since(t0)

	// Proposed per-request: serve one proof from a snapshot retained at `mid`.
	var serveCow time.Duration
	{
		s := NewSMST(0)
		for i := uint32(0); i < mid; i++ {
			if _, err := s.insertCOW(nonces[i], leaves[i]); err != nil {
				t.Fatalf("cow build: %v", err)
			}
		}
		midSnap := s.snapshot()
		t0 = time.Now()
		view := s.snapshotView(midSnap)
		if _, _, err := view.GetLeafByDenseIndex(mid / 2); err != nil {
			t.Fatalf("cow serve: %v", err)
		}
		serveCow = time.Since(t0)
	}

	t.Logf("N=%d", n)
	t.Logf("build (mutating / current): %v, heap +%d MB", buildMut, heapMut>>20)
	t.Logf("build (cow):                %v, heap +%d MB", buildCow, heapCow>>20)
	t.Logf("snapshot proof, CURRENT (rebuild O(N) + serve): %v", serveRebuild)
	t.Logf("snapshot proof, COW (retained snapshot O(depth)): %v", serveCow)
	if serveCow > 0 {
		t.Logf("per-request speedup: %.0fx", float64(serveRebuild)/float64(serveCow))
	}
}
