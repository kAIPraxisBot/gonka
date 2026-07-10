package artifacts

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// R&D harness for the x0152 copy-on-write proposal: measure before (eager
// in-place Insert + O(N) rebuild-for-proof) vs after (insertCOW + O(depth)
// snapshot proof) on one branch where both implementations coexist.
//
// Env-gated. Run one scale per process so heap is released between runs:
//
//	SMST_RND_N=1000000 go test ./poc/artifacts/ -run TestCOWRnD -v -timeout 40m
//
// commitEvery controls how many COW snapshots are retained (one per commit),
// modelling "hold a version root per committed count" instead of rebuild.

func rndLeaf(i int) (int32, []byte) { return int32(i * 100), smstHashLeaf(testVector(i)) }

func gcStats() (heap uint64, numGC uint32, pauseNs uint64, totalAlloc uint64) {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc, m.NumGC, m.PauseTotalNs, m.TotalAlloc
}

func TestCOWRnD(t *testing.T) {
	v := os.Getenv("SMST_RND_N")
	if v == "" {
		t.Skip("set SMST_RND_N to run the R&D profile")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("bad SMST_RND_N=%q", v)
	}
	commitEvery := n / 32 // ~32 committed snapshots over the stage
	if commitEvery == 0 {
		commitEvery = 1
	}
	// Cap the rebuild-heavy measures (proof latency + DoS) to keep runtime sane;
	// each rebuild is a full O(N) replay.
	rebuildOK := n <= 1_000_000
	dosK := 10

	mb := func(b uint64) float64 { return float64(b) / (1024 * 1024) }
	line := func(f string, a ...interface{}) { t.Logf("RND N=%d :: "+f, append([]interface{}{n}, a...)...) }

	// ---- BEFORE: eager in-place Insert ----
	h0, g0, p0, a0 := gcStats()
	t0 := time.Now()
	before := NewSMST(0)
	for i := 0; i < n; i++ {
		nonce, leaf := rndLeaf(i)
		if _, err := before.Insert(nonce, leaf); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	before.GetRoot()
	ingestBefore := time.Since(t0)
	h1, g1, p1, a1 := gcStats()
	runtime.KeepAlive(before)
	line("BEFORE ingest=%-11s heap=%7.1fMB alloc=%7.1fMB gc=%d pause=%.1fms",
		ingestBefore.Round(time.Millisecond), mb(h1-h0), mb(a1-a0), g1-g0, float64(p1-p0)/1e6)
	beforeRoot, _ := before.GetRoot()
	before = nil

	// ---- AFTER: insertCOW, retaining a snapshot per commit ----
	h2, g2, p2, a2 := gcStats()
	t1 := time.Now()
	after := NewSMST(0)
	var snaps []smstSnapshot
	for i := 0; i < n; i++ {
		nonce, leaf := rndLeaf(i)
		if _, err := after.insertCOW(nonce, leaf); err != nil {
			t.Fatalf("insertCOW %d: %v", i, err)
		}
		if (i+1)%commitEvery == 0 {
			snaps = append(snaps, after.snapshot())
		}
	}
	after.GetRoot()
	ingestAfter := time.Since(t1)
	h3, g3, p3, a3 := gcStats()
	runtime.KeepAlive(after)
	runtime.KeepAlive(snaps)
	line("AFTER  ingest=%-11s heap=%7.1fMB alloc=%7.1fMB gc=%d pause=%.1fms  (+%d retained snapshots)",
		ingestAfter.Round(time.Millisecond), mb(h3-h2), mb(a3-a2), g3-g2, float64(p3-p2)/1e6, len(snaps))

	afterRoot, _ := after.GetRoot()
	if string(beforeRoot) != string(afterRoot) {
		t.Fatalf("ROOT MISMATCH before != after")
	}
	line("root byte-identical: %x", afterRoot[:8])

	// ---- AXIS 2: historical-count proof latency (count = 2N/3) ----
	histCount := uint32(n * 2 / 3)
	// find the snapshot at or before histCount for the AFTER path
	var histSnap smstSnapshot
	for _, s := range snaps {
		if s.count <= histCount {
			histSnap = s
		}
	}
	denseIdx := histSnap.count / 2

	// AFTER: serve proof from the retained snapshot, O(depth).
	tA := time.Now()
	view := after.snapshotView(histSnap)
	_, proofA, err := view.GetLeafByDenseIndex(denseIdx)
	afterProof := time.Since(tA)
	if err != nil {
		t.Fatalf("after proof: %v", err)
	}

	if rebuildOK {
		// BEFORE: rebuild a fresh tree by replaying histSnap.count inserts
		// (models rebuildTreeFromInputs O(N)), then serve one proof.
		tB := time.Now()
		rebuilt := NewSMST(0)
		for i := 0; i < int(histSnap.count); i++ {
			nonce, leaf := rndLeaf(i)
			rebuilt.Insert(nonce, leaf)
		}
		_, proofB, err := rebuilt.GetLeafByDenseIndex(denseIdx)
		beforeProof := time.Since(tB)
		if err != nil {
			t.Fatalf("before proof: %v", err)
		}
		line("AXIS2 proof@%d  BEFORE(rebuild)=%-11s  AFTER(cow)=%-9s  speedup=%.0fx  (proofLen %d/%d)",
			histCount, beforeProof.Round(time.Millisecond), afterProof.Round(time.Microsecond),
			float64(beforeProof)/float64(afterProof), len(proofB), len(proofA))

		// ---- AXIS 6: DoS — K distinct historical counts ----
		counts := make([]uint32, 0, dosK)
		for k := 1; k <= dosK; k++ {
			counts = append(counts, uint32(int(histSnap.count)*k/(dosK+1)))
		}
		// BEFORE: each distinct count = a full rebuild.
		tDB := time.Now()
		for _, c := range counts {
			r := NewSMST(0)
			for i := 0; i < int(c); i++ {
				nonce, leaf := rndLeaf(i)
				r.Insert(nonce, leaf)
			}
			r.GetLeafByDenseIndex(c / 2)
		}
		dosBefore := time.Since(tDB)
		// AFTER: each distinct count served from its nearest retained snapshot.
		tDA := time.Now()
		for _, c := range counts {
			var sc smstSnapshot
			for _, s := range snaps {
				if s.count <= c {
					sc = s
				}
			}
			after.snapshotView(sc).GetLeafByDenseIndex(sc.count / 2)
		}
		dosAfter := time.Since(tDA)
		line("AXIS6 DoS K=%d distinct  BEFORE=%-11s  AFTER=%-9s  speedup=%.0fx",
			dosK, dosBefore.Round(time.Millisecond), dosAfter.Round(time.Microsecond),
			float64(dosBefore)/float64(dosAfter))
	} else {
		line("AXIS2/6 skipped at N=%d (rebuild too heavy); AFTER proof=%s", n, afterProof.Round(time.Microsecond))
	}
	fmt.Fprintln(os.Stderr, "RND-DONE")
}
