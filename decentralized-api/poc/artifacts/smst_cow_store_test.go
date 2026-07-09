package artifacts

import (
	"fmt"
	"sync"
	"testing"
)

// TestStoreRetainedSnapshotServesHistoricalProofs drives the real store: it adds
// artifacts with periodic flushes, then requests proofs at an early flushed
// count that the live tree has long grown past. The count is served from a
// retained COW snapshot (asserted), and every proof verifies against the
// authoritative flushed root.
func TestStoreRetainedSnapshotServesHistoricalProofs(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	defer store.Close()

	const flushEvery = 100
	const total = 1000
	var flushCounts []uint32
	for i := 0; i < total; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), fmt.Sprintf("node-%d", i%4)); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if (i+1)%flushEvery == 0 {
			if err := store.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			flushCounts = append(flushCounts, store.Count())
		}
	}

	k := flushCounts[1] // early flushed count, far below live count
	if _, ok := store.retainedSnapshotView(k); !ok {
		t.Fatalf("expected count %d to be retained", k)
	}

	rootK, err := store.GetRootAt(k)
	if err != nil {
		t.Fatalf("GetRootAt(%d): %v", k, err)
	}

	view, _ := store.retainedSnapshotView(k)
	vr, vc := view.GetRoot()
	if vc != k || !bytesEqual(vr, rootK) {
		t.Fatalf("retained view root/count != flushed root at %d", k)
	}

	entries, err := store.GetArtifactsAndProofs([]uint32{0, k / 2, k - 1}, k)
	if err != nil {
		t.Fatalf("GetArtifactsAndProofs: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for _, e := range entries {
		leaf := encodeLeaf(e.Nonce, e.Vector)
		if !VerifySMSTProofSlice(rootK, k, e.Nonce, leaf, e.Proof) {
			t.Fatalf("proof failed at denseIndex=%d nonce=%d count=%d", e.DenseIndex, e.Nonce, k)
		}
	}
}

// TestStoreNonRetainedCountFallsBackToRebuild requests a snapshot at a non-flush
// count (never retained), proving the rebuild fallback still serves a correct,
// verifiable proof.
func TestStoreNonRetainedCountFallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	defer store.Close()

	for i := 0; i < 500; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), "n"); err != nil {
			t.Fatalf("add: %v", err)
		}
		if (i+1)%100 == 0 {
			if err := store.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}

	mid := uint32(150) // between flush boundaries -> not retained
	if _, ok := store.retainedSnapshotView(mid); ok {
		t.Fatalf("count %d unexpectedly retained", mid)
	}

	rootMid, err := store.GetRootAt(mid)
	if err != nil {
		t.Fatalf("GetRootAt(%d): %v", mid, err)
	}
	entries, err := store.GetArtifactsAndProofs([]uint32{0, mid - 1}, mid)
	if err != nil {
		t.Fatalf("GetArtifactsAndProofs: %v", err)
	}
	for _, e := range entries {
		leaf := encodeLeaf(e.Nonce, e.Vector)
		if !VerifySMSTProofSlice(rootMid, mid, e.Nonce, leaf, e.Proof) {
			t.Fatalf("rebuild-fallback proof failed at nonce=%d count=%d", e.Nonce, mid)
		}
	}
}

// TestStoreEvictedFlushCountFallsBackToRebuild pushes more flushes than the
// retention window, then proves a flush count evicted from retention still
// serves a correct, verifiable proof via the rebuild fallback.
func TestStoreEvictedFlushCountFallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	defer store.Close()

	const flushEvery = 10
	totalFlushes := retainedSnapshotWindow + 5
	var firstFlushCount uint32
	n := 0
	for f := 0; f < totalFlushes; f++ {
		for i := 0; i < flushEvery; i++ {
			if err := store.AddWithNode(int32(n), testVector(n), "x"); err != nil {
				t.Fatalf("add: %v", err)
			}
			n++
		}
		if err := store.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if f == 0 {
			firstFlushCount = store.Count()
		}
	}

	if _, ok := store.retainedSnapshotView(firstFlushCount); ok {
		t.Fatalf("count %d should have been evicted from the retention window", firstFlushCount)
	}

	rootK, err := store.GetRootAt(firstFlushCount)
	if err != nil {
		t.Fatalf("GetRootAt(%d): %v", firstFlushCount, err)
	}
	entries, err := store.GetArtifactsAndProofs([]uint32{0, firstFlushCount - 1}, firstFlushCount)
	if err != nil {
		t.Fatalf("GetArtifactsAndProofs: %v", err)
	}
	for _, e := range entries {
		leaf := encodeLeaf(e.Nonce, e.Vector)
		if !VerifySMSTProofSlice(rootK, firstFlushCount, e.Nonce, leaf, e.Proof) {
			t.Fatalf("evicted-count rebuild-fallback proof failed nonce=%d", e.Nonce)
		}
	}
}

// TestStoreConcurrentProofsDuringInserts proves the COW property makes a
// retained snapshot immune to concurrent mutation: readers verify proofs at a
// pinned count while a writer keeps inserting and flushing. Run under -race.
func TestStoreConcurrentProofsDuringInserts(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	defer store.Close()

	const seed = 300
	for i := 0; i < seed; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), "n"); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	k := store.Count()
	rootK, err := store.GetRootAt(k)
	if err != nil {
		t.Fatalf("GetRootAt: %v", err)
	}

	stop := make(chan struct{})
	var writerWg, readerWg sync.WaitGroup

	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		i := seed
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = store.AddWithNode(int32(i), testVector(i), "n")
			i++
			if i%50 == 0 {
				_ = store.Flush()
			}
		}
	}()

	for r := 0; r < 4; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for j := 0; j < 300; j++ {
				entries, err := store.GetArtifactsAndProofs([]uint32{0, k / 2, k - 1}, k)
				if err != nil {
					t.Errorf("concurrent proof: %v", err)
					return
				}
				for _, e := range entries {
					leaf := encodeLeaf(e.Nonce, e.Vector)
					if !VerifySMSTProofSlice(rootK, k, e.Nonce, leaf, e.Proof) {
						t.Errorf("concurrent proof failed nonce=%d", e.Nonce)
						return
					}
				}
			}
		}()
	}

	readerWg.Wait()
	close(stop)
	writerWg.Wait()
}

// TestCOWSnapshotViewNonceDomainGuard proves a snapshot view answers nonce
// existence exactly like the map-based rebuild path — in particular that an
// out-of-domain nonce (>= 2^depth) that collides mod 2^depth with a present
// nonce is reported absent, not resolved to the colliding leaf.
func TestCOWSnapshotViewNonceDomainGuard(t *testing.T) {
	present := []int32{5, 7, 99999}
	tree := NewSMST(0)
	rebuilt := NewSMST(0)
	for _, n := range present {
		if _, err := tree.insertCOW(n, cowLeaf(n)); err != nil {
			t.Fatalf("insertCOW %d: %v", n, err)
		}
		if _, err := rebuilt.Insert(n, cowLeaf(n)); err != nil {
			t.Fatalf("insert %d: %v", n, err)
		}
	}
	view := tree.snapshotView(tree.snapshot())
	if view.depth != smstDefaultDepth {
		t.Fatalf("expected depth %d, got %d", smstDefaultDepth, view.depth)
	}

	collide := int32(5) + (1 << smstDefaultDepth) // ≡ 5 (mod 2^24), but >= 2^24
	if view.HasNonce(collide) != rebuilt.HasNonce(collide) {
		t.Fatalf("view.HasNonce(%d)=%v disagrees with rebuild=%v", collide, view.HasNonce(collide), rebuilt.HasNonce(collide))
	}
	if view.HasNonce(collide) {
		t.Fatalf("out-of-domain collide nonce %d wrongly reported present", collide)
	}
	if _, err := view.denseIndexForNonce(collide); err == nil {
		t.Fatalf("denseIndexForNonce(%d) must error for out-of-domain nonce", collide)
	}
	for _, n := range present {
		if !view.HasNonce(n) {
			t.Fatalf("present nonce %d reported absent", n)
		}
	}
}

// TestCOWSnapshotDepthAtNAfterExpansion proves a snapshot captured before a
// depth expansion still serves at the depth in force at that count: its root and
// proofs match a from-scratch replay of exactly those leaves, even though the
// live tree has since expanded deeper.
func TestCOWSnapshotDepthAtNAfterExpansion(t *testing.T) {
	small := []int32{1, 5, 100, 12345, 99999}
	tree := NewSMST(0)
	for _, n := range small {
		if _, err := tree.insertCOW(n, cowLeaf(n)); err != nil {
			t.Fatalf("insert %d: %v", n, err)
		}
	}
	snap := tree.snapshot()
	if snap.depth != smstDefaultDepth {
		t.Fatalf("expected snapshot depth %d, got %d", smstDefaultDepth, snap.depth)
	}

	big := int32(1 << 25) // requires depth 26 -> forces expansion
	if _, err := tree.insertCOW(big, cowLeaf(big)); err != nil {
		t.Fatalf("insert big: %v", err)
	}
	if tree.Depth() <= smstDefaultDepth {
		t.Fatalf("expected expansion beyond %d, got %d", smstDefaultDepth, tree.Depth())
	}

	replay := NewSMST(0)
	for _, n := range small {
		if _, err := replay.Insert(n, cowLeaf(n)); err != nil {
			t.Fatalf("replay insert: %v", err)
		}
	}
	rr, rc := replay.GetRoot()

	view := tree.snapshotView(snap)
	vr, vc := view.GetRoot()
	if vc != rc || !bytesEqual(vr, rr) {
		t.Fatalf("snapshot root != replay root after expansion")
	}
	for idx := uint32(0); idx < rc; idx++ {
		vn, vp, ve := view.GetLeafByDenseIndex(idx)
		rn, rp, re := replay.GetLeafByDenseIndex(idx)
		if ve != nil || re != nil {
			t.Fatalf("proof error idx=%d: view=%v replay=%v", idx, ve, re)
		}
		if vn != rn || len(vp) != len(rp) {
			t.Fatalf("proof shape mismatch at idx=%d", idx)
		}
		for k := range vp {
			if !bytesEqual(vp[k], rp[k]) {
				t.Fatalf("sibling %d mismatch at idx=%d", k, idx)
			}
		}
	}
}
