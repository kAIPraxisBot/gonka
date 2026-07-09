package artifacts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// testVector returns a deterministic 24-byte (DefaultKDim=12 FP16) vector.
func testVector(i int) []byte {
	v := make([]byte, 24)
	binary.LittleEndian.PutUint64(v[0:8], uint64(i))
	binary.LittleEndian.PutUint64(v[8:16], uint64(i*2654435761))
	return v
}

func buildStore(t *testing.T, dir string, n int, treeCache string) (count uint32, root []byte) {
	t.Helper()
	t.Setenv("POC_SMST_TREE_CACHE", treeCache)
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), fmt.Sprintf("node-%d", i%8)); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	count, root = store.GetFlushedRoot()
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return count, root
}

func reopen(t *testing.T, dir, treeCache string) *SMSTArtifactStore {
	t.Helper()
	t.Setenv("POC_SMST_TREE_CACHE", treeCache)
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return store
}

// A cache-recovered store must be identical to a replay-recovered one: root,
// count, and every per-nonce dense index, vector, and proof.
func TestTreeCacheRoundTripMatchesReplay(t *testing.T) {
	dir := t.TempDir()
	n := 500
	wantCount, wantRoot := buildStore(t, dir, n, "on") // writes smst.tree

	if _, err := os.Stat(filepath.Join(dir, treeCacheFileName)); err != nil {
		t.Fatalf("expected tree cache file: %v", err)
	}

	cacheStore := reopen(t, dir, "on")
	defer cacheStore.Close()
	replayStore := reopen(t, dir, "")
	defer replayStore.Close()

	cCount, cRoot := cacheStore.GetFlushedRoot()
	rCount, rRoot := replayStore.GetFlushedRoot()

	if cCount != wantCount || rCount != wantCount {
		t.Fatalf("count mismatch: cache=%d replay=%d want=%d", cCount, rCount, wantCount)
	}
	if !bytes.Equal(cRoot, wantRoot) || !bytes.Equal(rRoot, wantRoot) {
		t.Fatalf("root mismatch: cache=%x replay=%x want=%x", cRoot, rRoot, wantRoot)
	}

	for i := 0; i < n; i++ {
		nonce := int32(i)
		cIdx, cVec, cProof, err := cacheStore.GetArtifactAndProofByNonce(nonce, wantCount)
		if err != nil {
			t.Fatalf("cache proof nonce %d: %v", nonce, err)
		}
		rIdx, rVec, rProof, err := replayStore.GetArtifactAndProofByNonce(nonce, wantCount)
		if err != nil {
			t.Fatalf("replay proof nonce %d: %v", nonce, err)
		}
		if cIdx != rIdx || !bytes.Equal(cVec, rVec) {
			t.Fatalf("nonce %d: idx/vec differ (cache idx=%d replay idx=%d)", nonce, cIdx, rIdx)
		}
		if len(cProof) != len(rProof) {
			t.Fatalf("nonce %d: proof len differ %d vs %d", nonce, len(cProof), len(rProof))
		}
		for j := range cProof {
			if !bytes.Equal(cProof[j], rProof[j]) {
				t.Fatalf("nonce %d: proof elem %d differs", nonce, j)
			}
		}
		if !VerifySMSTProofWithDenseIndex(cRoot, wantCount, cIdx, nonce, encodeLeaf(nonce, cVec), cProof) {
			t.Fatalf("nonce %d: cache proof does not verify", nonce)
		}
	}
}

// Nonces past the default depth force expansion, which the cache must restore.
func TestTreeCacheDepthExpansion(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("POC_SMST_TREE_CACHE", "on")
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	nonces := []int32{0, 1, 5, 1 << 20, (1 << 24) + 7, (1 << 26) + 3, 1<<30 - 1}
	for i, nonce := range nonces {
		if err := store.AddWithNode(nonce, testVector(i), "n"); err != nil {
			t.Fatalf("add %d: %v", nonce, err)
		}
	}
	store.Flush()
	wantCount, wantRoot := store.GetFlushedRoot()
	store.Close()

	cacheStore := reopen(t, dir, "on")
	defer cacheStore.Close()
	gotCount, gotRoot := cacheStore.GetFlushedRoot()
	if gotCount != wantCount || !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("expanded-depth cache mismatch: got (%d,%x) want (%d,%x)", gotCount, gotRoot, wantCount, wantRoot)
	}
	for _, nonce := range nonces {
		if !cacheStore.smst.HasNonce(nonce) {
			t.Fatalf("nonce %d missing after cache load", nonce)
		}
	}
}

// A tampered sidecar must be rejected by the checksum and fall back to replay.
func TestTreeCacheCorruptionFallsBack(t *testing.T) {
	dir := t.TempDir()
	wantCount, wantRoot := buildStore(t, dir, 300, "on")

	// Flip a byte inside the serialized tree body.
	path := filepath.Join(dir, treeCacheFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[treeCacheHeaderLen+10] ^= 0xff
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}

	store := reopen(t, dir, "on")
	defer store.Close()
	gotCount, gotRoot := store.GetFlushedRoot()
	if gotCount != wantCount || !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("corrupt-cache recovery mismatch: got (%d,%x) want (%d,%x)", gotCount, gotRoot, wantCount, wantRoot)
	}
}

// If artifacts.data grows past the cached count, the stale cache is ignored and
// replay recovers the true state.
func TestTreeCacheStaleFallsBack(t *testing.T) {
	dir := t.TempDir()
	buildStore(t, dir, 200, "on")

	// Append 100 more with the flag off, so the on-disk cache lags the log.
	t.Setenv("POC_SMST_TREE_CACHE", "")
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 200; i < 300; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), "n"); err != nil {
			t.Fatal(err)
		}
	}
	store.Flush()
	wantCount, wantRoot := store.GetFlushedRoot()
	store.Close()
	if wantCount != 300 {
		t.Fatalf("expected 300 after append, got %d", wantCount)
	}

	reopened := reopen(t, dir, "on")
	defer reopened.Close()
	gotCount, gotRoot := reopened.GetFlushedRoot()
	if gotCount != wantCount || !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("stale-cache recovery mismatch: got (%d,%x) want (%d,%x)", gotCount, gotRoot, wantCount, wantRoot)
	}
}

// Rebuilding internal hashes from the cached leaves must reproduce the root.
func TestRecomputeTreeRootMatchesCommitted(t *testing.T) {
	dir := t.TempDir()
	wantCount, wantRoot := buildStore(t, dir, 400, "on")

	tree, count, _, err := loadTreeCache(dir)
	if err != nil {
		t.Fatalf("loadTreeCache: %v", err)
	}
	if count != wantCount {
		t.Fatalf("count: got %d want %d", count, wantCount)
	}
	if got := recomputeTreeRoot(tree); !bytes.Equal(got, wantRoot) {
		t.Fatalf("recomputed root %x != committed %x", got, wantRoot)
	}
}

// End-to-end shadow path: load plus structural recompute.
func TestTreeCacheShadowMode(t *testing.T) {
	dir := t.TempDir()
	wantCount, wantRoot := buildStore(t, dir, 250, "shadow")
	store := reopen(t, dir, "shadow")
	defer store.Close()
	gotCount, gotRoot := store.GetFlushedRoot()
	if gotCount != wantCount || !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("shadow mismatch: got (%d,%x) want (%d,%x)", gotCount, gotRoot, wantCount, wantRoot)
	}
}

// Cache recovery must beat replay for a non-trivial store; logs the speedup.
func TestTreeCacheRecoverPerf(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf measurement in -short mode")
	}
	dir := t.TempDir()
	n := 50000
	_, wantRoot := buildStore(t, dir, n, "on")

	startReplay := time.Now()
	replayStore := reopen(t, dir, "")
	replayDur := time.Since(startReplay)
	_, rRoot := replayStore.GetFlushedRoot()
	replayStore.Close()

	startCache := time.Now()
	cacheStore := reopen(t, dir, "on")
	cacheDur := time.Since(startCache)
	_, cRoot := cacheStore.GetFlushedRoot()
	cacheStore.Close()

	if !bytes.Equal(rRoot, wantRoot) || !bytes.Equal(cRoot, wantRoot) {
		t.Fatalf("perf recovery root mismatch")
	}

	speedup := float64(replayDur) / float64(cacheDur)
	t.Logf("recover(%d leaves): replay=%v  cache=%v  speedup=%.2fx", n, replayDur, cacheDur, speedup)
	if cacheDur >= replayDur {
		t.Fatalf("expected cache recovery faster than replay: replay=%v cache=%v", replayDur, cacheDur)
	}
}

var recoverBenchSizes = []int{10000, 50000, 100000}

// Large-scale before/after recover measurement. Opt-in via SMST_PERF_N
// (e.g. 1000000); heavy on RAM/time.
func TestTreeCacheRecoverPerfLarge(t *testing.T) {
	spec := os.Getenv("SMST_PERF_N")
	if spec == "" {
		t.Skip("set SMST_PERF_N to run the large-scale perf measurement")
	}
	n, err := strconv.Atoi(spec)
	if err != nil || n <= 0 {
		t.Fatalf("bad SMST_PERF_N=%q", spec)
	}
	dir := t.TempDir()

	t.Setenv("POC_SMST_TREE_CACHE", "on")
	buildStart := time.Now()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), "n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	_, wantRoot := store.GetFlushedRoot()
	store.Close()
	t.Logf("built %d artifacts in %v", n, time.Since(buildStart))

	if fi, err := os.Stat(filepath.Join(dir, treeCacheFileName)); err == nil {
		t.Logf("tree cache file size: %.1f MB", float64(fi.Size())/(1<<20))
	}

	startReplay := time.Now()
	rs := reopen(t, dir, "")
	replayDur := time.Since(startReplay)
	_, rRoot := rs.GetFlushedRoot()
	rs.Close()

	startCache := time.Now()
	cs := reopen(t, dir, "on")
	cacheDur := time.Since(startCache)
	_, cRoot := cs.GetFlushedRoot()
	cs.Close()

	if !bytes.Equal(rRoot, wantRoot) || !bytes.Equal(cRoot, wantRoot) {
		t.Fatalf("large-perf root mismatch")
	}
	t.Logf("recover(%d): BEFORE replay=%v  AFTER cache=%v  speedup=%.1fx",
		n, replayDur, cacheDur, float64(replayDur)/float64(cacheDur))
}

func benchmarkRecover(b *testing.B, n int, mode string) {
	b.Helper()
	dir := b.TempDir()
	b.Setenv("POC_SMST_TREE_CACHE", "on") // build the sidecar
	store, err := OpenSMST(dir)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := store.AddWithNode(int32(i), testVector(i), "n"); err != nil {
			b.Fatal(err)
		}
	}
	store.Flush()
	store.Close()

	b.Setenv("POC_SMST_TREE_CACHE", mode) // "" = replay (before), "on" = cache (after)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := OpenSMST(dir)
		if err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

// BenchmarkRecoverReplay is the BEFORE case: recover by replaying artifacts.data.
func BenchmarkRecoverReplay(b *testing.B) {
	for _, n := range recoverBenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) { benchmarkRecover(b, n, "") })
	}
}

// BenchmarkRecoverCache is the AFTER case: recover by loading the tree cache.
func BenchmarkRecoverCache(b *testing.B) {
	for _, n := range recoverBenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) { benchmarkRecover(b, n, "on") })
	}
}
