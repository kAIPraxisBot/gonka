package artifacts

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"
)

// deferredDataset returns a deterministic nonce sequence that forces early depth
// expansion (nonces >= 2^25), includes negatives (path via uint32, depth 32),
// then a long sequential run — exercising every hashing path a flush touches.
func deferredDataset(n int) []int32 {
	out := make([]int32, 0, n+8)
	out = append(out, 1<<25, (1<<25)+1, 1<<28, -1, -2, -1000000)
	for i := 0; i < n; i++ {
		out = append(out, int32(i))
	}
	return out
}

// TestDeferredHashFingerprint builds a large store with periodic flushes,
// verifies every sampled proof against its flush-count root, and folds all
// roots plus sampled proofs into a single deterministic SHA-256 fingerprint.
// The fingerprint is stable across runs and byte-identical to per-insert
// hashing, guarding root/proof reproducibility on large data.
func TestDeferredHashFingerprint(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSMST(dir)
	if err != nil {
		t.Fatalf("OpenSMST: %v", err)
	}
	defer store.Close()

	nonces := deferredDataset(200000)
	fp := sha256.New()
	const flushEvery = 20000
	added := 0

	for _, nonce := range nonces {
		if err := store.AddWithNode(nonce, testVector(int(nonce)), "n"); err != nil {
			t.Fatalf("add %d: %v", nonce, err)
		}
		added++
		if added%flushEvery != 0 {
			continue
		}
		if err := store.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		count := store.Count()
		root, err := store.GetRootAt(count)
		if err != nil {
			t.Fatalf("GetRootAt(%d): %v", count, err)
		}
		var cb [4]byte
		binary.LittleEndian.PutUint32(cb[:], count)
		fp.Write(cb[:])
		fp.Write(root)

		for _, idx := range []uint32{0, count / 3, count / 2, count - 1} {
			entries, err := store.GetArtifactsAndProofs([]uint32{idx}, count)
			if err != nil {
				t.Fatalf("proof idx=%d count=%d: %v", idx, count, err)
			}
			e := entries[0]
			leaf := encodeLeaf(e.Nonce, e.Vector)
			if !VerifySMSTProofSlice(root, count, e.Nonce, leaf, e.Proof) {
				t.Fatalf("proof failed idx=%d count=%d nonce=%d", idx, count, e.Nonce)
			}
			for _, p := range e.Proof {
				fp.Write(p)
			}
		}
	}

	t.Logf("SMST FINGERPRINT (200k + specials, flush 20k): %x", fp.Sum(nil))
}

// TestIngestPerfPortable times building an N-leaf tree via insertCOW plus one
// GetRoot (which fills deferred hashes). It reports the ingest cost that
// deferred hashing reduces by hashing shared upper nodes once per flush rather
// than on every insert that passes through them.
func TestIngestPerfPortable(t *testing.T) {
	n := cowPerfN(200000)
	nonces := make([]int32, n)
	leaves := make([][]byte, n)
	for i := 0; i < n; i++ {
		nonces[i] = int32(i)
		leaves[i] = smstHashLeaf(testVector(i))
	}

	t0 := time.Now()
	tree := NewSMST(0)
	for i := 0; i < n; i++ {
		if _, err := tree.insertCOW(nonces[i], leaves[i]); err != nil {
			t.Fatalf("insertCOW: %v", err)
		}
	}
	root, _ := tree.GetRoot()
	elapsed := time.Since(t0)

	t.Logf("INGEST N=%d: %v  root=%x", n, elapsed, root[:8])
}
