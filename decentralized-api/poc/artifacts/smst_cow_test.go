package artifacts

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func testVector(i int) []byte {
	v := make([]byte, 24)
	binary.LittleEndian.PutUint64(v[0:8], uint64(i))
	binary.LittleEndian.PutUint64(v[8:16], uint64(i*2654435761))
	return v
}

func cowUniqueNonces(rng *rand.Rand, n int, max int32) []int32 {
	seen := make(map[int32]bool, n)
	out := make([]int32, 0, n)
	for len(out) < n {
		v := int32(rng.Intn(int(max)))
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func cowLeaf(nonce int32) []byte {
	return smstHashLeaf(testVector(int(nonce)))
}

func cowProofWithCounts(tree *SMST, nonce int32) []smstProofElement {
	path := tree.noncePath(nonce)
	elements := make([]smstProofElement, 0, tree.depth)
	var collect func(node *smstNode, level int)
	collect = func(node *smstNode, level int) {
		if level == tree.depth || node == nil {
			return
		}
		if path[level] {
			elements = append(elements, smstProofElement{
				siblingHash:  tree.nodeHash(node.left, level+1),
				siblingCount: tree.nodeCount(node.left),
			})
			collect(node.right, level+1)
		} else {
			elements = append(elements, smstProofElement{
				siblingHash:  tree.nodeHash(node.right, level+1),
				siblingCount: tree.nodeCount(node.right),
			})
			collect(node.left, level+1)
		}
	}
	collect(tree.root, 0)
	return elements
}

// TestCOWRootsMatchMutatingImpl proves the copy-on-write insert commits to a
// byte-identical root and count as the in-place Insert at every step: same
// commitment, so the fraud/security model is unchanged by construction.
func TestCOWRootsMatchMutatingImpl(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	nonces := cowUniqueNonces(rng, 3000, 1<<23)

	mut := NewSMST(0)
	cow := NewSMST(0)

	for i, nonce := range nonces {
		leaf := cowLeaf(nonce)
		mc, err := mut.Insert(nonce, leaf)
		if err != nil {
			t.Fatalf("mutating insert %d: %v", i, err)
		}
		cc, err := cow.insertCOW(nonce, leaf)
		if err != nil {
			t.Fatalf("cow insert %d: %v", i, err)
		}
		if mc != cc {
			t.Fatalf("count mismatch at %d: mut=%d cow=%d", i, mc, cc)
		}
		mr, mrc := mut.GetRoot()
		cr, crc := cow.GetRoot()
		if mrc != crc || !bytesEqual(mr, cr) {
			t.Fatalf("root mismatch at insert %d", i)
		}
	}
}

// TestCOWSnapshotsAreFreeAndIdentical proves a snapshot captured mid-stream and
// then left untouched while the tree keeps growing still equals a from-scratch
// replay to that count — root, every dense-index nonce, and every proof sibling
// are byte-identical, and each proof verifies against the snapshot root. This is
// the core claim: historical proofs come from retained nodes in O(depth), never
// an O(N) rebuild, with zero change to what a validator verifies.
func TestCOWSnapshotsAreFreeAndIdentical(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const total = 4000
	nonces := cowUniqueNonces(rng, total, 1<<23)
	captureAt := map[uint32]bool{250: true, 1111: true, 3333: true}

	cow := NewSMST(0)
	snaps := map[uint32]smstSnapshot{}

	for _, nonce := range nonces {
		c, err := cow.insertCOW(nonce, cowLeaf(nonce))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if captureAt[c] {
			snaps[c] = cow.snapshot()
		}
	}

	for count, snap := range snaps {
		replay := NewSMST(0)
		for _, nonce := range nonces[:count] {
			if _, err := replay.Insert(nonce, cowLeaf(nonce)); err != nil {
				t.Fatalf("replay insert: %v", err)
			}
		}

		rr, rc := replay.GetRoot()
		if snap.root == nil || snap.count != count {
			t.Fatalf("snapshot count mismatch: got %d want %d", snap.count, count)
		}
		if rc != count || !bytesEqual(rr, snap.root.hash) {
			t.Fatalf("snapshot root != replay root at count=%d", count)
		}

		view := cow.snapshotView(snap)
		for idx := uint32(0); idx < count; idx++ {
			sn, sp, se := view.GetLeafByDenseIndex(idx)
			rn, rp, re := replay.GetLeafByDenseIndex(idx)
			if se != nil || re != nil {
				t.Fatalf("proof error at idx=%d: snap=%v replay=%v", idx, se, re)
			}
			if sn != rn {
				t.Fatalf("nonce mismatch at idx=%d count=%d: snap=%d replay=%d", idx, count, sn, rn)
			}
			if len(sp) != len(rp) {
				t.Fatalf("proof length mismatch at idx=%d", idx)
			}
			for k := range sp {
				if !bytesEqual(sp[k], rp[k]) {
					t.Fatalf("proof sibling %d mismatch at idx=%d count=%d", k, idx, count)
				}
			}

			elems := cowProofWithCounts(view, sn)
			if !verifySMSTProofWithCounts(rr, count, sn, testVector(int(sn)), elems) {
				t.Fatalf("snapshot proof failed verification at idx=%d count=%d", idx, count)
			}
		}
	}
}
