package artifacts

// smstSnapshot is an immutable capture of the tree at a specific leaf count.
// insertCOW never mutates existing nodes, so a captured root stays valid for
// the life of the tree and serves proofs without any rebuild.
type smstSnapshot struct {
	root  *smstNode
	depth int
	count uint32
}

// insertCOW is a copy-on-write Insert: it rewrites only the nodes on the
// root->leaf path and shares every untouched sibling subtree. Roots and counts
// are byte-identical to Insert; the only difference is that prior roots are not
// clobbered, which is what makes snapshots free.
func (s *SMST) insertCOW(nonce int32, leafHash []byte) (uint32, error) {
	if s.hasNonce[nonce] {
		return 0, ErrDuplicateNonce
	}

	requiredDepth := s.requiredDepth(nonce)
	if requiredDepth > s.depth {
		s.expandDepth(requiredDepth)
	}

	path := s.noncePath(nonce)
	s.root = s.insertAtCOW(s.root, path, 0, leafHash)

	s.hasNonce[nonce] = true
	s.leafCount++

	return s.leafCount, nil
}

func (s *SMST) insertAtCOW(node *smstNode, path []bool, level int, leafHash []byte) *smstNode {
	if level == s.depth {
		return &smstNode{hash: leafHash, count: 1}
	}

	newNode := &smstNode{}
	if node != nil {
		newNode.left = node.left
		newNode.right = node.right
	}

	if path[level] {
		newNode.right = s.insertAtCOW(newNode.right, path, level+1, leafHash)
	} else {
		newNode.left = s.insertAtCOW(newNode.left, path, level+1, leafHash)
	}

	newNode.count = s.nodeCount(newNode.left) + s.nodeCount(newNode.right)
	newNode.hash = s.computeHash(newNode, level)

	return newNode
}

// snapshot captures the current tree. O(1): it retains the root pointer and the
// depth in force at this count, which is the depth a historical proof must use.
func (s *SMST) snapshot() smstSnapshot {
	return smstSnapshot{root: s.root, depth: s.depth, count: s.leafCount}
}

// snapshotView returns a read-only SMST bound to a captured snapshot so proof
// serving uses the depth that was in force at that count, not the live depth.
// emptyHash indices 0..depth are stable across expansion (append-only), so the
// live table is safe to share.
func (s *SMST) snapshotView(snap smstSnapshot) *SMST {
	return &SMST{
		root:      snap.root,
		depth:     snap.depth,
		emptyHash: s.emptyHash,
		leafCount: snap.count,
	}
}
