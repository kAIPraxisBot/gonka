package artifacts

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// A checksummed sidecar ("smst.tree") holding the built flushed tree, so
// recover() loads precomputed node hashes instead of replaying artifacts.data.
// It is a disposable cache over the authoritative log: a load is accepted only
// when it stays consistent with the log, otherwise recovery falls back to a full
// replay. Gated off by default (POC_SMST_TREE_CACHE). Changes no on-chain output.

const (
	treeCacheFileName = "smst.tree"
	treeCacheVersion  = uint32(1)
)

var treeCacheMagic = [8]byte{'G', 'O', 'N', 'K', 'S', 'M', 'S', 'T'}

// magic(8) + version(4) + depth(4) + leafCount(4) + dataOffset(8) + rootHash(32).
const treeCacheHeaderLen = 8 + 4 + 4 + 4 + 8 + 32

var errTreeCacheMiss = errors.New("smst tree cache miss")

// treeCacheMode: "" disabled, "on" fast path, "shadow" fast path plus a root
// recompute before trusting.
func treeCacheMode() string {
	switch os.Getenv("POC_SMST_TREE_CACHE") {
	case "1", "on", "true":
		return "on"
	case "shadow":
		return "shadow"
	default:
		return ""
	}
}

func treeCacheEnabled() bool { return treeCacheMode() != "" }

func writeTreeCache(dir string, tree *SMST, dataOffset uint64) error {
	body, err := serializeTree(tree, dataOffset)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	full := append(body, sum[:]...)

	tmp := filepath.Join(dir, treeCacheFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create tree cache tmp: %w", err)
	}
	if _, err := f.Write(full); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tree cache: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync tree cache: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tree cache: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, treeCacheFileName)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename tree cache: %w", err)
	}
	return nil
}

func serializeTree(tree *SMST, dataOffset uint64) ([]byte, error) {
	if tree == nil || tree.root == nil {
		return nil, errors.New("serializeTree: empty tree")
	}
	root, count := tree.GetRoot()
	if len(root) != sha256.Size {
		return nil, fmt.Errorf("serializeTree: unexpected root hash length %d", len(root))
	}

	var buf bytes.Buffer
	buf.Grow(treeCacheHeaderLen + int(count)*40)

	buf.Write(treeCacheMagic[:])
	writeU32(&buf, treeCacheVersion)
	writeU32(&buf, uint32(tree.depth))
	writeU32(&buf, count)
	writeU64(&buf, dataOffset)
	buf.Write(root)

	if err := serializeNode(&buf, tree.root, 0, tree.depth); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Leaves (level == depth) carry no children; their nonce is recoverable from the
// path, so it is not stored.
func serializeNode(buf *bytes.Buffer, node *smstNode, level, depth int) error {
	if node == nil {
		buf.WriteByte(0)
		return nil
	}
	if len(node.hash) != sha256.Size {
		return fmt.Errorf("serializeNode: unexpected hash length %d at level %d", len(node.hash), level)
	}
	buf.WriteByte(1)
	buf.Write(node.hash)
	writeU32(buf, node.count)
	if level < depth {
		if err := serializeNode(buf, node.left, level+1, depth); err != nil {
			return err
		}
		if err := serializeNode(buf, node.right, level+1, depth); err != nil {
			return err
		}
	}
	return nil
}

// loadTreeCache validates the sidecar's self-contained integrity (magic, version,
// checksum, root byte) and reconstructs the tree. errTreeCacheMiss means "no
// usable cache, replay instead". The caller ties it to the authoritative log.
func loadTreeCache(dir string) (tree *SMST, count uint32, dataOffset uint64, err error) {
	path := filepath.Join(dir, treeCacheFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, errTreeCacheMiss
		}
		return nil, 0, 0, fmt.Errorf("read tree cache: %w", err)
	}
	if len(raw) < treeCacheHeaderLen+sha256.Size {
		return nil, 0, 0, errTreeCacheMiss
	}

	body := raw[:len(raw)-sha256.Size]
	want := raw[len(raw)-sha256.Size:]
	got := sha256.Sum256(body)
	if !bytes.Equal(got[:], want) {
		return nil, 0, 0, errTreeCacheMiss
	}

	r := bytes.NewReader(body)
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil || magic != treeCacheMagic {
		return nil, 0, 0, errTreeCacheMiss
	}
	version := readU32(r)
	depth := readU32(r)
	count = readU32(r)
	dataOffset = readU64(r)
	rootHash := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, rootHash); err != nil {
		return nil, 0, 0, errTreeCacheMiss
	}
	if version != treeCacheVersion || depth == 0 || depth > smstMaxDepth || count == 0 {
		return nil, 0, 0, errTreeCacheMiss
	}

	tree = NewSMST(int(depth))
	pathBits := make([]bool, depth)
	root, err := deserializeNode(r, 0, int(depth), pathBits, tree.hasNonce)
	if err != nil || r.Len() != 0 {
		return nil, 0, 0, errTreeCacheMiss
	}
	tree.root = root
	tree.leafCount = count

	gotRoot, gotCount := tree.GetRoot()
	if gotCount != count || !bytes.Equal(gotRoot, rootHash) || uint32(len(tree.hasNonce)) != count {
		return nil, 0, 0, errTreeCacheMiss
	}
	return tree, count, dataOffset, nil
}

// At each leaf the nonce is recovered from its root-to-leaf path into hasNonce.
func deserializeNode(r *bytes.Reader, level, depth int, pathBits []bool, hasNonce map[int32]bool) (*smstNode, error) {
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if flag == 0 {
		return nil, nil
	}
	if flag != 1 {
		return nil, fmt.Errorf("deserializeNode: bad flag %d", flag)
	}
	hash := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, hash); err != nil {
		return nil, err
	}
	if r.Len() < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	node := &smstNode{hash: hash, count: readU32(r)}
	if level < depth {
		pathBits[level] = false
		if node.left, err = deserializeNode(r, level+1, depth, pathBits, hasNonce); err != nil {
			return nil, err
		}
		pathBits[level] = true
		if node.right, err = deserializeNode(r, level+1, depth, pathBits, hasNonce); err != nil {
			return nil, err
		}
	} else {
		nonce := pathBitsToNonce(pathBits, depth)
		if hasNonce[nonce] {
			return nil, fmt.Errorf("deserializeNode: duplicate nonce %d", nonce)
		}
		hasNonce[nonce] = true
	}
	return node, nil
}

func pathBitsToNonce(pathBits []bool, depth int) int32 {
	var n uint32
	for i := 0; i < depth; i++ {
		if pathBits[i] {
			n |= 1 << (depth - 1 - i)
		}
	}
	return int32(n)
}

// recomputeTreeRoot rebuilds internal hashes bottom-up from the cached leaf
// hashes so a caller can assert the committed root. O(N); shadow mode and tests.
func recomputeTreeRoot(tree *SMST) []byte {
	var walk func(node *smstNode, level int) []byte
	walk = func(node *smstNode, level int) []byte {
		if node == nil {
			return tree.emptyHash[tree.depth-level]
		}
		if level == tree.depth {
			return node.hash
		}
		left := walk(node.left, level+1)
		right := walk(node.right, level+1)
		return smstHashNode(left, right, node.count)
	}
	if tree.root == nil {
		return tree.emptyHash[tree.depth]
	}
	return walk(tree.root, 0)
}

// scanArtifactOffsets rebuilds the offset slice and nonce index by reading only
// the 8-byte record headers (skipping vectors), far cheaper than a full replay.
// end is the end-of-data offset, for the staleness check against the cache.
func scanArtifactOffsets(f *os.File) (offsets []uint64, nonceToOffset map[int32]uint64, end uint64, err error) {
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, 0, fmt.Errorf("seek data file: %w", err)
	}
	r := bufio.NewReader(f)
	offsets = make([]uint64, 0, 1024)
	nonceToOffset = make(map[int32]uint64)

	var offset uint64
	var header [8]byte
	for {
		if _, err = io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, 0, fmt.Errorf("scan header at %d: %w", offset, err)
		}
		totalLen := binary.LittleEndian.Uint32(header[0:4])
		nonce := int32(binary.LittleEndian.Uint32(header[4:8]))
		if totalLen < 4 {
			return nil, nil, 0, fmt.Errorf("scan: bad record length %d at %d", totalLen, offset)
		}
		vectorLen := totalLen - 4
		offsets = append(offsets, offset)
		nonceToOffset[nonce] = offset
		if _, err = r.Discard(int(vectorLen)); err != nil {
			return nil, nil, 0, fmt.Errorf("scan skip vector at %d: %w", offset, err)
		}
		offset += uint64(8) + uint64(vectorLen)
	}
	return offsets, nonceToOffset, offset, nil
}

func writeU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeU64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

func readU32(r *bytes.Reader) uint32 {
	var b [4]byte
	_, _ = io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint32(b[:])
}

func readU64(r *bytes.Reader) uint64 {
	var b [8]byte
	_, _ = io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint64(b[:])
}
