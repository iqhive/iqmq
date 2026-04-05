package offset

// tree is the ordered-tree tracker: the in-flight offsets live in a treap
// (a randomised balanced search tree) whose nodes are stored in one slice and
// recycled through a free list, so no node is ever allocated in steady state.
// The watermark is the leftmost key, or the offset after the newest record
// when the tree is empty.
//
// A plain search tree would degenerate here: offsets arrive in increasing
// order, so every insert would be the rightmost node and the tree a list. The
// treap fixes that without rebalancing bookkeeping, by giving each key a
// priority derived from the key itself (so a run is reproducible) and keeping
// the heap order on priorities.
type tree struct {
	common
	nodes []treeNode
	root  int32
	free  int32 // head of the free list, -1 when empty
	found bool  // set by del: whether the key was in the tree
}

type treeNode struct {
	off         int64
	prio        uint32
	left, right int32
}

const nilNode = int32(-1)

func newTree(cfg Config) *tree {
	return &tree{
		common: newCommon(Tree, cfg),
		nodes:  make([]treeNode, 0, mapHint(cfg.Window)),
		root:   nilNode,
		free:   nilNode,
	}
}

// priority mixes the offset into a priority with splitmix64's finaliser, so
// the tree shape depends only on the keys and every run is reproducible.
func priority(off int64) uint32 {
	x := uint64(off) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return uint32(x ^ (x >> 31))
}

// alloc returns a node index for off, reusing a freed node when possible.
func (t *tree) alloc(off int64) int32 {
	i := t.free
	if i >= 0 {
		t.free = t.nodes[i].left
	} else {
		t.nodes = append(t.nodes, treeNode{})
		i = int32(len(t.nodes) - 1)
	}
	t.nodes[i] = treeNode{off: off, prio: priority(off), left: nilNode, right: nilNode}
	return i
}

// release returns node i to the free list.
func (t *tree) release(i int32) {
	t.nodes[i].left = t.free
	t.free = i
}

// Track implements Tracker.
func (t *tree) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	if t.pend == 0 {
		t.base = off
	}
	t.root = t.insert(t.root, off)
	t.high = off + 1
	t.pend++
	return nil
}

// insert adds off below i and returns the new subtree root.
func (t *tree) insert(i int32, off int64) int32 {
	if i == nilNode {
		return t.alloc(off)
	}
	if off < t.nodes[i].off {
		t.nodes[i].left = t.insert(t.nodes[i].left, off)
		if t.nodes[t.nodes[i].left].prio > t.nodes[i].prio {
			i = t.rotateRight(i)
		}
		return i
	}
	t.nodes[i].right = t.insert(t.nodes[i].right, off)
	if t.nodes[t.nodes[i].right].prio > t.nodes[i].prio {
		i = t.rotateLeft(i)
	}
	return i
}

func (t *tree) rotateRight(i int32) int32 {
	l := t.nodes[i].left
	t.nodes[i].left = t.nodes[l].right
	t.nodes[l].right = i
	return l
}

func (t *tree) rotateLeft(i int32) int32 {
	r := t.nodes[i].right
	t.nodes[i].right = t.nodes[r].left
	t.nodes[r].left = i
	return r
}

// Complete implements Tracker.
func (t *tree) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen || off < t.base || off >= t.high {
		return t.base, false
	}
	t.found = false
	t.root = t.del(t.root, off)
	if !t.found {
		return t.base, false
	}
	t.pend--
	before := t.base
	if t.root == nilNode {
		t.base = t.high
	} else if m := t.min(); m > t.base {
		t.base = m
	}
	return t.base, t.base > before
}

// del removes off from the subtree at i and returns the new subtree root,
// setting found when the key was present.
func (t *tree) del(i int32, off int64) int32 {
	if i == nilNode {
		return nilNode
	}
	switch {
	case off < t.nodes[i].off:
		t.nodes[i].left = t.del(t.nodes[i].left, off)
	case off > t.nodes[i].off:
		t.nodes[i].right = t.del(t.nodes[i].right, off)
	default:
		t.found = true
		l, r := t.nodes[i].left, t.nodes[i].right
		t.release(i)
		return t.join(l, r)
	}
	return i
}

// join merges two subtrees whose keys are already ordered.
func (t *tree) join(l, r int32) int32 {
	switch {
	case l == nilNode:
		return r
	case r == nilNode:
		return l
	case t.nodes[l].prio > t.nodes[r].prio:
		t.nodes[l].right = t.join(t.nodes[l].right, r)
		return l
	default:
		t.nodes[r].left = t.join(l, t.nodes[r].left)
		return r
	}
}

// min returns the smallest key in the tree; the caller checks emptiness.
func (t *tree) min() int64 {
	i := t.root
	for t.nodes[i].left != nilNode {
		i = t.nodes[i].left
	}
	return t.nodes[i].off
}

// Reset implements Tracker.
func (t *tree) Reset(next int64) uint32 {
	t.nodes, t.root, t.free = t.nodes[:0], nilNode, nilNode
	return t.rebase(next)
}
