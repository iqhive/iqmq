package offset

// heapMap is the heap-and-map tracker: the in-flight offsets live in a binary
// min-heap, and a map from offset to heap position makes the arbitrary
// removal a completion needs O(log n). The watermark is then read directly —
// it is the heap root, or the offset after the newest record when nothing is
// in flight — so no scan is needed however wide the window is.
//
// Because a partition delivers offsets in increasing order every push lands at
// the end of the heap and never sifts up, which is what makes the structure
// cheap on the delivery path; the work happens on completion.
type heapMap struct {
	common
	// h is the min-heap of delivered, incomplete offsets.
	h []int64
	// pos maps an offset to its index in h.
	pos map[int64]int32
}

func newHeapMap(cfg Config) *heapMap {
	return &heapMap{
		common: newCommon(HeapMap, cfg),
		h:      make([]int64, 0, mapHint(cfg.Window)),
		pos:    make(map[int64]int32, mapHint(cfg.Window)),
	}
}

// Track implements Tracker.
func (t *heapMap) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	if t.pend == 0 {
		t.base = off
	}
	t.h = append(t.h, off)
	i := int32(len(t.h) - 1)
	t.pos[off] = i
	t.up(i)
	t.high = off + 1
	t.pend++
	return nil
}

// Complete implements Tracker.
func (t *heapMap) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen {
		return t.base, false
	}
	i, ok := t.pos[off]
	if !ok {
		return t.base, false
	}
	t.remove(i)
	delete(t.pos, off)
	t.pend--
	before := t.base
	if len(t.h) == 0 {
		t.base = t.high
	} else if t.h[0] > t.base {
		t.base = t.h[0]
	}
	return t.base, t.base > before
}

// remove takes element i out of the heap.
func (t *heapMap) remove(i int32) {
	last := int32(len(t.h) - 1)
	if i != last {
		t.h[i] = t.h[last]
		t.pos[t.h[i]] = i
	}
	t.h = t.h[:last]
	if i < last {
		if !t.down(i) {
			t.up(i)
		}
	}
}

// up sifts element i towards the root.
func (t *heapMap) up(i int32) {
	for i > 0 {
		parent := (i - 1) / 2
		if t.h[parent] <= t.h[i] {
			return
		}
		t.swap(parent, i)
		i = parent
	}
}

// down sifts element i towards the leaves and reports whether it moved.
func (t *heapMap) down(i int32) bool {
	n := int32(len(t.h))
	start := i
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		small := l
		if r := l + 1; r < n && t.h[r] < t.h[l] {
			small = r
		}
		if t.h[i] <= t.h[small] {
			break
		}
		t.swap(i, small)
		i = small
	}
	return i > start
}

func (t *heapMap) swap(i, j int32) {
	t.h[i], t.h[j] = t.h[j], t.h[i]
	t.pos[t.h[i]] = i
	t.pos[t.h[j]] = j
}

// Reset implements Tracker.
func (t *heapMap) Reset(next int64) uint32 {
	t.h = t.h[:0]
	clear(t.pos)
	return t.rebase(next)
}
