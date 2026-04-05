package offset

import "math/bits"

// Segment geometry: one segment covers segSize consecutive offsets in
// segWords machine words per bitmap, so a segment costs 1 KiB and covers 4096
// offsets.
const (
	segShift = 12
	segSize  = 1 << segShift
	segMask  = segSize - 1
	segWords = segSize / 64
)

// segment holds the delivered and completed bits of one range of offsets.
type segment struct {
	delivered [segWords]uint64
	completed [segWords]uint64
}

// segmented is the segmented-bitmap tracker: the bitset of the window is cut
// into fixed segments that are allocated only where records are in flight and
// recycled once the watermark passes them. A wide window with a few
// outstanding records costs one segment per island of activity instead of a
// bitset spanning the whole window, which is what makes it the variant for
// wide gaps; the cost is a map lookup per operation.
type segmented struct {
	common
	segs map[int64]*segment
	// pool recycles segments so a steady stream of offsets allocates none.
	pool []*segment
}

func newSegmented(cfg Config) *segmented {
	return &segmented{
		common: newCommon(Segmented, cfg),
		segs:   make(map[int64]*segment, 8),
		pool:   make([]*segment, 0, 8),
	}
}

// maxPooled bounds the recycled segments so a wide burst does not pin memory.
const maxPooled = 8

func (t *segmented) get() *segment {
	if n := len(t.pool); n > 0 {
		s := t.pool[n-1]
		t.pool = t.pool[:n-1]
		return s
	}
	return new(segment)
}

func (t *segmented) put(s *segment) {
	if len(t.pool) < maxPooled {
		t.pool = append(t.pool, s)
	}
}

// Track implements Tracker.
func (t *segmented) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	si := off >> segShift
	if t.pend == 0 {
		// Nothing is in flight: the watermark jumps to the new record. Any
		// segment left below it is empty and can be recycled; the check on
		// the segment index keeps that off the per-record path.
		if si != t.base>>segShift {
			t.dropBelow(si)
		}
		t.base = off
	}
	s := t.segs[si]
	if s == nil {
		s = t.get()
		t.segs[si] = s
	}
	i := off & segMask
	s.delivered[i>>6] |= uint64(1) << uint(i&63)
	t.high = off + 1
	t.pend++
	return nil
}

// Complete implements Tracker.
func (t *segmented) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen || off < t.base || off >= t.high {
		return t.base, false
	}
	s := t.segs[off>>segShift]
	if s == nil {
		return t.base, false
	}
	i := off & segMask
	w, b := i>>6, uint64(1)<<uint(i&63)
	if s.delivered[w]&b == 0 || s.completed[w]&b != 0 {
		return t.base, false
	}
	s.completed[w] |= b
	t.pend--
	before := t.base
	t.advance()
	return t.base, t.base > before
}

// advance moves the watermark over every resolved offset. A segment with no
// delivered offsets is skipped whole; within a segment the scan works word by
// word and clears the bits it passes, so a recycled segment is always clean.
func (t *segmented) advance() {
	for t.base < t.high {
		si := t.base >> segShift
		segEnd := (si + 1) << segShift
		limit := min(segEnd, t.high)
		s := t.segs[si]
		if s == nil {
			t.base = limit
			continue
		}
		for t.base < limit {
			i := t.base & segMask
			w, bit := i>>6, uint(i&63)
			blocked := (s.delivered[w] &^ s.completed[w]) >> bit << bit
			if blocked != 0 {
				stop := uint(bits.TrailingZeros64(blocked))
				clearRange(s.delivered[:], s.completed[:], int(w), bit, stop)
				t.base += int64(stop - bit)
				return
			}
			clearRange(s.delivered[:], s.completed[:], int(w), bit, 64)
			next := (t.base &^ 63) + 64
			if next > limit {
				next = limit
			}
			t.base = next
		}
		if t.base >= segEnd {
			delete(t.segs, si)
			t.put(s)
		}
	}
}

// dropBelow recycles every segment below index si. It is called only when
// nothing is in flight, so the segments it drops carry no live bits.
func (t *segmented) dropBelow(si int64) {
	for i, s := range t.segs {
		if i < si {
			delete(t.segs, i)
			t.put(s)
		}
	}
}

// Reset implements Tracker.
func (t *segmented) Reset(next int64) uint32 {
	for si, s := range t.segs {
		delete(t.segs, si)
		clear(s.delivered[:])
		clear(s.completed[:])
		t.put(s)
	}
	return t.rebase(next)
}
