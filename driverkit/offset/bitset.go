package offset

import "math/bits"

// bitset is the sliding-window tracker: two bits per offset (delivered,
// completed) in a circular array of machine words sized to the window rounded
// up to a power of two, so an offset maps to a bit by masking. Bits below the
// watermark are cleared as it advances, which is what makes the window
// circular: an offset can only alias one the watermark has already passed,
// and Track refuses offsets beyond the window before that can happen.
//
// Memory is fixed at construction: Window/4 bytes, 16 KiB for the default
// window, whatever the completion pattern.
type bitset struct {
	common
	mask      uint64
	delivered []uint64
	completed []uint64
}

func newBitset(cfg Config) *bitset {
	// The ring holds at least one whole word so that a word covers exactly
	// 64 consecutive offsets: advance relies on that to skip a resolved word
	// in one step. The window is held to pow2ceil's domain here as well as in
	// Validate, so the size cannot overflow; Track refuses what a smaller ring
	// cannot hold.
	size := max(64, pow2ceil(min(cfg.Window, maxWindow)))
	words := size / 64
	return &bitset{
		common:    newCommon(Bitset, cfg),
		mask:      uint64(size - 1),
		delivered: make([]uint64, words),
		completed: make([]uint64, words),
	}
}

// index splits an offset into a word index and a bit mask.
func (t *bitset) index(off int64) (w int, b uint64) {
	i := uint64(off) & t.mask
	return int(i >> 6), uint64(1) << (i & 63)
}

// Track implements Tracker.
func (t *bitset) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	if t.pend > 0 && off-t.base > int64(t.mask) {
		// Two offsets a ring apart share a slot, and completing the later
		// one would move the watermark over the earlier one while it is
		// still in flight. A validated window always fits the ring, so this
		// refuses nothing checkTrack accepts there.
		return ErrWindowFull
	}
	if t.pend == 0 {
		t.base = off
	}
	w, b := t.index(off)
	t.delivered[w] |= b
	t.high = off + 1
	t.pend++
	return nil
}

// Complete implements Tracker.
func (t *bitset) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen || off < t.base || off >= t.high {
		return t.base, false
	}
	w, b := t.index(off)
	if t.delivered[w]&b == 0 || t.completed[w]&b != 0 {
		return t.base, false
	}
	t.completed[w] |= b
	t.pend--
	before := t.base
	t.advance()
	return t.base, t.base > before
}

// advance moves the watermark over every resolved offset, clearing the bits
// it passes. It walks whole words: a word whose delivered-but-not-completed
// bits are all clear is passed in one step.
func (t *bitset) advance() {
	for t.base < t.high {
		i := uint64(t.base) & t.mask
		w, bit := int(i>>6), uint(i&63)
		blocked := (t.delivered[w] &^ t.completed[w]) >> bit << bit
		if blocked != 0 {
			stop := uint(bits.TrailingZeros64(blocked))
			clearRange(t.delivered, t.completed, w, bit, stop)
			t.base += int64(stop - bit)
			return
		}
		clearRange(t.delivered, t.completed, w, bit, 64)
		next := (t.base &^ 63) + 64
		if next > t.high {
			next = t.high
		}
		t.base = next
	}
}

// clearRange clears bits [lo, hi) of word w in both bitsets.
func clearRange(delivered, completed []uint64, w int, lo, hi uint) {
	var m uint64
	if hi >= 64 {
		m = ^uint64(0) << lo
	} else {
		m = (^uint64(0) << lo) &^ (^uint64(0) << hi)
	}
	delivered[w] &^= m
	completed[w] &^= m
}

// Reset implements Tracker.
func (t *bitset) Reset(next int64) uint32 {
	clear(t.delivered)
	clear(t.completed)
	return t.rebase(next)
}
