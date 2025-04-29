package wire

import (
	"hash/maphash"
	"sync/atomic"
)

// internSlots is the size of the per-layout intern table. Header names and
// property values in real traffic are drawn from a small vocabulary; the
// table needs only to hold that vocabulary, not to be a general cache.
const internSlots = 256

// internMaxLen bounds what the table holds, so a hostile flood of long
// distinct keys cannot make the table pin large strings.
const internMaxLen = 64

// interner turns short byte strings into Go strings without allocating on
// a hit. It is a direct-mapped table of atomic pointers: a lookup hashes
// the bytes, loads one slot and compares content; a miss allocates the
// string once and publishes it. Slots are overwritten on collision, so the
// table is bounded and always correct (content is compared, never
// trusted from the hash).
type interner struct {
	slots [internSlots]atomic.Pointer[string]
}

var internSeed = maphash.MakeSeed()

// get returns a string equal to b.
func (in *interner) get(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b) > internMaxLen {
		return string(b)
	}
	slot := &in.slots[maphash.Bytes(internSeed, b)&(internSlots-1)]
	if s := slot.Load(); s != nil && *s == string(b) {
		return *s
	}
	s := string(b)
	slot.Store(&s)
	return s
}
