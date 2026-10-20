// Package timer holds the scheduler variants the data structure that orders delayed deliveries, lease
// deadlines, TTLs and retry backoffs by due time.
//
// Entries are intrusive: the caller owns an Entry, adds it with a due time in
// nanoseconds, and receives it back from Fire once due. No kind ever fires an
// entry early; wheel kinds round due times up to their tick and may fire up
// to one Resolution late. Next returns a time no later than the first
// firing: the (rounded) due time of the earliest entry, or the last now
// passed to Fire or New when that entry is already due. A caller that sleeps
// until Next and then calls Fire therefore never misses a deadline, and
// calling Fire(Next()) either fires something or advances Next; every kind
// reaches the first entry within a handful of such steps.
//
// Times are int64 nanoseconds on any monotonic origin, positive or negative:
// nothing here assumes an origin at zero. Due times within one tick of
// math.MaxInt64 round up past the representable range, so a wheel kind never
// reaches them and never fires them, which is what such a "never" sentinel
// asks for; exact kinds fire them at now == due.
package timer

import (
	"errors"
	"math"
	"time"
)

// Kind identifies a scheduler variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Heap is the Safe reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Heap is a binary min-heap ordered by due time: exact, O(log n).
	Heap
	// Heap4 is a 4-ary min-heap: shallower, more cache-friendly sift-down.
	Heap4
	// HierarchicalWheel is a four-level timing wheel with 1 ms ticks and 256
	// slots per level, cascading between levels; entries more than 2^32
	// ticks (about 50 days) ahead wait in an overflow heap. O(1) add and
	// cancel; Fire jumps straight to the next occupied slot.
	HierarchicalWheel
	// HashedWheel is a single-level 1 ms wheel of 4096 slots whose entries
	// keep their absolute tick; a slot may hold entries from several laps
	// and Fire visits each occupied slot at most once per lap.
	HashedWheel
	// HybridHeapWheel keeps entries due within one second in a 1 ms wheel
	// and later ones in a heap, migrating them as the horizon advances.
	HybridHeapWheel
	// Calendar is an adaptive calendar queue (Brown 1988): due-sorted
	// buckets whose count and width follow the number of entries and their
	// spacing. Exact and ordered, O(1) amortised add, cancel and fire.
	Calendar

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "heap", "heap4", "hwheel", "hashedwheel", "hybrid", "calendar"}

var kindIdents = [kindCount]string{"Auto", "Heap", "Heap4", "HierarchicalWheel", "HashedWheel", "HybridHeapWheel", "Calendar"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := Heap; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Resolution is the tick of the kind: 0 for exact kinds.
func (k Kind) Resolution() time.Duration {
	switch k.Resolve() {
	case HierarchicalWheel, HashedWheel, HybridHeapWheel:
		return tick
	default:
		return 0
	}
}

// Ordered reports whether Fire returns entries in non-decreasing due order.
func (k Kind) Ordered() bool {
	switch k.Resolve() {
	case Heap, Heap4, Calendar:
		return true
	default:
		return false
	}
}

// tick is the wheel granularity.
const tick = time.Millisecond

// Entry is an intrusive scheduler entry. The caller owns it, sets Value, and
// must not add it to two schedulers or add it twice without a Fire or Cancel
// in between. Gen increments on every Add and every removal so a caller can
// detect a stale reference to a re-armed entry; moving an entry between the
// internal structures of one scheduler never changes Gen.
type Entry[V any] struct {
	Value V
	// Gen counts arm/disarm transitions.
	Gen uint32

	due   int64
	next  *Entry[V]
	prev  *Entry[V]
	idx   int32 // heap index, or wheel level<<8|slot, or calendar bucket
	where uint8 // whereNone, whereHeap, whereWheel
}

// Where an entry currently lives.
const (
	whereNone uint8 = iota
	whereHeap
	whereWheel
)

// Due returns the due time the entry was added with.
func (e *Entry[V]) Due() int64 { return e.due }

// Scheduled reports whether the entry is currently in a scheduler.
func (e *Entry[V]) Scheduled() bool { return e.where != whereNone }

// Scheduler orders entries by due time. Times are int64 nanoseconds on any
// monotonic origin; the caller passes the same origin to New and Fire.
type Scheduler[V any] interface {
	// Add arms e at due, an int64 nanosecond time on the caller's origin.
	// An e already scheduled in this scheduler is moved to due, so a
	// caller's double Add cannot leave the entry linked twice. A due time
	// already in the past fires at the next Fire.
	Add(e *Entry[V], due int64)
	// Cancel disarms e and reports whether it was scheduled.
	Cancel(e *Entry[V]) bool
	// Next returns a time no later than the first firing (see the package
	// documentation), or false when empty.
	Next() (int64, bool)
	// Fire removes every entry whose (rounded) due time is at or before now,
	// appends them to dst and returns dst. now must not decrease; a smaller
	// now is treated as the last one.
	Fire(now int64, dst []*Entry[V]) []*Entry[V]
	// Len returns the number of scheduled entries.
	Len() int
	// Resolution returns the kind's tick, 0 when exact.
	Resolution() time.Duration
	// Fresh returns an empty scheduler of the same kind whose wheel cursors
	// start at now. A caller that rebuilds its structure takes it from the
	// one it has, so the rebuild cannot meet an invalid kind.
	Fresh(now int64) Scheduler[V]
}

// New builds a scheduler of kind k whose wheel cursors start at now, or
// fails on an invalid kind.
func New[V any](k Kind, now int64) (Scheduler[V], error) {
	k = k.Resolve()
	switch k {
	case Heap:
		return &heap[V]{arity: 2}, nil
	case Heap4:
		return &heap[V]{arity: 4}, nil
	case HierarchicalWheel:
		return newHWheel[V](now), nil
	case HashedWheel:
		return newHashedWheel[V](now), nil
	case HybridHeapWheel:
		return newHybrid[V](now), nil
	case Calendar:
		return newCalendar[V](), nil
	default:
		return nil, errors.New("timer: invalid kind")
	}
}

// tickNone is the "no tick" sentinel of the wheel kinds: smaller than any
// tick a due time can produce, so it never collides with a real tick. A
// negative monotonic origin makes real ticks negative, which is why 0 and -1
// cannot serve as sentinels here.
const tickNone = math.MinInt64

// ceilTick returns the first tick boundary at or after t. Integer division
// truncates towards zero, which is already the ceiling for t <= 0; the
// remainder correction for t > 0 is written so that no due time overflows.
func ceilTick(t int64) int64 {
	q := t / int64(tick)
	if t > 0 && t%int64(tick) != 0 {
		q++
	}
	return q
}

// floorTick returns the tick boundary at or before t.
func floorTick(t int64) int64 {
	q := t / int64(tick)
	if t < 0 && t%int64(tick) != 0 {
		q--
	}
	return q
}

// tickTime converts a tick to nanoseconds, saturating rather than wrapping
// for ticks beyond the int64 range (see the package documentation on due
// times near math.MaxInt64).
func tickTime(t int64) int64 {
	switch {
	case t > math.MaxInt64/int64(tick):
		return math.MaxInt64
	case t < math.MinInt64/int64(tick):
		return math.MinInt64
	}
	return t * int64(tick)
}

// arm marks e as scheduled at due.
func (e *Entry[V]) arm(due int64, where uint8) {
	e.due = due
	e.where = where
	e.Gen++
}

// disarm marks e as removed.
func (e *Entry[V]) disarm() {
	e.where = whereNone
	e.idx = -1
	e.next, e.prev = nil, nil
	e.Gen++
}
