package timer

import "time"

// Calendar queue policy (Brown, "Calendar Queues: A Fast O(1) Priority Queue
// Implementation for the Simulation Event Set Problem", CACM 1988).
const (
	calMinBuckets   = 256     // never shrink below this many buckets
	calMaxBuckets   = 1 << 24 // never grow beyond this many buckets
	calSamples      = 25      // earliest entries sampled for the width estimate
	calDefaultWidth = int64(time.Millisecond)
)

// calendar is an adaptive calendar queue: nb buckets ("days") of width w
// nanoseconds form a year of nb*w; an entry lives in bucket
// floor(due/w) mod nb, sorted by due within the bucket, so a bucket holds
// entries from several years. The bucket count doubles when the queue holds
// more than two entries per bucket and halves below one half, and every
// resize re-estimates the width from the spacing of the earliest entries,
// which keeps buckets short (O(1) sorted insertion) for the distribution
// actually present. Fire is exact and ordered.
//
// The dequeue position (lastBucket, bucketTop) satisfies: every scheduled
// entry has due >= bucketTop-width, and bucketTop is the exclusive upper due
// of lastBucket in the year being scanned. Scanning buckets from there in
// order, the first head with due < its bucket's top is the minimum; a whole
// empty year falls back to a direct search over every bucket.
//
// A width estimated from a dense head is far too small once only a sparse
// tail remains and every peek then walks long runs of empty buckets. The
// queue counts the buckets it scans and, when that work exceeds the cost of
// a rebuild, rebuilds at the same size with a fresh width estimate; the
// scanning overhead is thereby bounded by a constant factor of the rebuild
// work it triggers.
type calendar[V any] struct {
	buckets    []bucket[V]
	mask       int64
	width      int64
	n          int
	lastBucket int
	bucketTop  int64
	minCache   *Entry[V] // known minimum, nil when unknown
	scanned    int       // buckets examined by peek since the last resize
	resizing   bool
}

func newCalendar[V any]() *calendar[V] {
	return &calendar[V]{
		buckets:   make([]bucket[V], calMinBuckets),
		mask:      calMinBuckets - 1,
		width:     calDefaultWidth,
		bucketTop: calDefaultWidth,
	}
}

func (c *calendar[V]) Resolution() time.Duration { return 0 }

func (c *calendar[V]) Len() int { return c.n }

// floorDiv is a/b rounded towards negative infinity, for b > 0.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func (c *calendar[V]) bucketOf(due int64) int { return int(floorDiv(due, c.width) & c.mask) }

// topOf returns the exclusive upper due of the bucket day containing due.
func (c *calendar[V]) topOf(due int64) int64 { return (floorDiv(due, c.width) + 1) * c.width }

// insert files e in its bucket without touching n or the position.
func (c *calendar[V]) insert(e *Entry[V]) {
	i := c.bucketOf(e.due)
	e.idx = int32(i)
	c.buckets[i].insertSorted(e)
}

func (c *calendar[V]) Add(e *Entry[V], due int64) {
	e.checkFree()
	e.arm(due, whereWheel)
	c.insert(e)
	c.n++
	if c.n == 1 {
		c.lastBucket, c.bucketTop, c.minCache = c.bucketOf(due), c.topOf(due), e
	} else {
		if due < c.bucketTop-c.width {
			c.lastBucket, c.bucketTop = c.bucketOf(due), c.topOf(due)
		}
		if c.minCache != nil && due < c.minCache.due {
			c.minCache = e
		}
	}
	if !c.resizing && c.n > 2*len(c.buckets) && len(c.buckets) < calMaxBuckets {
		c.resize(2 * len(c.buckets))
	}
}

// unlink removes e from its bucket without disarming it.
func (c *calendar[V]) unlink(e *Entry[V]) {
	c.buckets[e.idx].remove(e)
	c.n--
	if c.minCache == e {
		c.minCache = nil
	}
	if !c.resizing && c.n < len(c.buckets)/2 && len(c.buckets) > calMinBuckets {
		c.resize(len(c.buckets) / 2)
	}
}

func (c *calendar[V]) Cancel(e *Entry[V]) bool {
	if e.where != whereWheel {
		return false
	}
	c.unlink(e)
	e.disarm()
	return true
}

// peek returns the earliest entry, or nil when empty.
func (c *calendar[V]) peek() *Entry[V] {
	if c.minCache != nil || c.n == 0 {
		return c.minCache
	}
	i, top := c.lastBucket, c.bucketTop
	for k := 1; k <= len(c.buckets); k++ {
		if h := c.buckets[i].head; h != nil && h.due < top {
			c.lastBucket, c.bucketTop, c.minCache = i, top, h
			c.noteScan(k)
			return h
		}
		i = int((int64(i) + 1) & c.mask)
		top += c.width
	}
	// A whole year without an event: find the minimum directly.
	var m *Entry[V]
	for j := range c.buckets {
		if h := c.buckets[j].head; h != nil && (m == nil || h.due < m.due) {
			m = h
		}
	}
	c.lastBucket, c.bucketTop, c.minCache = c.bucketOf(m.due), c.topOf(m.due), m
	c.noteScan(2 * len(c.buckets))
	return m
}

// noteScan accounts for k buckets examined and re-estimates the width when
// scanning has cost more than rebuilding would.
func (c *calendar[V]) noteScan(k int) {
	c.scanned += k
	if !c.resizing && c.scanned > 4*(len(c.buckets)+c.n) {
		c.resize(len(c.buckets))
	}
}

func (c *calendar[V]) Next() (int64, bool) {
	if e := c.peek(); e != nil {
		return e.due, true
	}
	return 0, false
}

func (c *calendar[V]) Fire(now int64, dst []*Entry[V]) []*Entry[V] {
	for {
		e := c.peek()
		if e == nil || e.due > now {
			return dst
		}
		c.unlink(e)
		e.disarm()
		dst = append(dst, e)
	}
}

// resize rebuilds the queue with newNb buckets and a width estimated from
// the spacing of the earliest entries. Shrinking reuses the bucket array;
// growing past its capacity allocates once.
func (c *calendar[V]) resize(newNb int) {
	c.resizing = true
	var sample [calSamples]*Entry[V]
	k := 0
	for k < len(sample) && c.n > 0 {
		e := c.peek()
		c.unlink(e)
		sample[k] = e
		k++
	}
	c.width = c.estimateWidth(sample[:k])
	// Chain every remaining entry through next and empty the buckets.
	var chain *Entry[V]
	for i := range c.buckets {
		if b := &c.buckets[i]; b.head != nil {
			b.tail.next = chain
			chain = b.head
			*b = bucket[V]{}
		}
	}
	if newNb <= cap(c.buckets) {
		old := len(c.buckets)
		c.buckets = c.buckets[:newNb]
		if newNb > old {
			clear(c.buckets[old:])
		}
	} else {
		c.buckets = make([]bucket[V], newNb)
	}
	c.mask = int64(newNb - 1)
	for chain != nil {
		next := chain.next
		c.insert(chain)
		chain = next
	}
	for _, e := range sample[:k] {
		c.insert(e)
	}
	c.n += k
	if k > 0 {
		m := sample[0]
		c.lastBucket, c.bucketTop, c.minCache = c.bucketOf(m.due), c.topOf(m.due), m
	} else {
		c.lastBucket, c.bucketTop, c.minCache = 0, c.width, nil
	}
	c.scanned = 0
	c.resizing = false
}

// estimateWidth is Brown's rule: three times the average separation of the
// sampled (sorted) entries, averaged once more without separations above
// twice the first average so that a few outliers do not widen the buckets.
// Fewer than two samples or identical dues keep the current width.
func (c *calendar[V]) estimateWidth(sample []*Entry[V]) int64 {
	if len(sample) < 2 {
		return c.width
	}
	var sum int64
	for i := 1; i < len(sample); i++ {
		sum += sample[i].due - sample[i-1].due
	}
	avg := sum / int64(len(sample)-1)
	if avg <= 0 {
		return c.width
	}
	var sum2, cnt2 int64
	for i := 1; i < len(sample); i++ {
		if g := sample[i].due - sample[i-1].due; g <= 2*avg {
			sum2 += g
			cnt2++
		}
	}
	if cnt2 == 0 || sum2 == 0 {
		return 3 * avg
	}
	return max(3*sum2/cnt2, 1)
}
