package engine

// PriorityLevels is the number of priority rings a lane holds on a topic with
// TopologyConfig.Priority set (the memory engine, "optional per-priority
// rings"). Level 0 is the lowest and PriorityLevels-1 the highest; a
// publication naming a larger level is clamped to the highest.
//
// Four levels is the smallest set that expresses the distinctions brokers
// actually offer — bulk, normal, high, expedited — while keeping the ring
// array small enough to sit inside one lane without a second allocation per
// level for the topics that never use it.
const PriorityLevels = 4

// priorityOf returns the ring a publication belongs in: 0 on a topic without
// priorities, and the clamped Publish.Priority otherwise.
func (t *Topic[P]) priorityOf(p *Publish[P]) uint8 {
	if !t.cfg.Priority {
		return 0
	}
	if p.Priority >= PriorityLevels {
		return PriorityLevels - 1
	}
	return p.Priority
}

// priorityStore is the per-priority ready structure of a lane: one store of
// the engine's architecture per level, served from the highest level down.
//
// It is a composition rather than a fifth architecture on purpose. Every
// architecture gains priorities at once, the lane keeps one
// in-flight list, one settlement path and one accounting, and a topic without
// priorities pays nothing for them because the lane holds the base store
// directly.
//
// Priorities order the store, not the lane's redeliveries. A record that
// failed is served before the store whatever its level is, which is what the
// retry placement promises — a redelivery is never behind the
// records published after it — and the levels then decide among the records
// nobody has tried yet. Making the levels outrank a redelivery would mean a
// low-priority record that keeps failing could never be retried while urgent
// work arrives, which is the starvation the placement exists to prevent.
type priorityStore[P any] struct {
	rings [PriorityLevels]partitionStore[P]
}

// newPriorityStore builds one store per level.
func newPriorityStore[P any](ln *lane[P], now int64) *priorityStore[P] {
	s := &priorityStore[P]{}
	for i := range s.rings {
		s.rings[i] = newBaseStore(ln, now)
	}
	return s
}

// ring returns the store a record belongs to.
func (s *priorityStore[P]) ring(prio uint8) partitionStore[P] {
	if prio >= PriorityLevels {
		prio = PriorityLevels - 1
	}
	return s.rings[prio]
}

// Publish admits rec to its own level's store.
func (s *priorityStore[P]) Publish(rec *record[P], at, now int64) {
	s.ring(rec.prio).Publish(rec, at, now)
}

// Requeue returns e to the level it was published at, so a redelivery never
// changes a record's urgency.
func (s *priorityStore[P]) Requeue(e *Envelope[P], at, now int64) {
	s.ring(e.rec.prio).Requeue(e, at, now)
}

// Settle releases whatever the record's level holds for it.
func (s *priorityStore[P]) Settle(e *Envelope[P]) { s.ring(e.rec.prio).Settle(e) }

// Claim serves the most urgent level that has anything to give, and moves on
// to the next only once it is empty. A level that is never empty therefore
// starves the ones below it, which is what a priority ring is for: a caller
// that wants fairness across levels does not ask for priorities.
func (s *priorityStore[P]) Claim(now int64, dst []*Envelope[P]) int {
	n := 0
	for i := PriorityLevels - 1; i >= 0 && n < len(dst); i-- {
		n += s.rings[i].Claim(now, dst[n:])
	}
	return n
}

// Depth sums the levels.
func (s *priorityStore[P]) Depth(now int64) (ready, delayed int) {
	for _, r := range s.rings {
		q, d := r.Depth(now)
		ready += q
		delayed += d
	}
	return ready, delayed
}

// DropOldest gives up the least urgent record the lane holds, so that a full
// lane displaces bulk work before expedited work.
func (s *priorityStore[P]) DropOldest() (*Envelope[P], bool) {
	for _, r := range s.rings {
		if e, ok := r.DropOldest(); ok {
			return e, true
		}
	}
	return nil, false
}

// Drain empties every level, least urgent first.
func (s *priorityStore[P]) Drain(dst []*Envelope[P]) []*Envelope[P] {
	for _, r := range s.rings {
		dst = r.Drain(dst)
	}
	return dst
}

// Promote promotes the due records of every level.
func (s *priorityStore[P]) Promote(now int64) int {
	n := 0
	for _, r := range s.rings {
		n += r.Promote(now)
	}
	return n
}

// Next returns the earliest due time across the levels.
func (s *priorityStore[P]) Next() int64 {
	at := int64(0)
	for _, r := range s.rings {
		if t := r.Next(); t > 0 && (at == 0 || t < at) {
			at = t
		}
	}
	return at
}

// ExpireTTL sweeps every level and returns the earliest expiry left.
func (s *priorityStore[P]) ExpireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	next := int64(0)
	for _, r := range s.rings {
		var at int64
		dst, at = r.ExpireTTL(now, dst)
		if at > 0 && (next == 0 || at < next) {
			next = at
		}
	}
	return dst, next
}
