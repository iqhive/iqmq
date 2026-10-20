package engine

import (
	"sync"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/timer"
)

// persubStore is the reference architecture (PerSubEnvelopes, and the storage FullCopy also uses): the ready set is a kind of the queue family
// (the specification) holding one pooled envelope per record, and the delayed set is a kind
// of the scheduler family. Nothing is shared with another subscription,
// so a subscription that stops consuming fills its own lane and no one
// else's.
type persubStore[P any] struct {
	ln    *lane[P]
	ready readyQueue[*Envelope[P]]

	// ndelay mirrors the length of the delayed structure so that a consumer
	// can skip the store mutex when nothing is waiting for its due time.
	ndelay atomic.Int32

	mu      sync.Mutex
	delayed timer.Scheduler[*Envelope[P]]
	fired   []*timer.Entry[*Envelope[P]]
}

func newPersubStore[P any](ln *lane[P], now int64) (*persubStore[P], error) {
	ks := ln.sub.eng.kinds
	qk, qcap := ks.Queue, ln.sub.capMessages
	if qcap <= 0 {
		// Only an explicitly negative CapacityMessages reaches this branch:
		// TopologyConfig.normalise reads a zero capacity as "unset" and
		// gives it DefaultCapacityMessages, so that the engine's zero value
		// is the bounded 65,536-record lane the memory defaults ask for
		// (adr/ADR-017-assumptions.md, "Capacity zero means the default").
		// A lane the caller asked to be unbounded takes the unbounded kind
		// whatever kind was selected, since no other kind can
		// grow past its capacity.
		qk, qcap = queue.Chunked, 1
	}
	ready, err := newReadyQueue[*Envelope[P]](qk, qcap)
	if err != nil {
		return nil, err
	}
	delayed, err := timer.New[*Envelope[P]](ks.Timer, now)
	if err != nil {
		return nil, err
	}
	return &persubStore[P]{
		ln:      ln,
		ready:   ready,
		delayed: delayed,
	}, nil
}

// Publish admits a freshly published record.
func (s *persubStore[P]) Publish(rec *record[P], at, now int64) {
	s.place(s.ln.newEnvelope(rec), at, now)
}

// Requeue returns a claimed envelope.
func (s *persubStore[P]) Requeue(e *Envelope[P], at, now int64) { s.place(e, at, now) }

// Settle releases nothing: a per-subscription envelope owns no shared slot.
func (s *persubStore[P]) Settle(*Envelope[P]) {}

// place puts e in the ready queue or the delayed structure.
func (s *persubStore[P]) place(e *Envelope[P], at, now int64) {
	if at > now {
		s.arm(e, at)
		return
	}
	e.deadline = 0
	if !s.ready.TryPush(e) {
		// The ready queue is sized for the lane capacity, which is reserved
		// before a record is admitted, so this is unreachable; parking the
		// record on the scheduler at the current instant loses nothing and
		// keeps the record accounted for.
		s.arm(e, now)
	}
}

// arm parks e in the delayed structure until at.
func (s *persubStore[P]) arm(e *Envelope[P], at int64) {
	e.deadline = at
	s.mu.Lock()
	s.delayed.Add(&e.tnode, at)
	s.ndelay.Add(1)
	s.mu.Unlock()
}

// Claim promotes every record that has come due and takes up to len(dst) of
// the ready ones.
func (s *persubStore[P]) Claim(now int64, dst []*Envelope[P]) int {
	if s.ndelay.Load() > 0 {
		s.mu.Lock()
		s.pump(now)
		s.mu.Unlock()
	}
	n := 0
	for n < len(dst) {
		e, ok := s.ready.TryPop()
		if !ok {
			break
		}
		dst[n] = e
		n++
	}
	return n
}

// pump moves every delayed envelope that is due at now into the ready queue
// and returns the number promoted. The caller holds the store mutex.
func (s *persubStore[P]) pump(now int64) int {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.fired = s.delayed.Fire(now, s.fired[:0])
	sortFired(s.fired)
	n := 0
	for _, ent := range s.fired {
		e := ent.Value
		if s.ready.TryPush(e) {
			n++
			continue
		}
		// Unreachable for the same reason as in place; keeping the record
		// delayed loses nothing.
		s.delayed.Add(&e.tnode, now)
	}
	s.ndelay.Add(int32(-n))
	clear(s.fired)
	s.fired = s.fired[:0]
	return n
}

// Depth reports the ready and delayed counts. Records that are due but not
// yet promoted count as ready, so that the gauge does not depend on whether
// a consumer has looked at the lane since the clock moved.
func (s *persubStore[P]) Depth(now int64) (int, int) {
	ready := s.ready.Len()
	if s.ndelay.Load() == 0 {
		return ready, 0
	}
	s.mu.Lock()
	ready += s.pump(now)
	delayed := int(s.ndelay.Load())
	s.mu.Unlock()
	return ready, delayed
}

// DropOldest takes the oldest record the store holds: what a consumer would
// have taken next, then what is waiting for a due time.
func (s *persubStore[P]) DropOldest() (*Envelope[P], bool) {
	if e, ok := s.ready.TryPop(); ok {
		return e, true
	}
	if s.ndelay.Load() == 0 {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		e  *Envelope[P]
		ok bool
	)
	s.delayed, s.fired, e, ok = oldestDelayed(s.delayed, s.ln.sub.eng.now(), s.fired)
	clear(s.fired)
	s.fired = s.fired[:0]
	if !ok {
		return nil, false
	}
	s.ndelay.Add(-1)
	return e, true
}

// Drain removes everything the store holds.
func (s *persubStore[P]) Drain(dst []*Envelope[P]) []*Envelope[P] {
	s.mu.Lock()
	s.fired = s.delayed.Fire(neverExpires, s.fired[:0])
	sortFired(s.fired)
	for _, ent := range s.fired {
		dst = append(dst, ent.Value)
	}
	// Firing at the end of time leaves a wheel kind's own clock there, where
	// every record armed afterwards would be due at once; a caller that puts
	// the survivors back (expireByDrain) needs the structure as it was.
	s.delayed = s.delayed.Fresh(s.ln.sub.eng.now())
	s.ndelay.Store(0)
	clear(s.fired)
	s.fired = s.fired[:0]
	s.mu.Unlock()
	for {
		e, ok := s.ready.TryPop()
		if !ok {
			return dst
		}
		dst = append(dst, e)
	}
}

// Next returns the earliest due time of a delayed record.
func (s *persubStore[P]) Next() int64 {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.delayed.Next()
	if !ok {
		return 0
	}
	return at
}

// ExpireTTL drains the store, drops what has expired and puts the rest back.
func (s *persubStore[P]) ExpireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	return expireByDrain[P](s, now, dst)
}

// Promote moves the records that have come due into the ready set.
func (s *persubStore[P]) Promote(now int64) int {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pump(now)
}
