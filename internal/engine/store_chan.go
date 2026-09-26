package engine

import (
	"sync"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/timer"
)

// maxChanCap bounds the buffer a channel-backed lane allocates up front. A
// lane may be configured with a far larger capacity — or with none at all —
// and a Go channel has to reserve its whole buffer when it is made, so beyond
// this size the store keeps the surplus in an overflow slice and refills the
// channel from it. That is exactly the awkwardness the baseline exists to
// expose.
const maxChanCap = 4096

// chanStore is the channel baseline (ChanFanout): the ready set of a lane
// is a buffered Go channel instead of a kind of the queue family, which is
// what an engine written the obvious way would use. Everything else — the
// delayed structure, the envelopes, the accounting — is the same as the
// reference architecture, so the benchmark difference is the queue and
// nothing else.
type chanStore[P any] struct {
	ln *lane[P]
	ch chan *Envelope[P]

	// overN mirrors len(over) so that the common push and pop skip the
	// mutex entirely.
	overN  atomic.Int32
	ndelay atomic.Int32

	// tkind is the scheduler kind the delayed structure is rebuilt with
	// when it has to be emptied (oldestDelayed).
	tkind timer.Kind

	mu      sync.Mutex
	over    []*Envelope[P]
	delayed timer.Scheduler[*Envelope[P]]
	fired   []*timer.Entry[*Envelope[P]]
}

func newChanStore[P any](ln *lane[P]) *chanStore[P] {
	n := ln.sub.capMessages
	if n <= 0 || n > maxChanCap {
		n = maxChanCap
	}
	return &chanStore[P]{
		ln:      ln,
		tkind:   ln.sub.eng.kinds.Timer,
		ch:      make(chan *Envelope[P], n),
		delayed: timer.New[*Envelope[P]](ln.sub.eng.kinds.Timer, ln.sub.eng.now()),
	}
}

// Publish admits a freshly published record.
func (s *chanStore[P]) Publish(rec *record[P], at, now int64) {
	s.place(s.ln.newEnvelope(rec), at, now)
}

// Requeue returns a claimed envelope.
func (s *chanStore[P]) Requeue(e *Envelope[P], at, now int64) { s.place(e, at, now) }

// Settle releases nothing: a channel lane owns no shared slot.
func (s *chanStore[P]) Settle(*Envelope[P]) {}

func (s *chanStore[P]) place(e *Envelope[P], at, now int64) {
	if at > now {
		s.arm(e, at)
		return
	}
	e.deadline = 0
	s.push(e)
}

func (s *chanStore[P]) arm(e *Envelope[P], at int64) {
	e.deadline = at
	s.mu.Lock()
	s.delayed.Add(&e.tnode, at)
	s.ndelay.Add(1)
	s.mu.Unlock()
}

// push appends e to the ready set, in the channel when it fits and in the
// overflow behind whatever is already waiting there otherwise.
func (s *chanStore[P]) push(e *Envelope[P]) {
	if s.overN.Load() == 0 {
		select {
		case s.ch <- e:
			return
		default:
		}
	}
	s.mu.Lock()
	s.over = append(s.over, e)
	s.overN.Store(int32(len(s.over)))
	s.mu.Unlock()
}

// refill moves as much of the overflow into the channel as fits, oldest
// first.
func (s *chanStore[P]) refill() {
	if s.overN.Load() == 0 {
		return
	}
	s.mu.Lock()
	moved := 0
loop:
	for moved < len(s.over) {
		select {
		case s.ch <- s.over[moved]:
			moved++
		default:
			break loop
		}
	}
	if moved > 0 {
		s.over = append(s.over[:0], s.over[moved:]...)
		s.overN.Store(int32(len(s.over)))
	}
	s.mu.Unlock()
}

// pop takes the oldest ready envelope.
func (s *chanStore[P]) pop() (*Envelope[P], bool) {
	select {
	case e := <-s.ch:
		s.refill()
		return e, true
	default:
	}
	if s.overN.Load() == 0 {
		return nil, false
	}
	s.refill()
	select {
	case e := <-s.ch:
		return e, true
	default:
		return nil, false
	}
}

// Claim promotes what has come due and takes up to len(dst) ready records.
func (s *chanStore[P]) Claim(now int64, dst []*Envelope[P]) int {
	if s.ndelay.Load() > 0 {
		s.mu.Lock()
		s.pump(now)
		s.mu.Unlock()
	}
	n := 0
	for n < len(dst) {
		e, ok := s.pop()
		if !ok {
			break
		}
		dst[n] = e
		n++
	}
	return n
}

// pump moves the due delayed envelopes into the ready set. The caller holds
// the store mutex, which push never takes for the channel path; the overflow
// path takes it, so the promoted records are pushed after it is released.
func (s *chanStore[P]) pump(now int64) int {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.fired = s.delayed.Fire(now, s.fired[:0])
	sortFired(s.fired)
	n := 0
	for _, ent := range s.fired {
		e := ent.Value
		select {
		case s.ch <- e:
		default:
			s.over = append(s.over, e)
			s.overN.Store(int32(len(s.over)))
		}
		n++
	}
	s.ndelay.Add(int32(-n))
	clear(s.fired)
	s.fired = s.fired[:0]
	return n
}

// Depth reports the ready and delayed counts.
func (s *chanStore[P]) Depth(now int64) (int, int) {
	ready := len(s.ch) + int(s.overN.Load())
	if s.ndelay.Load() == 0 {
		return ready, 0
	}
	s.mu.Lock()
	ready += s.pump(now)
	delayed := int(s.ndelay.Load())
	s.mu.Unlock()
	return ready, delayed
}

// DropOldest takes the oldest record the store holds.
func (s *chanStore[P]) DropOldest() (*Envelope[P], bool) {
	if e, ok := s.pop(); ok {
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
	s.delayed, s.fired, e, ok = oldestDelayed(s.delayed, s.tkind, s.ln.sub.eng.now(), s.fired)
	clear(s.fired)
	s.fired = s.fired[:0]
	if !ok {
		return nil, false
	}
	s.ndelay.Add(-1)
	return e, true
}

// Drain removes everything the store holds, ready records first and in their
// queue order: the channel holds the oldest and the overflow the rest. That
// order is what a caller which puts the survivors back — the time-to-live
// sweep, through expireByDrain — needs, because it requeues them in the order
// they came out and would otherwise permute a lane deep enough to have used
// the overflow. The mutex is held across both halves so that a concurrent
// refill cannot move a record from the overflow into the channel in between.
func (s *chanStore[P]) Drain(dst []*Envelope[P]) []*Envelope[P] {
	s.mu.Lock()
	for done := false; !done; {
		select {
		case e := <-s.ch:
			dst = append(dst, e)
		default:
			done = true
		}
	}
	dst = append(dst, s.over...)
	clear(s.over)
	s.over = s.over[:0]
	s.overN.Store(0)
	s.fired = s.delayed.Fire(neverExpires, s.fired[:0])
	sortFired(s.fired)
	for _, ent := range s.fired {
		dst = append(dst, ent.Value)
	}
	// See persubStore.Drain: a fired-out wheel would make every record
	// armed afterwards due at once.
	s.delayed = timer.New[*Envelope[P]](s.tkind, s.ln.sub.eng.now())
	s.ndelay.Store(0)
	clear(s.fired)
	s.fired = s.fired[:0]
	s.mu.Unlock()
	return dst
}

// Next returns the earliest due time of a delayed record.
func (s *chanStore[P]) Next() int64 {
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
func (s *chanStore[P]) ExpireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	return expireByDrain[P](s, now, dst)
}

// Promote moves the records that have come due into the ready set.
func (s *chanStore[P]) Promote(now int64) int {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pump(now)
}
