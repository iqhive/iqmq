package pool

import (
	"sync"
	"sync/atomic"
)

// stripe is one mutex-guarded LIFO on its own cache line.
type stripe[T any] struct {
	mu    sync.Mutex
	items []*T
	_     [32]byte
}

// stripedStack is a bounded LIFO spread over several stripes. Callers start
// at the stripe the last Put landed in (so an uncontended caller finds its
// own items again) and probe with TryLock, so a stripe another goroutine
// holds is skipped rather than waited for; a Get scans every stripe before
// allocating and a Put drops the item when every stripe is full or locked.
type stripedStack[T any] struct {
	hint      atomic.Uint32 // stripe of the last successful Put
	stripes   []stripe[T]
	mask      uint32
	perStripe int
}

func newStripedStack[T any](maxIdle int) stripedStack[T] {
	n, per := stripeLayout(maxIdle)
	return stripedStack[T]{stripes: make([]stripe[T], n), mask: uint32(n - 1), perStripe: per}
}

// stripeLayout returns the stripe count and per-stripe bound for maxIdle
// idle items in total: never more stripes than items, and count × bound
// <= maxIdle.
func stripeLayout(maxIdle int) (count, perStripe int) {
	count = stripeCount()
	for count > maxIdle {
		count >>= 1
	}
	return count, max(1, maxIdle/count)
}

// pop returns an idle item, or nil.
func (s *stripedStack[T]) pop() *T {
	start := s.hint.Load()
	for i := range uint32(len(s.stripes)) {
		st := &s.stripes[(start+i)&s.mask]
		if !st.mu.TryLock() {
			continue
		}
		if n := len(st.items); n > 0 {
			v := st.items[n-1]
			st.items[n-1] = nil
			st.items = st.items[:n-1]
			st.mu.Unlock()
			return v
		}
		st.mu.Unlock()
	}
	return nil
}

// push files v on a stripe with room and reports whether it was retained.
func (s *stripedStack[T]) push(v *T) bool {
	start := s.hint.Load()
	for i := range uint32(len(s.stripes)) {
		idx := (start + i) & s.mask
		st := &s.stripes[idx]
		if !st.mu.TryLock() {
			continue
		}
		if len(st.items) < s.perStripe {
			st.items = append(st.items, v)
			st.mu.Unlock()
			if idx != start {
				s.hint.Store(idx)
			}
			return true
		}
		st.mu.Unlock()
	}
	return false
}

// stripedObjects is the SizeClass object pool: a fixed-size T has exactly
// one class, so the pool is one shared striped stack.
type stripedObjects[T any] struct {
	stack  stripedStack[T]
	reset  func(*T)
	canary canary[T]
}

func newStripedObjects[T any](cfg Config[T]) *stripedObjects[T] {
	return &stripedObjects[T]{stack: newStripedStack[T](cfg.maxIdle()), reset: cfg.Reset}
}

func (p *stripedObjects[T]) Get() *T {
	if v := p.stack.pop(); v != nil {
		p.canary.get(v)
		return v
	}
	return fresh(p.reset)
}

func (p *stripedObjects[T]) Put(v *T) {
	if v == nil {
		return
	}
	p.canary.put(v)
	resetObject(v, p.reset)
	p.stack.push(v)
}

// classedBuffers is the SizeClass buffer pool: one striped stack per
// power-of-two class from MinClass to maxSize. Stripes store slice headers
// directly, so Get and Put copy 24 bytes and allocate nothing.
type classedBuffers struct {
	classes []bufStripes
	maxSize int
	canary  canary[byte]
}

// bufStripes is a stripedStack specialised for slice headers.
type bufStripes struct {
	hint      atomic.Uint32
	stripes   []bufStripe
	mask      uint32
	perStripe int
}

type bufStripe struct {
	mu    sync.Mutex
	items [][]byte
	_     [32]byte
}

func newBufStripes(maxIdle int) bufStripes {
	n, per := stripeLayout(maxIdle)
	return bufStripes{stripes: make([]bufStripe, n), mask: uint32(n - 1), perStripe: per}
}

func (s *bufStripes) pop() ([]byte, bool) {
	start := s.hint.Load()
	for i := range uint32(len(s.stripes)) {
		st := &s.stripes[(start+i)&s.mask]
		if !st.mu.TryLock() {
			continue
		}
		if n := len(st.items); n > 0 {
			b := st.items[n-1]
			st.items[n-1] = nil
			st.items = st.items[:n-1]
			st.mu.Unlock()
			return b, true
		}
		st.mu.Unlock()
	}
	return nil, false
}

func (s *bufStripes) push(b []byte) {
	start := s.hint.Load()
	for i := range uint32(len(s.stripes)) {
		idx := (start + i) & s.mask
		st := &s.stripes[idx]
		if !st.mu.TryLock() {
			continue
		}
		if len(st.items) < s.perStripe {
			st.items = append(st.items, b)
			st.mu.Unlock()
			if idx != start {
				s.hint.Store(idx)
			}
			return
		}
		st.mu.Unlock()
	}
}

func newClassedBuffers(cfg BufferConfig) *classedBuffers {
	maxSize := cfg.maxSize()
	p := &classedBuffers{classes: make([]bufStripes, classIndex(maxSize)+1), maxSize: maxSize}
	for i := range p.classes {
		p.classes[i] = newBufStripes(cfg.maxIdle())
	}
	return p
}

func (p *classedBuffers) Get(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	if n > p.maxSize {
		return make([]byte, n)
	}
	c := classOf(n)
	if b, ok := p.classes[classIndex(c)].pop(); ok {
		p.canary.get(bufKey(b))
		return b[:n]
	}
	return make([]byte, n, c)
}

func (p *classedBuffers) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:cap(b)]
	p.canary.put(bufKey(b))
	c := floorClass(cap(b))
	if c == 0 || cap(b) > p.maxSize {
		return
	}
	poison(b)
	p.classes[classIndex(c)].push(b)
}
