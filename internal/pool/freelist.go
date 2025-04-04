package pool

import "sync"

// freeList is one mutex-guarded bounded LIFO of objects. The engine creates
// one per lane so that publishers and consumers of a lane share a short,
// mostly uncontended critical section.
type freeList[T any] struct {
	mu      sync.Mutex
	items   []*T
	maxIdle int
	reset   func(*T)
	canary  canary[T]
}

func newFreeList[T any](cfg Config[T]) *freeList[T] {
	return &freeList[T]{maxIdle: cfg.maxIdle(), reset: cfg.Reset}
}

func (p *freeList[T]) Get() *T {
	p.mu.Lock()
	if n := len(p.items); n > 0 {
		v := p.items[n-1]
		p.items[n-1] = nil
		p.items = p.items[:n-1]
		p.mu.Unlock()
		p.canary.get(v)
		return v
	}
	p.mu.Unlock()
	return fresh(p.reset)
}

func (p *freeList[T]) Put(v *T) {
	if v == nil {
		return
	}
	p.canary.put(v)
	resetObject(v, p.reset)
	p.mu.Lock()
	if len(p.items) < p.maxIdle {
		p.items = append(p.items, v)
	}
	p.mu.Unlock()
}

// freeBuffers is one mutex-guarded bounded LIFO of buffers of any capacity
// up to maxSize. Get grows on demand: a popped buffer too small for the
// request is dropped and a fresh one allocated, so the list converges on
// the sizes actually requested.
type freeBuffers struct {
	mu      sync.Mutex
	items   [][]byte
	maxIdle int
	maxSize int
	canary  canary[byte]
}

func newFreeBuffers(cfg BufferConfig) *freeBuffers {
	return &freeBuffers{maxIdle: cfg.maxIdle(), maxSize: cfg.maxSize()}
}

func (p *freeBuffers) Get(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	if n > p.maxSize {
		return make([]byte, n)
	}
	p.mu.Lock()
	if k := len(p.items); k > 0 {
		b := p.items[k-1]
		p.items[k-1] = nil
		p.items = p.items[:k-1]
		p.mu.Unlock()
		if cap(b) >= n {
			p.canary.get(bufKey(b))
			return b[:n]
		}
		return make([]byte, n)
	}
	p.mu.Unlock()
	return make([]byte, n)
}

func (p *freeBuffers) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:cap(b)]
	p.canary.put(bufKey(b))
	if cap(b) > p.maxSize {
		return
	}
	poison(b)
	p.mu.Lock()
	if len(p.items) < p.maxIdle {
		p.items = append(p.items, b)
	}
	p.mu.Unlock()
}
