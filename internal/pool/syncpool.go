package pool

import "sync"

// syncObjects wraps sync.Pool. Objects are reset on Put so the pool only
// ever holds clean objects; the runtime drops idle objects after two GC
// cycles.
type syncObjects[T any] struct {
	pool   sync.Pool
	reset  func(*T)
	canary canary[T]
}

func newSyncObjects[T any](cfg Config[T]) *syncObjects[T] {
	p := &syncObjects[T]{reset: cfg.Reset}
	p.pool.New = func() any { return fresh(p.reset) }
	return p
}

func (p *syncObjects[T]) Get() *T {
	v, _ := p.pool.Get().(*T)
	p.canary.get(v)
	return v
}

func (p *syncObjects[T]) Put(v *T) {
	if v == nil {
		return
	}
	p.canary.put(v)
	resetObject(v, p.reset)
	p.pool.Put(v)
}

// syncBuffers keeps buffers in one sync.Pool. Slices are stored in a
// bufBox behind a pointer (a slice stored directly would allocate its
// interface box on every Put); the boxes are themselves recycled through a second pool so
// the steady state allocates nothing. Get grows on demand: a pooled buffer
// too small for the request is dropped and a fresh one allocated.
type syncBuffers struct {
	bufs    sync.Pool // *bufBox with cap > 0
	boxes   sync.Pool // *bufBox, empty
	maxSize int
	canary  canary[byte]
}

// bufBox holds a pooled buffer, so that the pool stores a pointer and Put
// allocates no interface box.
type bufBox struct {
	b []byte
}

func newSyncBuffers(cfg BufferConfig) *syncBuffers {
	return &syncBuffers{maxSize: cfg.maxSize()}
}

func (p *syncBuffers) Get(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	if n > p.maxSize {
		return make([]byte, n)
	}
	box, _ := p.bufs.Get().(*bufBox)
	if box == nil {
		return make([]byte, n)
	}
	b := box.b
	box.b = nil
	p.boxes.Put(box)
	if cap(b) < n {
		return make([]byte, n)
	}
	p.canary.get(bufKey(b))
	return b[:n]
}

func (p *syncBuffers) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:cap(b)]
	p.canary.put(bufKey(b))
	if cap(b) > p.maxSize {
		return
	}
	poison(b)
	box, _ := p.boxes.Get().(*bufBox)
	if box == nil {
		box = &bufBox{}
	}
	box.b = b
	p.bufs.Put(box)
}
