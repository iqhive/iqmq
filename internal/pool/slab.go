package pool

import "sync"

// slabObjects allocates objects chunk at a time ([]T of Config.Chunk
// elements) and keeps the free ones on a mutex-guarded bounded LIFO. Objects
// of one chunk share one allocation, so the chunk stays resident until every
// one of its objects is unreachable.
type slabObjects[T any] struct {
	mu      sync.Mutex
	items   []*T
	maxIdle int
	chunk   int
	reset   func(*T)
	canary  canary[T]
}

func newSlabObjects[T any](cfg Config[T]) *slabObjects[T] {
	return &slabObjects[T]{maxIdle: cfg.maxIdle(), chunk: cfg.chunk(), reset: cfg.Reset}
}

func (p *slabObjects[T]) Get() *T {
	p.mu.Lock()
	if n := len(p.items); n > 0 {
		v := p.items[n-1]
		p.items[n-1] = nil
		p.items = p.items[:n-1]
		p.mu.Unlock()
		p.canary.get(v)
		return v
	}
	mem := make([]T, p.chunk)
	if p.reset != nil {
		for i := range mem {
			p.reset(&mem[i])
		}
	}
	for i := 1; i < len(mem); i++ {
		p.items = append(p.items, &mem[i])
	}
	p.mu.Unlock()
	return &mem[0]
}

func (p *slabObjects[T]) Put(v *T) {
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

// slabBuffers carves byte chunks into equal slots per power-of-two class
// and keeps free slots on a per-class mutex-guarded bounded LIFO. A slot is
// a three-index slice of its chunk, so appending to it can never spill into
// a neighbour.
type slabBuffers struct {
	classes []slabClass
	maxSize int
	canary  canary[byte]
}

type slabClass struct {
	mu    sync.Mutex
	items [][]byte
	size  int // slot capacity
	slots int // slots per chunk
	limit int // idle slots retained
}

func newSlabBuffers(cfg BufferConfig) *slabBuffers {
	maxSize, maxIdle, chunk := cfg.maxSize(), cfg.maxIdle(), cfg.chunk()
	p := &slabBuffers{classes: make([]slabClass, classIndex(maxSize)+1), maxSize: maxSize}
	for i := range p.classes {
		size := MinClass << i
		p.classes[i] = slabClass{size: size, slots: max(1, min(chunk/size, maxIdle)), limit: maxIdle}
	}
	return p
}

func (p *slabBuffers) Get(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	if n > p.maxSize {
		return make([]byte, n)
	}
	c := &p.classes[classIndex(classOf(n))]
	c.mu.Lock()
	if k := len(c.items); k > 0 {
		b := c.items[k-1]
		c.items[k-1] = nil
		c.items = c.items[:k-1]
		c.mu.Unlock()
		p.canary.get(bufKey(b))
		return b[:n]
	}
	mem := make([]byte, c.slots*c.size)
	for i := 1; i < c.slots; i++ {
		off := i * c.size
		c.items = append(c.items, mem[off:off:off+c.size])
	}
	c.mu.Unlock()
	return mem[:n:c.size]
}

func (p *slabBuffers) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:cap(b)]
	p.canary.put(bufKey(b))
	fc := floorClass(cap(b))
	if fc == 0 || cap(b) > p.maxSize {
		return
	}
	poison(b)
	c := &p.classes[classIndex(fc)]
	c.mu.Lock()
	if len(c.items) < c.limit {
		c.items = append(c.items, b)
	}
	c.mu.Unlock()
}
