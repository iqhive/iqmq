package pool

// noneObjects is the GC-only reference: Get allocates, Put discards.
type noneObjects[T any] struct {
	reset  func(*T)
	canary canary[T]
}

func (p *noneObjects[T]) Get() *T { return fresh(p.reset) }

func (p *noneObjects[T]) Put(v *T) {
	if v == nil {
		return
	}
	p.canary.put(v)
}

// noneBuffers is the GC-only reference for buffers.
type noneBuffers struct {
	canary canary[byte]
}

func (*noneBuffers) Get(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	return make([]byte, n)
}

func (p *noneBuffers) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	p.canary.put(bufKey(b))
}
