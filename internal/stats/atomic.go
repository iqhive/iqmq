package stats

import "sync/atomic"

// atomicCounter is the reference: one cell every writer contends on.
type atomicCounter struct {
	v atomic.Int64
}

func (c *atomicCounter) Add(delta int64)          { c.v.Add(delta) }
func (c *atomicCounter) AddAt(_ int, delta int64) { c.v.Add(delta) }
func (c *atomicCounter) Load() int64              { return c.v.Load() }
