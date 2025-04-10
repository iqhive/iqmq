package stats

// striped spreads increments over a power-of-two number of cells. AddAt
// folds the worker hint onto a cell, so workers with distinct hints never
// share a line; Add picks a pseudo-random cell. Load sums every cell.
type striped struct {
	cells []cell
	mask  uint64
}

func newStriped(workers int) *striped {
	n := pow2ceil(max(workers, 1))
	return &striped{cells: make([]cell, n), mask: uint64(n - 1)}
}

func (c *striped) Add(delta int64) { c.cells[pick(c.mask)].v.Add(delta) }

func (c *striped) AddAt(worker int, delta int64) {
	c.cells[uint64(worker)&c.mask].v.Add(delta)
}

func (c *striped) Load() int64 {
	var sum int64
	for i := range c.cells {
		sum += c.cells[i].v.Load()
	}
	return sum
}
