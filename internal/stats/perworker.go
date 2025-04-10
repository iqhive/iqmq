package stats

// perWorker gives each worker hint its own cell, which only that worker
// writes when hints are distinct and in range, so an AddAt never bounces a
// cache line between cores. Anonymous Adds land on one shared cell. Load
// aggregates every cell on read.
type perWorker struct {
	shared cell
	cells  []cell
	mask   uint64
}

func newPerWorker(workers int) *perWorker {
	n := pow2ceil(max(workers, 1))
	return &perWorker{cells: make([]cell, n), mask: uint64(n - 1)}
}

func (c *perWorker) Add(delta int64) { c.shared.v.Add(delta) }

func (c *perWorker) AddAt(worker int, delta int64) {
	c.cells[uint64(worker)&c.mask].v.Add(delta)
}

func (c *perWorker) Load() int64 {
	sum := c.shared.v.Load()
	for i := range c.cells {
		sum += c.cells[i].v.Load()
	}
	return sum
}
