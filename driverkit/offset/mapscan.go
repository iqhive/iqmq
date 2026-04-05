package offset

// mapScan is the reference tracker: one map entry per delivered offset that
// has not yet been passed by the watermark, holding whether it completed, and
// a linear scan upward from the watermark after every completion.
//
// It is the simplest implementation that satisfies the contract, so the other
// kinds are measured against it and the model is checked against it first.
type mapScan struct {
	common
	// state maps a delivered offset in [base, high) to its completion flag.
	// Offsets the watermark has passed are deleted, so the map holds at most
	// Window entries.
	state map[int64]bool
}

// mapHint bounds the map preallocation: a full window would reserve memory a
// steady-state tracker never uses.
func mapHint(window int) int {
	if window < 1024 {
		return window
	}
	return 1024
}

func newMapScan(cfg Config) *mapScan {
	return &mapScan{common: newCommon(MapScan, cfg), state: make(map[int64]bool, mapHint(cfg.Window))}
}

// Track implements Tracker.
func (t *mapScan) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	if t.pend == 0 {
		t.base = off
	}
	t.state[off] = false
	t.high = off + 1
	t.pend++
	return nil
}

// Complete implements Tracker.
func (t *mapScan) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen || off < t.base || off >= t.high {
		return t.base, false
	}
	done, ok := t.state[off]
	if !ok || done {
		return t.base, false
	}
	t.state[off] = true
	t.pend--
	before := t.base
	for t.base < t.high {
		d, ok := t.state[t.base]
		if ok && !d {
			break
		}
		if ok {
			delete(t.state, t.base)
		}
		t.base++
	}
	return t.base, t.base > before
}

// Reset implements Tracker.
func (t *mapScan) Reset(next int64) uint32 {
	clear(t.state)
	return t.rebase(next)
}
