// Package load provides open-loop pacing and closed-loop capacity
// calibration for coordinated-omission-resistant latency benchmarks.
package load

import (
	"time"
)

// Pacer schedules operations at a fixed rate. Intended send times are
// derived from the start time and the operation index, so a slow operation
// does not delay the schedule of the next one (open loop).
type Pacer struct {
	start    time.Time
	interval time.Duration
	n        int64
}

// NewPacer returns a Pacer emitting rate operations per second starting now.
// A non-positive rate yields a pacer that never waits.
func NewPacer(rate float64) *Pacer {
	p := &Pacer{start: time.Now()}
	if rate > 0 {
		p.interval = time.Duration(float64(time.Second) / rate)
	}
	return p
}

// Next returns the intended send time of the next operation and advances the
// schedule.
func (p *Pacer) Next() time.Time {
	t := p.start.Add(time.Duration(p.n) * p.interval)
	p.n++
	return t
}

// Wait returns the intended send time of the next operation after sleeping
// until it is due. The last 50µs are spun to keep timer error small.
func (p *Pacer) Wait() time.Time {
	t := p.Next()
	if p.interval == 0 {
		return t
	}
	for {
		d := time.Until(t)
		if d <= 0 {
			return t
		}
		if d > 50*time.Microsecond {
			time.Sleep(d - 50*time.Microsecond)
			continue
		}
	}
}

// Backlog returns how far behind schedule the pacer is at now: the number
// of operations that should already have been started but have not.
func (p *Pacer) Backlog(now time.Time) int64 {
	if p.interval == 0 {
		return 0
	}
	due := int64(now.Sub(p.start) / p.interval)
	if due < p.n {
		return 0
	}
	return due - p.n
}

// Calibrate runs fn back to back for d and returns the achieved operations
// per second: the closed-loop capacity from which open-loop rates are
// derived.
func Calibrate(d time.Duration, fn func()) float64 {
	start := time.Now()
	n := 0
	for time.Since(start) < d {
		for range 64 {
			fn()
		}
		n += 64
	}
	return float64(n) / time.Since(start).Seconds()
}

// Rates returns the target rates for the standard open-loop sweep at
// 50/80/100/120 percent of capacity.
func Rates(capacity float64) map[string]float64 {
	return map[string]float64{
		"rate=50pct":  capacity * 0.5,
		"rate=80pct":  capacity * 0.8,
		"rate=100pct": capacity,
		"rate=120pct": capacity * 1.2,
	}
}
