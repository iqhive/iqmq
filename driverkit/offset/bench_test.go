package offset

import (
	"runtime"
	"testing"
)

// The four delivery patterns of the performance budgets (KAF-02): sequential
// (every record completes in order), random (a window of records completes in
// arbitrary order), gaps (a quarter of the offsets are never delivered) and
// widewindow (one slow record keeps the window open over thousands of
// completed ones). One iteration is one record: it is delivered and completed,
// so ns/op is the "ack + watermark" cost the budget bounds at 20 ns with zero
// allocations.

// benchInflight is the number of records in flight for the random pattern.
const benchInflight = 64

// BenchmarkKafkaOffsetTracker/<kind>/<pattern>
func BenchmarkKafkaOffsetTracker(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("sequential", func(b *testing.B) { benchSequential(b, k) })
			b.Run("random", func(b *testing.B) { benchRandom(b, k) })
			b.Run("gaps", func(b *testing.B) { benchGaps(b, k) })
			b.Run("widewindow", func(b *testing.B) { benchWide(b, k) })
		})
	}
}

func benchSequential(b *testing.B, k Kind) {
	tr := New(k, Config{Window: 4096})
	gen := tr.Gen()
	var off int64
	b.ReportAllocs()
	for b.Loop() {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		tr.Complete(gen, off)
		off++
	}
}

func benchRandom(b *testing.B, k Kind) {
	tr := New(k, Config{Window: 4096})
	gen := tr.Gen()
	inflight := make([]int64, 0, benchInflight)
	var off int64
	rng := uint64(0x9e3779b97f4a7c15)
	for range benchInflight {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		inflight = append(inflight, off)
		off++
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		rng = xorshift(rng)
		i := int(rng % uint64(len(inflight)))
		tr.Complete(gen, inflight[i])
		inflight[i] = off
		off++
	}
}

func benchGaps(b *testing.B, k Kind) {
	tr := New(k, Config{Window: 4096})
	gen := tr.Gen()
	inflight := make([]int64, 0, 8)
	var off int64
	rng := uint64(0xbf58476d1ce4e5b9)
	step := func() int64 {
		// Three of every four offsets are delivered; the fourth is a record
		// the broker never hands over (a compacted key or a marker).
		rng = xorshift(rng)
		if rng&3 == 0 {
			return 2
		}
		return 1
	}
	for range 8 {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		inflight = append(inflight, off)
		off += step()
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		rng = xorshift(rng)
		i := int(rng % uint64(len(inflight)))
		tr.Complete(gen, inflight[i])
		inflight[i] = off
		off += step()
	}
}

// benchWide holds one record in flight while several thousand later records
// complete, so the tracking window stays wide: the pattern that separates a
// flat bitset from a segmented one and from the ordered structures.
func benchWide(b *testing.B, k Kind) {
	const window = 1 << 16
	const span = 4096
	tr := New(k, Config{Window: window})
	gen := tr.Gen()
	var off int64
	blocker := off
	if err := tr.Track(gen, blocker); err != nil {
		b.Fatal(err)
	}
	off++
	n := 0
	b.ReportAllocs()
	for b.Loop() {
		if err := tr.Track(gen, off); err != nil {
			b.Fatal(err)
		}
		tr.Complete(gen, off)
		off++
		if n++; n == span {
			// Release the blocker, which advances the watermark over the whole
			// span, and open a new one.
			tr.Complete(gen, blocker)
			blocker = off
			if err := tr.Track(gen, blocker); err != nil {
				b.Fatal(err)
			}
			off++
			n = 0
		}
	}
	b.StopTimer()
	b.ReportMetric(footprint(k, Config{Window: window}, span), "B/pending")
}

// footprint measures the heap a tracker holds while pending records are in
// flight, in bytes per record: the "bounded memory" half of the criterion.
func footprint(k Kind, cfg Config, pending int) float64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	tr := New(k, cfg)
	gen := tr.Gen()
	// One blocker at the bottom keeps every later record in the window.
	if err := tr.Track(gen, 0); err != nil {
		panic(err)
	}
	for off := int64(1); off <= int64(pending); off++ {
		if err := tr.Track(gen, off); err != nil {
			panic(err)
		}
		if off%2 == 0 {
			tr.Complete(gen, off)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(tr)
	return float64(after.HeapAlloc-before.HeapAlloc) / float64(pending)
}

// xorshift is a dependency-free pseudo-random step; the sequence only needs to
// be reproducible and cheap, not statistically strong.
func xorshift(x uint64) uint64 {
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	return x
}
