// Package latency records latency samples in a log-linear histogram with
// roughly three significant digits of resolution and reports percentiles as
// custom benchmark metrics.
//
// Recording is enabled with the -bench.latency flag, passed to the test
// binary after -args:
//
//	go test -run=^$ -bench=. -args -bench.latency
package latency

import (
	"flag"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
	"testing"
	"time"
)

// Enabled reports whether latency recording was requested.
var Enabled = flag.Bool("bench.latency", false, "record and report latency percentiles as p50-ns/p99-ns/p999-ns")

const (
	subBits = 10 // 1024 sub-buckets per power of two ≈ 0.1% resolution
	subMask = 1<<subBits - 1
	buckets = (1 << subBits) + (63-subBits)<<subBits
)

// Histogram is a fixed-size log-linear histogram of non-negative int64
// values (nanoseconds by convention). The zero value is ready to use. A
// Histogram is not safe for concurrent use; record per worker and Merge.
type Histogram struct {
	counts [buckets]uint64
	total  uint64
	sum    float64
	max    int64
	min    int64
}

func index(v int64) int {
	if v <= 0 {
		return 0
	}
	u := uint64(v)
	e := bits.Len64(u) - 1
	if e < subBits {
		return int(u)
	}
	mant := int(u>>(uint(e)-subBits)) & subMask
	return (1 << subBits) + (e-subBits)<<subBits + mant
}

// valueOf returns the highest value that maps to bucket i.
func valueOf(i int) int64 {
	if i < 1<<subBits {
		return int64(i)
	}
	k := i - (1 << subBits)
	e := subBits + k>>subBits
	mant := k & subMask
	width := int64(1) << (uint(e) - subBits)
	lo := int64(1)<<uint(e) | int64(mant)*width
	return lo + width - 1
}

// Record adds one sample.
func (h *Histogram) Record(v int64) {
	if v < 0 {
		v = 0
	}
	h.counts[index(v)]++
	if h.total == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.total++
	h.sum += float64(v)
}

// RecordSince records the elapsed time since start in nanoseconds.
func (h *Histogram) RecordSince(start time.Time) { h.Record(int64(time.Since(start))) }

// Count returns the number of samples.
func (h *Histogram) Count() uint64 { return h.total }

// Max returns the largest sample.
func (h *Histogram) Max() int64 { return h.max }

// Min returns the smallest sample.
func (h *Histogram) Min() int64 { return h.min }

// Mean returns the arithmetic mean, or 0 without samples.
func (h *Histogram) Mean() float64 {
	if h.total == 0 {
		return 0
	}
	return h.sum / float64(h.total)
}

// Quantile returns the value at quantile q in [0,1]. Without samples it
// returns 0.
func (h *Histogram) Quantile(q float64) int64 {
	if h.total == 0 {
		return 0
	}
	if q <= 0 {
		return h.min
	}
	if q >= 1 {
		return h.max
	}
	target := uint64(q*float64(h.total) + 0.5)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range &h.counts {
		cum += c
		if cum >= target {
			v := valueOf(i)
			if v > h.max {
				v = h.max
			}
			return v
		}
	}
	return h.max
}

// Merge adds o's samples into h.
func (h *Histogram) Merge(o *Histogram) {
	if o.total == 0 {
		return
	}
	for i := range h.counts {
		h.counts[i] += o.counts[i]
	}
	if h.total == 0 || o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
	h.total += o.total
	h.sum += o.sum
}

// Reset clears the histogram.
func (h *Histogram) Reset() { *h = Histogram{} }

// Report publishes p50-ns, p99-ns, p999-ns and max-ns as custom metrics.
func (h *Histogram) Report(b *testing.B) {
	if h.total == 0 {
		return
	}
	b.ReportMetric(float64(h.Quantile(0.5)), "p50-ns")
	b.ReportMetric(float64(h.Quantile(0.99)), "p99-ns")
	b.ReportMetric(float64(h.Quantile(0.999)), "p999-ns")
	b.ReportMetric(float64(h.max), "max-ns")
}

// Write dumps a textual summary and the non-empty buckets.
func (h *Histogram) Write(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "count=%d min=%d mean=%.1f p50=%d p90=%d p99=%d p999=%d p9999=%d max=%d\n",
		h.total, h.min, h.Mean(), h.Quantile(0.5), h.Quantile(0.9), h.Quantile(0.99), h.Quantile(0.999), h.Quantile(0.9999), h.max); err != nil {
		return err
	}
	for i, c := range &h.counts {
		if c == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "%d\t%d\n", valueOf(i), c); err != nil {
			return err
		}
	}
	return nil
}

// Dump writes the histogram to path (conventionally with a .hdr suffix,
// which the repository ignores).
func (h *Histogram) Dump(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := h.Write(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Set records many histograms, one per worker, and merges them for a report.
type Set struct{ hs []*Histogram }

// NewSet returns a Set with n empty histograms.
func NewSet(n int) *Set {
	s := &Set{hs: make([]*Histogram, n)}
	for i := range s.hs {
		s.hs[i] = &Histogram{}
	}
	return s
}

// At returns the i-th histogram.
func (s *Set) At(i int) *Histogram { return s.hs[i] }

// Merged returns a histogram combining every member.
func (s *Set) Merged() *Histogram {
	m := &Histogram{}
	for _, h := range s.hs {
		m.Merge(h)
	}
	return m
}

// Percentiles returns the listed quantiles as a sorted slice of values.
func (h *Histogram) Percentiles(qs ...float64) []int64 {
	out := make([]int64, len(qs))
	sorted := append([]float64(nil), qs...)
	sort.Float64s(sorted)
	for i, q := range sorted {
		out[i] = h.Quantile(q)
	}
	return out
}
