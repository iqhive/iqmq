package latency

import (
	"bytes"
	"flag"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestIndexMonotoneAndResolution(t *testing.T) {
	prev := -1
	for _, v := range []int64{0, 1, 2, 1023, 1024, 1025, 2047, 2048, 4096, 1 << 20, 1<<20 + 512, 1 << 40, 1 << 62} {
		i := index(v)
		if i < prev {
			t.Fatalf("index not monotone at %d", v)
		}
		prev = i
		if i >= buckets {
			t.Fatalf("index %d out of range for %d", i, v)
		}
		hi := valueOf(i)
		if hi < v {
			t.Fatalf("valueOf(index(%d)) = %d < value", v, hi)
		}
		if v >= 1024 && float64(hi-v)/float64(v) > 0.002 {
			t.Fatalf("resolution at %d is %d (%.4f)", v, hi-v, float64(hi-v)/float64(v))
		}
	}
	if index(-5) != 0 || index(1<<63-1) < buckets-2000 {
		t.Error("edge indices")
	}
}

func TestQuantilesAgainstSort(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var h Histogram
	xs := make([]int64, 0, 100000)
	for range 100000 {
		v := int64(r.ExpFloat64() * 50000)
		xs = append(xs, v)
		h.Record(v)
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := xs[int(q*float64(len(xs)))]
		got := h.Quantile(q)
		if float64(got) < float64(exact)*0.99 || float64(got) > float64(exact)*1.01+1 {
			t.Errorf("q%.3f: histogram %d vs exact %d", q, got, exact)
		}
	}
	if h.Count() != 100000 || h.Min() != xs[0] || h.Max() != xs[len(xs)-1] {
		t.Errorf("count/min/max = %d/%d/%d", h.Count(), h.Min(), h.Max())
	}
	if h.Quantile(0) != h.Min() || h.Quantile(1) != h.Max() {
		t.Error("boundary quantiles")
	}
	var empty Histogram
	if empty.Quantile(0.5) != 0 || empty.Mean() != 0 {
		t.Error("empty histogram")
	}
}

func TestMergeAndSet(t *testing.T) {
	s := NewSet(3)
	for i := range 3 {
		for v := int64(1); v <= 100; v++ {
			s.At(i).Record(v * int64(i+1))
		}
	}
	m := s.Merged()
	if m.Count() != 300 || m.Min() != 1 || m.Max() != 300 {
		t.Errorf("merged count/min/max = %d/%d/%d", m.Count(), m.Min(), m.Max())
	}
	var buf bytes.Buffer
	if err := m.Write(&buf); err != nil || !bytes.HasPrefix(buf.Bytes(), []byte("count=300")) {
		t.Errorf("Write = %q, %v", buf.String(), err)
	}
	ps := m.Percentiles(0.99, 0.5)
	if len(ps) != 2 || ps[0] > ps[1] {
		t.Errorf("percentiles = %v", ps)
	}
	m.Reset()
	if m.Count() != 0 {
		t.Error("Reset")
	}
	var h Histogram
	h.Record(-1)
	if h.Min() != 0 || h.Max() != 0 || h.Count() != 1 {
		t.Error("negative sample clamp")
	}
	if err := h.Dump(t.TempDir() + "/x.hdr"); err != nil {
		t.Error(err)
	}
}

// enabled reports whether latency recording was requested.
var enabled = flag.Bool("bench.latency", false, "record and report latency percentiles as p50-ns/p99-ns/p999-ns")

func BenchmarkRecord(b *testing.B) {
	var h Histogram
	v := int64(12345)
	for b.Loop() {
		h.Record(v)
		v = (v*7 + 3) & 0xffffff
	}
	if *enabled {
		h.Report(b)
	}
}
