package fuzzutil

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"
)

func TestReaderWriterRoundTrip(t *testing.T) {
	w := NewWriter()
	w.Byte(7)
	w.Bool(true)
	w.Bool(false)
	w.Uint32(0xdeadbeef)
	w.Uint64(1 << 40)
	w.Int(0, 10, 4)
	w.Int(-5, 5, -5)
	w.Int(0, 300, 299)
	w.Int(0, 1<<20, 1<<19)
	w.Int(0, 1<<40, 1<<35)
	w.Choice(3, 2)
	w.Duration(time.Hour, 90*time.Second)
	w.Bytes(16, []byte("hello"))
	w.String(8, "hi")
	w.Bytes(4, []byte{})

	r := NewReader(w.Data())
	if v := r.Byte(); v != 7 {
		t.Errorf("Byte = %d", v)
	}
	if !r.Bool() || r.Bool() {
		t.Error("Bool mismatch")
	}
	if v := r.Uint32(); v != 0xdeadbeef {
		t.Errorf("Uint32 = %x", v)
	}
	if v := r.Uint64(); v != 1<<40 {
		t.Errorf("Uint64 = %d", v)
	}
	if v := r.Int(0, 10); v != 4 {
		t.Errorf("Int(0,10) = %d", v)
	}
	if v := r.Int(-5, 5); v != -5 {
		t.Errorf("Int(-5,5) = %d", v)
	}
	if v := r.Int(0, 300); v != 299 {
		t.Errorf("Int(0,300) = %d", v)
	}
	if v := r.Int(0, 1<<20); v != 1<<19 {
		t.Errorf("Int(0,1<<20) = %d", v)
	}
	if v := r.Int(0, 1<<40); v != 1<<35 {
		t.Errorf("Int(0,1<<40) = %d", v)
	}
	if v := r.Choice(3); v != 2 {
		t.Errorf("Choice = %d", v)
	}
	if v := r.Duration(time.Hour); v != 90*time.Second {
		t.Errorf("Duration = %v", v)
	}
	if v := r.Bytes(16); string(v) != "hello" {
		t.Errorf("Bytes = %q", v)
	}
	if v := r.String(8); v != "hi" {
		t.Errorf("String = %q", v)
	}
	if v := r.Bytes(4); v == nil || len(v) != 0 {
		t.Errorf("empty Bytes = %v", v)
	}
	if !r.Exhausted() {
		t.Error("reader not exhausted after consuming everything")
	}
}

func TestReaderExhaustedReturnsZero(t *testing.T) {
	r := NewReader([]byte{1})
	_ = r.Byte()
	if r.Byte() != 0 || r.Bool() || r.Uint32() != 0 || r.Uint64() != 0 || r.Int(3, 9) != 3 || r.Choice(5) != 0 || r.Duration(time.Second) != 0 || len(r.Bytes(9)) != 0 || r.String(9) != "" {
		t.Error("exhausted reader returned non-zero values")
	}
	if !r.Exhausted() || r.Remaining() != 0 {
		t.Error("exhaustion not reported")
	}
	// Partial reads pad with zeros and mark exhaustion.
	r = NewReader([]byte{0xff, 0xff})
	if v := r.Uint32(); v != 0xffff {
		t.Errorf("padded Uint32 = %x", v)
	}
	if !r.Exhausted() {
		t.Error("partial read did not mark exhaustion")
	}
	// Truncated Bytes returns what remains.
	r = NewReader([]byte{5, 'a', 'b'})
	if v := r.Bytes(10); string(v) != "ab" || !r.Exhausted() {
		t.Errorf("truncated Bytes = %q exhausted=%v", v, r.Exhausted())
	}
}

func TestReaderEdgeRanges(t *testing.T) {
	r := NewReader(bytes.Repeat([]byte{0xff}, 32))
	if v := r.Int(5, 5); v != 5 {
		t.Errorf("Int(5,5) = %d", v)
	}
	if v := r.Int(9, 2); v != 9 || r.pos != 1 {
		t.Errorf("Int(hi<lo) = %d pos=%d", v, r.pos)
	}
	if v := r.Choice(0); v != 0 {
		t.Errorf("Choice(0) = %d", v)
	}
	if v := r.Choice(1); v != 0 {
		t.Errorf("Choice(1) = %d", v)
	}
	if v := r.Duration(0); v != 0 {
		t.Errorf("Duration(0) = %v", v)
	}
	if v := r.Bytes(-1); len(v) != 0 {
		t.Errorf("Bytes(-1) = %v", v)
	}
	for range 100 {
		if v := r.Int(-3, 3); v < -3 || v > 3 {
			t.Fatalf("Int out of range: %d", v)
		}
	}
}

type counter struct {
	n    int
	sum  int
	last string
}

func TestScriptRunAndWriteStep(t *testing.T) {
	sc := &Script[*counter]{
		Steps: []Step[*counter]{
			{Name: "inc", Weight: 3, Run: func(t testing.TB, r *Reader, c *counter) { c.n++; c.last = "inc" }},
			{Name: "add", Run: func(t testing.TB, r *Reader, c *counter) { c.sum += r.Int(1, 10); c.last = "add" }},
		},
		Check: func(t testing.TB, c *counter) {
			if c.sum < 0 {
				t.Fatal("negative sum")
			}
		},
	}
	w := NewWriter()
	sc.WriteStep(w, "add")
	w.Int(1, 10, 7)
	sc.WriteStep(w, "inc")
	sc.WriteStep(w, "add")
	w.Int(1, 10, 2)
	c := &counter{}
	if n := sc.Run(t, w.Data(), c); n != 3 {
		t.Errorf("Run executed %d steps, want 3", n)
	}
	if c.n != 1 || c.sum != 9 || c.last != "add" {
		t.Errorf("counter = %+v", *c)
	}
	// MaxSteps bounds execution.
	sc.MaxSteps = 1
	c = &counter{}
	if n := sc.Run(t, w.Data(), c); n != 1 {
		t.Errorf("Run with MaxSteps=1 executed %d", n)
	}
	// Empty input executes nothing.
	if n := sc.Run(t, nil, &counter{}); n != 0 {
		t.Errorf("Run on empty input executed %d", n)
	}
	defer func() {
		if recover() == nil {
			t.Error("WriteStep with unknown name did not panic")
		}
	}()
	sc.WriteStep(NewWriter(), "nope")
}

func TestScriptWeightedSelectionCoversAllSteps(t *testing.T) {
	seen := map[string]int{}
	sc := &Script[map[string]int]{
		Steps: []Step[map[string]int]{
			{Name: "a", Weight: 1, Run: func(t testing.TB, r *Reader, m map[string]int) { m["a"]++ }},
			{Name: "b", Weight: 10, Run: func(t testing.TB, r *Reader, m map[string]int) { m["b"]++ }},
			{Name: "c", Run: func(t testing.TB, r *Reader, m map[string]int) { m["c"]++ }},
		},
		MaxSteps: 1 << 20,
	}
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	sc.Run(t, data, seen)
	if seen["a"] == 0 || seen["b"] == 0 || seen["c"] == 0 {
		t.Errorf("weighted selection missed a step: %v", seen)
	}
	if seen["b"] < seen["a"] || seen["b"] < seen["c"] {
		t.Errorf("heavier step not selected more often: %v", seen)
	}
}

func TestActors(t *testing.T) {
	var total atomic.Int64
	data := bytes.Repeat([]byte{1}, 100)
	Actors(t, data, 4, func(id int, r *Reader) {
		for !r.Exhausted() {
			MaybeYield(r)
			total.Add(1)
		}
	})
	if total.Load() != 100 {
		t.Errorf("actors consumed %d bytes, want 100", total.Load())
	}
	Actors(t, nil, 0, func(int, *Reader) { t.Error("fn called for n=0") })
	Actors(t, []byte{1}, 8, func(id int, r *Reader) { r.Byte() })
}

func FuzzReaderNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add(bytes.Repeat([]byte{0xff}, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(data)
		ctl := NewReader(data)
		for i := 0; i < 64 && !ctl.Exhausted(); i++ {
			switch ctl.Choice(9) {
			case 0:
				r.Byte()
			case 1:
				r.Bool()
			case 2:
				r.Uint32()
			case 3:
				r.Uint64()
			case 4:
				lo, hi := ctl.Int(-100, 100), ctl.Int(-100, 100)
				if v := r.Int(lo, hi); hi >= lo && (v < lo || v > hi) {
					t.Fatalf("Int(%d,%d) = %d", lo, hi, v)
				}
			case 5:
				n := ctl.Int(0, 5)
				if v := r.Choice(n); n > 1 && (v < 0 || v >= n) {
					t.Fatalf("Choice(%d) = %d", n, v)
				}
			case 6:
				max := time.Duration(ctl.Int(-1, 1000))
				if v := r.Duration(max); v < 0 || v > max && max > 0 {
					t.Fatalf("Duration(%v) = %v", max, v)
				}
			case 7:
				max := ctl.Int(-1, 40)
				if v := r.Bytes(max); max >= 0 && len(v) > max {
					t.Fatalf("Bytes(%d) len %d", max, len(v))
				}
			case 8:
				r.String(ctl.Int(0, 40))
			}
		}
		if r.Remaining() < 0 {
			t.Fatal("negative remaining")
		}
	})
}
