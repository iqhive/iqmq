package fuzzutil

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// Step is one operation of an op script. Run decodes the operation's
// arguments from r, applies it to the system under test and the reference
// model held in s, and reports any divergence through t.
type Step[S any] struct {
	// Name identifies the step in traces and seeds.
	Name string
	// Weight is the relative probability of the step; zero means one.
	Weight int
	// Run executes the step.
	Run func(t testing.TB, r *Reader, s S)
}

// Script interprets fuzz input as a sequence of Steps.
type Script[S any] struct {
	// Steps are the available operations.
	Steps []Step[S]
	// MaxSteps bounds the number of steps executed; zero means 256.
	MaxSteps int
	// Check, when non-nil, runs after every step to assert invariants.
	Check func(t testing.TB, s S)
}

func (sc *Script[S]) weights() (total int, cum []int) {
	cum = make([]int, len(sc.Steps))
	for i, st := range sc.Steps {
		w := st.Weight
		if w <= 0 {
			w = 1
		}
		total += w
		cum[i] = total
	}
	return total, cum
}

// Run decodes and executes steps from data against s until the input is
// exhausted, MaxSteps is reached, or the test fails. It returns the number of
// steps executed. When the test fails, the decoded trace is logged so the
// failing script is readable.
func (sc *Script[S]) Run(t testing.TB, data []byte, s S) int {
	t.Helper()
	if len(sc.Steps) == 0 {
		t.Fatal("fuzzutil: script has no steps")
	}
	max := sc.MaxSteps
	if max <= 0 {
		max = 256
	}
	total, cum := sc.weights()
	r := NewReader(data)
	trace := make([]string, 0, 32)
	defer func() {
		if t.Failed() {
			t.Logf("op-script trace (%d steps):\n  %s", len(trace), strings.Join(trace, "\n  "))
		}
	}()
	n := 0
	for n < max && !r.Exhausted() {
		pick := r.Int(0, total-1)
		idx := 0
		for idx < len(cum)-1 && pick >= cum[idx] {
			idx++
		}
		st := sc.Steps[idx]
		before := r.pos
		trace = append(trace, st.Name)
		st.Run(t, r, s)
		n++
		trace[len(trace)-1] = fmt.Sprintf("%3d %s [%d bytes]", n, st.Name, r.pos-before)
		if t.Failed() {
			return n
		}
		if sc.Check != nil {
			sc.Check(t, s)
			if t.Failed() {
				return n
			}
		}
	}
	return n
}

// WriteStep encodes the choice of the named step into w, so that a seed
// authored with Writer selects that step on decode. Unknown names panic,
// which surfaces typos in seed authoring at test time.
func (sc *Script[S]) WriteStep(w *Writer, name string) {
	total, cum := sc.weights()
	for i, st := range sc.Steps {
		if st.Name == name {
			start := 0
			if i > 0 {
				start = cum[i-1]
			}
			w.Int(0, total-1, start)
			return
		}
	}
	panic("fuzzutil: unknown step " + name)
}

// Actors splits data into n readers and runs fn for each in its own
// goroutine, returning after all finish. It is the building block for
// concurrent op scripts.
func Actors(t testing.TB, data []byte, n int, fn func(id int, r *Reader)) {
	t.Helper()
	if n <= 0 {
		return
	}
	var wg sync.WaitGroup
	chunk := (len(data) + n - 1) / n
	for i := range n {
		lo := i * chunk
		hi := lo + chunk
		if lo > len(data) {
			lo = len(data)
		}
		if hi > len(data) {
			hi = len(data)
		}
		r := NewReader(data[lo:hi])
		wg.Go(func() { fn(i, r) })
	}
	wg.Wait()
}

// MaybeYield consumes one byte and yields the processor when its low bit is
// set, injecting scheduling variety into concurrent scripts.
func MaybeYield(r *Reader) {
	if r.Bool() {
		runtime.Gosched()
	}
}
