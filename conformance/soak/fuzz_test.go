package soak

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzAnalyse holds the profile analysis to the properties that make its
// verdict readable, over series a reader would not think to write.
//
// The interesting one is the PASS precondition. A soak's whole value is that a
// clean verdict cannot be reached from a run too short to support it, and that
// is a property of the analysis rather than of any one series: a `pass` must
// imply the run was long enough, took enough samples, fitted a long enough
// window and made progress. Fuzzing it is how a future change to the grading —
// an extra branch, a reordered switch — is caught rather than argued about.
func FuzzAnalyse(f *testing.F) {
	f.Add(WriteSeries(synthSeed(20, 30*time.Minute, 0)))
	f.Add(WriteSeries(synthSeed(20, 30*time.Minute, 64<<20)))
	f.Add(WriteSeries(synthSeed(4, 3*time.Second, 0)))
	f.Add([]byte{0})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		s := DecodeSeries(data)
		d := s.Analyse()

		for name, v := range map[string]float64{
			"heap":       d.HeapPerHour,
			"objects":    d.ObjectsPerHour,
			"goroutines": d.GoroutinesPerHour,
		} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				t.Fatalf("the %s slope is %v over %d sample(s): a non-finite slope compares false against every ceiling, so the run would pass silently", name, v, len(s.Samples))
			}
		}
		if len(d.Reasons) == 0 {
			t.Fatalf("grade %s with no reason given: %s", d.Grade, d)
		}
		if d.Grade > GradeFail {
			t.Fatalf("grade %d is not one of %v", d.Grade, Grades())
		}

		full := s.Defaults()
		if d.Grade == GradePass {
			switch {
			case d.Duration < full.MinDuration:
				t.Fatalf("a %v run graded pass, under the %v a conclusion needs: %s", d.Duration, full.MinDuration, d)
			case len(s.Samples) < full.MinSamples:
				t.Fatalf("a %d-sample run graded pass, under the %d a slope needs: %s", len(s.Samples), full.MinSamples, d)
			case d.FitSpan < full.MinFitSpan:
				t.Fatalf("a run whose fit spans %v graded pass, under the %v a slope needs: %s", d.FitSpan, full.MinFitSpan, d)
			case d.Progress <= 0:
				t.Fatalf("a run whose workload made no progress graded pass: %s", d)
			}
		}
		if d.Grade == GradeFail {
			// Every failure names the resource, so that a red weekly job
			// says what grew rather than only that something did.
			joined := strings.Join(d.Reasons, " ")
			if !strings.Contains(joined, "grew at") {
				t.Fatalf("a failing grade names no resource: %s", d)
			}
		}
		// Err and Grade must not disagree: a caller that tests only one of
		// them would otherwise read a different verdict from the other.
		if (d.Err() != nil) != (d.Grade == GradeFail) {
			t.Fatalf("Err() = %v with grade %s: %s", d.Err(), d.Grade, d)
		}
	})
}

// FuzzParseDuration holds the TIME= parser to its contract: any input either
// errors or yields a positive duration, and never panics.
//
// `make soak TIME=` passes a string straight through, so this is the one place
// a stranger's bytes reach this package in production use.
func FuzzParseDuration(f *testing.F) {
	f.Add("4h", "1h")
	f.Add("10minutes", "1h")
	f.Add("", "12m")
	f.Add("-5m", "1h")
	f.Add("0", "")
	f.Fuzz(func(t *testing.T, v, def string) {
		d, err := ParseDuration(v, def)
		switch {
		case err != nil:
			if d != 0 {
				t.Fatalf("ParseDuration(%q, %q) = %v with an error", v, def, d)
			}
		case d <= 0:
			t.Fatalf("ParseDuration(%q, %q) = %v with no error", v, def, d)
		}
	})
}

// DecodeSeries builds a series from fuzz bytes. It is exported so that the
// fuzz target above and a future campaign share one decoder, and so an
// interesting series can be written back as a seed.
func DecodeSeries(data []byte) Series {
	r := fuzzutil.NewReader(data)
	s := Series{Name: "fuzz"}
	// The thresholds stay at their defaults: what is being fuzzed is the
	// SERIES, and a fuzzer that could also move the ceilings would spend
	// its inputs discovering that a high enough ceiling passes anything.
	n := r.Int(0, 40)
	var elapsed time.Duration
	heap, objects, goroutines, progress := int64(32<<20), int64(200_000), 40, int64(0)
	for range n {
		elapsed += r.Duration(20 * time.Minute)
		heap += int64(r.Int(-1<<20, 4<<20))
		objects += int64(r.Int(-5_000, 20_000))
		goroutines += r.Int(-2, 4)
		progress += int64(r.Int(0, 100))
		if heap < 0 {
			heap = 0
		}
		if objects < 0 {
			objects = 0
		}
		if goroutines < 0 {
			goroutines = 0
		}
		s.Samples = append(s.Samples, Sample{
			Elapsed: elapsed, HeapAlloc: uint64(heap), HeapObjects: uint64(objects),
			Goroutines: goroutines, Progress: progress,
		})
	}
	return s
}

// WriteSeries is not the inverse of DecodeSeries — a series is built from
// deltas the reader consumed — so it writes the bytes that would PRODUCE a
// series of the same shape: the sample count and then one delta per sample.
// It exists so a seed can be committed with a name rather than as a byte
// blob.
func WriteSeries(s Series) []byte {
	w := fuzzutil.NewWriter()
	w.Int(0, 40, len(s.Samples))
	prevHeap, prevObjects, prevGoroutines := int64(32<<20), int64(200_000), 40
	var prevElapsed time.Duration
	for _, x := range s.Samples {
		w.Duration(20*time.Minute, x.Elapsed-prevElapsed)
		w.Int(-1<<20, 4<<20, clamp(int64(x.HeapAlloc)-prevHeap, -1<<20, 4<<20))
		w.Int(-5_000, 20_000, clamp(int64(x.HeapObjects)-prevObjects, -5_000, 20_000))
		w.Int(-2, 4, clamp(int64(x.Goroutines-prevGoroutines), -2, 4))
		w.Int(0, 100, 10)
		prevElapsed = x.Elapsed
		prevHeap, prevObjects, prevGoroutines = int64(x.HeapAlloc), int64(x.HeapObjects), x.Goroutines
	}
	return w.Data()
}

func clamp(v, lo, hi int64) int {
	switch {
	case v < lo:
		return int(lo)
	case v > hi:
		return int(hi)
	}
	return int(v)
}

// synthSeed is a series with a known growth rate, for a committed seed.
func synthSeed(n int, d time.Duration, heapPerHour float64) Series {
	s := Series{Name: "seed"}
	step := d / time.Duration(n)
	for i := range n {
		elapsed := time.Duration(i+1) * step
		s.Samples = append(s.Samples, Sample{
			Elapsed:     elapsed,
			HeapAlloc:   uint64(32<<20 + heapPerHour*elapsed.Hours()),
			HeapObjects: 200_000,
			Goroutines:  40,
			Progress:    int64(i) * 10,
		})
	}
	return s
}
