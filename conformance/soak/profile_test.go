package soak

import (
	"math"
	"strings"
	"testing"
	"time"
)

// synth builds a series of n samples over d, with heap and goroutines growing
// at the given rate per hour from a baseline.
func synth(n int, d time.Duration, heapPerHour, objPerHour, grPerHour float64) Series {
	s := Series{Name: "synthetic", MinDuration: 10 * time.Minute}
	step := d / time.Duration(n-1)
	const baseHeap = 32 << 20
	const baseObjects = 200_000
	const baseGoroutines = 40
	for i := range n {
		elapsed := time.Duration(i) * step
		h := elapsed.Hours()
		s.Samples = append(s.Samples, Sample{
			Elapsed:     elapsed,
			HeapAlloc:   uint64(baseHeap + heapPerHour*h),
			HeapObjects: uint64(baseObjects + objPerHour*h),
			Goroutines:  baseGoroutines + int(grPerHour*h),
			Progress:    int64(i) * 100,
		})
	}
	return s
}

// TestAShortCleanRunIsInconclusiveAndNotAPass is the whole point of the
// package's three grades.
//
// A four-minute soak whose samples are perfectly flat must NOT report "no
// leak": a leak of a kilobyte a minute is invisible in four minutes, so the
// only honest verdict is that the run cannot support the conclusion. A soak
// suite whose short runs grade Pass is a suite that reports success for not
// having looked.
func TestAShortCleanRunIsInconclusiveAndNotAPass(t *testing.T) {
	d := synth(20, 4*time.Minute, 0, 0, 0).Analyse()
	if d.Grade != GradeInconclusive {
		t.Fatalf("a flat four-minute run graded %s, want inconclusive: %s", d.Grade, d)
	}
	if err := d.Err(); err != nil {
		t.Errorf("an inconclusive run returned an error: %v", err)
	}
	joined := strings.Join(d.Reasons, " ")
	for _, want := range []string{"4m0s", "10m0s", "has not tested a leak slower"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the reason %q does not name %q", joined, want)
		}
	}
	// And the same samples over a long enough run do pass, or the check
	// above is a check that always fails.
	long := synth(20, 30*time.Minute, 0, 0, 0).Analyse()
	if long.Grade != GradePass {
		t.Fatalf("a flat thirty-minute run graded %s, want pass: %s", long.Grade, long)
	}
}

// TestLeaksAreDetected is the negative control: each row injects one kind of
// growth and the analysis must fail on it, naming the resource.
func TestLeaksAreDetected(t *testing.T) {
	cases := []struct {
		name                          string
		heapPerHour, objs, goroutines float64
		want                          string
	}{
		{"a heap leak", 64 << 20, 0, 0, "live heap grew"},
		{"an object leak", 0, 200_000, 0, "live objects grew"},
		{"a goroutine leak", 0, 0, 60, "goroutines grew"},
		{"all three", 64 << 20, 200_000, 60, "goroutines grew"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := synth(20, 30*time.Minute, c.heapPerHour, c.objs, c.goroutines).Analyse()
			if d.Grade != GradeFail {
				t.Fatalf("graded %s, want fail: %s", d.Grade, d)
			}
			err := d.Err()
			if err == nil {
				t.Fatal("a failing grade returned no error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the failure does not name the resource: %v", err)
			}
			t.Logf("rejected: %v", err)
		})
	}
}

// TestALeakFoundInAShortRunStillFails is the other half of the three grades:
// a short run cannot be called clean, but a leak it DID find is a real
// finding, not something to be deferred to a longer run.
func TestALeakFoundInAShortRunStillFails(t *testing.T) {
	// Four minutes and not two: the slope is fitted to the tail, so a
	// two-minute run's fit spans 57 seconds and falls under MinFitSpan —
	// which is the guard doing its job, and is why
	// "short" here means short of the ten-minute CONCLUSION threshold
	// rather than short of the one-minute FIT threshold.
	d := synth(20, 4*time.Minute, 256<<20, 0, 600).Analyse()
	if d.Grade != GradeFail {
		t.Fatalf("a four-minute run with a large leak graded %s, want fail: %s", d.Grade, d)
	}
	if d.Err() == nil {
		t.Fatal("no error")
	}
	// And the caveat about the duration is still reported, so that a reader
	// knows the run was short even though it failed.
	if !strings.Contains(strings.Join(d.Reasons, " "), "a conclusion needs") {
		t.Errorf("a short failing run did not report that it was short: %s", d)
	}
	t.Logf("rejected: %v", d.Err())
}

// TestAVeryShortRunCannotFailOnNoise is the guard watched working. A per-hour slope fitted to a two-second window extrapolates
// by a factor of eighteen hundred, and the run that found this graded a
// perfectly healthy two-second soak FAIL on "+51946 objects/h" of ordinary
// allocation noise. Below MinFitSpan no slope may fail the run.
func TestAVeryShortRunCannotFailOnNoise(t *testing.T) {
	// Two seconds, and growth steep enough that the extrapolated slope is
	// enormous while the absolute increase over the window is trivial:
	// 200 KiB of heap and one goroutine.
	s := Series{Name: "noise", MinDuration: time.Second, MinSamples: 2}
	for i := range 6 {
		s.Samples = append(s.Samples, Sample{
			Elapsed:     time.Duration(i) * 400 * time.Millisecond,
			HeapAlloc:   uint64(32<<20 + i*40<<10),
			HeapObjects: uint64(200_000 + i*40),
			Goroutines:  40 + i/5,
			Progress:    int64(i) * 10,
		})
	}
	d := s.Analyse()
	if d.Grade == GradeFail {
		t.Fatalf("a two-second run failed on extrapolated noise: %s", d)
	}
	if d.Grade != GradeInconclusive {
		t.Fatalf("graded %s, want inconclusive: %s", d.Grade, d)
	}
	if !strings.Contains(strings.Join(d.Reasons, " "), "is noise") {
		t.Errorf("the reason does not say the window was too short to fit: %s", d)
	}
	// The slopes are still REPORTED, so a reader can see what was measured.
	if d.HeapPerHour <= 0 {
		t.Errorf("the slope was not reported: %s", d)
	}
	t.Logf("%s", d)
}

// TestAMaterialFloorIsNeededAsWellAsASlope is the other half of the same
// guard: a long, steady, TINY growth — a slope above the ceiling whose
// absolute increase over the window is a few kilobytes — is noise too.
func TestAMaterialFloorIsNeededAsWellAsASlope(t *testing.T) {
	// A slope of 9 MiB/h is over the 8 MiB/h ceiling, but the window is
	// only long enough for 300 KiB of it.
	s := Series{Name: "tiny", MinDuration: time.Minute, MinSamples: 4, MinFitSpan: 30 * time.Second}
	for i := range 20 {
		elapsed := time.Duration(i) * 8 * time.Second
		s.Samples = append(s.Samples, Sample{
			Elapsed:     elapsed,
			HeapAlloc:   uint64(32<<20) + uint64(9<<20*elapsed.Hours()),
			HeapObjects: 200_000,
			Goroutines:  40,
			Progress:    int64(i),
		})
	}
	d := s.Analyse()
	if d.HeapPerHour <= 8<<20 {
		t.Fatalf("the fixture's slope is %.0f B/h, under the ceiling, so it proves nothing", d.HeapPerHour)
	}
	if d.Grade == GradeFail {
		t.Fatalf("a slope over the ceiling with 300 KiB of actual growth failed the run: %s", d)
	}
	t.Logf("%s", d)
}

// TestWarmUpIsNotALeak covers why the slope is fitted to the tail. A program
// that allocates its buffers in the first minute and then holds them steady
// has a large first-to-last heap difference and no leak at all, and a
// first-versus-last comparison would fail it.
func TestWarmUpIsNotALeak(t *testing.T) {
	s := Series{Name: "warm-up", MinDuration: 10 * time.Minute}
	for i := range 40 {
		elapsed := time.Duration(i) * 45 * time.Second
		heap := uint64(8 << 20)
		if i >= 4 {
			heap = 96 << 20 // allocated once during warm-up and held
		}
		s.Samples = append(s.Samples, Sample{
			Elapsed: elapsed, HeapAlloc: heap, HeapObjects: 100_000,
			Goroutines: 50, Progress: int64(i) * 10,
		})
	}
	d := s.Analyse()
	if d.Grade != GradePass {
		t.Fatalf("a run that allocated during warm-up and then held steady graded %s: %s", d.Grade, d)
	}
	if d.LastHeap <= d.FirstHeap {
		t.Fatal("the fixture does not actually grow between its first and last sample, so it proves nothing")
	}
	if math.Abs(d.HeapPerHour) > 1<<20 {
		t.Errorf("the tail's slope is %.0f B/h over a flat tail", d.HeapPerHour)
	}
	t.Logf("accepted: %s", d)
}

// TestAStalledWorkloadIsInconclusive keeps a soak from passing because nothing
// happened. A workload that died in the first minute leaves a flat heap and a
// flat goroutine count, which is indistinguishable from a clean run unless the
// run's own progress counter is read.
func TestAStalledWorkloadIsInconclusive(t *testing.T) {
	s := synth(20, 30*time.Minute, 0, 0, 0)
	for i := range s.Samples {
		s.Samples[i].Progress = 7 // never advances
	}
	d := s.Analyse()
	if d.Grade != GradeInconclusive {
		t.Fatalf("a soak whose workload never advanced graded %s: %s", d.Grade, d)
	}
	if !strings.Contains(strings.Join(d.Reasons, " "), "the workload did not") {
		t.Errorf("the reason does not say the workload stalled: %s", d)
	}
}

// TestTooFewSamplesIsInconclusive covers the third way a run cannot conclude.
func TestTooFewSamplesIsInconclusive(t *testing.T) {
	d := synth(3, 30*time.Minute, 0, 0, 0).Analyse()
	if d.Grade != GradeInconclusive {
		t.Fatalf("a three-sample run graded %s: %s", d.Grade, d)
	}
	empty := Series{Name: "empty"}.Analyse()
	if empty.Grade != GradeInconclusive {
		t.Errorf("a run with no samples graded %s", empty.Grade)
	}
	if !strings.Contains(strings.Join(empty.Reasons, " "), "no samples") {
		t.Errorf("an empty run does not say so: %s", empty)
	}
	// The zero Diff must read as inconclusive, so that a Diff nobody filled
	// in cannot be mistaken for a pass.
	if (Diff{}).Grade != GradeInconclusive {
		t.Error("the zero Diff does not grade inconclusive")
	}
}

// TestDiffStringCarriesTheNumbers holds the report line to the run: the
// duration is in it, because "no leak" without a duration is not a statement.
func TestDiffStringCarriesTheNumbers(t *testing.T) {
	got := synth(20, 30*time.Minute, 0, 0, 0).Analyse().String()
	for _, want := range []string{"pass", "30m0s", "samples", "heap", "goroutines", "objects", "progress="} {
		if !strings.Contains(got, want) {
			t.Errorf("Diff.String() = %q, missing %q", got, want)
		}
	}
}

// TestSlopeIsFiniteOnDegenerateInput keeps a NaN from grading a run. Every
// sample at the same instant makes the least-squares denominator zero, and a
// NaN compares false against every threshold — so such a run would have passed
// silently.
func TestSlopeIsFiniteOnDegenerateInput(t *testing.T) {
	s := Series{Name: "degenerate", MinDuration: time.Second}
	for range 20 {
		s.Samples = append(s.Samples, Sample{Elapsed: time.Hour, HeapAlloc: 1 << 20, Goroutines: 3, Progress: 1})
	}
	s.Samples[len(s.Samples)-1].Progress = 99
	d := s.Analyse()
	for _, v := range []float64{d.HeapPerHour, d.ObjectsPerHour, d.GoroutinesPerHour} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("a degenerate series produced a non-finite slope: %s", d)
		}
	}
	if d.Grade == GradeFail {
		t.Errorf("a degenerate series failed: %s", d)
	}
}

// TestParseDurationRefusesNonsense covers the value `make soak TIME=` passes
// through. A soak told "10minutes" must not quietly run for the default.
func TestParseDurationRefusesNonsense(t *testing.T) {
	if _, err := ParseDuration("10minutes", "1h"); err == nil {
		t.Error("an unparseable duration was accepted")
	}
	if _, err := ParseDuration("0s", "1h"); err == nil {
		t.Error("a zero duration was accepted")
	}
	if _, err := ParseDuration("-5m", "1h"); err == nil {
		t.Error("a negative duration was accepted")
	}
	got, err := ParseDuration("", "12m")
	if err != nil || got != 12*time.Minute {
		t.Errorf("ParseDuration(\"\", \"12m\") = %v, %v", got, err)
	}
	if got, err := ParseDuration("90s", "1h"); err != nil || got != 90*time.Second {
		t.Errorf("ParseDuration(\"90s\") = %v, %v", got, err)
	}
}

// TestSampleEveryGivesEnoughSamples holds the sampling interval to what the
// slope needs: a run that sampled four times cannot be graded, and the
// interval is the only thing that decides it.
func TestSampleEveryGivesEnoughSamples(t *testing.T) {
	for _, d := range []time.Duration{time.Minute, 10 * time.Minute, time.Hour, 24 * time.Hour} {
		every := SampleEvery(d, 8)
		if every <= 0 {
			t.Fatalf("SampleEvery(%v) = %v", d, every)
		}
		if n := int(d / every); n < 8 {
			t.Errorf("a %v run at one sample every %v takes %d samples, under the 8 a slope needs", d, every, n)
		}
	}
	// And a 24-hour run must not take a hundred thousand samples: each one
	// calls runtime.GC.
	if n := int(24 * time.Hour / SampleEvery(24*time.Hour, 8)); n > 2000 {
		t.Errorf("a 24-hour run would take %d samples, each with its own runtime.GC", n)
	}
}

// TestRecorderCollectsRealSamples exercises Take against the live runtime, so
// that a change to MemStats' field names is a build failure here rather than a
// soak that reports zeros.
func TestRecorderCollectsRealSamples(t *testing.T) {
	start := time.Now()
	r := NewRecorder("live", start, Series{MinDuration: time.Millisecond, MinSamples: 3})
	held := make([][]byte, 0, 4)
	for i := range 4 {
		held = append(held, make([]byte, 1<<16))
		r.Sample(start.Add(time.Duration(i+1)*time.Minute), int64(i+1))
	}
	s := r.Series()
	if len(s.Samples) != 4 {
		t.Fatalf("recorded %d sample(s)", len(s.Samples))
	}
	for i, x := range s.Samples {
		if x.HeapAlloc == 0 || x.Goroutines == 0 {
			t.Errorf("sample %d read zeros from the runtime: %+v", i, x)
		}
	}
	d := r.Analyse()
	if d.Samples != 4 || d.Progress != 3 {
		t.Errorf("Analyse() = %s", d)
	}
	_ = held
}
