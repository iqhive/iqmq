// Package soak runs long conformance workloads and decides what a run of a
// given length can conclude.
//
// # What a soak can and cannot conclude
//
// The one thing this package refuses to do is call a short run clean. The specification
// asks for a 24-hour soak; `make soak TIME=10m` is a ten-minute run, and the
// honest verdict of a ten-minute run is NOT "no leak" — it is "no leak
// detectable in ten minutes", which for a leak of a kilobyte a minute is the
// same sentence as "not tested". So [Diff] has three grades and not two, every
// grade carries the duration it was reached in, and a run shorter than
// [Series.MinDuration] cannot grade Pass however flat its samples are.
//
// The leak criterion is a least-squares slope over the TAIL of the series
// rather than a comparison of the first and last samples. A process that
// allocates its buffers in the first thirty seconds and then holds them steady
// has a large first-to-last difference and no leak; one that grows by a
// kilobyte a minute for six hours has a small difference between any two
// adjacent samples and a fatal one. The slope over the tail separates them,
// and dropping the head is what keeps warm-up out of the fit.
package soak

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Sample is one observation of the process under soak.
type Sample struct {
	// Elapsed is how long the soak had been running.
	Elapsed time.Duration
	// HeapAlloc is runtime.MemStats.HeapAlloc: bytes of live heap.
	HeapAlloc uint64
	// HeapObjects is the live object count, which catches a leak of many
	// small objects that HeapAlloc's noise hides.
	HeapObjects uint64
	// Goroutines is runtime.NumGoroutine.
	Goroutines int
	// Progress is the workload's own counter — messages settled — so that
	// a soak whose workload stalled is distinguishable from one that ran.
	Progress int64
}

// Take reads one sample. It calls runtime.GC first, deliberately: HeapAlloc
// without a collection measures garbage as well as live data, and a soak
// sampled that way has a slope dominated by where in the GC cycle each sample
// fell.
func Take(elapsed time.Duration, progress int64) Sample {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Sample{
		Elapsed:     elapsed,
		HeapAlloc:   ms.HeapAlloc,
		HeapObjects: ms.HeapObjects,
		Goroutines:  runtime.NumGoroutine(),
		Progress:    progress,
	}
}

// Series is one soak's samples and the thresholds it is judged against.
type Series struct {
	// Name identifies the run.
	Name string
	// Samples are in time order.
	Samples []Sample
	// MinDuration is the shortest run that may grade Pass. Below it the
	// verdict is Inconclusive whatever the samples show.
	MinDuration time.Duration
	// MinSamples is the fewest samples a slope may be fitted to.
	MinSamples int
	// HeapGrowthPerHour is the live-heap slope, in bytes per hour, above
	// which the run fails.
	HeapGrowthPerHour float64
	// ObjectGrowthPerHour is the same for the live object count.
	ObjectGrowthPerHour float64
	// GoroutineGrowthPerHour is the goroutine slope above which the run
	// fails. It is the tightest of the three because a goroutine leak is
	// never noise: a correct program's goroutine count is bounded.
	GoroutineGrowthPerHour float64
	// TailFraction is how much of the series the slope is fitted to,
	// counted from the end. 0.5 drops the first half as warm-up.
	TailFraction float64
	// MinFitSpan is the shortest window a SLOPE may be judged over.
	//
	// It exists because a per-hour slope fitted to a two-second window is
	// an extrapolation by a factor of eighteen hundred, and the run that
	// found this graded FAIL on "+51946 objects/h" measured over two
	// seconds of ordinary allocation noise. A leak verdict from a window
	// that short is a false positive, not a finding.
	MinFitSpan time.Duration
	// HeapMaterial, ObjectMaterial and GoroutineMaterial are the absolute
	// increases, over the fitted window, a failure also needs.
	//
	// Slope AND magnitude, not slope alone: the slope says the growth is
	// sustained and the magnitude says it is real. A short window can
	// produce a large slope from nothing; it cannot produce four megabytes
	// of live heap from nothing.
	HeapMaterial      uint64
	ObjectMaterial    uint64
	GoroutineMaterial int
}

// Defaults fills in the thresholds a run that names none is judged by.
//
// The three numbers are deliberately loose enough that a correct program
// passes on a noisy shared host and tight enough that a real leak fails inside
// the minimum duration: 8 MiB of live heap per hour is about 2 KiB a second,
// 20,000 live objects per hour is about six a second, and four goroutines per
// hour is one every fifteen minutes. A run that wants different numbers sets
// them and says why.
func (s Series) Defaults() Series {
	if s.MinDuration <= 0 {
		s.MinDuration = 10 * time.Minute
	}
	if s.MinSamples <= 0 {
		s.MinSamples = 8
	}
	if s.HeapGrowthPerHour <= 0 {
		s.HeapGrowthPerHour = 8 << 20
	}
	if s.ObjectGrowthPerHour <= 0 {
		s.ObjectGrowthPerHour = 20_000
	}
	if s.GoroutineGrowthPerHour <= 0 {
		s.GoroutineGrowthPerHour = 4
	}
	if s.TailFraction <= 0 || s.TailFraction > 1 {
		s.TailFraction = 0.5
	}
	if s.MinFitSpan <= 0 {
		s.MinFitSpan = time.Minute
	}
	if s.HeapMaterial == 0 {
		s.HeapMaterial = 4 << 20
	}
	if s.ObjectMaterial == 0 {
		s.ObjectMaterial = 10_000
	}
	if s.GoroutineMaterial == 0 {
		s.GoroutineMaterial = 2
	}
	return s
}

// Duration is how long the soak ran, from the last sample.
func (s Series) Duration() time.Duration {
	if len(s.Samples) == 0 {
		return 0
	}
	return s.Samples[len(s.Samples)-1].Elapsed
}

// Grade is a soak's verdict.
type Grade uint8

// Grade values.
const (
	// GradeInconclusive means the run cannot support a conclusion: too
	// short, too few samples, or a workload that made no progress. It is
	// the ZERO value, so a Diff nobody filled in reads as inconclusive
	// rather than as a pass.
	GradeInconclusive Grade = iota
	// GradePass means no growth above the thresholds over a run long
	// enough to say so.
	GradePass
	// GradeFail means growth above a threshold.
	GradeFail
)

// String implements fmt.Stringer.
func (g Grade) String() string {
	switch g {
	case GradeInconclusive:
		return "inconclusive"
	case GradePass:
		return "pass"
	case GradeFail:
		return "fail"
	}
	return "invalid"
}

// Diff is the profile diff: the slopes and the verdict.
type Diff struct {
	// Name and Duration identify the run. Duration is printed beside every
	// conclusion, because "no leak" means nothing without it.
	Name     string
	Duration time.Duration
	// Samples is how many samples the verdict was reached from, and Fitted
	// how many of them the slopes were fitted to.
	Samples, Fitted int
	// FitSpan is the wall time the fitted window covered. A slope over a
	// span shorter than Series.MinFitSpan is reported and never judged.
	FitSpan time.Duration
	// Grade is the verdict.
	Grade Grade
	// HeapPerHour, ObjectsPerHour and GoroutinesPerHour are the fitted
	// slopes over the tail.
	HeapPerHour       float64
	ObjectsPerHour    float64
	GoroutinesPerHour float64
	// FirstHeap and LastHeap bracket the run, for a reader who wants the
	// raw numbers rather than the fit.
	FirstHeap, LastHeap uint64
	// FirstGoroutines and LastGoroutines do the same.
	FirstGoroutines, LastGoroutines int
	// Progress is how much the workload's counter advanced.
	Progress int64
	// Reasons says why the grade is what it is, one line each.
	Reasons []string
}

// Err returns an error for a run that failed, and nil for a pass.
//
// An INCONCLUSIVE run returns nil: it is not a failure of the code under test,
// and making `make soak TIME=1m` red would only teach people to pass a longer
// TIME they do not have. What it must not do is read as a pass, which is what
// [Diff.String] and the grade are for — and the tagged runner logs the grade
// at the top of its output.
func (d Diff) Err() error {
	if d.Grade != GradeFail {
		return nil
	}
	return fmt.Errorf("soak %s over %v: %s", d.Name, d.Duration.Round(time.Second), strings.Join(d.Reasons, "; "))
}

// String is the line a soak run prints. Every number a reader needs to
// disagree with the verdict is in it.
func (d Diff) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s over %v (%d samples, %d fitted over %v, progress=%d)",
		d.Name, d.Grade, d.Duration.Round(time.Second), d.Samples, d.Fitted,
		d.FitSpan.Round(time.Second), d.Progress)
	fmt.Fprintf(&b, "; heap %s -> %s (%+.1f KiB/h)",
		bytes(d.FirstHeap), bytes(d.LastHeap), d.HeapPerHour/1024)
	fmt.Fprintf(&b, "; goroutines %d -> %d (%+.2f/h)",
		d.FirstGoroutines, d.LastGoroutines, d.GoroutinesPerHour)
	fmt.Fprintf(&b, "; objects %+.0f/h", d.ObjectsPerHour)
	for _, r := range d.Reasons {
		fmt.Fprintf(&b, "\n    - %s", r)
	}
	return b.String()
}

// bytes renders a byte count.
func bytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// Analyse grades the series.
func (s Series) Analyse() Diff {
	s = s.Defaults()
	d := Diff{Name: s.Name, Samples: len(s.Samples), Duration: s.Duration()}
	if len(s.Samples) == 0 {
		d.Reasons = append(d.Reasons, "the run recorded no samples at all")
		return d
	}
	first, last := s.Samples[0], s.Samples[len(s.Samples)-1]
	d.FirstHeap, d.LastHeap = first.HeapAlloc, last.HeapAlloc
	d.FirstGoroutines, d.LastGoroutines = first.Goroutines, last.Goroutines
	d.Progress = last.Progress - first.Progress

	tail := s.tail()
	d.Fitted = len(tail)
	if len(tail) >= 2 {
		d.FitSpan = tail[len(tail)-1].Elapsed - tail[0].Elapsed
		d.HeapPerHour = slopePerHour(tail, func(x Sample) float64 { return float64(x.HeapAlloc) })
		d.ObjectsPerHour = slopePerHour(tail, func(x Sample) float64 { return float64(x.HeapObjects) })
		d.GoroutinesPerHour = slopePerHour(tail, func(x Sample) float64 { return float64(x.Goroutines) })
	}

	// The three reasons a run cannot conclude. They are checked before the
	// slopes are judged, so a short run never grades Pass and never grades
	// Fail on a fit of three samples either.
	var inconclusive []string
	if d.Duration < s.MinDuration {
		inconclusive = append(inconclusive, fmt.Sprintf(
			"the run lasted %v and a conclusion needs %v: a soak that reports no leak after %v has not tested a leak slower than it",
			d.Duration.Round(time.Second), s.MinDuration, d.Duration.Round(time.Second),
		))
	}
	if len(s.Samples) < s.MinSamples {
		inconclusive = append(inconclusive, fmt.Sprintf(
			"the run took %d sample(s) and a slope needs %d", len(s.Samples), s.MinSamples,
		))
	}
	if d.Progress <= 0 {
		inconclusive = append(inconclusive, fmt.Sprintf(
			"the workload's counter advanced by %d: the soak ran and the workload did not", d.Progress,
		))
	}

	// A leak is judged whatever the run's total duration — a leak found in
	// four minutes is a real finding, and it is the CLEAN verdict that a
	// short run cannot support. But the FITTED WINDOW must be long enough
	// for a per-hour slope to mean anything, and the growth must be
	// material as well as sustained.
	var failures []string
	switch {
	case len(tail) < 2:
		inconclusive = append(inconclusive, "the tail holds fewer than two samples, so no slope was fitted")
	case d.FitSpan < s.MinFitSpan:
		inconclusive = append(inconclusive, fmt.Sprintf(
			"the slopes were fitted to %v of samples and a leak verdict needs %v: a per-hour figure extrapolated from %v is noise",
			d.FitSpan.Round(time.Second), s.MinFitSpan, d.FitSpan.Round(time.Second),
		))
	default:
		head := tail[0]
		tip := tail[len(tail)-1]
		grGrew := tip.Goroutines-head.Goroutines >= s.GoroutineMaterial
		heapGrew := tip.HeapAlloc > head.HeapAlloc && tip.HeapAlloc-head.HeapAlloc >= s.HeapMaterial
		objGrew := tip.HeapObjects > head.HeapObjects && tip.HeapObjects-head.HeapObjects >= s.ObjectMaterial
		if d.GoroutinesPerHour > s.GoroutineGrowthPerHour && grGrew {
			failures = append(failures, fmt.Sprintf(
				"goroutines grew at %+.2f/h over %v, above the %.2f/h ceiling (%d -> %d over the fit, %d -> %d over the run)",
				d.GoroutinesPerHour, d.FitSpan.Round(time.Second), s.GoroutineGrowthPerHour,
				head.Goroutines, tip.Goroutines, d.FirstGoroutines, d.LastGoroutines,
			))
		}
		if d.HeapPerHour > s.HeapGrowthPerHour && heapGrew {
			failures = append(failures, fmt.Sprintf(
				"live heap grew at %+.1f KiB/h over %v, above the %.1f KiB/h ceiling (%s -> %s over the fit, %s -> %s over the run)",
				d.HeapPerHour/1024, d.FitSpan.Round(time.Second), s.HeapGrowthPerHour/1024,
				bytes(head.HeapAlloc), bytes(tip.HeapAlloc), bytes(d.FirstHeap), bytes(d.LastHeap),
			))
		}
		if d.ObjectsPerHour > s.ObjectGrowthPerHour && objGrew {
			failures = append(failures, fmt.Sprintf(
				"live objects grew at %+.0f/h over %v, above the %.0f/h ceiling (%d -> %d over the fit)",
				d.ObjectsPerHour, d.FitSpan.Round(time.Second), s.ObjectGrowthPerHour,
				head.HeapObjects, tip.HeapObjects,
			))
		}
	}

	switch {
	case len(failures) > 0:
		d.Grade = GradeFail
		d.Reasons = failures
		// A failure found in a run too short to clear is still a failure,
		// and the caveat belongs in the report rather than in the grade.
		d.Reasons = append(d.Reasons, inconclusive...)
	case len(inconclusive) > 0:
		d.Grade = GradeInconclusive
		d.Reasons = inconclusive
	default:
		d.Grade = GradePass
		d.Reasons = []string{fmt.Sprintf(
			"no growth above the thresholds over %v and %d samples; a leak slower than %.1f KiB/h would not have shown",
			d.Duration.Round(time.Second), len(s.Samples), s.HeapGrowthPerHour/1024,
		)}
	}
	return d
}

// tail is the last TailFraction of the samples, with at least two when there
// are two: the head is warm-up, and fitting a slope through it reads the
// program's start-up as a leak.
func (s Series) tail() []Sample {
	n := len(s.Samples)
	if n < 2 {
		return s.Samples
	}
	keep := int(float64(n) * s.TailFraction)
	if keep < 2 {
		keep = 2
	}
	if keep > n {
		keep = n
	}
	return s.Samples[n-keep:]
}

// slopePerHour is the least-squares slope of f over the samples' elapsed time,
// scaled to units per hour.
func slopePerHour(xs []Sample, f func(Sample) float64) float64 {
	n := float64(len(xs))
	if n < 2 {
		return 0
	}
	var sx, sy, sxy, sxx float64
	for _, s := range xs {
		x := s.Elapsed.Hours()
		y := f(s)
		sx += x
		sy += y
		sxy += x * y
		sxx += x * x
	}
	den := n*sxx - sx*sx
	if den == 0 {
		// Every sample at the same instant: no slope is defined, and
		// returning zero here rather than dividing is what stops a
		// NaN grading a run.
		return 0
	}
	return (n*sxy - sx*sy) / den
}

// Recorder collects samples on an interval. It is the tagged runner's own
// instrument and lives here so that its behaviour is tested without a broker.
type Recorder struct {
	series Series
	start  time.Time
}

// NewRecorder starts a recorder for a series.
func NewRecorder(name string, start time.Time, cfg Series) *Recorder {
	cfg.Name = name
	return &Recorder{series: cfg.Defaults(), start: start}
}

// Sample records one observation at now.
func (r *Recorder) Sample(now time.Time, progress int64) Sample {
	s := Take(now.Sub(r.start), progress)
	r.series.Samples = append(r.series.Samples, s)
	return s
}

// Series returns the collected series.
func (r *Recorder) Series() Series { return r.series }

// Analyse grades what has been collected so far.
func (r *Recorder) Analyse() Diff { return r.series.Analyse() }

// ParseDuration reads IQMQ_SOAK_DURATION, which `make soak TIME=` sets.
//
// An unparseable value is an error rather than a silent default: a soak told
// TIME=10minutes and running for the default hour is a surprise, and a soak
// told TIME=10minutes and running for a second is worse.
func ParseDuration(v, def string) (time.Duration, error) {
	if v == "" {
		v = def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("IQMQ_SOAK_DURATION=%q is not a duration: %w", v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("IQMQ_SOAK_DURATION=%q is not positive", v)
	}
	return d, nil
}

// SampleEvery is how often a soak of d should sample, so that every run has
// enough samples for a slope without a 24-hour run holding a hundred thousand
// of them.
func SampleEvery(d time.Duration, minSamples int) time.Duration {
	if minSamples < 2 {
		minSamples = 2
	}
	// Twice the minimum, so that the tail alone has the minimum.
	every := d / time.Duration(2*minSamples)
	if every < time.Second {
		every = time.Second
	}
	if every > time.Minute {
		every = time.Minute
	}
	return every
}

// Grades is every grade's name, for a table-driven test's failure message.
func Grades() []string {
	out := []string{GradeInconclusive.String(), GradePass.String(), GradeFail.String()}
	sort.Strings(out)
	return out
}
