package backoff

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// TestModel checks the envelope every policy must respect and the
// distribution each kind promises: bounds, determinism, monotone
// unjittered schedule, saturation at Max, behaviour at the extremes of the
// configuration space, and the statistical shape of the jitter.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			t.Run("bounds", func(t *testing.T) { modelBounds(t, k) })
			t.Run("deterministic", func(t *testing.T) { modelDeterministic(t, k) })
			t.Run("interval-monotone", func(t *testing.T) { modelInterval(t, k) })
			t.Run("extremes", func(t *testing.T) { modelExtremes(t, k) })
			t.Run("distribution", func(t *testing.T) { modelDistribution(t, k) })
			t.Run("config", func(t *testing.T) { modelConfig(t, k) })
		})
	}
}

// modelConfigs are the configurations the contract subtests sweep,
// including the degenerate ones a caller can write by accident.
func modelConfigs() []Config {
	return []Config{
		{},
		{Base: time.Millisecond, Max: time.Second},
		{Base: 200 * time.Millisecond, Max: 30 * time.Second, Growth: 2},
		{Base: time.Nanosecond, Max: time.Hour, Growth: 1.5},
		{Base: time.Second, Max: time.Second},
		{Base: time.Hour, Max: time.Second},       // base above the cap
		{Base: -1, Max: time.Second},              // negative base
		{Base: time.Second, Max: -1},              // no cap
		{Base: time.Second, Growth: 0.5},          // growth below 1
		{Base: math.MaxInt64, Max: math.MaxInt64}, // saturated
		{Base: time.Millisecond, Max: math.MaxInt64, Growth: 1e6},
	}
}

func modelAttempts() []int {
	return []int{-5, 0, 1, 2, 3, 4, 8, 16, 31, 32, 33, 63, 64, 100, 1000, 1 << 20, math.MaxInt32}
}

func modelBounds(t *testing.T, k Kind) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, cfg := range modelConfigs() {
		p := NewWith(k, cfg)
		got := p.Config()
		for _, attempt := range modelAttempts() {
			for range 200 {
				prev := time.Duration(r.Int64N(int64(time.Hour)))
				if r.IntN(4) == 0 {
					prev = 0
				}
				d := p.Next(attempt, prev, r.Uint64())
				if d < 0 {
					t.Fatalf("%+v attempt %d: negative delay %v", cfg, attempt, d)
				}
				if d > got.Max {
					t.Fatalf("%+v attempt %d: %v exceeds Max %v", cfg, attempt, d, got.Max)
				}
				if iv := p.Interval(attempt); iv < 0 || iv > got.Max {
					t.Fatalf("%+v attempt %d: Interval %v out of bounds", cfg, attempt, iv)
				}
			}
		}
	}
}

func modelDeterministic(t *testing.T, k Kind) {
	p := New(k)
	r := rand.New(rand.NewPCG(3, 4))
	for range 1000 {
		attempt := r.IntN(50)
		prev := time.Duration(r.Int64N(int64(time.Minute)))
		word := r.Uint64()
		a := p.Next(attempt, prev, word)
		b := p.Next(attempt, prev, word)
		if a != b {
			t.Fatalf("Next(%d, %v, %#x) = %v then %v", attempt, prev, word, a, b)
		}
	}
}

// modelInterval: the unjittered schedule never decreases and reaches the
// cap for the kinds that grow.
func modelInterval(t *testing.T, k Kind) {
	for _, cfg := range modelConfigs() {
		p := NewWith(k, cfg)
		prev := time.Duration(-1)
		for attempt := 1; attempt <= 200; attempt++ {
			iv := p.Interval(attempt)
			if iv < prev {
				t.Fatalf("%+v: Interval(%d) = %v is below Interval(%d) = %v", cfg, attempt, iv, attempt-1, prev)
			}
			prev = iv
		}
		max := p.Config().Max
		switch {
		case k.Exponential() && p.Config().Base > 0 && p.Interval(1000) != max:
			t.Fatalf("%+v: Interval(1000) = %v, want the cap %v", cfg, p.Interval(1000), max)
		case k == Constant && p.Interval(1000) != p.Config().Base:
			t.Fatal("constant schedule grew")
		case k == None && p.Interval(1000) != 0:
			t.Fatal("none schedule is not zero")
		}
		// Attempt numbers below one behave like the first attempt.
		if p.Interval(0) != p.Interval(1) || p.Interval(-7) != p.Interval(1) {
			t.Fatal("non-positive attempt numbers differ from the first attempt")
		}
	}
}

// modelExtremes: the largest representable inputs neither overflow nor
// wrap into a negative delay.
func modelExtremes(t *testing.T, k Kind) {
	p := NewWith(k, Config{Base: math.MaxInt64 / 4, Max: math.MaxInt64, Growth: 1e30})
	for _, attempt := range []int{1, 2, 1000, math.MaxInt32, math.MaxInt} {
		for _, prev := range []time.Duration{0, -1, math.MaxInt64, math.MaxInt64 / 2} {
			for _, word := range []uint64{0, 1, math.MaxUint64, math.MaxUint64 / 2} {
				d := p.Next(attempt, prev, word)
				if d < 0 {
					t.Fatalf("attempt %d prev %v word %#x: %v", attempt, prev, word, d)
				}
			}
		}
	}
}

// modelDistribution checks the shape each kind promises: full jitter
// covers [0, interval] with mean about half; equal jitter never returns
// less than half the interval; decorrelated grows from the previous delay;
// the deterministic kinds ignore the random word entirely.
func modelDistribution(t *testing.T, k Kind) {
	cfg := Config{Base: time.Second, Max: time.Hour, Growth: 2}
	p := NewWith(k, cfg)
	r := rand.New(rand.NewPCG(5, 6))
	const n = 20000
	const attempt = 5
	iv := p.Interval(attempt)
	var sum time.Duration
	minD, maxD := time.Duration(math.MaxInt64), time.Duration(0)
	for range n {
		d := p.Next(attempt, iv, r.Uint64())
		sum += d
		minD = min(minD, d)
		maxD = max(maxD, d)
	}
	mean := sum / n
	switch k {
	case None:
		if minD != 0 || maxD != 0 {
			t.Fatalf("none returned %v…%v", minD, maxD)
		}
	case Constant:
		if minD != cfg.Base || maxD != cfg.Base {
			t.Fatalf("constant returned %v…%v, want %v", minD, maxD, cfg.Base)
		}
	case Full:
		if maxD > iv || minD > iv/50 || maxD < iv*49/50 {
			t.Fatalf("full jitter covered %v…%v of %v", minD, maxD, iv)
		}
		if mean < iv*45/100 || mean > iv*55/100 {
			t.Fatalf("full jitter mean %v, want about %v", mean, iv/2)
		}
	case Equal:
		if minD < iv/2 || maxD > iv {
			t.Fatalf("equal jitter covered %v…%v, want within %v…%v", minD, maxD, iv/2, iv)
		}
		if mean < iv*70/100 || mean > iv*80/100 {
			t.Fatalf("equal jitter mean %v, want about %v", mean, iv*3/4)
		}
	case Decorrelated:
		if minD < cfg.Base || maxD > min(iv*2, cfg.Max) {
			t.Fatalf("decorrelated covered %v…%v from prev %v", minD, maxD, iv)
		}
	default:
		t.Fatalf("no distribution expectation for %v", k)
	}
	// The deterministic kinds ignore the random word.
	if !k.Jittered() {
		a := p.Next(attempt, iv, 0)
		b := p.Next(attempt, iv, math.MaxUint64)
		if a != b {
			t.Fatalf("%v is jittered after all: %v vs %v", k, a, b)
		}
	}
	// The jittered kinds actually use it.
	if k.Jittered() {
		seen := map[time.Duration]bool{}
		for i := range 100 {
			seen[p.Next(attempt, iv, uint64(i)*0x9E3779B97F4A7C15)] = true
		}
		if len(seen) < 50 {
			t.Fatalf("%v produced only %d distinct delays from 100 words", k, len(seen))
		}
	}
}

// modelConfig: the resolved configuration applies the documented defaults.
func modelConfig(t *testing.T, k Kind) {
	if got := New(k).Config(); got.Base != DefaultBase || got.Max != DefaultMax || got.Growth != DefaultGrowth {
		t.Fatalf("default config = %+v", got)
	}
	if got := NewWith(k, Config{Base: time.Hour, Max: time.Second}).Config(); got.Base != time.Second {
		t.Fatalf("base above the cap resolved to %v", got.Base)
	}
	if got := NewWith(k, Config{Max: -1}).Config(); got.Max != math.MaxInt64 {
		t.Fatalf("negative Max resolved to %v", got.Max)
	}
	if got := NewWith(k, Config{Base: -1}).Config(); got.Base != 0 {
		t.Fatalf("negative Base resolved to %v", got.Base)
	}
	if got := NewWith(k, Config{Growth: 0.5}).Config(); got.Growth != DefaultGrowth {
		t.Fatalf("growth below 1 resolved to %v", got.Growth)
	}
}
