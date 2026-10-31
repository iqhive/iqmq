package iqmq_test

import (
	"math"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestBackoffStaysInsideItsEnvelope pins the backoff shape: the unjittered
// schedule doubles from Base and is capped at Max, and every jitter draws
// inside it. A jittered delay is random, so the assertion is the envelope and
// not a value — and the envelope is the whole contract a retry storm depends
// on.
func TestBackoffStaysInsideItsEnvelope(t *testing.T) {
	const rounds = 200
	b := mq.ExponentialBackoff{Base: 100 * time.Millisecond, Max: time.Second}
	cases := []struct {
		name   string
		jitter mq.Jitter
		lo     func(env time.Duration) time.Duration
	}{
		{"full", mq.FullJitter, func(time.Duration) time.Duration { return 0 }},
		{"equal", mq.EqualJitter, func(env time.Duration) time.Duration { return env / 2 }},
		{"none", mq.NoJitter, func(env time.Duration) time.Duration { return env }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := b
			p.Jitter = tc.jitter
			for attempt := 2; attempt <= 8; attempt++ {
				env := envelope(b.Base, b.Max, attempt)
				lo := tc.lo(env)
				for range rounds {
					d := p.Delay(attempt)
					if d < lo || d > env {
						t.Fatalf("%s Delay(%d) = %v, want [%v, %v]", tc.name, attempt, d, lo, env)
					}
				}
			}
		})
	}
}

// envelope is the unjittered schedule attempt n draws from: Base doubled once
// per redelivery, capped at Max.
func envelope(base, maxDelay time.Duration, attempt int) time.Duration {
	d := base
	for range attempt - 2 {
		d *= 2
		if d >= maxDelay {
			return maxDelay
		}
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}

// TestBackoffIsZeroBeforeTheFirstDelivery pins the counting: attempt 1 is the
// first delivery, which nothing waited for. A backoff that charged a delay
// before the first delivery would delay every message rather than every
// retry.
func TestBackoffIsZeroBeforeTheFirstDelivery(t *testing.T) {
	for _, b := range []mq.Backoff{
		mq.ExponentialBackoff{},
		mq.ExponentialBackoff{Jitter: mq.NoJitter},
		mq.ConstantBackoff(time.Second),
	} {
		for _, attempt := range []int{-1, 0, 1} {
			if d := b.Delay(attempt); d != 0 {
				t.Fatalf("%T.Delay(%d) = %v, want 0", b, attempt, d)
			}
		}
	}
}

// TestConstantBackoffWaitsTheSameEveryTime pins the specification's "constant", and
// ConstantBackoff(0) as its "none".
func TestConstantBackoffWaitsTheSameEveryTime(t *testing.T) {
	b := mq.ConstantBackoff(250 * time.Millisecond)
	for attempt := 2; attempt <= 6; attempt++ {
		if d := b.Delay(attempt); d != 250*time.Millisecond {
			t.Fatalf("Delay(%d) = %v, want 250ms", attempt, d)
		}
	}
	if d := mq.ConstantBackoff(0).Delay(5); d != 0 {
		t.Fatalf("ConstantBackoff(0).Delay(5) = %v, want 0", d)
	}
}

// TestZeroExponentialBackoffIsTheDocumentedDefault pins the zero value:
// ADR-011's 200 ms base and 30 s cap, with full jitter. A caller who writes
// mq.ExponentialBackoff{} gets the safe default and not "no delay at all".
func TestZeroExponentialBackoffIsTheDocumentedDefault(t *testing.T) {
	b := mq.ExponentialBackoff{Jitter: mq.NoJitter}
	if d := b.Delay(2); d != mq.DefaultRetryBase {
		t.Fatalf("Delay(2) = %v, want DefaultRetryBase %v", d, mq.DefaultRetryBase)
	}
	if d := b.Delay(64); d != mq.DefaultRetryMax {
		t.Fatalf("Delay(64) = %v, want the cap DefaultRetryMax %v", d, mq.DefaultRetryMax)
	}
}

// TestExponentialBackoffWithANonFiniteGrowthDoubles: a growth the caller
// computed can come out NaN or +Inf. Neither is below 1, so neither meant 2: NaN
// gave negative delays, which the runner reads as "redeliver at once", and +Inf
// made the first retry wait Max.
func TestExponentialBackoffWithANonFiniteGrowthDoubles(t *testing.T) {
	for _, g := range []float64{math.NaN(), math.Inf(1)} {
		b := mq.ExponentialBackoff{Base: 200 * time.Millisecond, Max: 30 * time.Second, Growth: g, Jitter: mq.NoJitter}
		for attempt := 2; attempt <= 12; attempt++ {
			if got, want := b.Delay(attempt), envelope(b.Base, b.Max, attempt); got != want {
				t.Errorf("Growth %v: Delay(%d) = %v, want %v", g, attempt, got, want)
			}
		}
		for _, j := range []mq.Jitter{mq.FullJitter, mq.EqualJitter} {
			b.Jitter = j
			for attempt := 2; attempt <= 12; attempt++ {
				if d := b.Delay(attempt); d < 0 || d > envelope(b.Base, b.Max, attempt) {
					t.Errorf("Growth %v, %v: Delay(%d) = %v, outside [0, %v]", g, j, attempt, d, envelope(b.Base, b.Max, attempt))
				}
			}
		}
	}
}

// TestJitterNames pins the metric-label spellings and the validity rule, for
// the reason every other enum in this package has one: a name that changes is
// a dashboard that breaks.
func TestJitterNames(t *testing.T) {
	for j, want := range map[mq.Jitter]string{
		mq.FullJitter:  "full",
		mq.EqualJitter: "equal",
		mq.NoJitter:    "none",
	} {
		if got := j.String(); got != want {
			t.Fatalf("Jitter(%d).String() = %q, want %q", j, got, want)
		}
		if !j.Valid() {
			t.Fatalf("Jitter(%d) is not Valid", j)
		}
	}
	if bad := mq.Jitter(9); bad.Valid() || bad.String() != "Jitter(?)" {
		t.Fatalf("Jitter(9) = %q valid=%v, want the unknown spelling", bad.String(), bad.Valid())
	}
}

// TestFailurePolicyAndSettlementModeNames pins the other two enums the runner
// exports.
func TestFailurePolicyAndSettlementModeNames(t *testing.T) {
	for p, want := range map[mq.FailurePolicy]string{
		mq.FailureDefault:         "default",
		mq.FailureDeadLetter:      "deadletter",
		mq.FailureHalt:            "halt",
		mq.FailureSkip:            "skip",
		mq.FailureInfiniteRequeue: "infiniterequeue",
	} {
		if got := p.String(); got != want || !p.Valid() {
			t.Fatalf("FailurePolicy(%d) = %q valid=%v, want %q", p, got, p.Valid(), want)
		}
	}
	if bad := mq.FailurePolicy(9); bad.Valid() || bad.String() != "FailurePolicy(?)" {
		t.Fatalf("FailurePolicy(9) = %q valid=%v", bad.String(), bad.Valid())
	}
	for m, want := range map[mq.SettlementMode]string{
		mq.SettlementAuto:   "auto",
		mq.SettlementManual: "manual",
	} {
		if got := m.String(); got != want || !m.Valid() {
			t.Fatalf("SettlementMode(%d) = %q valid=%v, want %q", m, got, m.Valid(), want)
		}
	}
	if bad := mq.SettlementMode(9); bad.Valid() || bad.String() != "SettlementMode(?)" {
		t.Fatalf("SettlementMode(9) = %q valid=%v", bad.String(), bad.Valid())
	}
}
