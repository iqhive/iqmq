package iqmq

import (
	"math/rand/v2"
	"time"

	"github.com/iqhive/iqmq/internal/backoff"
)

// The runner's retry defaults (the message contract, ADR-011's provisional safe
// default: "5 attempts, bounded exponential jittered backoff").
const (
	// DefaultMaxAttempts is how many times a message is delivered before the
	// failure policy decides what becomes of it. It counts deliveries and
	// not retries, so 5 means the handler sees the message at most five
	// times.
	DefaultMaxAttempts = 5
	// DefaultRetryBase is the first delay of the default backoff.
	DefaultRetryBase = 200 * time.Millisecond
	// DefaultRetryMax caps every delay of the default backoff.
	DefaultRetryMax = 30 * time.Second
)

// Jitter is how much randomness a backoff draws into its delay.
//
// The zero value is FullJitter, because ADR-011's provisional default is
// "bounded exponential jittered backoff": a caller that names a base and a
// cap and nothing else gets the jitter, and has to ask for the retry storm.
type Jitter uint8

// The jitter policies.
const (
	// FullJitter draws uniformly from [0, interval]. It is the zero value.
	FullJitter Jitter = iota
	// EqualJitter draws uniformly from [interval/2, interval].
	EqualJitter
	// NoJitter returns the interval itself. Every client that fails at the
	// same moment then retries at the same moment, which is the retry storm
	// The specification names; it exists because a test wants a predictable delay.
	NoJitter

	jitterCount
)

var jitterNames = [jitterCount]string{"full", "equal", "none"}

// String returns the jitter's lower-case name.
func (j Jitter) String() string {
	if j < jitterCount {
		return jitterNames[j]
	}
	return "Jitter(?)"
}

// Valid reports whether j names a jitter policy.
func (j Jitter) Valid() bool { return j < jitterCount }

// kind maps the public jitter onto the internal backoff family's kind. Every
// jitter shares one envelope — Base x Growth^(n-1) capped at Max — so NoJitter
// takes the Full policy and reads Interval rather than Next; internal/backoff
// has no unjittered exponential kind, and Constant is a different schedule
// rather than the same one without the draw.
func (j Jitter) kind() backoff.Kind {
	switch j {
	case EqualJitter:
		return backoff.Equal
	case FullJitter, NoJitter, jitterCount:
		return backoff.Full
	default:
		return backoff.Full
	}
}

// Backoff computes how long a message waits before it is delivered again
//
// A Backoff is asked once per failed delivery, on the goroutine that ran the
// handler, and must be safe for concurrent use: the runner calls it from
// every lane at once. It must not block and must not read the wall clock —
// the answer is a duration, and the runner turns it into a delayed requeue.
//
// The implementations this package ships are ExponentialBackoff and
// ConstantBackoff. The interface is exported so that a caller can supply its
// own; the decorrelated jitter is deliberately not among them, because it
// needs the previous delay and this runner retries by nack, which hands the
// message back to the broker and keeps nothing about it.
type Backoff interface {
	// Delay returns how long to wait before attempt, which counts from 1:
	// Delay(2) is the wait before the second delivery. A non-positive
	// result means "redeliver immediately".
	Delay(attempt int) time.Duration
}

// ExponentialBackoff is the bounded exponential backoff with jitter.
//
// The unjittered schedule is Base × Growth^(attempt-2) for attempt ≥ 2,
// capped at Max; the Jitter draws the delay out of that envelope. The zero
// value is the default of ADR-011: 200 ms base, 30 s cap, doubling, full
// jitter.
type ExponentialBackoff struct {
	// Base is the first delay. Zero means DefaultRetryBase; a negative
	// value means no delay at all.
	Base time.Duration
	// Max caps every delay. Zero means DefaultRetryMax; a negative value
	// means no cap.
	Max time.Duration
	// Growth is the factor between consecutive unjittered delays. A value
	// below 1 means 2.
	Growth float64
	// Jitter is how the delay is drawn out of the envelope. The zero value
	// is FullJitter.
	Jitter Jitter
}

// Delay implements Backoff.
//
// It is safe for concurrent use: the arithmetic is internal/backoff's pure
// policy and the random word comes from math/rand/v2's per-thread generator,
// which is the one piece of randomness the specification's "no global mutable state" does
// not mean — it is the runtime's, it carries no configuration, and nothing a
// caller does can observe it through this package.
//
// Building the policy puts a value in an interface, so this method allocates;
// the runner does not call it. It compiles the policy once, when Run starts,
// and calls that (compile below), which is rule 4 and why a retry costs
// no allocation on the delivery's path.
func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	return b.compile().Delay(attempt)
}

// compile builds the policy once so that a delay costs an indirect call and
// no allocation.
func (b ExponentialBackoff) compile() Backoff {
	return compiledBackoff{
		p: backoff.NewWith(b.Jitter.kind(), backoff.Config{
			Base:   b.Base,
			Max:    b.Max,
			Growth: b.Growth,
		}),
		plain: b.Jitter == NoJitter,
	}
}

// compiledBackoff is an ExponentialBackoff whose policy has already been
// built. The runner holds one for the life of a Run.
type compiledBackoff struct {
	p backoff.Policy
	// plain reports NoJitter: the delay is the envelope itself rather than a
	// draw out of it.
	plain bool
}

// Delay implements Backoff.
func (c compiledBackoff) Delay(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	// internal/backoff counts the delay before attempt n as Interval(n), so
	// the wait before the SECOND delivery is its first interval. prev is
	// zero because this runner retries by nack and therefore keeps nothing
	// about a message between attempts.
	if c.plain {
		return c.p.Interval(attempt - 1)
	}
	//nolint:gosec // jitter spreads retries, it does not keep a secret: a predictable draw costs fairness, not safety
	return c.p.Next(attempt-1, 0, rand.Uint64())
}

// ConstantBackoff waits the same time before every redelivery (the specification's
// "constant"). ConstantBackoff(0) is the specification's "none": redeliver immediately.
type ConstantBackoff time.Duration

// Delay implements Backoff.
func (b ConstantBackoff) Delay(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	return time.Duration(b)
}

// defaultBackoff is ADR-011's provisional safe default, compiled once: a
// Backoff is asked per failed delivery, and the default must not allocate to
// answer.
var defaultBackoff = ExponentialBackoff{
	Base:   DefaultRetryBase,
	Max:    DefaultRetryMax,
	Jitter: FullJitter,
}.compile()
