// Package backoff holds the retry-delay policies how long a publisher, a consumer or a driver
// waits before attempt n after a failure.
//
// A Policy is a pure function. Next takes the attempt number, the delay
// the caller used last and one random word, and returns the next delay; it
// reads no clock, holds no state and allocates nothing, so the same inputs
// always produce the same delay and a whole retry storm can be simulated
// deterministically (benchmarks/sim/retrystorm).
//
// Every policy respects the same envelope: the delay is never negative,
// never exceeds Config.Max, and the *unjittered* schedule Interval(n) is
// non-decreasing in n and saturates at Config.Max. The kinds differ only
// in how much of that envelope they give away to randomness, which is what
// the retry-storm simulation measures: without jitter every client that
// failed together retries together, and the peak of the second wave is as
// high as the first.
package backoff

import (
	"errors"
	"math"
	"time"
)

// Kind identifies a backoff policy. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Constant is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// None retries immediately: the delay is always zero.
	None
	// Constant waits Config.Base before every attempt. It is the
	// reference: deterministic, bounded and trivially checkable.
	Constant
	// Full is exponential backoff with full jitter: a delay drawn
	// uniformly from [0, Interval(attempt)].
	Full
	// Equal is exponential backoff with equal jitter: half of
	// Interval(attempt) plus a uniform draw from the other half.
	Equal
	// Decorrelated grows from the previous delay instead of the attempt
	// number: a uniform draw from [Base, prev×Growth], capped at Max.
	Decorrelated

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "none", "constant", "full", "equal", "decorrelated"}

var kindIdents = [kindCount]string{"Auto", "None", "Constant", "Full", "Equal", "Decorrelated"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := None; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Jittered reports whether the delay depends on the random word.
func (k Kind) Jittered() bool {
	switch k.Resolve() {
	case Full, Equal, Decorrelated:
		return true
	default:
		return false
	}
}

// UsesPrev reports whether the delay depends on the previous delay rather
// than on the attempt number alone.
func (k Kind) UsesPrev() bool { return k.Resolve() == Decorrelated }

// Exponential reports whether the unjittered schedule grows with the
// attempt number.
func (k Kind) Exponential() bool {
	switch k.Resolve() {
	case Full, Equal, Decorrelated:
		return true
	default:
		return false
	}
}

// Default configuration values (the message contract: 200 ms base, 30 s cap,
// doubling).
const (
	DefaultBase   = 200 * time.Millisecond
	DefaultMax    = 30 * time.Second
	DefaultGrowth = 2
)

// Config parameterises a policy. The zero Config means the defaults above.
type Config struct {
	// Base is the first delay of the unjittered schedule and the floor of
	// the decorrelated draw. Zero means DefaultBase; a negative value is
	// treated as zero delay.
	Base time.Duration
	// Max caps every delay. Zero means DefaultMax; a negative value means
	// no cap beyond the representable range.
	Max time.Duration
	// Growth is the factor between consecutive unjittered delays. Values
	// below 1 mean DefaultGrowth.
	Growth float64
}

// Policy computes retry delays. Implementations are pure, safe for
// concurrent use and allocation-free.
type Policy interface {
	// Kind returns the concrete kind (never Auto).
	Kind() Kind
	// Next returns the delay to wait before attempt, which counts from 1.
	// prev is the delay returned for the previous attempt (zero on the
	// first) and rand is a uniformly distributed random word; a policy
	// that ignores either still accepts it, so callers need not know
	// which kind they hold. The result is in [0, Max].
	Next(attempt int, prev time.Duration, rand uint64) time.Duration
	// Interval returns the unjittered delay for attempt: the top of the
	// envelope the jitter draws from. It is non-decreasing in attempt.
	Interval(attempt int) time.Duration
	// Config returns the resolved configuration.
	Config() Config
}

// New returns a policy of kind k with the default configuration.
func New(k Kind) (Policy, error) { return NewWith(k, Config{}) }

// NewFull returns a Full-jitter policy. Its kind is fixed, so it cannot fail.
func NewFull(cfg Config) Policy { return policy{kind: Full, cfg: cfg.resolve()} }

// NewEqual returns an Equal-jitter policy. Its kind is fixed, so it cannot
// fail.
func NewEqual(cfg Config) Policy { return policy{kind: Equal, cfg: cfg.resolve()} }

// NewWith returns a policy of kind k, or fails on an invalid kind.
func NewWith(k Kind, cfg Config) (Policy, error) {
	k = k.Resolve()
	cfg = cfg.resolve()
	switch k {
	case None, Constant, Full, Equal, Decorrelated:
		return policy{kind: k, cfg: cfg}, nil
	default:
		return nil, errors.New("backoff: invalid kind")
	}
}

// resolve applies the defaults and normalises the bounds.
func (c Config) resolve() Config {
	if c.Base == 0 {
		c.Base = DefaultBase
	}
	if c.Base < 0 {
		c.Base = 0
	}
	switch {
	case c.Max == 0:
		c.Max = DefaultMax
	case c.Max < 0:
		c.Max = math.MaxInt64
	}
	if c.Base > c.Max {
		c.Base = c.Max
	}
	if c.Growth < 1 {
		c.Growth = DefaultGrowth
	}
	return c
}

// policy implements every kind; the kind selects the arithmetic. One
// concrete type keeps the value small enough to pass by value and lets the
// compiler inline Next at a call site that knows the kind.
type policy struct {
	kind Kind
	cfg  Config
}

func (p policy) Kind() Kind     { return p.kind }
func (p policy) Config() Config { return p.cfg }

// Interval is the unjittered schedule: Base × Growth^(attempt-1), capped
// at Max, computed in floating point and saturated so that a large attempt
// number cannot overflow into a negative duration.
func (p policy) Interval(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	switch p.kind {
	case None:
		return 0
	case Constant:
		return p.cfg.Base
	default: // the exponential kinds are computed below
	}
	if p.cfg.Base == 0 {
		return 0
	}
	// Growth^(attempt-1) overflows quickly; the cap makes everything past
	// the crossing point identical, so the exponent is clamped first.
	span := math.Log(float64(p.cfg.Max)/float64(p.cfg.Base))/math.Log(p.cfg.Growth) + 1
	if float64(attempt) >= span {
		return p.cfg.Max
	}
	d := float64(p.cfg.Base) * math.Pow(p.cfg.Growth, float64(attempt-1))
	if d >= float64(p.cfg.Max) {
		return p.cfg.Max
	}
	return time.Duration(d)
}

// Next implements Policy.
func (p policy) Next(attempt int, prev time.Duration, rand uint64) time.Duration {
	switch p.kind {
	case None:
		return 0
	case Constant:
		return p.cfg.Base
	case Full:
		return uniform(rand, p.Interval(attempt))
	case Equal:
		half := p.Interval(attempt) / 2
		return half + uniform(rand, p.Interval(attempt)-half)
	case Decorrelated:
		lo := p.cfg.Base
		hi := scale(prev, p.cfg.Growth)
		if prev <= 0 {
			hi = scale(lo, p.cfg.Growth)
		}
		if hi > p.cfg.Max {
			hi = p.cfg.Max
		}
		if hi <= lo {
			return lo
		}
		return lo + uniform(rand, hi-lo)
	default:
		return 0
	}
}

// uniform maps rand to a duration in [0, span] without bias worth caring
// about (the modulo bias is below one part in 2^40 for any span a retry
// policy can express) and without overflow.
func uniform(rand uint64, span time.Duration) time.Duration {
	if span <= 0 {
		return 0
	}
	return time.Duration(rand % uint64(span+1))
}

// scale multiplies d by f, saturating at the largest duration.
func scale(d time.Duration, f float64) time.Duration {
	if d <= 0 {
		return 0
	}
	v := float64(d) * f
	if v >= math.MaxInt64 {
		return math.MaxInt64
	}
	return time.Duration(v)
}
