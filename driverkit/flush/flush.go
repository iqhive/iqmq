// Package flush holds the batch-settler flush policies when a queue of pending
// settlements leaves for the broker.
//
// It is here rather than in the SQS driver for the two reasons the specification's
// package gives. The decision is broker-independent — every broker whose
// settlement is a priced round trip faces the same four answers — and the
// bake-off measures a family in one package of the ROOT module, which is where
// points: runs one `go test` in that
// package from the repository root, and a root-module `go test` cannot reach a
// nested driver module (and the specification's own failure).
//
// # What a policy is, and what it is not
//
// [driverkit.BatchSettler] is the MECHANISM: it holds the queue, takes the
// lock, splits an oversized batch, sends through the driver's native batch and
// hands each item the entry of the *BatchError at its index. A [Policy] is the
// DECISION inside it — "given that a settlement has just been queued and the
// queue is now this long, send or wait?" — and nothing else. It is pure: no
// clock, no lock, no I/O, no state between calls. That is what makes the whole
// family model-testable and fuzzable without a broker, and what lets
// [Kind.Batching] bind a kind to the kit rather than reimplementing it.
//
// So there is exactly one implementation of the batching mechanism in this
// repository, and this package is the statement of its policy in a form a
// bake-off can measure. `TestPolicyAgreesWithTheKit` and `FuzzSettleFlush` are
// what keep the two from drifting: they drive a real
// [driverkit.BatchSettler] over a counting settler and check that the round
// trips it makes are the ones the policy predicted.
//
// # The four kinds
//
//   - [Immediate] sends every settlement on its own. It is the reference
//     implementation and the only kind that adds no latency and holds no
//     settlement the caller believes has happened. One round trip per
//     settlement is what it costs.
//   - [Count] sends once MaxItems settlements have accumulated. It is the
//     cheapest kind per settlement and the one whose worst case is unbounded:
//     the last settlement of a burst waits for a batch that never fills.
//   - [Time] sends every Interval. Its cost falls with the arrival rate and
//     its latency is bounded by the interval, whatever the rate.
//   - [Hybrid] sends at whichever comes first, which is the expected
//     winner ("hybrid (≤ 100 ms flush)"): the count bounds the cost and the
//     interval bounds the latency.
//
// # Shutdown
//
// the third criterion term is "shutdown correctness", and it is a property
// of the mechanism rather than of any policy: [driverkit.BatchSettler.Close]
// takes the closed flag and the queue under one lock, so a settlement either
// lands before the drain and is sent or is refused with ErrClosed. What a
// policy decides is how much is still in that queue when Close runs, which is
// what [Policy.Held] reports and what the model asserts on: nothing a policy
// holds may be lost, and [Immediate] holds nothing at all.
package flush

import (
	"time"

	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Kind identifies a policy variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Immediate is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Immediate sends every settlement on its own: no latency, no held
	// settlement, one round trip each.
	Immediate
	// Count sends once MaxItems settlements have accumulated.
	Count
	// Time sends every Interval.
	Time
	// Hybrid sends at whichever of the count and the interval comes first.
	Hybrid

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "immediate", "count", "time", "hybrid"}

var kindIdents = [kindCount]string{"Auto", "Immediate", "Count", "Time", "Hybrid"}

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
	for k := Immediate; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Slugs lists every concrete kind's slug, which is what a benchmark name and a
// DSN parameter use.
func Slugs() []string {
	out := make([]string, 0, kindCount-1)
	for _, k := range Kinds() {
		out = append(out, k.String())
	}
	return out
}

// Batching is the [driverkit.SettleBatching] this kind binds to.
//
// It is the whole point of the package: the kit already implements the
// mechanism for all four, so a driver that has chosen a kind converts it here
// rather than reimplementing the coalescing. Auto is resolved first, so a
// driver never hands the kit a kind that means "ask the bake-off".
func (k Kind) Batching() driverkit.SettleBatching {
	switch k.Resolve() {
	case Count:
		return driverkit.SettleCount
	case Time:
		return driverkit.SettleTime
	case Hybrid:
		return driverkit.SettleHybrid
	case Immediate, Auto:
		return driverkit.SettleImmediate
	default:
		return driverkit.SettleImmediate
	}
}

// The policy defaults, which are the kit's own so that a policy and the
// [driverkit.BatchSettler] it is bound to cannot disagree about a zero.
const (
	// DefaultMaxItems is the batch size a policy gets by not saying. Ten is
	// SQS's DeleteMessageBatch limit, which is the smallest of the brokers
	// that batch settlement at all.
	DefaultMaxItems = driverkit.DefaultSettleBatch
	// DefaultInterval is how often a time-based policy sends.
	DefaultInterval = driverkit.DefaultSettleInterval
)

// Config configures a policy. The zero value is usable for every kind.
type Config struct {
	// MaxItems is the batch size [Count] and [Hybrid] send at, and the
	// ceiling every policy splits a larger batch on. Zero means
	// DefaultMaxItems.
	MaxItems int
	// Interval is how often [Time] and [Hybrid] send. Zero means
	// DefaultInterval.
	Interval time.Duration
}

// resolve fills in the defaults.
func (c Config) resolve() Config {
	if c.MaxItems == 0 {
		c.MaxItems = DefaultMaxItems
	}
	if c.Interval == 0 {
		c.Interval = DefaultInterval
	}
	return c
}

// Validate reports whether the configuration can be used with k.
func (c Config) Validate(k Kind) error {
	switch {
	case !k.Valid():
		return ErrInvalidKind
	case c.MaxItems < 0 || c.Interval < 0:
		return ErrInvalidConfig
	default:
		return nil
	}
}

// Policy decides when a queue of pending settlements leaves for the broker.
//
// Every method is pure and safe to call from any goroutine: a policy holds no
// state, so the same value may drive every source of a client.
type Policy interface {
	// Submitted reports whether to send now, having just queued one
	// settlement. pending is the queue's length including it, and is
	// always at least one.
	Submitted(pending int) bool
	// Ticked reports whether to send now, the shared ticker having just
	// fired. pending is the queue's length, which may be zero.
	Ticked(pending int) bool
	// Ticker is the period of the shared ticker this policy needs, and
	// whether it needs one at all. A policy that does not is one timer
	// fewer per source.
	Ticker() (time.Duration, bool)
	// MaxItems is the ceiling an oversized batch is split on, which is the
	// broker's own batch limit and never zero.
	MaxItems() int
	// MaxHeld is the most settlements this policy can be holding at once,
	// and -1 when the policy bounds nothing itself.
	//
	// It is the specification's "shutdown correctness" as a number: what a Close has to
	// drain. [Immediate] holds nothing, the two count-bearing kinds hold at
	// most one short of a full batch, and [Time] bounds NOTHING — between
	// two ticks a policy that never sends on submit accumulates whatever
	// arrives, so the only bound is the number of settlements in flight,
	// which belongs to the consumer's prefetch and not to the policy. Saying
	// so is the point: a caller who needs a bound picks a kind that has one.
	MaxHeld() int
	// Kind returns the variant this policy implements.
	Kind() Kind
}

// New returns the policy of kind k configured by cfg.
//
// It refuses an invalid kind or configuration rather than panicking, because
// both reach it from a caller's own driver configuration and a driver
// validates its configuration before it starts background work.
func New(k Kind, cfg Config) (Policy, error) {
	if err := cfg.Validate(k); err != nil {
		return nil, err
	}
	base := policy{cfg: cfg.resolve(), kind: k.Resolve()}
	switch k.Resolve() {
	case Immediate:
		return immediate{base}, nil
	case Count:
		return count{base}, nil
	case Time:
		return timed{base}, nil
	case Hybrid:
		return hybrid{base}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// policy is the state every kind shares, which is its configuration and its
// identity. There is nothing else: a policy is a decision, not a queue.
type policy struct {
	cfg  Config
	kind Kind
}

// MaxItems is the ceiling an oversized batch is split on.
func (p policy) MaxItems() int { return p.cfg.MaxItems }

// Kind returns the variant.
func (p policy) Kind() Kind { return p.kind }

// The policy refusals. The specification names no sentinel for these, and errors.Is
// matches by code, so a caller compares mq.CodeOf(err).
var (
	// ErrInvalidKind reports a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq: the settlement batching policy is not one of the four kinds")
	// ErrInvalidConfig reports a Config that Validate rejects.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig,
		"iqmq: invalid settlement batching configuration")
)
