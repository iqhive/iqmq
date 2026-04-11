// Package nack holds the negative-settlement strategies what a driver does with a
// delivery the application refused.
//
// It is here rather than in the Kafka driver for two reasons. The decision is
// broker-independent — every cumulative-acknowledgement broker faces the same
// four answers — and the bake-off measures a family in one package of the root
// module, which is where points. The Kafka driver binds
// a [Strategy] and applies its [Plan]; nothing in here knows about Kafka, and
// nothing in here performs I/O.
//
// # The four strategies
//
// A negative settlement on a broker whose acknowledgement is a cumulative
// offset has no native form: there is no per-record nack, so the driver has to
// choose between holding the partition still and moving the record somewhere
// else. The specification requires all four answers to be implemented and none of them to
// be selected silently, because two of them change ordering and one of them
// stops the consumer:
//
//   - [BlockPartition] holds the record in process, pauses the partition and
//     redelivers it. The offset stays uncommitted, so nothing after the failed
//     record is committed before it succeeds and the partition's order is
//     preserved. It is the reference implementation and the default
//     (ADR-010 SafeOnly emulation policy: the only one of the four
//     that changes neither ordering nor durability).
//   - [RetryTopic] publishes the record to `<topic>.retry.<n>` with the
//     attempt in a header and then completes the original offset, so the
//     partition keeps moving. The record is reordered with respect to its
//     partition, which is why it is never chosen by default.
//   - [Stop] stops the consumer. Nothing is reordered and nothing is lost,
//     and the subscription makes no further progress until somebody restarts
//     it: a poison record becomes an outage rather than a duplicate.
//   - [Custom] hands the decision to the caller, and validates what comes
//     back. A custom strategy cannot return a plan that commits an offset it
//     also asks to have redelivered.
//
// # Purity
//
// [Strategy.Plan] is a pure function of the delivery and the negative
// settlement's options: no clock, no I/O, no per-partition state. That is what
// makes a strategy safe to call from any goroutine, and what lets the whole
// family be model-tested and fuzzed without a broker. Everything stateful — the
// pause, the republication, the commit — is the driver's, and the plan is the
// instruction it follows.
package nack

import (
	"strconv"
	"strings"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Kind identifies a strategy variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. BlockPartition is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// BlockPartition holds the record in process, pauses the partition and
	// redelivers it, leaving the offset uncommitted: the only one of the
	// four that changes neither ordering nor durability.
	BlockPartition
	// RetryTopic republishes the record to `<topic>.retry.<n>` and commits
	// the original offset, so the partition keeps moving and the record is
	// reordered with respect to it.
	RetryTopic
	// Stop stops the consumer, so nothing is reordered and nothing after
	// the failed record is consumed until somebody restarts it.
	Stop
	// Custom hands the decision to the caller's own function, whose plan is
	// validated before it is followed.
	Custom

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "blockpartition", "retrytopic", "stop", "custom"}

var kindIdents = [kindCount]string{"Auto", "BlockPartition", "RetryTopic", "Stop", "Custom"}

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
	for k := BlockPartition; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Op is what a driver does with a negatively settled delivery.
type Op uint8

// The operations a [Plan] can ask for.
const (
	// OpRetryInPlace holds the record in process and redelivers it. The
	// offset is NOT completed, so the commit watermark does not pass it and
	// the partition's order survives.
	OpRetryInPlace Op = iota
	// OpRepublish publishes the record to [Plan.Topic] and then completes
	// the original offset. The record is reordered with respect to its
	// partition.
	OpRepublish
	// OpDeadLetter publishes the record to the dead-letter topic and then
	// completes the original offset (ADR-011).
	OpDeadLetter
	// OpDrop completes the original offset without republishing the record
	// anywhere. It is only ever reached from an explicit
	// mq.NackActionDrop: nothing here discards a message on its own.
	OpDrop
	// OpStop stops the consumer without completing the offset, which is
	// ADR-011's "halt when there is no dead-letter destination".
	OpStop

	opCount
)

var opNames = [opCount]string{"retry-in-place", "republish", "dead-letter", "drop", "stop"}

// String returns the operation's name.
func (o Op) String() string {
	if o < opCount {
		return opNames[o]
	}
	return "Op(?)"
}

// Valid reports whether o names an operation.
func (o Op) Valid() bool { return o < opCount }

// Completes reports whether the operation lets the commit watermark pass the
// record's offset. It is the half of a plan that decides durability: an
// operation that completes an offset the broker has not been told about again
// is how a nacked message is lost.
func (o Op) Completes() bool {
	switch o {
	case OpRepublish, OpDeadLetter, OpDrop:
		return true
	case OpRetryInPlace, OpStop:
		return false
	default:
		return false
	}
}

// PreservesOrder reports whether the operation keeps the partition's delivery
// order. It is what the specification's "ordering violations" column counts: a plan that
// completes an offset and republishes the record elsewhere has moved that
// record behind everything still in the partition.
func (o Op) PreservesOrder() bool {
	switch o {
	case OpRetryInPlace, OpStop:
		return true
	case OpRepublish, OpDeadLetter, OpDrop:
		return false
	default:
		return false
	}
}

// Delivery is the record a strategy decides about.
//
// It carries no native handle and no payload: a strategy reads coordinates and
// attempt counts, which is what keeps it pure and what lets the model drive it
// without a broker.
type Delivery struct {
	// Topic is the topic the record was delivered from, which is a retry
	// topic when the record has already been republished once.
	Topic string
	// Partition is the record's partition.
	Partition int32
	// Offset is the record's offset within its partition.
	Offset int64
	// Attempt is the delivery attempt, 1 on the first delivery. Zero is
	// read as one: the specification makes zero "the broker does not count attempts",
	// and a strategy that read it as "attempt zero" would give a record one
	// extra try on every broker that does not count.
	Attempt uint32
}

// attempt is Attempt with the zero folded into the first attempt.
func (d Delivery) attempt() uint32 {
	if d.Attempt == 0 {
		return 1
	}
	return d.Attempt
}

// Plan is one strategy's decision about one delivery.
//
// It is a value, so planning allocates nothing. The driver follows it
// literally: there is no field here the driver is expected to second-guess,
// which is what makes the strategies comparable at all.
type Plan struct {
	// Op is the operation to perform.
	Op Op
	// Topic is the destination of OpRepublish and OpDeadLetter, and empty
	// for every other operation.
	Topic string
	// Attempt is the attempt number the republished record carries, so that
	// the consumer of a retry topic knows how many tries this record has
	// had. It is zero for an operation that publishes nothing.
	Attempt uint32
	// Delay is how long the driver holds the record before redelivering it
	// (OpRetryInPlace) or before the republished record becomes visible.
	Delay time.Duration
	// Pause asks for the partition to be paused until the record has been
	// redelivered, which is what keeps OpRetryInPlace from spinning on a
	// broker that would otherwise keep fetching.
	Pause bool
	// Reason is local diagnostics, never transported and never a payload
	Reason string
}

// Completes reports whether this plan lets the commit watermark pass the
// record's offset.
func (p Plan) Completes() bool { return p.Op.Completes() }

// PreservesOrder reports whether this plan keeps the partition's order.
func (p Plan) PreservesOrder() bool { return p.Op.PreservesOrder() }

// Validate reports whether the plan is self-consistent.
//
// It exists for [Custom]: a caller's own strategy is the one place a plan can
// be built by code this package has not seen, and the two combinations below
// are the ones that lose or duplicate a message rather than merely behaving
// differently.
//
//   - A publishing operation with no topic publishes nowhere and then commits.
//   - A paused partition whose offset is completed is a partition that will be
//     resumed with the record already committed, so the redelivery the pause
//     was waiting for never comes.
func (p Plan) Validate() error {
	switch {
	case !p.Op.Valid():
		return ErrInvalidPlan
	case p.Delay < 0:
		return ErrInvalidPlan
	case (p.Op == OpRepublish || p.Op == OpDeadLetter) && p.Topic == "":
		return ErrInvalidPlan
	case p.Op != OpRepublish && p.Op != OpDeadLetter && p.Topic != "":
		return ErrInvalidPlan
	case p.Pause && p.Op.Completes():
		return ErrInvalidPlan
	default:
		return nil
	}
}

// Strategy decides what happens to a delivery the application refused.
//
// Plan is pure: no clock, no I/O, no state. It is safe to call from any
// goroutine and from several at once.
type Strategy interface {
	// Plan decides the operation for one negative settlement. It returns an
	// error only for a configuration the strategy cannot honour at all —
	// an invalid plan from a custom function, or a nack action this
	// strategy has no answer for — and the driver then refuses the
	// settlement rather than guessing.
	Plan(d Delivery, o mq.NackOptions) (Plan, error)
	// Kind returns the variant this strategy implements.
	Kind() Kind
}

// DefaultRetries is the number of retry topics [RetryTopic] uses when
// [Config.Retries] is zero: `<topic>.retry.1` … `<topic>.retry.3`.
const DefaultRetries = 3

// Config configures a strategy. The zero value is usable for every kind but
// [Custom], which has no default decision to fall back on.
type Config struct {
	// MaxAttempts is how many deliveries a record may have before the
	// strategy gives up on retrying it and takes the failure route —
	// the dead-letter topic when there is one, and a stop when there is
	// not (ADR-011). Zero means no limit, which is what a caller who
	// lets the core count attempts asks for.
	MaxAttempts uint32
	// DeadLetterTopic is the destination for a record whose attempts are
	// exhausted and for an explicit mq.NackActionDeadLetter. Empty means
	// there is none, and ADR-011's failure policy then halts instead.
	DeadLetterTopic string
	// Retries is how many retry topics [RetryTopic] walks through before it
	// takes the failure route. Zero means DefaultRetries.
	Retries int
	// Delay is the hold applied to a redelivery when the negative
	// settlement named none. Zero means no hold, which is what a caller
	// that has its own backoff wants.
	Delay time.Duration
	// Decide is [Custom]'s decision function. It is required for that kind
	// and ignored by every other. Its plan is validated before it is
	// followed, so a function that returns an inconsistent one is refused
	// rather than obeyed.
	Decide func(d Delivery, o mq.NackOptions) (Plan, error)
}

// retries is Retries with its default applied.
func (c Config) retries() int {
	if c.Retries <= 0 {
		return DefaultRetries
	}
	return c.Retries
}

// delay is the hold a plan carries: the settlement's own when it named one,
// and the configuration's otherwise.
func (c Config) delay(o mq.NackOptions) time.Duration {
	if o.Delay > 0 {
		return o.Delay
	}
	return c.Delay
}

// exhausted reports whether the record has had all the attempts it may have.
func (c Config) exhausted(d Delivery) bool {
	return c.MaxAttempts > 0 && d.attempt() >= c.MaxAttempts
}

// Validate reports whether the configuration can be used with k, without any
// I/O.
//
// A [Custom] strategy with no function is the one refusal that matters: the
// alternative is a driver that silently fell back to another kind, which is
// exactly what the specification means by "none silently chosen".
func (c Config) Validate(k Kind) error {
	switch {
	case !k.Valid():
		return ErrInvalidKind
	case c.Delay < 0 || c.Retries < 0:
		return ErrInvalidConfig
	case k.Resolve() == Custom && c.Decide == nil:
		return ErrNoDecideFunc
	case c.DeadLetterTopic != "" && !validTopicName(c.DeadLetterTopic):
		return ErrInvalidConfig
	default:
		return nil
	}
}

// New returns a strategy of kind k configured by cfg.
//
// It refuses an invalid kind or configuration rather than panicking, because
// both reach it from a caller's own [bitbucket.org/iqhive/iqmq.ConsumerConfig]
// and a driver validates its configuration before it starts background work
func New(k Kind, cfg Config) (Strategy, error) {
	if err := cfg.Validate(k); err != nil {
		return nil, err
	}
	switch k.Resolve() {
	case BlockPartition:
		return blockPartition{cfg: cfg}, nil
	case RetryTopic:
		return retryTopic{cfg: cfg}, nil
	case Stop:
		return stop{cfg: cfg}, nil
	case Custom:
		return custom{cfg: cfg}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// terminal answers the two nack actions that are not a retry at all, for the
// three built-in kinds that share them.
//
// mq.NackActionDeadLetter and mq.NackActionDrop are the caller's explicit
// instruction and not a strategy's choice, so every kind honours them
// identically: what the kinds differ about is what happens to a record the
// caller asked to have redelivered. A dead-letter action with no destination
// is refused rather than turned into a drop, which is the rule that only
// an explicit dead-letter destination authorises a route off the topic.
func terminal(cfg Config, o mq.NackOptions) (Plan, bool, error) {
	switch o.Action {
	case mq.NackActionDeadLetter:
		if cfg.DeadLetterTopic == "" {
			return Plan{}, true, mq.ErrUnsupported
		}
		return Plan{
			Op: OpDeadLetter, Topic: cfg.DeadLetterTopic,
			Delay: cfg.delay(o), Reason: o.Reason,
		}, true, nil
	case mq.NackActionDrop:
		return Plan{Op: OpDrop, Reason: o.Reason}, true, nil
	case mq.NackActionRequeue:
		return Plan{}, false, nil
	default:
		return Plan{}, true, ErrInvalidAction
	}
}

// failureRoute is ADR-011's failure policy as a plan: dead-letter when an
// explicit destination exists, and halt when it does not.
//
// The halt is the part that matters. A record whose attempts are exhausted on
// a topic with no dead-letter destination has nowhere to go, and the two
// alternatives — dropping it, or redelivering it for ever — are silent data
// loss and a silent livelock. The specification chooses the outage.
func failureRoute(cfg Config, o mq.NackOptions, reason string) Plan {
	if cfg.DeadLetterTopic != "" {
		return Plan{
			Op: OpDeadLetter, Topic: cfg.DeadLetterTopic,
			Delay: cfg.delay(o), Reason: reason,
		}
	}
	return Plan{Op: OpStop, Reason: reason}
}

// RetryTopicName is the retry topic of attempt n for a base topic: the
// `<topic>.retry.<n>` of the plan's row.
//
// n is one-based, so the first republication goes to `<topic>.retry.1`. A
// non-positive n returns the base topic, which is the identity a caller gets
// for "no retry yet".
func RetryTopicName(base string, n int) string {
	if n <= 0 {
		return base
	}
	return base + retryInfix + strconv.Itoa(n)
}

// retryInfix separates a base topic from its retry level.
const retryInfix = ".retry."

// ParseRetryTopic splits a retry topic into the base topic and the retry
// level, and reports whether the name was one.
//
// It is the inverse of [RetryTopicName] for every name that function produces,
// and it is deliberately strict about everything else: a topic called
// "orders.retry.01" or "orders.retry.-1" or "orders.retry.9999999999999999999"
// is NOT a retry topic, because reading it as one would make the driver
// republish to a level it never created. FuzzNackRetryTopic is the round trip.
func ParseRetryTopic(topic string) (base string, n int, ok bool) {
	i := strings.LastIndex(topic, retryInfix)
	if i <= 0 {
		return topic, 0, false
	}
	digits := topic[i+len(retryInfix):]
	if digits == "" || len(digits) > 9 || (len(digits) > 1 && digits[0] == '0') {
		return topic, 0, false
	}
	for j := range len(digits) {
		if digits[j] < '0' || digits[j] > '9' {
			return topic, 0, false
		}
	}
	v, err := strconv.Atoi(digits)
	if err != nil || v <= 0 {
		return topic, 0, false
	}
	return topic[:i], v, true
}

// validTopicName refuses a configured topic that cannot be a topic: empty, or
// carrying a byte no broker accepts in a name. It is deliberately minimal —
// the portable validator is the core's and a broker's own rules are the
// driver's — and exists so that a dead-letter destination with a newline in it
// is a construction error rather than a produce request nobody can read.
func validTopicName(t string) bool {
	if t == "" || len(t) > 249 {
		return false
	}
	for i := range len(t) {
		if c := t[i]; c <= ' ' || c >= 0x7f {
			return false
		}
	}
	return true
}

// The strategy refusals. The specification names no sentinel for these, and errors.Is
// matches by code, so a caller compares mq.CodeOf(err).
var (
	// ErrInvalidKind reports a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq: the nack strategy is not one of the four kinds")
	// ErrInvalidConfig reports a Config that Validate rejects.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig,
		"iqmq: invalid nack strategy configuration")
	// ErrNoDecideFunc reports a Custom strategy with no decision function.
	// It is refused rather than defaulted: a strategy that changes ordering
	// or durability is never selected silently.
	ErrNoDecideFunc = errcode.New(errcode.InvalidConfig,
		"iqmq: the custom nack strategy needs Config.Decide; no other kind is substituted for it")
	// ErrInvalidPlan reports a plan whose fields contradict each other,
	// which only a Custom strategy can produce.
	ErrInvalidPlan = errcode.New(errcode.InvalidArgument,
		"iqmq: the nack strategy returned a plan that commits an offset it also asks to have redelivered")
	// ErrNoTopic reports a delivery with no topic handed to a strategy that
	// has to build a destination from it.
	ErrNoTopic = errcode.New(errcode.InvalidArgument,
		"iqmq: the delivery names no topic, so a retry topic cannot be derived from it")
	// ErrInvalidAction reports a nack action outside the three.
	ErrInvalidAction = errcode.New(errcode.InvalidArgument,
		"iqmq: the negative settlement names no action of the message contract")
)
