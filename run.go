package iqmq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/leasectx"
)

// DefaultDrainTimeout is how long a shutting-down runner waits for the
// handlers already in flight before it cancels their context.
const DefaultDrainTimeout = 30 * time.Second

// FailurePolicy is what becomes of a message whose attempts are exhausted
//
// The zero value is FailureDefault, which is ADR-011's provisional safe
// default: dead-letter when an explicit dead-letter topic was configured, and
// otherwise halt. Neither branch drops a message, and FailureSkip — the one
// that does — is never reached without a caller asking for it by name.
type FailurePolicy uint8

// The failure policies.
const (
	// FailureDefault dead-letters when WithDeadLetter named a topic and
	// halts otherwise. It is the zero value and ADR-011's default.
	FailureDefault FailurePolicy = iota
	// FailureDeadLetter publishes the message to the dead-letter topic with
	// provenance headers and acks the original only after that publication
	// is confirmed. It requires WithDeadLetter.
	FailureDeadLetter
	// FailureHalt requeues the message and stops the runner, which reports
	// a *PoisonError carrying CodePoisonMessage.
	FailureHalt
	// FailureSkip acks the message without success. It destroys data and is
	// never a default: the specification makes it a thing a caller asks for by name.
	FailureSkip
	// FailureInfiniteRequeue keeps requeueing the message with the
	// configured backoff and never gives up on it.
	FailureInfiniteRequeue

	failurePolicyCount
)

var failurePolicyNames = [failurePolicyCount]string{
	"default", "deadletter", "halt", "skip", "infiniterequeue",
}

// String returns the policy's lower-case name.
func (p FailurePolicy) String() string {
	if p < failurePolicyCount {
		return failurePolicyNames[p]
	}
	return "FailurePolicy(?)"
}

// Valid reports whether p names a failure policy.
func (p FailurePolicy) Valid() bool { return p < failurePolicyCount }

// SettlementMode is who settles a delivery the runner dispatched.
type SettlementMode uint8

// The settlement modes.
const (
	// SettlementAuto acks a delivery whose handler returned nil and settles
	// a failed one by the retry and failure policies. It is the zero value.
	// A delivery the handler settled itself is left alone.
	SettlementAuto SettlementMode = iota
	// SettlementManual settles nothing: the handler owns every delivery it
	// is given, and one it does not settle stays in flight until its lease
	// expires or the consumer closes.
	SettlementManual

	settlementModeCount
)

var settlementModeNames = [settlementModeCount]string{"auto", "manual"}

// String returns the mode's lower-case name.
func (m SettlementMode) String() string {
	if m < settlementModeCount {
		return settlementModeNames[m]
	}
	return "SettlementMode(?)"
}

// Valid reports whether m names a settlement mode.
func (m SettlementMode) Valid() bool { return m < settlementModeCount }

// runField names the setting a RunOption carries. The zero value names none,
// so a stray RunOption{} is refused rather than silently doing nothing.
type runField uint8

const (
	runFieldNone runField = iota
	runFieldConcurrency
	runFieldMaxInFlight
	runFieldMaxAttempts
	runFieldBackoff
	runFieldDeadLetter
	runFieldFailurePolicy
	runFieldSettlement
	runFieldMiddleware
	runFieldRecovery
	runFieldOrdering
	runFieldOrderingMode
	runFieldHandlerTimeout
	runFieldLeaseExtension
	runFieldDrainTimeout
	runFieldDeadLetterConfirmation
	runFieldNativeDeadLetter
	runFieldLeaseDeadline

	runFieldCount
)

// RunOption is one setting of a Run.
//
// It is a value type and not a closure, which is the winner (ADR-006,
// Value): the option carries no captured environment, so a Run's option list
// is built on the stack. WithMiddleware is the exception and says so — a list
// of middleware is a slice however it is spelled.
//
// The zero RunOption names no setting and is reported as CodeInvalidConfig
// rather than applied.
type RunOption struct {
	field   runField
	num     int64
	str     string
	backoff Backoff
	mw      []Middleware
}

// WithConcurrency sets how many handler goroutines run over the consumer
//
// The default is 1, which is ADR-011's "one serial worker per
// assigned partition/lane, and 1 for unpartitioned sources". Deliveries that
// share a key are routed to one lane whatever the concurrency, so raising it
// does not reorder one key's messages (lanes.go).
//
// n must be between 1 and MaxConcurrency.
func WithConcurrency(n int) RunOption {
	return RunOption{field: runFieldConcurrency, num: int64(n)}
}

// WithMaxInFlight bounds how many deliveries the runner holds unsettled
// Zero means ConsumerConfig.MaxInFlight, which itself follows the
// prefetch.
//
// It is an exact bound and not a target: the runner takes an admission slot
// before it fetches and returns it when the delivery is settled, so it never
// holds more than n deliveries at once.
func WithMaxInFlight(n int) RunOption {
	return RunOption{field: runFieldMaxInFlight, num: int64(n)}
}

// WithMaxAttempts sets how many times a message may be delivered before the
// failure policy decides what becomes of it. The default is
// DefaultMaxAttempts.
//
// It counts the broker's own attempt, which is what Delivery.Attempt reports.
// A broker that does not count attempts reports zero, and this runner then
// cannot bound them: such a message is retried until it succeeds or the
// handler names a directive.
func WithMaxAttempts(n int) RunOption {
	return RunOption{field: runFieldMaxAttempts, num: int64(n)}
}

// WithRetryBackoff sets how long a failed message waits before it is
// delivered again. The default is ADR-011's bounded exponential
// backoff with full jitter.
//
// A nil Backoff is refused rather than treated as "no backoff", which is
// ConstantBackoff(0).
func WithRetryBackoff(b Backoff) RunOption {
	return RunOption{field: runFieldBackoff, backoff: b}
}

// WithDeadLetter names the topic a message is published to when its attempts
// are exhausted, and makes FailureDeadLetter the default policy.
//
// The topic is explicit and is never created by the runner: ADR-011's "never
// auto-create a production DLQ". The publisher is opened when Run starts, so
// a topic that does not exist fails the Run rather than the first poison
// message.
func WithDeadLetter(topic string) RunOption {
	return RunOption{field: runFieldDeadLetter, str: topic}
}

// WithFailurePolicy sets what becomes of a message whose attempts are
// exhausted. FailureDeadLetter requires WithDeadLetter.
func WithFailurePolicy(p FailurePolicy) RunOption {
	return RunOption{field: runFieldFailurePolicy, num: int64(p)}
}

// WithSettlement chooses who settles a delivery.
func WithSettlement(m SettlementMode) RunOption {
	return RunOption{field: runFieldSettlement, num: int64(m)}
}

// WithMiddleware wraps the handler, outermost first.
//
// The chain is composed once, when Run starts, and never per message (
// rule 6). A nil middleware in the list is skipped.
//
// It is the one RunOption that allocates at the call site: a list of
// middleware is a slice however it is spelled, and composing it is what
// happens once instead of per message.
func WithMiddleware(mw ...Middleware) RunOption {
	return RunOption{field: runFieldMiddleware, mw: mw}
}

// WithoutRecovery turns off the runner's panic recovery (ADR-011's).
//
// With recovery on — the default — a handler that panics has its panic turned
// into a *PanicError, the delivery takes the configured failure policy, and
// the runner keeps going. With it off the panic propagates out of the worker
// goroutine, which nothing above it can recover: it terminates the program.
// That is what opting out of recovery means, and it is the only behaviour a
// caller who wants a panic to be fatal can rely on.
func WithoutRecovery() RunOption {
	return RunOption{field: runFieldRecovery, num: 0}
}

// WithHandlerTimeout bounds how long one handler may run.
//
// It OVERRIDES the lease-derived deadline rather than narrowing it further,
// which is the word: a caller who says "a handler gets two seconds" has
// said what the handler gets, and a lease that happens to be shorter would
// otherwise make the option mean something else on every driver.
//
// A handler whose context is done is not interrupted — nothing in Go can
// interrupt a goroutine — so the deadline is a thing a handler has to read.
// One that ignores it runs to completion and its settlement is refused if the
// broker took the delivery back in the meantime.
//
// d must be positive. The default is no bound of its own: the handler's
// context ends with the delivery's lease, and with Run's own context.
func WithHandlerTimeout(d time.Duration) RunOption {
	return RunOption{field: runFieldHandlerTimeout, num: int64(d)}
}

// WithLeaseDeadline ends the handler's context with the delivery's lease
// (the specification's "handler ctx derives from Run's ctx plus the lease deadline minus
// margin"), so that a handler which overruns is cancelled rather than working
// on a message the broker has given to somebody else.
//
// It is OFF by default, and the reason is a measurement rather than a
// preference. Deriving a context per delivery is what the specification decided the
// mechanism for, and its bound kind costs one context object plus the
// parent's cancellation watch — five allocations and about 1.2 µs a message
// on the run stored with this increment. The specification budgets the core at two
// allocations per message and none under BufferReuse, so a runner that
// narrowed by default would breach that ceiling on every driver that grants a
// lease, which is every real one. What a caller gets by default is Run's own
// context, which costs nothing.
//
// A delivery whose driver reports no lease expiry has no deadline to take, so
// this option costs it nothing either. WithHandlerTimeout overrides it, and a
// runner whose lease scheduler is keeping the lease alive ignores it — there
// is nothing to be cancelled at a deadline the scheduler has just moved.
func WithLeaseDeadline() RunOption {
	return RunOption{field: runFieldLeaseDeadline, num: 1}
}

// WithLeaseExtension turns on the shared lease-extension scheduler: while
// a handler holds a delivery, the runner extends its lease lead before it
// would expire.
//
// There is ONE scheduler and ONE timer for the whole runner, never a timer
// per delivery (rule 3). A delivery whose driver reports no lease
// expiry is not tracked at all, because there is no deadline to race.
//
// It is off by default, and it is refused on a driver whose settler is not a
// DeadlineExtender: a scheduler that silently extended nothing would be
// the forbidden silent emulation, and the runner would look as though it
// were keeping leases alive when it was not.
//
// lead must be positive and is subtracted from the lease deadline, so it must
// be long enough for one round trip to the broker.
//
// The lease itself may be shorter than lead — a visibility timeout is a
// property of the queue and a driver may report none at all — and lead then
// cannot be honoured: no extension can buy more notice than the lease it
// renews. The runner extends more OFTEN instead of asking for more time, with
// a floor of half the lease between two extensions, so the deadline is never
// less than half a lease away. Asking for more than the lease is what the
// three drivers with a visibility ceiling refuse outright, and scheduling the
// next extension at a lead that the lease cannot cover is what made the
// scheduler fire the same delivery for ever.
func WithLeaseExtension(lead time.Duration) RunOption {
	return RunOption{field: runFieldLeaseExtension, num: int64(lead)}
}

// WithDrainTimeout bounds how long a shutting-down runner waits for the
// handlers already in flight. The default is
// DefaultDrainTimeout.
//
// When it expires the handlers' contexts are cancelled — which is the only
// lever anything has over a running goroutine — and the runner then still
// waits for them, because an abandoned handler is an unsettled delivery the
// caller was told nothing about. A handler that ignores its context therefore
// still delays the shutdown; one that reads it stops.
//
// d must be positive.
func WithDrainTimeout(d time.Duration) RunOption {
	return RunOption{field: runFieldDrainTimeout, num: int64(d)}
}

// WithDeadLetterConfirmation sets how strong a confirmation the dead-letter
// publication waits for before the original is acked (ADR-011).
func WithDeadLetterConfirmation(m ConfirmationMode) RunOption {
	return RunOption{field: runFieldDeadLetterConfirmation, num: int64(m)}
}

// WithNativeDeadLetter routes a dead-lettered message through the DRIVER's
// own dead-letter destination rather than through a publication.
//
// ADR-011 keys dead-lettering on an explicit topic, and that is still the
// default: a dead-letter destination the caller did not name is a place
// messages go that the caller has not seen, so the runner does not use one
// unless asked. This is the asking. A driver that advertises FeatureDeadLetter
// knows where its dead letters go — a Kafka DLQ topic, an SQS redrive policy,
// a RabbitMQ dead-letter exchange — and settling with Nack(NackDeadLetter())
// gets the message there in one native settlement, with no publish to
// confirm and no window in which the message exists in both places.
//
// It cannot be combined with WithDeadLetter: two dead-letter destinations is
// a question with one answer. It is refused on a driver that advertises no
// dead-letter route, because emulating one is exactly what the specification forbids
// doing in silence.
func WithNativeDeadLetter() RunOption {
	return RunOption{field: runFieldNativeDeadLetter, num: 1}
}

// runConfig is a resolved option set: every default filled in, so that
// nothing below this point asks what a zero means.
type runConfig struct {
	concurrency int
	maxInFlight int
	maxAttempts int
	backoff     Backoff
	deadLetter  string
	policy      FailurePolicy
	settlement  SettlementMode
	mw          []Middleware
	recover     bool

	ordering       Ordering
	orderMode      OrderingMode
	handlerTimeout time.Duration
	leaseLead      time.Duration
	drainTimeout   time.Duration
	dlqConfirm     ConfirmationMode
	nativeDLQ      bool
	leaseDeadline  bool
}

// resolveRun folds the options over the consumer's own configuration and
// validates the result.
func resolveRun(cc ConsumerConfig, opts []RunOption) (runConfig, error) {
	cfg := runConfig{
		concurrency: 1,
		maxInFlight: cc.MaxInFlight,
		maxAttempts: DefaultMaxAttempts,
		backoff:     defaultBackoff,
		policy:      FailureDefault,
		settlement:  SettlementAuto,
		recover:     true,
		// the routing and the retry: by key, and by nack. The specification makes
		// OrderingStrict the default "when ordering is enabled", and
		// WithOrderingByKey is how a caller enables it.
		ordering:     OrderingByKey,
		orderMode:    OrderingRelaxed,
		drainTimeout: DefaultDrainTimeout,
	}
	for _, o := range opts {
		switch o.field {
		case runFieldConcurrency:
			cfg.concurrency = int(o.num)
		case runFieldMaxInFlight:
			// Zero is the option's documented fallback to
			// ConsumerConfig.MaxInFlight, which resolveRun started from.
			if o.num != 0 {
				cfg.maxInFlight = int(o.num)
			}
		case runFieldMaxAttempts:
			cfg.maxAttempts = int(o.num)
		case runFieldBackoff:
			if o.backoff == nil {
				return cfg, errRunNilBackoff
			}
			cfg.backoff = compileBackoff(o.backoff)
		case runFieldDeadLetter:
			// An option that names no topic is a mistake and not a way to
			// spell "no dead-letter destination", which is the option not
			// being there at all.
			if o.str == "" {
				return cfg, errRunNoDeadLetterTopic
			}
			cfg.deadLetter = o.str
		case runFieldFailurePolicy:
			cfg.policy = FailurePolicy(o.num)
		case runFieldSettlement:
			cfg.settlement = SettlementMode(o.num)
		case runFieldMiddleware:
			cfg.mw = o.mw
		case runFieldRecovery:
			cfg.recover = o.num != 0
		case runFieldOrdering:
			cfg.ordering = Ordering(o.num)
		case runFieldOrderingMode:
			if o.num < 0 {
				return cfg, errRunOrderingModes
			}
			cfg.ordering, cfg.orderMode = OrderingByKey, OrderingMode(o.num)
		case runFieldHandlerTimeout:
			// Zero is how the option is spelled absent, so naming it is a
			// mistake rather than a way to say "no bound".
			if o.num <= 0 {
				return cfg, errRunHandlerTimeout
			}
			cfg.handlerTimeout = time.Duration(o.num)
		case runFieldLeaseExtension:
			if o.num <= 0 {
				return cfg, errRunLeaseExtension
			}
			cfg.leaseLead = time.Duration(o.num)
		case runFieldDrainTimeout:
			if o.num <= 0 {
				return cfg, errRunDrainTimeout
			}
			cfg.drainTimeout = time.Duration(o.num)
		case runFieldDeadLetterConfirmation:
			cfg.dlqConfirm = ConfirmationMode(o.num)
		case runFieldNativeDeadLetter:
			cfg.nativeDLQ = true
		case runFieldLeaseDeadline:
			cfg.leaseDeadline = true
		case runFieldNone, runFieldCount:
			return cfg, errNoRunOption
		default:
			return cfg, errNoRunOption
		}
	}
	switch {
	case cfg.concurrency < 1 || cfg.concurrency > MaxConcurrency:
		return cfg, errRunConcurrency
	case cfg.maxInFlight < 1 || cfg.maxInFlight > MaxPrefetch:
		return cfg, errRunMaxInFlight
	case cfg.maxAttempts < 1:
		return cfg, errRunMaxAttempts
	case !cfg.policy.Valid():
		return cfg, errRunFailurePolicy
	case !cfg.settlement.Valid():
		return cfg, errRunSettlement
	case !cfg.ordering.Valid():
		return cfg, errRunOrdering
	case !cfg.orderMode.Valid():
		return cfg, errRunOrderingMode
	case !cfg.dlqConfirm.Valid():
		return cfg, errRunDeadLetterConfirmation
	}
	if cfg.deadLetter != "" {
		if cfg.nativeDLQ {
			return cfg, errRunTwoDeadLetters
		}
		if err := ValidateTopic(cfg.deadLetter); err != nil {
			return cfg, err
		}
	}
	if cfg.dlqConfirm != ConfirmDefault && cfg.deadLetter == "" {
		// A confirmation boundary for a publication the runner will not make.
		return cfg, errRunDeadLetterConfirmationUnused
	}
	if cfg.policy == FailureDefault {
		// ADR-011: "if an explicit DLQ is configured the message is
		// dead-lettered, otherwise the consumer halts". Never skip. A
		// caller who asked for the driver's own route has configured one
		// just as explicitly as a topic.
		cfg.policy = FailureHalt
		if cfg.deadLetter != "" || cfg.nativeDLQ {
			cfg.policy = FailureDeadLetter
		}
	}
	if cfg.policy == FailureDeadLetter && cfg.deadLetter == "" && !cfg.nativeDLQ {
		// A policy that names a destination the caller did not configure is
		// a configuration error and not a silent downgrade to halt: the
		// downgrade would look like dead-lettering until the first poison
		// message.
		return cfg, errRunNoDeadLetter
	}
	return cfg, nil
}

// compileBackoff turns one of this package's own backoff values into its
// compiled form, so that a delay costs no allocation on the delivery's path.
// A caller's own Backoff is used as it is.
func compileBackoff(b Backoff) Backoff {
	if e, ok := b.(ExponentialBackoff); ok {
		return e.compile()
	}
	return b
}

// PanicError is a handler panic, recovered by the runner (ADR-011's
// ).
//
// The delivery then takes the configured failure policy exactly as a returned
// error would, which is the specification's "treated as retryable failure", and the
// runner keeps running. No panic may silently ack, so a *PanicError is never
// the nil the auto settlement acks on.
type PanicError struct {
	// Value is what the handler panicked with.
	Value any
	// Stack is the stack of the panicking goroutine, captured where the
	// panic was recovered.
	Stack []byte
}

// Error renders the panic value. The stack is deliberately not part of it: an
// error string goes into logs and metrics labels, and a stack is what the
// field is for.
func (e *PanicError) Error() string {
	switch v := e.Value.(type) {
	case string:
		return "iqmq: handler panic: " + v
	case error:
		return "iqmq: handler panic: " + v.Error()
	case nil:
		return "iqmq: handler panic: nil"
	default:
		return "iqmq: handler panic: " + fmt.Sprint(v)
	}
}

// Unwrap returns the panic value when it is an error, so that errors.Is and
// errors.As reach a handler that panicked with one.
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// PoisonError is the delivery that stopped the runner (ADR-011).
//
// A handler error whose attempts are exhausted and which has no usable
// dead-letter destination halts the consumer rather than dropping the message:
// "never skip by default". Halting has to be observable, so Run returns this,
// and it names the delivery as well as the failure.
//
// errors.Is(err, ErrPoisonMessage) is true, CodeOf reports CodePoisonMessage,
// and the handler's own error is reachable through errors.Is and errors.As —
// all three because Unwrap returns both.
type PoisonError struct {
	// Topic is the delivery's resolved topic.
	Topic string
	// Subscription is the subscription it came from.
	Subscription string
	// MessageID is the message's application identifier, empty when it has
	// none. The body and the headers are deliberately not here: the specification keeps
	// payloads out of errors.
	MessageID string
	// Attempt is the delivery attempt that failed.
	Attempt int
	// Err is what the handler returned or panicked with.
	Err error
}

// Error renders the failure without the payload.
func (e *PoisonError) Error() string {
	msg := "iqmq: poison message"
	if e.Topic != "" {
		msg += " topic=" + e.Topic
	}
	if e.Subscription != "" {
		msg += " subscription=" + e.Subscription
	}
	if e.MessageID != "" {
		msg += " id=" + e.MessageID
	}
	msg += " attempt=" + strconv.Itoa(e.Attempt)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns ErrPoisonMessage and the handler's own error, so that
// errors.Is finds both and CodeOf reports CodePoisonMessage.
func (e *PoisonError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrPoisonMessage}
	}
	return []error{ErrPoisonMessage, e.Err}
}

// runItem is one delivery in flight through the runner: the delivery itself
// and the admission slot it occupies.
//
// The runner allocates exactly MaxInFlight of them when Run starts and
// recycles them for ever, so a steady-state run allocates nothing per
// message. The slot is what bounds the in-flight set: an item is taken from
// the free list before the fetch that fills it and returned after the
// delivery is settled.
type runItem struct {
	d Delivery
	// attempts is how many times THIS runner has re-invoked the handler on
	// this delivery without giving it back to the broker, which is what a
	// strictly ordered retry does. It is zero for every delivery the runner
	// retried by nack, and it is what lets WithMaxAttempts bound a broker
	// that counts no attempts of its own ('s first half).
	attempts int
	// lease is the scheduler entry for this delivery. It lives here, in a
	// slot allocated once per Run, so that tracking a delivery's lease costs
	// no allocation (the specification's "never one timer per delivery").
	lease leaseTicket
	// group is the deliveries a RunBatch dispatch handed to one worker
	// together, and ds is the same set as the slice the BatchHandler is
	// given. Both belong to the group's LEADER — the first item of the
	// group, which is what the lane pool carries — and both are grown by
	// append rather than preallocated, because preallocating a fetch buffer
	// per admission slot is megabytes for a runner that may never batch.
	group []*runItem
	ds    []*Delivery
	// hold is the timer a strictly ordered retry waits on. It is created the
	// first time this slot holds a failed delivery and reset afterwards, so
	// a runner arms at most one timer per admission slot and none at all
	// until something fails (rule 3).
	hold clock.Timer
}

// runner is one Run: the fetch loop, the lanes, the retry and failure
// policies and the dead-letter publisher.
type runner struct {
	c   *ConsumerHandle
	h   Handler
	bh  BatchHandler
	cfg runConfig

	runCtx context.Context
	// stopHandlers cancels runCtx. It is what WithDrainTimeout spends when
	// the drain runs out of patience: the specification drains in-flight handlers rather
	// than interrupting them, and cancelling their context is the only lever
	// anything has once the patience is gone.
	stopHandlers context.CancelFunc
	// fetchCtx is Run's context plus the halt: cancelling it is what
	// unblocks a fetch parked in a driver when a lane decides the runner
	// must stop. A handler keeps runCtx, which Run's context does not
	// cancel, because the specification drains in-flight handlers rather
	// than cancelling them.
	fetchCtx  context.Context
	stopFetch context.CancelFunc

	lanes *lanes
	dlq   *PublisherHandle
	// deadlines derives the handler's context from the lease deadline and
	// from WithHandlerTimeout. It is nil when neither
	// narrows anything, which is what keeps a delivery from paying for a
	// deadline nobody asked for.
	deadlines leasectx.Source
	// leases is the shared lease-extension scheduler, nil when
	// WithLeaseExtension is off.
	leases *leaseScheduler
	// dlqCtx bounds every dead-letter publication. It is the runner's own
	// and NOT Run's, for the reason ConsumerHandle.settleCtx is not the
	// caller's: a handler draining after Run's context is done may still
	// have to dead-letter its delivery, and the specification makes that publication the
	// thing that decides whether the message survives. It is cancelled when
	// the lanes have drained, which is the last moment a publication can be
	// made, or earlier when the drain timeout passes.
	dlqCtx  context.Context
	stopDLQ context.CancelFunc
	// delayed reports that the driver can requeue a message with a delay,
	// read once at Run (rule 4).
	delayed bool

	// buf is the fetch buffer and held the admission slots that back it,
	// both allocated once. Only the fetch goroutine touches either.
	buf  []Delivery
	held []*runItem
	free *runFree

	haltOnce sync.Once
	haltErr  error
	halted   chan struct{}
	// drained is closed once the fetch loop has ended and every handler has
	// finished. It is what Draining phase waits on.
	drained chan struct{}
}

// Run consumes deliveries and dispatches them to h until ctx is done, the
// consumer closes, or the runner halts.
//
// This is the loop one bounded receive loop, an admission slot per
// in-flight delivery, lane routing by key, a fixed worker pool, the
// middleware chain composed once, the handler, and then the settlement the
// handler's result asks for. There is no goroutine and no timer per delivery.
//
// The handler's result decides the delivery. Nil acks it; an error
// nacks it for redelivery after the configured backoff, until the attempts
// are exhausted and the failure policy decides; the control errors —
// RetryAfter, DeadLetter, Discard and Halt — override that. A handler that
// settled the delivery itself is not overruled.
//
// Exactly one Run may be active on a consumer. A second concurrent call is
// refused with an error carrying CodeConflict rather than being allowed to
// race the first one's fetches and share its in-flight bound; a call after
// the first has returned is a fresh runner and is allowed, which is how a
// caller restarts after a halt.
//
// Run returns:
//
//   - the halt: a *PoisonError when a message exhausted its attempts with no
//     usable dead-letter destination, or the handler's own error when the
//     handler returned Halt(err). Either way the message is requeued first,
//     so halting loses nothing.
//   - ctx.Err() when ctx is done, after the in-flight handlers have drained
//   - ErrClosed when the consumer closed.
//
// In every case the lanes are drained before Run returns: an accepted
// delivery is always handled and settled, never abandoned.
func (c *ConsumerHandle) Run(ctx context.Context, h Handler, opts ...RunOption) error {
	if h == nil {
		return errRunNoHandler
	}
	cfg, err := resolveRun(c.cfg, opts)
	if err != nil {
		return err
	}
	// The refusal lives here, on the handle, and not in Receive: two runners
	// on one consumer is a configuration mistake the API can name, where a
	// second fetch loop discovered at run time is a fairness bug a caller
	// would have to infer from throughput. Receive itself stays serialised
	// rather than refused, because the specification allows a caller to drive it from one
	// goroutine at a time.
	if !c.running.CompareAndSwap(false, true) {
		return errRunAlreadyRunning
	}
	defer c.running.Store(false)
	if c.closed() {
		return ErrClosed
	}
	r, err := newRunner(ctx, c, h, nil, cfg)
	if err != nil {
		return err
	}
	c.run.Store(r)
	defer c.run.Store(nil)
	return r.run(ctx)
}

// newRunner builds the runner and everything it needs for the whole Run: the
// admission slots, the fetch buffer, the composed handler, the lanes and the
// dead-letter publisher.
func newRunner(ctx context.Context, c *ConsumerHandle, h Handler, bh BatchHandler, cfg runConfig) (*runner, error) {
	if len(cfg.mw) > 0 && h != nil {
		// The specification rule 6: composed once, here, and never per message.
		h = Chain(cfg.mw...)(h)
	}
	if cfg.nativeDLQ && !c.client.caps.Has(FeatureDeadLetter) {
		// The specification: no silent emulation. A runner told to use the driver's own
		// route on a driver that has none would halt on the first poison
		// message instead of dead-lettering it, which the caller would
		// discover at the worst possible moment.
		return nil, errRunNoNativeDeadLetter
	}
	if cfg.leaseLead > 0 && c.extender == nil {
		// The specification: a capability the driver does not have is refused, never
		// emulated in silence. A scheduler that extended nothing would leave
		// the runner looking as though it were keeping leases alive.
		return nil, errRunNoLeaseExtension
	}
	r := &runner{
		c:       c,
		h:       h,
		bh:      bh,
		cfg:     cfg,
		delayed: c.client.caps.Has(FeatureDelayedRedelivery),
		halted:  make(chan struct{}),
		drained: make(chan struct{}),
	}
	// The cancel is stopHandlers; shutdown calls it on every path out of run,
	// and newRunner calls shutdown itself on the one path that fails here.
	// Run's context ending stops the fetch and not the handlers: they keep
	// its values and drain until WithDrainTimeout runs out.
	r.runCtx, r.stopHandlers = context.WithCancel(context.WithoutCancel(ctx))
	if cfg.leaseLead > 0 {
		fallback := c.cfg.AckDeadline
		if fallback <= 0 {
			fallback = DefaultLeaseExtension
		}
		leases, err := newLeaseScheduler(c.clock, cfg.leaseLead, fallback)
		if err != nil {
			//nolint:contextcheck // shutdown takes no context on purpose: nothing is admitted yet
			r.shutdown()
			return nil, err
		}
		r.leases = leases
	}
	if cfg.handlerTimeout > 0 || r.narrowsToLease() {
		deadlines, err := leasectx.New(leasectx.Auto, leasectx.Config{Clock: c.clock})
		if err != nil {
			//nolint:contextcheck // shutdown takes no context on purpose: nothing is admitted yet
			r.shutdown()
			return nil, fmt.Errorf("iqmq: handler deadlines: %w", err)
		}
		r.deadlines = deadlines
	}
	if cfg.deadLetter != "" {
		// Opened once, and now rather than at the first failure: a
		// dead-letter topic that does not exist is a configuration error,
		// and discovering it while holding a poison message is the worst
		// moment to discover it.
		p, err := c.client.Publisher(ctx, PublisherConfig{
			Topic:        cfg.deadLetter,
			Confirmation: cfg.dlqConfirm,
		})
		if err != nil {
			//nolint:contextcheck // shutdown takes no context on purpose: nothing is admitted yet, and the context that opened the publisher is the one that just failed
			r.shutdown()
			return nil, err
		}
		r.dlq = p
		// The specification: the dead-letter publication outlives Run's context on
		// purpose ; stopDLQ is called by shutdown once
		// the lanes have drained.
		r.dlqCtx, r.stopDLQ = context.WithCancel(context.Background())
	}
	n := cfg.maxInFlight
	r.free = newRunFree(n)
	batch := min(n, c.cfg.Prefetch)
	r.buf = make([]Delivery, batch)
	r.held = make([]*runItem, batch)
	r.fetchCtx, r.stopFetch = context.WithCancel(ctx)
	lane := r.handle
	if bh != nil {
		lane = r.handleGroup
	}
	lanes, err := newLanes(cfg.concurrency, n, cfg.ordering, lane)
	if err != nil {
		//nolint:contextcheck // shutdown takes no context on purpose: nothing is admitted yet
		r.shutdown()
		return nil, err
	}
	r.lanes = lanes
	return r, nil
}

// narrowsToLease reports whether the handler's context ends with the
// delivery's lease.
//
// It does when the caller asked for it — WithLeaseDeadline, which is off by
// default because the derivation breaches the allocation budget
//
//	— and when the runner is not keeping that lease alive
//
// itself: a runner with the lease scheduler on has told the broker it still
// wants the delivery, so cancelling the handler at the deadline the scheduler
// has just moved would be a bound on nothing.
func (r *runner) narrowsToLease() bool { return r.cfg.leaseDeadline && r.leases == nil }

// run is the fetch loop plus the drain. The loop's error is what the fetch
// reported; result decides what Run answers with once everything has drained,
// because a handler draining after the loop may still halt.
func (r *runner) run(ctx context.Context) error {
	defer close(r.drained)
	err := r.loop()
	//nolint:contextcheck // shutdown takes no context on purpose: the drain is the lane pool, and the handles it closes have nothing admitted left to flush (see shutdown)
	r.shutdown()
	return r.result(ctx, err)
}

// loop is the one bounded receive loop.
func (r *runner) loop() error {
	for {
		select {
		case <-r.fetchCtx.Done():
			return r.fetchCtx.Err()
		default:
		}
		n, err := r.acquire()
		if err != nil {
			return err
		}
		got, ferr := r.c.Receive(r.fetchCtx, r.buf[:n])
		r.recycleRange(got, n)
		var serr error
		if r.bh != nil {
			serr = r.dispatchGroups(got)
		} else {
			serr = r.dispatch(got)
		}
		if serr != nil {
			return serr
		}
		if ferr != nil {
			return ferr
		}
	}
}

// acquire takes admission slots for the next fetch: one, blocking, and then
// as many more as are free without blocking, up to the fetch buffer. The
// blocking half is the in-flight bound — a runner holding MaxInFlight
// unsettled deliveries does not fetch again until one is settled.
func (r *runner) acquire() (int, error) {
	return r.free.take(r.fetchCtx, r.held)
}

// dispatch submits the first got deliveries to their lanes. An item the
// submission could not place goes back to the broker with every one fetched
// after it, before its slot is recycled: the consumer outlives a stopped
// fetch (ConsumerHandle.Drain, a cancelled Run, a halt), so nothing else would
// ever settle them, and they would hold InFlight and the broker's credit
// until a lease expired — or for ever, on a broker with none.
func (r *runner) dispatch(got int) error {
	for i := range got {
		it := r.take(i)
		if err := r.lanes.submit(r.fetchCtx, it); err != nil {
			r.giveBack(it)
			for j := i + 1; j < got; j++ {
				r.giveBack(r.take(j))
			}
			return err
		}
	}
	return nil
}

// take moves the fetched delivery at i into its admission slot.
//
// The fetch buffer must not keep a settlement handle the runner has handed
// on: under ConsumerConfig.BufferReuse a stale copy would name a record a
// later fetch has recycled, and in every mode it would pin a settled message's
// body until the next fetch overwrote it.
func (r *runner) take(i int) *runItem {
	it := r.held[i]
	r.held[i] = nil
	it.d = r.buf[i]
	r.buf[i] = Delivery{}
	return it
}

// giveBack requeues a delivery no handler was given and recycles its slot. It
// requeues under SettlementManual too, because the caller who owns the
// settlement in that mode never received the delivery.
func (r *runner) giveBack(it *runItem) {
	r.requeue(&it.d, 0)
	r.recycle(it)
}

// recycleRange returns the slots a short fetch did not fill.
func (r *runner) recycleRange(from, to int) {
	for i := from; i < to; i++ {
		r.recycle(r.held[i])
		r.held[i] = nil
	}
}

// recycle returns an admission slot to the free list, clearing the delivery
// it held: the slot outlives the delivery and must not keep the message's
// storage, or its settlement handle, alive with it.
func (r *runner) recycle(it *runItem) {
	if it == nil {
		return
	}
	it.d = Delivery{}
	it.attempts = 0
	r.free.put(it)
}

// handle runs one delivery on a worker goroutine: the handler, then the
// settlement its result asks for, then the slot back.
//
// The loop is StrictOrdering: a failed delivery that is to be retried
// in order is held here — on the lane's own worker, so that the deliveries
// behind it on that lane wait — and the handler is called again. Every other
// mode settles once and leaves.
func (r *runner) handle(it *runItem) {
	defer r.recycle(it)
	d := &it.d
	if r.stopped(d) {
		return
	}
	if r.leases != nil {
		r.leases.track(&it.lease, d)
		// Released before the slot is recycled, so that no extension is in
		// flight against a delivery whose storage is about to be reused.
		defer r.leases.release(&it.lease)
	}
	for r.dispatchOne(it, d) {
	}
}

// stopped reports that the runner has halted and gives the delivery back if
// it has.
//
// A halted runner does not start handling another delivery: the halt is the
// runner saying it has stopped, and a message it has not looked at is the
// broker's to redeliver — to this consumer when it is restarted, or to
// another one. Handling it anyway is what made a halt take one more attempt
// per lane than it asked for, and under OrderingStopOnFailure a whole second
// pass over the message that stopped it.
//
// It requeues under SettlementManual as well: the caller owns the settlement
// of what its handler is given, and this delivery never reached the handler.
func (r *runner) stopped(d *Delivery) bool {
	select {
	case <-r.halted:
	default:
		return false
	}
	r.requeue(d, 0)
	return true
}

// dispatchOne calls the handler once and settles the result. It reports
// whether the delivery is to be handled again.
func (r *runner) dispatchOne(it *runItem, d *Delivery) bool {
	ctx, release := r.handlerCtx(d)
	err := r.invoke(ctx, d)
	release()
	return r.settle(it, d, err)
}

// handlerCtx is the context one handler is given: Run's own context,
// narrowed by WithHandlerTimeout, or by the delivery's lease deadline when
// nothing else bounds it.
//
// A runner that narrows nothing hands over Run's context itself, which costs
// no allocation and no scheduler work — the case every driver without a lease
// takes, and the one the dispatch budget is measured on.
func (r *runner) handlerCtx(d *Delivery) (context.Context, func()) {
	if r.deadlines == nil {
		return r.runCtx, noRelease
	}
	if r.cfg.handlerTimeout > 0 {
		// The specification: WithHandlerTimeout OVERRIDES the lease deadline rather than
		// narrowing it further, so the option means the same thing on every
		// driver.
		ctx, rel := r.deadlines.Derive(r.runCtx, r.c.clock.Now().Add(r.cfg.handlerTimeout))
		return ctx, rel
	}
	expires := d.Info().LeaseExpires
	if expires.IsZero() {
		return r.runCtx, noRelease
	}
	ctx, rel := r.deadlines.Derive(r.runCtx, expires)
	return ctx, rel
}

// noRelease is the release of a handler context that was never derived.
func noRelease() {}

// invoke calls the handler, turning a panic into a *PanicError when recovery
// is on (ADR-011's).
//
// The recovery is here, inside the item, and not around the worker loop: a
// panic that reached the worker would take the worker goroutine and with it
// every lane that worker owns, so the sibling lanes would stop handling
// deliveries that had nothing to do with the panic. Recovering per delivery
// is what makes a panicking lane the panicking delivery's problem alone.
func (r *runner) invoke(ctx context.Context, d *Delivery) (err error) {
	if r.cfg.recover {
		defer func() {
			if v := recover(); v != nil {
				err = &PanicError{Value: v, Stack: stack()}
			}
		}()
	}
	return r.h.Handle(ctx, d)
}

// stack captures the panicking goroutine's stack. It is called from a
// deferred function while the panic is still unwinding, so the frames that
// panicked are still on it.
func stack() []byte {
	buf := make([]byte, 8192)
	return buf[:runtime.Stack(buf, false)]
}

// settle applies to one handled delivery. It reports whether the
// delivery is to be handled again, which only a strictly ordered retry asks
// for.
func (r *runner) settle(it *runItem, d *Delivery, err error) bool {
	if r.cfg.settlement == SettlementManual {
		return false
	}
	if d.Settled() {
		// The handler settled it itself and is not overruled. This
		// is a fast path and not the guard: the guard is that every
		// settlement below tolerates ErrAlreadySettled, because a handler
		// may settle on another goroutine and win the race with this check.
		return false
	}
	if err == nil {
		r.ack(d)
		return false
	}
	return r.fail(it, d, err)
}

// fail decides what a failed delivery becomes.
func (r *runner) fail(it *runItem, d *Delivery, err error) bool {
	directive, delay := DirectiveOf(err)
	return r.failAs(it, d, err, directive, delay)
}

// failAs is fail with the directive already read from err.
func (r *runner) failAs(it *runItem, d *Delivery, err error, directive Directive, delay time.Duration) bool {
	switch directive {
	case DirectiveDiscard:
		// The specification: ack without success, logged and counted, never silent. The
		// count is the consumer's acked counter and the record is this
		// branch: a discard is the handler's decision and is never the
		// runner's.
		r.ack(d)
	case DirectiveHalt:
		// The runner stops FIRST and the message goes back after it: the
		// requeue is synchronous, so halting still loses nothing, and
		// stopping the fetch before the record is redeliverable is what
		// keeps this runner from handing itself the same message again on
		// its way out.
		r.halt(err)
		r.requeue(d, 0)
	case DirectiveDeadLetter:
		// "dead-letter the message now, without exhausting its attempts". A
		// runner with no dead-letter destination cannot, and dropping the
		// message instead is the one answer the specification forbids, so it halts.
		if !r.canDeadLetter() {
			r.poison(d, err)
			return false
		}
		r.deadLetter(it, d, err)
	case DirectiveRetryAfter:
		// The directive replaces the backoff, not the attempt bound.
		return r.retry(it, d, err, delay)
	default:
		// DirectiveNone, and any directive a later version of this package
		// adds that this runner does not know about: the configured
		// backoff and bound, which is the answer that neither drops the
		// message nor invents a disposition for it.
		return r.retry(it, d, err, r.cfg.backoff.Delay(attemptOf(it, d)+1))
	}
	return false
}

// retry gives a failed message another attempt, or hands it to the failure
// policy when its attempts are gone. It reports whether the delivery is to be
// handled again on this worker.
//
// the retry interaction decides HOW the message gets its next attempt:
func (r *runner) retry(it *runItem, d *Delivery, err error, delay time.Duration) bool {
	if r.exhausted(it, d) {
		r.exhaust(it, d, err)
		return false
	}
	switch r.cfg.orderMode {
	case OrderingStopOnFailure:
		r.poison(d, err)
		return false
	case OrderingStrict:
		if !r.wait(it, delay) {
			// The runner is stopping. The message goes back rather than
			// being handled again, so a shutdown in the middle of a backoff
			// loses nothing.
			r.requeue(d, 0)
			return false
		}
		it.attempts++
		return true
	default:
		r.requeue(d, delay)
		return false
	}
}

// wait holds the lane for delay, reporting whether the wait completed.
//
// The timer belongs to the admission slot and is reset rather than recreated,
// so a runner arms at most one timer per slot however many times a delivery
// is retried, and none at all until something fails (rule 3).
func (r *runner) wait(it *runItem, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	if it.hold == nil {
		it.hold = r.c.clock.NewTimer(delay)
	} else {
		it.hold.Reset(delay)
	}
	defer it.hold.Stop()
	select {
	case <-it.hold.C():
		return true
	case <-r.fetchCtx.Done():
		return false
	case <-r.halted:
		return false
	}
}

// exhaust applies the failure policy to a message that has run out of
// attempts.
func (r *runner) exhaust(it *runItem, d *Delivery, err error) {
	switch r.cfg.policy {
	case FailureDeadLetter:
		r.deadLetter(it, d, err)
	case FailureSkip:
		// the one destructive policy. It is never reached without
		// WithFailurePolicy(FailureSkip).
		r.ack(d)
	case FailureInfiniteRequeue:
		r.requeue(d, r.cfg.backoff.Delay(attemptFloor(d)+1))
	default:
		// FailureHalt, and FailureDefault, which resolveRun has already
		// resolved into one of the others. ADR-011's "otherwise halt" is
		// also the answer for a policy value this runner does not know:
		// halting is the disposition that cannot lose the message.
		r.poison(d, err)
	}
}

// exhausted reports whether this delivery has used up its attempts.
//
// Delivery.Attempt is the broker's count and is zero for a broker that does
// not keep one, so a message this runner gave back can never be reported
// exhausted for such a broker — recorded rather than
// guessed at.
//
// A delivery this runner HELD is different: it never left the process, so the
// count is exact whatever the broker does, and it is what bounds a strictly
// ordered retry loop.
func (r *runner) exhausted(it *runItem, d *Delivery) bool {
	if it.attempts > 0 {
		return attemptOf(it, d) >= r.cfg.maxAttempts
	}
	return d.Attempt > 0 && d.Attempt >= r.cfg.maxAttempts
}

// attemptOf is the attempt the handler has just finished: the broker's count,
// floored at 1 so that a broker which does not count attempts still gets the
// first interval of the backoff rather than an immediate redelivery, plus the
// attempts this runner made without giving the message back.
func attemptOf(it *runItem, d *Delivery) int { return attemptFloor(d) + it.attempts }

// attemptFloor is the broker's attempt count, floored at 1.
func attemptFloor(d *Delivery) int {
	if d.Attempt < 1 {
		return 1
	}
	return d.Attempt
}

// poison requeues the message and halts the runner with a *PoisonError, which
// is ADR-011's "otherwise halt with CodePoisonMessage".
func (r *runner) poison(d *Delivery, err error) {
	info := d.Info()
	r.halt(&PoisonError{
		Topic:        info.Topic,
		Subscription: info.Subscription,
		MessageID:    d.ID,
		Attempt:      d.Attempt,
		Err:          err,
	})
	r.requeue(d, 0)
}

// settleRetries is how many more times the runner offers a settlement the
// driver definitely refused. A definite refusal leaves the delivery
// settleable (Settler), and the refusals worth retrying are the transient ones
// — a connection being re-established, a broker briefly unavailable — which
// two more attempts on the configured backoff cover without holding a lane
// for long.
const settleRetries = 2

// ack settles the delivery positively, tolerating the handler having done it
// first.
func (r *runner) ack(d *Delivery) {
	err := d.Ack()
	for i := 1; i <= settleRetries && refused(d, err); i++ {
		r.pause(i + 1)
		err = d.Ack()
	}
	r.settleFailed(d, err)
}

// requeue nacks the delivery for redelivery, asking for the delay natively
// where the driver has it.
//
// A driver with no native delayed redelivery is asked for an immediate one
// instead. The fallback is a capability check compiled at Run plus the
// ErrUnsupported answer as a backstop, because a driver that advertises the
// feature and then refuses the action must still not leave the delivery
// unsettled.
func (r *runner) requeue(d *Delivery, delay time.Duration) {
	err := r.nack(d, delay)
	for i := 1; i <= settleRetries && refused(d, err); i++ {
		r.pause(i + 1)
		err = r.nack(d, delay)
	}
	r.settleFailed(d, err)
}

// nack is one requeue attempt, falling back to an immediate redelivery when
// the delayed one is refused as unsupported.
func (r *runner) nack(d *Delivery, delay time.Duration) error {
	if delay > 0 && r.delayed {
		err := d.Nack(NackDelay(delay))
		if !errors.Is(err, ErrUnsupported) {
			return err
		}
	}
	return d.Nack()
}

// refused reports that a settlement failed definitely and left the delivery
// settleable: not settled by anyone, not ambiguous, and its lease not gone.
// It is the cell that answers, rather than the error's code, because a
// handler settling on another goroutine, an unknown outcome and a released
// record all leave the cell somewhere other than Active.
//
// The one exception is ErrClosed: a closed consumer refuses before the cell
// is reached, so the cell is still Active, but no retry can succeed and the
// close has already zeroed InFlight, so releasing the record as well would
// count it out twice and hand it to a dead consumer's free list.
func refused(d *Delivery, err error) bool {
	return err != nil && !errors.Is(err, ErrClosed) && d.handle != nil && d.handle.active(d.gen)
}

// pause waits out the configured backoff before a settlement is offered
// again. attempt counts the settlement's attempts from 1, as Backoff.Delay
// does, so the first retry is attempt 2 and waits the first interval. A
// stopping runner does not wait: the attempt is still made, because
// a settlement is the one thing a drain must not skip.
func (r *runner) pause(attempt int) {
	delay := r.cfg.backoff.Delay(attempt)
	if delay <= 0 {
		return
	}
	t := r.c.clock.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C():
	case <-r.fetchCtx.Done():
	case <-r.halted:
	}
}

// settleFailed gives up a delivery whose settlement was still refused after
// its retries. Leaving it live is what stranded it: the runner recycles the
// slot, nothing holds the Delivery any more, and the record kept InFlight
// raised and, under BufferReuse, out of the free list for ever. Released as
// lost, it is the broker's to redeliver when the lease or the connection
// ends. It counts nothing: the settlement already counted its own failure in
// ConsumerStats.SettleFailures.
func (r *runner) settleFailed(d *Delivery, err error) {
	if refused(d, err) {
		d.handle.abandon(d.gen)
	}
}

// canDeadLetter reports whether this runner has a dead-letter destination at
// all: a topic it publishes to, or the driver's own route.
func (r *runner) canDeadLetter() bool { return r.dlq != nil || r.cfg.nativeDLQ }

// halt records the first reason the runner stopped and unblocks the fetch.
//
// Cancelling the fetch context is what makes a halt prompt: a fetch parked in
// a driver would otherwise keep the runner alive until the next message
// arrived. The handler's context is NOT cancelled — the specification drains in-flight
// handlers rather than interrupting them.
func (r *runner) halt(err error) {
	r.haltOnce.Do(func() {
		r.haltErr = err
		close(r.halted)
		r.stopFetch()
	})
}

// shutdown is the order for a runner: stop fetching, wait for the
// handlers already in flight within the drain timeout, then close the
// dead-letter publisher and the machinery the handlers were using.
//
// The dead-letter publisher goes AFTER the drain because a handler still
// draining may need to dead-letter its delivery, and the lease scheduler and
// the deadline source go after it for the same reason: a dead-letter
// publication is made while the delivery it is about is still unsettled.
func (r *runner) shutdown() {
	if r.stopFetch != nil {
		r.stopFetch()
	}
	if r.lanes != nil {
		r.drain()
		_ = r.lanes.close()
	}
	if r.dlq != nil {
		// Every lane has drained, so no handler can still be about to
		// dead-letter anything; the publications that were in flight have
		// returned, and the context that bounded them ends here.
		r.stopDLQ()
		// Close and not Shutdown(ctx), deliberately: every dead-letter
		// publication this runner makes is synchronous and has returned
		// before the lane that made it finished draining, so there is
		// nothing admitted left to flush — and Run's own context may be the
		// cancelled one that ended the run, which would make a graceful
		// shutdown a prompt close with extra steps.
		_ = r.dlq.Close()
	}
	if r.leases != nil {
		r.leases.close()
	}
	if r.deadlines != nil {
		_ = r.deadlines.Close()
	}
	// Last: the handlers are gone, so nothing is using the context any more,
	// and vet's lostcancel is satisfied on every path out of newRunner too.
	r.stopHandlers()
}

// drain waits for the handlers already in flight, bounded by the drain
// timeout (the specification's "wait for in-flight handlers within DrainTimeout").
//
// When the patience runs out the handlers' context is cancelled — the only
// lever there is over a running goroutine — and lanes.close then still waits
// for them, because an item abandoned in a lane is an unsettled delivery
// nobody was told about. A handler that reads its context stops; one that
// does not still delays the shutdown, and says so by delaying it.
func (r *runner) drain() {
	t := r.c.clock.NewTimer(r.cfg.drainTimeout)
	defer t.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.lanes.pool.Drain(r.runCtx)
	}()
	select {
	case <-done:
	case <-t.C():
		r.stopHandlers()
		// A dead-letter publication outlives Run's context on purpose, but
		// not the drain's patience: lanes.close waits for the worker making
		// it, and stopDLQ would otherwise run only after that wait. The
		// publication fails and its delivery is requeued, which loses
		// nothing.
		if r.stopDLQ != nil {
			r.stopDLQ()
		}
	}
}

// result decides what Run answers with, once everything has drained.
//
// The halt wins over the context: a runner that halted stopped for a reason
// the caller has no other way to learn, while a cancelled context is
// something the caller did. The specification's "Run returns ctx.Err() after drain" is
// the second branch and is what an uneventful shutdown takes.
func (r *runner) result(ctx context.Context, err error) error {
	select {
	case <-r.halted:
		if r.haltErr != nil {
			return r.haltErr
		}
	default:
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if r.c.closed() {
		// the specification's "Run returns ErrClosed when the consumer closed", which is
		// also what a consumer SHUTDOWN looks like from here: the Draining
		// phase cancels the fetch, so the loop's own error is the
		// cancellation rather than the reason for it.
		return ErrClosed
	}
	return err
}

// The runner's configuration failures. The specification names no sentinel for an invalid
// configuration and errors.Is matches by code, so a caller compares
// CodeOf(err) against CodeInvalidConfig — or, for the second runner, against
// CodeConflict.
var (
	errRunNoHandler         = errcode.New(errcode.InvalidConfig, "iqmq: Run needs a handler")
	errNoRunOption          = errcode.New(errcode.InvalidConfig, "iqmq: run option names no setting")
	errRunConcurrency       = errcode.New(errcode.InvalidConfig, "iqmq: concurrency is below 1 or above MaxConcurrency")
	errRunMaxInFlight       = errcode.New(errcode.InvalidConfig, "iqmq: max in flight is below 1 or above MaxPrefetch")
	errRunMaxAttempts       = errcode.New(errcode.InvalidConfig, "iqmq: max attempts is below 1")
	errRunNilBackoff        = errcode.New(errcode.InvalidConfig, "iqmq: retry backoff is nil")
	errRunFailurePolicy     = errcode.New(errcode.InvalidConfig, "iqmq: failure policy is not a policy")
	errRunSettlement        = errcode.New(errcode.InvalidConfig, "iqmq: settlement mode is not a mode")
	errRunNoDeadLetter      = errcode.New(errcode.InvalidConfig, "iqmq: FailureDeadLetter needs WithDeadLetter")
	errRunNoDeadLetterTopic = errcode.New(errcode.InvalidConfig, "iqmq: WithDeadLetter names no topic")
	errRunAlreadyRunning    = errcode.New(errcode.Conflict, "iqmq: the consumer already has a runner")

	errRunOrdering               = errcode.New(errcode.InvalidConfig, "iqmq: ordering is not an ordering")
	errRunOrderingMode           = errcode.New(errcode.InvalidConfig, "iqmq: ordering mode is not a retry interaction")
	errRunOrderingModes          = errcode.New(errcode.InvalidConfig, "iqmq: WithOrderingByKey takes at most one mode")
	errRunHandlerTimeout         = errcode.New(errcode.InvalidConfig, "iqmq: handler timeout is not positive")
	errRunLeaseExtension         = errcode.New(errcode.InvalidConfig, "iqmq: lease extension lead is not positive")
	errRunDrainTimeout           = errcode.New(errcode.InvalidConfig, "iqmq: drain timeout is not positive")
	errRunDeadLetterConfirmation = errcode.New(errcode.InvalidConfig, "iqmq: dead-letter confirmation is not a confirmation mode")
	errRunNoLeaseExtension       = errcode.New(errcode.Unsupported, "iqmq: the driver cannot extend a delivery's lease")
	errRunTwoDeadLetters         = errcode.New(errcode.InvalidConfig, "iqmq: WithDeadLetter and WithNativeDeadLetter name two destinations")
	errRunNoNativeDeadLetter     = errcode.New(errcode.Unsupported, "iqmq: the driver has no dead-letter route of its own")

	errRunDeadLetterConfirmationUnused = errcode.New(errcode.InvalidConfig, "iqmq: WithDeadLetterConfirmation needs WithDeadLetter")
)
