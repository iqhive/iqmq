// Package confirm holds the publisher-confirm strategies and the delivery-tag bookkeeping all
// three share.
//
// It is in the root module rather than in `drivers/rabbitmq` for the reason
// families, and the specification record: measures a family by
// running one `go test` in one package, and a root-module `go test` cannot
// reach a nested driver module. Nothing here needs a client
// library — a confirm is four integers and a flag — so the whole family is
// measurable and fuzzable with no broker, and the driver binds a [Strategy] and
// drives the [Tracker].
//
// # What a confirm is on this broker
//
// `confirm.select` puts an AMQP 0-9-1 channel into confirm mode. From then on
// every `basic.publish` on that channel is given the next delivery tag — 1, 2,
// 3, … — and the broker eventually answers with `basic.ack{delivery-tag,
// multiple}` or `basic.nack{delivery-tag, multiple, requeue}`. Four properties
// of that protocol are what this package exists to get right, and each of them
// has cost a real deployment a lost message:
//
//   - **A confirm may be cumulative.** `multiple=true` means "this tag and
//     every lower tag still outstanding", so one frame can resolve a hundred
//     publications. A driver that resolved only the named tag would leak the
//     other ninety-nine for ever and then report them Ambiguous at close.
//   - **A confirm may arrive out of order.** Tag 7 can be acked before tag 5:
//     the broker confirms a publication when it is safe, and two queues
//     behind one exchange do not fsync in lockstep.
//   - **An ack is not "delivered".** With `mandatory` set, a publication that
//     no queue is bound for produces a `basic.return` and then a
//     `basic.ack` — the broker took it, could not route it, and handed it
//     back. The specification makes that outcome **Rejected**, so the return has to reach
//     the publication before its ack resolves it, and [Tracker.Returned] is
//     how.
//   - **A channel close with confirms outstanding is Ambiguous, not failed.**
//     The broker may have persisted every one of them; it may have persisted
//     none. both say a driver must not report that as a
//     definite failure, because a caller that retries a publication the
//     broker took has duplicated it. [Tracker.Detach] is the only place in
//     this package that produces [Ambiguous], and it produces one per
//     outstanding publication.
//
// # Mechanism and decision
//
// [Tracker] is the MECHANISM: it holds the outstanding publications, applies
// cumulative and out-of-order confirms, remembers returns, and turns a channel
// close into one ambiguous outcome per publication in flight. There is exactly
// one of it, and all three kinds use it.
//
// A [Strategy] is the DECISION on top — "having just published, does the
// publisher wait for this confirm, carry on, wait for the window to drain, or
// never wait at all?" — and nothing else. It is pure: no clock, no lock, no
// I/O, no state between calls, which is what makes the family model-testable
// and what makes `ns/op` on it meaningful rather than a measurement of a
// mutex. That is the split, with the mechanism in this package rather
// than in `driverkit` because delivery tags, the `multiple` flag and
// `basic.return` are this protocol's and no other broker has them.
//
// # The three kinds
//
//   - [PerMessage] keeps one publication outstanding and waits for its
//     confirm. It is the reference kind and the expected default for
//     `Publish`: the confirmation boundary is exactly the caller's call, the
//     most a close can leave ambiguous is one publication, and a `mandatory`
//     return needs no correlation because there is only one candidate. It
//     pays one full round trip per message, so its throughput is
//     1/round-trip whatever the broker can do.
//   - [Windowed] lets up to [Config.Window] publications be outstanding and
//     blocks the publisher when the window is full. It is the expected
//     default for a batch: the window amortises the round trip over the
//     window's width, and the price is that a close can leave a whole window
//     ambiguous and that a `mandatory` return is no longer attributable
//     without a correlation token.
//   - [Async] never blocks the publisher: an outcome is reported to a
//     callback, which is `mq.AsyncSink`'s contract. The window is then
//     backpressure rather than a wait — the specification forbids an unbounded pending
//     set — so it holds as much as [Windowed] and blocks nobody.
//
// # Correlating a mandatory return
//
// `basic.return` carries the returned message, not its delivery tag, so with
// more than one publication outstanding there is nothing in the frame that
// says which one came back. [Strategy.NeedsCorrelation] is where that shows
// up: [PerMessage] needs no token because only one publication can be
// outstanding, and the other two need one, which the driver carries as a
// header on `mandatory` publications only. A driver that skipped it would
// attribute a return to the wrong publication — reporting one message
// Rejected and another Confirmed when the truth is the other way round, which
// is worse than reporting neither.
package confirm

import "github.com/iqhive/iqmq/internal/errcode"

// Kind identifies a strategy variant. Auto resolves to AutoKind.
type Kind uint8

// The kinds. PerMessage is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// PerMessage keeps one publication outstanding and waits for its own
	// confirm.
	PerMessage
	// Windowed allows Config.Window publications outstanding and blocks the
	// publisher when the window is full.
	Windowed
	// Async never blocks the publisher and reports each outcome to a
	// callback, bounded by Config.Window.
	Async

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "permessage", "windowed", "async"}

var kindIdents = [kindCount]string{"Auto", "PerMessage", "Windowed", "Async"}

// String returns the kind's slug as used in benchmark names, DSN parameters
// and ADRs.
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
	for k := PerMessage; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Slugs lists every concrete kind's slug.
func Slugs() []string {
	out := make([]string, 0, kindCount-1)
	for _, k := range Kinds() {
		out = append(out, k.String())
	}
	return out
}

// Action is what a publisher does having just published.
type Action uint8

// The actions a [Strategy] can ask for.
const (
	// WaitTag waits for this publication's own confirm before the call
	// returns. It is PerMessage's answer and the one that makes the
	// confirmation boundary exactly the caller's call.
	WaitTag Action = iota
	// Continue carries on without waiting: the publication is outstanding
	// and the caller will wait for it later, at the end of its batch or its
	// flush.
	Continue
	// WaitWindow waits until the outstanding count is below the window
	// before the next publication is admitted. It is backpressure and not a
	// confirmation boundary: the publication that triggered it is still
	// outstanding when the wait ends.
	WaitWindow
	// Defer never waits: the outcome is reported to a callback.
	Defer

	actionCount
)

var actionNames = [actionCount]string{"waittag", "continue", "waitwindow", "defer"}

// String returns the action's lower-case name.
func (a Action) String() string {
	if a < actionCount {
		return actionNames[a]
	}
	return "Action(?)"
}

// DefaultWindow is how many publications may be outstanding when a
// configuration does not say.
//
// Sixty-four is chosen from the shape of the trade rather than from a
// measurement, and the bake-off is what may move it: the window's benefit is
// bounded by the bandwidth-delay product of one channel, which for a
// millisecond-scale round trip and a few-kilobyte message is reached well
// before a hundred, and its cost is the number of publications a channel close
// leaves ambiguous. A window of one is [PerMessage].
const DefaultWindow = 64

// MaxWindow bounds the configured window.
//
// It is not the protocol's limit — a delivery tag is 64 bits — it is the specification:
// an unbounded pending set is a memory leak with a broker at the other end of
// it, and every publication in the window is a message the driver is holding.
const MaxWindow = 1 << 20

// Config configures a strategy. The zero value is usable for every kind.
type Config struct {
	// Window is how many publications may be outstanding at once for
	// [Windowed] and [Async]. Zero means DefaultWindow. It is ignored by
	// [PerMessage], whose window is one by definition.
	Window int
}

// resolve fills in the defaults.
func (c Config) resolve(k Kind) Config {
	if k.Resolve() == PerMessage {
		c.Window = 1
		return c
	}
	if c.Window == 0 {
		c.Window = DefaultWindow
	}
	return c
}

// Validate reports whether the configuration can be used with k.
func (c Config) Validate(k Kind) error {
	switch {
	case !k.Valid():
		return ErrInvalidKind
	case c.Window < 0 || c.Window > MaxWindow:
		return ErrInvalidWindow
	case c.Window > 1 && k.Resolve() == PerMessage:
		// A per-message strategy with a window of eight is a caller who
		// thinks they configured something. Refusing is cheaper than
		// ignoring it, because the ignored value is a number they will
		// later read back from the configuration and believe.
		return ErrWindowWithPerMessage
	default:
		return nil
	}
}

// Strategy decides when a publisher waits for a confirm.
//
// Every method is pure and safe to call from any goroutine: a strategy holds
// no state, so one value may drive every sink of a client.
type Strategy interface {
	// OnSubmit reports what to do having just published one message.
	// outstanding is the number of publications now awaiting a confirm,
	// including this one, and is always at least one.
	OnSubmit(outstanding int) Action
	// Window is how many publications may be outstanding at once. It is
	// always at least one.
	Window() int
	// NeedsCorrelation reports whether a `mandatory` publication needs a
	// correlation token in order to attribute a `basic.return` to it.
	//
	// It is false only when at most one publication can be outstanding, in
	// which case the return has exactly one candidate. See the package
	// comment: the frame carries no delivery tag.
	NeedsCorrelation() bool
	// Blocks reports whether the publishing goroutine ever waits. It is
	// false for [Async] alone, and it is what tells a driver whether this
	// strategy can serve `mq.Sink.Publish` at all: a synchronous publish
	// whose outcome is reported to a callback has no outcome to return.
	Blocks() bool
	// MaxAmbiguous is the most publications a channel close can leave
	// ambiguous under this strategy.
	//
	// It is the specification's "ambiguity accuracy" as a number, and it is the price of
	// the throughput the other two kinds buy: one for [PerMessage] and the
	// whole window for [Windowed] and [Async].
	MaxAmbiguous() int
	// Kind returns the variant this strategy implements.
	Kind() Kind
}

// New returns the strategy of kind k configured by cfg.
//
// It refuses an invalid kind or configuration rather than panicking: both
// reach it from a caller's own driver configuration, and a driver validates
// its configuration before it opens a channel.
func New(k Kind, cfg Config) (Strategy, error) {
	if err := cfg.Validate(k); err != nil {
		return nil, err
	}
	base := strategy{cfg: cfg.resolve(k), kind: k.Resolve()}
	switch k.Resolve() {
	case PerMessage:
		return perMessage{base}, nil
	case Windowed:
		return windowed{base}, nil
	case Async:
		return async{base}, nil
	case Auto:
		// Unreachable: Resolve maps Auto to a concrete kind. Returning the
		// reference kind rather than panicking keeps a future AutoKind typo
		// from crashing a caller's Open.
		return perMessage{base}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// strategy is the shared state every kind carries: its configuration and its
// own identity.
type strategy struct {
	cfg  Config
	kind Kind
}

// Window is the resolved window.
func (s strategy) Window() int { return s.cfg.Window }

// Kind returns the variant.
func (s strategy) Kind() Kind { return s.kind }

// The family's refusals.
var (
	// ErrInvalidKind is a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the publisher-confirm strategy is not a known kind")
	// ErrInvalidWindow is a negative or absurd window.
	ErrInvalidWindow = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the publisher-confirm window must be between 0 (the default) and 1048576")
	// ErrWindowWithPerMessage is a window configured for a strategy whose
	// window is one by definition.
	ErrWindowWithPerMessage = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the per-message publisher-confirm strategy keeps one publication outstanding, so it has no configurable window")
)

// MaxStalls is the most times a publisher can block while publishing n
// messages in batches of batch under this strategy.
//
//	PerMessage  n                         one wait per publication, always.
//	Windowed    batches x (batch-W+1 + 1) nothing waits while the window has
//	                                      room, so the first W-1 publications
//	                                      of a batch never block; every later
//	                                      one may, and the batch's own flush
//	                                      is one more.
//	Async       n-W+1                     the same backpressure, no flush,
//	                                      and no batch boundary at all.
//
// The batch width is a parameter because it changes the answer for a BLOCKING
// strategy and not for an asynchronous one, and that asymmetry is the family's
// most easily missed property: a blocking strategy owes its caller an outcome
// at every batch boundary, so it empties its window there, and a windowed
// strategy publishing one message at a time IS a per-message strategy. An
// asynchronous strategy owes no such wait, so its window spans the whole run
// and the batch width does not enter.
//
// The bound is the WORST case and the model checks it as an upper bound, with
// one exact claim beside it: [PerMessage] stalls exactly n times, which is the
// claim that separates it from the other two and the only one a looser bound
// would let slide.
func MaxStalls(s Strategy, n, batch int) int {
	if n <= 0 {
		return 0
	}
	w := s.Window()
	if w < 1 {
		w = 1
	}
	if w == 1 {
		// A per-message strategy waits for every publication, and its
		// flush at a batch boundary is always a no-op because nothing is
		// outstanding by then.
		return n
	}
	if !s.Blocks() {
		return max(0, n-w+1)
	}
	if batch <= 0 || batch > n {
		batch = n
	}
	batches := (n + batch - 1) / batch
	return batches * (max(0, batch-w+1) + 1)
}

// Result is a publication's outcome, which is what a [Tracker] resolves to.
type Result uint8

// The three outcomes a publication can have.
const (
	// Pending is the zero value: the publication is outstanding.
	Pending Result = iota
	// Confirmed is `basic.ack` with no preceding return: the broker took the
	// publication and routed it.
	Confirmed
	// Rejected is `basic.nack`, or `basic.ack` preceded by a
	// `basic.return` — the broker took the publication and could not route
	// it. Both are definite: the message is not in a queue.
	Rejected
	// Ambiguous is a channel that closed with the publication outstanding.
	// The broker may have taken it. The specification forbids reporting this as a
	// definite failure, and the specification names doing so a settlement disqualifier.
	Ambiguous

	resultCount
)

var resultNames = [resultCount]string{"pending", "confirmed", "rejected", "ambiguous"}

// String returns the result's lower-case name.
func (r Result) String() string {
	if r < resultCount {
		return resultNames[r]
	}
	return "Result(?)"
}

// Terminal reports whether the result is final. Pending is the only one that
// is not.
func (r Result) Terminal() bool { return r != Pending && r < resultCount }

// Outcome is one publication's resolution.
type Outcome struct {
	// Tag is the publication's delivery tag on its channel.
	Tag uint64
	// Token is the correlation token the driver submitted with it, empty
	// when the strategy needed none.
	Token string
	// Result is what happened.
	Result Result
	// Requeue is `basic.nack`'s requeue flag, which RabbitMQ always sends
	// false and which is carried so that a driver does not have to assume.
	Requeue bool
	// Returned reports that this publication came back through
	// `basic.return`: the broker accepted it and no queue was bound for its
	// routing key. It is the difference between "the broker refused it" and
	// "the broker took it and threw it away", and a caller acts on it
	// differently — the second means the topology is wrong.
	Returned bool
}
