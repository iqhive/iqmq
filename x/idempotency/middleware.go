package idempotency

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// KeyFunc derives the idempotency key of one delivery, and reports whether
// there is one.
//
// It has no default. A store keyed on the wrong thing is worse than no store:
// two distinct messages that derive one key make the second a false duplicate,
// which DISCARDS work, and that is the only failure in this package that can
// lose a message. So the caller names the key, and [KeyFromHeader] and
// [KeyFromID] are the two obvious answers rather than the assumed one.
type KeyFunc func(d *mq.Delivery) (string, bool)

// KeyFromHeader reads the key from a header.
//
// This is the shape to use when the PUBLISHER owns the key, which is the usual
// case: the publisher writes the business identifier of the operation — an
// order id, a payment reference — and every retry of that operation carries
// the same one. A header key must not use the reserved prefix, and
// [Config.Validate] refuses one that does rather than leaving a middleware
// that can never find its header on a driver that strips the prefix.
func KeyFromHeader(name string) KeyFunc {
	return func(d *mq.Delivery) (string, bool) {
		v, ok := d.Headers.Get(name)
		if !ok || len(v) == 0 {
			return "", false
		}
		return string(v), true
	}
}

// KeyFromID reads the key from [mq.Message.ID].
//
// It is the right choice only where the identifier is the publisher's and is
// stable across ITS retries. A driver-assigned or broker-assigned identifier
// is not: two publications of one logical operation get two identifiers, both
// read as [StatusFirst], and the store catches nothing while appearing to
// work. IDGenerator is per publication, so a publisher configured with
// one is exactly the case this must not be used for.
func KeyFromID() KeyFunc {
	return func(d *mq.Delivery) (string, bool) {
		if d.ID == "" {
			return "", false
		}
		return d.ID, true
	}
}

// Unavailable is what the middleware does when the store cannot answer.
type Unavailable int

const (
	// OnUnavailableProcess runs the handler. It is the default.
	//
	// The cost is a duplicate side effect for the duration of the outage. The
	// reason it is the default is that the substrate underneath is
	// at-least-once already: this package REDUCES a duplicate rate that was
	// never zero, so failing open returns the system to the guarantee it
	// advertises rather than to a new one. The alternative turns a cache
	// outage into a consumer that makes no progress, and a consumer that
	// makes no progress on a broker with a max-attempts policy dead-letters
	// everything it is sent.
	OnUnavailableProcess Unavailable = iota
	// OnUnavailableRetry nacks the delivery so it is redelivered later.
	//
	// This is the choice for a handler whose effect is genuinely not safe to
	// repeat — a payment capture, an outbound SMS. Its cost is stated so that
	// it cannot be chosen by accident: while the store is unreachable NOTHING
	// is processed, every redelivery consumes an attempt, and a runner
	// configured with [mq.WithMaxAttempts] and a dead-letter topic will
	// dead-letter perfectly good messages because a cache was down.
	OnUnavailableRetry
)

// MissingKey is what the middleware does with a delivery [Config.Key] finds no
// key on.
type MissingKey int

const (
	// MissingKeyProcess runs the handler, unguarded, and counts the delivery
	// in [Counts.NoKey]. It is the default: a topic that mixes keyed and
	// unkeyed messages is ordinary, and refusing the unkeyed ones would make
	// adding this middleware a breaking change to a running service.
	MissingKeyProcess MissingKey = iota
	// MissingKeyFail nacks the delivery. It is the choice for a topic where
	// every message MUST carry a key, and it turns a publisher that forgot
	// one into a visible failure rather than an unguarded handler call.
	MissingKeyFail
)

// Config configures [New].
type Config struct {
	// Store holds the claims. Required.
	Store Store
	// Key derives the key. Required; see [KeyFunc].
	Key KeyFunc
	// Window is how long a committed claim suppresses a duplicate. Required
	// and positive.
	//
	// It is the only number here that is a real engineering choice. It must
	// outlast the longest redelivery the substrate can produce — a lease that
	// expires, a rebalance, a consumer restart, an operator replaying a
	// day — because a duplicate that arrives after the window is a duplicate
	// that is processed. It must also be short enough that the store holds
	// the resulting number of keys.
	Window time.Duration
	// InFlightLease is how long an OPEN claim lasts. Zero takes
	// [DefaultInFlightLease]; negative is refused. It is capped at Window.
	//
	// It is what a worker that dies mid-handler costs: its redelivery reads
	// [StatusInFlight] and is postponed until the lease lapses, so a lease as
	// long as a day-long Window would hold back for a day a message nothing
	// is processing. It must still outlast the slowest handler,
	// because a claim that lapses under a running handler lets a redelivery
	// run it a second time, and that handler's commit is [Counts.CommitLost].
	InFlightLease time.Duration
	// OnUnavailable is the policy for a store that cannot answer. The zero
	// value is [OnUnavailableProcess].
	OnUnavailable Unavailable
	// OnMissingKey is the policy for a delivery with no key. The zero value
	// is [MissingKeyProcess].
	OnMissingKey MissingKey
	// InFlightRetryAfter is how long to wait before a message whose key
	// another worker holds an open claim on is offered again. Zero leaves the
	// runner's configured backoff in charge.
	//
	// A collision is NOT a failure of the message, so the middleware returns
	// [mq.Postpone] with this delay rather than an error: the runner never
	// exhausts, dead-letters or skips a message for it, however long the
	// other worker holds the claim. Where the runner gives the delivery back
	// to the broker to wait (see [mq.Postpone]), a broker that counts
	// deliveries counts that one.
	InFlightRetryAfter time.Duration
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	switch {
	case c.Store == nil:
		return ErrNoStore
	case c.Key == nil:
		return ErrNoKeyFunc
	case c.Window <= 0:
		return ErrBadWindow
	case c.InFlightLease < 0:
		return ErrBadInFlightLease
	}
	return nil
}

// DefaultInFlightLease is the [Config.InFlightLease] a zero value takes: five
// minutes, long enough for an ordinary handler and short enough that a dead
// worker's redelivery is deferred for minutes rather than for the window.
const DefaultInFlightLease = 5 * time.Minute

// lease is the open claim's lifetime: InFlightLease, defaulted and capped at
// Window.
func (c Config) lease() time.Duration {
	lease := c.InFlightLease
	if lease == 0 {
		lease = DefaultInFlightLease
	}
	return min(lease, c.Window)
}

// The configuration errors.
var (
	// ErrNoStore reports a Config with no Store.
	ErrNoStore = errors.New("idempotency: Config.Store is required")
	// ErrNoKeyFunc reports a Config with no Key. There is deliberately no
	// default; see [KeyFunc].
	ErrNoKeyFunc = errors.New("idempotency: Config.Key is required and has no default, because a store keyed on the wrong thing discards work")
	// ErrBadInFlightLease reports a negative Config.InFlightLease.
	ErrBadInFlightLease = errors.New("idempotency: Config.InFlightLease must not be negative; zero takes the default")
	// ErrMissingKey is what [MissingKeyFail] returns to the runner.
	ErrMissingKey = errors.New("idempotency: this delivery carries no idempotency key")
	// ErrInFlight is what the middleware returns to the runner, inside
	// [mq.Postpone], for a key another worker holds an open claim on. It is
	// a postponement and never a discard: the work may not have happened.
	ErrInFlight = errors.New("idempotency: another worker holds an open claim on this key; redeliver this message")
)

// Counts is what one [Interceptor] has counted.
//
// A delivery that a middleware behind this one postponed ([mq.Postpone], as a
// rate limit or an open circuit does) never reached the handler, so it is
// counted in Seen and nowhere else; the offer that does reach the handler is
// counted as usual.
type Counts struct {
	// Seen is how many deliveries reached the middleware.
	Seen int64
	// Processed is how many reached the handler because the claim was
	// [StatusFirst].
	Processed int64
	// Skipped is how many did not reach the handler because the store
	// reported [StatusDuplicate]. This is the number the middleware exists
	// to make non-zero.
	Skipped int64
	// Deferred is how many were postponed because another worker held an
	// open claim.
	Deferred int64
	// Unguarded is how many reached the handler WITHOUT a claim: the store
	// could not answer and the policy is [OnUnavailableProcess]. Every one is
	// a delivery this middleware did not protect, and a non-zero value is the
	// number to alert on.
	Unguarded int64
	// Refused is how many were sent back because the store could not answer
	// and the policy is [OnUnavailableRetry].
	Refused int64
	// NoKey is how many carried no key.
	NoKey int64
	// Failed is how many handler calls returned an error, or returned nil
	// after settling their own delivery with a Nack or a Reject, and whose
	// claim was therefore aborted so that the redelivery is processed.
	Failed int64
	// CommitLost is how many handlers SUCCEEDED and could not record it: the
	// claim had lapsed or been taken by the time Commit ran. The delivery is
	// acked anyway — the work is done — so each one is a future duplicate
	// this store will not catch.
	CommitLost int64
}

// Interceptor is the middleware and its counters.
type Interceptor struct {
	cfg Config
	c   struct {
		seen, processed, skipped, deferred atomic.Int64
		unguarded, refused, noKey          atomic.Int64
		failed, commitLost                 atomic.Int64
	}
}

// New builds the interceptor.
func New(cfg Config) (*Interceptor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Interceptor{cfg: cfg}, nil
}

// Counts reports what this interceptor has counted.
func (i *Interceptor) Counts() Counts {
	return Counts{
		Seen:       i.c.seen.Load(),
		Processed:  i.c.processed.Load(),
		Skipped:    i.c.skipped.Load(),
		Deferred:   i.c.deferred.Load(),
		Unguarded:  i.c.unguarded.Load(),
		Refused:    i.c.refused.Load(),
		NoKey:      i.c.noKey.Load(),
		Failed:     i.c.failed.Load(),
		CommitLost: i.c.commitLost.Load(),
	}
}

// Middleware returns the [mq.Middleware] that guards a handler.
//
// The chain is composed once when the runner starts, so the closure
// below is built once and the per-delivery cost is one claim, one key
// derivation and — on the first delivery of a key — one commit.
func (i *Interceptor) Middleware() mq.Middleware {
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			return i.handle(ctx, next, d)
		})
	}
}

// handle is one guarded delivery.
func (i *Interceptor) handle(ctx context.Context, next mq.Handler, d *mq.Delivery) error {
	i.c.seen.Add(1)
	key, ok := i.cfg.Key(d)
	if !ok {
		i.c.noKey.Add(1)
		if i.cfg.OnMissingKey == MissingKeyFail {
			return ErrMissingKey
		}
		return next.Handle(ctx, d)
	}
	claim, err := i.cfg.Store.Claim(ctx, key, i.cfg.lease())
	switch {
	case err != nil, claim.Status == StatusUnknown:
		if i.cfg.OnUnavailable == OnUnavailableRetry {
			i.c.refused.Add(1)
			return i.redeliver(err)
		}
		// Fail open: the handler runs with no claim, so nothing is committed
		// afterwards and this delivery is not protected. It is counted
		// separately from Processed precisely so that "the store was down and
		// we carried on" is visible rather than inferred.
		i.c.unguarded.Add(1)
		return next.Handle(ctx, d)
	case claim.Status == StatusDuplicate:
		// The work was done inside the window, so the delivery is acked: the
		// message HAS been processed, and a nack would ask the broker to
		// deliver it again for ever. The middleware acks it itself because
		// it has taken the handler's place: under mq.SettlementManual
		// nothing else settles a delivery the handler never saw, and
		// returning nil left the duplicate in flight to be redelivered each
		// time its lease ran out.
		i.c.skipped.Add(1)
		return ackDuplicate(d)
	case claim.Status == StatusInFlight:
		// Not a duplicate. See the package documentation: skipping this would
		// lose the work if the holder dies. It is postponed, not failed: this
		// worker has not run the handler, and a refusal the runner counted as
		// an attempt halted it, dead-lettered the message or skipped it
		// whenever the holder took longer than the attempts added up to.
		i.c.deferred.Add(1)
		return mq.Postpone(ErrInFlight, i.cfg.InFlightRetryAfter)
	}

	// A PANICKING handler releases its claim too, and the panic carries on.
	//
	// The specification recovers a panicking handler unless the caller asked it not to, so
	// without this the panic unwinds past the Abort below and the claim is
	// held for the whole lease: the redelivery then reads StatusInFlight and
	// is postponed until the lease lapses, with nothing processing it. A
	// panic is exactly when the claim should be released.
	//
	// The re-panic is what keeps the runner's own recovery -- and
	// mq.WithoutRecovery -- in charge of what a panic means.
	done := false
	defer func() {
		if done {
			return
		}
		i.c.processed.Add(1)
		r := recover()
		if r == nil {
			// The handler returned through some other non-local exit, or the
			// goroutine is being torn down. Release the claim either way.
			i.abort(ctx, claim)
			return
		}
		i.c.failed.Add(1)
		i.abort(ctx, claim)
		panic(r)
	}()
	herr := next.Handle(ctx, d)
	done = true
	if postponed(herr) {
		// Something behind the guard (a rate limit, an open circuit) turned
		// the delivery away before the handler ran. The work has not
		// happened, so the claim is released for the next offer, and the
		// delivery is counted as neither processed nor failed: the offer
		// that reaches the handler is.
		i.abort(ctx, claim)
		return herr
	}
	i.c.processed.Add(1)
	if herr != nil {
		i.c.failed.Add(1)
		// Release the claim so the redelivery is processed rather than
		// skipped. An Abort that itself fails is not escalated: the claim
		// lapses on its own at the end of the lease, and turning a failed
		// abort into a different error would hide the handler's own error —
		// which is the one the specification routes on.
		i.abort(ctx, claim)
		return herr
	}
	if settledWithoutAck(d) {
		// The handler nacked or rejected its own delivery and returned nil:
		// it has asked for the message again, and a committed key would
		// make that redelivery a duplicate, acked unprocessed. So the claim
		// is released as for a failure, and the handler's nil stands --
		// the runner leaves a settled delivery alone either way.
		i.c.failed.Add(1)
		i.abort(ctx, claim)
		return nil
	}
	// The open claim was the lease; the committed one is the whole window.
	claim.Window = i.cfg.Window
	if err := i.commit(ctx, claim); err != nil {
		// The work is DONE. Acking is the only answer that does not repeat
		// it, so the error is counted and not returned.
		i.c.commitLost.Add(1)
	}
	return nil
}

// ackDuplicate settles a duplicate the handler will never see and returns
// what the middleware should. ErrAlreadySettled is not a failure here, since
// whoever settled it first already decided; ErrNotSettleable is not either,
// since a delivery with no settlement right (a backend that has none, or one
// a caller built) has nothing to settle. Any other failure is the runner's to
// act on.
func ackDuplicate(d *mq.Delivery) error {
	err := d.Ack()
	switch {
	case err == nil, errors.Is(err, mq.ErrAlreadySettled), errors.Is(err, mq.ErrNotSettleable):
		return nil
	default:
		return err
	}
}

// settledWithoutAck reports whether the handler settled d itself with
// something other than an Ack. A delivery it left to the runner, or acked,
// is work done; one it nacked or rejected is not.
func settledWithoutAck(d *mq.Delivery) bool {
	return d.Settled() && !d.Acked()
}

// postponed reports whether err asks the runner to offer the delivery again
// without counting an attempt, which means the handler did not run.
func postponed(err error) bool {
	dir, _ := mq.DirectiveOf(err)
	return dir == mq.DirectivePostpone
}

// finishTimeout bounds a Commit or an Abort, which run detached from the
// delivery's context.
const finishTimeout = 10 * time.Second

// finishCtx is the context a claim is finished on: the delivery's values
// without its cancellation. A handler that finished as its deadline passed,
// or as the runner shut down, has still done its work, and a Commit on the
// already-cancelled context failed at once -- leaving the key open for the
// lease and the next redelivery free to repeat the work. An Abort lost the
// same way kept the redelivery in ErrInFlight for the whole lease.
func finishCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
}

func (i *Interceptor) commit(ctx context.Context, claim Claim) error {
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	return i.cfg.Store.Commit(fctx, claim)
}

func (i *Interceptor) abort(ctx context.Context, claim Claim) {
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	_ = i.cfg.Store.Abort(fctx, claim)
}

// redeliver turns a reason into the error the runner should see for a delivery
// that must come back.
func (i *Interceptor) redeliver(cause error) error {
	if cause == nil {
		cause = ErrInFlight
	}
	if i.cfg.InFlightRetryAfter > 0 {
		return mq.RetryAfter(cause, i.cfg.InFlightRetryAfter)
	}
	return cause
}
