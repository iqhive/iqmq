package inbox

import (
	"context"
	"errors"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/idempotency"
)

// KeyFunc derives a delivery's key. It is
// [github.com/iqhive/iqmq/x/idempotency].KeyFunc, re-exported rather than
// redeclared: the key-derivation problem is identical, and two copies of
// [KeyFromHeader] that could drift apart would be worse than the coupling.
type KeyFunc = idempotency.KeyFunc

// KeyFromHeader reads the key from a header. See
// [github.com/iqhive/iqmq/x/idempotency].KeyFromHeader — it is the shape to
// use when the PUBLISHER owns the key, which is the usual case.
func KeyFromHeader(name string) KeyFunc { return idempotency.KeyFromHeader(name) }

// KeyFromID reads the key from [mq.Message.ID]. It is correct only where the
// identifier is the publisher's and is stable across ITS retries; see
// [github.com/iqhive/iqmq/x/idempotency].KeyFromID for why a driver-assigned
// id is not.
func KeyFromID() KeyFunc { return idempotency.KeyFromID() }

// Unavailable is what the middleware does when the store cannot answer.
type Unavailable int

const (
	// OnUnavailableRetry nacks the delivery so it is redelivered. It is the
	// DEFAULT here, and that is the opposite of
	// [github.com/iqhive/iqmq/x/idempotency]'s default, on purpose.
	//
	// The reason is what the two stores are. An idempotency store is a cache
	// beside the work: it fails open because the substrate underneath is
	// at-least-once already, so processing during an outage returns the
	// system to the guarantee it advertises. An inbox store IS the database
	// the work happens in — if it cannot be reached, the side effect cannot
	// happen either, and there is nothing to fail open to. Processing
	// anyway is not "a duplicate for the duration of the outage"; it is a
	// handler that will fail for the same reason a moment later.
	OnUnavailableRetry Unavailable = iota
	// OnUnavailableProcess runs the handler UNGUARDED.
	//
	// It is here for the deployment whose handler writes somewhere other
	// than the inbox's database — which the package documentation explains
	// is already the case this pattern does not cover. Its cost is a
	// duplicate side effect for the duration of the outage, and it should be
	// chosen only by someone who has read that paragraph.
	OnUnavailableProcess
)

// MissingKey is what the middleware does with a delivery no key was found on.
type MissingKey int

const (
	// MissingKeyFail nacks the delivery with [ErrMissingKey]. It is the
	// DEFAULT here, and again the opposite of
	// [github.com/iqhive/iqmq/x/idempotency]'s.
	//
	// An idempotency middleware that refused unkeyed messages would make
	// adding it a breaking change to a running service, and its cost for
	// letting one through is a duplicate. Here the cost is a side effect
	// that is never recorded and will therefore be repeated on every
	// redelivery for ever, which is the failure the pattern was adopted to
	// remove. So an unkeyed message is visible rather than silently
	// unguarded.
	MissingKeyFail MissingKey = iota
	// MissingKeyProcess runs the handler unguarded and counts the delivery
	// in [Counts.NoKey]. It is for a topic that genuinely mixes keyed and
	// unkeyed messages.
	MissingKeyProcess
)

// Config configures [New].
type Config struct {
	// Store records the keys. Required.
	Store Store
	// Key derives the key. Required; there is no default, and the package
	// documentation explains why.
	Key KeyFunc
	// OnUnavailable is the policy for a store that cannot answer. The zero
	// value is [OnUnavailableRetry].
	OnUnavailable Unavailable
	// OnMissingKey is the policy for a delivery with no key. The zero value
	// is [MissingKeyFail].
	OnMissingKey MissingKey
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	switch {
	case c.Store == nil:
		return ErrNoStore
	case c.Key == nil:
		return ErrNoKeyFunc
	}
	return nil
}

// Counts is what a [Middleware] has seen. Every field is read with
// [Middleware.Counts] and is monotonic.
type Counts struct {
	// Processed is how many deliveries ran the handler inside a
	// transaction that committed.
	Processed uint64
	// Duplicate is how many were acked without running the handler because
	// their key was already recorded.
	Duplicate uint64
	// Failed is how many ran the handler and rolled back, so their key was
	// NOT recorded and they will be retried. A handler that settled its own
	// delivery with a Nack or a Reject and returned nil is rolled back and
	// counted here too: it asked for the message again.
	Failed uint64
	// NoKey is how many carried no key.
	NoKey uint64
	// Unavailable is how many met a store that could not answer.
	Unavailable uint64
}

// Middleware is the inbox guard. Use [New] and then [Middleware.Wrap].
//
// It is a type rather than a bare [mq.Middleware] so that [Counts] is
// reachable: a deployment that adopts this pattern wants to know its actual
// duplicate rate, and a middleware that only returned a function would leave
// that number nowhere.
type Middleware struct {
	cfg    Config
	counts struct {
		processed, duplicate, failed, noKey, unavailable atomic.Uint64
	}
}

// New returns an inbox middleware.
func New(cfg Config) (*Middleware, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Middleware{cfg: cfg}, nil
}

// Counts returns what this middleware has seen.
func (m *Middleware) Counts() Counts {
	return Counts{
		Processed:   m.counts.processed.Load(),
		Duplicate:   m.counts.duplicate.Load(),
		Failed:      m.counts.failed.Load(),
		NoKey:       m.counts.noKey.Load(),
		Unavailable: m.counts.unavailable.Load(),
	}
}

// errSettledWithoutAck is what fn returns to roll back the transaction of a
// handler that settled its own delivery without an Ack. It never leaves Wrap.
var errSettledWithoutAck = errors.New("iqmq/x/inbox: the handler settled its delivery without an Ack")

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
// something other than an Ack.
func settledWithoutAck(d *mq.Delivery) bool {
	return d.Settled() && !d.Acked()
}

// Wrap returns the [mq.Middleware].
func (m *Middleware) Wrap() mq.Middleware {
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			key, ok := m.cfg.Key(d)
			if !ok || key == "" {
				m.counts.noKey.Add(1)
				if m.cfg.OnMissingKey == MissingKeyFail {
					return ErrMissingKey
				}
				return next.Handle(ctx, d)
			}
			// fn runs inside the store's transaction, and the context it
			// gets is the store's rather than the caller's: a handler that
			// writes through a transaction-carrying context must see the
			// one the record was written in, which is the whole mechanism.
			//
			// handlerRan is how the store's failures are told from the
			// handler's, and it is STRUCTURAL rather than a look at the
			// error. The first version of this code classified
			// code -- CodeUnavailable, CodeClosed and so on -- and that is
			// wrong in the one direction that matters: a HANDLER whose
			// downstream service is down returns CodeUnavailable too, and
			// under OnUnavailableProcess it would then have been run a
			// second time, immediately and unguarded, for having failed.
			// Whether fn was called is a fact this middleware owns; what an
			// arbitrary handler's error looks like is not.
			handlerRan := false
			ran, err := m.cfg.Store.Once(ctx, key, func(txctx context.Context) error {
				handlerRan = true
				if err := next.Handle(txctx, d); err != nil {
					return err
				}
				if settledWithoutAck(d) {
					// The handler nacked or rejected its own delivery and
					// returned nil: it has asked for the message again, and
					// a committed record would make that redelivery a
					// duplicate, acked unprocessed. Rolling back drops the
					// record with whatever the handler wrote, which the
					// redelivery writes again.
					return errSettledWithoutAck
				}
				return nil
			})
			switch {
			case ran:
				m.counts.processed.Add(1)
				return nil
			case handlerRan && errors.Is(err, errSettledWithoutAck):
				// The rollback was this middleware's, not a failure the
				// handler reported, so the handler's nil stands; the runner
				// leaves a settled delivery alone either way.
				m.counts.failed.Add(1)
				return nil
			case handlerRan:
				// The handler ran and the transaction did not commit --
				// either because the handler failed and it rolled back, or
				// because the commit itself failed or is of unknown
				// outcome. All three want the same thing: report the error
				// so the delivery is retried. The key is not recorded in
				// the rollback case, and in the ambiguous case the record
				// is exactly what makes the retry safe.
				m.counts.failed.Add(1)
				return err
			case err == nil:
				// The key was already recorded: the work is done. Ack. The
				// middleware acks it itself because it has taken the
				// handler's place: under mq.SettlementManual nothing else
				// settles a delivery the handler never saw, and returning
				// nil left the duplicate in flight to be redelivered each
				// time its lease ran out.
				m.counts.duplicate.Add(1)
				return ackDuplicate(d)
			default:
				// fn was never called and the store reported an error, so
				// this is the store's failure and nobody else's.
				m.counts.unavailable.Add(1)
				if m.cfg.OnUnavailable == OnUnavailableProcess {
					return next.Handle(ctx, d)
				}
				return err
			}
		})
	}
}
