package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Store is the relay's half of the persistence. See the package
// documentation for why there is no Append.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Claim takes up to n unsent, unlocked rows, locks them, and returns
	// them in row order.
	//
	// Two relays running at once MUST get disjoint rows, which on Postgres
	// is `FOR UPDATE SKIP LOCKED` inside the UPDATE that sets the lock. A
	// store that let both claim the same row would publish it twice per
	// pass rather than once, which is a duplicate rate set by the number of
	// relays.
	//
	// The lock must LAPSE. A relay that dies holding a claim must not
	// strand its rows for ever, and the lapse is what turns a crashed relay
	// into a duplicate rather than a loss. Set it to rather more than the
	// longest publish timeout.
	Claim(ctx context.Context, n int) ([]Row, error)
	// MarkSent marks rows as sent. A row already marked is not an error:
	// the relay is at-least-once in this direction too.
	MarkSent(ctx context.Context, ids []string) error
	// Release gives a claim back without marking the rows sent, so another
	// pass takes them immediately rather than waiting for the lock to
	// lapse.
	//
	// It is an optimisation and not a correctness requirement — a store may
	// make it a no-op and rely on the lock lapsing — but a relay that shut
	// down cleanly with rows in hand would otherwise leave them invisible
	// for a whole lock period.
	Release(ctx context.Context, ids []string) error
	// Fail records that a row could not be published, so that a store which
	// wants a dead-letter column has somewhere to write one.
	//
	// A reason that is [ErrPoisonRow] (errors.Is) is terminal: the store
	// MUST take the row out of rotation, so that Claim never returns it
	// again: a dead-letter column, a flag Claim filters on, or a move to
	// another table. The relay does not release a poison row, so a store
	// that kept it claimable would hand it back once its lock lapsed, and a
	// batch of them at the head of the table would fill every claim and
	// stall the relay for good. Any other reason records one failed attempt
	// and the row stays in rotation, bounded by MaxAttempts; a store may
	// make that case a no-op.
	Fail(ctx context.Context, id string, reason error) error
	// Close releases the store's own resources. It is idempotent and does
	// not close a database handle the caller lent it.
	Close() error
}

// PublishFunc is how the relay publishes. It is [mq.PublishFunc]'s shape so
// that a caller can hand it a publisher wrapped in the same middleware chain
// the rest of their publications use.
type PublishFunc = mq.PublishFunc

// Defaults.
const (
	// DefaultBatch is how many rows one pass claims.
	DefaultBatch = 100
	// DefaultInterval is how long [Relay.Run] waits after a pass that found
	// nothing. A pass that found rows does not wait: the queue is not empty,
	// so there is no reason to sleep before looking again.
	DefaultInterval = 500 * time.Millisecond
	// DefaultMaxAttempts is the claim count past which a row that fails for
	// a reason of its own is poison. Zero takes this; a negative value is
	// refused, and there is deliberately no "unbounded" -- see [ErrPoisonRow].
	DefaultMaxAttempts = 10
)

// maxBackoffShift caps how far [Relay.Run] doubles [Config.Interval] across
// consecutive passes that met a broker that was down: 64 intervals, which is
// 32s at the default, so an outage is probed at a bounded rate and the relay
// is back within one probe of the broker returning.
const maxBackoffShift = 6

// finishTimeout bounds the bookkeeping after a publication (MarkSent, Release
// and Fail), which runs detached from the caller's context.
const finishTimeout = 10 * time.Second

// finishCtx is the context a row's outcome is recorded on: the caller's
// values without its cancellation. A pass cancelled after its rows reached
// the broker, such as a graceful shutdown under load, must still mark them sent,
// or a store that honours its context fails the mark at once and every one of
// them is published again when its lock lapses. The bound keeps a store that
// hangs from holding the shutdown up for ever.
func finishCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
}

// finishError is a MarkSent or Release failure. It is marked so that
// [Relay.Run] reports it even when its own context has ended: the pass's
// cancellation did not cause it, and swallowing it would hide the duplicates
// it costs.
type finishError struct{ err error }

func (e finishError) Error() string { return e.err.Error() }
func (e finishError) Unwrap() error { return e.err }

// Config configures a [Relay].
type Config struct {
	// Store holds the rows. Required.
	Store Store
	// Publish publishes them. Required.
	Publish PublishFunc
	// Batch is how many rows one pass claims. Zero takes [DefaultBatch].
	Batch int
	// Interval is how long [Relay.Run] waits after an empty pass. Zero takes
	// [DefaultInterval].
	Interval time.Duration
	// MaxAttempts is the claim count past which a row whose publication
	// fails for a reason of its own becomes [ErrPoisonRow]. A reason of its
	// own is a row that cannot be turned into a message, or a publication
	// refused as an invalid argument, too large or corrupt, and not
	// retryable. Any other failure -- the broker down, busy, unsure,
	// refusing the credentials or missing the topic, or an error with no
	// code at all -- is not the row's fault and never makes it poison,
	// however many claims it costs. Zero takes [DefaultMaxAttempts].
	MaxAttempts int
	// Concurrency is how many rows are published at once within a pass. Zero
	// and 1 both mean one at a time, which PRESERVES ROW ORDER. Anything
	// above 1 reorders; the package documentation says so and this field is
	// the only way to ask for it.
	Concurrency int
	// Clock is the time source. Nil takes the wall clock.
	Clock mq.Clock
}

func (c Config) resolve() (Config, error) {
	switch {
	case c.Store == nil:
		return Config{}, ErrNoStore
	case c.Publish == nil:
		return Config{}, ErrNoPublisher
	case c.Batch < 0 || c.Interval < 0 || c.MaxAttempts < 0 || c.Concurrency < 0:
		return Config{}, ErrInvalidConfig
	}
	if c.Batch == 0 {
		c.Batch = DefaultBatch
	}
	if c.Interval == 0 {
		c.Interval = DefaultInterval
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	return c, nil
}

// Stats is what a relay has done. Every field is monotonic.
type Stats struct {
	// Passes is how many times [Relay.Drain] has run.
	Passes uint64
	// Claimed is how many rows have been claimed.
	Claimed uint64
	// Sent is how many have been published and marked.
	Sent uint64
	// Failed is how many failed to publish and were released for another
	// attempt.
	Failed uint64
	// Ambiguous is how many publications reported an unknown outcome and
	// were therefore left UNSENT.
	//
	// It is its own counter because it is the number that explains a
	// duplicate a consumer saw: a non-zero Ambiguous is the duplicate this
	// pattern converts a possible loss into, and an operator looking at a
	// duplicate wants to see it here rather than inside Failed.
	Ambiguous uint64
	// Poison is how many exceeded [Config.MaxAttempts] and then failed for a
	// reason of their own, and were taken out of rotation.
	Poison uint64
}

// Relay moves rows from the store to the broker.
//
// Use [Relay.Drain] for one pass — which is what a test wants, and what a
// caller driving the relay from their own scheduler wants — or [Relay.Run]
// for the loop.
type Relay struct {
	cfg   Config
	clock mq.Clock

	stats struct {
		passes, claimed, sent, failed, ambiguous, poison atomic.Uint64
	}

	mu     sync.Mutex
	closed bool
}

// New returns a relay.
func New(cfg Config) (*Relay, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	clock := c.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Relay{cfg: c, clock: clock}, nil
}

// realClock is the wall clock under [mq.Clock].
//
// internal/clock.Real exists and cannot be used here: Go's interface
// satisfaction is invariant in return types, so a clock whose NewTimer
// returns clock.Timer does not satisfy mq.Clock however identical the two
// Timer interfaces are. The root's own clockBridge adapts in the other
// direction ; this is the same one line the other way
// round, and nothing on a hot path crosses it -- one timer per EMPTY pass,
// never one per row.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) mq.Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Stats returns what this relay has done.
func (r *Relay) Stats() Stats {
	return Stats{
		Passes:    r.stats.passes.Load(),
		Claimed:   r.stats.claimed.Load(),
		Sent:      r.stats.sent.Load(),
		Failed:    r.stats.failed.Load(),
		Ambiguous: r.stats.ambiguous.Load(),
		Poison:    r.stats.poison.Load(),
	}
}

// Close stops the relay. It is idempotent. It does not close the store, which
// the caller owns.
func (r *Relay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// Drain runs one pass: claim, publish, mark. It returns how many rows it
// published.
//
// A pass publishes each row and then marks the ones that succeeded. The
// marking is deliberately AFTER the publishing and covers only what was
// published, which is the second of the package documentation's two rules: a
// batch whose third row fails marks the first two and releases the rest.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	n, _, err := r.drain(ctx)
	var fe finishError
	if errors.As(err, &fe) {
		return n, fe.err
	}
	return n, err
}

// drain is [Relay.Drain] that also reports whether the pass met a broker that
// was down, which is what [Relay.Run] paces its next pass by.
func (r *Relay) drain(ctx context.Context) (n int, down bool, err error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return 0, false, ErrClosed
	}
	r.stats.passes.Add(1)

	rows, err := r.cfg.Store.Claim(ctx, r.cfg.Batch)
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	r.stats.claimed.Add(uint64(len(rows)))

	sent, unsent, down := r.publishRows(ctx, rows)
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	// Mark first, release second. A crash between the two leaves the
	// released rows locked until the lock lapses, which costs a delay; the
	// other order would leave PUBLISHED rows unmarked, which costs a
	// duplicate on every subsequent pass until the mark finally lands.
	// Both are survivable and this one is cheaper.
	if len(sent) > 0 {
		if err := r.cfg.Store.MarkSent(fctx, sent); err != nil {
			// The rows WERE published. Failing to mark them means they
			// will be published again, which is the duplicate
			// at-least-once already allowed. Report it so the caller can
			// see it; do not release, because releasing would make the
			// duplicate immediate rather than merely possible.
			return len(sent), down, finishError{err}
		}
		r.stats.sent.Add(uint64(len(sent)))
	}
	if len(unsent) > 0 {
		if err := r.cfg.Store.Release(fctx, unsent); err != nil {
			return len(sent), down, finishError{err}
		}
	}
	return len(sent), down, nil
}

// publishRows publishes each row, partitions the ids into sent and unsent, and
// reports whether a publication met a broker that was down.
//
// The ordered path STOPS at the first row it could not send and releases that
// row and every row after it unattempted. Publishing the rows behind a failed
// one would put them on the broker ahead of it, which is the reorder
// Concurrency 1 promises not to make. A poison row does not stop the pass: it
// leaves rotation, so it has no place in the order any more.
func (r *Relay) publishRows(ctx context.Context, rows []Row) (sent, unsent []string, down bool) {
	if r.cfg.Concurrency > 1 {
		return r.publishConcurrent(ctx, rows)
	}
	// Indexed rather than ranged by value: a Row is 128 bytes and copying
	// one per iteration is a cost with no purpose, which `gocritic`'s
	// rangeValCopy is right about on a path that runs once per outbox row.
	for i := range rows {
		switch r.publishOne(ctx, &rows[i]) {
		case rowSent:
			sent = append(sent, rows[i].ID)
		case rowDead:
		case rowDown:
			return sent, appendIDs(unsent, rows[i:]), true
		default:
			return sent, appendIDs(unsent, rows[i:]), false
		}
	}
	return sent, unsent, false
}

// appendIDs appends the ids of rows to ids.
func appendIDs(ids []string, rows []Row) []string {
	for i := range rows {
		ids = append(ids, rows[i].ID)
	}
	return ids
}

// publishConcurrent is the same thing with Concurrency workers, and it
// REORDERS. It is a separate function rather than a width-1 special case of
// one loop so that the ordered path stays obviously ordered.
//
// Once one publication finds the broker down it starts no more: the rest of
// the claim would fail the same way, each costing the broker a request it
// cannot answer. The rows it did not start are released.
func (r *Relay) publishConcurrent(ctx context.Context, rows []Row) (sent, unsent []string, down bool) {
	got := make([]rowOutcome, len(rows))
	limit := newInFlight(r.cfg.Concurrency)
	var (
		wg      sync.WaitGroup
		stopped atomic.Bool
	)
	for i := range rows {
		limit.enter()
		if stopped.Load() {
			limit.leave()
			got[i] = rowUnsent
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer limit.leave()
			got[i] = r.publishOne(ctx, &rows[i])
			if got[i] == rowDown {
				stopped.Store(true)
			}
		}(i)
	}
	wg.Wait()
	for i := range rows {
		switch got[i] {
		case rowSent:
			sent = append(sent, rows[i].ID)
		case rowUnsent:
			unsent = append(unsent, rows[i].ID)
		case rowDown:
			unsent = append(unsent, rows[i].ID)
			down = true
		default:
		}
	}
	return sent, unsent, down
}

// rowOutcome is what one row's publication decided about the row.
type rowOutcome uint8

const (
	// rowSent: published, to be marked sent.
	rowSent rowOutcome = iota
	// rowUnsent: not published this pass, to be released for the next.
	rowUnsent
	// rowDead: poison, out of rotation and neither marked nor released.
	rowDead
	// rowDown: not published because the broker was down, busy or unsure, to
	// be released for a later pass. The row is not at fault, so the outcome
	// never makes it poison.
	rowDown
)

// publishOne publishes one row and reports whether it may be marked sent,
// is to be released, or is poison.
//
// It answers rowSent only for a definite success, and the
// ambiguous case is the one that matters: the ambiguous outcome is a
// publication whose acceptance is UNKNOWN, and marking it sent converts "we
// do not know" into "we are done" -- the only way this pattern can lose a
// message. Leaving it unsent produces a duplicate, which at-least-once
// already promised.
//
// A row over the bound is still attempted, and is poison only when that
// attempt fails for a reason of its own. The store counts claims, and a claim
// during a broker outage is not the row's failure: a bound that counted it
// dead-lettered every row an outage of MaxAttempts passes touched, which is a
// loss the relay chose.
func (r *Relay) publishOne(ctx context.Context, row *Row) rowOutcome {
	m, err := row.Message()
	if err != nil {
		// A row with no topic will fail identically on every attempt, so
		// it is recorded, and the bound takes it out of rotation.
		return r.rowFailed(ctx, row, err)
	}
	if err := r.cfg.Publish(ctx, row.Topic, []*mq.Message{m}, nil); err != nil {
		// Both halves of the ambiguity surface: the code, and the
		// *OpError's own Ambiguous flag, which mq.IsAmbiguous reads. A
		// driver may set either, and a relay that checked only one would
		// mark half the ambiguous publications sent.
		if mq.CodeOf(err) == mq.CodeAmbiguous || mq.IsAmbiguous(err) {
			r.stats.ambiguous.Add(1)
			return rowDown
		}
		if brokerDown(err) {
			r.stats.failed.Add(1)
			r.fail(ctx, row.ID, err)
			return rowDown
		}
		return r.rowFailed(ctx, row, err)
	}
	return rowSent
}

// rowFailed records a failure that is the row's own, and makes the row poison
// once it is over the bound.
func (r *Relay) rowFailed(ctx context.Context, row *Row, err error) rowOutcome {
	if row.Attempts > r.cfg.MaxAttempts {
		r.stats.poison.Add(1)
		// Not released, whether or not the store records it: releasing
		// would make it the first row of the next claim again, which is the
		// stall the bound exists to end. At worst its lock lapses.
		r.fail(ctx, row.ID, ErrPoisonRow)
		return rowDead
	}
	r.stats.failed.Add(1)
	r.fail(ctx, row.ID, err)
	return rowUnsent
}

// fail records a row's failure on a [finishCtx], for the same reason the
// marks do: a poison row whose Fail was cancelled stays in rotation. The
// store's own error is dropped, as it always was: the row's outcome is
// already decided by then.
func (r *Relay) fail(ctx context.Context, id string, reason error) {
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	_ = r.cfg.Store.Fail(fctx, id, reason)
}

// brokerDown reports whether a failed publication says the broker, and not
// the row, is the problem: the same row would succeed once the broker is back.
//
// It names the row's faults and treats everything else as the broker's,
// rather than the other way round. Attempts counts every claim, including
// the ones an outage cost, so after a long outage a row is already past the
// bound and its next failure makes it poison: a failure wrongly blamed on the
// row -- revoked credentials, a topic deleted and being recreated, a raw
// network error from a custom PublishFunc that carries no code -- would
// dead-letter every row it touched on the first try. A failure wrongly
// blamed on the broker only keeps the row in rotation, with Run backing off,
// and Store.Fail still records why.
func brokerDown(err error) bool {
	if mq.IsRetryable(err) {
		return true
	}
	switch mq.CodeOf(err) {
	case mq.CodeInvalidArgument, mq.CodeMessageTooLarge, mq.CodeCorruptMessage:
		return false
	default:
		return true
	}
}

// Run drains repeatedly until ctx is done or the relay is closed.
//
// A pass that published something does not wait before the next one: the
// queue is not empty, and sleeping between full batches is how a relay falls
// behind a burst. A pass that found nothing waits [Config.Interval].
//
// A pass that met a broker that was down waits too, whatever it published,
// and each consecutive one waits twice as long, up to 64 intervals. The
// released rows would otherwise be claimed straight back and fail again, as
// fast as the store answers, for as long as the outage lasts.
func (r *Relay) Run(ctx context.Context) error {
	downs := 0
	var fe finishError
	for {
		n, down, err := r.drain(ctx)
		switch {
		case errors.Is(err, ErrClosed):
			return nil
		case errors.As(err, &fe):
			// Marking or releasing failed on its detached context, so the
			// shutdown did not cause it, and the rows it leaves locked are
			// duplicates the caller should hear about.
			return fe.err
		case err != nil && ctx.Err() != nil:
			// The context ended mid-pass. That is a clean stop and not a
			// relay failure, and reporting the store's context error here
			// would make every shutdown look like one.
			return nil
		case err != nil:
			// A store or publish failure is reported to the caller, who
			// decides whether to keep going. A relay that swallowed them
			// and looped would hide a permanently broken database behind a
			// process that looks healthy.
			return err
		}
		wait := r.cfg.Interval
		switch {
		case down:
			wait = r.cfg.Interval << min(downs, maxBackoffShift)
			downs++
		case n > 0:
			downs = 0
			if ctx.Err() != nil {
				return nil
			}
			continue
		default:
			downs = 0
		}
		t := r.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		}
	}
}
