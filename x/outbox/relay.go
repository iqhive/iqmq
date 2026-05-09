package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
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
	// wants a dead-letter column has somewhere to write one. A store with
	// no such column may make it a no-op; the relay's own MaxAttempts bound
	// does not depend on it.
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
	// DefaultMaxAttempts is the claim count at which a row is poison. Zero
	// takes this; a negative value is refused, and there is deliberately no
	// "unbounded" -- see [ErrPoisonRow].
	DefaultMaxAttempts = 10
)

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
	// MaxAttempts is the claim count at which a row becomes
	// [ErrPoisonRow]. Zero takes [DefaultMaxAttempts].
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
	// Poison is how many exceeded [Config.MaxAttempts] and were not
	// attempted.
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
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return 0, ErrClosed
	}
	r.stats.passes.Add(1)

	rows, err := r.cfg.Store.Claim(ctx, r.cfg.Batch)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	r.stats.claimed.Add(uint64(len(rows)))

	sent, unsent := r.publishRows(ctx, rows)
	// Mark first, release second. A crash between the two leaves the
	// released rows locked until the lock lapses, which costs a delay; the
	// other order would leave PUBLISHED rows unmarked, which costs a
	// duplicate on every subsequent pass until the mark finally lands.
	// Both are survivable and this one is cheaper.
	if len(sent) > 0 {
		if err := r.cfg.Store.MarkSent(ctx, sent); err != nil {
			// The rows WERE published. Failing to mark them means they
			// will be published again, which is the duplicate
			// at-least-once already allowed. Report it so the caller can
			// see it; do not release, because releasing would make the
			// duplicate immediate rather than merely possible.
			return len(sent), err
		}
		r.stats.sent.Add(uint64(len(sent)))
	}
	if len(unsent) > 0 {
		if err := r.cfg.Store.Release(ctx, unsent); err != nil {
			return len(sent), err
		}
	}
	return len(sent), nil
}

// publishRows publishes each row and partitions the ids into sent and unsent.
func (r *Relay) publishRows(ctx context.Context, rows []Row) (sent, unsent []string) {
	if r.cfg.Concurrency > 1 {
		return r.publishConcurrent(ctx, rows)
	}
	// Indexed rather than ranged by value: a Row is 128 bytes and copying
	// one per iteration is a cost with no purpose, which `gocritic`'s
	// rangeValCopy is right about on a path that runs once per outbox row.
	for i := range rows {
		if r.publishOne(ctx, &rows[i]) {
			sent = append(sent, rows[i].ID)
			continue
		}
		unsent = append(unsent, rows[i].ID)
	}
	return sent, unsent
}

// publishConcurrent is the same thing with Concurrency workers, and it
// REORDERS. It is a separate function rather than a width-1 special case of
// one loop so that the ordered path stays obviously ordered.
func (r *Relay) publishConcurrent(ctx context.Context, rows []Row) (sent, unsent []string) {
	ok := make([]bool, len(rows))
	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := range rows {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			ok[i] = r.publishOne(ctx, &rows[i])
		}(i)
	}
	wg.Wait()
	for i := range rows {
		if ok[i] {
			sent = append(sent, rows[i].ID)
			continue
		}
		unsent = append(unsent, rows[i].ID)
	}
	return sent, unsent
}

// publishOne publishes one row and reports whether it may be marked sent.
//
// It returns false for every outcome that is not a definite success, and the
// ambiguous case is the one that matters: the ambiguous outcome is a
// publication whose acceptance is UNKNOWN, and marking it sent converts "we
// do not know" into "we are done" -- the only way this pattern can lose a
// message. Leaving it unsent produces a duplicate, which at-least-once
// already promised.
func (r *Relay) publishOne(ctx context.Context, row *Row) bool {
	if row.Attempts > r.cfg.MaxAttempts {
		r.stats.poison.Add(1)
		_ = r.cfg.Store.Fail(ctx, row.ID, ErrPoisonRow)
		return false
	}
	m, err := row.Message()
	if err != nil {
		// A row with no topic will fail identically on every attempt, so
		// it is recorded and not retried into the attempt bound one pass
		// at a time.
		r.stats.failed.Add(1)
		_ = r.cfg.Store.Fail(ctx, row.ID, err)
		return false
	}
	if err := r.cfg.Publish(ctx, row.Topic, []*mq.Message{m}); err != nil {
		// Both halves of the ambiguity surface: the code, and the
		// *OpError's own Ambiguous flag, which mq.IsAmbiguous reads. A
		// driver may set either, and a relay that checked only one would
		// mark half the ambiguous publications sent.
		if mq.CodeOf(err) == mq.CodeAmbiguous || mq.IsAmbiguous(err) {
			r.stats.ambiguous.Add(1)
			return false
		}
		r.stats.failed.Add(1)
		_ = r.cfg.Store.Fail(ctx, row.ID, err)
		return false
	}
	return true
}

// Run drains repeatedly until ctx is done or the relay is closed.
//
// A pass that published something does not wait before the next one: the
// queue is not empty, and sleeping between full batches is how a relay falls
// behind a burst. A pass that found nothing waits [Config.Interval].
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.Drain(ctx)
		switch {
		case errors.Is(err, ErrClosed):
			return nil
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
		if n > 0 {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		t := r.clock.NewTimer(r.cfg.Interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		}
	}
}
