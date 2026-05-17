package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Recorder is the publisher decorator of the memory engine: it forwards
// every publication to the publisher it wraps and keeps what it saw, so that
// a test can assert on what was published rather than only on what was
// delivered.
//
// It is what makes the fault injector useful: a rejected publication and an
// admitted one look identical from the delivery side and differ here, because
// the Recorder keeps the outcome the publisher reported for each of them.
//
// It implements [bitbucket.org/iqhive/iqmq.Publisher], so it goes wherever a
// publisher goes — including into the code under test, which is the point of a
// decorator rather than a hook.
//
// It is safe for concurrent use.
type Recorder struct {
	p     mq.Publisher
	clock mq.Clock
	limit int

	mu   sync.Mutex
	recs []Recorded
	seen int64
}

var _ mq.Publisher = (*Recorder)(nil)

// Recorded is one publication a Recorder observed, with the outcome the
// publisher reported for it.
type Recorded struct {
	// Topic is the destination the publication named, which for a bound
	// publisher is the publisher's own.
	Topic string
	// Message is an independent copy of the message as the caller made it.
	// It is a copy because lets a caller reuse its Key, Body and
	// header storage the instant Publish returns, so keeping the pointer
	// would record whatever the caller put there next.
	Message *mq.Message
	// Options are the controls the CALL named, folded onto a zero option
	// set. They are not the publisher's resolved set: a decorator sees the
	// tokens and not the defaults they were applied over.
	Options mq.PublishOptions
	// At is the time the publication was made, on the clock the Recorder
	// was built with.
	At time.Time
	// Err is what the publisher returned: nil for an admission, ErrPartial
	// for a partial fan-out, and the refusal otherwise.
	Err error
	// Batch reports that the publication was one element of a
	// PublishBatch, and Index is its position in that batch.
	Batch bool
	// Index is the element's position in its batch, and 0 for a single
	// publication.
	Index int
}

// NewRecorder wraps p, keeping at most limit publications. A non-positive
// limit keeps every one of them.
//
// When the limit is reached the oldest publication is dropped, so a long run
// keeps the most recent window rather than growing without bound; [Recorder.Seen]
// still counts every publication, including the ones the limit evicted.
//
// The clock is the one the timestamps are read from. A nil clock reads the
// wall clock, which follows the bubble clock inside testing/synctest.
func NewRecorder(p mq.Publisher, limit int, c mq.Clock) *Recorder {
	if c == nil {
		c = realClock{}
	}
	return &Recorder{p: p, clock: c, limit: limit}
}

// Publish forwards one message and records it with the outcome.
func (r *Recorder) Publish(ctx context.Context, m *mq.Message, opts ...mq.PublishOption) error {
	if r.p == nil {
		return errNoPublisher
	}
	err := r.p.Publish(ctx, m, opts...)
	r.record(m, opts, err, false, 0)
	return err
}

// PublishBatch forwards a batch and records every element of it, including
// the ones the publisher refused.
//
// A *BatchError is index-aligned with ms, so each element gets its own
// outcome; a single error is every element's outcome, which is what a single
// error from a batch means.
func (r *Recorder) PublishBatch(ctx context.Context, ms []*mq.Message, opts ...mq.PublishOption) error {
	if r.p == nil {
		return errNoPublisher
	}
	err := r.p.PublishBatch(ctx, ms, opts...)
	var be *mq.BatchError
	aligned := asBatchError(err, len(ms), &be)
	for i, m := range ms {
		one := err
		if aligned {
			one = be.Errs[i]
		}
		r.record(m, opts, one, true, i)
	}
	return err
}

// asBatchError reports whether err is a per-element answer for a batch of n.
func asBatchError(err error, n int, out **mq.BatchError) bool {
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != n {
		return false
	}
	*out = be
	return true
}

// Flush forwards the flush; nothing is recorded, because a flush publishes
// nothing.
func (r *Recorder) Flush(ctx context.Context) error {
	if r.p == nil {
		return errNoPublisher
	}
	return r.p.Flush(ctx)
}

// Close closes the wrapped publisher. The records survive it: a test asserts
// on what was published after it has shut the publisher down.
func (r *Recorder) Close() error {
	if r.p == nil {
		return errNoPublisher
	}
	return r.p.Close()
}

// Records returns a copy of the publications the Recorder is holding, oldest
// first.
func (r *Recorder) Records() []Recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Recorded, len(r.recs))
	copy(out, r.recs)
	return out
}

// Len returns the number of publications the Recorder is holding.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recs)
}

// Seen returns the number of publications the Recorder observed, including
// the ones its limit has evicted.
func (r *Recorder) Seen() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen
}

// Reset forgets every publication and the count of them.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.recs)
	r.recs = r.recs[:0]
	r.seen = 0
}

// record keeps one publication.
func (r *Recorder) record(m *mq.Message, opts []mq.PublishOption, err error, batch bool, index int) {
	rec := Recorded{
		At:    r.clock.Now(),
		Err:   err,
		Batch: batch,
		Index: index,
	}
	if m != nil {
		rec.Topic = m.Topic
		rec.Message = m.Clone()
	}
	// The tokens the call named, folded onto a zero set. A malformed token
	// is what the publisher already refused, so the fold cannot fail in a
	// way that matters; the error is dropped rather than hidden, because the
	// publication's own outcome is recorded above.
	_ = rec.Options.Apply(opts...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen++
	r.recs = append(r.recs, rec)
	if r.limit > 0 && len(r.recs) > r.limit {
		drop := len(r.recs) - r.limit
		clear(r.recs[:drop])
		r.recs = append(r.recs[:0], r.recs[drop:]...)
	}
}

// errNoPublisher is what a Recorder built over no publisher answers with.
var errNoPublisher = errcode.New(errcode.InvalidConfig,
	"iqmq/memory: the Recorder wraps no publisher")
