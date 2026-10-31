package engine

import (
	"context"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Recorded is one publication a Recorder observed, together with the outcome
// the engine reported for it.
type Recorded[P any] struct {
	// Topic is the name of the topic the publication was made to.
	Topic string
	// Payload is the value the caller published. A Recorder holds the value
	// itself, so a recorded owned publication must not be read after the
	// engine has released it (Recorder).
	Payload P
	// Key is the routing key.
	Key []byte
	// Subject is the routing subject of a wildcard topic.
	Subject string
	// DedupKey is the deduplication key.
	DedupKey []byte
	// At is the engine time the publication was made at.
	At time.Time
	// Err is what Publish returned: nil for an admission, errcode.ErrPartial
	// for a partial fan-out, and the refusal otherwise.
	Err error
	// Size is the accounted payload size.
	Size int
	// Partition is the partition the caller asked for, unresolved: -1 means
	// the engine chose.
	Partition int
	// Mode is the ownership contract the publication was made under.
	Mode CopyMode
	// Priority is the priority level the publication asked for.
	Priority uint8
}

// Recorder is the publisher decorator of the memory engine: it forwards
// every publication to the engine and keeps what it saw, so a test can assert
// on what was published rather than only on what was delivered.
//
// It records the publication as the caller made it, the engine time it was
// made at and the outcome the engine reported, which is what makes it useful
// against the fault injector: a rejected publication and an admitted one look
// the same from the delivery side and differ here.
//
// A Recorder keeps the payload value, not a copy of it. For a safe
// publication that is the caller's own payload and stays valid; for an owned
// one it is a value the engine will release, so a test that reads a recorded
// owned payload must do so before the record is settled — the same contract
// the publication itself was made under.
//
// It is safe for concurrent use.
type Recorder[P any] struct {
	mu    sync.Mutex
	recs  []Recorded[P]
	limit int
	seen  int64
}

// NewRecorder returns a Recorder keeping at most limit publications; a
// non-positive limit keeps every one of them. When the limit is reached the
// oldest publication is dropped, so a long run keeps the most recent window
// rather than growing without bound.
func NewRecorder[P any](limit int) *Recorder[P] { return &Recorder[P]{limit: limit} }

// Publish forwards p to t and records it with the outcome.
func (r *Recorder[P]) Publish(ctx context.Context, t *Topic[P], p Publish[P]) error {
	at := t.eng.clock.Now()
	err := t.Publish(ctx, p)
	r.record(Recorded[P]{
		Topic:     t.Name(),
		Payload:   p.Payload,
		Key:       p.Key,
		Subject:   p.Subject,
		DedupKey:  p.DedupKey,
		At:        at,
		Err:       err,
		Size:      p.Size,
		Partition: p.Partition,
		Mode:      t.mode(&p),
		Priority:  p.Priority,
	})
	return err
}

// PublishBatch forwards each element of ps to t and records each of them,
// including the one the engine refused. It returns what Topic.PublishBatch
// returns, errcode.ErrPartial included: a batch with an element the fan-out
// admitted to only some subscriptions answered nil. It fills partial as
// Topic.PublishBatch does.
func (r *Recorder[P]) PublishBatch(ctx context.Context, t *Topic[P], ps []Publish[P], partial []bool) (int, error) {
	anyPartial := false
	for i := range ps {
		switch err := r.Publish(ctx, t, ps[i]); {
		case err == nil:
		case isPartial(err):
			anyPartial = true
			if partial != nil {
				partial[i] = true
			}
		default:
			return i, err
		}
	}
	if anyPartial {
		return len(ps), errcode.ErrPartial
	}
	return len(ps), nil
}

// record appends one observation, evicting the oldest when the limit is
// reached.
func (r *Recorder[P]) record(rec Recorded[P]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen++
	if r.limit > 0 && len(r.recs) == r.limit {
		copy(r.recs, r.recs[1:])
		r.recs[len(r.recs)-1] = rec
		return
	}
	r.recs = append(r.recs, rec)
}

// Records returns a copy of the publications the Recorder is holding, oldest
// first.
func (r *Recorder[P]) Records() []Recorded[P] {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Recorded[P], len(r.recs))
	copy(out, r.recs)
	return out
}

// Len returns the number of publications the Recorder is holding.
func (r *Recorder[P]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recs)
}

// Seen returns the number of publications the Recorder observed, including
// the ones its limit has evicted.
func (r *Recorder[P]) Seen() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen
}

// Reset forgets every publication and the count of them.
func (r *Recorder[P]) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.recs)
	r.recs = r.recs[:0]
	r.seen = 0
}
