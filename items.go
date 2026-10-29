package iqmq

import (
	"context"

	"github.com/iqhive/iqmq/internal/lifecycle"
)

// PublishItem is one message of a heterogeneous batch: a message and the
// option set it is published under (PublishItems).
//
// PublishBatch carries one option set for the whole batch, which is what a
// native batch API takes. A caller whose messages need different controls —
// a different delay, a different partition, a different dedup key per
// message — publishes them as items instead.
//
// Options is a pointer so that a caller pays nothing per item for the common
// cases. A nil Options means the publisher's Defaults, and a caller with a
// handful of distinct sets points several items at the same one; a caller
// with a set per item keeps them in a slice of its own and points each item
// into it, which costs one allocation for the whole batch rather than one per
// item. The pointed-at sets are read, never written.
type PublishItem struct {
	// Message is the message to publish. It must not be nil.
	Message *Message
	// Options is the resolved option set for this message, or nil for the
	// publisher's Defaults.
	Options *PublishOptions
}

// PublishItems publishes a heterogeneous batch: each item under its own
// option set.
//
// It returns what PublishBatch returns: nil when every message was accepted,
// a *BatchError whose Errs are index-aligned with items when some were — so
// errors.Is(err, ErrPartial) is true and the caller can see which — and a
// single error when none were and every item has the same outcome. A
// batch none of which was accepted but whose items' outcomes differ, some
// refused and some ambiguous, is a *BatchError as well, and IsAmbiguous is
// true of it. Nothing is published at all when a message or an option set
// fails validation: the specification puts that check before any I/O.
//
// Items that resolved to the same option set and sit next to each other are
// handed to the driver as one native batch, so a caller whose items mostly
// share a set still gets batching; items whose sets differ are published one
// at a time, because the specification gives a native batch one option set for every message
// in it. Ordering items by their option set is therefore worth doing when a
// batch is large. A run larger than the broker's maximum is split as
// PublishBatch splits a batch, and a key that failed in one piece is held
// back from the later ones in the same way.
func (p *PublisherHandle) PublishItems(ctx context.Context, items []PublishItem) error {
	if len(items) == 0 {
		return nil
	}
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return ErrClosed
	}
	defer p.life.Leave(t)

	var bytes int64
	for i := range items {
		m := items[i].Message
		if m == nil {
			return errNilMessage
		}
		o := items[i].Options
		if o == nil {
			o = &p.cfg.Defaults
		}
		if err := p.check(m, o); err != nil {
			return err
		}
		bytes += int64(len(m.Body))
	}
	for i := range items {
		p.stamp(items[i].Message)
		p.resetReceipt(items[i].Message)
	}

	start, seq := p.begin(ctx, len(items), bytes)
	// One container for the whole batch, not one per item: the messages of a
	// run are handed to the driver as a slice, and the outcomes come back
	// index-aligned with items.
	ms := make([]*Message, 0, len(items))
	errs := make([]error, len(items))
	for lo := 0; lo < len(items); {
		o := items[lo].Options
		hi := lo + 1
		for hi < len(items) && items[hi].Options == o {
			hi++
		}
		ms = ms[:0]
		for _, it := range items[lo:hi] {
			ms = append(ms, it.Message)
		}
		resolved := o
		if resolved == nil {
			resolved = &p.cfg.Defaults
		}
		p.sendAligned(ctx, ms, resolved, errs[lo:hi])
		if fatal := firstFatal(errs[lo:hi]); fatal != nil {
			// sendAligned stops only its own run. The later runs would be
			// offered to the same dead sink or expired context, and an
			// answer from there may read as ambiguous for a message that
			// was never sent.
			fillNotAttempted(errs[hi:], p.notAttempted(fatal))
			break
		}
		lo = hi
	}
	err := p.itemsOutcome(errs)
	p.finish(ctx, start, seq, len(items), bytes, err)
	return err
}

// itemsOutcome turns the per-item results into the batch's one error, and
// counts the outcome.
//
// The three answers are PublishBatch's: nil when every item was accepted, a
// *BatchError naming which failed when some were, and, when none were, the
// single error only if it is every item's outcome (noneAccepted). The specification
// forbids constructing a BatchError on the success path, which is why the nil
// case is answered first and errs is never wrapped.
func (p *PublisherHandle) itemsOutcome(errs []error) error {
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	switch {
	case failed == 0:
		p.counters.published.Add(int64(len(errs)))
		return nil
	case failed == len(errs):
		p.counters.failed.Add(int64(failed))
		return noneAccepted(errs)
	default:
		p.counters.published.Add(int64(len(errs) - failed))
		p.counters.failed.Add(int64(failed))
		return &BatchError{Errs: errs}
	}
}
