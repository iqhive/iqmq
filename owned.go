package iqmq

import (
	"context"

	"github.com/iqhive/iqmq/internal/lifecycle"
)

// OwnedPublisher is the specification's ownership-transfer publication interface.
//
// Ownership transfers at invocation: the callee owns the message, its
// Key, its Body and its header storage from the moment the call is made, and
// Release is called exactly once — on success, on rejection, on close and on
// permanent failure alike — and never while an internal lock is held.
//
// It is for a caller that pools its messages: the release is the signal that
// the storage may go back into the pool, so a release that is skipped is a
// leak the caller cannot see and a release that happens twice is a
// use-after-free of somebody else's buffer.
type OwnedPublisher interface {
	// PublishOwned publishes om and releases it exactly once.
	PublishOwned(ctx context.Context, om OwnedMessage, opts ...PublishOption) error
	// PublishBatchOwned publishes every message in oms and releases each of
	// them exactly once, whatever the batch's outcome.
	PublishBatchOwned(ctx context.Context, oms []OwnedMessage, opts ...PublishOption) error
}

var _ OwnedPublisher = (*PublisherHandle)(nil)

// release calls om.Release once, with the message it was given. A message
// with no Release is a transfer to nobody, which is legal and costs nothing.
func (om OwnedMessage) release() {
	if om.Release != nil {
		om.Release(om.Message)
	}
}

// PublishOwned publishes om, taking ownership of its message.
//
// Release runs exactly once on every path out of this function: before the
// driver is reached (a nil message, a closed publisher, an option the backend
// cannot honour, a message past a limit) the core releases; once the driver
// has taken the message, the driver releases, which is OwnedSink's contract.
//
// Release never runs under an internal lock, and never inside the publication
// gate: it is the caller's code, it may take the caller's own locks, and it
// may publish again. A publisher with no OwnedSink is adapted rather than
// refused — the core publishes through the mandatory single-message sink,
// which may not retain the message, and releases as soon as it returns.
func (p *PublisherHandle) PublishOwned(ctx context.Context, om OwnedMessage, opts ...PublishOption) error {
	if om.Message == nil {
		om.release()
		return errNilMessage
	}
	transferred, err := p.publishOwned(ctx, om, opts)
	if !transferred {
		om.release()
	}
	return err
}

// publishOwned is PublishOwned's body. It never releases and never returns
// while holding anything: whether the driver took ownership is the second
// result, and the release happens in the caller, outside the gate.
func (p *PublisherHandle) publishOwned(ctx context.Context, om OwnedMessage, opts []PublishOption) (transferred bool, err error) {
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return false, ErrClosed
	}
	defer p.life.Leave(t)
	o := &p.cfg.Defaults
	if len(opts) > 0 {
		pooled := p.getOptions()
		defer p.optsPool.Put(pooled)
		*pooled = p.cfg.Defaults
		if err := pooled.Apply(opts...); err != nil {
			return false, err
		}
		o = pooled
	}
	p.stamp(om.Message)
	if err := p.check(om.Message, o); err != nil {
		return false, err
	}
	p.resetReceipt(om.Message)
	return p.sendOwned(ctx, om, o)
}

// sendOwned hands one owned message to the driver and counts the outcome.
func (p *PublisherHandle) sendOwned(ctx context.Context, om OwnedMessage, o *PublishOptions) (transferred bool, err error) {
	bytes := int64(len(om.Message.Body))
	start, seq := p.begin(ctx, 1, bytes)
	if p.owned != nil {
		// Ownership is the driver's from here, including the release, which
		// The specification requires even for a rejection or a permanent failure.
		transferred = true
		err = p.owned.PublishOwned(ctx, om, o)
	} else {
		err = p.sink.Publish(ctx, om.Message, o)
	}
	if err != nil {
		p.counters.failed.Add(1)
	} else {
		p.counters.published.Add(1)
	}
	p.finish(ctx, start, seq, 1, bytes, err)
	return transferred, err
}

// PublishBatchOwned publishes every message in oms, taking ownership of all
// of them.
//
// Every message is released exactly once, whatever happens: a validation
// failure releases the whole batch, because ownership transferred at
// invocation for every item and nothing in oms belongs to the caller any
// more. Nothing is published at all when a message or an option fails
// validation, which is the specification's "before any I/O".
//
// The result is PublishBatch's: nil when every message was accepted, a
// *BatchError index-aligned with oms when some were, and when none were, the
// single error if every message has the same outcome and the
// index-aligned *BatchError if some were refused and some are ambiguous.
func (p *PublisherHandle) PublishBatchOwned(ctx context.Context, oms []OwnedMessage, opts ...PublishOption) error {
	if len(oms) == 0 {
		return nil
	}
	transferred, err := p.publishBatchOwned(ctx, oms, opts)
	for i := range oms {
		if !transferred[i] {
			oms[i].release()
		}
	}
	return err
}

// publishBatchOwned is PublishBatchOwned's body. transferred is index-aligned
// with oms and says which messages the driver took ownership of; the caller
// releases the rest, outside the gate.
func (p *PublisherHandle) publishBatchOwned(ctx context.Context, oms []OwnedMessage, opts []PublishOption) (transferred []bool, err error) {
	transferred = make([]bool, len(oms))
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return transferred, ErrClosed
	}
	defer p.life.Leave(t)
	o := &p.cfg.Defaults
	if len(opts) > 0 {
		pooled := p.getOptions()
		defer p.optsPool.Put(pooled)
		*pooled = p.cfg.Defaults
		if err := pooled.Apply(opts...); err != nil {
			return transferred, err
		}
		o = pooled
	}
	for i := range oms {
		if oms[i].Message == nil {
			return transferred, errNilMessage
		}
		p.stamp(oms[i].Message)
		if err := p.check(oms[i].Message, o); err != nil {
			return transferred, err
		}
	}
	// One owned publication per message: the specification gives ownership transfer no batch
	// method, so a batch of owned messages is a loop over the single-message
	// one. The results are index-aligned all the same, which is what a caller
	// reading a *BatchError needs.
	var errs []error
	for i := range oms {
		p.resetReceipt(oms[i].Message)
		took, err := p.sendOwned(ctx, oms[i], o)
		transferred[i] = took
		if err == nil {
			continue
		}
		if errs == nil {
			errs = make([]error, len(oms))
		}
		errs[i] = err
		if isFatalBatch(err) {
			fillNotAttempted(errs[i+1:], p.notAttempted(err))
			// sendOwned counted only the messages it sent. The ones the
			// fatal error kept from the driver failed too, and PublishBatch
			// counts them so (batchOutcome).
			p.counters.failed.Add(int64(len(errs) - i - 1))
			break
		}
	}
	if errs == nil {
		return transferred, nil
	}
	failed := 0
	for _, e := range errs {
		if e != nil {
			failed++
		}
	}
	if failed == len(oms) {
		return transferred, noneAccepted(errs)
	}
	return transferred, &BatchError{Errs: errs}
}
