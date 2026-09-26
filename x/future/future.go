// # Why it is here and not in the core
//
// The specification says of the future adapter that "it is not the default if it needs a
// channel allocation per message", and it does: a handle that can be waited
// on from another goroutine needs somewhere to park, and the cheapest thing
// to park on is a channel. The core's own asynchronous surface is therefore a
// callback — PublisherHandle.PublishAsync — which allocates nothing per
// publication, and this package is the opt-in adapter over it for callers who
// would rather write the wait than the callback.
//
// The cost is stated rather than hidden: Publish allocates exactly one Future
// and one channel per publication. A caller publishing millions of messages a
// second wants the callback; a caller publishing a handful and needing the
// receipt wants this.
//
// # Contract
//
// A Future resolves exactly once, and it always resolves: the core runs an
// asynchronous publication's callback exactly once, including for a
// publication a drop mode discarded and for one still outstanding when the
// publisher closed. Wait is therefore safe to call any
// number of times, from any number of goroutines, before or after the
// resolution, and every caller sees the same answer.
//
// Wait returns the publication's own error. A Wait whose context ends first
// returns the context's error and leaves the Future unresolved — the
// publication is still outstanding, and a later Wait still sees its outcome.
// Cancelling a wait never cancels the publication: nothing in iqmq's
// asynchronous path can be recalled once it has been admitted.
package future

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
)

// Future is the pending outcome of one asynchronous publication.
//
// The zero value is not usable; Publish and New return ready ones. A Future
// is safe for concurrent use by multiple goroutines.
type Future struct {
	done chan struct{}
	// once guards the single transition to resolved. The callback the core
	// runs is documented to run exactly once, and this is the belt to that
	// brace: a driver that ran it twice would close a closed channel and
	// panic in iqmq's own goroutine rather than in the driver's.
	once sync.Once
	res  mq.PublishResult
}

// New returns an unresolved Future and the callback that resolves it.
//
// It is the seam Publish is built on, and it is exported for callers who
// drive PublishAsync themselves — a batch loop that wants one Future per
// message but publishes through its own options, for example:
//
//	f, done := future.New()
//	if err := pub.PublishAsync(ctx, m, done); err != nil {
//		// The publication was refused before it was admitted, so done will
//		// never run: the Future is dead and must not be waited on.
//	}
//
// A refused PublishAsync never runs the callback, so a caller that ignores
// its error and then waits on the Future waits for ever. Publish exists so
// that the common case cannot make that mistake.
func New() (*Future, func(mq.PublishResult)) {
	f := &Future{done: make(chan struct{})}
	return f, f.resolve
}

// Publish publishes m asynchronously and returns a Future for its outcome.
//
// The returned error is the admission failure: the publisher refused the
// publication before it was admitted, no callback will run, and the Future is
// nil. A nil error means the publication is outstanding and its outcome —
// success or failure — arrives at the Future.
//
// Ownership follows m belongs to the publisher until the Future
// resolves, and returns to the caller then.
func Publish(ctx context.Context, p mq.AsyncPublisher, m *mq.Message, opts ...mq.PublishOption) (*Future, error) {
	f, done := New()
	if err := p.PublishAsync(ctx, m, done, opts...); err != nil {
		return nil, err
	}
	return f, nil
}

// resolve records the outcome and wakes every waiter.
func (f *Future) resolve(res mq.PublishResult) {
	f.once.Do(func() {
		f.res = res
		close(f.done)
	})
}

// Done returns a channel closed when the publication's outcome is known. It
// is the select-friendly form of Wait.
func (f *Future) Done() <-chan struct{} { return f.done }

// Resolved reports whether the outcome is already known, without blocking.
func (f *Future) Resolved() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Result returns the publication's outcome and reports whether it is known
// yet. It never blocks.
func (f *Future) Result() (mq.PublishResult, bool) {
	if !f.Resolved() {
		return mq.PublishResult{}, false
	}
	// The write to res happens before the close of done, and this read
	// happens after the receive from it, so the value is published.
	return f.res, true
}

// Wait blocks until the outcome is known or ctx ends, and returns the
// publication's own error: nil when the broker accepted it, and the failure
// otherwise.
//
// A ctx that ends first returns ctx.Err() and leaves the publication
// outstanding. A Future that is ALREADY resolved answers with its own outcome
// whatever ctx says: a select over two ready channels picks between them at
// random, so without this first check a resolved publication whose caller
// happened to pass a dead context would report the context's error as the
// broker's, at random, one time in two.
func (f *Future) Wait(ctx context.Context) error {
	select {
	case <-f.done:
		return f.res.Err
	default:
	}
	select {
	case <-f.done:
		return f.res.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receipt blocks like Wait and returns what the broker said about the
// publication as well. The receipt is the zero value unless the
// publisher was configured with Receipts and the message carried a slot.
func (f *Future) Receipt(ctx context.Context) (mq.PublishReceipt, error) {
	if err := f.Wait(ctx); err != nil {
		return mq.PublishReceipt{}, err
	}
	return f.res.Receipt, nil
}

// WaitAll waits for every future in fs and returns the outcome of the batch.
//
// It returns nil when every publication succeeded, the single error when the
// WHOLE batch failed and it held one publication, and a *mq.BatchError
// index-aligned with fs otherwise — which is the rule for a batch result,
// and the alignment a caller needs to tell which message failed.
//
// The condition on the collapse is "nothing was accepted", not "exactly one
// failed". It used to be the latter, so one refusal among
// a thousand acceptances answered with that refusal alone — indistinguishable
// from a batch of one that was wholly refused, and the specification forbids blindly
// retrying a partially successful batch because republishing it duplicates
// everything the broker did accept. A *mq.BatchError unwraps to its entries,
// so errors.Is and errors.As still reach the one failure's own cause.
//
// A batch every publication of which failed is still reported per item when
// it held more than one: unlike a driver batch answer, WaitAll is under
// no obligation to squeeze the outcome into a single value, and the aligned
// answer can never misattribute one publication's outcome to another.
//
// A ctx that ends first returns ctx.Err(): the batch's own outcome is not
// known, so reporting a partial one would be a lie about the publications
// still outstanding. A nil entry in fs is treated as an outcome that already
// succeeded, so a caller that skipped a message need not compact its slice —
// and a batch holding one is therefore not wholly failed.
func WaitAll(ctx context.Context, fs ...*Future) error {
	var errs []error
	failed, first := 0, error(nil)
	for i, f := range fs {
		if f == nil {
			continue
		}
		err := f.Wait(ctx)
		if err != nil && !f.Resolved() {
			// The wait ended on the context and not on the publication, so
			// the batch's own outcome is not known.
			return err
		}
		if err == nil {
			continue
		}
		if errs == nil {
			errs = make([]error, len(fs))
		}
		errs[i] = err
		failed++
		if first == nil {
			first = err
		}
	}
	switch {
	case failed == 0:
		return nil
	case failed == len(fs) && failed == 1:
		// Nothing was accepted and there is only one outcome to report, so
		// the single error misattributes nothing.
		return first
	default:
		return &mq.BatchError{Errs: errs}
	}
}
