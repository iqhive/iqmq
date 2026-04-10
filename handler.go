package iqmq

import (
	"context"
	"strconv"
	"strings"
)

// Handler processes one delivery.
//
// Returning nil acks the delivery under the default settlement mode;
// returning an error nacks it with backoff. The control errors change
// that: RetryAfter, DeadLetter, Discard and Halt are wrappers a handler
// returns to name the disposition it wants.
//
// A handler that settles the delivery itself is not overruled: the runner's
// ack is skipped for a delivery that is already settled.
type Handler interface {
	// Handle processes the delivery. ctx is the runner's context, narrowed
	// by the lease deadline and by WithHandlerTimeout.
	Handle(ctx context.Context, d *Delivery) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, d *Delivery) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, d *Delivery) error { return f(ctx, d) }

// BatchHandler processes a batch of deliveries.
//
// The result decides the whole batch: nil acks every delivery, a *BatchError
// settles each delivery by its own entry — which is why the entries are
// index-aligned with ds — and any other error nacks every delivery in the
// batch.
type BatchHandler interface {
	// HandleBatch processes the deliveries.
	HandleBatch(ctx context.Context, ds []*Delivery) error
}

// BatchHandlerFunc adapts a function to BatchHandler.
//
// RunBatch takes the interface and the root package offered no adapter
// for it, so every caller of the batch path had to declare a type of its own
// while the single-delivery path had [HandlerFunc] -- an asymmetry with no
// reason behind it.
type BatchHandlerFunc func(ctx context.Context, ds []*Delivery) error

// HandleBatch calls f.
func (f BatchHandlerFunc) HandleBatch(ctx context.Context, ds []*Delivery) error { return f(ctx, ds) }

// Middleware wraps a Handler. A chain is composed once, when the
// runner starts, and never per message (rule 6).
type Middleware func(Handler) Handler

// Chain composes middleware into one, applied outermost-first: the first
// middleware of the chain sees the delivery before the ones inside it and
// their result after them.
//
// The chain is built when Chain is called, so a later change to mw cannot
// change what a composed handler does, and applying it allocates nothing
// beyond the handlers the middleware themselves build. A nil middleware is
// skipped rather than panicking, because a table of optional middleware is
// how a caller assembles one.
func Chain(mw ...Middleware) Middleware {
	composed := make([]Middleware, 0, len(mw))
	for _, m := range mw {
		if m != nil {
			composed = append(composed, m)
		}
	}
	return func(h Handler) Handler {
		for i := len(composed) - 1; i >= 0; i-- {
			h = composed[i](h)
		}
		return h
	}
}

// BatchError is the per-item outcome of a batch operation: a batch
// publication in which some messages were accepted and others were not, or a
// BatchHandler that processed some deliveries and refused others. It is also
// a batch publication none of whose messages was accepted when their
// outcomes differ -- some definitely refused, some ambiguous -- because no
// single error can report both, and reporting the first would have called an
// ambiguous message refused.
//
// Errs is index-aligned with the batch, and that alignment is the whole
// contract. A result aligned with anything else — the failures only, or a
// native sub-batch the driver split the request into — reports one message's
// outcome for another, which is how a batch publish loses a message in
// silence. FuzzBatchResults exists for exactly this, and disqualifies
// an implementation that "can lose an accepted message outside a documented
// failure boundary".
//
// A BatchError carries CodePartial, so errors.Is(err, ErrPartial) is true and
// CodeOf reports CodePartial whatever the individual entries carry. Unwrap
// returns the entries, so errors.Is and errors.As reach a single item's
// cause, and IsAmbiguous reads every entry: a BatchError is ambiguous when
// any of its items is. The specification forbids constructing one on the success path: a
// batch in which everything was accepted reports nil.
//
// The other half of that rule is a PERMISSION, not an obligation. A batch in
// which nothing was accepted and every item has the same outcome MAY report
// the single error, because collapsing it misattributes nothing, and a
// driver batch method does exactly that through
// [bitbucket.org/iqhive/iqmq/driverkit.CollapseBatch] — it has one error to
// return and no other way to answer. A caller-facing helper under no such
// constraint may keep the aligned answer instead, which is strictly more
// information and never wrong: a wait-all helper
// collapses only a wholly failed batch of ONE and reports every other failing
// batch per item. What is forbidden either way is
// collapsing a batch whose items do NOT agree, because one error cannot say
// both "refused" and "nobody knows".
type BatchError struct {
	// Errs is index-aligned with the batch: Errs[i] is nil when item i
	// succeeded and the item's own failure otherwise.
	Errs []error
}

// Error renders the counts without naming a message or a payload.
func (e *BatchError) Error() string {
	failed := e.Failed()
	var b strings.Builder
	b.Grow(48)
	b.WriteString("iqmq: batch of ")
	b.WriteString(strconv.Itoa(len(e.Errs)))
	b.WriteString(": ")
	b.WriteString(strconv.Itoa(failed))
	b.WriteString(" failed")
	if failed > 0 {
		for _, err := range e.Errs {
			if err != nil {
				b.WriteString(": first ")
				b.WriteString(err.Error())
				break
			}
		}
	}
	return b.String()
}

// Code returns CodePartial: a BatchError is a batch that was not uniformly
// accepted.
func (e *BatchError) Code() ErrorCode { return CodePartial }

// Is reports whether target carries CodePartial, which is the matching
// rule, so errors.Is(err, ErrPartial) holds for any BatchError.
func (e *BatchError) Is(target error) bool { return CodeOf(target) == CodePartial }

// Unwrap returns the per-item errors, so errors.Is and errors.As reach an
// item's own cause. The slice includes the nil entries of the items that
// succeeded, which errors.Is and errors.As both skip.
func (e *BatchError) Unwrap() []error { return e.Errs }

// Failed reports how many items failed.
func (e *BatchError) Failed() int {
	n := 0
	for _, err := range e.Errs {
		if err != nil {
			n++
		}
	}
	return n
}
