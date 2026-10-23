package iqmq

import (
	"context"
	"errors"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// RunBatch consumes deliveries and dispatches them to h in batches until ctx
// is done, the consumer closes, or the runner halts.
//
// It is Run's loop with one difference: what a worker is given is the GROUP of
// deliveries one fetch produced for one lane, rather than one delivery. The
// admission slots, the lane routing, the failure policy, the dead-letter route
// and the shutdown are Run's, unchanged — a batch is a handler shape, not a
// second runner.
//
// A batch never crosses a lane. The deliveries of one fetch are split into
// maximal runs that route the same way, and each run is handed to its lane as
// one batch, so the ordering survives batching: under the default
// OrderingByKey a batch holds the deliveries of one key hash, and under
// OrderingUnordered a whole fetch is one batch. A batch is therefore as large
// as the fetch and the routing allow, and a runner whose every message has a
// distinct key hands over batches of one.
//
// The result decides the batch:
//
//   - nil acks every delivery in it;
//   - a *BatchError settles each delivery by its own entry, which is why the
//     entries are index-aligned with the slice the handler was given: a nil
//     entry acks, and a non-nil one takes the failure path exactly as a
//     Handler's returned error does, including the control errors;
//   - any other error takes that path for every delivery in the batch.
//
// A handler that settled a delivery itself is not overruled, as in Run.
//
// Two options are refused rather than quietly meaning something else:
// WithMiddleware, because Middleware wraps a Handler and there is no
// Handler here to wrap, and OrderingStrict, because its retry holds one
// delivery and calls the handler again — which for a batch would mean calling
// HandleBatch a second time with a subset of the batch it was given, a shape
// The specification does not describe.
//
// Exactly one runner may be active on a consumer, and Run and RunBatch share
// that exclusion.
func (c *ConsumerHandle) RunBatch(ctx context.Context, h BatchHandler, opts ...RunOption) error {
	if h == nil {
		return errRunNoBatchHandler
	}
	cfg, err := resolveRun(c.cfg, opts)
	if err != nil {
		return err
	}
	switch {
	case len(cfg.mw) > 0:
		return errRunBatchMiddleware
	case cfg.orderMode == OrderingStrict:
		return errRunBatchStrictOrdering
	}
	if !c.running.CompareAndSwap(false, true) {
		return errRunAlreadyRunning
	}
	defer c.running.Store(false)
	if c.closed() {
		return ErrClosed
	}
	r, err := newRunner(ctx, c, nil, h, cfg)
	if err != nil {
		return err
	}
	c.run.Store(r)
	defer c.run.Store(nil)
	return r.run(ctx)
}

// dispatchGroups submits the first got deliveries to their lanes in groups:
// maximal runs of deliveries that route the same way.
//
// The group's LEADER is its first item and is what the lane pool carries; the
// rest hang off it. A group the submission could not place goes back to the
// broker with every delivery fetched after it, for the reason dispatch gives.
func (r *runner) dispatchGroups(got int) error {
	for i := 0; i < got; {
		lead := r.held[i]
		route := r.lanes.route(&r.buf[i])
		lead.group = lead.group[:0]
		j := i
		for ; j < got; j++ {
			if j > i && r.lanes.route(&r.buf[j]) != route {
				break
			}
			lead.group = append(lead.group, r.take(j))
		}
		if err := r.lanes.pool.Submit(r.fetchCtx, route, lead); err != nil {
			for _, it := range lead.group {
				r.requeue(&it.d, 0)
			}
			r.recycleGroup(lead)
			for k := j; k < got; k++ {
				r.giveBack(r.take(k))
			}
			return err
		}
		i = j
	}
	return nil
}

// recycleGroup returns every slot of a group, the leader last.
//
// The leader goes last because the group slice belongs to it: a leader back
// on the free list may be taken by the fetch loop at once, and the fetch loop
// rewrites that slice.
func (r *runner) recycleGroup(lead *runItem) {
	items := lead.group
	for _, it := range items {
		if it != lead {
			r.recycle(it)
		}
	}
	lead.group = lead.group[:0]
	r.recycle(lead)
}

// handleGroup runs one batch on a worker goroutine: the handler, then the
// settlement its result asks for, then the slots back.
func (r *runner) handleGroup(lead *runItem) {
	items := lead.group
	// The slots go back LAST, whatever this function does next: a released
	// lease ticket is the promise that nothing is still extending the
	// delivery a slot held, and recycling the slot before that promise is the
	// race FuzzDispatcher found. Deferred functions run
	// last-registered-first, so this one is registered first.
	defer r.recycleGroup(lead)
	select {
	case <-r.halted:
		// A halted runner does not start handling another batch; every
		// delivery in it goes back to the broker, in every settlement mode
		// (see runner.stopped).
		for _, it := range items {
			if !it.d.Settled() {
				r.requeue(&it.d, 0)
			}
		}
		return
	default:
	}
	if r.leases != nil {
		for _, it := range items {
			r.leases.track(&it.lease, &it.d)
		}
		defer func() {
			for _, it := range items {
				r.leases.release(&it.lease)
			}
		}()
	}
	ds := lead.ds[:0]
	for _, it := range items {
		ds = append(ds, &it.d)
	}
	lead.ds = ds
	// The batch's deadline is the leader's, because a group is one fetch's
	// worth of deliveries and their leases were granted together; a handler
	// that is given a batch is given one context for it.
	ctx, release := r.handlerCtx(&lead.d)
	err := r.invokeBatch(ctx, ds)
	release()
	r.settleGroup(items, err)
}

// invokeBatch calls the batch handler, turning a panic into a *PanicError when
// recovery is on (ADR-011's).
//
// The recovery is per BATCH and not per delivery, because the handler is: a
// panic halfway through a batch says nothing about which deliveries were
// processed, so every delivery of the batch takes the failure — which is
// exactly what the specification says a non-*BatchError result means.
func (r *runner) invokeBatch(ctx context.Context, ds []*Delivery) (err error) {
	if r.cfg.recover {
		defer func() {
			if v := recover(); v != nil {
				err = &PanicError{Value: v, Stack: stack()}
			}
		}()
	}
	return r.bh.HandleBatch(ctx, ds)
}

// settleGroup applies the batch result to every delivery in the batch.
//
// A *BatchError whose entries are not index-aligned with the batch would
// report one delivery's outcome for another, which is how a batch loses a
// message in silence. The alignment is the handler's contract and is
// checked here rather than trusted: a shorter list settles the deliveries it
// covers by their own entries and the rest by the *BatchError itself, and a
// longer one's surplus entries name no delivery and are ignored. An entry's
// directive speaks for its own delivery only, so the rest fail under the
// ordinary policy unless a directive wraps the whole *BatchError.
func (r *runner) settleGroup(items []*runItem, err error) {
	if r.cfg.settlement == SettlementManual {
		return
	}
	if err == nil {
		for _, it := range items {
			if !it.d.Settled() {
				r.ack(&it.d)
			}
		}
		return
	}
	var errs []error
	be := asBatchError(err)
	if be != nil {
		errs = be.Errs
	}
	for i, it := range items {
		d := &it.d
		if d.Settled() {
			continue
		}
		// The strict hold is refused for a batch runner, so fail never asks
		// for another attempt here.
		if be != nil && i >= len(errs) {
			directive, delay := directiveAbove(err, be)
			_ = r.failAs(it, d, err, directive, delay)
			continue
		}
		cause := err
		if i < len(errs) {
			cause = errs[i]
		}
		if cause == nil {
			r.ack(d)
			continue
		}
		_ = r.fail(it, d, cause)
	}
}

// directiveAbove reports the directive err carries above be in its chain: one
// wrapped around the whole *BatchError, never one of be's own entries.
func directiveAbove(err error, be *BatchError) (Directive, time.Duration) {
	for err != nil && err != error(be) { //nolint:errorlint // the chain is walked by hand to stop at be
		if e, ok := err.(*ControlError); ok { //nolint:errorlint // as DirectiveOf
			return e.Directive, e.Delay
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return DirectiveNone, 0
}

// asBatchError reports the *BatchError in err, if there is one.
func asBatchError(err error) *BatchError {
	var be *BatchError
	if errors.As(err, &be) {
		return be
	}
	return nil
}

// The batch runner's configuration failures.
var (
	errRunNoBatchHandler      = errcode.New(errcode.InvalidConfig, "iqmq: RunBatch needs a handler")
	errRunBatchMiddleware     = errcode.New(errcode.InvalidConfig, "iqmq: WithMiddleware wraps a Handler and RunBatch has none")
	errRunBatchStrictOrdering = errcode.New(errcode.InvalidConfig, "iqmq: RunBatch cannot hold a batch for a strictly ordered retry")
)
