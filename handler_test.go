package iqmq_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestHandlerFuncAdaptsAFunction pins the adapter and the one thing a
// handler does with a delivery: settle it, or leave it to the runner.
func TestHandlerFuncAdaptsAFunction(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	var seen *mq.Delivery
	var h mq.Handler = mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		seen = d
		return nil
	})
	if err := h.Handle(context.Background(), &d); err != nil {
		t.Fatal(err)
	}
	if seen != &d {
		t.Fatal("the handler was given a different delivery")
	}
	if string(seen.Body) != "body" {
		t.Fatalf("the handler saw the body %q", seen.Body)
	}
	// The embedded Message is reachable without an accessor, which is row
	// the whole point.
	if seen.ID != "0" || seen.Attempt != 1 {
		t.Fatalf("the handler saw %+v", seen.Message)
	}
	if err := seen.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestChainAppliesOutermostFirst pins the composition order, which is the
// same order the publish-side middleware of the specification documents: the first
// middleware of the chain sees the delivery before the ones inside it and
// their result after them.
func TestChainAppliesOutermostFirst(t *testing.T) {
	var order []string
	mw := func(name string) mq.Middleware {
		return func(next mq.Handler) mq.Handler {
			return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
				order = append(order, "->"+name)
				err := next.Handle(ctx, d)
				order = append(order, "<-"+name)
				return err
			})
		}
	}
	base := mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		order = append(order, "handle")
		return nil
	})
	// A nil middleware is skipped rather than panicking: a table of
	// optional middleware is how a caller assembles one.
	chain := mq.Chain(mw("a"), nil, mw("b"))
	if err := chain(base).Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	want := "->a,->b,handle,<-b,<-a"
	if got := strings.Join(order, ","); got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}

	// The chain is built when Chain is called, so a later change to the
	// slice cannot change what a composed handler does (rule 6).
	mws := []mq.Middleware{mw("x")}
	composed := mq.Chain(mws...)
	mws[0] = mw("y")
	order = order[:0]
	if err := composed(base).Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "->x,handle,<-x" {
		t.Fatalf("the composed chain changed with the caller's slice: %s", got)
	}

	// An empty chain is the identity.
	order = order[:0]
	if err := mq.Chain()(base).Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "handle" {
		t.Fatalf("the empty chain is not the identity: %s", got)
	}
}

// batchRecorder is a BatchHandler that records what it was given.
type batchRecorder struct {
	seen []*mq.Delivery
	err  error
}

func (h *batchRecorder) HandleBatch(_ context.Context, ds []*mq.Delivery) error {
	h.seen = ds
	return h.err
}

// TestBatchHandlerResult pins the batch result: a *BatchError is
// index-aligned with the batch it was given, so the runner can settle each
// delivery by its own entry.
func TestBatchHandlerResult(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 3)
	dst := make([]mq.Delivery, 3)
	n, err := c.Receive(context.Background(), dst)
	if err != nil || n != 3 {
		t.Fatalf("Receive = (%d, %v)", n, err)
	}
	ds := []*mq.Delivery{&dst[0], &dst[1], &dst[2]}
	failed := errors.New("the handler refused this one")
	h := &batchRecorder{err: &mq.BatchError{Errs: []error{nil, failed, nil}}}
	var bh mq.BatchHandler = h
	err = bh.HandleBatch(context.Background(), ds)
	var be *mq.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("HandleBatch = %v, want a *BatchError", err)
	}
	if len(be.Errs) != len(ds) {
		t.Fatalf("the result carries %d entries for %d deliveries", len(be.Errs), len(ds))
	}
	if be.Failed() != 1 {
		t.Fatalf("Failed = %d, want 1", be.Failed())
	}
	if !errors.Is(err, mq.ErrPartial) {
		t.Fatal("a mixed batch result does not match ErrPartial")
	}
	if !errors.Is(err, failed) {
		t.Fatal("a mixed batch result does not reach the item's own cause")
	}
	if got := mq.CodeOf(err); got != mq.CodePartial {
		t.Fatalf("CodeOf = %v, want CodePartial", got)
	}
	for i := range ds {
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("acking delivery %d: %v", i, err)
		}
	}
}

// TestBatchErrorReportsWithoutAPayload pins the rule for the message a
// batch error renders: counts and causes, never a body, a key or a header.
func TestBatchErrorReportsWithoutAPayload(t *testing.T) {
	be := &mq.BatchError{Errs: []error{nil, errors.New("refused"), nil, errors.New("refused")}}
	msg := be.Error()
	if !strings.Contains(msg, "batch of 4") || !strings.Contains(msg, "2 failed") {
		t.Fatalf("Error() = %q, want the counts", msg)
	}
	if be.Code() != mq.CodePartial {
		t.Fatalf("Code() = %v, want CodePartial", be.Code())
	}
	// The code wins over the entries': a batch that was not uniformly
	// accepted is partial, whatever its items carry.
	coded := &mq.BatchError{Errs: []error{nil, mq.ErrClosed}}
	if got := mq.CodeOf(coded); got != mq.CodePartial {
		t.Fatalf("CodeOf = %v, want CodePartial: the batch's own code, not the first failing item's", got)
	}
	if !errors.Is(coded, mq.ErrClosed) {
		t.Fatal("errors.Is does not reach an item's own sentinel through the batch")
	}
	empty := &mq.BatchError{}
	if got := empty.Failed(); got != 0 {
		t.Fatalf("an empty BatchError reports %d failures", got)
	}
	if got := empty.Error(); !strings.Contains(got, "batch of 0") {
		t.Fatalf("an empty BatchError renders %q", got)
	}
}
