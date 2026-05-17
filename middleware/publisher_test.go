package middleware_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/middleware"
)

// publishRecorder is an mq.Publisher that keeps what it was asked to publish
// and the options it was given.
type publishRecorder struct {
	mu      sync.Mutex
	sent    []*mq.Message
	batch   [][]*mq.Message
	opts    []mq.PublishOptions
	flushes int
	closes  int
	refuse  error
}

func (r *publishRecorder) Publish(_ context.Context, m *mq.Message, opts ...mq.PublishOption) error {
	if r.refuse != nil {
		return r.refuse
	}
	var o mq.PublishOptions
	if err := o.Apply(opts...); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, m.Clone())
	r.opts = append(r.opts, o)
	return nil
}

func (r *publishRecorder) PublishBatch(_ context.Context, ms []*mq.Message, opts ...mq.PublishOption) error {
	if r.refuse != nil {
		return r.refuse
	}
	var o mq.PublishOptions
	if err := o.Apply(opts...); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*mq.Message, len(ms))
	for i, m := range ms {
		if m != nil {
			out[i] = m.Clone()
		}
	}
	r.batch = append(r.batch, out)
	r.opts = append(r.opts, o)
	return nil
}

func (r *publishRecorder) Flush(context.Context) error {
	r.flushes++
	return nil
}

func (r *publishRecorder) Close() error {
	r.closes++
	return nil
}

var _ mq.Publisher = (*publishRecorder)(nil)

func TestWrapPublisherComposesOnceAndNotPerPublication(t *testing.T) {
	// The publish half rule 6. A wrapper that rebuilt the chain per
	// call would behave identically and cost a composition a message.
	var compositions atomic.Int64
	counting := func(next mq.PublishFunc) mq.PublishFunc {
		compositions.Add(1)
		return next
	}
	rec := &publishRecorder{}
	mw := []mq.PublishMiddleware{counting, counting, counting, nil, counting, counting}
	p := middleware.WrapPublisher(rec, mw...)
	for range 500 {
		if err := p.Publish(context.Background(), &mq.Message{Topic: "t", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if got := compositions.Load(); got != 5 {
		t.Fatalf("the publish chain was composed %d times over 500 publications, want 5 (once per middleware, and the nil one skipped)", got)
	}
}

func TestWrapPublisherAppliesTheChainOutermostFirst(t *testing.T) {
	var order []string
	mark := func(name string) mq.PublishMiddleware {
		return func(next mq.PublishFunc) mq.PublishFunc {
			return func(ctx context.Context, topic string, ms []*mq.Message) error {
				order = append(order, name)
				return next(ctx, topic, ms)
			}
		}
	}
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, mark("outer"), mark("inner"))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "outer,inner" {
		t.Fatalf("order = %v, want [outer inner]", order)
	}
}

func TestWrapPublisherRoutesOneMessageAndABatchDifferently(t *testing.T) {
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec)
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t", Body: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if len(rec.sent) != 1 || len(rec.batch) != 0 {
		t.Fatalf("a single publication reached %d Publish and %d PublishBatch calls, want 1 and 0: a driver's native single-message path must survive the wrapper", len(rec.sent), len(rec.batch))
	}
	ms := []*mq.Message{{Topic: "t", Body: []byte("a")}, {Topic: "t", Body: []byte("b")}}
	if err := p.PublishBatch(context.Background(), ms); err != nil {
		t.Fatal(err)
	}
	if len(rec.batch) != 1 || len(rec.batch[0]) != 2 {
		t.Fatalf("batches = %v, want one batch of two: the driver's native batch path must survive the wrapper", rec.batch)
	}
	if err := p.PublishBatch(context.Background(), nil); err != nil {
		t.Fatalf("PublishBatch of nothing = %v, want nil", err)
	}
	if len(rec.batch) != 1 {
		t.Fatal("an empty batch reached the publisher")
	}
}

func TestWrapPublisherCarriesPerCallOptions(t *testing.T) {
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.ID(func() string { return "generated" }))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t"},
		mq.Delay(time.Minute), mq.Priority(7)); err != nil {
		t.Fatal(err)
	}
	got := rec.opts[0]
	if got.Delay != time.Minute {
		t.Fatalf("Delay = %v, want a minute: PublishFunc has no room for options, so they travel in the context", got.Delay)
	}
	if !got.PrioritySet || got.Priority != 7 {
		t.Fatalf("Priority = %v/%v, want 7", got.Priority, got.PrioritySet)
	}
	// And a call with no options carries nothing at all.
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if rec.opts[1] != (mq.PublishOptions{}) {
		t.Fatalf("options = %+v for a call that named none, want the zero value", rec.opts[1])
	}
}

func TestWrapPublisherForwardsFlushAndClose(t *testing.T) {
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec)
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if rec.flushes != 1 || rec.closes != 1 {
		t.Fatalf("flushes = %d closes = %d, want 1 and 1", rec.flushes, rec.closes)
	}
	if p.Unwrap() != mq.Publisher(rec) {
		t.Fatal("Unwrap did not return the publisher underneath")
	}
}

func TestWrapPublisherReportsTheDriversFailure(t *testing.T) {
	rec := &publishRecorder{refuse: mq.ErrClosed}
	p := middleware.WrapPublisher(rec, middleware.ID(func() string { return "x" }))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t"}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish = %v, want the driver's own failure", err)
	}
}

func TestWrapPublisherRefusesANilPublisher(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WrapPublisher(nil) did not panic")
		}
	}()
	middleware.WrapPublisher(nil)
}

// ---------------------------------------------------------------- ID

func TestIDStampsOnlyWhatCarriesNone(t *testing.T) {
	var n int
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.ID(func() string {
		n++
		return "gen-" + string(rune('0'+n))
	}))
	ms := []*mq.Message{
		{Topic: "t"},
		{Topic: "t", ID: "the caller's own"},
		nil,
		{Topic: "t"},
	}
	if err := p.PublishBatch(context.Background(), ms); err != nil {
		t.Fatal(err)
	}
	got := rec.batch[0]
	if got[0].ID != "gen-1" {
		t.Fatalf("message 0 id = %q, want a generated one", got[0].ID)
	}
	if got[1].ID != "the caller's own" {
		t.Fatalf("message 1 id = %q; the caller's identifier must win -- it may be re-publishing a message whose identity must not change", got[1].ID)
	}
	if got[3].ID != "gen-2" {
		t.Fatalf("message 3 id = %q, want the second generated one", got[3].ID)
	}
}

func TestIDWithNoGeneratorCostsTheChainNothing(t *testing.T) {
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.ID(nil))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if rec.sent[0].ID != "" {
		t.Fatalf("id = %q, want none", rec.sent[0].ID)
	}
}

// ---------------------------------------------------------------- Timestamp

func TestTimestampStampsOnlyWhatCarriesNone(t *testing.T) {
	clk := newFakeClock()
	then := clk.Now().Add(-time.Hour)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.Timestamp(clk))
	ms := []*mq.Message{
		{Topic: "t"},
		{Topic: "t", Properties: mq.Properties{CreatedAt: then}},
		nil,
		{Topic: "t"},
	}
	if err := p.PublishBatch(context.Background(), ms); err != nil {
		t.Fatal(err)
	}
	got := rec.batch[0]
	if !got[0].Properties.CreatedAt.Equal(clk.Now()) {
		t.Fatalf("message 0 created at %v, want the clock's own %v", got[0].Properties.CreatedAt, clk.Now())
	}
	if !got[1].Properties.CreatedAt.Equal(then) {
		t.Fatalf("message 1 created at %v, want the caller's own %v", got[1].Properties.CreatedAt, then)
	}
	if !got[3].Properties.CreatedAt.Equal(got[0].Properties.CreatedAt) {
		t.Fatal("two messages of one batch got different timestamps; the batch was created by one call and an ordering between them is invented")
	}
}
