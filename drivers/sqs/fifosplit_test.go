package sqs_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestAFIFOBatchTheCoreSplitsStopsAGroupAcrossItsPieces is a FIFO batch of
// twelve, two more than the driver's MaxBatchMessages, so the core hands it
// to the driver in two pieces. m1 of group "b" is refused in the first
// SendMessageBatch call, and the driver holds group b back for the rest of
// that piece. The core sent m11 of group b in the second piece anyway, so it
// was on the queue ahead of the caller's retry of m1.
// TestAFIFOBatchStopsAGroupAtItsFirstFailure has ten messages and never
// reaches the split.
func TestAFIFOBatchTheCoreSplitsStopsAGroupAcrossItsPieces(t *testing.T) {
	if !coreHoldsAKeyAcrossPieces(t) {
		// CI tests this module against the published root module, and
		// v1.1.1 predates the hold; the version bump of the next release
		// turns the test on. The root module's own tests pin the hold.
		t.Skip("this build's root module does not hold a failed key back across the pieces of a split batch")
	}
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) {
		c.FIFO = true
		c.MaxMessageBytes = 1024
	})
	name := topic(t, "fifo-split")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// Three 300-byte messages fit under the 1024-byte limit, so the first
	// piece goes in four calls, 0-2, 3-5, 6-8 and 9, and m1's refusal is in
	// the first of them.
	const n = 12
	ms := make([]*mq.Message, n)
	group := func(i int) string { return map[bool]string{true: "a", false: "b"}[i%2 == 0] }
	pad := strings.Repeat("x", 300)
	for i := range ms {
		ms[i] = &mq.Message{Key: []byte(group(i)), Body: []byte("m" + strconv.Itoa(i) + pad)}
	}
	server.Fault(sqsfake.Fault{
		Op:     "SendMessageBatch",
		Bodies: map[string]string{string(ms[1].Body): "InternalError"},
		Count:  -1,
	})
	err = pub.PublishBatch(t.Context(), ms)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != n {
		t.Fatalf("PublishBatch = %v, want a *BatchError with %d outcomes", err, n)
	}
	on := map[string]bool{}
	for _, body := range h.bodies(name) {
		on[body] = true
	}
	for i := 3; i < n; i++ {
		sent := on[string(ms[i].Body)]
		if group(i) == "a" {
			if be.Errs[i] != nil || !sent {
				t.Errorf("m%d of group a, which had no failure, reported %v (on the queue: %v)", i, be.Errs[i], sent)
			}
			continue
		}
		if sent {
			t.Errorf("m%d of group b was sent after m1 of its group was refused; the caller's retry of m1 lands behind it", i)
		}
		if !errors.Is(be.Errs[i], errcode.ErrNotAttempted) || !mq.IsRetryable(be.Errs[i]) {
			t.Errorf("m%d of group b reported %v, want a retryable not-attempted answer", i, be.Errs[i])
		}
	}
}

// coreHoldsAKeyAcrossPieces reports whether the linked root module holds a
// key back from the later pieces of a batch it splits, once a message of the
// key has failed. It asks through a driver that takes one message a call,
// keeps order per key and refuses the first message it is given.
func coreHoldsAKeyAcrossPieces(t *testing.T) bool {
	t.Helper()
	var caps mq.Capabilities
	if err := caps.Set(mq.FeatureOrderedByKey, mq.Support{Level: mq.Native, Scope: mq.ScopeKey}); err != nil {
		t.Fatalf("building the probe's capabilities: %v", err)
	}
	caps.SetLimits(mq.Limits{MaxBatchMessages: mq.Limit[int]{Value: 1, Known: true}})
	b := &probeBackend{caps: caps}
	cl := mq.NewClient(b)
	defer func() { _ = cl.Close() }()
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "probe"})
	if err != nil {
		t.Fatalf("opening the probe's publisher: %v", err)
	}
	_ = pub.PublishBatch(t.Context(), []*mq.Message{
		{Key: []byte("k"), Body: []byte("refused")},
		{Key: []byte("k"), Body: []byte("held")},
	})
	return b.sink.calls == 1
}

// probeBackend is the smallest driver the probe needs: a sink with a native
// batch method, and nothing to consume.
type probeBackend struct {
	caps mq.Capabilities
	sink probeSink
}

func (b *probeBackend) Info() mq.DriverInfo           { return mq.DriverInfo{Name: "probe"} }
func (b *probeBackend) Capabilities() mq.Capabilities { return b.caps }
func (b *probeBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	return &b.sink, nil
}

func (b *probeBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return nil, mq.ErrUnsupported
}
func (b *probeBackend) Ping(context.Context) error { return nil }
func (b *probeBackend) Native() any                { return nil }
func (b *probeBackend) Close() error               { return nil }

// probeSink refuses the first call it is given and accepts the rest.
type probeSink struct{ calls int }

func (s *probeSink) Publish(context.Context, *mq.Message, *mq.PublishOptions) error {
	return s.answer()
}

func (s *probeSink) PublishBatch(context.Context, []*mq.Message, *mq.PublishOptions) error {
	return s.answer()
}

func (s *probeSink) Close() error { return nil }

func (s *probeSink) answer() error {
	s.calls++
	if s.calls == 1 {
		return mq.ErrBackpressure
	}
	return nil
}
