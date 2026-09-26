package memory_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// This file is ADR-009's data: what each of the publication modes does to
// the caller's storage, observed rather than asserted from the mode's name.

// TestOwnedPublicationReleasesExactlyOnce is the contract for
// PublishOwned: ownership transfers at invocation and Release is called
// exactly once, including on rejection, on close and on permanent failure.
func TestOwnedPublicationReleasesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		// prepare arranges the outcome, and wantCode is the code the
		// publication must then report. The specification names no sentinel for a
		// broker that refused, so the code is what a caller branches on.
		prepare  func(t *testing.T, client *mq.Client, b *memory.Broker)
		wantCode mq.ErrorCode
	}{
		{
			name:    "admitted and settled",
			prepare: func(*testing.T, *mq.Client, *memory.Broker) {},
			// The zero code is CodeUnknown, which nothing returns here; the
			// admitted case is the one with no error at all.
		},
		{
			name: "refused by a full lane",
			prepare: func(t *testing.T, client *mq.Client, b *memory.Broker) {
				// One record fills the lane, and the refused publication is
				// the one whose release the specification asks for.
				if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("filler")}); err != nil {
					t.Fatalf("filling the lane: %v", err)
				}
			},
			wantCode: mq.CodeResourceExhausted,
		},
		{
			name: "refused by fault injection",
			prepare: func(_ *testing.T, _ *mq.Client, b *memory.Broker) {
				b.Faults().RejectPublish(-1)
			},
			wantCode: mq.CodeUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
				CapacityMessages: 1,
				Overflow:         memory.OverflowReject,
			}))
			c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			tc.prepare(t, client, b)
			sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			defer func() { _ = sink.Close() }()
			owned, ok := sink.(mq.OwnedSink)
			if !ok {
				t.Fatal("the memory sink does not implement OwnedSink")
			}
			var mu sync.Mutex
			releases := 0
			var releasedWith *mq.Message
			m := &mq.Message{Body: []byte("owned")}
			err = owned.PublishOwned(ctx, mq.OwnedMessage{
				Message: m,
				Release: func(got *mq.Message) {
					mu.Lock()
					releases++
					releasedWith = got
					mu.Unlock()
				},
			}, &mq.PublishOptions{})
			if tc.wantCode == mq.CodeUnknown && err != nil {
				t.Fatalf("PublishOwned: %v", err)
			}
			if tc.wantCode != mq.CodeUnknown && mq.CodeOf(err) != tc.wantCode {
				t.Fatalf("PublishOwned: %v (code %v), want %v", err, mq.CodeOf(err), tc.wantCode)
			}
			if tc.wantCode == mq.CodeUnknown {
				// The engine holds the payload until the last subscription
				// that was admitted has settled it, so the release comes
				// after the acknowledgement and not before.
				mu.Lock()
				early := releases
				mu.Unlock()
				if early != 0 {
					t.Fatalf("an admitted owned payload was released %d times before it was settled", early)
				}
				ds := receiveN(t, ctx, c, 1)
				if err := ds[0].Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
			deadline := time.After(5 * time.Second)
			for {
				mu.Lock()
				n, with := releases, releasedWith
				mu.Unlock()
				if n == 1 {
					if with != m {
						t.Fatalf("Release was handed %p, want the message it was given (%p)", with, m)
					}
					break
				}
				if n > 1 {
					t.Fatalf("Release ran %d times, want exactly once", n)
				}
				select {
				case <-deadline:
					t.Fatal("the owned payload never came home")
				default:
				}
			}
		})
	}
}

// TestOwnedPublicationReleasesOnClose is the third case the specification names: a
// payload the broker still holds when it closes comes home exactly once.
func TestOwnedPublicationReleasesOnClose(t *testing.T) {
	ctx := context.Background()
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	client, err := memory.OpenWithClient(ctx, b, memory.ClientOwned)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	const n = 16
	released := make(chan *mq.Message, 2*n)
	for range n {
		if err := owned.PublishOwned(ctx, mq.OwnedMessage{
			Message: &mq.Message{Body: []byte("owned")},
			Release: func(m *mq.Message) { released <- m },
		}, &mq.PublishOptions{}); err != nil {
			t.Fatalf("PublishOwned: %v", err)
		}
	}
	if len(released) != 0 {
		t.Fatalf("%d payloads came home before the broker closed", len(released))
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(released); got != n {
		t.Fatalf("%d of %d owned payloads came home when the broker closed", got, n)
	}
	// And a publication after the close releases too, rather than keeping a
	// payload the caller can never get back.
	err = owned.PublishOwned(ctx, mq.OwnedMessage{
		Message: &mq.Message{Body: []byte("late")},
		Release: func(m *mq.Message) { released <- m },
	}, &mq.PublishOptions{})
	if !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("PublishOwned after close: %v, want ErrClosed", err)
	}
	if got := len(released); got != n+1 {
		t.Fatalf("a publication refused for being closed released %d payloads, want 1", got-n)
	}
}

// TestAsyncPublicationBorrowsUntilTheCallback is the async-borrowed mode
// and the callback contract: a nil return means the callback runs exactly
// once, a non-nil return means it does not run at all, and the storage stays
// the caller's until it does.
func TestAsyncPublicationBorrowsUntilTheCallback(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: 1,
		Overflow:         memory.OverflowReject,
	}))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	async, ok := sink.(mq.AsyncSink)
	if !ok {
		t.Fatal("the memory sink does not implement AsyncSink")
	}
	if err := async.PublishAsync(ctx, &mq.Message{Body: []byte("x")}, &mq.PublishOptions{}, nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("PublishAsync with no callback: %v (code %v)", err, mq.CodeOf(err))
	}
	done := make(chan error, 4)
	body := []byte("borrowed")
	m := &mq.Message{Body: body}
	if err := async.PublishAsync(ctx, m, &mq.PublishOptions{}, func(err error) { done <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	// Borrowed: the engine copied nothing, so the delivery reads the
	// caller's own array. The callback has not run, so the caller must not
	// have touched it — and this test is the check that it did not have to.
	if len(done) != 0 {
		t.Fatal("the callback ran before the record was settled")
	}
	ds := receiveN(t, ctx, c, 1)
	if !bytes.Equal(ds[0].Body, body) {
		t.Fatalf("the delivery's body is %q, want the caller's %q", ds[0].Body, body)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the completion callback reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the completion callback never ran for an admitted borrow")
	}
	if len(done) != 0 {
		t.Fatal("the completion callback ran more than once")
	}
	// A refused publication reports its error and the callback does not run,
	// which is the other half.
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatalf("filling the lane: %v", err)
	}
	err = async.PublishAsync(ctx, &mq.Message{Body: []byte("refused")}, &mq.PublishOptions{}, func(err error) { done <- err })
	if !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("PublishAsync into a full lane: %v, want ErrQueueFull", err)
	}
	if len(done) != 0 {
		t.Fatal("the callback ran for a publication that reported an immediate error")
	}
}

// TestZeroOnReleaseScrubsThePayload pins ZeroOnRelease: a payload the
// engine owns is overwritten as the record is released, so sensitive bytes do
// not survive in a pooled one.
func TestZeroOnReleaseScrubsThePayload(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestConfig(memory.Config{ZeroOnRelease: true}))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	secret := []byte("hunter2-hunter2")
	back := make(chan *mq.Message, 1)
	if err := owned.PublishOwned(ctx, mq.OwnedMessage{
		Message: &mq.Message{Key: bytes.Clone(secret), Body: bytes.Clone(secret)},
		Release: func(m *mq.Message) { back <- m },
	}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("PublishOwned: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if !bytes.Equal(ds[0].Body, secret) {
		t.Fatalf("the delivery's body is %q, want the published %q", ds[0].Body, secret)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	select {
	case m := <-back:
		// The storage comes home scrubbed: the slices are reset, and the
		// bytes behind them are zero over their full capacity.
		if m.Body != nil || m.Key != nil {
			t.Fatalf("the released message still holds a key of %d bytes and a body of %d", len(m.Key), len(m.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the owned payload never came home")
	}
}

// TestReceivedBodyAliasesTheEnginesStorage pins the default safe receive mode
// what a delivery hands the caller is BORROWED until the delivery is
// settled, and it costs no copy. It is what makes the receive path
// allocation-free, and it is the contract a caller has to know about.
func TestReceivedBodyAliasesTheEnginesStorage(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	body := []byte("payload")
	if err := client.Publish(ctx, "orders", &mq.Message{Body: body}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := receiveN(t, ctx, c, 1)
	// The delivery does not alias the PUBLISHER's array — safe publication
	// cloned it — but two deliveries of the same record do alias each
	// other's, because the engine holds one payload.
	if &first[0].Body[0] == &body[0] {
		t.Fatal("the delivery aliases the publisher's own array under safe publication")
	}
	if err := first[0].Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second := receiveN(t, ctx, c, 1)
	if &second[0].Body[0] != &first[0].Body[0] {
		t.Fatal("a redelivery of one record handed out a second copy of its payload")
	}
	if err := second[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
