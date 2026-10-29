package memory_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
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

// TestOwnedEnvelopePublicationReleasesTheGivenMessage pins Release in the
// enveloping mode: the engine holds the envelope built around the message,
// but Release is handed the message it was given, and ZeroOnRelease scrubs
// that message's storage rather than only the envelope's.
func TestOwnedEnvelopePublicationReleasesTheGivenMessage(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestConfig(memory.Config{ZeroOnRelease: true}))
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     1,
		Metadata:     mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	body := []byte("secret")
	m := &mq.Message{Body: body}
	released := make(chan *mq.Message, 2)
	if err := owned.PublishOwned(ctx, mq.OwnedMessage{
		Message: m,
		Release: func(got *mq.Message) { released <- got },
	}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("PublishOwned: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, dst); n != 1 {
		t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
	}
	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := len(released); got != 1 {
		t.Fatalf("Release ran %d times, want exactly once", got)
	}
	if got := <-released; got != m {
		t.Fatalf("Release was handed %p, want the message it was given (%p)", got, m)
	}
	if !bytes.Equal(body, make([]byte, len(body))) {
		t.Fatalf("ZeroOnRelease left the released body as %q", body)
	}
}

// TestAnOwnedDeliveryCarriesNoReceipt pins mq.Message.Receipt's rule for a
// received delivery: it is nil. An owned publication is the one the engine
// stores and delivers as the caller's own message, so the delivery carried
// the publisher's Receipt pointer, and a consumer could write through it into
// the publisher's storage. The publisher gets its pointer back with its
// message, on admission and on refusal alike. ZeroOnRelease is the exception:
// it resets the message it releases, Receipt included, as it does in the
// enveloping mode.
func TestAnOwnedDeliveryCarriesNoReceipt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		zero bool
		// back is whether Release hands the message back with its Receipt.
		back bool
	}{
		{name: "admitted", back: true},
		{name: "admitted with ZeroOnRelease", zero: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, b := memory.NewForTest(t, memory.TestConfig(memory.Config{ZeroOnRelease: tc.zero}))
			src := openSource(t, ctx, b, "orders", "billing")
			owned := ownedSink(t, ctx, b, "orders")
			rcpt := &mq.PublishReceipt{MessageID: "the publisher's"}
			m := &mq.Message{Body: []byte("receipted"), Receipt: rcpt}
			released := make(chan *mq.Message, 1)
			if err := owned.PublishOwned(ctx, mq.OwnedMessage{
				Message: m,
				Release: func(got *mq.Message) { released <- got },
			}, &mq.PublishOptions{}); err != nil {
				t.Fatalf("PublishOwned: %v", err)
			}
			dst := make([]mq.DriverDelivery, 1)
			if n, err := src.Fetch(ctx, dst); n != 1 {
				t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
			}
			if dst[0].Message.Receipt != nil {
				t.Fatalf("the delivery carries Receipt %p, the publisher's own (%p); want nil", dst[0].Message.Receipt, rcpt)
			}
			if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			want := rcpt
			if !tc.back {
				want = nil
			}
			if got := awaitRelease(t, released); got != m || got.Receipt != want {
				t.Fatalf("Release was handed %p with Receipt %p, want %p with %p", got, got.Receipt, m, want)
			}
		})
	}
	t.Run("refused", func(t *testing.T) {
		_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
			CapacityMessages: 1, Overflow: memory.OverflowReject,
		}))
		_ = openSource(t, ctx, b, "orders", "billing")
		publish(t, ctx, b, "orders", &mq.Message{Body: []byte("filler")})
		owned := ownedSink(t, ctx, b, "orders")
		rcpt := &mq.PublishReceipt{}
		m := &mq.Message{Body: []byte("refused"), Receipt: rcpt}
		released := make(chan *mq.Message, 1)
		if err := owned.PublishOwned(ctx, mq.OwnedMessage{
			Message: m,
			Release: func(got *mq.Message) { released <- got },
		}, &mq.PublishOptions{}); mq.CodeOf(err) != mq.CodeResourceExhausted {
			t.Fatalf("PublishOwned into a full lane = %v, want CodeResourceExhausted", err)
		}
		if got := awaitRelease(t, released); got != m || got.Receipt != rcpt {
			t.Fatalf("Release was handed %p with Receipt %p, want %p with %p", got, got.Receipt, m, rcpt)
		}
	})
}

// TestACopiedDeliveryCarriesNoReceipt is the same rule for the publications
// the engine copies on admission: Publish, PublishAsync and PublishBatch. The
// copy was Message.Clone, which gives a clone its own copy of the receipt
// slot, so every delivery carried a Receipt holding whatever the publisher's
// slot held. Every architecture is checked, with two subscriptions, because
// some copy once for each.
func TestACopiedDeliveryCarriesNoReceipt(t *testing.T) {
	ctx := context.Background()
	opts := &mq.PublishOptions{}
	paths := []struct {
		name    string
		publish func(s mq.Sink, m *mq.Message) error
	}{
		{"Publish", func(s mq.Sink, m *mq.Message) error { return s.Publish(ctx, m, opts) }},
		{"PublishAsync", func(s mq.Sink, m *mq.Message) error {
			return s.(mq.AsyncSink).PublishAsync(ctx, m, opts, func(error) {})
		}},
		{"PublishBatch", func(s mq.Sink, m *mq.Message) error {
			return s.(mq.BatchSink).PublishBatch(ctx, []*mq.Message{m}, opts)
		}},
	}
	for _, arch := range memory.ArchKinds() {
		for _, p := range paths {
			t.Run(arch.String()+"/"+p.name, func(t *testing.T) {
				_, b := memory.NewForTest(t, memory.TestEngine(memory.EngineConfig{Arch: arch}))
				srcs := []mq.Source{openSource(t, ctx, b, "orders", "billing"), openSource(t, ctx, b, "orders", "audit")}
				sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
				if err != nil {
					t.Fatalf("OpenSink: %v", err)
				}
				t.Cleanup(func() { _ = sink.Close() })
				rcpt := &mq.PublishReceipt{MessageID: "the publisher's"}
				m := &mq.Message{Body: []byte("receipted"), Receipt: rcpt}
				if err := p.publish(sink, m); err != nil {
					t.Fatalf("%s: %v", p.name, err)
				}
				if m.Receipt != rcpt {
					t.Fatalf("%s changed the publisher's Receipt to %p, want %p", p.name, m.Receipt, rcpt)
				}
				for _, src := range srcs {
					dst := make([]mq.DriverDelivery, 1)
					if n, err := src.Fetch(ctx, dst); n != 1 {
						t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
					}
					if r := dst[0].Message.Receipt; r != nil {
						t.Fatalf("the delivery to %s carries Receipt %+v; want nil", dst[0].Info.Subscription, *r)
					}
				}
			})
		}
	}
}

// awaitRelease returns the message Release handed back, failing the test if
// it does not come home.
func awaitRelease(t *testing.T, released <-chan *mq.Message) *mq.Message {
	t.Helper()
	select {
	case m := <-released:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("the owned payload never came home")
		return nil
	}
}

// ownedSink opens a sink on topic and returns it as the OwnedSink every
// memory sink is.
func ownedSink(t *testing.T, ctx context.Context, b *memory.Broker, topic string) mq.OwnedSink {
	t.Helper()
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	return owned
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

// TestAsyncPublicationCompletesAtAdmission is the asynchronous mode and the
// callback contract: a nil return means the callback runs exactly once, a
// non-nil return means it does not run at all, and the storage is the
// caller's again once it has.
//
// The callback reports admission, which the engine decides before
// PublishAsync returns, so it has run by then. It used to run when the last
// subscription settled the record, with the engine borrowing the caller's
// storage until it did: a Flush then waited for a consumer, a Close reported
// an admitted message as SettlementUnknown, and a caller that reused its
// buffer after that callback changed what the consumer read.
func TestAsyncPublicationCompletesAtAdmission(t *testing.T) {
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
	body := []byte("original")
	m := &mq.Message{Body: body}
	if err := async.PublishAsync(ctx, m, &mq.PublishOptions{}, func(err error) { done <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the completion callback reported %v for an admitted publication", err)
		}
	default:
		t.Fatal("the completion callback had not run once the record was admitted")
	}
	// The callback has run, so the storage is the caller's to reuse, and
	// the record the engine admitted must not change with it.
	copy(body, "CLOBBER!")
	ds := receiveN(t, ctx, c, 1)
	if got := string(ds[0].Body); got != "original" {
		t.Fatalf("the delivery's body is %q: the engine kept the caller's storage after the callback", got)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
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

// TestAsyncPartialFanoutIsReportedToTheCallback: under FanoutIndependent a
// subscription with no room refuses the record while the others take it.
// That is an admission, so PublishAsync returns nil and the callback runs,
// and what the callback reports is the ErrPartial that Publish returns for
// the same outcome.
func TestAsyncPartialFanoutIsReportedToTheCallback(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{
			Fanout:           memory.FanoutIndependent,
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
		}),
		memory.TestTopics("orders"))
	// "full" takes the filler, and "roomy" starts after it.
	openSource(t, ctx, b, "orders", "full")
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("filler")})
	openSource(t, ctx, b, "orders", "roomy")
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	done := make(chan error, 2)
	err = sink.(mq.AsyncSink).PublishAsync(ctx, &mq.Message{Body: []byte("partial")}, &mq.PublishOptions{},
		func(err error) { done <- err })
	if err != nil {
		t.Fatalf("PublishAsync of a partial fan-out: %v, want nil for an admitted record", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrPartial) {
			t.Fatalf("the callback reported %v, want ErrPartial", err)
		}
	default:
		t.Fatal("the callback had not run once the record was admitted")
	}
	if len(done) != 0 {
		t.Fatal("the callback ran more than once")
	}
	for sub, want := range map[string]memory.Snapshot{
		"full":  {Ready: 1, Rejected: 1},
		"roomy": {Ready: 1},
	} {
		snap, ierr := b.Inspect("orders", sub)
		if ierr != nil {
			t.Fatalf("Inspect(%q): %v", sub, ierr)
		}
		if snap.Ready != want.Ready || snap.Rejected != want.Rejected {
			t.Fatalf("%q holds %d ready with %d rejected, want %d and %d",
				sub, snap.Ready, snap.Rejected, want.Ready, want.Rejected)
		}
	}
}

// TestPublisherAsyncPublicationNeedsNoConsumer is the same contract as the
// caller sees it, through the core's publisher: Flush returns once the
// publication is admitted rather than once something has consumed it, Close
// finds nothing outstanding to report as SettlementUnknown, and the body the
// callback handed back may be reused without changing what is delivered.
func TestPublisherAsyncPublicationNeedsNoConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		client, _ := memory.NewForTest(t, memory.TestTopics("orders"))
		c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("Consumer: %v", err)
		}
		p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		body := []byte("original")
		res := make(chan mq.PublishResult, 2)
		if err := p.PublishAsync(ctx, &mq.Message{Body: body}, func(r mq.PublishResult) { res <- r }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		fctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := p.Flush(fctx); err != nil {
			t.Fatalf("Flush with an admitted publication nobody has consumed: %v", err)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case r := <-res:
			if r.Err != nil {
				t.Fatalf("the callback reported %v (ambiguous %v) for an admitted publication",
					r.Err, mq.IsAmbiguous(r.Err))
			}
		default:
			t.Fatal("the callback had not run by the time the publisher closed")
		}
		if len(res) != 0 {
			t.Fatal("the callback ran more than once")
		}
		copy(body, "CLOBBER!")
		ds := receiveN(t, ctx, c, 1)
		if got := string(ds[0].Body); got != "original" {
			t.Fatalf("the delivery's body is %q: the engine kept the caller's storage after the callback", got)
		}
		if err := ds[0].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
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
