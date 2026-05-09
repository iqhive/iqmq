package iqmq_test

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// TestClientSurface pins the answers that do not involve a message.
func TestClientSurface(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	if got := cl.Info(); got.Name != "fake" || got.DriverVersion != "v0" {
		t.Fatalf("Info = %+v", got)
	}
	if got := cl.Info().BrokerVersion; got != "" {
		t.Fatalf("BrokerVersion = %q, want empty: the specification makes an empty string \"the driver did not say\"", got)
	}
	if !cl.Capabilities().Has(mq.FeatureExplicitAck) {
		t.Fatal("Capabilities does not report the backend's own answer")
	}
	if cl.Capabilities().Has(mq.FeatureTransactions) {
		t.Fatal("Capabilities reports a feature the backend never advertised")
	}
	if cl.Native() != any(b) {
		t.Fatal("Native did not return the driver's own client")
	}
	if err := cl.Ping(context.Background()); err != nil {
		t.Fatalf("Ping = %v", err)
	}
	if got := cl.Health(); got.State != mq.HealthReady {
		t.Fatalf("Health = %+v, want ready", got)
	}
	if _, err := cl.Admin(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Admin on a backend with none = %v, want ErrUnsupported", err)
	}
}

// TestClientRejectsAMissingBackend pins the price of the frozen signature:
// NewClient returns no error, so a client built over nothing reports the
// failure from every operation it offers rather than panicking.
func TestClientRejectsAMissingBackend(t *testing.T) {
	cl := mq.NewClient(nil)
	ctx := context.Background()
	if got := mq.CodeOf(cl.Ping(ctx)); got != mq.CodeInvalidConfig {
		t.Fatalf("Ping on a client with no backend = %v, want CodeInvalidConfig", got)
	}
	if _, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("Publisher = %v, want CodeInvalidConfig", err)
	}
	if _, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "s"}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("Consumer = %v, want CodeInvalidConfig", err)
	}
	if err := cl.Publish(ctx, "orders", &mq.Message{}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("Publish = %v, want CodeInvalidConfig", err)
	}
	if got := cl.Health(); got.State != mq.HealthDegraded || got.Err == nil {
		t.Fatalf("Health = %+v, want degraded with a reason", got)
	}
	if cl.Native() != nil {
		t.Fatal("Native returned something for a client with no backend")
	}
	// A malformed option is the same kind of failure.
	bad := mq.NewClient(newFakeBroker(fakeConfig{}, "orders"), mq.ClientOption{})
	if got := mq.CodeOf(bad.Ping(ctx)); got != mq.CodeInvalidArgument {
		t.Fatalf("a client built with a zero ClientOption = %v, want CodeInvalidArgument", got)
	}
	nilClock := mq.NewClient(newFakeBroker(fakeConfig{}, "orders"), mq.WithClock(nil))
	if got := mq.CodeOf(nilClock.Ping(ctx)); got != mq.CodeInvalidArgument {
		t.Fatalf("a client built with a nil Clock = %v, want CodeInvalidArgument", got)
	}
}

// fakeAdminBackend is a backend with the optional topology surface.
type fakeAdminBackend struct {
	*fakeBroker
	ensured []string
}

// Admin is the optional AdminBackend, asserted once when the client is
// built.
func (b *fakeAdminBackend) Admin() (mq.Admin, error) { return b, nil }

func (b *fakeAdminBackend) EnsureTopic(_ context.Context, spec mq.TopicSpec) error {
	b.ensured = append(b.ensured, spec.Name)
	return nil
}

func (b *fakeAdminBackend) DeleteTopic(context.Context, string) error { return nil }

func (b *fakeAdminBackend) ListTopics(context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		yield("orders", nil)
	}
}

func (b *fakeAdminBackend) EnsureSubscription(context.Context, mq.SubscriptionSpec) error { return nil }

func (b *fakeAdminBackend) DeleteSubscription(context.Context, string, string) error { return nil }

func (b *fakeAdminBackend) Purge(context.Context, string, string) error { return nil }

func (b *fakeAdminBackend) Stats(_ context.Context, topic, sub string) (mq.QueueStats, error) {
	c := b.counts(topic, sub)
	return mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: int64(c.ready), Known: true},
		InFlight: mq.Limit[int64]{Value: int64(c.inflight), Known: true},
	}, nil
}

// TestClientAdmin pins the surface and the optional-interface rule: a
// backend that has an administrative API gets one, a backend that has none
// answers ErrUnsupported, and an unreported statistic is unknown rather than
// zero.
func TestClientAdmin(t *testing.T) {
	b := &fakeAdminBackend{fakeBroker: newFakeBroker(fakeConfig{}, "orders")}
	cl := mq.NewClient(b)
	defer func() { _ = cl.Close() }()
	adm, err := cl.Admin()
	if err != nil {
		t.Fatalf("Admin = %v", err)
	}
	ctx := context.Background()
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if len(b.ensured) != 1 || b.ensured[0] != "orders" {
		t.Fatalf("EnsureTopic recorded %v", b.ensured)
	}
	topics := 0
	for name, err := range adm.ListTopics(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if name != "orders" {
			t.Fatalf("ListTopics yielded %q", name)
		}
		topics++
	}
	if topics != 1 {
		t.Fatalf("ListTopics yielded %d topics, want 1", topics)
	}
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 2)
	d := receiveOne(t, c)
	stats, err := adm.Stats(ctx, "orders", "billing")
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Depth.Known || stats.Depth.Value != 1 {
		t.Fatalf("Stats.Depth = %+v, want a known 1", stats.Depth)
	}
	if !stats.InFlight.Known || stats.InFlight.Value != 1 {
		t.Fatalf("Stats.InFlight = %+v, want a known 1", stats.InFlight)
	}
	if stats.Lag.Known {
		t.Fatal("Stats.Lag is reported known by a broker that never set it: an unknown must not be an overloaded zero ")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

// errDegraded is the reason the health-reporting backend gives.
var errDegraded = errors.New("the fake broker is degraded")

// fakeHealthBackend is a backend with the optional health surface.
type fakeHealthBackend struct {
	*fakeBroker
	state mq.HealthState
}

// Health is what the specification asks a driver with background work to report.
func (b *fakeHealthBackend) Health() mq.HealthSnapshot {
	return mq.HealthSnapshot{State: b.state, Err: errDegraded}
}

// TestClientHealthComesFromTheDriver pins that a driver's own health answer
// wins while the client is open, and that a closed client reports closed
// whatever the driver says.
func TestClientHealthComesFromTheDriver(t *testing.T) {
	b := &fakeHealthBackend{fakeBroker: newFakeBroker(fakeConfig{}, "orders"), state: mq.HealthDegraded}
	cl := mq.NewClient(b)
	if got := cl.Health(); got.State != mq.HealthDegraded {
		t.Fatalf("Health = %+v, want the driver's degraded answer", got)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cl.Health(); got.State != mq.HealthClosed {
		t.Fatalf("Health after close = %+v, want closed", got)
	}
}

// TestClientClosesItsChildren pins the specification's "closing a client closes all
// child handles", for both stops, and the idempotence the specification requires of
// both.
func TestClientClosesItsChildren(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(cl *mq.Client) error
	}{
		{"close", func(cl *mq.Client) error { return cl.Close() }},
		{"shutdown", func(cl *mq.Client) error { return cl.Shutdown(context.Background()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{}, "orders")
			cl := mq.NewClient(b)
			ctx := context.Background()
			p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				t.Fatal(err)
			}
			c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.stop(cl); err != nil {
				t.Fatalf("stopping the client: %v", err)
			}
			if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("publishing through a child of a closed client = %v, want ErrClosed", err)
			}
			dst := make([]mq.Delivery, 1)
			if n, err := c.Receive(ctx, dst); n != 0 || !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("receiving from a child of a closed client = (%d, %v), want (0, ErrClosed)", n, err)
			}
			// Idempotent, in every combination.
			if err := tc.stop(cl); err != nil {
				t.Fatalf("a second stop = %v, want nil", err)
			}
			if err := cl.Close(); err != nil {
				t.Fatalf("Close after the stop = %v, want nil", err)
			}
			if err := cl.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown after the stop = %v, want nil", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("closing an already-closed publisher = %v, want nil", err)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("closing an already-closed consumer = %v, want nil", err)
			}
			// A new handle on a closed client is refused rather than opened
			// and orphaned.
			if _, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"}); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("opening a publisher on a closed client = %v, want ErrClosed", err)
			}
			if _, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("opening a consumer on a closed client = %v, want ErrClosed", err)
			}
		})
	}
}

// TestClientPublishIsBoundPerTopic pins the winner as the client's
// convenience path (ADR-021, Bound): the client keeps one publisher per
// topic, so a second publication to a topic opens no second sink.
func TestClientPublishIsBoundPerTopic(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders", "audit")
	cl := mq.NewClient(b)
	defer func() { _ = cl.Close() }()
	ctx := context.Background()
	for range 4 {
		if err := cl.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		if err := cl.Publish(ctx, "audit", &mq.Message{Body: []byte("y")}); err != nil {
			t.Fatal(err)
		}
	}
	// An empty topic falls back to Message.Topic, which is what makes the
	// field meaningful on this path.
	if err := cl.Publish(ctx, "", &mq.Message{Topic: "orders", Body: []byte("z")}); err != nil {
		t.Fatalf("publishing with the topic in the message = %v", err)
	}
	if err := cl.Publish(ctx, "", &mq.Message{Body: []byte("z")}); !errors.Is(err, mq.ErrInvalidTopic) {
		t.Fatalf("publishing with no topic at all = %v, want ErrInvalidTopic", err)
	}
	if err := cl.Publish(ctx, "unknown", &mq.Message{Body: []byte("z")}); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("publishing to a topic the broker does not have = %v, want ErrTopicNotFound", err)
	}
	if got := b.nativeCalls(); got != 9 {
		t.Fatalf("the driver saw %d publications, want 9", got)
	}
}

// TestPublisherIsBoundToItsTopic pins the other half: a bound
// publisher does not re-read its destination per message, so Message.Topic
// is ignored.
func TestPublisherIsBoundToItsTopic(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders", "audit")
	cl := mq.NewClient(b)
	defer func() { _ = cl.Close() }()
	ctx := context.Background()
	if _, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatal(err)
	}
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Topic() != "orders" {
		t.Fatalf("Topic = %q", p.Topic())
	}
	// The message names another topic; the bound publisher ignores it.
	if err := p.Publish(ctx, &mq.Message{Topic: "audit", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := b.counts("orders", "billing"); got.published != 1 {
		t.Fatalf("the publication reached %q %d times, want 1", "orders", got.published)
	}
	if got := p.Stats(); got.Published != 1 || got.Failed != 0 || got.Pending != 0 {
		t.Fatalf("Stats = %+v, want 1 published", got)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush on a driver that buffers nothing = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Flush after close = %v, want ErrClosed", err)
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish after close = %v, want ErrClosed", err)
	}
	if err := p.PublishBatch(ctx, []*mq.Message{{Body: []byte("x")}}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("PublishBatch after close = %v, want ErrClosed", err)
	}
	if err := p.Publish(ctx, nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("publishing a nil message = %v, want CodeInvalidArgument", err)
	}
	// A closed handle answers for being closed and not for an option it
	// will never send, so the specification's "after close all ops return ErrClosed"
	// holds without exception.
	if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}, mq.Delay(time.Minute)); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish of an unsupported option after close = %v, want ErrClosed", err)
	}
}

// TestConfigValidation pins the other cost: a config struct's zero
// value is a legal expression, so what a positional constructor would have
// refused at compile time has to be refused at construction — before any
// I/O, and with a code a caller can branch on.
func TestConfigValidation(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	ctx := context.Background()
	t.Run("consumer", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  mq.ConsumerConfig
			code mq.ErrorCode
		}{
			{"empty", mq.ConsumerConfig{}, mq.CodeInvalidConfig},
			{"notopic", mq.ConsumerConfig{Subscription: "billing"}, mq.CodeInvalidConfig},
			{"nosubscription", mq.ConsumerConfig{Topic: "orders"}, mq.CodeInvalidConfig},
			{"negativeprefetch", mq.ConsumerConfig{Topic: "orders", Subscription: "b", Prefetch: -1}, mq.CodeInvalidConfig},
			{"prefetchtoolarge", mq.ConsumerConfig{Topic: "orders", Subscription: "b", Prefetch: mq.MaxPrefetch + 1}, mq.CodeInvalidConfig},
			{"negativeinflight", mq.ConsumerConfig{Topic: "orders", Subscription: "b", MaxInFlight: -1}, mq.CodeInvalidConfig},
			{"negativeackdeadline", mq.ConsumerConfig{Topic: "orders", Subscription: "b", AckDeadline: -time.Second}, mq.CodeInvalidConfig},
			{"unknownmetadata", mq.ConsumerConfig{Topic: "orders", Subscription: "b", Metadata: mq.MetadataMode(9)}, mq.CodeInvalidConfig},
			{"unmetrequirement", mq.ConsumerConfig{
				Topic: "orders", Subscription: "b",
				Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureTransactions, mq.Native)},
			}, mq.CodeUnsupported},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := b.nativeCalls()
				c, err := cl.Consumer(ctx, tc.cfg)
				if got := mq.CodeOf(err); got != tc.code {
					t.Fatalf("Consumer(%+v) = %v (%v), want %v", tc.cfg, err, got, tc.code)
				}
				if c != nil {
					t.Fatal("a refused Consumer returned a handle")
				}
				if after := b.nativeCalls(); after != before {
					t.Fatal("a refused configuration reached the driver")
				}
			})
		}
		// The defaults the specification states are filled in, and the source is opened
		// with them: the fake refuses a source whose prefetch is zero.
		good, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "defaults"})
		if err != nil {
			t.Fatalf("a minimal configuration = %v", err)
		}
		if err := good.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("publisher", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  mq.PublisherConfig
			code mq.ErrorCode
		}{
			{"empty", mq.PublisherConfig{}, mq.CodeInvalidConfig},
			{"unknownconfirmation", mq.PublisherConfig{Topic: "orders", Confirmation: mq.ConfirmationMode(9)}, mq.CodeInvalidConfig},
			{"unknownmetadata", mq.PublisherConfig{Topic: "orders", Metadata: mq.MetadataMode(9)}, mq.CodeInvalidConfig},
			{"invalidtopic", mq.PublisherConfig{Topic: "not a topic"}, mq.CodeInvalidArgument},
			{"unmetrequirement", mq.PublisherConfig{
				Topic:        "orders",
				Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureTransactions, mq.Native)},
			}, mq.CodeUnsupported},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				p, err := cl.Publisher(ctx, tc.cfg)
				if got := mq.CodeOf(err); got != tc.code {
					t.Fatalf("Publisher(%+v) = %v (%v), want %v", tc.cfg, err, got, tc.code)
				}
				if p != nil {
					t.Fatal("a refused Publisher returned a handle")
				}
			})
		}
	})
}

// TestStartPositionResolves pins the start positions and
// default: the zero value is "the caller did not say" and resolves to
// latest, so a consumer of an unbounded topic does not replay its history by
// omission.
func TestStartPositionResolves(t *testing.T) {
	var unset mq.StartPosition
	if unset.Kind() != mq.StartKindUnset {
		t.Fatalf("the zero StartPosition is %v", unset.Kind())
	}
	if got := unset.Resolve(); got.Kind() != mq.StartKindLatest {
		t.Fatalf("the zero StartPosition resolves to %v, want latest ", got.Kind())
	}
	at := time.Unix(1700000000, 0)
	cases := []struct {
		pos  mq.StartPosition
		kind mq.StartKind
	}{
		{mq.StartLatest(), mq.StartKindLatest},
		{mq.StartEarliest(), mq.StartKindEarliest},
		{mq.StartAt(at), mq.StartKindTime},
		{mq.StartAtSequence(42), mq.StartKindSequence},
	}
	for _, tc := range cases {
		if got := tc.pos.Kind(); got != tc.kind {
			t.Fatalf("kind = %v, want %v", got, tc.kind)
		}
		if got := tc.pos.Resolve(); got.Kind() != tc.kind {
			t.Fatalf("Resolve changed %v into %v", tc.kind, got.Kind())
		}
	}
	if got := mq.StartAt(at).Time(); !got.Equal(at) {
		t.Fatalf("StartAt(%v).Time() = %v", at, got)
	}
	if got := mq.StartAtSequence(42).Sequence(); got != 42 {
		t.Fatalf("StartAtSequence(42).Sequence() = %d", got)
	}
	if got := mq.StartAt(at).Sequence(); got != 0 {
		t.Fatalf("StartAt(...).Sequence() = %d, want 0", got)
	}
}

// TestHooksFire pins the five observation points: each one runs with the
// event the operation it observes produced, and the durations come from the
// client's clock — which is what makes WithClock's adapter load-bearing
// rather than decorative.
func TestHooksFire(t *testing.T) {
	var (
		starts   []mq.PublishStartEvent
		dones    []mq.PublishDoneEvent
		receives []mq.ReceiveEvent
		settles  []mq.SettleEvent
	)
	fake := clock.NewFake(time.Unix(1000, 0))
	b := newFakeBroker(fakeConfig{}, "orders")
	cl := mq.NewClient(b,
		mq.WithClock(newTestClock(fake)),
		mq.WithHooks(mq.Hooks{
			OnPublishStart: func(_ context.Context, e mq.PublishStartEvent) { starts = append(starts, e) },
			OnPublishDone:  func(_ context.Context, e mq.PublishDoneEvent) { dones = append(dones, e) },
			OnReceive:      func(_ context.Context, e mq.ReceiveEvent) { receives = append(receives, e) },
			OnSettle:       func(_ context.Context, e mq.SettleEvent) { settles = append(settles, e) },
			OnReconnect:    func(context.Context, mq.ReconnectEvent) {},
		}),
	)
	defer func() { _ = cl.Close() }()
	ctx := context.Background()
	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Publish(ctx, "orders", &mq.Message{Body: []byte("four")}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 1 || starts[0].Topic != "orders" || starts[0].Messages != 1 || starts[0].Bytes != 4 {
		t.Fatalf("OnPublishStart saw %+v", starts)
	}
	if len(dones) != 1 || dones[0].Err != nil || dones[0].Bytes != 4 {
		t.Fatalf("OnPublishDone saw %+v", dones)
	}
	d := receiveOne(t, c)
	if len(receives) != 1 || receives[0].Messages != 1 || receives[0].Subscription != "billing" {
		t.Fatalf("OnReceive saw %+v", receives)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if len(settles) != 1 || settles[0].Kind != mq.SettleAck || settles[0].Err != nil || settles[0].Attempt != 1 {
		t.Fatalf("OnSettle saw %+v", settles)
	}
	if got := settles[0].Kind.String(); got != "ack" {
		t.Fatalf("SettleKind.String() = %q, want the metric label \"ack\"", got)
	}
	// A failed settlement is reported too, with the failure the specification names.
	if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatal(err)
	}
	if len(settles) != 2 || !errors.Is(settles[1].Err, mq.ErrAlreadySettled) {
		t.Fatalf("OnSettle saw %+v, want the second settlement's failure", settles)
	}
}

// testClock adapts internal/clock's Clock to the root's, which is the
// direction a test needs and the mirror of the adapter WithClock uses.
type testClock struct{ c clock.Clock }

func newTestClock(c clock.Clock) mq.Clock { return testClock{c: c} }

func (t testClock) Now() time.Time { return t.c.Now() }

func (t testClock) NewTimer(d time.Duration) mq.Timer { return t.c.NewTimer(d) }

// TestReceiveEdges pins the receive path's edges: an empty slice is a no-op,
// a context that is already done is reported, and a driver that breaks the
// SPI's contract is reported rather than trusted.
func TestReceiveEdges(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	if n, err := c.Receive(context.Background(), nil); n != 0 || err != nil {
		t.Fatalf("Receive into an empty slice = (%d, %v), want (0, nil)", n, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dst := make([]mq.Delivery, 1)
	if n, err := c.Receive(ctx, dst); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive with a cancelled context = (%d, %v), want (0, context.Canceled)", n, err)
	}
}

// brokenSource is a driver that breaks Fetch contract in the two ways
// the core can detect.
type brokenSource struct {
	count       int
	nilMessages bool
}

func (s *brokenSource) Fetch(_ context.Context, dst []mq.DriverDelivery) (int, error) {
	if s.nilMessages {
		for i := range dst {
			dst[i] = mq.DriverDelivery{}
		}
		return len(dst), nil
	}
	return s.count, nil
}

func (s *brokenSource) Settler() mq.Settler { return nil }
func (s *brokenSource) Close() error        { return nil }

// brokenBackend hands out brokenSource.
type brokenBackend struct {
	*fakeBroker
	src *brokenSource
}

func (b *brokenBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return b.src, nil
}

// TestReceiveRefusesABrokenDriver pins that a driver which reports more
// deliveries than the buffer holds, or a delivery with no message, is
// reported rather than trusted: the core reads dst[:n] and a wrong n is a
// read past what the driver filled.
func TestReceiveRefusesABrokenDriver(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  *brokenSource
	}{
		{"countpastthebuffer", &brokenSource{count: 99}},
		{"nilmessage", &brokenSource{nilMessages: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &brokenBackend{fakeBroker: newFakeBroker(fakeConfig{}, "orders"), src: tc.src}
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]mq.Delivery, 2)
			n, err := c.Receive(context.Background(), dst)
			if err == nil {
				t.Fatalf("Receive from a broken driver = (%d, nil), want an error", n)
			}
			if n != 0 {
				t.Fatalf("Receive from a broken driver handed over %d deliveries", n)
			}
		})
	}
}

// TestShutdownWithACancelledContext pins the contract for the graceful
// stop (ADR-006, Shutdown): a context that is already done makes it report
// that context's error and still close, which is the case
// x/apibench/b11's StopScripts call gracefulcancelled.
func TestShutdownWithACancelledContext(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders")
	cl := mq.NewClient(b)
	ctx := context.Background()
	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	if err := cl.Shutdown(done); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown with a cancelled context = %v, want nil or context.Canceled", err)
	}
	// It still closes: the specification's "never waits indefinitely" does not mean
	// "does not close". A caller whose bound expired is told so and the run
	// goes on, so the prompt Close — which waits for it — is what makes the
	// observation deterministic, and it is the order x/apibench/b11's
	// RunStop checks the same script in.
	if err := cl.Close(); err != nil {
		t.Fatalf("Close after a cancelled Shutdown = %v, want nil", err)
	}
	dst := make([]mq.Delivery, 1)
	if n, err := c.Receive(ctx, dst); n != 0 || !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Receive after a cancelled Shutdown = (%d, %v), want (0, ErrClosed)", n, err)
	}
}

// TestCloseUnblocksAParkedReceive pins the specification's "Close unblocks blocked
// operations" across the SPI boundary: the fetch is parked inside the driver,
// and the specification makes Source.Close responsible for waking it — which is the
// obligation the core relies on instead of merging the caller's context with
// the handle's on every receive.
func TestCloseUnblocksAParkedReceive(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(cl *mq.Client, c *mq.ConsumerHandle) error
	}{
		{"consumerclose", func(_ *mq.Client, c *mq.ConsumerHandle) error { return c.Close() }},
		{"clientclose", func(cl *mq.Client, _ *mq.ConsumerHandle) error { return cl.Close() }},
		{"consumershutdown", func(_ *mq.Client, c *mq.ConsumerHandle) error {
			return c.Shutdown(context.Background())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{}, "orders")
			cl := mq.NewClient(b)
			c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatal(err)
			}
			parked := make(chan error, 1)
			go func() {
				dst := make([]mq.Delivery, 1)
				_, err := c.Receive(context.Background(), dst)
				parked <- err
			}()
			// Give the fetch a moment to reach the driver and park: the
			// broker has no messages, so the only thing that can return it
			// is the close.
			runtime.Gosched()
			if err := tc.stop(cl, c); err != nil {
				t.Fatalf("stopping: %v", err)
			}
			select {
			case err := <-parked:
				if !errors.Is(err, mq.ErrClosed) {
					t.Fatalf("the parked Receive returned %v, want ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the parked Receive was never unblocked by the close")
			}
			if err := cl.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
