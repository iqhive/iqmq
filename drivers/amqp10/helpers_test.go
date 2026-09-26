package amqp10_test

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// The test fixture: a backend over the scripted peer of internal/client, and a
// profile shaped like the two brokers this core was written for.
//
// Nothing here starts a container and nothing here needs a network. Every
// scenario the driver has to get right is a value the peer is given, which is
// what makes the four critical tests deterministic rather than a race against a
// broker.

// testProfile is an Artemis-shaped profile: an address for a sink, a fully
// qualified queue name for a source, a delivery-delay annotation, a
// de-duplication property and a durable boundary.
//
// It is deliberately not a toy. Every seam a real profile has to fill is filled
// here, because a core whose tests only ever exercised the empty profile would
// not be exercising the seam at all.
func testProfile() aq.Profile {
	return aq.Profile{
		Driver:  "amqp10test",
		Name:    "artemislike",
		Version: "v0.0.0-test",
		SinkAddress: func(topic string) (string, error) {
			if topic == "" {
				return "", mq.ErrInvalidTopic
			}
			return topic, nil
		},
		SourceAddress: func(topic, sub string) (string, error) {
			if topic == "" || sub == "" {
				return "", mq.ErrInvalidTopic
			}
			return topic + "::" + sub, nil
		},
		Delay: aq.DelayRule{
			Slot: aq.SlotMessageAnnotation,
			Key:  "x-opt-delivery-delay",
			Max:  time.Hour,
		},
		NackDelay: aq.NackDelayRule{
			Supported:      true,
			Slot:           aq.SlotMessageAnnotation,
			Key:            "x-opt-redelivery-delay",
			DeliveryFailed: true,
			Max:            time.Hour,
		},
		Dedup:            aq.SlotRule{Slot: aq.SlotApplicationProperty, Key: "_AMQ_DUPL_ID"},
		Durable:          aq.DurableRule{Supported: true, Note: "the accepted disposition from a durable queue, which this broker writes before it accepts"},
		AnnotationPrefix: "x-opt-",
		AutoCreates:      true,
		Capabilities: func(aq.Facts) []aq.Claim {
			return []aq.Claim{
				aq.Native(mq.FeatureDurableStorage, mq.ScopeTopic),
				aq.Native(mq.FeatureDeadLetter, mq.ScopeTopic),
				aq.Native(mq.FeatureConsumerGroups, mq.ScopeGroup),
				aq.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup),
				aq.Native(mq.FeatureFanOut, mq.ScopeTopic),
				aq.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
					"Message.Key travels as the AMQP group-id property and this broker orders a message group"),
			}
		},
	}
}

// bareProfile is the smallest profile that can be opened: two addressing
// functions and a name. It is what a reader of [aq.Profile]'s documentation
// would write first, and a core that only worked with a full profile would be
// hiding a required field.
func bareProfile() aq.Profile {
	return aq.Profile{
		Driver:        "amqp10bare",
		SinkAddress:   func(topic string) (string, error) { return topic, nil },
		SourceAddress: func(topic, sub string) (string, error) { return topic + "::" + sub, nil },
	}
}

// testFacts is a peer that says it is an Artemis-shaped broker.
func testFacts() map[string]any {
	return map[string]any{"product": "apache-activemq-artemis", "version": "2.44.0", "platform": "test"}
}

// fixture is one backend over one scripted peer.
type fixture struct {
	tb   testing.TB
	peer *client.Fake
	b    *aq.Backend
}

// open builds a backend over a scripted peer, closing it when the test ends.
func open(tb testing.TB, p aq.Profile, cfg aq.Config) *fixture {
	tb.Helper()
	peer := client.NewFake(testFacts())
	return openWith(tb, p, cfg, peer)
}

// openWith is [open] over a peer the test has already scripted.
func openWith(tb testing.TB, p aq.Profile, cfg aq.Config, peer *client.Fake) *fixture {
	tb.Helper()
	b, err := aq.BackendWithDialer(context.Background(), p, cfg, peer)
	if err != nil {
		tb.Fatalf("opening the backend: %v", err)
	}
	f := &fixture{tb: tb, peer: peer, b: b}
	tb.Cleanup(func() {
		if err := b.Close(); err != nil {
			tb.Errorf("closing the backend: %v", err)
		}
	})
	return f
}

// sink opens a sink on the fixture.
func (f *fixture) sink(cfg mq.SinkConfig) *aq.Sink {
	f.tb.Helper()
	if cfg.Topic == "" {
		cfg.Topic = "topic"
	}
	s, err := f.b.OpenSink(context.Background(), cfg)
	if err != nil {
		f.tb.Fatalf("opening the sink: %v", err)
	}
	snk, ok := s.(*aq.Sink)
	if !ok {
		f.tb.Fatalf("OpenSink returned %T", s)
	}
	return snk
}

// source opens a source on the fixture.
func (f *fixture) source(cfg mq.SourceConfig) *aq.Source {
	f.tb.Helper()
	if cfg.Topic == "" {
		cfg.Topic = "topic"
	}
	if cfg.Subscription == "" {
		cfg.Subscription = "sub"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 4
	}
	s, err := f.b.OpenSource(context.Background(), cfg)
	if err != nil {
		f.tb.Fatalf("opening the source: %v", err)
	}
	src, ok := s.(*aq.Source)
	if !ok {
		f.tb.Fatalf("OpenSource returned %T", s)
	}
	return src
}

// receiver is the peer's receiving link for a source, waiting for the source's
// first fetch to attach one.
func (f *fixture) receiver(budget time.Duration) *client.FakeReceiver {
	f.tb.Helper()
	return f.receiverExcept(nil, budget)
}

// receiverExcept waits for a receiving link the caller has not already seen.
//
// Identity rather than a count, because a peer is free to drop a detached
// link from the list, which would make "more than before" false for a link
// that really is new.
func (f *fixture) receiverExcept(seen []*client.FakeReceiver, budget time.Duration) *client.FakeReceiver {
	f.tb.Helper()
	old := make(map[*client.FakeReceiver]bool, len(seen))
	for _, r := range seen {
		old[r] = true
	}
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if c := f.peer.LastConn(); c != nil {
			rs := c.Receivers()
			for i := len(rs) - 1; i >= 0; i-- {
				if !old[rs[i]] {
					return rs[i]
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	f.tb.Fatalf("the source did not attach a receiving link the test had not already seen within %v", budget)
	return nil
}

// sender is the peer's sending link, waiting for the sink's first publication
// to attach one.
func (f *fixture) sender(budget time.Duration) *client.FakeSender {
	f.tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if c := f.peer.LastConn(); c != nil {
			if ss := c.Senders(); len(ss) > 0 {
				return ss[len(ss)-1]
			}
		}
		time.Sleep(time.Millisecond)
	}
	f.tb.Fatalf("the sink did not attach a sending link within %v", budget)
	return nil
}

// message is a native message the peer can deliver, with a delivery tag a test
// can correlate a disposition by.
func message(tag, body string) *amqp.Message {
	return &amqp.Message{
		DeliveryTag: []byte(tag),
		Data:        [][]byte{[]byte(body)},
		Header:      &amqp.MessageHeader{Durable: true},
	}
}

// fetchOne fetches exactly one delivery, or fails.
func fetchOne(tb testing.TB, src *aq.Source, budget time.Duration) mq.DriverDelivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, dst)
	if err != nil {
		tb.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		tb.Fatalf("Fetch returned %d deliveries, want 1", n)
	}
	return dst[0]
}

// waitFor polls until cond holds, or fails.
//
// It polls a PREDICATE rather than sleeping, because every asynchronous thing in
// this driver — the supervisor noticing a dead connection, the asynchronous
// publication worker resolving a receipt — happens on a goroutine of its own,
// and a test that slept would be slow on a quiet machine and flaky on a busy
// one. This host runs about fifty other people's containers.
func waitFor(tb testing.TB, budget time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("timed out after %v waiting for %s", budget, what)
}

// assertCode fails unless err carries the portable code wanted.
func assertCode(tb testing.TB, err error, want mq.ErrorCode, what string) {
	tb.Helper()
	if got := mq.CodeOf(err); got != want {
		tb.Fatalf("%s: code %v (%v), want %v", what, got, err, want)
	}
}

// assertIs fails unless err matches the sentinel wanted.
func assertIs(tb testing.TB, err, want error, what string) {
	tb.Helper()
	if !errors.Is(err, want) {
		tb.Fatalf("%s: %v, want %v", what, err, want)
	}
}

// budget is the wall-clock allowance one asynchronous step gets. It is generous
// because the host is shared.
const budget = 5 * time.Second

// deliverAndFetch queues a message for a source's link and fetches it.
//
// The first fetch is what ATTACHES the link, so the message cannot be queued
// before it: the helper waits for the attach on a goroutine of its own and
// delivers there, which is what makes every source test one call rather than
// three.
func deliverAndFetch(tb testing.TB, f *fixture, src *aq.Source, tag, body string) (*client.FakeReceiver, mq.DriverDelivery) {
	tb.Helper()
	// Whatever links the peer already has are not the one this call is
	// waiting for: a second call after a detach must deliver on the
	// REATTACHED link, and the dead one is still listed.
	var seen []*client.FakeReceiver
	if c := f.peer.LastConn(); c != nil {
		seen = c.Receivers()
	}
	ready := make(chan *client.FakeReceiver, 1)
	go func() {
		rcv := f.receiverExcept(seen, budget)
		rcv.Deliver(message(tag, body))
		ready <- rcv
	}()
	d := fetchOne(tb, src, budget)
	return <-ready, d
}

// newPeer is a scripted peer with the test facts.
func newPeer() *client.Fake { return client.NewFake(testFacts()) }

// newPeerWith is a scripted peer that announces the connection properties
// given, which is what a profile's version gate reads.
func newPeerWith(props map[string]any) *client.Fake { return client.NewFake(props) }

// fakeAdmin is a topology surface a profile can supply, so that the Admin seam
// can be tested without a management endpoint.
//
// Every method answers ErrUnsupported: what the test is about is whether the
// SURFACE is reachable at all, which is the part the core decides.
type fakeAdmin struct{}

func (*fakeAdmin) EnsureTopic(context.Context, mq.TopicSpec) error { return mq.ErrUnsupported }
func (*fakeAdmin) DeleteTopic(context.Context, string) error       { return mq.ErrUnsupported }
func (*fakeAdmin) ListTopics(context.Context) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func (*fakeAdmin) EnsureSubscription(context.Context, mq.SubscriptionSpec) error {
	return mq.ErrUnsupported
}

func (*fakeAdmin) DeleteSubscription(context.Context, string, string) error { return mq.ErrUnsupported }

func (*fakeAdmin) Purge(context.Context, string, string) error { return mq.ErrUnsupported }

func (*fakeAdmin) Stats(context.Context, string, string) (mq.QueueStats, error) {
	return mq.QueueStats{}, mq.ErrUnsupported
}

// compile-time proof that the stub really is the SPI's surface.
var _ mq.Admin = (*fakeAdmin)(nil)

// sinkCtx is a background context, named so that a table-driven open reads as
// one line.
func sinkCtx() context.Context { return context.Background() }

// goroutines is the process's current goroutine count, which is how a test
// checks the specification's "no goroutine per message" without counting stacks.
func goroutines() int { return runtime.NumGoroutine() }

// itoa is strconv.Itoa under a shorter name, for the many places a test builds
// a delivery tag out of a loop index.
func itoa(i int) string { return strconv.Itoa(i) }

// testingAllocsPerRun is testing.AllocsPerRun, named here so that every
// allocation assertion in this package reads the same and so that the one place
// the count is measured is easy to find.
func testingAllocsPerRun(runs int, fn func()) float64 { return testing.AllocsPerRun(runs, fn) }

// clientHold is a send plan that withholds the disposition, named here because
// several tests want one and the literal reads badly inline.
func clientHold() client.SendPlan { return client.SendPlan{Hold: true} }

// recordingDialer is a dialler that reports every address it was asked for,
// which is the only way to see the failover list being walked: the addresses
// are the DRIVER's own and nothing in the peer records them.
type recordingDialer struct {
	inner  *client.Fake
	onDial func(addr string)
}

// Dial records the address and delegates.
func (d *recordingDialer) Dial(ctx context.Context, addr string, opts *amqp.ConnOptions) (client.Conn, error) {
	if d.onDial != nil {
		d.onDial(addr)
	}
	return d.inner.Dial(ctx, addr, opts)
}

// compile-time proof that the recorder is the seam's own dialler.
var _ client.Dialer = (*recordingDialer)(nil)

// asOpError is errors.As for an *mq.OpError, named so that a fuzz target reads
// as a property rather than as a type assertion.
func asOpError(err error, dst **mq.OpError) bool { return errors.As(err, dst) }

// errorsIsSame reports whether the mapped error still reaches the native one,
// which is the specification's "native errors reachable".
//
// It compares by IDENTITY through the unwrap chain rather than with errors.Is,
// because errors.Is matches by code in this repository — so a mapped error would
// match a native one of the same code even if the native value had been thrown
// away, and the check would prove nothing.
func errorsIsSame(mapped, native error) bool {
	for err := mapped; err != nil; {
		if err == native { //nolint:errorlint // identity is the question: errors.Is here would match by code and prove nothing
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// utf8Valid reports whether s is valid UTF-8, which everything this driver
// renders has to be: the errors and the log lines both go through a
// pipeline that cannot carry anything else.
func utf8Valid(s string) bool { return utf8.ValidString(s) }
