package jetstream_test

import (
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestSPISurfaces is the compile-and-assert list of what this driver offers the
// core, and what it deliberately does not.
//
// Two absences are decisions rather than omissions. There is no
// [mq.BatchSettler]: JetStream settles one delivery per message on that
// delivery's own reply subject, so a batch settler would be a loop wearing a
// name that promises a saving. There is no [mq.OwnedSink]: every asynchronous
// path hands the message to the client library, which holds it until its PubAck,
// so there is nothing to release earlier than the PubAck itself.
func TestSPISurfaces(t *testing.T) {
	b := openBackend(t)
	if _, ok := any(b).(mq.Backend); !ok {
		t.Fatal("the backend is not an mq.Backend")
	}
	if _, ok := any(b).(mq.HealthReporter); !ok {
		t.Error("the backend is not a HealthReporter although it runs background work")
	}
	if _, ok := any(b).(mq.AdminBackend); ok {
		t.Error("the backend claims an AdminBackend it does not implement")
	}

	topic := newTopic("spi")
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"BatchSink", asBatchSink(sink)},
		{"FlushSink", asFlushSink(sink)},
	} {
		if !c.ok {
			t.Errorf("the sink is not a %s", c.name)
		}
	}
	if asAsyncSink(sink) { t.Error("the pinned client does not expose the native async acknowledgement callback") }
	if asOwnedSink(sink) {
		t.Error("the sink claims OwnedSink: the client library holds the message until its PubAck, so there is nothing to release early")
	}

	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	settler := src.Settler()
	if settler == nil {
		t.Fatal("Settler() is nil: that is AckNone model, and this profile settles")
	}
	if _, ok := settler.(mq.DeadlineExtender); !ok {
		t.Error("the settler is not a DeadlineExtender although +WPI extends a lease")
	}
	if _, ok := settler.(mq.BatchSettler); ok {
		t.Error("the settler claims BatchSettler: JetStream has no batch ack verb, and a loop that wore the name would advertise a saving it does not make")
	}
}

func asBatchSink(s mq.Sink) bool { _, ok := s.(mq.BatchSink); return ok }
func asAsyncSink(s mq.Sink) bool { _, ok := s.(mq.AsyncSink); return ok }
func asFlushSink(s mq.Sink) bool { _, ok := s.(mq.FlushSink); return ok }
func asOwnedSink(s mq.Sink) bool { _, ok := s.(mq.OwnedSink); return ok }

// TestSourceRefusesAnUnresolvedPrefetch holds the SPI's own contract: the core
// resolves the portable default before opening a source, so a
// non-positive prefetch here is a bug above the driver rather than a condition.
func TestSourceRefusesAnUnresolvedPrefetch(t *testing.T) {
	b := openBackend(t)
	for _, prefetch := range []int{0, -1} {
		if _, err := b.OpenSource(t.Context(), mq.SourceConfig{
			Topic: newTopic("prefetch"), Subscription: "workers", Prefetch: prefetch,
		}); err == nil {
			t.Errorf("OpenSource accepted a prefetch of %d", prefetch)
		}
	}
}

// TestSinkAndSourceRefuseAnInvalidMetadataMode holds the enumeration: a mode
// outside the four is a construction error and never a silent strict.
func TestSinkAndSourceRefuseAnInvalidMetadataMode(t *testing.T) {
	b := openBackend(t)
	topic := newTopic("meta")
	if _, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Metadata: 99}); err == nil {
		t.Error("OpenSink accepted a metadata mode outside the four")
	}
	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "workers", Prefetch: 4, Metadata: 99,
	}); err == nil {
		t.Error("OpenSource accepted a metadata mode outside the four")
	}
}

// TestOperationsAfterCloseReportClosed holds a closed handle
// answers ErrClosed and never a panic or a hang.
func TestOperationsAfterCloseReportClosed(t *testing.T) {
	b := openBackend(t)
	topic := newTopic("closed")
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink Close: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Errorf("a second sink Close: %v", err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Body: []byte("x")}, nil); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Publish after Close reported %v, want ErrClosed", err)
	}
	if fs, ok := sink.(mq.FlushSink); ok {
		if err := fs.Flush(t.Context()); !errors.Is(err, mq.ErrClosed) {
			t.Errorf("Flush after Close reported %v, want ErrClosed", err)
		}
	}
	if err := src.Close(); err != nil {
		t.Fatalf("source Close: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("a second source Close: %v", err)
	}
	dst := make([]mq.DriverDelivery, 2)
	if _, err := src.Fetch(t.Context(), dst); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Fetch after Close reported %v, want ErrClosed", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("backend Close: %v", err)
	}
	if err := b.Ping(t.Context()); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Ping after Close reported %v, want ErrClosed", err)
	}
	if _, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic}); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("OpenSink after Close reported %v, want ErrClosed", err)
	}
	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "workers", Prefetch: 4,
	}); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("OpenSource after Close reported %v, want ErrClosed", err)
	}
}

// TestNativeEscapeHatches holds the caller that needs the client
// library has a named route to it, and so does the caller that needs the stream.
func TestNativeEscapeHatches(t *testing.T) {
	b := openBackend(t)
	if b.Conn() == nil {
		t.Error("Conn() is nil")
	}
	if b.JetStream() == nil {
		t.Error("JetStream() is nil")
	}
	if b.Stream() == nil {
		t.Error("Stream() is nil")
	}
	if b.Native() == nil {
		t.Error("Native() is nil: the specification allows a driver to surface nothing, but this one has a context to surface")
	}
	c := mq.NewClient(b)
	if c.Native() == nil {
		t.Error("the client surfaces no native escape hatch")
	}
}

// TestSettlementsAfterSourceCloseAreRefused is the other half: a source that
// has closed naks what it held, so a settlement afterwards must not reach the
// wire and claim to have worked.
func TestSettlementsAfterSourceCloseAreRefused(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.AckWait = 30 * time.Second })
	topic := newTopic("settle-after-close")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Ack(); err == nil {
		t.Error("acking a delivery from a closed consumer succeeded")
	}
	// The message is not lost: the close naked it, so another consumer sees
	// it well inside the thirty-second lease.
	next := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	again, ok := tryReceive(t, next, 5*time.Second)
	if !ok {
		t.Fatal("the delivery a closed consumer was holding did not come back inside five seconds of a thirty-second lease")
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestDriverNameAndSchemeAreTheProfilesOwn holds ADR-010 two modules,
// two schemes, and no route by which a caller can reach one profile's
// capabilities through the other's name.
func TestDriverNameAndSchemeAreTheProfilesOwn(t *testing.T) {
	if nq.DriverName != "jetstream" {
		t.Errorf("DriverName = %q", nq.DriverName)
	}
	if nq.Scheme != "natsjs" {
		t.Errorf("Scheme = %q, want natsjs: \"nats\" is the Core driver's", nq.Scheme)
	}
	if nq.Scheme == "nats" {
		t.Error("the JetStream driver claims the Core driver's scheme")
	}
}
