package stomp

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
)

// warmSink builds a sink outside the measured loop, which is what "warmed"
// means here: every buffer the mapping reuses — the `protocol.Fields` and its
// `Props` slice, the slice of frame options — has already grown to the size
// this message needs, so what the loop measures is the steady state rather
// than the first publication.
func warmSink(t *testing.T, m *mq.Message, o *mq.PublishOptions) *Sink {
	t.Helper()
	cfg := Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}.resolve()
	caps, err := cfg.capabilities()
	if err != nil {
		t.Fatalf("building the capability table: %v", err)
	}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info: cfg.info(), Capabilities: caps, Ownership: driverkit.ClientBorrowed,
	}, nil)
	if err != nil {
		t.Fatalf("driverkit.NewBase: %v", err)
	}
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		t.Fatalf("protocol.New(STOMP): %v", err)
	}
	env, err := newEnvelope(cfg, caps)
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}
	b := &Backend{cfg: cfg, clock: fixedTestClock{}, BaseBackend: base}
	s := &Sink{b: b, topic: "orders", proto: p, envelope: env, receipts: true, confirm: mq.ConfirmAccepted}
	// Two warm-up passes, because the first grows the slices and the second
	// proves they are not re-grown.
	for range 2 {
		s.mu.Lock()
		if _, _, err := s.frameFor(m, o); err != nil {
			s.mu.Unlock()
			t.Fatalf("warming the sink: %v", err)
		}
		s.mu.Unlock()
	}
	return s
}

// fixedTestClock is a clock the publish path reads for a TTL, held still so
// that the measurement does not depend on the wall clock.
type fixedTestClock struct{}

// Now is a fixed instant.
func (fixedTestClock) Now() time.Time { return time.Unix(1_757_851_200, 0) }

// NewTimer is never called on this path.
func (fixedTestClock) NewTimer(time.Duration) mq.Timer { return nil }

// TestAllocsSharedMappingIsFree holds `internal/activemq/protocol` to the
// promise its own package comment makes: `Fields` is a reusable buffer, so
// putting the shared mapping in the middle of the publish path costs no
// allocation per message.
//
// It is checked from HERE as well as from the protocol package's own
// `TestAllocs`, and the reason is that this driver is the one that runs it on
// every publication. The AMQP 1.0 arm's real mapping is
// `drivers/amqp10`'s `Mapper`; `Carry` and `Restore` ARE this driver's
// mapping, so a regression in them is a regression in this driver's hot path
// and nothing else would notice.
func TestAllocsSharedMappingIsFree(t *testing.T) {
	m := &mq.Message{
		Body: []byte("a body the mapping never copies"),
		Key:  []byte("customer-7"),
		Properties: mq.Properties{
			ContentType:   "application/json",
			CorrelationID: "corr-1",
		},
	}
	m.Headers.Add("tenant", []byte("acme"))
	m.Headers.Add("region", []byte("eu-west-1"))
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		t.Fatalf("protocol.New(STOMP): %v", err)
	}
	opts := protocol.Options{
		Broker:     activemq.Artemis,
		Persistent: true,
		KeyHeader:  protocol.KeyHeader,
		KeyAsGroup: true,
		Receipt:    "iqmq-1",
	}
	var fields protocol.Fields
	// Warm the Props slice before measuring.
	for range 2 {
		if err := p.Carry(&fields, m, opts); err != nil {
			t.Fatalf("Carry: %v", err)
		}
	}
	if got := testing.AllocsPerRun(200, func() {
		if err := p.Carry(&fields, m, opts); err != nil {
			t.Fatalf("Carry: %v", err)
		}
	}); got != 0 {
		t.Errorf("Carry allocates %.1f times per message with a warmed Fields, want 0. The whole reason Fields is "+
			"a buffer the caller owns rather than a value the mapping returns is that this is on the publish path "+
			"of every message", got)
	}
	var out mq.Message
	for range 2 {
		out.Headers.Reset()
		if err := p.Restore(&out, &fields, opts); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	}
	if got := testing.AllocsPerRun(200, func() {
		out.Headers.Reset()
		if err := p.Restore(&out, &fields, opts); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	}); got != 0 {
		t.Errorf("Restore allocates %.1f times per delivery into a message whose header storage is already grown, "+
			"want 0", got)
	}
}

// TestAllocsPublishMappingPath pins what the DRIVER's own mapping path costs,
// which is not zero and cannot be.
//
// `Sink.frameFor` is the shared mapping plus one step of this driver's own:
// turning the mapped fields into the SEND frame's options. That step is where
// the allocations are, and they are the client library's rather than this
// driver's — `gostomp.SendOpt.Header(k, v)` returns a CLOSURE over the key and
// the value, and go-stomp offers no other way to set a header on a frame it is
// about to build. So the cost is one closure per header written, and the only
// way to reach zero would be to stop using the library's option API and build
// the `frame.Frame` directly, which would mean re-implementing
// `Conn.Send`'s content-length handling, its receipt bookkeeping and its write
// channel — a fork of the library's send path to save a closure.
//
// So the number is PINNED rather than driven to zero, and it is pinned as a
// ceiling rather than an equality: a ceiling catches the regression this test
// exists for (a per-publication allocation somebody added to the mapping)
// without failing on a library that got cheaper.
func TestAllocsPublishMappingPath(t *testing.T) {
	m := &mq.Message{
		Body: []byte("a body the mapping never copies"),
		Key:  []byte("customer-7"),
		Properties: mq.Properties{
			ContentType:   "application/json",
			CorrelationID: "corr-1",
		},
	}
	m.Headers.Add("tenant", []byte("acme"))
	m.Headers.Add("region", []byte("eu-west-1"))
	o := &mq.PublishOptions{TTL: 90 * time.Second, Priority: 4, PrioritySet: true}
	s := warmSink(t, m, o)

	got := testing.AllocsPerRun(200, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, _, err := s.frameFor(m, o); err != nil {
			t.Fatalf("frameFor: %v", err)
		}
	})
	// The headers this message produces: content-type, correlation-id,
	// JMSXGroupID, persistent, priority, expires, x-iqmq-key, tenant, region
	// and receipt -- ten, each one a `SendOpt.Header` closure, plus the
	// decimal renderings of the priority and the expiry.
	//
	// Seventeen and not sixteen, and the extra one is deliberate: the
	// option slice `frameFor` returns is a COPY of the sink's scratch,
	// because the scratch outlives the lock that guards it and the shared
	// backing array was a real data race under concurrent publication (see
	// `Sink.frameFor`). A `sync.Pool` was tried and
	// measured eighteen, so the copy is the cheaper of the two answers that
	// are correct. A number above seventeen still means something was added
	// to the driver's own mapping.
	const ceiling = 17
	if got > ceiling {
		t.Errorf("the publish mapping path allocates %.1f times per message, over the pinned ceiling of %d. The "+
			"allocations are go-stomp's option closures -- one per header, because `SendOpt.Header` returns a "+
			"closure over the key and the value and the library offers no other way to set a header -- so a number "+
			"above the ceiling means something was added to the driver's own mapping rather than to the header "+
			"count", got, ceiling)
	}
	t.Logf("the publish mapping path allocates %.1f times per message (ceiling %d): one `SendOpt.Header` closure "+
		"per header written, plus the decimal renderings of the numeric headers. The shared mapping's own half is "+
		"zero, which TestAllocsSharedMappingIsFree holds it to", got, ceiling)
}

// TestAllocsSubscribeOptionsAreBuiltOnce is the consume side's equivalent, and
// it is a ceiling for the same reason: the SUBSCRIBE frame's headers are
// go-stomp option closures too.
//
// It is worth measuring at all because a subscription is re-sent on every
// reconnection and on every requeue, which on this driver is the negative
// settlement — so a nack-heavy consumer runs this path per nack rather than
// once per source.
func TestAllocsSubscribeOptionsAreBuiltOnce(t *testing.T) {
	cfg := Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}.resolve()
	b := &Backend{cfg: cfg, clock: fixedTestClock{}}
	src := &Source{b: b, cfg: mq.SourceConfig{Prefetch: 8}}
	for range 2 {
		_ = src.subscribeOptions()
	}
	got := testing.AllocsPerRun(200, func() { _ = src.subscribeOptions() })
	// The slice, plus one closure per header: subscription-type and
	// consumer-window-size on Artemis, and the window's decimal rendering.
	const ceiling = 8
	if got > ceiling {
		t.Errorf("building the SUBSCRIBE options allocates %.1f times, over the pinned ceiling of %d. A requeue "+
			"is a re-subscription on this driver, so this runs once per negative settlement rather than once per "+
			"source", got, ceiling)
	}
	t.Logf("building the SUBSCRIBE options allocates %.1f times (ceiling %d)", got, ceiling)
}
