package amqp10_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver that refuses any key
// under that prefix refuses iqmq's own provenance, the publication fails before
// any I/O, the runner requeues the message and counts a DeadLetterFailures, and
// the message is redelivered for ever.
//
// This driver states the rule in two places, and both had to move: keysValid
// refuses a reserved-prefix key that is not one of the names the mapping
// consumes, and onlyReservedHeaders re-decides mq.Headers.Validate's blanket
// reserved-prefix verdict for the names this driver really maps.

// deadLetterCopy is the message the route publishes, built exactly as
// dlq.go's deadLetterMessage builds it.
func deadLetterCopy() *mq.Message {
	m := &mq.Message{Topic: "orders.dlq", Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Set(mq.HeaderOriginTopic, []byte("orders"))
	m.Headers.Set(mq.HeaderOriginSubscription, []byte("billing"))
	m.Headers.Set(mq.HeaderAttempts, []byte("3"))
	m.Headers.Set(mq.HeaderFailure, []byte("schema mismatch"))
	m.Headers.Set(mq.HeaderFirstDeliveredAt, []byte("2026-09-18T10:11:12.5Z"))
	return m
}

// provenance is the five keys in the order dlq.go writes them.
var provenance = [...]string{
	mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts,
	mq.HeaderFailure, mq.HeaderFirstDeliveredAt,
}

// TestTheCoreDeadLetterRouteIsRepresentable holds every admission gate on the
// publish path plus the round trip: the five provenance headers travel as
// ordinary application properties and come back readable, which is what an
// operator reading the dead-letter queue needs.
func TestTheCoreDeadLetterRouteIsRepresentable(t *testing.T) {
	src := deadLetterCopy()
	if err := aq.Representable(testProfile(), src); err != nil {
		t.Fatalf("Representable(the dead-letter copy the specification publishes) = %v, want nil: "+
			"every one of the five provenance headers is the CORE's and not a caller's", err)
	}
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataAdaptive,
		mq.MetadataPortableEnvelope, mq.MetadataLenientDrop,
	} {
		m := mapperFor(t, mode)
		if err := m.CheckPublish(src, nil); err != nil {
			t.Errorf("CheckPublish under %v = %v, want nil", mode, err)
		}
	}

	m := mapperFor(t, mq.MetadataNativeStrict)
	out, err := m.Encode(aq.NewBuf(), src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for _, k := range provenance {
		if _, ok := out.ApplicationProperties[k]; !ok {
			t.Errorf("Encode left %q off the wire; application properties are %v", k, out.ApplicationProperties)
		}
	}

	var got mq.Message
	got.Topic = "orders.dlq"
	lossy, err := m.Decode(&got, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if lossy != 0 {
		t.Errorf("the decode could not represent %d field(s) of a message this driver encoded itself", lossy)
	}
	for _, k := range provenance {
		v, ok := got.Headers.Get(k)
		if !ok {
			t.Errorf("the round trip dropped %q: an operator reading the dead-letter queue cannot see where the message came from", k)
			continue
		}
		want, _ := src.Headers.Get(k)
		if !bytes.Equal(v, want) {
			t.Errorf("%q came back as %q, want %q", k, v, want)
		}
	}
	if v, ok := got.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("the caller's own header came back as %q, %v", v, ok)
	}

	// And the decoded message republishes, which is what makes a forwarding
	// consumer of a dead-letter queue possible.
	if err := m.CheckPublish(&got, nil); err != nil {
		t.Errorf("republishing the decoded dead-letter copy: %v", err)
	}
}

// TestACallersReservedHeaderIsStillRefused keeps the widening to exactly five
// keys: the rule about a CALLER's header under the reserved prefix is
// unchanged for every other name.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "attempt",
		mq.ReservedHeaderPrefix + "id",
		driverkit.EnvelopeMarker,
	} {
		src := &mq.Message{Topic: "t", Body: []byte("b")}
		src.Headers.Set(k, []byte("v"))
		if err := m.CheckPublish(src, nil); err == nil {
			t.Errorf("CheckPublish accepted the caller's reserved header %q", k)
		} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("CheckPublish(%q) = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterAddress is the broker arm, and
// it is env-gated because this module starts no container and needs none: every
// other test here runs against the scripted peer of internal/client.
//
// The always-running arm above is what makes this one allowed to skip
// : it holds every admission gate on the publish path and the
// round trip, so a run with no broker still fails the day the refusal comes
// back. What only a broker can answer is whether an AMQP server accepts an
// application property named `iqmq-origin-topic`, and that answer is recorded
// with the image it was measured against.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterAddress(t *testing.T) {
	addr := os.Getenv("IQMQ_AMQP10_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_AMQP10_ADDR (and IQMQ_AMQP10_USER/IQMQ_AMQP10_PASSWORD for a broker that needs them) " +
			"to run the dead-letter route against an AMQP 1.0 broker; the mapping arm above needs none")
	}
	cfg := aq.Config{Addrs: strings.Split(addr, ",")}
	if u := os.Getenv("IQMQ_AMQP10_USER"); u != "" {
		cfg.SASL = aq.SASLPlain
		cfg.Username = u
		cfg.Password = dsn.Secret(os.Getenv("IQMQ_AMQP10_PASSWORD"))
		cfg.InsecurePlainSASL = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := aq.Open(ctx, genericProfile(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = client.Close() }()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "iqmq-dl-" + suffix
	dlqTopic := "iqmq-dl-" + suffix + "-dlq"

	dlq, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: dlqTopic, Subscription: "audit"})
	if err != nil {
		t.Fatalf("dead-letter consumer: %v", err)
	}
	defer func() { _ = dlq.Close() }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	// The read starts FIRST. A generic AMQP 1.0 address may be multicast --
	// it is on Artemis, whose auto-created address has no queue until a
	// receiver attaches -- and this driver attaches its receiver lazily on
	// the first Fetch, so a dead-letter copy published before that has
	// nowhere to be routed. The same is why the poison message is published
	// in a loop below rather than once.
	type received struct {
		d   mq.Delivery
		n   int
		err error
	}
	arrived := make(chan received, 1)
	go func() {
		dst := make([]mq.Delivery, 1)
		c, cancelRead := context.WithTimeout(ctx, 40*time.Second)
		defer cancelRead()
		n, err := dlq.Receive(c, dst)
		arrived <- received{d: dst[0], n: n, err: err}
	}()

	var handled atomic.Int64
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			handled.Add(1)
			return errors.New("schema mismatch")
		}), mq.WithMaxAttempts(2), mq.WithDeadLetter(dlqTopic),
			mq.WithOrderingByKey(mq.OrderingStrict),
			mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	}()

	src := &mq.Message{Body: []byte(`{"id":42}`)}
	src.Headers.Set("content-kind", []byte("order"))
	deadline := time.Now().Add(20 * time.Second)
	for handled.Load() == 0 && time.Now().Before(deadline) {
		if err := client.Publish(ctx, topic, src); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("the context expired before the handler was reached")
		}
	}
	if handled.Load() == 0 {
		t.Fatal("the handler was never reached: this profile's addressing does not deliver on this broker, " +
			"so the run says nothing about the route")
	}

	r := <-arrived
	if r.n != 1 {
		t.Fatalf("nothing arrived on %q (Receive = %d, %v); the dead-letter publication never landed", dlqTopic, r.n, r.err)
	}
	got := r.d.Message
	for _, k := range provenance {
		if _, ok := got.Headers.Get(k); !ok {
			t.Errorf("the dead-lettered copy carries no %q", k)
		}
	}
	if v, _ := got.Headers.Get(mq.HeaderFailure); string(v) != "schema mismatch" {
		t.Errorf("%s = %q, want %q", mq.HeaderFailure, v, "schema mismatch")
	}
	if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != topic {
		t.Errorf("%s = %q, want %q", mq.HeaderOriginTopic, v, topic)
	}
	if err := r.d.Ack(); err != nil {
		t.Errorf("acking the dead-lettered copy: %v", err)
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if st := consumer.Stats(); st.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the route was refused and the message requeued", st.DeadLetterFailures)
	}
}
