package gcppubsub_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// The core's own dead-letter route, mq.WithDeadLetter, is the subject of this
// file. The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of them
// is under mq.ReservedHeaderPrefix. This driver refused any header under that
// prefix, so the copy was refused before any I/O, the runner requeued the
// message and counted a DeadLetterFailures, and the message was redelivered
// for ever. The decode path dropped every such attribute as well, so a copy
// that got there some other way reached its reader without its provenance.
//
// The five keys are named through the root package's constants: the contract
// is the core's.

// provenance is the five keys in the order the core writes them.
var provenance = [...]string{
	mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts,
	mq.HeaderFailure, mq.HeaderFirstDeliveredAt,
}

// stamped is a message carrying a caller's header and the five provenance
// headers, as the core's dead-letter route builds its copy.
func stamped() *mq.Message {
	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Set(mq.HeaderOriginTopic, []byte("orders"))
	m.Headers.Set(mq.HeaderOriginSubscription, []byte("billing"))
	m.Headers.Set(mq.HeaderAttempts, []byte("3"))
	m.Headers.Set(mq.HeaderFailure, []byte("schema mismatch"))
	m.Headers.Set(mq.HeaderFirstDeliveredAt, []byte("2026-09-18T10:11:12.5Z"))
	return m
}

// TestTheProvenanceHeadersRoundTrip is the mapping half, in every metadata
// mode: a message carrying the five is published, and a consumer reads every
// one of them back.
func TestTheProvenanceHeadersRoundTrip(t *testing.T) {
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataAdaptive, mq.MetadataPortableEnvelope, mq.MetadataLenientDrop,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			c, _ := scripted(t)
			topic := newTopic("provenance")
			provision(t, c, topic, "s")
			ctx := context.Background()
			pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic, Metadata: mode})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			t.Cleanup(func() { _ = pub.Close() })
			m := stamped()
			if err := pub.Publish(ctx, m); err != nil {
				t.Fatalf("publishing the core's dead-letter copy: %v (code %v); the five provenance headers are the core's and not a caller's",
					err, mq.CodeOf(err))
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s", Metadata: mode})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			t.Cleanup(func() { _ = cons.Close() })
			d := receiveOne(t, cons, 5*time.Second)
			for _, k := range provenance {
				got, ok := d.Headers.Get(k)
				want, _ := m.Headers.Get(k)
				if !ok || !bytes.Equal(got, want) {
					t.Errorf("%s came back as %q (present %v), want %q: a reader of the dead-letter topic cannot see where the message came from",
						k, got, ok, want)
				}
			}
			if v, ok := d.Headers.Get("content-kind"); !ok || string(v) != "order" {
				t.Errorf("the caller's own header came back as %q, %v", v, ok)
			}
			_ = d.Ack()
		})
	}
}

// TestACallersReservedHeaderIsStillRefused keeps the widening to exactly the
// five: a caller's header under the reserved prefix, one of this driver's own
// attribute names included, is refused as before.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	c, _ := scripted(t)
	topic := newTopic("reserved")
	provision(t, c, topic, "s")
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "key", mq.ReservedHeaderPrefix + "origin", mq.ReservedHeaderPrefix + "attempt",
		"iqmq-k", "iqmq-id", "iqmq-env", gps.ReservedAttributePrefix + "-x",
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set(k, []byte("v"))
		if err := c.Publish(context.Background(), topic, m); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("publishing a caller's header %q = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic is the end-to-end
// half, against the scripted fake and against the client library's pstest: a
// handler that always fails, mq.WithDeadLetter, and the stamped copy read
// back off the dead-letter topic with its provenance.
//
// The subscription has a dead-letter policy, because without one the service
// counts no delivery attempts and the runner can never find a message that it
// gave back exhausted: that is how a Pub/Sub caller has to configure the
// route (docs/drivers/gcppubsub.md). The runner's two attempts run out before
// the service's five, so it is the core's route that dead-letters, to a topic
// of its own.
//
// It asserts ConsumerStats.DeadLetterFailures too, because the defect was a
// SILENT one: the runner counts and requeues rather than returning the error,
// so a route that cannot publish looks like a message taking a while.
//
// The emulator arm runs against the endpoint TestConformance reads,
// IQMQ_PUBSUB_ADDR, and skips without one.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	for _, arm := range []string{"scripted", "pstest", "emulator"} {
		t.Run(arm, func(t *testing.T) {
			serviceDead := newTopic("dl-service-dead")
			policy := func(cfg *gps.Config) {
				cfg.DeadLetterTopic = serviceDead
				cfg.MaxDeliveryAttempts = 5
			}
			var c *mq.Client
			switch arm {
			case "scripted":
				c, _ = scripted(t, policy)
			case "pstest":
				c = newLive(t).open(t, policy)
			default:
				addr := os.Getenv("IQMQ_PUBSUB_ADDR")
				if addr == "" {
					t.Skip("IQMQ_PUBSUB_ADDR is not set")
				}
				project := os.Getenv("IQMQ_PUBSUB_PROJECT")
				if project == "" {
					project = "iqmq-test"
				}
				cfg := gps.Config{ProjectID: project, Endpoint: addr, InsecureTransport: true, AutoCreate: true}
				policy(&cfg)
				var err error
				if c, err = gps.Open(context.Background(), cfg); err != nil {
					t.Fatalf("Open: %v", err)
				}
				t.Cleanup(func() { _ = c.Close() })
			}
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if err := admin.EnsureTopic(context.Background(), mq.TopicSpec{Name: serviceDead}); err != nil {
				t.Fatalf("EnsureTopic(%s): %v", serviceDead, err)
			}
			topic, dead := newTopic("dl-orders"), newTopic("dl-orders-dead")
			provision(t, c, topic, "billing")
			provision(t, c, dead, "operator")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			m := &mq.Message{Body: []byte(`{"id":42}`)}
			m.Headers.Set("content-kind", []byte("order"))
			if err := c.Publish(ctx, topic, m); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "billing"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() {
				done <- cons.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					return errors.New("schema mismatch")
				}), mq.WithMaxAttempts(2), mq.WithRetryBackoff(mq.ConstantBackoff(0)), mq.WithDeadLetter(dead))
			}()

			reader, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: dead, Subscription: "operator"})
			if err != nil {
				t.Fatalf("Consumer on the dead-letter topic: %v", err)
			}
			defer func() { _ = reader.Close() }()
			rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
			defer rcancel()
			dst := make([]mq.Delivery, 1)
			n, rerr := reader.Receive(rctx, dst)
			stop()
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run: %v", err)
			}
			st := cons.Stats()
			if n != 1 {
				t.Fatalf("nothing reached the dead-letter topic within 10s (%v); DeadLetterFailures = %d, "+
					"DeadLetterPublished = %d: the route was refused and the message requeued", rerr, st.DeadLetterFailures, st.DeadLetterPublished)
			}
			got := dst[0]
			// The copy is made on the second attempt, so the core writes no
			// first-delivered-at: it knows the time of this delivery only.
			for _, k := range provenance[:4] {
				if _, ok := got.Headers.Get(k); !ok {
					t.Errorf("the dead-lettered copy carries no %s", k)
				}
			}
			if v, _ := got.Headers.Get(mq.HeaderFailure); string(v) != "schema mismatch" {
				t.Errorf("%s = %q, want %q", mq.HeaderFailure, v, "schema mismatch")
			}
			if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != topic {
				t.Errorf("%s = %q, want %q", mq.HeaderOriginTopic, v, topic)
			}
			if string(got.Body) != `{"id":42}` {
				t.Errorf("the copy's body is %q, want the original's", got.Body)
			}
			_ = got.Ack()
			if st.DeadLetterFailures != 0 || st.DeadLetterPublished != 1 {
				t.Errorf("DeadLetterFailures = %d, DeadLetterPublished = %d; want 0 and 1", st.DeadLetterFailures, st.DeadLetterPublished)
			}
		})
	}
}
