package jetstream_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// This file holds the driver's own dead-letter route, Reject and the copy of
// an undecodable message. deadletter_test.go holds the core's route over this
// driver.

// TestADeadLetterCopyCarriesOnlyItsOwnReason holds the copy's reason header to
// the Reject that made it. The copy keeps the original's headers, so a message
// read off one dead-letter topic and rejected again with no reason went on
// with the first hop's iqmq-dead-letter-reason, and the second destination
// read a diagnosis nobody had given it. The other two dead-letter headers were
// already the new hop's.
func TestADeadLetterCopyCarriesOnlyItsOwnReason(t *testing.T) {
	_, cfg := openBackendCfg(t, func(cfg *nq.Config) { cfg.DeadLetterTopic = "dead.first" })
	first, err := nq.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	cfg.DeadLetterTopic = "dead.second"
	second, err := nq.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	topic := newTopic("hops")
	cons := consumerOn(t, first, topic, "workers")
	firstDead := consumerWith(t, second, mq.ConsumerConfig{Topic: "dead.first", Subscription: "triage", Start: mq.StartEarliest()})
	secondDead := consumerWith(t, second, mq.ConsumerConfig{Topic: "dead.second", Subscription: "audit", Start: mq.StartEarliest()})
	publish(t, first, topic, &mq.Message{Body: []byte("poison")})

	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Nack(mq.NackDeadLetter(), mq.NackReason("unparsable")); err != nil {
		t.Fatalf("the first Reject: %v", err)
	}
	copied := receiveOne(t, firstDead, 10*time.Second)
	if got := headerOf(copied, nq.HeaderDeadLetterReason); got != "unparsable" {
		t.Fatalf("the first copy carries reason %q, want unparsable", got)
	}
	if err := copied.Reject(); err != nil {
		t.Fatalf("the second Reject: %v", err)
	}
	again := receiveOne(t, secondDead, 10*time.Second)
	if got, ok := again.Headers.Get(nq.HeaderDeadLetterReason); ok {
		t.Errorf("a copy rejected with no reason carries the earlier hop's reason %q", got)
	}
	if from := headerOf(again, nq.HeaderDeadLetterFrom); from != cfg.SubjectPrefix+".dead.first" {
		t.Errorf("the second copy names %q as where it came from, want %q", from, cfg.SubjectPrefix+".dead.first")
	}
	if string(again.Body) != "poison" {
		t.Errorf("the second copy carries %q, want the original payload", again.Body)
	}
	_ = again.Ack()
}

// TestADeadLetterCopyOnAnInterestStreamWaitsForItsConsumer holds both
// dead-letter routes on a stream with interest retention. The server stores a
// message there only when a consumer filters its subject, and acknowledges it
// either way, so with no consumer of the dead-letter topic yet the copy was
// acknowledged and never stored, the original was terminated, and the message
// was in neither place. A Reject now fails with a definite NotFound and
// settles nothing, an undecodable message stays in the stream, and both are
// copied once a consumer of the dead-letter topic exists.
func TestADeadLetterCopyOnAnInterestStreamWaitsForItsConsumer(t *testing.T) {
	const ackWait = 2 * time.Second
	for _, reject := range []bool{true, false} {
		route := "Reject"
		if !reject {
			route = "an undecodable message"
		}
		t.Run(route, func(t *testing.T) {
			stream := provisionStream(t, func(sc *sjs.StreamConfig) { sc.Retention = sjs.InterestPolicy })
			c := openOnStream(t, stream, func(cfg *nq.Config) {
				cfg.DeadLetterTopic = "dead"
				cfg.AckWait = ackWait
			})
			topic := newTopic("dl-interest")
			cfg := mq.ConsumerConfig{Topic: topic, Subscription: "workers", Start: mq.StartEarliest()}
			if !reject {
				cfg.Metadata = mq.MetadataPortableEnvelope
			}
			cons := consumerWith(t, c, cfg)
			publishPoison(t, c, topic)
			var again mq.Delivery
			if reject {
				d := receiveOne(t, cons, 10*time.Second)
				err := d.Reject()
				if mq.CodeOf(err) != mq.CodeNotFound || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
					t.Fatalf("Reject with no consumer of the dead-letter topic = %v (code %v, ambiguous %v), want a definite NotFound",
						err, mq.CodeOf(err), mq.IsAmbiguous(err))
				}
				if d.Settled() {
					t.Fatal("a Reject whose copy could not be stored settled the delivery")
				}
				// The original was not terminated, so it comes back.
				var ok bool
				if again, ok = tryReceive(t, cons, 2*ackWait); !ok {
					t.Fatal("the rejected original did not come back: it was terminated with no copy stored")
				}
			} else {
				// Every delivery of it until then finds no consumer.
				time.Sleep(ackWait)
			}
			dlq := consumerWith(t, c, mq.ConsumerConfig{Topic: "dead", Subscription: "audit", Start: mq.StartEarliest()})
			if reject {
				if err := again.Reject(); err != nil {
					t.Fatalf("Reject with a consumer of the dead-letter topic: %v (code %v)", err, mq.CodeOf(err))
				}
			}
			copied, ok := tryReceive(t, dlq, 3*ackWait)
			if !ok {
				t.Fatal("the message is in neither place: no copy reached the dead-letter consumer")
			}
			if string(copied.Body) != "not an envelope" {
				t.Errorf("the dead-letter copy carries %q, want the original payload", copied.Body)
			}
			_ = copied.Ack()
		})
	}
}

// TestADeadLetterCopyListsConsumersOnlyUntilOneFiltersIt holds the list a
// copy on a stream with interest retention is checked against to the pages it
// needs. The server lists consumers by name, 256 to a page, and the list went
// on to the last page after one had matched. Here a consumer of the
// dead-letter topic sorts first among 302, so one page answers.
func TestADeadLetterCopyListsConsumersOnlyUntilOneFiltersIt(t *testing.T) {
	stream := provisionStream(t, func(sc *sjs.StreamConfig) { sc.Retention = sjs.InterestPolicy })
	prefix := strings.ToLower(stream)
	nc := natsConn(t)
	jsc, err := sjs.New(nc)
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	if _, err := jsc.CreateConsumer(t.Context(), stream, sjs.ConsumerConfig{
		Durable: "a-audit", FilterSubject: prefix + ".dead", AckPolicy: sjs.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("the dead-letter consumer: %v", err)
	}
	for i := range 300 {
		if _, err := jsc.CreateConsumer(t.Context(), stream, sjs.ConsumerConfig{
			Durable: fmt.Sprintf("z-%03d", i), FilterSubject: fmt.Sprintf("%s.other.%d", prefix, i), AckPolicy: sjs.AckExplicitPolicy,
		}); err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
	}
	var lists atomic.Int64
	if _, err := nc.Subscribe("$JS.API.CONSUMER.LIST."+stream, func(*natsgo.Msg) { lists.Add(1) }); err != nil {
		t.Fatalf("watching the list requests: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c := openOnStream(t, stream, func(cfg *nq.Config) { cfg.DeadLetterTopic = "dead" })
	topic := newTopic("dl-pages")
	cons := consumerOn(t, c, topic, "workers")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject with a consumer of the dead-letter topic: %v (code %v)", err, mq.CodeOf(err))
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := lists.Load(); got != 1 {
		t.Errorf("one copy listed the stream's consumers in %d requests, want 1: the first page holds a consumer of the dead-letter topic", got)
	}
}
