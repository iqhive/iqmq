package rabbitmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// TestTopologyMapping is the naming, checked against what the broker really
// holds rather than against the derivation that produced it.
//
// The derivation is already pinned in `internal/rabbit`; what this adds is
// that the driver DECLARES what it derived, with the arguments the specification asks for,
// and in an order that works: the dead-letter exchange has to exist before the
// queue that names it, because RabbitMQ accepts a queue whose
// `x-dead-letter-exchange` does not exist and then drops every dead-lettered
// message silently.
func TestTopologyMapping(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Prefix, c.Declare, c.DeadLetter = "iqmq.", true, true
	})
	consumeOn(t, client, "orders", "workers")

	q, ok := broker.Queue("iqmq.orders.workers")
	if !ok {
		t.Fatalf("the subscription's queue was not declared; the broker holds %v", queueNames(broker))
	}
	if got := q.Args["x-queue-type"]; got != "quorum" {
		t.Errorf("x-queue-type = %v, want quorum: the specification asks for a quorum queue per <topic>.<subscription>", got)
	}
	if got := q.Args["x-dead-letter-exchange"]; got != "iqmq.orders.workers.dlx" {
		t.Errorf("x-dead-letter-exchange = %v", got)
	}
	if _, ok := broker.Queue("iqmq.orders.workers.dlq"); !ok {
		t.Errorf("the dead-letter queue was not declared; the broker holds %v", queueNames(broker))
	}
	ex, ok := broker.Exchange("iqmq.orders")
	if !ok {
		t.Fatal("the topic's exchange was not declared")
	}
	if ex.Kind != "topic" {
		t.Errorf("the exchange is %q, want topic", ex.Kind)
	}
	bound := false
	for _, b := range ex.Bindings {
		if b.Queue == "iqmq.orders.workers" && b.Key == "orders" {
			bound = true
		}
	}
	if !bound {
		t.Errorf("the queue is not bound to the exchange with the topic as the routing key: %+v", ex.Bindings)
	}
}

// TestNoHiddenCreation is the rule, and the test is that the driver REFUSES
// rather than that it happens not to declare.
//
// A driver that merely omitted the declaration would open successfully and
// then consume nothing for ever, which is indistinguishable from an idle
// topic. The passive declare is what turns that into an error at open.
func TestNoHiddenCreation(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare = false })
	_, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err == nil {
		t.Fatal("a consumer opened against a queue nobody declared: the API forbids creating it, and opening successfully would leave the caller waiting for ever on a queue that does not exist")
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Fatalf("the refusal carries %v, want %v: %v", code, mq.CodeNotFound, err)
	}
	if len(broker.Queues()) != 0 {
		t.Errorf("the broker holds %v: nothing may be created", queueNames(broker))
	}
	// And the positive direction, so the test is not satisfied by a driver
	// that refuses everything.
	_, client2 := openFake(t, func(c *rq.Config) { c.Declare = true })
	if _, err := client2.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"}); err != nil {
		t.Fatalf("a consumer with Declare on was refused: %v", err)
	}
}

// TestPublishRoundTrip is the ordinary path, end to end through the driver and
// the fake: publish, receive, acknowledge.
func TestPublishRoundTrip(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	m := &mq.Message{Body: []byte("hello"), Key: []byte("k1")}
	m.Headers.Add("trace", []byte("abc"))
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	if string(d.Body) != "hello" {
		t.Errorf("body = %q", d.Body)
	}
	if string(d.Key) != "k1" {
		t.Errorf("key = %q, want k1: with the key in a header it has to come back from the header", d.Key)
	}
	if got, ok := d.Headers.Get("trace"); !ok || string(got) != "abc" {
		t.Errorf("header trace = %q (present=%v)", got, ok)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}
	broker.Sync()
	if settled := broker.Settlements(); len(settled) != 1 || settled[0].Kind != rabbitfake.SettleAck {
		t.Fatalf("the broker recorded %+v, want one ack", settled)
	}
}

// TestPrefetchBoundsTheUnacknowledged is the specification's `Qos(prefetch)` and the
// backpressure the whole consume path rests on.
//
// The order in which the driver sends the two frames is what this really
// checks: RabbitMQ applies a prefetch to the consumers started AFTER it, so a
// driver that consumed before it set the QoS would have an unbounded consumer
// for its whole life and the ring would be the only bound — which means the
// broker's own flow control never sees a slow consumer.
func TestPrefetchBoundsTheUnacknowledged(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 3 })
	pub := publishOn(t, client, "orders")
	for i := range 10 {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	// Receive one and hold it: the broker may then have at most the prefetch
	// outstanding at this consumer.
	d := receiveOne(t, cons)
	broker.Sync()
	// The consumer's channel is not the last one opened -- the driver opens a
	// throwaway channel for the declarations and one per publisher -- so the
	// prefetch is looked for rather than assumed.
	found := false
	for _, ch := range broker.Channels() {
		if n, _ := ch.Prefetch(); n == 3 {
			found = true
		}
	}
	if !found {
		t.Errorf("no channel has a prefetch of 3: the QoS was not sent")
	}
	unacked := 0
	for _, ch := range broker.Channels() {
		unacked += ch.Unacked()
	}
	if unacked > 3 {
		t.Fatalf("%d deliveries are unacknowledged with a prefetch of 3: the QoS was not applied before the consume", unacked)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestNackRequeueRedelivers is the immediate negative settlement, and the
// attempt count that comes with it.
func TestNackRequeueRedelivers(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	first := receiveOne(t, cons)
	if first.Info().Attempt != 1 {
		t.Errorf("the first delivery reports Attempt %d, want 1", first.Info().Attempt)
	}
	if err := first.Nack(); err != nil {
		t.Fatalf("nacking: %v", err)
	}
	second := receiveOne(t, cons)
	if !second.Info().Redelivered {
		t.Error("the redelivery is not marked redelivered")
	}
	if got := second.Info().Attempt; got != 2 {
		t.Errorf("the redelivery reports Attempt %d, want 2: x-delivery-count is the attempt counter and a quorum queue is what carries it", got)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	_ = broker
}

// TestDropIsAnAckAndNotANack is the mapping that looks wrong and is right.
//
// Drop destroys the message. `basic.ack` is the frame that removes a
// message from a queue without routing it anywhere; `basic.nack{requeue=false}`
// would DEAD-LETTER it whenever the queue has a dead-letter exchange, which is
// the opposite of destroying it. So Drop is an ack, and this is what would
// fail if somebody "fixed" it to a nack.
func TestDropIsAnAckAndNotANack(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.DeadLetter = true, true })
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if err := d.Nack(mq.NackDrop()); err != nil {
		t.Fatalf("dropping: %v", err)
	}
	broker.Sync()
	if dl := broker.DeadLettered(); len(dl) != 0 {
		t.Fatalf("a dropped message was dead-lettered to %+v: Drop destroys the message, and a nack with requeue=false would route it", dl)
	}
	settled := broker.Settlements()
	if len(settled) != 1 || settled[0].Kind != rabbitfake.SettleAck {
		t.Fatalf("the broker recorded %+v, want one ack", settled)
	}
}

// TestDeadLetterRoutesToTheDLQ is the dead-letter action and the specification's
// `<queue>.dlq`.
func TestDeadLetterRoutesToTheDLQ(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.DeadLetter = true, true })
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if err := d.Nack(mq.NackDeadLetter()); err != nil {
		t.Fatalf("dead-lettering: %v", err)
	}
	broker.Sync()
	dl := broker.DeadLettered()
	if len(dl) != 1 {
		t.Fatalf("the broker recorded %d dead-letter hops, want 1", len(dl))
	}
	if dl[0].Exchange != "orders.workers.dlx" || dl[0].Reason != rabbitfake.DeathRejected {
		t.Errorf("the hop went to %q for %q", dl[0].Exchange, dl[0].Reason)
	}
	if got := broker.Ready("orders.workers.dlq"); len(got) != 1 || string(got[0].Body) != "poison" {
		t.Errorf("the dead-letter queue holds %d messages", len(got))
	}
}

// TestDeadLetterIsRefusedWithoutARoute is the rule for a driver that cannot
// honour a nack's action: ErrUnsupported, and NOTHING settled.
//
// The second half is the one that matters. A driver that refused the action and
// acknowledged the delivery anyway would have destroyed the message while
// telling the caller it had not acted.
func TestDeadLetterIsRefusedWithoutARoute(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	err := d.Nack(mq.NackDeadLetter())
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a dead-letter nack with no route = %v, want ErrUnsupported", err)
	}
	broker.Sync()
	if settled := broker.Settlements(); len(settled) != 0 {
		t.Fatalf("the refused settlement settled something: %+v", settled)
	}
	// The delivery is still settleable, which is what makes the refusal
	// recoverable: the core's own policy can take over.
	if err := d.Nack(); err != nil {
		t.Fatalf("the delivery was not settleable after the refusal: %v", err)
	}
}

// TestDelayedNackParksAndDeadLettersBack is the specification's `ttldlx` arm,
// end to end.
//
// What it proves is the whole mechanism: a delay queue is declared with the
// bucket's TTL, the message is published into it, the ORIGINAL is
// acknowledged only after the parked copy is confirmed, and the broker
// dead-letters it back to this subscription's own queue when the clock
// advances.
func TestDelayedNackParksAndDeadLettersBack(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.DelayKind = delay.TTLDLX
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("retry-me")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Sync()

	// The bucket: the default ladder doubles from 100 ms, so a three-second
	// delay is granted the 3.2-second bucket and the queue is named after it.
	const bucket = "orders.workers.delay.3200"
	q, ok := broker.Queue(bucket)
	if !ok {
		t.Fatalf("the delay queue %q was not declared; the broker holds %v", bucket, queueNames(broker))
	}
	if got := q.Args["x-message-ttl"]; got != int64(3200) {
		t.Errorf("x-message-ttl = %v, want 3200", got)
	}
	if got := q.Args["x-dead-letter-exchange"]; got != "" {
		t.Errorf("x-dead-letter-exchange = %v, want the default exchange: the topic's exchange would fan the redelivery out to every subscription", got)
	}
	if got := q.Args["x-dead-letter-routing-key"]; got != "orders.workers" {
		t.Errorf("x-dead-letter-routing-key = %v, want the subscription's own queue", got)
	}
	if got := broker.Ready(bucket); len(got) != 1 {
		t.Fatalf("the delay queue holds %d messages, want 1", len(got))
	}

	// Nothing comes back before the TTL elapses, which is the half a test
	// that only advanced the clock would not prove.
	broker.Advance(2 * time.Second)
	if got := broker.Ready("orders.workers"); len(got) != 0 {
		t.Fatalf("the message came back after 2s of a 3.2s delay: %d in the queue", len(got))
	}
	broker.Advance(2 * time.Second)
	again := receiveOne(t, cons)
	if string(again.Body) != "retry-me" {
		t.Fatalf("the redelivered body is %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestDelayedNackUsesThePluginWhenItIsDetected is the other arm and
// the detection the specification asks for.
//
// The two halves are both here: with the plugin installed the driver upgrades
// from the fallback and publishes to the delayed exchange with an `x-delay`
// header and NO delay queue at all; without it, the fallback is what runs.
func TestDelayedNackUsesThePluginWhenItIsDetected(t *testing.T) {
	broker, client := openFakeWith(t,
		rabbitfake.Config{DelayedMessagePlugin: true},
		func(c *rq.Config) {
			c.Declare, c.DeadLetter, c.DetectDelayPlugin = true, true, true
		})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("later")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Sync()
	if _, ok := broker.Queue("orders.workers.delay.3200"); ok {
		t.Error("the plugin arm declared a delay queue: the exchange holds the message and no queue is needed")
	}
	delayed := broker.Delayed()
	if len(delayed) != 1 {
		t.Fatalf("the broker is holding %d delayed messages, want 1", len(delayed))
	}
	if got := delayed[0].Headers["x-delay"]; got != int64(3000) {
		t.Errorf("x-delay = %v, want 3000: the plugin arm's delay is exact", got)
	}
	broker.Advance(3 * time.Second)
	again := receiveOne(t, cons)
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestDelayPluginDetectionIsNegativeWhenAbsent is the other direction of the
// detection, and it is the one that decides the default.
//
// answer is "plugin when detected, TTL+DLX fallback", and the
// fallback is the default precisely because the plugin is not in the official
// image. A driver that claimed the plugin without probing would fail at open
// against a stock broker.
func TestDelayPluginDetectionIsNegativeWhenAbsent(t *testing.T) {
	_, client := openFakeWith(t,
		rabbitfake.Config{DelayedMessagePlugin: false},
		func(c *rq.Config) { c.Declare, c.DetectDelayPlugin = true, true })
	b, ok := rq.BackendForTest(client)
	if !ok {
		t.Fatal("the client's Native is not this driver's backend")
	}
	facts := rq.FactsForTest(b)
	if !contains(facts, "delayplugin=false") {
		t.Errorf("the detection reported %s, want delayplugin=false", facts)
	}
	if !contains(facts, "delaykind=ttldlx") {
		t.Errorf("the driver did not fall back: %s", facts)
	}
	// And the capability table says so, which is what a caller reads.
	if s := client.Capabilities().Support(mq.FeatureScheduledPublish); s.Level != mq.Unsupported {
		t.Errorf("FeatureScheduledPublish is %v without the plugin: there is no other way to schedule a publication on this broker", s.Level)
	}
}

// TestConfirmStrategiesAllDeliver is the three arms driven through
// the real driver, under the confirm framings the LIBRARY interposes.
//
// The model already holds each strategy to its own cost claims; what this adds
// is that all three really publish when the broker confirms CUMULATIVELY (an
// ack the library expands into one confirmation per tag) and OUT OF ORDER (one
// the library resequences). A driver tested only against a broker that
// confirmed one tag at a time in tag order would never run either path.
//
// The batch width and the publication call are chosen per kind, and the reason
// is the specification's own result rather than a convenience. A framing that
// withholds confirms until N publications have arrived needs N publications in
// flight, and a SYNCHRONOUS `Publish` has exactly one: the specification makes the call's
// own confirm the boundary, so every blocking strategy waits for its own tag
// however wide its window is. So the window is only reachable through
// `PublishAsync` — or through several goroutines publishing at once — and that
// is what "windowed/async for batch" means in practice. The core's own batch
// loop does NOT reach it either: `PublisherHandle.publishBatch` calls
// `sink.Publish` once per message, serially, for a driver with no native
// BatchSink, so a batch of a hundred through a windowed publisher is a hundred
// round trips (recorded in the maintenance notes).
func TestConfirmStrategiesAllDeliver(t *testing.T) {
	for _, kind := range []confirm.Kind{confirm.PerMessage, confirm.Windowed, confirm.Async} {
		for _, style := range []rabbitfake.ConfirmStyle{
			rabbitfake.ConfirmPerTag, rabbitfake.ConfirmCumulative, rabbitfake.ConfirmReversed,
		} {
			t.Run(kind.String()+"/"+confirmStyleName(style), func(t *testing.T) {
				const n = 12
				batch := 1
				if kind.Resolve() != confirm.PerMessage {
					batch = 4
				}
				broker, client := openFakeWith(t,
					rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: style, Batch: batch}},
					func(c *rq.Config) {
						c.Declare = true
						c.ConfirmKind = kind
						if kind != confirm.PerMessage {
							c.ConfirmWindow = 8
						}
					})
				cons := consumeOn(t, client, "orders", "workers")
				pub := publishOn(t, client, "orders")

				if batch == 1 {
					for i := range n {
						if err := pub.Publish(t.Context(), &mq.Message{Body: []byte{byte('a' + i)}}); err != nil {
							t.Fatalf("publishing %d: %v", i, err)
						}
					}
				} else {
					// Asynchronously, so that `batch` publications really are
					// in flight and the framing has something to coalesce.
					done := make(chan error, n)
					for i := range n {
						if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte{byte('a' + i)}},
							func(r mq.PublishResult) { done <- r.Err }); err != nil {
							t.Fatalf("publishing %d: %v", i, err)
						}
					}
					for i := range n {
						select {
						case err := <-done:
							if err != nil {
								t.Fatalf("publication %d: %v", i, err)
							}
						case <-time.After(10 * time.Second):
							t.Fatalf("publication %d never completed", i)
						}
					}
				}
				if err := pub.Flush(t.Context()); err != nil {
					t.Fatalf("flushing: %v", err)
				}
				broker.Sync()
				for i := range n {
					d := receiveOne(t, cons)
					if err := d.Ack(); err != nil {
						t.Fatalf("acknowledging %d: %v", i, err)
					}
				}
			})
		}
	}
}

// confirmStyleName names a framing for a subtest.
func confirmStyleName(s rabbitfake.ConfirmStyle) string {
	switch s {
	case rabbitfake.ConfirmPerTag:
		return "pertag"
	case rabbitfake.ConfirmCumulative:
		return "cumulative"
	case rabbitfake.ConfirmReversed:
		return "reversed"
	default:
		return "manual"
	}
}

// TestUnroutablePublicationIsRejected is the specification's "Mandatory + NotifyReturn =
// Rejected".
//
// A publication no queue is bound for comes back as `basic.return` and is then
// acked: the broker took the message and threw it away. Reporting that ack as
// a success is the most expensive mistake this driver could make, because the
// caller then believes a message is in a queue that never received it.
func TestUnroutablePublicationIsRejected(t *testing.T) {
	broker, client := openFake(t, mandating)
	// A publisher on a topic whose exchange exists and has no binding: the
	// publication is routable nowhere.
	pub := publishOn(t, client, "orders")
	err := pub.Publish(t.Context(), &mq.Message{Body: []byte("nowhere")})
	if err == nil {
		t.Fatal("an unroutable publication succeeded: the broker returned it and then acked it, and reporting that as success tells the caller a message is in a queue that never received it")
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Fatalf("the refusal carries %v, want %v: %v", code, mq.CodeNotFound, err)
	}
	var oe *mq.OpError
	if errors.As(err, &oe) && oe.Retryable {
		t.Error("the refusal is marked retryable: retrying will throw the message away again, and the fix is the topology")
	}
	broker.Sync()
	if dropped := broker.Dropped(); len(dropped) != 1 {
		t.Errorf("the broker recorded %d drops, want 1", len(dropped))
	}
	// And the positive direction: with a queue bound, the same publication
	// succeeds.
	_ = consumeOn(t, client, "orders", "workers")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("somewhere")}); err != nil {
		t.Fatalf("a routable publication was refused: %v", err)
	}
}

// TestConfirmNoneDoesNotWait is the weakest boundary, and the point is that
// it is REACHABLE: a publisher that asked for no confirmation must not be put
// on the confirm path at all.
func TestConfirmNoneDoesNotWait(t *testing.T) {
	broker, client := openFakeWith(t,
		// The broker confirms nothing at all, so a publisher that waited
		// would hang.
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		declaring)
	// A consumer is opened so that the queue exists and the publication is
	// routable, and then CLOSED: a live consumer would take the message into
	// its own prefetch and the queue would read as empty.
	cons := consumeOn(t, client, "orders", "workers")
	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("a ConfirmNone publication waited for a confirm that never comes: %v", err)
	}
	broker.Sync()
	if got := broker.Ready("orders.workers"); len(got) != 1 {
		t.Errorf("the queue holds %d messages", len(got))
	}
}

// TestDurableConfirmNeedsAQuorumQueue is the rule that a publisher asking
// for a durable boundary fails at CONSTRUCTION on a profile that cannot define
// one.
func TestDurableConfirmNeedsAQuorumQueue(t *testing.T) {
	_, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.QueueType = true, rq.QueueClassic
	})
	_, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Confirmation: mq.ConfirmDurable})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("ConfirmDurable on a classic queue = %v, want ErrUnsupported: a confirm there is returned once the broker holds the message in memory, which is not a durable boundary", err)
	}
	// And the positive direction.
	_, quorum := openFake(t, declaring)
	if _, err := quorum.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Confirmation: mq.ConfirmDurable}); err != nil {
		t.Fatalf("ConfirmDurable on a quorum queue was refused: %v", err)
	}
}

// TestSeekIsRefused is the negative direction of the two seek claims, made
// provable.
//
// A queue has no history and no position, so a driver that accepted a start
// position and ignored it would leave a caller planning on a replay that
// silently never happens. Refusing at open is what makes the capability
// table's Unsupported mean something.
func TestSeekIsRefused(t *testing.T) {
	_, client := openFake(t, declaring)
	for _, start := range []mq.StartPosition{
		mq.StartAt(time.Unix(0, 0)),
		mq.StartAtSequence(1),
	} {
		_, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers", Start: start})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("a consumer with start %v = %v, want ErrUnsupported", start, err)
		}
	}
	// The two a queue CAN express are accepted, and they are the same place.
	for _, start := range []mq.StartPosition{mq.StartLatest(), mq.StartEarliest()} {
		c, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers", Start: start})
		if err != nil {
			t.Errorf("a consumer with start %v was refused: %v", start, err)
			continue
		}
		_ = c.Close()
	}
}

// TestPriorityIsRefusedWithoutTheQueueArgument is the refusal that makes
// Priority honest on this broker.
//
// RabbitMQ IGNORES a priority on a message to a queue that was not declared
// with `x-max-priority`: the publication succeeds and the ordering is not what
// the caller asked for, which is the worst kind of unsupported. The driver
// advertises it Unsupported and refuses the option.
func TestPriorityIsRefusedWithoutTheQueueArgument(t *testing.T) {
	_, client := openFake(t, declaring)
	_ = consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}, mq.Priority(5))
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a priority with no x-max-priority = %v, want ErrUnsupported: the broker would accept the publication and ignore the priority", err)
	}
	// With the ceiling declared the same publication is accepted.
	_, withPriority := openFake(t, func(c *rq.Config) {
		c.Declare, c.MaxPriority, c.QueueType = true, 9, rq.QueueClassic
	})
	_ = consumeOn(t, withPriority, "orders", "workers")
	pub2 := publishOn(t, withPriority, "orders")
	if err := pub2.Publish(t.Context(), &mq.Message{Body: []byte("x")}, mq.Priority(5)); err != nil {
		t.Fatalf("a priority with x-max-priority declared was refused: %v", err)
	}
}
