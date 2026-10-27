package rabbitmq_test

import (
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestBothRedeliveryCounterNamesAreRead is the version matrix's most
// consequential finding, held by a test.
//
// RabbitMQ renamed the quorum queue's redelivery counter: 3.13.7 and 4.1.8
// send `x-delivery-count` and 4.3.5 sends `x-acquired-count`. A driver that
// read only the documented name would report Attempt 1 on every delivery from
// a 4.3 broker, so MaxAttempts would never fire and a poison message
// would be retried for ever with nothing to say why.
//
// Both names are driven through the same code, which is the point: the arm
// names the header and everything else is identical, so a reading that handles
// one and not the other fails exactly one arm.
func TestBothRedeliveryCounterNamesAreRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"x-delivery-count (RabbitMQ 3.13, 4.1)", rabbitfake.DeliveryCountHeader},
		{"x-acquired-count (RabbitMQ 4.3)", rabbitfake.AcquiredCountHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, client := openFakeWith(t,
				rabbitfake.Config{DeliveryCountName: tc.header}, declaring)
			_ = broker
			cons := consumeOn(t, client, "orders", "workers")
			pub := publishOn(t, client, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("again")}); err != nil {
				t.Fatalf("publishing: %v", err)
			}
			first := receiveOne(t, cons)
			if got := first.Info().Attempt; got != 1 {
				t.Fatalf("the first delivery reports Attempt %d, want 1", got)
			}
			if err := first.Nack(mq.NackRequeue()); err != nil {
				t.Fatalf("nack: %v", err)
			}
			second := receiveOne(t, cons)
			if got := second.Info().Attempt; got != 2 {
				t.Fatalf("the redelivery reports Attempt %d under %q, want 2: MaxAttempts is compared against a count that never rises", got, tc.header)
			}
			if err := second.Ack(); err != nil {
				t.Fatalf("ack: %v", err)
			}
		})
	}
}

// TestNilAndEmptyBodiesStayApart is the observable distinction over a
// protocol that does not have it.
//
// An AMQP 0-9-1 body is a sequence of content frames, and a zero-length body
// is no content frames -- which is exactly what a nil body is too. The driver
// carries the one bit in a reserved header, and this is what says the two
// round-trip as themselves.
func TestNilAndEmptyBodiesStayApart(t *testing.T) {
	_, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	for _, body := range [][]byte{nil, {}} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
			t.Fatalf("publishing a %d-byte body: %v", len(body), err)
		}
	}
	for i, want := range [][]byte{nil, {}} {
		d := receiveOne(t, cons)
		got := d.Body
		if (got == nil) != (want == nil) {
			t.Errorf("body %d came back nil=%v, want nil=%v: the specification makes the distinction observable", i, got == nil, want == nil)
		}
		if len(got) != 0 {
			t.Errorf("body %d came back %d bytes long", i, len(got))
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	// And a header the application can see is not created by it: the carrier
	// is under the reserved prefix, so it must not reach a caller.
	if err := pub.Publish(t.Context(), &mq.Message{Body: nil}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	if _, ok := d.Headers.Get(rq.NilBodyHeader); ok {
		t.Errorf("the nil-body carrier %q reached the caller's headers", rq.NilBodyHeader)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestDelayedRedeliveryCarriesTheAttempt is the count surviving the one
// mechanism that destroys it.
//
// A delayed redelivery is a RE-PUBLICATION: the parked copy is a new queue
// entry, so the broker's own count restarts at zero and every delayed
// redelivery would report attempt 1 for ever. A caller whose policy is
// "dead-letter after three attempts" would then retry a poison message without
// end. The driver carries the count itself.
func TestDelayedRedeliveryCarriesTheAttempt(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("later")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	// Three seconds, because the default ladder doubles from 100 ms and the
	// granted bucket is therefore 3.2 seconds: the advance has to clear the
	// BUCKET and not the request.
	const delay = 3 * time.Second
	d := receiveOne(t, cons)
	if got := d.Info().Attempt; got != 1 {
		t.Fatalf("the first delivery reports Attempt %d, want 1", got)
	}
	// Twice, because one hop would be satisfied by a driver that hard-coded
	// two: the count has to keep RISING across successive delayed
	// redeliveries, which is what MaxAttempts compares against.
	for want := uint32(2); want <= 3; want++ {
		if err := d.Nack(mq.NackDelay(delay)); err != nil {
			t.Fatalf("a delayed nack: %v", err)
		}
		broker.Sync()
		broker.Advance(4 * time.Second)
		broker.Sync()
		d = receiveOne(t, cons)
		if got := d.Info().Attempt; got != want {
			t.Fatalf("the delayed redelivery reports Attempt %d, want %d: the parked copy is a new queue entry and the broker's count restarts", got, want)
		}
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestAnImmediateRequeueOfAParkedCopyRaisesTheAttempt is the carried count
// meeting the broker's own on the parked copy.
//
// The parked copy is a new queue entry and its `x-delivery-count` starts again
// from nothing, so the attempt is what the driver carried PLUS what the broker
// has counted since. Taking the larger of the two instead held the attempt
// still across immediate requeues (3, 3, 3, 4 after two delayed redeliveries)
// until the new entry's count overtook the carried one, and a MaxAttempts
// policy let the message through that many extra times.
func TestAnImmediateRequeueOfAParkedCopyRaisesTheAttempt(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("again")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	// Two delayed redeliveries, for TestDelayedRedeliveryCarriesTheAttempt's
	// three-second bucket: the parked copy then carries attempt 2.
	for range 2 {
		if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
			t.Fatalf("a delayed nack: %v", err)
		}
		broker.Sync()
		broker.Advance(4 * time.Second)
		broker.Sync()
		d = receiveOne(t, cons)
	}
	if got := d.Info().Attempt; got != 3 {
		t.Fatalf("the second delayed redelivery reports Attempt %d, want 3", got)
	}
	for want := uint32(4); want <= 6; want++ {
		if err := d.Nack(mq.NackRequeue()); err != nil {
			t.Fatalf("an immediate requeue: %v", err)
		}
		broker.Sync()
		d = receiveOne(t, cons)
		if got := d.Info().Attempt; got != want {
			t.Fatalf("an immediate requeue of the parked copy reports Attempt %d, want %d: the broker's count on the new entry adds to the carried one", got, want)
		}
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestAsyncPublishDoesNotWaitForItsOwnConfirm is the contract on the path
// the per-message strategy nearly broke.
//
// `PublishAsync` under the default per-message strategy used to call the same
// wait a synchronous publish does, and the confirm loop had already delivered
// the outcome to the callback -- so the wait had nothing left to wake it and
// every asynchronous publication failed with a deadline. The three clauses
// are what this holds: a nil return, a callback that runs exactly once, and an
// outcome that is the publication's own.
func TestAsyncPublishDoesNotWaitForItsOwnConfirm(t *testing.T) {
	_, client := openFake(t, declaring)
	_ = consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	const n = 8
	type outcome struct {
		body string
		err  error
	}
	got := make(chan outcome, n*2)
	for i := range n {
		body := "async-" + string(rune('a'+i))
		m := &mq.Message{Body: []byte(body)}
		if err := pub.PublishAsync(t.Context(), m, func(r mq.PublishResult) {
			got <- outcome{body, r.Err}
		}); err != nil {
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	seen := map[string]int{}
	for range n {
		select {
		case o := <-got:
			seen[o.body]++
			if o.err != nil {
				t.Errorf("the asynchronous publication %q reported %v", o.body, o.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d callbacks ran: an asynchronous publish that waits for its own confirm never gets one", len(seen), n)
		}
	}
	if len(seen) != n {
		t.Fatalf("%d distinct callbacks ran, want %d: %v", len(seen), n, seen)
	}
	select {
	case o := <-got:
		t.Fatalf("the callback for %q ran twice: the specification says exactly once, and a caller that releases a pooled message in it releases it twice", o.body)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestTheConsumerAttachesOnTheFirstFetch is the lazy attachment, and the two
// broker behaviours that depend on it.
//
// RabbitMQ PUSHES: a `basic.consume` makes the broker start sending, and every
// message it sends counts against the prefetch and starts its acknowledgement
// deadline whether or not anybody has asked for a delivery. So the consume is
// the first Fetch's, not the open's -- and what falls out is that a message
// expires by its own TTL while it is QUEUED and a priority queue serves the
// most urgent message it HOLDS.
func TestTheConsumerAttachesOnTheFirstFetch(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	if got := consumerTags(broker); got != 0 {
		t.Fatalf("%d consumers are registered before the first Fetch: the broker is pushing into a consumer nobody has read from, and every message it sends is a lease ticking against a caller who has not asked for one", got)
	}
	if _, ok := broker.Queue("orders.workers"); !ok {
		t.Fatalf("the subscription's queue was not declared at open: a publication made before the first Fetch would be discarded. Queues: %v", queueNames(broker))
	}
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("queued")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	if got := consumerTags(broker); got != 1 {
		t.Fatalf("%d consumers are registered after the first Fetch, want 1", got)
	}
	if string(d.Body) != "queued" {
		t.Fatalf("the delivery carries %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestAPriorityQueueServesWhatItHolds is the first of the two behaviours lazy
// attachment makes observable.
//
// The order is the BROKER's and is decided over the messages a queue holds. A
// consumer that was already attached when the first message arrived has it
// pushed immediately, and no priority can overtake a message that has already
// left the queue -- which is why the conformance kit's own probe, which opens
// the consumer first and publishes second, failed against a real broker.
func TestAPriorityQueueServesWhatItHolds(t *testing.T) {
	_, client := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.MaxPriority = 9
		c.QueueType = rq.QueueClassic
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	for _, p := range []struct {
		body     string
		priority int
	}{{"low", 0}, {"high", 9}} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(p.body)}, mq.Priority(p.priority)); err != nil {
			t.Fatalf("publishing %s: %v", p.body, err)
		}
	}
	d := receiveOne(t, cons)
	if string(d.Body) != "high" {
		t.Fatalf("the first delivery is %q, want the higher-priority message: a consumer attached before the publications has them pushed in arrival order and priority buys nothing", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestAWildcardTopicIsRefusedAsUnsupported is the negative half of
// FeatureWildcardSubscribe, and the sentinel is the whole point.
//
// A topic exchange HAS wildcards; this driver's topology cannot use them for
// what FeatureWildcardSubscribe names, because a topic maps to its own
// exchange and a pattern over topic names would have to bind one queue to
// every matching exchange. The refusal has to be ErrUnsupported: the specification's
// negative probe reads that sentinel and no other, and an invalid-argument
// refusal would leave the claim unverifiable.
func TestAWildcardTopicIsRefusedAsUnsupported(t *testing.T) {
	_, client := openFake(t, declaring)
	if s := client.Capabilities().Support(mq.FeatureWildcardSubscribe); s.Level != mq.Unsupported {
		t.Fatalf("FeatureWildcardSubscribe is claimed at %v", s.Level)
	}
	for _, topic := range []string{"orders.#", "orders.*", "#"} {
		_, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
		if err == nil {
			t.Fatalf("a consumer of the pattern %q was accepted by a driver that claims no wildcards", topic)
		}
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("the pattern %q was refused with %v (code %v), want ErrUnsupported", topic, err, mq.CodeOf(err))
		}
	}
}

// TestDeadLetterTopicRoutesToAnotherTopic is the destination being a place
// a caller can read from.
//
// The derived `<queue>.dlq` is a QUEUE and not a topic, so nothing can consume
// it through mq.Client. [rq.Config.DeadLetterTopic] points the route at
// another topic's exchange instead, and this is what says a rejected message
// arrives there -- through the portable API, as a delivery.
func TestDeadLetterTopicRoutesToAnotherTopic(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter, c.DeadLetterTopic = true, true, "parked"
	})
	dlq := consumeOn(t, client, "parked", "audit")
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	if err := d.Reject(); err != nil {
		t.Fatalf("rejecting: %v", err)
	}
	broker.Sync()
	dead := receiveOne(t, dlq)
	if string(dead.Body) != "poison" {
		t.Fatalf("the dead-letter topic holds %q", dead.Body)
	}
	if err := dead.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// And the derived queue was NOT declared: it is nobody's destination
	// here, and a queue nothing consumes that receives dead letters is a
	// silent message sink.
	if _, ok := broker.Queue("orders.workers.dlq"); ok {
		t.Errorf("the derived dead-letter queue was declared as well as the named topic, so a rejected message has two destinations. Queues: %v", queueNames(broker))
	}
}
