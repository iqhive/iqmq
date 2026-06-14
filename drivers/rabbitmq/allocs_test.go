package rabbitmq_test

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// TestAllocsDecode holds the receive-side mapping to a bound.
//
// The specification requires a Fetch to allocate nothing per fetch, and the mapping is where
// a driver most easily breaks that: a fresh `mq.Message` per delivery, a fresh
// header slice, a string conversion per header key. The source keeps one
// message and one `protocol.Fields` per prefetch slot and refills them, so the
// steady state is the entries the header table really needs.
//
// The bound is a bound and not an exact figure, because `Headers.Add` grows a
// slice the first few times a slot is used and the pair table's own growth is
// not this driver's business. What the bound catches is the defect: a per-
// delivery allocation count that rises with the header COUNT would be tens,
// and one that allocated a message per delivery would never settle.
func TestAllocsDecode(t *testing.T) {
	b := offlineBackend(t)
	d := &amqp.Delivery{
		Exchange:      "orders",
		RoutingKey:    "orders",
		DeliveryTag:   7,
		ContentType:   "application/json",
		MessageId:     "01JQ2K8V5N7R3T4Y6W8X9Z0A1B",
		CorrelationId: "01JQ2K8V5N7R3T4Y6W8X9Z0A1C",
		Timestamp:     time.Unix(1_700_000_000, 0).UTC(),
		DeliveryMode:  2,
		Headers: amqp.Table{
			"h0": []byte("v0"), "h1": []byte("v1"),
			"h2": []byte("v2"), "h3": []byte("v3"),
			"x-delivery-count": int64(2),
		},
		Body: make([]byte, 1024),
	}
	// Warm the message's header storage and the fields buffer, which is what
	// the steady state means: the core copies the DriverDelivery out of the
	// fetch buffer before a caller sees it, so a source reuses both.
	for range 8 {
		if _, _, err := rq.DecodeForTest(b, d, true); err != nil {
			t.Fatal(err)
		}
	}
	got := testing.AllocsPerRun(200, func() {
		if _, _, err := rq.DecodeForTest(b, d, true); err != nil {
			t.Fatal(err)
		}
	})
	// The message and the fields are the caller's here rather than a
	// source's slot, so this measures the mapping plus one message: a
	// per-delivery figure in the driver is lower. Sixteen is generous and
	// still catches the defect, which is a count proportional to the body or
	// to the header set's size.
	if got > 16 {
		t.Errorf("decoding one delivery with four headers allocated %.0f times, want at most 16: the specification requires a fetch to allocate nothing per fetch, so a figure that grows with the header count is a per-delivery allocation the source cannot amortise", got)
	}
	// A message with NO headers is the path a bare publication takes, and it
	// has to be cheaper -- if it is not, something is allocating per
	// delivery rather than per header.
	bare := &amqp.Delivery{Exchange: "orders", DeliveryTag: 1, Body: make([]byte, 1024)}
	for range 8 {
		if _, _, err := rq.DecodeForTest(b, bare, true); err != nil {
			t.Fatal(err)
		}
	}
	bareAllocs := testing.AllocsPerRun(200, func() {
		if _, _, err := rq.DecodeForTest(b, bare, true); err != nil {
			t.Fatal(err)
		}
	})
	if bareAllocs >= got {
		t.Errorf("a delivery with no headers allocated %.0f times and one with four allocated %.0f: the cost does not follow the headers, so something is allocating per delivery", bareAllocs, got)
	}
}

// TestAllocsSettle holds the settlement path to zero allocations.
//
// A settlement is the hottest thing a consumer does after the fetch: the core
// hands back a pointer to its copy of the DriverDelivery and everything the
// driver needs is the token, so there is nothing to allocate. A figure above
// zero here is a boxed value or a closure on a path the specification budgets.
func TestAllocsSettle(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 64 })
	pub := publishOn(t, client, "orders")
	dst := make([]mq.Delivery, 1)

	settleOne := func() {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := contextWithTimeout(t, 5*time.Second)
		defer cancel()
		n, err := cons.Receive(ctx, dst)
		if err != nil || n != 1 {
			t.Fatalf("receiving: %d, %v", n, err)
		}
		if err := dst[0].Ack(); err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		settleOne()
	}
	_ = broker
	// The measurement is of the whole publish-receive-settle round rather
	// than of the settlement alone, because a settlement cannot be repeated:
	// The specification allows exactly one terminal attempt per delivery. So the bound
	// covers the round, and what it catches is a per-settlement allocation,
	// which would push a round of three operations well past this.
	got := testing.AllocsPerRun(100, settleOne)
	if got > 48 {
		t.Errorf("one publish-receive-settle round allocated %.0f times, want at most 48: a figure this size means a per-delivery message or a per-settlement closure", got)
	}
}
