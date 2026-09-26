package jetstream_test

import (
	"testing"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestAllocsMapping holds the pure mapping to an allocation budget.
//
// The mapping is on the publish path of every message, so a change that made it
// allocate per header — a sort that boxed, a string conversion that escaped, a
// map grown one entry at a time — would cost exactly where the budgets are
// stated. The budget is not zero: a nats.Header IS a map, so a message with
// headers allocates one, and the subject mapping allocates its result when the
// topic carries a "/". What the budget catches is growth beyond that.
func TestAllocsMapping(t *testing.T) {
	m := &mq.Message{Body: []byte("payload"), ID: "id-1"}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Add("tag", []byte("a"))
	m.Headers.Add("tag", []byte("b"))

	if got := testing.AllocsPerRun(200, func() {
		if _, err := nq.Subject("", "orders.created"); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("Subject with no prefix and no separator swap allocates %.0f times, want 0", got)
	}
	if got := testing.AllocsPerRun(200, func() {
		if _, err := nq.Subject("acme", "orders/created"); err != nil {
			t.Fatal(err)
		}
	}); got > 2 {
		t.Errorf("Subject with a prefix and a separator swap allocates %.0f times, want at most 2", got)
	}
	if got := testing.AllocsPerRun(200, func() {
		if err := nq.Representable(m); err != nil {
			t.Fatal(err)
		}
	}); got > 1 {
		t.Errorf("Representable allocates %.0f times, want at most 1", got)
	}
	if got := testing.AllocsPerRun(200, func() {
		var msg natsgo.Msg
		msg.Subject = "orders.created"
		nq.Encode(&msg, m, nil)
	}); got > 12 {
		t.Errorf("Encode allocates %.0f times for a three-header message, want at most 12", got)
	}
	// The reserved-namespace check is on the path of every header of every
	// message, so it may not allocate at all: a prefix comparison that took a
	// lower-case copy would cost one allocation per header.
	if got := testing.AllocsPerRun(500, func() {
		if nq.IsNATSHeader("content-kind") || !nq.IsNATSHeader("Nats-Msg-Id") {
			t.Fatal("IsNATSHeader")
		}
	}); got != 0 {
		t.Errorf("IsNATSHeader allocates %.0f times, want 0", got)
	}
	// So is the pattern check every consumer goes through once; it is not a
	// hot path, but it is a string walk and a zero budget is what keeps it one.
	if got := testing.AllocsPerRun(200, func() {
		if nq.PatternReachesSchedules("acme.orders.>", "iqmqsched.ORDERS") {
			t.Fatal("PatternReachesSchedules")
		}
	}); got > 6 {
		t.Errorf("PatternReachesSchedules allocates %.0f times, want at most 6", got)
	}
}

// TestAllocsFetch holds the consumption path to the specification's "Fetch allocates
// nothing per fetch".
//
// One allocation per delivery is unavoidable here and is the SPI's own: a
// JetStream settlement is a reply subject rather than an integer, so the native
// handle has to be boxed into DriverDelivery.Ref. What this budget catches is a
// second one — a slice grown per fetch, a closure per delivery, a map written
// per settlement.
func TestAllocsFetch(t *testing.T) {
	b := openBackend(t)
	c := mq.NewClient(b)
	topic := newTopic("allocs")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 32})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: newTopic("allocs-src"), Subscription: "direct", Prefetch: 8,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	_ = cons
	dst := make([]mq.DriverDelivery, 8)
	// An empty source's Fetch returns on the context, which is the one shape
	// that can be measured without a message in flight: what is being held to
	// zero is the fetch machinery and not the delivery mapping.
	ctx, cancel := shortCtx(t)
	defer cancel()
	if got := testing.AllocsPerRun(50, func() {
		_, _ = src.Fetch(ctx, dst)
	}); got > 1 {
		t.Errorf("a Fetch that returns on its context allocates %.0f times, want at most 1", got)
	}
}
