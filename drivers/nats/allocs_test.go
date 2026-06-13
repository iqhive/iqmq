package nats_test

import (
	"testing"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
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
}
