package pulsar_test

import (
	"bytes"
	"context"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestSmokeRoundTrip is the first thing to run and the first thing to break:
// one publication, one delivery, one acknowledgement, through the in-process
// model.
func TestSmokeRoundTrip(t *testing.T) {
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)

	m := &mq.Message{Key: []byte("customer-7"), Body: []byte("hello"), ID: "id-1"}
	m.Headers.Set("kind", []byte("order"))
	m.Properties.ContentType = "application/json"
	publish(t, snk, m)

	d := fetchOne(t, src)
	if !bytes.Equal(d.Message.Body, []byte("hello")) {
		t.Fatalf("the body came back as %q", d.Message.Body)
	}
	if string(d.Message.Key) != "customer-7" {
		t.Fatalf("the key came back as %q", d.Message.Key)
	}
	if d.Message.ID != "id-1" {
		t.Fatalf("the identifier came back as %q", d.Message.ID)
	}
	if v, ok := d.Message.Headers.Get("kind"); !ok || string(v) != "order" {
		t.Fatalf("the header came back as %q (present %v)", v, ok)
	}
	if d.Message.Properties.ContentType != "application/json" {
		t.Fatalf("the content type came back as %q", d.Message.Properties.ContentType)
	}
	if d.Info.Topic != "orders" {
		t.Fatalf("the delivery reports topic %q", d.Info.Topic)
	}
	if d.Info.Attempt != 1 {
		t.Fatalf("the first delivery reports attempt %d, want 1", d.Info.Attempt)
	}
	if d.Info.LeaseExpires.IsZero() {
		t.Fatal("the delivery carries no lease, and this rig configures one")
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if st := server.Stats("persistent://public/default/orders", "workers"); st.Acked != 1 {
		t.Fatalf("the broker acknowledged %d message(s), want 1", st.Acked)
	}
}

// TestSmokeNilAndEmptyBodiesSurviveNatively is the one distinction Pulsar keeps
// that most brokers do not: its message metadata carries a null-value flag, so
// a nil body and an empty one are two different messages without an envelope
func TestSmokeNilAndEmptyBodiesSurviveNatively(t *testing.T) {
	b, _ := newRig(t, nil)
	src := sourceOf(t, b, "bodies", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "bodies", mq.MetadataNativeStrict)

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"bytes", []byte{0x00, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			publish(t, snk, &mq.Message{Body: tc.body})
			d := fetchOne(t, src)
			if (d.Message.Body == nil) != (tc.body == nil) {
				t.Fatalf("a %s body came back as %#v", tc.name, d.Message.Body)
			}
			if !bytes.Equal(d.Message.Body, tc.body) {
				t.Fatalf("the body came back as %q, want %q", d.Message.Body, tc.body)
			}
			if err := src.Settler().Ack(context.Background(), &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}
