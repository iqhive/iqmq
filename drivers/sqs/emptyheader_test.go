package sqs_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestAnEmptyHeaderValueRoundTrips publishes a header whose value is empty.
// SQS refuses a message attribute with an empty value, of either type, so the
// driver used to fail the whole publication for one; it now carries the value
// in a form the service takes and hands back an empty value on receipt.
func TestAnEmptyHeaderValueRoundTrips(t *testing.T) {
	h := open(t)
	name := topic(t, "emptyheader")
	m := &mq.Message{Body: []byte("payload")}
	m.Headers.Add("note", []byte{})
	m.Headers.Add("kind", []byte("order"))
	h.publish(name, m)
	d := h.receiveOne(h.consumer(name, 4), 5*time.Second)
	if v, ok := d.Headers.Get("note"); !ok || len(v) != 0 {
		t.Fatalf("empty header = %q, %v; want present and empty", v, ok)
	}
	if v, ok := d.Headers.Get("kind"); !ok || string(v) != "order" {
		t.Fatalf("header kind = %q, %v", v, ok)
	}
}
