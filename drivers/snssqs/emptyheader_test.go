package snssqs_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestAnEmptyHeaderValueRoundTrips publishes a header whose value is empty.
// SNS, like SQS, refuses a message attribute with an empty value, so the
// driver used to fail the whole publication for one; it now carries the value
// in a form both services take and hands back an empty value on receipt.
func TestAnEmptyHeaderValueRoundTrips(t *testing.T) {
	h := open(t)
	name := topic(t, "emptyheader")
	h.provision(name, "s")
	m := &mq.Message{Body: []byte("payload")}
	m.Headers.Add("note", []byte{})
	h.publish(name, m)
	d := h.receiveOne(h.consumer(name, "s", 4), 5*time.Second)
	if v, ok := d.Headers.Get("note"); !ok || len(v) != 0 {
		t.Fatalf("empty header = %q, %v; want present and empty", v, ok)
	}
}
