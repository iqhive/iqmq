package snssqs_test

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestACharacterOutsideTheBMPReachesTheQueue publishes an identifier, a
// property and a header value holding characters outside the Basic
// Multilingual Plane, alone and in a batch. SNS refuses such a character in a
// String message attribute (it calls it a surrogate value) although the queue
// service takes it, and the shared SQS encoder sent each of them as a String,
// so one emoji failed the whole publication as InvalidArgument. They now go to
// SNS as Binary and come back from the queue unchanged.
func TestACharacterOutsideTheBMPReachesTheQueue(t *testing.T) {
	h := open(t)
	name := topic(t, "surrogate")
	h.provision(name, "s")
	message := func(i int) *mq.Message {
		m := &mq.Message{Body: []byte("payload " + strconv.Itoa(i)), ID: "order-" + strconv.Itoa(i) + "-\U0001F600"}
		m.Properties.CorrelationID = "trace-\U0001F680"
		m.Headers.Add("mood", []byte("ok \U0001F642"))
		m.Headers.Add("plain", []byte("ascii"))
		return m
	}
	h.publish(name, message(0))
	p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	if err := p.PublishBatch(t.Context(), []*mq.Message{message(1), message(2)}); err != nil {
		t.Fatalf("PublishBatch of messages whose metadata holds an emoji: %v", err)
	}

	ds := h.receive(h.consumer(name, "s", 4), 3, 5*time.Second)
	got := make(map[string]*mq.Delivery, len(ds))
	for i := range ds {
		got[ds[i].ID] = &ds[i]
	}
	for i := range 3 {
		want := message(i)
		d, ok := got[want.ID]
		if !ok {
			t.Errorf("no delivery carries the identifier %q", want.ID)
			continue
		}
		if !bytes.Equal(d.Body, want.Body) {
			t.Errorf("message %q: body %q, want %q", want.ID, d.Body, want.Body)
		}
		if d.Properties.CorrelationID != want.Properties.CorrelationID {
			t.Errorf("message %q: correlation identifier %q, want %q", want.ID, d.Properties.CorrelationID, want.Properties.CorrelationID)
		}
		for _, k := range []string{"mood", "plain"} {
			v, _ := d.Headers.Get(k)
			if w, _ := want.Headers.Get(k); !bytes.Equal(v, w) {
				t.Errorf("message %q: header %q = %q, want %q", want.ID, k, v, w)
			}
		}
	}
}
