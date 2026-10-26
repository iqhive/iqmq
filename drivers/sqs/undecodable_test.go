package sqs_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// TestAnUndecodableMessageWaitsOutItsWindow is the poller that never stopped
// receiving one message.
//
// A message the driver could not decode was made visible again the moment it
// was received, on the reasoning that the queue's redrive policy would count
// the receives and take it to the dead-letter queue. Visible again, it was the
// first thing the next ReceiveMessage returned, so the poller spun on it: about
// 40,000 calls a second against this fake, each one a billed request on AWS,
// and for ever on a queue with no redrive policy. It now keeps the visibility
// the receive gave it, so it is received once per AckDeadline, and the
// messages around it are handed out as before.
//
// The second case is the dead-letter queue configured as the source's own
// queue. A copy sent there would be received straight back, which is the same
// spin with a send and a delete added, so the message is left alone there too.
func TestAnUndecodableMessageWaitsOutItsWindow(t *testing.T) {
	const deadline = 30 * time.Second
	for _, tc := range []struct {
		name string
		// self routes the dead letters to the source's own queue.
		self bool
	}{
		{"no dead-letter queue", false},
		{"dead-letter queue is the source queue", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := topic(t, "undecodable-window")
			dlq := ""
			if tc.self {
				q, err := sq.QueueName(name, "", false)
				if err != nil {
					t.Fatalf("QueueName(%q): %v", name, err)
				}
				dlq = q
			}
			api := newObservedAPI()
			h := openObserved(t, api, func(c *sq.Config) {
				c.Pollers = 1
				c.AckDeadline = deadline
				c.DeadLetterQueue = dlq
			})
			h.publish(name, &mq.Message{Body: []byte("before")})
			sendUndecodable(t, h, api, name, 1)
			h.publish(name, &mq.Message{Body: []byte("after")})

			cons := h.consumer(name, 4)
			ackBodies(t, h.receive(cons, 2, 5*time.Second), "after", "before")
			// The fake's clock stands still until the test moves it, so a
			// message that keeps its window is received once and no more.
			h.expectNothing(cons, 200*time.Millisecond, "undecodable message handed out")
			if got := receipts(api); got != 3 {
				t.Fatalf("the poller received %d messages from a queue of three with the clock standing still; "+
					"the undecodable one is being made visible again and received on every round", got)
			}
			if got := releases(api); got != 0 {
				t.Fatalf("the source made %d visibility changes to zero; nothing should be released", got)
			}

			// Once its window has passed it comes back, once.
			h.server.Advance(deadline)
			waitFor(t, 5*time.Second, "the undecodable message to be received again after its window",
				func() bool { return receipts(api) == 4 })
			h.expectNothing(cons, 200*time.Millisecond, "undecodable message handed out")
			if got := receipts(api); got != 4 {
				t.Fatalf("the poller received %d messages after one visibility window, want 4: "+
					"the undecodable message once per window", got)
			}
			if got := h.depth(name); got != 1 {
				t.Fatalf("the queue holds %d messages, want the undecodable one: it must not be destroyed", got)
			}
		})
	}
}

// TestAnUndecodableMessageIsDeadLettered: with Config.DeadLetterQueue set, a
// message the driver cannot decode is moved there as it arrives, by the same
// send-then-delete a Reject makes. The copy is the raw message, with the
// attributes it was published with and the provenance saying why it moved.
func TestAnUndecodableMessageIsDeadLettered(t *testing.T) {
	api := newObservedAPI()
	h := openObserved(t, api, func(c *sq.Config) {
		c.Pollers = 1
		c.DeadLetterQueue = "dead"
	})
	name := topic(t, "undecodable-dlq")
	h.publish(name, &mq.Message{Body: []byte("before")})
	sendUndecodable(t, h, api, name, 1)
	h.publish(name, &mq.Message{Body: []byte("after")})

	cons := h.consumer(name, 4)
	ackBodies(t, h.receive(cons, 2, 5*time.Second), "after", "before")
	waitFor(t, 5*time.Second, "the source queue to empty", func() bool { return h.depth(name) == 0 })
	if got := h.server.Bodies("dead"); len(got) != 1 || got[0] != "undecodable" {
		t.Fatalf("the dead-letter queue holds %q, want the undecodable message's raw body", got)
	}
	out, err := api.Server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(api.QueueURL("dead")),
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil || len(out.Messages) != 1 {
		t.Fatalf("reading the dead-letter queue: %v", err)
	}
	attrs := out.Messages[0].MessageAttributes
	if got := aws.ToString(attrs["iqmq-enc"].StringValue); got != "bogus" {
		t.Errorf("the copy's iqmq-enc is %q, want the original's %q", got, "bogus")
	}
	if got := aws.ToString(attrs["iqmq-dlq-origin"].StringValue); got != h.queue(name) {
		t.Errorf("the copy's iqmq-dlq-origin is %q, want %q", got, h.queue(name))
	}
	if got := aws.ToString(attrs["iqmq-dlq-reason"].StringValue); !strings.HasPrefix(got, "undecodable: ") {
		t.Errorf("the copy's iqmq-dlq-reason is %q, want one saying it could not be decoded", got)
	}
	h.expectNothing(cons, 100*time.Millisecond, "undecodable message handed out")
	if got := receipts(api); got != 3 {
		t.Fatalf("the poller received %d messages from a queue of three; the undecodable one is going round", got)
	}
	if got := releases(api); got != 0 {
		t.Fatalf("the source made %d visibility changes to zero; a dead-lettered message has nothing to release", got)
	}
	if got := h.back.Health(); got.State != mq.HealthReady {
		t.Fatalf("Health is %v (%v) after a message was dead-lettered; nothing is wrong", got.State, got.Err)
	}
}

// ackBodies acknowledges ds and fails unless their bodies are want, in sorted
// order.
func ackBodies(t *testing.T, ds []mq.Delivery, want ...string) {
	t.Helper()
	got := make([]string, 0, len(ds))
	for i := range ds {
		got = append(got, string(ds[i].Body))
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("received %q, want the decodable messages %q", got, want)
	}
}

// receipts is how many messages the observed fake's ReceiveMessage answers
// have carried: every receive mints a fresh receipt handle.
func receipts(api *observedAPI) int {
	api.mu.Lock()
	defer api.mu.Unlock()
	return len(api.received)
}

// releases is how many visibility changes to zero the source has made.
func releases(api *observedAPI) int {
	api.mu.Lock()
	defer api.mu.Unlock()
	total := 0
	for _, n := range api.released {
		total += n
	}
	return total
}
