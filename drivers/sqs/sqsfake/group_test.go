package sqsfake

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The FIFO group rule this server models, and the one state it used to reach
// that the service cannot: a group held by a message that is VISIBLE.
//
// SQS blocks a message group while one of its messages is in flight and
// releases it when that message becomes visible again or is deleted. The
// server used to keep the hold in a counter decremented by a delete, by a
// visibility set to zero and by ExpireVisibility — and NOT by the clock. So a
// window that lapsed inside Advance left the message visible with its group
// still held, and the queue reported depth 1, invisible 0 and every receive
// empty for ever (closed by).

// fifoQueue is one FIFO queue with the given visibility timeout in seconds.
func fifoQueue(t *testing.T, s *Server, seconds string) string {
	t.Helper()
	const name = "g.fifo"
	if _, err := s.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):         "true",
			string(types.QueueAttributeNameVisibilityTimeout): seconds,
		},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return name
}

// send publishes one message into a group.
func send(t *testing.T, s *Server, name, group, body string) {
	t.Helper()
	if _, err := s.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(s.QueueURL(name)),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(body),
	}); err != nil {
		t.Fatalf("SendMessage(%q): %v", body, err)
	}
}

// receiveBodies is one non-blocking receive, as bodies.
func receiveBodies(t *testing.T, s *Server, name string, want int) []string {
	t.Helper()
	out, err := s.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.QueueURL(name)),
		MaxNumberOfMessages: int32(want),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	bodies := make([]string, 0, len(out.Messages))
	for i := range out.Messages {
		bodies = append(bodies, aws.ToString(out.Messages[i].Body))
	}
	return bodies
}

// TestAdvanceReleasesTheGroupOfAMessageItMadeVisible is the specification's
// test, and it is the one an agent must watch fail: before the fix the second
// receive produces NOTHING and the queue is deadlocked against the fake rather
// than against SQS.
//
// It fails rather than hanging because the receive asks for no long poll: an
// undeliverable queue answers "no messages" immediately, which is exactly what
// made the state so hard to recognise in the first place.
func TestAdvanceReleasesTheGroupOfAMessageItMadeVisible(t *testing.T) {
	s := New(Options{})
	name := fifoQueue(t, s, "5")
	send(t, s, name, "g0", "b0")
	send(t, s, name, "g0", "b1")

	if got := receiveBodies(t, s, name, 10); len(got) != 1 || got[0] != "b0" {
		t.Fatalf("the first receive produced %v, want [b0]: a FIFO queue delivers one message per group at a time", got)
	}
	// The window lapses by the clock alone, which is what Advance is for.
	s.Advance(6 * time.Second)
	if n := s.Invisible(name); n != 0 {
		t.Fatalf("%d message(s) are still invisible %v after a %v window opened", n, 6*time.Second, 5*time.Second)
	}
	if d := s.Depth(name); d != 2 {
		t.Fatalf("the queue holds %d message(s), want 2: nothing was settled", d)
	}
	// The message whose window lapsed goes back to the head of its group, so
	// the next receive produces it again.
	got := receiveBodies(t, s, name, 10)
	if len(got) == 0 {
		t.Fatalf("a receive on a queue of depth %d with %d invisible produced nothing: the message's FIFO group is still held by a message that is VISIBLE, so this queue can never be drained ",
			s.Depth(name), s.Invisible(name))
	}
	if len(got) != 1 || got[0] != "b0" {
		t.Fatalf("the receive after the window lapsed produced %v, want [b0]: an unsettled message goes back to the head of its group", got)
	}
}

// TestAGroupIsHeldOnlyByAMessageInFlight is the general statement of the same
// rule, over the four ways a hold ends: a delete, a visibility set to zero,
// ExpireVisibility, and the clock.
//
// It is one test over four routes rather than four tests because the routes
// used to be four separate decrements of one counter, which is precisely how
// three of them ended up releasing the group and the fourth did not.
func TestAGroupIsHeldOnlyByAMessageInFlight(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release func(t *testing.T, s *Server, queue, handle string)
	}{
		{"delete", func(t *testing.T, s *Server, queue, handle string) {
			if _, err := s.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(s.QueueURL(queue)), ReceiptHandle: aws.String(handle),
			}); err != nil {
				t.Fatalf("DeleteMessage: %v", err)
			}
		}},
		{"visibility-zero", func(t *testing.T, s *Server, queue, handle string) {
			if _, err := s.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
				QueueUrl: aws.String(s.QueueURL(queue)), ReceiptHandle: aws.String(handle), VisibilityTimeout: 0,
			}); err != nil {
				t.Fatalf("ChangeMessageVisibility: %v", err)
			}
		}},
		{"expire-visibility", func(_ *testing.T, s *Server, queue, _ string) {
			s.ExpireVisibility(queue)
		}},
		{"clock", func(_ *testing.T, s *Server, _, _ string) {
			s.Advance(6 * time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(Options{})
			name := fifoQueue(t, s, "5")
			send(t, s, name, "g0", "b0")
			send(t, s, name, "g0", "b1")
			out, err := s.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
				QueueUrl: aws.String(s.QueueURL(name)), MaxNumberOfMessages: 1,
			})
			if err != nil || len(out.Messages) != 1 {
				t.Fatalf("the first receive produced %d message(s), %v", len(out.Messages), err)
			}
			handle := aws.ToString(out.Messages[0].ReceiptHandle)
			if got := receiveBodies(t, s, name, 10); len(got) != 0 {
				t.Fatalf("a receive while g0 is in flight produced %v; a FIFO group holds its later messages", got)
			}
			tc.release(t, s, name, handle)
			if got := receiveBodies(t, s, name, 10); len(got) == 0 {
				t.Fatalf("after %s the group is still held though no message of it is in flight: depth %d, invisible %d ",
					tc.name, s.Depth(name), s.Invisible(name))
			}
		})
	}
}
