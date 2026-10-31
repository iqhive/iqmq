package sqs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// TestAReceiveAdmittedLateDoesNotSupersedeAFresherOne is two pollers
// receiving the same message. Poller A's answer holds it behind a message the
// full ring will not take, so A parks before admitting it while its window
// runs out. Poller B then receives the message again, with a fresh receipt and
// a whole window, and admits it. When the ring makes room, A admits its own
// copy, whose window closed long ago. The copy admitted last used to become
// the one the source vouched for, so B's fresh delivery was marked lost, Fetch
// dropped A's as expired and B's as lost, and the message sat invisible under
// B's receipt for a whole AckDeadline with nobody working on it. Had B's
// delivery been handed out already, its Ack was refused with ErrLeaseLost and
// the message was processed twice.
func TestAReceiveAdmittedLateDoesNotSupersedeAFresherOne(t *testing.T) {
	const deadline = 30 * time.Second
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	clk := &stepClock{at: server.Now()}
	b, err := BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed, Config{
		Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "not-a-real-secret",
		AutoCreate: true, WaitTime: time.Second, AckDeadline: deadline, Clock: clk,
		Pollers: 2, MaxMessages: 1,
	})
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	url, err := b.res.URL(t.Context(), "t")
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	if _, err := server.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("d"),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	receive := func() (*types.Message, time.Time) {
		t.Helper()
		out, err := server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 1,
			VisibilityTimeout: visibilitySeconds(deadline), MessageAttributeNames: []string{"All"},
		})
		if err != nil || len(out.Messages) != 1 {
			t.Fatalf("ReceiveMessage: %d message(s), %v", len(out.Messages), err)
		}
		return &out.Messages[0], clk.Now()
	}
	stale, staleAt := receive()
	server.Advance(deadline + time.Second)
	clk.advance(deadline + time.Second)
	fresh, freshAt := receive()

	enc, err := newEncoder(b.cfg, b.caps, mq.MetadataAdaptive)
	if err != nil {
		t.Fatalf("newEncoder: %v", err)
	}
	src, err := b.newSource(mq.SourceConfig{Topic: "t", Subscription: "s", Prefetch: 4}, enc, url)
	if err != nil {
		t.Fatalf("newSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	// B admits first; A, parked since its receive, admits second.
	for _, rx := range []struct {
		m  *types.Message
		at time.Time
	}{{fresh, freshAt}, {stale, staleAt}} {
		if ok, err := src.admit(t.Context(), rx.m, rx.at); !ok || err != nil {
			t.Fatalf("admitting %s: %v, %v", aws.ToString(rx.m.ReceiptHandle), ok, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 4)
	n, err := src.Fetch(ctx, dst)
	if n != 1 {
		t.Fatalf("Fetch handed out %d deliveries (%v), want the fresh one: its receipt has a whole window left, "+
			"and the copy admitted after it, whose window had closed, marked it lost (%d dropped)", n, err, src.Stale())
	}
	if got, want := dst[0].Ref.(*delivery).handle, aws.ToString(fresh.ReceiptHandle); got != want {
		t.Fatalf("Fetch handed out receipt %s, want the fresh %s", got, want)
	}
	if err := src.set.Ack(t.Context(), &dst[0]); err != nil {
		t.Fatalf("the fresh delivery's Ack returned %v", err)
	}
	if got := server.Depth(QueueNameOfURL(url)); got != 0 {
		t.Fatalf("the queue holds %d message(s) after the fresh delivery's Ack, want 0", got)
	}
}
