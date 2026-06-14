package sqsfake

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The refusals every operation shares.
//
// The fake's happy paths are driven by every test of `drivers/sqs`; its
// REFUSALS are driven by none of them, because a driver that is working never
// addresses a queue that is not there and never has a fault installed. Those
// are the paths the fake exists to produce on demand -- a real endpoint will
// not fail on request -- so a fake that got them wrong would make every
// fault-injection test in the driver assert against the wrong error.

// seeded is a server holding one ordinary queue and one FIFO queue.
func seeded(t *testing.T) *Server {
	t.Helper()
	s := New(Options{})
	for _, name := range []string{"q", "q.fifo"} {
		if _, err := s.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)}); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	return s
}

// urlOf is a queue's URL, as the driver would have it.
func urlOf(t *testing.T, s *Server, name string) string {
	t.Helper()
	out, err := s.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("GetQueueUrl(%s): %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// operations is every operation, as a call the table below can make against a
// server. Each is given the URL of a queue that may or may not exist.
func operations(s *Server, url string) []struct {
	name string
	call func(context.Context) error
} {
	return []struct {
		name string
		call func(context.Context) error
	}{
		{"CreateQueue", func(ctx context.Context) error {
			_, err := s.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("fresh")})
			return err
		}},
		{"DeleteQueue", func(ctx context.Context) error {
			_, err := s.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
			return err
		}},
		{"GetQueueUrl", func(ctx context.Context) error {
			_, err := s.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(nameOf(url))})
			return err
		}},
		{"ListQueues", func(ctx context.Context) error {
			_, err := s.ListQueues(ctx, &sqs.ListQueuesInput{})
			return err
		}},
		{"GetQueueAttributes", func(ctx context.Context) error {
			_, err := s.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url)})
			return err
		}},
		{"SetQueueAttributes", func(ctx context.Context) error {
			_, err := s.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl: aws.String(url), Attributes: map[string]string{"VisibilityTimeout": "30"},
			})
			return err
		}},
		{"PurgeQueue", func(ctx context.Context) error {
			_, err := s.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(url)})
			return err
		}},
		{"SendMessage", func(ctx context.Context) error {
			_, err := s.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl: aws.String(url), MessageBody: aws.String("x"),
			})
			return err
		}},
		{"SendMessageBatch", func(ctx context.Context) error {
			_, err := s.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				QueueUrl: aws.String(url),
				Entries: []types.SendMessageBatchRequestEntry{
					{Id: aws.String("1"), MessageBody: aws.String("x")},
				},
			})
			return err
		}},
		{"ReceiveMessage", func(ctx context.Context) error {
			_, err := s.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url)})
			return err
		}},
		{"DeleteMessage", func(ctx context.Context) error {
			_, err := s.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: aws.String("r"),
			})
			return err
		}},
		{"DeleteMessageBatch", func(ctx context.Context) error {
			_, err := s.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(url),
				Entries: []types.DeleteMessageBatchRequestEntry{
					{Id: aws.String("1"), ReceiptHandle: aws.String("r")},
				},
			})
			return err
		}},
		{"ChangeMessageVisibility", func(ctx context.Context) error {
			_, err := s.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl: aws.String(url), ReceiptHandle: aws.String("r"),
			})
			return err
		}},
		{"ChangeMessageVisibilityBatch", func(ctx context.Context) error {
			_, err := s.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
				QueueUrl: aws.String(url),
				Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
					{Id: aws.String("1"), ReceiptHandle: aws.String("r")},
				},
			})
			return err
		}},
		{"StartMessageMoveTask", func(ctx context.Context) error {
			_, err := s.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
				SourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:" + nameOf(url)),
			})
			return err
		}},
	}
}

// TestAnInstalledFaultReachesEveryOperation.
//
// A fault with an empty Op applies to every operation, and that is what the
// driver's tests rely on when they install one for a whole scenario. An
// operation that forgot to consult the table would succeed in the middle of a
// test that had asked the endpoint to be down.
func TestAnInstalledFaultReachesEveryOperation(t *testing.T) {
	boom := errors.New("the endpoint is unavailable")
	for _, op := range operations(seeded(t), "") {
		s := seeded(t)
		url := urlOf(t, s, "q")
		s.Fault(Fault{Err: boom})
		for _, tc := range operations(s, url) {
			if tc.name != op.name {
				continue
			}
			if err := tc.call(t.Context()); err == nil {
				t.Errorf("%s succeeded with a fault installed for every operation", tc.name)
			}
		}
	}
}

// TestAFaultIsScopedToItsOperationAndItsQueue.
//
// Both selectors exist so a test can fail ONE call in a scenario, and a fake
// that ignored either would fail calls the test meant to succeed -- which
// reads as a driver bug and is the instrument's.
func TestAFaultIsScopedToItsOperationAndItsQueue(t *testing.T) {
	boom := errors.New("only this one")

	s := seeded(t)
	url := urlOf(t, s, "q")
	s.Fault(Fault{Op: "SendMessage", Err: boom})
	if _, err := s.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("x"),
	}); !errors.Is(err, boom) {
		t.Errorf("SendMessage with a fault on SendMessage is %v, want the installed error", err)
	}
	if _, err := s.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(url)}); err != nil {
		t.Errorf("ReceiveMessage is %v with a fault installed for SendMessage only", err)
	}

	s2 := seeded(t)
	// Both URLs are read BEFORE the fault is installed: GetQueueUrl is an
	// operation too, and a queue-scoped fault applies to it.
	other := urlOf(t, s2, "q.fifo")
	faulted := urlOf(t, s2, "q")
	s2.Fault(Fault{Queue: "q", Err: boom})
	if _, err := s2.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(faulted), MessageBody: aws.String("x"),
	}); !errors.Is(err, boom) {
		t.Errorf("SendMessage to the faulted queue is %v, want the installed error", err)
	}
	if _, err := s2.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(other), MessageBody: aws.String("x"), MessageGroupId: aws.String("g"),
	}); err != nil {
		t.Errorf("SendMessage to another queue is %v with a fault installed for \"q\" only", err)
	}
}

// TestEveryOperationOnAQueueThatIsNotThereIsQueueDoesNotExist.
//
// It is the error the driver maps onto CodeNotFound, and a fake that answered
// something else -- or nothing -- would let a driver's mapping be wrong in
// the one case a deployment actually meets: a queue deleted underneath it.
func TestEveryOperationOnAQueueThatIsNotThereIsQueueDoesNotExist(t *testing.T) {
	s := seeded(t)
	gone := s.url("never-created")

	for _, tc := range operations(s, gone) {
		switch tc.name {
		case "CreateQueue", "ListQueues", "StartMessageMoveTask":
			continue // these do not address an existing queue
		case "SendMessageBatch", "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
			// The BATCH operations report a missing queue per ENTRY and
			// return nil at the call level. That is a divergence from the
			// service, which fails the whole request with
			// NonExistentQueue -- see
			// TestABatchAgainstAMissingQueueFailsPerEntry, which pins it
			// where a reader will look for it.
			continue
		}
		err := tc.call(t.Context())
		if err == nil {
			t.Errorf("%s against a queue that does not exist returned nil", tc.name)
			continue
		}
		var missing *types.QueueDoesNotExist
		if !errors.As(err, &missing) {
			t.Errorf("%s against a queue that does not exist is %v (%T), want QueueDoesNotExist", tc.name, err, err)
		}
	}
}

// TestEveryOperationRespectsACancelledContext.
//
// The check is after the fault table and before the work, which is the order
// that lets a test install a fault AND cancel without the two racing.
func TestEveryOperationRespectsACancelledContext(t *testing.T) {
	s := seeded(t)
	url := urlOf(t, s, "q")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, tc := range operations(s, url) {
		if err := tc.call(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context is %v, want context.Canceled", tc.name, err)
		}
	}
}

// TestAFifoQueueRefusesWhatItCannotOrder.
//
// A FIFO queue needs a group on every message and takes no per-message delay.
// Both are the service's rules and both are refusals a driver has to avoid
// tripping, so the fake has to produce them.
func TestAFifoQueueRefusesWhatItCannotOrder(t *testing.T) {
	s := seeded(t)
	url := urlOf(t, s, "q.fifo")

	_, err := s.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("x"),
	})
	var bad *types.InvalidAttributeValue
	if !errors.As(err, &bad) {
		t.Errorf("sending to a FIFO queue with no group is %v (%T), want InvalidAttributeValue", err, err)
	}

	_, err = s.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("x"),
		MessageGroupId: aws.String("g"), DelaySeconds: 5,
	})
	if !errors.As(err, &bad) {
		t.Errorf("sending to a FIFO queue with a per-message delay is %v (%T), want InvalidAttributeValue",
			err, err)
	}
}

// TestAnEmptyBodyAndAnEmptyNameAreRefused.
func TestAnEmptyBodyAndAnEmptyNameAreRefused(t *testing.T) {
	s := seeded(t)
	url := urlOf(t, s, "q")

	_, err := s.SendMessage(t.Context(), &sqs.SendMessageInput{QueueUrl: aws.String(url)})
	var contents *types.InvalidMessageContents
	if !errors.As(err, &contents) {
		t.Errorf("sending an empty body is %v (%T), want InvalidMessageContents", err, err)
	}

	_, err = s.CreateQueue(t.Context(), &sqs.CreateQueueInput{})
	var addr *types.InvalidAddress
	if !errors.As(err, &addr) {
		t.Errorf("creating a queue with no name is %v (%T), want InvalidAddress", err, err)
	}
}

// TestCreatingAQueueTwiceIsTheSameQueueUnlessItsKindChanges.
//
// Re-creating is how the conformance kit provisions on every run, so it has
// to be idempotent -- but a request that changes a queue from standard to
// FIFO is not the same queue, and the service says so rather than silently
// keeping the old one.
func TestCreatingAQueueTwiceIsTheSameQueueUnlessItsKindChanges(t *testing.T) {
	s := seeded(t)

	first := urlOf(t, s, "q")
	out, err := s.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("q")})
	if err != nil {
		t.Fatalf("creating an existing queue again: %v", err)
	}
	if aws.ToString(out.QueueUrl) != first {
		t.Errorf("re-creating gave the URL %q, want the existing %q", aws.ToString(out.QueueUrl), first)
	}

	_, err = s.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String("q"),
		Attributes: map[string]string{string(types.QueueAttributeNameFifoQueue): "true"},
	})
	var exists *types.QueueNameExists
	if !errors.As(err, &exists) {
		t.Errorf("re-creating a standard queue as FIFO is %v (%T), want QueueNameExists", err, err)
	}
}

// TestABatchAgainstAMissingQueueFailsPerEntry pins a divergence rather than a
// rule.
//
// The service fails the whole request with NonExistentQueue when the URL
// names no queue; this fake reports the same condition on every ENTRY and
// returns nil at the call level. The per-entry error CODE is the same one, so
// a driver that reads the result entries sees the right thing -- and both
// drivers here do, because a partial batch is the ordinary case they were
// written for.
//
// It is pinned because it is load-bearing either way: a change that made the
// fake fail the call would break nothing visible in the driver and everything
// in a test that counts failed entries, and a reader comparing this fake with
// the service needs to be told which way round it is. the maintenance notes
// carries what closing it would need.
func TestABatchAgainstAMissingQueueFailsPerEntry(t *testing.T) {
	s := seeded(t)
	gone := s.url("never-created")

	out, err := s.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(gone),
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("1"), MessageBody: aws.String("x")},
			{Id: aws.String("2"), MessageBody: aws.String("y")},
		},
	})
	if err != nil {
		t.Fatalf("SendMessageBatch against a missing queue is %v; this fake reports it per entry", err)
	}
	if len(out.Successful) != 0 {
		t.Errorf("%d entries succeeded against a queue that does not exist", len(out.Successful))
	}
	if len(out.Failed) != 2 {
		t.Fatalf("%d entries failed against a queue that does not exist, want both", len(out.Failed))
	}
	for _, f := range out.Failed {
		if aws.ToString(f.Code) == "" {
			t.Errorf("entry %s failed with no code; the code is what the driver maps", aws.ToString(f.Id))
		}
	}
}
