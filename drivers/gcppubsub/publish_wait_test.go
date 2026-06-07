package gcppubsub_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestAPublicationWhoseWaitEndedIsAmbiguous: a context that ends while the
// driver waits on PublishResult.Get does not un-send the message. The
// library's bundler already holds it, and the service may store it. The
// driver mapped that context error with Map, so the caller was told a
// DEFINITE DeadlineExceeded or Canceled, and a caller who republishes the
// definite refusals -- which the confirmation notes advises --
// duplicated it (the review's concurrency-4). It is now mapped as a failure
// after the request may have left: ambiguous, with its code kept.
func TestAPublicationWhoseWaitEndedIsAmbiguous(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			c, srv := scripted(t)
			topic := newTopic("wait")
			provision(t, c, topic, "s")
			srv.PublishErr = func(string, *psapi.Outgoing) error {
				return fmt.Errorf("waiting for the publish result: %w", cause)
			}
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("opening the publisher: %v", err)
			}
			for name, publish := range map[string]func() error{
				"Publish": func() error {
					return c.Publish(context.Background(), topic, &mq.Message{Body: []byte("maybe")})
				},
				"PublishBatch": func() error {
					return p.PublishBatch(context.Background(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
				},
			} {
				err := publish()
				if err == nil {
					t.Fatalf("%s: the injected failure was not reported", name)
				}
				if !mq.IsAmbiguous(err) {
					t.Fatalf("%s = %v (code %v): IsAmbiguous = false; the bundler held the message when the wait ended, "+
						"so the service may store it", name, err, mq.CodeOf(err))
				}
				want := mq.CodeDeadlineExceeded
				if errors.Is(cause, context.Canceled) {
					want = mq.CodeCanceled
				}
				if mq.CodeOf(err) != want && mq.CodeOf(err) != mq.CodePartial {
					t.Fatalf("%s = %v: code %v, want %v", name, err, mq.CodeOf(err), want)
				}
			}
		})
	}
}
