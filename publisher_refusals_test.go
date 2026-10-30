package iqmq_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestEveryBatchSurfaceKeepsDefiniteFailuresThatDisagree is the definite half
// of TestEveryBatchSurfaceKeepsAnAmbiguousMessage. When none of a batch was
// accepted and every message was definitely refused, the single error is
// every message's outcome only if every message has the same code and the
// same retryability: a caller retries a retryable refusal and drops the
// others. Comparing ambiguity alone answered [refused, throttled] with the
// refusal, so the caller dropped the throttled message it should have
// retried, and a split batch whose held-back messages carry a retryable
// not-attempted error reported them all as the first piece's refusal.
//
// The plain shape is the publisher's own gathering of per-message answers,
// and the batch shape is a driver answering through driverkit.CollapseBatch.
func TestEveryBatchSurfaceKeepsDefiniteFailuresThatDisagree(t *testing.T) {
	fail := func(_ int, m *mq.Message) error {
		switch string(m.Body) {
		case "refuse":
			return &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeInvalidArgument}
		case "throttle":
			return &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeThrottled, Retryable: true}
		}
		return nil
	}
	patterns := []struct {
		bodies []string
		// mixed says the outcomes differ, so the answer is per message.
		mixed bool
	}{
		{[]string{"refuse", "throttle"}, true},
		{[]string{"throttle", "refuse", "refuse"}, true},
		{[]string{"refuse", "refuse"}, false},
		{[]string{"throttle", "throttle"}, false},
	}
	shapes := map[string]func(*advBroker, mq.SinkConfig) mq.Sink{"plain": openPlain, "batch": openBatch}
	for surface, publish := range batchSurfaces {
		for shape, open := range shapes {
			for _, pat := range patterns {
				name := surface + "/" + shape + "/" + strings.Join(pat.bodies, ",")
				t.Run(name, func(t *testing.T) {
					b, p := advClient(t, open, mq.PublisherConfig{})
					b.setFail(fail)
					err := publish(context.Background(), p, pat.bodies)
					if err == nil {
						t.Fatal("the batch succeeded; every message was meant to fail")
					}
					var be *mq.BatchError
					if !pat.mixed {
						if errors.As(err, &be) {
							t.Fatalf("%v: want the single error; every message has the same outcome", err)
						}
						if got, want := mq.IsRetryable(err), pat.bodies[0] == "throttle"; got != want {
							t.Fatalf("%v: IsRetryable = %v, want %v", err, got, want)
						}
						return
					}
					if !errors.As(err, &be) {
						t.Fatalf("%v (code %v, IsRetryable %v): want a *BatchError; the messages were refused with "+
							"different codes and retryability, and one error cannot say both", err, mq.CodeOf(err), mq.IsRetryable(err))
					}
					if len(be.Errs) != len(pat.bodies) {
						t.Fatalf("%v carries %d results for %d messages", err, len(be.Errs), len(pat.bodies))
					}
					for i, itemErr := range be.Errs {
						retryable := pat.bodies[i] == "throttle"
						if itemErr == nil || mq.IsRetryable(itemErr) != retryable {
							t.Fatalf("message %d (%s) is reported as %v, want IsRetryable = %v", i, pat.bodies[i], itemErr, retryable)
						}
					}
				})
			}
		}
	}
}

// TestEveryBatchSurfaceOnADoneContextReturnsTheContextsError is a batch that
// stops at its first message because the context has ended. The messages
// behind it are answered as never attempted, which is retryable because they
// never left the process, while the context's own error is not. They still
// have the one outcome the context's error names, so the batch is that error:
// a *BatchError here read as a partial success (errors.Is(err, ErrPartial))
// with nothing accepted, and IsRetryable read the retryable fill.
//
// The plain shape is the core's loop over Publish, and the split shape is a
// native batch cut into pieces of two, whose first piece the sink refuses.
func TestEveryBatchSurfaceOnADoneContextReturnsTheContextsError(t *testing.T) {
	shapes := map[string]func(t *testing.T) *mq.PublisherHandle{
		"plain": func(t *testing.T) *mq.PublisherHandle {
			_, p := advClient(t, openPlain, mq.PublisherConfig{})
			return p
		},
		"split": func(t *testing.T) *mq.PublisherHandle {
			b := newAdvBroker(openBatch)
			b.caps = advCapsWithBatchLimit(2)
			return advPublisher(t, b, mq.PublisherConfig{})
		},
	}
	for surface, publish := range batchSurfaces {
		for shape, open := range shapes {
			t.Run(surface+"/"+shape, func(t *testing.T) {
				p := open(t)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := publish(ctx, p, []string{"a", "b", "c", "d", "e"})
				var be *mq.BatchError
				if errors.As(err, &be) || errors.Is(err, mq.ErrPartial) {
					t.Fatalf("%v (code %v, IsRetryable %v): a batch stopped at its first message by the context reads "+
						"as a partial success", err, mq.CodeOf(err), mq.IsRetryable(err))
				}
				if !errors.Is(err, context.Canceled) || mq.IsRetryable(err) {
					t.Fatalf("%v (IsRetryable %v), want the context's error, which is not retryable", err, mq.IsRetryable(err))
				}
			})
		}
	}
}
