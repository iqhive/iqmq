package kafkago_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// closeBound is how long a client's Close may take with a publication still
// in kafka-go's writer. The driver's bound is the backoff sleep a retrying
// batch is already in, at most WriteBackoffMax (1s here); the rest is slack.
// Waiting out the writer took 8 seconds of backoff for the retrying
// publication below, and the whole WriteTimeout for the one held in flight.
const closeBound = 4 * time.Second

// TestCloseEndsARetryingPublish is "Close unblocks blocked operations" for a
// publication kafka-go's writer still holds.
//
// The writer retries a failed batch up to MaxAttempts times behind backoff
// sleeps nothing interrupts, and Writer.Close waits for every batch. The core's
// Close closes the sink to release a synchronous publication it is waiting
// for, and the sink's Close did not reach the wait; the backend's Close then
// waited for the writers. So a client's Close waited out every attempt: here
// 10 of them, refused by the broker, or one the broker never answers, for the
// whole WriteTimeout.
func TestCloseEndsARetryingPublish(t *testing.T) {
	for _, held := range []string{"retrying", "in flight"} {
		for _, surface := range []string{"publish", "batch", "async"} {
			t.Run(held+"/"+surface, func(t *testing.T) {
				topic := newTopic("close-retry")
				cl := cluster(t, 1, []string{topic})
				c := openOn(t, cl, func(cfg *kq.Config) {
					cfg.MaxAttempts = 10
					cfg.WriteBackoffMin = 250 * time.Millisecond
					cfg.WriteBackoffMax = time.Second
					cfg.WriteTimeout = 30 * time.Second
				})
				pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
				if err != nil {
					t.Fatalf("Publisher: %v", err)
				}
				reached := hold(t, cl, topic, held)

				done := make(chan error, 1)
				go func() {
					// No deadline of the caller's: Close is the only thing
					// that may end this wait.
					m := &mq.Message{Body: []byte("x")}
					switch surface {
					case "batch":
						done <- pub.PublishBatch(context.Background(), []*mq.Message{m})
					case "async":
						if err := pub.PublishAsync(context.Background(), m, func(r mq.PublishResult) { done <- r.Err }); err != nil {
							done <- err
						}
					default:
						done <- pub.Publish(context.Background(), m)
					}
				}()
				select {
				case <-reached:
				case <-time.After(20 * time.Second):
					t.Fatal("the produce never reached the broker")
				}

				closed := make(chan error, 1)
				start := time.Now()
				go func() { closed <- c.Close() }()
				select {
				case err := <-closed:
					if err != nil {
						t.Errorf("Close: %v", err)
					}
					t.Logf("Close returned after %v", time.Since(start))
				case <-time.After(closeBound):
					t.Fatalf("Close has not returned after %v: it is waiting for kafka-go's writer", time.Since(start))
				}
				select {
				case err := <-done:
					if err == nil || !mq.IsAmbiguous(err) {
						t.Errorf("the publication Close ended = %v, want an ambiguous outcome: an attempt may have been appended", err)
					}
				case <-time.After(closeBound):
					t.Fatal("the publication had no outcome after Close returned")
				}
			})
		}
	}
}

// hold makes the broker keep the topic's produce from succeeding, and returns
// a channel closed once a produce has reached it: "retrying" refuses every
// produce with a retriable NOT_ENOUGH_REPLICAS, and "in flight" holds the
// first produce unanswered until the test ends.
func hold(t *testing.T, cl *kfake.Cluster, topic, how string) <-chan struct{} {
	t.Helper()
	reached := make(chan struct{})
	if how == "retrying" {
		fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotEnoughReplicas, Count: -1})
		t.Cleanup(fault.Remove)
		go func() {
			if fault.Wait(t.Context(), 1) == nil {
				close(reached)
			}
		}()
		return reached
	}
	release := make(chan struct{})
	var (
		holding atomic.Bool
		freed   sync.Once
	)
	t.Cleanup(func() { freed.Do(func() { close(release) }) })
	cl.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r, ok := req.(*kmsg.ProduceRequest)
		if !ok || len(r.Topics) == 0 || r.Topics[0].Topic != topic || !holding.CompareAndSwap(false, true) {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
		}
		close(reached)
		cl.SleepControl(func() { <-release })
		return nil, nil, false //nolint:nilnil // as above
	})
	return reached
}
