package kafkago_test

import (
	"context"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// A writer handed to OpenWithWriter is a synchronous one, which is what its
// doc asks for. The asynchronous paths (PublishAsync, PublishOwned and a
// ConfirmNone Publish) used it as well, so on a borrowed writer they waited
// for the broker like a synchronous Publish, and everything that depends on
// them not waiting broke.

// borrowedWriter is a synchronous kafka-go writer of the caller's own, as
// OpenWithWriter's doc asks for.
func borrowedWriter(t *testing.T, cl *kfake.Cluster, mutate ...func(*segkafka.Writer)) *segkafka.Writer {
	t.Helper()
	tr := &segkafka.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	w := &segkafka.Writer{
		Addr:         segkafka.TCP(cl.ListenAddrs()...),
		Transport:    tr,
		RequiredAcks: segkafka.RequireAll,
		BatchSize:    1,
		WriteTimeout: 10 * time.Second,
	}
	for _, m := range mutate {
		m(w)
	}
	return w
}

// readLog reads a topic's first n record values with a client of its own.
func readLog(t *testing.T, cl *kfake.Cluster, topic string, n int) []string {
	t.Helper()
	rd, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var got []string
	for len(got) < n && ctx.Err() == nil {
		rd.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
}

// TestABorrowedWriterKeepsAnOwnedMessageUntilTheBrokerAnswers is kf-12.
//
// PublishOwned copies nothing, and PublishAsync borrows the message until its
// callback runs. On a synchronous writer, WriteMessages returns the caller's
// ctx.Err() while the writer still holds the record and will retry it, and
// the driver took that return as the end: it released the owned message, or
// ran the callback, and the owner reused its storage. The retry then sent the
// owner's new bytes, so the log held a publication nobody made.
func TestABorrowedWriterKeepsAnOwnedMessageUntilTheBrokerAnswers(t *testing.T) {
	for _, surface := range []string{"PublishOwned", "PublishAsync"} {
		t.Run(surface, func(t *testing.T) {
			topic := newTopic("borrowed-owned")
			cl := cluster(t, 1, []string{topic})
			w := borrowedWriter(t, cl, func(w *segkafka.Writer) {
				w.MaxAttempts = 5
				w.WriteBackoffMin = 500 * time.Millisecond
				w.WriteBackoffMax = 500 * time.Millisecond
			})
			b, err := kq.BackendWithWriter(t.Context(), w, driverkit.ClientOwned, config(cl))
			if err != nil {
				t.Fatalf("BackendWithWriter: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			if err := snk.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, nil); err != nil {
				t.Fatalf("warm: %v", err)
			}
			// The first attempt is refused with a retriable answer, so the
			// writer holds the record through a backoff and tries again.
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotEnoughReplicas, Count: 1})
			defer fault.Remove()

			const want = "the owner's record"
			m := &mq.Message{Body: []byte(want)}
			released := make(chan error, 1)
			// The owner takes its storage back and reuses it, which is what
			// both contracts allow once the message is handed back.
			reuse := func(m *mq.Message) { copy(m.Body, "OVERWRITTEN BY OWN") }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errs := make(chan error, 1)
			go func() {
				if surface == "PublishOwned" {
					errs <- snk.(mq.OwnedSink).PublishOwned(ctx, mq.OwnedMessage{Message: m, Release: func(m *mq.Message) {
						reuse(m)
						released <- nil
					}}, nil)
					return
				}
				errs <- snk.(mq.AsyncSink).PublishAsync(ctx, m, nil, func(err error) {
					reuse(m)
					released <- err
				})
			}()
			if err := fault.Wait(t.Context(), 1); err != nil {
				t.Fatalf("the produce never reached the broker: %v", err)
			}
			// The writer is in its backoff before the retry.
			cancel()
			perr := <-errs
			var rerr error
			select {
			case rerr = <-released:
			case <-time.After(10 * time.Second):
				t.Fatal("the message was never handed back")
			}
			t.Logf("%s returned %v, and its outcome was %v", surface, perr, rerr)
			if rerr != nil {
				t.Errorf("the publication's outcome was %v, though the writer's retry appended it", rerr)
			}
			waitFor(t, 10*time.Second, "the retry appended", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= 2 })
			got := readLog(t, cl, topic, 2)
			t.Logf("the log: %q", got)
			if len(got) != 2 || got[1] != want {
				t.Errorf("the log holds %q, want [warm %q]: the message was handed back while kafka-go still held the record, and the retry sent the owner's new bytes", got, want)
			}
		})
	}
}

// TestAPublicationFromACallbackOnABorrowedWriterDoesNotWait is kf-16.
//
// The completion, and a CallbackInline callback with it, runs on the kafka-go
// partition writer's own goroutine. On a borrowed synchronous writer a
// publication made from that callback to the same partition queued a batch
// only that goroutine could send, so it waited until its ctx ended, and the
// partition with it: for good under a ctx with no deadline.
func TestAPublicationFromACallbackOnABorrowedWriterDoesNotWait(t *testing.T) {
	for _, nested := range []string{"Publish", "PublishAsync"} {
		t.Run(nested, func(t *testing.T) {
			topic := newTopic("borrowed-reentry")
			cl := cluster(t, 1, []string{topic})
			c, err := kq.OpenWithWriter(t.Context(), borrowedWriter(t, cl), driverkit.ClientOwned, config(cl))
			if err != nil {
				t.Fatalf("OpenWithWriter: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			publish(t, c, topic, &mq.Message{Body: []byte("warm")})

			type outcome struct {
				err  error
				took time.Duration
			}
			// call is how long the nested call itself took, and result is the
			// nested publication's outcome, which for PublishAsync arrives
			// only once the outer callback has returned.
			call := make(chan outcome, 1)
			result := make(chan error, 1)
			outer := make(chan error, 1)
			err = pub.PublishAsync(context.Background(), &mq.Message{Body: []byte("outer")}, func(r mq.PublishResult) {
				outer <- r.Err
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				start := time.Now()
				inner := &mq.Message{Body: []byte("inner")}
				if nested == "Publish" {
					err := pub.Publish(ctx, inner)
					call <- outcome{err, time.Since(start)}
					result <- err
					return
				}
				err := pub.PublishAsync(ctx, inner, func(r mq.PublishResult) { result <- r.Err })
				call <- outcome{err, time.Since(start)}
			})
			if err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			if err := <-outer; err != nil {
				t.Fatalf("the outer publication: %v", err)
			}
			var o outcome
			select {
			case o = <-call:
			case <-time.After(20 * time.Second):
				t.Fatal("the publication made inside the callback never returned")
			}
			var rerr error
			select {
			case rerr = <-result:
			case <-time.After(20 * time.Second):
				t.Fatal("the publication made inside the callback has no outcome")
			}
			t.Logf("the %s made inside the callback returned %v after %v; its outcome %v", nested, o.err, o.took.Round(time.Millisecond), rerr)
			if o.took > 2*time.Second || rerr != nil {
				t.Errorf("a %s from inside a PublishAsync callback on a borrowed writer waited %v and ended %v: it queued behind the goroutine it was blocking", nested, o.took.Round(time.Millisecond), rerr)
			}
			if got := readLog(t, cl, topic, 3); len(got) != 3 || got[2] != "inner" {
				t.Errorf("the log holds %q, want [warm outer inner]", got)
			}
		})
	}
}
