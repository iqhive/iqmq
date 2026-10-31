package kafkago_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestFlushWaitsOnlyForItsOwnPublications is kf-14.
//
// FlushSink's contract is the publications the sink had already admitted. The
// driver counted admissions for the whole backend and waited for the count to
// reach zero, so one publisher's Flush waited for every other publisher's
// records, other topics included, and for records admitted after it began:
// it did not return on a publisher that keeps publishing, which the core's
// PublisherHandle.Flush promises it does.
func TestFlushWaitsOnlyForItsOwnPublications(t *testing.T) {
	t.Run("another publisher's record", func(t *testing.T) {
		ta, tb := newTopic("flush-a"), newTopic("flush-b")
		cl := cluster(t, 1, []string{ta, tb})
		c := openOn(t, cl, func(cfg *kq.Config) {
			cfg.MaxAttempts = 100
			cfg.WriteBackoffMin = 100 * time.Millisecond
			cfg.WriteBackoffMax = 200 * time.Millisecond
			cfg.WriteTimeout = 30 * time.Second
		})
		pa, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: ta, Confirmation: mq.ConfirmNone})
		if err != nil {
			t.Fatalf("Publisher A: %v", err)
		}
		pb, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: tb})
		if err != nil {
			t.Fatalf("Publisher B: %v", err)
		}
		// B's record is refused with a retriable answer for as long as the
		// fault stands, so kafka-go's writer keeps retrying it.
		fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: tb, Err: kerr.NotEnoughReplicas, Count: -1})
		defer fault.Remove()
		bdone := make(chan error, 1)
		if err := pb.PublishAsync(context.Background(), &mq.Message{Body: []byte("b")}, func(r mq.PublishResult) { bdone <- r.Err }); err != nil {
			t.Fatalf("PublishAsync B: %v", err)
		}
		if err := fault.Wait(t.Context(), 1); err != nil {
			t.Fatalf("B's produce never reached the broker: %v", err)
		}
		if err := pa.Publish(t.Context(), &mq.Message{Body: []byte("a")}); err != nil {
			t.Fatalf("Publish A: %v", err)
		}
		waitFor(t, 5*time.Second, "A's record appended", func() bool { return cl.PartitionInfo(ta, 0).HighWatermark >= 1 })

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		start := time.Now()
		if err := pa.Flush(ctx); err != nil {
			t.Errorf("A's Flush = %v after %v, though A's only record is in the log: it waited for B's record on another topic", err, time.Since(start).Round(time.Millisecond))
		}
		fault.Remove()
		select {
		case err := <-bdone:
			if err != nil {
				t.Errorf("B's publication, retried once the fault was gone: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("B's publication never completed")
		}
	})

	t.Run("a record admitted after the flush began", func(t *testing.T) {
		topic := newTopic("flush-later")
		cl := cluster(t, 2, []string{topic})
		c := openOn(t, cl, func(cfg *kq.Config) { cfg.WriteTimeout = 30 * time.Second })
		pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		for p := range 2 {
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, mq.Partition(p)); err != nil {
				t.Fatalf("warm %d: %v", p, err)
			}
		}
		if err := pub.Flush(t.Context()); err != nil {
			t.Fatalf("warm Flush: %v", err)
		}
		first, releaseFirst := holdProduce(t, cl, topic, 0)
		later, _ := holdProduce(t, cl, topic, 1)
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}, mq.Partition(0)); err != nil {
			t.Fatalf("Publish before: %v", err)
		}
		<-first
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		flushed := make(chan error, 1)
		go func() { flushed <- pub.Flush(ctx) }()
		select {
		case err := <-flushed:
			t.Fatalf("Flush = %v while the record it waits for was held at the broker", err)
		case <-time.After(300 * time.Millisecond):
		}
		// Admitted after the Flush began, and held at the broker until the
		// test ends.
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}, mq.Partition(1)); err != nil {
			t.Fatalf("Publish after: %v", err)
		}
		<-later
		releaseFirst()
		start := time.Now()
		if err := <-flushed; err != nil {
			t.Errorf("Flush = %v %v after the record admitted before it was answered: it waited for a record admitted after it began", err, time.Since(start).Round(time.Millisecond))
		}
	})
}
