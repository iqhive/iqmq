package kafkago_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// holdProduce holds the first produce request for (topic, partition) at the
// broker until release runs, and closes reached once the broker has it.
func holdProduce(t *testing.T, cl *kfake.Cluster, topic string, partition int32) (reached <-chan struct{}, release func()) {
	t.Helper()
	r, rel := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(rel) }) }
	t.Cleanup(release)
	var holding atomic.Bool
	cl.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		pr, ok := req.(*kmsg.ProduceRequest)
		if !ok || len(pr.Topics) == 0 || pr.Topics[0].Topic != topic ||
			len(pr.Topics[0].Partitions) == 0 || pr.Topics[0].Partitions[0].Partition != partition ||
			!holding.CompareAndSwap(false, true) {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
		}
		close(r)
		cl.SleepControl(func() { <-rel })
		return nil, nil, false //nolint:nilnil // as above
	})
	return r, release
}

// TestAFailedPublicationLeavesItsReceiptZeroed is kf-13.
//
// A synchronous Publish whose ctx ends while its produce is in flight returns
// at once, ambiguous, and kafka-go carries on with the batch. The writer's
// completion filled the receipt when the batch was answered, after Publish
// had returned: Message.Receipt says a publication that fails leaves the
// receipt zeroed, and a caller that reuses one receipt across a loop had a
// later publication's coordinates overwritten with the failed one's.
func TestAFailedPublicationLeavesItsReceiptZeroed(t *testing.T) {
	t.Run("its own receipt", func(t *testing.T) {
		topic := newTopic("receipt-late")
		cl := cluster(t, 1, []string{topic})
		c := openOn(t, cl, func(cfg *kq.Config) { cfg.WriteTimeout = 10 * time.Second; cfg.MaxAttempts = 1 })
		pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		publish(t, c, topic, &mq.Message{Body: []byte("warm")})
		reached, release := holdProduce(t, cl, topic, 0)
		var r mq.PublishReceipt
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs := make(chan error, 1)
		go func() { errs <- pub.Publish(ctx, &mq.Message{Body: []byte("held"), Receipt: &r}) }()
		<-reached
		cancel()
		perr := <-errs
		if !mq.IsAmbiguous(perr) {
			t.Fatalf("Publish = %v, want an ambiguous outcome", perr)
		}
		release()
		waitFor(t, 5*time.Second, "the held record appended", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= 2 })
		// The completion runs on the writer's goroutine once the broker has
		// answered, so give it the time to.
		time.Sleep(200 * time.Millisecond)
		if got := r; got != (mq.PublishReceipt{}) {
			t.Errorf("the receipt of a publication that returned %v was filled after Publish returned: %+v", perr, got)
		}
	})

	t.Run("a reused receipt", func(t *testing.T) {
		topic := newTopic("receipt-reused")
		cl := cluster(t, 2, []string{topic})
		c := openOn(t, cl, func(cfg *kq.Config) { cfg.WriteTimeout = 10 * time.Second; cfg.MaxAttempts = 1 })
		pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		for p := range 2 {
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, mq.Partition(p)); err != nil {
				t.Fatalf("warm %d: %v", p, err)
			}
		}
		reached, release := holdProduce(t, cl, topic, 0)
		// One receipt slot, reused across a loop, as Message.Receipt allows.
		var r mq.PublishReceipt
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs := make(chan error, 1)
		go func() { errs <- pub.Publish(ctx, &mq.Message{Body: []byte("first"), Receipt: &r}, mq.Partition(0)) }()
		<-reached
		cancel()
		if err := <-errs; !mq.IsAmbiguous(err) {
			t.Fatalf("the first Publish = %v, want an ambiguous outcome", err)
		}
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("second"), Receipt: &r}, mq.Partition(1)); err != nil {
			t.Fatalf("the second Publish: %v", err)
		}
		if r.Partition != 1 || r.Offset != 1 {
			t.Fatalf("the second publication's receipt = %+v, want partition 1 offset 1", r)
		}
		release()
		waitFor(t, 5*time.Second, "the held record appended", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= 2 })
		time.Sleep(200 * time.Millisecond)
		if r.Partition != 1 || r.Offset != 1 {
			t.Errorf("the second publication's receipt now says partition %d offset %d: the first, failed publication's late completion overwrote it", r.Partition, r.Offset)
		}
	})
}
