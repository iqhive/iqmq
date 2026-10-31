package claimcheck_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/claimcheck"
)

// partialFanout returns a memory client whose topic "orders" has two
// subscriptions under FanoutIndependent: "full", which has no room, and
// "room", which has room for one record. A publication then reaches "room"
// and not "full", and the broker answers mq.ErrPartial.
func partialFanout(t *testing.T) (*mq.Client, *mq.ConsumerHandle) {
	t.Helper()
	ctx := context.Background()
	client, _ := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
			Fanout:           memory.FanoutIndependent,
		},
	}))
	full, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "full"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = full.Close() })
	// Only "full" exists yet, so this one record fills it.
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatal(err)
	}
	room, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "room"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = room.Close() })
	return client, room
}

// TestAPartialFanoutKeepsThePayload: a publication the broker took for some
// of the topic's subscriptions and refused for the others (mq.ErrPartial) IS
// in the broker, on the subscriptions that took it. Its payload is the only
// copy of the body those subscriptions will ever see, so PublishCheckIn keeps
// it and the delivered reference checks out.
func TestAPartialFanoutKeepsThePayload(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	pm, mw := pipeline(t, claimcheck.Config{Store: s, Threshold: 16})
	client, room := partialFanout(t)

	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
		return client.Publish(ctx, topic, ms[0], opts...)
	})
	body := bytes.Repeat([]byte("a large order "), 64)
	m := &mq.Message{ID: "o-1", Body: body}
	perr := publish(ctx, "orders", []*mq.Message{m}, nil)
	if !errors.Is(perr, mq.ErrPartial) {
		t.Fatalf("Publish = %v, want the partial fan-out this test is about", perr)
	}
	if s.Len() != 1 || !claimcheck.IsRef(m.Body) {
		t.Fatalf("after a partial fan-out the store holds %d payload(s) and the body is a reference: %v; want 1 and true",
			s.Len(), claimcheck.IsRef(m.Body))
	}

	dst := make([]mq.Delivery, 1)
	n, err := room.Receive(ctx, dst)
	if err != nil || n != 1 {
		t.Fatalf("Receive on the subscription that took the record = %d, %v", n, err)
	}
	var seen []byte
	herr := mw(mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		seen = bytes.Clone(d.Body)
		return nil
	})).Handle(ctx, &dst[0])
	if herr != nil || !bytes.Equal(seen, body) {
		t.Fatalf("CheckOut of the delivered reference = %v (payload gone %v), and the handler saw %d bytes of %d",
			herr, errors.Is(herr, claimcheck.ErrPayloadGone), len(seen), len(body))
	}
}

// TestAPartialFanoutBatchKeepsThePayload is the batch form: an entry of a
// *mq.BatchError that is mq.ErrPartial was taken by a subscription, so its
// payload stays, while the entry the broker refused outright gets its body
// back.
func TestAPartialFanoutBatchKeepsThePayload(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	pm, _ := pipeline(t, claimcheck.Config{Store: s, Threshold: 16})
	client, _ := partialFanout(t)
	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	publish := pm(func(ctx context.Context, _ string, ms []*mq.Message, opts []mq.PublishOption) error {
		return pub.PublishBatch(ctx, ms, opts...)
	})
	// "room" takes the first message and is then full too, so the second is
	// refused by both subscriptions.
	taken := &mq.Message{Body: bytes.Repeat([]byte("a large order "), 64)}
	body := bytes.Repeat([]byte("another large order "), 64)
	refused := &mq.Message{Body: body}
	perr := publish(ctx, "orders", []*mq.Message{taken, refused}, nil)
	var be *mq.BatchError
	if !errors.As(perr, &be) || len(be.Errs) != 2 || mq.CodeOf(be.Errs[0]) != mq.CodePartial || be.Errs[1] == nil {
		t.Fatalf("PublishBatch = %v, want a *mq.BatchError of [partial, refused]", perr)
	}
	if !claimcheck.IsRef(taken.Body) || !bytes.Equal(refused.Body, body) || s.Len() != 1 {
		t.Fatalf("the partly taken message is a reference: %v, the refused one has its body back: %v, the store holds %d payload(s); want true, true and 1",
			claimcheck.IsRef(taken.Body), bytes.Equal(refused.Body, body), s.Len())
	}
}
