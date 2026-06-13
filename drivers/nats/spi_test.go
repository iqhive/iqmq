package nats_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
)

// openSource opens a driver Source directly, which is what the tests below need:
// the portable ConsumerHandle applies bounds of its own, so a check of the
// DRIVER's bound has to go through the SPI rather than through the core.
func openSource(tb testing.TB, b *nq.Backend, topic, sub string, prefetch int) mq.Source {
	tb.Helper()
	src, err := b.OpenSource(tb.Context(), mq.SourceConfig{
		Topic: topic, Subscription: sub, Prefetch: prefetch,
	})
	if err != nil {
		tb.Fatalf("OpenSource: %v", err)
	}
	return src
}

// TestSourceFetchIsBoundedByPrefetch is the specification's "all queues bounded" at the
// driver's own seam.
//
// The portable consumer bounds a Receive by its prefetch on its own, so a test
// that went through ConsumerHandle would pass over a driver whose ring was
// unbounded — it did, which is why this one exists. Here the ring is the only
// bound there is: a source with a prefetch of 4 never hands out more than four
// deliveries in one Fetch, however many the broker pushed at it.
func TestSourceFetchIsBoundedByPrefetch(t *testing.T) {
	b := openBackend(t)
	c := mq.NewClient(b)
	topic := newTopic("spibound")
	const prefetch = 4
	src := openSource(t, b, topic, "workers", prefetch)
	defer func() { _ = src.Close() }()

	for i := range 64 {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 256)
	n, err := src.Fetch(ctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n == 0 {
		t.Fatal("Fetch returned nothing")
	}
	if n > prefetch {
		t.Fatalf("a source with a prefetch of %d handed out %d deliveries in one Fetch: the ring is not a bound", prefetch, n)
	}
}

// TestSourceHasNoSettler is the SPI half of AckNone model: the source
// says so, rather than the core inferring it.
func TestSourceHasNoSettler(t *testing.T) {
	b := openBackend(t)
	src := openSource(t, b, newTopic("spisettler"), "workers", 4)
	defer func() { _ = src.Close() }()
	if s := src.Settler(); s != nil {
		t.Fatalf("Settler = %T, want nil: AckNone model is declared by a nil settler", s)
	}
}

// TestBackendCloseUnblocksAFetchNobodyClosed is the specification's "a backend closes what it
// opened", reached the way it really happens: a caller who closes the backend
// while a source it opened is parked in Fetch.
//
// A client closes its consumer handles first, so this path is invisible from
// the portable API — the earlier version of this check went through
// mq.Client.Close and passed with the backend's own teardown deleted.
func TestBackendCloseUnblocksAFetchNobodyClosed(t *testing.T) {
	b := openBackend(t)
	src := openSource(t, b, newTopic("spiclose"), "workers", 4)
	done := make(chan error, 1)
	go func() {
		_, err := src.Fetch(t.Context(), make([]mq.DriverDelivery, 1))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a parked Fetch woke with %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the backend did not unblock a Fetch in a source nobody closed")
	}
}

// TestRequestOverridesTheCallersReplyTo is the one line of Request that is not
// obvious: the reply subject belongs to the request, so a caller who set
// Properties.ReplyTo on a request message must not have the answer sent
// somewhere they are not listening.
func TestRequestOverridesTheCallersReplyTo(t *testing.T) {
	b := openBackend(t)
	c := mq.NewClient(b)
	topic := newTopic("spirpc")
	cons := consumerOn(t, c, topic, "responders")
	// A subscription on the subject the caller wrongly named as the reply
	// destination. Nothing may arrive there.
	stray := newTopic("stray")
	strayCons := consumerOn(t, c, stray, "watchers")

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 1)
		if n, _ := cons.Receive(ctx, dst); n == 1 {
			_ = b.Respond(ctx, dst[0].Properties.ReplyTo, &mq.Message{Body: []byte("pong")})
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req := &mq.Message{Body: []byte("ping")}
	req.Properties.ReplyTo = stray
	reply, err := b.Request(ctx, topic, req)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(reply.Body) != "pong" {
		t.Fatalf("reply = %q", reply.Body)
	}
	sctx, scancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer scancel()
	if n, _ := strayCons.Receive(sctx, make([]mq.Delivery, 1)); n != 0 {
		t.Fatal("the request was sent with the caller's reply subject: the answer would go where the requester is not listening")
	}
}

// TestDeliveryInfoReportsTheResolvedSubject is the metadata names the
// concrete topic a wildcard delivery arrived on, not the pattern the
// subscription was opened with.
//
// mq.Delivery embeds Message, so d.Topic is the MESSAGE's topic and Info() is
// the delivery metadata; a check that only read d.Topic passed with the
// metadata reporting the pattern, which is what this one adds.
func TestDeliveryInfoReportsTheResolvedSubject(t *testing.T) {
	b := openBackend(t)
	c := mq.NewClient(b)
	root := newTopic("spiwild")
	src := openSource(t, b, root+".*", "watchers", 4)
	defer func() { _ = src.Close() }()
	concrete := root + ".created"
	if err := c.Publish(t.Context(), concrete, &mq.Message{Body: []byte("w")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, dst)
	if n != 1 {
		t.Fatalf("Fetch returned %d, err %v", n, err)
	}
	if dst[0].Info.Topic != concrete {
		t.Fatalf("DeliveryInfo.Topic = %q, want the resolved %q", dst[0].Info.Topic, concrete)
	}
	if dst[0].Message.Topic != concrete {
		t.Fatalf("Message.Topic = %q, want the resolved %q", dst[0].Message.Topic, concrete)
	}
	// The specification: an attempt count the broker does not keep is zero, and a lease
	// it does not grant is the zero time. Neither is a default standing in
	// for something better.
	if dst[0].Info.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0: NATS Core counts no attempts", dst[0].Info.Attempt)
	}
	if !dst[0].Info.LeaseExpires.IsZero() {
		t.Errorf("LeaseExpires = %v, want the zero time: NATS Core grants no lease", dst[0].Info.LeaseExpires)
	}
	if dst[0].Info.HasOffset || dst[0].Info.HasSequence || dst[0].Info.HasPartition {
		t.Error("the delivery claims an offset, a sequence or a partition: NATS Core has none of the three")
	}
	if dst[0].Info.Subscription != "watchers" {
		t.Errorf("Subscription = %q, want watchers", dst[0].Info.Subscription)
	}
}
