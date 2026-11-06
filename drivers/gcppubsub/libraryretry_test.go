package gcppubsub_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// The client library's publisher repeats a Publish RPC on DEADLINE_EXCEEDED,
// INTERNAL, RESOURCE_EXHAUSTED, ABORTED, UNAVAILABLE and a few more, and the
// result carries the last attempt's status alone. So when the service stored
// an earlier attempt and refused the retry (the topic deleted, an IAM binding
// removed, a credential lapsed between the attempts), the publication rules
// read a definite refusal while the message is on the topic, and a caller that
// repeats a definite failure publishes it twice. These tests put a gRPC server
// in front of pstest that stores an attempt and loses its answer, and refuses
// another, and open the driver with Open, so the client is one it dials.

// faultyPublish is pstest behind a gRPC server whose Publish answers follow
// script once armed: for the n-th Publish to a topic ending in topic, "store"
// stores it and answers DEADLINE_EXCEEDED, as when the answer to a stored
// publication is lost, and any other code refuses it.
type faultyPublish struct {
	*pstest.Server
	addr     string
	topic    string
	script   []codes.Code
	armed    atomic.Bool
	attempts atomic.Int32
}

// store is the script step that stores the attempt and loses its answer.
const store = codes.OK

// newFaultyPublish starts the server. It is stopped by the test's cleanup.
func newFaultyPublish(t *testing.T, script ...codes.Code) *faultyPublish {
	t.Helper()
	f := &faultyPublish{Server: pstest.NewServer(), script: script}
	gs := grpc.NewServer(grpc.UnaryInterceptor(f.intercept))
	pubsubpb.RegisterPublisherServer(gs, &f.GServer)
	pubsubpb.RegisterSubscriberServer(gs, &f.GServer)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f.addr = lis.Addr().String()
	var wg sync.WaitGroup
	wg.Go(func() { _ = gs.Serve(lis) })
	t.Cleanup(func() {
		gs.Stop()
		wg.Wait()
		_ = f.Close()
	})
	return f
}

func (f *faultyPublish) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	in, ok := req.(*pubsubpb.PublishRequest)
	if !ok || !f.armed.Load() || !strings.HasSuffix(in.GetTopic(), "/"+f.topic) {
		return handler(ctx, req)
	}
	n := int(f.attempts.Add(1))
	if n > len(f.script) {
		return handler(ctx, req)
	}
	if code := f.script[n-1]; code != store {
		return nil, status.Error(code, "injected: the attempt is refused")
	}
	if _, err := handler(ctx, req); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.DeadlineExceeded, "injected: the answer to a stored publication was lost")
}

// open opens the driver over the server, dialling its own client.
func (f *faultyPublish) open(t *testing.T, tweak ...func(*gps.Config)) *mq.Client {
	t.Helper()
	cfg := gps.Config{
		ProjectID:             "iqmq-test",
		Endpoint:              f.addr,
		InsecureTransport:     true,
		AutoCreate:            true,
		PublishCountThreshold: 1,
		PublishDelayThreshold: time.Millisecond,
	}
	for _, fn := range tweak {
		fn(&cfg)
	}
	c, err := gps.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// storedOn is how many messages pstest holds for topics ending in topic.
func (f *faultyPublish) storedOn(topic string) int {
	n := 0
	for _, m := range f.Messages() {
		if strings.HasSuffix(m.Topic, "/"+topic) {
			n++
		}
	}
	return n
}

// TestAPublicationTheLibraryRetriedAfterItWasStoredIsAmbiguous is hunt 5's
// dk-1 on this driver: the first attempt is stored and its answer lost, and
// the library's retry is refused with a code it does not repeat. The
// publication was reported as a definite refusal; it is ambiguous now, with
// the refusal's own code. A refusal of a first attempt stays definite.
func TestAPublicationTheLibraryRetriedAfterItWasStoredIsAmbiguous(t *testing.T) {
	for _, refusal := range []codes.Code{codes.PermissionDenied, codes.NotFound, codes.InvalidArgument} {
		t.Run(refusal.String()+" after a stored attempt", func(t *testing.T) {
			f := newFaultyPublish(t, store, refusal)
			c := f.open(t)
			f.topic = newTopic("libretry")
			provision(t, c, f.topic, "s")

			f.armed.Store(true)
			err := c.Publish(t.Context(), f.topic, &mq.Message{Body: []byte("once")})
			t.Logf("%d attempt(s), %d stored: %v (code %v, ambiguous %v)",
				f.attempts.Load(), f.storedOn(f.topic), err, mq.CodeOf(err), mq.IsAmbiguous(err))
			if f.storedOn(f.topic) != 1 || f.attempts.Load() < 2 {
				t.Fatalf("setup: %d attempt(s), %d stored, want the first stored and a retry", f.attempts.Load(), f.storedOn(f.topic))
			}
			if err == nil || !mq.IsAmbiguous(err) {
				t.Fatalf("Publish = %v (code %v, ambiguous false), though the library's first attempt is on the "+
					"topic: a caller that repeats a definite failure duplicates the message", err, mq.CodeOf(err))
			}
			var e *mq.OpError
			if !errors.As(err, &e) || e.Retryable {
				t.Fatalf("Publish = %v, want the refusal's own judgement, not retryable", err)
			}
		})
	}

	t.Run("a refusal of the first attempt", func(t *testing.T) {
		f := newFaultyPublish(t, codes.PermissionDenied)
		c := f.open(t)
		f.topic = newTopic("libretry-first")
		provision(t, c, f.topic, "s")

		f.armed.Store(true)
		err := c.Publish(t.Context(), f.topic, &mq.Message{Body: []byte("never")})
		if f.storedOn(f.topic) != 0 || f.attempts.Load() != 1 {
			t.Fatalf("setup: %d attempt(s), %d stored, want one refused attempt", f.attempts.Load(), f.storedOn(f.topic))
		}
		if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) {
			t.Fatalf("Publish = %v (code %v, ambiguous %v), want a definite Forbidden: nothing was stored",
				err, mq.CodeOf(err), mq.IsAmbiguous(err))
		}
	})
}

// TestADeadLetterCopyTheLibraryRetriedAfterItWasStoredIsAmbiguous is the same
// for a Reject's copy: the copy is on the dead-letter topic, so the Reject
// is ambiguous and a second one is refused rather than sending another copy.
func TestADeadLetterCopyTheLibraryRetriedAfterItWasStoredIsAmbiguous(t *testing.T) {
	f := newFaultyPublish(t, store, codes.PermissionDenied)
	dlq := newTopic("libretry-dlq")
	c := f.open(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
	})
	topic := newTopic("libretry-reject")
	provision(t, c, dlq, "d")
	provision(t, c, topic, "s")
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	d := receiveOne(t, cons, 5*time.Second)

	f.topic = dlq
	f.armed.Store(true)
	err = d.Reject()
	t.Logf("%d attempt(s), %d stored: %v (code %v, ambiguous %v)",
		f.attempts.Load(), f.storedOn(dlq), err, mq.CodeOf(err), mq.IsAmbiguous(err))
	if f.storedOn(dlq) != 1 || f.attempts.Load() < 2 {
		t.Fatalf("setup: %d attempt(s), %d stored, want the first stored and a retry", f.attempts.Load(), f.storedOn(dlq))
	}
	if err == nil || !mq.IsAmbiguous(err) {
		t.Fatalf("Reject = %v (code %v, ambiguous false), though its copy is on the dead-letter topic", err, mq.CodeOf(err))
	}
	if again := d.Reject(); !errors.Is(again, mq.ErrSettlementUnknown) {
		t.Fatalf("a second Reject = %v, want ErrSettlementUnknown rather than another copy", again)
	}
	if got := f.storedOn(dlq); got != 1 {
		t.Fatalf("the dead-letter topic holds %d copies, want 1", got)
	}
}
