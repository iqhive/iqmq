package reqreply_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/reqreply"
)

// flakyBackend is a memory broker whose reply-topic source fails its next
// fetches with err.
type flakyBackend struct {
	mq.Backend
	fails   atomic.Int32
	err     error
	fetched chan struct{}
}

func (b *flakyBackend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	src, err := b.Backend.OpenSource(ctx, cfg)
	if err != nil || cfg.Topic != "replies" {
		return src, err
	}
	return &flakySource{Source: src, b: b}, nil
}

type flakySource struct {
	mq.Source
	b *flakyBackend
}

func (s *flakySource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if s.b.fails.Add(-1) >= 0 {
		select {
		case s.b.fetched <- struct{}{}:
		default:
		}
		return 0, s.b.err
	}
	return s.Source.Fetch(ctx, dst)
}

// flakyRig is a request/reply client over a flakyBackend, with an echo
// responder on "rpc" that counts what it served.
func flakyRig(t *testing.T, fails int32, err error) (*reqreply.Client, *atomic.Int32, *flakyBackend) {
	t.Helper()
	broker, berr := memory.NewBroker(memory.Config{AutoCreate: true})
	if berr != nil {
		t.Fatal(berr)
	}
	fb := &flakyBackend{Backend: broker, err: err, fetched: make(chan struct{}, 1)}
	fb.fails.Store(fails)
	cl := mq.NewClient(fb)
	t.Cleanup(func() { _ = cl.Close() })

	rc, cerr := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
	if cerr != nil {
		t.Fatal(cerr)
	}
	t.Cleanup(func() { _ = rc.Close() })
	var served atomic.Int32
	serve, serr := reqreply.Serve(cl, func(c context.Context, d *mq.Delivery) (*mq.Message, error) {
		served.Add(1)
		return echo(c, d)
	})
	if serr != nil {
		t.Fatal(serr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = rc.Run(ctx, serve) }()

	rr, rerr := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"}, reqreply.WithEmulatedRequestReply())
	if rerr != nil {
		t.Fatal(rerr)
	}
	t.Cleanup(func() { _ = rr.Close() })
	return rr, &served, fb
}

// TestATransientReceiveErrorDoesNotEndTheClient: the reply loop used to end
// on any receive error, after which every Request published its request and
// returned ErrClosed without waiting for the reply.
func TestATransientReceiveErrorDoesNotEndTheClient(t *testing.T) {
	down := &mq.OpError{Operation: "fetch", Code: mq.CodeUnavailable, Retryable: true, Err: errors.New("the broker blinked")}
	rr, _, _ := flakyRig(t, 2, down)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("Request = %v after a transient receive failure, want the reply", err)
	}
	if string(reply.Body) != "re: ping" {
		t.Errorf("reply = %q", reply.Body)
	}
}

// TestATerminalReceiveErrorRefusesBeforePublishing: once the reply loop has
// ended, a Request must be refused before its request is published, because
// a request nobody will read the reply to still makes the responder do the
// work.
func TestATerminalReceiveErrorRefusesBeforePublishing(t *testing.T) {
	// A bubble, so the test can wait for the reply loop to have ended rather
	// than guess how long that takes on a loaded runner.
	synctest.Test(t, func(t *testing.T) {
		gone := &mq.OpError{Operation: "fetch", Code: mq.CodeNotFound, Subscription: "replies-rr", Err: errors.New("the subscription was deleted")}
		rr, served, fb := flakyRig(t, 1, gone)
		select {
		case <-fb.fetched:
		case <-time.After(5 * time.Second):
			t.Fatal("the reply loop never fetched")
		}
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")}); !errors.Is(err, reqreply.ErrClosed) {
			t.Fatalf("Request = %v, want ErrClosed", err)
		}
		synctest.Wait()
		if n := served.Load(); n != 0 {
			t.Errorf("the responder served %d request(s) the client could never read the reply to", n)
		}
	})
}
