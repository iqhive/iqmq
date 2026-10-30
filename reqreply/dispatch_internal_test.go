package reqreply

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestCloseOfAnotherClientFromOnUnmatchedWaitsForItsLoop: Close skips its wait
// only when it runs inside the client's own OnUnmatched. Called from another
// client's callback, it used to skip it too, because the caller was a reply
// loop, so it returned while this client's loop was still handing over a
// reply and its correlation sweep never ran.
func TestCloseOfAnotherClientFromOnUnmatchedWaitsForItsLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cl, _ := memory.NewForTest(t, memory.TestTopics("rpc", "replies", "strays"))
		b, err := New(context.Background(), cl, Config{ReplyTopic: "replies"}, WithEmulatedRequestReply())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		taken := make(chan struct{})
		release := make(chan struct{})
		b.tookReply = func() {
			close(taken)
			<-release
		}
		loopEnded := make(chan bool, 1)
		a, err := New(context.Background(), cl, Config{
			ReplyTopic: "strays",
			OnUnmatched: func(context.Context, *mq.Delivery) {
				_ = b.Close()
				select {
				case <-b.done:
					loopEnded <- true
				default:
					loopEnded <- false
				}
			},
		}, WithEmulatedRequestReply())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Close() })

		rc, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rc.Close() })
		serve, err := Serve(cl, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
			return &mq.Message{Body: d.Body}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(context.Background())
		t.Cleanup(stop)
		go func() { _ = rc.Run(runCtx, serve) }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() { _, _ = b.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")}) }()
		<-taken
		// b's loop is now handing a reply over. A stray on a's topic runs a's
		// callback, which closes b.
		if err := cl.Publish(ctx, "strays", &mq.Message{Body: []byte("stray")}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		close(release)
		if !<-loopEnded {
			t.Fatal("b.Close from a's OnUnmatched returned while b's reply loop was still running, so b's correlation sweep never ran")
		}
	})
}

// TestADeadlineDuringDispatchStillGetsTheReply: a request whose deadline
// passed after the reply loop had taken its entry, but before the loop handed
// the reply over, used to look at its channel once, find it empty and report
// the deadline. The reply was then delivered to nobody and acked, and the
// caller was told an answered request had timed out.
func TestADeadlineDuringDispatchStillGetsTheReply(t *testing.T) {
	// A bubble, so the test can wait for the requester to have reached its
	// decision instead of guessing how long that takes on a loaded runner.
	synctest.Test(t, func(t *testing.T) {
		cl, _ := memory.NewForTest(t, memory.TestTopics("rpc", "replies"))
		rr, err := New(context.Background(), cl, Config{ReplyTopic: "replies"}, WithEmulatedRequestReply())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rr.Close() })

		taken := make(chan struct{})
		release := make(chan struct{})
		rr.tookReply = func() {
			close(taken)
			<-release
		}

		rc, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rc.Close() })
		serve, err := Serve(cl, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
			return &mq.Message{Body: append([]byte("re: "), d.Body...)}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(context.Background())
		t.Cleanup(stop)
		go func() { _ = rc.Run(runCtx, serve) }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		type result struct {
			reply *mq.Message
			err   error
		}
		got := make(chan result, 1)
		go func() {
			reply, err := rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
			got <- result{reply, err}
		}()

		<-taken
		cancel()
		// The requester has now seen its deadline and lost the entry to the
		// reply loop, which is still holding the reply.
		synctest.Wait()
		close(release)

		r := <-got
		if errors.Is(r.err, context.Canceled) {
			t.Fatal("Request reported its deadline for a request the reply loop had already answered")
		}
		if r.err != nil {
			t.Fatalf("Request = %v, want the reply", r.err)
		}
		if string(r.reply.Body) != "re: ping" {
			t.Errorf("reply = %q, want %q", r.reply.Body, "re: ping")
		}
	})
}
