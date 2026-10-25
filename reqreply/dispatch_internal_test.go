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
