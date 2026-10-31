package reqreply_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/reqreply"
)

// deliveredBackend is a memory broker whose request topic, "rpc", delivers
// every publication and then answers it with err: the shape of a broker that
// took a request and could not say so, or took it for only some of the
// topic's subscriptions.
type deliveredBackend struct {
	mq.Backend
	err error
}

func (b *deliveredBackend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	s, err := b.Backend.OpenSink(ctx, cfg)
	if err != nil || cfg.Topic != "rpc" {
		return s, err
	}
	return deliveredSink{Sink: s, err: b.err}, nil
}

type deliveredSink struct {
	mq.Sink
	err error
}

func (s deliveredSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.Sink.Publish(ctx, m, o); err != nil {
		return err
	}
	return s.err
}

// runResponder runs the responder until the test ends, and waits for it.
func runResponder(t *testing.T, rc *mq.ConsumerHandle, h mq.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { _ = rc.Run(ctx, h) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// deliveredRig is a request/reply client over a deliveredBackend, with a
// responder on "rpc" that answers with answer.
func deliveredRig(t *testing.T, err error, answer reqreply.Answer) *reqreply.Client {
	t.Helper()
	broker, berr := memory.NewBroker(memory.Config{AutoCreate: true})
	if berr != nil {
		t.Fatal(berr)
	}
	cl := mq.NewClient(&deliveredBackend{Backend: broker, err: err})
	t.Cleanup(func() { _ = cl.Close() })
	rc, cerr := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
	if cerr != nil {
		t.Fatal(cerr)
	}
	t.Cleanup(func() { _ = rc.Close() })
	serve, serr := reqreply.Serve(cl, answer)
	if serr != nil {
		t.Fatal(serr)
	}
	runResponder(t, rc, serve)
	rr, rerr := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"}, reqreply.WithEmulatedRequestReply())
	if rerr != nil {
		t.Fatal(rerr)
	}
	t.Cleanup(func() { _ = rr.Close() })
	return rr
}

// TestARequestThatMayHaveArrivedWaitsForItsReply: a request publication that
// did not definitely fail may have reached a responder: one the broker took
// for some of the topic's subscriptions (mq.ErrPartial), when the responder's
// is among them, and one whose outcome is unknown (mq.IsAmbiguous). Request
// used to read either as "the request never left", return the publication's
// error at once and drop the slot, so the responder's reply arrived unmatched
// and a caller who retried had the work done twice. It now waits for the
// reply, and a wait that ends without one returns the publication's own error,
// so the caller still sees what the broker said.
func TestARequestThatMayHaveArrivedWaitsForItsReply(t *testing.T) {
	partial := &mq.OpError{Operation: mq.OpPublish, Topic: "rpc", Code: mq.CodePartial, Err: mq.ErrPartial}
	ambiguous := &mq.OpError{
		Operation: mq.OpPublish, Topic: "rpc", Code: mq.CodeUnavailable, Ambiguous: true,
		Err: errors.New("the connection closed after the request was written"),
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"a partial fan-out", partial},
		{"an ambiguous publication", ambiguous},
	} {
		t.Run(tc.name+" gets its reply", func(t *testing.T) {
			rr := deliveredRig(t, tc.err, echo)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reply, err := rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
			if err != nil || reply == nil || string(reply.Body) != "re: ping" {
				t.Fatalf("Request = %v, %v; want the responder's reply \"re: ping\": the request reached it", reply, err)
			}
			if p := rr.Pending(); p != 0 {
				t.Fatalf("%d correlation entries held after the reply, want 0", p)
			}
		})
		// No reply: the responder's subscription refused the request, or it
		// answers nothing. The caller is told what the broker said, not a
		// bare deadline.
		t.Run(tc.name+" with no reply returns the publication's error", func(t *testing.T) {
			rr := deliveredRig(t, tc.err, func(context.Context, *mq.Delivery) (*mq.Message, error) {
				return noReply, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
			if !errors.Is(err, tc.err) || mq.IsAmbiguous(err) != mq.IsAmbiguous(tc.err) {
				t.Fatalf("Request = %v, want the publication's own error %v", err, tc.err)
			}
			if p := rr.Pending(); p != 0 {
				t.Fatalf("%d correlation entries held after the wait ended, want 0", p)
			}
		})
	}
}

// replyPublisher is a reqreply.Publisher whose every publication answers err.
type replyPublisher struct{ err error }

func (p replyPublisher) Publish(context.Context, string, *mq.Message, ...mq.PublishOption) error {
	return p.err
}

// TestServeHandlesARequestWhoseReplyFannedOutPartly: a reply the broker took
// for some of the reply topic's subscriptions (mq.ErrPartial) is published.
// Serve returned the error to the runner as a handler failure, so the request
// was redelivered, the answer ran again and a second reply went to every
// subscription that took the first. A reply refused outright still fails the
// handler.
func TestServeHandlesARequestWhoseReplyFannedOutPartly(t *testing.T) {
	partial := &mq.OpError{Operation: mq.OpPublish, Topic: "replies", Code: mq.CodePartial, Err: mq.ErrPartial}
	refused := &mq.OpError{Operation: mq.OpPublish, Topic: "replies", Code: mq.CodeResourceExhausted, Err: mq.ErrQueueFull}
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"a partial fan-out", partial, nil},
		{"a refusal", refused, refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := reqreply.Serve(replyPublisher{err: tc.err}, echo)
			if err != nil {
				t.Fatal(err)
			}
			d := &mq.Delivery{Message: mq.Message{Body: []byte("ping")}}
			d.Properties.ReplyTo = "replies"
			d.Properties.CorrelationID = "c-1"
			if got := h.Handle(context.Background(), d); !errors.Is(got, tc.want) {
				t.Fatalf("the handler returned %v, want %v", got, tc.want)
			}
		})
	}
}

// TestARequestToAPartialFanoutGetsItsReply is the same over the memory
// broker's own FanoutIndependent: the request topic has a responder's
// subscription with room and a second subscription that is full, so the
// publication answers mq.ErrPartial and the responder still gets the request.
func TestARequestToAPartialFanoutGetsItsReply(t *testing.T) {
	ctx := context.Background()
	cl, _ := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
			Fanout:           memory.FanoutIndependent,
		},
	}), memory.TestTopics("rpc", "replies"))
	audit, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "rpc", Subscription: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	// Only "audit" exists yet, so this one record fills it.
	if err := cl.Publish(ctx, "rpc", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatal(err)
	}
	rc, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Close() })
	serve, err := reqreply.Serve(cl, echo)
	if err != nil {
		t.Fatal(err)
	}
	runResponder(t, rc, serve)
	rr, err := reqreply.New(ctx, cl, reqreply.Config{ReplyTopic: "replies"}, reqreply.WithEmulatedRequestReply())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rr.Close() })

	qctx, qcancel := context.WithTimeout(ctx, 5*time.Second)
	defer qcancel()
	reply, err := rr.Request(qctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil || reply == nil || string(reply.Body) != "re: ping" {
		t.Fatalf("Request = %v, %v; want the reply from the subscription that took the request", reply, err)
	}
}
