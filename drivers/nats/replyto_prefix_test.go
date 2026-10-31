package nats_test

import (
	"context"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/reqreply"
)

// replyReaches publishes a request carrying replyTo on reqTopic, lets answer
// reply to it, and reports whether the reply reached a consumer of replyTo on
// the requester's own client.
func replyReaches(t *testing.T, c *mq.Client, reqTopic, replyTo string, answer func(ctx context.Context)) bool {
	t.Helper()
	replies := consumerOn(t, c, replyTo, "requester")
	req := &mq.Message{Body: []byte("ping")}
	req.Properties.ReplyTo = replyTo
	if err := c.Publish(t.Context(), reqTopic, req); err != nil {
		t.Fatalf("publishing the request: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	answer(ctx)
	rctx, rcancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer rcancel()
	dst := make([]mq.Delivery, 1)
	n, _ := replies.Receive(rctx, dst)
	return n == 1
}

// TestAReplyToUnderTheSubjectPrefixIsAnswered holds that Properties.ReplyTo
// is a topic under SubjectPrefix like any other. Two clients with the prefix
// "app" and the reply topic "app/replies": the requester listens on
// "app.app.replies". The reply subject used to go on the wire unprefixed, as
// "app.replies", and the responder read it back with the prefix stripped, as
// "replies", so its answer went to "app.replies" and nobody heard it.
func TestAReplyToUnderTheSubjectPrefixIsAnswered(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.SubjectPrefix = "app" })
	reqTopic := newTopic("svc")
	reqs := consumerOn(t, c, reqTopic, "responder")
	var seen string
	ok := replyReaches(t, c, reqTopic, "app/replies", func(ctx context.Context) {
		d := receiveOne(t, reqs, 5*time.Second)
		seen = d.Properties.ReplyTo
		if err := c.Publish(ctx, d.Properties.ReplyTo, &mq.Message{Body: []byte("pong")}); err != nil {
			t.Fatalf("publishing the reply: %v", err)
		}
	})
	if !ok {
		t.Fatalf("the reply was lost: ReplyTo %q reached the responder as %q", "app/replies", seen)
	}
}

// TestANativeResponderAnswersAPrefixedReplyTo holds that the reply subject on
// the wire is the one the requester listens on, so a plain nats.go responder
// that answers with msg.Respond reaches it.
func TestANativeResponderAnswersAPrefixedReplyTo(t *testing.T) {
	url := serverURL(t)
	c, err := nq.Open(t.Context(), nq.Config{Addrs: []string{url}, SubjectPrefix: "app"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	topic := newTopic("svc")
	gotReply := make(chan string, 1)
	sub, err := nc.Subscribe("app."+topic, func(m *natsgo.Msg) {
		gotReply <- m.Reply
		_ = m.Respond([]byte("pong"))
	})
	if err != nil {
		t.Fatalf("subscribing natively: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing the native subscription: %v", err)
	}
	var wire string
	ok := replyReaches(t, c, topic, "replies", func(ctx context.Context) {
		select {
		case wire = <-gotReply:
		case <-ctx.Done():
			t.Fatal("the native responder never saw the request")
		}
	})
	if !ok {
		t.Fatalf("the reply was lost: the native responder answered %q; the requester listens on %q", wire, "app.replies")
	}
}

// TestTheReqReplyPackageWorksUnderASubjectPrefix is the reqreply package's
// emulated request/reply with its documented ReplyTopic "svc.replies", on a
// client whose SubjectPrefix is "svc". Every request used to time out with
// its answer published.
func TestTheReqReplyPackageWorksUnderASubjectPrefix(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.SubjectPrefix = "svc" })
	topic := newTopic("pricing")
	reqs := consumerOn(t, c, topic, "responder")
	rr, err := reqreply.New(t.Context(), c, reqreply.Config{ReplyTopic: "svc.replies", Timeout: 2 * time.Second},
		reqreply.WithEmulatedRequestReply())
	if err != nil {
		t.Fatalf("reqreply.New: %v", err)
	}
	t.Cleanup(func() { _ = rr.Close() })
	served := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 1)
		if n, err := reqs.Receive(ctx, dst); n != 1 {
			served <- err
			return
		}
		served <- reqreply.Respond(ctx, c, &dst[0], &mq.Message{Body: []byte("42")})
	}()
	_, err = rr.Request(t.Context(), topic, &mq.Message{Body: []byte("quote?")})
	if serr := <-served; serr != nil {
		t.Fatalf("the responder: %v", serr)
	}
	if err != nil {
		t.Fatalf("Request with its reply published: %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestAReplyToTooLongForThePrefixIsRefused holds that the strict mode refuses,
// before any I/O, a reply-to that fits a subject on its own but not under the
// prefix, rather than publishing the message without it.
func TestAReplyToTooLongForThePrefixIsRefused(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.SubjectPrefix = "app" })
	m := &mq.Message{Body: []byte("x")}
	long := make([]byte, nq.MaxSubjectLen-2)
	for i := range long {
		long[i] = 'r'
	}
	m.Properties.ReplyTo = string(long)
	if err := nq.Representable(m); err != nil {
		t.Fatalf("the reply-to does not fit a subject even unprefixed, so the test proves nothing: %v", err)
	}
	err := c.Publish(t.Context(), newTopic("svc"), m)
	if mq.CodeOf(err) != mq.CodeUnsupported {
		t.Fatalf("Publish with a reply-to that does not fit under the prefix = %v (code %v), want Unsupported", err, mq.CodeOf(err))
	}
}

// TestAnInboxNamedTopicUnderAPrefixArrives holds that a topic in the inbox
// namespace maps to one subject on both sides. A publication to it is left
// unprefixed so that a reply reaches a requester's inbox, and a subscription
// to it used to be prefixed, so under SubjectPrefix "app" a publisher and a
// consumer of "_INBOX.jobs" on one client used "_INBOX.jobs" and
// "app._INBOX.jobs" and never met.
func TestAnInboxNamedTopicUnderAPrefixArrives(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.SubjectPrefix = "app" })
	topic := "_INBOX." + newTopic("jobs")
	cons := consumerOn(t, c, topic, "workers")
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("job")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if n != 1 {
		t.Fatalf("a publication to %q never reached a consumer of it on the same client: %v", topic, err)
	}
	if dst[0].Info().Topic != topic {
		t.Fatalf("the delivery reports topic %q, want %q", dst[0].Info().Topic, topic)
	}
}
