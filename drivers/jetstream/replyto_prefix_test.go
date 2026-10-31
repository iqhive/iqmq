package jetstream_test

import (
	"testing"
	"time"

	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestAReplyToUnderTheSubjectPrefixIsAnswered holds that Properties.ReplyTo
// is a topic under SubjectPrefix like any other, including one whose first
// token is the prefix. The reply-to header used to carry the bare topic,
// "<p>.replies" for the topic "<p>/replies", and the responder read it back
// with the prefix stripped, as "replies", so its answer was stored on
// "<p>.replies" while the requester consumed "<p>.<p>.replies".
func TestAReplyToUnderTheSubjectPrefixIsAnswered(t *testing.T) {
	var cfg nq.Config
	cl := openClient(t, func(c *nq.Config) { cfg = *c })
	replyTo := cfg.SubjectPrefix + "/replies"
	reqTopic := newTopic("svc")
	reqs := consumerOn(t, cl, reqTopic, "responder")
	replies := consumerOn(t, cl, replyTo, "requester")

	req := &mq.Message{Body: []byte("ping")}
	req.Properties.ReplyTo = replyTo
	publish(t, cl, reqTopic, req)
	d := receiveOne(t, reqs, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("acking the request: %v", err)
	}
	publish(t, cl, d.Properties.ReplyTo, &mq.Message{Body: []byte("pong")})
	if _, ok := tryReceive(t, replies, 2*time.Second); !ok {
		t.Fatalf("the reply was lost: ReplyTo %q reached the responder as %q", replyTo, d.Properties.ReplyTo)
	}
}

// TestAnInboxReplyToIsCarriedBare holds that a reply-to in the inbox namespace
// goes into the reply-to header unprefixed, as the Core driver writes it as
// the reply subject: a requester listens on its inbox bare, so a header that
// carried "<p>._INBOX..." sent a responder with another prefix, or none, to
// a subject nobody listens on.
func TestAnInboxReplyToIsCarriedBare(t *testing.T) {
	var cfg nq.Config
	cl := openClient(t, func(c *nq.Config) { cfg = *c })
	topic := newTopic("svc")
	m := &mq.Message{Body: []byte("x")}
	m.Properties.ReplyTo = "_INBOX.requester"
	publish(t, cl, topic, m)
	js, err := sjs.New(natsConn(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(t.Context(), cfg.Stream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	raw, err := stream.GetLastMsgForSubject(t.Context(), cfg.SubjectPrefix+"."+topic)
	if err != nil {
		t.Fatalf("reading the stored message: %v", err)
	}
	if got := raw.Header.Get(nq.HeaderReplyTo); got != "_INBOX.requester" {
		t.Fatalf("the reply-to header carries %q, want the inbox %q", got, "_INBOX.requester")
	}
}

// TestAReplyToTooLongForThePrefixIsRefused holds that the strict mode refuses,
// before any I/O, a reply-to that fits a subject on its own but not under the
// prefix, rather than publishing the message without it.
func TestAReplyToTooLongForThePrefixIsRefused(t *testing.T) {
	cl := openClient(t)
	m := &mq.Message{Body: []byte("x")}
	long := make([]byte, nq.MaxSubjectLen-2)
	for i := range long {
		long[i] = 'r'
	}
	m.Properties.ReplyTo = string(long)
	if err := nq.Representable(m); err != nil {
		t.Fatalf("the reply-to does not fit a subject even unprefixed, so the test proves nothing: %v", err)
	}
	err := cl.Publish(t.Context(), newTopic("svc"), m)
	if mq.CodeOf(err) != mq.CodeUnsupported {
		t.Fatalf("Publish with a reply-to that does not fit under the prefix = %v (code %v), want Unsupported", err, mq.CodeOf(err))
	}
}
