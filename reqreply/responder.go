package reqreply

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// HeaderReplyTo is the portable header a reply topic travels in when the
// backend has no native property for it.
const HeaderReplyTo = "reply-to"

// ReplyToOf returns the topic a reply to d should be published to: the typed
// property when the driver filled it, and the reply-to header otherwise
// (the specification maps the property to a native field on the backends that have one and
// to the header everywhere else). It returns "" for a delivery that is not a
// request.
func ReplyToOf(d *mq.Delivery) string {
	if t := d.Properties.ReplyTo; t != "" {
		return t
	}
	if v, ok := d.Headers.Get(HeaderReplyTo); ok {
		return string(v)
	}
	return ""
}

// Respond publishes reply as the answer to the request d.
//
// The reply is addressed to d's reply topic and stamped with d's correlation
// id, which is the whole protocol: the requesting client matches on that id
// and on nothing else. A request that names no reply topic is ErrNoReplyTo —
// there is nowhere to send the answer, and publishing it to a guessed topic
// would be worse than refusing.
//
// The correlation id is copied from the request and never invented. A
// responder that generated its own would produce a reply no waiter matches,
// and the request would time out with its answer sitting acked on the reply
// topic.
//
// It does not settle d: settling the request is the handler's business, and
// the ordering — reply first, then settle, or settle first and risk a crash
// between the two — is a choice only the application can make.
func Respond(ctx context.Context, p Publisher, d *mq.Delivery, reply *mq.Message, opts ...mq.PublishOption) error {
	topic := ReplyToOf(d)
	if topic == "" {
		return ErrNoReplyTo
	}
	if reply == nil {
		return ErrInvalidConfig
	}
	reply.Properties.CorrelationID = correlationOf(d)
	return p.Publish(ctx, topic, reply, opts...)
}

// Publisher is the publishing half Respond needs: *mq.Client implements it.
// It is an interface so that a responder can be given a wrapped publisher — a
// middleware chain, a test double — without this package depending on the
// concrete handle.
type Publisher interface {
	// Publish publishes m to topic.
	Publish(ctx context.Context, topic string, m *mq.Message, opts ...mq.PublishOption) error
}

var _ Publisher = (*mq.Client)(nil)

// Answer produces the reply to one request. Returning a nil message and a nil
// error means "no reply": the request is handled and nothing is published,
// which is what a one-way message on a request topic is.
type Answer func(ctx context.Context, d *mq.Delivery) (*mq.Message, error)

// Serve wraps an Answer into the mq.Handler a runner takes, so that the
// responder side is one call:
//
//	_ = c.Run(ctx, reqreply.Serve(client, func(ctx context.Context, d *mq.Delivery) (*mq.Message, error) {
//		return &mq.Message{Body: answer(d.Body)}, nil
//	}))
//
// The answer's error is returned unchanged, so the runner's failure policy —
// retry, dead-letter, halt — decides exactly as it would for any handler.
//
// A request that names no reply topic is handled and NOT failed: a publisher
// that sent a message to a request topic without asking for an answer has
// done nothing wrong, and failing such a delivery would dead-letter perfectly
// good traffic. A caller who requires a reply topic checks ReplyToOf itself.
func Serve(p Publisher, fn Answer) mq.Handler {
	if p == nil || fn == nil {
		panic(ErrInvalidConfig)
	}
	return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		reply, err := fn(ctx, d)
		if err != nil {
			return err
		}
		if reply == nil || ReplyToOf(d) == "" {
			return nil
		}
		return Respond(ctx, p, d, reply)
	})
}
