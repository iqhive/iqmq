package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
)

// Request performs a native NATS request and waits for one reply (the specification's
// FeatureRequestReply).
//
// It is a driver method rather than a portable one because there is no portable
// request/reply surface in the frozen root API: a caller reaches it through
// client.Native().(*nats.Conn) or, typed, through [Backend.Request] on the
// backend that [OpenBackend] returned. That is a gap rather than a design —
// recorded in the maintenance notes — and it is why this feature has no conformance
// probe and its claim stays unverified in the generated matrix.
//
// What makes it "native" is that the server does the correlation: the requester
// subscribes to a private inbox, the request carries it as the reply subject,
// and a responder that publishes to [github.com/iqhive/iqmq.Properties.ReplyTo]
// answers it. Nothing is stored and nothing is retried; a request that reaches
// no subscriber fails at once with CodeNotFound rather than waiting for the
// deadline, which is the one thing NATS request/reply gives that a hand-rolled
// correlation over two topics does not.
//
// ctx bounds the wait and is required to have a deadline in practice: a request
// with no deadline on a subject nobody answers waits until ctx is done.
// [Backend.Close] ends a request still waiting, on a borrowed connection as on
// an owned one, with an ambiguous CodeClosed: the responder may have it.
func (b *Backend) Request(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errNilMessage
	}
	subject, err := Subject(b.cfg.SubjectPrefix, topic)
	if err != nil {
		return nil, err
	}
	if err := Representable(m); err != nil {
		return nil, err
	}
	if lim := b.caps.Limit(mq.LimitMaxMessageBytes); lim.Known && int64(len(m.Body)) > lim.Value {
		return nil, mq.ErrMessageTooLarge
	}
	var msg natsgo.Msg
	msg.Subject = subject
	Encode(&msg, m, nil)
	// Close cancels rctx, which is how it wakes a request on a connection
	// it does not own.
	rctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if !b.track(&cancel) {
		return nil, mq.ErrClosed
	}
	defer b.untrack(&cancel)
	// The reply subject is the request's own inbox and never the message's.
	// nats.go supplies it — RequestMsgWithContext reads only the subject,
	// the headers and the payload — so a caller who set Properties.ReplyTo
	// on a request has it ignored rather than honoured, and the answer comes
	// back here instead of going somewhere the requester is not listening.
	// TestRequestOverridesTheCallersReplyTo is what holds that.
	// nats.go refuses a request on a connection that has already closed
	// before it writes anything, so that refusal stays definite.
	closedBefore := b.nc.IsClosed()
	reply, err := b.nc.RequestMsgWithContext(rctx, &msg)
	if err != nil && closedBefore {
		return nil, b.errs.MapNet(mq.OpPublish, topic, "", err, false)
	}
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(rctx), errClosedMidRequest) {
		// The request had been written, so the backend's Close leaves
		// it as unknown as a cut connection does.
		return nil, b.errs.MapNet(mq.OpPublish, topic, "", fmt.Errorf("%w: %w", errClosedMidRequest, err), true)
	}
	if err != nil {
		// The request had left the socket by the time anything could fail,
		// so a failure here is ambiguous: the responder may have acted on
		// it and only the reply may have been lost. The server's own "no
		// responders" is the exception, and stays a definite NotFound.
		return nil, b.failWritten(topic, err)
	}
	out := &mq.Message{}
	Decode(out, reply, b.cfg.SubjectPrefix)
	return out, nil
}

// track records a Request in flight, so that [Backend.teardown] can end it.
// It reports false once the backend is closing, when the request must not
// start.
func (b *Backend) track(cancel *context.CancelCauseFunc) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.reqs[cancel] = struct{}{}
	return true
}

// untrack drops a Request that has returned.
func (b *Backend) untrack(cancel *context.CancelCauseFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.reqs, cancel)
}

// Respond publishes m as the answer to a delivery, to the reply subject the
// delivery carried in [github.com/iqhive/iqmq.Properties.ReplyTo].
//
// A handler can do this itself with client.Publish(ctx, d.Properties.ReplyTo,
// m) — the reply subject really is a topic, which is what makes the mapping
// worth having — and this is the form that does not go through the publisher
// cache for a subject that will never be published to twice.
func (b *Backend) Respond(ctx context.Context, replyTo string, m *mq.Message) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	if m == nil {
		return errNilMessage
	}
	if replyTo == "" {
		return errNoReplySubject
	}
	subject, err := b.publishSubject(replyTo)
	if err != nil {
		return err
	}
	if err := Representable(m); err != nil {
		return err
	}
	var msg natsgo.Msg
	msg.Subject = subject
	Encode(&msg, m, nil)
	// A reply is terminal in this API: clearing the reply subject stops a
	// responder that echoed the delivery's properties from asking for a
	// reply to its own reply, which is a loop with no bound.
	msg.Reply = ""
	before := b.nc.LastError()
	if err := b.nc.PublishMsg(&msg); err != nil {
		return b.errs.MapNet(mq.OpPublish, replyTo, "", err, false)
	}
	if err := b.flush(ctx); err != nil {
		// The reply had been written, so a flush that fails leaves it
		// unknown, as a sink's does. It used to come back as nats.go's
		// own error, unmapped.
		return b.failWritten(replyTo, err)
	}
	if err := b.refusal(before, subject, ""); err != nil {
		// The server discarded the reply for permissions, as it does a
		// sink's publication ([sink.confirmOne]).
		return b.errs.MapNet(mq.OpPublish, replyTo, "", err, false)
	}
	return nil
}

// publishSubject is the subject a publication to topic goes to.
//
// An inbox is a subject a requester made up on its own connection and
// listens on bare, so SubjectPrefix does not apply to it: prefixing it sent
// every reply to "<prefix>._INBOX..." where nobody listens, and every Request
// to a prefixed responder timed out. Decode hands such a reply address over
// unprefixed because it never carried the prefix, so leaving it alone here
// is what lets a handler publish to Properties.ReplyTo as the topic it is.
func (b *Backend) publishSubject(topic string) (string, error) {
	if !inInbox(b.nc.Opts.InboxPrefix, topic) {
		return Subject(b.cfg.SubjectPrefix, topic)
	}
	return Subject("", topic)
}

// subscribeSubject is the subject a subscription to topic listens on, with
// the same inbox rule as [Backend.publishSubject]. A subscription that
// prefixed an inbox topic listened on "<prefix>._INBOX..." while every
// publication to the topic went to "_INBOX...", so the two never met.
func (b *Backend) subscribeSubject(topic string) (string, error) {
	if !inInbox(b.nc.Opts.InboxPrefix, topic) {
		return Pattern(b.cfg.SubjectPrefix, topic)
	}
	return Pattern("", topic)
}

// inInbox reports whether subject is in nats.go's default inbox namespace or
// in this connection's custom one. The default is always checked because the
// requester's connection, not this one, chose the reply subject.
func inInbox(custom, subject string) bool {
	if strings.HasPrefix(subject, natsgo.InboxPrefix) {
		return true
	}
	return custom != "" && strings.HasPrefix(subject, custom+".")
}
