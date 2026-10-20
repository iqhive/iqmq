// Package typed puts message types over the byte-level publisher and consumer.
//
// The core is bytes in, bytes out and stays that way — nothing here is
// mandatory, no codec is imported by the root package, and a program that
// already holds bytes never pays for any of this. What the typed layer adds
// is the compiler: a Publisher[Order] refuses an Invoice where a bytes
// publisher would have published it.
//
//	orders, err := typed.NewPublisher(pub, codec.JSON[Order]())
//	if err != nil {
//		return err
//	}
//	_ = orders.Publish(ctx, Order{ID: 1})
//	_ = typed.Run(ctx, consumer, codec.JSON[Order](), func(ctx context.Context, d *typed.Delivery[Order]) error {
//		return process(d.Value)
//	})
//
// # The decode failure path is explicit
//
// A delivery that does not decode is the one case this package exists to get
// right, because the two obvious implementations are both wrong: a panic
// takes the worker with it, and a silent drop destroys a message iqmq's own
// The specification says must never be skipped by default. So a body that will not decode
// is an error the handler never sees, and what becomes of the delivery is a
// named policy — HandlerConfig.OnDecodeError, defaulting to DeadLetter.
//
// DeadLetter is the default because a decode failure is not transient: the
// same bytes will not decode on the next attempt either, so retrying is a
// loop and discarding is data loss. Dead-lettering hands the message to the
// route ADR-011 defines, which halts the consumer when there is no such
// route — "DeadLetter if explicit DLQ else Halt, never Skip".
//
// # Ownership and concurrency
//
// A Publisher[T] is safe for concurrent use if the mq.Publisher and the codec
// under it are, which both are for every implementation in this repository. A
// handler built by NewHandler is safe for concurrent use; the *Delivery[T] it
// is given belongs to the call and must not be retained after it returns, for
// the same reason an mq.Delivery must not be.
package typed

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// ErrNilCodec reports a nil codec handed to NewPublisher or NewHandler. It is
// a programming error and carries CodeInvalidArgument.
var ErrNilCodec error = errcode.New(errcode.InvalidArgument, "iqmq/typed: codec is nil")

// ErrNilHandler reports a nil handler handed to NewHandler.
var ErrNilHandler error = errcode.New(errcode.InvalidArgument, "iqmq/typed: handler is nil")

var (
	errNilPublisher      = errcode.New(errcode.InvalidArgument, "iqmq/typed: publisher is nil")
	errContentTypePolicy = errcode.New(errcode.InvalidConfig, "iqmq/typed: HandlerConfig.ContentType names no policy")
)

// Publisher publishes values of T through a bytes publisher and a codec.
//
// The zero value is not usable; NewPublisher returns a ready one. It does not
// own the mq.Publisher under it: closing that is the caller's, because a
// typed view of a publisher is a view and not a second handle.
type Publisher[T any] struct {
	pub    mq.Publisher
	codec  codec.Typed[T]
	ctype  string
	schema string
}

// NewPublisher binds p and c into a typed publisher.
//
// It fails on a nil codec or a nil publisher: both are programming errors
// discovered at construction, and the specification keeps construction failures out of the
// per-call path. The content type is read once, here, and stamped on every
// message — a codec's content type does not change.
func NewPublisher[T any](p mq.Publisher, c codec.Typed[T]) (*Publisher[T], error) {
	if c == nil {
		return nil, ErrNilCodec
	}
	if p == nil {
		return nil, errNilPublisher
	}
	tp := &Publisher[T]{pub: p, codec: c, ctype: c.ContentType()}
	if s, ok := c.(codec.Schemer); ok {
		tp.schema = s.SchemaID()
	}
	return tp, nil
}

// ContentType returns the media type this publisher stamps, which is its
// codec's.
func (p *Publisher[T]) ContentType() string { return p.ctype }

// Encode encodes v into m: the body, the content type, and the schema id when
// the codec has one (the specification's "content-type/schema-id set automatically").
//
// Everything else on m is the caller's and is left alone, so a caller that
// wants a key, headers or an identifier fills those in and calls this — which
// is what PublishMessage does.
func (p *Publisher[T]) Encode(m *mq.Message, v T) error {
	b, err := p.codec.Encode(v)
	if err != nil {
		return err
	}
	m.Body = b
	m.Properties.ContentType = p.ctype
	if p.schema != "" {
		m.Headers.Set(codec.HeaderSchemaID, []byte(p.schema))
	}
	return nil
}

// Publish encodes v and publishes it.
//
// The message it builds is its own: a caller that needs a key, headers or an
// identifier on the message wants PublishMessage.
func (p *Publisher[T]) Publish(ctx context.Context, v T, opts ...mq.PublishOption) error {
	var m mq.Message
	if err := p.Encode(&m, v); err != nil {
		return err
	}
	return p.pub.Publish(ctx, &m, opts...)
}

// PublishMessage encodes v into m and publishes m, so that a caller decides
// the key, the headers and the identifier and this package decides the body.
//
// m's Body, Properties.ContentType and schema-id header are overwritten;
// everything else on it is published as the caller left it.
func (p *Publisher[T]) PublishMessage(ctx context.Context, m *mq.Message, v T, opts ...mq.PublishOption) error {
	if err := p.Encode(m, v); err != nil {
		return err
	}
	return p.pub.Publish(ctx, m, opts...)
}

// PublishBatch encodes every value and publishes them as one batch.
//
// A value that does not encode fails the whole call before anything is
// published: a batch that published the encodable half and reported the rest
// would be a partial publication the caller never asked for, and the specification's
// *BatchError is for a broker that accepted some of them, not for a caller
// that handed over one bad value.
func (p *Publisher[T]) PublishBatch(ctx context.Context, vs []T, opts ...mq.PublishOption) error {
	if len(vs) == 0 {
		return nil
	}
	ms := make([]mq.Message, len(vs))
	ptrs := make([]*mq.Message, len(vs))
	for i := range vs {
		if err := p.Encode(&ms[i], vs[i]); err != nil {
			return err
		}
		ptrs[i] = &ms[i]
	}
	return p.pub.PublishBatch(ctx, ptrs, opts...)
}

// Flush flushes the publisher underneath.
func (p *Publisher[T]) Flush(ctx context.Context) error { return p.pub.Flush(ctx) }

// Delivery is one received message decoded into a T.
//
// The embedded *mq.Delivery is the untyped delivery: Ack, Nack, Reject,
// Info, Headers and the raw Body all reach through it, so a typed handler
// settles exactly as an untyped one does. It belongs to the Handle call and
// must not be retained after it returns.
type Delivery[T any] struct {
	*mq.Delivery
	// Value is the decoded body. It is only meaningful inside Handle: the
	// handler was not called at all for a body that did not decode.
	Value T
}

// Handler processes one decoded delivery.
type Handler[T any] interface {
	// Handle processes the delivery.
	Handle(ctx context.Context, d *Delivery[T]) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc[T any] func(ctx context.Context, d *Delivery[T]) error

// Handle calls f.
func (f HandlerFunc[T]) Handle(ctx context.Context, d *Delivery[T]) error { return f(ctx, d) }

// ContentTypePolicy is how strictly a delivery's declared content type is
// held to the codec's (the specification's "mismatches → *codec.ContentTypeError").
type ContentTypePolicy uint8

// The content-type policies.
const (
	// ContentTypeIgnore decodes whatever arrives. It is the zero value and
	// the default, because a great many brokers carry no content type at all
	// and a typed consumer of a single-schema topic does not need one.
	ContentTypeIgnore ContentTypePolicy = iota
	// ContentTypeCheck refuses a delivery whose declared content type is
	// not the codec's, and accepts one that declares none.
	ContentTypeCheck
	// ContentTypeRequire additionally refuses a delivery that declares no
	// content type at all.
	ContentTypeRequire

	contentTypePolicyCount
)

var contentTypePolicyNames = [contentTypePolicyCount]string{"ignore", "check", "require"}

// String returns the policy's lower-case name.
func (p ContentTypePolicy) String() string {
	if p < contentTypePolicyCount {
		return contentTypePolicyNames[p]
	}
	return "ContentTypePolicy(?)"
}

// Valid reports whether p names a policy.
func (p ContentTypePolicy) Valid() bool { return p < contentTypePolicyCount }

// DecodeErrorFunc decides what becomes of a delivery that did not decode.
//
// It is given the raw delivery and the codec's own error — a
// *codec.DecodeError for a malformed body, a *codec.ContentTypeError for a
// mismatch — and returns the error the runner acts on. Returning nil acks the
// delivery, which is the one answer this package will not choose for a
// caller: see DeadLetter.
//
// A DecodeErrorFunc may settle the delivery itself; the runner does not
// overrule a delivery that is already settled.
type DecodeErrorFunc func(ctx context.Context, d *mq.Delivery, err error) error

// DeadLetter is the default DecodeErrorFunc: the delivery is dead-lettered
// now, without exhausting its attempts.
//
// A decode failure is not transient — the same bytes will not decode on the
// next attempt — so retrying is a loop with a message in it, and discarding
// is the silent drop the specification forbids. Dead-lettering hands it to the route
// ADR-011 defines, and halts the consumer when there is no such route, which
// is the failure a human can act on.
func DeadLetter(_ context.Context, _ *mq.Delivery, err error) error {
	return mq.DeadLetter(err)
}

// Discard is the DecodeErrorFunc for a caller who has decided that an
// undecodable message on this topic is noise: the delivery is acked without
// success, logged and counted by the runner (DirectiveDiscard).
//
// It is never the default. Choosing it is choosing to lose the message.
func Discard(_ context.Context, _ *mq.Delivery, err error) error {
	return mq.Discard(err)
}

// Fail is the DecodeErrorFunc that hands the error to the configured failure
// policy unchanged, so the delivery is nacked and redelivered with backoff
// until its attempts run out.
//
// It is right when the body may become decodable — a schema registry that
// has not caught up yet — and wrong otherwise, because nothing about the
// bytes changes between attempts.
func Fail(_ context.Context, _ *mq.Delivery, err error) error { return err }

// HandlerConfig configures a typed handler. The zero value is usable:
// ContentTypeIgnore and DeadLetter.
type HandlerConfig struct {
	// ContentType is how strictly a delivery's declared content type is held
	// to the codec's.
	ContentType ContentTypePolicy
	// OnDecodeError decides what becomes of a delivery that did not decode.
	// Nil selects DeadLetter.
	OnDecodeError DecodeErrorFunc
}

// typedHandler is the composed mq.Handler NewHandler returns. It is built
// once, at construction: the policy, the codec and the callback are read from
// fields and never re-derived per delivery.
type typedHandler[T any] struct {
	codec  codec.Typed[T]
	h      Handler[T]
	onFail DecodeErrorFunc
	policy ContentTypePolicy
	ctype  string
}

// NewHandler wraps a typed handler into the mq.Handler the runner calls.
//
// It fails on a nil codec or handler, or a content-type policy that names
// none: a handler that returned an error for every delivery instead would be
// a consumer that halts in production for a bug visible at construction.
func NewHandler[T any](c codec.Typed[T], h Handler[T], cfg HandlerConfig) (mq.Handler, error) {
	if c == nil {
		return nil, ErrNilCodec
	}
	if h == nil {
		return nil, ErrNilHandler
	}
	if !cfg.ContentType.Valid() {
		return nil, errContentTypePolicy
	}
	th := &typedHandler[T]{
		codec:  c,
		h:      h,
		onFail: cfg.OnDecodeError,
		policy: cfg.ContentType,
		ctype:  c.ContentType(),
	}
	if th.onFail == nil {
		th.onFail = DeadLetter
	}
	return th, nil
}

// Handle decodes the delivery and calls the typed handler.
func (t *typedHandler[T]) Handle(ctx context.Context, d *mq.Delivery) error {
	if err := t.checkContentType(d); err != nil {
		return t.onFail(ctx, d, err)
	}
	v, err := t.codec.Decode(d.Body)
	if err != nil {
		return t.onFail(ctx, d, err)
	}
	td := Delivery[T]{Delivery: d, Value: v}
	return t.h.Handle(ctx, &td)
}

// checkContentType applies the policy to the delivery's declared content
// type, which is its typed property when it has one and its content-type
// header otherwise: the specification maps the property to a native field on the backends
// that have one and to the header everywhere else, so both are the same
// thing arriving by different routes.
func (t *typedHandler[T]) checkContentType(d *mq.Delivery) error {
	if t.policy == ContentTypeIgnore {
		return nil
	}
	got := d.Properties.ContentType
	if got == "" {
		if v, ok := d.Headers.Get(codec.HeaderContentType); ok {
			got = string(v)
		}
	}
	if got == "" {
		if t.policy == ContentTypeRequire {
			return &codec.ContentTypeError{Want: t.ctype, Reason: "the delivery declares none"}
		}
		return nil
	}
	return codec.Match(t.ctype, got)
}

// Run runs fn over consumer c, decoding each delivery with cd.
//
// It is the spelling — typed.Run(ctx, consumer, codec.JSON[Order](), fn)
// — and it takes the default HandlerConfig. A caller who wants a content-type
// policy or a different decode-failure policy builds the handler with
// NewHandler and calls c.Run with it.
func Run[T any](
	ctx context.Context, c *mq.ConsumerHandle, cd codec.Typed[T],
	fn func(ctx context.Context, d *Delivery[T]) error, opts ...mq.RunOption,
) error {
	h, err := NewHandler(cd, HandlerFunc[T](fn), HandlerConfig{})
	if err != nil {
		return err
	}
	return c.Run(ctx, h, opts...)
}
