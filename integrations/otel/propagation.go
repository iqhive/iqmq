package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/middleware"
)

// HeaderCarrier carries a W3C trace context in an iqmq message's headers.
//
// It implements [propagation.TextMapCarrier], so any OpenTelemetry
// propagator — including the baggage propagator the specification names — can read and
// write an iqmq message. The keys are the portable lower-case spellings
// [middleware.HeaderTraceParent], [middleware.HeaderTraceState] and
// "baggage".
//
// # The envelope, for a broker with no headers
//
// There is nothing here for it, and that is the design. A backend whose body
// is an opaque blob carries the whole header table in iqmq's own envelope
// (MetadataPortableEnvelope, the `wire` package), so a traceparent
// written into Headers travels inside the envelope on exactly the brokers
// that have no header field — NSQ, and SQS past its ten-attribute limit —
// and travels as a native header everywhere else. A second, trace-specific
// envelope would be a second encoding of the same bytes and a second thing
// to get wrong. TestTraceContextSurvivesTheEnvelope proves the claim through
// `wire` itself rather than asserting it in a comment.
type HeaderCarrier struct {
	// Headers is the message's header table. A carrier over a nil pointer
	// reads as empty and discards what is written to it, which is what makes
	// an extraction from a message that has no headers a miss rather than a
	// panic.
	Headers *mq.Headers
}

// Get returns the value of one header, or the empty string.
func (c HeaderCarrier) Get(key string) string {
	if c.Headers == nil {
		return ""
	}
	v, ok := c.Headers.Get(key)
	if !ok {
		return ""
	}
	return string(v)
}

// Set writes one header, replacing any value it had.
func (c HeaderCarrier) Set(key, value string) {
	if c.Headers == nil {
		return
	}
	c.Headers.Set(key, []byte(value))
}

// Keys returns the header names the carrier holds.
func (c HeaderCarrier) Keys() []string {
	if c.Headers == nil {
		return nil
	}
	keys := make([]string, 0, c.Headers.Len())
	for k := range c.Headers.All() {
		keys = append(keys, k)
	}
	return keys
}

// HeaderBaggage is the header the W3C baggage propagator uses.
const HeaderBaggage = "baggage"

// Inject writes ctx's span context into h as a W3C traceparent and
// tracestate.
//
// It renders through [middleware.TraceContext], which is W3C
// implementation in the root module — the one the correlation middleware and
// [middleware.FuzzTraceContext]'s corpus already hold to round-tripping. This
// package deliberately has no second W3C parser or renderer: two
// implementations of one wire format in one repository is how they come to
// disagree, and the one that would have been written here is the one a broker
// hands untrusted bytes to.
//
// A span context that is not valid writes nothing, so a message never carries
// a traceparent that names no trace.
func Inject(h *mq.Headers, sc trace.SpanContext) {
	if h == nil || !sc.IsValid() {
		return
	}
	t := middleware.TraceContext{
		TraceID: sc.TraceID(),
		SpanID:  sc.SpanID(),
		Flags:   byte(sc.TraceFlags()),
	}
	s := t.String()
	if s == "" {
		return
	}
	h.Set(middleware.HeaderTraceParent, []byte(s))
	if state := sc.TraceState().String(); state != "" && len(state) <= middleware.MaxTraceStateLen {
		h.Set(middleware.HeaderTraceState, []byte(state))
	}
}

// Extract reads a W3C trace context out of h.
//
// It returns the zero [trace.SpanContext] for a message that carries none and
// for one whose traceparent is malformed: the specification makes broker-delivered metadata
// untrusted, and a publisher must not be able to break a consumer by writing
// nonsense into a header. The parse is
// [middleware.ParseTraceParent]'s — bounded, allocation-free and fuzzed — and
// the extracted context is marked remote, which is what makes the span this
// process starts a child of another process's span rather than a
// continuation of its own.
//
// A tracestate the parser accepted is carried through; one the OpenTelemetry
// tracestate grammar rejects is dropped and the trace and span identifiers
// are kept, because losing the vendor list is better than losing the trace.
func Extract(h *mq.Headers) trace.SpanContext {
	if h == nil {
		return trace.SpanContext{}
	}
	v, ok := h.Get(middleware.HeaderTraceParent)
	if !ok {
		return trace.SpanContext{}
	}
	t, err := middleware.ParseTraceParent(string(v))
	if err != nil {
		return trace.SpanContext{}
	}
	cfg := trace.SpanContextConfig{
		TraceID:    t.TraceID,
		SpanID:     t.SpanID,
		TraceFlags: trace.TraceFlags(t.Flags),
		Remote:     true,
	}
	if raw, ok := h.Get(middleware.HeaderTraceState); ok {
		if ts, err := trace.ParseTraceState(boundedState(raw)); err == nil {
			cfg.TraceState = ts
		}
	}
	return trace.NewSpanContext(cfg)
}

// boundedState truncates a tracestate to the bound the specification put on it. The specification
// requires bounded metadata and a vendor list a broker handed over is exactly
// that; the W3C limit of 512 bytes is the bound.
func boundedState(raw []byte) string {
	if len(raw) > middleware.MaxTraceStateLen {
		raw = raw[:middleware.MaxTraceStateLen]
	}
	return string(raw)
}

// inject writes the propagation headers for one message: the configured
// propagator's, when there is one, and this package's W3C pair otherwise.
func (i *Instrumentation) inject(ctx context.Context, h *mq.Headers) {
	if h == nil {
		return
	}
	if i.propagator != nil {
		i.propagator.Inject(ctx, HeaderCarrier{Headers: h})
		return
	}
	Inject(h, trace.SpanContextFromContext(ctx))
}

// extract reads the propagation headers of one delivery into ctx.
func (i *Instrumentation) extract(ctx context.Context, h *mq.Headers) context.Context {
	if h == nil {
		return ctx
	}
	if i.propagator != nil {
		return i.propagator.Extract(ctx, HeaderCarrier{Headers: h})
	}
	if sc := Extract(h); sc.IsValid() {
		return trace.ContextWithRemoteSpanContext(ctx, sc)
	}
	return ctx
}

// W3CPropagator is the OpenTelemetry propagator pair a caller who wants
// baggage as well as trace context passes in [Config.Propagator]:
//
//	otelmq.Config{Propagator: otelmq.W3CPropagator()}
//
// It is upstream's TraceContext and Baggage propagators, not this package's,
// and setting it replaces [Inject] and [Extract] on every path — including
// the W3C trace context, which upstream then parses with its own
// implementation. That is the caller's choice and it is why it is not the
// default: the default path is the one middleware.FuzzTraceContext covers.
func W3CPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}
