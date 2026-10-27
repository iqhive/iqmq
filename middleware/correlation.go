package middleware

import (
	"context"
	"encoding/hex"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Header names this package reads and writes. They are the portable,
// lower-case spellings of the reserved set.
const (
	// HeaderCorrelationID carries the correlation id when the backend has no
	// native property for it.
	HeaderCorrelationID = "correlation-id"
	// HeaderTraceParent carries the W3C trace context.
	HeaderTraceParent = "traceparent"
	// HeaderTraceState carries the W3C vendor trace state.
	HeaderTraceState = "tracestate"
)

// TraceParentLen is the length of a well-formed traceparent value:
// "00-" + 32 hex + "-" + 16 hex + "-" + 2 hex.
const TraceParentLen = 55

// FlagSampled is the W3C sampled flag.
const FlagSampled uint8 = 1

// ErrBadTraceParent (CodeInvalidArgument) reports a traceparent value that is
// not a W3C trace context.
var ErrBadTraceParent error = errcode.New(errcode.InvalidArgument, "iqmq/middleware: malformed traceparent")

// TraceContext is a parsed W3C trace context (the traceparent/tracestate).
//
// It is a value type with no allocation of its own: a TraceContext is 27
// bytes of identifiers plus the vendor state, which is a slice of the header
// the parser was given and is never copied.
//
// The zero value is not a trace: Valid reports false for it, and for any
// context whose trace id or span id is all zeros, which the W3C
// specification declares invalid.
type TraceContext struct {
	// Version is the trace-context version. Only 0 is defined; a higher
	// version is parsed on the "forward compatible" rule and reported here.
	Version uint8
	// TraceID is the 16-byte trace identifier.
	TraceID [16]byte
	// SpanID is the 8-byte identifier of the caller's span, which is the
	// parent of any span this process starts.
	SpanID [8]byte
	// Flags are the trace flags; FlagSampled is bit 0.
	Flags uint8
	// State is the raw tracestate value, uninterpreted. It is bounded by
	// MaxTraceStateLen: the specification makes broker-delivered metadata untrusted, and a
	// vendor list a broker hands over is exactly that.
	State string
}

// MaxTraceStateLen bounds the tracestate this package will carry. The W3C
// specification's own limit is 512 bytes and is not a MUST; the specification requires
// bounded metadata, so it is one here.
const MaxTraceStateLen = 512

// Valid reports whether t names a trace: a non-zero trace id and a non-zero
// span id, which is the W3C rule.
func (t TraceContext) Valid() bool {
	return t.TraceID != [16]byte{} && t.SpanID != [8]byte{}
}

// Sampled reports whether the sampled flag is set.
func (t TraceContext) Sampled() bool { return t.Flags&FlagSampled != 0 }

// String renders t as a traceparent header value. It returns the empty string
// for a context that is not Valid, so that an invalid trace is never
// propagated as though it were one.
func (t TraceContext) String() string {
	if !t.Valid() {
		return ""
	}
	var b [TraceParentLen]byte
	hex.Encode(b[0:2], []byte{t.Version})
	b[2] = '-'
	hex.Encode(b[3:35], t.TraceID[:])
	b[35] = '-'
	hex.Encode(b[36:52], t.SpanID[:])
	b[52] = '-'
	hex.Encode(b[53:55], []byte{t.Flags})
	return string(b[:])
}

// ParseTraceParent parses a W3C traceparent header value.
//
// It accepts exactly what the specification calls well formed: two lower-case
// hex digits of version, a dash, thirty-two of trace id, a dash, sixteen of
// span id, a dash, two of flags — and nothing else, no trailing fields. The
// forward-compatibility rule is honoured for the version only: a version
// above 0 whose first four fields are well formed is accepted and its version
// reported, and the trailing fields a future version may add are rejected
// rather than guessed at.
//
// Version 255 is invalid by the specification and is rejected. So is an
// all-zero trace id or span id.
//
// It never panics, allocates nothing, and retains nothing: FuzzTraceContext
// is the target 24 and holds it to all three.
func ParseTraceParent(s string) (TraceContext, error) {
	// Every refusal returns the ZERO value, never the half-filled one the
	// parse had reached: a caller that ignored the error would otherwise
	// propagate a trace whose flags, or whose span, came from nowhere.
	// FuzzTraceContext found exactly that on "...-0g", where the trace id and
	// span id had already been decoded before the flags were refused.
	var t TraceContext
	if len(s) != TraceParentLen {
		return TraceContext{}, ErrBadTraceParent
	}
	if s[2] != '-' || s[35] != '-' || s[52] != '-' {
		return TraceContext{}, ErrBadTraceParent
	}
	v, ok := hexByte(s[0], s[1])
	if !ok || v == 0xff {
		return TraceContext{}, ErrBadTraceParent
	}
	t.Version = v
	if !hexInto(t.TraceID[:], s[3:35]) {
		return TraceContext{}, ErrBadTraceParent
	}
	if !hexInto(t.SpanID[:], s[36:52]) {
		return TraceContext{}, ErrBadTraceParent
	}
	f, ok := hexByte(s[53], s[54])
	if !ok {
		return TraceContext{}, ErrBadTraceParent
	}
	t.Flags = f
	if !t.Valid() {
		return TraceContext{}, ErrBadTraceParent
	}
	return t, nil
}

// hexInto decodes src into dst and reports success. It is written out rather
// than delegated to encoding/hex so that upper-case input is rejected: the
// W3C specification requires lower case, and hex.Decode accepts both, which
// would make String(Parse(s)) differ from s for an input a broker handed over.
func hexInto(dst []byte, src string) bool {
	if len(src) != 2*len(dst) {
		return false
	}
	for i := range dst {
		b, ok := hexByte(src[2*i], src[2*i+1])
		if !ok {
			return false
		}
		dst[i] = b
	}
	return true
}

// hexByte decodes two lower-case hex digits.
func hexByte(hi, lo byte) (byte, bool) {
	h, ok := hexNibble(hi)
	if !ok {
		return 0, false
	}
	l, ok := hexNibble(lo)
	if !ok {
		return 0, false
	}
	return h<<4 | l, true
}

// hexNibble decodes one lower-case hex digit.
func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	default:
		return 0, false
	}
}

// ContextWithTrace returns a context carrying t.
func ContextWithTrace(ctx context.Context, t TraceContext) context.Context {
	return context.WithValue(ctx, keyTrace, t)
}

// TraceFromContext returns the trace context CorrelationID put in ctx.
func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	t, ok := ctx.Value(keyTrace).(TraceContext)
	return t, ok
}

// ContextWithCorrelationID returns a context carrying id.
func ContextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyCorrelation, id)
}

// CorrelationFromContext returns the correlation id CorrelationID put in ctx.
func CorrelationFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(keyCorrelation).(string)
	return id, ok
}

// CorrelationConfig configures CorrelationID. The zero value reads what
// arrived and generates nothing.
type CorrelationConfig struct {
	// Generate produces a correlation id for a delivery that carries none.
	// Nil leaves such a delivery without one, which is the honest answer: an
	// id this process invented correlates nothing with the sender.
	Generate IDFunc
	// Trace, when true, also parses the traceparent and tracestate headers
	// into the context (W3C propagation). A malformed traceparent is
	// ignored rather than failing the delivery: the specification makes broker-delivered
	// metadata untrusted, and a broken trace header is not a broken message.
	Trace bool
}

// CorrelationID puts the delivery's correlation id — and, with
// CorrelationConfig.Trace, its W3C trace context — into the handler's context
//
// The id is Properties.CorrelationID when the driver filled it and the
// correlation-id header otherwise, because the specification maps the typed property to a
// native field on the backends that have one and to the header everywhere
// else. A delivery that carries neither gets one from Generate, or none.
//
// Nothing is written back onto the message: a consumer-side middleware that
// mutated the delivery would be changing what a later middleware, a retry or
// a dead-letter copy sees. The id travels in the context, which is where a
// logger, a tracer and a reqreply responder look for it.
func CorrelationID(cfg CorrelationConfig) mq.Middleware {
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if id := correlationOf(d, cfg.Generate); id != "" {
				ctx = ContextWithCorrelationID(ctx, id)
			}
			if cfg.Trace {
				if t, ok := traceOf(d); ok {
					ctx = ContextWithTrace(ctx, t)
				}
			}
			return next.Handle(ctx, d)
		})
	}
}

// correlationOf reads the delivery's correlation id, generating one when it
// has none and gen is set.
func correlationOf(d *mq.Delivery, gen IDFunc) string {
	if id := d.Properties.CorrelationID; id != "" {
		return id
	}
	if v, ok := d.Headers.Get(HeaderCorrelationID); ok && len(v) > 0 {
		return string(v)
	}
	if gen != nil {
		return gen()
	}
	return ""
}

// traceOf parses the delivery's trace headers.
func traceOf(d *mq.Delivery) (TraceContext, bool) {
	v, ok := d.Headers.Get(HeaderTraceParent)
	if !ok {
		return TraceContext{}, false
	}
	t, err := ParseTraceParent(string(v))
	if err != nil {
		return TraceContext{}, false
	}
	if s, ok := d.Headers.Get(HeaderTraceState); ok {
		state := boundTraceState(string(s))
		// A tracestate is a comma-separated list; a value with a NUL or a
		// newline in it is not one, and forwarding it would let a broker
		// inject into whatever renders it.
		if strings.IndexFunc(state, func(r rune) bool { return r < 0x20 || r == 0x7f }) < 0 {
			t.State = state
		}
	}
	return t, true
}

// boundTraceState cuts a tracestate over MaxTraceStateLen at the last
// list-member boundary inside the bound, dropping whole members from the end
// as the W3C specification says to. A cut at the byte left half a member
// behind, and a member cut inside its value was handed on as that vendor's
// state with the wrong value.
func boundTraceState(state string) string {
	if len(state) <= MaxTraceStateLen {
		return state
	}
	cut := strings.LastIndexByte(state[:MaxTraceStateLen+1], ',')
	if cut < 0 {
		return ""
	}
	return strings.TrimRight(state[:cut], " \t")
}
