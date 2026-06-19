package otel_test

import (
	"strings"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/middleware"
	"bitbucket.org/iqhive/iqmq/wire"

	otelmq "bitbucket.org/iqhive/iqmq/integrations/otel"
)

// FuzzTraceContext is the test suite's target 24 as this module owns it:
// "W3C parse never panics; valid contexts round trip; bounded metadata".
//
// middleware.FuzzTraceContext fuzzes the PARSER and the correlation
// middleware; this one fuzzes the CARRIAGE, which is the half that lives here
// and has failure modes of its own:
//
//   - Extract of a header a stranger wrote never panics and never produces an
//     invalid span context — an invalid one would be propagated onward as
//     though it named a trace.
//   - A context this package accepted survives Inject, the WIRE ENVELOPE and
//     Extract unchanged, which is the specification's "envelope when header-less" stated
//     as a property rather than as a comment.
//   - Whatever a broker put in `tracestate`, the span context this package
//     builds carries a bounded, grammatical vendor list or none at all.
//
// The seeds are middleware.TraceParentSeeds(), the same corpus
// middleware.FuzzTraceContext uses: one W3C implementation in the repository
// means one corpus.
func FuzzTraceContext(f *testing.F) {
	for _, s := range middleware.TraceParentSeeds() {
		f.Add(s.Parent, s.State)
	}

	layouts := wire.Kinds()

	f.Fuzz(func(t *testing.T, parent, state string) {
		var h mq.Headers
		h.Set(middleware.HeaderTraceParent, []byte(parent))
		h.Set(middleware.HeaderTraceState, []byte(state))

		sc := otelmq.Extract(&h)
		if !sc.IsValid() {
			// A refusal is the zero value, so nothing that is not a trace is
			// ever carried onward as one. trace.SpanContext is not
			// comparable (its TraceState holds a slice), so the zero is
			// checked field by field.
			zero := oteltrace.SpanContext{}
			if sc.TraceID() != zero.TraceID() || sc.SpanID() != zero.SpanID() ||
				sc.TraceFlags() != zero.TraceFlags() || sc.IsRemote() ||
				sc.TraceState().Len() != 0 {
				t.Fatalf("Extract(%q) refused and returned %+v", parent, sc)
			}
			return
		}
		if !sc.IsRemote() {
			t.Fatalf("Extract(%q) produced a local span context; a delivery's parent is another process's", parent)
		}
		if ts := sc.TraceState().String(); len(ts) > middleware.MaxTraceStateLen {
			t.Fatalf("tracestate is %d bytes, over the %d-byte bound the specification requires",
				len(ts), middleware.MaxTraceStateLen)
		}
		if ts := sc.TraceState().String(); strings.ContainsFunc(ts, func(r rune) bool {
			return r < 0x20 || r == 0x7f
		}) {
			t.Fatalf("tracestate %q carries a control character", ts)
		}

		// Round trip through a fresh header table: what was extracted
		// injects and extracts back to the same identifiers.
		var out mq.Headers
		otelmq.Inject(&out, sc)
		again := otelmq.Extract(&out)
		if again.TraceID() != sc.TraceID() || again.SpanID() != sc.SpanID() ||
			again.TraceFlags() != sc.TraceFlags() {
			t.Fatalf("round trip: %+v became %+v", sc, again)
		}

		// And through the portable envelope, which is how a broker with no
		// header field carries it (the `wire` package).
		for _, k := range layouts {
			m := &mq.Message{Topic: "orders", Body: []byte("body")}
			otelmq.Inject(&m.Headers, sc)
			buf, err := wire.New(k).Encode(nil, m)
			if err != nil {
				t.Fatalf("%v: encoding: %v", k, err)
			}
			var back mq.Message
			if err := wire.New(k).Decode(buf, &back, nil); err != nil {
				t.Fatalf("%v: decoding: %v", k, err)
			}
			through := otelmq.Extract(&back.Headers)
			if through.TraceID() != sc.TraceID() || through.SpanID() != sc.SpanID() ||
				through.TraceFlags() != sc.TraceFlags() {
				t.Fatalf("%v: the envelope lost the trace: %+v became %+v", k, sc, through)
			}
		}
	})
}
