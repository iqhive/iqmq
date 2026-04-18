package middleware

// TraceParentSeed is one input of the W3C trace-context seed corpus: a
// `traceparent` header value and the `tracestate` beside it.
type TraceParentSeed struct {
	// Parent is a traceparent header value, well formed or not.
	Parent string
	// State is the tracestate header value that accompanies it.
	State string
}

// TraceParentSeeds is the seed corpus of the test suite's target 24,
// "W3C parse never panics; valid contexts round trip; bounded metadata".
//
// It is exported because the parser and the fuzz target assigns to it
// are in different MODULES. [ParseTraceParent] lives here, in the root
// module, so that the correlation middleware could use it without waiting for and so that there would be exactly one W3C implementation in the
// repository; the specification puts `FuzzTraceContext` in `integrations/otel`, which is
// a module of its own and can neither reach a _test.go file here nor read a
// testdata directory outside its own module tree. A corpus copied into both
// would be two corpora the day after somebody added a case to one, so the
// corpus is a function both call.
//
// Each seed is here because it is a case the parser has to get right, and
// several are regressions: the `-0g` case is the one that found a refusal
// returning a half-filled context, the upper-case case is why the hex decoder
// is written out rather than delegated to encoding/hex, and the last two are
// a broker injecting control characters into a vendor list.
//
// A fuzz target adds Go's own committed corpus under its
// testdata/fuzz/<target>/ on top of these, per module, which is Go's
// mechanism and not this one's.
func TraceParentSeeds() []TraceParentSeed {
	return []TraceParentSeed{
		// A well-formed sampled context, with and without a vendor list.
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "acme=1"},
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", ""},
		// A future version, which the forward-compatibility rule accepts.
		{"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "a=1,b=2"},
		// Version 255 is invalid by the specification.
		{"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ""},
		// An all-zero trace id and an all-zero span id name no trace.
		{"00-00000000000000000000000000000000-00f067aa0ba902b7-01", ""},
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", ""},
		// Upper-case hex, which the specification forbids and encoding/hex
		// would accept.
		{"00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01", ""},
		// Trailing fields, a truncated value and the wrong separators.
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", ""},
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", ""},
		{"00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01", ""},
		{"", ""},
		// A bad flags nibble: the case that found a refusal returning the
		// trace and span ids it had already decoded.
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g", ""},
		// A vendor list with control characters in it, which the specification makes
		// untrusted input.
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "acme=1\ninjected"},
		{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "\x00"},
	}
}
