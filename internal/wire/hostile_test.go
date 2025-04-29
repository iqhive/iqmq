package wire

import (
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// hostileBudget is how long one hostile envelope may take to decode.
//
// It is deliberately enormous -- five seconds for a one-megabyte body a linear
// decoder finishes in single-digit milliseconds -- because the defect it
// separates from is three orders of magnitude the other side of it. A budget
// with a five-hundredfold margin cannot be made to fail by the load on a
// shared host, and what it measures is not a performance figure: it is the
// difference between a decoder whose cost is bounded by its input and one
// whose cost is the square of it.
const hostileBudget = 5 * time.Second

// hostileEnvelope returns a canonical envelope of layout k carrying n headers
// with an empty key and no value -- the cheapest header the layout can encode,
// and therefore the most a body of a given size can hold.
//
// Nothing here is malformed: Encode produced it, so every length, the field
// order and the canonical form are the decoder's own. The point is the SHAPE,
// which is what a producer on the other side of a broker controls for free.
func hostileEnvelope(t *testing.T, k Kind, n int) []byte {
	t.Helper()
	p := &payload.Payload{Headers: make([]payload.KV, n)}
	out, err := New(k).Encode(nil, p)
	if err != nil {
		t.Fatalf("%v: Encode of %d headers: %v", k, n, err)
	}
	return out
}

// within runs f and reports how long it took, giving up on waiting for it once
// the budget is spent.
//
// The wait has to be bounded rather than simply timed, because the failure this
// test exists for took THREE MINUTES for one megabyte and grows with the square
// of the header count: a test that waited for it would report the package's
// own timeout panic minutes later instead of the budget it broke, and a CI run
// would lose every other test in the package with it.
//
// The abandoned goroutine keeps running to completion. That is acceptable
// exactly once -- the test has already failed by the time it happens -- and it
// is why over is reported separately rather than by returning a longer
// duration.
func within(budget time.Duration, f func()) (d time.Duration, over bool) {
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		f()
		done <- time.Since(start)
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case d = <-done:
		return d, false
	case <-timer.C:
		return budget, true
	}
}

// TestDecodeCostIsBoundedByTheEnvelopeSize pins this package's own security
// claim -- "a hostile envelope can cost at most its own size" (the package
// documentation, the security checks) -- against a legal envelope whose header
// count is as large as its length allows.
//
// It is not an allocation claim, which the two-pass decoder honours: scan
// counts the headers and checks the count against the limit before headersFor
// allocates one. It is a TIME claim, and the commit pass broke it for the TLV
// layout: the fixed layout carries its cursor forward between entries, and TLV
// restarted the walk from the first field of the metadata section for every
// header index, which is quadratic in the header count. Measured before the
// fix, with the default limits raised to accept the envelope:
//
//	tlv: decoding 262144 headers in a 1048586-byte envelope took 3m1.036116522s
//
// and DetectFull on the same bytes had not finished when the package's own
// four-minute timeout fired, in wire.headerAt.
//
// The three measurements are the three ways an attacker reaches the decoder:
// the refusal a consumer's limit produces, the decode a consumer that raised
// the limit performs, and DetectFull -- which removes every limit on purpose,
// so it is the path a REFUSED envelope escalates to whatever the consumer
// configured.
func TestDecodeCostIsBoundedByTheEnvelopeSize(t *testing.T) {
	// One mebibyte of envelope, which is under every shipped driver's own
	// message ceiling. The header count is what that buys at the layout's
	// minimum entry size, so it is derived rather than chosen.
	const target = 1 << 20
	for _, k := range []Kind{Fixed, TLV} {
		perHeader := 4
		if k == Fixed {
			perHeader = 7
		}
		n := target / perHeader
		if k == Fixed && n > 65535 {
			// The fixed layout's header count is a uint16, so 65535 is the
			// worst case that layout has.
			n = 65535
		}
		env := hostileEnvelope(t, k, n)

		// A consumer at the default limits refuses this envelope, and the
		// refusal must be cheap: that is the case the limit exists for.
		var p payload.Payload
		refuse, over := within(hostileBudget, func() {
			if err := New(k).Decode(env, &p, nil); err == nil {
				t.Errorf("%v: Decode at DefaultLimits accepted %d headers (MaxHeaders is %d)",
					k, n, DefaultLimits.MaxHeaders)
			}
		})
		if over {
			t.Errorf("%v: refusing %d headers in a %d-byte envelope took over the %v budget",
				k, n, len(env), hostileBudget)
		}

		// A consumer that raised the limit accepts it, and accepting must
		// also be bounded by the envelope's size.
		lim := DefaultLimits
		lim.MaxHeaders = n
		accept, over := within(hostileBudget, func() {
			if err := New(k).Decode(env, &p, &lim); err != nil {
				t.Errorf("%v: Decode of %d headers under a raised limit: %v", k, n, err)
			}
		})
		if over {
			t.Errorf("%v: decoding %d headers in a %d-byte envelope took over the %v budget",
				k, n, len(env), hostileBudget)
		}

		probe, over := within(hostileBudget, func() {
			if gotK, ok := DetectFull(env); !ok || gotK != k {
				t.Errorf("%v: DetectFull refused a whole envelope (%v, %v)", k, gotK, ok)
			}
		})
		if over {
			t.Errorf("%v: DetectFull on %d headers in a %d-byte envelope took over the %v budget",
				k, n, len(env), hostileBudget)
		}
		t.Logf("%v: %d headers in %d bytes: refuse=%v accept=%v probe=%v",
			k, n, len(env), refuse, accept, probe)
	}
}
