package wire

import (
	"unicode/utf8"

	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// checkPayload reports whether Decode under lim would accept the envelope
// Encode produces for p.
//
// It lives beside the decoder and reads the decoder's own resolved limits
// rather than a second set of numbers, because the whole class of defect it
// answers is a second set of numbers that drifted: the specification found a 259-byte header
// key accepted at publish and undecodable for ever afterwards, then a 259-byte
// identifier against a fix that bounded only the headers, and the specification recorded
// streams.DefaultLimits.MaxHeaderKey = 259 against DefaultLimits.MaxHeaderKey =
// 256 as the same mistake one layer out.
//
// What keeps this function in step with Decode is not care: it is
// FuzzWireCheck, which requires Check(p) == nil exactly when
// Decode(Encode(p)) == nil for every layout and every payload the fuzzer can
// build. A rule added to one and not the other fails that target.
//
// The created-at field needs no check. Decode refuses nanoseconds at or above
// one second and refuses the zero instant, and neither is reachable from a
// time.Time: Nanosecond() is always below 1e9, and the encoder writes the zero
// instant through the flag's absence rather than as a field.
func checkPayload(p *payload.Payload, lim *Limits) error {
	r := resolve(lim)
	if len(p.Key) > r.key || len(p.Body) > r.body || len(p.ID) > r.id || len(p.Headers) > r.headers {
		return ErrLimit
	}
	if !validText(p.ID) {
		return ErrUTF8
	}
	for i := range p.Headers {
		h := &p.Headers[i]
		if len(h.Key) > r.hkey || len(h.Value) > r.hval {
			return ErrLimit
		}
		if !validText(h.Key) {
			return ErrUTF8
		}
	}
	pr := &p.Props
	for _, s := range [...]string{
		pr.ContentType, pr.ContentEncoding, pr.CorrelationID, pr.CausationID, pr.ReplyTo,
	} {
		if len(s) > r.prop {
			return ErrLimit
		}
		if !validText(s) {
			return ErrUTF8
		}
	}
	return nil
}

// validText is validString for a string field. The decoder's fields arrive as
// byte slices and the encoder's arrive as strings; the predicate is the same
// one, and FuzzWireCheck is what holds the two spellings together.
func validText(s string) bool { return utf8.ValidString(s) }

// Check reports whether Decode under lim would accept the envelope Encode
// produces for p, which for this layout is checkPayload plus the fixed widths
// Encode itself refuses.
func (*fixedLayout) Check(p *payload.Payload, lim *Limits) error {
	if !fixedFits(p) {
		return ErrUnencodable
	}
	return checkPayload(p, lim)
}

// Check reports whether Decode under lim would accept the envelope Encode
// produces for p. The varint layout has no widths of its own to overflow, so
// the limits are the whole answer.
func (*tlvLayout) Check(p *payload.Payload, lim *Limits) error {
	return checkPayload(p, lim)
}
