package wire

import (
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/iqhive/iqmq/internal/payload"
)

// checkPayload reports whether Decode under the resolved limits r would
// accept the envelope Encode produces for p.
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
func checkPayload(p *payload.Payload, r *resolved) error {
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
// Encode itself refuses, plus the decoder's own bound on where the header
// list may end.
func (*fixedLayout) Check(p *payload.Payload, lim *Limits) error {
	if !fixedFits(p) {
		return ErrUnencodable
	}
	r := resolve(lim)
	if err := checkPayload(p, &r); err != nil {
		return err
	}
	// The decoders record a header's position as an int32 and refuse a
	// header list that ends past math.MaxInt32, which only unbounded or very
	// large limits let an envelope reach. Check used to pass such a payload,
	// so a publication it had passed became a record nothing could read back.
	// FuzzWireCheck cannot build a payload that large, so
	// TestCheckRefusesAHeaderListThatEndsPast2GiB holds the rule instead.
	// Here the header list ends where the body starts.
	if len(p.Headers) > 0 && !headersEndWithin(&r) && fixedSize(p)-len(p.Body) > math.MaxInt32 {
		return ErrLimit
	}
	return nil
}

// Check reports whether Decode under lim would accept the envelope Encode
// produces for p. The varint layout has no widths of its own to overflow, so
// the limits are the whole answer, with the decoder's own bound on where the
// header list may end.
func (*tlvLayout) Check(p *payload.Payload, lim *Limits) error {
	r := resolve(lim)
	if err := checkPayload(p, &r); err != nil {
		return err
	}
	if len(p.Headers) > 0 && !headersEndWithin(&r) && tlvHeadersEnd(p) > math.MaxInt32 {
		return ErrLimit
	}
	return nil
}

// headersEndWithin reports whether every payload within r has a header list
// that ends at or before math.MaxInt32 in either layout, so Check need not
// walk the headers to find out. It counts every field up to the end of the
// header list at its limit and every length, tag and flag at its longest. The
// default limits put the end under 68 MB, so the walk, which made Check a
// quarter slower, is only taken under limits that let a list reach 2 GiB.
func headersEndWithin(r *resolved) bool {
	const most = math.MaxInt32
	if r.key > most || r.id > most || r.prop > most || r.headers > most || r.hkey > most || r.hval > most {
		return false
	}
	// A TLV header is a tag, a length, a flags byte and a key length before
	// its key and value; the fixed layout's seven-byte entry is shorter, and
	// so is its 42-byte header beside the TLV lengths and tags counted here.
	perHeader := uint64(3*binary.MaxVarintLen64+1) + uint64(r.hkey) + uint64(r.hval)
	end := uint64(prefixLen+6*binary.MaxVarintLen64) + uint64(r.key) + uint64(r.id) + 5*uint64(r.prop) +
		uint64(r.headers)*perHeader
	return end <= most
}

// tlvHeadersEnd is the offset in the envelope at which the last header field
// ends: the properties and the body follow it.
func tlvHeadersEnd(p *payload.Payload) int {
	lead := tlvLeadSize(p)
	meta := lead + tlvPropsSize(p)
	return prefixLen + uvarintLen(uint64(meta)) + uvarintLen(uint64(len(p.Body))) + lead
}
