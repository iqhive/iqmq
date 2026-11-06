package wire

import (
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/iqhive/iqmq/internal/payload"
)

// Check's rules are the decoder's, read from the decoder's own resolved
// limits rather than a second set of numbers, because the whole class of
// defect it answers is a second set of numbers that drifted: the
// specification found a 259-byte header key accepted at publish and
// undecodable for ever afterwards, then a 259-byte identifier against a fix
// that bounded only the headers, and the specification recorded
// streams.DefaultLimits.MaxHeaderKey = 259 against DefaultLimits.MaxHeaderKey
// = 256 as the same mistake one layer out.
//
// Each layout's Check walks the rules in the order its decoder meets them and
// returns the first one the payload breaks, so a payload that breaks two
// answers with the error Decode would have returned: ErrLimit
// (CodeMessageTooLarge) or ErrUTF8 (CodeCorruptMessage). One order for both
// layouts answered ErrLimit where the decoder answered ErrUTF8, or the other
// way round.
//
// What keeps Check in step with Decode is not care: it is FuzzWireCheck,
// which requires Check(p) to be the error Decode(Encode(p)) returns, nil
// included, for every layout and every payload the fuzzer can build. A rule
// added to one and not the other, or met in another order, fails that target.
//
// The created-at field needs no check. Decode refuses nanoseconds at or above
// one second and refuses the zero instant, and neither is reachable from a
// time.Time: Nanosecond() is always below 1e9, and the encoder writes the zero
// instant through the flag's absence rather than as a field.

// validText is validString for a string field. The decoder's fields arrive as
// byte slices and the encoder's arrive as strings; the predicate is the same
// one, and FuzzWireCheck is what holds the two spellings together.
func validText(s string) bool { return utf8.ValidString(s) }

// checkHeader is one header's rules in the order both decoders apply them:
// its bounds, then its key's UTF-8.
func checkHeader(h *payload.KV, r *resolved) error {
	if len(h.Key) > r.hkey || len(h.Value) > r.hval {
		return ErrLimit
	}
	if !validText(h.Key) {
		return ErrUTF8
	}
	return nil
}

// Check reports whether Decode under lim would accept the envelope Encode
// produces for p, which for this layout is the fixed widths Encode itself
// refuses, then the decoder's rules in the decoder's order: every length
// bound, then the UTF-8 of the identifier and the properties, then the
// decoder's own bound on where the header list may end, then each header.
func (*fixedLayout) Check(p *payload.Payload, lim *Limits) error {
	if !fixedFits(p) {
		return ErrUnencodable
	}
	r := resolve(lim)
	pr := &p.Props
	// Written out field by field: two loops over an array of the five
	// properties made this, the default layout's Check, about a tenth slower.
	if len(p.Key) > r.key || len(p.Body) > r.body || len(p.ID) > r.id || len(p.Headers) > r.headers ||
		len(pr.ContentType) > r.prop || len(pr.ContentEncoding) > r.prop || len(pr.CorrelationID) > r.prop ||
		len(pr.CausationID) > r.prop || len(pr.ReplyTo) > r.prop {
		return ErrLimit
	}
	if !validText(p.ID) || !validText(pr.ContentType) || !validText(pr.ContentEncoding) ||
		!validText(pr.CorrelationID) || !validText(pr.CausationID) || !validText(pr.ReplyTo) {
		return ErrUTF8
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
	for i := range p.Headers {
		if err := checkHeader(&p.Headers[i], &r); err != nil {
			return err
		}
	}
	return nil
}

// Check reports whether Decode under lim would accept the envelope Encode
// produces for p. The varint layout has no widths of its own to overflow, so
// the limits are the whole answer, in the decoder's order: the body's bound,
// then each field in tag order with its bound before its UTF-8. That is the
// key, the identifier, each header (whether it is one too many, then where it
// ends, against the decoder's own bound, then its own rules) and the
// properties.
func (*tlvLayout) Check(p *payload.Payload, lim *Limits) error {
	r := resolve(lim)
	if len(p.Body) > r.body || len(p.Key) > r.key || len(p.ID) > r.id {
		return ErrLimit
	}
	if !validText(p.ID) {
		return ErrUTF8
	}
	// The default limits rule out a header list past 2 GiB, and the walk
	// that finds where each header ends made Check a quarter slower, so it
	// is taken only under limits that let a list get there.
	if len(p.Headers) > 0 && !headersEndWithin(&r) {
		if err := tlvCheckHeadersWalking(p, &r); err != nil {
			return err
		}
	} else {
		// The decoder refuses the header one past the count only once it
		// has read the ones before it, so those are checked first.
		hs := p.Headers
		if len(hs) > r.headers {
			hs = hs[:r.headers]
		}
		for i := range hs {
			h := &hs[i]
			if len(h.Key) > r.hkey || len(h.Value) > r.hval {
				return ErrLimit
			}
			if !validText(h.Key) {
				return ErrUTF8
			}
		}
		if len(p.Headers) > r.headers {
			return ErrLimit
		}
	}
	pr := &p.Props
	for _, s := range [...]string{pr.ContentType, pr.ContentEncoding, pr.CorrelationID, pr.CausationID, pr.ReplyTo} {
		if len(s) > r.prop {
			return ErrLimit
		}
		if !validText(s) {
			return ErrUTF8
		}
	}
	return nil
}

// tlvCheckHeadersWalking is the TLV header rules under limits that let a
// header list end past 2 GiB, in the decoder's order for each header: whether
// it is one too many, then where it ends, then its bounds and its key.
func tlvCheckHeadersWalking(p *payload.Payload, r *resolved) error {
	end := tlvHeadersStart(p)
	for i := range p.Headers {
		h := &p.Headers[i]
		if i >= r.headers {
			return ErrLimit
		}
		end += fieldSize(tagHeader, 1+uvarintLen(uint64(len(h.Key)))+len(h.Key)+len(h.Value))
		if end > math.MaxInt32 {
			return ErrLimit
		}
		if err := checkHeader(h, r); err != nil {
			return err
		}
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

// tlvHeadersStart is the offset in the envelope at which the first header
// field starts: the key and the identifier come before it.
func tlvHeadersStart(p *payload.Payload) int {
	meta := tlvMetaSize(p)
	n := prefixLen + uvarintLen(uint64(meta)) + uvarintLen(uint64(len(p.Body)))
	if p.Key != nil {
		n += fieldSize(tagKey, len(p.Key))
	}
	if p.ID != "" {
		n += fieldSize(tagID, len(p.ID))
	}
	return n
}
