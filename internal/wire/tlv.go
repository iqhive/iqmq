package wire

import (
	"math"

	"github.com/iqhive/iqmq/internal/payload"
)

// TLV layout (canonical form: fields sorted by ascending tag, headers in
// publication order):
//
//	"IQMQ" 0x01 flags  uvarint(metadata length)  uvarint(body length)
//	metadata: sequence of  uvarint(tag) uvarint(length) value
//	body
//
// Tags are even for critical fields (an unknown even tag is corrupt) and
// odd for optional fields a decoder may skip; version 1 defines no odd
// tags, so every skipped field comes from a later minor version. Singular
// tags may appear once; the header tag repeats. A header value is
// u8 flags, uvarint(key length), key, value.
const (
	tagKey             = 2
	tagID              = 4
	tagHeader          = 6
	tagContentType     = 8
	tagContentEncoding = 10
	tagCorrelationID   = 12
	tagCausationID     = 14
	tagReplyTo         = 16
	tagCreatedAt       = 18
	tagMax             = tagCreatedAt
)

type tlvLayout struct {
	intern interner
}

func (*tlvLayout) Kind() Kind { return TLV }

// fieldSize is the encoded size of one tagged field of n bytes.
func fieldSize(tag, n int) int {
	return uvarintLen(uint64(tag)) + uvarintLen(uint64(n)) + n
}

// tlvMetaSize is the size of the metadata section.
func tlvMetaSize(p *payload.Payload) int {
	n := 0
	if p.Key != nil {
		n += fieldSize(tagKey, len(p.Key))
	}
	if p.ID != "" {
		n += fieldSize(tagID, len(p.ID))
	}
	for i := range p.Headers {
		h := &p.Headers[i]
		inner := 1 + uvarintLen(uint64(len(h.Key))) + len(h.Key) + len(h.Value)
		n += fieldSize(tagHeader, inner)
	}
	pr := &p.Props
	if pr.ContentType != "" {
		n += fieldSize(tagContentType, len(pr.ContentType))
	}
	if pr.ContentEncoding != "" {
		n += fieldSize(tagContentEncoding, len(pr.ContentEncoding))
	}
	if pr.CorrelationID != "" {
		n += fieldSize(tagCorrelationID, len(pr.CorrelationID))
	}
	if pr.CausationID != "" {
		n += fieldSize(tagCausationID, len(pr.CausationID))
	}
	if pr.ReplyTo != "" {
		n += fieldSize(tagReplyTo, len(pr.ReplyTo))
	}
	if !pr.CreatedAt.IsZero() {
		n += fieldSize(tagCreatedAt, timestampLen)
	}
	return n
}

func (*tlvLayout) Size(p *payload.Payload) int {
	meta := tlvMetaSize(p)
	return prefixLen + uvarintLen(uint64(meta)) + uvarintLen(uint64(len(p.Body))) + meta + len(p.Body)
}

// appendUvarint appends the canonical LEB128 encoding of v.
func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func appendField(dst []byte, tag int, b []byte) []byte {
	dst = appendUvarint(dst, uint64(tag))
	dst = appendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func appendStringField(dst []byte, tag int, s string) []byte {
	dst = appendUvarint(dst, uint64(tag))
	dst = appendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func (l *tlvLayout) Encode(dst []byte, p *payload.Payload) ([]byte, error) {
	meta := tlvMetaSize(p)
	dst = grow(dst, l.Size(p))
	dst = appendPrefix(dst, verTLV, flagsOf(p))
	dst = appendUvarint(dst, uint64(meta))
	dst = appendUvarint(dst, uint64(len(p.Body)))
	if p.Key != nil {
		dst = appendField(dst, tagKey, p.Key)
	}
	if p.ID != "" {
		dst = appendStringField(dst, tagID, p.ID)
	}
	for i := range p.Headers {
		h := &p.Headers[i]
		inner := 1 + uvarintLen(uint64(len(h.Key))) + len(h.Key) + len(h.Value)
		dst = appendUvarint(dst, tagHeader)
		dst = appendUvarint(dst, uint64(inner))
		var hf byte
		if h.Value != nil {
			hf = hflagValue
		}
		dst = append(dst, hf)
		dst = appendUvarint(dst, uint64(len(h.Key)))
		dst = append(dst, h.Key...)
		dst = append(dst, h.Value...)
	}
	pr := &p.Props
	if pr.ContentType != "" {
		dst = appendStringField(dst, tagContentType, pr.ContentType)
	}
	if pr.ContentEncoding != "" {
		dst = appendStringField(dst, tagContentEncoding, pr.ContentEncoding)
	}
	if pr.CorrelationID != "" {
		dst = appendStringField(dst, tagCorrelationID, pr.CorrelationID)
	}
	if pr.CausationID != "" {
		dst = appendStringField(dst, tagCausationID, pr.CausationID)
	}
	if pr.ReplyTo != "" {
		dst = appendStringField(dst, tagReplyTo, pr.ReplyTo)
	}
	if !pr.CreatedAt.IsZero() {
		dst = appendUvarint(dst, tagCreatedAt)
		dst = appendUvarint(dst, timestampLen)
		dst = appendTimestamp(dst, pr.CreatedAt)
	}
	dst = append(dst, p.Body...)
	return dst, nil
}

// tlvScan is the result of the validation pass over the metadata.
type tlvScan struct {
	key, id, ct, ce, co, ca, rt []byte
	ts                          []byte
	seen                        uint32 // bit per tag/2
	nHeaders                    int
	skipped                     int
	lastTag                     uint64
	refs                        [inlineHeaders]headerRef
}

func (s *tlvScan) singular(tag int, val []byte) error {
	bit := uint32(1) << (tag / 2)
	if s.seen&bit != 0 {
		return ErrDuplicate
	}
	s.seen |= bit
	switch tag {
	case tagKey:
		s.key = val
	case tagID:
		s.id = val
	case tagContentType:
		s.ct = val
	case tagContentEncoding:
		s.ce = val
	case tagCorrelationID:
		s.co = val
	case tagCausationID:
		s.ca = val
	case tagReplyTo:
		s.rt = val
	case tagCreatedAt:
		s.ts = val
	}
	return nil
}

func (s *tlvScan) has(tag int) bool { return s.seen&(uint32(1)<<(tag/2)) != 0 }

// scan validates the metadata section and records field positions. base is
// meta's offset within the envelope, so recorded header positions are
// envelope-relative. It touches nothing outside meta and allocates nothing.
func scan(meta []byte, base int, flags byte, r *resolved, s *tlvScan) error {
	pos := 0
	for pos < len(meta) {
		var (
			tag uint64
			vl  int
		)
		// A one-byte tag followed by a one-byte length is every field a
		// v1 encoder writes whose value is shorter than 128 bytes, which
		// is every header name, every property and every key of a normal
		// message. Reading the pair here rather than through two calls
		// keeps the loop free of calls in that case; tags above 127 come
		// only from a later minor version, and longer values from an
		// unusually large field.
		if pos+1 < len(meta) && meta[pos] < 0x80 && meta[pos+1] < 0x80 {
			tag, vl = uint64(meta[pos]), int(meta[pos+1])
			pos += 2
			if vl > len(meta)-pos {
				return ErrTruncated
			}
		} else {
			t, tn := uvarint(meta[pos:])
			if tn <= 0 {
				if tn == 0 {
					return ErrTruncated
				}
				return ErrVarint
			}
			tag = t
			pos += tn
			v, vn := readLen(meta[pos:])
			if vn <= 0 {
				return lenError(vn)
			}
			vl = v
			pos += vn
		}
		valOff := pos
		val := meta[pos : pos+vl : pos+vl]
		pos += vl
		if tag < s.lastTag {
			return ErrFieldOrder
		}
		s.lastTag = tag
		if tag&1 == 1 {
			s.skipped++
			continue
		}
		if tag == 0 || tag > tagMax {
			return ErrUnknownField
		}
		switch tag {
		case tagHeader:
			// One header entry, validated in place: the key and value are
			// only ever described by offset and length, never sliced, so
			// this costs no slice headers and no call.
			if flags&flagHeaders == 0 {
				return ErrFlags
			}
			s.nHeaders++
			if s.nHeaders > r.headers {
				return ErrLimit
			}
			if vl < 1 {
				return ErrTruncated
			}
			if base+valOff+vl > math.MaxInt32 {
				// headerRef records positions as int32, so a header that
				// ends past 2 GiB, which only unbounded limits admit,
				// would be recorded at a negative offset.
				return ErrLimit
			}
			hf := val[0]
			if hf&^hflagKnown != 0 {
				return ErrFlags
			}
			var kl, koff int
			if vl > 1 && val[1] < 0x80 { // the one-byte key length, as above
				kl, koff = int(val[1]), 2
				if kl > vl-2 {
					return ErrTruncated
				}
			} else {
				k, kn := readLen(val[1:])
				if kn <= 0 {
					return lenError(kn)
				}
				kl, koff = k, 1+kn
			}
			hvl := vl - koff - kl // value length; the length read bounded kl
			if hf&hflagValue == 0 && hvl != 0 {
				return ErrFlags
			}
			if kl > r.hkey || hvl > r.hval {
				return ErrLimit
			}
			if !validString(val[koff : koff+kl : koff+kl]) {
				return ErrUTF8
			}
			if i := s.nHeaders - 1; i < inlineHeaders {
				s.refs[i] = headerRef{
					off:   int32(base + valOff + koff),
					klen:  int32(kl),
					vlen:  int32(hvl),
					flags: hf,
				}
			}
		case tagKey:
			if flags&flagKey == 0 {
				return ErrFlags
			}
			if vl > r.key {
				return ErrLimit
			}
			if err := s.singular(int(tag), val); err != nil {
				return err
			}
		case tagCreatedAt:
			if flags&flagCreatedAt == 0 {
				return ErrFlags
			}
			if err := s.singular(int(tag), val); err != nil {
				return err
			}
		default: // string fields
			if vl == 0 {
				return ErrFlags // empty strings are encoded by absence
			}
			max := r.prop
			if tag == tagID {
				max = r.id
			}
			if vl > max {
				return ErrLimit
			}
			if !validString(val) {
				return ErrUTF8
			}
			if err := s.singular(int(tag), val); err != nil {
				return err
			}
		}
	}
	if flags&flagKey != 0 && !s.has(tagKey) || flags&flagCreatedAt != 0 && !s.has(tagCreatedAt) {
		return ErrFlags
	}
	return nil
}

func (l *tlvLayout) Decode(src []byte, p *payload.Payload, lim *Limits) error {
	_, err := l.decode(src, p, lim)
	return err
}

// decode is Decode that also reports how many optional fields it skipped.
func (l *tlvLayout) decode(src []byte, p *payload.Payload, lim *Limits) (int, error) {
	flags, err := checkPrefix(src, verTLV)
	if err != nil {
		return 0, err
	}
	r := resolve(lim)
	pos := prefixLen
	metaLen, n := readLen(src[pos:])
	if n <= 0 {
		return 0, lenError(n)
	}
	pos += n
	bodyLen, n := readLenReserve(src[pos:], metaLen)
	if n <= 0 {
		return 0, lenError(n)
	}
	pos += n
	if flags&flagBody == 0 && bodyLen != 0 {
		return 0, ErrFlags
	}
	if bodyLen > r.body {
		return 0, ErrLimit
	}
	if pos+metaLen+bodyLen != len(src) {
		return 0, ErrTrailing
	}
	meta := src[pos : pos+metaLen : pos+metaLen]
	body := src[pos+metaLen:]

	var s tlvScan
	if err := scan(meta, pos, flags, &r, &s); err != nil {
		return 0, err
	}
	var created payloadTime
	if flags&flagCreatedAt != 0 {
		created, err = decodeTimestamp(s.ts)
		if err != nil {
			return 0, err
		}
	}

	// Commit.
	p.Key = cloneInto(p.Key, s.key, flags&flagKey != 0, r.zeroCopy)
	p.Body = cloneInto(p.Body, body, flags&flagBody != 0, r.zeroCopy)
	setID(p, s.id)
	pr := &p.Props
	pr.ContentType = l.intern.keep(pr.ContentType, s.ct)
	pr.ContentEncoding = l.intern.keep(pr.ContentEncoding, s.ce)
	pr.CorrelationID = l.intern.keep(pr.CorrelationID, s.co)
	pr.CausationID = l.intern.keep(pr.CausationID, s.ca)
	pr.ReplyTo = l.intern.keep(pr.ReplyTo, s.rt)
	pr.CreatedAt = created
	hs := headersFor(p, s.nHeaders, flags&flagHeaders != 0)
	// Beyond the inline window the header list is walked again -- ONCE, with
	// the cursor carried between iterations, which is what the fixed layout
	// does and what this loop used to get wrong: it asked headerAt for the
	// i-th header, and headerAt restarted from the first field of the
	// metadata section every time. That made the commit pass quadratic in the
	// header count, and the count is bounded only by the envelope's length --
	// a legal one-mebibyte TLV envelope carrying 262,144 empty headers took
	// three minutes to decode, and DetectFull, which removes every limit on
	// purpose, is the path a REFUSED envelope escalates to whatever the
	// consumer's own MaxHeaders is. The cursor re-checks nothing: scan
	// already accepted every field it walks over.
	//
	// It is created LAZILY, at the first index past the inline window, and
	// the first thing it does is skip the entries scan already remembered.
	// That keeps the common case -- a message with at most inlineHeaders
	// headers, which is the hot path the specification is about -- doing exactly what it
	// did before this fix: read s.refs[i] and nothing else. Advancing the
	// cursor through the inline window on every decode would have been a
	// per-header varint read added to the cheapest path in the package, paid
	// by every message to fix a defect only a long header list has.
	var cur *headerCursor
	for i := range hs {
		var ref headerRef
		if i < inlineHeaders {
			ref = s.refs[i]
		} else {
			if cur == nil {
				cur = &headerCursor{meta: meta, base: pos}
				for range inlineHeaders {
					cur.next()
				}
			}
			ref = cur.next()
		}
		ko, kn, vn := int(ref.off), int(ref.klen), int(ref.vlen)
		hs[i].Key = l.intern.keep(hs[i].Key, src[ko:ko+kn:ko+kn])
		hs[i].Value = cloneInto(hs[i].Value, src[ko+kn:ko+kn+vn:ko+kn+vn], ref.flags&hflagValue != 0, r.zeroCopy)
	}
	p.Headers = hs
	return s.skipped, nil
}

// headerCursor walks the header entries of an already validated metadata
// section once, in order.
//
// It replaced a headerAt(meta, base, i) that restarted the walk for every
// index, which is the whole difference between a commit pass that costs the
// envelope's length and one that costs its square. It re-checks nothing: scan
// already accepted every field it walks over, so next reads lengths it knows
// to be well formed and in range.
type headerCursor struct {
	// meta is the validated metadata section.
	meta []byte
	// base is meta's offset within the envelope, so that the reference next
	// returns points into the envelope rather than into the section.
	base int
	// pos is how far into meta the walk has got.
	pos int
}

// next returns the next header entry, or the zero headerRef once the section
// has no more. It is called once per header of the commit pass, including the
// entries scan remembered inline, so that one walk serves the whole list.
func (c *headerCursor) next() headerRef {
	for c.pos < len(c.meta) {
		tag, n := uvarint(c.meta[c.pos:])
		c.pos += n
		vl, n := readLen(c.meta[c.pos:])
		c.pos += n
		at := c.pos
		c.pos += vl
		if tag != tagHeader {
			continue
		}
		kl, kn := readLen(c.meta[at+1:])
		koff := 1 + kn
		return headerRef{
			off:   int32(c.base + at + koff),
			klen:  int32(kl),
			vlen:  int32(vl - koff - kl),
			flags: c.meta[at],
		}
	}
	return headerRef{}
}
