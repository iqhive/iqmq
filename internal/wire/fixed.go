package wire

import (
	"encoding/binary"

	"github.com/iqhive/iqmq/internal/payload"
)

// Fixed layout:
//
//	 0  "IQMQ"
//	 4  layout/version = 0x11
//	 5  flags
//	 6  u16 header count
//	 8  u32 key length
//	12  u32 id length
//	16  u16 content-type length
//	18  u16 content-encoding length
//	20  u16 correlation-id length
//	22  u16 causation-id length
//	24  u16 reply-to length
//	26  i64 created-at seconds
//	34  u32 created-at nanoseconds
//	38  u32 body length
//	42  key, id, content-type, content-encoding, correlation-id,
//	    causation-id, reply-to, headers, body
//
// Each header is u8 flags, u16 key length, u32 value length, key, value.
// Absent fields have length zero; the flags byte tells nil from empty.
const (
	fixedHeaderLen = 42
	fixedEntryLen  = 7
	maxU16         = 1<<16 - 1
	maxU32         = 1<<32 - 1
)

type fixedLayout struct {
	intern interner
}

func (*fixedLayout) Kind() Kind { return Fixed }

// fixedFits reports whether every field fits its width.
func fixedFits(p *payload.Payload) bool {
	if len(p.Headers) > maxU16 || len(p.Key) > maxU32 || len(p.Body) > maxU32 || len(p.ID) > maxU32 {
		return false
	}
	pr := &p.Props
	if len(pr.ContentType) > maxU16 || len(pr.ContentEncoding) > maxU16 || len(pr.CorrelationID) > maxU16 ||
		len(pr.CausationID) > maxU16 || len(pr.ReplyTo) > maxU16 {
		return false
	}
	for i := range p.Headers {
		if len(p.Headers[i].Key) > maxU16 || len(p.Headers[i].Value) > maxU32 {
			return false
		}
	}
	return true
}

func fixedSize(p *payload.Payload) int {
	n := fixedHeaderLen + len(p.Key) + len(p.ID) + len(p.Body)
	pr := &p.Props
	n += len(pr.ContentType) + len(pr.ContentEncoding) + len(pr.CorrelationID) + len(pr.CausationID) + len(pr.ReplyTo)
	for i := range p.Headers {
		n += fixedEntryLen + len(p.Headers[i].Key) + len(p.Headers[i].Value)
	}
	return n
}

func (*fixedLayout) Size(p *payload.Payload) int {
	if !fixedFits(p) {
		return 0
	}
	return fixedSize(p)
}

func (*fixedLayout) Encode(dst []byte, p *payload.Payload) ([]byte, error) {
	if !fixedFits(p) {
		return dst, ErrUnencodable
	}
	dst = grow(dst, fixedSize(p))
	pr := &p.Props
	dst = appendPrefix(dst, verFixed, flagsOf(p))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(p.Headers)))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(p.Key)))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(p.ID)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pr.ContentType)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pr.ContentEncoding)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pr.CorrelationID)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pr.CausationID)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pr.ReplyTo)))
	if pr.CreatedAt.IsZero() {
		dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	} else {
		dst = appendTimestamp(dst, pr.CreatedAt)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(p.Body)))
	dst = append(dst, p.Key...)
	dst = append(dst, p.ID...)
	dst = append(dst, pr.ContentType...)
	dst = append(dst, pr.ContentEncoding...)
	dst = append(dst, pr.CorrelationID...)
	dst = append(dst, pr.CausationID...)
	dst = append(dst, pr.ReplyTo...)
	for i := range p.Headers {
		h := &p.Headers[i]
		var hf byte
		if h.Value != nil {
			hf = hflagValue
		}
		dst = append(dst, hf)
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(h.Key)))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(h.Value)))
		dst = append(dst, h.Key...)
		dst = append(dst, h.Value...)
	}
	dst = append(dst, p.Body...)
	return dst, nil
}

func (l *fixedLayout) Decode(src []byte, p *payload.Payload, lim *Limits) error {
	flags, err := checkPrefix(src, verFixed)
	if err != nil {
		return err
	}
	if len(src) < fixedHeaderLen {
		return ErrTruncated
	}
	r := resolve(lim)
	nHeaders := int(binary.LittleEndian.Uint16(src[6:]))
	keyLen := int(binary.LittleEndian.Uint32(src[8:]))
	idLen := int(binary.LittleEndian.Uint32(src[12:]))
	ctLen := int(binary.LittleEndian.Uint16(src[16:]))
	ceLen := int(binary.LittleEndian.Uint16(src[18:]))
	coLen := int(binary.LittleEndian.Uint16(src[20:]))
	caLen := int(binary.LittleEndian.Uint16(src[22:]))
	rtLen := int(binary.LittleEndian.Uint16(src[24:]))
	ts := src[26:38]
	bodyLen := int(binary.LittleEndian.Uint32(src[38:]))

	// Presence consistency before anything is read.
	if flags&flagKey == 0 && keyLen != 0 || flags&flagBody == 0 && bodyLen != 0 || flags&flagHeaders == 0 && nHeaders != 0 {
		return ErrFlags
	}
	if flags&flagCreatedAt == 0 {
		for _, b := range ts {
			if b != 0 {
				return ErrFlags
			}
		}
	}
	// Fixed-size part must fit before limits are consulted, so a hostile
	// length costs nothing.
	rest := int64(len(src) - fixedHeaderLen)
	fixedPart := int64(keyLen) + int64(idLen) + int64(ctLen) + int64(ceLen) + int64(coLen) +
		int64(caLen) + int64(rtLen) + int64(nHeaders)*fixedEntryLen + int64(bodyLen)
	if fixedPart > rest {
		return ErrTruncated
	}
	if keyLen > r.key || bodyLen > r.body || idLen > r.id || nHeaders > r.headers ||
		ctLen > r.prop || ceLen > r.prop || coLen > r.prop || caLen > r.prop || rtLen > r.prop {
		return ErrLimit
	}

	pos := fixedHeaderLen
	key := src[pos : pos+keyLen : pos+keyLen]
	pos += keyLen
	id := src[pos : pos+idLen : pos+idLen]
	pos += idLen
	ct := src[pos : pos+ctLen : pos+ctLen]
	pos += ctLen
	ce := src[pos : pos+ceLen : pos+ceLen]
	pos += ceLen
	co := src[pos : pos+coLen : pos+coLen]
	pos += coLen
	ca := src[pos : pos+caLen : pos+caLen]
	pos += caLen
	rt := src[pos : pos+rtLen : pos+rtLen]
	pos += rtLen
	if !validString(id) || !validString(ct) || !validString(ce) || !validString(co) || !validString(ca) || !validString(rt) {
		return ErrUTF8
	}

	// Headers: validate every entry before touching p, so a failure leaves
	// no half-written slice and nothing is allocated for a bad envelope.
	// The first inlineHeaders positions are remembered, so the commit pass
	// below re-parses only an unusually long header list.
	var refs [inlineHeaders]headerRef
	hdrStart := pos
	end := len(src) - bodyLen
	for i := range nHeaders {
		if end-pos < fixedEntryLen {
			return ErrTruncated
		}
		hf := src[pos]
		kl := int(binary.LittleEndian.Uint16(src[pos+1:]))
		vl := int(binary.LittleEndian.Uint32(src[pos+3:]))
		pos += fixedEntryLen
		if hf&^hflagKnown != 0 || hf&hflagValue == 0 && vl != 0 {
			return ErrFlags
		}
		if int64(kl)+int64(vl) > int64(end-pos) {
			return ErrTruncated
		}
		if kl > r.hkey || vl > r.hval {
			return ErrLimit
		}
		if !validString(src[pos : pos+kl : pos+kl]) {
			return ErrUTF8
		}
		if i < inlineHeaders {
			refs[i] = headerRef{off: int32(pos), klen: int32(kl), vlen: int32(vl), flags: hf}
		}
		pos += kl + vl
	}
	if pos != end {
		return ErrTrailing
	}
	body := src[end:len(src):len(src)]

	var created payloadTime
	if flags&flagCreatedAt != 0 {
		t, err := decodeTimestamp(ts)
		if err != nil {
			return err
		}
		created = t
	}

	// Commit.
	p.Key = cloneInto(p.Key, key, flags&flagKey != 0, r.zeroCopy)
	p.Body = cloneInto(p.Body, body, flags&flagBody != 0, r.zeroCopy)
	setID(p, id)
	pr := &p.Props
	pr.ContentType = l.intern.keep(pr.ContentType, ct)
	pr.ContentEncoding = l.intern.keep(pr.ContentEncoding, ce)
	pr.CorrelationID = l.intern.keep(pr.CorrelationID, co)
	pr.CausationID = l.intern.keep(pr.CausationID, ca)
	pr.ReplyTo = l.intern.keep(pr.ReplyTo, rt)
	pr.CreatedAt = created
	hs := headersFor(p, nHeaders, flags&flagHeaders != 0)
	pos = hdrStart
	for i := range hs {
		var ref headerRef
		if i < inlineHeaders {
			ref = refs[i]
		} else {
			// Beyond the inline window the entry is re-read from the
			// position the previous iteration left; the whole list was
			// validated above, so no check is repeated.
			ref = headerRef{
				off:   int32(pos + fixedEntryLen),
				klen:  int32(binary.LittleEndian.Uint16(src[pos+1:])),
				vlen:  int32(binary.LittleEndian.Uint32(src[pos+3:])),
				flags: src[pos],
			}
		}
		pos = int(ref.off) + int(ref.klen) + int(ref.vlen)
		ko, kn, vn := int(ref.off), int(ref.klen), int(ref.vlen)
		hs[i].Key = l.intern.keep(hs[i].Key, src[ko:ko+kn:ko+kn])
		hs[i].Value = cloneInto(hs[i].Value, src[ko+kn:ko+kn+vn:ko+kn+vn], ref.flags&hflagValue != 0, r.zeroCopy)
	}
	p.Headers = hs
	return nil
}
