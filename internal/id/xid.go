package id

import "encoding/binary"

// xidGen produces identifiers in the layout of rs/xid.
//
// Layout (big-endian): 32 bits of Unix seconds, five identity bytes and a
// 24-bit counter. Upstream fills the identity bytes with a hash of the
// host name and the process id; iqmq derives them from Config.Seed and
// Config.Node instead, because an identifier that travels with a message
// must not describe the machine that produced it.
//
// The counter is monotonic per generator: an overflow borrows the next
// second rather than repeating, so identifiers never collide or go
// backwards even at more than 16.7 million per second.
type xidGen struct {
	base
	identity [5]byte
}

func (*xidGen) Kind() Kind { return XID }

const (
	xidSeqBits  = 24
	xidTimeBits = 32
)

func (g *xidGen) NextValue() Value {
	sec, seq := g.advance(sinceEpoch(g.clk.Now().Unix(), xidTimeBits), xidSeqBits)
	var v Value
	v.kind = XID
	b := &v.b
	binary.BigEndian.PutUint32(b[0:4], uint32(sec))
	copy(b[4:9], g.identity[:])
	b[9] = byte(seq >> 16)
	b[10] = byte(seq >> 8)
	b[11] = byte(seq)
	return v
}

func (g *xidGen) Append(dst []byte) []byte {
	v := g.NextValue()
	return appendXID(dst, &v.b)
}

func (g *xidGen) Next() string {
	var buf [20]byte
	return string(g.Append(buf[:0]))
}

// base32hex is the lower-case extended-hex alphabet rs/xid renders with.
const base32hex = "0123456789abcdefghijklmnopqrstuv"

// appendXID writes the 20-character lower-case base32hex form. Twelve
// bytes are 96 bits and twenty characters hold 100, so the last character
// carries one bit of data in its top position and four zero bits of
// padding: a canonical XID always ends in '0' or 'g'.
func appendXID(dst []byte, b *[16]byte) []byte {
	var buf [20]byte
	var acc uint64
	bits := uint(0)
	j := 0
	for i := range 12 {
		acc = acc<<8 | uint64(b[i])
		bits += 8
		for bits >= 5 {
			bits -= 5
			buf[j] = base32hex[acc>>bits&0x1f]
			j++
		}
	}
	buf[j] = base32hex[acc<<(5-bits)&0x1f]
	return append(dst, buf[:]...)
}

// unbase32hex maps a lower-case base32hex character to its value, or 0xff.
func unbase32hex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'v':
		return c - 'a' + 10
	default:
		return 0xff
	}
}

// parseXID accepts 20 canonical characters whose final character carries
// no padding bits (a rendering with padding bits set has no binary form
// and would not round trip).
func parseXID[T text](s T, out *[16]byte) bool {
	if len(s) != 20 {
		return false
	}
	var acc uint64
	bits := uint(0)
	j := 0
	for i := range 20 {
		v := unbase32hex(s[i])
		if v == 0xff {
			return false
		}
		if i == 19 && v&0x0f != 0 {
			return false // padding bits set: not a canonical rendering
		}
		acc = acc<<5 | uint64(v)
		bits += 5
		for bits >= 8 {
			bits -= 8
			out[j] = byte(acc >> bits)
			j++
		}
	}
	return j == 12
}
