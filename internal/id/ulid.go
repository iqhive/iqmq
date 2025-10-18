package id

// ulidGen produces ULIDs.
//
// Layout (big-endian): 48 bits of Unix milliseconds, a 16-bit monotonic
// sequence and 64 random bits. The sequence occupies the most significant
// bits of the 80-bit randomness field, which is where the ULID
// specification puts a monotonic counter, so identifiers created within
// one millisecond still sort in creation order.
type ulidGen struct{ base }

func (*ulidGen) Kind() Kind { return ULID }

const ulidSeqBits = 16

func (g *ulidGen) NextValue() Value {
	ms, seq := g.advance(uint64(g.clk.Now().UnixMilli()), ulidSeqBits)
	tail := g.rng.next()
	var v Value
	v.kind = ULID
	b := &v.b
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = byte(seq >> 8)
	b[7] = byte(seq)
	b[8] = byte(tail >> 56)
	b[9] = byte(tail >> 48)
	b[10] = byte(tail >> 40)
	b[11] = byte(tail >> 32)
	b[12] = byte(tail >> 24)
	b[13] = byte(tail >> 16)
	b[14] = byte(tail >> 8)
	b[15] = byte(tail)
	return v
}

func (g *ulidGen) Append(dst []byte) []byte {
	v := g.NextValue()
	return appendULID(dst, &v.b)
}

func (g *ulidGen) Next() string {
	var buf [26]byte
	return string(g.Append(buf[:0]))
}

// crockford is Crockford's base32 alphabet: the digits and the upper-case
// letters without I, L, O and U.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// appendULID writes the 26-character upper-case Crockford base32 form.
// The first character encodes only two bits, so a ULID never starts above
// '7'.
func appendULID(dst []byte, b *[16]byte) []byte {
	// 128 bits = 25 full 5-bit groups plus 3 leading bits.
	hi := uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
	lo := uint64(b[8])<<56 | uint64(b[9])<<48 | uint64(b[10])<<40 | uint64(b[11])<<32 |
		uint64(b[12])<<24 | uint64(b[13])<<16 | uint64(b[14])<<8 | uint64(b[15])
	var buf [26]byte
	for i := 25; i >= 0; i-- {
		buf[i] = crockford[lo&0x1f]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return append(dst, buf[:]...)
}

// uncrockford maps a canonical Crockford character to its value, or 0xff.
// Only the canonical upper-case alphabet is accepted: the lower-case and
// the I/L/O aliases would give two spellings of one identifier.
func uncrockford(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'H':
		return c - 'A' + 10
	case c == 'J' || c == 'K':
		return c - 'J' + 18
	case c >= 'M' && c <= 'N':
		return c - 'M' + 20
	case c >= 'P' && c <= 'T':
		return c - 'P' + 22
	case c >= 'V' && c <= 'Z':
		return c - 'V' + 27
	default:
		return 0xff
	}
}

// parseULID accepts 26 canonical Crockford characters that do not overflow
// 128 bits (the first character encodes two bits, so it is at most '7').
func parseULID[T text](s T, out *[16]byte) bool {
	if len(s) != 26 {
		return false
	}
	var hi, lo uint64
	for i := range 26 {
		v := uncrockford(s[i])
		if v == 0xff {
			return false
		}
		if i == 0 && v > 7 {
			return false // would overflow 128 bits
		}
		hi = hi<<5 | lo>>59
		lo = lo<<5 | uint64(v)
	}
	for i := range 8 {
		out[i] = byte(hi >> (56 - 8*i))
		out[8+i] = byte(lo >> (56 - 8*i))
	}
	return true
}
