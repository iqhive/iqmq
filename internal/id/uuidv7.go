package id

// uuidGen produces RFC 9562 version 7 UUIDs.
//
// Layout (big-endian): 48 bits of Unix milliseconds, 4 bits of version,
// 12 bits of rand_a used as the monotonic sequence (RFC 9562 §6.2 method
// 2), 2 bits of variant and 62 random bits.
type uuidGen struct{ base }

func (*uuidGen) Kind() Kind { return UUIDv7 }

const uuidSeqBits = 12

func (g *uuidGen) NextValue() Value {
	ms, seq := g.advance(uint64(g.clk.Now().UnixMilli()), uuidSeqBits)
	tail := g.rng.next()
	var v Value
	v.kind = UUIDv7
	b := &v.b
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = 0x70 | byte(seq>>8)&0x0f
	b[7] = byte(seq)
	b[8] = 0x80 | byte(tail>>56)&0x3f
	b[9] = byte(tail >> 48)
	b[10] = byte(tail >> 40)
	b[11] = byte(tail >> 32)
	b[12] = byte(tail >> 24)
	b[13] = byte(tail >> 16)
	b[14] = byte(tail >> 8)
	b[15] = byte(tail)
	return v
}

func (g *uuidGen) Append(dst []byte) []byte {
	v := g.NextValue()
	return appendUUID(dst, &v.b)
}

func (g *uuidGen) Next() string {
	var buf [36]byte
	return string(g.Append(buf[:0]))
}

const hexdigits = "0123456789abcdef"

// appendUUID writes the canonical 8-4-4-4-12 lower-case form.
func appendUUID(dst []byte, b *[16]byte) []byte {
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			dst = append(dst, '-')
		}
		dst = append(dst, hexdigits[c>>4], hexdigits[c&0x0f])
	}
	return dst
}

// unhex maps a lower-case hex digit to its value, or 0xff.
func unhex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return 0xff
	}
}

// parseUUID accepts the canonical form only: 36 characters, lower-case
// hex, hyphens in place, version 7 and the RFC variant.
func parseUUID[T text](s T, out *[16]byte) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	j := 0
	for _, g := range [5][2]int{{0, 8}, {9, 13}, {14, 18}, {19, 23}, {24, 36}} {
		for i := g[0]; i < g[1]; i += 2 {
			hi, lo := unhex(s[i]), unhex(s[i+1])
			if hi == 0xff || lo == 0xff {
				return false
			}
			out[j] = hi<<4 | lo
			j++
		}
	}
	return out[6]>>4 == 7 && out[8]>>6 == 0b10
}
