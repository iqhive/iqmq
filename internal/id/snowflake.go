package id

import "encoding/binary"

// snowflakeGen produces 64-bit Snowflake identifiers.
//
// Layout: one zero bit (so the value is a positive int64 wherever it is
// stored), 41 bits of milliseconds since Epoch, a 10-bit node number and a
// 12-bit sequence. The sequence is monotonic per generator; identifiers
// from two generators are distinct as long as their node numbers differ.
type snowflakeGen struct{ base }

func (*snowflakeGen) Kind() Kind { return Snowflake }

const (
	snowflakeSeqBits  = 12
	snowflakeNodeBits = 10
	snowflakeTimeBits = 41
)

func (g *snowflakeGen) NextValue() Value {
	ms, seq := g.advance(sinceEpoch(g.clk.Now().Sub(Epoch).Milliseconds(), snowflakeTimeBits), snowflakeSeqBits)
	raw := ms&(1<<snowflakeTimeBits-1)<<(snowflakeNodeBits+snowflakeSeqBits) |
		uint64(g.node)&(1<<snowflakeNodeBits-1)<<snowflakeSeqBits | seq
	var v Value
	v.kind = Snowflake
	binary.BigEndian.PutUint64(v.b[:8], raw)
	return v
}

func (g *snowflakeGen) Append(dst []byte) []byte {
	v := g.NextValue()
	return v.AppendText(dst)
}

func (g *snowflakeGen) Next() string {
	var buf [19]byte
	return string(g.Append(buf[:0]))
}

// parseSnowflake accepts decimal digits with no sign, no leading zero
// (except the single digit "0") and no value above math.MaxInt64.
func parseSnowflake[T text](s T, out *[16]byte) bool {
	if len(s) == 0 || len(s) > 19 {
		return false
	}
	if s[0] == '0' && len(s) > 1 {
		return false
	}
	var v uint64
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		v = v*10 + uint64(c-'0')
	}
	if v > 1<<63-1 {
		return false
	}
	binary.BigEndian.PutUint64(out[:8], v)
	return true
}
