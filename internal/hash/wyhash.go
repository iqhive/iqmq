// Derived from wyhash final version 4.3 by Wang Yi, under the Unlicense.
// See licenses/wyhash.UNLICENSE and THIRD_PARTY_NOTICES.md.

package hash

import "math/bits"

// The default secret of wyhash final version 4.3.
const (
	wyP0 = 0x2d358dccaa6c78a5
	wyP1 = 0x8bb84b93962eacc9
	wyP2 = 0x4b33a62ed433d4a3
	wyP3 = 0x4d5a2da51de1aa47
)

// wymix is wyhash's MUM: the xor of both halves of the 128-bit product.
func wymix(a, b uint64) uint64 { return mul128fold64(a, b) }

// wyhash is wyhash final version 4.3 (WYHASH_CONDOM=1, 64-bit MUM) with the
// default secret, ported from the upstream header statement by statement.
func wyhash[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	seed ^= wymix(seed^wyP0, wyP1)
	var a, c uint64
	switch {
	case n <= 16:
		switch {
		case n >= 4:
			a = load32(b, 0)<<32 | load32(b, (n>>3)<<2)
			c = load32(b, n-4)<<32 | load32(b, n-4-((n>>3)<<2))
		case n > 0:
			a = uint64(b[0])<<16 | uint64(b[n>>1])<<8 | uint64(b[n-1])
		}
	default:
		i, p := n, 0
		if i >= 48 {
			see1, see2 := seed, seed
			for i >= 48 {
				blk := b[p : p+48] // one bounds check per block
				seed = wymix(load64(blk, 0)^wyP1, load64(blk, 8)^seed)
				see1 = wymix(load64(blk, 16)^wyP2, load64(blk, 24)^see1)
				see2 = wymix(load64(blk, 32)^wyP3, load64(blk, 40)^see2)
				p += 48
				i -= 48
			}
			seed ^= see1 ^ see2
		}
		for i > 16 {
			blk := b[p : p+16]
			seed = wymix(load64(blk, 0)^wyP1, load64(blk, 8)^seed)
			i -= 16
			p += 16
		}
		tail := b[p+i-16 : p+i]
		a = load64(tail, 0)
		c = load64(tail, 8)
	}
	a ^= wyP1
	c ^= seed
	c, a = bits.Mul64(a, c) // _wymum: a = low half, c = high half
	return wymix(a^wyP0^uint64(n), c^wyP1)
}
