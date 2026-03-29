/*
 * xxHash - Extremely Fast Hash algorithm
 * Header File
 * Copyright (C) 2012-2023 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

package hash

import "math/bits"

// xxHash primes and XXH3 mixing constants (xxHash 0.8.3).
const (
	xxhPrime32_1 = 0x9E3779B1
	xxhPrime32_2 = 0x85EBCA77
	xxhPrime32_3 = 0xC2B2AE3D
	xxhPrime64_1 = 0x9E3779B185EBCA87
	xxhPrime64_2 = 0xC2B2AE3D27D4EB4F
	xxhPrime64_3 = 0x165667B19E3779F9
	xxhPrime64_4 = 0x85EBCA77C2B2AE63
	xxhPrime64_5 = 0x27D4EB2F165667C5
	xxhPrimeMX1  = 0x165667919E3779F9
	xxhPrimeMX2  = 0x9FB21C651E98DF25
)

// XXH3 layout constants.
const (
	xxh3SecretSize        = 192 // XXH_SECRET_DEFAULT_SIZE
	xxh3SecretSizeMin     = 136 // XXH3_SECRET_SIZE_MIN
	xxh3MidsizeStartOff   = 3   // XXH3_MIDSIZE_STARTOFFSET
	xxh3MidsizeLastOff    = 17  // XXH3_MIDSIZE_LASTOFFSET
	xxh3StripeLen         = 64  // XXH_STRIPE_LEN
	xxh3SecretConsumeRate = 8   // XXH_SECRET_CONSUME_RATE
	xxh3StripesPerBlock   = (xxh3SecretSize - xxh3StripeLen) / xxh3SecretConsumeRate
	xxh3BlockLen          = xxh3StripeLen * xxh3StripesPerBlock
	xxh3LastAccStart      = 7  // XXH_SECRET_LASTACC_START
	xxh3MergeAccsStart    = 11 // XXH_SECRET_MERGEACCS_START
)

// xxh3Secret is XXH3_kSecret. It is a string constant so that reads at
// constant offsets fold at compile time and the same generic loaders serve
// input and secret alike.
const xxh3Secret = "\xb8\xfe\x6c\x39\x23\xa4\x4b\xbe\x7c\x01\x81\x2c\xf7\x21\xad\x1c" +
	"\xde\xd4\x6d\xe9\x83\x90\x97\xdb\x72\x40\xa4\xa4\xb7\xb3\x67\x1f" +
	"\xcb\x79\xe6\x4e\xcc\xc0\xe5\x78\x82\x5a\xd0\x7d\xcc\xff\x72\x21" +
	"\xb8\x08\x46\x74\xf7\x43\x24\x8e\xe0\x35\x90\xe6\x81\x3a\x26\x4c" +
	"\x3c\x28\x52\xbb\x91\xc3\x00\xcb\x88\xd0\x65\x8b\x1b\x53\x2e\xa3" +
	"\x71\x64\x48\x97\xa2\x0d\xf9\x4e\x38\x19\xef\x46\xa9\xde\xac\xd8" +
	"\xa8\xfa\x76\x3f\xe3\x9c\x34\x3f\xf9\xdc\xbb\xc7\xc7\x0b\x4f\x1d" +
	"\x8a\x51\xe0\x4b\xcd\xb4\x59\x31\xc8\x9f\x7e\xc9\xd9\x78\x73\x64" +
	"\xea\xc5\xac\x83\x34\xd3\xeb\xc3\xc5\x81\xa0\xff\xfa\x13\x63\xeb" +
	"\x17\x0d\xdd\x51\xb7\xf0\xda\x49\xd3\x16\x55\x26\x29\xd4\x68\x9e" +
	"\x2b\x16\xbe\x58\x7d\x47\xa1\xfc\x8f\xf8\xb8\xd1\x7a\xd0\x31\xce" +
	"\x45\xcb\x3a\x8f\x95\x16\x04\x28\xaf\xd7\xfb\xca\xbb\x4b\x40\x7e"

// xxh3 is XXH3_64bits_withSeed, ported from the scalar code paths of
// xxHash 0.8.3 statement by statement.
func xxh3[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	switch {
	case n <= 16:
		return xxh3Len0to16(seed, b)
	case n <= 128:
		return xxh3Len17to128(seed, b)
	case n <= 240:
		return xxh3Len129to240(seed, b)
	case seed == 0:
		return xxh3Long(b, xxh3Secret)
	default:
		var custom [xxh3SecretSize]byte
		xxh3InitCustomSecret(&custom, seed)
		return xxh3Long(b, custom[:])
	}
}

func xxh64Avalanche(h uint64) uint64 {
	h ^= h >> 33
	h *= xxhPrime64_2
	h ^= h >> 29
	h *= xxhPrime64_3
	h ^= h >> 32
	return h
}

func xxh3Avalanche(h uint64) uint64 {
	h ^= h >> 37
	h *= xxhPrimeMX1
	h ^= h >> 32
	return h
}

func xxh3Rrmxmx(h, n uint64) uint64 {
	h ^= bits.RotateLeft64(h, 49) ^ bits.RotateLeft64(h, 24)
	h *= xxhPrimeMX2
	h ^= (h >> 35) + n
	h *= xxhPrimeMX2
	return h ^ (h >> 28)
}

func xxh3Len0to16[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	switch {
	case n > 8:
		bitflip1 := (load64(xxh3Secret, 24) ^ load64(xxh3Secret, 32)) + seed
		bitflip2 := (load64(xxh3Secret, 40) ^ load64(xxh3Secret, 48)) - seed
		lo := load64(b, 0) ^ bitflip1
		hi := load64(b, n-8) ^ bitflip2
		acc := uint64(n) + bits.ReverseBytes64(lo) + hi + mul128fold64(lo, hi)
		return xxh3Avalanche(acc)
	case n >= 4:
		seed ^= uint64(bits.ReverseBytes32(uint32(seed))) << 32
		in1 := load32(b, 0)
		in2 := load32(b, n-4)
		bitflip := (load64(xxh3Secret, 8) ^ load64(xxh3Secret, 16)) - seed
		return xxh3Rrmxmx((in2+in1<<32)^bitflip, uint64(n))
	case n > 0:
		c1, c2, c3 := uint32(b[0]), uint32(b[n>>1]), uint32(b[n-1])
		combined := c1<<16 | c2<<24 | c3 | uint32(n)<<8
		bitflip := (load32(xxh3Secret, 0) ^ load32(xxh3Secret, 4)) + seed
		return xxh64Avalanche(uint64(combined) ^ bitflip)
	default:
		return xxh64Avalanche(seed ^ (load64(xxh3Secret, 56) ^ load64(xxh3Secret, 64)))
	}
}

// xxh3Mix16B mixes the 16 input bytes at b[i:] with the 16 secret bytes at
// offset s.
func xxh3Mix16B[T bytesOrString](b T, i, s int, seed uint64) uint64 {
	in := b[i : i+16] // one bounds check; the offsets below are constants
	sec := xxh3Secret[s : s+16]
	return mul128fold64(load64(in, 0)^(load64(sec, 0)+seed), load64(in, 8)^(load64(sec, 8)-seed))
}

func xxh3Len17to128[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	acc := uint64(n) * xxhPrime64_1
	if n > 32 {
		if n > 64 {
			if n > 96 {
				acc += xxh3Mix16B(b, 48, 96, seed)
				acc += xxh3Mix16B(b, n-64, 112, seed)
			}
			acc += xxh3Mix16B(b, 32, 64, seed)
			acc += xxh3Mix16B(b, n-48, 80, seed)
		}
		acc += xxh3Mix16B(b, 16, 32, seed)
		acc += xxh3Mix16B(b, n-32, 48, seed)
	}
	acc += xxh3Mix16B(b, 0, 0, seed)
	acc += xxh3Mix16B(b, n-16, 16, seed)
	return xxh3Avalanche(acc)
}

func xxh3Len129to240[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	acc := uint64(n) * xxhPrime64_1
	rounds := n / 16
	for i := range 8 {
		acc += xxh3Mix16B(b, 16*i, 16*i, seed)
	}
	accEnd := xxh3Mix16B(b, n-16, xxh3SecretSizeMin-xxh3MidsizeLastOff, seed)
	acc = xxh3Avalanche(acc)
	for i := 8; i < rounds; i++ {
		accEnd += xxh3Mix16B(b, 16*i, 16*(i-8)+xxh3MidsizeStartOff, seed)
	}
	return xxh3Avalanche(acc + accEnd)
}

// xxh3InitCustomSecret derives the per-seed secret used for long inputs.
func xxh3InitCustomSecret(dst *[xxh3SecretSize]byte, seed uint64) {
	for i := 0; i < xxh3SecretSize; i += 16 {
		store64(dst[i:i+8], load64(xxh3Secret, i)+seed)
		store64(dst[i+8:i+16], load64(xxh3Secret, i+8)-seed)
	}
}

// store64 writes v little-endian into b[0:8].
func store64(b []byte, v uint64) {
	_ = b[7]
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	b[4], b[5], b[6], b[7] = byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56)
}

// xxh3Accumulate512 is one scalar stripe: 64 input bytes at b[i:] against
// 64 secret bytes at secret[s:].
func xxh3Accumulate512[T, S bytesOrString](acc *[8]uint64, b T, i int, secret S, s int) {
	in := b[i : i+64] // one bounds check per stripe; lanes use constant offsets
	sec := secret[s : s+64]
	xxh3Round(acc, 0, load64(in, 0), load64(sec, 0))
	xxh3Round(acc, 1, load64(in, 8), load64(sec, 8))
	xxh3Round(acc, 2, load64(in, 16), load64(sec, 16))
	xxh3Round(acc, 3, load64(in, 24), load64(sec, 24))
	xxh3Round(acc, 4, load64(in, 32), load64(sec, 32))
	xxh3Round(acc, 5, load64(in, 40), load64(sec, 40))
	xxh3Round(acc, 6, load64(in, 48), load64(sec, 48))
	xxh3Round(acc, 7, load64(in, 56), load64(sec, 56))
}

// xxh3Round is XXH3_scalarRound: one lane of a stripe.
func xxh3Round(acc *[8]uint64, lane int, dataVal, key uint64) {
	dataKey := dataVal ^ key
	acc[lane^1] += dataVal // swap adjacent lanes
	acc[lane] += uint64(uint32(dataKey)) * (dataKey >> 32)
}

// xxh3ScrambleAcc mixes the accumulators after every full block.
func xxh3ScrambleAcc[S bytesOrString](acc *[8]uint64, secret S, s int) {
	sec := secret[s : s+64]
	for lane := range 8 {
		a := acc[lane]
		a ^= a >> 47
		a ^= load64(sec, 8*lane)
		a *= xxhPrime32_1
		acc[lane] = a
	}
}

// xxh3Long is XXH3_hashLong_64b_internal for inputs above 240 bytes.
func xxh3Long[T, S bytesOrString](b T, secret S) uint64 {
	n := len(b)
	acc := [8]uint64{xxhPrime32_3, xxhPrime64_1, xxhPrime64_2, xxhPrime64_3, xxhPrime64_4, xxhPrime32_2, xxhPrime64_5, xxhPrime32_1}
	blocks := (n - 1) / xxh3BlockLen
	for blk := range blocks {
		base := blk * xxh3BlockLen
		for s := range xxh3StripesPerBlock {
			xxh3Accumulate512(&acc, b, base+s*xxh3StripeLen, secret, s*xxh3SecretConsumeRate)
		}
		xxh3ScrambleAcc(&acc, secret, xxh3SecretSize-xxh3StripeLen)
	}
	base := blocks * xxh3BlockLen
	stripes := ((n - 1) - base) / xxh3StripeLen
	for s := range stripes {
		xxh3Accumulate512(&acc, b, base+s*xxh3StripeLen, secret, s*xxh3SecretConsumeRate)
	}
	xxh3Accumulate512(&acc, b, n-xxh3StripeLen, secret, xxh3SecretSize-xxh3StripeLen-xxh3LastAccStart)
	result := uint64(n) * xxhPrime64_1
	sec := secret[xxh3MergeAccsStart : xxh3MergeAccsStart+64]
	for i := range 4 {
		result += mul128fold64(acc[2*i]^load64(sec, 16*i), acc[2*i+1]^load64(sec, 16*i+8))
	}
	return xxh3Avalanche(result)
}
