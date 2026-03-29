package hash

import "hash/maphash"

// maphashSeed is the runtime seed; it is random per process, which is why
// the Maphash kind is not Stable.
var maphashSeed = maphash.MakeSeed()

// maphashBytes hashes b with the runtime hash and folds the caller's seed in
// with a bijective mixer, so every seed is a different permutation of the
// same collision-free-as-maphash value.
func maphashBytes(seed uint64, b []byte) uint64 {
	return fmix64(maphash.Bytes(maphashSeed, b) ^ seed)
}

// maphashString is the string entry point of maphashBytes.
func maphashString(seed uint64, s string) uint64 {
	return fmix64(maphash.String(maphashSeed, s) ^ seed)
}

// fmix64 is MurmurHash3's 64-bit finaliser: a bijection with full avalanche.
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
