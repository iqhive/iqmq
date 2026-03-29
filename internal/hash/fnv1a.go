package hash

// FNV-1a parameters (Fowler–Noll–Vo, 64-bit).
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// fnv1a is 64-bit FNV-1a. The seed is xored into the offset basis, so seed 0
// is the standard function (hash/fnv New64a) and every other seed is FNV-1a
// with an alternative basis, which the algorithm permits.
func fnv1a[T bytesOrString](seed uint64, b T) uint64 {
	h := uint64(fnvOffset64) ^ seed
	for i := range len(b) {
		h ^= uint64(b[i])
		h *= fnvPrime64
	}
	return h
}
