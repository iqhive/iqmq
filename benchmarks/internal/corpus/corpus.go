// Package corpus generates deterministic payloads, headers and keys for
// benchmarks so that every layer measures the same inputs.
package corpus

import (
	"fmt"
	"math/rand/v2"
	"sync"
)

// Sizes are the standard payload sizes of specification.
var Sizes = []int{0, 64, 1024, 64 * 1024, 1 << 20}

// SizeName returns the conventional benchmark segment for a size.
func SizeName(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1024:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// HeaderCounts are the standard header counts of specification.
var HeaderCounts = []int{0, 2, 8, 32, 128}

// KV is a header pair.
type KV struct {
	Key   string
	Value []byte
}

var (
	mu     sync.Mutex
	bodies = map[int][]byte{}
)

// Body returns a deterministic pseudo-random body of n bytes. The result is
// shared and must not be mutated.
func Body(n int) []byte {
	mu.Lock()
	defer mu.Unlock()
	if b, ok := bodies[n]; ok {
		return b
	}
	r := rand.New(rand.NewPCG(uint64(n), 0x1234))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + r.IntN(26))
	}
	bodies[n] = b
	return b
}

// Headers returns n headers named h0..h(n-1) with 8-byte values. Every
// fourth value is binary.
func Headers(n int) []KV {
	out := make([]KV, n)
	for i := range out {
		v := []byte(fmt.Sprintf("%08d", i))
		if i%4 == 3 {
			v = []byte{0, 1, 2, byte(i), 0xff, 0xfe, 0xfd, 0xfc}
		}
		out[i] = KV{Key: fmt.Sprintf("h%d", i), Value: v}
	}
	return out
}

// Keys returns n distinct keys of the given size.
func Keys(n, size int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		k := make([]byte, size)
		s := fmt.Sprintf("key-%d", i)
		copy(k, s)
		for j := len(s); j < size; j++ {
			k[j] = byte('a' + (i+j)%26)
		}
		out[i] = k
	}
	return out
}

// ZipfKeys returns count key indices drawn from a Zipf distribution over n
// keys with exponent s (s > 1 for skew), deterministically.
func ZipfKeys(n int, s float64, count int) []int {
	r := rand.New(rand.NewPCG(uint64(n), uint64(count)))
	z := rand.NewZipf(r, s, 1, uint64(n-1))
	out := make([]int, count)
	for i := range out {
		out[i] = int(z.Uint64())
	}
	return out
}
