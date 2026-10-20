// Package hash holds the key→lane hash functions every candidate for turning a message key into
// a stable 64-bit value that the engine reduces to a lane index with Lane,
// plus the Kafka-compatible Murmur2 partitioner that the Kafka driver needs
// whatever the bake-off decides.
//
// Every kind is a pure function of (seed, input) and safe for concurrent use.
// The byte and string entry points of one kind return identical values for
// identical content, so callers never convert between the two on a hot path.
// Kinds differ in output width (Kind.Bits) and in whether the value is
// reproducible across processes (Kind.Stable); Lane copes with both.
package hash

import (
	"errors"
	"math/bits"
)

// Kind identifies a hash function. Auto resolves to AutoKind.
type Kind uint8

// Kinds. FNV1a is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// FNV1a is 64-bit FNV-1a with the seed folded into the offset basis; at
	// seed 0 it equals hash/fnv's New64a. It is the reference.
	FNV1a
	// Wyhash is wyhash final version 4.3 with the default secret.
	Wyhash
	// XXH3 is XXH3_64bits_withSeed of xxHash 0.8.3 (scalar).
	XXH3
	// Maphash is hash/maphash over a process-random runtime seed, with the
	// caller's seed folded into the result by a 64-bit mixer. Its values are
	// not reproducible across processes (Stable reports false).
	Maphash
	// Murmur2 is 32-bit MurmurHash2 as Kafka computes it: seeded with
	// KafkaSeed it reproduces Kafka's Utils.murmur2 (Bits reports 32).
	Murmur2

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "fnv1a", "wyhash", "xxh3", "maphash", "murmur2"}

var kindIdents = [kindCount]string{"Auto", "FNV1a", "Wyhash", "XXH3", "Maphash", "Murmur2"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := FNV1a; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Bits reports the output width: every value is below 1<<Bits(). Lane
// tolerates narrow outputs, so a 32-bit kind still spreads over lanes.
func (k Kind) Bits() int {
	if k.Resolve() == Murmur2 {
		return 32
	}
	return 64
}

// Stable reports whether the kind returns the same value for the same
// (seed, input) in every process. Unstable kinds are fine for in-process
// lanes and unusable for anything persisted or shared between processes.
func (k Kind) Stable() bool { return k.Resolve() != Maphash }

// Func hashes b with seed.
type Func func(seed uint64, b []byte) uint64

// StringFunc hashes s with seed without converting it to a byte slice.
type StringFunc func(seed uint64, s string) uint64

// Hash is one kind's pair of entry points, resolved once by New so hot
// paths pay a single indirect call instead of a switch per key.
type Hash struct {
	// Kind is the concrete kind (never Auto).
	Kind Kind
	// Bytes hashes a byte slice.
	Bytes Func
	// String hashes a string; it agrees with Bytes on identical content.
	String StringFunc
}

// New resolves k and returns its entry points, or fails on an invalid kind.
func New(k Kind) (Hash, error) {
	k = k.Resolve()
	switch k {
	case FNV1a:
		return Hash{Kind: k, Bytes: fnv1a[[]byte], String: fnv1a[string]}, nil
	case Wyhash:
		return Hash{Kind: k, Bytes: wyhash[[]byte], String: wyhash[string]}, nil
	case XXH3:
		return Hash{Kind: k, Bytes: xxh3[[]byte], String: xxh3[string]}, nil
	case Maphash:
		return Hash{Kind: k, Bytes: maphashBytes, String: maphashString}, nil
	case Murmur2:
		return Hash{Kind: k, Bytes: murmur2[[]byte], String: murmur2[string]}, nil
	default:
		return Hash{}, errors.New("hash: invalid kind")
	}
}

// Bytes hashes b with kind k and seed. It dispatches on k for every call;
// hot paths bind the kind once with New. It fails on an invalid kind.
func Bytes(k Kind, seed uint64, b []byte) (uint64, error) {
	switch k.Resolve() {
	case FNV1a:
		return fnv1a(seed, b), nil
	case Wyhash:
		return wyhash(seed, b), nil
	case XXH3:
		return xxh3(seed, b), nil
	case Maphash:
		return maphashBytes(seed, b), nil
	case Murmur2:
		return murmur2(seed, b), nil
	default:
		return 0, errors.New("hash: invalid kind")
	}
}

// String hashes s with kind k and seed; it agrees with Bytes on identical
// content and never copies s. It fails on an invalid kind.
func String(k Kind, seed uint64, s string) (uint64, error) {
	switch k.Resolve() {
	case FNV1a:
		return fnv1a(seed, s), nil
	case Wyhash:
		return wyhash(seed, s), nil
	case XXH3:
		return xxh3(seed, s), nil
	case Maphash:
		return maphashString(seed, s), nil
	case Murmur2:
		return murmur2(seed, s), nil
	default:
		return 0, errors.New("hash: invalid kind")
	}
}

// laneMul is an odd constant (2^64/φ). Multiplying by it moves entropy from
// the low bits of h into the high bits, so a 32-bit output or FNV-1a's
// weak low bits still spread over lanes.
const laneMul = 0x9E3779B97F4A7C15

// Lane maps h to a lane index in [0, n) without dividing: it multiplies h
// by laneMul and takes the high word of the 128-bit product with n
// (Lemire's fast range reduction). It returns 0 for n <= 1. Lane is not
// Kafka's partitioner; see KafkaPartition.
func Lane(h uint64, n int) int {
	if n <= 1 {
		return 0
	}
	hi, _ := bits.Mul64(h*laneMul, uint64(n))
	return int(hi)
}

// bytesOrString is the type set shared by the byte and string entry points;
// one generic body serves both without converting between them.
type bytesOrString interface{ ~[]byte | ~string }

// load64 reads the little-endian uint64 at b[i:i+8]. The index hint costs
// one bounds check (against len, which a caller's reslice makes constant;
// a reslice here would check against cap, which never is) and the compiler
// fuses the eight loads into one.
func load64[T bytesOrString](b T, i int) uint64 {
	_ = b[i+7]
	return uint64(b[i]) | uint64(b[i+1])<<8 | uint64(b[i+2])<<16 | uint64(b[i+3])<<24 |
		uint64(b[i+4])<<32 | uint64(b[i+5])<<40 | uint64(b[i+6])<<48 | uint64(b[i+7])<<56
}

// load32 reads the little-endian uint32 at b[i:i+4], widened.
func load32[T bytesOrString](b T, i int) uint64 {
	_ = b[i+3]
	return uint64(b[i]) | uint64(b[i+1])<<8 | uint64(b[i+2])<<16 | uint64(b[i+3])<<24
}

// mul128fold64 returns the xor of both halves of the 128-bit product a*b.
func mul128fold64(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return lo ^ hi
}
