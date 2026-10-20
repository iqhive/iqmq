package hash

import (
	"hash/fnv"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// fuzzState is the op-script state: the bound kind and the current seed.
type fuzzState struct {
	h    Hash
	seed uint64
}

// oracle returns an independent implementation of k when one exists:
// hash/fnv for FNV-1a at seed 0 and the MurmurHash2 port for Murmur2 at
// 32-bit seeds.
// Wyhash, XXH3 and Maphash have no simpler reference; their known answers
// live in TestModel/<kind>/kat and the fuzzer checks their invariants.
func oracle(k Kind, seed uint64, key []byte) (uint64, bool) {
	switch k {
	case FNV1a:
		if seed != 0 {
			return 0, false
		}
		std := fnv.New64a()
		_, _ = std.Write(key)
		return std.Sum64(), true
	case Murmur2:
		if seed>>32 != 0 {
			return 0, false
		}
		return uint64(murmur2Ref(uint32(seed), key)), true
	default:
		return 0, false
	}
}

// fuzzScript is shared by FuzzHash and its seed corpus.
var fuzzScript = fuzzutil.Script[*fuzzState]{
	Steps: []fuzzutil.Step[*fuzzState]{
		{Name: "kind", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			s.h = mustNew(t, Kinds()[r.Choice(len(Kinds()))])
		}},
		{Name: "seed", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			s.seed = r.Uint64()
		}},
		{Name: "hash", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			key := r.Bytes(2600)
			k := s.h.Kind
			x := s.h.Bytes(s.seed, key)
			if y := s.h.String(s.seed, string(key)); y != x {
				t.Fatalf("%v len %d seed %#x: Bytes %#x != String %#x", k, len(key), s.seed, x, y)
			}
			if z := s.h.Bytes(s.seed, append([]byte(nil), key...)); z != x {
				t.Fatalf("%v len %d: not deterministic (%#x vs %#x)", k, len(key), x, z)
			}
			if mustBytes(t, k, s.seed, key) != x || mustString(t, k, s.seed, string(key)) != x {
				t.Fatalf("%v: package-level dispatch differs", k)
			}
			if k.Bits() < 64 && x>>k.Bits() != 0 {
				t.Fatalf("%v: %#x exceeds %d bits", k, x, k.Bits())
			}
			if want, ok := oracle(k, s.seed, key); ok && want != x {
				t.Fatalf("%v len %d seed %#x: %#x, oracle %#x", k, len(key), s.seed, x, want)
			}
		}},
		{Name: "lane", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			n := r.Int(-2, 1<<20)
			key := r.Bytes(64)
			h := s.h.Bytes(s.seed, key)
			l := Lane(h, n)
			switch {
			case n <= 1 && l != 0:
				t.Fatalf("Lane(%#x, %d) = %d", h, n, l)
			case n > 1 && (l < 0 || l >= n):
				t.Fatalf("Lane(%#x, %d) = %d out of range", h, n, l)
			case Lane(h, n) != l:
				t.Fatal("Lane not deterministic")
			}
		}},
		{Name: "kafka", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
			key := r.Bytes(300)
			n := r.Int(-1, 4096)
			got := Murmur2Kafka(key)
			if want := murmur2Ref(uint32(KafkaSeed), key); got != want {
				t.Fatalf("Murmur2Kafka(len %d) = %#x, reference %#x", len(key), got, want)
			}
			if mustBytes(t, Murmur2, KafkaSeed, key) != uint64(got) {
				t.Fatalf("Murmur2 kind with KafkaSeed differs from Murmur2Kafka")
			}
			p := KafkaPartition(key, n)
			switch {
			case n <= 1 && p != 0:
				t.Fatalf("KafkaPartition(·, %d) = %d", n, p)
			case n > 1 && p != int((got&0x7fffffff)%uint32(n)):
				t.Fatalf("KafkaPartition(·, %d) = %d, Kafka formula %d", n, p, (got&0x7fffffff)%uint32(n))
			}
		}},
	},
	MaxSteps: 64,
}

// FuzzHash runs fuzz-chosen kinds, seeds and keys through every entry point
// and checks the invariants every kind shares (byte/string agreement,
// determinism, width, lane range, Kafka formula) plus the independent
// oracles where one exists.
func FuzzHash(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		fuzzScript.WriteStep(f, w, "kind")
		w.Choice(len(Kinds()), ki)
		fuzzScript.WriteStep(f, w, "seed")
		w.Uint64(uint64(ki) * 0x9E3779B97F4A7C15)
		for _, n := range []int{0, 1, 3, 4, 8, 9, 16, 17, 47, 48, 129, 240, 241, 1024, 1025, 2100} {
			fuzzScript.WriteStep(f, w, "hash")
			w.Bytes(2600, katBuffer()[:n])
		}
		fuzzScript.WriteStep(f, w, "lane")
		w.Int(-2, 1<<20, 64)
		w.Bytes(64, []byte("lane-key"))
		fuzzScript.WriteStep(f, w, "kafka")
		w.Bytes(300, []byte("a-little-bit-long-string"))
		w.Int(-1, 4096, 12)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzScript.Run(t, data, &fuzzState{h: mustNew(t, FNV1a)})
	})
}
