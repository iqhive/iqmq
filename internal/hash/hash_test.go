package hash

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 5 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		h := New(k)
		if h.Kind != k || h.Bytes == nil || h.String == nil {
			t.Fatalf("New(%v) = %+v", k, h)
		}
		if k.Bits() != 32 && k.Bits() != 64 {
			t.Fatalf("%v.Bits() = %d", k, k.Bits())
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if New(Auto).Kind != AutoKind || Auto.Bits() != AutoKind.Bits() || Auto.Stable() != AutoKind.Stable() {
		t.Fatal("Auto resolves through New/Bits/Stable")
	}
	if Murmur2.Bits() != 32 || FNV1a.Bits() != 64 || Wyhash.Bits() != 64 || XXH3.Bits() != 64 || Maphash.Bits() != 64 {
		t.Fatal("Bits")
	}
	if Maphash.Stable() || !FNV1a.Stable() || !Wyhash.Stable() || !XXH3.Stable() || !Murmur2.Stable() {
		t.Fatal("Stable")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	for _, f := range []func(){
		func() { New(Kind(99)) },
		func() { Bytes(Kind(99), 0, nil) },
		func() { String(Kind(99), 0, "") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid kind should panic")
				}
			}()
			f()
		}()
	}
	// Package-level dispatch agrees with the bound entry points.
	key := []byte("dispatch-agreement")
	for _, k := range Kinds() {
		h := New(k)
		if Bytes(k, 9, key) != h.Bytes(9, key) || String(k, 9, string(key)) != h.String(9, string(key)) {
			t.Fatalf("%v: Bytes/String dispatch differs from New", k)
		}
	}
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "hash")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds hash yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds hash to %q but AutoKind is %q", slug, AutoKind)
	}
}

var allocSink uint64

// TestAllocs pins every entry point of every kind at zero allocations,
// including the seeded long-input path of XXH3 (stack-built secret) and
// the Kafka partitioner.
func TestAllocs(t *testing.T) {
	sizes := []int{0, 3, 8, 13, 32, 100, 200, 256, 300, 1025, 4096}
	seeds := []uint64{0, 12345, KafkaSeed}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			h := New(k)
			for _, n := range sizes {
				key := katBuffer()[:n]
				s := string(key)
				for _, seed := range seeds {
					if a := testing.AllocsPerRun(200, func() { allocSink += h.Bytes(seed, key) }); a != 0 {
						t.Fatalf("Bytes(n=%d, seed=%#x): %.1f allocs/op", n, seed, a)
					}
					if a := testing.AllocsPerRun(200, func() { allocSink += h.String(seed, s) }); a != 0 {
						t.Fatalf("String(n=%d, seed=%#x): %.1f allocs/op", n, seed, a)
					}
					if a := testing.AllocsPerRun(200, func() { allocSink += Bytes(k, seed, key) }); a != 0 {
						t.Fatalf("package Bytes(n=%d): %.1f allocs/op", n, a)
					}
				}
			}
		})
	}
	key := []byte("lane-and-partition")
	if a := testing.AllocsPerRun(200, func() { allocSink += uint64(Lane(allocSink, 64) + KafkaPartition(key, 12)) }); a != 0 {
		t.Fatalf("Lane/KafkaPartition: %.1f allocs/op", a)
	}
}

// TestLane checks the range contract and that a uniform 64-bit input stays
// uniform over lanes.
func TestLane(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{-1, 0, 1, 2, 3, 5, 7, 8, 64, 100, 1000, 1 << 20, 1 << 31, 1 << 62} {
		for range 1000 {
			h := r.Uint64()
			l := Lane(h, n)
			if n <= 1 {
				if l != 0 {
					t.Fatalf("Lane(%#x, %d) = %d, want 0", h, n, l)
				}
				continue
			}
			if l < 0 || l >= n {
				t.Fatalf("Lane(%#x, %d) = %d out of range", h, n, l)
			}
		}
	}
	// Full use of the range.
	used := map[int]bool{}
	for range 100000 {
		used[Lane(r.Uint64(), 7)] = true
	}
	if len(used) != 7 {
		t.Fatalf("Lane(·, 7) reached only %d lanes", len(used))
	}
	// Uniform input, 64 lanes, 1e6 samples: χ² below the p=0.001 critical value.
	var counts [64]int
	for range 1000000 {
		counts[Lane(r.Uint64(), 64)]++
	}
	if x := chiSquare(counts[:], 1000000); x >= chiSquareCrit63 {
		t.Fatalf("Lane on uniform input: χ² = %.1f", x)
	}
	// Narrow (32-bit) inputs must still spread: the multiply moves entropy up.
	counts = [64]int{}
	for range 1000000 {
		counts[Lane(uint64(r.Uint32()), 64)]++
	}
	if x := chiSquare(counts[:], 1000000); x >= chiSquareCrit63 {
		t.Fatalf("Lane on 32-bit input: χ² = %.1f", x)
	}
}

// TestKafka checks the Kafka partitioner against tables generated by
// Kafka's own Java code (testdata/kat/murmur2.txt).
func TestKafka(t *testing.T) {
	buf := katBuffer()
	var vectors, generated int
	for _, f := range katRows(t, "murmur2.txt") {
		switch f[0] {
		case "V":
			want, err := strconv.ParseInt(f[2], 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			key := []byte(f[1])
			if got := int64(int32(Murmur2Kafka(key))); got != want {
				t.Fatalf("Murmur2Kafka(%q) = %d, Kafka says %d", f[1], got, want)
			}
			vectors++
		case "K":
			n, _ := strconv.Atoi(f[1])
			want, err := strconv.ParseUint(f[2], 16, 32)
			if err != nil {
				t.Fatal(err)
			}
			key := buf[:n]
			got := Murmur2Kafka(key)
			if uint64(got) != want {
				t.Fatalf("Murmur2Kafka(len %d) = %#x, Kafka says %#x", n, got, want)
			}
			if Bytes(Murmur2, KafkaSeed, key) != uint64(got) || String(Murmur2, KafkaSeed, string(key)) != uint64(got) {
				t.Fatalf("len %d: Murmur2 kind seeded with KafkaSeed differs from Murmur2Kafka", n)
			}
			parts := []int{1, 2, 3, 7, 12, 50, 64, 100, 1000, 65536}
			if len(f) != 3+len(parts) {
				t.Fatalf("malformed row %q", f)
			}
			for i, p := range parts {
				want, _ := strconv.Atoi(f[3+i])
				if got := KafkaPartition(key, p); got != want {
					t.Fatalf("KafkaPartition(len %d, %d) = %d, Kafka says %d", n, p, got, want)
				}
			}
			generated++
		}
	}
	if vectors != 6 || generated < 80 {
		t.Fatalf("table has %d vectors and %d generated rows", vectors, generated)
	}
	if KafkaPartition([]byte("k"), 0) != 0 || KafkaPartition([]byte("k"), -3) != 0 || KafkaPartition([]byte("k"), 1) != 0 {
		t.Fatal("KafkaPartition with numPartitions <= 1")
	}
	// Both seed words fold into the 32-bit seed.
	if Bytes(Murmur2, 1, buf[:20]) == Bytes(Murmur2, 1<<32|1, buf[:20]) {
		t.Fatal("high seed word ignored")
	}
}

// TestStable runs the test binary again and compares hashes across the two
// processes: Stable kinds must agree, Maphash must not.
func TestStable(t *testing.T) {
	const env = "IQMQ_HASH_STABLE_CHILD"
	probe := []byte("iqmq-stable-probe")
	if os.Getenv(env) == "1" {
		for _, k := range Kinds() {
			fmt.Printf("%s %016x\n", k, Bytes(k, 7, probe))
		}
		os.Exit(0)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestStable$", "-test.count=1")
	cmd.Env = append(os.Environ(), env+"=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	seen := 0
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 2 {
			continue
		}
		k, ok := ParseKind(f[0])
		if !ok {
			continue
		}
		child, err := strconv.ParseUint(f[1], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		mine := Bytes(k, 7, probe)
		switch {
		case k.Stable() && child != mine:
			t.Errorf("%v is declared Stable but hashed %#x here and %#x in another process", k, mine, child)
		case !k.Stable() && child == mine:
			t.Errorf("%v is declared unstable but hashed identically (%#x) in two processes", k, mine)
		}
		seen++
	}
	if seen != len(Kinds()) {
		t.Fatalf("child reported %d kinds:\n%s", seen, out)
	}
}

// katBuffer regenerates the key material of testdata/kat/gen/keys.h: the
// xxHash sanity PRNG (byteGen starts at PRIME32, is multiplied by PRIME64,
// and emits its top byte).
func katBuffer() []byte {
	buf := make([]byte, 4096)
	g := uint64(2654435761)
	for i := range buf {
		buf[i] = byte(g >> 56)
		g *= 11400714785074694797
	}
	return buf
}

// katRows returns the whitespace-split data rows of a testdata/kat table.
func katRows(t testing.TB, name string) [][]string {
	t.Helper()
	data, err := os.ReadFile("testdata/kat/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	for line := range strings.SplitSeq(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		rows = append(rows, f)
	}
	return rows
}
