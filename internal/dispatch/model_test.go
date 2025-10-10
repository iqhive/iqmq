package dispatch

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// TestModel runs every kind against the model of what was submitted: the
// authored seed scripts, pseudo-random op scripts over every lane queue and
// wait strategy, and the three workloads the runner produces — a keyed
// stream that must keep its order, a keyless stream that must use the whole
// pool, and a stream with one straggler key.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("seeds", func(t *testing.T) { modelSeeds(t, k) })
			t.Run("script", func(t *testing.T) { modelRandomScripts(t, k) })
			t.Run("keyed", func(t *testing.T) { modelStreams(t, k, 8, 200, 0) })
			t.Run("partitioned", func(t *testing.T) { modelStreams(t, k, 8, 200, 4) })
			t.Run("keyless", func(t *testing.T) { modelKeyless(t, k) })
			t.Run("straggler", func(t *testing.T) { modelStraggler(t, k) })
		})
	}
}

func modelSeeds(t *testing.T, k Kind) {
	sc := dispatchScript()
	for i, seed := range dispatchSeeds(sc) {
		r := fuzzutil.NewReader(seed)
		if Kinds()[r.Choice(len(Kinds()))] != k {
			continue
		}
		if n := runDispatchScript(t, seed); n == 0 {
			t.Fatalf("seed %d executed no steps", i)
		}
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
}

func modelRandomScripts(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(uint64(k), 0x0a8))
	for run := range 40 {
		data := make([]byte, 96+rng.IntN(256))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		// The first byte selects the kind: force it to this one so every
		// kind sees the same scripts.
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), int(k)-1)
		data = append(w.Data(), data...)
		runDispatchScript(t, data)
		if t.Failed() {
			t.Fatalf("run %d diverged", run)
		}
	}
}

// modelStreams drives the shape the runner produces for an ordered
// subscription: several keys, each a stream of items that must be handled in
// order and never two at a time.
func modelStreams(t *testing.T, k Kind, keys, n int, partitions int32) {
	s := newSink()
	p := New(k, Config{Workers: 4, Lanes: 8, Capacity: 8}, s.handle)
	defer func() { _ = p.Close() }()
	want := map[uint64][]int{}
	ctx := context.Background()
	for i := range n {
		for key := uint64(1); key <= uint64(keys); key++ {
			r := Keyed(key)
			if partitions > 0 {
				r = r.InPartition(int32(mix(key) % uint64(partitions)))
			}
			if err := p.Submit(ctx, r, item{key: key, seq: i}); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			want[key] = append(want[key], i)
		}
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	s.compare(t, want, k.PreservesKeyOrder())
}

// modelKeyless drives an unordered subscription: nothing may be lost or
// duplicated, and the items must not all end up on one lane.
func modelKeyless(t *testing.T, k Kind) {
	s := newSink()
	p := New(k, Config{Workers: 4, Lanes: 4, Capacity: 8}, s.handle)
	defer func() { _ = p.Close() }()
	want := map[uint64][]int{}
	ctx := context.Background()
	for i := range 500 {
		if err := p.Submit(ctx, Keyless(), item{key: keylessKey, seq: i}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		want[keylessKey] = append(want[keylessKey], i)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	s.compare(t, want, false)
}

// modelStraggler keeps one key slow: its own stream stays ordered, and the
// other keys must still get through.
func modelStraggler(t *testing.T, k Kind) {
	s := newSink()
	s.slow[1] = time.Millisecond
	p := New(k, Config{Workers: 4, Lanes: 8, Capacity: 4}, s.handle)
	defer func() { _ = p.Close() }()
	want := map[uint64][]int{}
	ctx := context.Background()
	for i := range 30 {
		for key := uint64(1); key <= 6; key++ {
			if err := p.Submit(ctx, Keyed(key), item{key: key, seq: i}); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			want[key] = append(want[key], i)
		}
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	s.compare(t, want, k.PreservesKeyOrder())
}
