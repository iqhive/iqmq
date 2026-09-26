package corr

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestModel runs every kind against the map oracle: the authored seed
// scripts, pseudo-random op scripts, and the two shapes the request/reply
// client produces (a steady stream of matched replies, and a stream in which
// a share of the requests time out).
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("seeds", func(t *testing.T) { modelSeeds(t, k) })
			t.Run("script", func(t *testing.T) { modelRandomScripts(t, k) })
			t.Run("replies", func(t *testing.T) { modelReplies(t, k, 0) })
			t.Run("timeouts", func(t *testing.T) { modelReplies(t, k, 3) })
		})
	}
}

func modelSeeds(t *testing.T, k Kind) {
	sc := corrScript()
	for i, seed := range corrSeeds(sc) {
		r := fuzzutil.NewReader(seed)
		if Kinds()[r.Choice(len(Kinds()))] != k {
			continue
		}
		h := newCorrHarness(k, corrConfigFrom(r))
		if n := sc.Run(t, seed[len(seed)-r.Remaining():], h); n == 0 {
			t.Fatalf("seed %d executed no steps", i)
		}
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
}

func modelRandomScripts(t *testing.T, k Kind) {
	sc := corrScript()
	rng := rand.New(rand.NewPCG(uint64(k), 0xc0de))
	for run := range 200 {
		data := make([]byte, 512+rng.IntN(1536))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		r := fuzzutil.NewReader(data)
		cfg := corrConfigFrom(r)
		h := newCorrHarness(k, cfg)
		sc.Run(t, data[len(data)-r.Remaining():], h)
		if t.Failed() {
			t.Fatalf("run %d diverged (config %+v)", run, cfg)
		}
	}
}

// modelReplies drives the shape a request/reply client produces: requests
// arrive, replies come back out of order, and one in timeoutRate requests is
// never answered and must be released by the sweep. timeoutRate 0 answers
// every request.
func modelReplies(t *testing.T, k Kind, timeoutRate int) {
	h := newCorrHarness(k, Config{Capacity: 512, Shards: 8})
	rng := rand.New(rand.NewPCG(uint64(k), uint64(timeoutRate)))
	var outstanding []ID
	for i := range 20000 {
		if len(outstanding) < 64 {
			id := h.next
			h.next++
			h.put(t, id, h.now+int64(10+rng.IntN(50)))
			outstanding = append(outstanding, id)
		}
		if timeoutRate == 0 || i%timeoutRate != 0 {
			j := rng.IntN(len(outstanding))
			h.take(t, outstanding[j])
			outstanding[j] = outstanding[len(outstanding)-1]
			outstanding = outstanding[:len(outstanding)-1]
		}
		if i%16 == 0 {
			h.now += 20
			h.expire(t, h.now)
			outstanding = outstanding[:0]
			outstanding = append(outstanding, h.live...)
		}
		if t.Failed() {
			t.Fatalf("diverged at request %d", i)
		}
	}
	h.expire(t, math.MaxInt64)
	if h.s.Len() != 0 {
		t.Fatalf("drain left %d entries", h.s.Len())
	}
}
