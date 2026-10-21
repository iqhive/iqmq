package corr

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestPutRefusesOnlyAFullStore holds every kind to the one refusal condition
// the package documents: "Put refuses a new entry once Len reaches [the
// capacity]". A store that refuses while it holds a few dozen of thousands of
// slots has failed the bounded-memory contract from the
// other side -- it drops work the caller was entitled to place, and a
// request/reply client that reads a refusal as "already outstanding" loses
// the reply.
//
// The shape is the one the family's own benchmark applies (benchStore at
// P=32): every goroutine owns a disjoint block of ids and does a Put and the
// matching Take, over a store whose capacity is far above anything resident.
// It is a race over the placement, so it is counted in rounds: LockFreeOA
// refused about once per half-million placements at 32 goroutines, which is
// why the campaign saw it in two of ten measured rounds and no test had.
//
// Watched failing on lockfreeoa :
//
//	lockfreeoa: Put(180231) refused with 32 of 8192 slots held: the store is
//	not full, and a refusal is reserved for a store that is
func TestPutRefusesOnlyAFullStore(t *testing.T) {
	const (
		workers  = 32
		capacity = 128
		perRound = 20_000
	)
	rounds := 4
	if testing.Short() {
		rounds = 1
	}
	if runtime.GOMAXPROCS(0) < workers {
		t.Logf("GOMAXPROCS=%d < %d workers: the placements interleave less",
			runtime.GOMAXPROCS(0), workers)
	}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			c := newCall(1)
			for round := range rounds {
				s := mustNew[*call](t, k, Config{Capacity: capacity, Shards: 16})
				var next atomic.Uint64
				next.Store(1)
				var refused atomic.Int64
				var wg sync.WaitGroup
				for range workers {
					wg.Go(func() {
						base := ID(next.Add(perRound))
						for i := range perRound {
							id := base + ID(i)
							if !s.Put(id, 1<<40, c) {
								if refused.Add(1) == 1 {
									t.Errorf("Put(%d) refused with %d of %d slots held: the store is not full, and a refusal is reserved for a store that is (round %d)",
										id, s.Len(), capacity, round)
								}
								return
							}
							if _, ok := s.Take(id); !ok {
								t.Errorf("Take(%d) missed the entry Put(%d) placed", id, id)
								return
							}
						}
					})
				}
				wg.Wait()
				if t.Failed() {
					return
				}
				if s.Len() != 0 {
					t.Fatalf("round %d: Len = %d after every entry was taken", round, s.Len())
				}
			}
		})
	}
}

// TestConcurrentPutsOfOneIDPlaceItOnce is the other half of the placement
// race: the refusal Put does give, "already outstanding", has to hold when the
// duplicates arrive at once. LockFreeOA's loser of a race for one slot rescanned
// while the winner was still writing and had not stored its key, found no
// duplicate, and placed the id a second time.
func TestConcurrentPutsOfOneIDPlaceItOnce(t *testing.T) {
	const (
		workers = 8
		ids     = 5_000
	)
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			c := newCall(1)
			s := mustNew[*call](t, k, Config{Capacity: 128, Shards: 16})
			for id := ID(1); id <= ids; id++ {
				var placed atomic.Int64
				var wg sync.WaitGroup
				for range workers {
					wg.Go(func() {
						if s.Put(id, 1<<40, c) {
							placed.Add(1)
						}
					})
				}
				wg.Wait()
				if n := placed.Load(); n != 1 {
					t.Fatalf("%d concurrent Puts of id %d placed it %d times, want once", workers, id, n)
				}
				if _, ok := s.Take(id); !ok {
					t.Fatalf("Take(%d) missed the entry", id)
				}
				if _, ok := s.Take(id); ok {
					t.Fatalf("id %d was stored twice", id)
				}
			}
		})
	}
}
