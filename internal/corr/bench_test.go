package corr

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

// benchMatrix returns the goroutine counts and the outstanding-request depths
// IQMQ_BENCH_MATRIX selects.
//
// `full` is the grid the bake-off measures. The specification is decided on "ns/op
// under contention; simpler wins ties" over evidence that names "request
// counts", so both dimensions are the criterion's: the outstanding-request
// depth is the request count and is swept over four orders of magnitude, and
// the goroutine count carries the contention. P=4 is dropped -- the harness
// sweeps GOMAXPROCS=1,4,8,32 on every row -- which is 12 rows per kind rather
// than 16. The specification records the rule.
func benchMatrix() (ps, pendings []int) {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 8, 32}, []int{1, 64, 1024, 8192}
	}
	return []int{1, 8}, []int{64, 1024}
}

// BenchmarkReqReply/Store/<kind>/P=<n>/pending=<m> measures one request and
// its reply through the store — a Put and the matching Take — with m requests
// already outstanding and n goroutines running concurrently, which is the
// shape of one reply consumer serving many callers.
func BenchmarkReqReply(b *testing.B) {
	ps, pendings := benchMatrix()
	b.Run("Store", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, p := range ps {
					for _, pending := range pendings {
						b.Run(fmt.Sprintf("P=%d/pending=%d", p, pending), func(b *testing.B) {
							benchStore(b, k, p, pending)
						})
					}
				}
			})
		}
	})
	// The sweep is the store's other half: it runs on a shared ticker and
	// must not walk the whole table when nothing is due.
	b.Run("Sweep", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, pending := range pendings {
					b.Run(fmt.Sprintf("pending=%d", pending), func(b *testing.B) {
						benchSweep(b, k, pending)
					})
				}
			})
		}
	})
}

// benchCapacity leaves room for the outstanding requests and the ones the
// benchmark adds while running.
func benchCapacity(pending int) int { return 2*pending + 4096 }

func benchStore(b *testing.B, k Kind, p, pending int) {
	s := mustNew[*call](b, k, Config{Capacity: benchCapacity(pending), Shards: 16})
	c := newCall(1)
	for i := ID(1); i <= ID(pending); i++ {
		if !s.Put(i, 1<<40, c) {
			b.Fatalf("preload %d refused", i)
		}
	}
	var next atomic.Uint64
	next.Store(uint64(pending) + 1)
	per := b.N / p
	extra := b.N - per*p
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := per
		if g == 0 {
			n += extra
		}
		wg.Go(func() {
			// Each goroutine owns a disjoint block of ids, as a client owns
			// the correlation ids it allocates.
			base := ID(next.Add(uint64(n+1)) - uint64(n))
			for i := range n {
				id := base + ID(i)
				if !s.Put(id, 1<<40, c) {
					b.Errorf("Put(%d) refused", id)
					return
				}
				if _, ok := s.Take(id); !ok {
					b.Errorf("Take(%d) missed", id)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	if s.Len() != pending {
		b.Fatalf("Len = %d, want %d", s.Len(), pending)
	}
}

// benchSweep measures one Expire pass over a store holding pending requests,
// none of which is due: the cost the reply consumer pays on every tick.
func benchSweep(b *testing.B, k Kind, pending int) {
	s := mustNew[*call](b, k, Config{Capacity: benchCapacity(pending), Shards: 16})
	c := newCall(1)
	for i := ID(1); i <= ID(pending); i++ {
		if !s.Put(i, 1<<40, c) {
			b.Fatalf("preload %d refused", i)
		}
	}
	dst := make([]Pending[*call], 0, 16)
	b.ReportAllocs()
	for b.Loop() {
		dst = s.Expire(1<<20, dst[:0])
		if len(dst) != 0 {
			b.Fatalf("swept %d entries that were not due", len(dst))
		}
	}
	b.ReportMetric(float64(pending), "pending")
}
