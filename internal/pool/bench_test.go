package pool

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

// envelope approximates the engine's per-subscription record: 96 bytes of
// payload pointer, sequence, attempt/state/generation words, due and
// deadline instants, owner, list links and a timer node.
type envelope struct {
	Payload  *byte
	Seq      uint64
	Attempts uint32
	State    uint32
	Gen      uint32
	_        uint32
	DueAt    int64
	Deadline int64
	Owner    uintptr
	Links    [2]*envelope
	Tnode    [3]uint64
}

// benchMatrix returns the dimension sweeps IQMQ_BENCH_MATRIX selects.
//
// `full` is the grid the bake-off measures. The specification is decided on "throughput +
// GC pause; adopt only if measurably better", and the two dimensions that
// move either are the goroutine count -- a per-P cache (sync.Pool) and a
// shared free list part company under contention and nowhere else -- and the
// buffer size, where the payload ladder reaches 64 KiB and where a pool
// stops competing with the allocator and starts competing with the GC. So
// `full` sweeps P over the 1/8/32 (the 4 rung is dropped: the harness
// sweeps GOMAXPROCS=1,4,8,32 on every row already) and every size the specification
// names, and leaves the in-flight depth at the default's 8, which no
// criterion reads. That is 12 rows per kind against the
// 20-row full cross this used to return, and 60 against 100 for the family.
// The specification records the rule.
func benchMatrix() (ps, inflights, sizes []int) {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 8, 32}, []int{8}, []int{64, 1024, 65536}
	}
	return []int{1, 4}, []int{8}, []int{64, 1024}
}

func BenchmarkPool(b *testing.B) {
	if size := unsafe.Sizeof(envelope{}); size != 96 {
		b.Fatalf("envelope is %d bytes, want 96", size)
	}
	ps, inflights, sizes := benchMatrix()
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, p := range ps {
				for _, h := range inflights {
					b.Run(fmt.Sprintf("GetPut/P=%d/inflight=%d", p, h), func(b *testing.B) { benchObjects(b, k, p, h) })
				}
			}
			for _, p := range ps {
				for _, n := range sizes {
					b.Run(fmt.Sprintf("Buffers/P=%d/size=%d", p, n), func(b *testing.B) { benchBuffers(b, k, p, n) })
				}
			}
		})
	}
}

// gcReporter records GC cycles around the timed region.
type gcReporter struct{ before runtime.MemStats }

func startGC() *gcReporter {
	g := &gcReporter{}
	runtime.ReadMemStats(&g.before)
	return g
}

func (g *gcReporter) report(b *testing.B) {
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(after.NumGC-g.before.NumGC)/float64(b.N), "gc-cycles/op")
	b.ReportMetric(maxRSSMB(), "max-rss-MB")
}

func benchObjects(b *testing.B, k Kind, p, inflight int) {
	pool := New(k, Config[envelope]{MaxIdle: 4096})
	per := b.N / p
	extra := b.N - per*p
	var wg sync.WaitGroup
	b.ReportAllocs()
	g := startGC()
	b.ResetTimer()
	for pi := range p {
		n := per
		if pi == 0 {
			n += extra
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			held := make([]*envelope, inflight)
			for i := range n {
				j := i % inflight
				if held[j] != nil {
					pool.Put(held[j])
				}
				e := pool.Get()
				e.Seq = uint64(i)
				e.State = 1
				held[j] = e
			}
			for _, e := range held {
				pool.Put(e)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	g.report(b)
}

func benchBuffers(b *testing.B, k Kind, p, size int) {
	const inflight = 8
	pool := NewBuffers(k, BufferConfig{MaxIdle: 4096})
	per := b.N / p
	extra := b.N - per*p
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.SetBytes(int64(size))
	g := startGC()
	b.ResetTimer()
	for pi := range p {
		n := per
		if pi == 0 {
			n += extra
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			held := make([][]byte, inflight)
			for i := range n {
				j := i % inflight
				if held[j] != nil {
					pool.Put(held[j])
				}
				buf := pool.Get(size)
				buf[0] = byte(i)
				buf[size-1] = byte(i >> 8)
				held[j] = buf
			}
			for _, buf := range held {
				pool.Put(buf)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	g.report(b)
}
