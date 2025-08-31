package settlement

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"unsafe"
)

// benchParallelism returns the goroutine counts; IQMQ_BENCH_MATRIX=full
// widens the sweep to the spec's 8/32/64 contention points.
func benchParallelism() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 8, 32, 64}
	}
	return []int{1, 8}
}

// slot is a table entry padded to its own cache line, as an engine envelope
// (≥ 64 B) is; unpadded 8-byte cells would measure false sharing instead of
// the settlement word.
type slot struct {
	c Cell
	_ [56]byte
}

// BenchmarkAPI/Settlement/token/op=<settle|stale|twice>/P=<n> measures the packed-token settlement representation over a table of cells, one slot
// per goroutine: settle is the full delivery lifecycle (lease, Begin,
// Commit through a Token), stale a Begin with an outdated token
// (ErrLeaseLost), twice a second Begin after success (ErrAlreadySettled).
// The closure and object representations were measured, whose
// prototypes were removed at the M6 disposition review (ADR-025); their
// numbers are in benchmarks/reports/apibench-scorecard.json.
func BenchmarkAPI(b *testing.B) {
	b.Run("Settlement/token", func(b *testing.B) {
		for _, p := range benchParallelism() {
			for _, op := range []string{"settle", "stale", "twice"} {
				b.Run(fmt.Sprintf("op=%s/P=%d", op, p), func(b *testing.B) { benchToken(b, op, p) })
			}
		}
	})
}

func benchToken(b *testing.B, op string, p int) {
	cells := make([]slot, p)
	tokens := make([]Token, p)
	for i := range cells {
		g, _ := cells[i].c.Lease()
		tokens[i] = NewToken(uint32(i), g)
		if op == "twice" {
			_ = cells[i].c.Begin(g)
			cells[i].c.Commit(g)
		}
		if op == "stale" {
			tokens[i] = NewToken(uint32(i), g-1)
		}
	}
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok := tokens[g]
			c := &cells[tok.Slot()].c
			switch op {
			case "settle":
				for range n {
					if err := c.Begin(tok.Gen()); err != nil {
						b.Error(err)
						return
					}
					c.Commit(tok.Gen())
					gen, _ := c.Lease()
					tok = NewToken(tok.Slot(), gen)
				}
			default:
				for range n {
					if err := c.Begin(tok.Gen()); err == nil {
						b.Error("stale or repeated Begin won")
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(unsafe.Sizeof(Cell{})), "bytes/cell")
}
