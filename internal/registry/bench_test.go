package registry

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// benchKeys is the number of topics a benchmark registry holds.
const benchKeys = 256

// benchTopic stands in for the engine's *topic: a pointer-shaped value, so
// boxing into sync.Map does not allocate and the kinds compare fairly.
type benchTopic struct{ id int }

// benchParallelism returns the goroutine counts; IQMQ_BENCH_MATRIX=full
// widens the sweep.
func benchParallelism() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 4, 8, 32}
	}
	return []int{1, 4}
}

var benchKeySet = func() []Key {
	keys := make([]Key, benchKeys)
	for i := range keys {
		keys[i] = Intern("bench/topic/" + itoa(i))
	}
	return keys
}()

var benchMissKey = Intern("bench/missing")

// BenchmarkRegistry/<kind>/<workload>/P=<n> spreads b.N operations over P
// goroutines against a registry pre-populated with benchKeys topics.
//
//	readheavy: Load only, one miss in sixteen
//	churn:     alternating LoadOrStore (new topic) and Delete
//	mixed:     90 % Load, 5 % LoadOrStore, 5 % Delete
func BenchmarkRegistry(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, wl := range []string{"readheavy", "churn", "mixed"} {
				for _, p := range benchParallelism() {
					b.Run(fmt.Sprintf("%s/P=%d", wl, p), func(b *testing.B) { benchWorkload(b, k, wl, p) })
				}
			}
		})
	}
}

func benchWorkload(b *testing.B, k Kind, wl string, p int) {
	r := mustNew[*benchTopic](b, k)
	topics := make([]*benchTopic, benchKeys)
	for i := range topics {
		topics[i] = &benchTopic{id: i}
		if wl != "churn" || i%2 == 0 {
			r.LoadOrStore(benchKeySet[i], topics[i])
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
			var sink int
			switch wl {
			case "readheavy":
				for i := range n {
					key := benchKeySet[(i*p+g)%benchKeys]
					if i&15 == 15 {
						key = benchMissKey
					}
					if t, ok := r.Load(key); ok {
						sink += t.id
					}
				}
			case "churn":
				for i := range n {
					idx := (i*p + g) % benchKeys
					if _, loaded := r.LoadOrStore(benchKeySet[idx], topics[idx]); loaded {
						r.Delete(benchKeySet[idx])
					}
				}
			case "mixed":
				for i := range n {
					idx := (i*p + g) % benchKeys
					switch i % 20 {
					case 0:
						r.LoadOrStore(benchKeySet[idx], topics[idx])
					case 1:
						r.Delete(benchKeySet[idx])
					default:
						if t, ok := r.Load(benchKeySet[idx]); ok {
							sink += t.id
						}
					}
				}
			}
			_ = sink
		}()
	}
	wg.Wait()
}
