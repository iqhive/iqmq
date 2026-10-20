package engine

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/internal/queue"
)

// The lane-dispatch benchmark of the performance budgets layer 1 and variant
// The specification. The canonical name is
//
//	BenchmarkAPI/Dispatch/{interface,concrete,native}/P=<n>
//
// with the dimensions in that fixed order: the mechanism the lane's ready
// queue is called through, and the number of goroutines sharing the ring.
//
// All three mechanisms are measured in one binary, whatever the build is
// bound to, so the bake-off needs one run rather than three: what the
// iqmq_queue_<slug> tags change is which of them the *engine* uses, and that
// is what BenchmarkMemory/PubSubAck shows once a binding is chosen.
//
// The three arms hold the same algorithm — Vyukov's bounded ring, the SeqMPMC
// kind — and do the same work: one push and the pop that takes
// it back, which is what a lane does per record. The only thing that varies
// is the call:
//
//   - interface: through queue.Queue[T], an indirect call per operation.
//   - concrete: through *queue.MPMC[T], a static call the compiler may inline
//     across the package boundary.
//   - native: through this package's own ring, with no package boundary in
//     the way at all.
//
// The three loops are written out rather than shared behind a function value
// or a type parameter, and that is deliberate: a closure or a generic
// dictionary would put an indirect call in front of every arm and the
// benchmark would measure it instead of the mechanisms. The goroutines are
// spawned explicitly rather than through RunParallel, because RunParallel
// runs a multiple of GOMAXPROCS goroutines and the P dimension has to mean
// what it says. The payload is a pointer, as an envelope is, so the arms
// measure the call and not the copy.

// dispatchCapacity is the ring size every arm is measured at: large enough
// that a producer/consumer pair never finds it full or empty, small enough to
// stay in cache.
const dispatchCapacity = 1024

// dispatchPayload is what the ring moves: one pointer, as a lane moves one
// envelope pointer.
type dispatchPayload struct{ n int }

// benchDispatchParallelism is the P sweep: the call on its own, and the same
// call with the ring shared the way a lane's is.
func benchDispatchParallelism() []int {
	if runtime.GOMAXPROCS(0) < 4 {
		return []int{1, 2}
	}
	return []int{1, 4}
}

func BenchmarkAPI(b *testing.B) {
	for _, d := range Dispatches() {
		for _, p := range benchDispatchParallelism() {
			b.Run(fmt.Sprintf("Dispatch/%s/P=%d", d, p), func(b *testing.B) {
				switch d {
				case DispatchInterface:
					benchDispatchInterface(b, p)
				case DispatchConcrete:
					benchDispatchConcrete(b, p)
				case DispatchNative:
					benchDispatchNative(b, p)
				case dispatchCount:
					b.Fatalf("dispatch %d is not a mechanism", d)
				}
			})
		}
	}
}

// dispatchShare splits b.N over p goroutines, giving the remainder to the
// first, as internal/settlement's BenchmarkAPI does.
func dispatchShare(n, p, g int) int {
	per := n / p
	if g == 0 {
		return per + n - per*p
	}
	return per
}

func benchDispatchInterface(b *testing.B, p int) {
	v := &dispatchPayload{n: 1}
	q, err := queue.New[*dispatchPayload](queue.SeqMPMC, dispatchCapacity)
	if err != nil {
		b.Fatal(err)
	}
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := dispatchShare(b.N, p, g)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n {
				if q.TryPush(v) {
					q.TryPop()
				}
			}
		}()
	}
	wg.Wait()
}

func benchDispatchConcrete(b *testing.B, p int) {
	v := &dispatchPayload{n: 1}
	q := queue.NewMPMC[*dispatchPayload](dispatchCapacity)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := dispatchShare(b.N, p, g)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n {
				if q.TryPush(v) {
					q.TryPop()
				}
			}
		}()
	}
	wg.Wait()
}

func benchDispatchNative(b *testing.B, p int) {
	v := &dispatchPayload{n: 1}
	q := newNativeRing[*dispatchPayload](dispatchCapacity)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := dispatchShare(b.N, p, g)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n {
				if q.TryPush(v) {
					q.TryPop()
				}
			}
		}()
	}
	wg.Wait()
}
