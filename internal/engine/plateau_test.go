package engine

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/pool"
)

// plateauCycles is the number of publish/fetch/acknowledge cycles one phase
// of the plateau test runs. Two phases of this length are enough for an
// engine that leaks one object per message to grow visibly, and short enough
// that the test stays a unit test on a loaded shared host.
const plateauCycles = 20000

// heapObjects returns the live heap object count after two collections, which
// is what makes the second sweep observe the finalisers of the first.
func heapObjects() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapObjects
}

// TestMemoryPlateau is the memory-plateau invariant of the memory engine and under bounded load a bounded engine reaches a steady state and stays
// there — the live heap does not grow with the number of messages, no
// goroutine is created per message, and no timer is either.
//
// It is deliberately a shape test rather than a budget: the absolute numbers
// depend on the allocator and on what else the process is doing, so what is
// asserted is that ten times the work does not cost ten times the memory. A
// leak of one object per message would put the second phase far outside the
// slack; a steady state puts it inside it whatever the host is doing.
func TestMemoryPlateau(t *testing.T) {
	if testing.Short() {
		t.Skipf("plateau test runs %dk publish/fetch/ack cycles", 11*plateauCycles/1000)
	}
	counting := clock.NewCounting(clock.Real{})
	e, err := New(Config[int64]{
		// A retaining pool is the configuration the steady state is claimed
		// for; the GC-only kind allocates by construction.
		Kinds: KindSet{Pool: pool.FreeList},
		Clock: counting,
		DefaultTopology: TopologyConfig{
			Partitions: 4, CapacityMessages: 64, AckDeadline: time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dst := make([]Claim[int64], 8)
	cycle := func(n int) {
		for i := range n {
			if err := top.Publish(ctx, Publish[int64]{
				Payload: int64(i), Size: 64, Partition: -1, Mode: CopyOwned,
				Release: func(int64) {},
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			got, err := c.Fetch(poll(t), dst)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Fetch: %v", err)
			}
			for j := range got {
				if err := c.Ack(dst[j].Env, dst[j].Gen); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
			if snap := sub.Inspect(); snap.Ready+snap.InFlight+snap.Delayed > 4*64 {
				t.Fatalf("the bounded engine holds %+v", snap)
			}
		}
	}

	// Warm the pools and the queues, then take the baseline.
	cycle(plateauCycles)
	base := heapObjects()
	goroutines := runtime.NumGoroutine()
	timers := counting.Created()

	// Ten times the work of the phase that set the baseline.
	for range 10 {
		cycle(plateauCycles)
	}
	after := heapObjects()
	// The slack is generous: what is being excluded is growth proportional
	// to the message count, which for ten more phases would be an order of
	// magnitude, not a factor of two plus a fixed allowance.
	if after > 2*base+4096 {
		t.Fatalf("heap objects grew from %d to %d over %d further messages: no plateau",
			base, after, 10*plateauCycles)
	}
	if got := runtime.NumGoroutine(); got > goroutines {
		t.Fatalf("goroutines grew from %d to %d: a goroutine per message", goroutines, got)
	}
	// The scheduler arms one timer per sleep, never one per message.
	if grown := counting.Created() - timers; grown > int64(plateauCycles) {
		t.Fatalf("the engine created %d timers over %d messages: a timer per message",
			grown, 10*plateauCycles)
	}
	t.Logf("plateau: heap objects %d -> %d, goroutines %d, timers +%d over %d messages",
		base, after, goroutines, counting.Created()-timers, 10*plateauCycles)
	if snap := sub.Inspect(); !snap.Conserved() {
		t.Fatalf("the subscription is not conserved: %+v", snap)
	}
}
