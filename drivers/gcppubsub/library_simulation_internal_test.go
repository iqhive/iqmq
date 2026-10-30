package gcppubsub

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// libraryFlowControl is the client library's flow control as pubsub v2.8.0 has
// it: a slot and min(len(Data), MaxOutstandingBytes) of data for each message,
// taken in order by the one receive loop before the callback is spawned, and
// given back after the callback returns.
type libraryFlowControl struct {
	mu          sync.Mutex
	slots, used int
	bytes, held int64
	freed       chan struct{}
}

func (f *libraryFlowControl) acquire(size int64) {
	need := min(size, f.bytes)
	for {
		f.mu.Lock()
		if f.used < f.slots && f.held+need <= f.bytes {
			f.used++
			f.held += need
			f.mu.Unlock()
			return
		}
		f.mu.Unlock()
		<-f.freed
	}
}

func (f *libraryFlowControl) release(size int64) {
	f.mu.Lock()
	f.used--
	f.held -= min(size, f.bytes)
	f.mu.Unlock()
	select {
	case f.freed <- struct{}{}:
	default:
	}
}

// librarySimulation is one run: what the service sends and how the
// application fetches.
type librarySimulation struct {
	prefetch         int
	bytes            int64
	minSize, maxSize int
	batchGap         time.Duration
	fetchGap, stall  time.Duration
	batches          int
}

// TestNoLeaseOutlastsTheLibrarysBudget drives admit the way pubsub v2.8.0's
// receive loop does and holds every delivery to the guarantee the arrivals
// account exists for: its lease ends no later than the library's own budget
// for the message, which starts when the loop's receive returns the batch. The
// loop takes each message's flow control in order and spawns a goroutine for
// its callback, so the callbacks one return lets through reach the account in
// any order. Each run is made twice, once with the loop yielding after every
// spawn, which tends to keep that order, and once without, where the one
// spawned last tends to come first. The application fetches at random and now
// and then stalls, which is what holds callbacks on the ring and messages in
// the library.
func TestNoLeaseOutlastsTheLibrarysBudget(t *testing.T) {
	for _, sim := range []librarySimulation{
		{4, 1 << 20, 100, 900 << 10, 60 * time.Millisecond, 150 * time.Millisecond, 3 * time.Second, 120},
		{4, 1 << 20, 10, 2 << 20, 30 * time.Millisecond, 100 * time.Millisecond, 4 * time.Second, 120},
		{16, 1 << 20, 10, 300 << 10, 20 * time.Millisecond, 80 * time.Millisecond, 2 * time.Second, 120},
		{1, 1000, 1, 1500, 50 * time.Millisecond, 200 * time.Millisecond, 3 * time.Second, 120},
		{8, libraryOutstandingBytes, 10, 1000, 40 * time.Millisecond, 120 * time.Millisecond, 5 * time.Second, 120},
		{8, 4000, 100, 3000, 40 * time.Millisecond, 120 * time.Millisecond, 5 * time.Second, 120},
	} {
		for seed := range uint64(2) {
			for _, yield := range []bool{false, true} {
				t.Run(fmt.Sprintf("prefetch-%d-bytes-%d-seed-%d-yield-%v", sim.prefetch, sim.bytes, seed, yield),
					func(t *testing.T) { sim.run(t, seed, yield) })
			}
		}
	}
}

func (sim librarySimulation) run(t *testing.T, seed uint64, yield bool) {
	synctest.Test(t, func(t *testing.T) {
		const budget = 10 * time.Second
		ctx := context.Background()
		src := leaseSource(t, sim.prefetch, budget)
		src.arrivals.bytes = sim.bytes
		fc := &libraryFlowControl{slots: sim.prefetch, bytes: sim.bytes, freed: make(chan struct{}, 1)}
		var mu sync.Mutex
		pulled := map[string]time.Time{}
		var callbacks sync.WaitGroup
		loop := make(chan struct{})
		go func() {
			defer close(loop)
			rng := rand.New(rand.NewPCG(seed, 7))
			id := 0
			for range sim.batches {
				time.Sleep(time.Duration(rng.Int64N(int64(sim.batchGap) + 1)))
				pull := time.Now()
				for range 1 + rng.IntN(sim.prefetch) {
					size := sim.minSize + rng.IntN(sim.maxSize-sim.minSize+1)
					out, err := src.enc.encode(&mq.Message{Body: make([]byte, size)}, nil)
					if err != nil {
						t.Errorf("encode: %v", err)
						return
					}
					wire := int64(len(out.Out.Data))
					fc.acquire(wire)
					in := &psapi.Incoming{
						ID: strconv.Itoa(id), Data: out.Out.Data, Attributes: out.Out.Attributes,
						Settle: &recordedSettlement{},
					}
					id++
					mu.Lock()
					pulled[in.ID] = pull
					mu.Unlock()
					callbacks.Add(1)
					go func() {
						defer callbacks.Done()
						src.admit(ctx, in)
						fc.release(wire)
					}()
					if yield {
						runtime.Gosched()
					}
				}
			}
		}()
		rng := rand.New(rand.NewPCG(seed, 9))
		dst := make([]mq.DriverDelivery, sim.prefetch)
		over, worst := 0, time.Duration(0)
		for {
			gap := time.Duration(rng.Int64N(int64(sim.fetchGap) + 1))
			if rng.IntN(40) == 0 {
				gap = sim.stall
			}
			time.Sleep(gap)
			fetchCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
			n, _ := src.Fetch(fetchCtx, dst)
			cancel()
			for i := range dst[:n] {
				d := &dst[i]
				mu.Lock()
				p := pulled[d.Ref.(*delivery).id]
				mu.Unlock()
				if late := d.Info.LeaseExpires.Sub(p.Add(budget)); late > 0 {
					over++
					worst = max(worst, late)
				}
			}
			synctest.Wait()
			if returned(loop) && src.ring.Len() == 0 {
				break
			}
		}
		callbacks.Wait()
		if over > 0 {
			t.Errorf("%d deliveries were handed out with a lease that outlasts the library's budget, the worst by %v",
				over, worst)
		}
	})
}
