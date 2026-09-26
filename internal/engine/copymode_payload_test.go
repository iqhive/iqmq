package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
)

// TestCopyModePayloadUnderConcurrency drives every ownership contract of
// the memory engine through a concurrent publish/fetch/settle loop over
// *payload.Payload -- the representation the memory adapter above this engine
// uses -- and holds each publication to being released exactly once.
//
// It replaces the one piece of coverage that went with the retired
// benchmarks/memory/pubsuback (adr/ADR-017-assumptions.md).
// That file's TestPubSubAckHarness was -race-gated on every increment, and
// what it uniquely exercised was this: the copy modes over a pointer payload
// with competing publishers and consumers and a retaining pool. The rest of
// what it covered is carried elsewhere and was already -- TestAllocs runs the
// owned publish/fetch/ack cycle over *payload.Payload across every retaining
// pool kind and every ready-queue and wait kind, TestEngineMatrix covers 32
// kind sets, FuzzEngineOwnedRelease/Aliasing/ZeroCopy hold the ownership
// contract itself under fuzz-chosen op scripts, and TestPubSubAckBodyIsTheArm
// drives all four contracts at 1P1C, 4P4C and 8P8C -- but all of those use
// byte-slice payloads or one goroutine.
//
// Release exactly once under concurrency is also where the engine's recurring
// defect lives (hack/PLAYBOOK.md): a record placed while the engine is closing
// whose Release never runs consumes its reservation for ever.
func TestCopyModePayloadUnderConcurrency(t *testing.T) {
	for _, arm := range []struct {
		name string
		mode CopyMode
		recv ReceiveMode
	}{
		{"safe", CopySafe, ReceiveShared},
		{"owned", CopyOwned, ReceiveShared},
		{"borrowed", CopyBorrowed, ReceiveShared},
		{"zerocopy", CopyOwned, ReceiveZeroCopy},
	} {
		t.Run(arm.name, func(t *testing.T) {
			const (
				publishers = 4
				consumers  = 4
				perPub     = 250
				total      = publishers * perPub
			)
			eng, err := New(Config[*payload.Payload]{
				// A retaining pool and a real ready queue: the record and
				// envelope a release recycles are the ones a pooled engine
				// hands to the next publication.
				Kinds: KindSet{Pool: pool.FreeList, Queue: queue.MutexRing},
				Clone: func(p *payload.Payload) *payload.Payload { return p.Clone() },
				Zero:  func(p *payload.Payload) { clear(p.Body) },
				Poison: func(p *payload.Payload) {
					for i := range p.Body {
						p.Body[i] = canaryByte
					}
				},
				Receive: arm.recv,
				DefaultTopology: TopologyConfig{
					Kind:             TopologyQueue,
					Partitions:       publishers,
					CapacityMessages: 64,
					Overflow:         OverflowBlock,
					AckDeadline:      -1,
				},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			top, err := eng.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			sub, err := top.Subscribe("s", SubscriptionConfig{})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			var mu sync.Mutex
			releases := map[*payload.Payload]int{}
			var released atomic.Int64
			record := func(p *payload.Payload) {
				mu.Lock()
				releases[p]++
				mu.Unlock()
				released.Add(1)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var drain sync.WaitGroup
			var settled atomic.Int64
			for range consumers {
				c, err := sub.Attach(ConsumerConfig{})
				if err != nil {
					t.Fatalf("Attach: %v", err)
				}
				drain.Add(1)
				go func() {
					defer drain.Done()
					dst := make([]Claim[*payload.Payload], 16)
					for {
						n, err := c.Fetch(ctx, dst)
						if err != nil {
							return
						}
						for i := range n {
							// Read the aliased body before settling: under
							// ReceiveZeroCopy it is the engine's own and dies
							// at settlement, and the canary is what a reader
							// of a settled delivery is meant to see.
							if b := dst[i].Payload.Body; len(b) == 0 || b[0] == 0 {
								t.Errorf("%s: delivered an empty or scrubbed body", arm.name)
							}
							if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
								t.Errorf("ack: %v", err)
								cancel()
								return
							}
						}
						settled.Add(int64(n))
					}
				}()
			}

			var pubWG sync.WaitGroup
			var admitted atomic.Int64
			for id := range publishers {
				pubWG.Add(1)
				go func() {
					defer pubWG.Done()
					for range perPub {
						// A fresh payload per publication, so that a release
						// is attributable to exactly one publication.
						p := &payload.Payload{Body: []byte{0xA5, byte(id)}}
						err := top.Publish(ctx, Publish[*payload.Payload]{
							Payload: p, Size: len(p.Body), Partition: id,
							Mode: arm.mode, Release: record,
						})
						switch {
						case err == nil, isPartial(err):
							admitted.Add(1)
						default:
							t.Errorf("publish: %v", err)
							return
						}
					}
				}()
			}
			pubWG.Wait()
			if got := admitted.Load(); got != total {
				t.Fatalf("admitted %d of %d publications", got, total)
			}
			// Every admitted publication must come home. A safe one is
			// released at admission, the other three at settlement, so this
			// waits for the consumers rather than assuming a timing.
			deadline := time.Now().Add(30 * time.Second)
			for released.Load() < total {
				if time.Now().After(deadline) {
					t.Fatalf("%s: %d of %d payloads released after 30s (settled %d): "+
						"a payload the engine admitted was never released (terminal release)",
						arm.name, released.Load(), total, settled.Load())
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			drain.Wait()
			if err := eng.Close(); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Close: %v", err)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(releases) != total {
				t.Fatalf("%s: %d distinct payloads released, want %d", arm.name, len(releases), total)
			}
			for p, n := range releases {
				if n != 1 {
					t.Fatalf("%s: a payload was released %d times, want exactly 1 (%p)", arm.name, n, p)
				}
			}
		})
	}
}
