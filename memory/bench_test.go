package memory_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/memory"
)

// The routes are measured in one package, by one harness, under identical
// conditions, and the benchmark's first dimension is which of them ran:
//
//	BenchmarkMemory/PubSubAck/api/1P1C/bytes=64/reuse=on
//	BenchmarkMemory/PubSubAck/spi/1P1C/bytes=64/reuse=on
//	BenchmarkMemory/PubSubAck/engine/1P1C/bytes=64/reuse=on
//
// There are three and not two because two cannot answer the question. The
// `api` route is the whole public path; `engine` is the engine on its own;
// and `spi` is this driver's Sink, Source and Settler with the core's handles
// taken out — the same mapping of a *mq.Message onto an engine publication
// and of a claim onto a DriverDelivery, reached without the publisher's
// admission ticket, the consumer's settlement record or either one's
// counters. So `spi` minus `engine` is what THIS increment costs and `api`
// minus `spi` is what the core above it costs, which is the difference
// between a number and a diagnosis.
//
// The engine's own the specification gives this name's first segment to the copy
// mode ; here it is the route, because that is what this
// row decides and because a family's benchmark root selects a kind out of the
// segment after the root.
//
// What differs between the two routes is exactly the layer this increment
// added plus the core above it: the publisher's admission ticket, option
// resolution and counters; the sink's mapping of a *mq.Message onto an engine
// publication; the source's filling of a DriverDelivery from a claim; and the
// consumer's settlement record, cell and counters. Nothing else: both routes
// publish in the same copy mode, into the same topology, with the same bodies.
//
// The `reuse` dimension is the core's settlement-record discipline
// (ConsumerConfig.BufferReuse), so it moves the api rows and is a no-op on
// the engine rows, which have no settlement record to recycle. The engine
// rows are measured under both spellings anyway, so that the grid is
// rectangular and a reader comparing one api row against its engine row never
// has to find the row it belongs with.
func BenchmarkMemory(b *testing.B) {
	b.Run("PubSubAck", func(b *testing.B) {
		for _, route := range []string{"api", "spi", "engine"} {
			b.Run(route, func(b *testing.B) {
				for _, sh := range pubSubShapes() {
					b.Run(sh.name, func(b *testing.B) {
						for _, size := range pubSubBodies() {
							b.Run("bytes="+strconv.Itoa(size), func(b *testing.B) {
								for _, reuse := range []bool{true, false} {
									name := "reuse=off"
									if reuse {
										name = "reuse=on"
									}
									b.Run(name, func(b *testing.B) {
										runPubSubAck(b, route, sh, size, reuse)
									})
								}
							})
						}
					})
				}
			})
		}
	})
}

func BenchmarkRouteBatch(b *testing.B) {
	for _, route := range []string{"api", "spi", "engine"} {
		b.Run(route, func(b *testing.B) {
			for _, sh := range pubSubShapes() {
				b.Run(sh.name, func(b *testing.B) {
					for _, size := range pubSubBodies() {
						b.Run("bytes="+strconv.Itoa(size), func(b *testing.B) {
							b.Run("batch="+strconv.Itoa(routeBatch), func(b *testing.B) {
								for _, reuse := range []bool{true, false} {
									name := "reuse=off"
									if reuse {
										name = "reuse=on"
									}
									b.Run(name, func(b *testing.B) {
										runRouteBatch(b, route, sh, size, reuse)
									})
								}
							})
						})
					}
				})
			}
		})
	}
}

// routeBatch is how many publications one timed iteration makes. A hundred is
// `benchmarks/opbench`'s figure for the same reason: it is enough to amortise
// the loop and the timer without making one iteration long enough to matter
// against the three-second per-benchmark ceiling (a hundred 64-byte cycles is
// about 170 µs here).
const routeBatch = 100

// runRouteBatch measures one row. It is runPubSubAck's body with a batched
// timed loop; the two are deliberately separate functions rather than one
// with a count parameter, because runPubSubAck's own comment pins "one
// publication per iteration whatever the shape" as what makes -benchmem
// divide a shape's allocations by that shape's operation count, and this function breaks that on purpose.
func runRouteBatch(b *testing.B, route string, sh pubSubShape, size int, reuse bool) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var settled atomic.Int64
	var consumers sync.WaitGroup
	var rig pubSubRig
	switch route {
	case "api":
		rig = newAPIRig(b, sh, size, reuse)
	case "spi":
		rig = newSPIRig(b, sh, size)
	case "engine":
		rig = newEngineRig(b, sh, size)
	default:
		b.Fatalf("unknown route %q", route)
	}
	defer rig.stop()
	for id := range sh.cons {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			rig.consume(ctx, id, &settled)
		}()
	}
	var stalled atomic.Bool
	watchPubSubStall(ctx, cancel, &settled, &stalled)

	var failMu sync.Mutex
	var failure error
	fail := func(err error) {
		failMu.Lock()
		if failure == nil {
			failure = err
		}
		failMu.Unlock()
	}
	tokens := make(chan struct{}, sh.pubs)
	var publishers sync.WaitGroup
	if sh.pubs > 1 {
		for id := range sh.pubs {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				for range tokens {
					for range routeBatch {
						if err := rig.publish(ctx, id); err != nil {
							fail(err)
							return
						}
					}
				}
			}()
		}
	}
	single := sh.pubs == 1
	for b.Loop() {
		if single {
			for range routeBatch {
				if err := rig.publish(ctx, 0); err != nil {
					fail(err)
					break
				}
			}
			continue
		}
		select {
		case tokens <- struct{}{}:
		case <-ctx.Done():
		}
	}
	b.StopTimer()
	close(tokens)
	publishers.Wait()
	cancel()
	consumers.Wait()
	if stalled.Load() {
		b.Fatalf("no record settled for %v: the run measured nothing", pubSubStall)
	}
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, mq.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		msgs := float64(settled.Load())
		b.ReportMetric(msgs/secs, "msgs/s")
		b.ReportMetric(msgs*float64(size)/secs, "bytes/s")
		// ns per MESSAGE, which is the figure the 3 % comparison is about:
		// ns/op here is a hundred of them and would not be comparable with
		// BenchmarkMemory/PubSubAck's rows at all.
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(max(msgs, 1)), "ns/msg")
	}
}

// pubSubShape is one concurrency shape.
type pubSubShape struct {
	name string
	pubs int
	cons int
}

// pubSubShapes are the shapes measured. 1P1C is the shape the throughput
// criterion is taken from and the one the 3 % comparison is decided on; 4P4C
// is there so that the answer is not a statement about an uncontended path
// alone.
func pubSubShapes() []pubSubShape {
	return []pubSubShape{{"1P1C", 1, 1}, {"4P4C", 4, 4}}
}

// pubSubBodies is the body size the specification states its figures at.
func pubSubBodies() []int { return []int{64} }

// pubSubDepth is the lane capacity a shape runs with. It is deep enough that
// a publisher and a consumer of the same lane do not hand the lane back and
// forth on every record, and bounded because an unbounded lane would measure
// the chunked queue's growth instead of the path.
func pubSubDepth(pubs int) int { return 1024 * pubs }

// pubSubStall is how long the harness waits for a settlement before it
// declares the run stuck. A run that settles nothing measures nothing, and a
// benchmark that reports a number for it is worse than one that fails.
const pubSubStall = 5 * time.Second

// runPubSubAck measures one row.
func runPubSubAck(b *testing.B, route string, sh pubSubShape, size int, reuse bool) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var settled atomic.Int64
	var consumers sync.WaitGroup
	var rig pubSubRig
	switch route {
	case "api":
		rig = newAPIRig(b, sh, size, reuse)
	case "spi":
		rig = newSPIRig(b, sh, size)
	case "engine":
		rig = newEngineRig(b, sh, size)
	default:
		b.Fatalf("unknown route %q", route)
	}
	defer rig.stop()
	for id := range sh.cons {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			rig.consume(ctx, id, &settled)
		}()
	}
	var stalled atomic.Bool
	watchPubSubStall(ctx, cancel, &settled, &stalled)

	var failMu sync.Mutex
	var failure error
	fail := func(err error) {
		failMu.Lock()
		if failure == nil {
			failure = err
		}
		failMu.Unlock()
	}
	tokens := make(chan struct{}, sh.pubs)
	var publishers sync.WaitGroup
	if sh.pubs > 1 {
		for id := range sh.pubs {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				for range tokens {
					if err := rig.publish(ctx, id); err != nil {
						fail(err)
						return
					}
				}
			}()
		}
	}
	// One publication per iteration whatever the shape, which is what makes
	// -benchmem divide one shape's allocations by that shape's operation
	// count. The body carries no b.Fatalf and no early
	// return other than the recorded break.
	single := sh.pubs == 1
	for b.Loop() {
		if single {
			if err := rig.publish(ctx, 0); err != nil {
				fail(err)
				break
			}
			continue
		}
		select {
		case tokens <- struct{}{}:
		case <-ctx.Done():
		}
	}
	b.StopTimer()
	close(tokens)
	publishers.Wait()
	cancel()
	consumers.Wait()
	if stalled.Load() {
		b.Fatalf("no record settled for %v: the run measured nothing", pubSubStall)
	}
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, mq.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		msgs := float64(settled.Load())
		b.ReportMetric(msgs/secs, "msgs/s")
		b.ReportMetric(msgs*float64(size)/secs, "bytes/s")
	}
}

// pubSubRig is one route's publish and consume half.
type pubSubRig struct {
	publish func(ctx context.Context, id int) error
	consume func(ctx context.Context, id int, settled *atomic.Int64)
	stop    func()
}

// benchTopology is the topology both routes run on. It is the same struct on
// both sides: a single lane, a deep bounded capacity, the blocking overflow
// policy — a refusal would let a route post its throughput for refusing
// rather than for delivering  — and a lease long enough
// that nothing expires inside the run.
func benchTopology(pubs int) memory.TopologyConfig {
	return memory.TopologyConfig{
		Partitions:       1,
		CapacityMessages: pubSubDepth(pubs),
		Overflow:         memory.OverflowBlock,
		AckDeadline:      time.Hour,
	}
}

// newAPIRig builds the public-API route: a client over the memory driver,
// one bound publisher and one consumer per shape slot.
func newAPIRig(b *testing.B, sh pubSubShape, size int, reuse bool) pubSubRig {
	b.Helper()
	ctx := context.Background()
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate:      true,
		DefaultTopology: benchTopology(sh.pubs),
	})
	if err != nil {
		b.Fatalf("NewBroker: %v", err)
	}
	client, err := memory.OpenWithClient(ctx, broker, memory.ClientOwned)
	if err != nil {
		b.Fatalf("OpenWithClient: %v", err)
	}
	cons := make([]*mq.ConsumerHandle, sh.cons)
	for i := range cons {
		c, cerr := client.Consumer(ctx, mq.ConsumerConfig{
			Topic:        benchTopic,
			Subscription: benchSub,
			Prefetch:     benchPrefetch,
			BufferReuse:  reuse,
		})
		if cerr != nil {
			b.Fatalf("Consumer: %v", cerr)
		}
		cons[i] = c
	}
	pubs := make([]*mq.PublisherHandle, sh.pubs)
	bodies := make([][]byte, sh.pubs)
	for i := range pubs {
		p, perr := client.Publisher(ctx, mq.PublisherConfig{Topic: benchTopic})
		if perr != nil {
			b.Fatalf("Publisher: %v", perr)
		}
		pubs[i] = p
		bodies[i] = bytes.Repeat([]byte{byte('a' + i)}, size)
	}
	msgs := make([]*mq.Message, sh.pubs)
	for i := range msgs {
		msgs[i] = &mq.Message{Key: []byte(benchKey), Body: bodies[i]}
	}
	return pubSubRig{
		publish: func(ctx context.Context, id int) error {
			return pubs[id].Publish(ctx, msgs[id])
		},
		consume: func(ctx context.Context, id int, settled *atomic.Int64) {
			c := cons[id]
			dst := make([]mq.Delivery, benchPrefetch)
			for ctx.Err() == nil {
				n, err := c.Receive(ctx, dst)
				for i := range n {
					if aerr := dst[i].Ack(); aerr == nil {
						settled.Add(1)
					}
				}
				if err != nil && n == 0 {
					return
				}
			}
		},
		stop: func() { _ = client.Close() },
	}
}

// newSPIRig builds the driver's own route: the Sink, the Source and the
// Settler this increment wrote, with the core's handles taken out.
//
// It is the same driver the api route uses — the same broker, the same
// topology, the same mapping — so what separates the two is exactly the core:
// the publisher's admission ticket, option resolution and counters, and the
// consumer's fetch ticket, settlement record, settlement cell and counters.
// The reuse dimension does not reach it: the settlement record it would
// recycle is the core's.
func newSPIRig(b *testing.B, sh pubSubShape, size int) pubSubRig {
	b.Helper()
	ctx := context.Background()
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate:      true,
		DefaultTopology: benchTopology(sh.pubs),
	})
	if err != nil {
		b.Fatalf("NewBroker: %v", err)
	}
	srcs := make([]mq.Source, sh.cons)
	for i := range srcs {
		src, serr := broker.OpenSource(ctx, mq.SourceConfig{
			Topic:        benchTopic,
			Subscription: benchSub,
			Prefetch:     benchPrefetch,
			Start:        mq.StartLatest(),
		})
		if serr != nil {
			b.Fatalf("OpenSource: %v", serr)
		}
		srcs[i] = src
	}
	sinks := make([]mq.Sink, sh.pubs)
	msgs := make([]*mq.Message, sh.pubs)
	for i := range sinks {
		sink, kerr := broker.OpenSink(ctx, mq.SinkConfig{Topic: benchTopic})
		if kerr != nil {
			b.Fatalf("OpenSink: %v", kerr)
		}
		sinks[i] = sink
		msgs[i] = &mq.Message{
			Key:  []byte(benchKey),
			Body: bytes.Repeat([]byte{byte('a' + i)}, size),
		}
	}
	var opts mq.PublishOptions
	return pubSubRig{
		publish: func(ctx context.Context, id int) error {
			return sinks[id].Publish(ctx, msgs[id], &opts)
		},
		consume: func(ctx context.Context, id int, settled *atomic.Int64) {
			src := srcs[id]
			st := src.Settler()
			dst := make([]mq.DriverDelivery, benchPrefetch)
			for ctx.Err() == nil {
				n, err := src.Fetch(ctx, dst)
				for i := range n {
					if aerr := st.Ack(ctx, &dst[i]); aerr == nil {
						settled.Add(1)
					}
				}
				if err != nil && n == 0 {
					return
				}
			}
		},
		stop: func() { _ = broker.Close() },
	}
}

// newEngineRig builds the engine-direct route: the same engine the driver
// wraps, with the same payload type and the same topology, reached without
// the driver or the core.
func newEngineRig(b *testing.B, sh pubSubShape, size int) pubSubRig {
	b.Helper()
	topo, err := memory.EngineTopologyForBench(benchTopology(sh.pubs))
	if err != nil {
		b.Fatalf("topology: %v", err)
	}
	eng, err := engine.New(engine.Config[*mq.Message]{
		DefaultTopology: topo,
		Clone:           func(m *mq.Message) *mq.Message { return m.Clone() },
		CopyMode:        engine.CopySafe,
		Receive:         engine.ReceiveShared,
	})
	if err != nil {
		b.Fatalf("engine.New: %v", err)
	}
	topic, err := eng.CreateTopic(benchTopic, topo)
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	sub, err := topic.Subscribe(benchSub, engine.SubscriptionConfig{})
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	cons := make([]*engine.Consumer[*mq.Message], sh.cons)
	for i := range cons {
		c, cerr := sub.Attach(engine.ConsumerConfig{Prefetch: benchPrefetch})
		if cerr != nil {
			b.Fatalf("Attach: %v", cerr)
		}
		cons[i] = c
	}
	msgs := make([]*mq.Message, sh.pubs)
	for i := range msgs {
		msgs[i] = &mq.Message{
			Key:  []byte(benchKey),
			Body: bytes.Repeat([]byte{byte('a' + i)}, size),
		}
	}
	key := []byte(benchKey)
	return pubSubRig{
		publish: func(ctx context.Context, id int) error {
			m := msgs[id]
			return topic.Publish(ctx, engine.Publish[*mq.Message]{
				Payload:   m,
				Key:       key,
				Size:      len(key) + len(m.Body),
				Partition: -1,
				Mode:      engine.CopySafe,
			})
		},
		consume: func(ctx context.Context, id int, settled *atomic.Int64) {
			c := cons[id]
			claims := make([]engine.Claim[*mq.Message], benchPrefetch)
			for ctx.Err() == nil {
				n, err := c.Fetch(ctx, claims)
				for i := range n {
					if aerr := c.Ack(claims[i].Env, claims[i].Gen); aerr == nil {
						settled.Add(1)
					}
				}
				if err != nil && n == 0 {
					return
				}
			}
		},
		stop: func() { _ = eng.Close() },
	}
}

// The names and sizes both routes share, so that neither is measured against
// a different workload.
const (
	benchTopic    = "orders"
	benchSub      = "billing"
	benchKey      = "cust-42"
	benchPrefetch = 64
)

// watchPubSubStall fails a run that settles nothing. It is the guard against
// the worst kind of benchmark result: a number produced by a path that was
// not doing the work.
func watchPubSubStall(ctx context.Context, cancel context.CancelFunc, settled *atomic.Int64, stalled *atomic.Bool) {
	go func() {
		last := settled.Load()
		t := time.NewTicker(pubSubStall)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := settled.Load()
				if now == last {
					stalled.Store(true)
					cancel()
					return
				}
				last = now
			}
		}
	}()
}
