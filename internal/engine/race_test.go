package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
)

// laneReport renders the per-lane accounting of a subscription, which makes a
// failed post-close assertion readable.
func laneReport(s *Subscription[int64]) string {
	var b strings.Builder
	now := s.eng.now()
	for i, ln := range s.lanes {
		ready, delayed := ln.store.Depth(now)
		rready, rdelayed := ln.retries.depth(now)
		ln.mu.Lock()
		live := ln.live.Len()
		ln.mu.Unlock()
		fmt.Fprintf(&b, "lane %d depth=%d ready=%d live=%d delayed=%d retry=%d/%d bytes=%d\n",
			i, ln.depth.Load(), ready, live, delayed, rready, rdelayed, ln.bytes.Load())
	}
	return b.String()
}

// settleErr reports whether err is one of the sentinels a losing settlement
// attempt may see.
func settleErr(err error) bool {
	return errors.Is(err, errcode.ErrLeaseLost) || errors.Is(err, errcode.ErrAlreadySettled) ||
		errors.Is(err, errcode.ErrSettlementInProgress) || errors.Is(err, errcode.ErrSettlementUnknown) ||
		errors.Is(err, errcode.ErrNotSettleable) || errors.Is(err, errcode.ErrClosed)
}

// raceRig is one engine under concurrent load, with the ledgers the race
// targets assert on.
type raceRig struct {
	eng   *Engine[int64]
	top   *Topic[int64]
	sub   *Subscription[int64]
	cons  *Consumer[int64]
	poll  context.Context
	pubs  atomic.Int64
	mu    sync.Mutex
	adm   map[int64]bool
	rel   map[int64]int
	won   map[[2]uint64]int
	fail  atomic.Bool
	cause atomic.Pointer[string]
}

// newRaceRig builds an engine with one topic, one subscription and one
// consumer.
func newRaceRig(t *testing.T, ks KindSet, partitions, capacity int, deadline time.Duration) *raceRig {
	t.Helper()
	eng, err := New(Config[int64]{
		Kinds: ks,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions:       partitions,
			CapacityMessages: capacity,
			AckDeadline:      deadline,
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
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	return &raceRig{
		eng: eng, top: top, sub: sub, cons: c, poll: poll,
		adm: map[int64]bool{}, rel: map[int64]int{}, won: map[[2]uint64]int{},
	}
}

// abort records the first failure seen by any actor.
func (rg *raceRig) abort(msg string) {
	if rg.fail.CompareAndSwap(false, true) {
		rg.cause.Store(&msg)
	}
}

// report fails the test when any actor aborted.
func (rg *raceRig) report(t *testing.T) {
	t.Helper()
	if rg.fail.Load() {
		t.Fatal(*rg.cause.Load())
	}
}

// publish admits one record and tracks both its admission and its release.
// Only a publication that returned nil transferred ownership of the payload to
// the engine, so only those are expected to come back through Release.
func (rg *raceRig) publish(ctx context.Context, size int) error {
	v := rg.pubs.Add(1)
	err := rg.top.Publish(ctx, Publish[int64]{
		Payload: v,
		Size:    size,
		Release: func(x int64) {
			rg.mu.Lock()
			rg.rel[x]++
			rg.mu.Unlock()
		},
	})
	if err == nil {
		rg.mu.Lock()
		rg.adm[v] = true
		rg.mu.Unlock()
	}
	return err
}

// win records a successful settlement of one delivery.
func (rg *raceRig) win(seq uint64, gen uint32) {
	key := [2]uint64{seq, uint64(gen)}
	rg.mu.Lock()
	rg.won[key]++
	n := rg.won[key]
	rg.mu.Unlock()
	if n > 1 {
		rg.abort("two settlements won the same delivery")
	}
}

// checkReleases asserts every admitted payload was released exactly once and
// that nothing else was released. Both halves matter: a lost release leaks the
// payload and permanently consumes the engine's message and byte budget, and a
// second release hands the same payload back twice.
func (rg *raceRig) checkReleases(t *testing.T) {
	t.Helper()
	rg.mu.Lock()
	defer rg.mu.Unlock()
	for v := int64(1); v <= rg.pubs.Load(); v++ {
		switch n := rg.rel[v]; {
		case rg.adm[v] && n != 1:
			t.Fatalf("admitted payload %d released %d times, want exactly 1\n%s",
				v, n, laneReport(rg.sub))
		case !rg.adm[v] && n != 0:
			t.Fatalf("payload %d was refused but released %d times", v, n)
		}
	}
}

// FuzzEngineSettle races settlement attempts against redelivery: one actor
// fetches and shares its claims, several actors settle them with live and
// forged generations, and one actor expires leases and closes consumers.
// Checked: exactly one attempt wins each delivery, every loser sees a
// settlement sentinel, and every payload is released exactly once.
func FuzzEngineSettle(f *testing.F) {
	for _, seed := range [][4]int{{2, 8, 16, 1}, {4, 16, 32, 0}, {1, 4, 8, 2}} {
		w := fuzzutil.NewWriter()
		w.Int(1, 6, seed[0])
		w.Int(1, 32, seed[1])
		w.Int(1, 64, seed[2])
		w.Int(1, 3, seed[3]+1)
		for i := range 96 {
			w.Byte(byte(i * 31))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		settlers := r.Int(1, 6)
		records := r.Int(1, 32)
		per := r.Int(1, 64)
		parts := r.Int(1, 3)
		sets := kindMatrix("one")
		rg := newRaceRig(t, sets[r.Int(0, len(sets)-1)], parts, 64, 5*time.Millisecond)
		defer func() { _ = rg.eng.Close() }()
		ctx := context.Background()
		for range records {
			if err := rg.publish(ctx, 8); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
		var mu sync.Mutex
		shared := make([]Claim[int64], 0, records)
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], settlers+2, func(id int, ar *fuzzutil.Reader) {
			switch id {
			case 0:
				dst := make([]Claim[int64], 4)
				for i := 0; i < per && !rg.fail.Load(); i++ {
					n, err := rg.cons.Fetch(rg.poll, dst)
					if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errcode.ErrClosed) {
						rg.abort("fetch: " + err.Error())
						return
					}
					mu.Lock()
					shared = append(shared, dst[:n]...)
					mu.Unlock()
					fuzzutil.MaybeYield(ar)
				}
			case 1:
				for i := 0; i < per && !rg.fail.Load(); i++ {
					rg.eng.Faults().ExpireLeases()
					fuzzutil.MaybeYield(ar)
				}
			default:
				for i := 0; i < per && !rg.fail.Load(); i++ {
					mu.Lock()
					n := len(shared)
					var cl Claim[int64]
					if n > 0 {
						cl = shared[ar.Int(0, n-1)]
					}
					mu.Unlock()
					if n == 0 {
						fuzzutil.MaybeYield(ar)
						continue
					}
					gen := cl.Gen
					switch ar.Choice(8) {
					case 0:
						gen--
					case 1:
						gen++
					}
					var err error
					switch ar.Choice(4) {
					case 0:
						err = rg.cons.Nack(cl.Env, gen, 0)
					case 1:
						err = rg.cons.Extend(cl.Env, gen, time.Millisecond)
						if err == nil {
							continue // an extension settles nothing
						}
					default:
						err = rg.cons.Ack(cl.Env, gen)
					}
					if err == nil {
						rg.win(cl.Info.Seq, gen)
						continue
					}
					if !settleErr(err) {
						rg.abort("unexpected settlement error: " + err.Error())
						return
					}
				}
			}
		})
		rg.report(t)
		if err := rg.eng.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		rg.checkReleases(t)
		snap := rg.sub.Inspect()
		if !snap.Conserved() {
			t.Fatalf("not conserved after close: %+v", snap)
		}
		if snap.Ready != 0 || snap.InFlight != 0 || snap.Delayed != 0 {
			t.Fatalf("records survived close: %+v\n%s", snap, laneReport(rg.sub))
		}
	})
}

// FuzzEngineCloseRace publishes, fetches and settles while another actor
// closes the engine. Checked: no operation panics, every error is one the
// engine may return, every operation after Close returns ErrClosed, every
// payload is released exactly once and nothing survives the close.
func FuzzEngineCloseRace(f *testing.F) {
	for _, seed := range [][3]int{{2, 16, 4}, {4, 32, 12}, {1, 8, 1}} {
		w := fuzzutil.NewWriter()
		w.Int(1, 6, seed[0])
		w.Int(1, 64, seed[1])
		w.Int(1, 64, seed[2])
		w.Int(1, 3, 1)
		for i := range 96 {
			w.Byte(byte(i*17 + 3))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		actors := r.Int(1, 6)
		per := r.Int(1, 64)
		closeAfter := r.Int(1, 64)
		parts := r.Int(1, 3)
		sets := kindMatrix("one")
		rg := newRaceRig(t, sets[r.Int(0, len(sets)-1)], parts, 32, time.Second)
		defer func() { _ = rg.eng.Close() }()
		ctx := context.Background()
		liveErr := func(err error) bool {
			return err == nil || settleErr(err) || errors.Is(err, errcode.ErrQueueFull) ||
				errors.Is(err, context.Canceled)
		}
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], actors+1, func(id int, ar *fuzzutil.Reader) {
			if id == 0 {
				for i := 0; i < closeAfter && !rg.fail.Load(); i++ {
					fuzzutil.MaybeYield(ar)
				}
				if err := rg.eng.Close(); err != nil {
					rg.abort("close: " + err.Error())
				}
				return
			}
			dst := make([]Claim[int64], 2)
			for i := 0; i < per && !rg.fail.Load(); i++ {
				switch ar.Choice(3) {
				case 0:
					if err := rg.publish(ctx, ar.Int(0, 32)); !liveErr(err) {
						rg.abort("publish: " + err.Error())
						return
					}
				case 1:
					n, err := rg.cons.Fetch(rg.poll, dst)
					if !liveErr(err) {
						rg.abort("fetch: " + err.Error())
						return
					}
					for j := range n {
						if err := rg.cons.Ack(dst[j].Env, dst[j].Gen); !liveErr(err) {
							rg.abort("ack: " + err.Error())
							return
						}
					}
				default:
					rg.eng.Faults().ExpireLeases()
				}
				fuzzutil.MaybeYield(ar)
			}
		})
		rg.report(t)
		if err := rg.publish(ctx, 8); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("publish after close: %v", err)
		}
		if _, err := rg.cons.Fetch(context.Background(), make([]Claim[int64], 1)); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("fetch after close: %v", err)
		}
		if _, err := rg.top.Subscribe("s2", SubscriptionConfig{}); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("subscribe after close: %v", err)
		}
		if _, err := rg.sub.Attach(ConsumerConfig{}); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("attach after close: %v", err)
		}
		rg.checkReleases(t)
		snap := rg.sub.Inspect()
		if snap.Ready != 0 || snap.InFlight != 0 || snap.Delayed != 0 {
			t.Fatalf("records survived close: %+v", snap)
		}
		if !snap.Conserved() {
			t.Fatalf("not conserved after close: %+v", snap)
		}
	})
}

// TestPublishRacesSubscriptionClose closes a subscription underneath live
// publishers. A publisher that loaded the topic's copy-on-write subscription
// snapshot before the close commits its record after Subscription.Close has
// already detached the subscription from the topic and drained its lanes, so
// no later close will ever visit those lanes again: the publisher itself has
// to drop what it just admitted. Checked, per round: every publication the
// engine accepted releases its payload exactly once, a refused one releases
// nothing, and the engine's message and byte totals return to zero — a leaked
// record consumes MaxTotalMessages and MaxTotalBytes budget for good.
func TestPublishRacesSubscriptionClose(t *testing.T) {
	const (
		rounds = 96
		perRun = 48
	)
	sets := make([]KindSet, 0, 3+len(Arches()))
	sets = append(sets, KindSet{}, KindSet{Safe: true},
		KindSet{Queue: queue.Chan, Pool: pool.FreeList, Wait: wait.Hybrid})
	// Every architecture is raced too: the shared log stamps its slot index
	// on the record under the log's own lock, and a claim that overtook the
	// stamp would settle the wrong slot.
	for _, a := range Arches() {
		sets = append(sets, KindSet{Arch: a})
	}
	for _, ks := range sets {
		t.Run(ks.String(), func(t *testing.T) {
			for round := range rounds {
				var mu sync.Mutex
				adm := map[int64]bool{}
				rel := map[int64]int{}
				eng, err := New(Config[int64]{
					Kinds: ks,
					Clone: func(v int64) int64 { return v },
					DefaultTopology: TopologyConfig{
						Partitions:       2,
						CapacityMessages: 128,
						AckDeadline:      time.Second,
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
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for i := range perRun {
						v := int64(i + 1)
						err := top.Publish(context.Background(), Publish[int64]{
							Payload:   v,
							Key:       []byte{byte(i)},
							Size:      8,
							Partition: -1,
							Release: func(x int64) {
								mu.Lock()
								rel[x]++
								mu.Unlock()
							},
						})
						if err != nil {
							t.Errorf("publish: %v", err)
							return
						}
						mu.Lock()
						adm[v] = true
						mu.Unlock()
					}
				}()
				go func() {
					defer wg.Done()
					// Stagger the close across the publishing window so that
					// every round lands on a different interleaving.
					for range round % 8 {
						runtime.Gosched()
					}
					if err := sub.Close(); err != nil {
						t.Errorf("close subscription: %v", err)
					}
				}()
				wg.Wait()
				if err := eng.Close(); err != nil {
					t.Fatalf("close engine: %v", err)
				}
				if t.Failed() {
					return
				}
				mu.Lock()
				var bad string
				for v := int64(1); v <= perRun; v++ {
					switch n := rel[v]; {
					case adm[v] && n != 1:
						bad = fmt.Sprintf("admitted payload %d released %d times, want exactly 1", v, n)
					case !adm[v] && n != 0:
						bad = fmt.Sprintf("payload %d was refused but released %d times", v, n)
					}
				}
				mu.Unlock()
				if bad != "" {
					t.Fatalf("round %d: %s\n%s", round, bad, laneReport(sub))
				}
				if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
					t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
				}
			}
		})
	}
}

// TestRetryDeadLetterUnderLoad drives every slice-3 path at once — publication
// under an overflow policy, redelivery through the retry placement, an attempt
// budget that dead-letters, and a dead-letter fault that makes the publication
// fail — from several goroutines, and checks afterwards that everything the
// engine admitted is accounted for and that nothing was released twice.
func TestRetryDeadLetterUnderLoad(t *testing.T) {
	for _, k := range RetryLanes() {
		for _, overflow := range []OverflowPolicy{OverflowReject, OverflowDropNewest, OverflowDropOldest} {
			t.Run(k.String()+"/"+overflow.String(), func(t *testing.T) {
				t.Parallel()
				raceRetryDeadLetter(t, k, overflow)
			})
		}
	}
}

func raceRetryDeadLetter(t *testing.T, k RetryLane, overflow OverflowPolicy) {
	t.Helper()
	ks := KindSet{RetryLane: k}
	e := newTestEngine(t, ks, TopologyConfig{
		Partitions:       2,
		CapacityMessages: 16,
		AckDeadline:      time.Millisecond,
		Overflow:         overflow,
	})
	if _, err := e.CreateTopic("dlq", TopologyConfig{CapacityMessages: -1}); err != nil {
		t.Fatal(err)
	}
	dsub, err := mustTopic(t, e, "dlq").Subscribe("d", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: 3, DeadLetterTopic: "dlq"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var released atomic.Int64
	var twice atomic.Int64
	seen := sync.Map{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	drain := func(s *Subscription[int64], nack bool) {
		c, err := s.Attach(ConsumerConfig{})
		if err != nil {
			t.Errorf("attach: %v", err)
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]Claim[int64], 4)
			for n := 0; ; n++ {
				got, err := c.Fetch(ctx, dst)
				if err != nil {
					return
				}
				for j := range got {
					cl := dst[j]
					var err error
					if nack && (n+j)%3 != 0 {
						err = c.Nack(cl.Env, cl.Gen, 0)
					} else {
						err = c.Ack(cl.Env, cl.Gen)
					}
					if err != nil && !settleErr(err) && !errors.Is(err, errcode.ErrClosed) {
						t.Errorf("settle: %v", err)
						return
					}
				}
			}
		}()
	}
	for range 3 {
		drain(sub, true)
	}
	drain(dsub, false)

	// A fault that keeps failing dead-letter publications, toggled while the
	// consumers run: a failed one must never settle its record.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil && i < 64; i++ {
			e.Faults().FailDeadLetter(i % 2)
			runtime.Gosched()
		}
		e.Faults().FailDeadLetter(0)
	}()

	for p := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 400 {
				v := int64(p*1000 + i)
				err := top.Publish(ctx, Publish[int64]{
					Payload: v, Size: 8, Partition: -1,
					Release: func(x int64) {
						if _, dup := seen.LoadOrStore(x, true); dup {
							twice.Add(1)
						}
						released.Add(1)
					},
				})
				if err != nil && !errors.Is(err, errcode.ErrQueueFull) &&
					!errors.Is(err, errcode.ErrClosed) && !errors.Is(err, context.Canceled) {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}
	// Let the consumers work through the backlog, then close everything.
	for range 50 {
		if snap := sub.Inspect(); snap.Published > 0 && snap.Acked+snap.DeadLettered+snap.Dropped > 0 {
			break
		}
		runtime.Gosched()
	}
	cancel()
	wg.Wait()
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := twice.Load(); n != 0 {
		t.Fatalf("%d payloads were released twice", n)
	}
	snap := sub.Inspect()
	if snap.Ready != 0 || snap.InFlight != 0 || snap.Delayed != 0 {
		t.Fatalf("closed subscription still holds records: %+v\n%s", snap, laneReport(sub))
	}
	if !snap.Conserved() {
		t.Fatalf("not conserved: %+v", snap)
	}
	if snap.DeadLettered > 0 && snap.DeadLetterFailures == 0 {
		t.Logf("no dead-letter publication failed in this run: %+v", snap)
	}
}

// mustTopic returns an existing topic or fails the test.
func mustTopic(t *testing.T, e *Engine[int64], name string) *Topic[int64] {
	t.Helper()
	top, err := e.Topic(name)
	if err != nil {
		t.Fatalf("topic %s: %v", name, err)
	}
	return top
}

// TestNackRacesSubscriptionClose races a negative acknowledgement against the
// close of its subscription and of its engine. Nack's decision to put the
// record back is taken outside the lane mutex and the placement lands in the
// lane's redelivery queue, which Subscription.drain has its own critical
// section for, so a nack that reads "not closed" can still place its record
// after the drain that was meant to collect it — into a lane that has been
// detached from its topic and that nothing will ever visit again. Checked per
// round: the payload's Release callback runs exactly once and the engine-wide
// message and byte reservations return to zero, because a record stranded in
// a drained lane consumes both for the life of the process.
//
// This is the publish-vs-close hole of on the settlement path; every
// route back into a lane is covered by the same re-check.
func TestNackRacesSubscriptionClose(t *testing.T) {
	for _, k := range RetryLanes() {
		for _, closer := range []string{"sub-close", "engine-close"} {
			t.Run(k.String()+"/"+closer, func(t *testing.T) {
				t.Parallel()
				raceNackClose(t, KindSet{RetryLane: k}, closer == "sub-close")
			})
		}
	}
}

// raceNackClose races a negative acknowledgement against a close on the kind
// set ks. It is called from here over every retry placement, and from
// TestModel/<arch>/ownership over every architecture: the architecture used to
// be whichever one the bake-off had bound, and this is the route
// the contributor notes's recurring defect class lives on
// (adr/ADR-017-assumptions.md).
func raceNackClose(t *testing.T, ks KindSet, subClose bool) {
	t.Helper()
	const rounds = 2000
	leaked := 0
	first := ""
	for round := range rounds {
		var mu sync.Mutex
		released := 0
		eng, err := New(Config[int64]{
			Kinds: ks,
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions:       1,
				CapacityMessages: 8,
				AckDeadline:      time.Second,
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
		c, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if err = top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 8, Partition: -1,
			Release: func(int64) {
				mu.Lock()
				released++
				mu.Unlock()
			},
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("round %d: fetch = %v", round, cl)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = c.Nack(cl[0].Env, cl[0].Gen, 0)
		}()
		go func() {
			defer wg.Done()
			<-start
			// Stagger the close across the nack so that every round lands on
			// a different interleaving.
			for range round % 4 {
				runtime.Gosched()
			}
			if subClose {
				_ = sub.Close()
				return
			}
			_ = eng.Close()
		}()
		close(start)
		wg.Wait()
		// Every door the API has, so that a leak cannot be a shutdown
		// ordering artefact.
		_ = c.Close()
		_ = sub.Close()
		_ = eng.Close()
		mu.Lock()
		n := released
		mu.Unlock()
		msgs, bytes, _ := eng.Totals()
		if n != 1 || msgs != 0 || bytes != 0 {
			leaked++
			if first == "" {
				first = fmt.Sprintf("round %d: payload released %d times (want 1); "+
					"engine totals msgs=%d bytes=%d\n%s", round, n, msgs, bytes, laneReport(sub))
			}
		}
	}
	if leaked > 0 {
		t.Fatalf("%d/%d rounds leaked a record nacked while it closed\n%s", leaked, rounds, first)
	}
}

// TestStrictLaneNeverOpensWhileARedeliveryReturns is the concurrent half of
// the specification's strict-blocks-lane gate. Under OrderingStrict a redelivery
// blocks its lane: nothing published behind it is delivered until it has been
// settled or terminally routed. The record leaves the in-flight list and is
// placed in the lane's redelivery queue in separate steps, so a lane that
// gave its seal back before the placement briefly looked both empty and open
// to a concurrent claim — and served the record published behind the
// redelivery.
//
// One consumer takes record 1, fails it, takes the redelivery (which now
// holds the lane's seal) and fails it again while a second consumer polls.
// Record 2 must never come out while record 1 is unsettled.
func TestStrictLaneNeverOpensWhileARedeliveryReturns(t *testing.T) {
	for _, k := range RetryLanes() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			raceStrictSeal(t, k)
		})
	}
}

func raceStrictSeal(t *testing.T, k RetryLane) {
	t.Helper()
	const rounds = 3000
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a poll: Fetch takes what is there and never waits
	overtaken := 0
	for round := range rounds {
		ks := referenceKinds()
		ks.RetryLane = k
		eng, err := New(Config[int64]{
			Kinds: ks,
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions:       1,
				CapacityMessages: 16,
				AckDeadline:      -1,
				Ordering:         OrderingStrict,
				Overflow:         OverflowReject,
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
		a, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		b, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		for i := 1; i <= 2; i++ {
			if err = top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 4}); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		dst := make([]Claim[int64], 1)
		n, _ := a.Fetch(ctx, dst)
		if n != 1 || dst[0].Info.Seq != 1 {
			t.Fatalf("round %d: first fetch = %v", round, seqs(dst[:n]))
		}
		if err = a.Nack(dst[0].Env, dst[0].Gen, 0); err != nil {
			t.Fatalf("round %d: first nack: %v", round, err)
		}
		// The redelivery is now outstanding and holds the lane's seal.
		if n, _ = a.Fetch(ctx, dst); n != 1 || dst[0].Info.Seq != 1 {
			t.Fatalf("round %d: the sealed lane served %v, want the redelivery", round, seqs(dst[:n]))
		}
		cl := dst[0]
		start := make(chan struct{})
		var served atomic.Uint64
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = a.Nack(cl.Env, cl.Gen, 0)
		}()
		go func() {
			defer wg.Done()
			buf := make([]Claim[int64], 1)
			<-start
			for range 128 {
				if m, _ := b.Fetch(ctx, buf); m > 0 {
					served.Store(buf[0].Info.Seq)
					return
				}
			}
		}()
		close(start)
		wg.Wait()
		if got := served.Load(); got > 1 {
			overtaken++
		}
		if err = eng.Close(); err != nil {
			t.Fatalf("round %d: close: %v", round, err)
		}
	}
	if overtaken > 0 {
		t.Fatalf("%d/%d rounds served the record published behind an outstanding redelivery", overtaken, rounds)
	}
}

// streamRaceRounds is how many interleavings the stream close races drive.
// The class they cover — a record admitted into a structure a concurrent
// close has already emptied — shows up at a handful of failures per thousand
// rounds, so a hundred proves nothing.
const streamRaceRounds = 3000

// TestRetainRacesEngineClose closes an engine underneath live publishers on a
// stream topic. A publication that passed Topic.publish's look at the closed
// flag reaches the retained log after Engine.Close has already emptied every
// log — which it does exactly once — so the publication itself has to drop
// what it just retained. Checked, per round: every payload the engine
// accepted is released exactly once, and the engine's totals return to zero.
func TestRetainRacesEngineClose(t *testing.T) {
	const publishers = 4
	for round := range streamRaceRounds {
		var mu sync.Mutex
		rel := map[int64]int{}
		var accepted atomic.Int64
		eng, err := New(Config[int64]{
			// A stream: pinned, because a shared partition log has no way to
			// admit a replayed record.
			Kinds: KindSet{Arch: featureArch},
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 128, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 64},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(publishers + 1)
		for g := range publishers {
			go func() {
				defer wg.Done()
				for i := range 8 {
					v := int64(g*8 + i + 1)
					err := top.Publish(context.Background(), Publish[int64]{
						Payload: v, Size: 1, Partition: 0, Mode: CopyOwned,
						Release: func(x int64) {
							mu.Lock()
							rel[x]++
							mu.Unlock()
						},
					})
					if err == nil {
						accepted.Add(1)
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			for range round % 7 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		if err := eng.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
		if t.Failed() {
			return
		}
		mu.Lock()
		released := 0
		bad := ""
		for v, n := range rel {
			released += n
			if n != 1 {
				bad = fmt.Sprintf("payload %d released %d times", v, n)
			}
		}
		mu.Unlock()
		if bad != "" {
			t.Fatalf("round %d: %s", round, bad)
		}
		if got := int(accepted.Load()); released != got {
			t.Fatalf("round %d: %d payloads accepted, %d released (leaked %d)",
				round, got, released, got-released)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
		}
	}
}

// TestSeekRacesSubscriptionClose seeks a stream subscription while it closes.
// Subscription.Seek looks at the closed flag, drains and then admits the
// replayed records; a Close landing in between has already drained the lane
// and detached the subscription, so nothing visits it again. Checked, per
// round: the engine's message and byte reservations return to zero — a leaked
// one consumes MaxTotalMessages and MaxTotalBytes budget for good — and every
// payload is released exactly once.
func TestSeekRacesSubscriptionClose(t *testing.T) {
	const retained = 16
	for round := range streamRaceRounds {
		var mu sync.Mutex
		rel := map[int64]int{}
		eng, err := New(Config[int64]{
			// A stream: pinned, as above.
			Kinds: KindSet{Arch: featureArch},
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 128, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: retained},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		for i := range retained {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i + 1), Size: 1, Partition: 0, Mode: CopyOwned,
				Release: func(x int64) {
					mu.Lock()
					rel[x]++
					mu.Unlock()
				},
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 8 {
				if _, err := sub.Seek(StartPosition{Kind: StartEarliest}); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range round % 5 {
				runtime.Gosched()
			}
			if err := sub.Close(); err != nil {
				t.Errorf("close subscription: %v", err)
			}
		}()
		wg.Wait()
		if err := eng.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
		if t.Failed() {
			return
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine reservations leaked msgs=%d bytes=%d", round, msgs, bytes)
		}
		mu.Lock()
		bad := ""
		for v := int64(1); v <= retained; v++ {
			if rel[v] != 1 {
				bad = fmt.Sprintf("payload %d released %d times, want once", v, rel[v])
			}
		}
		mu.Unlock()
		if bad != "" {
			t.Fatalf("round %d: %s", round, bad)
		}
	}
}

// TestReplayRacesRetentionTrim replays a stream while publications trim the
// records the replay is reading. A retained record the log has let go of is
// reset and handed to the engine's record pool, so a replay that reads its
// fields — or clones its payload — after the log released it is a data race
// and a use-after-free. It runs on a recycling pool kind, because the default
// (pool.None) never reuses a record and so never exposes it.
func TestReplayRacesRetentionTrim(t *testing.T) {
	// A stream: the architecture is pinned, because a shared partition log
	// has no way to admit a replayed record. The pool kind
	// is what this test varies.
	for _, ks := range []KindSet{
		{Arch: featureArch, Pool: pool.SyncPool},
		{Arch: featureArch, Pool: pool.SyncPool, Safe: true},
	} {
		t.Run(ks.String(), func(t *testing.T) {
			for range streamRaceRounds / 6 {
				eng, err := New(Config[int64]{
					Kinds: ks,
					Clone: func(v int64) int64 { return v },
					DefaultTopology: TopologyConfig{
						Partitions: 1, CapacityMessages: 256, AckDeadline: time.Second,
					},
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				top, err := eng.CreateTopic("t", TopologyConfig{
					Kind: TopologyStream, Retention: Retention{MaxMessages: 8},
				})
				if err != nil {
					t.Fatalf("CreateTopic: %v", err)
				}
				sub, err := top.Subscribe("s", SubscriptionConfig{})
				if err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
				c, err := sub.Attach(ConsumerConfig{})
				if err != nil {
					t.Fatalf("Attach: %v", err)
				}
				var stop atomic.Bool
				var wg sync.WaitGroup
				wg.Add(3)
				go func() {
					defer wg.Done()
					for i := range 64 {
						_ = top.Publish(context.Background(), Publish[int64]{
							Payload: int64(i + 1), Size: 1, Partition: 0,
						})
					}
					stop.Store(true)
				}()
				go func() {
					defer wg.Done()
					for !stop.Load() {
						if _, err := sub.Seek(StartPosition{Kind: StartEarliest}); err != nil {
							return
						}
					}
				}()
				go func() {
					defer wg.Done()
					dst := make([]Claim[int64], 8)
					for !stop.Load() {
						n, err := c.Fetch(poll(t), dst)
						if err != nil && !errors.Is(err, context.Canceled) {
							return
						}
						for i := range n {
							_ = c.Ack(dst[i].Env, dst[i].Gen)
						}
					}
				}()
				wg.Wait()
				if err := eng.Close(); err != nil {
					t.Fatalf("close engine: %v", err)
				}
				if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
					t.Fatalf("engine totals after close: msgs=%d bytes=%d", msgs, bytes)
				}
			}
		})
	}
}

// duplicateRaceRounds and refusalRaceRounds are how many interleavings the
// two lane-admission close races below drive. They cover the same defect
// class as streamRaceRounds — a record put into a lane a concurrent close has
// already emptied — and the same arithmetic applies: the window is a handful
// of instructions wide, so the failures come at single digits per thousand
// rounds and a hundred rounds proves nothing.
const (
	duplicateRaceRounds = 3000
	refusalRaceRounds   = 3000
)

// dupCloseRig is one engine, topic, subscription and consumer built for the
// lane-admission close races, together with the release ledger the rounds
// assert on. The ledger is per round, so a leak is reported against the round
// that produced it.
type dupCloseRig struct {
	eng      *Engine[int64]
	top      *Topic[int64]
	sub      *Subscription[int64]
	cons     *Consumer[int64]
	mu       sync.Mutex
	rel      map[int64]int
	accepted atomic.Int64
}

// newDupCloseRig builds the engine one round runs on.
//
// The architecture is pinned rather than left to ArchAuto: Faults.duplicate
// is a documented no-op on a shared partition log (Arch.Shared), so an Auto
// that resolved to LogCursor would leave the duplicate races pinning
// nothing. DebugOwnership turns a second release of one payload into a panic
// instead of a silently corrupted ledger, which is the other half of the
// conservation the rounds check.
func newDupCloseRig(t *testing.T, partitions, capacity int, sticky bool) *dupCloseRig {
	t.Helper()
	eng, err := New(Config[int64]{
		Kinds:          KindSet{Arch: PerSubEnvelopes, Pool: pool.SyncPool},
		Clone:          func(v int64) int64 { return v },
		DebugOwnership: true,
		DefaultTopology: TopologyConfig{
			Partitions:       partitions,
			CapacityMessages: capacity,
			AckDeadline:      time.Second,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: sticky})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return &dupCloseRig{eng: eng, top: top, sub: sub, cons: c, rel: map[int64]int{}}
}

// publish admits one payload and tracks its release. Only a publication that
// returned nil handed the payload to the engine, so only those are owed a
// Release call.
func (rg *dupCloseRig) publish(v int64, partition int) {
	err := rg.top.Publish(context.Background(), Publish[int64]{
		Payload: v, Size: 1, Partition: partition, Mode: CopyOwned,
		Release: func(x int64) {
			rg.mu.Lock()
			rg.rel[x]++
			rg.mu.Unlock()
		},
	})
	if err == nil {
		rg.accepted.Add(1)
	}
}

// conserved closes the engine and asserts the round conserved everything the
// engine took ownership of: every accepted payload released exactly once, no
// payload released that was never accepted, and the engine-wide message and
// byte reservations back at zero. A record left in a lane a close has already
// emptied fails all three — its Release never runs and its reservation is
// consumed for the life of the process.
func (rg *dupCloseRig) conserved(t *testing.T, round int) {
	t.Helper()
	if err := rg.eng.Close(); err != nil {
		t.Fatalf("round %d: close engine: %v", round, err)
	}
	if t.Failed() {
		t.FailNow()
	}
	rg.mu.Lock()
	released, bad := 0, ""
	for v, n := range rg.rel {
		released += n
		if n != 1 {
			bad = fmt.Sprintf("payload %d released %d times, want exactly 1", v, n)
		}
	}
	rg.mu.Unlock()
	if bad != "" {
		t.Fatalf("round %d: %s\n%s", round, bad, laneReport(rg.sub))
	}
	if got := int(rg.accepted.Load()); released != got {
		t.Fatalf("round %d: accepted %d payloads, released %d (leaked %d)\n%s",
			round, got, released, got-released, laneReport(rg.sub))
	}
	if msgs, bytes, _ := rg.eng.Totals(); msgs != 0 || bytes != 0 {
		t.Fatalf("round %d: engine reservations leaked after close: msgs=%d bytes=%d\n%s",
			round, msgs, bytes, laneReport(rg.sub))
	}
}

// TestDuplicateRacesEngineClose closes an engine and its subscription
// underneath a consumer whose every delivery is duplicated
// (Faults.DuplicateDelivery). lane.duplicate is an admission like any other:
// it runs with the lane mutex released, because the reservation it takes may
// have to be given back and a successful one wakes the subscription's
// waiters. So it owes the admission discipline twice over — it must not
// admit after a close, and, because a close can land between its look at the
// closed flags and the placement, it must look again afterwards and re-drain.
// Without either look the duplicate lands in a lane the close has already
// emptied and nothing ever visits it again: the payload's Release never runs
// and its engine-wide message and byte reservation is consumed for good.
//
// Checked, per round: every accepted payload is released exactly once and the
// engine's totals return to zero.
//
// What each look is worth is not symmetric. Dropping the re-drain fails this
// within a few hundred rounds, and so does dropping both looks. Dropping only
// the entry guard does not: the re-drain runs in the same goroutine straight
// after the placement and empties every lane of the subscription, so an
// admission the entry guard would have refused is one the re-drain takes back
// again. The entry guard is real discipline — it is what stops the engine
// doing an admission's work after a close, and it is the half that survives
// if the placement ever grows a route the re-drain cannot reach — but it has
// no failure of its own to pin while the re-drain stands.
func TestDuplicateRacesEngineClose(t *testing.T) {
	const publishers = 3
	const perPublisher = 6
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	for round := range duplicateRaceRounds {
		rg := newDupCloseRig(t, 1, 256, false)
		rg.eng.Faults().DuplicateDelivery(-1)
		other, err := rg.sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(publishers + 3)
		for g := range publishers {
			go func() {
				defer wg.Done()
				for i := range perPublisher {
					rg.publish(int64(g*perPublisher+i+1), 0)
				}
			}()
		}
		for _, c := range [...]*Consumer[int64]{rg.cons, other} {
			go func() {
				defer wg.Done()
				dst := make([]Claim[int64], 1)
				for range publishers * perPublisher * 2 {
					if _, err := c.Fetch(poll, dst); err != nil {
						if errors.Is(err, errcode.ErrClosed) {
							return
						}
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			// Close on the duplicate stream rather than at a fixed offset
			// from the start of the round: the window this pins is the one
			// between lane.duplicate's look at the closed flags and its
			// placement, so the close has to land while duplicates are
			// being admitted, not before the first fetch has run.
			for i, want := 0, int64(round%12); i < 4096; i++ {
				if rg.eng.Faults().Duplicates() >= want {
					break
				}
				runtime.Gosched()
			}
			// Half the rounds close the subscription and half the engine.
			// The two windows differ: Subscription.Close sets the flag and
			// drains at once, while Engine.Close walks its topics in
			// between, so a duplicate that has to land after the drain has
			// a different amount of room in each.
			if round%2 == 0 {
				if err := rg.sub.Close(); err != nil {
					t.Errorf("close subscription: %v", err)
				}
			} else if err := rg.eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		rg.conserved(t, round)
	}
}

// TestLeaseRefusalRacesSubscriptionClose races a fetch against a rebalance
// and a close. lane.claim takes a batch of envelopes out of the lane's store
// and then leases them one at a time under the lane mutex; a rebalance that
// lands in between moves the lane to another consumer, and the lease has to
// refuse and put the envelope back. That placement runs with the lane mutex
// released — it has to, because it may end in a drop, which runs the
// publisher's release callback — so a close that lands under it has already
// drained this lane and will never visit it again. The refusal therefore has
// to go through the route that looks at the closed flags again
// (lane.requeueOrDrop) rather than place unconditionally (lane.requeue):
// a bare requeue puts the envelope back into an emptied lane and leaks it.
//
// Ownership is flipped by membership rather than by Subscription.Rebalance
// alone, because a sticky assignment is a function of the membership: the
// same members always deal out the same table, so only a join or a leave
// actually moves a lane.
//
// Checked, per round: every accepted payload is released exactly once and the
// engine's totals return to zero.
func TestLeaseRefusalRacesSubscriptionClose(t *testing.T) {
	const partitions = 4
	const records = 32
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	for round := range refusalRaceRounds {
		rg := newDupCloseRig(t, partitions, 256, true)
		for i := range records {
			rg.publish(int64(i+1), i%partitions)
		}
		second, err := rg.sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(4)
		for _, c := range [...]*Consumer[int64]{rg.cons, second} {
			go func() {
				defer wg.Done()
				// A wide batch is the point: lane.claim takes the batch out of
				// the store in one call and then leases it one envelope at a
				// time, so the later envelopes of a batch spend the longest
				// outside the store, which is the window a close can land in.
				dst := make([]Claim[int64], records)
				for range 48 {
					n, err := c.Fetch(poll, dst)
					// Every claim goes straight back, so the lanes never run
					// dry and the fetch is inside lane.claim for most of the
					// round rather than spinning on empty lanes: the close has
					// to land while a batch is out of the store.
					for i := range n {
						if e := c.Nack(dst[i].Env, dst[i].Gen, 0); e != nil && !settleErr(e) {
							t.Errorf("nack: %v", e)
							return
						}
					}
					if err != nil && errors.Is(err, errcode.ErrClosed) {
						return
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			for range 24 {
				c, err := rg.sub.Attach(ConsumerConfig{})
				if err != nil {
					return
				}
				if err := c.Close(); err != nil {
					t.Errorf("close consumer: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range round % 8 {
				runtime.Gosched()
			}
			if round%2 == 0 {
				if err := rg.sub.Close(); err != nil {
					t.Errorf("close subscription: %v", err)
				}
			} else if err := rg.eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		rg.conserved(t, round)
	}
}

// sweepRig is a lane that a time-to-live sweep is due on: one record that has
// expired by the time the sweep runs, and survivors behind it that have not.
// The survivors are what keeps the sweep inside its window: on an
// architecture whose store expires by draining (expireByDrain), the sweep
// takes every record out of the store and puts the survivors back, all under
// the lane mutex, and the more it has to move the longer the window that
// anything not taking that mutex can run into.
type sweepRig struct {
	eng      *Engine[int64]
	top      *Topic[int64]
	sub      *Subscription[int64]
	released atomic.Int64
	// sweepAt is a time past the expiring record's TTL. The survivors have
	// none.
	sweepAt int64
}

// sweepSurvivors is how many records outlive the sweep of a sweepRig.
const sweepSurvivors = 1024

// newSweepRig builds a sweepRig on arch with topo as the engine's default
// topology and tc as the topic's. The lane is left holding the expiring
// record and sweepSurvivors survivors, none of them in flight.
func newSweepRig(t *testing.T, arch Arch, topo, tc TopologyConfig) *sweepRig {
	t.Helper()
	eng, err := New(Config[int64]{
		Kinds:           KindSet{Arch: arch},
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: topo,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", tc)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	rg := &sweepRig{eng: eng, top: top, sub: sub, sweepAt: eng.now() + int64(2*time.Hour)}
	release := func(int64) { rg.released.Add(1) }
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 0, Size: 1, Mode: CopyOwned, TTL: time.Hour, Release: release,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i := range sweepSurvivors {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(i + 1), Size: 1, Mode: CopyOwned, Release: release,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	return rg
}

// race runs the rig's sweep against op. The sweep is staggered behind op by
// a different amount each round: op has to reach the lane first and then the
// store while the sweep holds it empty.
func (rg *sweepRig) race(round int, op func()) {
	ln := rg.sub.lanes[0]
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range round % 8 {
			runtime.Gosched()
		}
		ln.expireTTL(rg.sweepAt)
	}()
	go func() {
		defer wg.Done()
		<-start
		op()
	}()
	close(start)
	wg.Wait()
}

// sweepRounds is how many interleavings each sweep race drives.
const sweepRounds = 200

// TestTTLSweepRacesSubscriptionClose races a time-to-live sweep against the
// close of its subscription and of its engine. A close that drained the store
// while the sweep held it empty found nothing there, so the survivors went
// back into a lane that had been detached and that nothing would visit again:
// their payloads were never released, and they held the engine-wide message
// and byte reservations for the life of the process. Checked per round: every
// payload is released exactly once and the engine's totals return to zero.
func TestTTLSweepRacesSubscriptionClose(t *testing.T) {
	for _, arch := range Arches() {
		for _, closer := range []string{"sub-close", "engine-close"} {
			t.Run(arch.String()+"/"+closer, func(t *testing.T) {
				t.Parallel()
				raceSweepClose(t, arch, closer == "sub-close")
			})
		}
	}
}

// raceSweepClose is one case of TestTTLSweepRacesSubscriptionClose.
func raceSweepClose(t *testing.T, arch Arch, subClose bool) {
	t.Helper()
	leaked := 0
	first := ""
	for round := range sweepRounds {
		rg := newSweepRig(t, arch, TopologyConfig{
			Partitions: 1, CapacityMessages: 2 * sweepSurvivors, AckDeadline: -1,
		}, TopologyConfig{})
		rg.race(round, func() {
			if subClose {
				_ = rg.sub.Close()
				return
			}
			_ = rg.eng.Close()
		})
		_ = rg.sub.Close()
		_ = rg.eng.Close()
		msgs, bytes, _ := rg.eng.Totals()
		// Under FullCopy the publisher's payload goes back at admission, so
		// the count is what the release callback can say for every
		// architecture; the totals are what a stranded copy keeps booked.
		if n := rg.released.Load(); n != sweepSurvivors+1 || msgs != 0 || bytes != 0 {
			leaked++
			if first == "" {
				first = fmt.Sprintf("round %d: %d of %d payloads released; engine totals msgs=%d bytes=%d\n%s",
					round, n, sweepSurvivors+1, msgs, bytes, laneReport(rg.sub))
			}
		}
	}
	if leaked > 0 {
		t.Fatalf("%d/%d rounds left records in a closed subscription\n%s", leaked, sweepRounds, first)
	}
}

// TestTTLSweepRacesSeek seeks a stream subscription back to its earliest
// record while a time-to-live sweep runs on it. Seek drains the subscription
// and replays the retained log into it, so a drain that found the store empty
// while the sweep held it left the survivors to come back beside their own
// replayed copies: every one of them was delivered twice.
func TestTTLSweepRacesSeek(t *testing.T) {
	const retained = sweepSurvivors + 1
	doubled := 0
	first := ""
	for round := range sweepRounds {
		rg := newSweepRig(t, featureArch, TopologyConfig{
			Partitions: 1, CapacityMessages: 4 * retained, AckDeadline: -1,
		}, TopologyConfig{Kind: TopologyStream, Retention: Retention{MaxMessages: retained}})
		var serr error
		rg.race(round, func() {
			_, serr = rg.sub.Seek(StartPosition{Kind: StartEarliest})
		})
		if serr != nil {
			t.Fatalf("round %d: Seek: %v", round, serr)
		}
		// The sweep may land after the replay and expire the replayed copy
		// of the expiring record, so one fewer is right too.
		if snap := rg.sub.Inspect(); snap.Ready > retained {
			doubled++
			if first == "" {
				first = fmt.Sprintf("round %d: %d records ready after a seek over %d retained\n%s",
					round, snap.Ready, retained, laneReport(rg.sub))
			}
		}
		if err := rg.eng.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if doubled > 0 {
		t.Fatalf("%d/%d seeks left records the sweep put back beside their replay\n%s", doubled, sweepRounds, first)
	}
}

// holdLikeASweep puts the lane where a time-to-live sweep on an architecture
// that expires by draining has it halfway through (expireByDrain): the lane
// mutex held and every record out of the store. The records are still the
// lane's and still booked against it. resume puts them back as the sweep
// does, in the order they came out, and releases the mutex.
//
// A race against the real sweep reaches the store empty only between its
// last take and its first put-back, because a concurrent claim competes with
// the drain rather than finding nothing, so it is held here instead.
func holdLikeASweep(ln *lane[int64]) (resume func()) {
	ln.mu.Lock()
	held := ln.store.Drain(nil)
	return func() {
		now := ln.sub.eng.now()
		for _, e := range held {
			ln.store.Requeue(e, e.deadline, now)
		}
		ln.mu.Unlock()
	}
}

// sweepWait is how long a test gives an operation that must wait out a held
// sweep to show that it did not: one that acts on the emptied store returns
// well inside it.
const sweepWait = 50 * time.Millisecond

// TestPurgeWaitsOutATTLSweep purges a subscription while a time-to-live sweep
// has the store's records out. A purge that claimed from the store then
// found it empty and returned, and the survivors came back afterwards: the
// subscription stayed full of the records it was asked to discard.
func TestPurgeWaitsOutATTLSweep(t *testing.T) {
	for _, arch := range []Arch{PerSubEnvelopes, ChanFanout, FullCopy} {
		t.Run(arch.String(), func(t *testing.T) {
			t.Parallel()
			rg := newSweepRig(t, arch, TopologyConfig{
				Partitions: 1, CapacityMessages: 2 * sweepSurvivors, AckDeadline: -1,
			}, TopologyConfig{})
			defer func() { _ = rg.eng.Close() }()
			resume := holdLikeASweep(rg.sub.lanes[0])
			done := make(chan int, 1)
			go func() { done <- rg.sub.Purge() }()
			select {
			case n := <-done:
				resume()
				t.Fatalf("Purge returned %d while the sweep held the records out; %d records ready after it",
					n, rg.sub.Inspect().Ready)
			case <-time.After(sweepWait):
			}
			resume()
			if n := <-done; n != sweepSurvivors+1 {
				t.Fatalf("Purge = %d, want %d", n, sweepSurvivors+1)
			}
			if snap := rg.sub.Inspect(); snap.Ready != 0 {
				t.Fatalf("%d records ready after the purge\n%s", snap.Ready, laneReport(rg.sub))
			}
		})
	}
}

// TestDisplacementWaitsOutATTLSweep publishes into a full lane, under a
// policy that displaces the oldest record, while a time-to-live sweep has the
// store's records out. Every record the lane holds is ready, so there is
// always one to displace. A publication that looked for it while the sweep
// held the store empty found none and was refused with ErrQueueFull, as if
// every record were in flight.
//
// The DropSlow fan-out goes straight to the displacement, which is how the
// hold reaches it. OverflowDropOldest first asks whether the drops would make
// room (lane.roomAfterDrops), and that look takes the lane mutex, so a hold
// placed before the publication only delays it; a sweep that starts between
// the look and the displacement lands in the same window, and the case is
// here so that the look is not mistaken for the fix.
func TestDisplacementWaitsOutATTLSweep(t *testing.T) {
	policies := []struct {
		name     string
		fanout   FanoutPolicy
		overflow OverflowPolicy
	}{
		{"drop-slow", DropSlow, OverflowReject},
		{"drop-oldest", Atomic, OverflowDropOldest},
	}
	for _, arch := range []Arch{PerSubEnvelopes, ChanFanout, FullCopy} {
		for _, pol := range policies {
			t.Run(arch.String()+"/"+pol.name, func(t *testing.T) {
				t.Parallel()
				rg := newSweepRig(t, arch, TopologyConfig{
					Partitions: 1, CapacityMessages: sweepSurvivors + 1, AckDeadline: -1,
					Fanout: pol.fanout, Overflow: pol.overflow,
				}, TopologyConfig{})
				defer func() { _ = rg.eng.Close() }()
				resume := holdLikeASweep(rg.sub.lanes[0])
				done := make(chan error, 1)
				go func() {
					done <- rg.top.Publish(context.Background(), Publish[int64]{Payload: -1, Size: 1})
				}()
				select {
				case err := <-done:
					resume()
					t.Fatalf("Publish returned %v while the sweep held the records out", err)
				case <-time.After(sweepWait):
				}
				resume()
				if err := <-done; err != nil {
					t.Fatalf("Publish into a full lane of ready records: %v", err)
				}
				if snap := rg.sub.Inspect(); snap.Ready != sweepSurvivors+1 || snap.Dropped != 1 {
					t.Fatalf("after the publication: %+v", snap)
				}
			})
		}
	}
}

// TestBlockedPublishRacesSubscriptionClose closes the full subscription a
// publisher under OverflowBlock is about to wait on, while another
// subscription of the topic has room. A publisher that parks first is woken
// by the close, re-reads the subscription set and is admitted to the one
// left. One that reached the wait after the close was refused with
// ErrClosed instead, on a topic that is still open: ErrClosed is not
// retryable, so the caller gave up a publication the topic would have taken.
func TestBlockedPublishRacesSubscriptionClose(t *testing.T) {
	const rounds = 2000
	refused := 0
	first := ""
	for round := range rounds {
		eng, err := New(Config[int64]{
			Kinds: KindSet{Arch: featureArch},
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, AckDeadline: -1, Overflow: OverflowBlock,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		a, err := top.Subscribe("a", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe(a): %v", err)
		}
		b, err := top.Subscribe("b", SubscriptionConfig{CapacityMessages: 1})
		if err != nil {
			t.Fatalf("Subscribe(b): %v", err)
		}
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		start := make(chan struct{})
		var perr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			perr = top.Publish(ctx, Publish[int64]{Payload: 2, Size: 1})
		}()
		go func() {
			defer wg.Done()
			<-start
			// Stagger the close across the publication, so that it lands
			// before, inside and after the reservation pass.
			for range round % 8 {
				runtime.Gosched()
			}
			_ = b.Close()
		}()
		close(start)
		wg.Wait()
		if perr != nil {
			refused++
			if first == "" {
				first = fmt.Sprintf("round %d: Publish = %v, subscription a holds %+v", round, perr, a.Inspect())
			}
		} else if snap := a.Inspect(); snap.Ready != 2 {
			t.Fatalf("round %d: subscription a holds %+v after both publications were taken", round, snap)
		}
		if err := eng.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if refused > 0 {
		t.Fatalf("%d/%d publications to an open topic were refused when a full subscription closed\n%s",
			refused, rounds, first)
	}
}
