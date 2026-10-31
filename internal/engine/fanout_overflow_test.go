package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// This file holds the fan-out policies to the overflow policies they meet: a
// publisher under OverflowBlock behind a fan-out that admits wherever a
// record fits, and DropSlow's displacement under byte limits and the
// engine-wide limit.

// fanTopic builds a per-subscription topic of e with one subscription, and
// one consumer, for each of cfgs, named a, b and so on.
func fanTopic(t *testing.T, e *Engine[int64], cfgs ...SubscriptionConfig) (
	*Topic[int64], []*Subscription[int64], []*Consumer[int64],
) {
	t.Helper()
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subs := make([]*Subscription[int64], len(cfgs))
	cs := make([]*Consumer[int64], len(cfgs))
	for i, cfg := range cfgs {
		if subs[i], err = top.Subscribe(string(rune('a'+i)), cfg); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if cs[i], err = subs[i].Attach(ConsumerConfig{}); err != nil {
			t.Fatalf("Attach: %v", err)
		}
	}
	return top, subs, cs
}

// isNeverFits reports whether err is ErrNeverFits itself. errors.Is matches an
// errcode sentinel by its code, which ErrNeverFits shares with ErrQueueFull.
func isNeverFits(err error) bool {
	var e *errcode.Error
	return errors.As(err, &e) && e == ErrNeverFits
}

// parkPublish starts a publication of p on its own goroutine, waits until it
// has parked, and returns its outcome and the cancel that ends its wait. The
// caller runs inside a synctest bubble.
func parkPublish(t *testing.T, top *Topic[int64], p Publish[int64]) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- top.Publish(ctx, p) }()
	synctest.Wait()
	select {
	case err := <-done:
		cancel()
		t.Fatalf("the publisher did not park: %v", err)
	default:
	}
	return done, cancel
}

// TestADropSlowPublisherDisplacesOnceALeaseEndsUnsettled: a DropSlow
// publisher under OverflowBlock that a lease held up waits for it, and a lease
// can end without an Ack: a Nack, an expiry or the consumer's Close puts the
// record back where a drop reaches it. Only an Ack woke the parked publisher,
// so it stayed parked behind a record a publication arriving a moment later
// displaced at once, until its context ended.
func TestADropSlowPublisherDisplacesOnceALeaseEndsUnsettled(t *testing.T) {
	ends := []struct {
		name     string
		deadline time.Duration
		end      func(c *Consumer[int64], cl Claim[int64]) error
	}{
		{"nack", -1, func(c *Consumer[int64], cl Claim[int64]) error { return c.Nack(cl.Env, cl.Gen, 0) }},
		{"close", -1, func(c *Consumer[int64], _ Claim[int64]) error { return c.Close() }},
		{"expiry", time.Second, func(*Consumer[int64], Claim[int64]) error {
			time.Sleep(2 * time.Second)
			return nil
		}},
	}
	for _, arch := range []Arch{LogCursor, PerSubEnvelopes} {
		for _, tc := range ends {
			t.Run(arch.String()+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					e := newTestEngine(t, KindSet{Arch: arch}, TopologyConfig{
						Partitions: 1, CapacityMessages: 1, AckDeadline: tc.deadline,
						Overflow: OverflowBlock, Fanout: DropSlow,
					})
					top, sub, c := queueSub(t, e)
					if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
						t.Fatal(err)
					}
					cl := fetchSeqs(t, c, 1)
					if len(cl) != 1 {
						t.Fatalf("fetch = %v", cl)
					}
					done, cancel := parkPublish(t, top, Publish[int64]{Payload: 1, Size: 1})
					defer cancel()
					if err := tc.end(c, cl[0]); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					select {
					case err := <-done:
						if err != nil {
							t.Fatalf("the parked publication: %v", err)
						}
					default:
						t.Fatalf("the publisher stayed parked after the lease ended (%s), behind a record "+
							"DropSlow may displace: %+v", tc.name, sub.Inspect())
					}
					if snap := sub.Inspect(); snap.Dropped != 1 || snap.Published != 2 || !snap.Conserved() {
						t.Fatalf("after the displacement: %+v, want the first record dropped for the second", snap)
					}
				})
			})
		}
	}
}

// waitHookCtx is a context whose Err runs fn once, the first time a
// publisher's wait for room asks it, which is after the reservation pass
// that refused the publication and before the wait registers on its gates.
type waitHookCtx struct {
	context.Context
	once sync.Once
	fn   func()
}

func (c *waitHookCtx) Err() error {
	if calledFrom("(*Topic[", ").waitAnyRoom") || calledFrom("(*Topic[", ").waitLogRoom") {
		c.once.Do(c.fn)
	}
	return c.Context.Err()
}

// TestADropSlowPublisherSeesALeaseEndJustBeforeItParks: the record a lease
// gives back unsettled can come back between the displacement a DropSlow
// publisher tried and its registration on the gate it parks on, and the wake
// for it is then lost. The wait's last look is what catches it, so the look
// has to include the room a displacement would now make: it read the room the
// lane or the log had and nothing else, and the publisher parked behind a
// record it may displace with nothing left to wake it.
func TestADropSlowPublisherSeesALeaseEndJustBeforeItParks(t *testing.T) {
	for _, arch := range []Arch{LogCursor, PerSubEnvelopes} {
		t.Run(arch.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: arch}, TopologyConfig{
					Partitions: 1, CapacityMessages: 1, AckDeadline: -1,
					Overflow: OverflowBlock, Fanout: DropSlow,
				})
				top, sub, c := queueSub(t, e)
				if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
					t.Fatal(err)
				}
				cl := fetchSeqs(t, c, 1)
				if len(cl) != 1 {
					t.Fatalf("fetch = %v", cl)
				}
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &waitHookCtx{Context: base, fn: func() {
					if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
						t.Errorf("Nack: %v", err)
					}
				}}
				done := make(chan error, 1)
				go func() { done <- top.Publish(ctx, Publish[int64]{Payload: 1, Size: 1}) }()
				synctest.Wait()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("the publication: %v", err)
					}
				default:
					t.Fatalf("the publisher parked behind a record it may displace, given back as it began "+
						"to wait: %+v", sub.Inspect())
				}
				if snap := sub.Inspect(); snap.Dropped != 1 || snap.Published != 2 || !snap.Conserved() {
					t.Fatalf("after the displacement: %+v, want the first record dropped for the second", snap)
				}
			})
		})
	}
}

// TestAPartialFanOutPublisherWaitsForRoomInAnySubscription: Independent,
// BestEffort and DropSlow admit a record wherever it fits, and under
// OverflowBlock a publication none of the subscriptions had room for waits
// until one of them has. It waited on the first subscription that refused it
// alone, so room in any other subscription woke nobody, and a publisher
// behind a subscription that never reads stayed parked for ever beside one
// that had drained.
func TestAPartialFanOutPublisherWaitsForRoomInAnySubscription(t *testing.T) {
	for _, fan := range []FanoutPolicy{Independent, BestEffort, DropSlow} {
		t.Run(fan.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: PerSubEnvelopes}, TopologyConfig{
					Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock, Fanout: fan,
				})
				top, subs, cs := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
				if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
					t.Fatal(err)
				}
				// Both subscriptions hold the record on a lease, which DropSlow
				// cannot displace either.
				var held [2]Claim[int64]
				for i, c := range cs {
					cl := fetchSeqs(t, c, 1)
					if len(cl) != 1 {
						t.Fatalf("fetch %d = %v", i, cl)
					}
					held[i] = cl[0]
				}
				done, cancel := parkPublish(t, top, Publish[int64]{Payload: 1, Size: 1})
				defer cancel()
				// b makes room; a, the first subscription to refuse, never does.
				if err := cs[1].Ack(held[1].Env, held[1].Gen); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, errcode.ErrPartial) {
						t.Fatalf("the parked publication: %v", err)
					}
				default:
					t.Fatalf("the publisher stayed parked after b had room: b %+v", subs[1].Inspect())
				}
				if snap := subs[1].Inspect(); snap.Published != 2 || snap.Ready != 1 {
					t.Fatalf("b after the wait: %+v, want the record", snap)
				}
				if snap := subs[0].Inspect(); snap.Published != 1 || snap.Rejected != 1 {
					t.Fatalf("a after the wait: %+v, want the refusal counted", snap)
				}
			})
		})
	}
}

// TestAPartialFanOutPublisherIsRefusedOnlyForARecordNoSubscriptionCanHold: a
// publisher under OverflowBlock is refused rather than parked when the record
// can never fit, and under a fan-out that admits wherever the record fits
// that means none of its subscriptions could ever hold it. The first
// subscription to refuse decided it alone, so a record larger than its byte
// capacity was refused while another subscription, full only for now, would
// have taken it once it drained.
func TestAPartialFanOutPublisherIsRefusedOnlyForARecordNoSubscriptionCanHold(t *testing.T) {
	for _, fan := range []FanoutPolicy{Independent, BestEffort, DropSlow} {
		t.Run(fan.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: PerSubEnvelopes}, TopologyConfig{
					Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock, Fanout: fan,
				})
				top, subs, cs := fanTopic(t, e, SubscriptionConfig{CapacityBytes: 8}, SubscriptionConfig{CapacityBytes: 128})
				if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
					t.Fatal(err)
				}
				cl := fetchSeqs(t, cs[1], 1)
				if len(cl) != 1 {
					t.Fatalf("fetch = %v", cl)
				}
				done, cancel := parkPublish(t, top, Publish[int64]{Payload: 1, Size: 64})
				defer cancel()
				if err := cs[1].Ack(cl[0].Env, cl[0].Gen); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, errcode.ErrPartial) {
						t.Fatalf("a 64-byte record b can hold once it drains: %v", err)
					}
				default:
					t.Fatalf("the publisher stayed parked after b had room: b %+v", subs[1].Inspect())
				}
				if snap := subs[1].Inspect(); snap.Published != 2 {
					t.Fatalf("b after the wait: %+v, want the record", snap)
				}
				// A record neither subscription can ever hold is still refused
				// at once.
				err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 256})
				if !errors.Is(err, errcode.ErrQueueFull) || !isNeverFits(err) {
					t.Fatalf("a record no subscription can ever hold: %v, want ErrNeverFits", err)
				}
			})
		})
	}
}

// TestANeverFittingPublicationIsRefusedWithErrNeverFits: a publication no
// settlement can ever make room for, a record larger than the lane or the
// log, or an Atomic one whose records, one for each subscription, the
// engine-wide limit can never hold, was refused with the ErrQueueFull a full
// lane gets, which tells its caller to retry. Every retry got the same
// answer. It is refused with ErrNeverFits, still an ErrQueueFull by code,
// under OverflowBlock and OverflowReject alike, and a lane that is only full
// for now still answers ErrQueueFull itself.
func TestANeverFittingPublicationIsRefusedWithErrNeverFits(t *testing.T) {
	cases := []struct {
		name     string
		lane     int64 // the lane's (or the log's) byte capacity
		maxTotal int   // the engine-wide record limit
		size     int
	}{
		{"larger-than-the-lane", 8, 0, 64},
		{"atomic-over-the-engine-limit", 0, 1, 1},
	}
	for _, arch := range []Arch{PerSubEnvelopes, LogCursor} {
		for _, ov := range []OverflowPolicy{OverflowBlock, OverflowReject} {
			for _, tc := range cases {
				t.Run(arch.String()+"/"+ov.String()+"/"+tc.name, func(t *testing.T) {
					e, err := New(Config[int64]{
						Kinds: KindSet{Arch: arch}, MaxTotalMessages: tc.maxTotal,
						Clone: func(v int64) int64 { return v },
						DefaultTopology: TopologyConfig{
							Partitions: 1, CapacityMessages: 1, CapacityBytes: tc.lane, AckDeadline: -1,
							Overflow: ov, Fanout: Atomic,
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = e.Close() })
					top, _, _ := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					err = top.Publish(ctx, Publish[int64]{Payload: 1, Size: tc.size})
					if !errors.Is(err, errcode.ErrQueueFull) || !isNeverFits(err) {
						t.Fatalf("a publication that can never fit: %v, want ErrNeverFits", err)
					}
				})
			}
		}
	}
	t.Run("full-for-now", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{
			Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowReject,
		})
		top, _, _ := queueSub(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
		err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1})
		if !errors.Is(err, errcode.ErrQueueFull) || isNeverFits(err) {
			t.Fatalf("a publication to a lane that is full for now: %v, want ErrQueueFull itself", err)
		}
	})
}

// TestDropSlowUnderAByteLimitDropsUntilTheRecordFits: DropSlow "makes room in
// a full subscription by dropping its oldest record, so it never refuses
// while it holds anything". Under a byte limit one drop of a small record
// does not make room for a larger one, and the policy dropped exactly one,
// found the reservation still refused and refused the publication with the
// record already gone. It drops until the record fits now, and drops nothing
// when the leases leave no room whatever it drops, as DropOldest does.
func TestDropSlowUnderAByteLimitDropsUntilTheRecordFits(t *testing.T) {
	cases := []struct {
		name     string
		arch     Arch
		lane     int64 // the subscription's (or the log's) byte capacity
		maxTotal int64 // the engine-wide byte limit
	}{
		{"persub-lane-bytes", PerSubEnvelopes, 10, 0},
		{"persub-engine-bytes", PerSubEnvelopes, 0, 10},
		{"logcursor-log-bytes", LogCursor, 10, 0},
		{"logcursor-engine-bytes", LogCursor, 0, 10},
	}
	for _, tc := range cases {
		build := func(t *testing.T) (*Topic[int64], *Subscription[int64], *Consumer[int64]) {
			t.Helper()
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: tc.arch}, MaxTotalBytes: tc.maxTotal,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 64, CapacityBytes: tc.lane, AckDeadline: -1,
					Overflow: OverflowReject, Fanout: DropSlow,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			return queueSub(t, e)
		}
		t.Run(tc.name, func(t *testing.T) {
			top, sub, _ := build(t)
			for i := range 10 {
				if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			err := top.Publish(t.Context(), Publish[int64]{Payload: 100, Size: 5})
			snap := sub.Inspect()
			if err != nil {
				t.Fatalf("a 5-byte publication was refused (%v) while the subscription held ten droppable "+
					"records, %d of them dropped for it: %+v", err, snap.Dropped, snap)
			}
			if snap.Dropped != 5 || snap.Ready != 6 || snap.Bytes != 10 || !snap.Conserved() {
				t.Fatalf("after the publication: %+v, want the five oldest records dropped for it", snap)
			}
		})
		t.Run(tc.name+"/leased", func(t *testing.T) {
			// Eight of the ten bytes are out on leases, so a three-byte record
			// does not fit whatever the subscription drops.
			top, sub, c := build(t)
			for i := range 2 {
				if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 4}); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			if cl := fetchSeqs(t, c, 2); len(cl) != 2 {
				t.Fatalf("fetched %d records, want 2", len(cl))
			}
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1}); err != nil {
				t.Fatal(err)
			}
			err := top.Publish(t.Context(), Publish[int64]{Payload: 3, Size: 3})
			if !errors.Is(err, errcode.ErrQueueFull) {
				t.Fatalf("a publication the leases leave no room for: %v", err)
			}
			if snap := sub.Inspect(); snap.Dropped != 0 || snap.Ready != 1 || !snap.Conserved() {
				t.Fatalf("the subscription dropped a record for a refused publication: %+v", snap)
			}
		})
	}
}

// TestDropSlowServesTheSubscriptionKeepingUpWhenTheDropsServeOnlySome: under
// DropSlow with an engine-wide limit another topic shares, the slow
// subscription's drops make room for one of the two subscriptions taking the
// record. The lanes booked that room in subscription order, so the slow one,
// first in the set, dropped a record, took the new one itself, and the one
// keeping up was refused while the publisher heard nil. The subscription that
// held nothing to drop books first now. The shared log admits a publication
// to every subscription or to none, so there the publication is refused and
// nothing is dropped for it.
func TestDropSlowServesTheSubscriptionKeepingUpWhenTheDropsServeOnlySome(t *testing.T) {
	for _, arch := range Arches() {
		t.Run(arch.String(), func(t *testing.T) {
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: arch}, MaxTotalMessages: 4,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 64, AckDeadline: -1,
					Overflow: OverflowReject, Fanout: DropSlow,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, subs, cs := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
			slow, fast := subs[0], subs[1]
			for i := range 2 {
				if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
					t.Fatal(err)
				}
			}
			// fast keeps up; slow leases m0 and sits on it, and m1 waits.
			for _, cl := range fetchSeqs(t, cs[1], 2) {
				if err := cs[1].Ack(cl.Env, cl.Gen); err != nil {
					t.Fatal(err)
				}
			}
			if cl := fetchSeqs(t, cs[0], 1); len(cl) != 1 {
				t.Fatalf("slow fetch = %v", cl)
			}
			// Another topic takes the two free engine-wide records.
			other, err := e.CreateTopic("other", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := other.Subscribe("o", SubscriptionConfig{}); err != nil {
				t.Fatal(err)
			}
			for i := range 2 {
				if err := other.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
					t.Fatalf("other %d: %v", i, err)
				}
			}
			perr := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1})
			fs, ss := fast.Inspect(), slow.Inspect()
			if arch.Shared() {
				if !errors.Is(perr, errcode.ErrQueueFull) || ss.Dropped != 0 || fs.Rejected != 1 || ss.Rejected != 1 {
					t.Fatalf("Publish = %v; fast %+v, slow %+v; want the publication refused for both and "+
						"nothing dropped for it", perr, fs, ss)
				}
				return
			}
			if perr != nil || fs.Published != 3 || fs.Rejected != 0 {
				t.Fatalf("Publish = %v; the fast subscription was refused the record (fast %+v) while the slow "+
					"one dropped and took it (slow %+v)", perr, fs, ss)
			}
			if ss.Published != 2 || ss.Rejected != 1 || ss.Dropped != 1 || ss.InFlight != 1 || !ss.Conserved() {
				t.Fatalf("the slow subscription: %+v, want m1 dropped for the record and the record refused", ss)
			}
		})
	}
}

// TestASharedLogDropSlowSparesABaseThatWouldNotMakeRoom: a full shared log
// reclaims from its base, and the slots from the oldest leased one on stay
// whatever is dropped. DropSlow gave up the base for a record those slots
// leave no room for, and refused the publication with the base gone.
func TestASharedLogDropSlowSparesABaseThatWouldNotMakeRoom(t *testing.T) {
	top, subs, cs := sharedTopic(t, TopologyConfig{
		Partitions: 1, CapacityMessages: 64, CapacityBytes: 10, AckDeadline: -1,
		Overflow: OverflowReject, Fanout: DropSlow,
	})
	for i, size := range []int{1, 8} {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: size}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	// a settles the base and leases the eight-byte slot behind it; b holds
	// both and reads neither.
	cl := fetchSeqs(t, cs[0], 1)
	if len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	if err := cs[0].Ack(cl[0].Env, cl[0].Gen); err != nil {
		t.Fatal(err)
	}
	if cl = fetchSeqs(t, cs[0], 1); len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 3})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("a record the leased slot leaves no room for: %v", err)
	}
	if snap := subs[1].Inspect(); snap.Dropped != 0 || snap.Ready != 2 || !snap.Conserved() {
		t.Fatalf("b gave up the base for a refused publication: %+v", snap)
	}
}

// TestDropSlowAtTheEngineLimitTakesTheRoomFromTheSubscriptionsThatHoldRecords:
// under DropSlow "a subscription that is not keeping up loses its backlog
// rather than its publisher". With an engine-wide limit the slow
// subscription's backlog fills, the per-subscription architectures booked the
// limit lane by lane in subscription order and let each lane drop only its own
// records: the slow subscription took the last free record, the fast one,
// holding nothing to drop, was refused, and the publisher was told nil. The
// shared log under the same configuration has every subscription give up its
// oldest record in turn, and delivers to both. Every architecture now does
// that.
func TestDropSlowAtTheEngineLimitTakesTheRoomFromTheSubscriptionsThatHoldRecords(t *testing.T) {
	for _, arch := range Arches() {
		t.Run(arch.String(), func(t *testing.T) {
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: arch}, MaxTotalMessages: 2,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 64, AckDeadline: -1,
					Overflow: OverflowReject, Fanout: DropSlow,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, subs, cs := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
			slow, fast := subs[0], subs[1]
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
				t.Fatal(err)
			}
			// fast keeps up; slow never reads.
			cl := fetchSeqs(t, cs[1], 1)
			if len(cl) != 1 {
				t.Fatalf("fetch = %v", cl)
			}
			if err := cs[1].Ack(cl[0].Env, cl[0].Gen); err != nil {
				t.Fatal(err)
			}
			perr := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1})
			fs, ss := fast.Inspect(), slow.Inspect()
			if perr != nil || fs.Published != 2 || fs.Rejected != 0 {
				t.Fatalf("Publish = %v; the fast subscription was refused the record (fast %+v), while the "+
					"slow one kept its backlog (slow %+v)", perr, fs, ss)
			}
			if ss.Published != 2 || ss.Dropped != 1 || ss.Ready != 1 || !ss.Conserved() {
				t.Fatalf("the slow subscription: %+v, want its oldest record dropped for the new one", ss)
			}
		})
	}
}
