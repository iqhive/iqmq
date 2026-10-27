package engine

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/topicname"
)

// dlqEngine builds a topic whose records are dead-lettered to "dlq" after
// attempts deliveries, with a consumer on each.
func dlqEngine(t *testing.T, attempts uint32, deadline time.Duration) (
	*Engine[int64], *Topic[int64], *Subscription[int64], *Consumer[int64], *Consumer[int64],
) {
	t.Helper()
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions:       1,
		CapacityMessages: 8,
		AckDeadline:      deadline,
		Overflow:         OverflowReject,
	})
	dlq, err := e.CreateTopic("dlq", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	dsub, err := dlq.Subscribe("d", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	dc, err := dsub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: attempts, DeadLetterTopic: "dlq"})
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
	return e, top, sub, c, dc
}

// TestDeadLetterAfterMaxAttempts: the record leaves the subscription for the
// dead-letter topic once its attempts are used up, and only then.
func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	_, top, sub, c, dc := dlqEngine(t, 2, -1)
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 7, Size: 4}); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 || int(cl[0].Info.Attempts) != attempt {
			t.Fatalf("attempt %d: %v", attempt, cl)
		}
		if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
			t.Fatalf("nack: %v", err)
		}
		snap := sub.Inspect()
		if attempt == 1 && (snap.DeadLettered != 0 || snap.Ready != 1) {
			t.Fatalf("dead-lettered before the budget ran out: %+v", snap)
		}
	}
	snap := sub.Inspect()
	if snap.DeadLettered != 1 || snap.Ready != 0 || snap.InFlight != 0 || snap.Dropped != 0 {
		t.Fatalf("after the budget ran out: %+v", snap)
	}
	if !snap.Conserved() {
		t.Fatalf("not conserved: %+v", snap)
	}
	// The record itself is in the dead-letter topic, payload and all.
	got := fetchSeqs(t, dc, 2)
	if len(got) != 1 || got[0].Payload != 7 {
		t.Fatalf("dead-letter topic holds %v", got)
	}
}

// TestDeadLetterOnLeaseExpiry: a lease that runs out is a failed delivery, so
// it consumes the budget and routes the record away like a nack.
func TestDeadLetterOnLeaseExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, top, sub, c, dc := dlqEngine(t, 1, 20*time.Millisecond)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 9, Size: 4}); err != nil {
			t.Fatal(err)
		}
		if cl := fetchSeqs(t, c, 1); len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		snap := sub.Inspect()
		if snap.DeadLettered != 1 || snap.Expired != 1 || snap.Requeued != 0 {
			t.Fatalf("after the lease expired: %+v", snap)
		}
		if got := fetchSeqs(t, dc, 1); len(got) != 1 || got[0].Payload != 9 {
			t.Fatalf("dead-letter topic holds %v", got)
		}
		_ = e
	})
}

// TestDeadLetterFailureNeverSettles is the rule ADR-011 states twice: a
// dead-letter publication that fails leaves the record in its lane.
func TestDeadLetterFailureNeverSettles(t *testing.T) {
	e, top, sub, c, dc := dlqEngine(t, 1, -1)
	e.Faults().FailDeadLetter(1)
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 5, Size: 4}); err != nil {
		t.Fatal(err)
	}
	cl := fetchSeqs(t, c, 1)
	if len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	snap := sub.Inspect()
	if snap.DeadLettered != 0 || snap.DeadLetterFailures != 1 || snap.Ready != 1 || snap.Acked != 0 {
		t.Fatalf("after a failed dead-letter publication: %+v", snap)
	}
	if !snap.Conserved() {
		t.Fatalf("not conserved: %+v", snap)
	}
	if got := fetchSeqs(t, dc, 1); len(got) != 0 {
		t.Fatalf("the dead-letter topic holds %v after a failed publication", got)
	}
	if n := e.Faults().DeadLetterFailures(); n != 1 {
		t.Fatalf("fault count = %d", n)
	}
	// The fault was armed for one publication, so the next attempt commits.
	cl = fetchSeqs(t, c, 1)
	if len(cl) != 1 || cl[0].Info.Attempts != 2 {
		t.Fatalf("redelivery after the failure: %v", cl)
	}
	if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if snap := sub.Inspect(); snap.DeadLettered != 1 || snap.Ready != 0 {
		t.Fatalf("after the retry: %+v", snap)
	}
}

// TestDeadLetterReleasesOnce: the payload of an owned publication is released
// exactly once, after the dead-letter copy has been settled too.
func TestDeadLetterReleasesOnce(t *testing.T) {
	_, top, _, c, dc := dlqEngine(t, 1, -1)
	released := 0
	if err := top.Publish(t.Context(), Publish[int64]{
		Payload: 3, Size: 4, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	cl := fetchSeqs(t, c, 1)
	if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatalf("released %d times while the dead-letter copy was unsettled", released)
	}
	got := fetchSeqs(t, dc, 1)
	if len(got) != 1 {
		t.Fatalf("dead-letter fetch = %v", got)
	}
	if err := dc.Ack(got[0].Env, got[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("released %d times after the dead-letter copy was acked", released)
	}
}

// TestMaxAttemptsWithoutDeadLetterDrops: an attempt budget with nowhere to
// send the record discards it and counts it, which is what a broker with a
// redelivery limit and no dead-letter exchange does.
func TestMaxAttemptsWithoutDeadLetterDrops(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, AckDeadline: -1,
		MaxAttempts: 1, Overflow: OverflowReject,
	})
	top, sub, c := queueSub(t, e)
	released := 0
	if err := top.Publish(t.Context(), Publish[int64]{
		Payload: 1, Size: 4, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	cl := fetchSeqs(t, c, 1)
	if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
		t.Fatal(err)
	}
	snap := sub.Inspect()
	if snap.Dropped != 1 || snap.DeadLettered != 0 || snap.DeadLetterFailures != 0 || snap.Ready != 0 {
		t.Fatalf("%+v", snap)
	}
	if !snap.Conserved() {
		t.Fatalf("not conserved: %+v", snap)
	}
	if released != 1 {
		t.Fatalf("released %d times", released)
	}
}

// TestDeadLetterTopicMustExist: the engine never creates a dead-letter topic
// behind the operator's back, so an unknown one is a failed publication.
func TestDeadLetterTopicMustExist(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
	})
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: 1, DeadLetterTopic: "missing"})
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
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	cl := fetchSeqs(t, c, 1)
	if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Topic("missing"); !errors.Is(err, errcode.ErrTopicNotFound) {
		t.Fatalf("the engine created the dead-letter topic: %v", err)
	}
	if snap := sub.Inspect(); snap.Dropped != 1 || snap.DeadLettered != 0 {
		t.Fatalf("%+v", snap)
	}
}

// TestSubscriptionDeadLetterOverride: a subscription may name its own
// dead-letter topic or turn the topic's off.
func TestSubscriptionDeadLetterOverride(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
	})
	for _, name := range []string{"dlq", "other"} {
		if _, err := e.CreateTopic(name, TopologyConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: 1, DeadLetterTopic: "dlq"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		cfg  SubscriptionConfig
		want string
	}{
		{"inherits", SubscriptionConfig{}, "dlq"},
		{"overrides", SubscriptionConfig{DeadLetterTopic: "other"}, "other"},
		{"off", SubscriptionConfig{DeadLetterTopic: noDeadLetter}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub, err := top.Subscribe(tc.name, tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if sub.dlqName != tc.want {
				t.Fatalf("dead-letter topic = %q, want %q", sub.dlqName, tc.want)
			}
		})
	}
	if _, err := top.Subscribe("bad", SubscriptionConfig{DeadLetterTopic: "!"}); err == nil {
		t.Fatal("an invalid dead-letter topic name was accepted")
	}
	if _, err := e.CreateTopic("badtopic", TopologyConfig{DeadLetterTopic: "!"}); err == nil {
		t.Fatal("an invalid dead-letter topic name was accepted by CreateTopic")
	}
}

// TestNoDeadLetterSentinelCannotName: the value that turns a dead-letter
// topic off is a spelling no topic can have, so a legal configuration is
// never silently reinterpreted as "no dead-letter topic". "-", the spelling
// this started out with, is a valid one-segment name and would have shadowed
// a topic that could exist.
func TestNoDeadLetterSentinelCannotName(t *testing.T) {
	if err := topicname.Validate(noDeadLetter); err == nil {
		t.Fatalf("the no-dead-letter sentinel %q is a valid topic name", noDeadLetter)
	}
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	if _, err := e.CreateTopic(noDeadLetter, TopologyConfig{}); err == nil {
		t.Fatalf("a topic named %q was created", noDeadLetter)
	}
	// A topic that does carry the sentinel as its dead-letter topic has none,
	// and its subscriptions inherit that rather than a name that cannot
	// resolve.
	top, err := e.CreateTopic("t", TopologyConfig{DeadLetterTopic: noDeadLetter})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if sub.dlqName != "" {
		t.Fatalf("dead-letter topic = %q, want none", sub.dlqName)
	}
}

// TestMaxAttemptsOverride: a subscription may raise, lower or remove the
// topic's attempt budget.
func TestMaxAttemptsOverride(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
	})
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		cfg  SubscriptionConfig
		want uint32
	}{
		{"inherits", SubscriptionConfig{}, 3},
		{"raises", SubscriptionConfig{MaxAttempts: 9}, 9},
		{"unlimited", SubscriptionConfig{MaxAttempts: -1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub, err := top.Subscribe(tc.name, tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if sub.maxAttempts != tc.want {
				t.Fatalf("MaxAttempts = %d, want %d", sub.maxAttempts, tc.want)
			}
		})
	}
}

// blockingDLQEngine builds a dead-letter topic that holds one record and
// parks a publisher that finds it full (OverflowBlock), already filled by a
// subscription that never consumes; a source topic that dead-letters after
// one failed delivery; and an unrelated topic that shares the engine's single
// scheduler goroutine.
func blockingDLQEngine(t *testing.T, deadline time.Duration) (
	*Topic[int64], *Subscription[int64], *Consumer[int64], *Topic[int64], *Consumer[int64],
) {
	t.Helper()
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions:       1,
		CapacityMessages: 8,
		AckDeadline:      deadline,
		Overflow:         OverflowReject,
	})
	dlq, err := e.CreateTopic("dlq", TopologyConfig{CapacityMessages: 1, Overflow: OverflowBlock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dlq.Subscribe("d", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	// The one record the dead-letter lane can hold, and nothing consumes it.
	if err = dlq.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	top, err := e.CreateTopic("t", TopologyConfig{MaxAttempts: 1, DeadLetterTopic: "dlq"})
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
	other, err := e.CreateTopic("other", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	osub, err := other.Subscribe("o", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	oc, err := osub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return top, sub, c, other, oc
}

// TestDeadLetterNeverParksTheScheduler: an engine-internal dead-letter
// publication that finds no room is a dead-letter failure, not a wait. The
// scheduler callback that routes an expired lease away must do bounded work
// (the memory engine), so one full dead-letter topic under OverflowBlock may
// never stop delayed promotion, lease expiry and TTL sweeps for every other
// lane of the engine.
func TestDeadLetterNeverParksTheScheduler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		top, sub, c, other, oc := blockingDLQEngine(t, 20*time.Millisecond)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 9, Size: 4}); err != nil {
			t.Fatal(err)
		}
		if cl := fetchSeqs(t, c, 1); len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		// The lease expires on the scheduler goroutine, which then has to
		// route the record to a dead-letter topic with no room for it.
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		snap := sub.Inspect()
		if snap.DeadLetterFailures != 1 || snap.DeadLettered != 0 {
			t.Fatalf("a dead-letter topic with no room is a dead-letter failure: %+v", snap)
		}
		if snap.Ready != 1 || !snap.Conserved() {
			t.Fatalf("the record did not go back to its lane: %+v", snap)
		}
		// Every other lane of the engine is still being served.
		if err := other.Publish(t.Context(), Publish[int64]{
			Payload: 3, Size: 4, Delay: 20 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		if got := fetchSeqs(t, oc, 1); len(got) != 1 || got[0].Payload != 3 {
			t.Fatalf("the scheduler never promoted a delayed record on an unrelated topic: %v", got)
		}
	})
}

// TestNackNeverParksOnAFullDeadLetterTopic: Consumer.Nack takes no context,
// so a caller has nothing to end a wait with. The dead-letter publication it
// makes must therefore never park either.
func TestNackNeverParksOnAFullDeadLetterTopic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		top, sub, c, _, _ := blockingDLQEngine(t, -1)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 9, Size: 4}); err != nil {
			t.Fatal(err)
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		done := make(chan error, 1)
		go func() { done <- c.Nack(cl[0].Env, cl[0].Gen, 0) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("nack: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Consumer.Nack parked inside the dead-letter publication")
		}
		snap := sub.Inspect()
		if snap.DeadLetterFailures != 1 || snap.DeadLettered != 0 || snap.Ready != 1 {
			t.Fatalf("after a dead-letter publication with no room: %+v", snap)
		}
	})
}

// TestADeadLetterLeavesTheSharedPayloadAlone pins the scrub a dead-letter
// publication used to run on a payload it did not own.
//
// The publication carries the record's own payload, not a copy, under the
// engine's copy mode. Under CopyOwned that made the dead-letter record a
// second owner of the storage: once the dead-letter topic settled it, its
// release ran Config.Zero (or the Poison canary) over the payload, while
// another subscription of the source topic still held the same record and
// was handed zeros (or the canary) when it fetched it.
//
// The payload is the source record's, which holds a reference for as long as
// the dead-letter record lives and scrubs it when the last one goes; the
// dead-letter record only borrows it.
func TestADeadLetterLeavesTheSharedPayloadAlone(t *testing.T) {
	for _, recv := range []ReceiveMode{ReceiveShared, ReceiveZeroCopy} {
		for _, a := range Arches() {
			t.Run(recv.String()+"/"+a.String(), func(t *testing.T) {
				e, err := New(Config[[]byte]{
					Kinds:    KindSet{Arch: a},
					Clone:    bytes.Clone,
					CopyMode: CopyOwned,
					Receive:  recv,
					Zero:     func(b []byte) { clear(b) },
					Poison: func(b []byte) {
						for i := range b {
							b[i] = canaryByte
						}
					},
					DefaultTopology: TopologyConfig{Partitions: 1, AckDeadline: -1, Overflow: OverflowReject},
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				t.Cleanup(func() { _ = e.Close() })
				attach := func(top *Topic[[]byte], name string, cfg SubscriptionConfig) *Consumer[[]byte] {
					t.Helper()
					sub, err := top.Subscribe(name, cfg)
					if err != nil {
						t.Fatalf("Subscribe(%s): %v", name, err)
					}
					c, err := sub.Attach(ConsumerConfig{})
					if err != nil {
						t.Fatalf("Attach(%s): %v", name, err)
					}
					return c
				}
				fetch := func(c *Consumer[[]byte]) Claim[[]byte] {
					t.Helper()
					dst := make([]Claim[[]byte], 1)
					if n, _ := c.Fetch(poll(t), dst); n != 1 {
						t.Fatalf("fetched %d records, want 1", n)
					}
					return dst[0]
				}
				dlq, err := e.CreateTopic("dlq", TopologyConfig{})
				if err != nil {
					t.Fatal(err)
				}
				dc := attach(dlq, "d", SubscriptionConfig{})
				top, err := e.CreateTopic("t", TopologyConfig{})
				if err != nil {
					t.Fatal(err)
				}
				failing := attach(top, "failing", SubscriptionConfig{MaxAttempts: 1, DeadLetterTopic: "dlq"})
				other := attach(top, "other", SubscriptionConfig{})

				const want = "payload"
				var released atomic.Int32
				if err := top.Publish(t.Context(), Publish[[]byte]{
					Payload: []byte(want), Size: len(want), Release: func([]byte) { released.Add(1) },
				}); err != nil {
					t.Fatal(err)
				}
				cl := fetch(failing)
				if err := failing.Nack(cl.Env, cl.Gen, 0); err != nil {
					t.Fatalf("Nack: %v", err)
				}
				cl = fetch(dc)
				if err := dc.Ack(cl.Env, cl.Gen); err != nil {
					t.Fatalf("Ack(dead letter): %v", err)
				}
				cl = fetch(other)
				if got := string(cl.Payload); got != want {
					t.Fatalf("the other subscription read %q, want %q: the dead-letter record's release scrubbed "+
						"a payload this subscription still held", got, want)
				}
				if err := other.Ack(cl.Env, cl.Gen); err != nil {
					t.Fatalf("Ack(other): %v", err)
				}
				if n := released.Load(); n != 1 {
					t.Fatalf("the payload was released %d times once every holder settled, want 1", n)
				}
			})
		}
	}
}
