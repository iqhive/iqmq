package engine

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
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

// TestAFullEngineStillDeadLettersItsRecords pins the dead-letter move that
// needed room for a second copy of its record.
//
// The dead-letter publication booked an engine-wide record, and its bytes,
// for the copy while the original still held its own, and the original
// leaves only once the copy is in. So a record could be dead-lettered only
// while MaxTotalMessages and MaxTotalBytes had room for both: an engine
// filled by records waiting to be dead-lettered refused every copy, and the
// records went back to their lane and failed again for ever, since nothing
// but their own departure could free the room; and a record larger than half
// of MaxTotalBytes could never be dead-lettered at all. The copy books the
// original's share first now, so a move needs room only for what it adds:
// nothing for one copy, one record for each further subscription of the
// dead-letter topic.
//
// The accounting is checked as well as the outcome: once the records are
// moved the engine holds exactly the copies, and a move the dead-letter topic
// refused leaves the original's share where it was.
func TestAFullEngineStillDeadLettersItsRecords(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxMsgs  int
		maxBytes int64
		dlqSubs  int  // subscriptions of the dead-letter topic
		dlqFull  bool // the dead-letter lane holds a record nobody takes
		sizes    []int
		moved    bool
	}{
		{"full-engine", 2, 0, 1, false, []int{4, 4}, true},
		{"half-the-bytes", 0, 10, 1, false, []int{6}, true},
		{"a-copy-for-each-subscription", 2, 0, 2, false, []int{4}, true},
		{"no-dead-letter-subscription", 1, 0, 0, false, []int{4}, true},
		{"dead-letter-topic-full", 3, 0, 1, true, []int{4}, false},
	} {
		for _, arch := range []Arch{PerSubEnvelopes, LogCursor} {
			t.Run(tc.name+"/"+arch.Ident(), func(t *testing.T) {
				e, err := New(Config[int64]{
					MaxTotalMessages: tc.maxMsgs,
					MaxTotalBytes:    tc.maxBytes,
					Clone:            func(v int64) int64 { return v },
					Kinds:            KindSet{Arch: arch},
					DefaultTopology: TopologyConfig{
						Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				dlqCfg := TopologyConfig{}
				if tc.dlqFull {
					dlqCfg.CapacityMessages = 1
				}
				dlq, err := e.CreateTopic("dlq", dlqCfg)
				if err != nil {
					t.Fatal(err)
				}
				var dsubs []*Subscription[int64]
				for i := range tc.dlqSubs {
					ds, err := dlq.Subscribe("d"+strconv.Itoa(i), SubscriptionConfig{})
					if err != nil {
						t.Fatal(err)
					}
					dsubs = append(dsubs, ds)
				}
				if tc.dlqFull {
					if err := dlq.Publish(t.Context(), Publish[int64]{Payload: 99, Size: 1}); err != nil {
						t.Fatal(err)
					}
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
				var sum int64
				for i, size := range tc.sizes {
					if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i + 1), Size: size}); err != nil {
						t.Fatal(err)
					}
					sum += int64(size)
				}
				msgs0, bytes0, _ := e.Totals()
				// The consumer fails every delivery. Each has used its one
				// attempt, so every nack routes it to the dead-letter topic.
				for range 20 {
					cl := fetchSeqs(t, c, len(tc.sizes))
					if len(cl) == 0 {
						break
					}
					for _, x := range cl {
						if err := c.Nack(x.Env, x.Gen, 0); err != nil {
							t.Fatalf("nack: %v", err)
						}
					}
				}
				snap := sub.Inspect()
				msgs, b, _ := e.Totals()
				n := int64(len(tc.sizes))
				if !tc.moved {
					if snap.DeadLettered != 0 || snap.Ready != int(n) || msgs != msgs0 || b != bytes0 || !snap.Conserved() {
						t.Fatalf("a move the dead-letter topic refused: %+v; the engine holds %d records and %d "+
							"bytes, want the %d and %d it held before", snap, msgs, b, msgs0, bytes0)
					}
					return
				}
				if snap.DeadLettered != n || snap.Ready != 0 || snap.InFlight != 0 || !snap.Conserved() {
					t.Fatalf("after 20 rounds of failed deliveries with MaxAttempts 1, %d of %d records were "+
						"dead-lettered and %d dead-letter publications failed (attempts %d; the engine holds "+
						"%d records, %d bytes): %+v", snap.DeadLettered, n, snap.DeadLetterFailures,
						snap.Attempts, msgs, b, snap)
				}
				for _, ds := range dsubs {
					if dsnap := ds.Inspect(); dsnap.Published != n {
						t.Fatalf("dead-letter subscription %s holds %+v, want the %d copies", ds.Name(), dsnap, n)
					}
				}
				copies := int64(tc.dlqSubs)
				if msgs != n*copies || b != sum*copies {
					t.Fatalf("the engine holds %d records and %d bytes once the records moved, want the %d "+
						"copies and their %d bytes", msgs, b, n*copies, sum*copies)
				}
			})
		}
	}
}

// TestAnExplicitDeadLetterBooksTheCopyFromTheDeliverysShare holds the
// explicit dead-letter action (Consumer.DeadLetter, the memory driver's
// Reject) to the rule the attempt-limit route keeps: the copy takes the
// delivery's own engine-wide share, so an engine at its limit can still move
// the record, and a refused copy leaves the delivery leased with its share.
func TestAnExplicitDeadLetterBooksTheCopyFromTheDeliverysShare(t *testing.T) {
	for _, refused := range []bool{false, true} {
		name := "moved"
		if refused {
			name = "refused"
		}
		t.Run(name, func(t *testing.T) {
			e, err := New(Config[int64]{
				MaxTotalMessages: 2,
				Clone:            func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			dlqCfg := TopologyConfig{}
			if refused {
				dlqCfg.CapacityMessages = 1
			}
			dlq, err := e.CreateTopic("dlq", dlqCfg)
			if err != nil {
				t.Fatal(err)
			}
			dsub, err := dlq.Subscribe("d", SubscriptionConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if refused {
				if err := dlq.Publish(t.Context(), Publish[int64]{Payload: 99, Size: 1}); err != nil {
					t.Fatal(err)
				}
			}
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
			// The engine is at its limit: two records, or the dead-letter
			// filler and one.
			n := 2
			if refused {
				n = 1
			}
			for i := range n {
				if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i + 1), Size: 4}); err != nil {
					t.Fatal(err)
				}
			}
			cl := fetchSeqs(t, c, n)
			if len(cl) != n {
				t.Fatalf("fetch = %v", cl)
			}
			for _, x := range cl {
				err := c.DeadLetter(t.Context(), x.Env, x.Gen, dlq, Publish[int64]{Payload: x.Payload, Size: 4, Partition: -1})
				switch {
				case !refused && err != nil:
					t.Fatalf("DeadLetter on an engine at its limit: %v", err)
				case refused && !errors.Is(err, errcode.ErrQueueFull):
					t.Fatalf("DeadLetter into a full dead-letter lane: %v, want ErrQueueFull", err)
				}
			}
			snap, dsnap := sub.Inspect(), dsub.Inspect()
			msgs, _, _ := e.Totals()
			if refused {
				if snap.InFlight != 1 || snap.Acked != 0 || msgs != 2 || !snap.Conserved() {
					t.Fatalf("after a refused copy: %+v, the engine holds %d records; want the delivery "+
						"still leased and the 2 records held before", snap, msgs)
				}
				if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
					t.Fatalf("the delivery is no longer settleable: %v", err)
				}
				if msgs, _, _ := e.Totals(); msgs != 1 {
					t.Fatalf("the engine holds %d records after the ack, want the filler", msgs)
				}
				return
			}
			if snap.Acked != 2 || snap.InFlight != 0 || dsnap.Published != 2 || msgs != 2 {
				t.Fatalf("source %+v, dead-letter topic %+v, the engine holds %d records; want both moved "+
					"and the engine holding the 2 copies", snap, dsnap, msgs)
			}
		})
	}
}

// TestAShutdownThatRefusesACopyLeavesTheOriginalItsShare reaches the
// rollback of a dead-letter copy that has already booked the original's
// engine-wide share (publication.giveBack): the copy is parked in the clock
// read of its dedup check, a graceful Shutdown bars the publish gate (its
// drain waits for the two leases, so the settling walk does not run), and
// the copy then books its lanes from the lent share and has its ticket
// refused. The rollback must put the share back where the original can keep
// it: given to the engine instead, the original went back to its lease
// holding a share the engine no longer counted, and the totals fell below
// the records held (1 record, then -1 once the shutdown dropped them).
func TestAShutdownThatRefusesACopyLeavesTheOriginalItsShare(t *testing.T) {
	for _, arch := range []Arch{PerSubEnvelopes, LogCursor} {
		t.Run(arch.Ident(), func(t *testing.T) {
			trap := newClockTrap(clock.NewFake(time.Unix(0, 0)), "(*Topic[", ").publish")
			e, err := New(Config[int64]{
				MaxTotalMessages: 2,
				Clock:            trap,
				Clone:            func(v int64) int64 { return v },
				Kinds:            KindSet{Arch: arch},
				DrainTimeout:     -1,
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			dlq, err := e.CreateTopic("dlq", TopologyConfig{DedupWindow: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := dlq.Subscribe("d", SubscriptionConfig{}); err != nil {
				t.Fatal(err)
			}
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
			for i := range 2 {
				if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i + 1), Size: 4}); err != nil {
					t.Fatal(err)
				}
			}
			cl := fetchSeqs(t, c, 2)
			if len(cl) != 2 {
				t.Fatalf("fetch = %v", cl)
			}
			trap.arm()
			dlErr := make(chan error, 1)
			go func() {
				dlErr <- c.DeadLetter(context.Background(), cl[0].Env, cl[0].Gen, dlq,
					Publish[int64]{Payload: cl[0].Payload, Size: 4, Partition: -1, DedupKey: []byte("k")})
			}()
			<-trap.parked
			var wg sync.WaitGroup
			shutErr := make(chan error, 1)
			wg.Go(func() { shutErr <- e.Shutdown(context.Background()) })
			for !e.closing() {
				time.Sleep(time.Millisecond)
			}
			trap.let()
			if err := <-dlErr; !errors.Is(err, errcode.ErrClosed) {
				t.Errorf("DeadLetter across the shutdown = %v, want ErrClosed", err)
			}
			if msgs, b, _ := e.Totals(); msgs != 2 || b != 8 {
				t.Errorf("after the rolled-back copy the engine holds %d records and %d bytes, want the 2 "+
					"originals and their 8 bytes", msgs, b)
			}
			if snap := sub.Inspect(); snap.InFlight != 2 || !snap.Conserved() {
				t.Errorf("source after the rolled-back copy: %+v, want both still leased", snap)
			}
			for _, x := range cl {
				if err := c.Ack(x.Env, x.Gen); err != nil {
					t.Errorf("Ack after the refused copy: %v", err)
				}
			}
			wg.Wait()
			if err := <-shutErr; err != nil {
				t.Errorf("Shutdown: %v", err)
			}
			if msgs, b, _ := e.Totals(); msgs != 0 || b != 0 {
				t.Errorf("after the shutdown the engine holds %d records and %d bytes, want none", msgs, b)
			}
		})
	}
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
