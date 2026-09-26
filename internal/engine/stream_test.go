package engine

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// streamRig builds a stream topic with the given retention and one
// subscription starting at pos, and returns the topic, the subscription and a
// consumer.
func streamRig(t *testing.T, e *Engine[int64], r Retention, pos StartPosition) (
	*Topic[int64], *Subscription[int64], *Consumer[int64],
) {
	t.Helper()
	top, err := e.CreateTopic("t", TopologyConfig{Kind: TopologyStream, Retention: r})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Start: pos})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return top, sub, c
}

// drainSeqs fetches everything the consumer can take now and returns the
// record sequences in the order it saw them, acknowledging each.
func drainSeqs(t *testing.T, c *Consumer[int64], max int) []uint64 {
	t.Helper()
	dst := make([]Claim[int64], max)
	out := []uint64{}
	for {
		n, err := c.Fetch(poll(t), dst)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Fetch: %v", err)
		}
		if n == 0 {
			return out
		}
		for i := range n {
			out = append(out, dst[i].Info.Seq)
			if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
}

// TestStreamRetainsAndReplays: a stream keeps its publications and a
// subscription that starts at the earliest retained record receives them all,
// while one that starts at the live end receives only what follows it.
func TestStreamRetainsAndReplays(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 8},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// TestStreamRetainsWithoutSubscription covers the publish-before-
	// subscribe case; here a subscription is present throughout.
	first, err := top.Subscribe("first", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(i), Size: 1, Partition: 0,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if n, _ := top.Retained(); n != 4 {
		t.Fatalf("retained %d records, want 4", n)
	}
	// A late subscription starting at the live end sees nothing of them.
	late, err := top.Subscribe("late", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := late.Inspect().Ready; got != 0 {
		t.Fatalf("a StartLatest subscription holds %d records", got)
	}
	// One starting at the earliest retained record sees all four.
	replay, err := top.Subscribe("replay", SubscriptionConfig{
		Start: StartPosition{Kind: StartEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := replay.Inspect().Ready; got != 4 {
		t.Fatalf("a StartEarliest subscription holds %d records, want 4", got)
	}
	if got := first.Inspect().Ready; got != 4 {
		t.Fatalf("the original subscription holds %d records, want 4", got)
	}
	rc, err := replay.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := drainSeqs(t, rc, 8); len(got) != 4 {
		t.Fatalf("replayed %v, want four records", got)
	}
	// Snapshot.Retained reports the topic's log for every subscription.
	if got := replay.Inspect().Retained; got != 4 {
		t.Fatalf("Snapshot.Retained = %d, want 4", got)
	}
}

// TestStreamRetentionBounds: each of the three bounds trims the log on its
// own.
func TestStreamRetentionBounds(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		top, _, _ := streamRig(t, e, Retention{MaxMessages: 3}, StartPosition{})
		for i := range 10 {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 1, Partition: 0,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		n, bytes := top.Retained()
		if n != 3 || bytes != 3 {
			t.Fatalf("retained %d records / %d bytes, want 3 / 3", n, bytes)
		}
		if seq, ok := top.Oldest(); !ok || seq != 8 {
			t.Fatalf("oldest retained = %d (%v), want 8", seq, ok)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		top, _, _ := streamRig(t, e, Retention{MaxBytes: 10}, StartPosition{})
		for i := range 8 {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 4, Partition: 0,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		n, bytes := top.Retained()
		if bytes > 10 || n != 2 {
			t.Fatalf("retained %d records / %d bytes, want 2 / <= 10", n, bytes)
		}
	})
	t.Run("age", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
			top, _, _ := streamRig(t, e, Retention{MaxAge: 10 * tick}, StartPosition{})
			for i := range 3 {
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: int64(i), Size: 1, Partition: 0,
				}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			if n, _ := top.Retained(); n != 3 {
				t.Fatalf("retained %d records before the age bound", n)
			}
			time.Sleep(11 * tick)
			synctest.Wait()
			if n, _ := top.Retained(); n != 0 {
				t.Fatalf("retained %d records past the age bound", n)
			}
		})
	})
}

// TestStreamSeek: Seek discards what the subscription holds and replays the
// retained log from the requested position, in publication order.
func TestStreamSeek(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
		top, sub, c := streamRig(t, e, Retention{MaxMessages: 16}, StartPosition{})
		var at time.Time
		for i := range 6 {
			if i == 3 {
				at = e.Clock().Now()
			}
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 1, Partition: 0,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			time.Sleep(tick)
		}
		if got := drainSeqs(t, c, 8); len(got) != 6 {
			t.Fatalf("first pass delivered %v", got)
		}
		if cur := sub.Cursors(); len(cur) != 1 || cur[0] != 6 {
			t.Fatalf("cursors = %v, want [6]", cur)
		}
		// Replay everything.
		n, err := sub.Seek(StartPosition{Kind: StartEarliest})
		if err != nil {
			t.Fatalf("Seek: %v", err)
		}
		if n != 6 {
			t.Fatalf("Seek replayed %d records, want 6", n)
		}
		got := drainSeqs(t, c, 8)
		if len(got) != 6 {
			t.Fatalf("replay delivered %v", got)
		}
		for i, seq := range got {
			if seq != uint64(i+1) {
				t.Fatalf("replay delivered %v, want publication order", got)
			}
		}
		// Replay from a sequence.
		if n, err := sub.Seek(StartPosition{Kind: StartAtSeq, Seq: 4}); err != nil || n != 3 {
			t.Fatalf("Seek(seq=4) = %d, %v; want 3, nil", n, err)
		}
		if got := drainSeqs(t, c, 8); len(got) != 3 || got[0] != 4 {
			t.Fatalf("seek by sequence delivered %v", got)
		}
		// Replay from a time: the record published at `at` and everything
		// after it.
		if n, err := sub.Seek(StartPosition{Kind: StartAtTime, At: at}); err != nil || n != 3 {
			t.Fatalf("Seek(time) = %d, %v; want 3, nil", n, err)
		}
		if got := drainSeqs(t, c, 8); len(got) != 3 || got[0] != 4 {
			t.Fatalf("seek by time delivered %v", got)
		}
		// StartLatest replays nothing and leaves the subscription empty.
		if n, err := sub.Seek(StartPosition{Kind: StartLatest}); err != nil || n != 0 {
			t.Fatalf("Seek(latest) = %d, %v", n, err)
		}
		if snap := sub.Inspect(); snap.Ready != 0 || !snap.Conserved() {
			t.Fatalf("after Seek(latest): %+v", snap)
		}
	})
}

// TestStreamSeekRefusals: Seek is refused on a topology that retains nothing,
// on an undefined position and after a close.
func TestStreamSeekRefusals(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	_, plain, _ := queueSub(t, e)
	if _, err := plain.Seek(StartPosition{Kind: StartEarliest}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Seek on a topic = %v, want ErrInvalidConfig", err)
	}
	top, err := e.CreateTopic("st", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Seek(StartPosition{Kind: StartKind(9)}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Seek with an undefined position = %v", err)
	}
	if _, err := top.Subscribe("bad", SubscriptionConfig{
		Start: StartPosition{Kind: StartKind(9)},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Subscribe with an undefined position = %v", err)
	}
	// A start position on a topology that retains nothing is refused rather
	// than ignored.
	pt, err := e.CreateTopic("plainstart", TopologyConfig{Kind: TopologyTopic})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pt.Subscribe("s", SubscriptionConfig{
		Start: StartPosition{Kind: StartEarliest},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a start position on a topic = %v, want ErrInvalidConfig", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Seek(StartPosition{Kind: StartEarliest}); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("Seek after close = %v, want ErrClosed", err)
	}
}

// TestStreamRetentionHoldsOwnedPayload: an owned payload published to a
// stream is handed back only once retention has let go of it, because the
// retained log holds a reference of its own.
func TestStreamRetentionHoldsOwnedPayload(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, _, c := streamRig(t, e, Retention{MaxMessages: 1}, StartPosition{})
	released := map[int64]int{}
	for i := range 3 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(i), Size: 1, Partition: 0, Mode: CopyOwned,
			Release: func(v int64) { released[v]++ },
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	drainSeqs(t, c, 8)
	// Settling every delivery is not enough: the log still holds the last
	// record, so only the two it has trimmed are back with the publisher.
	if released[0] != 1 || released[1] != 1 || released[2] != 0 {
		t.Fatalf("release ledger %v: the retained record was released early", released)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if released[2] != 1 {
		t.Fatalf("closing the engine left the retained payload unreleased: %v", released)
	}
	for v, n := range released {
		if n != 1 {
			t.Fatalf("payload %d released %d times", v, n)
		}
	}
}

// TestStreamDefaultRetention: a stream that names no bound retains one lane's
// worth of records per partition rather than growing without limit.
func TestStreamDefaultRetention(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 4})
	top, err := e.CreateTopic("t", TopologyConfig{Kind: TopologyStream})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got := top.Topology().Retention.MaxMessages; got != 4 {
		t.Fatalf("default stream retention = %d, want the lane capacity 4", got)
	}
}

// TestStreamRetainsWithoutSubscription: a stream's retained log is a property
// of the topic, not of its subscription set, so a publication made before
// anyone subscribed is retained and a subscription created afterwards with
// StartEarliest replays it. That is the case the memory engine names, and it
// is what makes a stream different from a topic.
func TestStreamRetainsWithoutSubscription(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 8},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	released := map[int64]int{}
	for i := range 4 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(i), Size: 1, Partition: 0, Mode: CopyOwned,
			Release: func(v int64) { released[v]++ },
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if n, b := top.Retained(); n != 4 || b != 4 {
		t.Fatalf("a stream with no subscription retained %d records (%d bytes), want 4 (4)", n, b)
	}
	for v, n := range released {
		if n != 0 {
			t.Fatalf("retained payload %d was released %d times while the log still holds it", v, n)
		}
	}
	sub, err := top.Subscribe("late", SubscriptionConfig{
		Start: StartPosition{Kind: StartEarliest},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	got := drainSeqs(t, c, 8)
	if len(got) != 4 {
		t.Fatalf("a StartEarliest subscription created after the publications replayed %d records %v, want 4",
			len(got), got)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if released[int64(i)] != 1 {
			t.Fatalf("payload %d released %d times, want once: %v", i, released[int64(i)], released)
		}
	}
}

// TestStreamRetainsUnmatchedSubject: a publication whose subject no filter
// matches reaches no subscription and is still retained, so a subscription
// whose filter does match it can replay it afterwards.
func TestStreamRetainsUnmatchedSubject(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Wildcards: true, Retention: Retention{MaxMessages: 8},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := top.Subscribe("a", SubscriptionConfig{Filter: "a.>"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for range 3 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Subject: "b.x",
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if n, _ := top.Retained(); n != 3 {
		t.Fatalf("a publication no filter matched left %d records retained, want 3", n)
	}
	sub, err := top.Subscribe("b", SubscriptionConfig{
		Filter: "b.>", Start: StartPosition{Kind: StartEarliest},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := sub.Inspect().Ready; got != 3 {
		t.Fatalf("a b.> subscription replayed %d of the 3 retained b.x records", got)
	}
}

// TestStreamRetainsWhatItAccepts: a stream retains every publication the
// engine accepted, and only those. A full lane under OverflowDropNewest is an
// acceptance — the engine reports success and takes the payload — so the
// record is retained; a refusal under OverflowReject is not, because the
// caller still owns the payload and the error asks it to retry.
func TestStreamRetainsWhatItAccepts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		overflow OverflowPolicy
		want     int
		wantErr  error
	}{
		{"dropnewest", OverflowDropNewest, 4, nil},
		{"reject", OverflowReject, 2, errcode.ErrQueueFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
			top, err := e.CreateTopic("t", TopologyConfig{
				Kind: TopologyStream, Partitions: 1, CapacityMessages: 2,
				Overflow: tc.overflow, Retention: Retention{MaxMessages: 16},
			})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			released := map[int64]int{}
			for i := range 4 {
				err := top.Publish(context.Background(), Publish[int64]{
					Payload: int64(i), Size: 1, Partition: 0, Mode: CopyOwned,
					Release: func(v int64) { released[v]++ },
				})
				want := error(nil)
				if i >= 2 {
					want = tc.wantErr
				}
				if !errors.Is(err, want) {
					t.Fatalf("publication %d = %v, want %v", i, err, want)
				}
			}
			if n, _ := top.Retained(); n != tc.want {
				t.Fatalf("%s: retained %d records, want %d", tc.name, n, tc.want)
			}
			// A refused publication is still the caller's: the engine never
			// releases a payload it did not take.
			for i := 2; i < 4; i++ {
				if tc.wantErr != nil && released[int64(i)] != 0 {
					t.Fatalf("refused payload %d was released by the engine", i)
				}
			}
		})
	}
}

// TestStreamReplayHonoursFilter: a replay is an admission like any other, so
// a subscription of a stream with wildcards is never handed a retained record
// its subject filter excludes — neither by Seek nor by a subscribe-time start
// position.
func TestStreamReplayHonoursFilter(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Wildcards: true, Retention: Retention{MaxMessages: 16},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	narrow, err := top.Subscribe("narrow", SubscriptionConfig{Filter: "a.*"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for _, subj := range []string{"a.b", "x.y", "a.c", "x.z"} {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Subject: subj,
		}); err != nil {
			t.Fatalf("Publish(%s): %v", subj, err)
		}
	}
	if got := narrow.Inspect().Ready; got != 2 {
		t.Fatalf("the live path admitted %d of the 4 publications, want the 2 matching a.*", got)
	}
	if n, _ := top.Retained(); n != 4 {
		t.Fatalf("the topic retained %d records, want all 4", n)
	}
	// Seek discards what the subscription holds and replays: the replay
	// must admit only the records the filter matches.
	n, err := narrow.Seek(StartPosition{Kind: StartEarliest})
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if n != 2 {
		t.Fatalf("Seek(earliest) replayed %d records into an a.* subscription, want 2", n)
	}
	if got := narrow.Inspect().Ready; got != 2 {
		t.Fatalf("after Seek the a.* subscription holds %d records, want 2", got)
	}
	// The subscribe-time start position takes the same route.
	late, err := top.Subscribe("late", SubscriptionConfig{
		Filter: "x.*", Start: StartPosition{Kind: StartEarliest},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := late.Inspect().Ready; got != 2 {
		t.Fatalf("a StartEarliest x.* subscription holds %d of the 4 retained records, want 2", got)
	}
	// And a subscription with no filter still replays everything.
	all, err := top.Subscribe("all", SubscriptionConfig{
		Start: StartPosition{Kind: StartEarliest},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := all.Inspect().Ready; got != 4 {
		t.Fatalf("an unfiltered StartEarliest subscription holds %d records, want 4", got)
	}
}

// TestWildcardDeliveryCarriesSubject: a delivery from a topic with wildcards
// reports the resolved subject, which is what the adapter above the engine
// needs for the API's DeliveryInfo.Topic.
func TestWildcardDeliveryCarriesSubject(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 32})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Wildcards: true, Retention: Retention{MaxMessages: 8},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Filter: "a.>"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 7, Size: 1, Partition: 0, Subject: "a.b.c",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]Claim[int64], 1)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		t.Fatalf("Fetch returned %d claims, want 1", n)
	}
	if dst[0].Info.Subject != "a.b.c" {
		t.Fatalf("Info.Subject = %q, want the resolved subject a.b.c", dst[0].Info.Subject)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// A replayed record reports it too.
	if _, err := sub.Seek(StartPosition{Kind: StartEarliest}); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	n, err = c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 || dst[0].Info.Subject != "a.b.c" {
		t.Fatalf("a replayed delivery reported subject %q (n=%d), want a.b.c", dst[0].Info.Subject, n)
	}
}

// TestSubscribeReplaysOutsideTheTopicMutex: the replay a start position asks
// for runs with the topic mutex released, so the publisher's Release callback
// never runs under an internal lock (the message contract). A replay gives
// records back on three routes — the age bound it applies before it walks,
// the entries its reservations had no room for, and the drain of a
// subscription that closed under it — and a callback that took the topic (or
// subscribed to it again) under t.mu would deadlock on a mutex that is not
// reentrant.
func TestSubscribeReplaysOutsideTheTopicMutex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxAge: 10 * tick},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		released, underMutex := 0, 0
		rel := func(int64) {
			released++
			if !top.mu.TryLock() {
				underMutex++
				return
			}
			top.mu.Unlock()
		}
		for i := range 3 {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 1, Partition: 0, Mode: CopyOwned, Release: rel,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		// The records are the retained log's alone, and past their age bound.
		// Nothing has read the log since, so the trim is the subscribe's.
		time.Sleep(11 * tick)
		synctest.Wait()
		if released != 0 {
			t.Fatalf("%d records were released before the subscribe", released)
		}
		if _, err := top.Subscribe("s", SubscriptionConfig{
			Start: StartPosition{Kind: StartEarliest},
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if released != 3 {
			t.Fatalf("the subscribe released %d of the 3 over-age retained records", released)
		}
		if underMutex != 0 {
			t.Fatalf("%d of %d release callbacks ran under the topic mutex", underMutex, released)
		}
	})
}

// TestRetainedLogIsOrderedBySequence: concurrent publications append to a
// partition's retained log in publication-sequence order and with
// non-decreasing times. Three readers rest on it — retainedLog.replayFrom
// stops at the first entry past its bound, trimLocked's age loop stops at the
// first entry the bounds still cover, and Topic.Oldest reads the head — so a
// log that is not the sequence it claims to be answers all three wrongly, and
// the reference model, which draws the sequence and appends in one sequential
// step, can never show it.
func TestRetainedLogIsOrderedBySequence(t *testing.T) {
	const rounds, writers, each = 40, 8, 16
	for round := range rounds {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 4096})
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: writers * each},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		var wg sync.WaitGroup
		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range each {
					if err := top.Publish(context.Background(), Publish[int64]{
						Payload: int64(i), Size: 1, Partition: 0,
					}); err != nil {
						t.Errorf("Publish: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()
		lg := top.retained[0]
		lg.mu.Lock()
		ents := make([]retainedEntry[int64], len(lg.ents))
		copy(ents, lg.ents)
		lg.mu.Unlock()
		if len(ents) != writers*each {
			t.Fatalf("round %d: the log holds %d of the %d publications", round, len(ents), writers*each)
		}
		for i := 1; i < len(ents); i++ {
			if ents[i].seq <= ents[i-1].seq {
				t.Fatalf("round %d: retained entry %d has sequence %d after sequence %d",
					round, i, ents[i].seq, ents[i-1].seq)
			}
			if ents[i].at < ents[i-1].at {
				t.Fatalf("round %d: retained entry %d is timed %d after %d",
					round, i, ents[i].at, ents[i-1].at)
			}
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestStreamSubscribeAdmitsAPublicationInFlightExactlyOnce: a subscription
// that joins a stream while a publication is in flight is admitted that
// record exactly once — by the live path if the publication routed on a set
// that already held it, by the replay otherwise, and never by neither.
//
// The window is between the moment a publication reads the subscription set
// and the moment it appends to the retained log: a record whose publication
// read the set before the subscription was published is not delivered live,
// and a replay that walked the log before the append does not find it either,
// so the topic keeps a record no subscription of it will ever hold. The
// reference model draws its sequence and appends in one sequential step, so
// the differential cannot show this.
func TestStreamSubscribeAdmitsAPublicationInFlightExactlyOnce(t *testing.T) {
	const rounds, records = 200, 64
	for round := range rounds {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 4096})
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 4096},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := range records {
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: int64(i), Size: 1, Partition: 0,
				}); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
		// Join while the publisher is running: the first retained record says
		// it has started, and it has 63 more to go.
		for {
			if n, _ := top.Retained(); n > 0 {
				break
			}
			runtime.Gosched()
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{
			Start: StartPosition{Kind: StartEarliest},
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		<-done
		kept, _ := top.Retained()
		if snap := sub.Inspect(); snap.Ready != kept || snap.Published != int64(kept) {
			t.Fatalf("round %d: the subscription holds %d records and was published %d of the %d the topic retains",
				round, snap.Ready, snap.Published, kept)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// joinBoundaryDeadline is how long a publication whose callback joins its own
// topic is given before the test calls it a deadlock. The failure it looks
// for is a hard one — a single goroutine spinning on a boundary only it can
// clear — so any generous bound distinguishes it; the passing case takes
// microseconds.
const joinBoundaryDeadline = 5 * time.Second

// publishWithin runs one publication in its own goroutine and fails the test
// when it has not returned inside joinBoundaryDeadline. The goroutine is
// deliberately left behind on failure: a publication deadlocked on the join
// boundary is unkillable, and the report is what matters.
func publishWithin(t *testing.T, what string, top *Topic[int64], p Publish[int64]) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- top.Publish(context.Background(), p) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: Publish: %v", what, err)
		}
	case <-time.After(joinBoundaryDeadline):
		t.Fatalf("%s: the publication never returned: it is waiting for the join boundary its own callback runs inside", what)
	}
}

// TestPublishRunsNoCallbackInsideTheJoinBoundary: a callback of the caller's
// that joins the topic it is publishing to must not deadlock the publication
// that is running it.
//
// A stream publication holds the topic's join boundary from the moment it
// reads the subscription set until it has appended to the retained log
// , and Topic.subscribe waits on that boundary. Every callback
// of the caller's that the publication's own work would otherwise run inside
// it — the Release of every record the retention trim gives back, the Release
// of a record a reservation displaced, and Config.Clone — is therefore run
// outside it: the clone before the boundary is taken, the releases after it
// is given up (publication.end, the message contract).
//
// Each case is a single goroutine with no concurrency, no fault injection and
// no timing: the publication either returns in microseconds or waits for
// itself for ever.
func TestPublishRunsNoCallbackInsideTheJoinBoundary(t *testing.T) {
	// join subscribes to top from inside a callback, exactly once, and
	// reports whether it ever ran.
	join := func(t *testing.T, top **Topic[int64], ran *int) func(int64) {
		return func(int64) {
			*ran++
			if _, err := (*top).Subscribe("joined-from-a-callback", SubscriptionConfig{}); err != nil {
				t.Errorf("Subscribe from a callback: %v", err)
			}
		}
	}

	t.Run("retention-trim-release", func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		var top *Topic[int64]
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 1},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		ran := 0
		// Nobody subscribes, so the record is retained and nothing else; the
		// next publication trims it inside Topic.retain.
		publishWithin(t, "the retained publication", top, Publish[int64]{
			Payload: 0, Size: 1, Partition: 0, Mode: CopyOwned,
			Release: join(t, &top, &ran),
		})
		publishWithin(t, "the publication that trims it", top, Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Mode: CopyOwned, Release: func(int64) {},
		})
		if ran != 1 {
			t.Fatalf("the trimmed record's Release ran %d times, want 1", ran)
		}
	})

	t.Run("commit-trim-release", func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		var top *Topic[int64]
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 1},
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
		ran := 0
		publishWithin(t, "the retained publication", top, Publish[int64]{
			Payload: 0, Size: 1, Partition: 0, Mode: CopyOwned,
			Release: join(t, &top, &ran),
		})
		// The subscription gives its reference up, so the retained log holds
		// the last one and the next publication's trim is what releases it —
		// from Topic.commit this time, not Topic.retainOnly.
		dst := make([]Claim[int64], 1)
		n, err := c.Fetch(poll(t), dst)
		if err != nil || n != 1 {
			t.Fatalf("Fetch: n=%d err=%v", n, err)
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		publishWithin(t, "the publication that trims it", top, Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Mode: CopyOwned, Release: func(int64) {},
		})
		if ran != 1 {
			t.Fatalf("the trimmed record's Release ran %d times, want 1", ran)
		}
	})

	t.Run("displaced-reservation-release", func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
		var top *Topic[int64]
		// The retention keeps one record and the lane holds two, so record 0
		// is out of the log by the time the lane displaces it: the lane's is
		// then the last reference, and the drop that makes room is what runs
		// its Release.
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 1},
			CapacityMessages: 2, Overflow: OverflowDropOldest,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ran := 0
		publishWithin(t, "the publication that is displaced", top, Publish[int64]{
			Payload: 0, Size: 1, Partition: 0, Mode: CopyOwned,
			Release: join(t, &top, &ran),
		})
		publishWithin(t, "the publication that fills the lane", top, Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Mode: CopyOwned, Release: func(int64) {},
		})
		// The lane is full and the policy is DropOldest, so this reservation
		// displaces record 0 — inside the boundary, from Topic.reserveLane.
		publishWithin(t, "the publication that displaces it", top, Publish[int64]{
			Payload: 2, Size: 1, Partition: 0, Mode: CopyOwned, Release: func(int64) {},
		})
		if ran != 1 {
			t.Fatalf("the displaced record's Release ran %d times, want 1", ran)
		}
	})

	t.Run("clone", func(t *testing.T) {
		var top *Topic[int64]
		ran := 0
		e, err := New(Config[int64]{
			Kinds: KindSet{Safe: true},
			Clone: func(v int64) int64 {
				if top != nil {
					join(t, &top, &ran)(v)
				}
				return v
			},
			DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 64},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = e.Close() })
		top, err = e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 8},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		publishWithin(t, "the copying publication", top, Publish[int64]{
			Payload: 0, Size: 1, Partition: 0,
		})
		if ran == 0 {
			t.Fatal("Config.Clone never ran, so the case is vacuous")
		}
	})
}

// TestSlowCallbackDoesNotStallAJoin: the boundary a joining subscription
// waits on spans no code of the caller's, so a slow callback delays the
// publication that runs it and nothing else.
//
// This is the half that re-entrancy does not show: even a
// callback that never touches the engine used to hold every Subscribe on the
// topic — and burn a core doing it — for as long as it ran.
func TestSlowCallbackDoesNotStallAJoin(t *testing.T) {
	const slow = 300 * time.Millisecond
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 1},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	inRelease := make(chan struct{})
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 0, Size: 1, Partition: 0, Mode: CopyOwned,
		// Run by the publication that trims record 0, not by this one.
		Release: func(int64) { close(inRelease); time.Sleep(slow) },
	}); err != nil {
		t.Fatalf("Publish 0: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Trims record 0, whose Release sleeps.
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Partition: 0, Mode: CopyOwned, Release: func(int64) {},
		}); err != nil {
			t.Errorf("Publish 1: %v", err)
		}
	}()
	<-inRelease
	start := time.Now()
	if _, err := top.Subscribe("late", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if took := time.Since(start); took > slow/2 {
		t.Fatalf("Subscribe took %v while a Release callback was running: the join waits on the boundary, and the boundary spans the callback", took)
	}
	<-done
}

// TestStreamJoinAdmitsEveryPublicationExactlyOnce is the same boundary as
// TestStreamSubscribeAdmitsAPublicationInFlightExactlyOnce in the shape that
// actually exercises it: one publisher and one joiner per topic, over many
// topics at once.
//
// The shape is the point. With several publishers on one topic a join that
// straddles the boundary is incidentally repaired — the next join waits on the
// other publishers, which gives the straddling one exactly the time it needs
// to reach its append — so the four-publisher test above passes tens of
// millions of publications whatever the boundary does. With one publisher per
// topic there is nothing else to wait for, and a boundary that lets a
// publication read one side of it and append on the other loses the record to
// the joining subscription: the live path excluded it, and the replay walked
// the log before the append.
//
// It is a logic race, not a data race — every access on the way is an atomic
// or under a mutex — so `-race` never reported one and only rounds find it.
// On the two-counter gate this replaced it lost a record about once in a
// hundred rounds under `-race` and about once in twenty-five without, so 300
// rounds is a gate rather than a gesture; raise the count to find it sooner.
//
// The engine is built and closed inside the round rather than through
// newTestEngine, and its lanes are sized for the 32 records a round
// publishes rather than for the default capacity. newTestEngine registers
// `t.Cleanup(e.Close)`, which keeps every engine of the loop reachable until
// the test returns however promptly each round closes its own, and a lane
// preallocates its ready queue: 64 topics x 4 subscriptions x 65,536 slots is
// 128 MB a round, so the loop reached 43 GB of resident memory and the test
// binary was SIGKILLed under `-race -count=5`. A test that cannot survive
// `-count=5` is a landmine rather than a gate.
func TestStreamJoinAdmitsEveryPublicationExactlyOnce(t *testing.T) {
	const rounds, topics, records, joiners = 300, 64, 32, 4
	for round := range rounds {
		e, err := New(Config[int64]{
			Kinds:           KindSet{Arch: featureArch},
			Clone:           func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 1 << 10},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		tops := make([]*Topic[int64], topics)
		subs := make([][]*Subscription[int64], topics)
		for i := range tops {
			top, err := e.CreateTopic("t"+strconv.Itoa(i), TopologyConfig{
				Kind: TopologyStream, Retention: Retention{MaxMessages: 1 << 16},
			})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			tops[i] = top
		}
		var wg sync.WaitGroup
		for i, top := range tops {
			wg.Add(2)
			go func() {
				defer wg.Done()
				for j := range records {
					if err := top.Publish(context.Background(), Publish[int64]{
						Payload: int64(j), Size: 1, Partition: 0,
					}); err != nil {
						t.Errorf("Publish: %v", err)
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				// Join while the publisher is running: the first retained
				// record says it has started.
				for {
					if n, _ := top.Retained(); n > 0 {
						break
					}
					runtime.Gosched()
				}
				for k := range joiners {
					s, err := top.Subscribe("s"+strconv.Itoa(k), SubscriptionConfig{
						Start: StartPosition{Kind: StartEarliest},
					})
					if err != nil {
						t.Errorf("Subscribe: %v", err)
						return
					}
					subs[i] = append(subs[i], s)
				}
			}()
		}
		wg.Wait()
		if t.Failed() {
			_ = e.Close()
			return
		}
		for i, top := range tops {
			kept, _ := top.Retained()
			for _, s := range subs[i] {
				snap := s.Inspect()
				if snap.Ready != kept || snap.Published != int64(kept) {
					t.Fatalf("round %d: topic %s subscription %s holds %d records (published %d) of the %d the topic retains",
						round, top.Name(), s.Name(), snap.Ready, snap.Published, kept)
				}
			}
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestJoinBoundaryExcludesAPublicationInFlight pins, deterministically and
// from inside the package, the one property the exactly-once argument rests on: a subscription cannot join a stream while a
// publication holds the boundary.
//
// The statistical test above is what catches a boundary that is merely
// wrong; this is what catches a boundary that is no longer there. It is the
// reader half of the mutual exclusion — the writer half, that a publication
// starting under a join reads the set the join stored, is the same lock seen
// from the other side and has no separate observation point.
func TestJoinBoundaryExcludesAPublicationInFlight(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 64})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 8},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	pb := top.begin(CopySafe, 0, false)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
			t.Errorf("Subscribe: %v", err)
		}
	}()
	select {
	case <-joined:
		t.Fatal("a subscription joined the topic while a publication held the join boundary")
	case <-time.After(50 * time.Millisecond):
	}
	pb.end()
	select {
	case <-joined:
	case <-time.After(joinBoundaryDeadline):
		t.Fatal("the join never completed after the publication gave the boundary up")
	}
}

// TestStartAtTimeIsAPositionInTheLog pins the contract of `StartAtTime`
// against the times `Topic.retain` clamps.
//
// The log keeps its entry times non-decreasing, because the clock is read
// outside the partition mutex and the append is inside it, so two concurrent
// publications can otherwise carry a later reading into an earlier slot.
// A `StartAtTime` replay filters on those clamped times, so
// it is a position in the log and not a predicate over publication instants:
// a record whose real instant precedes the requested one is replayed when the
// publication that overtook it gave it a later time in the log.
//
// The alternative — keeping a raw time on the entry beside the clamped one
// and filtering on the raw one — was rejected: the replay would then start at
// an entry the trim's age bound may already have passed over, so the same
// position would name different records depending on which of the two orders
// a reader used, and the log would no longer be one sequence read three ways.
// The clamp is what makes retention a property of a prefix; the position
// follows the log.
func TestStartAtTimeIsAPositionInTheLog(t *testing.T) {
	e, fake := fakeEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 16})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxMessages: 16},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	pub := func(v int64) {
		t.Helper()
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: v, Size: 1, Partition: 0,
		}); err != nil {
			t.Fatalf("Publish %d: %v", v, err)
		}
	}
	fake.Set(time.Unix(0, 100))
	pub(0)
	// The clock goes backwards, which is what one publication overtaking
	// another looks like to the log; record 1's entry time is clamped up to
	// record 0's.
	fake.Set(time.Unix(0, 50))
	pub(1)
	sub, err := top.Subscribe("s", SubscriptionConfig{
		Start: StartPosition{Kind: StartAtTime, At: time.Unix(0, 75)},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Both are replayed: record 1's instant is 50, before the requested 75,
	// and it is replayed anyway because the log times it at 100.
	if snap := sub.Inspect(); snap.Ready != 2 {
		t.Fatalf("StartAtTime(75) replayed %d records, want both: the position is the log's clamped time, not the publication instant", snap.Ready)
	}
}
