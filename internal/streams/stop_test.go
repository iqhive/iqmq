package streams_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestStopKeepsWhatAParkedReadReturnsBehindTheHold is a hold placed while a
// read was already waiting at the server. The read chose its shards before
// the nack, so the server answers it with whatever is published during the
// block, and that entry is behind the failed one: delivering it would hand
// the handler the next message before the retry of the one that failed.
//
// The entry has to come back after the retry, as a first delivery: the sweep
// restarts its idle clock while it waits, rather than claiming it and
// counting the wait as an attempt.
func TestStopKeepsWhatAParkedReadReturnsBehindTheHold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) {
			c.Nack = streams.Stop
			c.Block = 2 * time.Second
		})
		r.publish(t, "first")
		ds := r.read(t)
		if len(ds) != 1 {
			t.Fatalf("Read returned %d deliveries, want 1", len(ds))
		}
		type result struct {
			ds  []streams.Delivery
			err error
		}
		out := make(chan result, 1)
		go func() {
			got, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
			out <- result{got, err}
		}()
		synctest.Wait()
		if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		second := r.publish(t, "second")
		got := <-out
		if got.err != nil {
			t.Fatalf("Read: %v", got.err)
		}
		if len(got.ds) != 0 {
			t.Fatalf("the read that was waiting at the server delivered %v behind %v, which failed and "+
				"has not been retried", got.ds[0].Key, ds[0].Key)
		}

		r.srv.Advance(testLease + time.Second)
		retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if len(retry) != 1 || retry[0].Key != ds[0].Key {
			t.Fatalf("the sweep delivered %d entries, want only the retry of %v", len(retry), ds[0].Key)
		}
		if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
			t.Fatalf("Ack of the retry: %v", errs[0])
		}
		next := r.read(t)
		if len(next) != 1 || next[0].Key.ID != second {
			t.Fatalf("after the retry was settled the read delivered %d entries, want the one held back", len(next))
		}
		if next[0].Attempt != 1 || next[0].Redelivered {
			t.Fatalf("the entry held back reports attempt %d (redelivered %v), want a first delivery: "+
				"waiting behind the hold was counted as an attempt", next[0].Attempt, next[0].Redelivered)
		}
	})
}

// TestStopHoldsAShardUntilEveryFailedEntryIsSettled is two deliveries of one
// shard failing before either is settled. The shard waits for both: settling
// one of them must not reopen it while the other is still to be retried.
func TestStopHoldsAShardUntilEveryFailedEntryIsSettled(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	second := r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	for _, d := range ds {
		if err := r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{}, r.srv.Now()); err != nil {
			t.Fatalf("Nack of %v: %v", d.Key, err)
		}
	}
	// Another consumer claims the second entry and settles it, which ends
	// that entry's part of the hold and nothing more.
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", second) {
		t.Fatal("hand-off failed")
	}
	if _, err := r.srv.Do(r.ctx, streams.XAck(r.keys.Stream(0), testGroup, []streams.ID{second})); err != nil {
		t.Fatalf("XACK: %v", err)
	}
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n := r.cons.Held(); n != 1 {
		t.Fatalf("Held() = %d after the second entry was settled, want 1: %v has not been retried yet", n, ds[0].Key)
	}
	r.publish(t, "third")
	if more := r.read(t); len(more) != 0 {
		t.Fatalf("the shard delivered %d new entries while %v was still waiting for its retry", len(more), ds[0].Key)
	}

	r.srv.Advance(testLease + time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retry) != 1 || retry[0].Key != ds[0].Key {
		t.Fatalf("the sweep delivered %d entries, want the retry of %v", len(retry), ds[0].Key)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
		t.Fatalf("Ack of the retry: %v", errs[0])
	}
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d once every failed entry was settled, want 0", n)
	}
	if next := r.read(t); len(next) != 1 {
		t.Fatalf("the shard delivered %d entries after its hold lifted, want 1", len(next))
	}
}

// TestStopRetriesTheFailedEntriesOfAShardInOrder is the same two failures
// with the first one's retry delayed, so the sweep could claim the second one
// first. The second is behind the first, and its retry waits for the first
// one's to be settled.
func TestStopRetriesTheFailedEntriesOfAShardInOrder(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	r.srv.Advance(2 * time.Second)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Delay: testLease}, r.srv.Now()); err != nil {
		t.Fatalf("Nack of the first: %v", err)
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[1], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack of the second: %v", err)
	}
	// The second entry has been idle for a deadline and the first, whose
	// idle clock the delay restarted, has not.
	r.srv.Advance(testLease - 2*time.Second + 500*time.Millisecond)
	early, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(early) != 0 {
		t.Fatalf("the sweep delivered %v before the retry of %v, which is ahead of it in the shard",
			early[0].Key, ds[0].Key)
	}
	r.srv.Advance(2 * time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retry) != 1 || retry[0].Key != ds[0].Key {
		t.Fatalf("the sweep delivered %d entries, want only the retry of %v", len(retry), ds[0].Key)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
		t.Fatalf("Ack of the first retry: %v", errs[0])
	}
	if n := r.cons.Held(); n != 1 {
		t.Fatalf("Held() = %d with the second entry still to be retried, want 1", n)
	}
	r.publish(t, "third")
	next := r.read(t)
	if len(next) != 1 || next[0].Key != ds[1].Key {
		t.Fatalf("the read delivered %d entries, want only the retry of %v", len(next), ds[1].Key)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, next); errs[0] != nil {
		t.Fatalf("Ack of the second retry: %v", errs[0])
	}
	if last := r.read(t); len(last) != 1 || last[0].Entry.Fields[1] != "third" {
		t.Fatalf("the shard delivered %d entries after both retries were settled, want the third", len(last))
	}
}

// TestStopForgetsAQueuedEntryAnotherConsumerTook is an entry waiting behind
// the hold that another consumer claims meanwhile. It is that consumer's to
// deliver now, so once the hold lifts this one must not hand it out as well:
// that would be a second delivery whose settlement the guard refuses.
func TestStopForgetsAQueuedEntryAnotherConsumerTook(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	second := r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	// The second delivery's lease runs out too, and the sweep that retries
	// the first claims it from behind the hold and keeps it back.
	r.srv.Advance(testLease + time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retry) != 1 || retry[0].Key != ds[0].Key {
		t.Fatalf("the sweep delivered %d entries, want only the retry of %v", len(retry), ds[0].Key)
	}
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", second) {
		t.Fatal("hand-off failed")
	}
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
		t.Fatalf("Ack of the retry: %v", errs[0])
	}
	if next := r.read(t); len(next) != 0 {
		t.Fatalf("the read delivered %v, which another consumer holds", next[0].Key)
	}
}

// TestStopRestartsAQueuedEntrysIdleClockAsItHandsItOut is an entry queued
// behind the hold and handed out once the retry in front of it is settled.
//
// Its idle clock last restarted when the sweep queued it, or at the sweep
// after, which can be most of a sweep interval before the read that hands it
// out. The caller counts the delivery's lease from that read, so another
// consumer's sweep took the entry while the caller still believed it had
// time, and the settlement was refused.
func TestStopRestartsAQueuedEntrysIdleClockAsItHandsItOut(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	second := r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	// The sweep retries the first entry and claims the second from behind
	// the hold, which queues it.
	r.srv.Advance(testLease + time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retry) != 1 || retry[0].Key != ds[0].Key {
		t.Fatalf("the sweep delivered %d entries, want only the retry of %v", len(retry), ds[0].Key)
	}
	r.srv.Advance(3 * time.Second)
	if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
		t.Fatalf("Ack of the retry: %v", errs[0])
	}
	next := r.read(t)
	if len(next) != 1 || next[0].Key.ID != second {
		t.Fatalf("the read after the retry was settled delivered %d entries, want %v", len(next), second)
	}
	for _, p := range r.srv.Pending(r.keys.Stream(0), testGroup) {
		if p.ID == second && p.Idle != 0 {
			t.Fatalf("the read handed %v out idle for %v, so its lease runs out %v before the one counted from the read",
				second, p.Idle, p.Idle)
		}
	}
}

// TestStopTakesBackADeliveryNobodyWasHanded is a delivery a read returned
// that its caller never handed to a handler, given back once the entry in
// front of it has failed.
//
// It has to come back after the retry as it went out: the same generation,
// so it is still settleable, and the same attempt, because nobody tried it.
// The only way to give it back was a nack, which left it for the sweep to
// claim, and the claim counted an attempt nobody made, so a failure policy
// fired one handler call early.
func TestStopTakesBackADeliveryNobodyWasHanded(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	second := r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	if r.cons.Return(ds[1]) {
		t.Fatalf("Return took %v back with no hold in front of it; its caller hands it out itself", second)
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if !r.cons.Return(ds[1]) {
		t.Fatalf("Return refused %v, which is behind the hold", second)
	}
	if more := r.read(t); len(more) != 0 {
		t.Fatalf("the read handed out %v before the retry of %v", more[0].Key, ds[0].Key)
	}
	r.srv.Advance(testLease + time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retry) != 1 || retry[0].Key != ds[0].Key {
		t.Fatalf("the sweep delivered %d entries, want only the retry of %v", len(retry), ds[0].Key)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, retry); errs[0] != nil {
		t.Fatalf("Ack of the retry: %v", errs[0])
	}
	next := r.read(t)
	if len(next) != 1 || next[0].Key.ID != second {
		t.Fatalf("the read after the retry was settled delivered %d entries, want %v", len(next), second)
	}
	if next[0].Gen != ds[1].Gen || next[0].Attempt != 1 || next[0].Redelivered {
		t.Fatalf("%v came back as generation %d, attempt %d (redelivered %v), want generation %d, attempt 1",
			second, next[0].Gen, next[0].Attempt, next[0].Redelivered, ds[1].Gen)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, next); errs[0] != nil {
		t.Fatalf("Ack of %v: %v", second, errs[0])
	}
}

// TestStopReturnKeepsTheNewerQueuedGeneration is the window between a
// caller's generation check and its Return: a sweep claims the entry again
// and queues the newer generation behind the hold, and then the caller gives
// back its older copy.
//
// The older copy must not replace the newer one. Replacing it, as queueing a
// delivery does, left a superseded generation in the queue, which the read
// drops when it hands it out, and the entry waited for the next sweep to
// claim it again with another attempt counted.
func TestStopReturnKeepsTheNewerQueuedGeneration(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	second := r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	// The caller still holds ds[1] when the sweep claims it again: it is idle
	// past the deadline, so the sweep registers a newer generation and queues
	// it behind the hold.
	r.srv.Advance(testLease + time.Second)
	retry, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if r.cons.Reclaimer().Check(ds[1].Key, ds[1].Gen) == nil {
		t.Fatalf("the sweep did not supersede %v, so the test proves nothing", second)
	}
	if !r.cons.Return(ds[1]) {
		t.Fatalf("Return refused the older copy of an entry already queued")
	}
	for _, d := range retry {
		if errs := r.cons.Ack(r.ctx, r.srv, []streams.Delivery{d}); errs[0] != nil {
			t.Fatalf("Ack of the retry: %v", errs[0])
		}
	}
	next := r.read(t)
	if len(next) != 1 || next[0].Key.ID != second {
		t.Fatalf("the read after the retry delivered %d entries, want %v", len(next), second)
	}
	if next[0].Gen == ds[1].Gen {
		t.Fatalf("the queue handed out the older generation %d of %v; the sweep's newer one was replaced",
			next[0].Gen, second)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, next); errs[0] != nil {
		t.Fatalf("Ack of %v (generation %d, attempt %d): %v", second, next[0].Gen, next[0].Attempt, errs[0])
	}
}

// TestStopLiftsTheHoldOfAnEntryTrimmedAway is a held entry trimmed out of the
// stream before the sweep brings it back. Nothing can deliver it again, so
// nothing will settle it, and a hold that waits for that settlement stops its
// shard for good.
//
// Redis 7.0's XAUTOCLAIM drops such an entry from the pending list and names
// it in its reply, which lifted the hold. Redis 6.2 does neither: it claims
// the entry like any other and answers a null in its place, so the pending row
// stays this consumer's and the hold's own check found it still waiting.
func TestStopLiftsTheHoldOfAnEntryTrimmedAway(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cmder func(*fakestreams.Server) streams.Commander
	}{
		{"redis-7", func(srv *fakestreams.Server) streams.Commander { return srv }},
		{"redis-6.2", func(srv *fakestreams.Server) streams.Commander { return redis62{srv} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
			cmder := tc.cmder(r.srv)
			first := r.publish(t, "first")
			ds := r.read(t)
			if len(ds) != 1 {
				t.Fatalf("Read returned %d deliveries, want 1", len(ds))
			}
			if err := r.cons.Nack(r.ctx, cmder, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			if _, err := r.srv.Do(r.ctx, streams.XDel(r.keys.Stream(0), []streams.ID{first})); err != nil {
				t.Fatalf("XDEL: %v", err)
			}
			second := r.publish(t, "second")

			for range 3 {
				r.srv.Advance(testLease + time.Second)
				if _, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil); err != nil {
					t.Fatalf("Sweep: %v", err)
				}
			}
			if n := r.cons.Held(); n != 0 {
				t.Fatalf("Held() = %d three deadlines after %v was trimmed away, want 0: nothing can deliver it again",
					n, first)
			}
			next, _, err := r.cons.Read(r.ctx, cmder, nil, nil)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(next) != 1 || next[0].Key.ID != second {
				t.Fatalf("the read after the hold lifted delivered %d entries, want %v", len(next), second)
			}
			for _, p := range r.srv.Pending(r.keys.Stream(0), testGroup) {
				if p.ID == first {
					t.Errorf("%v is still pending to %s, with nothing left in the stream to deliver", first, p.Consumer)
				}
			}
		})
	}
}

// trimmedRetry is a Stop hold whose retry is with its handler when the entry
// is trimmed out of the stream, with a round of the sweep since and a second
// entry published behind it. It returns the retry and the second entry.
func trimmedRetry(t *testing.T, r *rig, cmder streams.Commander) (retry streams.Delivery, second streams.ID) {
	t.Helper()
	first := r.publish(t, "first")
	ds := r.read(t)
	if len(ds) != 1 {
		t.Fatalf("Read returned %d deliveries, want 1", len(ds))
	}
	if err := r.cons.Nack(r.ctx, cmder, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	r.srv.Advance(testLease + time.Second)
	retries, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(retries) != 1 || retries[0].Key.ID != first {
		t.Fatalf("the sweep delivered %d entries, want the retry of %v", len(retries), first)
	}
	if _, err := r.srv.Do(r.ctx, streams.XDel(r.keys.Stream(0), []streams.ID{first})); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
	second = r.publish(t, "second")
	if _, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	return retries[0], second
}

// expectHeldThen checks that the shard is still held, then runs settle,
// which must end the hold, and that the read after it delivers second.
func expectHeldThen(t *testing.T, r *rig, cmder streams.Commander, second streams.ID, settle func()) {
	t.Helper()
	if n := r.cons.Held(); n != 1 {
		t.Fatalf("Held() = %d while the retry is with its handler, want 1", n)
	}
	if more, _, _ := r.cons.Read(r.ctx, cmder, nil, nil); len(more) != 0 {
		t.Fatalf("the read delivered %v beside the retry", more[0].Key)
	}
	settle()
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d once the retry was done with, want 0", n)
	}
	next, _, err := r.cons.Read(r.ctx, cmder, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(next) != 1 || next[0].Key.ID != second {
		t.Fatalf("the read after the retry was done with delivered %d entries, want %v", len(next), second)
	}
}

// TestStopKeepsTheHoldOfATrimmedEntryAHandlerHas is the other half: an entry
// trimmed away after the sweep brought it back is with a handler, and its
// settlement is what ends the hold. Lifting the hold at the trim would deliver
// the next entry beside the retry rather than after it.
//
// Redis 7.0 and later drop the pending row of an entry gone from the stream
// however recently it was delivered, and name it among XAUTOCLAIM's deleted
// entries; the held check then finds no row. Either released the hold, so on
// 7.x the entry behind the retry went out while the retry's handler ran
// (against a real 7.4, 670ms after the trim). The settlement is refused
// there, the row being gone, and the refusal ends the hold.
func TestStopKeepsTheHoldOfATrimmedEntryAHandlerHas(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cmder func(*fakestreams.Server) streams.Commander
		lost  bool
	}{
		{"redis-7", func(srv *fakestreams.Server) streams.Commander { return srv }, true},
		{"redis-6.2", func(srv *fakestreams.Server) streams.Commander { return redis62{srv} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
			cmder := tc.cmder(r.srv)
			retry, second := trimmedRetry(t, r, cmder)
			expectHeldThen(t, r, cmder, second, func() {
				err := r.cons.Ack(r.ctx, cmder, []streams.Delivery{retry})[0]
				if tc.lost != errors.Is(err, mq.ErrLeaseLost) {
					t.Fatalf("Ack of the retry: %v; want a refusal: %v", err, tc.lost)
				}
			})
		})
	}
}

// TestStopEndsTheHoldOfATrimmedRetryHoweverItsHandlerEnds is every other way
// the retry's handler can finish with it: a nack that drops it, dead-letters
// it or fails it again, and a lease extension, which finds the row gone and
// tells the handler to stop. Each ends the hold, the requeue at the next
// round because nothing can bring the entry back.
func TestStopEndsTheHoldOfATrimmedRetryHoweverItsHandlerEnds(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	for _, tc := range []struct {
		name   string
		settle func(r *rig, retry streams.Delivery) error
		lost   bool
	}{
		{"nack-drop", func(r *rig, d streams.Delivery) error {
			return r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{Action: mq.NackActionDrop}, r.srv.Now())
		}, true},
		{"nack-dead-letter", func(r *rig, d streams.Delivery) error {
			return r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now())
		}, true},
		{"nack-requeue", func(r *rig, d streams.Delivery) error {
			if err := r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{}, r.srv.Now()); err != nil {
				return err
			}
			_, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
			return err
		}, false},
		{"extend", func(r *rig, d streams.Delivery) error {
			return r.cons.Extend(r.ctx, r.srv, []streams.Delivery{d})[0]
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) {
				c.Nack = streams.Stop
				c.DeadLetterStream = keys.Stream(0) + ":dlq"
			})
			retry, second := trimmedRetry(t, r, r.srv)
			expectHeldThen(t, r, r.srv, second, func() {
				if err := tc.settle(r, retry); tc.lost != errors.Is(err, mq.ErrLeaseLost) {
					t.Fatalf("%s of the retry: %v; want a refusal: %v", tc.name, err, tc.lost)
				}
			})
		})
	}
}

// TestStopEndsTheHoldOfATrimmedRetryWhoseAckFailed is a retry whose handler
// tried to acknowledge it and failed in transport. The delivery stays
// settleable, but nothing says the handler will try again, so the hold that
// waited for the handler cannot wait for ever: the next round ends it.
//
// On 6.2 the trimmed retry's row stays this consumer's, so only the round's
// lookup of the entry in the stream can find it gone. That lookup was left
// out while this consumer held any delivery of the entry, a failed one
// included, and the shard stayed held for good.
func TestStopEndsTheHoldOfATrimmedRetryWhoseAckFailed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cmder func(*fakestreams.Server) streams.Commander
	}{
		{"redis-7", func(srv *fakestreams.Server) streams.Commander { return srv }},
		{"redis-6.2", func(srv *fakestreams.Server) streams.Commander { return redis62{srv} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
			cmder := tc.cmder(r.srv)
			retry, second := trimmedRetry(t, r, cmder)
			expectHeldThen(t, r, cmder, second, func() {
				r.srv.Fail("EVALSHA", errors.New("LOADING Redis is loading the dataset in memory"))
				if err := r.cons.Ack(r.ctx, cmder, []streams.Delivery{retry})[0]; err == nil {
					t.Fatal("the Ack the server refused reported success")
				}
				if _, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil); err != nil {
					t.Fatalf("Sweep: %v", err)
				}
			})
		})
	}
}

// duringAck is the fake running a hook just before the first pipeline that
// carries an acknowledgement script, which is an Ack in flight.
type duringAck struct {
	*fakestreams.Server
	ackSHA string
	hook   func()
}

func (d *duringAck) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	for _, c := range cmds {
		if c.Name() == "EVALSHA" && len(c.Tokens) > 1 && c.Tokens[1] == d.ackSHA && d.hook != nil {
			hook := d.hook
			d.hook = nil
			hook()
			break
		}
	}
	return d.Server.DoMulti(ctx, cmds)
}

// TestStopKeepsTheHoldOfATrimmedRetryWhileItsAckIsInFlight is a round that
// runs while the retry's acknowledgement is on its way. The handler has not
// failed to settle it, it is settling it, so the round keeps the hold and the
// acknowledgement's answer ends it.
func TestStopKeepsTheHoldOfATrimmedRetryWhileItsAckIsInFlight(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	retry, second := trimmedRetry(t, r, r.srv)
	cmder := &duringAck{Server: r.srv, ackSHA: streams.SHAOf(streams.ScriptAckGuard)}
	cmder.hook = func() {
		if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
			t.Errorf("Sweep: %v", err)
		}
		if n := r.cons.Held(); n != 1 {
			t.Errorf("Held() = %d after a round run while the retry's Ack was in flight, want 1", n)
		}
	}
	expectHeldThen(t, r, r.srv, second, func() {
		if err := r.cons.Ack(r.ctx, cmder, []streams.Delivery{retry})[0]; !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("Ack of the retry: %v, want a refusal", err)
		}
		if cmder.hook != nil {
			t.Fatal("the Ack sent no acknowledgement script, so the test proves nothing")
		}
	})
}

// TestStopLetsGoOfAHoldAnotherConsumerSettled is a held entry another
// consumer claimed and acknowledged once this one's lease had run out. With
// no delivery of it left here the hold is not this consumer's to keep and the
// next round ends it; with this consumer's retry still with its handler, the
// hold waits for that handler, as it does for a trimmed entry.
func TestStopLetsGoOfAHoldAnotherConsumerSettled(t *testing.T) {
	for _, handling := range []bool{false, true} {
		t.Run("handling="+strconv.FormatBool(handling), func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
			key := r.keys.Stream(0)
			first := r.publish(t, "first")
			ds := r.read(t)
			if len(ds) != 1 {
				t.Fatalf("Read returned %d deliveries, want 1", len(ds))
			}
			if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			r.srv.Advance(testLease + time.Second)
			var retry []streams.Delivery
			if handling {
				var err error
				if retry, _, err = r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil || len(retry) != 1 {
					t.Fatalf("Sweep = %d, %v; want the retry", len(retry), err)
				}
				r.srv.Advance(testLease + time.Second)
			}
			if !r.srv.Deliver(key, testGroup, "source-b", first) {
				t.Fatal("the fake could not hand the entry to another consumer")
			}
			if _, err := r.srv.Do(r.ctx, streams.XAck(key, testGroup, []streams.ID{first})); err != nil {
				t.Fatalf("XACK: %v", err)
			}
			second := r.publish(t, "second")
			if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !handling {
				if n := r.cons.Held(); n != 0 {
					t.Fatalf("Held() = %d for an entry another consumer settled, with no delivery of it here", n)
				}
				return
			}
			expectHeldThen(t, r, r.srv, second, func() {
				if err := r.cons.Ack(r.ctx, r.srv, retry)[0]; !errors.Is(err, mq.ErrLeaseLost) {
					t.Fatalf("Ack of the retry another consumer settled: %v, want a refusal", err)
				}
			})
		})
	}
}

// redis62 is the fake answering XAUTOCLAIM as Redis 6.2 does.
//
// The fake follows 7.0, which drops a pending entry whose stream entry is gone
// and names it in a third reply element. 6.2 has no third element: it claims
// such an entry like any other, so the row stays pending to the claimer with
// its idle clock restarted, and the reply carries a null in the entry's place.
// The commands of a pipeline run one at a time, so a command after the claim
// sees the row the claim left.
//
// 6.2 checks idle before it checks the stream, so a deleted entry's row that
// is not yet idle is left with its holder, where 7.0 drops it whatever its
// idle time. Such a row is given back to its holder, with its idle clock and
// counter restarted, which is all these tests can tell.
type redis62 struct{ *fakestreams.Server }

func (s redis62) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if !strings.EqualFold(c.Name(), "XAUTOCLAIM") || len(c.Tokens) < 5 {
		return s.Server.Do(ctx, c)
	}
	key, group, consumer := c.Tokens[1], c.Tokens[2], c.Tokens[3]
	minIdle, perr := strconv.ParseInt(c.Tokens[4], 10, 64)
	if perr != nil {
		return streams.Nil(), perr
	}
	before := map[streams.ID]streams.Pending{}
	for _, p := range s.Pending(key, group) {
		before[p.ID] = p
	}
	v, err := s.Server.Do(ctx, c)
	if err != nil {
		return v, err
	}
	top, _ := v.Arr()
	if len(top) < 3 {
		return v, nil
	}
	claimed, _ := top[1].Arr()
	deleted, _ := top[2].Arr()
	for _, d := range deleted {
		raw, _ := d.Str()
		id, perr := streams.ParseID(raw)
		if perr != nil {
			return streams.Nil(), perr
		}
		if was, ok := before[id]; ok && was.Idle < time.Duration(minIdle)*time.Millisecond {
			s.Deliver(key, group, was.Consumer, id)
			continue
		}
		s.Deliver(key, group, consumer, id)
		claimed = append(claimed, streams.Nil())
	}
	return streams.Arr(top[0], streams.ArrOf(claimed)), nil
}

func (s redis62) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	out := make([]streams.Result, len(cmds))
	for i, c := range cmds {
		v, err := s.Do(ctx, c)
		out[i] = streams.Result{Value: v, Err: err}
	}
	return out, nil
}
