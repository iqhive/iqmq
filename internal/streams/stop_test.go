package streams_test

import (
	"context"
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

// TestStopKeepsTheHoldOfATrimmedEntryAHandlerHas is the other half: an entry
// trimmed away after the sweep brought it back is with a handler, and its
// settlement is what ends the hold. Lifting the hold at the trim would deliver
// the next entry beside the retry rather than after it.
func TestStopKeepsTheHoldOfATrimmedEntryAHandlerHas(t *testing.T) {
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
			r.srv.Advance(testLease + time.Second)
			retry, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(retry) != 1 || retry[0].Key.ID != first {
				t.Fatalf("the sweep delivered %d entries, want the retry of %v", len(retry), first)
			}
			if _, err := r.srv.Do(r.ctx, streams.XDel(r.keys.Stream(0), []streams.ID{first})); err != nil {
				t.Fatalf("XDEL: %v", err)
			}
			second := r.publish(t, "second")
			if _, _, err := r.cons.Sweep(r.ctx, cmder, nil, nil); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if n := r.cons.Held(); n != 1 {
				t.Fatalf("Held() = %d while the retry of %v is with its handler, want 1", n, first)
			}
			if more, _, _ := r.cons.Read(r.ctx, cmder, nil, nil); len(more) != 0 {
				t.Fatalf("the read delivered %v beside the retry of %v", more[0].Key, first)
			}
			if errs := r.cons.Ack(r.ctx, cmder, retry); errs[0] != nil {
				t.Fatalf("Ack of the retry: %v", errs[0])
			}
			next, _, err := r.cons.Read(r.ctx, cmder, nil, nil)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(next) != 1 || next[0].Key.ID != second {
				t.Fatalf("the read after the retry was settled delivered %d entries, want %v", len(next), second)
			}
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
type redis62 struct{ *fakestreams.Server }

func (s redis62) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	v, err := s.Server.Do(ctx, c)
	if err != nil || !strings.EqualFold(c.Name(), "XAUTOCLAIM") {
		return v, err
	}
	top, _ := v.Arr()
	if len(top) < 3 {
		return v, nil
	}
	claimed, _ := top[1].Arr()
	deleted, _ := top[2].Arr()
	key, group, consumer := c.Tokens[1], c.Tokens[2], c.Tokens[3]
	for _, d := range deleted {
		raw, _ := d.Str()
		id, perr := streams.ParseID(raw)
		if perr != nil {
			return streams.Nil(), perr
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
