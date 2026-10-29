package jetstream

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/testkit"
)

// TestAMessageThatWaitedBehindABlockedCallbackKeepsTheServersLease holds the
// lease of a message that queued in the client library while the consume
// callback was blocked.
//
// nats.go hands the callback one message at a time, so the message behind a
// blocked one waits in the library with the server's lease already running.
// Its lease used to start when the callback was handed it, half a lease after
// the server's had, and an ack in that half reached the wire after the server
// could have given the record to another consumer. It starts when the blocked
// callback started instead. After a pause the library is waiting for a
// message rather than holding one, so the next lease starts at once.
func TestAMessageThatWaitedBehindABlockedCallbackKeepsTheServersLease(t *testing.T) {
	ctx := context.Background()
	src, clk := newRingSource(t, 1)
	start := clk.Now()
	src.onMessage(&recordMsg{seq: 1, delivered: 1})
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		src.onMessage(&recordMsg{seq: 2, delivered: 1})
	}()
	eventually(t, "the callback blocks on the full ring", func() bool { return src.push.Blocked() == 1 })
	clk.Advance(src.ackWait / 2)
	fetchScripted(t, src)
	<-blocked
	fetchScripted(t, src)

	queued := &recordMsg{seq: 3, delivered: 1}
	src.onMessage(queued)
	d := fetchScripted(t, src)
	if want := start.Add(src.ackWait); d.Info.LeaseExpires.After(want) {
		t.Errorf("the message that waited behind the blocked callback reports a lease to %v, %v after the server's ends",
			d.Info.LeaseExpires.Sub(start), d.Info.LeaseExpires.Sub(want))
	}
	clk.Advance(src.ackWait/2 - leaseGuard(src.ackWait)/2)
	if err := src.set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("an ack inside the guard of the server's lease reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
	if queued.acks != 0 {
		t.Error("the ack reached the wire after the server could have handed the record to another consumer")
	}

	clk.Advance(idleGap)
	src.onMessage(&recordMsg{seq: 4, delivered: 1})
	d = fetchScripted(t, src)
	if want := clk.Now().Add(src.ackWait); !d.Info.LeaseExpires.Equal(want) {
		t.Errorf("a message handed over after a pause reports a lease to %v, want %v", d.Info.LeaseExpires, want)
	}
}

// TestDeadDeliveriesInAFullRingDoNotHoldUpTheCallback holds the consume
// callback to the rule that it blocks only for live deliveries.
//
// When nobody fetches for a lease the server sends every record again, and the
// first redelivery used to supersede its own copy in the ring and then block
// behind it, with the others waiting in the client library and their leases
// running. A copy that was superseded, or whose lease ran out in the ring,
// leaves the ring instead, and the delivery that owns the record takes its
// place.
func TestDeadDeliveriesInAFullRingDoNotHoldUpTheCallback(t *testing.T) {
	t.Run("superseded", func(t *testing.T) {
		src, _ := newRingSource(t, 1)
		src.onMessage(&recordMsg{seq: 1, delivered: 1})
		if !callbackReturns(t, src, &recordMsg{seq: 1, delivered: 2}) {
			t.Fatal("the redelivery blocked behind the copy it superseded")
		}
		if d := fetchScripted(t, src); d.Info.Attempt != 2 {
			t.Errorf("the ring hands out attempt %d, want the redelivery", d.Info.Attempt)
		}
		if got := src.b.SupersededDeliveries(); got != 1 {
			t.Errorf("SupersededDeliveries = %d, want 1", got)
		}
	})
	t.Run("expired", func(t *testing.T) {
		src, clk := newRingSource(t, 1)
		src.onMessage(&recordMsg{seq: 1, delivered: 1})
		clk.Advance(src.ackWait)
		if !callbackReturns(t, src, &recordMsg{seq: 2, delivered: 1}) {
			t.Fatal("the next message blocked behind a delivery whose lease had run out in the ring")
		}
		if d := fetchScripted(t, src); d.Info.Sequence != 2 {
			t.Errorf("the ring hands out record %d, want 2", d.Info.Sequence)
		}
		if got := src.b.SupersededDeliveries(); got != 1 {
			t.Errorf("SupersededDeliveries = %d, want 1", got)
		}
	})
}

// TestAFullTokenTableHoldsUpTheCallbackInsteadOfNaking holds the consume
// callback's wait for a token slot to its three exits.
//
// A delivery that found the table full was naked, and on a durable with no
// MaxAckPending the server sent it straight back to a table that was still
// full, in a loop. The callback waits instead. A settlement that frees a slot
// lets the message in, a Close requeues it, and a lease that runs out while it
// waits drops it without a nak, so that the redeliveries queued behind it can
// free their own slots.
func TestAFullTokenTableHoldsUpTheCallbackInsteadOfNaking(t *testing.T) {
	// park fills a two-slot table with deliveries the caller holds and hands
	// the callback a third record, which has to wait.
	park := func(t *testing.T) (*source, []mq.DriverDelivery, *recordMsg, <-chan struct{}) {
		t.Helper()
		src := newBubbleSource(t, 2)
		src.onMessage(&recordMsg{seq: 1, delivered: 1})
		src.onMessage(&recordMsg{seq: 2, delivered: 1})
		held := []mq.DriverDelivery{fetchScripted(t, src), fetchScripted(t, src)}
		waiting := &recordMsg{seq: 3, delivered: 1}
		done := make(chan struct{})
		go func() {
			defer close(done)
			src.onMessage(waiting)
		}()
		if returned(done) {
			t.Fatalf("the callback returned with the token table full (%d naks, %d consume errors)", waiting.naks, src.b.ConsumeErrors())
		}
		if src.push.Blocked() != 0 {
			t.Fatal("the callback is waiting for room in the ring, not for a token slot")
		}
		return src, held, waiting, done
	}
	t.Run("a settlement frees a slot", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src, held, waiting, done := park(t)
			if err := src.set.Ack(context.Background(), &held[0]); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			<-done
			if d := fetchScripted(t, src); d.Info.Sequence != 3 {
				t.Errorf("the ring hands out record %d, want the one that waited", d.Info.Sequence)
			}
			if waiting.naks != 0 || src.b.ConsumeErrors() != 0 {
				t.Errorf("the message that waited was naked %d times with %d consume errors", waiting.naks, src.b.ConsumeErrors())
			}
		})
	})
	t.Run("the source closes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src, _, waiting, done := park(t)
			// Close begins with the closing flag, and its next step, the
			// consumer's drain, waits for the callback to return. So the
			// flag alone has to end the wait, or every Close waits out
			// DrainTimeout.
			src.stop()
			if !returned(done) {
				t.Fatal("the callback still waits for a slot after Close began")
			}
			if err := src.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if waiting.naks != 1 {
				t.Errorf("the message waiting when the source closed was naked %d times, want once", waiting.naks)
			}
			if got := src.b.ConsumeErrors(); got != 0 {
				t.Errorf("a Close during the wait reported %d consume errors, want 0", got)
			}
		})
	})
	t.Run("the lease runs out", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src, _, waiting, done := park(t)
			time.Sleep(src.ackWait - leaseGuard(src.ackWait))
			if !returned(done) {
				t.Fatal("the callback still waits for a slot after its message's lease reached the guard")
			}
			if waiting.naks != 0 {
				t.Errorf("a message whose lease ran out while it waited was naked %d times: the server may already have sent it elsewhere", waiting.naks)
			}
			if got := src.b.SupersededDeliveries(); got != 1 {
				t.Errorf("SupersededDeliveries = %d, want 1", got)
			}
			// The server sends a held record again once its lease has
			// ended.
			time.Sleep(leaseGuard(src.ackWait))
			redelivery := &recordMsg{seq: 1, delivered: 2}
			if !callbackReturns(t, src, redelivery) {
				t.Fatal("the redelivery of a held record waited for a slot its own arrival frees")
			}
			if d := fetchScripted(t, src); d.Info.Sequence != 1 || d.Info.Attempt != 2 {
				t.Errorf("the ring hands out record %d attempt %d, want the redelivery of record 1", d.Info.Sequence, d.Info.Attempt)
			}
		})
	})
}

// returned reports whether the goroutine that closes done has finished once
// everything else in the synctest bubble is blocked.
func returned(done <-chan struct{}) bool {
	synctest.Wait()
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// newBubbleSource is newScriptedSource on the wall clock, which inside a
// synctest bubble is the bubble's, so that a lease runs out while a callback
// waits on a channel.
func newBubbleSource(tb testing.TB, slots int) *source {
	tb.Helper()
	src := newScriptedSource(tb, slots)
	src.b.clock = nil
	return src
}

// TestTheDeadLetterCopyOfAnUndecodableMessageKeepsToItsLease holds the one
// round trip the consume callback makes to the lease rules the settler keeps.
//
// The copy is published before the original is terminated, on the callback,
// so the messages behind it wait in the client library for the PubAck: the
// next one's lease starts from before the copy, not after it. And a copy whose
// publication outlived the undecodable message's own lease writes no Term,
// because the Term would land on whichever delivery owns the record now.
func TestTheDeadLetterCopyOfAnUndecodableMessageKeepsToItsLease(t *testing.T) {
	src, clk := newRingSource(t, 4)
	src.meta = metaAdaptive
	src.b.env = scriptedEnvelope(t)
	src.b.facts.deadLetter = "scripted.dlq"
	slow := &slowPublisher{clk: clk, by: src.ackWait / 2}
	src.b.js = slow

	start := clk.Now()
	quick := undecodableMsg(1)
	src.onMessage(quick)
	if slow.published != 1 || quick.terms != 1 {
		t.Fatalf("a copy published inside the lease: %d copies and %d terms, want one of each", slow.published, quick.terms)
	}
	src.onMessage(&recordMsg{seq: 2, delivered: 1})
	if d := fetchScripted(t, src); d.Info.LeaseExpires.After(start.Add(src.ackWait)) {
		t.Errorf("the message behind the dead-letter copy reports a lease %v longer than the server's", d.Info.LeaseExpires.Sub(start.Add(src.ackWait)))
	}

	clk.Advance(idleGap)
	slow.by = src.ackWait
	late := undecodableMsg(3)
	src.onMessage(late)
	if slow.published != 2 {
		t.Fatalf("the dead-letter copy was published %d times, want twice", slow.published)
	}
	if late.terms != 0 {
		t.Error("the original was terminated after its lease ran out: the server may have handed the record to another consumer, and the Term lands on that delivery")
	}
	if got := src.b.DeadLettered(); got != 1 {
		t.Errorf("DeadLettered = %d, want 1: the second copy terminated nothing", got)
	}
	if got := src.b.StaleSettlements(); got != 1 {
		t.Errorf("StaleSettlements = %d, want 1 for the Term that was not written", got)
	}
}

// TestTheLeaseFollowsTheDurablesBackOff holds a source bound to a durable with
// a BackOff to the lease the server gives each delivery.
//
// The server times delivery n of a record by BackOff[n-1], the last step for
// every delivery past the list, and a `+WPI` restarts the same step. Every
// delivery used to get AckWait, which the server sets to the first step, so a
// valid ack early in a ten-second redelivery was refused, and so was an Extend
// of the lease it had. The guard is a fifth of the delivery's own lease too.
func TestTheLeaseFollowsTheDurablesBackOff(t *testing.T) {
	ctx := context.Background()
	src, clk := newClockedSource(t, 8)
	backoff := []time.Duration{time.Second, 10 * time.Second}
	if err := src.bindDurable(&js.ConsumerInfo{Config: js.ConsumerConfig{AckWait: backoff[0], BackOff: backoff}}, 8); err != nil {
		t.Fatalf("bindDurable: %v", err)
	}
	for _, tc := range []struct {
		delivered uint64
		want      time.Duration
	}{{1, backoff[0]}, {2, backoff[1]}, {5, backoff[1]}} {
		d := deliverAttempt(t, src, &scriptedMsg{}, tc.delivered, tc.delivered)
		if lease := d.Info.LeaseExpires.Sub(d.Info.ReceivedAt); lease != tc.want {
			t.Errorf("delivery %d reports a %v lease, want %v", tc.delivered, lease, tc.want)
		}
	}

	second := &scriptedMsg{}
	d := deliverAttempt(t, src, second, 10, 2)
	clk.Advance(backoff[0] + backoff[0]/5)
	if err := src.set.Extend(ctx, &d, backoff[1]); err != nil {
		t.Fatalf("Extend(%v) of a second delivery reported %v (code %v): +WPI restarts its %v step", backoff[1], err, mq.CodeOf(err), backoff[1])
	}
	clk.Advance(backoff[1] - time.Second)
	if err := src.set.Ack(ctx, &d); err != nil {
		t.Fatalf("an ack inside the extended %v lease reported %v (code %v)", backoff[1], err, mq.CodeOf(err))
	}
	if second.acks != 1 || second.progress != 1 {
		t.Errorf("the second delivery wrote %d acks and %d +WPI, want one of each", second.acks, second.progress)
	}

	first := &scriptedMsg{}
	d = deliverAttempt(t, src, first, 11, 1)
	if err := src.set.Extend(ctx, &d, backoff[1]); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("Extend(%v) of a first delivery reported %v (code %v), want InvalidArgument: +WPI restarts its %v step and no more",
			backoff[1], err, mq.CodeOf(err), backoff[0])
	}
	clk.Advance(backoff[0] + backoff[0]/5)
	if err := src.set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("an ack after a first delivery's %v step reported %v (code %v), want ErrLeaseLost", backoff[0], err, mq.CodeOf(err))
	}

	d = deliverAttempt(t, src, &scriptedMsg{}, 12, 2)
	clk.Advance(backoff[1] - 3*maxLeaseGuard/5)
	if err := src.set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("an ack %v before a %v lease ends reported %v (code %v), want ErrLeaseLost: the guard is a fifth of the delivery's lease, at most %v",
			3*maxLeaseGuard/5, backoff[1], err, mq.CodeOf(err), maxLeaseGuard)
	}
}

// newRingSource is newClockedSource with a fetch ring of ring deliveries and
// the token table a source of that prefetch has, so that a test can fill the
// ring and still claim slots.
func newRingSource(tb testing.TB, ring int) (*source, *testkit.Clock) {
	tb.Helper()
	src, clk := newClockedSource(tb, ring+tokenHeadroom)
	push, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: ring})
	if err != nil {
		tb.Fatalf("NewPushAdapter: %v", err)
	}
	src.push = push
	src.arrivals = arrivals{window: 2 * ring}
	tb.Cleanup(func() { _ = push.Close() })
	return src, clk
}

// scriptedEnvelope is the envelope shim a backend builds, for a scripted
// source that has to tell an envelope it cannot decode.
func scriptedEnvelope(tb testing.TB) *driverkit.Envelope {
	tb.Helper()
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		tb.Fatalf("NewEnvelope: %v", err)
	}
	return env
}

// fetchScripted takes one delivery from a scripted source, or fails.
func fetchScripted(tb testing.TB, src *source) mq.DriverDelivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, dst); n != 1 {
		tb.Fatalf("Fetch returned %d deliveries (%v)", n, err)
	}
	return dst[0]
}

// callbackReturns hands m to the consume callback and reports whether the
// callback returned. One that is still blocked after two seconds is released
// by closing the ring, so that it does not outlive the test.
func callbackReturns(tb testing.TB, src *source, m js.Msg) bool {
	tb.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		src.onMessage(m)
	}()
	select {
	case <-done:
		return true
	case <-time.After(2 * time.Second):
		_ = src.push.Close()
		<-done
		return false
	}
}

// eventually polls cond until it holds, or fails after five seconds.
func eventually(tb testing.TB, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("%s did not happen within five seconds", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// recordMsg is a scriptedMsg that is one delivery of one record, for a test
// that hands messages to the consume callback itself.
type recordMsg struct {
	scriptedMsg
	seq, delivered uint64
	hdr            natsgo.Header
	body           []byte
}

func (m *recordMsg) Metadata() (*js.MsgMetadata, error) {
	return &js.MsgMetadata{
		Sequence:     js.SequencePair{Stream: m.seq, Consumer: m.seq},
		NumDelivered: m.delivered,
		Stream:       "scripted",
	}, nil
}

func (m *recordMsg) Data() []byte           { return m.body }
func (m *recordMsg) Headers() natsgo.Header { return m.hdr }

var _ js.Msg = (*recordMsg)(nil)

// undecodableMsg is the first delivery of record seq, marked as an envelope
// and carrying a body that is not one.
func undecodableMsg(seq uint64) *recordMsg {
	return &recordMsg{
		seq: seq, delivered: 1,
		hdr:  natsgo.Header{HeaderEnvelope: []string{EnvelopeMarkerValue}},
		body: []byte("not an envelope"),
	}
}
