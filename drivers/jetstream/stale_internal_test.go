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

// FuzzNATSStaleAck is the op-script fuzz of this driver's one safety-critical
// piece of bookkeeping.
//
// The hazard it exists for is specific to this broker. JetStream acknowledges by
// reply subject, and the server's AckExplicit path deletes its pending entry for
// the message's STREAM SEQUENCE without looking at which delivery the reply came
// from, so the reply subject of an expired delivery still works — acking it
// acknowledges the record a later delivery now owns. Nothing in the client or
// the server reports the loss, so the guard has to be the driver's, and the only
// thing that can hold a guard like this over every interleaving is a fuzz target
// with a model beside it.
//
// The model is two maps: which token currently owns each record, and which
// records have been settled. The invariant is one sentence — a settlement that
// SUCCEEDS must name the token that currently owns its record — and every
// failure mode the hazard has is a violation of it: a superseded token that
// settles, a settled record that settles twice, a slot reused under a live
// token.
//
// It runs against the real [source] bookkeeping and the real [settler], with a
// scripted message in place of the wire: what is being checked is which
// settlements reach the wire at all.
func FuzzNATSStaleAck(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0})                            // deliver, ack
	f.Add([]byte{0, 0, 0, 0, 1, 0})                      // deliver, redeliver, ack the first
	f.Add([]byte{0, 0, 0, 0, 1, 1})                      // deliver, redeliver, ack the second
	f.Add([]byte{0, 0, 0, 0, 1, 0, 1, 1})                // both, in order
	f.Add([]byte{0, 0, 1, 0, 0, 0, 1, 0})                // settle, redeliver, settle again
	f.Add([]byte{0, 0, 2, 0})                            // deliver, extend
	f.Add([]byte{0, 0, 0, 0, 2, 0})                      // deliver, redeliver, extend the first
	f.Add([]byte{0, 0, 3, 0})                            // deliver, nack
	f.Add([]byte{0, 0, 0, 1, 0, 2, 1, 0, 3, 1, 2, 2})    // a mixed script over two records
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4}) // fill the table, then sweep
	f.Fuzz(func(t *testing.T, script []byte) {
		const slots = 4
		src := newScriptedSource(t, slots)
		set := src.set

		// The model. live[seq] is the token that currently owns the record;
		// settled[seq] records a terminal settlement.
		live := map[uint64]uint64{}
		settled := map[uint64]bool{}
		// history is every token ever handed out, with the record it named,
		// so that the script can reach a superseded one.
		type issued struct {
			token uint64
			seq   uint64
		}
		var history []issued

		ctx := context.Background()
		for i := 0; i+1 < len(script); i += 2 {
			op, arg := script[i]%5, script[i+1]
			switch op {
			case 0: // deliver or redeliver a record
				seq := uint64(arg%3) + 1
				h := scriptedHeld(src, &scriptedMsg{}, seq, 1)
				token, err := src.claim(h)
				if err != nil {
					// The table is full, which is the only refusal claim
					// has. Nothing changes in the model.
					continue
				}
				live[seq] = token
				settled[seq] = false
				history = append(history, issued{token: token, seq: seq})
			case 1, 3: // settle a delivery, positively or negatively
				if len(history) == 0 {
					continue
				}
				h := history[int(arg)%len(history)]
				var err error
				if op == 1 {
					err = set.Ack(ctx, &mq.DriverDelivery{Token: h.token})
				} else {
					err = set.Nack(ctx, &mq.DriverDelivery{Token: h.token}, mq.NackOptions{})
				}
				if err == nil {
					// THE INVARIANT. A settlement that succeeded reached
					// the wire, and on this broker the wire takes it: it
					// had better have named the delivery that owns the
					// record.
					if cur, ok := live[h.seq]; !ok || cur != h.token {
						t.Fatalf("a settlement of token %d succeeded for record %d, whose current token is %d (present %v): on JetStream that acknowledges the record another delivery owns ",
							h.token, h.seq, cur, ok)
					}
					if settled[h.seq] {
						t.Fatalf("record %d was settled twice", h.seq)
					}
					settled[h.seq] = true
					delete(live, h.seq)
					continue
				}
				// A refusal is one of the answers and never a bare
				// failure: a caller cannot act on "something went wrong".
				if !isSettlementAnswer(err) {
					t.Fatalf("a refused settlement of token %d reported %v (code %v), which is not one of the answers",
						h.token, err, mq.CodeOf(err))
				}
			case 2: // extend a lease
				if len(history) == 0 {
					continue
				}
				h := history[int(arg)%len(history)]
				err := set.Extend(ctx, &mq.DriverDelivery{Token: h.token}, time.Second)
				if err == nil {
					if cur, ok := live[h.seq]; !ok || cur != h.token {
						t.Fatalf("an extension of token %d succeeded for record %d, whose current token is %d (present %v): the handler would keep working on a delivery someone else owns",
							h.token, h.seq, cur, ok)
					}
					continue
				}
				if !isSettlementAnswer(err) {
					t.Fatalf("a refused extension reported %v (code %v)", err, mq.CodeOf(err))
				}
			case 4: // close: every outstanding delivery is naked
				src.nakOutstanding()
				for seq, token := range live {
					if err := set.Ack(ctx, &mq.DriverDelivery{Token: token}); err == nil {
						t.Fatalf("token %d for record %d settled after the source swept it", token, seq)
					}
				}
				live = map[uint64]uint64{}
			}
		}
		// Whatever the script did, the sweep is safe to run and leaves
		// nothing settleable behind.
		src.nakOutstanding()
		for _, h := range history {
			if err := set.Ack(ctx, &mq.DriverDelivery{Token: h.token}); err == nil {
				t.Fatalf("token %d settled after the final sweep", h.token)
			}
		}
	})
}

// TestStaleAckSeedsAreAScript is the seed corpus as a test, so that `make
// fuzz-seeds` really exercises the invariant under -race rather than only
// compiling it.
func TestStaleAckSeedsAreAScript(t *testing.T) {
	src := newScriptedSource(t, 2)
	h1 := scriptedHeld(src, &scriptedMsg{}, 7, 1)
	first, err := src.claim(h1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	h2 := scriptedHeld(src, &scriptedMsg{}, 7, 2)
	second, err := src.claim(h2)
	if err != nil {
		t.Fatalf("claim of the redelivery: %v", err)
	}
	if first == second {
		t.Fatal("a redelivery took the same token as the delivery it replaced")
	}
	ctx := context.Background()
	if err := src.set.Ack(ctx, &mq.DriverDelivery{Token: first}); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acking the superseded delivery reported %v, want ErrLeaseLost", err)
	}
	if err := src.set.Ack(ctx, &mq.DriverDelivery{Token: second}); err != nil {
		t.Fatalf("acking the current delivery: %v", err)
	}
	if got := h2.msg.(*scriptedMsg).acks; got != 1 {
		t.Errorf("the current delivery was acked %d times, want 1", got)
	}
	if got := h1.msg.(*scriptedMsg).acks; got != 0 {
		t.Errorf("the superseded delivery was acked %d times, want 0: the ack reached the wire", got)
	}
	if got := src.b.StaleSettlements(); got != 1 {
		t.Errorf("StaleSettlements = %d, want 1", got)
	}
}

// isSettlementAnswer reports whether err is one of the answers the specification gives a
// settlement that did not happen.
func isSettlementAnswer(err error) bool {
	switch mq.CodeOf(err) {
	case mq.CodeLeaseLost, mq.CodeAlreadySettled, mq.CodeSettlementInProgress,
		mq.CodeSettlementUnknown, mq.CodeResourceExhausted, mq.CodeUnsupported,
		mq.CodeInvalidArgument, mq.CodeClosed:
		return true
	default:
		return false
	}
}

// newScriptedSource builds a source with the real token table, the real
// stream-sequence index and the real settler, and no connection at all.
//
// The bookkeeping is what is under test, so the wire is replaced and nothing
// else is: a fuzz target that reached a server would run a hundred inputs a
// second instead of a million, and the interleavings that matter are the
// bookkeeping's.
func newScriptedSource(tb testing.TB, slots int) *source {
	tb.Helper()
	table, err := driverkit.NewTokenTable(slots)
	if err != nil {
		tb.Fatalf("NewTokenTable: %v", err)
	}
	push, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: slots})
	if err != nil {
		tb.Fatalf("NewPushAdapter: %v", err)
	}
	// The clock stands still unless a test moves it, so no lease in a script
	// runs out however long the script takes.
	// The connection is never dialled: its statistics are all zero, which is
	// a connection that has not reconnected.
	b := &Backend{cfg: Config{}.resolve(), errs: newErrorMapper(), clock: testkit.NewClock(time.Time{}), nc: &natsgo.Conn{}}
	// The health reporter is the real one: TestDoubleNakIsNotABackgroundFailure
	// asserts on what a background failure does to it, and a nil reporter
	// would make that assertion a panic rather than a check.
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Ready()
	src := &source{
		b:            b,
		push:         push,
		table:        table,
		topic:        "scripted",
		subscription: "workers",
		ackWait:      time.Second,
		arrivals:     arrivals{prefetch: slots},
		seqs:         map[uint64]uint64{},
	}
	src.closing, src.stop = context.WithCancel(context.Background())
	src.set = &settler{src: src}
	return src
}

// scriptedHeld is a held for record seq whose lease starts now on the source's
// clock, as [source.delivery]'s does.
func scriptedHeld(src *source, msg js.Msg, seq, delivered uint64) *held {
	h := &held{msg: msg, seq: seq, delivered: delivered, lease: src.leaseOf(delivered)}
	h.expires.Store(src.b.now().Add(h.lease).UnixNano())
	return h
}

// newClockedSource is newScriptedSource with the backend's clock in the test's
// hands.
func newClockedSource(tb testing.TB, slots int) (*source, *testkit.Clock) {
	tb.Helper()
	src := newScriptedSource(tb, slots)
	clk := testkit.NewClock(time.Time{})
	src.b.clock = clk
	return src, clk
}

// deliverScripted hands msg out as the first delivery of record seq, through
// the source's own delivery path, which is where its lease starts.
func deliverScripted(tb testing.TB, src *source, msg js.Msg, seq uint64) mq.DriverDelivery {
	tb.Helper()
	return deliverAttempt(tb, src, msg, seq, 1)
}

// deliverAttempt is deliverScripted for the delivered-th delivery of the
// record.
func deliverAttempt(tb testing.TB, src *source, msg js.Msg, seq, delivered uint64) mq.DriverDelivery {
	tb.Helper()
	meta := &js.MsgMetadata{Sequence: js.SequencePair{Stream: seq, Consumer: seq}, NumDelivered: delivered, Stream: "scripted"}
	d, err := src.delivery(msg, meta, &mq.Message{}, src.b.now())
	if err != nil {
		tb.Fatalf("delivering record %d (delivery %d): %v", seq, delivered, err)
	}
	return d
}

// TestSettlementPastTheLeaseIsRefused holds the lease half of the stale-ack
// guard, with the clock in the test's hands.
//
// A redelivery the server handed to ANOTHER source never reaches this
// source's index, so the token is still live when the late settlement comes.
// Once the delivery's AckWait has run out the server may have given the record
// to someone else, and an ack, nak or term written now would settle their
// delivery. So nothing is written, and the delivery stays refused. Halfway
// through the lease the same settlement is written: the guard is not a
// refusal of everything.
func TestSettlementPastTheLeaseIsRefused(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		settle func(*settler, *mq.DriverDelivery) error
	}{
		{"ack", func(s *settler, d *mq.DriverDelivery) error { return s.Ack(ctx, d) }},
		{"requeue", func(s *settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionRequeue})
		}},
		{"drop", func(s *settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDrop})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, clk := newClockedSource(t, 4)
			inside := &scriptedMsg{}
			d := deliverScripted(t, src, inside, 1)
			clk.Advance(src.ackWait / 2)
			if err := tc.settle(src.set, &d); err != nil {
				t.Fatalf("a settlement halfway through the lease reported %v (code %v)", err, mq.CodeOf(err))
			}

			late := &scriptedMsg{}
			d = deliverScripted(t, src, late, 2)
			clk.Advance(src.ackWait)
			err := tc.settle(src.set, &d)
			if !errors.Is(err, mq.ErrLeaseLost) {
				t.Fatalf("a settlement after the lease ran out reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
			}
			if wrote := late.acks + late.naks + late.terms; wrote != 0 {
				t.Fatalf("a settlement after the lease ran out reached the wire %d times: the server may have handed the record to another source, and it settles that delivery", wrote)
			}
			if got := src.b.StaleSettlements(); got != 1 {
				t.Errorf("StaleSettlements = %d, want 1", got)
			}
			if err := src.set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
				t.Errorf("a second settlement of the refused delivery reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
			}
			if late.acks != 0 {
				t.Error("the second settlement of the refused delivery reached the wire")
			}
		})
	}
}

// TestCloseDoesNotNakADeliveryWhoseLeaseRanOut holds Close's sweep to the
// lease guard every settlement keeps.
//
// Close naks what the caller was handed and never settled, so that a restart
// resumes at once. A delivery whose lease has reached the guard may already
// belong to another consumer of the durable, and a nak on its reply subject
// makes the record deliverable again under that consumer's handler. The sweep
// used to write it all the same. It is left to come back when the server's
// own lease ends, as a refused settlement is, and one still inside its lease
// is naked as before.
func TestCloseDoesNotNakADeliveryWhoseLeaseRanOut(t *testing.T) {
	src, clk := newClockedSource(t, 4)
	late := &scriptedMsg{}
	deliverScripted(t, src, late, 1)
	clk.Advance(src.ackWait - leaseGuard(src.ackWait)/2)
	inside := &scriptedMsg{}
	deliverScripted(t, src, inside, 2)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if late.naks != 0 {
		t.Error("Close naked a delivery whose lease had reached the guard: the server may have handed the record to another consumer, and the nak redelivers it under that consumer's handler")
	}
	if inside.naks != 1 {
		t.Errorf("Close naked a delivery inside its lease %d times, want once", inside.naks)
	}
	if got := src.b.StaleSettlements(); got != 1 {
		t.Errorf("StaleSettlements = %d, want 1 for the nak that was not written", got)
	}
}

// TestCloseDoesNotNakAMessageThatWaitedOutItsLeaseInTheClient holds the
// consume callback to the same guard while the source closes.
//
// Close wakes a callback blocked on the ring and drains what the client
// library still holds through the callback, and both nak what they are
// handed. A message that waited out its lease behind a long block used to be
// naked as well, on a record the server may already have handed to another
// consumer. It is dropped instead, as the callback drops one whose lease ran
// out before Close, and a message inside its lease is still naked.
func TestCloseDoesNotNakAMessageThatWaitedOutItsLeaseInTheClient(t *testing.T) {
	t.Run("blocked on the ring or drained", func(t *testing.T) {
		src, clk := newRingSource(t, 1)
		inRing := &recordMsg{seq: 1, delivered: 1}
		src.onMessage(inRing)
		blockedOn := &recordMsg{seq: 2, delivered: 1}
		blocked := make(chan struct{})
		go func() {
			defer close(blocked)
			src.onMessage(blockedOn)
		}()
		eventually(t, "the callback blocks on the full ring", func() bool { return src.push.Blocked() == 1 })
		// A missed heartbeat, whose re-pull lets the library queue the next
		// message behind the block.
		src.b.consumeErrHandler(src)(nil, js.ErrNoHeartbeat)
		clk.Advance(src.ackWait)
		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		<-blocked
		drained := &recordMsg{seq: 3, delivered: 1}
		src.onMessage(drained)
		for _, m := range []*recordMsg{inRing, blockedOn, drained} {
			if m.naks != 0 {
				t.Errorf("record %d, whose lease ran out before the source closed, was naked %d times", m.seq, m.naks)
			}
		}
		clk.Advance(idleGap)
		fresh := &recordMsg{seq: 4, delivered: 1}
		src.onMessage(fresh)
		if fresh.naks != 1 {
			t.Errorf("a message handed to the closing source inside its lease was naked %d times, want once", fresh.naks)
		}
	})
	t.Run("waiting for a token slot", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src, clk := newClockedSource(t, 1)
			deliverScripted(t, src, &scriptedMsg{}, 1)
			waiting := &recordMsg{seq: 2, delivered: 1}
			done := make(chan struct{})
			go func() {
				defer close(done)
				src.onMessage(waiting)
			}()
			synctest.Wait()
			// The driver's clock moves and the bubble's does not, so the
			// wait is still parked when Close begins.
			clk.Advance(src.ackWait)
			src.stop()
			<-done
			if waiting.naks != 0 {
				t.Errorf("a message whose lease ran out while it waited for a slot was naked %d times when the source closed", waiting.naks)
			}
		})
	})
}

// TestAnUndecodableMessageThatWaitedOutItsLeaseIsNotNaked holds the
// undecodable path to the guard every other write keeps.
//
// With no dead-letter subject, an undecodable message is naked with a delay so
// that it comes back once a lease rather than at once. One that waited out its
// lease in the client library may already belong to another consumer of the
// durable, and the nak used to be written all the same, which redelivers the
// record under that consumer's handler. It is left to come back when the
// server's lease ends, and one inside its lease is still naked.
func TestAnUndecodableMessageThatWaitedOutItsLeaseIsNotNaked(t *testing.T) {
	src, clk := newRingSource(t, 2)
	src.meta = metaAdaptive
	src.b.env = scriptedEnvelope(t)
	src.onMessage(&recordMsg{seq: 1, delivered: 1})
	src.onMessage(&recordMsg{seq: 2, delivered: 1})
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		src.onMessage(&recordMsg{seq: 3, delivered: 1})
	}()
	eventually(t, "the callback blocks on the full ring", func() bool { return src.push.Blocked() == 1 })
	clk.Advance(src.ackWait)
	// Off the ring as they are: a Fetch would drop them, and then wait for
	// the record that was blocked, whose lease has run out as well.
	if n, err := src.push.Fetch(context.Background(), make([]mq.DriverDelivery, 2)); n != 2 {
		t.Fatalf("the ring gave up %d deliveries (%v), want 2", n, err)
	}
	<-blocked

	late := undecodableMsg(4)
	src.onMessage(late)
	if late.naks != 0 {
		t.Errorf("an undecodable message that waited out its lease behind the block was naked %d times", late.naks)
	}
	if got := src.b.StaleSettlements(); got != 1 {
		t.Errorf("StaleSettlements = %d, want 1 for the nak that was not written", got)
	}
	clk.Advance(idleGap)
	inside := undecodableMsg(5)
	src.onMessage(inside)
	if inside.naks != 1 {
		t.Errorf("an undecodable message inside its lease was naked %d times, want once", inside.naks)
	}
}

// TestExtendMovesTheLease holds Extend against the lease guard.
//
// `+WPI` restarts the server's AckWait, so the local lease moves with it, from
// the moment before the `+WPI` was written: a settlement after the original
// lease has run out, inside the extended one, is written. An Extend in the
// guard but before the lease ends is still written, because the core's lease
// scheduler asks one lead before the deadline and a lead shorter than the
// guard must not lose the lease. An Extend once the lease has ended writes
// nothing: the `+WPI` would restart the timer of whichever delivery owns the
// record now, and moving this one's lease with it would let its late
// settlement through.
func TestExtendMovesTheLease(t *testing.T) {
	ctx := context.Background()
	src, clk := newClockedSource(t, 4)
	extended := &scriptedMsg{}
	d := deliverScripted(t, src, extended, 1)
	clk.Advance(src.ackWait / 2)
	if err := src.set.Extend(ctx, &d, src.ackWait); err != nil {
		t.Fatalf("Extend halfway through the lease: %v", err)
	}
	clk.Advance(src.ackWait * 3 / 4)
	if err := src.set.Ack(ctx, &d); err != nil {
		t.Fatalf("an ack past the original lease but inside the extended one reported %v (code %v)", err, mq.CodeOf(err))
	}
	if extended.acks != 1 || extended.progress != 1 {
		t.Errorf("the extended delivery wrote %d acks and %d +WPI, want one of each", extended.acks, extended.progress)
	}

	lastMoment := &scriptedMsg{}
	d = deliverScripted(t, src, lastMoment, 2)
	clk.Advance(src.ackWait - time.Millisecond)
	if err := src.set.Extend(ctx, &d, src.ackWait); err != nil {
		t.Fatalf("Extend a millisecond before the lease ends reported %v (code %v): a lease scheduler whose lead is shorter than the guard would lose every lease", err, mq.CodeOf(err))
	}
	if err := src.set.Ack(ctx, &d); err != nil {
		t.Fatalf("an ack right after a last-moment Extend reported %v (code %v)", err, mq.CodeOf(err))
	}

	expired := &scriptedMsg{}
	d = deliverScripted(t, src, expired, 3)
	clk.Advance(src.ackWait)
	if err := src.set.Extend(ctx, &d, src.ackWait); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend after the lease ran out reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
	if expired.progress != 0 {
		t.Fatal("an Extend after the lease ran out wrote a +WPI, which restarts the timer of whichever delivery owns the record now")
	}
	if err := src.set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("an ack after a refused Extend reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
}

// TestDeadLetterThatOutlivesTheLeaseDoesNotTerminate holds the lease across the
// dead-letter route, the one settlement with a round trip before its write.
//
// The copy is published first and the original terminated only once its
// PubAck is in hand, so a publication slower than what was left of the lease
// used to write its `+TERM` after the server could have handed the record to
// another consumer, and terminate that consumer's delivery. The Term is refused
// instead: the copy stays and the original comes back, a duplicate rather
// than another consumer's message terminated under it.
func TestDeadLetterThatOutlivesTheLeaseDoesNotTerminate(t *testing.T) {
	ctx := context.Background()
	src, clk := newClockedSource(t, 4)
	src.b.facts.deadLetter = "scripted.dlq"
	slow := &slowPublisher{clk: clk, by: src.ackWait}
	src.b.js = slow
	msg := &scriptedMsg{}
	d := deliverScripted(t, src, msg, 1)
	err := src.set.Nack(ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a dead-letter route whose publication outlived the lease reported %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
	if slow.published != 1 {
		t.Fatalf("the dead-letter copy was published %d times, want once", slow.published)
	}
	if msg.terms != 0 {
		t.Fatal("the original was terminated after its lease ran out: the server may have handed the record to another consumer, and the Term lands on that delivery")
	}
	if got := src.b.DeadLettered(); got != 0 {
		t.Errorf("DeadLettered = %d for a route that terminated nothing, want 0", got)
	}
}

// slowPublisher is a [github.com/nats-io/nats.go/jetstream.JetStream] whose
// PublishMsg takes by on the test's clock and succeeds. Nothing else of the
// interface is called.
type slowPublisher struct {
	js.JetStream
	clk       *testkit.Clock
	by        time.Duration
	published int
}

func (p *slowPublisher) PublishMsg(context.Context, *natsgo.Msg, ...js.PublishOpt) (*js.PubAck, error) {
	p.clk.Advance(p.by)
	p.published++
	return &js.PubAck{Stream: "scripted", Sequence: 1}, nil
}

// scriptedMsg is a [github.com/nats-io/nats.go/jetstream.Msg] that records the
// settlement verbs instead of sending them.
//
// It is the whole of the wire in FuzzNATSStaleAck: what the target checks is
// which settlements reach a message at all, so a message that counts them is
// exactly the observation needed.
type scriptedMsg struct {
	acks, naks, terms, progress int
}

func (m *scriptedMsg) Metadata() (*js.MsgMetadata, error) {
	return &js.MsgMetadata{Sequence: js.SequencePair{Stream: 1, Consumer: 1}, NumDelivered: 1}, nil
}

func (m *scriptedMsg) Data() []byte           { return nil }
func (m *scriptedMsg) Headers() natsgo.Header { return nil }
func (m *scriptedMsg) Subject() string        { return "scripted" }
func (m *scriptedMsg) Reply() string          { return "$JS.ACK.scripted" }

func (m *scriptedMsg) Ack() error                       { m.acks++; return nil }
func (m *scriptedMsg) DoubleAck(context.Context) error  { m.acks++; return nil }
func (m *scriptedMsg) Nak() error                       { m.naks++; return nil }
func (m *scriptedMsg) NakWithDelay(time.Duration) error { m.naks++; return nil }
func (m *scriptedMsg) InProgress() error                { m.progress++; return nil }
func (m *scriptedMsg) Term() error                      { m.terms++; return nil }
func (m *scriptedMsg) TermWithReason(string) error      { m.terms++; return nil }

// compile-time proof that the scripted message is the client library's own
// interface, so the settler under test is the real one.
var _ js.Msg = (*scriptedMsg)(nil)

// TestDoubleNakIsNotABackgroundFailure holds the one rule a closing source needs
// of the error mapping.
//
// The consume callback and the closing sweep can both reach one message: the
// callback pushes it into the ring, the sweep naks it, the push then fails
// because the ring is closed and the callback naks it too. The client library
// answers the second nak with ErrMsgAlreadyAckd, which is not a failure —
// somebody already settled the delivery, which is what the nak wanted — and
// reporting it would make a Close degrade the client's health and count a
// consume error.
func TestDoubleNakIsNotABackgroundFailure(t *testing.T) {
	src := newScriptedSource(t, 2)
	// A message that answers every settlement the way a message somebody has
	// already settled does.
	settled := &settledMsg{}
	src.nak(settled)
	if got := src.b.ConsumeErrors(); got != 0 {
		t.Errorf("a nak of an already-settled delivery reported %d consume errors, want 0", got)
	}
	if got := src.b.DegradedTransitions(); got != 0 {
		t.Errorf("a nak of an already-settled delivery degraded the client %d times, want 0", got)
	}
	// A nak that fails for a reason that is NOT "already settled" still
	// reaches health: a check that swallowed everything would hide a broken
	// link.
	src.nak(&brokenMsg{})
	if got := src.b.ConsumeErrors(); got != 1 {
		t.Errorf("a nak that failed for another reason reported %d consume errors, want 1", got)
	}
}

// settledMsg answers every settlement with the client library's
// "already acknowledged".
type settledMsg struct{ scriptedMsg }

func (m *settledMsg) Nak() error { return js.ErrMsgAlreadyAckd }

// brokenMsg answers with a failure that is nobody's normal close path.
type brokenMsg struct{ scriptedMsg }

func (m *brokenMsg) Nak() error { return natsgo.ErrMaxPayload }

var (
	_ js.Msg = (*settledMsg)(nil)
	_ js.Msg = (*brokenMsg)(nil)
)

// TestAmbiguousAckFreesItsSlot holds that an ack whose outcome is unknown
// does not keep its token slot.
//
// The core records the ambiguous answer as final and never settles the
// delivery again, and when the server did take the ack nothing redelivers
// the record to free the slot through the index. A table that kept such
// slots filled after a handful of lost replies, and then every new delivery
// was naked back as table-full.
func TestAmbiguousAckFreesItsSlot(t *testing.T) {
	const slots = 2
	src := newScriptedSource(t, slots)
	src.b.cfg.DoubleAck = true
	ctx := context.Background()
	for seq := uint64(1); seq <= 2*slots; seq++ {
		token, err := src.claim(scriptedHeld(src, &timeoutMsg{}, seq, 1))
		if err != nil {
			t.Fatalf("claim of record %d after %d ambiguous acks: %v", seq, seq-1, err)
		}
		err = src.set.Ack(ctx, &mq.DriverDelivery{Token: token})
		if err == nil || !unknownOutcome(err) {
			t.Fatalf("an ack that timed out reported %v (code %v), want an ambiguous outcome", err, mq.CodeOf(err))
		}
		if err := src.set.Ack(ctx, &mq.DriverDelivery{Token: token}); err == nil {
			t.Fatalf("token %d settled again after its ambiguous ack", token)
		}
	}
}

// timeoutMsg answers a double ack the way a request whose reply never came
// before the deadline does: the ack was written, and the server may have it.
type timeoutMsg struct{ scriptedMsg }

func (m *timeoutMsg) DoubleAck(context.Context) error { return context.DeadlineExceeded }

var _ js.Msg = (*timeoutMsg)(nil)
