package streams_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
)

// apartDLQ is a dead-letter stream on another hash slot than the topic's, so
// the dead-letter move is the two-command plan rather than the script.
const apartDLQ = "dead.letters"

// apartRig is a rig whose dead-letter stream is on another slot.
func apartRig(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.DeadLetterStream = apartDLQ })
	if streams.SlotSafe(r.keys.Stream(0), apartDLQ) {
		t.Fatal("the dead-letter stream shares the topic's slot, so this case would test the script")
	}
	return r
}

// TestACrossSlotDeadLetterWhoseXAddFailsAcknowledgesNothing holds the order
// of the two-command move: the acknowledgement is sent only once the copy is
// in the dead-letter stream, or a failed XADD beside a successful
// acknowledgement destroys the message.
func TestACrossSlotDeadLetterWhoseXAddFailsAcknowledgesNothing(t *testing.T) {
	r := apartRig(t)
	r.publish(t, "payload")
	ds := r.read(t)
	r.srv.Fail("XADD", errors.New("injected"))
	err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now())
	if err == nil {
		t.Fatal("a dead-letter move whose XADD failed reported success")
	}
	if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 1 {
		t.Fatalf("the source holds %d pending entries after a failed move, want the 1 it had", n)
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now()); err != nil {
		t.Fatalf("a retry of the move: %v", err)
	}
	if n := len(r.srv.Entries(apartDLQ)); n != 1 {
		t.Fatalf("the dead-letter stream holds %d entries after the retry, want 1", n)
	}
}

// TestACrossSlotDeadLetterWhoseAckFailsAfterTheCopyIsUnknown holds the move
// half done: the copy is in the dead-letter stream and the acknowledgement
// that ends the move failed, so the entry is still pending. Whatever the
// failure was, a dial the server refused, a reply that never came or a
// refusal it answered, a retried Reject would put a second copy in the
// dead-letter stream, so the outcome is unknown.
//
// The acknowledgement's error used to come back as it came. drivers/redis
// reads a refusal the server answered as definite, and a dial that connected
// nowhere too once it decides that itself, so the Reject read as one that had
// not happened and the delivery stayed settleable.
func TestACrossSlotDeadLetterWhoseAckFailsAfterTheCopyIsUnknown(t *testing.T) {
	for name, cause := range map[string]error{
		"a refused dial":   &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)},
		"a lost reply":     &net.OpError{Op: "read", Net: "tcp", Err: io.EOF},
		"a server refusal": errors.New("BUSY Redis is busy running a script"),
		"an ended context": context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			r := apartRig(t)
			r.publish(t, "payload")
			ds := r.read(t)
			r.srv.Fail("EVALSHA", cause)
			err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now())
			if n := len(r.srv.Entries(apartDLQ)); n != 1 {
				t.Fatalf("setup: the dead-letter stream holds %d entries, want the copy", n)
			}
			if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 1 {
				t.Fatalf("setup: the source holds %d pending entries, want the 1 the failed acknowledgement left", n)
			}
			if !mq.IsAmbiguous(err) || !errors.Is(err, mq.ErrSettlementUnknown) {
				t.Fatalf("Nack = %v (ambiguous %v): the copy is in the dead-letter stream, so a retried Reject "+
					"writes a second one, and the outcome is unknown", err, mq.IsAmbiguous(err))
			}
			if !errors.Is(err, cause) {
				t.Fatalf("Nack = %v, which no longer carries the native failure %v", err, cause)
			}
			// However a driver maps it, also as a request that never left.
			m := &driverkit.ErrorMapper{Driver: "redis"}
			for _, sent := range []bool{false, true} {
				if mapped := m.MapNet(mq.OpNack, "t", "g", err, sent); !mq.IsAmbiguous(mapped) {
					t.Fatalf("MapNet(sent=%v) = %v, which reads as definite", sent, mapped)
				}
			}
		})
	}
}

// TestACrossSlotDeadLetterRefusedByTheGuardTakesItsCopyBack holds the other
// half: when the entry turns out to be another consumer's, that consumer
// settles it, so the copy this move added is removed rather than left as a
// duplicate.
func TestACrossSlotDeadLetterRefusedByTheGuardTakesItsCopyBack(t *testing.T) {
	r := apartRig(t)
	id := r.publish(t, "payload")
	ds := r.read(t)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now())
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("dead-lettering a delivery another consumer holds returned %v, want ErrLeaseLost", err)
	}
	if n := len(r.srv.Entries(apartDLQ)); n != 0 {
		t.Fatalf("the dead-letter stream holds %d entries for a move the guard refused, want 0", n)
	}
}

// TestStopReleasesAShardWhoseHeldEntryIsGone holds that the Stop strategy's
// hold ends with the entry however it ends: an entry deleted from the stream
// is never redelivered, so a hold that waited for its settlement would stop
// the shard for ever.
func TestStopReleasesAShardWhoseHeldEntryIsGone(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	ds := r.read(t)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if _, err := r.srv.Do(r.ctx, streams.XDel(r.keys.Stream(0), []streams.ID{ds[0].Key.ID})); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
	r.srv.Advance(testLease + time.Second)
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d after the held entry was deleted, want 0", n)
	}
}

// TestStopReleasesAShardWhoseHeldEntryWasTaken is the same for an entry
// another consumer took: the extension finds it lost, and the hold goes with
// it.
func TestStopReleasesAShardWhoseHeldEntryWasTaken(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	id := r.publish(t, "first")
	ds := r.read(t)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	r.srv.Advance(testLease + time.Second)
	got, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the sweep redelivered %d entries, want the held one", len(got))
	}
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	if errs := r.cons.Extend(r.ctx, r.srv, got); !errors.Is(errs[0], mq.ErrLeaseLost) {
		t.Fatalf("Extend of a taken entry returned %v, want ErrLeaseLost", errs[0])
	}
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d after the held entry was taken, want 0", n)
	}
}
