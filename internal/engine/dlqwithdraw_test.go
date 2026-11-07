package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// moveRig is an engine whose explicit dead-letter moves (Consumer.DeadLetter,
// the memory driver's Reject) are refused after they have begun. The
// dead-letter topic holds 4 bytes, so the 4-byte stored record fits and the
// 8-byte copy a move publishes never does, and the copy parks in the clock
// read of the dead-letter topic's dedup check (clockTrap) until the test has
// withdrawn the lease underneath it.
type moveRig struct {
	fake *clock.Fake
	trap *clockTrap
	e    *Engine[int64]
	dlq  *Topic[int64]
	dsub *Subscription[int64]
	top  *Topic[int64]
	sub  *Subscription[int64]
}

// moveRigConfig is the source topic of a moveRig: its partitions, its attempt
// limit (dead-lettering to the rig's dead-letter topic), whether its
// subscription is sticky and whether it retains its records for a Seek. Its
// leases run for one second.
type moveRigConfig struct {
	partitions int
	attempts   uint32
	sticky     bool
	retain     bool
}

func newMoveRig(t *testing.T, mc moveRigConfig) *moveRig {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	r := &moveRig{fake: fake, trap: newClockTrap(fake, "(*Topic[", ").publish")}
	var err error
	r.e, err = New(Config[int64]{
		Clock: r.trap,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: mc.partitions, CapacityMessages: 8, AckDeadline: time.Second, Overflow: OverflowReject,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.e.Close() })
	if r.dlq, err = r.e.CreateTopic("dlq", TopologyConfig{Partitions: 1, DedupWindow: time.Hour, CapacityBytes: 4}); err != nil {
		t.Fatal(err)
	}
	if r.dsub, err = r.dlq.Subscribe("d", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	cfg := TopologyConfig{MaxAttempts: mc.attempts, DeadLetterTopic: "dlq"}
	if mc.retain {
		cfg.Kind, cfg.Retention = TopologyStream, Retention{MaxMessages: 8}
	}
	if r.top, err = r.e.CreateTopic("t", cfg); err != nil {
		t.Fatal(err)
	}
	if r.sub, err = r.top.Subscribe("s", SubscriptionConfig{Sticky: mc.sticky}); err != nil {
		t.Fatal(err)
	}
	return r
}

// attach adds a consumer to the rig's subscription.
func (r *moveRig) attach(t *testing.T) *Consumer[int64] {
	t.Helper()
	c, err := r.sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// publish puts one 4-byte record on each of the first n partitions.
func (r *moveRig) publish(t *testing.T, n int) {
	t.Helper()
	for part := range n {
		if err := r.top.Publish(context.Background(), Publish[int64]{Payload: int64(part + 1), Size: 4, Partition: part}); err != nil {
			t.Fatal(err)
		}
	}
}

// move starts the explicit dead-letter move of cl through c, waits until its
// copy is parked, runs during and lets the copy go on to be refused. It
// returns what DeadLetter returned.
func (r *moveRig) move(c *Consumer[int64], cl Claim[int64], during func()) error {
	r.trap.arm()
	errc := make(chan error, 1)
	go func() {
		errc <- c.DeadLetter(context.Background(), cl.Env, cl.Gen, r.dlq,
			Publish[int64]{Payload: cl.Payload, Size: 8, Partition: -1, DedupKey: []byte("k")})
	}()
	select {
	case <-r.trap.parked:
	case err := <-errc:
		// Every move reads the clock before its copy is placed; one that
		// stops doing so reports here instead of blocking the test.
		return errors.New("the move returned before its copy read the clock: " + fmt.Sprint(err))
	}
	during()
	r.trap.let()
	return <-errc
}

// expire runs the lease expiry one second after every lease of the rig ran
// out.
func (r *moveRig) expire() {
	r.fake.Advance(2 * time.Second)
	r.e.Faults().ExpireLeases()
}

// TestAnExpiryDuringARefusedDeadLetterMoveKeepsTheAttemptLimit: a lease that
// runs out while an explicit dead-letter move is publishing its copy cannot
// be withdrawn, and the expiry is left pending on the move. When the
// dead-letter topic then refuses the copy, the expiry is carried out as the
// expiry itself would have carried it out: it is counted in Snapshot.Expired,
// a record that has used its attempts goes to the attempt-limit route, and
// the move reports the lease lost rather than the copy's refusal, which a
// retry could never get past.
//
// The refusal used to hand every pending withdrawal back as a close would: an
// exhausted record went back to its lane and was delivered again past
// MaxAttempts, with no expiry, no dead-letter attempt and the copy's
// retryable error.
func TestAnExpiryDuringARefusedDeadLetterMoveKeepsTheAttemptLimit(t *testing.T) {
	t.Run("an-exhausted-record-takes-the-attempt-limit-route", func(t *testing.T) {
		r := newMoveRig(t, moveRigConfig{partitions: 1, attempts: 1})
		c := r.attach(t)
		r.publish(t, 1)
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 || cl[0].Info.Attempts != 1 {
			t.Fatalf("first fetch = %+v", cl)
		}
		if err := r.move(c, cl[0], r.expire); !errors.Is(err, errcode.ErrLeaseLost) {
			t.Errorf("DeadLetter of a lease that expired during its refused copy = %v, want ErrLeaseLost", err)
		}
		snap, dsnap := r.sub.Inspect(), r.dsub.Inspect()
		if again := fetchSeqs(t, c, 1); len(again) > 0 {
			t.Errorf("the record was delivered again with attempt %d under MaxAttempts 1 (source %+v)",
				again[0].Info.Attempts, snap)
		}
		if snap.DeadLettered != 1 || snap.Expired != 1 || dsnap.Published != 1 || snap.Requeued != 0 || !snap.Conserved() {
			t.Errorf("source %+v, dead-letter topic Published %d: want the stored record expired and "+
				"dead-lettered by the attempt limit", snap, dsnap.Published)
		}
	})
	t.Run("a-record-with-attempts-left-goes-back-as-an-expiry", func(t *testing.T) {
		r := newMoveRig(t, moveRigConfig{partitions: 1, attempts: 3})
		c := r.attach(t)
		r.publish(t, 1)
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("first fetch = %+v", cl)
		}
		if err := r.move(c, cl[0], r.expire); !errors.Is(err, errcode.ErrLeaseLost) {
			t.Errorf("DeadLetter of a lease that expired during its refused copy = %v, want ErrLeaseLost", err)
		}
		snap := r.sub.Inspect()
		if snap.Expired != 1 || snap.Requeued != 1 || snap.DeadLettered != 0 || snap.Ready != 1 || !snap.Conserved() {
			t.Errorf("source %+v: want the record back in its lane through one expiry", snap)
		}
		again := fetchSeqs(t, c, 1)
		if len(again) != 1 || again[0].Info.Attempts != 2 {
			t.Errorf("the redelivery = %+v, want the record at attempt 2: an expiry keeps the attempt", again)
		}
	})
	t.Run("the-same-refusal-without-a-withdrawal-keeps-the-lease", func(t *testing.T) {
		r := newMoveRig(t, moveRigConfig{partitions: 1, attempts: 1})
		c := r.attach(t)
		r.publish(t, 1)
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("first fetch = %+v", cl)
		}
		if err := r.move(c, cl[0], func() {}); !errors.Is(err, ErrExceedsCapacity) {
			t.Fatalf("DeadLetter of a copy the dead-letter topic can never hold = %v, want ErrExceedsCapacity", err)
		}
		if snap := r.sub.Inspect(); snap.InFlight != 1 || snap.Expired != 0 {
			t.Fatalf("source %+v: want the delivery still leased", snap)
		}
		if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
			t.Fatalf("Ack after the refused copy: %v", err)
		}
	})
}

// TestAReassignmentDuringARefusedDeadLetterMoveGivesTheAttemptBack: a sticky
// reassignment that lands while an explicit dead-letter move is publishing its
// copy is left pending on the move, and a refusal of the copy carries it out
// with the reassignment's own rule: the attempt the lease counted is given
// back, so the new owner is handed the record at the attempt the moved
// delivery had (Subscription.revoke: a reassignment is not a failure). It
// used to keep the attempt, so the new owner saw one more than a reassignment
// of an Active lease reports.
//
// When an expiry landed first, the expiry is the withdrawal carried out, as
// it would have taken an Active lease before the reassignment found it.
func TestAReassignmentDuringARefusedDeadLetterMoveGivesTheAttemptBack(t *testing.T) {
	for _, expiryFirst := range []bool{false, true} {
		name := "reassignment"
		if expiryFirst {
			name = "expiry-then-reassignment"
		}
		t.Run(name, func(t *testing.T) {
			r := newMoveRig(t, moveRigConfig{partitions: 2, sticky: true})
			c1 := r.attach(t)
			r.publish(t, 2)
			cl := fetchSeqs(t, c1, 2)
			if len(cl) != 2 || cl[1].Info.Partition != 1 {
				t.Fatalf("first fetch = %+v", cl)
			}
			var c2 *Consumer[int64]
			// The second consumer takes lane 1, the lane of the moved record
			// (members in id order, lane i to member i mod 2).
			err := r.move(c1, cl[1], func() {
				if expiryFirst {
					r.expire()
				}
				c2 = r.attach(t)
			})
			if !errors.Is(err, errcode.ErrLeaseLost) {
				t.Errorf("DeadLetter of a lease reassigned during its refused copy = %v, want ErrLeaseLost", err)
			}
			got := fetchSeqs(t, c2, 2)
			if len(got) != 1 || got[0].Info.Partition != 1 {
				t.Fatalf("the second consumer fetched %+v, want the one record of lane 1", got)
			}
			snap := r.sub.Inspect()
			wantAttempts, wantExpired := uint32(1), int64(0)
			if expiryFirst {
				// Both leases ran out: lane 0's was Active and went back at
				// once, lane 1's waited for the refusal.
				wantAttempts, wantExpired = 2, 2
			}
			if got[0].Info.Attempts != wantAttempts {
				t.Errorf("the new owner's delivery reports attempt %d, want %d", got[0].Info.Attempts, wantAttempts)
			}
			if snap.Expired != wantExpired || !snap.Conserved() {
				t.Errorf("source %+v: want Expired %d", snap, wantExpired)
			}
		})
	}
}

// TestACloseDuringARefusedDeadLetterMoveFollowsTheClosePolicy: a consumer
// that closes while one of its explicit dead-letter moves is publishing its
// copy leaves the close pending on the move, and a refusal of the copy puts
// the record back with its attempt, which is the close policy, and reports
// ErrClosed, which is what a retry of the move is told.
func TestACloseDuringARefusedDeadLetterMoveFollowsTheClosePolicy(t *testing.T) {
	r := newMoveRig(t, moveRigConfig{partitions: 1})
	c := r.attach(t)
	r.publish(t, 1)
	cl := fetchSeqs(t, c, 1)
	if len(cl) != 1 {
		t.Fatalf("first fetch = %+v", cl)
	}
	err := r.move(c, cl[0], func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if !errors.Is(err, errcode.ErrClosed) {
		t.Errorf("DeadLetter by a consumer that closed during its refused copy = %v, want ErrClosed", err)
	}
	if err := c.DeadLetter(context.Background(), cl[0].Env, cl[0].Gen, r.dlq, Publish[int64]{Payload: 1, Size: 4}); !errors.Is(err, errcode.ErrClosed) {
		t.Errorf("a retry of the move = %v, want ErrClosed", err)
	}
	got := fetchSeqs(t, r.attach(t), 1)
	if len(got) != 1 || got[0].Info.Attempts != 2 {
		t.Errorf("the next consumer fetched %+v, want the record at attempt 2", got)
	}
	if snap := r.sub.Inspect(); snap.Requeued != 1 || snap.Expired != 0 || !snap.Conserved() {
		t.Errorf("source %+v: want one requeue and no expiry", snap)
	}
}

// TestASeekDuringARefusedDeadLetterMoveLeavesOneCopy: a Seek discards what
// the subscription holds and replays the retained log, and a delivery whose
// explicit dead-letter move is publishing its copy is left to the move. A
// refusal of the copy then drops the record, as the Seek would have: putting
// it back, as it did, left it in the lane beside its own replay, delivered
// twice.
func TestASeekDuringARefusedDeadLetterMoveLeavesOneCopy(t *testing.T) {
	r := newMoveRig(t, moveRigConfig{partitions: 1, retain: true})
	c := r.attach(t)
	r.publish(t, 1)
	cl := fetchSeqs(t, c, 1)
	if len(cl) != 1 {
		t.Fatalf("first fetch = %+v", cl)
	}
	err := r.move(c, cl[0], func() {
		if n, err := r.sub.Seek(StartPosition{Kind: StartEarliest}); err != nil || n != 1 {
			t.Errorf("Seek = %d, %v; want the one retained record", n, err)
		}
	})
	if !errors.Is(err, errcode.ErrClosed) {
		t.Errorf("DeadLetter of a delivery a Seek discarded = %v, want ErrClosed", err)
	}
	got := fetchSeqs(t, c, 2)
	if len(got) != 1 || got[0].Info.Seq != cl[0].Info.Seq || got[0].Info.Attempts != 1 {
		t.Errorf("after the Seek the consumer fetched %+v, want the replay of record %d alone, at attempt 1",
			got, cl[0].Info.Seq)
	}
	if snap := r.sub.Inspect(); snap.Dropped != 1 || snap.Requeued != 0 || !snap.Conserved() {
		t.Errorf("source %+v: want the discarded delivery dropped", snap)
	}
}
