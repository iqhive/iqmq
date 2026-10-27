package streams_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// otherGroup is a second subscription on the rig's topic.
const otherGroup = "audit"

// joinGroup provisions another consumer group over the rig's topic, on the
// rig's server: a second subscription reading the same stream.
func joinGroup(t *testing.T, r *rig, group string) *streams.Consumer {
	t.Helper()
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys:        r.keys,
		Group:       group,
		Name:        group + "-a",
		Prefetch:    16,
		AckDeadline: testLease,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := cons.Provision(r.ctx, r.srv, streams.IDZero, true); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	return cons
}

// TestARetryReachesOnlyTheGroupThatNackedIt holds that a nack under a
// strategy that puts the message back into the stream retries it for the
// nacking subscription only. The stream is shared by every group on the
// topic, so an unmarked copy was delivered again to a subscription that had
// already settled the message.
func TestARetryReachesOnlyTheGroupThatNackedIt(t *testing.T) {
	for _, k := range []streams.Kind{streams.ReAdd, streams.ZSetMover} {
		t.Run(k.String(), func(t *testing.T) {
			r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = k })
			audit := joinGroup(t, r, otherGroup)
			r.publish(t, "payload")
			theirs, _, err := audit.Read(r.ctx, r.srv, nil, nil)
			if err != nil || len(theirs) != 1 {
				t.Fatalf("the other group read %d deliveries (%v), want 1", len(theirs), err)
			}
			if errs := audit.Ack(r.ctx, r.srv, theirs); errs[0] != nil {
				t.Fatalf("Ack: %v", errs[0])
			}
			ds := r.read(t)
			if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			if k == streams.ZSetMover {
				movePark(t, r)
			}

			again, _, err := audit.Read(r.ctx, r.srv, nil, nil)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(again) != 0 {
				t.Fatalf("the other group was delivered %d copies of a message it had already settled, want 0", len(again))
			}
			if n := len(r.srv.Pending(r.keys.Stream(0), otherGroup)); n != 0 {
				t.Fatalf("the other group holds %d pending entries after skipping the retry, want 0", n)
			}

			mine := r.read(t)
			if len(mine) != 1 {
				t.Fatalf("the nacking group was redelivered %d copies, want 1", len(mine))
			}
			if mine[0].Attempt != 2 {
				t.Fatalf("the retry's attempt is %d, want 2", mine[0].Attempt)
			}
		})
	}
}

// TestADeadLetteredRetryCarriesNoGroupStamp holds that the stamp stays on the
// retry path: the dead-letter stream is read by groups of its own, and a copy
// still stamped for the source's group would be skipped by every one of them.
func TestADeadLetteredRetryCarriesNoGroupStamp(t *testing.T) {
	dlq := testKeys(t).Stream(0) + ":dlq"
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) {
		c.Nack = streams.ReAdd
		c.DeadLetterStream = dlq
	})
	r.publish(t, "payload")
	ds := r.read(t)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	retry := r.read(t)
	if len(retry) != 1 {
		t.Fatalf("the retry was redelivered %d times, want 1", len(retry))
	}
	if err := r.cons.Nack(r.ctx, r.srv, retry[0], mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now()); err != nil {
		t.Fatalf("dead-letter Nack: %v", err)
	}
	dead := r.srv.Entries(dlq)
	if len(dead) != 1 {
		t.Fatalf("the dead-letter stream holds %d entries, want 1", len(dead))
	}
	if g, ok := streams.Internal(dead[0].Fields, streams.FieldPrefixInternal+"g"); ok {
		t.Fatalf("the dead-lettered copy is stamped for group %q", g)
	}
}

// TestADeadLetteredCopyStartsAtTheFirstAttempt holds that the dead-letter
// copy leaves the source's attempt count behind. The copy carried it in x:a,
// which a reader adds to the broker's own counter, so the dead-letter
// subscription's FIRST delivery reported attempt N+1 and the driver reported
// it as a redelivery.
//
// Both places the count lives are covered (the broker's delivery counter
// under leave-pending, where the move wrote the rejected attempt into the
// copy, and an x:a already on the entry under re-add), and so are both
// dead-letter routes, the co-located script and the cross-slot pair. A reader can be opened only
// on the cross-slot destination, which is a topic of its own; the
// co-located one is a key beside the source's shard, and the field is what a
// reader's count would be read from.
func TestADeadLetteredCopyStartsAtTheFirstAttempt(t *testing.T) {
	for _, k := range []streams.Kind{streams.LeavePending, streams.ReAdd} {
		t.Run(k.String()+"/same-slot", func(t *testing.T) {
			dlq := testKeys(t).Stream(0) + ":dlq"
			r := rejectAfterRetries(t, k, dlq)
			assertNoCarriedAttempt(t, r, dlq)
		})
		t.Run(k.String()+"/cross-slot", func(t *testing.T) {
			dlqKeys, err := streams.NewKeys(testTopic+".dlq", 1)
			if err != nil {
				t.Fatalf("NewKeys: %v", err)
			}
			r := rejectAfterRetries(t, k, dlqKeys.Stream(0))
			assertNoCarriedAttempt(t, r, dlqKeys.Stream(0))
			reader, err := streams.NewConsumer(streams.ConsumerConfig{
				Keys: dlqKeys, Group: "dlq-readers", Name: "dlq-a", Prefetch: 4, AckDeadline: testLease,
			})
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			if err := reader.Provision(r.ctx, r.srv, streams.IDZero, true); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			first, _, err := reader.Read(r.ctx, r.srv, nil, nil)
			if err != nil || len(first) != 1 {
				t.Fatalf("the dead-letter reader read %d deliveries (%v), want 1", len(first), err)
			}
			if first[0].Attempt != 1 || first[0].Redelivered {
				t.Fatalf("the dead-letter subscription's first delivery reports attempt %d (redelivered %v), want 1",
					first[0].Attempt, first[0].Redelivered)
			}
		})
	}
}

// rejectAfterRetries publishes one message, nacks it three times under k and
// dead-letters its fourth delivery to dlq.
func rejectAfterRetries(t *testing.T, k streams.Kind, dlq string) *rig {
	t.Helper()
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) {
		c.Nack = k
		c.DeadLetterStream = dlq
	})
	r.publish(t, "payload")
	d := r.read(t)[0]
	for range 3 {
		if err := r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{}, r.srv.Now()); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		got := redeliver(t, r, k)
		if len(got) != 1 {
			t.Fatalf("redelivered %d messages, want 1", len(got))
		}
		d = got[0]
	}
	if d.Attempt != 4 {
		t.Fatalf("the fourth delivery reports attempt %d, want 4: there would be no count to leave behind", d.Attempt)
	}
	if err := r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{Action: mq.NackActionDeadLetter}, r.srv.Now()); err != nil {
		t.Fatalf("dead-letter Nack: %v", err)
	}
	return r
}

// assertNoCarriedAttempt checks the one entry in dlq carries no attempt count.
func assertNoCarriedAttempt(t *testing.T, r *rig, dlq string) {
	t.Helper()
	dead := r.srv.Entries(dlq)
	if len(dead) != 1 {
		t.Fatalf("the dead-letter stream holds %d entries, want 1", len(dead))
	}
	if a, ok := streams.Internal(dead[0].Fields, streams.FieldAttempt); ok {
		t.Fatalf("the dead-lettered copy carries the source's attempt count %q, so its first reader starts past 1", a)
	}
	if dead[0].Fields[1] != "payload" {
		t.Fatalf("the dead-lettered copy's body is %q", dead[0].Fields[1])
	}
}

// testKeys is the rig's key naming, for a key that has to share its slot.
func testKeys(t *testing.T) streams.Keys {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	return keys
}

// TestStopReleasesAShardWhoseHeldEntryAnotherConsumerSettled holds that the
// Stop strategy's hold ends when the held entry stops being this consumer's.
// Another consumer's sweep can claim the entry and acknowledge it, and then
// this consumer never sees it again: a hold waiting for its own settlement of
// that entry stopped the shard for good.
func TestStopReleasesAShardWhoseHeldEntryAnotherConsumerSettled(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	id := r.publish(t, "first")
	ds := r.read(t)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n := r.cons.Held(); n != 1 {
		t.Fatalf("Held() = %d while the entry is still this consumer's, want 1", n)
	}
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	if _, err := r.srv.Do(r.ctx, streams.XAck(r.keys.Stream(0), testGroup, []streams.ID{id})); err != nil {
		t.Fatalf("XACK: %v", err)
	}
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d after another consumer settled the held entry, want 0", n)
	}
	r.publish(t, "second")
	if next := r.read(t); len(next) != 1 {
		t.Fatalf("the shard delivered %d entries after the hold lifted, want 1", len(next))
	}
}

// TestStopReleasesAShardNackedAfterItsLeaseMoved is the same hold placed too
// late: the nack arrives after another consumer has already claimed the
// entry, so this consumer will never settle it.
func TestStopReleasesAShardNackedAfterItsLeaseMoved(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	id := r.publish(t, "first")
	ds := r.read(t)
	r.srv.Advance(testLease + time.Second)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if _, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n := r.cons.Held(); n != 0 {
		t.Fatalf("Held() = %d for an entry another consumer holds, want 0", n)
	}
}
