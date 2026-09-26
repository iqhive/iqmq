package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
)

// The Admin surface's destroying and inspecting halves.
//
// The creating half is already driven by every test that provisions a topic.
// DeleteSubscription had no test at all, and Stats and Purge were covered
// only as far as their happy paths -- which for Stats is the least
// interesting part, because the whole point of it is what it does when Redis
// answers something other than a number.

// adminOf is the admin surface over a rig.
func adminOf(t *testing.T, r *rig) mq.Admin {
	t.Helper()
	adm, err := r.b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return adm
}

// provisioned is a rig with one topic and one subscription on it.
func provisioned(t *testing.T, sub string) (*rig, mq.Admin) {
	t.Helper()
	r := newRig(t)
	adm := adminOf(t, r)
	if err := adm.EnsureTopic(r.ctx, mq.TopicSpec{Name: testTopic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := adm.EnsureSubscription(r.ctx, mq.SubscriptionSpec{Topic: testTopic, Name: sub}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	return r, adm
}

// TestPurgeSkipsTheUndeliveredAndKeepsSomebodysInFlightWork.
//
// A purge here is XGROUP SETID to the end of the stream, which moves the
// group past everything it has not delivered. It deliberately does NOT touch
// the pending-entries list: an entry in the pending list HAS been delivered
// and is somebody's in-flight work, so deleting it would be a purge that
// threw away a message a handler is running.
//
// Both halves are asserted here, because a purge that emptied the pending
// list too would still pass a test that only checked the depth.
func TestPurgeSkipsTheUndeliveredAndKeepsSomebodysInFlightWork(t *testing.T) {
	r, adm := provisioned(t, "workers")
	sink, src := r.openPair(t)

	for range 3 {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// One delivered and unsettled: this is what must survive the purge.
	dst := make([]mq.DriverDelivery, 1)
	fctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	if n, err := src.Fetch(fctx, dst); err != nil || n != 1 {
		t.Fatalf("Fetch returned %d, %v; want one delivery to hold unsettled", n, err)
	}

	before, err := adm.Stats(r.ctx, testTopic, "workers")
	if err != nil {
		t.Fatalf("Stats before the purge: %v", err)
	}
	// However many the source PREFETCHED is how many are pending: a fetch of
	// one reads a batch, and every entry in that batch has been delivered.
	// The count is whatever it is; what matters is that the purge does not
	// change it.
	if before.InFlight.Value == 0 {
		t.Fatal("nothing is in flight before the purge, so the purge would prove nothing about pending work")
	}

	if err := adm.Purge(r.ctx, testTopic, "workers"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	after, err := adm.Stats(r.ctx, testTopic, "workers")
	if err != nil {
		t.Fatalf("Stats after the purge: %v", err)
	}
	if after.Depth.Value != 0 {
		t.Errorf("the subscription is still %d entries behind after Purge, want 0", after.Depth.Value)
	}
	if after.InFlight.Value != before.InFlight.Value {
		t.Errorf("Purge changed the in-flight count from %d to %d; a purge is about what has NOT been "+
			"delivered, and a pending entry is a message somebody's handler is running",
			before.InFlight.Value, after.InFlight.Value)
	}
}

// TestDeleteSubscriptionRemovesTheGroupAndIsIdempotent.
//
// Every shard's group is destroyed, and a shard that has no group is not a
// failure: NOGROUP is what a second delete sees, and a teardown that failed
// on it would need a pre-check with the window a pre-check always has.
func TestDeleteSubscriptionRemovesTheGroupAndIsIdempotent(t *testing.T) {
	r, adm := provisioned(t, "workers")

	if _, err := adm.Stats(r.ctx, testTopic, "workers"); err != nil {
		t.Fatalf("Stats on the provisioned subscription: %v", err)
	}
	if err := adm.DeleteSubscription(r.ctx, testTopic, "workers"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if _, err := adm.Stats(r.ctx, testTopic, "workers"); err == nil {
		t.Error("Stats still answers for a subscription that was deleted; the group is still there")
	}
	if err := adm.DeleteSubscription(r.ctx, testTopic, "workers"); err != nil {
		t.Errorf("DeleteSubscription a second time is %v, want nil: NOGROUP is what a second delete sees "+
			"and the caller asked for the group to be gone, which it is", err)
	}
}

// TestStatsForASubscriptionNobodyCreatedIsNotAnEmptyQueue.
//
// A group that exists nowhere sums to zero on every shard, exactly as an
// empty one does. Answering QueueStats{} would tell a caller their
// subscription is drained and up to date when in fact nothing is reading.
func TestStatsForASubscriptionNobodyCreatedIsNotAnEmptyQueue(t *testing.T) {
	r, adm := provisioned(t, "workers")

	_, err := adm.Stats(r.ctx, testTopic, "nobody-created-this")
	if err == nil {
		t.Fatal("Stats for a group that exists nowhere returned figures; it sums to the same zero an empty " +
			"subscription does, and the two are very different facts")
	}
}

// TestStatsReportsRedisOwnLagAndSaysWhenItCannot.
//
// Redis reports a group's `lag` as NIL once an XDEL or an XTRIM has made the
// count unreliable, and Limit is how "the server did not say" is
// reported rather than as a zero. The pending count is never unknown, so the
// two fields have to move independently: a driver that took one Known for
// both would either lose the pending count or claim a lag nobody computed.
func TestStatsReportsRedisOwnLagAndSaysWhenItCannot(t *testing.T) {
	r, adm := provisioned(t, "workers")
	sink, _ := r.openPair(t)
	for range 2 {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	stats, err := adm.Stats(r.ctx, testTopic, "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Lag.Known {
		t.Error("Stats reports the lag as unknown on an untrimmed stream, where Redis does report it")
	}
	if stats.Depth.Value != stats.Lag.Value {
		t.Errorf("Stats reports depth %d and lag %d; on a consumer group they are the same figure -- the "+
			"entries the group has not been delivered", stats.Depth.Value, stats.Lag.Value)
	}
	if !stats.InFlight.Known {
		t.Error("Stats reports the in-flight count as unknown; the pending count is always a number")
	}
}

// TestAnAdministrativeFailureIsReportedAndNotSwallowed.
//
// Every one of these goes through DoMulti, and every one of them has a
// per-result error to check as well as the call's own -- DoMulti itself
// returns nil and puts the failure in the result. A driver that checked only
// the call would report success for a command the server refused.
//
// The fake's fault table is one-shot and keyed by verb and subcommand, so
// each of these fails exactly ONE shard's command and the rest succeed: the
// per-result check is the only thing that can notice.
func TestAnAdministrativeFailureIsReportedAndNotSwallowed(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail string
		call func(mq.Admin, *rig) error
	}{
		{"EnsureTopic", "XGROUP CREATE", func(a mq.Admin, r *rig) error {
			return a.EnsureTopic(r.ctx, mq.TopicSpec{Name: "another.topic"})
		}},
		{"EnsureSubscription", "XGROUP CREATE", func(a mq.Admin, r *rig) error {
			return a.EnsureSubscription(r.ctx, mq.SubscriptionSpec{Topic: testTopic, Name: "second"})
		}},
		{"DeleteSubscription", "XGROUP DESTROY", func(a mq.Admin, r *rig) error {
			return a.DeleteSubscription(r.ctx, testTopic, "workers")
		}},
		{"Purge", "XGROUP SETID", func(a mq.Admin, r *rig) error {
			return a.Purge(r.ctx, testTopic, "workers")
		}},
		{"Stats", "XINFO", func(a mq.Admin, r *rig) error {
			_, err := a.Stats(r.ctx, testTopic, "workers")
			return err
		}},
		{"DeleteTopic", "DEL", func(a mq.Admin, r *rig) error {
			return a.DeleteTopic(r.ctx, testTopic)
		}},
		{"ListTopics", "SCAN", func(a mq.Admin, r *rig) error {
			for _, err := range a.ListTopics(r.ctx) {
				if err != nil {
					return err
				}
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, adm := provisioned(t, "workers")
			r.srv.Fail(tc.fail, errors.New("ERR the server refused it"))
			if err := tc.call(adm, r); err == nil {
				t.Errorf("%s returned nil while %s was failing; the per-result error has to be checked as "+
					"well as the call's own, or a refused command reads as success", tc.name, tc.fail)
			}
		})
	}
}

// TestATopicNameTheKeyLayoutRefusesIsRefusedByEveryCall.
//
// The key layout puts a topic in a hash tag, so a topic containing a brace
// cannot be addressed at all. The refusal belongs on every call that takes a
// name -- one that forgot would build a key that collides with another
// topic's.
func TestATopicNameTheKeyLayoutRefusesIsRefusedByEveryCall(t *testing.T) {
	r, adm := provisioned(t, "workers")
	const bad = "orders{shard}"

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"EnsureTopic", func() error { return adm.EnsureTopic(r.ctx, mq.TopicSpec{Name: bad}) }},
		{"DeleteTopic", func() error { return adm.DeleteTopic(r.ctx, bad) }},
		{"EnsureSubscription", func() error {
			return adm.EnsureSubscription(r.ctx, mq.SubscriptionSpec{Topic: bad, Name: "workers"})
		}},
		{"DeleteSubscription", func() error { return adm.DeleteSubscription(r.ctx, bad, "workers") }},
		{"Purge", func() error { return adm.Purge(r.ctx, bad, "workers") }},
		{"Stats", func() error { _, err := adm.Stats(r.ctx, bad, "workers"); return err }},
	} {
		if err := tc.call(); err == nil {
			t.Errorf("%s accepted the topic %q, which the key layout cannot address", tc.name, bad)
		}
	}
}

// TestAdminIsRefusedOnAClosedBackend.
func TestAdminIsRefusedOnAClosedBackend(t *testing.T) {
	r := newRig(t)
	if err := r.b.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}
	if _, err := r.b.Admin(); err == nil {
		t.Error("Admin() on a closed backend returned a surface; there is no connection left to administer over")
	}
	_ = rd.DriverName
}

// TestAnUnknownLagIsReportedAsUnknownAndNotAsZero.
//
// Redis answers a group's `lag` with NIL once an XDEL or an XTRIM has made
// the count unreliable. The specification forbids reporting that as zero: a caller
// watching the depth would see a backlog turn into "drained" the moment
// somebody trimmed the stream.
//
// The pending count is never unknown, so the two fields have to move
// independently -- a driver that took one Known for both would either lose
// the in-flight count or claim a lag nobody computed.
func TestAnUnknownLagIsReportedAsUnknownAndNotAsZero(t *testing.T) {
	r, adm := provisioned(t, "workers")
	sink, src := r.openPair(t)
	for range 2 {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	dst := make([]mq.DriverDelivery, 1)
	fctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	if _, err := src.Fetch(fctx, dst); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	r.srv.ReportUnknownLag()
	stats, err := adm.Stats(r.ctx, testTopic, "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Lag.Known {
		t.Errorf("Stats reports the lag as known (%d) while the server answered nil; a caller watching the "+
			"depth would see a backlog turn into \"drained\" the moment somebody trimmed the stream",
			stats.Lag.Value)
	}
	if stats.Depth.Known {
		t.Errorf("Stats reports the depth as known (%d) while the lag it comes from is not", stats.Depth.Value)
	}
	if !stats.InFlight.Known {
		t.Error("Stats lost the in-flight count along with the lag; the pending count is always a number")
	}
}

// TestDeletingASubscriptionOnATopicThatIsNotThereIsSuccess.
//
// The fake answers XGROUP DESTROY with Redis's documented integer reply -- 0
// destroyed groups -- for a key it does not hold, so this half needs no
// tolerance from the driver at all.
func TestDeletingASubscriptionOnATopicThatIsNotThereIsSuccess(t *testing.T) {
	r := newRig(t)
	adm := adminOf(t, r)

	if err := adm.DeleteSubscription(r.ctx, "topic.nobody.created", "workers"); err != nil {
		t.Errorf("DeleteSubscription on a topic that does not exist is %v, want nil: the caller asked for the "+
			"group to be gone, which it is", err)
	}
}

// TestDeleteSubscriptionToleratesNOGROUPFromTheServer.
//
// A server that answers NOGROUP instead of a count is saying the group is not
// there, which is what a delete wants. The tolerance is reached by INJECTING
// the reply rather than by provoking it: the fake models XGROUP DESTROY as
// the integer reply Redis documents, so nothing it does on its own produces a
// NOGROUP, and the branch would otherwise be unreachable without a container.
func TestDeleteSubscriptionToleratesNOGROUPFromTheServer(t *testing.T) {
	r, adm := provisioned(t, "workers")
	r.srv.Fail("XGROUP DESTROY", errors.New("NOGROUP No such key or consumer group"))

	if err := adm.DeleteSubscription(r.ctx, testTopic, "workers"); err != nil {
		t.Errorf("DeleteSubscription is %v when the server answered NOGROUP, want nil: the server is saying "+
			"the group is not there, which is what the caller asked for", err)
	}
}
