package kafka_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

//
// Each of them is a way for a Kafka driver to acknowledge a record it does not
// own, lose a record it said it kept, or deliver a record twice while claiming
// it did not — and each of them is unreachable from the portable API without
// control over the broker, which is why they are here against kfake rather
// than in the conformance suite.

// TestCriticalRebalanceWithDeliveriesInFlight is the sequence.
//
// One member owns both partitions of a topic and is holding an unsettled
// delivery from each. A second member joins, so cooperative-sticky MUST give
// one partition away — that is what balancing two partitions over two members
// means, and it makes the outcome deterministic rather than a race.
//
// What then has to be true:
//
//   - the delivery from the partition that MOVED is refused with
//     ErrLeaseLost. An acknowledgement that landed there would mark a record
//     complete in a partition another member is now processing, which is the
//     one thing a rebalance must not be able to do.
//   - the delivery from the partition that STAYED is acknowledged normally. A
//     driver that invalidated everything on any rebalance would turn every
//     scale-up into a burst of redeliveries.
func TestCriticalRebalanceWithDeliveriesInFlight(t *testing.T) {
	topic := newTopic("crit-rebalance")
	cl := cluster(t, 2, []string{topic})
	c, b := openPair(t, cl)
	first := consumerOn(t, c, topic, "g-rebalance")

	// One record in each partition, named explicitly so both are certain
	// to be there.
	for p := range 2 {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("p" + strconv.Itoa(p))}, mq.Partition(p)); err != nil {
			t.Fatalf("publishing to partition %d: %v", p, err)
		}
	}
	// Hold both, unsettled. The first member owns both partitions, so both
	// arrive here.
	dst := make([]mq.Delivery, 2)
	got := 0
	for got < 2 {
		n, err := first.Receive(t.Context(), dst[got:])
		got += n
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
	held := map[int32]*mq.Delivery{}
	for i := range dst {
		held[dst[i].Info().Partition] = &dst[i]
	}
	if len(held) != 2 {
		t.Fatalf("the two deliveries came from %d partition(s), so this member did not own both", len(held))
	}
	t.Logf("holding partitions %v unsettled", []int32{0, 1})

	// The second member. Its own fetch is what drives the join.
	second := consumerOn(t, c, topic, "g-rebalance")
	go func() {
		d, err := tryReceiveOne(t, second, 25*time.Second)
		if err == nil {
			_ = d.Ack()
		}
	}()
	waitFor(t, 30*time.Second, "the first member to be revoked a partition", func() bool { return b.Rebalances() > 0 })

	lost, kept := 0, 0
	for p, d := range held {
		err := d.Ack()
		switch {
		case errors.Is(err, mq.ErrLeaseLost):
			lost++
			t.Logf("partition %d was revoked, and its in-flight delivery was refused with ErrLeaseLost", p)
		case err == nil:
			kept++
			t.Logf("partition %d stayed, and its in-flight delivery was acknowledged", p)
		default:
			t.Fatalf("partition %d's Ack = %v; the only correct answers are nil (it stayed) and ErrLeaseLost (it did not)", p, err)
		}
	}
	if lost == 0 {
		t.Fatalf("both in-flight deliveries were acknowledged after %d revocation(s): a settlement for a revoked partition must report ErrLeaseLost ", b.Rebalances())
	}
	if kept+lost != 2 {
		t.Fatalf("%d kept and %d lost of two", kept, lost)
	}
}

// TestCriticalRevokedDeliveryReportsLeaseLost is the same rule for a partition
// that is LOST rather than revoked, which is the worse half.
//
// A revoke is cooperative: the driver gets to wait for its in-flight
// deliveries and commit the safe boundary. A loss is not — the coordinator has
// already taken the partition away, so the boundary CANNOT be committed and
// the records completed since the last commit are redelivered to whoever takes
// it. What must still hold is that no settlement lands: every delivery handed
// out before the loss reports ErrLeaseLost.
//
// The loss is forced by fencing the member's heartbeat, which is exactly what
// a member that missed its session timeout sees.
func TestCriticalRevokedDeliveryReportsLeaseLost(t *testing.T) {
	topic := newTopic("crit-lost")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons := consumerOn(t, c, topic, "g-lost")
	publish(t, c, topic, &mq.Message{Body: []byte("in-flight")})
	held := receiveOne(t, cons, 15*time.Second)

	// Every heartbeat is answered with UNKNOWN_MEMBER_ID, which is the
	// coordinator saying "you are not in this group any more". franz-go
	// rejoins, and because it cannot commit for a generation it no longer
	// has, the partitions go through OnPartitionsLost.
	fault := cl.Fault(kfake.Fault{
		Keys:     []kmsg.Key{kmsg.Heartbeat},
		Group:    "g-lost",
		Err:      kerr.UnknownMemberID,
		TopLevel: true,
		Count:    2,
	})
	defer fault.Remove()
	waitFor(t, 30*time.Second, "the partition to be taken away", func() bool {
		return b.Rebalances() > 0
	})

	err := held.Ack()
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack of a delivery whose partition was taken away = %v, want ErrLeaseLost: acknowledging it would complete a record this process no longer owns ", err)
	}
	// A negative settlement is refused for the same reason: neither
	// direction may reach a partition somebody else owns.
	if err := held.Nack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Nack of the same delivery = %v, want ErrLeaseLost", err)
	}
	// And the record is redelivered, because its boundary was never
	// committed. That is the at-least-once window opening, and
	// LostPartitions is what says so.
	again := receiveOne(t, cons, 20*time.Second)
	if string(again.Body) != "in-flight" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	t.Logf("rebalances=%d lost-partitions=%d", b.Rebalances(), b.LostPartitions())
}

// TestCriticalOutOfOrderCompletion is the offset tracker's whole purpose.
//
// A partition's records are completed in arbitrary order, and the committed
// offset must never pass one that has not completed. A driver that committed
// the highest completed offset instead would tell the next consumer to skip
// every record still in flight below it, and those records would be lost with
// nothing anywhere reporting a failure.
func TestCriticalOutOfOrderCompletion(t *testing.T) {
	topic := newTopic("crit-order")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons := consumerOn(t, c, topic, "g-order")
	const n = 12
	for i := range n {
		publish(t, c, topic, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	// Hold every delivery, then settle them from the top down. Nothing may
	// be committed until offset 0 completes, and then everything may.
	dst := make([]mq.Delivery, n)
	got := 0
	for got < n {
		m, err := cons.Receive(t.Context(), dst[got:])
		got += m
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
	for i := n - 1; i >= 1; i-- {
		if err := dst[i].Ack(); err != nil {
			t.Fatalf("acknowledging offset %d: %v", dst[i].Info().Offset, err)
		}
	}
	// Force a commit of whatever is safe, then prove it is nothing: the
	// lowest offset has not completed, so the watermark has not moved.
	if err := b.Commit(t.Context()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The group's own lag says it directly, and it is what a mutation that
	// committed the highest COMPLETED offset rather than the safe watermark
	// is caught by: eleven of twelve records have been acknowledged, and the
	// committed position must still be behind all twelve.
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	stats, err := admin.Stats(t.Context(), topic, "g-order")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Lag.Known {
		t.Fatal("the broker did not report the group's lag, so this check proves nothing")
	}
	if stats.Lag.Value != n {
		t.Fatalf("the group's lag is %d of %d: the commit passed a record whose completion everything above it is waiting for (cumulative acknowledgement)", stats.Lag.Value, n)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	replay := consumerOn(t, c, topic, "g-order")
	d := receiveOne(t, replay, 15*time.Second)
	if string(d.Body) != "0" {
		t.Fatalf("the reopened subscription starts at %q, want the record whose completion everything above it waited for", d.Body)
	}
	// Now complete it, and the watermark passes all twelve at once.
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	rest := receiveN(t, replay, n-1, 15*time.Second, func(x *mq.Delivery) error { return x.Ack() })
	if rest != n-1 {
		t.Fatalf("%d of the %d records above the gap were redelivered", rest, n-1)
	}
	if err := replay.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	final := consumerOn(t, c, topic, "g-order")
	expectNothing(t, final, time.Second, "records whose contiguous completion has been committed")
}

// TestCriticalLeaderChange: a partition's leader is re-elected underneath the
// consumer and the producer, and nothing is lost or duplicated.
//
// A leader change is the ordinary operational event that breaks a naive
// driver: the fetch fails with NOT_LEADER_FOR_PARTITION, the produce fails
// with the same, and both are retryable against the NEW leader rather than
// failures to report. What must not happen is a record vanishing or an
// acknowledgement landing on the wrong generation.
func TestCriticalLeaderChange(t *testing.T) {
	topic := newTopic("crit-leader")
	cl := cluster(t, 4, []string{topic}, kfake.NumBrokers(3))
	c, b := openPair(t, cl)
	cons := consumerOn(t, c, topic, "g-leader")
	const n = 24
	for i := range n / 2 {
		publish(t, c, topic, &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))})
	}
	seen := map[string]int{}
	var mu sync.Mutex
	take := func(count int, budget time.Duration) {
		receiveN(t, cons, count, budget, func(d *mq.Delivery) error {
			mu.Lock()
			seen[string(d.Body)]++
			mu.Unlock()
			return d.Ack()
		})
	}
	take(n/2, 20*time.Second)

	// Every partition gets a new leader and a bumped epoch, which is what
	// an operator's rolling restart looks like from a client.
	cl.ShufflePartitionLeaders()

	for i := n / 2; i < n; i++ {
		publish(t, c, topic, &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))})
	}
	take(n/2, 30*time.Second)

	mu.Lock()
	defer mu.Unlock()
	for i := range n {
		switch got := seen[strconv.Itoa(i)]; {
		case got == 0:
			t.Errorf("record %d was lost across the leader change", i)
		case got > 1:
			t.Logf("record %d arrived %d times: at-least-once across a leader change is allowed and is worth knowing", i, got)
		}
	}
	if b.LostPartitions() > 0 {
		t.Logf("%d partition(s) were lost rather than revoked, so their boundaries were not committed", b.LostPartitions())
	}
}

// TestCriticalRetryTopicPublicationFails is the ordering rule.
//
// The retry-topic strategy moves a nacked record to `<topic>.retry.1` and then
// commits the original offset. When that publication FAILS the original offset
// must NOT be committed and the settlement must fail, or the record is gone:
// acknowledged here, never written there, and nothing anywhere reporting a
// loss. The specification says it outright — "a failed or ambiguous DLQ publish never acks
// the original" — and this is the test that says the driver means it.
func TestCriticalRetryTopicPublicationFails(t *testing.T) {
	topic := newTopic("crit-retryfail")
	retry1 := topic + ".retry.1"
	cl := cluster(t, 1, []string{topic, retry1})
	c := openOn(t, cl, func(cfg *kq.Config) {
		cfg.Nack = nack.RetryTopic
		cfg.NackConfig.Retries = 2
	})
	cons := consumerOn(t, c, topic, "g-retryfail")
	publish(t, c, topic, &mq.Message{Body: []byte("must-not-vanish")})
	d := receiveOne(t, cons, 15*time.Second)

	// Every produce to the retry topic is refused, for as long as the
	// fault is installed.
	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: retry1,
		Err:   kerr.TopicAuthorizationFailed,
		Count: -1,
	})
	err := d.Nack()
	if err == nil {
		t.Fatal("the settlement succeeded while the republication was refused: the record has been acknowledged on the source topic and written nowhere ")
	}
	t.Logf("the refused republication surfaced as: %v", err)
	fault.Remove()

	// The record is still there to be settled, which is the whole point: a
	// definite refusal leaves the delivery settleable.
	if err := d.Nack(); err != nil {
		t.Fatalf("the retried settlement = %v; the record should still be settleable", err)
	}
	retryCons := consumerFrom(t, c, retry1, "g-retryfail-reader", mq.StartEarliest())
	got := receiveOne(t, retryCons, 15*time.Second)
	if string(got.Body) != "must-not-vanish" {
		t.Fatalf("the retry topic holds %q", got.Body)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// And exactly one copy reached the retry topic: the failed attempt
	// published nothing.
	expectNothing(t, retryCons, time.Second, "a second copy of a record whose first republication was refused")
}

// TestCriticalDeadLetterPublicationFails is the same rule on the dead-letter
// route, which is the one the specification names by name.
func TestCriticalDeadLetterPublicationFails(t *testing.T) {
	topic, dlq := newTopic("crit-dlfail"), newTopic("crit-dlfail-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	cons := consumerOn(t, c, topic, "g-dlfail")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 15*time.Second)

	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: dlq,
		Err:   kerr.NotEnoughReplicas,
		Count: -1,
	})
	if err := d.Reject(); err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication was refused: the specification forbids acknowledging the original")
	}
	fault.Remove()

	// The original is still uncommitted, so a reopened subscription gets
	// it again rather than skipping it.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "g-dlfail")
	back := receiveOne(t, again, 15*time.Second)
	if string(back.Body) != "poison" {
		t.Fatalf("the reopened subscription holds %q, want the record whose dead-lettering failed", back.Body)
	}
	if err := back.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// waitFor polls until cond holds, or fails.
func waitFor(tb testing.TB, budget time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out after %v waiting for %s", budget, what)
}
