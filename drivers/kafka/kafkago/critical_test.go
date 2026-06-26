package kafkago_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	mq "bitbucket.org/iqhive/iqmq"
)

//
// Each of them is a way for a Kafka driver to acknowledge a record it does not
// own, lose a record it said it kept, or deliver a record twice while claiming
// it did not — and each of them is unreachable from the portable API without
// control over the broker, which is why they are here against kfake rather
// than in the conformance suite.
//
// One of the five has a DIFFERENT CORRECT ANSWER on this client from the
// franz-go driver's, and that difference is the scenario's whole value: see
// TestCriticalRebalanceWithDeliveriesInFlight.

// TestCriticalRebalanceWithDeliveriesInFlight is the sequence, and it is
// where the two Kafka drivers legitimately disagree.
//
// One member owns both partitions of a topic and is holding an unsettled
// delivery from each. A second member joins, so the group must rebalance.
//
// On the franz-go driver, cooperative-sticky gives ONE partition away and the
// delivery from the partition that stayed is still acknowledgeable. kafka-go
// has no cooperative assignor at all — range and round-robin are both eager —
// so the whole assignment is revoked and handed back, and EVERY in-flight
// delivery is refused with ErrLeaseLost.
//
// What has to be true either way is the safety half: a settlement must never
// land on a partition another member is now processing. What this test pins is
// that the driver does not pretend otherwise — it refuses both, rather than
// keeping a handle whose generation the eager rebalance has already
// invalidated.
func TestCriticalRebalanceWithDeliveriesInFlight(t *testing.T) {
	topic := newTopic("crit-rebalance")
	cl := cluster(t, 2, []string{topic})
	c, b := openPair(t, cl)
	first := consumerOn(t, c, topic, "g-rebalance")

	// One record in each partition, named explicitly so both are certain to
	// be there.
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
	before := b.Rebalances()

	// The second member. Its own fetch is what drives the join.
	second := consumerOn(t, c, topic, "g-rebalance")
	go func() {
		d, err := tryReceiveOne(t, second, 25*time.Second)
		if err == nil {
			_ = d.Ack()
		}
	}()
	waitFor(t, 30*time.Second, "the first member's generation to end", func() bool { return b.Rebalances() > before })

	lost, kept := 0, 0
	for p, d := range held {
		err := d.Ack()
		switch {
		case errors.Is(err, mq.ErrLeaseLost):
			lost++
			t.Logf("partition %d's in-flight delivery was refused with ErrLeaseLost", p)
		case err == nil:
			kept++
			t.Logf("partition %d's in-flight delivery was acknowledged", p)
		default:
			t.Fatalf("partition %d's Ack = %v; the only correct answers are nil (this member still owns it) and ErrLeaseLost (it does not)", p, err)
		}
	}
	if lost+kept != 2 {
		t.Fatalf("%d kept and %d lost of two", kept, lost)
	}
	// The eager half. kafka-go revokes the whole assignment, so neither
	// delivery survives the rebalance; a driver that acknowledged either one
	// would be acknowledging a record for a generation the coordinator has
	// already ended.
	if kept != 0 {
		t.Fatalf("%d of two in-flight deliveries were acknowledged after the generation ended: kafka-go's assignors are eager, so a rebalance revokes the WHOLE assignment and every handle from the previous generation is stale ", kept)
	}
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
	// Hold every delivery, then settle them from the top down. Nothing may be
	// committed until offset 0 completes, and then everything may.
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
	// committed the highest COMPLETED offset rather than the safe watermark is
	// caught by: eleven of twelve records have been acknowledged, and the
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
// consumer and the producer, and nothing is lost.
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
	take := func(count int, budget time.Duration) int {
		return receiveN(t, cons, count, budget, func(d *mq.Delivery) error {
			mu.Lock()
			seen[string(d.Body)]++
			mu.Unlock()
			return d.Ack()
		})
	}
	take(n/2, 25*time.Second)

	// Every partition gets a new leader and a bumped epoch, which is what an
	// operator's rolling restart looks like from a client.
	cl.ShufflePartitionLeaders()

	for i := n / 2; i < n; i++ {
		publish(t, c, topic, &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))})
	}
	take(n/2, 40*time.Second)

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
		t.Logf("%d partition(s) had a boundary that could not be committed", b.LostPartitions())
	}
}
