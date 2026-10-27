package kafkago_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	mq "github.com/iqhive/iqmq"
)

// recreate deletes topic and creates it again with one partition, which is
// the plainest way to put a live consumer's position past the end of its log.
func recreate(tb testing.TB, c *kfake.Cluster, topic string) {
	tb.Helper()
	if err := c.DeleteTopic(topic); err != nil {
		tb.Fatalf("deleting %s: %v", topic, err)
	}
	if err := c.CreateTopic(topic, 1, nil); err != nil {
		tb.Fatalf("recreating %s: %v", topic, err)
	}
}

// publishBodies publishes one message per body, in order.
func publishBodies(tb testing.TB, cl *mq.Client, topic string, bodies ...string) {
	tb.Helper()
	for _, body := range bodies {
		publish(tb, cl, topic, &mq.Message{Body: []byte(body)})
	}
}

// receiveEach receives n deliveries, or as many as arrive within budget, each
// into a slot of its own. receiveN reuses its buffer from one Receive to the
// next, so a delivery a test holds there is overwritten by a later one.
func receiveEach(tb testing.TB, c *mq.ConsumerHandle, n int, budget time.Duration) []mq.Delivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, n)
	got := 0
	for got < n {
		m, err := c.Receive(ctx, dst[got:])
		got += m
		if err != nil {
			break
		}
	}
	return dst[:got]
}

// numbered is n bodies named prefix0, prefix1 and so on.
func numbered(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + strconv.Itoa(i)
	}
	return out
}

// A consumer whose position the log no longer holds resumes at the log's
// earliest offset, and the records there must be delivered even though their
// offsets are below ones it has already consumed. A topic deleted and
// recreated under a live consumer is the plain case: the new log starts again
// at zero, and the partition's tracker, still based where the old log ended,
// refused every record of the new log below that as out of order.
//
// The new log is shorter than the old one, so the position is past its end
// however the fetches and the publications interleave.
func TestOutOfRangeRewindDeliversTheNewLog(t *testing.T) {
	topic := newTopic("oor")
	c := cluster(t, 1, []string{topic})
	cl := openOn(t, c)
	cons := consumerFrom(t, cl, topic, "oor", mq.StartEarliest())
	publishBodies(t, cl, topic, numbered("a", 5)...)
	if got := receiveN(t, cons, 5, 10*time.Second, func(d *mq.Delivery) error { return d.Ack() }); got != 5 {
		t.Fatalf("received %d of the old log's 5 records", got)
	}

	recreate(t, c, topic)
	want := numbered("b", 3)
	publishBodies(t, cl, topic, want...)
	var got []string
	receiveN(t, cons, len(want), 15*time.Second, func(d *mq.Delivery) error {
		got = append(got, string(d.Body))
		return d.Ack()
	})
	if len(got) != len(want) {
		t.Fatalf("received %q from the recreated topic, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("received %q from the recreated topic, want %q", got, want)
		}
	}
}

// A delivery handed out from the old log must not be settleable once the
// partition has moved to the new one. Its offset is the old log's, and the
// new log reuses it: completing it would complete a record of the new log
// that nobody has processed, and the commit would then skip that record.
//
// a1, a3 and a4 are held across the move, so the old log's commit is 1, and
// b3 and b4 are held after it, at a3's and a4's offsets. The old deliveries
// must report ErrLeaseLost, and the commit must stop at b3.
func TestOutOfRangeRewindRefusesTheOldLogsDeliveries(t *testing.T) {
	topic := newTopic("oor")
	c := cluster(t, 1, []string{topic})
	cl, b := openPair(t, c)
	cons := consumerFrom(t, cl, topic, "oor-held", mq.StartEarliest())
	publishBodies(t, cl, topic, numbered("a", 7)...)
	old := receiveEach(t, cons, 7, 10*time.Second)
	if len(old) != 7 {
		t.Fatalf("received %d of the old log's 7 records", len(old))
	}
	var held []*mq.Delivery
	for i := range old {
		switch d := &old[i]; d.Info().Offset {
		case 1, 3, 4:
			held = append(held, d)
		default:
			if err := d.Ack(); err != nil {
				t.Fatalf("acknowledging %s: %v", d.Body, err)
			}
		}
	}

	recreate(t, c, topic)
	publishBodies(t, cl, topic, numbered("b", 5)...)
	fresh := receiveEach(t, cons, 5, 15*time.Second)
	if len(fresh) != 5 {
		t.Fatalf("received %d of the recreated topic's 5 records", len(fresh))
	}
	for i := range fresh {
		if d := &fresh[i]; d.Info().Offset < 3 {
			if err := d.Ack(); err != nil {
				t.Fatalf("acknowledging %s: %v", d.Body, err)
			}
		}
	}

	for _, d := range held {
		if err := d.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
			t.Errorf("acknowledging %s from the old log = %v, want ErrLeaseLost", d.Body, err)
		}
	}
	if err := b.Commit(t.Context()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	commit, ok := c.GroupInfo("oor-held").Commits[topic][0]
	if !ok || commit.Offset != 3 {
		t.Fatalf("the group's committed offset is %+v (present %v), want 3: b3 and b4 are unsettled", commit, ok)
	}
}
