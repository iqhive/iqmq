package kafkago_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// holdAssignment keeps the consumer group's SyncGroup inside the broker until
// release is called, so that whatever the test publishes before that lands
// before the group assigns the topic's partitions to the consumer. On a real
// cluster the join takes hundreds of milliseconds, and up to
// group.initial.rebalance.delay.ms; on kfake it takes a few, and the hold is
// what makes the order certain.
func holdAssignment(t *testing.T, cl *kfake.Cluster) (release func()) {
	t.Helper()
	rel := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(rel) }) }
	t.Cleanup(release)
	cl.ControlKey(int16(kmsg.SyncGroup), func(kmsg.Request) (kmsg.Response, error, bool) {
		cl.KeepControl()
		cl.SleepControl(func() { <-rel })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
	})
	return release
}

// publishTo publishes one message to one partition of topic.
func publishTo(t *testing.T, c *mq.Client, topic string, partition int, body string) {
	t.Helper()
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(body)}, mq.Partition(partition)); err != nil {
		t.Fatalf("publishing to %s/%d: %v", topic, partition, err)
	}
}

// receiveAfterOpen is the end of every test here: the first delivery is the
// record published after the consumer opened.
func receiveAfterOpen(t *testing.T, cons *mq.ConsumerHandle) {
	t.Helper()
	d, err := tryReceiveOne(t, cons, 10*time.Second)
	if err != nil {
		t.Fatalf("the record published after the consumer opened was never delivered (%v): its start position was read at the assignment, after the record", err)
	}
	if string(d.Body) != "after-open" {
		t.Errorf("first delivery = %q, want %q", d.Body, "after-open")
	}
	if err := d.Ack(); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

// TestARecordPublishedAfterTheOpenArrivesWhenTheStartTookAnotherRead is
// kf7-7. Opening a consumer reads its start position, "latest" as of the
// open, and the source keeps it for the partitions the group has no position
// for, so that a caller that opens a consumer and then publishes is owed what
// it published. A read that failed was not asked again: a partition the
// broker answered with an error was left out without a word, and a failed
// listing or a failed read of the group's positions left the start
// unresolved. The partition then started at the end of the log as of its
// ASSIGNMENT, after the record, which was never delivered. A failure asking
// again can cure is now asked again within the open's bound, whichever
// partition it is on, and a failed read of the group's positions no longer
// stops the start being read.
//
// The open reads the start once before the group's positions and again after,
// so a failure has to outlast two reads to need a retry: the faults below
// answer four requests.
func TestARecordPublishedAfterTheOpenArrivesWhenTheStartTookAnotherRead(t *testing.T) {
	const sub = "g-start-reread"
	listing := func(err *kerr.Error, count int, partitions ...int32) kfake.Fault {
		return kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Partitions: partitions, Err: err, Count: count}
	}
	for _, tc := range []struct {
		name       string
		partitions int32
		faults     []kfake.Fault
	}{
		{"nothing failing", 1, nil},
		{"a partition with no leader yet", 1, []kfake.Fault{listing(kerr.LeaderNotAvailable, 4)}},
		{"a new leader whose offsets are not known yet", 1, []kfake.Fault{listing(kerr.OffsetNotAvailable, 6)}},
		{"a listing that timed out", 1, []kfake.Fault{listing(kerr.RequestTimedOut, 4)}},
		{"an unexpected server error", 1, []kfake.Fault{listing(kerr.UnknownServerError, 4)}},
		{"a failed read of the group's positions", 1, []kfake.Fault{{Keys: []kmsg.Key{kmsg.OffsetFetch}, Group: sub, Err: kerr.UnknownServerError}}},
		// The partition published to is the last one; the other is refused
		// for good, and its refusal must not stop this one being asked
		// again.
		{"one partition refused for good beside one with no leader yet", 2, []kfake.Fault{listing(kerr.InvalidRequest, -1, 0), listing(kerr.LeaderNotAvailable, 4, 1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("start-reread")
			cl := cluster(t, tc.partitions, []string{topic})
			c := openOn(t, cl)
			to := int(tc.partitions) - 1
			publishTo(t, c, topic, to, "before")
			var faults []*kfake.FaultHandle
			for _, f := range tc.faults {
				if f.Group == "" {
					f.Topic = topic
				}
				faults = append(faults, cl.Fault(f))
			}
			release := holdAssignment(t, cl)
			cons := consumerOn(t, c, topic, sub)
			for _, f := range faults {
				if f.Hits() == 0 {
					t.Fatal("the open never met an injected failure, so the test proves nothing")
				}
				f.Remove()
			}
			// Opened, then published: the caller is owed this record.
			publishTo(t, c, topic, to, "after-open")
			release()
			receiveAfterOpen(t, cons)
		})
	}
}

// TestARecordPublishedAfterTheOpenArrivesWhileTheCoordinatorLoads: the read of
// the group's positions waits while the coordinator answers that it is still
// loading, until the open's bound ends. The start position was read only
// after it, on a context that had ended by then, so nothing was kept and the
// partition started at its assignment, which follows the coordinator, after
// the record. Under a context with no deadline the kafka-go driver used to
// wait for the coordinator however long it took; with the open bounded, the
// record was lost there too. The start is now read once before the group's
// positions.
func TestARecordPublishedAfterTheOpenArrivesWhileTheCoordinatorLoads(t *testing.T) {
	const sub = "g-start-coordinator"
	for _, tc := range []struct {
		name string
		// loads is when the coordinator finishes loading, counted from the
		// open; zero is as soon as the open has returned.
		loads time.Duration
	}{
		{"until the open's bound ends", 0},
		// The tests' Config.RequestTimeout is three seconds.
		{"past the bound, under a context with no deadline", 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("start-coordinator")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			publish(t, c, topic, &mq.Message{Body: []byte("before")})
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.OffsetFetch}, Group: sub, Err: kerr.CoordinatorLoadInProgress, Count: -1})
			began := time.Now()
			if tc.loads > 0 {
				loaded := time.AfterFunc(tc.loads, fault.Remove)
				defer loaded.Stop()
			}
			release := holdAssignment(t, cl)
			cons := consumerOn(t, c, topic, sub)
			if fault.Hits() == 0 {
				t.Fatal("the open never asked the coordinator, so the test proves nothing")
			}
			if tc.loads == 0 {
				fault.Remove()
			}
			publish(t, c, topic, &mq.Message{Body: []byte("after-open")})
			// The assignment can only follow the coordinator.
			time.Sleep(time.Until(began.Add(tc.loads)))
			release()
			receiveAfterOpen(t, cons)
		})
	}
}

// TestARecordPublishedAfterTheOpenArrivesWhenARequestToOneLeaderWasLost: the
// start read of a partition whose leader's connection drops is asked again.
// kafka-go splits an offset listing into a request per partition leader and
// reports one it lost while the others got through as UNKNOWN (-1) for that
// leader's partitions, which the driver did not ask again.
func TestARecordPublishedAfterTheOpenArrivesWhenARequestToOneLeaderWasLost(t *testing.T) {
	topic := newTopic("start-lost")
	cl := cluster(t, 2, []string{topic}, kfake.NumBrokers(2))
	lost := cl.LeaderFor(topic, 1)
	if cl.LeaderFor(topic, 0) == lost {
		if err := cl.MoveTopicPartition(topic, 1, (lost+1)%2); err != nil {
			t.Fatalf("moving partition 1 to another broker: %v", err)
		}
		lost = cl.LeaderFor(topic, 1)
	}
	c := openOn(t, cl)
	publishTo(t, c, topic, 1, "before")
	var armed atomic.Bool
	var dropped atomic.Int32
	armed.Store(true)
	cl.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
		cl.KeepControl()
		if !armed.Load() || cl.CurrentNode() != lost || dropped.Load() >= 4 {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
		}
		dropped.Add(1)
		return nil, errors.New("the connection dropped"), true
	})
	release := holdAssignment(t, cl)
	cons := consumerOn(t, c, topic, "g-start-lost")
	armed.Store(false)
	if dropped.Load() == 0 {
		t.Fatal("the open never sent a listing to the leader whose connection drops, so the test proves nothing")
	}
	publishTo(t, c, topic, 1, "after-open")
	release()
	receiveAfterOpen(t, cons)
}

// TestAStartPositionTheOpenCouldNotReadIsReported: a start position the open
// could not read is reported through health and the consumer still opens, as
// the driver's page says. A partition the broker answered with an error was
// left out without a word, so the health stayed Ready while the partition
// started at its assignment. A failure asking again can cure, an unexpected
// server error among them, is asked again only until the open's bound ends,
// and one that cannot is not asked again at all.
func TestAStartPositionTheOpenCouldNotReadIsReported(t *testing.T) {
	for _, tc := range []struct {
		name       string
		partitions int32
		fault      kfake.Fault
		retried    bool
	}{
		{"a partition the broker refuses for good", 1, kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Err: kerr.InvalidRequest, Count: -1}, false},
		{"an unexpected server error that does not clear", 1, kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Err: kerr.UnknownServerError, Count: -1}, true},
		{"one partition of two", 2, kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Partitions: []int32{1}, Err: kerr.UnknownServerError, Count: -1}, true},
		{"a partition that never has a leader", 1, kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Err: kerr.LeaderNotAvailable, Count: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("start-unread")
			cl := cluster(t, tc.partitions, []string{topic})
			// No commit round, so nothing but the open touches the
			// path the failure is reported on.
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.CommitInterval = -1 })
			f := tc.fault
			f.Topic = topic
			fault := cl.Fault(f)
			defer fault.Remove()
			transitions := b.DegradedTransitions()
			type open struct {
				h   *mq.ConsumerHandle
				err error
			}
			opened := make(chan open, 1)
			go func() {
				h, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "g-start-unread"})
				opened <- open{h, err}
			}()
			// The tests' Config.RequestTimeout is three seconds.
			var o open
			select {
			case o = <-opened:
			case <-time.After(20 * time.Second):
				t.Fatalf("Consumer(context.Background()) has not returned after 20s while the start position cannot be read (%d reads): the open is not bounded by Config.RequestTimeout", fault.Hits())
			}
			if o.err != nil {
				t.Fatalf("Consumer = %v, want the consumer to open with the failure in health", o.err)
			}
			defer func() { _ = o.h.Close() }()
			if fault.Hits() == 0 {
				t.Fatal("the open never met the injected failure, so the test proves nothing")
			}
			if b.DegradedTransitions() == transitions || b.Health().State != mq.HealthDegraded {
				t.Errorf("health after an open that could not read the start position = %v (%v), want Degraded", b.Health().State, b.Health().Err)
			}
			// Two reads, before and after the group's positions, ask
			// two requests each at most.
			switch {
			case tc.retried && fault.Hits() < 10:
				t.Errorf("the open read the start %d times, want it asked again until Config.RequestTimeout", fault.Hits())
			case !tc.retried && fault.Hits() > 4:
				t.Errorf("the open read the start %d times, want a refusal no retry can cure not asked again", fault.Hits())
			}
		})
	}
}
