package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
)

// A commit whose boundary was read before a revocation's commit must not land
// after it. The commit loop reads the boundary and then sends it, and the
// revocation can read a later boundary and send it in between: the loop's
// older offset then reached the group last, and the member that took the
// partition started below where this one had finished, so it redelivered the
// records between the two.
func TestAnOlderCommitDoesNotOverwriteTheRevocations(t *testing.T) {
	const topic, group = "t", "g-commit-order"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	s, p := lostFixture(t, time.Second)
	s.cl = cl
	s.b.cfg.RequestTimeout = 5 * time.Second

	// The commit loop's first half: the boundary is 10, and not yet sent.
	older, olderParts := s.pendingCommits(nil)
	// Offset 10 completes, and a revocation commits the boundary it makes.
	s.mu.Lock()
	p.tr.Complete(p.tr.Gen(), 10)
	p.inflight = 0
	s.mu.Unlock()
	s.handleRevocation(t.Context(), map[string][]int32{topic: {0}}, true)
	if got := committedOffset(t, c, group, topic); got != 11 {
		t.Fatalf("the revocation committed %d, want 11", got)
	}
	// The loop's second half, after the revocation's commit.
	if err := s.commit(t.Context(), older, olderParts); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := committedOffset(t, c, group, topic); got != 11 {
		t.Fatalf("the group's committed offset went back to %d after the revocation committed 11", got)
	}
}

// committedOffset is the group's committed offset for partition 0 of topic.
func committedOffset(t *testing.T, c *kfake.Cluster, group, topic string) int64 {
	t.Helper()
	commit, ok := c.GroupInfo(group).Commits[topic][0]
	if !ok {
		t.Fatalf("group %s has no commit for %s/0", group, topic)
	}
	return commit.Offset
}

// A settlement that arrives once the revocation's wait has run out must be
// refused. The wait ends at RevokeWait with the delivery still out, the
// boundary is read and committed without it, and the settlement used to be
// accepted until the generation was invalidated after that commit: the
// handler was told its record was done, and the commit that would have said
// so never came, so the record went to the next owner as well.
func TestASettlementAfterTheRevokeWaitIsRefused(t *testing.T) {
	const topic, group = "t", "g-revoke-wait"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	s, p := lostFixture(t, 20*time.Millisecond)
	s.cl = cl
	s.b.cfg.RequestTimeout = 5 * time.Second
	d := &mq.DriverDelivery{Token: 0, Gen: p.tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}

	// The revocation's commit is held at the broker, which is the window the
	// settlement lands in.
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		once.Do(func() { close(reached) })
		c.SleepControl(func() { <-release })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleRevocation(t.Context(), map[string][]int32{topic: {0}}, true)
	}()
	<-reached
	err = (&settler{s: s}).Ack(t.Context(), d)
	close(release)
	<-done
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack after the revocation's wait ran out = %v, want ErrLeaseLost: the commit already read carries nothing of it", err)
	}
}
