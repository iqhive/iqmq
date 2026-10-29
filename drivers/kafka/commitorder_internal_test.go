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
	"github.com/iqhive/iqmq/driverkit/nack"
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
	if err := s.commit(t.Context(), older, olderParts, false); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := committedOffset(t, c, group, topic); got != 11 {
		t.Fatalf("the group's committed offset went back to %d after the revocation committed 11", got)
	}
}

// A commit whose boundary was read before the partition was lost must not be
// sent after the loss. The commit loop reads the boundary and then sends it,
// and the loss can land in between, most easily while the commit waits for
// another one to finish. The position is then the member's that owns the
// partition now, and franz-go stamps a commit with the membership it holds
// when it sends it, so a commit that went out after the member had rejoined
// was taken by the coordinator and moved the new owner's position back to
// where this member had been.
func TestACommitReadBeforeALossIsNotSent(t *testing.T) {
	const topic, group = "t", "g-commit-lost"
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
	s, _ := lostFixture(t, time.Second)
	s.cl = cl
	s.b.cfg.RequestTimeout = 5 * time.Second

	// The commit loop's first half: the boundary is 10, and not yet sent.
	older, olderParts := s.pendingCommits(nil)
	if len(older) != 1 {
		t.Fatalf("an owned partition with an uncommitted watermark: %d topics pending, want 1", len(older))
	}
	s.onLost(t.Context(), nil, map[string][]int32{topic: {0}})
	// The loop's second half, after the loss.
	if err := s.commit(t.Context(), older, olderParts, false); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if info := c.GroupInfo(group); info != nil {
		if commit, ok := info.Commits[topic][0]; ok {
			t.Fatalf("the commit loop committed %d for a partition this member had lost", commit.Offset)
		}
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

// A completion that lands after the revocation has read its boundary must be
// refused, even when the settlement it completes was resolved before that
// read. resolve answers for the delivery and lets go of the lock, the
// revocation reads the boundary it commits, and the completion comes after:
// no commit carries it, so the Ack that made it must not be answered nil.
func TestACompletionAfterTheLastBoundaryIsRefused(t *testing.T) {
	s, p := lostFixture(t, time.Second)
	d := &mq.DriverDelivery{Token: 0, Gen: p.tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}

	// Step (1) of the revocation: dispatch has stopped, and the wait still
	// accepts the partition's settlements.
	s.mu.Lock()
	p.stale, p.draining = true, true
	s.mu.Unlock()
	resolved, err := s.resolve(d)
	if err != nil {
		t.Fatalf("resolve during the revocation's wait = %v, want nil", err)
	}
	offsets, _ := s.lastBoundary(map[tp]bool{p.tp: true})
	if got := offsets[p.tp.topic][p.tp.partition].Offset; got != 10 {
		t.Fatalf("the boundary read before the completion is %d, want 10", got)
	}
	if err := s.complete(resolved, d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a completion after the boundary was read = %v, want ErrLeaseLost: the commit carries nothing of it", err)
	}
	if wm := p.tr.Watermark(); wm != 10 {
		t.Fatalf("the refused completion moved the watermark to %d, want 10", wm)
	}
}

// stallingStrategy is a strategy whose Plan waits to be released, which holds
// a nack between resolve and apply for as long as a test needs it held.
type stallingStrategy struct {
	nack.Strategy
	planning, release chan struct{}
}

func (s stallingStrategy) Plan(d nack.Delivery, o mq.NackOptions) (nack.Plan, error) {
	close(s.planning)
	<-s.release
	return s.Strategy.Plan(d, o)
}

// nackAfterTheRevocation nacks lostFixture's delivery at offset 10 under a
// strategy of kind, holds the nack between resolve and apply while a
// revocation runs from start to finish, and returns what the nack answered.
// The revocation's wait runs out, because the nack is still on its way.
func nackAfterTheRevocation(t *testing.T, kind nack.Kind) (*source, *partState, *kgo.Client, error) {
	t.Helper()
	s, p := lostFixture(t, 20*time.Millisecond)
	// A client with no group: the revocation's commit is answered at once
	// with "not a group", and the client's pauses are all the tests read.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	s.cl = cl
	strategy, err := nack.New(kind, nack.Config{})
	if err != nil {
		t.Fatal(err)
	}
	stall := stallingStrategy{Strategy: strategy, planning: make(chan struct{}), release: make(chan struct{})}
	s.strategy = stall
	d := &mq.DriverDelivery{
		Token: 0, Gen: p.tr.Gen(), Native: &kgo.Record{Topic: "t", Offset: 10},
		Info: mq.DeliveryInfo{Source: "t", Offset: 10, Attempt: 1},
	}

	nacked := make(chan error, 1)
	go func() { nacked <- (&settler{s: s}).Nack(t.Context(), d, mq.NackOptions{}) }()
	select {
	case <-stall.planning:
	case <-time.After(10 * time.Second):
		t.Fatal("the nack never got past resolve")
	}
	s.handleRevocation(t.Context(), map[string][]int32{"t": {0}}, true)
	close(stall.release)
	return s, p, cl, <-nacked
}

// A retry-in-place nack that resolve accepted and that applies only after the
// revocation has finished must be refused, and must leave the partition as the
// revocation left it. The revocation committed and resumed the partition, and
// the nack then held the record and paused the partition at the client.
// franz-go keeps that pause across a reassignment, so when the partition came
// back it was never fetched again, and the handler had been told nil for a
// record the partition's next owner was handling.
func TestARetryInPlaceNackAfterTheRevocationIsRefused(t *testing.T) {
	s, p, cl, err := nackAfterTheRevocation(t, nack.BlockPartition)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a retry-in-place nack applied after the revocation = %v, want ErrLeaseLost: the record is the next owner's", err)
	}
	if paused := cl.PauseFetchPartitions(nil)["t"]; len(paused) != 0 {
		t.Fatalf("the client has partitions %v paused after the revocation resumed them: a reassigned partition would never be fetched again", paused)
	}
	s.mu.Lock()
	held, paused := len(p.held), p.paused
	s.mu.Unlock()
	if held != 0 || paused {
		t.Fatalf("the refused nack left %d records held and paused = %t, want none held and not paused", held, paused)
	}
}

// A stop nack that applies only after the revocation has finished is refused
// the way resolve refuses one that arrives after it: the record is the next
// owner's, and the in-flight count it would release was the revocation's to
// clear.
func TestAStopNackAfterTheRevocationIsRefused(t *testing.T) {
	s, _, _, err := nackAfterTheRevocation(t, nack.Stop)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a stop nack applied after the revocation = %v, want ErrLeaseLost: the record is the next owner's", err)
	}
	if err := s.state(t.Context()); err != nil {
		t.Fatalf("the refused stop nack stopped the source: %v", err)
	}
}

// A nack whose dead-letter copy is still being produced when the revocation's
// wait runs out must not be answered nil once the copy is confirmed. resolve
// accepted the nack during the wait, the revocation read its boundary without
// the record and sent it, and the completion that follows the confirmation
// used to land while that commit was on the wire: the handler was told the
// record was settled, and it went to the partition's next owner as well. Ack
// has the same gap between resolve and complete, too narrow to hold open from
// outside (TestACompletionAfterTheLastBoundaryIsRefused steps through it); a
// republication holds it open for a whole produce.
func TestANackConfirmedAfterTheRevocationReadItsBoundaryIsRefused(t *testing.T) {
	const topic, dlq, group = "t", "t-dlq", "g-revoke-nack"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, dlq))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	s, p := lostFixture(t, 20*time.Millisecond)
	s.cl, s.b.cl = cl, producer
	s.b.cfg.RequestTimeout = 5 * time.Second
	if s.strategy, err = nack.New(nack.Auto, nack.Config{DeadLetterTopic: dlq}); err != nil {
		t.Fatal(err)
	}
	d := &mq.DriverDelivery{
		Token: 0, Gen: p.tr.Gen(), Native: &kgo.Record{Topic: topic, Offset: 10},
		Info: mq.DeliveryInfo{Source: topic, Offset: 10, Attempt: 1},
	}

	// The dead-letter copy is held at the broker, which keeps the nack
	// between resolve and complete. The revocation's commit is held too,
	// which is the window the completion lands in.
	producing, releaseProduce := make(chan struct{}), make(chan struct{})
	committing, releaseCommit := make(chan struct{}), make(chan struct{})
	var produceOnce, commitOnce sync.Once
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		produceOnce.Do(func() { close(producing) })
		c.SleepControl(func() { <-releaseProduce })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
	})
	c.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		commitOnce.Do(func() { close(committing) })
		c.SleepControl(func() { <-releaseCommit })
		return nil, nil, false //nolint:nilnil // as above
	})
	nacked := make(chan error, 1)
	go func() {
		nacked <- (&settler{s: s}).Nack(t.Context(), d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	}()
	select {
	case <-producing:
	case <-time.After(10 * time.Second):
		t.Fatal("the dead-letter copy never reached the broker")
	}
	revoked := make(chan struct{})
	go func() {
		defer close(revoked)
		s.handleRevocation(t.Context(), map[string][]int32{topic: {0}}, true)
	}()
	select {
	case <-committing:
	case <-time.After(10 * time.Second):
		t.Fatal("the revocation's commit never reached the broker")
	}
	close(releaseProduce)
	err = <-nacked
	close(releaseCommit)
	<-revoked
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Nack confirmed after the revocation read its boundary = %v, want ErrLeaseLost: the commit on the wire carries nothing of it", err)
	}
	if wm := p.tr.Watermark(); wm != 10 {
		t.Fatalf("the refused completion moved the watermark to %d, want 10", wm)
	}
}
