package kafkago

import (
	"errors"
	"strconv"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// TestABackgroundSuccessClearsOnlyItsOwnPath: a background failure is
// cleared by the next success of the path it happened on, and by nothing else:
// not by another path's success, which would hide a path that keeps failing,
// whichever of the two failed first, and not by a new generation, which clears
// its own subscription's generation failure only. The health names the latest
// failure of a path still failing.
func TestABackgroundSuccessClearsOnlyItsOwnPath(t *testing.T) {
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.opened()
	errA, errB := errors.New("refused A"), errors.New("refused B")
	var sinkA, sinkB, gen healthPath
	state := func(want mq.HealthState, wantErr error, after string) {
		t.Helper()
		s := b.health.Snapshot()
		if s.State != want || !errors.Is(s.Err, wantErr) {
			t.Fatalf("Health after %s = %v (%v), want %v (%v)", after, s.State, s.Err, want, wantErr)
		}
	}
	b.failed(&sinkA, errA)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "another path's success")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errB, "the earlier failing path's success while the later one still fails")
	b.recovered(&sinkB)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "the later failing path's success while the earlier one still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	b.generationDown(&gen)
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errDisconnected, "a background success while a generation is down")
	b.generationUp(t.Context(), &gen, false)
	state(mq.HealthReady, nil, "a new generation")

	b.failed(&sinkA, errA)
	b.generationDown(&gen)
	b.generationUp(t.Context(), &gen, false)
	state(mq.HealthDegraded, errA, "a new generation while another path still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")
}

// TestAQuietRoundClearsNoDecodeFailureThatLandedDuringIt: a reader's empty
// fetch round clears an undecodable record's failure when no record failed to
// decode while it waited and none is left to deliver. It read the failure
// count before it looked at the queue, so a record still queued when it read
// the count could be taken, fail to decode and be settled before it looked,
// and the round cleared that fresh failure at once. A record of another
// partition that failed after the round had looked, and before it cleared the
// path, was cleared the same way.
func TestAQuietRoundClearsNoDecodeFailureThatLandedDuringIt(t *testing.T) {
	errUndecodable := errors.New("CorruptMessage: the envelope will not decode")
	setup := func() (*Backend, *source) {
		b := &Backend{errs: newErrorMapper()}
		b.health = driverkit.NewHealth(b.onHealth)
		b.opened()
		return b, &source{b: b, parts: map[int]*partState{}, recs: newRecQueue(4)}
	}

	t.Run("while the round looked at the queue", func(t *testing.T) {
		b, s := setup()
		if !s.recs.tryPush(fetched{msg: &kgo.Message{Partition: 0, Offset: 7}}, false) {
			t.Fatal("push")
		}
		failures := s.decodeFailures.Load() // the reader's round begins
		// The queue's mutex is held, so the round waits for the queue,
		// and the consumer takes the record, which fails to decode, and
		// settles it in the meantime.
		s.recs.mu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.quietRound(failures)
		}()
		time.Sleep(100 * time.Millisecond)
		s.recs.ring[s.recs.head] = fetched{}
		s.recs.head = (s.recs.head + 1) % len(s.recs.ring)
		s.recs.count--
		s.decodeFailures.Add(1)
		b.failed(&s.decodePath, errUndecodable)
		s.recs.mu.Unlock()
		<-done
		if h := b.health.Snapshot(); h.State != mq.HealthDegraded {
			t.Errorf("Health after a record failed to decode while the quiet round looked at the queue = %v (%v), want Degraded", h.State, h.Err)
		}
	})

	t.Run("after the round looked at the queue", func(t *testing.T) {
		b, s := setup()
		s.decodeFailures.Add(1)
		b.failed(&s.decodePath, errors.New("an earlier undecodable record"))
		failures := s.decodeFailures.Load() // the reader's round begins
		// The health's mutex is held, so the round has looked at the
		// queue and the backlogs and waits to clear the path; another
		// partition's record fails to decode in the meantime.
		b.healthMu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.quietRound(failures)
		}()
		time.Sleep(100 * time.Millisecond)
		s.decodeFailures.Add(1)
		b.healthMu.Unlock()
		b.failed(&s.decodePath, errUndecodable)
		<-done
		if h := b.health.Snapshot(); h.State != mq.HealthDegraded || !errors.Is(h.Err, errUndecodable) {
			t.Errorf("Health after a record failed to decode while the quiet round cleared the path = %v (%v), want Degraded (%v)", h.State, h.Err, errUndecodable)
		}
	})
}

// TestEnsureSubscriptionKeepsAPathOnlyWhileTheSubscriptionFails:
// EnsureSubscription made a health path for every subscription name it
// ensured, clean runs and runs on a topic that does not exist included, and
// only DeleteSubscription forgot one, so a long-lived backend that ensures
// generated names grew the map for its whole life. A subscription's path is
// kept only while its start position is failing, and forgotten once the
// subscription works.
func TestEnsureSubscriptionKeepsAPathOnlyWhileTheSubscriptionFails(t *testing.T) {
	const topic = "subpaths"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := OpenBackend(t.Context(), Config{Brokers: c.ListenAddrs(), RequestTimeout: 3 * time.Second, WriteTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	adm, err := b.Admin()
	if err != nil {
		t.Fatal(err)
	}
	kept := func() int {
		b.healthMu.Lock()
		defer b.healthMu.Unlock()
		return len(b.subPaths)
	}
	const n = 200
	for i := range n {
		if err := adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "tenant-" + strconv.Itoa(i)}); err != nil {
			t.Fatalf("EnsureSubscription %d: %v", i, err)
		}
	}
	if err := adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "no-such-topic", Name: "tenant-missing"}); err != nil {
		t.Fatalf("EnsureSubscription on a topic that does not exist: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Fatalf("Health = %v (%v), want Ready: every run was clean", h.State, h.Err)
	}
	if k := kept(); k != 0 {
		t.Errorf("after %d clean EnsureSubscription calls with distinct names, %d subscription paths are kept, want 0", n+1, k)
	}

	// The next start-position commit is refused, and only that one.
	c.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.OffsetCommitRequest)
		resp := r.ResponseKind().(*kmsg.OffsetCommitResponse)
		resp.Version = r.Version
		for _, rt := range r.Topics {
			st := kmsg.NewOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownServerError.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	spec := mq.SubscriptionSpec{Topic: topic, Name: "tenant-refused"}
	if err := adm.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription with its commit refused: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthDegraded {
		t.Fatalf("Health after a refused start position = %v, want Degraded", h.State)
	}
	if k := kept(); k != 1 {
		t.Errorf("%d subscription paths are kept while one subscription fails, want 1", k)
	}
	if err := adm.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription again: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Errorf("Health after the subscription's next clean run = %v (%v), want Ready", h.State, h.Err)
	}
	if k := kept(); k != 0 {
		t.Errorf("%d subscription paths are kept once the failing subscription works, want 0", k)
	}
}
