package kafka_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestClosingAConsumerLeavesTheClientReady: every source's group client
// carries the backend's hooks, and closing it disconnects each of its
// connections while the backend stays open. The hook read every disconnect as
// a lost link and set the backend Degraded ("a broker connection was lost; the
// client is reconnecting"), and only a new connection set it back, so closing
// a consumer, an ordinary operation, left a healthy client reporting Degraded.
func TestClosingAConsumerLeavesTheClientReady(t *testing.T) {
	topic := newTopic("close-health")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-close-health"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("one")})
	d := receiveOne(t, h, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if s := b.Health(); s.State != mq.HealthReady {
		t.Fatalf("Health before the close = %v (%v), want Ready", s.State, s.Err)
	}
	degraded, reconnects := b.DegradedTransitions(), b.Reconnects()
	if err := h.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	publish(t, c, topic, &mq.Message{Body: []byte("two")})
	if s := b.Health(); s.State != mq.HealthReady {
		t.Errorf("Health after a consumer's Close = %v (%v), and the producer publishes: the group client's own disconnects were read as a lost link", s.State, s.Err)
	}
	if n := b.DegradedTransitions() - degraded; n != 0 {
		t.Errorf("a consumer's Close counted %d degraded transitions, want 0", n)
	}
	if n := b.Reconnects() - reconnects; n != 0 {
		t.Errorf("a consumer's Close counted %d reconnections, want 0", n)
	}
}

// refuseNextProduce answers the next Produce request that carries topic with
// INVALID_RECORD on every partition of topic, which franz-go does not retry.
// Requests for other topics are the cluster's own. A request names its topic,
// or from Produce v13 its topic id.
func refuseNextProduce(cl *kfake.Cluster, topic string) {
	id := cl.TopicInfo(topic).TopicID
	cl.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.ProduceRequest)
		if len(r.Topics) != 1 || r.Topics[0].Topic != topic && r.Topics[0].TopicID != id {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
		}
		resp := r.ResponseKind().(*kmsg.ProduceResponse)
		resp.Version = r.Version
		rt := r.Topics[0]
		st := kmsg.NewProduceResponseTopic()
		st.Topic, st.TopicID = rt.Topic, rt.TopicID
		for _, rp := range rt.Partitions {
			sp := kmsg.NewProduceResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.ErrorCode = kerr.InvalidRecord.Code
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
		return resp, nil, true
	})
}

// refusedInHealth publishes one ConfirmNone record on p that the broker
// refuses, and waits for the refusal to reach the health, which is the only
// place it can.
func refusedInHealth(t *testing.T, cl *kfake.Cluster, b *kq.Backend, p *mq.PublisherHandle, topic string) {
	t.Helper()
	refuseNextProduce(cl, topic)
	if err := p.Publish(t.Context(), &mq.Message{Body: []byte("refused")}); err != nil {
		t.Fatalf("ConfirmNone Publish: %v", err)
	}
	waitFor(t, 5*time.Second, "the refusal in health", func() bool { return b.Health().State == mq.HealthDegraded })
}

// TestHealthIsReadyAgainAfterABackgroundFailure: a background failure, one
// a call cannot return, sets Health Degraded, and only a new broker connection
// set it Ready again. A client whose connections stayed up never dialled
// again, so one refused ConfirmNone record left it Degraded while every later
// publication succeeded, and a readiness probe keyed on Health failed for
// good. The path that failed working again sets it Ready, with no
// reconnection counted.
func TestHealthIsReadyAgainAfterABackgroundFailure(t *testing.T) {
	t.Run("a refused ConfirmNone record", func(t *testing.T) {
		topic := newTopic("health-none")
		cl := cluster(t, 1, []string{topic})
		c, b := openPair(t, cl)
		pn, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		publish(t, c, topic, &mq.Message{Body: []byte("warm")})
		reconnects := b.Reconnects()
		refusedInHealth(t, cl, b, pn, topic)
		if err := pn.Publish(t.Context(), &mq.Message{Body: []byte("ok")}); err != nil {
			t.Fatalf("ConfirmNone Publish: %v", err)
		}
		if err := pn.Flush(t.Context()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		waitFor(t, 5*time.Second, "Ready after the publisher's next publication succeeded", func() bool { return b.Health().State == mq.HealthReady })
		if n := b.Reconnects() - reconnects; n != 0 {
			t.Errorf("the recovery counted %d reconnections, want 0", n)
		}
	})
	t.Run("a failed commit", func(t *testing.T) {
		topic := newTopic("health-commit")
		cl := cluster(t, 1, []string{topic})
		c, b := openPair(t, cl)
		cons := consumerFrom(t, c, topic, "g-health-commit", mq.StartEarliest())
		publish(t, c, topic, &mq.Message{Body: []byte("one")})
		commits := b.Commits()
		var refused atomic.Bool
		cl.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
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
			refused.Store(true)
			return resp, nil, true
		})
		d := receiveOne(t, cons, 10*time.Second)
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		waitFor(t, 5*time.Second, "the refused commit in health", func() bool { return refused.Load() && b.Health().State == mq.HealthDegraded })
		waitFor(t, 5*time.Second, "Ready after the next commit", func() bool { return b.Health().State == mq.HealthReady })
		if b.Commits() == commits {
			t.Error("no commit succeeded, so the test proves nothing")
		}
	})
}

// TestHealthStaysDegradedWhileTheFailingPathHasNotWorked: a success clears
// only its own path's failure. One publisher's refused record is not cleared
// by another publisher's publications, which would hide a publisher whose
// every record is refused behind one whose records are not; it is cleared by
// its own publisher's next success.
func TestHealthStaysDegradedWhileTheFailingPathHasNotWorked(t *testing.T) {
	bad, good := newTopic("health-bad"), newTopic("health-good")
	cl := cluster(t, 1, []string{bad, good})
	c, b := openPair(t, cl)
	pn, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: bad, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	publish(t, c, bad, &mq.Message{Body: []byte("warm")})
	refusedInHealth(t, cl, b, pn, bad)
	for range 5 {
		publish(t, c, good, &mq.Message{Body: []byte("ok")})
	}
	if s := b.Health(); s.State != mq.HealthDegraded {
		t.Fatalf("Health after another publisher's publications = %v, want Degraded: the refused publisher has not published since", s.State)
	}
	if err := pn.Publish(t.Context(), &mq.Message{Body: []byte("ok")}); err != nil {
		t.Fatalf("ConfirmNone Publish: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready after the refused publisher's own success", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthIsReadyOnceTheFailingPublisherCloses: a closed publisher does not
// publish again, so its failure must not keep the client Degraded.
func TestHealthIsReadyOnceTheFailingPublisherCloses(t *testing.T) {
	topic := newTopic("health-close")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	pn, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("warm")})
	refusedInHealth(t, cl, b, pn, topic)
	if err := pn.Close(); err != nil {
		t.Fatalf("closing the publisher: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready once the refused publisher closed", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthStaysDegradedWhileAPathKeepsFailing: the health named the last
// path that failed, so a consumer whose every commit was refused read Ready
// whenever a publisher on the same backend had one record refused and the
// next accepted: the publisher's failure took the slot, and its recovery
// cleared it, with the consumer still failing. A readiness probe read a
// third of its samples Ready. Every failing path is kept until it recovers.
func TestHealthStaysDegradedWhileAPathKeepsFailing(t *testing.T) {
	topic, other := newTopic("health-mask"), newTopic("health-mask-pub")
	cl := cluster(t, 1, []string{topic, other})
	c, b := openPair(t, cl)
	cons := consumerFrom(t, c, topic, "g-health-mask", mq.StartEarliest())
	publish(t, c, topic, &mq.Message{Body: []byte("one")})
	var refusedCommits atomic.Int64
	cl.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cl.KeepControl()
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
		refusedCommits.Add(1)
		return resp, nil, true
	})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, 5*time.Second, "the refused commit in health", func() bool {
		return refusedCommits.Load() > 0 && b.Health().State == mq.HealthDegraded
	})
	pn, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: other, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	publish(t, c, other, &mq.Message{Body: []byte("warm")})

	var samples, ready int
	commitsBefore := refusedCommits.Load()
	for stop := time.Now().Add(3 * time.Second); time.Now().Before(stop); {
		refuseNextProduce(cl, other)
		for _, body := range []string{"refused", "ok"} {
			if err := pn.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
				t.Fatalf("ConfirmNone Publish: %v", err)
			}
			if err := pn.Flush(t.Context()); err != nil {
				t.Fatalf("Flush: %v", err)
			}
		}
		for range 10 {
			samples++
			if b.Health().State == mq.HealthReady {
				ready++
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if refusedCommits.Load() == commitsBefore {
		t.Fatal("no commit was refused while the publisher ran, so the test proves nothing")
	}
	if ready > 0 {
		t.Errorf("Health read Ready in %d of %d samples while every commit of the consumer was refused: the publisher's recovery cleared the consumer's failure", ready, samples)
	}
}

// TestHealthIsReadyAgainAfterAPublishersBatchSucceeds: a refused ConfirmNone
// record is Degraded until its publisher's next successful publication, and a
// PublishBatch that succeeds is one. On the kafka-go driver it cleared nothing.
func TestHealthIsReadyAgainAfterAPublishersBatchSucceeds(t *testing.T) {
	topic := newTopic("health-batch")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	pn, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("warm")})
	refusedInHealth(t, cl, b, pn, topic)
	if err := pn.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready after the refused publisher's batch succeeded", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthIsReadyOnceTheSubscriptionWorksAfterASoftEnsureFailure: an
// EnsureSubscription whose start-position commit is refused returns nil and
// reports the refusal to health. Only a later clean EnsureSubscription
// cleared it, and a deployment usually calls it once, so the health stayed
// Degraded for good while the subscription's consumer received, acked and
// committed. A commit by a consumer of the subscription clears it, as does a
// clean start position when one opens.
func TestHealthIsReadyOnceTheSubscriptionWorksAfterASoftEnsureFailure(t *testing.T) {
	topic := newTopic("health-ensure")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	publish(t, c, topic, &mq.Message{Body: []byte("one")})
	var refused atomic.Bool
	cl.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
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
		refused.Store(true)
		return resp, nil, true
	})
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "g-health-ensure"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	waitFor(t, 5*time.Second, "the refused start position in health", func() bool {
		return refused.Load() && b.Health().State == mq.HealthDegraded
	})
	cons := consumerFrom(t, c, topic, "g-health-ensure", mq.StartEarliest())
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready once the subscription's consumer has worked", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthIsReadyAfterAClosedPublishersLateRefusal: Close does not flush,
// so a ConfirmNone record buffered before its publisher closed can be
// refused after, once Close has cleared the publisher's path. The refusal set
// the path failing again, and a closed publisher never publishes to clear it.
func TestHealthIsReadyAfterAClosedPublishersLateRefusal(t *testing.T) {
	topic, other := newTopic("health-closed"), newTopic("health-closed-other")
	cl := cluster(t, 1, []string{topic, other})
	_, b := openPair(t, cl, func(cfg *kq.Config) { cfg.Linger = 300 * time.Millisecond })
	snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	var degraded atomic.Bool
	refuseNextProduce(cl, topic)
	if err := snk.Publish(t.Context(), &mq.Message{Body: []byte("refused after close")}, nil); err != nil {
		t.Fatalf("ConfirmNone Publish: %v", err)
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	transitions := b.DegradedTransitions()
	waitFor(t, 5*time.Second, "the late refusal to reach the health", func() bool {
		if b.Health().State == mq.HealthDegraded {
			degraded.Store(true)
		}
		return degraded.Load() || b.DegradedTransitions() > transitions
	})
	s2, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: other})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := s2.Publish(t.Context(), &mq.Message{Body: []byte("ok")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready after the closed publisher's late refusal", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthIsReadyAfterAFetchFailureOnAQuietTopic: a fetch failure was
// cleared only by a poll that brought records, so a consumer of a topic that
// went quiet stayed Degraded for as long as nothing was published, hours on a
// quiet topic. An error-free fetch round clears it, an empty one included.
func TestHealthIsReadyAfterAFetchFailureOnAQuietTopic(t *testing.T) {
	topic := newTopic("health-quiet")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	id := cl.TopicInfo(topic).TopicID
	var failed atomic.Bool
	cl.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.FetchRequest)
		var fails []kmsg.FetchRequestTopic
		for _, rt := range r.Topics {
			if rt.Topic == topic || rt.TopicID == id {
				fails = append(fails, rt)
			}
		}
		if len(fails) == 0 || failed.Load() {
			cl.KeepControl()
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
		}
		failed.Store(true)
		resp := r.ResponseKind().(*kmsg.FetchResponse)
		resp.Version = r.Version
		resp.SessionID = r.SessionID
		for _, rt := range fails {
			st := kmsg.NewFetchResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownServerError.Code
				sp.HighWatermark, sp.LastStableOffset, sp.LogStartOffset = -1, -1, -1
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	transitions := b.DegradedTransitions()
	cons := consumerFrom(t, c, topic, "g-health-quiet", mq.StartEarliest())
	degraded := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !degraded {
		_, _ = tryReceiveOne(t, cons, 200*time.Millisecond)
		degraded = failed.Load() && b.DegradedTransitions() > transitions
	}
	if !degraded {
		t.Fatalf("the injected fetch failure never reached the health (injected %v, health %v)", failed.Load(), b.Health().State)
	}
	// Nothing is published from here on.
	for range 5 {
		_, _ = tryReceiveOne(t, cons, time.Second)
		if b.Health().State == mq.HealthReady {
			return
		}
	}
	t.Errorf("Health after a fetch failure and five empty one-second receives on a quiet topic = %v (%v), want Ready", b.Health().State, b.Health().Err)
}

// TestHealthIsReadyOnceASoftFailedSubscriptionIsDeleted: a start position
// EnsureSubscription could not commit is cleared when the subscription
// works again, and a deleted subscription never does, so the health stayed
// Degraded for good while everything else worked.
func TestHealthIsReadyOnceASoftFailedSubscriptionIsDeleted(t *testing.T) {
	topic, other := newTopic("health-delsub"), newTopic("health-delsub-other")
	cl := cluster(t, 1, []string{topic, other})
	c, b := openPair(t, cl)
	publish(t, c, topic, &mq.Message{Body: []byte("one")})
	var refused atomic.Bool
	cl.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
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
		refused.Store(true)
		return resp, nil, true
	})
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "g-health-delsub"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	waitFor(t, 5*time.Second, "the refused start position in health", func() bool {
		return refused.Load() && b.Health().State == mq.HealthDegraded
	})
	if err := admin.DeleteSubscription(t.Context(), topic, "g-health-delsub"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	cons := consumerFrom(t, c, other, "g-health-delsub-other", mq.StartEarliest())
	publish(t, c, other, &mq.Message{Body: []byte("two")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, 5*time.Second, "Ready once the failed subscription was deleted", func() bool { return b.Health().State == mq.HealthReady })
}

// TestHealthStaysDegradedWhileEveryRecordIsUndecodable: a consumer whose
// every record carries the envelope marker and does not decode keeps failing,
// so the health reads Degraded throughout. A fetch round cleared the failure
// before the records it had fetched were delivered, so the health read Ready
// for a moment between a round and the next undecodable delivery.
func TestHealthStaysDegradedWhileEveryRecordIsUndecodable(t *testing.T) {
	topic := newTopic("health-undecodable")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-health-undecodable", Metadata: mq.MetadataAdaptive, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		for ctx.Err() == nil {
			_ = produceUndecodable(ctx, b, topic)
			time.Sleep(50 * time.Millisecond)
		}
	}()
	var samples, ready atomic.Int64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		for b.Health().State != mq.HealthDegraded {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			samples.Add(1)
			if b.Health().State == mq.HealthReady {
				ready.Add(1)
			}
		}
	}()
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); {
		if d, err := tryReceiveOne(t, cons, 100*time.Millisecond); err == nil && d != nil {
			_ = d.Ack()
		}
	}
	close(stop)
	<-sampled
	cancel()
	<-prodDone
	if samples.Load() == 0 {
		t.Fatal("the undecodable records never reached the health, so the test proves nothing")
	}
	if n := ready.Load(); n > 0 {
		t.Errorf("Health read Ready in %d of %d samples while every record the consumer received was undecodable", n, samples.Load())
	}
}

// TestHealthIsReadyAfterAnUndecodableRecordOnAQuietTopic: the last record of
// a topic does not decode and nothing follows it. The failure clears on the
// consumer's next error-free fetch round once nothing fetched is waiting to
// be delivered; it waited for a decodable record that never came.
func TestHealthIsReadyAfterAnUndecodableRecordOnAQuietTopic(t *testing.T) {
	topic := newTopic("health-quietdecode")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-health-quietdecode", Metadata: mq.MetadataAdaptive, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	if err := produceUndecodable(t.Context(), b, topic); err != nil {
		t.Fatalf("producing: %v", err)
	}
	transitions := b.DegradedTransitions()
	d := receiveOne(t, cons, 10*time.Second)
	_ = d.Ack()
	if b.DegradedTransitions() == transitions {
		t.Fatal("the undecodable record never reached the health, so the test proves nothing")
	}
	for range 5 {
		_, _ = tryReceiveOne(t, cons, time.Second)
		if b.Health().State == mq.HealthReady {
			return
		}
	}
	t.Errorf("Health after the quiet topic's last, undecodable record and five empty one-second receives = %v (%v), want Ready", b.Health().State, b.Health().Err)
}

// produceUndecodable publishes a record that carries the envelope marker and
// is not an envelope.
func produceUndecodable(ctx context.Context, b *kq.Backend, topic string) error {
	return b.Client().ProduceSync(ctx, &kgo.Record{
		Topic: topic, Value: []byte("not an envelope"),
		Headers: []kgo.RecordHeader{{Key: kq.HeaderEnvelope, Value: []byte(kq.EnvelopeMarkerValue)}},
	}).FirstErr()
}
