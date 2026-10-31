package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver whose Representable
// refuses any key under that prefix refuses iqmq's own provenance, the
// publication fails before any I/O, the runner requeues the message and counts
// a DeadLetterFailures, and the message is redelivered for ever.
//
// The five keys are named through the ROOT package's constants on purpose: the
// contract is the core's, and a test that spelled the strings itself would
// still pass on the day dlq.go renamed one.

// deadLetterCopy is the message the route publishes, built exactly as
// dlq.go's deadLetterMessage builds it: the original's key and body, plus the
// five provenance headers.
func deadLetterCopy() *mq.Message {
	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Set(mq.HeaderOriginTopic, []byte("orders"))
	m.Headers.Set(mq.HeaderOriginSubscription, []byte("billing"))
	m.Headers.Set(mq.HeaderAttempts, []byte("3"))
	m.Headers.Set(mq.HeaderFailure, []byte("schema mismatch"))
	m.Headers.Set(mq.HeaderFirstDeliveredAt, []byte("2026-09-18T10:11:12.5Z"))
	return m
}

// provenance is the five keys in the order dlq.go writes them.
var provenance = [...]string{
	mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts,
	mq.HeaderFailure, mq.HeaderFirstDeliveredAt,
}

// TestTheCoreDeadLetterRouteIsRepresentable is the mapping half: strict mode
// accepts the stamped copy, and the round trip hands every provenance header
// back to whoever reads the dead-letter topic.
func TestTheCoreDeadLetterRouteIsRepresentable(t *testing.T) {
	m := deadLetterCopy()
	if err := kq.Representable(m); err != nil {
		t.Fatalf("Representable(the dead-letter copy the specification publishes) = %v, want nil: "+
			"every one of the five provenance headers is the CORE's and not a caller's", err)
	}

	var rec kgo.Record
	_ = kq.Encode(&rec, m, "orders.dlq", nil)
	var got mq.Message
	kq.Decode(&got, &rec, "")
	for _, k := range provenance {
		v, ok := got.Headers.Get(k)
		if !ok {
			t.Errorf("the round trip dropped %q: an operator reading the dead-letter topic cannot see where the message came from", k)
			continue
		}
		want, _ := m.Headers.Get(k)
		if !bytes.Equal(v, want) {
			t.Errorf("%q came back as %q, want %q", k, v, want)
		}
	}
	if v, ok := got.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("the caller's own header came back as %q, %v", v, ok)
	}
}

// TestACallersReservedHeaderIsStillRefused is the other half, and it is what
// keeps the widening to exactly five keys: the rule about a CALLER's header
// under the reserved prefix is unchanged.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "key",
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "attempt",
		kq.HeaderID,
		kq.HeaderEnvelope,
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set(k, []byte("v"))
		if err := kq.Representable(m); err == nil {
			t.Errorf("Representable accepted the caller's reserved header %q", k)
		} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("Representable(%q) = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic is the end-to-end half:
// a handler that always fails, mq.WithDeadLetter, and the stamped copy read
// back off the dead-letter topic with its provenance intact.
//
// It asserts ConsumerStats.DeadLetterFailures is zero as well as reading the
// copy, because the defect's signature was a SILENT failure: the runner counts
// and requeues rather than returning the error, so a route that cannot publish
// looks from the caller's side like a message that is taking a while.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	const topic, dlqTopic, group = "dl-orders", "dl-orders-dlq", "dl-billing"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, dlqTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	addr := cluster.ListenAddrs()[0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kq.Open(ctx, kq.Config{Brokers: []string{addr}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = client.Close() }()

	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	if err := client.Publish(ctx, topic, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: topic, Subscription: group, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, _ *mq.Delivery) error {
			return errors.New("schema mismatch")
		}), mq.WithMaxAttempts(1), mq.WithDeadLetter(dlqTopic))
	}()

	// Read the dead-letter topic with a raw client: the copy has to be
	// readable by whoever operates the dead-letter topic, and that is not
	// necessarily an iqmq consumer.
	raw, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer raw.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 25*time.Second)
	defer fetchCancel()
	fetches := raw.PollRecords(fetchCtx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("polling %q: %v; the dead-letter publication never landed", dlqTopic, err)
	}
	recs := fetches.Records()
	if len(recs) == 0 {
		t.Fatal("nothing arrived on the dead-letter topic")
	}
	var got mq.Message
	kq.Decode(&got, recs[0], "")
	for _, k := range provenance {
		if _, ok := got.Headers.Get(k); !ok {
			t.Errorf("the dead-lettered copy carries no %q", k)
		}
	}
	if v, _ := got.Headers.Get(mq.HeaderFailure); string(v) != "schema mismatch" {
		t.Errorf("%s = %q, want %q", mq.HeaderFailure, v, "schema mismatch")
	}
	if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != topic {
		t.Errorf("%s = %q, want %q", mq.HeaderOriginTopic, v, topic)
	}
	if string(got.Body) != `{"id":42}` {
		t.Errorf("the copy's body is %q, want the original's", got.Body)
	}

	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	st := consumer.Stats()
	if st.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the route was refused and the message requeued", st.DeadLetterFailures)
	}
}

// TestFirstDeliveredAtSurvivesTheRetryChain: iqmq-first-delivered-at is when
// the message was first delivered, and a header holding a later attempt's
// receive time says something false (mq.HeaderFirstDeliveredAt). The move
// stamped the time this source first handed out the record's offset, so the
// retry topic's delivery, the message's second attempt, replaced the first
// hop's value with its own receive time, and the dead-letter copy said the
// message was first delivered when it was delivered the second time.
func TestFirstDeliveredAtSurvivesTheRetryChain(t *testing.T) {
	topic, dlq := newTopic("first"), newTopic("first-dead")
	retry1 := topic + ".retry.1"
	cl := cluster(t, 1, []string{topic, retry1, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) {
		cfg.Nack = nack.RetryTopic
		cfg.NackConfig = nack.Config{Retries: 1, DeadLetterTopic: dlq}
	})
	publish(t, c, topic, &mq.Message{Body: []byte("move-me")})
	d := receiveOne(t, consumerFrom(t, c, topic, "g-first", mq.StartEarliest()), 10*time.Second)
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack on the original topic: %v", err)
	}
	hop := receiveOne(t, consumerFrom(t, c, retry1, "g-first-retry", mq.StartEarliest()), 15*time.Second)
	first, ok := hop.Headers.Get(kq.HeaderFirstDeliveredAt)
	if !ok {
		t.Fatal("the retry copy carries no first-delivered-at")
	}
	// The retry copy is handed out again later, as the message's second
	// attempt, and nacked into the dead-letter topic.
	time.Sleep(200 * time.Millisecond)
	hop = receiveOne(t, consumerFrom(t, c, retry1, "g-first-retry-2", mq.StartEarliest()), 15*time.Second)
	if hop.Attempt != 2 {
		t.Fatalf("the retry copy is delivered as attempt %d, want 2", hop.Attempt)
	}
	if err := hop.Nack(); err != nil {
		t.Fatalf("Nack on the retry topic: %v", err)
	}
	dead := receiveOne(t, consumerFrom(t, c, dlq, "g-first-dead", mq.StartEarliest()), 15*time.Second)
	got, ok := dead.Headers.Get(kq.HeaderFirstDeliveredAt)
	if !ok || !bytes.Equal(got, first) {
		t.Fatalf("the dead-letter copy says first-delivered-at %q (present %v), want the first delivery's %q", got, ok, first)
	}
}

// TestFirstDeliveredAtIsOnlyAFirstDeliverysTime holds the move to the two
// halves of the header's rule, on records written with the headers a move
// leaves: a first-delivered-at the record carries is kept, and a delivery
// that is a later attempt stamps none of its own.
func TestFirstDeliveredAtIsOnlyAFirstDeliverysTime(t *testing.T) {
	const carried = "2001-02-03T04:05:06.789Z"
	for _, tc := range []struct {
		name    string
		headers []kgo.RecordHeader
		want    string // "" is no header at all
	}{
		{"carried", []kgo.RecordHeader{{Key: kq.HeaderFirstDeliveredAt, Value: []byte(carried)}}, carried},
		{"later attempt", []kgo.RecordHeader{{Key: kq.HeaderAttempts, Value: []byte("3")}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic, dlq := newTopic("first-rule"), newTopic("first-rule-dead")
			cl := cluster(t, 1, []string{topic, dlq})
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
			rec := &kgo.Record{Topic: topic, Value: []byte("move-me"), Headers: tc.headers}
			if err := b.Client().ProduceSync(t.Context(), rec).FirstErr(); err != nil {
				t.Fatalf("ProduceSync: %v", err)
			}
			d := receiveOne(t, consumerFrom(t, c, topic, "g-first-rule", mq.StartEarliest()), 10*time.Second)
			if err := d.Reject(); err != nil {
				t.Fatalf("Reject: %v", err)
			}
			dead := receiveOne(t, consumerFrom(t, c, dlq, "g-first-rule-dead", mq.StartEarliest()), 15*time.Second)
			got, ok := dead.Headers.Get(kq.HeaderFirstDeliveredAt)
			switch {
			case tc.want == "" && ok:
				t.Fatalf("the dead-letter copy of attempt %d says first-delivered-at %q: that is this attempt's receive time", d.Attempt, got)
			case tc.want != "" && string(got) != tc.want:
				t.Fatalf("the dead-letter copy says first-delivered-at %q (present %v), want the %q the record carried", got, ok, tc.want)
			}
		})
	}
}

// TestAnEnvelopedMoveKeepsItsProvenance: a move stamps the provenance on the
// copy's Kafka record headers, and the copy's value is still the envelope it
// was published in. A consumer of the dead-letter or retry topic that reads
// the envelope (MetadataPortableEnvelope, or MetadataAdaptive for a marked
// record) was handed the envelope's message, whose headers are the
// original's, and the stamped ones were dropped, so the operator reading the
// dead-letter topic could not see where the message came from or why it
// failed. driverkit.ReservedHeaderAllowed: a driver that carries a
// provenance header must also hand it back on the read path.
func TestAnEnvelopedMoveKeepsItsProvenance(t *testing.T) {
	for _, mode := range []mq.MetadataMode{mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			topic, dlq := newTopic("env-prov"), newTopic("env-prov-dead")
			cl := cluster(t, 1, []string{topic, dlq})
			c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			m := &mq.Message{Body: []byte("poison")}
			m.Headers.Set("app", []byte("1"))
			if err := p.Publish(t.Context(), m); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-env-prov", Start: mq.StartEarliest(), Metadata: mode})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			t.Cleanup(func() { _ = h.Close() })
			d := receiveOne(t, h, 10*time.Second)
			if err := d.Reject(); err != nil {
				t.Fatalf("Reject: %v", err)
			}
			dh, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: dlq, Subscription: "g-env-prov-dead", Start: mq.StartEarliest(), Metadata: mode})
			if err != nil {
				t.Fatalf("dead-letter Consumer: %v", err)
			}
			t.Cleanup(func() { _ = dh.Close() })
			dead := receiveOne(t, dh, 15*time.Second)
			if string(dead.Body) != "poison" {
				t.Fatalf("the dead-letter copy's body is %q, want the envelope's %q", dead.Body, "poison")
			}
			if v, ok := dead.Headers.Get("app"); !ok || string(v) != "1" {
				t.Errorf("the dead-letter copy's own header came back as %q (present %v), want %q", v, ok, "1")
			}
			want := map[string]string{
				kq.HeaderOriginTopic:        topic,
				kq.HeaderOriginSubscription: "g-env-prov",
				kq.HeaderOriginPartition:    "0",
				kq.HeaderOriginOffset:       "0",
			}
			for k, v := range want {
				if got, ok := dead.Headers.Get(k); !ok || string(got) != v {
					t.Errorf("the dead-letter copy read under %v says %s = %q (present %v), want %q", mode, k, got, ok, v)
				}
			}
			if _, ok := dead.Headers.Get(kq.HeaderFirstDeliveredAt); !ok {
				t.Errorf("the dead-letter copy read under %v carries no %s", mode, kq.HeaderFirstDeliveredAt)
			}
		})
	}
}

// TestAnEnvelopedMoveKeepsTheCarriedFirstDeliveredAt: a message that already
// carries iqmq-first-delivered-at, such as a dead-letter copy replayed with
// its headers, keeps it through a move, because it is when the message was
// first delivered and the move's own stamp is a later delivery's time. The
// native read path keeps it. Under an envelope the carried time is inside the
// envelope, the move stamps its own outside, and the read path replaced the
// carried time with the later one.
func TestAnEnvelopedMoveKeepsTheCarriedFirstDeliveredAt(t *testing.T) {
	const carried = "2020-01-01T00:00:00Z"
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			topic, dlq := newTopic("env-first"), newTopic("env-first-dead")
			cl := cluster(t, 1, []string{topic, dlq})
			c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
			publishMode := mode
			if mode == mq.MetadataAdaptive {
				publishMode = mq.MetadataPortableEnvelope
			}
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: publishMode})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			m := &mq.Message{Body: []byte("replayed")}
			m.Headers.Set(mq.HeaderFirstDeliveredAt, []byte(carried))
			if err := p.Publish(t.Context(), m); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-env-first", Start: mq.StartEarliest(), Metadata: mode})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			t.Cleanup(func() { _ = h.Close() })
			d := receiveOne(t, h, 10*time.Second)
			if err := d.Reject(); err != nil {
				t.Fatalf("Reject: %v", err)
			}
			dh, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: dlq, Subscription: "g-env-first-dead", Start: mq.StartEarliest(), Metadata: mode})
			if err != nil {
				t.Fatalf("dead-letter Consumer: %v", err)
			}
			t.Cleanup(func() { _ = dh.Close() })
			dead := receiveOne(t, dh, 15*time.Second)
			if vals := dead.Headers.Values(kq.HeaderFirstDeliveredAt); len(vals) != 1 || string(vals[0]) != carried {
				t.Errorf("the dead-letter copy read under %v carries %s %q, want only the carried %q", mode, kq.HeaderFirstDeliveredAt, vals, carried)
			}
			if got, _ := dead.Headers.Get(kq.HeaderOriginTopic); string(got) != topic {
				t.Errorf("the dead-letter copy read under %v says %s = %q, want %q: the rest of the move's provenance still wins", mode, kq.HeaderOriginTopic, got, topic)
			}
		})
	}
}
