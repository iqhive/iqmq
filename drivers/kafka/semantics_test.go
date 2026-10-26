package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestConfirmationBoundaries: both confirmation modes mean the same thing on
// Kafka — acks=all, every in-sync replica has the record — so neither is
// refused and neither costs an extra round trip. A caller asking for
// ConfirmDurable on a broker that can define one must not be refused.
func TestConfirmationBoundaries(t *testing.T) {
	for _, mode := range []mq.ConfirmationMode{mq.ConfirmDefault, mq.ConfirmNone, mq.ConfirmAccepted, mq.ConfirmDurable} {
		t.Run(mode.String(), func(t *testing.T) {
			c, topic := openClient(t)
			cons := consumerOn(t, c, topic, "g-"+mode.String())
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mode})
			if err != nil {
				t.Fatalf("opening a %v publisher: %v", mode, err)
			}
			defer func() { _ = p.Close() }()
			if err := p.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if mode == mq.ConfirmNone {
				// The record is in a buffer, not at the broker: the
				// flush is what a caller waits with.
				if err := p.Flush(t.Context()); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
			d := receiveOne(t, cons, 10*time.Second)
			if string(d.Body) != "x" {
				t.Fatalf("body %q", d.Body)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestReceiptCarriesTheCoordinates: Kafka's produce response gives the
// partition and the offset, which is what the receipt is for. Kafka assigns
// no message identifier, so an empty MessageID is "the broker assigns none"
// rather than a claim.
func TestReceiptCarriesTheCoordinates(t *testing.T) {
	c, topic := openClient(t)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
	if err != nil {
		t.Fatalf("opening a publisher with receipts: %v", err)
	}
	defer func() { _ = p.Close() }()
	var receipt mq.PublishReceipt
	m := &mq.Message{Body: []byte("r"), Receipt: &receipt}
	if err := p.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.Offset != 0 || receipt.Partition != 0 {
		t.Fatalf("receipt = %+v; the first record of a one-partition topic is 0/0", receipt)
	}
	if receipt.MessageID != "" {
		t.Fatalf("receipt names a message id %q; Kafka assigns none", receipt.MessageID)
	}
	if receipt.Confirmation != mq.ConfirmAccepted {
		t.Fatalf("receipt confirmation = %v", receipt.Confirmation)
	}
	// The second record of the same partition is at the next offset, which
	// is what makes the receipt useful.
	receipt = mq.PublishReceipt{}
	if err := p.Publish(t.Context(), &mq.Message{Body: []byte("r2"), Receipt: &receipt}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.Offset != 1 {
		t.Fatalf("the second record is at offset %d", receipt.Offset)
	}
}

// TestPublishRefusesWhatKafkaCannotDo is "enforced before I/O": a control
// the broker has no way to honour is refused before a byte is written, in
// every metadata mode, because none of them is metadata an envelope could
// carry.
//
// It goes through the portable API, so what it proves is the BEHAVIOUR a
// caller sees — and the core refuses each of these against the capability
// table before the driver is reached, which means a mutation that removed the
// driver's own check would leave this test green.
// TestSinkRefusesWhatKafkaCannotDo is the other half, through the SPI.
func TestPublishRefusesWhatKafkaCannotDo(t *testing.T) {
	for _, meta := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataAdaptive, mq.MetadataLenientDrop, mq.MetadataPortableEnvelope} {
		t.Run(meta.String(), func(t *testing.T) {
			c, topic := openClient(t)
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: meta})
			if err != nil {
				t.Fatalf("opening a publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			for name, opt := range map[string]mq.PublishOption{
				"delay":    mq.Delay(time.Minute),
				"ttl":      mq.TTL(time.Minute),
				"priority": mq.Priority(3),
				"dedup":    mq.DedupKey("d-1"),
			} {
				err := p.Publish(t.Context(), &mq.Message{Body: []byte("x")}, opt)
				if !errors.Is(err, mq.ErrUnsupported) {
					t.Errorf("%s was accepted under %v: %v", name, meta, err)
				}
			}
		})
	}
}

// TestSinkRefusesWhatKafkaCannotDo is the driver's OWN half, reached
// through the SPI rather than through the portable API.
//
// It exists because a mutation proved the portable test could not fail: the
// core validates a per-call control and a payload size against the capability
// table before it calls a sink, so the driver's checks are never reached from
// above. They still have to be there — the SPI is a public surface and the specification says
// a sink refuses what it cannot honour — and this is what holds them to it.
//
// The size limit gets a cluster of its own, and the reason is the only way to
// make the check FAILABLE. The specification says a limit is "enforced before I/O", and an
// error alone cannot tell a driver that checked from a driver that asked: a
// broker refuses an oversized record with MESSAGE_TOO_LARGE and franz-go
// refuses one past its own ProducerBatchMaxBytes locally, and this driver maps
// both to ErrMessageTooLarge. So the cluster is given a message.max.bytes
// SMALLER than franz-go's own default bound — which is the configuration a
// real cluster with a tightened limit has — and the check is that ZERO produce
// requests were sent. Without the driver's check, franz-go sends the record
// happily and the broker refuses it, which is one request.
func TestSinkRefusesWhatKafkaCannotDo(t *testing.T) {
	topic := newTopic("spi")
	cl := cluster(t, 1, []string{topic}, kfake.BrokerConfigs(map[string]string{
		"message.max.bytes": "4096",
	}))
	var produces atomic.Int64
	cl.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		produces.Add(1)
		cl.KeepControl()
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
	})
	b := backendOn(t, cl)
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	for name, o := range map[string]*mq.PublishOptions{
		"delay":      {Delay: time.Minute},
		"deliver at": {DeliverAt: time.Now().Add(time.Minute)},
		"ttl":        {TTL: time.Minute},
		"priority":   {Priority: 3, PrioritySet: true},
		"dedup key":  {DedupKey: "d-1"},
	} {
		err := sink.Publish(t.Context(), &mq.Message{Body: []byte("x")}, o)
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("the sink accepted %s: %v", name, err)
		}
	}
	// A nil message is a programming error the sink names rather than
	// dereferences.
	if err := sink.Publish(t.Context(), nil, nil); err == nil {
		t.Error("the sink accepted a nil message")
	}
	// And the size limit, which the specification says is enforced before any I/O.
	lim := b.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Fatal("the driver did not read the cluster's message.max.bytes, so the limit it advertises is unknown and nothing below can be checked")
	}
	if lim.Value != 4096 {
		t.Fatalf("the advertised limit is %d, want the cluster's own 4096", lim.Value)
	}
	big := &mq.Message{Body: make([]byte, lim.Value+1)}
	if err := sink.Publish(t.Context(), big, nil); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Errorf("the sink accepted a payload past the advertised limit: %v", err)
	}
	if n := produces.Load(); n != 0 {
		t.Errorf("%d produce request(s) were sent: a refusal the specification calls \"before I/O\" must not reach the broker", n)
	}
}

// TestBatchReceiveFillsSeveralSlots is what FeatureBatchReceive claims, and it
// is the property a mutation showed nothing else checked: a driver that
// drained one of franz-go's buffered fetch responses per Fetch would fill one
// slot of a caller's batch however many records were waiting, and every
// throughput test would still pass because they loop.
func TestBatchReceiveFillsSeveralSlots(t *testing.T) {
	c, topic := openClient(t)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-batchrecv", Prefetch: 32})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	const n = 16
	for i := range n {
		publish(t, c, topic, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	// SOME Receive fills more than one of sixteen slots -- not necessarily
	// the first.
	//
	// The first call cannot be the subject on its own: the consumer has to
	// exist before the publications (this driver starts at the LATEST offset,
	// so one opened afterwards sees nothing), and a Kafka fetch returns as
	// soon as one byte is available -- so franz-go can already hold a response
	// carrying record 1 alone, and the drain correctly takes exactly that.
	// Asserting on the first call made this case a race that failed every time
	// at GOMAXPROCS=2 and on a 2-core CI runner.
	//
	// The loop does not weaken what is under test, which is the defect a
	// mutation found nothing else checked: a driver serving one buffered fetch
	// RESPONSE per call fills exactly one slot EVERY time, so it never
	// satisfies this however many calls it gets, and the deadline is what says
	// so. Watched failing that way.
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, n)
	best, calls, seen := 0, 0, 0
	var rerr error
	for seen < n {
		var got int
		got, rerr = cons.Receive(ctx, dst)
		calls++
		for i := range got {
			if err := dst[i].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		seen += got
		best = max(best, got)
		if best >= 2 {
			break
		}
		if rerr != nil {
			break
		}
	}
	if best < 2 {
		t.Fatalf("%d Receive call(s) took %d of %d record(s) and the fullest filled %d slot(s) (err %v): a driver "+
			"advertising FeatureBatchReceive fills more than one slot when more than one record is waiting",
			calls, seen, n, best, rerr)
	}
}

// TestPublishToAPartition: a caller that names a partition gets it, which is
// FeatureOrderedByPartition's own escape hatch.
func TestPublishToAPartition(t *testing.T) {
	topic := newTopic("parts")
	cl := cluster(t, 4, []string{topic})
	c := openOn(t, cl)
	cons := consumerOn(t, c, topic, "g-parts")
	for p := range int32(4) {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("p" + strconv.Itoa(int(p)))}, mq.Partition(int(p))); err != nil {
			t.Fatalf("publishing to partition %d: %v", p, err)
		}
	}
	seen := map[int32]string{}
	receiveN(t, cons, 4, 15*time.Second, func(d *mq.Delivery) error {
		seen[d.Info().Partition] = string(d.Body)
		return d.Ack()
	})
	for p := range int32(4) {
		if seen[p] != "p"+strconv.Itoa(int(p)) {
			t.Fatalf("partition %d carried %q, want %q", p, seen[p], "p"+strconv.Itoa(int(p)))
		}
	}
}

// TestOrderedByKey is through the murmur2 partitioner: one key's records
// share a partition and a partition is delivered in order.
func TestOrderedByKey(t *testing.T) {
	topic := newTopic("ordered")
	cl := cluster(t, 8, []string{topic})
	c := openOn(t, cl)
	cons := consumerOn(t, c, topic, "g-ordered")
	const n = 40
	for i := range n {
		publish(t, c, topic, &mq.Message{Key: []byte("one-key"), Body: []byte(strconv.Itoa(i))})
	}
	got := make([]int, 0, n)
	part := int32(-1)
	receiveN(t, cons, n, 20*time.Second, func(d *mq.Delivery) error {
		if part < 0 {
			part = d.Info().Partition
		} else if d.Info().Partition != part {
			t.Errorf("one key's records landed on partitions %d and %d", part, d.Info().Partition)
		}
		v, _ := strconv.Atoi(string(d.Body))
		got = append(got, v)
		return d.Ack()
	})
	if len(got) != n {
		t.Fatalf("%d of %d records arrived", len(got), n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("record %d arrived at position %d: a partition is delivered in order", v, i)
		}
	}
}

// TestBatchPublishIsIndexAligned is the whole contract of a batch publication:
// a result aligned with anything but the input reports one message's outcome
// for another, and that is how a batch publish loses a message silently.
func TestBatchPublishIsIndexAligned(t *testing.T) {
	topic := newTopic("batch")
	cl := cluster(t, 4, []string{topic})
	c := openOn(t, cl)
	cons := consumerOn(t, c, topic, "g-batch")
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	const n = 50
	ms := make([]*mq.Message, n)
	for i := range ms {
		// Different keys, so the batch spans partitions and the promises
		// fire in whatever order the partitions resolve — which is what
		// makes an index-aligned result something to get wrong.
		ms[i] = &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))}
	}
	if err := p.PublishBatch(t.Context(), ms); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	receiveN(t, cons, n, 20*time.Second, func(d *mq.Delivery) error {
		seen[string(d.Body)] = true
		return d.Ack()
	})
	for i := range n {
		if !seen[strconv.Itoa(i)] {
			t.Fatalf("message %d of the batch never arrived", i)
		}
	}
}

// TestAsyncPublishRunsItsCallbackOnce is the contract: a nil return means
// done runs exactly once, and there is no goroutine per message.
func TestAsyncPublishRunsItsCallbackOnce(t *testing.T) {
	c, topic := openClient(t)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	const n = 25
	var (
		mu    sync.Mutex
		calls = map[string]int{}
		wg    sync.WaitGroup
	)
	wg.Add(n)
	for i := range n {
		body := strconv.Itoa(i)
		err := p.PublishAsync(t.Context(), &mq.Message{Body: []byte(body)}, func(res mq.PublishResult) {
			mu.Lock()
			calls[body]++
			mu.Unlock()
			if res.Err != nil {
				t.Errorf("publication %s: %v", body, res.Err)
			}
			wg.Done()
		})
		if err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for i := range n {
		if got := calls[strconv.Itoa(i)]; got != 1 {
			t.Fatalf("publication %d's callback ran %d times", i, got)
		}
	}
}

// TestOwnedPublishReleasesExactlyOnce is ownership transfers at
// invocation, so the release is the sink's obligation on every path.
func TestOwnedPublishReleasesExactlyOnce(t *testing.T) {
	c, topic := openClient(t)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	cons := consumerOn(t, c, topic, "g-owned")
	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		m := &mq.Message{Body: []byte("owned" + strconv.Itoa(i))}
		om := mq.OwnedMessage{Message: m, Release: func(*mq.Message) { wg.Done() }}
		if err := p.PublishOwned(t.Context(), om); err != nil {
			t.Fatalf("PublishOwned: %v", err)
		}
	}
	wg.Wait()
	if err := p.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := receiveN(t, cons, n, 20*time.Second, func(d *mq.Delivery) error { return d.Ack() })
	if got != n {
		t.Fatalf("%d of %d owned publications arrived", got, n)
	}
}

// TestEnvelopeRoundTrip: MetadataPortableEnvelope is the one mode that changes
// the bytes on the wire, and what it changes has to come back.
func TestEnvelopeRoundTrip(t *testing.T) {
	c, topic := openClient(t)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-env", Metadata: mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("opening an enveloping consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("opening an enveloping publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	m := &mq.Message{Key: []byte("k"), Body: []byte{0, 1, 0xff}, ID: "id-1"}
	m.Headers.Add("h", []byte{0x80})
	m.Headers.Add("h", []byte("second"))
	if err := p.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Key) != "k" || !bytes.Equal(d.Body, []byte{0, 1, 0xff}) || d.ID != "id-1" {
		t.Fatalf("delivery key %q body %v id %q", d.Key, d.Body, d.ID)
	}
	if vals := d.Headers.Values("h"); len(vals) != 2 || string(vals[1]) != "second" {
		t.Fatalf("headers came back as %q", vals)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestStartPositions covers the four positions, including the two that need
// clamping on an empty topic: a sequence beyond the log and a time no record
// is at or after.
func TestStartPositions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start mq.StartPosition
	}{
		{"latest", mq.StartLatest()},
		{"earliest", mq.StartEarliest()},
		{"time in the past", mq.StartAt(time.Now().Add(-time.Hour))},
		{"sequence beyond the log", mq.StartAtSequence(1)},
		{"sequence zero", mq.StartAtSequence(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, topic := openClient(t)
			cons := consumerFrom(t, c, topic, "g-start", tc.start)
			publish(t, c, topic, &mq.Message{Body: []byte("after")})
			d := receiveOne(t, cons, 10*time.Second)
			if string(d.Body) != "after" {
				t.Fatalf("body %q", d.Body)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestDurableSubscription is what FeatureDurableSubscriptions promises: a
// subscription that was opened and closed without consuming anything still
// remembers where it was asked to start, so a message published while nobody
// was reading is not skipped.
func TestDurableSubscription(t *testing.T) {
	c, topic := openClient(t)
	first, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-durable"})
	if err != nil {
		t.Fatalf("opening the first consumer: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("kept")})
	if err := first.Close(); err != nil {
		t.Fatalf("closing the first consumer: %v", err)
	}
	second := consumerFrom(t, c, topic, "g-durable", mq.StartLatest())
	d := receiveOne(t, second, 10*time.Second)
	if string(d.Body) != "kept" {
		t.Fatalf("the reopened subscription holds %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAckStopsRedelivery across consumer sessions: the commit is what makes an
// acknowledgement survive a reopen, and the commit interval is what bounds the
// window in which it has not happened yet.
func TestAckStopsRedeliveryAcrossSessions(t *testing.T) {
	// The commit ticker is OFF, so the only thing that can have committed
	// the acknowledgement is Close itself. With the ticker on, a mutation
	// that removed Close's synchronous commit left this test green — the
	// ticker had already done the work ('s neighbour).
	c, topic := openClient(t, func(cfg *kq.Config) { cfg.CommitInterval = -1 })
	cons := consumerOn(t, c, topic, "g-commit")
	publish(t, c, topic, &mq.Message{Body: []byte("done")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// Close commits synchronously, which is what narrows the window to
	// zero at the moment it matters most.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "g-commit")
	expectNothing(t, again, time.Second, "a record acknowledged by a previous session")
}

// TestNackRedeliversInPlace is the default nack strategy: the record comes
// back, the partition's order is kept, and the attempt count rises — which is
// what the block-partition strategy promises.
func TestNackRedeliversInPlace(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g-nack")
	publish(t, c, topic, &mq.Message{Body: []byte("retry-me")})
	d := receiveOne(t, cons, 10*time.Second)
	if d.Attempt != 1 {
		t.Fatalf("first delivery reports attempt %d", d.Attempt)
	}
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again := receiveOne(t, cons, 10*time.Second)
	if string(again.Body) != "retry-me" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if again.Attempt < 2 {
		t.Fatalf("the redelivery reports attempt %d, want at least 2", again.Attempt)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	expectNothing(t, cons, 500*time.Millisecond, "a record acknowledged on its second attempt")
}

// TestNackDelayHoldsTheRecord: the hold is in this process, which is why the
// claim is EmulatedVolatile, and it really holds.
func TestNackDelayHoldsTheRecord(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g-delay")
	publish(t, c, topic, &mq.Message{Body: []byte("later")})
	d := receiveOne(t, cons, 10*time.Second)
	const delay = 1500 * time.Millisecond
	start := time.Now()
	if err := d.Nack(mq.NackDelay(delay)); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	expectNothing(t, cons, 400*time.Millisecond, "a record held for a delayed redelivery")
	again := receiveOne(t, cons, 10*time.Second)
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("the redelivery arrived after %v, want at least %v", elapsed, delay)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestRejectRoutesToTheDeadLetterTopic is through the nack strategy: the
// record is published to the dead-letter topic with its provenance, and the
// original offset is committed only after that publication is confirmed.
func TestRejectRoutesToTheDeadLetterTopic(t *testing.T) {
	topic, dlq := newTopic("dl"), newTopic("dl-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	cons := consumerOn(t, c, topic, "g-dl")
	dead := consumerFrom(t, c, dlq, "g-dl-reader", mq.StartEarliest())
	publish(t, c, topic, &mq.Message{Body: []byte("poison"), Key: []byte("k")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	got := receiveOne(t, dead, 15*time.Second)
	if string(got.Body) != "poison" || string(got.Key) != "k" {
		t.Fatalf("the dead-letter destination holds key %q body %q", got.Key, got.Body)
	}
	for _, h := range []string{kq.HeaderOriginTopic, kq.HeaderOriginPartition, kq.HeaderOriginOffset, kq.HeaderFirstDeliveredAt} {
		if _, ok := got.Headers.Get(h); !ok {
			t.Errorf("the dead-lettered record carries no %s: the specification asks for the provenance", h)
		}
	}
	if v, _ := got.Headers.Get(kq.HeaderOriginTopic); string(v) != topic {
		t.Errorf("%s = %q, want %q", kq.HeaderOriginTopic, v, topic)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// The original is gone from the source topic, because the move
	// succeeded and only then was the offset completed.
	expectNothing(t, cons, time.Second, "a dead-lettered record")
}

// TestRejectWithNoDeadLetterTopicIsRefused is the other half: a route off
// the topic needs an explicit destination, and without one the settlement is
// refused rather than turned into a drop.
func TestRejectWithNoDeadLetterTopicIsRefused(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g-nodl")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 10*time.Second)
	err := d.Reject()
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Reject with no dead-letter topic = %v, want ErrUnsupported", err)
	}
	// A refused settlement leaves the delivery settleable, so the caller
	// may still do something else with it.
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack after a refused Reject: %v", err)
	}
}

// TestRetryTopicStrategy is the second kind end to end: the record is
// republished to `<topic>.retry.1`, its attempt count travels in a header, and
// the original offset is committed so the partition keeps moving.
func TestRetryTopicStrategy(t *testing.T) {
	topic := newTopic("rt")
	retry1 := topic + ".retry.1"
	cl := cluster(t, 1, []string{topic, retry1})
	c := openOn(t, cl, func(cfg *kq.Config) {
		cfg.Nack = nack.RetryTopic
		cfg.NackConfig.Retries = 2
	})
	cons := consumerOn(t, c, topic, "g-rt")
	retryCons := consumerFrom(t, c, retry1, "g-rt-retry", mq.StartEarliest())
	publish(t, c, topic, &mq.Message{Body: []byte("move-me")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	got := receiveOne(t, retryCons, 15*time.Second)
	if string(got.Body) != "move-me" {
		t.Fatalf("the retry topic holds %q", got.Body)
	}
	if v, ok := got.Headers.Get(kq.HeaderAttempts); !ok || string(v) != "2" {
		t.Fatalf("%s = %q (present %v), want the second attempt", kq.HeaderAttempts, v, ok)
	}
	if got.Attempt != 2 {
		t.Fatalf("the republished record reports attempt %d, want 2: the header is what makes the count survive a republication", got.Attempt)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// The original partition moved on, which is the whole point of the
	// strategy and the price it pays in ordering.
	expectNothing(t, cons, time.Second, "a record moved to a retry topic")
}

// TestStopStrategyHaltsTheConsumer is the third kind: nothing after
// the refused record is consumed, and the record is not acknowledged, so a
// reopened subscription sees it again. ADR-011's "halt" as a behaviour.
func TestStopStrategyHaltsTheConsumer(t *testing.T) {
	c, topic := openClient(t, func(cfg *kq.Config) { cfg.Nack = nack.Stop })
	cons := consumerOn(t, c, topic, "g-stop")
	publish(t, c, topic, &mq.Message{Body: []byte("halt")})
	publish(t, c, topic, &mq.Message{Body: []byte("after")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if _, err := tryReceiveOne(t, cons, time.Second); err == nil {
		t.Fatal("the consumer kept delivering after a stop-the-consumer nack")
	} else if mq.CodeOf(err) != mq.CodePoisonMessage {
		t.Fatalf("Receive after a stop = %v (code %v), want CodePoisonMessage (ADR-011's halt)", err, mq.CodeOf(err))
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "g-stop")
	d2 := receiveOne(t, again, 10*time.Second)
	if string(d2.Body) != "halt" {
		t.Fatalf("the reopened subscription starts at %q, want the record the stop refused", d2.Body)
	}
}

// TestExtendIsRefused: Kafka grants no per-record lease, so there is nothing
// an Extend could move and a settler that pretended otherwise would let a
// handler believe it had bought time it did not have.
func TestExtendIsRefused(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g-extend")
	publish(t, c, topic, &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Extend(time.Minute); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Extend = %v, want ErrUnsupported", err)
	}
	if !d.Info().LeaseExpires.IsZero() {
		t.Fatalf("the delivery names a lease expiry of %v; Kafka grants none", d.Info().LeaseExpires)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSecondSettlementIsRefused is exactly one terminal settlement per
// delivery, and the second is refused rather than applied.
func TestSecondSettlementIsRefused(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g-twice")
	publish(t, c, topic, &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("the second Ack = %v, want ErrAlreadySettled", err)
	}
	if err := d.Nack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a Nack after an Ack = %v, want ErrAlreadySettled", err)
	}
}

// TestConsumerGroupsCompete: two consumers of one subscription share the
// partitions, so exactly one of them sees a record.
func TestConsumerGroupsCompete(t *testing.T) {
	topic := newTopic("group")
	cl := cluster(t, 2, []string{topic})
	c := openOn(t, cl)
	a := consumerOn(t, c, topic, "g-shared")
	b := consumerOn(t, c, topic, "g-shared")
	const n = 20
	for i := range n {
		publish(t, c, topic, &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))})
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for _, cons := range []*mq.ConsumerHandle{a, b} {
		wg.Add(1)
		go func(h *mq.ConsumerHandle) {
			defer wg.Done()
			receiveN(t, h, n, 15*time.Second, func(d *mq.Delivery) error {
				mu.Lock()
				seen[string(d.Body)]++
				mu.Unlock()
				return d.Ack()
			})
		}(cons)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for i := range n {
		if got := seen[strconv.Itoa(i)]; got == 0 {
			t.Errorf("record %d reached neither consumer", i)
		}
	}
}

// TestFanOutAcrossSubscriptions: two subscriptions each receive everything,
// which is what makes a subscription a consumer group rather than a queue.
func TestFanOutAcrossSubscriptions(t *testing.T) {
	c, topic := openClient(t)
	a := consumerOn(t, c, topic, "fan-1")
	b := consumerOn(t, c, topic, "fan-2")
	publish(t, c, topic, &mq.Message{Body: []byte("both")})
	for name, cons := range map[string]*mq.ConsumerHandle{"fan-1": a, "fan-2": b} {
		d := receiveOne(t, cons, 10*time.Second)
		if string(d.Body) != "both" {
			t.Fatalf("%s received %q", name, d.Body)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("%s Ack: %v", name, err)
		}
	}
}

// TestReplayFromTheStart: a new subscription reads the history, which is what
// a retained log is for.
func TestReplayFromTheStart(t *testing.T) {
	c, topic := openClient(t)
	publish(t, c, topic, &mq.Message{Body: []byte("history")})
	a := consumerFrom(t, c, topic, "replay-a", mq.StartEarliest())
	d := receiveOne(t, a, 10*time.Second)
	if string(d.Body) != "history" {
		t.Fatalf("body %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	b := consumerFrom(t, c, topic, "replay-b", mq.StartEarliest())
	d2 := receiveOne(t, b, 10*time.Second)
	if string(d2.Body) != "history" {
		t.Fatalf("the replayed message is %q", d2.Body)
	}
	if err := d2.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAdminTopology covers the typed administration: EnsureTopic is idempotent
// and the rest of the surface answers.
func TestAdminTopology(t *testing.T) {
	cl := cluster(t, 1, nil)
	c := openOn(t, cl)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	topic := newTopic("admin")
	for range 2 {
		if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	}
	found := false
	for name, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if name == topic {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListTopics did not report %q", topic)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "g-admin"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if _, err := admin.Stats(t.Context(), topic, "g-admin"); err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if err := admin.Purge(t.Context(), topic, "g-admin"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), topic, "g-admin"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if err := admin.DeleteTopic(t.Context(), topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

// TestCloseUnblocksReceive is Close unblocks a blocked operation, and
// it has to reach a caller parked in a poll.
func TestCloseUnblocksReceive(t *testing.T) {
	c, topic := openClient(t)
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-unblock"})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		dst := make([]mq.Delivery, 1)
		_, err := cons.Receive(t.Context(), dst)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("the parked Receive returned %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not unblock a parked Receive")
	}
}

// TestPublishAfterClose is a closed handle answers ErrClosed.
func TestPublishAfterClose(t *testing.T) {
	c, topic := openClient(t)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Publish(t.Context(), &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("the second Close = %v; Close is idempotent", err)
	}
}

// TestOversizedPayloadIsRefusedBeforeIO: the limit is enforced by the
// driver before a byte is written, which is what makes the advertised number
// worth reading.
func TestOversizedPayloadIsRefusedBeforeIO(t *testing.T) {
	c, topic := openClient(t)
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Skip("the cluster did not report message.max.bytes")
	}
	body := make([]byte, lim.Value+1)
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: body}); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("publishing %d bytes over the limit = %v, want ErrMessageTooLarge", 1, err)
	}
}

// TestSettlementWakesAWaitingReceive: a Receive that is already waiting when
// the tracker pauses the partition is woken by the acknowledgements that
// unpause it. The records the paused partition was holding are only ever
// handed out by a Receive, so a waiting one that was not woken sat out its
// whole deadline with them in hand, and returned nothing.
func TestSettlementWakesAWaitingReceive(t *testing.T) {
	c, topic := openClient(t, func(cfg *kq.Config) { cfg.TrackerMaxPending = 4 })
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-wake", Start: mq.StartEarliest(), Prefetch: 2,
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	const n = 8
	for i := range n {
		publish(t, c, topic, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	// Four unsettled deliveries are what pause the partition.
	held := make([]mq.Delivery, 0, 4)
	for len(held) < 4 {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		dst := make([]mq.Delivery, min(2, 4-len(held)))
		got, err := cons.Receive(ctx, dst)
		cancel()
		if err != nil {
			t.Fatalf("Receive with %d held: %v", len(held), err)
		}
		held = append(held, dst[:got]...)
	}
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	dst := make([]mq.Delivery, 2)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		got, err := cons.Receive(ctx, dst)
		done <- result{got, err}
	}()
	// Long enough for the Receive to be waiting on the broker.
	time.Sleep(300 * time.Millisecond)
	for i := range held {
		if err := held[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	r := <-done
	if r.n == 0 {
		t.Fatalf("the waiting Receive returned nothing (%v) after every held delivery was acknowledged", r.err)
	}
	for i := range r.n {
		if err := dst[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestNackHoldsBackTheRestOfThePartition: while block-partition holds a
// nacked record for its delay, nothing later of that partition is handed out,
// and the topic's other partitions keep flowing. Record 1 was already
// fetched when record 0 was nacked, and it used to be handed out within a
// millisecond of the nack: the source held a partition's later records back
// for the tracker's pause and not for a nack's.
func TestNackHoldsBackTheRestOfThePartition(t *testing.T) {
	topic := newTopic("hold")
	cl := cluster(t, 2, []string{topic})
	c := openOn(t, cl)
	for _, body := range []string{"r0", "r1"} {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(body)}, mq.Partition(0)); err != nil {
			t.Fatalf("publishing %s to partition 0: %v", body, err)
		}
	}
	cons := consumerFrom(t, c, topic, "g-hold", mq.StartEarliest())
	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "r0" {
		t.Fatalf("the first delivery carries %q, want r0", d.Body)
	}
	const delay = 3 * time.Second
	start := time.Now()
	if err := d.Nack(mq.NackDelay(delay)); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("other")}, mq.Partition(1)); err != nil {
		t.Fatalf("publishing to partition 1: %v", err)
	}
	next := receiveOne(t, cons, 10*time.Second)
	elapsed := time.Since(start)
	switch {
	case string(next.Body) == "r1":
		t.Fatalf("record 1 of partition 0 was handed out %v after record 0 was nacked for %v", elapsed, delay)
	case string(next.Body) != "other":
		t.Fatalf("after the nack the first delivery carries %q (attempt %d, %v later), want partition 1's record", next.Body, next.Attempt, elapsed)
	case elapsed >= delay:
		t.Fatalf("partition 1's record arrived %v after the nack, so it waited for partition 0's hold", elapsed)
	}
	if err := next.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	again := receiveOne(t, cons, 10*time.Second)
	if string(again.Body) != "r0" || again.Attempt < 2 {
		t.Fatalf("after partition 1's record came %q at attempt %d, want record 0's redelivery", again.Body, again.Attempt)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("the redelivery arrived after %v, want at least %v", elapsed, delay)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	after := receiveOne(t, cons, 10*time.Second)
	if string(after.Body) != "r1" {
		t.Fatalf("after record 0's redelivery came %q, want r1", after.Body)
	}
	if err := after.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
