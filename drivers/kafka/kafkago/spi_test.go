package kafkago_test

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// The SPI surface, exercised directly rather than through the core.
//
// Two things can only be reached here. The optional interfaces are one: the specification
// makes them optional, so the CORE decides whether to use them and a driver
// that stopped offering one would simply get slower rather than fail. The
// other is the contract of a nil argument, which the core's own handles refuse
// before the driver sees them.

// TestSinkOffersEveryOptionalSurface is the compile-time assertion made
// observable: a driver that stopped implementing one of these would keep
// working and get slower, which is exactly the kind of regression no ordinary
// test notices.
func TestSinkOffersEveryOptionalSurface(t *testing.T) {
	topic := newTopic("spi-sink")
	cl := cluster(t, 1, []string{topic})
	b := backendOn(t, cl)
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	if _, ok := sink.(mq.BatchSink); !ok {
		t.Error("the sink does not implement BatchSink; a Kafka produce request carries many records")
	}
	if _, ok := sink.(mq.AsyncSink); !ok {
		t.Error("the sink does not implement AsyncSink; kafka-go's writer produces without waiting when Async is set")
	}
	if _, ok := sink.(mq.OwnedSink); !ok {
		t.Error("the sink does not implement OwnedSink")
	}
	if _, ok := sink.(mq.FlushSink); !ok {
		t.Error("the sink does not implement FlushSink; a ConfirmNone publication is in a batch and a caller has to be able to wait for it")
	}
}

// TestSourceOffersTheBatchedSettler is the same for the consumption side: a
// cumulative commit makes a batch of acknowledgements one round trip whatever
// its size, and a core that could not use AckBatch would pay a watermark
// computation per delivery.
func TestSourceOffersTheBatchedSettler(t *testing.T) {
	topic := newTopic("spi-source")
	cl := cluster(t, 1, []string{topic})
	b := backendOn(t, cl)
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "spi-g", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	if _, ok := src.Settler().(mq.BatchSettler); !ok {
		t.Error("the settler does not implement BatchSettler")
	}
	if _, ok := src.Settler().(mq.DeadlineExtender); ok {
		t.Error("the settler implements DeadlineExtender; Kafka grants no per-record lease, so there is nothing an Extend could move")
	}
}

// TestSPIRefusals are the arguments the driver refuses on its own, which a
// caller composing the SPI directly can reach and a caller going through the
// core cannot.
func TestSPIRefusals(t *testing.T) {
	topic := newTopic("spi-refuse")
	cl := cluster(t, 1, []string{topic})
	b := backendOn(t, cl)

	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Subscription: "g"}); err == nil {
		t.Error("OpenSource accepted a zero Prefetch; the core resolves it before opening a source, so a zero is a composition error")
	}
	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Prefetch: 1}); err == nil {
		t.Error("OpenSource accepted an empty Subscription; a subscription is the consumer group and Kafka has no anonymous one")
	}
	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Subscription: "bad group", Prefetch: 1}); err == nil {
		t.Error("OpenSource accepted a group name Kafka does not")
	}
	if _, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: "a/b"}); err == nil {
		t.Error("OpenSink accepted a topic Kafka does not")
	}
	if _, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Metadata: mq.MetadataMode(99)}); err == nil {
		t.Error("OpenSink accepted a metadata mode outside the four")
	}
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	async, _ := sink.(mq.AsyncSink)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("x")}, nil, nil); err == nil {
		t.Error("PublishAsync accepted a nil callback; the whole contract is about when the callback runs")
	}
	if err := sink.Publish(t.Context(), nil, nil); err == nil {
		t.Error("Publish accepted a nil message")
	}
	if _, err := kq.BackendWithWriter(t.Context(), nil, 0, kq.Config{}); err == nil {
		t.Error("BackendWithWriter accepted a nil writer")
	}
}

// TestSettlingAnotherSourcesDeliveryIsRefused is the token's own contract: a
// token names a partition of ONE source, and a handle from another one names
// a slot this source never issued.
func TestSettlingAnotherSourcesDeliveryIsRefused(t *testing.T) {
	topic := newTopic("spi-token")
	cl := cluster(t, 1, []string{topic})
	b := backendOn(t, cl)
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "token-g", Prefetch: 4, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	st := src.Settler()
	// A token no source has issued.
	err = st.Ack(t.Context(), &mq.DriverDelivery{Token: 9999, Gen: 1})
	if err == nil {
		t.Fatal("a settlement naming a slot this source never issued was accepted")
	}
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("it was refused with %v, want CodeInvalidArgument", mq.CodeOf(err))
	}
	// An empty batch is a no-op rather than an error, which is what a core
	// that batches by time needs.
	bs, _ := st.(mq.BatchSettler)
	if err := bs.AckBatch(t.Context(), nil); err != nil {
		t.Errorf("AckBatch(nil) = %v", err)
	}
	if err := bs.NackBatch(t.Context(), nil, mq.NackOptions{}); err != nil {
		t.Errorf("NackBatch(nil) = %v", err)
	}
}

// TestCloseRacesPublication is the contributor notes's recurring defect class on
// the publication side, and on this driver it has a second subject: the
// POOLED message buffer.
//
// A publication under ConfirmNone, an asynchronous publication and an owned
// publication all hand a pooled buffer to the writer and get it back in the
// completion. A Close that returned the buffer to the pool while the writer
// was still reading it — or a completion that ran twice — would corrupt
// another publication's record, which is a data race the -race detector sees
// and a corruption a caller never would. It also has to leave the driver's
// own admitted-work count at zero, or a later Flush waits for ever.
func TestCloseRacesPublication(t *testing.T) {
	const (
		rounds      = 20
		perRound    = 100
		closeAround = 30
	)
	topic := newTopic("pub-closerace")
	cl := cluster(t, 4, []string{topic})
	var (
		admitted atomic.Int64
		refused  atomic.Int64
		reported atomic.Int64
	)
	for round := range rounds {
		c, err := kq.Open(t.Context(), config(cl))
		if err != nil {
			t.Fatalf("round %d: Open: %v", round, err)
		}
		pub, err := c.Publisher(t.Context(), mq.PublisherConfig{
			Topic: topic, Confirmation: mq.ConfirmNone,
		})
		if err != nil {
			t.Fatalf("round %d: Publisher: %v", round, err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range perRound {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				m := &mq.Message{Key: []byte("k" + strconv.Itoa(i%7)), Body: []byte(strconv.Itoa(i))}
				var err error
				switch i % 3 {
				case 0:
					err = pub.Publish(t.Context(), m)
				case 1:
					err = pub.PublishAsync(t.Context(), m, func(mq.PublishResult) { reported.Add(1) })
				default:
					err = pub.PublishOwned(t.Context(), mq.OwnedMessage{Message: m, Release: func(*mq.Message) {}})
				}
				switch {
				case err == nil:
					admitted.Add(1)
				case errors.Is(err, mq.ErrClosed), mq.IsAmbiguous(err), mq.CodeOf(err) != mq.CodeUnknown:
					refused.Add(1)
				default:
					t.Errorf("round %d publication %d: %v (code %v)", round, i, err, mq.CodeOf(err))
				}
			}(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for admitted.Load()+refused.Load() < closeAround {
				if admitted.Load()+refused.Load() >= int64(perRound) {
					break
				}
			}
			if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
				t.Errorf("round %d: Close reported %v", round, err)
			}
		}()
		close(start)
		wg.Wait()
		// Every admitted asynchronous publication must have reported, and a
		// Flush on a closed publisher must refuse rather than wait: a count
		// that did not return to zero is a leak a later Flush would hang on.
		if err := pub.Flush(t.Context()); !errors.Is(err, mq.ErrClosed) && err != nil {
			t.Errorf("round %d: Flush after Close = %v", round, err)
		}
		_ = pub.Close()
	}
	t.Logf("%d publications crossed a Close: %d admitted, %d refused, %d asynchronous outcomes reported",
		rounds*perRound, admitted.Load(), refused.Load(), reported.Load())
	if admitted.Load()+refused.Load() != rounds*perRound {
		t.Fatalf("%d of %d publications reported nothing at all", rounds*perRound-admitted.Load()-refused.Load(), rounds*perRound)
	}
}

// TestFlushWaitsForAdmittedWork is what kafka-go has no API for: its only
// flush is Close, which is terminal, so the driver counts what it has
// admitted and Flush waits for the count.
func TestFlushWaitsForAdmittedWork(t *testing.T) {
	topic := newTopic("flush")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl, func(cfg *kq.Config) {
		// A batch of sixteen behind a long timer, so nothing is produced
		// until the batch fills — which is what a Flush has to be able to
		// wait through.
		cfg.BatchMaxRecords = 16
		cfg.BatchTimeout = 250 * time.Millisecond
	})
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	const n = 40
	for i := range n {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := pub.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Everything is in the log by now, which is what Flush means: a
	// subscription opened AFTER the flush and reading from the beginning sees
	// all of it, and the count is what proves the flush waited rather than
	// returning early.
	cons := consumerFrom(t, c, topic, "flush-g", mq.StartEarliest())
	if got := receiveN(t, cons, n, 25*time.Second, func(d *mq.Delivery) error { return d.Ack() }); got != n {
		t.Fatalf("%d of %d records were in the log after Flush returned", got, n)
	}
}
