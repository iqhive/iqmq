package jetstream_test

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestConfirmationBoundaries holds for this profile. ConfirmDurable is NOT
// refused, which is the whole difference from the Core driver: a PubAck is sent
// after the message is committed to the stream's storage, so "accepted" and
// "durable" are the same round trip.
func TestConfirmationBoundaries(t *testing.T) {
	for _, mode := range []mq.ConfirmationMode{
		mq.ConfirmDefault, mq.ConfirmNone, mq.ConfirmAccepted, mq.ConfirmDurable,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			c := openClient(t)
			topic := newTopic("confirm")
			cons := consumerOn(t, c, topic, "workers")
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mode})
			if err != nil {
				t.Fatalf("a publisher at %v was refused: %v", mode, err)
			}
			defer func() { _ = p.Close() }()
			if err := p.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish at %v: %v", mode, err)
			}
			d, ok := tryReceive(t, cons, 10*time.Second)
			if !ok {
				t.Fatalf("a publication at %v never arrived", mode)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestPublishToASubjectNoStreamCapturesIsNotFound is the specification's "no hidden
// creation", and the reason the refusal happens when the publisher is opened
// rather than at the first message: a ConfirmNone publication is a core publish
// with no reply, so a server that dropped it would tell nobody.
func TestPublishToASubjectNoStreamCapturesIsNotFound(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) {
		cfg.StreamSubjects = []string{cfg.SubjectPrefix + ".live.>"}
	})
	// ConfirmNone is the case that decides this, and it is the reason the
	// refusal happens when the publisher is OPENED. Under a confirmation the
	// server answers "no stream matches" and the failure is reported anyway;
	// under ConfirmNone the publication is a core publish with no reply, so a
	// driver that only relied on the server's answer would drop the message
	// and report success. A test that used the default confirmation would
	// therefore pass with the check removed.
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{
		Topic:        "elsewhere.orders",
		Confirmation: mq.ConfirmNone,
	})
	if err == nil {
		perr := p.Publish(t.Context(), &mq.Message{Body: []byte("x")})
		_ = p.Close()
		if perr == nil {
			t.Fatal("a ConfirmNone publication to a subject no stream captures reported success: the server drops it and tells nobody ")
		}
		err = perr
	}
	if !errors.Is(err, mq.ErrTopicNotFound) || mq.CodeOf(err) != mq.CodeNotFound {
		t.Errorf("the refusal reported %v (code %v), want ErrTopicNotFound", err, mq.CodeOf(err))
	}
	// A confirmed publication to the same subject is refused too, and the
	// subject the stream DOES capture is not refused: a check that refuses
	// everything proves nothing.
	if err := c.Publish(t.Context(), "elsewhere.orders", &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Errorf("a confirmed publication to an uncaptured subject reported %v", err)
	}
	if err := c.Publish(t.Context(), "live.orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing to a captured subject: %v", err)
	}
}

// TestBatchPublishIsIndexAligned holds the batch contract: every message gets
// its own PubAck, so each is durable, and the result is aligned with the batch.
func TestBatchPublishIsIndexAligned(t *testing.T) {
	c := openClient(t)
	topic := newTopic("batch")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 64})
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	const n = 24
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte("batch-" + strconv.Itoa(i))}
	}
	if err := p.PublishBatch(t.Context(), ms); err != nil {
		var be *mq.BatchError
		if errors.As(err, &be) && len(be.Errs) != n {
			t.Fatalf("a partial batch reported %d outcomes for %d messages: index alignment is the whole contract", len(be.Errs), n)
		}
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	for range n {
		d, ok := tryReceive(t, cons, 10*time.Second)
		if !ok {
			t.Fatalf("only %d of %d batched messages arrived", len(seen), n)
		}
		seen[string(d.Body)] = true
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	for i := range n {
		if !seen["batch-"+strconv.Itoa(i)] {
			t.Errorf("batch-%d never arrived", i)
		}
	}
}

// TestAsyncCallbackRunsExactlyOnce holds the contract clause by clause: a nil
// return means the callback runs exactly once, and it runs with the message that
// was published.
//
// It also holds the shape of the implementation: the callbacks are resolved from
// the client library's own reply goroutine through a map keyed by the native
// message, so there is no goroutine per publication. The
// goroutine count before and after is what says so.
func TestAsyncCallbackRunsExactlyOnce(t *testing.T) {
	c := openClient(t)
	topic := newTopic("async")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 64})
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	const n = 32
	var mu sync.Mutex
	calls := map[*mq.Message]int{}
	mismatched := 0
	fired := make(chan struct{}, n)
	ms := make([]*mq.Message, n)
	before := goroutines()
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte("async-" + strconv.Itoa(i))}
		m := ms[i]
		if err := p.PublishAsync(t.Context(), m, func(res mq.PublishResult) {
			mu.Lock()
			calls[m]++
			if res.Message != m {
				mismatched++
			}
			mu.Unlock()
			fired <- struct{}{}
		}); err != nil {
			_ = p.Close()
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	peak := goroutines()
	deadline := time.After(20 * time.Second)
	for range n {
		select {
		case <-fired:
		case <-deadline:
			mu.Lock()
			got := len(calls)
			mu.Unlock()
			_ = p.Close()
			t.Fatalf("only %d of %d asynchronous callbacks ran", got, n)
		}
	}
	if peak-before >= n {
		t.Errorf("%d asynchronous publications added %d goroutines: the specification forbids a goroutine per message", n, peak-before)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if mismatched != 0 {
		t.Errorf("%d callbacks reported a message other than the one published", mismatched)
	}
	for i, m := range ms {
		if calls[m] != 1 {
			t.Errorf("the callback for message %d ran %d times, want exactly once", i, calls[m])
		}
	}
	for range n {
		d, ok := tryReceive(t, cons, 10*time.Second)
		if !ok {
			t.Fatal("an asynchronously published message never arrived")
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestAsyncPublicationCopiesTheBody is on the one path where the caller's
// buffer really is still in use after the call returns.
//
// The client library holds the message — payload included — until its PubAck, so
// an asynchronous publication that aliased the caller's body would publish
// whatever the caller wrote next. The test overwrites the buffer the instant
// PublishAsync returns, which is exactly what the specification says a caller may do.
func TestAsyncPublicationCopiesTheBody(t *testing.T) {
	b := openBackend(t)
	c := mq.NewClient(b)
	topic := newTopic("alias")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 8})
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	body := []byte("original-payload")
	done := make(chan error, 1)
	before := b.AsyncBodyCopies()
	if err := p.PublishAsync(t.Context(), &mq.Message{Body: body}, func(res mq.PublishResult) {
		done <- res.Err
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	// The specification: the caller owns its buffers again the instant the call returns.
	for i := range body {
		body[i] = 'X'
	}
	if err := <-done; err != nil {
		t.Fatalf("the asynchronous publication failed: %v", err)
	}
	// The delivered body is the first half of the property, and it holds even
	// without the copy: nats.go appends the payload to its own write buffer
	// inside PublishMsg, so the FIRST attempt never aliases. What the copy
	// protects is the retry — the JetStream client keeps the *nats.Msg and
	// republishes it on "no responders" and after a reconnection — and a
	// conformance-shaped test cannot make a retry happen on demand. So the
	// copy itself is counted, which is a check that fails when it is removed.
	if got := b.AsyncBodyCopies() - before; got != 1 {
		t.Fatalf("an asynchronous publication copied the payload %d times, want exactly 1: the client library republishes the message it was given, so a retry would read whatever the caller wrote next", got)
	}
	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "original-payload" {
		t.Fatalf("the delivered body is %q: the asynchronous path aliased the caller's buffer", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// A nil body stays nil through the copy, because the specification makes nil and
	// empty different things and this driver carries the distinction in a
	// header.
	nilDone := make(chan error, 1)
	if err := p.PublishAsync(t.Context(), &mq.Message{}, func(res mq.PublishResult) {
		nilDone <- res.Err
	}); err != nil {
		t.Fatalf("PublishAsync of a nil body: %v", err)
	}
	if err := <-nilDone; err != nil {
		t.Fatalf("the nil-body publication failed: %v", err)
	}
	nd := receiveOne(t, cons, 10*time.Second)
	if nd.Body != nil {
		t.Errorf("a nil body came back as %d bytes", len(nd.Body))
	}
	if err := nd.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestReceiptCarriesTheStreamSequence holds FeatureReceipts: what the broker
// said about an accepted publication, in storage the caller owns.
func TestReceiptCarriesTheStreamSequence(t *testing.T) {
	c := openClient(t)
	topic := newTopic("receipt")
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
	if err != nil {
		t.Fatalf("opening a publisher with receipts: %v", err)
	}
	defer func() { _ = p.Close() }()
	var receipt mq.PublishReceipt
	m := &mq.Message{Body: []byte("x"), Receipt: &receipt}
	if err := p.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.Sequence == 0 {
		t.Error("the receipt carries no stream sequence")
	}
	if receipt.Confirmation != mq.ConfirmDurable {
		t.Errorf("the receipt reports confirmation %v, want durable: a PubAck IS the durable boundary here", receipt.Confirmation)
	}
	if receipt.ConfirmedAt.IsZero() {
		t.Error("the receipt carries no confirmation time")
	}
	if receipt.MessageID == "" {
		t.Error("the receipt carries no identifier")
	}
}

// TestDeduplicationDiscardsARepeat holds the Nats-Msg-Id claim: the same key
// inside the stream's duplicate window is stored once.
func TestDeduplicationDiscardsARepeat(t *testing.T) {
	c := openClient(t, func(cfg *nq.Config) { cfg.DuplicateWindow = time.Minute })
	topic := newTopic("dedup")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 8})
	for range 2 {
		publish(t, c, topic, &mq.Message{Body: []byte("dup")}, mq.DedupKey("k-1"))
	}
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if d, ok := tryReceive(t, cons, 2*time.Second); ok {
		t.Fatalf("both publications of one de-duplication key were delivered (the second is %q)", d.Body)
	}
	// A different key is a different message: a check that discards
	// everything proves nothing.
	publish(t, c, topic, &mq.Message{Body: []byte("other")}, mq.DedupKey("k-2"))
	other := receiveOne(t, cons, 10*time.Second)
	if string(other.Body) != "other" {
		t.Errorf("the second key was delivered as %q", other.Body)
	}
	if err := other.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestMessageTTLExpiresAMessage holds the conditional MessageTTL claim on a
// stream that has AllowMsgTTL.
func TestMessageTTLExpiresAMessage(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.AllowMsgTTL = true })
	c := openOnStream(t, stream)
	if !c.Capabilities().Has(mq.FeatureMessageTTL) {
		t.Fatal("a stream created with AllowMsgTTL does not advertise MessageTTL")
	}
	topic := newTopic("ttl")
	publish(t, c, topic, &mq.Message{Body: []byte("expires")}, mq.TTL(time.Second))
	time.Sleep(2500 * time.Millisecond)
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Start: mq.StartEarliest(),
	})
	if d, ok := tryReceive(t, cons, 2*time.Second); ok {
		t.Fatalf("a message past its TTL was still delivered: %q", d.Body)
	}
}

// TestMessageTTLIsRefusedWithoutTheStreamFlag is the negative direction of the
// same claim, and it is what makes the claim honest: a stream without
// AllowMsgTTL — which is every stream on a server older than 2.11 — advertises
// nothing and refuses the control before any I/O.
func TestMessageTTLIsRefusedWithoutTheStreamFlag(t *testing.T) {
	stream := provisionStream(t, nil)
	c := openOnStream(t, stream)
	if c.Capabilities().Has(mq.FeatureMessageTTL) {
		t.Fatal("a stream without AllowMsgTTL advertises MessageTTL")
	}
	err := c.Publish(t.Context(), newTopic("ttl"), &mq.Message{Body: []byte("x")}, mq.TTL(time.Second))
	if err == nil {
		t.Fatal("a per-message TTL was accepted on a stream that cannot honour one: the message would never expire and the caller would not know")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the refusal reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
}

// TestScheduledPublicationFiresLater holds the conditional ScheduledPublish
// claim: the driver writes a Nats-Schedule control message on its own subject
// tree with the topic as the target, and the server generates the message when
// the schedule fires.
func TestScheduledPublicationFiresLater(t *testing.T) {
	c := openClient(t)
	if !c.Capabilities().Has(mq.FeatureScheduledPublish) {
		t.Skip("this server does not support message schedules")
	}
	topic := newTopic("sched")
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Start: mq.StartEarliest(),
	})
	publish(t, c, topic, &mq.Message{Body: []byte("scheduled")}, mq.Delay(2*time.Second))
	if d, ok := tryReceive(t, cons, 700*time.Millisecond); ok {
		t.Fatalf("a scheduled publication was delivered early: %q", d.Body)
	}
	d, ok := tryReceive(t, cons, 15*time.Second)
	if !ok {
		t.Fatal("a scheduled publication never fired")
	}
	if string(d.Body) != "scheduled" {
		t.Errorf("the scheduled message arrived as %q", d.Body)
	}
	if d.Info().Topic != topic {
		t.Errorf("the scheduled message arrived on topic %q, want %q", d.Info().Topic, topic)
	}
	// The server's own schedule headers are not the caller's metadata.
	for k := range d.Headers.All() {
		if nq.IsNATSHeader(k) {
			t.Errorf("the scheduled delivery carries the server's header %q as a caller header", k)
		}
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestScheduledPublicationIsRefusedWithoutTheStreamFlag is the negative
// direction: a stream without AllowMsgSchedules — every stream on a server older
// than 2.12 — advertises nothing and refuses Delay and DeliverAt before any I/O.
func TestScheduledPublicationIsRefusedWithoutTheStreamFlag(t *testing.T) {
	stream := provisionStream(t, nil)
	c := openOnStream(t, stream)
	if c.Capabilities().Has(mq.FeatureScheduledPublish) {
		t.Fatal("a stream without AllowMsgSchedules advertises ScheduledPublish")
	}
	topic := newTopic("sched")
	err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}, mq.Delay(time.Second))
	if err == nil {
		t.Fatal("a scheduled publication was accepted on a stream that cannot schedule: it would be delivered at once")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the refusal reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
}

// TestUnsupportedPublishControlsAreRefusedBeforeIO holds a control this
// broker has no way to honour is refused, and nothing is published.
func TestUnsupportedPublishControlsAreRefusedBeforeIO(t *testing.T) {
	c := openClient(t)
	topic := newTopic("controls")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 8})
	for _, c2 := range []struct {
		name string
		opt  mq.PublishOption
	}{
		{"priority", mq.Priority(3)},
		{"partition", mq.Partition(1)},
	} {
		t.Run(c2.name, func(t *testing.T) {
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}, c2.opt)
			if err == nil {
				t.Fatalf("a %s was accepted", c2.name)
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Errorf("the refusal reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
			}
		})
	}
	if d, ok := tryReceive(t, cons, time.Second); ok {
		t.Fatalf("a refused publication reached the stream anyway: %q", d.Body)
	}
}

// TestScheduleInThePastIsRefused holds the one refusal a schedule has of its
// own: a caller who asked for a delay and got none has been told something
// untrue about when its message becomes visible.
func TestScheduleInThePastIsRefused(t *testing.T) {
	// DeliverAt has no PublishOption token in the frozen public API — only
	// Delay does — so the absolute form is reachable through the SPI alone.
	// The gap is recorded in the maintenance notes; this is the only route a test
	// has to the code that handles it.
	b := openBackend(t)
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: newTopic("past")})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	err = sink.Publish(t.Context(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{DeliverAt: time.Now().Add(-time.Hour)})
	if err == nil {
		t.Fatal("a publication scheduled in the past was accepted")
	}
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("the refusal reported code %v, want InvalidArgument", mq.CodeOf(err))
	}
	// Delay and DeliverAt together name one control twice, which is a mistake
	// rather than a request.
	err = sink.Publish(t.Context(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{Delay: time.Second, DeliverAt: time.Now().Add(time.Hour)})
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("Delay and DeliverAt together reported %v (code %v), want InvalidArgument", err, mq.CodeOf(err))
	}
}

// TestMetadataModes holds the four modes, all four of which behave
// differently on a broker whose headers are text lines.
func TestMetadataModes(t *testing.T) {
	binary := func() *mq.Message {
		m := &mq.Message{Body: []byte("payload")}
		m.Headers.Set("trace", []byte{0x00, 0xff})
		return m
	}

	t.Run("strict refuses before any I/O", func(t *testing.T) {
		c := openClient(t)
		topic := newTopic("strict")
		cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
		p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataNativeStrict})
		if err != nil {
			t.Fatalf("opening a strict publisher: %v", err)
		}
		defer func() { _ = p.Close() }()
		err = p.Publish(t.Context(), binary())
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("a strict publisher accepted a binary header: %v", err)
		}
		if d, ok := tryReceive(t, cons, time.Second); ok {
			t.Fatalf("the refused publication reached the stream: %q", d.Body)
		}
	})

	t.Run("envelope carries everything", func(t *testing.T) {
		c := openClient(t)
		topic := newTopic("envelope")
		cons := consumerWith(t, c, mq.ConsumerConfig{
			Topic: topic, Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataPortableEnvelope,
		})
		p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
		if err != nil {
			t.Fatalf("opening an enveloping publisher: %v", err)
		}
		defer func() { _ = p.Close() }()
		m := binary()
		m.Key = []byte("k")
		m.ID = "id-9"
		m.Headers.Add("dup", []byte("one"))
		m.Headers.Add("dup", []byte("two"))
		if err := p.Publish(t.Context(), m); err != nil {
			t.Fatalf("an enveloped publication: %v", err)
		}
		d := receiveOne(t, cons, 10*time.Second)
		if string(d.Body) != "payload" || string(d.Key) != "k" || d.ID != "id-9" {
			t.Errorf("the envelope round trip gave body %q, key %q, id %q", d.Body, d.Key, d.ID)
		}
		if v, ok := d.Headers.Get("trace"); !ok || !bytes.Equal(v, []byte{0x00, 0xff}) {
			t.Errorf("the envelope lost the binary header: %q (present %v)", v, ok)
		}
		if vals := d.Headers.Values("dup"); len(vals) != 2 {
			t.Errorf("the envelope lost a duplicate header: %d values", len(vals))
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})

	t.Run("lenient drops and counts", func(t *testing.T) {
		b := openBackend(t)
		c := mq.NewClient(b)
		topic := newTopic("drop")
		cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
		p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataLenientDrop})
		if err != nil {
			t.Fatalf("opening a lenient publisher: %v", err)
		}
		defer func() { _ = p.Close() }()
		if err := p.Publish(t.Context(), binary()); err != nil {
			t.Fatalf("a lenient publication: %v", err)
		}
		d := receiveOne(t, cons, 10*time.Second)
		if _, ok := d.Headers.Get("trace"); ok {
			t.Error("a header the broker cannot carry survived MetadataLenientDrop")
		}
		if string(d.Body) != "payload" {
			t.Errorf("the body changed: %q", d.Body)
		}
		if got := b.DroppedMetadata(); got == 0 {
			t.Error("DroppedMetadata did not count the drop: the specification asks for a metric")
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})

	t.Run("adaptive envelopes only what needs it", func(t *testing.T) {
		c := openClient(t)
		topic := newTopic("adaptive")
		cons := consumerWith(t, c, mq.ConsumerConfig{
			Topic: topic, Subscription: "workers", Prefetch: 8, Metadata: mq.MetadataAdaptive,
		})
		p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataAdaptive})
		if err != nil {
			t.Fatalf("opening an adaptive publisher: %v", err)
		}
		defer func() { _ = p.Close() }()
		plain := &mq.Message{Body: []byte("plain")}
		plain.Headers.Set("k", []byte("v"))
		if err := p.Publish(t.Context(), plain); err != nil {
			t.Fatalf("a plain adaptive publication: %v", err)
		}
		if err := p.Publish(t.Context(), binary()); err != nil {
			t.Fatalf("an adaptive publication needing the envelope: %v", err)
		}
		for range 2 {
			d := receiveOne(t, cons, 10*time.Second)
			switch string(d.Body) {
			case "plain":
				if v, ok := d.Headers.Get("k"); !ok || string(v) != "v" {
					t.Errorf("the native path lost the header: %q", v)
				}
			case "payload":
				if v, ok := d.Headers.Get("trace"); !ok || !bytes.Equal(v, []byte{0x00, 0xff}) {
					t.Errorf("the enveloped path lost the binary header: %q", v)
				}
			default:
				t.Errorf("an unexpected body: %q", d.Body)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	})
}

// TestBodyRoundTripsEveryShape holds the observable body distinctions over a
// real server.
func TestBodyRoundTripsEveryShape(t *testing.T) {
	c := openClient(t)
	topic := newTopic("bodies")
	cons := consumerWith(t, c, mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 16})
	bodies := [][]byte{nil, {}, []byte("plain"), {0x00, 0xff, 0x7f, 0x80}, bytes.Repeat([]byte{0xa5}, 64<<10)}
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	for i, body := range bodies {
		if err := p.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
			t.Fatalf("publishing body %d (%d bytes): %v", i, len(body), err)
		}
	}
	for i, want := range bodies {
		d := receiveOne(t, cons, 15*time.Second)
		switch {
		case want == nil && d.Body != nil:
			t.Errorf("body %d: a nil body came back as %d bytes; the specification makes the distinction observable", i, len(d.Body))
		case want != nil && d.Body == nil:
			t.Errorf("body %d: a non-nil body came back nil", i)
		case !bytes.Equal(d.Body, want):
			t.Errorf("body %d: %d bytes, want %d", i, len(d.Body), len(want))
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestOversizedPublicationIsRefused holds the one limit this driver really knows:
// the smaller of the server's max_payload and the stream's own MaxMsgSize.
func TestOversizedPublicationIsRefused(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.MaxMsgSize = 1024 })
	c := openOnStream(t, stream)
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Fatal("the driver advertises no message-size limit although the stream has one")
	}
	if lim.Value != 1024 {
		t.Errorf("MaxMessageBytes = %d, want the stream's own 1024: the smaller of the two ceilings is the only answer that cannot lie", lim.Value)
	}
	err := c.Publish(t.Context(), newTopic("big"), &mq.Message{Body: bytes.Repeat([]byte{'x'}, 2048)})
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Errorf("an oversized publication reported %v (code %v), want ErrMessageTooLarge", err, mq.CodeOf(err))
	}
}

// TestFlushWaitsForOutstandingPublications holds the optional flush surface.
func TestFlushWaitsForOutstandingPublications(t *testing.T) {
	c := openClient(t)
	topic := newTopic("flush")
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	for range 8 {
		if err := p.PublishAsync(t.Context(), &mq.Message{Body: []byte("x")}, func(mq.PublishResult) {}); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	if err := p.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if st := p.Stats(); st.Pending != 0 {
		t.Errorf("a flushed publisher reports %d pending publications", st.Pending)
	}
}
