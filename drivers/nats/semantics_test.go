package nats_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
)

// TestAckNoneSettlementIsRefused is AckNone model reaching a caller.
//
// The driver returns a nil Settler, so the core answers every settlement with
// ErrNotSettleable and never calls the driver. The test is worth having because
// the alternative — a settler whose Ack returns nil — would look identical from
// the outside and would be a lie about a broker that has already forgotten the
// message.
func TestAckNoneSettlementIsRefused(t *testing.T) {
	c := openClient(t)
	topic := newTopic("ackless")
	cons := consumerOn(t, c, topic, "workers")
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)
	for name, settle := range map[string]func() error{
		"Ack":    d.Ack,
		"Nack":   func() error { return d.Nack() },
		"Reject": d.Reject,
		"Extend": func() error { return d.Extend(time.Second) },
	} {
		if err := settle(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Errorf("%s = %v (code %v), want ErrNotSettleable", name, err, mq.CodeOf(err))
		}
	}
	if d.Settled() {
		t.Error("a delivery from a non-settleable source reports itself settled")
	}
}

// TestQueueGroupsCompete is what a subscription name means on this driver:
// two consumers of one subscription share a queue group, so exactly one of them
// gets each message.
func TestQueueGroupsCompete(t *testing.T) {
	c := openClient(t)
	topic := newTopic("compete")
	a := consumerOn(t, c, topic, "workers")
	b := consumerOn(t, c, topic, "workers")
	const n = 40
	for i := range n {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	seen := map[string]int{}
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, h := range []*mq.ConsumerHandle{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]mq.Delivery, 8)
			for {
				got, err := h.Receive(ctx, dst)
				mu.Lock()
				for i := range got {
					seen[string(dst[i].Body)]++
				}
				done := len(seen) == n
				mu.Unlock()
				if done {
					cancel()
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("two competing consumers saw %d distinct messages of %d", len(seen), n)
	}
	for body, count := range seen {
		if count != 1 {
			t.Errorf("message %q was delivered %d times to a queue group", body, count)
		}
	}
}

// TestFanOutAcrossSubscriptions is the other half: two DIFFERENT subscriptions
// are two queue groups, so each of them receives everything.
func TestFanOutAcrossSubscriptions(t *testing.T) {
	c := openClient(t)
	topic := newTopic("fanout")
	a := consumerOn(t, c, topic, "audit")
	b := consumerOn(t, c, topic, "billing")
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("both")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, h := range []*mq.ConsumerHandle{a, b} {
		d := receiveOne(t, h, 5*time.Second)
		if string(d.Body) != "both" {
			t.Fatalf("subscription %d received %q", i, d.Body)
		}
	}
}

// TestWildcardSubscription is FeatureWildcardSubscribe: a consumer topic may be
// a pattern, and the delivery reports the concrete subject it arrived on rather
// than the pattern.
func TestWildcardSubscription(t *testing.T) {
	c := openClient(t)
	root := newTopic("wild")
	cons := consumerOn(t, c, root+".*", "watchers")
	concrete := root + ".created"
	if err := c.Publish(t.Context(), concrete, &mq.Message{Body: []byte("w")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)
	if d.Topic != concrete {
		t.Fatalf("the delivery reports topic %q, want the resolved %q", d.Topic, concrete)
	}
	if info := d.Info(); info.Topic != concrete {
		t.Fatalf("the delivery metadata reports topic %q, want the resolved %q", info.Topic, concrete)
	}
	// A trailing ">" matches the rest of the subject, however deep.
	deep := consumerOn(t, c, root+".>", "deep")
	nested := root + ".a.b.c"
	if err := c.Publish(t.Context(), nested, &mq.Message{Body: []byte("d")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, deep, 5*time.Second); d.Topic != nested {
		t.Fatalf("the > subscription reports topic %q, want %q", d.Topic, nested)
	}
}

// TestWildcardPublishRefused is the negative half: a publication to a pattern
// is refused before any I/O, because publishing to "orders.>" would put the
// message on a subject nobody named.
func TestWildcardPublishRefused(t *testing.T) {
	c := openClient(t)
	for _, topic := range []string{"orders.*", "orders.>"} {
		_, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
		if err == nil {
			t.Errorf("a publisher bound to the pattern %q was accepted", topic)
		}
	}
}

// TestRequestReply is the native request/reply, end to end: the server
// does the correlation and a responder answers on Properties.ReplyTo.
func TestRequestReply(t *testing.T) {
	url := serverURL(t)
	b, err := nq.OpenBackend(t.Context(), nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("opening a backend: %v", err)
	}
	defer func() { _ = b.Close() }()
	client := mq.NewClient(b)
	topic := newTopic("rpc")
	cons := consumerOn(t, client, topic, "responders")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 1)
		n, rerr := cons.Receive(ctx, dst)
		if n != 1 {
			done <- rerr
			return
		}
		reply := &mq.Message{Body: append([]byte("re: "), dst[0].Body...)}
		done <- b.Respond(ctx, dst[0].Properties.ReplyTo, reply)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reply, err := b.Request(ctx, topic, &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(reply.Body) != "re: ping" {
		t.Fatalf("reply body = %q, want %q", reply.Body, "re: ping")
	}
	if rerr := <-done; rerr != nil {
		t.Fatalf("the responder: %v", rerr)
	}
}

// TestRequestWithNoResponderIsNotFound is what native request/reply gives that
// a hand-rolled correlation does not: the server says nobody is listening, at
// once, instead of the caller waiting out its deadline.
func TestRequestWithNoResponderIsNotFound(t *testing.T) {
	b := openBackend(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := b.Request(ctx, newTopic("nobody"), &mq.Message{Body: []byte("x")})
	if err == nil {
		t.Fatal("a request nobody answers succeeded")
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Fatalf("Request with no responder = %v (code %v), want NotFound", err, code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the refusal took %v: it waited for the deadline instead of the server answer", elapsed)
	}
}

// TestConfirmDurableRefusedAtConstruction is the rule reaching a caller: a
// profile with no durable boundary refuses the publisher, not the message.
func TestConfirmDurableRefusedAtConstruction(t *testing.T) {
	c := openClient(t)
	_, err := c.Publisher(t.Context(), mq.PublisherConfig{
		Topic:        newTopic("durable"),
		Confirmation: mq.ConfirmDurable,
	})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a ConfirmDurable publisher = %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
}

// TestUnsupportedPublishControlsRefusedBeforeIO is a control this broker
// cannot honour is refused rather than ignored.
//
// Ignoring is the dangerous half. A caller that asked for a delay, a time to
// live or de-duplication and got none would have no way to find out, and the
// message would already be delivered.
func TestUnsupportedPublishControlsRefusedBeforeIO(t *testing.T) {
	c := openClient(t)
	topic := newTopic("controls")
	cons := consumerOn(t, c, topic, "workers")
	for name, opt := range map[string]mq.PublishOption{
		"Delay":     mq.Delay(time.Minute),
		"TTL":       mq.TTL(time.Minute),
		"Priority":  mq.Priority(0),
		"DedupKey":  mq.DedupKey("k"),
		"Partition": mq.Partition(0),
	} {
		err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(name)}, opt)
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("Publish with %s = %v (code %v), want ErrUnsupported", name, err, mq.CodeOf(err))
		}
	}
	// Nothing was written: a refusal that had already published would be
	// the worst of both answers.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	dst := make([]mq.Delivery, 4)
	if n, _ := cons.Receive(ctx, dst); n != 0 {
		t.Fatalf("%d messages were published by a refused publication", n)
	}
}

// TestStrictMetadataRefusesBinaryHeaders is the default mode: metadata the
// broker has no field for fails with ErrUnsupported before any I/O.
func TestStrictMetadataRefusesBinaryHeaders(t *testing.T) {
	c := openClient(t)
	topic := newTopic("strict")
	m := &mq.Message{Body: []byte("x")}
	m.Headers.Set("trace-state", []byte{0x00, 0xff, 0xfe})
	if err := c.Publish(t.Context(), topic, m); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a binary header under strict metadata = %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
}

// TestAdaptiveMetadataEnvelopesOnlyWhatItMustis the mode that costs
// nothing until it has to: a representable message goes on the wire natively,
// and one that is not goes in the portable envelope.
func TestAdaptiveMetadataEnvelopesOnlyWhatItMust(t *testing.T) {
	c := openClient(t)
	topic := newTopic("adaptive")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataAdaptive})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	plain := &mq.Message{Body: []byte("plain")}
	plain.Headers.Set("content-kind", []byte("order"))
	binary := &mq.Message{Body: []byte("binary")}
	binary.Headers.Set("trace-state", []byte{0x00, 0xff, 0xfe, 0x80})
	for _, m := range []*mq.Message{plain, binary} {
		if err := pub.Publish(t.Context(), m); err != nil {
			t.Fatalf("publishing %q: %v", m.Body, err)
		}
	}
	for _, want := range []*mq.Message{plain, binary} {
		d := receiveOne(t, cons, 5*time.Second)
		if !bytes.Equal(d.Body, want.Body) {
			t.Fatalf("body = %q, want %q", d.Body, want.Body)
		}
		for k, v := range want.Headers.All() {
			got, ok := d.Headers.Get(k)
			if !ok || !bytes.Equal(got, v) {
				t.Fatalf("header %q came back as %q (present %v), want %q", k, got, ok, v)
			}
		}
	}
}

// TestLenientDropLeavesOutWhatItCannotCarry is the lenient mode, with the
// metric the specification asks for.
func TestLenientDropLeavesOutWhatItCannotCarry(t *testing.T) {
	url := serverURL(t)
	b, err := nq.OpenBackend(t.Context(), nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("opening a backend: %v", err)
	}
	defer func() { _ = b.Close() }()
	c := mq.NewClient(b)
	topic := newTopic("lenient")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataLenientDrop})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	m := &mq.Message{Body: []byte("lenient")}
	m.Headers.Set("keep", []byte("this"))
	m.Headers.Set("drop", []byte{0x00, 0xff})
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)
	if v, ok := d.Headers.Get("keep"); !ok || string(v) != "this" {
		t.Errorf("the representable header was dropped too: %q (present %v)", v, ok)
	}
	if _, ok := d.Headers.Get("drop"); ok {
		t.Error("the unrepresentable header arrived: lenient mode did not drop it")
	}
	if n := b.DroppedMetadata(); n != 1 {
		t.Errorf("DroppedMetadata = %d, want 1", n)
	}
}

// TestPortableEnvelopeRoundTrip is the explicit mode: everything travels in
// the envelope, including what NATS headers cannot carry.
func TestPortableEnvelopeRoundTrip(t *testing.T) {
	c := openClient(t)
	topic := newTopic("envelope")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Metadata: mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	m := &mq.Message{Key: []byte{0x01, 0x02}, Body: []byte("enveloped")}
	m.Headers.Set("trace-state", []byte{0x00, 0xff, 0xfe, 0x80})
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)
	if string(d.Body) != "enveloped" {
		t.Fatalf("body = %q", d.Body)
	}
	if v, ok := d.Headers.Get("trace-state"); !ok || !bytes.Equal(v, []byte{0x00, 0xff, 0xfe, 0x80}) {
		t.Fatalf("the binary header came back as %q (present %v)", v, ok)
	}
	if !bytes.Equal(d.Key, []byte{0x01, 0x02}) {
		t.Fatalf("the key came back as %q", d.Key)
	}
}

// TestKeyAndBodyRoundTrip is the observable distinctions over the wire:
// a binary key, a nil key, a nil body and an empty body all come back as they
// went out.
func TestKeyAndBodyRoundTrip(t *testing.T) {
	c := openClient(t)
	topic := newTopic("bodies")
	cons := consumerOn(t, c, topic, "workers")
	keys := [][]byte{nil, {}, []byte("customer-7"), {0x00, 0xff}}
	bodies := [][]byte{nil, {}, []byte("plain"), {0x00, 0xff, 0x7f, 0x80}}
	for i := range keys {
		m := &mq.Message{Key: keys[i], Body: bodies[i]}
		if err := c.Publish(t.Context(), topic, m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	for i := range keys {
		d := receiveOne(t, cons, 5*time.Second)
		if (d.Key == nil) != (keys[i] == nil) || !bytes.Equal(d.Key, keys[i]) {
			t.Errorf("key %d came back as %q (nil %v), want %q (nil %v)", i, d.Key, d.Key == nil, keys[i], keys[i] == nil)
		}
		if (d.Body == nil) != (bodies[i] == nil) || !bytes.Equal(d.Body, bodies[i]) {
			t.Errorf("body %d came back as %q (nil %v), want %q (nil %v)", i, d.Body, d.Body == nil, bodies[i], bodies[i] == nil)
		}
	}
}

// TestOversizedPayloadRefused is the advertised limit being enforced: the
// driver refuses before the client library does, with the portable code.
func TestOversizedPayloadRefused(t *testing.T) {
	c := openClient(t)
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Fatal("the driver advertises no maximum message size, so nothing enforces one")
	}
	err := c.Publish(t.Context(), newTopic("big"), &mq.Message{Body: make([]byte, lim.Value+1)})
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("an oversized publication = %v (code %v), want ErrMessageTooLarge", err, mq.CodeOf(err))
	}
}

// TestStartPositionOtherThanLatestRefused is what "retains nothing" means for
// a consumer: there is no history to start from, so asking for one is refused
// rather than silently answered with Latest.
func TestStartPositionOtherThanLatestRefused(t *testing.T) {
	c := openClient(t)
	topic := newTopic("start")
	for _, pos := range []mq.StartPosition{
		mq.StartEarliest(),
		mq.StartAt(time.Now().Add(-time.Hour)),
		mq.StartAtSequence(1),
	} {
		_, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "s", Start: pos})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("a consumer starting at %v = %v (code %v), want ErrUnsupported", pos, err, mq.CodeOf(err))
		}
	}
	if _, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "s", Start: mq.StartLatest()}); err != nil {
		t.Errorf("a consumer starting at the end was refused: %v", err)
	}
}

// TestAdminUnsupported is the answer for a backend with no topology surface.
func TestAdminUnsupported(t *testing.T) {
	c := openClient(t)
	if _, err := c.Admin(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Admin = %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
}

// TestInfoAndNative is the driver says what it is and hands over the
// native client.
func TestInfoAndNative(t *testing.T) {
	c := openClient(t)
	info := c.Info()
	if info.Name != "nats" || info.Profile != "core" {
		t.Errorf("Info = %+v, want the nats core profile", info)
	}
	if info.ClientName != "github.com/nats-io/nats.go" || info.ClientVersion == "" {
		t.Errorf("Info does not name the client library: %+v", info)
	}
	if info.BrokerVersion == "" {
		t.Error("Info does not carry the connected server version")
	}
	if _, ok := c.Native().(*natsgo.Conn); !ok {
		t.Errorf("Native = %T, want *nats.Conn", c.Native())
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Errorf("Ping: %v", err)
	}
}

// TestCloseUnblocksFetch is the specification's "Close unblocks blocked operations"
// reaching the driver's own ring.
func TestCloseUnblocksFetch(t *testing.T) {
	c := openClient(t)
	topic := newTopic("closewake")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, rerr := cons.Receive(t.Context(), make([]mq.Delivery, 1))
		done <- rerr
	}()
	time.Sleep(50 * time.Millisecond)
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a parked Receive woke with %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock a parked Receive")
	}
}

// TestClientCloseUnblocksFetch is the same obligation reaching a caller who
// closed the client rather than the consumer: the backend closes what it
// opened, so the source's ring is woken too.
func TestClientCloseUnblocksFetch(t *testing.T) {
	c := openClient(t)
	topic := newTopic("clientclose")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, rerr := cons.Receive(t.Context(), make([]mq.Delivery, 1))
		done <- rerr
	}()
	time.Sleep(50 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a parked Receive woke with %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the client did not unblock a parked Receive")
	}
}

// TestBorrowedConnectionSurvivesClose is the explicit ownership: iqmq closes
// a connection it was told it owns and never one it borrowed.
func TestBorrowedConnectionSurvivesClose(t *testing.T) {
	url := serverURL(t)
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer nc.Close()
	c, err := nq.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if nc.IsClosed() {
		t.Fatal("iqmq closed a borrowed connection")
	}

	nc2, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	c2, err := nq.OpenWithClient(t.Context(), nc2, driverkit.ClientOwned, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !nc2.IsClosed() {
		t.Fatal("iqmq did not close a connection it owns")
	}
}

// TestBatchPublishIsOneFlush is the native-batch claim: every message arrives,
// in order, and the batch cost one confirmation round trip rather than one per
// message.
func TestBatchPublishIsOneFlush(t *testing.T) {
	c := openClient(t)
	topic := newTopic("batch")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 64})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	const n = 32
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte(strconv.Itoa(i))}
	}
	if err := pub.PublishBatch(t.Context(), ms); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, n)
	got := 0
	for got < n {
		m, err := cons.Receive(ctx, dst[:n-got])
		for i := range m {
			if want := strconv.Itoa(got + i); string(dst[i].Body) != want {
				t.Fatalf("delivery %d is %q, want %q: a batch lost its order", got+i, dst[i].Body, want)
			}
		}
		got += m
		if err != nil {
			t.Fatalf("after %d of %d: %v", got, n, err)
		}
	}
}

// TestBatchPublishRefusesTheWholeBatchBeforeIO is again, for a batch: a
// batch with one unpublishable message publishes none of it, because a batch
// half refused before it started has no index alignment to report.
func TestBatchPublishRefusesTheWholeBatchBeforeIO(t *testing.T) {
	c := openClient(t)
	topic := newTopic("batchbad")
	cons := consumerOn(t, c, topic, "workers")
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	bad := &mq.Message{Body: []byte("bad")}
	bad.Headers.Set("trace-state", []byte{0x00, 0xff})
	err = pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("good")}, bad})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("PublishBatch with an unrepresentable message = %v, want ErrUnsupported", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if n, _ := cons.Receive(ctx, make([]mq.Delivery, 4)); n != 0 {
		t.Fatalf("%d messages of a refused batch were published", n)
	}
}

// TestSlowConsumerDegradesHealth is for the failure this broker really
// has: a subscriber that falls behind its pending limits loses messages, and
// nothing on the consuming path reports it.
//
// The health transition is the only place it surfaces, which is why a driver
// that did not wire nats.go's asynchronous error handler would leave a caller
// polling a Ready client while its messages went missing.
func TestSlowConsumerDegradesHealth(t *testing.T) {
	url := serverURL(t)
	b, err := nq.OpenBackend(t.Context(), nq.Config{
		Addrs: []string{url},
		// One message and a few bytes: the smallest bound nats.go will
		// take, so the drop happens in milliseconds rather than seconds.
		PendingMessages: 1,
		PendingBytes:    256,
	})
	if err != nil {
		t.Fatalf("opening a backend: %v", err)
	}
	defer func() { _ = b.Close() }()
	c := mq.NewClient(b)
	topic := newTopic("slow")
	// Prefetch 1, and nothing ever fetches: the ring fills, the callback
	// parks, and the client pending queue overflows.
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "slow", Prefetch: 1})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	deadline := time.Now().Add(20 * time.Second)
	body := bytes.Repeat([]byte("x"), 512)
	for time.Now().Before(deadline) {
		for range 200 {
			if err := pub.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		if b.SlowConsumers() > 0 && c.Health().State == mq.HealthDegraded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("a subscriber that never fetched did not become a slow consumer: health %v, slow %d",
		c.Health().State, b.SlowConsumers())
}

// TestPushAdapterBoundsWhatIsHeld is the specification's "all queues bounded" for the ring
// between the native callback and Fetch: a consumer never hands out more than
// its prefetch in one fetch.
func TestPushAdapterBoundsWhatIsHeld(t *testing.T) {
	c := openClient(t)
	topic := newTopic("bounded")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	for i := range 32 {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	n, _ := cons.Receive(ctx, make([]mq.Delivery, 64))
	if n == 0 {
		t.Fatal("nothing arrived")
	}
	if n > 4 {
		t.Fatalf("a consumer with a prefetch of 4 handed out %d deliveries in one fetch", n)
	}
}

// TestPublishAfterCloseAndIdempotentClose is the lifecycle on both handles.
func TestPublishAfterCloseAndIdempotentClose(t *testing.T) {
	c := openClient(t)
	topic := newTopic("lifecycle")
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
}
