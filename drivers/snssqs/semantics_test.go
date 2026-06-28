package snssqs_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	sns "bitbucket.org/iqhive/iqmq/drivers/snssqs"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
)

// TestPublishedBodiesArriveByteForByte is the promise across the whole
// path: the SNS character set, the fan-out and the queue.
//
// The bodies are the ones that break a text transport: nil, which is not the
// same as empty and which neither service will carry; empty, which SNS refuses
// outright; a plain string; arbitrary bytes, which need the base64 policy; and
// a large payload, which exercises the size accounting.
func TestPublishedBodiesArriveByteForByte(t *testing.T) {
	h := open(t)
	tp := topic(t, "bodies")
	h.provision(tp, "s")
	c := h.consumer(tp, "s", 8)

	bodies := [][]byte{nil, {}, []byte("plain"), {0x00, 0xff, 0x7f, 0x80}, bytes.Repeat([]byte{0xa5}, 64<<10)}
	for _, body := range bodies {
		h.publish(tp, &mq.Message{Body: body})
	}
	got := h.receive(c, len(bodies), 10*time.Second)
	seen := make([][]byte, 0, len(bodies))
	for i := range got {
		seen = append(seen, bytes.Clone(got[i].Body))
		if err := got[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	for _, want := range bodies {
		found := false
		for _, have := range seen {
			if bytes.Equal(have, want) && (have == nil) == (want == nil) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("no delivery carries the published body %q (nil=%v); got %q", want, want == nil, seen)
		}
	}
}

// TestMetadataSurvivesTheFanOut holds the promise for each of the four
// modes, through raw message delivery.
//
// The interesting mode is MetadataAdaptive with more metadata than ten
// attributes can carry: the message travels as an iqmq envelope in the SNS
// body, SNS delivers the body unchanged, and the SQS source unwraps it. A
// driver that enveloped on publish and did not unwrap on receive would hand
// the application framing instead of a payload, and only a round trip can see
// it.
func TestMetadataSurvivesTheFanOut(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode mq.MetadataMode
		// header is the header key the message carries besides the three
		// plain ones. A key that is not a legal message-attribute name is
		// what forces the envelope: the OTHER trigger — more metadata than
		// ten attributes hold — is unreachable through Client.Publish,
		// because the core enforces the driver's own advertised
		// MaxHeaderCount of nine before the driver sees the message
		// (recorded in the maintenance notes).
		header string
		// enveloped is whether the body on the queue should be iqmq
		// framing rather than the payload.
		enveloped bool
	}{
		{"native-strict", mq.MetadataNativeStrict, "plain", false},
		{"adaptive-native", mq.MetadataAdaptive, "plain", false},
		{"adaptive-enveloped", mq.MetadataAdaptive, "not a legal.attribute name", true},
		{"portable-envelope", mq.MetadataPortableEnvelope, "plain", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			h := open(t)
			tp := topic(t, "meta")
			h.provision(tp, "s")
			c, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{
				Topic: tp, Subscription: "s", Metadata: mode.mode,
			})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = c.Close() }()
			p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp, Metadata: mode.mode})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()

			var m mq.Message
			m.Body = []byte("payload")
			m.Key = []byte("key-1")
			m.ID = "id-1"
			m.Properties.ContentType = "application/json"
			m.Properties.CorrelationID = "corr-1"
			headers := []string{"h0", "h1", mode.header}
			for i, name := range headers {
				m.Headers.Set(name, []byte("v"+strconv.Itoa(i)))
			}
			if err := p.Publish(t.Context(), &m); err != nil {
				t.Fatalf("Publish under %v with the header %q: %v", mode.mode, mode.header, err)
			}

			// What is really on the queue, which is what makes the two
			// adaptive rows different tests rather than one test twice.
			bodies := h.bodies(tp, "s")
			if len(bodies) != 1 {
				t.Fatalf("the subscription's queue holds %d message(s), want 1", len(bodies))
			}
			if wrapped := bodies[0] != "payload"; wrapped != mode.enveloped {
				t.Fatalf("the body on the queue is %q, which is %s the envelope; want %s",
					bodies[0],
					map[bool]string{true: "in", false: "not in"}[wrapped],
					map[bool]string{true: "enveloped", false: "the payload unchanged"}[mode.enveloped])
			}

			d := h.receiveOne(c, 5*time.Second)
			switch {
			case string(d.Body) != "payload":
				t.Fatalf("the body is %q, want the payload: an enveloped message that was not unwrapped shows up here", d.Body)
			case string(d.Key) != "key-1":
				t.Fatalf("the key is %q", d.Key)
			case d.ID != "id-1":
				t.Fatalf("the identifier is %q", d.ID)
			case d.Properties.ContentType != "application/json":
				t.Fatalf("the content type is %q", d.Properties.ContentType)
			case d.Properties.CorrelationID != "corr-1":
				t.Fatalf("the correlation identifier is %q", d.Properties.CorrelationID)
			case d.Headers.Len() != len(headers):
				t.Fatalf("the delivery carries %d header(s), want %d", d.Headers.Len(), len(headers))
			}
			for i, name := range headers {
				if got, ok := d.Headers.Get(name); !ok || string(got) != "v"+strconv.Itoa(i) {
					t.Fatalf("header %q is %q (present %v)", name, got, ok)
				}
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			if got := d.Info().Topic; got != tp {
				t.Fatalf("the delivery names the topic %q, want the SNS topic %q: the inner driver is opened with the QUEUE name as its topic, and the specification wants the topic the message was published to", got, tp)
			}
		})
	}
}

// TestADelayedPublicationIsRefusedAtTheSink is the SNS difference a caller
// porting from `drivers/sqs` meets first.
//
// A queue has DelaySeconds and SNS has no per-message timer at all, so the
// option has to be refused rather than dropped — the specification makes silently dropping
// an unsupported option a disqualifier, and the SQS driver's shared encoder
// would happily map it to a field SNS's Publish has no parameter for.
//
// It goes through the SPI rather than the client, and deliberately: the CORE
// already refuses a delay for a driver that does not advertise
// FeatureScheduledPublish, so a test through the client would pass with the
// driver's own refusal deleted. Checking the sink is what holds the driver rather than holding the core to it.
func TestADelayedPublicationIsRefusedAtTheSink(t *testing.T) {
	h := open(t)
	tp := topic(t, "delay")
	h.provision(tp, "s")
	sink, err := h.back.OpenSink(t.Context(), mq.SinkConfig{Topic: tp})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()

	for _, tc := range []struct {
		name string
		opts mq.PublishOptions
	}{
		{"relative delay", mq.PublishOptions{Delay: 30 * time.Second}},
		{"absolute time", mq.PublishOptions{DeliverAt: h.queues.Now().Add(30 * time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sink.Publish(t.Context(), &mq.Message{Body: []byte("later")}, &tc.opts)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("publishing with a %s reported %v (code %v), want ErrUnsupported: SNS has no per-message timer, so a driver that accepted it would drop the guarantee",
					tc.name, err, mq.CodeOf(err))
			}
			if !containsAll(err.Error(), "per-message timer") {
				t.Fatalf("the refusal does not say what is missing: %v", err)
			}
			if got := h.depth(tp, "s"); got != 0 {
				t.Fatalf("the refused publication reached %d queue message(s): a refusal before I/O must not publish", got)
			}
		})
	}

	// The same sink publishes fine with no delay, so the refusal is about
	// the option rather than about the sink.
	if err := sink.Publish(t.Context(), &mq.Message{Body: []byte("now")}, nil); err != nil {
		t.Fatalf("publishing with no delay: %v", err)
	}
	if got := h.depth(tp, "s"); got != 1 {
		t.Fatalf("the undelayed publication reached %d queue message(s), want 1", got)
	}
}

// TestUnsupportedPublicationOptionsAreRefused is for the rest of the
// option set, which this driver refuses through the SQS driver's shared
// encoder.
//
// It goes through the SPI for the same reason the delay test does: the core
// refuses most of these itself for a driver whose table does not claim them,
// so a client-level test would pass with the driver's half deleted.
func TestUnsupportedPublicationOptionsAreRefused(t *testing.T) {
	h := open(t)
	tp := topic(t, "options")
	h.provision(tp, "s")
	sink, err := h.back.OpenSink(t.Context(), mq.SinkConfig{Topic: tp})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()

	for _, tc := range []struct {
		name string
		opts mq.PublishOptions
	}{
		{"time to live", mq.PublishOptions{TTL: time.Minute}},
		{"priority", mq.PublishOptions{Priority: 5, PrioritySet: true}},
		{"partition", mq.PublishOptions{Partition: 3, PartitionSet: true}},
		{"deduplication key", mq.PublishOptions{DedupKey: "d-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sink.Publish(t.Context(), &mq.Message{Body: []byte("x")}, &tc.opts)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("publishing with a %s reported %v (code %v), want ErrUnsupported", tc.name, err, mq.CodeOf(err))
			}
		})
	}
}

// TestAnOversizedPublicationIsRefusedAtSNSsLimit is the second difference from
// the queue driver: 256 KiB rather than a mebibyte.
//
// A body between the two is accepted by `drivers/sqs` and refused here, and
// the refusal is ErrMessageTooLarge before any I/O.
func TestAnOversizedPublicationIsRefusedAtSNSsLimit(t *testing.T) {
	h := open(t)
	tp := topic(t, "size")
	h.provision(tp, "s")
	sink, err := h.back.OpenSink(t.Context(), mq.SinkConfig{Topic: tp})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()

	// A body a queue would take and SNS will not.
	big := bytes.Repeat([]byte("x"), sns.MaxMessageBytes+1)
	if err := sink.Publish(t.Context(), &mq.Message{Body: big}, nil); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("publishing %d bytes reported %v (code %v), want ErrMessageTooLarge", len(big), err, mq.CodeOf(err))
	}
	if got := h.depth(tp, "s"); got != 0 {
		t.Fatalf("the refused publication reached %d queue message(s)", got)
	}
	// And one just inside the limit is accepted, so the limit is the limit
	// rather than a refusal of everything large.
	ok := bytes.Repeat([]byte("x"), sns.MaxMessageBytes-1024)
	if err := sink.Publish(t.Context(), &mq.Message{Body: ok}, nil); err != nil {
		t.Fatalf("publishing %d bytes: %v", len(ok), err)
	}
}

// TestAFIFOTopicNameIsRefused holds an SNS FIFO topic is a
// different capability table and this driver does not implement it, so the
// name is refused rather than accepted into a profile that does not exist.
func TestAFIFOTopicNameIsRefused(t *testing.T) {
	h := open(t)
	_, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders.fifo"})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("opening a publisher on a .fifo topic reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
	if !containsAll(err.Error(), "FIFO") {
		t.Fatalf("the refusal does not say what was refused: %v", err)
	}
}

// TestAWildcardTopicIsRefused is the negative direction for
// FeatureWildcardSubscribe, proved rather than assumed.
//
// SNS has no pattern subscription: a subscription names one topic ARN. The
// refusal comes before the portable topic grammar, because a metacharacter is
// outside the portable segment alphabet and mq.ValidateTopic would refuse it
// as an invalid topic — and the conformance kit's negative probe reads
// ErrUnsupported and nothing else.
func TestAWildcardTopicIsRefused(t *testing.T) {
	for _, pattern := range []string{"orders.*", "orders-*", "orders-#", "orders->", "orders-?"} {
		if _, err := sns.TopicName(pattern, ""); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("TopicName(%q) reported %v (code %v), want ErrUnsupported", pattern, err, mq.CodeOf(err))
		}
	}
}

// TestASubscriptionThatDoesNotExistIsRefusedNamingAutoCreate is the specification's
// provisioning opt-in.
//
// A fan-out has three things to create — a topic, a queue and a subscription —
// so a driver that provisioned by default would turn a typo into three
// resources. The refusal has to NAME the setting, or a consumer that receives
// nothing for ever is a mystery.
func TestASubscriptionThatDoesNotExistIsRefusedNamingAutoCreate(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.AutoCreate = false })
	tp := topic(t, "nocreate")
	_, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
	if err == nil {
		t.Fatalf("opening a consumer with AutoCreate off created the topology")
	}
	if !containsAll(err.Error(), "AutoCreate") {
		t.Fatalf("the refusal does not name the setting: %v", err)
	}
	if got := h.sns.TopicARN(h.topicName(tp)); got != "" {
		t.Fatalf("the refused open created the topic %q", got)
	}
}

// TestATopicARNIsUsedVerbatimAndVerified is the escape hatch for a program
// that already holds the ARN.
//
// It is VERIFIED rather than trusted, because a publication to an ARN that
// names nothing fails per message rather than at open — which is a
// configuration mistake reported once per message for the life of the
// process.
//
// It goes through the SPI, and that is a fact about the portable API rather
// than about this driver: mq.ValidateTopic refuses a name with colons in it,
// so an ARN never reaches a driver through Client.Publisher at all. A caller
// using the portable client names topics and maps them with
// [sns.Config.TopicARNs]; this branch is for a caller composing the SPI
// directly, and the driver page says so.
func TestATopicARNIsUsedVerbatimAndVerified(t *testing.T) {
	h := open(t)
	tp := topic(t, "arn")
	h.provision(tp, "s")
	arn := h.sns.TopicARN(h.topicName(tp))
	if arn == "" {
		t.Fatalf("the harness did not create the topic")
	}
	sink, err := h.back.OpenSink(t.Context(), mq.SinkConfig{Topic: arn})
	if err != nil {
		t.Fatalf("opening a sink on the topic ARN %q: %v", arn, err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Body: []byte("by-arn")}, nil); err != nil {
		t.Fatalf("publishing to the topic ARN: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("closing the sink: %v", err)
	}
	if got := h.bodies(tp, "s"); len(got) != 1 || got[0] != "by-arn" {
		t.Fatalf("the subscription's queue holds %v, want the publication made by ARN", got)
	}

	absent := arn + "-does-not-exist"
	_, err = h.back.OpenSink(t.Context(), mq.SinkConfig{Topic: absent})
	if err == nil {
		t.Fatalf("opening a sink on an ARN that names no topic succeeded: the failure would then be reported once per message for the life of the process")
	}
	if mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("the refusal reported code %v (%v), want CodeNotFound", mq.CodeOf(err), err)
	}

	// Config.TopicARNs is the same escape hatch for a caller using the
	// portable client, and it is the reachable one.
	byMap := open(t, func(c *sns.Config) { c.TopicARNs = map[string]string{"orders": arn} })
	if _, err := byMap.client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"}); err != nil {
		t.Fatalf("opening a publisher on a topic mapped to an ARN: %v", err)
	}
}

// TestExtendDoesNotShortenThisFacadesLease is the inherited half.
//
// This driver has no settler of its own: source.Settler hands back the inner
// SQS source's, wrapped so every optional surface survives the facade.
// So the defect and the fix are both drivers/sqs's, and
// what this test says is that the delegation really does carry them — a facade
// that had grown a settler of its own, or that forwarded only mq.Settler,
// would answer differently here and nothing else in this package would notice.
//
// The window is thirty seconds and the extension one, so an extension measured
// from NOW rather than from the deadline leaves one second and the Ack below
// reports ErrLeaseLost.
func TestExtendDoesNotShortenThisFacadesLease(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.AckDeadline = 30 * time.Second })
	tp := topic(t, "extend")
	h.provision(tp, "a")
	ca := h.consumer(tp, "a", 1)
	h.publish(tp, &mq.Message{Body: []byte("slow")})

	d := h.receiveOne(ca, 5*time.Second)
	if err := d.Extend(time.Second); err != nil {
		t.Fatalf("Extend(1s): %v", err)
	}
	// Past the extension asked for, and well short of the window the delivery
	// started with.
	h.queues.Advance(1500 * time.Millisecond)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack 1.5s after Extend(1s) on a 30s window: %v; the specification makes an extension move the deadline at least 1s BEYOND the one it had, and this facade's settler is the inner driver's ", err)
	}
}

// TestNackAndExtendActOnOneCopy holds the settlement half of the fan-out for
// the actions that are not an Ack.
//
// A requeue makes THIS subscription's copy visible again and increments ITS
// attempt count; an extension moves THIS copy's window. Neither is visible to
// another subscription, and the attempt count is the observable that says so.
func TestNackAndExtendActOnOneCopy(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.AckDeadline = 30 * time.Second })
	tp := topic(t, "nack")
	h.provision(tp, "a", "b")
	ca := h.consumer(tp, "a", 4)
	cb := h.consumer(tp, "b", 4)
	h.publish(tp, &mq.Message{Body: []byte("retry")})

	first := h.receiveOne(ca, 5*time.Second)
	if first.Attempt != 1 {
		t.Fatalf("the first delivery reports attempt %d, want 1", first.Attempt)
	}
	if err := first.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again := h.receiveOne(ca, 5*time.Second)
	if again.Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2: a requeue increments this copy's ApproximateReceiveCount", again.Attempt)
	}
	if err := again.Extend(time.Minute); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack after Extend: %v", err)
	}

	// The other subscription's copy never moved.
	other := h.receiveOne(cb, 5*time.Second)
	if other.Attempt != 1 {
		t.Fatalf("subscription b's copy reports attempt %d after subscription a nacked ITS copy, want 1", other.Attempt)
	}
	if err := other.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestRejectRoutesOneCopyToTheDeadLetterQueue is the route, per
// subscription.
//
// The destination is a queue, which is the only place a fan-out can put one: a
// message N subscribers failed to handle failed N times independently, so
// there is no dead-lettering "the publication".
func TestRejectRoutesOneCopyToTheDeadLetterQueue(t *testing.T) {
	h := open(t, func(c *sns.Config) {
		c.DeadLetterQueue = "iqmq-dlq"
		c.MaxReceiveCount = 5
	})
	tp := topic(t, "dlq")
	h.provision(tp, "a", "b")
	ca := h.consumer(tp, "a", 4)
	h.publish(tp, &mq.Message{Body: []byte("poison")})

	d := h.receiveOne(ca, 5*time.Second)
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if got := h.depth(tp, "a"); got != 0 {
		t.Fatalf("subscription a's queue holds %d message(s) after a Reject, want 0", got)
	}
	if got := h.queues.Bodies("iqmq-dlq"); len(got) != 1 || got[0] != "poison" {
		t.Fatalf("the dead-letter queue holds %v, want the rejected body once", got)
	}
	if got := h.depth(tp, "b"); got != 1 {
		t.Fatalf("subscription b's queue holds %d message(s) after subscription a rejected ITS copy, want 1: a message dead-lettered for one subscription is still in flight for the rest", got)
	}
}

// TestHealthReportsTheWorseOfTheTwoHalves holds for a driver that is two
// drivers.
//
// Everything that can fail in the background is a poller, and the pollers are
// the inner driver's. A Health that answered only this backend's own state
// would report Ready for a client whose consumption has been failing for an
// hour.
func TestHealthReportsTheWorseOfTheTwoHalves(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Queues: queues})
	h := openOn(t, server, queues)
	tp := topic(t, "health")
	h.provision(tp, "s")
	if got := h.client.Health().State; got != mq.HealthReady {
		t.Fatalf("a freshly opened client reports %v, want ready", got)
	}
	// Fault the RECEIVE, which is the inner driver's background work, and
	// keep the fault in place long enough for a poller to meet it.
	queues.Fault(sqsfake.Fault{Op: "ReceiveMessage", Err: errFaulted, Count: -1})
	c := h.consumer(tp, "s", 4)
	_ = c
	waitFor(t, 5*time.Second, "the fan-out client to report the inner driver's degradation", func() bool {
		return h.client.Health().State == mq.HealthDegraded
	})
	snap := h.client.Health()
	if snap.Err == nil {
		t.Fatalf("the degraded snapshot carries no reason")
	}
	if !strings.Contains(snap.Err.Error(), "faulted") {
		t.Fatalf("the degraded snapshot's reason is %v, want the poller's own failure", snap.Err)
	}
}

// TestTheCompositionKeepsEveryOptionalSettlementSurface holds the one thing
// the composition could silently lose.
//
// The specification makes the batched settlement and the lease extension OPTIONAL surfaces on
// a settler, and the value `drivers/sqs`'s source hands over implements both —
// through `driverkit.WrapSettler`, which was repaired for exactly this
// (`BatchSettler.Settler()` used to return something that
// implemented Settler and DeadlineExtender and NOT mq.BatchSettler). A fan-out
// source that wrapped the inner settler in a facade of its own would forward
// `mq.Settler` and retire the other two, and nothing in the portable API would
// say so: the core would just start settling one delivery at a time and
// `Delivery.Extend` would report ErrUnsupported.
//
// So the source hands the inner value straight back, and this is the assertion
// that it does — through the SPI, because the type is what is at stake.
func TestTheCompositionKeepsEveryOptionalSettlementSurface(t *testing.T) {
	h := open(t)
	tp := topic(t, "surfaces")
	h.provision(tp, "s")
	src, err := h.back.OpenSource(t.Context(), mq.SourceConfig{
		Topic: tp, Subscription: "s", Prefetch: 4,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	settler := src.Settler()
	if _, ok := settler.(mq.BatchSettler); !ok {
		t.Errorf("the fan-out source's settler (%T) does not implement mq.BatchSettler; the batched settlement would be silently retired and the core would settle one delivery per request", settler)
	}
	if _, ok := settler.(mq.DeadlineExtender); !ok {
		t.Errorf("the fan-out source's settler (%T) does not implement mq.DeadlineExtender; Delivery.Extend would report ErrUnsupported", settler)
	}

	// And the batched surface really settles: a surface that type-asserts and
	// then refuses would pass the two checks above.
	h.publish(tp, &mq.Message{Body: []byte("a")})
	h.publish(tp, &mq.Message{Body: []byte("b")})
	dst := make([]mq.DriverDelivery, 2)
	got := 0
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for got < 2 {
		n, err := src.Fetch(ctx, dst[got:])
		got += n
		if err != nil {
			t.Fatalf("Fetch: took %d of 2: %v", got, err)
		}
	}
	batch, ok := settler.(mq.BatchSettler)
	if !ok {
		t.Fatalf("no batched settlement to exercise")
	}
	refs := []*mq.DriverDelivery{&dst[0], &dst[1]}
	if err := batch.AckBatch(t.Context(), refs); err != nil {
		t.Fatalf("AckBatch of two deliveries: %v", err)
	}
	if n := h.depth(tp, "s"); n != 0 {
		t.Fatalf("the subscription's queue holds %d message(s) after a batched acknowledgement, want 0", n)
	}
}

// TestCloseIsIdempotentAndStopsEverything holds the close contract across the
// composition: this backend's children, the inner backend's children, and the
// inner backend itself.
//
// goleak in TestMain is the other half: every subscription is a set of
// pollers, and a fan-out opens one set per subscription.
func TestCloseIsIdempotentAndStopsEverything(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Queues: queues})
	back, err := sns.BackendWithAPI(t.Context(), server, queues, queues, server, ownBorrowed(), sns.Config{
		Region: "fake-region-1", AccountID: "000000000000", AutoCreate: true,
		WaitTime: time.Second, AckDeadline: 30 * time.Second, Clock: clock{s: queues},
	})
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	client := mq.NewClient(back)
	tp := "t-close-1"
	admin, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: tp, Name: "s"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if _, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if _, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp}); err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("a second Close reported %v, want the first one's answer", err)
	}
	if got := back.Health().State; got != mq.HealthClosed {
		t.Fatalf("a closed backend reports %v, want closed", got)
	}
	if _, err := back.OpenSink(t.Context(), mq.SinkConfig{Topic: tp}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("opening a sink on a closed backend reported %v, want ErrClosed", err)
	}
	if _, err := back.OpenSource(t.Context(), mq.SourceConfig{Topic: tp, Subscription: "s", Prefetch: 1}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("opening a source on a closed backend reported %v, want ErrClosed", err)
	}
	// The inner backend is closed too, which is the composition's own
	// obligation: a caller that only ever saw this backend has no handle on
	// the other one.
	if got := back.SQS().Health().State; got != mq.HealthClosed {
		t.Fatalf("the inner SQS backend reports %v after the fan-out backend closed, want closed", got)
	}
}
