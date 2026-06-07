package gcppubsub_test

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
)

// MaxAttributeValue is the service's attribute-value limit, named locally so
// that the test reads as the rule rather than as a number.
const MaxAttributeValue = gps.MaxAttributeValueBytes

// The metadata mapping is pure and every limit is checked before any I/O,
// which is what the specification asks of a driver. These tests go through the whole
// client, because the mapping is unexported and a test that reached into it
// would be testing a copy of the driver rather than the driver.

// TestRoundTripPreservesNilAndEmpty is the distinction, and the reason
// this driver spends a reserved attribute on it.
//
// Protobuf does not distinguish an absent `bytes` field from a present empty
// one: both decode to a nil slice. A driver with no marker therefore reports
// every empty body as nil, which is what `publish/roundtrip-bodies` exists to
// catch.
func TestRoundTripPreservesNilAndEmpty(t *testing.T) {
	l := newLive(t)
	c := l.open(t, func(cfg *gps.Config) { cfg.Ordering = true })
	for _, tc := range []struct {
		name string
		body []byte
		key  []byte
	}{
		{"nil-body-nil-key", nil, nil},
		{"empty-body-empty-key", []byte{}, []byte{}},
		{"nil-body-empty-key", nil, []byte{}},
		{"empty-body-nil-key", []byte{}, nil},
		{"text", []byte("hello"), []byte("customer-7")},
		{"binary", []byte{0x00, 0xff, 0x7f}, []byte{0x00, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("rt")
			provision(t, c, topic, "s")
			ctx := context.Background()
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			if err := c.Publish(ctx, topic, &mq.Message{Body: tc.body, Key: tc.key}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := receiveOne(t, cons, 10*time.Second)
			if (d.Body == nil) != (tc.body == nil) {
				t.Errorf("the body came back nil=%v, want nil=%v", d.Body == nil, tc.body == nil)
			}
			if !bytes.Equal(d.Body, tc.body) {
				t.Errorf("the body came back %q, want %q", d.Body, tc.body)
			}
			if (d.Key == nil) != (tc.key == nil) {
				t.Errorf("the key came back nil=%v, want nil=%v", d.Key == nil, tc.key == nil)
			}
			if !bytes.Equal(d.Key, tc.key) {
				t.Errorf("the key came back %q, want %q", d.Key, tc.key)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestHeadersAndPropertiesRoundTrip covers the attributes a message spends on the typed properties and on the caller's own headers.
func TestHeadersAndPropertiesRoundTrip(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	topic := newTopic("hdr")
	provision(t, c, topic, "s")
	ctx := context.Background()
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	created := time.Date(2026, 9, 14, 12, 0, 0, 123456789, time.UTC)
	m := &mq.Message{Body: []byte("x"), ID: "app-1"}
	m.Headers.Add("trace", []byte("abc"))
	m.Headers.Add("tenant", []byte("acme"))
	m.Properties = mq.Properties{
		ContentType: "application/json", ContentEncoding: "gzip",
		CorrelationID: "corr-1", CausationID: "cause-1", ReplyTo: "replies",
		CreatedAt: created,
	}
	if err := c.Publish(ctx, topic, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 10*time.Second)
	if d.ID != "app-1" {
		t.Errorf("the identifier came back %q", d.ID)
	}
	if got, _ := d.Headers.Get("trace"); string(got) != "abc" {
		t.Errorf("the trace header came back %q", got)
	}
	if got, _ := d.Headers.Get("tenant"); string(got) != "acme" {
		t.Errorf("the tenant header came back %q", got)
	}
	if d.Properties.ContentType != "application/json" || d.Properties.ContentEncoding != "gzip" ||
		d.Properties.CorrelationID != "corr-1" || d.Properties.CausationID != "cause-1" ||
		d.Properties.ReplyTo != "replies" {
		t.Errorf("the typed properties came back %+v", d.Properties)
	}
	if !d.Properties.CreatedAt.Equal(created) {
		t.Errorf("CreatedAt came back %v, want %v", d.Properties.CreatedAt, created)
	}
	// No reserved attribute is visible as a header: a caller that iterated
	// the headers must not see this driver's own bookkeeping.
	for k := range d.Headers.All() {
		if strings.HasPrefix(k, "iqmq-") {
			t.Errorf("the reserved attribute %q is visible as a header", k)
		}
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestMetadataModesAnswerTheAttributeLimits is the four answers for the
// things a Pub/Sub attribute cannot carry, and the distinction the test exists
// to draw is between the ones an ENVELOPE fixes and the one it does not.
//
// A binary value, a repeated key and a value over the service's 1024 bytes are
// all things the portable envelope carries happily, so MetadataAdaptive
// envelopes them. A header key over 256 bytes is not: the service's
// attribute-name limit and the envelope decoder's MaxHeaderKey are the SAME
// 256 bytes, so there is no representation for it in either half and every
// mode but MetadataLenientDrop refuses it. A driver that enveloped it anyway
// would publish a record its own Unwrap refuses for ever — undeliverable,
// therefore unsettleable — which is the hole and the specification
// record.
func TestMetadataModesAnswerTheAttributeLimits(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	for _, shape := range []struct {
		name string
		add  func(*mq.Message)
		// envelopeFixes says whether the portable envelope can carry this,
		// which is what decides MetadataAdaptive's answer.
		envelopeFixes bool
	}{
		{"binary-value", func(m *mq.Message) { m.Headers.Add("h", []byte{0x00, 0xff}) }, true},
		{"repeated-key", func(m *mq.Message) {
			m.Headers.Add("h", []byte("a"))
			m.Headers.Add("h", []byte("b"))
		}, true},
		{"value-too-long", func(m *mq.Message) {
			m.Headers.Add("h", []byte(strings.Repeat("v", MaxAttributeValue+1)))
		}, true},
		{"key-too-long", func(m *mq.Message) {
			m.Headers.Add(strings.Repeat("k", 300), []byte("v"))
		}, false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			for _, mode := range []struct {
				name string
				mode mq.MetadataMode
				// drops says the mode publishes without the metadata.
				drops bool
			}{
				{"strict", mq.MetadataNativeStrict, false},
				{"adaptive", mq.MetadataAdaptive, false},
				{"envelope", mq.MetadataPortableEnvelope, false},
				{"drop", mq.MetadataLenientDrop, true},
			} {
				t.Run(mode.name, func(t *testing.T) {
					refuses := !mode.drops &&
						(mode.mode == mq.MetadataNativeStrict || !shape.envelopeFixes)
					topic := newTopic("meta")
					provision(t, c, topic, "s")
					ctx := context.Background()
					pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic, Metadata: mode.mode})
					if err != nil {
						t.Fatalf("Publisher: %v", err)
					}
					defer func() { _ = pub.Close() }()
					m := &mq.Message{Body: []byte("payload")}
					shape.add(m)
					err = pub.Publish(ctx, m)
					if refuses {
						if err == nil {
							t.Fatalf("%v accepted metadata neither the attributes nor the envelope can carry; the specification requires the refusal before any I/O", mode.mode)
						}
						if code := mq.CodeOf(err); code != mq.CodeUnsupported && code != mq.CodeMessageTooLarge && code != mq.CodeInvalidArgument {
							t.Fatalf("the refusal reported code %v: %v", code, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("%v refused the publication: %v", mode.mode, err)
					}
					cons, cerr := c.Consumer(ctx, mq.ConsumerConfig{
						Topic: topic, Subscription: "s", Metadata: mode.mode,
					})
					if cerr != nil {
						t.Fatalf("Consumer: %v", cerr)
					}
					defer func() { _ = cons.Close() }()
					d := receiveOne(t, cons, 10*time.Second)
					if string(d.Body) != "payload" {
						t.Errorf("the body came back %q", d.Body)
					}
					if err := d.Ack(); err != nil {
						t.Fatalf("Ack: %v", err)
					}
				})
			}
		})
	}
}

// TestPublicationControlsThisBrokerRefuses is an option the driver
// cannot honour is refused before any I/O rather than silently ignored.
func TestPublicationControlsThisBrokerRefuses(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	topic := newTopic("opts")
	provision(t, c, topic, "s")
	ctx := context.Background()
	pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	for _, tc := range []struct {
		name string
		opt  mq.PublishOption
	}{
		{"delay", mq.Delay(time.Minute)},
		{"ttl", mq.TTL(time.Minute)},
		{"priority", mq.Priority(3)},
		{"partition", mq.Partition(1)},
		{"dedup-key", mq.DedupKey("k")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pub.Publish(ctx, &mq.Message{Body: []byte("x")}, tc.opt)
			if err == nil {
				t.Fatal("the publication was accepted; the specification requires a driver to refuse an option it cannot honour")
			}
			if code := mq.CodeOf(err); code != mq.CodeUnsupported {
				t.Fatalf("the refusal reported code %v, want Unsupported: %v", code, err)
			}
		})
	}
}

// TestOversizedPublicationIsRefusedBeforeAnyIO holds the size accounting to
// the advertised limit.
func TestOversizedPublicationIsRefusedBeforeAnyIO(t *testing.T) {
	c, _ := scripted(t)
	topic := newTopic("size")
	provision(t, c, topic, "s")
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Fatal("the driver advertises no message-size limit")
	}
	err := c.Publish(context.Background(), topic, &mq.Message{Body: make([]byte, lim.Value+1)})
	if code := mq.CodeOf(err); code != mq.CodeMessageTooLarge {
		t.Fatalf("a publication one byte over the advertised limit reported %v (code %v), want MessageTooLarge", err, code)
	}
}

// TestReservedAttributeCollisionIsRefusedInEveryMode is the specification's "conflicts
// rejected": a caller's header may not name one of this driver's own
// attributes, and no metadata mode makes that acceptable — an envelope would
// carry it and the decoder would then hand it back as a header the driver
// wrote.
func TestReservedAttributeCollisionIsRefusedInEveryMode(t *testing.T) {
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataAdaptive,
		mq.MetadataPortableEnvelope, mq.MetadataLenientDrop,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			c, _ := scripted(t)
			topic := newTopic("reserved")
			provision(t, c, topic, "s")
			ctx := context.Background()
			pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic, Metadata: mode})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = pub.Close() }()
			for _, key := range []string{"iqmq-k", "iqmq-anything", "goog-reserved", "googclient_x"} {
				m := &mq.Message{Body: []byte("x")}
				m.Headers.Add(key, []byte("v"))
				if err := pub.Publish(ctx, m); err == nil {
					t.Errorf("a header named %q was accepted in %v", key, mode)
				}
			}
		})
	}
}

// TestTopicAndSubscriptionMappingIsInjective is the property the "/" → "~"
// mapping exists for, and the one `drivers/sqs`'s "/" → "-" mapping does not
// have.
func TestTopicAndSubscriptionMappingIsInjective(t *testing.T) {
	cfg := gps.Config{ProjectID: "p"}
	seen := map[string]string{}
	for _, topic := range []string{"orders", "orders/eu", "orders/eu/west", "a", "a/b", "a~b"} {
		id, err := cfg.TopicID(topic)
		if topic == "a~b" {
			// "~" is not in mq.ValidateTopic's grammar, so a portable caller
			// cannot write it and the collision cannot be reached; the
			// mapping accepts it through the SPI and that is stated.
			if err != nil {
				t.Fatalf("TopicID(%q): %v", topic, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("TopicID(%q): %v", topic, err)
		}
		if prev, dup := seen[id]; dup {
			t.Fatalf("topics %q and %q both map to %q", prev, topic, id)
		}
		seen[id] = topic
	}
	// A subscription identifier carries the topic, because a Pub/Sub
	// subscription names exactly one topic.
	a, err := cfg.SubscriptionID("orders", "workers")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	b, err := cfg.SubscriptionID("invoices", "workers")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	if a == b {
		t.Fatalf("two topics share the subscription identifier %q: a consumer of one would read the other's messages", a)
	}
	// And the override is honoured.
	cfg.SubscriptionIDs = map[string]string{"orders/workers": "named-by-terraform"}
	got, err := cfg.SubscriptionID("orders", "workers")
	if err != nil || got != "named-by-terraform" {
		t.Fatalf("SubscriptionID with an override returned %q, %v", got, err)
	}
}

// TestDerivedIdentifiersAreRefusedWhenTooLong is the limit-before-I/O rule for
// the resource names themselves.
func TestDerivedIdentifiersAreRefusedWhenTooLong(t *testing.T) {
	cfg := gps.Config{ProjectID: "p"}
	long := strings.Repeat("t", 300)
	if _, err := cfg.TopicID(long); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("a 300-character topic reported %v", err)
	}
	if _, err := cfg.SubscriptionID(strings.Repeat("t", 200), strings.Repeat("s", 200)); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("a 400-character subscription identifier reported %v", err)
	}
}

// TestAttemptIsZeroWithoutADeadLetterPolicy is the specification's "an unknown is never an
// overloaded zero", read the other way round: DeliveryInfo.Attempt HAS a
// spelling for "the broker does not count attempts", and Pub/Sub reports
// DeliveryAttempt only on a subscription with a dead-letter policy.
func TestAttemptIsZeroWithoutADeadLetterPolicy(t *testing.T) {
	c, _ := scripted(t, func(cfg *gps.Config) { cfg.AckDeadline = time.Minute })
	topic := newTopic("attempt")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)
	if got := d.Info().Attempt; got != 0 {
		t.Fatalf("Attempt is %d on a subscription with no dead-letter policy, want 0: "+
			"the service counts nothing, and an invented 1 is an unknown reported as a fact", got)
	}
	if d.Info().Redelivered {
		t.Fatal("Redelivered is true on a first delivery the service did not count")
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAttemptCountsWithADeadLetterPolicy is the other direction: with the
// policy the service does count, and the driver reports what it counted.
func TestAttemptCountsWithADeadLetterPolicy(t *testing.T) {
	dlq := newTopic("dl")
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
		cfg.AckDeadline = time.Minute
	})
	topic := newTopic("attempts")
	provision(t, c, dlq, "d")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	first := receiveOne(t, cons, 5*time.Second)
	if got := first.Info().Attempt; got != 1 {
		t.Fatalf("the first delivery reports attempt %d, want 1", got)
	}
	if err := first.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second := receiveOne(t, cons, 5*time.Second)
	if got := second.Info().Attempt; got != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", got)
	}
	if !second.Info().Redelivered {
		t.Fatal("the redelivery does not report Redelivered")
	}
	if err := second.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	_ = srv
}

// TestReceiptCarriesTheServerAssignedIdentifier is the receipt, and what
// this broker can and cannot fill in.
func TestReceiptCarriesTheServerAssignedIdentifier(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	topic := newTopic("receipt")
	provision(t, c, topic, "s")
	ctx := context.Background()
	pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic, Receipts: true})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	var receipt mq.PublishReceipt
	if err := pub.Publish(ctx, &mq.Message{Body: []byte("x"), Receipt: &receipt}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.MessageID == "" {
		t.Fatal("the receipt carries no message identifier")
	}
	if receipt.Confirmation != mq.ConfirmDurable {
		t.Fatalf("the receipt reports the %v boundary; PublishResult.Get is the durable one", receipt.Confirmation)
	}
	// There is no partition, no offset and no sequence on this broker, so
	// those stay zero rather than being invented.
	if receipt.Partition != 0 || receipt.Offset != 0 || receipt.Sequence != 0 {
		t.Errorf("the receipt invents a partition, an offset or a sequence: %+v", receipt)
	}
	if _, err := strconv.Atoi(receipt.MessageID); err != nil && receipt.MessageID == "" {
		t.Errorf("the message identifier is %q", receipt.MessageID)
	}
}
