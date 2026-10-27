package pulsar_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// TestRepresentableNamesTheFiveLossesPulsarImposes holds the strict mode.
//
// Each refusal is something the envelope CAN carry, so each of them is also a
// reason MetadataAdaptive wraps — and the two that the inventory has no word
// for at all are the message key's two.
func TestRepresentableNamesTheFiveLossesPulsarImposes(t *testing.T) {
	for name, tc := range map[string]struct {
		build func(*mq.Message)
		code  mq.ErrorCode
	}{
		"a plain message":   {build: func(*mq.Message) {}},
		"a nil body":        {build: func(m *mq.Message) { m.Body = nil }},
		"an empty body":     {build: func(m *mq.Message) { m.Body = []byte{} }},
		"a binary body":     {build: func(m *mq.Message) { m.Body = []byte{0x00, 0xff} }},
		"a nil key":         {build: func(m *mq.Message) { m.Key = nil }},
		"a text key":        {build: func(m *mq.Message) { m.Key = []byte("k") }},
		"an empty key":      {build: func(m *mq.Message) { m.Key = []byte{} }, code: mq.CodeUnsupported},
		"a binary key":      {build: func(m *mq.Message) { m.Key = []byte{0xff} }, code: mq.CodeUnsupported},
		"a text header":     {build: func(m *mq.Message) { m.Headers.Set("k", []byte("v")) }},
		"a binary header":   {build: func(m *mq.Message) { m.Headers.Set("k", []byte{0xff}) }, code: mq.CodeUnsupported},
		"a duplicate key":   {build: func(m *mq.Message) { m.Headers.Add("k", []byte("a")); m.Headers.Add("k", []byte("b")) }, code: mq.CodeUnsupported},
		"an empty key name": {build: func(m *mq.Message) { m.Headers.Set("", []byte("v")) }, code: mq.CodeInvalidArgument},
		"a reserved key":    {build: func(m *mq.Message) { m.Headers.Set(pd.PropID, []byte("v")) }, code: mq.CodeInvalidArgument},
		"a client property": {build: func(m *mq.Message) { m.Headers.Set(pulsar.SysPropertyReconsumeTimes, []byte("9")) }, code: mq.CodeUnsupported},
		"an identifier":     {build: func(m *mq.Message) { m.ID = "id-1" }},
		"a typed property":  {build: func(m *mq.Message) { m.Properties.ContentType = "text/plain" }},
	} {
		t.Run(name, func(t *testing.T) {
			m := &mq.Message{Body: []byte("b")}
			tc.build(m)
			err := pd.Representable(m)
			switch {
			case tc.code == mq.CodeUnknown && err != nil:
				t.Fatalf("Representable refused a message Pulsar can carry: %v", err)
			case tc.code == mq.CodeUnknown:
			case err == nil:
				t.Fatalf("Representable accepted %s, which Pulsar cannot carry natively", name)
			case mq.CodeOf(err) != tc.code:
				t.Fatalf("Representable reported %v (code %v), want %v", err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

// TestEncodeDecodeRoundTrip holds the native mapping: everything that has a
// Pulsar field comes back through it, bit for bit.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	created := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	m := &mq.Message{Key: []byte("customer-7"), Body: []byte("payload"), ID: "id-9"}
	m.Headers.Set("kind", []byte("order"))
	m.Headers.Set("region", []byte("eu"))
	m.Properties = mq.Properties{
		ContentType: "application/json", ContentEncoding: "gzip",
		CorrelationID: "c-1", CausationID: "c-0", ReplyTo: "replies", CreatedAt: created,
	}
	var pm pulsar.ProducerMessage
	pd.Encode(&pm, m, map[string]string{})
	if string(pm.Payload) != "payload" || pm.Key != "customer-7" {
		t.Fatalf("Encode produced payload %q key %q", pm.Payload, pm.Key)
	}
	if !pm.EventTime.Equal(created) {
		t.Fatalf("Encode produced event time %v, want %v", pm.EventTime, created)
	}
	if _, ok := pm.Properties[pd.PropCreated]; !ok {
		t.Fatal("Encode did not mark the event time as the publisher's CreatedAt; a Pulsar message that " +
			"carries no event time arrives with the Unix epoch, so a decoder without the marker " +
			"invents a creation time of 1970 for every message")
	}
	var out mq.Message
	pd.Decode(&out, &encoded{pm: &pm, topic: "persistent://public/default/orders"}, "orders")
	if !bytes.Equal(out.Body, m.Body) || !bytes.Equal(out.Key, m.Key) || out.ID != m.ID {
		t.Fatalf("the round trip gave body %q key %q id %q", out.Body, out.Key, out.ID)
	}
	if out.Properties != m.Properties {
		t.Fatalf("the round trip gave properties %+v, want %+v", out.Properties, m.Properties)
	}
	if v, ok := out.Headers.Get("kind"); !ok || string(v) != "order" {
		t.Fatalf("the round trip gave header kind=%q (present %v)", v, ok)
	}
	if out.Headers.Len() != 2 {
		t.Fatalf("the round trip gave %d headers, want 2: the reserved properties are not headers", out.Headers.Len())
	}
	if out.Topic != "orders" {
		t.Fatalf("the round trip gave topic %q", out.Topic)
	}
}

// TestDecodeStripsPulsarsOwnProperties is what keeps a message that came back
// through the retry topic equal to the one that was published.
//
// pulsar-client-go stamps REAL_TOPIC, ORIGIN_MESSAGE_ID, RECONSUMETIMES and
// DELAY_TIME on a message it republished, and this driver's requeue adds
// RETRY_TOPIC. A driver that surfaced them as headers would give a redelivered
// message five headers the publisher never set.
func TestDecodeStripsPulsarsOwnProperties(t *testing.T) {
	const retry = "persistent://public/default/orders-workers-RETRY"
	pm := pulsar.ProducerMessage{Payload: []byte("b"), Properties: map[string]string{
		"kind":                            "order",
		pulsar.SysPropertyRealTopic:       "persistent://public/default/orders",
		pulsar.SysPropertyReconsumeTimes:  "3",
		pulsar.SysPropertyDelayTime:       "1000",
		pulsar.SysPropertyOriginMessageID: "1:2:0",
		pulsar.PropertyOriginMessageID:    "1:2:0",
		pulsar.SysPropertyRetryTopic:      retry,
	}}
	var out mq.Message
	msg := &encoded{pm: &pm, topic: retry, redeliveries: 1}
	pd.Decode(&out, msg, "orders")
	if out.Headers.Len() != 1 {
		var names []string
		for k := range out.Headers.All() {
			names = append(names, k)
		}
		t.Fatalf("a message that came back through the retry topic carries %d headers (%v); the "+
			"publisher set one", out.Headers.Len(), names)
	}
	// And the attempt count survives the hop, which is the other half: the
	// broker's redelivery counter starts again at zero for a republished
	// message.
	if got := pd.AttemptOf(msg); got != 5 {
		t.Fatalf("AttemptOf reported %d for a message with one broker redelivery and three retry-topic "+
			"hops, want 5: a driver that read only the broker's counter reports attempt 1 on every "+
			"delivery of a message that has been round the retry topic, and a MaxAttempts policy "+
			"above it never fires", got)
	}
}

// TestAttemptOfSaturates is a RECONSUMETIMES at the top of the range on a
// delivery that names no retry topic, which is a dead-letter copy an earlier
// version of this driver rejected at the largest MaxAttempts: an explicit
// Reject forced RECONSUMETIMES to MaxAttempts and the client added one, so the
// copy carries math.MaxUint32. The uint32 sum used to wrap, and the copy's
// first delivery reported attempt 0, which says the broker counts nothing.
func TestAttemptOfSaturates(t *testing.T) {
	for _, tc := range []struct {
		reconsumed   string
		redeliveries uint32
	}{
		{"4294967295", 0},
		{"4294967295", 2},
		{"4294967294", 1},
		{"18446744073709551615", 0},
	} {
		pm := pulsar.ProducerMessage{Properties: map[string]string{
			pulsar.SysPropertyReconsumeTimes: tc.reconsumed,
		}}
		msg := &encoded{pm: &pm, topic: "persistent://public/default/capped-dlq", redeliveries: tc.redeliveries}
		if got := pd.AttemptOf(msg); got != math.MaxUint32 {
			t.Errorf("RECONSUMETIMES %s with %d broker redeliveries reports attempt %d, want %d",
				tc.reconsumed, tc.redeliveries, got, uint32(math.MaxUint32))
		}
	}
}

// TestAdaptiveEnvelopesWhatPulsarCannotCarry is the adaptive mode end to
// end: the two key cases driverkit.Envelope.Needed cannot express, and the
// binary and duplicate headers it can.
func TestAdaptiveEnvelopesWhatPulsarCannotCarry(t *testing.T) {
	b, _ := newRig(t, func(c *pd.Config) { c.Metadata = mq.MetadataAdaptive })
	src := sourceOf(t, b, "adaptive", "workers", mq.MetadataAdaptive, 16)
	snk := sinkOf(t, b, "adaptive", mq.MetadataAdaptive)
	for name, build := range map[string]func(*mq.Message){
		"an empty key":      func(m *mq.Message) { m.Key = []byte{} },
		"a binary key":      func(m *mq.Message) { m.Key = []byte{0x00, 0xff} },
		"a binary header":   func(m *mq.Message) { m.Headers.Set("trace", []byte{0x00, 0xff}) },
		"duplicate headers": func(m *mq.Message) { m.Headers.Add("t", []byte("a")); m.Headers.Add("t", []byte("b")) },
	} {
		t.Run(name, func(t *testing.T) {
			m := &mq.Message{Body: []byte("b")}
			build(m)
			want := m.Clone()
			publish(t, snk, m)
			d := fetchOne(t, src)
			if (d.Message.Key == nil) != (want.Key == nil) {
				t.Fatalf("the key came back as %#v, want %#v", d.Message.Key, want.Key)
			}
			if !bytes.Equal(d.Message.Key, want.Key) {
				t.Fatalf("the key came back as %q, want %q", d.Message.Key, want.Key)
			}
			if d.Message.Headers.Len() != want.Headers.Len() {
				t.Fatalf("the headers came back as %d values, want %d", d.Message.Headers.Len(), want.Headers.Len())
			}
			for k, v := range want.Headers.All() {
				got := d.Message.Headers.Values(k)
				found := false
				for _, g := range got {
					if bytes.Equal(g, v) {
						found = true
					}
				}
				if !found {
					t.Fatalf("header %q lost the value %q", k, v)
				}
			}
			if err := src.Settler().Ack(context.Background(), &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestStrictModeRefusesBeforeAnyIO is the strict mode at the publication
// control, and the specification's "before any I/O".
func TestStrictModeRefusesBeforeAnyIO(t *testing.T) {
	b, server := newRig(t, nil)
	snk := sinkOf(t, b, "strict", mq.MetadataNativeStrict)
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Set("trace", []byte{0x00, 0xff})
	err := snk.Publish(context.Background(), m, nil)
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a strict publisher given a binary header reported %v (code %v), want ErrUnsupported",
			err, mq.CodeOf(err))
	}
	if n := server.Messages("persistent://public/default/strict"); n != 0 {
		t.Fatalf("the broker holds %d message(s) after a refused publication; the specification refuses before any I/O", n)
	}
}

// encoded adapts a pulsar.ProducerMessage to the pulsar.Message interface, so
// that Encode and Decode can be held to each other without a broker.
type encoded struct {
	pm           *pulsar.ProducerMessage
	topic        string
	redeliveries uint32
}

func (e *encoded) Topic() string                 { return e.topic }
func (e *encoded) ProducerName() string          { return "test" }
func (e *encoded) Properties() map[string]string { return e.pm.Properties }
func (e *encoded) Payload() []byte               { return e.pm.Payload }
func (e *encoded) IsNullValue() bool             { return e.pm.Payload == nil }
func (e *encoded) ID() pulsar.MessageID          { return pulsar.EarliestMessageID() }
func (e *encoded) PublishTime() time.Time        { return time.Unix(0, 0) }
func (e *encoded) EventTime() time.Time          { return e.pm.EventTime }
func (e *encoded) Key() string                   { return e.pm.Key }
func (e *encoded) OrderingKey() string           { return e.pm.OrderingKey }
func (e *encoded) RedeliveryCount() uint32       { return e.redeliveries }
func (e *encoded) IsReplicated() bool            { return false }
func (e *encoded) GetReplicatedFrom() string     { return "" }
func (e *encoded) GetSchemaValue(any) error      { return errors.New("no schema") }
func (e *encoded) SchemaVersion() []byte         { return nil }
func (e *encoded) Index() *uint64                { return nil }
func (e *encoded) BrokerPublishTime() *time.Time { return nil }

func (e *encoded) GetEncryptionContext() *pulsar.EncryptionContext { return nil }
