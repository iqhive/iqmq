package sqs

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
)

// encoderFor builds an encoder for one metadata mode, which is what every
// mapping test starts with.
func encoderFor(t *testing.T, cfg Config, mode mq.MetadataMode) *encoder {
	t.Helper()
	caps, err := capabilities(cfg.resolve())
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	enc, err := newEncoder(cfg.resolve(), caps, mode)
	if err != nil {
		t.Fatalf("newEncoder: %v", err)
	}
	return enc
}

// TestEncodeDecodeRoundTrip is the mapping's whole contract: what goes in
// comes back, for every shape of metadata the native path can carry.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	created := time.Date(2026, 4, 1, 9, 30, 0, 500, time.UTC)
	for _, tc := range []struct {
		name string
		in   func() *mq.Message
	}{
		{"bare", func() *mq.Message { return &mq.Message{Body: []byte("hello")} }},
		{"nil body", func() *mq.Message { return &mq.Message{} }},
		{"empty body", func() *mq.Message { return &mq.Message{Body: []byte{}} }},
		{"binary body", func() *mq.Message { return &mq.Message{Body: []byte{0x00, 0xff, 0x01, 0x80}} }},
		{"key", func() *mq.Message { return &mq.Message{Key: []byte("k-1"), Body: []byte("b")} }},
		{"binary key", func() *mq.Message { return &mq.Message{Key: []byte{0, 1, 2}, Body: []byte("b")} }},
		{"identifier", func() *mq.Message { return &mq.Message{ID: "id-7", Body: []byte("b")} }},
		{"one header", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("content-kind", []byte("order"))
			return m
		}},
		{"binary header", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("trace-state", []byte{0x00, 0xff, 0xfe, 0x80})
			return m
		}},
		{"nine headers", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			for i := range MaxAttributes - 1 {
				m.Headers.Add("h-"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
			}
			return m
		}},
		{"properties", func() *mq.Message {
			return &mq.Message{Body: []byte("b"), Properties: mq.Properties{
				ContentType: "application/json", ContentEncoding: "gzip",
				CorrelationID: "c-1", CausationID: "c-0", ReplyTo: "replies",
				CreatedAt: created,
			}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{}, mq.MetadataNativeStrict)
			in := tc.in()
			out, err := enc.encode(in, &mq.PublishOptions{})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if out.Enveloped {
				t.Fatal("the native path enveloped a message that fits")
			}
			if len(out.Attrs) > MaxAttributes {
				t.Fatalf("%d attributes, over the service's %d", len(out.Attrs), MaxAttributes)
			}
			if out.Body == "" {
				t.Fatal("the body is empty, which the service refuses outright")
			}
			if !sqsSafeText([]byte(out.Body)) {
				t.Fatalf("the encoded body is not in the service's character set: %q", out.Body)
			}
			var got mq.Message
			if err := enc.decode(received(out), "t", &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			assertSameMessage(t, in, &got)
		})
	}
}

// received turns an encoded publication into the message a receive produces, so
// that a round trip goes through the same shape the service uses.
func received(out encoded) *types.Message {
	return &types.Message{
		MessageId:         aws.String("m-1"),
		ReceiptHandle:     aws.String("rh-1"),
		Body:              aws.String(out.Body),
		MessageAttributes: out.Attrs,
		Attributes:        map[string]string{},
	}
}

// assertSameMessage compares everything the mapping promises to carry.
func assertSameMessage(t *testing.T, want, got *mq.Message) {
	t.Helper()
	switch {
	case want.Body == nil && got.Body != nil:
		t.Errorf("a nil body came back as %#v; the specification distinguishes nil from empty", got.Body)
	case want.Body != nil && got.Body == nil:
		t.Errorf("a body of %d byte(s) came back nil", len(want.Body))
	case !bytes.Equal(want.Body, got.Body):
		t.Errorf("body %q came back as %q", want.Body, got.Body)
	}
	switch {
	case len(want.Key) == 0 && len(got.Key) != 0:
		t.Errorf("no key came back as %q", got.Key)
	case !bytes.Equal(want.Key, got.Key):
		t.Errorf("key %q came back as %q", want.Key, got.Key)
	}
	if want.ID != got.ID {
		t.Errorf("identifier %q came back as %q", want.ID, got.ID)
	}
	if want.Properties != got.Properties {
		t.Errorf("properties %+v came back as %+v", want.Properties, got.Properties)
	}
	if want.Headers.Len() != got.Headers.Len() {
		t.Errorf("%d header(s) came back as %d", want.Headers.Len(), got.Headers.Len())
		return
	}
	// Compared per DISTINCT key and over the whole ordered value list, not
	// through Get: Get answers with the first value, so a comparator built
	// on it reports a repeated header as matching whatever its second value
	// is. The first version of this helper did exactly that and passed a
	// duplicate-header round trip that had lost a value.
	seen := map[string]bool{}
	for k := range want.Headers.All() {
		if seen[k] {
			continue
		}
		seen[k] = true
		wantVals, gotVals := want.Headers.Values(k), got.Headers.Values(k)
		if len(wantVals) != len(gotVals) {
			t.Errorf("header %q had %d value(s) and came back with %d", k, len(wantVals), len(gotVals))
			continue
		}
		for i := range wantVals {
			if !bytes.Equal(wantVals[i], gotVals[i]) {
				t.Errorf("header %q value %d came back as %q, want %q", k, i, gotVals[i], wantVals[i])
			}
		}
	}
}

// TestStrictRefusesWhatDoesNotFit is the strict mode: metadata the service
// cannot represent is refused before any I/O, with a refusal that names what
// was refused.
func TestStrictRefusesWhatDoesNotFit(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   func() *mq.Message
		want error
	}{
		{"eleven attributes", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			for i := range MaxAttributes {
				m.Headers.Add("h-"+strconv.Itoa(i), []byte("v"))
			}
			return m
		}, errTooManyAttributes},
		{"budget spent by the key and the properties", func() *mq.Message {
			m := &mq.Message{Key: []byte("k"), ID: "i", Body: []byte("b")}
			m.Properties = mq.Properties{
				ContentType: "a", ContentEncoding: "b", CorrelationID: "c",
				CausationID: "d", ReplyTo: "e", CreatedAt: time.Unix(1, 0),
			}
			m.Headers.Add("one", []byte("1"))
			m.Headers.Add("two", []byte("2"))
			return m
		}, errTooManyAttributes},
		{"repeated key", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("tag", []byte("1"))
			m.Headers.Add("tag", []byte("2"))
			return m
		}, errDuplicateHeader},
		{"illegal attribute name", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("x/y", []byte("1"))
			return m
		}, errHeaderName},
		{"reserved name", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add(attrKey, []byte("1"))
			return m
		}, errReservedHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{}, mq.MetadataNativeStrict)
			_, err := enc.encode(tc.in(), &mq.PublishOptions{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("encode = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestAdaptiveEnvelopesWhatDoesNotFit is the other half: the same messages the
// strict mode refuses travel in an envelope and come back whole.
//
// Two of the reasons are ones [driverkit.Envelope.Needed] cannot see — a count
// and a name grammar — so this test is what proves the driver's own reason
// reaches the envelope rather than being answered "no" by the kit's capability
// reading.
func TestAdaptiveEnvelopesWhatDoesNotFit(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   func() *mq.Message
	}{
		{"eleven attributes", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			for i := range MaxAttributes {
				m.Headers.Add("h-"+strconv.Itoa(i), []byte("v"))
			}
			return m
		}},
		{"repeated key", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("tag", []byte("1"))
			m.Headers.Add("tag", []byte("2"))
			return m
		}},
		{"illegal attribute name", func() *mq.Message {
			m := &mq.Message{Body: []byte("b")}
			m.Headers.Add("x/y", []byte("1"))
			return m
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{}, mq.MetadataAdaptive)
			in := tc.in()
			out, err := enc.encode(in, &mq.PublishOptions{})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !out.Enveloped {
				t.Fatal("the adaptive mode did not envelope a message the native path cannot carry")
			}
			if len(out.Attrs) != 2 {
				t.Fatalf("an enveloped message carries %d attribute(s), want the two markers: %v", len(out.Attrs), out.Attrs)
			}
			var got mq.Message
			if err := enc.decode(received(out), "t", &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			assertSameMessage(t, in, &got)
		})
	}
}

// TestAdaptiveLeavesAFittingMessageNative is the half that makes the adaptive
// mode worth having: a message that fits is NOT enveloped, so an ordinary
// message stays readable by a non-iqmq consumer.
func TestAdaptiveLeavesAFittingMessageNative(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataAdaptive)
	m := &mq.Message{Body: []byte(`{"id":1}`), Key: []byte("k")}
	m.Headers.Add("content-type", []byte("application/json"))
	out, err := enc.encode(m, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if out.Enveloped {
		t.Fatal("a message that fits the native path was enveloped, so every consumer now has to decode")
	}
	if out.Body != `{"id":1}` {
		t.Fatalf("the body on the wire is %q, want the caller's own bytes", out.Body)
	}
}

// TestPortableEnvelopeAlwaysEnvelopes is the mode that changes the bytes for
// every message, which is what a caller chooses it for.
func TestPortableEnvelopeAlwaysEnvelopes(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataPortableEnvelope)
	m := &mq.Message{Body: []byte("plain")}
	out, err := enc.encode(m, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !out.Enveloped {
		t.Fatal("the portable mode did not envelope a message")
	}
	var got mq.Message
	if err := enc.decode(received(out), "t", &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertSameMessage(t, m, &got)
}

// TestLenientDropDropsAndCounts is the lenient mode, with the metric it
// requires: a mode that drops without a metric is indistinguishable from one
// that loses.
func TestLenientDropDropsAndCounts(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataLenientDrop)
	m := &mq.Message{Body: []byte("b"), ID: "keep-me"}
	for i := range MaxAttributes + 5 {
		m.Headers.Add("h-"+strconv.Itoa(i), []byte("v"))
	}
	m.Headers.Add("x/y", []byte("illegal"))
	out, err := enc.encode(m, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if out.Enveloped {
		t.Fatal("the lenient mode enveloped rather than dropped")
	}
	if len(out.Attrs) > MaxAttributes {
		t.Fatalf("%d attributes survived the drop, over the service's %d", len(out.Attrs), MaxAttributes)
	}
	if enc.Dropped() == 0 {
		t.Fatal("headers were dropped and the metric reports none, so the \"with a hook and a metric\" is not satisfied")
	}
	var got mq.Message
	if err := enc.decode(received(out), "t", &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != "keep-me" {
		t.Fatalf("the identifier was dropped (%q): the lenient mode drops HEADERS, and losing an identifier breaks correlation rather than metadata", got.ID)
	}
	if got.Headers.Len() == 0 {
		t.Fatal("every header was dropped; the mode keeps what fits")
	}
	if _, ok := got.Headers.Get("x/y"); ok {
		t.Fatal("an illegal attribute name survived, which the service would refuse")
	}
}

// TestBodyPolicy is the binary-body policy for this driver, all three
// answers.
func TestBodyPolicy(t *testing.T) {
	binary := []byte{0x00, 0xff, 0x01}
	for _, tc := range []struct {
		name     string
		policy   BodyEncoding
		body     []byte
		wantErr  error
		wantWire string
	}{
		{"auto passes text", BodyAuto, []byte("hello"), nil, "hello"},
		{"auto encodes binary", BodyAuto, binary, nil, "AP8B"},
		{"text passes text", BodyText, []byte("hello"), nil, "hello"},
		{"text refuses binary", BodyText, binary, errBodyNotText, ""},
		{"base64 encodes text", BodyBase64, []byte("hello"), nil, "aGVsbG8="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{Body: tc.policy}, mq.MetadataNativeStrict)
			out, err := enc.encode(&mq.Message{Body: tc.body}, &mq.PublishOptions{})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("encode = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				return
			}
			if out.Body != tc.wantWire {
				t.Fatalf("the body on the wire is %q, want %q", out.Body, tc.wantWire)
			}
			var got mq.Message
			if err := enc.decode(received(out), "t", &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(got.Body, tc.body) {
				t.Fatalf("body %q came back as %q", tc.body, got.Body)
			}
		})
	}
}

// TestOptionsRefusedBeforeIO holds the disqualifier: a driver that
// silently dropped an unsupported option would be selected by no benchmark and
// noticed by no caller.
func TestOptionsRefusedBeforeIO(t *testing.T) {
	for _, tc := range []struct {
		name string
		fifo bool
		opts mq.PublishOptions
		want error
	}{
		{"time to live", false, mq.PublishOptions{TTL: time.Minute}, errNoTTL},
		{"priority", false, mq.PublishOptions{Priority: 3, PrioritySet: true}, errNoPriority},
		{"partition", false, mq.PublishOptions{Partition: 1, PartitionSet: true}, errNoPartition},
		{"deduplication on a standard queue", false, mq.PublishOptions{DedupKey: "k"}, errNoDedup},
		{"delay on a FIFO queue", true, mq.PublishOptions{Delay: time.Second}, errFIFODelay},
		{"delay over the service's timer", false, mq.PublishOptions{Delay: 20 * time.Minute}, errDelayTooLong},
		{"delay and deliver-at together", false, mq.PublishOptions{
			Delay: time.Second, DeliverAt: time.Unix(1<<31, 0),
		}, errDelayAndDeliverAt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{FIFO: tc.fifo}, mq.MetadataNativeStrict)
			opts := tc.opts
			_, err := enc.encode(&mq.Message{Body: []byte("b")}, &opts)
			if !errors.Is(err, tc.want) {
				t.Fatalf("encode = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestDelayRoundsUp is a sub-second delay that rounded down
// to zero would deliver the message immediately, which is the guarantee the
// caller asked for being quietly withdrawn.
func TestDelayRoundsUp(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		delay time.Duration
		want  int32
	}{
		{0, 0},
		{time.Nanosecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{1500 * time.Millisecond, 2},
		{MaxDelay, int32(MaxDelay / time.Second)},
	} {
		opts := mq.PublishOptions{Delay: tc.delay}
		out, err := enc.encode(&mq.Message{Body: []byte("b")}, &opts)
		if err != nil {
			t.Fatalf("delay %v: %v", tc.delay, err)
		}
		if out.Delay != tc.want {
			t.Fatalf("a delay of %v became %d second(s), want %d: rounding down withdraws the delay the caller asked for", tc.delay, out.Delay, tc.want)
		}
	}
}

// TestOversizedMessageRefusedBeforeIO is the specification's "limits before I/O".
func TestOversizedMessageRefusedBeforeIO(t *testing.T) {
	enc := encoderFor(t, Config{MaxMessageBytes: 4096}, mq.MetadataNativeStrict)
	_, err := enc.encode(&mq.Message{Body: bytes.Repeat([]byte("x"), 5000)}, &mq.PublishOptions{})
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("encode = %v, want ErrMessageTooLarge", err)
	}
	// The attributes count too: a message whose body just fits and whose
	// headers push it over is over.
	m := &mq.Message{Body: bytes.Repeat([]byte("x"), 4000)}
	m.Headers.Add("big", bytes.Repeat([]byte("y"), 200))
	if _, err := enc.encode(m, &mq.PublishOptions{}); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("encode with headers over the limit = %v, want ErrMessageTooLarge: the service counts the attribute names and values", err)
	}
}

// TestAttributeDataTypesCountTowardsTheSize holds the accounted size to what
// the service counts: each attribute's name, data type and value. A message
// whose names and values fit exactly, with the data types left out, is over.
func TestAttributeDataTypesCountTowardsTheSize(t *testing.T) {
	m := &mq.Message{Body: []byte("body")}
	m.Headers.Add("h", []byte("v"))
	out, err := encoderFor(t, Config{}, mq.MetadataNativeStrict).encode(m, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := len(out.Body)
	for k, v := range out.Attrs {
		want += len(k) + len(aws.ToString(v.DataType)) + len(aws.ToString(v.StringValue)) + len(v.BinaryValue)
	}
	if out.Bytes != want {
		t.Fatalf("Bytes = %d, want %d: the service counts each attribute's data type", out.Bytes, want)
	}
	withoutTypes := want
	for _, v := range out.Attrs {
		withoutTypes -= len(aws.ToString(v.DataType))
	}
	enc := encoderFor(t, Config{MaxMessageBytes: int64(withoutTypes)}, mq.MetadataNativeStrict)
	if _, err := enc.encode(m, &mq.PublishOptions{}); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("encode under a %d-byte limit = %v, want ErrMessageTooLarge: the data types put it over", withoutTypes, err)
	}
}

// TestAttributeNameGrammar holds the name check to the service's own rules.
func TestAttributeNameGrammar(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"content-type", true},
		{"a_b", true},
		{"a.b", true},
		{"a1", true},
		{"", false},
		{".a", false},
		{"a.", false},
		{"a..b", false},
		{"a/b", false},
		{"a b", false},
		{"AWS.x", false},
		{"aws.x", false},
		{"Amazon.x", false},
		{"amazon.x", false},
		{strings.Repeat("a", 256), true},
		{strings.Repeat("a", 257), false},
	} {
		if got := attributeNameOK(tc.name); got != tc.ok {
			t.Errorf("attributeNameOK(%q) = %v, want %v", tc.name, got, tc.ok)
		}
	}
}

// TestAttemptCount is the specification's "ApproximateReceiveCount→Attempt", including
// the rule that a count the broker did not send is zero rather than one.
func TestAttemptCount(t *testing.T) {
	const key = "ApproximateReceiveCount"
	for _, tc := range []struct {
		attrs map[string]string
		want  uint32
	}{
		{nil, 0},
		{map[string]string{}, 0},
		{map[string]string{key: ""}, 0},
		{map[string]string{key: "0"}, 0},
		{map[string]string{key: "1"}, 1},
		{map[string]string{key: "7"}, 7},
		{map[string]string{key: "not-a-number"}, 0},
	} {
		if got := attemptOf(tc.attrs); got != tc.want {
			t.Errorf("attemptOf(%v) = %d, want %d", tc.attrs, got, tc.want)
		}
	}
}

// TestSequenceTooLargeIsAbsent is a FIFO sequence number is
// a 128-bit decimal and a uint64 cannot hold every one, so one that does not
// fit is reported absent rather than truncated.
func TestSequenceTooLargeIsAbsent(t *testing.T) {
	const key = "SequenceNumber"
	if _, ok := sequenceOf(map[string]string{key: "18446744073709551616"}); ok {
		t.Fatal("a sequence number too large for a uint64 was reported as a value; the specification forbids an unknown becoming an overloaded number")
	}
	got, ok := sequenceOf(map[string]string{key: "42"})
	if !ok || got != 42 {
		t.Fatalf("sequenceOf = %d, %v", got, ok)
	}
}

// TestGroupIDRendering is a key that fits is verbatim and a
// key that does not is hashed, and the hash may only MERGE groups.
func TestGroupIDRendering(t *testing.T) {
	if got := GroupID(nil); got != DefaultGroupID {
		t.Fatalf("GroupID(nil) = %q, want %q", got, DefaultGroupID)
	}
	if got := GroupID([]byte("orders-eu")); got != "orders-eu" {
		t.Fatalf("a short printable key rendered as %q rather than verbatim", got)
	}
	long := bytes.Repeat([]byte("k"), maxGroupIDLen+1)
	got := GroupID(long)
	switch {
	case len(got) > maxGroupIDLen:
		t.Fatalf("a long key rendered as %d characters, over the service's %d", len(got), maxGroupIDLen)
	case !strings.HasPrefix(got, "h-"):
		t.Fatalf("a long key rendered as %q, which is not the hashed form", got)
	}
	if GroupID([]byte{0, 1, 2}) == GroupID([]byte{0, 1, 3}) {
		t.Fatal("two different binary keys rendered to one group; a collision is safe and a systematic one is not")
	}
	if GroupID([]byte("a b")) == "a b" {
		t.Fatal("a key with a space was used verbatim; the service's group grammar has no space")
	}
}

// TestQueueNameMapping is the topic mapping, with both directions of its
// refusals.
func TestQueueNameMapping(t *testing.T) {
	for _, tc := range []struct {
		topic  string
		prefix string
		fifo   bool
		want   string
		err    bool
	}{
		{"orders", "", false, "orders", false},
		{"orders/eu", "", false, "orders-eu", false},
		{"orders", "dev-", false, "dev-orders", false},
		{"orders", "", true, "orders.fifo", false},
		{"orders", "dev-", true, "dev-orders.fifo", false},
		{"orders-*", "", false, "", true},
		{"orders", strings.Repeat("p", 80), false, "", true},
		{"", "", false, "", true},
	} {
		got, err := QueueName(tc.topic, tc.prefix, tc.fifo)
		if tc.err {
			if err == nil {
				t.Errorf("QueueName(%q, %q, %v) = %q, want a refusal", tc.topic, tc.prefix, tc.fifo, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("QueueName(%q, %q, %v): %v", tc.topic, tc.prefix, tc.fifo, err)
			continue
		}
		if got != tc.want {
			t.Errorf("QueueName(%q, %q, %v) = %q, want %q", tc.topic, tc.prefix, tc.fifo, got, tc.want)
		}
	}
}

// TestWildcardIsUnsupported is the negative direction of
// FeatureWildcardSubscribe: the refusal has to be ErrUnsupported, because that
// is what the conformance kit reads.
func TestWildcardIsUnsupported(t *testing.T) {
	for _, topic := range []string{"orders-*", "orders-?", "orders.#", "orders.>"} {
		_, err := QueueName(topic, "", false)
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("QueueName(%q) = %v, want ErrUnsupported: the negative direction is proved by this refusal", topic, err)
		}
	}
}

// TestSQSSafeText holds the body character-set check to the service's own set.
func TestSQSSafeText(t *testing.T) {
	for _, tc := range []struct {
		body []byte
		ok   bool
	}{
		{[]byte("plain"), true},
		{[]byte("tab\there"), true},
		{[]byte("nl\nhere"), true},
		{[]byte("cr\rhere"), true},
		{[]byte("naïve ☃"), true},
		{[]byte{0x00}, false},
		{[]byte{0x01}, false},
		{[]byte{0x0b}, false},
		{[]byte{0xff}, false},
		{[]byte("￾"), false},
		{[]byte("￿"), false},
		{[]byte("\U0001F600"), true},
	} {
		if got := sqsSafeText(tc.body); got != tc.ok {
			t.Errorf("sqsSafeText(%q) = %v, want %v", tc.body, got, tc.ok)
		}
	}
}

// TestDecodeRefusesAnUnknownEncoding is the forward-compatibility refusal: a
// message written by a newer driver is a refusal rather than a body the
// application is handed as though it were the payload.
func TestDecodeRefusesAnUnknownEncoding(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataNativeStrict)
	msg := &types.Message{
		Body: aws.String("whatever"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			attrEncoding: stringAttr("zstd-in-2029"),
		},
	}
	var got mq.Message
	if err := enc.decode(msg, "t", &got); !errors.Is(err, errBodyEncodingUnknown) {
		t.Fatalf("decode = %v, want the unknown-encoding refusal", err)
	}
	msg.MessageAttributes[attrEncoding] = stringAttr(encBase64)
	msg.Body = aws.String("not base64!!")
	if err := enc.decode(msg, "t", &got); !errors.Is(err, errBodyNotBase64) {
		t.Fatalf("decode of a body that says base64 and is not = %v", err)
	}
}

// wireMessage is the received form of an encoded publication, used by the
// round-trip tests and by FuzzSQSAttributes.
type wireMessage = types.Message

// newWireMessage builds the message a receive produces from what encode
// produced, so that a round trip goes through the same shape the service uses.
func newWireMessage(out encoded) *types.Message { return received(out) }
