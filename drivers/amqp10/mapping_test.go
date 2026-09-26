package amqp10_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
)

// mapperFor builds a mapping over the test profile's capability table.
func mapperFor(tb testing.TB, mode mq.MetadataMode) *aq.Mapper {
	tb.Helper()
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		tb.Fatalf("building the capability table: %v", err)
	}
	m, err := aq.MapperForTest(testProfile(), mode, caps, true)
	if err != nil {
		tb.Fatalf("building the mapping: %v", err)
	}
	return m
}

// TestMappingRoundTrip is the whole point of mapping.go: what goes out comes
// back, field for field.
//
// Every case is one field of the metadata or one shape of the headers, and
// each is checked in BOTH directions — a mapping that encoded a property and
// decoded it into a different field would pass a one-way test.
func TestMappingRoundTrip(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	created := time.Date(2026, 9, 14, 12, 0, 0, 123456789, time.UTC).Local()
	expiry := created.Add(time.Hour)

	src := &mq.Message{
		Topic: "topic",
		Key:   []byte("order-42"),
		Body:  []byte("payload"),
		ID:    "msg-1",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			CausationID:     "cause-1",
			ReplyTo:         "replies",
			CreatedAt:       created,
		},
	}
	src.Headers.Add("plain", []byte("value"))
	src.Headers.Add("binary", []byte{0x00, 0xff, 0xfe})
	src.Headers.Add("x-opt-broker-thing", []byte("annotation"))
	src.Headers.Add(aq.HeaderTo, []byte("node"))
	src.Headers.Add(aq.HeaderSubject, []byte("subject"))
	src.Headers.Add(aq.HeaderUserID, []byte{1, 2, 3})
	src.Headers.Add(aq.HeaderGroupSequence, []byte("7"))
	src.Headers.Add(aq.HeaderReplyToGroupID, []byte("reply-group"))
	src.Headers.Add(aq.HeaderAbsoluteExpiry, []byte(expiry.Format(time.RFC3339Nano)))
	src.Headers.Add(aq.HeaderFirstAcquirer, []byte("true"))

	buf := aq.NewBuf()
	out, err := m.Encode(buf, src, &mq.PublishOptions{TTL: 30 * time.Second, Priority: 4, PrioritySet: true})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// The native shape, which is what a non-iqmq consumer reads.
	if out.Properties == nil || out.Properties.GroupID == nil || *out.Properties.GroupID != "order-42" {
		t.Fatalf("Message.Key did not become the group-id property: %+v", out.Properties)
	}
	if out.Properties.MessageID != "msg-1" {
		t.Fatalf("message-id = %v, want msg-1", out.Properties.MessageID)
	}
	if out.Header == nil || !out.Header.Durable || out.Header.Priority != 4 || out.Header.TTL != 30*time.Second {
		t.Fatalf("the AMQP header is %+v, want durable, priority 4, ttl 30s", out.Header)
	}
	if got, ok := out.ApplicationProperties["plain"].(string); !ok || got != "value" {
		t.Fatalf("a UTF-8 header value travelled as %T (%v), want a string so that a non-iqmq consumer reads it",
			out.ApplicationProperties["plain"], out.ApplicationProperties["plain"])
	}
	if got, ok := out.ApplicationProperties["binary"].([]byte); !ok || !bytes.Equal(got, []byte{0x00, 0xff, 0xfe}) {
		t.Fatalf("a header value that is not valid UTF-8 travelled as %T, want an AMQP binary",
			out.ApplicationProperties["binary"])
	}
	if _, ok := out.Annotations["x-opt-broker-thing"]; !ok {
		t.Fatalf("a header under the profile's annotation prefix did not become a message annotation: %v", out.Annotations)
	}
	if _, ok := out.ApplicationProperties["x-opt-broker-thing"]; ok {
		t.Fatal("a header under the profile's annotation prefix ALSO became an application property")
	}

	// And back.
	var got mq.Message
	got.Topic = "topic"
	lossy, err := m.Decode(&got, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if lossy != 0 {
		t.Fatalf("the decode could not represent %d field(s) of a message this driver encoded itself", lossy)
	}
	if !bytes.Equal(got.Key, src.Key) {
		t.Fatalf("Key round-tripped as %q, want %q", got.Key, src.Key)
	}
	if !bytes.Equal(got.Body, src.Body) {
		t.Fatalf("Body round-tripped as %q, want %q", got.Body, src.Body)
	}
	if got.ID != src.ID {
		t.Fatalf("ID round-tripped as %q, want %q", got.ID, src.ID)
	}
	if got.Properties.ContentType != src.Properties.ContentType ||
		got.Properties.ContentEncoding != src.Properties.ContentEncoding ||
		got.Properties.CorrelationID != src.Properties.CorrelationID ||
		got.Properties.CausationID != src.Properties.CausationID ||
		got.Properties.ReplyTo != src.Properties.ReplyTo {
		t.Fatalf("the properties round-tripped as %+v, want %+v", got.Properties, src.Properties)
	}
	if !got.Properties.CreatedAt.Equal(src.Properties.CreatedAt) {
		t.Fatalf("CreatedAt round-tripped as %v, want %v", got.Properties.CreatedAt, src.Properties.CreatedAt)
	}
	for _, want := range []struct{ key, val string }{
		{"plain", "value"},
		{"x-opt-broker-thing", "annotation"},
		{aq.HeaderTo, "node"},
		{aq.HeaderSubject, "subject"},
		{aq.HeaderGroupSequence, "7"},
		{aq.HeaderReplyToGroupID, "reply-group"},
		{aq.HeaderFirstAcquirer, "true"},
	} {
		v, ok := got.Headers.Get(want.key)
		if !ok || string(v) != want.val {
			t.Fatalf("header %q round-tripped as %q (present=%v), want %q", want.key, v, ok, want.val)
		}
	}
	if v, ok := got.Headers.Get("binary"); !ok || !bytes.Equal(v, []byte{0x00, 0xff, 0xfe}) {
		t.Fatalf("a binary header round-tripped as %q, want the original bytes", v)
	}
	if v, ok := got.Headers.Get(aq.HeaderUserID); !ok || !bytes.Equal(v, []byte{1, 2, 3}) {
		t.Fatalf("the user-id header round-tripped as %q", v)
	}
	if v, ok := got.Headers.Get(aq.HeaderAbsoluteExpiry); !ok {
		t.Fatal("the absolute-expiry header did not round-trip")
	} else if parsed, perr := time.Parse(time.RFC3339Nano, string(v)); perr != nil || !parsed.Equal(expiry) {
		t.Fatalf("the absolute-expiry header round-tripped as %q (%v), want %v", v, perr, expiry)
	}
}

// TestMappingNilAndEmptyBody holds the one distinction
// [github.com/iqhive/iqmq.Message] promises and that a body mapping is most
// likely to lose: a nil Body is "no body" and an empty non-nil Body is "an
// empty body".
func TestMappingNilAndEmptyBody(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"one byte", []byte{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := aq.NewBuf()
			out, err := m.Encode(buf, &mq.Message{Body: tc.body}, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var got mq.Message
			if _, err := m.Decode(&got, out); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if (tc.body == nil) != (got.Body == nil) {
				t.Fatalf("a %s body round-tripped as nil=%v; a nil Body is \"no body\" and an empty one is "+
					"\"an empty body\", and both are valid", tc.name, got.Body == nil)
			}
			if !bytes.Equal(got.Body, tc.body) {
				t.Fatalf("a %s body round-tripped as %q, want %q", tc.name, got.Body, tc.body)
			}
		})
	}
}

// TestMappingStrictRefusesWhatItCannotCarry is MetadataNativeStrict's rule
// unsupported populated metadata fails with ErrUnsupported BEFORE any
// I/O, and no hidden envelope is used.
func TestMappingStrictRefusesWhatItCannotCarry(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		name  string
		build func() *mq.Message
		code  mq.ErrorCode
	}{
		{
			name: "a repeated header key",
			build: func() *mq.Message {
				msg := &mq.Message{Body: []byte("x")}
				msg.Headers.Add("k", []byte("1"))
				msg.Headers.Add("k", []byte("2"))
				return msg
			},
			// application-properties is a map, so one key cannot carry two
			// values and no profile can make it otherwise.
			code: mq.CodeUnsupported,
		},
		{
			name: "a key that is not valid UTF-8",
			build: func() *mq.Message {
				msg := &mq.Message{Body: []byte("x")}
				msg.Headers.Add(string([]byte{0xff, 0xfe}), []byte("1"))
				return msg
			},
			code: mq.CodeInvalidArgument,
		},
		{
			name: "a reserved header this driver does not map",
			build: func() *mq.Message {
				msg := &mq.Message{Body: []byte("x")}
				msg.Headers.Add(mq.ReservedHeaderPrefix+"invented", []byte("1"))
				return msg
			},
			code: mq.CodeInvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := aq.NewBuf()
			_, err := m.Encode(buf, tc.build(), nil)
			if err == nil {
				t.Fatalf("strict accepted %s; the specification requires unsupported populated metadata to fail with "+
					"ErrUnsupported before any I/O", tc.name)
			}
			assertCode(t, err, tc.code, tc.name)
		})
	}
}

// TestMappingEnvelopeCarriesWhatNativeFieldsCannot is the other half:
// what strict refuses, the envelope carries — and what the envelope carries
// comes back.
func TestMappingEnvelopeCarriesWhatNativeFieldsCannot(t *testing.T) {
	for _, mode := range []mq.MetadataMode{mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			m := mapperFor(t, mode)
			src := &mq.Message{Body: []byte("payload"), Key: []byte{0xff, 0xfe}}
			src.Headers.Add("k", []byte("1"))
			src.Headers.Add("k", []byte("2"))
			buf := aq.NewBuf()
			out, err := m.Encode(buf, src, nil)
			if err != nil {
				t.Fatalf("Encode in %v: %v", mode, err)
			}
			if len(out.ApplicationProperties) != 0 {
				t.Fatalf("the enveloped message also carries %d application propert(ies); a message that put half "+
					"its metadata in the envelope and half beside it would decode to something else",
					len(out.ApplicationProperties))
			}
			var got mq.Message
			if _, err := m.Decode(&got, out); err != nil {
				t.Fatalf("Decode in %v: %v", mode, err)
			}
			if !bytes.Equal(got.Key, src.Key) {
				t.Fatalf("the envelope lost the binary key: %q", got.Key)
			}
			vals := got.Headers.Values("k")
			if len(vals) != 2 || string(vals[0]) != "1" || string(vals[1]) != "2" {
				t.Fatalf("the envelope round-tripped the repeated key as %q, want both values in order", vals)
			}
			if !bytes.Equal(got.Body, src.Body) {
				t.Fatalf("the envelope lost the body: %q", got.Body)
			}
		})
	}
}

// TestMappingAdaptiveUsesNativeFieldsWhenItCan is the distinction that makes
// MetadataAdaptive worth having: a message native fields can carry is NOT
// enveloped, so a non-iqmq consumer reads it.
func TestMappingAdaptiveUsesNativeFieldsWhenItCan(t *testing.T) {
	m := mapperFor(t, mq.MetadataAdaptive)
	src := &mq.Message{Body: []byte("payload"), Key: []byte("key")}
	src.Headers.Add("k", []byte("1"))
	buf := aq.NewBuf()
	out, err := m.Encode(buf, src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got, ok := out.ApplicationProperties["k"].(string); !ok || got != "1" {
		t.Fatalf("adaptive enveloped a message whose metadata the native fields can carry: %+v", out)
	}
	if out.Properties == nil || out.Properties.GroupID == nil || *out.Properties.GroupID != "key" {
		t.Fatalf("adaptive did not put the key in the group-id property: %+v", out.Properties)
	}
}

// TestMappingAdaptiveEnvelopesABodyItsDecoderWouldClaim is the exception to the
// test above, and the reason it exists is the decoder: under MetadataAdaptive
// the read path decides "is this an envelope" from the body, so a native body
// that BEGINS like one is claimed on the way back and refused when it does not
// decode. A message whose metadata the native fields can carry is therefore
// still enveloped when its body starts with the envelope's magic, which is the
// kit's own publish-side rule  and the one this driver's
// predicate skipped.
func TestMappingAdaptiveEnvelopesABodyItsDecoderWouldClaim(t *testing.T) {
	m := mapperFor(t, mq.MetadataAdaptive)
	// FuzzAMQPMapping's seed accf8580a55f6015, reduced: the magic, a
	// version, and bytes that are not a whole envelope of any layout.
	src := &mq.Message{Body: []byte("IQMQ\x01\x02\x00\x0200"), Key: []byte("0"), ID: "0"}
	src.Headers.Add("0", []byte("0"))
	out, err := m.Encode(aq.NewBuf(), src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(out.ApplicationProperties) != 0 {
		t.Fatalf("adaptive published a body its own decoder would claim as an envelope NATIVELY (application "+
			"properties %v): the decode then refuses it and the record can be neither delivered nor settled",
			out.ApplicationProperties)
	}
	var got mq.Message
	if _, err := m.Decode(&got, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got.Body, src.Body) || !bytes.Equal(got.Key, src.Key) || got.ID != src.ID {
		t.Fatalf("the round trip returned body=%q key=%q id=%q, want %q %q %q",
			got.Body, got.Key, got.ID, src.Body, src.Key, src.ID)
	}
	if v, ok := got.Headers.Get("0"); !ok || string(v) != "0" {
		t.Fatalf("the header came back as %q (present=%v), want %q", v, ok, "0")
	}
}

// TestMappingLenientDropsAndCounts is MetadataLenientDrop's rule:
// unsupported metadata is dropped WITH a count, never silently. The specification forbids
// "silently dropping unsupported metadata" in every mode.
func TestMappingLenientDropsAndCounts(t *testing.T) {
	m := mapperFor(t, mq.MetadataLenientDrop)
	src := &mq.Message{Body: []byte("x")}
	src.Headers.Add("k", []byte("first"))
	src.Headers.Add("k", []byte("second"))
	buf := aq.NewBuf()
	out, err := m.Encode(buf, src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got, ok := out.ApplicationProperties["k"].(string); !ok || got != "first" {
		t.Fatalf("the surviving value is %v, want the first", out.ApplicationProperties["k"])
	}
	if buf.Dropped() != 1 {
		t.Fatalf("the encode dropped %d value(s) and counted %d; the specification forbids dropping metadata silently",
			1, buf.Dropped())
	}
}

// TestMappingRefusesControlsTheProfileCannotExpress is the specification's "enforced before
// I/O", and it is the seam's test as much as the mapping's: the same message is
// accepted by a profile that can express the control and refused by one that
// cannot.
func TestMappingRefusesControlsTheProfileCannotExpress(t *testing.T) {
	rich := testProfile()
	bare := bareProfile()
	caps, err := aq.CapabilitiesForTest(rich, aq.Facts{})
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	bareCaps, err := aq.CapabilitiesForTest(bare, aq.Facts{})
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	richMapper, err := aq.MapperForTest(rich, mq.MetadataNativeStrict, caps, true)
	if err != nil {
		t.Fatalf("mapper: %v", err)
	}
	bareMapper, err := aq.MapperForTest(bare, mq.MetadataNativeStrict, bareCaps, true)
	if err != nil {
		t.Fatalf("mapper: %v", err)
	}
	for _, tc := range []struct {
		name string
		opts mq.PublishOptions
	}{
		{"a delay", mq.PublishOptions{Delay: time.Minute}},
		{"a de-duplication key", mq.PublishOptions{DedupKey: "k"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := &mq.Message{Body: []byte("x")}
			if _, err := richMapper.Encode(aq.NewBuf(), msg, &tc.opts); err != nil {
				t.Fatalf("the profile that CAN express %s refused it: %v", tc.name, err)
			}
			_, err := bareMapper.Encode(aq.NewBuf(), msg, &tc.opts)
			if err == nil {
				t.Fatalf("the profile that cannot express %s accepted it; the specification requires the refusal before any I/O",
					tc.name)
			}
			assertIs(t, err, mq.ErrUnsupported, tc.name+" on a profile that cannot express it")
		})
	}
	// A partition is the one control NO profile can express: AMQP 1.0 has no
	// partitions at all, so the refusal is the core's.
	_, err = richMapper.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{Partition: 1, PartitionSet: true})
	assertIs(t, err, mq.ErrUnsupported, "a partition")
}

// TestMappingDelayAndDedupReachTheProfilesSlots checks that the profile's rule
// really decides WHERE a control goes, which is the whole reason the rules are
// structs with a slot in them.
func TestMappingDelayAndDedupReachTheProfilesSlots(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	buf := aq.NewBuf()
	out, err := m.Encode(buf, &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{Delay: 90 * time.Second, DedupKey: "dup-1"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got, ok := out.Annotations["x-opt-delivery-delay"].(int64); !ok || got != 90_000 {
		t.Fatalf("the delay reached %v (%T) under x-opt-delivery-delay, want 90000 milliseconds as an int64",
			out.Annotations["x-opt-delivery-delay"], out.Annotations["x-opt-delivery-delay"])
	}
	if got, ok := out.ApplicationProperties["_AMQ_DUPL_ID"].(string); !ok || got != "dup-1" {
		t.Fatalf("the de-duplication key reached %v, want the application property the profile named",
			out.ApplicationProperties["_AMQ_DUPL_ID"])
	}
}

// TestMappingRefusesADelayBeyondTheProfilesCeiling holds the profile's own Max,
// which the specification makes an advertised limit and makes a refusal before I/O.
func TestMappingRefusesADelayBeyondTheProfilesCeiling(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	_, err := m.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{Delay: 2 * time.Hour})
	assertCode(t, err, mq.CodeInvalidArgument, "a delay beyond the profile's ceiling")
}

// TestMappingRefusesASubMillisecondTTL is a small rule with a sharp edge: an
// AMQP ttl of zero means "no expiry", so a time to live that rounds to zero
// would publish a message that never expires after the caller asked for one
// that expires at once.
func TestMappingRefusesASubMillisecondTTL(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	_, err := m.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{TTL: time.Microsecond})
	assertCode(t, err, mq.CodeInvalidArgument, "a sub-millisecond time to live")
	out, err := m.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{TTL: time.Millisecond})
	if err != nil {
		t.Fatalf("a one-millisecond time to live was refused: %v", err)
	}
	if out.Header.TTL != time.Millisecond {
		t.Fatalf("the header's ttl is %v, want 1ms", out.Header.TTL)
	}
}

// TestMappingRefusesABodyItCannotRepresent is the other direction: a
// received message this driver cannot represent is a REFUSAL and not a silent
// drop, because handing a caller a message whose body is missing would be
// handing it something it cannot act on.
func TestMappingRefusesABodyItCannotRepresent(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		name string
		msg  *amqp.Message
	}{
		{"an amqp-sequence body", &amqp.Message{Sequence: [][]any{{int64(1), "two"}}}},
		{"an amqp-value map body", &amqp.Message{Value: map[string]any{"a": 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got mq.Message
			_, err := m.Decode(&got, tc.msg)
			assertCode(t, err, mq.CodeCorruptMessage, tc.name)
		})
	}
}

// TestMappingDecodesForeignValueBodies is the friendlier half: a body another
// producer sent as an AMQP string or binary VALUE is a body this driver can
// represent, and refusing it would make every message from a JMS client
// undeliverable.
func TestMappingDecodesForeignValueBodies(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		name string
		in   *amqp.Message
		want []byte
	}{
		{"a string value", &amqp.Message{Value: "hello"}, []byte("hello")},
		{"a binary value", &amqp.Message{Value: []byte("hello")}, []byte("hello")},
		{"a null value", &amqp.Message{Value: amqp.Null{}}, nil},
		{"two data sections", &amqp.Message{Data: [][]byte{[]byte("he"), []byte("llo")}}, []byte("hello")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got mq.Message
			if _, err := m.Decode(&got, tc.in); err != nil {
				t.Fatalf("Decode %s: %v", tc.name, err)
			}
			if !bytes.Equal(got.Body, tc.want) {
				t.Fatalf("%s decoded to %q, want %q", tc.name, got.Body, tc.want)
			}
			if (tc.want == nil) != (got.Body == nil) {
				t.Fatalf("%s decoded to nil=%v, want nil=%v", tc.name, got.Body == nil, tc.want == nil)
			}
		})
	}
}

// TestMappingCountsWhatItCannotRepresent holds the count rather than the
// silence. An application property of a type no simple AMQP value covers is
// legal only from a non-conforming peer, and the specification forbids dropping it without
// saying so.
func TestMappingCountsWhatItCannotRepresent(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	in := &amqp.Message{
		Data:                  [][]byte{[]byte("x")},
		ApplicationProperties: map[string]any{"weird": []string{"a", "b"}},
		Annotations:           amqp.Annotations{uint64(7): "keyed by a ulong"},
	}
	var got mq.Message
	lossy, err := m.Decode(&got, in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if lossy != 2 {
		t.Fatalf("the decode reported %d unrepresentable field(s), want 2: an application property of a type no "+
			"simple AMQP value covers, and an annotation keyed by a ulong", lossy)
	}
	if _, ok := got.Headers.Get("weird"); ok {
		t.Fatal("an unrepresentable value became an empty header, which is indistinguishable from an AMQP null " +
			"a producer really sent")
	}
}

// TestMappingRendersForeignSimpleValues is the rest of the decode table: every
// simple AMQP type a producer may use reaches the caller as bytes rather than
// vanishing.
func TestMappingRendersForeignSimpleValues(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	uuid := amqp.UUID{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := &amqp.Message{
		Data: [][]byte{[]byte("x")},
		ApplicationProperties: map[string]any{
			"i64":  int64(-7),
			"u32":  uint32(7),
			"f64":  float64(1.5),
			"bt":   true,
			"bf":   false,
			"sym":  amqp.Symbol("symbolic"),
			"time": when,
			"uuid": uuid,
			"null": nil,
		},
	}
	var got mq.Message
	lossy, err := m.Decode(&got, in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if lossy != 0 {
		t.Fatalf("the decode could not represent %d of nine simple AMQP values", lossy)
	}
	for _, want := range []struct{ key, val string }{
		{"i64", "-7"},
		{"u32", "7"},
		{"f64", "1.5"},
		{"bt", "true"},
		{"bf", "false"},
		{"sym", "symbolic"},
		{"time", when.Format(time.RFC3339Nano)},
		{"uuid", "01234567-89ab-cdef-0123-456789abcdef"},
	} {
		v, ok := got.Headers.Get(want.key)
		if !ok || string(v) != want.val {
			t.Fatalf("%s rendered as %q (present=%v), want %q", want.key, v, ok, want.val)
		}
	}
	if v, ok := got.Headers.Get("null"); !ok || v != nil {
		t.Fatalf("an AMQP null rendered as %q (present=%v), want a present header with a nil value", v, ok)
	}
}

// TestMappingReservedHeadersRoundTripThroughAPublish is what makes a forwarding
// consumer possible: a message this driver decoded carries reserved header
// names, and the specification refuses those from a caller — so a driver that applied the
// rule bluntly could not republish anything it received.
func TestMappingReservedHeadersRoundTripThroughAPublish(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	subject := "subj"
	to := "node"
	in := &amqp.Message{
		Data:       [][]byte{[]byte("x")},
		Properties: &amqp.MessageProperties{Subject: &subject, To: &to},
	}
	var decoded mq.Message
	if _, err := m.Decode(&decoded, in); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	out, err := m.Encode(aq.NewBuf(), &decoded, nil)
	if err != nil {
		t.Fatalf("republishing a message this driver decoded: %v; the reserved names the decode wrote have to be "+
			"publishable again, or a forwarding consumer is impossible", err)
	}
	if out.Properties == nil || out.Properties.Subject == nil || *out.Properties.Subject != subject {
		t.Fatalf("the subject did not survive the round trip through the header set: %+v", out.Properties)
	}
	if out.Properties.To == nil || *out.Properties.To != to {
		t.Fatalf("the `to` property did not survive: %+v", out.Properties)
	}
	if _, ok := out.ApplicationProperties[aq.HeaderSubject]; ok {
		t.Fatal("a reserved header was published as an application property rather than consumed into its property")
	}
}

// TestReservedHeaderNamesTheSetItMaps holds the exported predicate to the set,
// because a caller uses it to decide whether a key is publishable.
func TestReservedHeaderNamesTheSetItMaps(t *testing.T) {
	for _, k := range []string{
		aq.HeaderTo, aq.HeaderSubject, aq.HeaderUserID, aq.HeaderAbsoluteExpiry,
		aq.HeaderGroupSequence, aq.HeaderReplyToGroupID, aq.HeaderCausationID, aq.HeaderFirstAcquirer,
	} {
		if !aq.ReservedHeader(k) {
			t.Errorf("ReservedHeader(%q) = false, want true", k)
		}
		if !strings.HasPrefix(k, mq.ReservedHeaderPrefix) {
			t.Errorf("%q is not under the reserved prefix", k)
		}
	}
	for _, k := range []string{"", "plain", mq.ReservedHeaderPrefix + "invented", "x-opt-thing"} {
		if aq.ReservedHeader(k) {
			t.Errorf("ReservedHeader(%q) = true, want false", k)
		}
	}
}

// TestRepresentableAnswersTheSameQuestionAsStrict holds the exported predicate
// to the mode it describes: a caller that asks before publishing must get the
// answer strict would give.
func TestRepresentableAnswersTheSameQuestionAsStrict(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	dup := &mq.Message{Body: []byte("x")}
	dup.Headers.Add("k", []byte("1"))
	dup.Headers.Add("k", []byte("2"))
	ok := &mq.Message{Body: []byte("x")}
	ok.Headers.Add("k", []byte("1"))

	if err := aq.Representable(testProfile(), ok); err != nil {
		t.Fatalf("Representable refused a message strict accepts: %v", err)
	}
	if _, err := m.Encode(aq.NewBuf(), ok, nil); err != nil {
		t.Fatalf("strict refused a message Representable accepts: %v", err)
	}
	if err := aq.Representable(testProfile(), dup); err == nil {
		t.Fatal("Representable accepted a message strict refuses")
	}
	if _, err := m.Encode(aq.NewBuf(), dup, nil); err == nil {
		t.Fatal("strict accepted a message Representable refuses")
	}
}

// TestMappingRefusesAnOversizedBodyBeforeAnyIO is the rule over the specification's
// advertised limit: the refusal happens in the mapping, before the transfer.
func TestMappingRefusesAnOversizedBodyBeforeAnyIO(t *testing.T) {
	p := testProfile()
	p.Limits = func(_ aq.Facts, lim mq.Limits) mq.Limits {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: 8, Known: true}
		return lim
	}
	caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	m, err := aq.MapperForTest(p, mq.MetadataNativeStrict, caps, true)
	if err != nil {
		t.Fatalf("mapper: %v", err)
	}
	if _, err := m.Encode(aq.NewBuf(), &mq.Message{Body: make([]byte, 8)}, nil); err != nil {
		t.Fatalf("a body at the limit was refused: %v", err)
	}
	_, err = m.Encode(aq.NewBuf(), &mq.Message{Body: make([]byte, 9)}, nil)
	assertIs(t, err, mq.ErrMessageTooLarge, "a body over the advertised limit")
}

// TestABinaryKeyTravelsAsABinaryProperty is "a key is bytes",
// over a protocol whose key property is a string.
//
// `group-id` is an AMQP string, so a key that is not valid UTF-8 has no
// property to live in — and refusing the publication, which is what this driver
// used to do, is the wrong answer twice over: MetadataNativeStrict is the
// DEFAULT mode, so a caller whose keys are hashes found every publication
// refused until they changed a mode they had never set, and the portable API
// promises the key round trip in every mode. An application property carries
// binary, so the key has a native home after all.
//
// Both arms are here because the split is the whole point: a TEXTUAL key stays
// in `group-id`, where a broker that orders a message group can see it, and
// only a key that cannot go there takes the property.
func TestABinaryKeyTravelsAsABinaryProperty(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	for _, tc := range []struct {
		name    string
		key     []byte
		inGroup bool
	}{
		{"a textual key is the message group", []byte("customer-7"), true},
		{"a key that is not valid UTF-8 is a binary property", []byte{0x00, 0xff}, false},
		{"a key that is nearly UTF-8", []byte{0xc3}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := aq.NewBuf()
			out, err := m.Encode(buf, &mq.Message{Body: []byte("x"), Key: tc.key}, nil)
			if err != nil {
				t.Fatalf("encoding a message with key %q: %v", tc.key, err)
			}
			gotGroup := out.Properties != nil && out.Properties.GroupID != nil
			if gotGroup != tc.inGroup {
				t.Errorf("the key is in group-id = %v, want %v", gotGroup, tc.inGroup)
			}
			if _, inProp := out.ApplicationProperties[aq.HeaderKey]; inProp == tc.inGroup {
				t.Errorf("the key is in the %s property = %v, want %v", aq.HeaderKey, inProp, !tc.inGroup)
			}
			var back mq.Message
			if _, err := m.Decode(&back, out); err != nil {
				t.Fatalf("decoding: %v", err)
			}
			if !bytes.Equal(back.Key, tc.key) {
				t.Errorf("the key came back as %q, want %q", back.Key, tc.key)
			}
			if _, leaked := back.Headers.Get(aq.HeaderKey); leaked {
				t.Errorf("the reserved carrier %q reached the caller's headers", aq.HeaderKey)
			}
		})
	}
}
