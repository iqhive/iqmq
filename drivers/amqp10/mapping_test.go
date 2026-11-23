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

// keyBoundMapperFor is [mapperFor] over the test profile with a bound on a
// header key ([aq.Profile.MaxHeaderKeyBytes]).
func keyBoundMapperFor(tb testing.TB, mode mq.MetadataMode, bound int) *aq.Mapper {
	tb.Helper()
	p := testProfile()
	p.MaxHeaderKeyBytes = bound
	caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
	if err != nil {
		tb.Fatalf("building the capability table: %v", err)
	}
	m, err := aq.MapperForTest(p, mode, caps, true)
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

// TestMappingEnvelopeKeepsTheKeysGroup is the one native field an enveloped
// message still needs: the `group-id` its key stands for.
//
// A broker dispatches a message group by `group-id` and cannot read an
// envelope, and the wrapped message has no key. So every enveloped
// publication went out in no group, and ordering by key, which ActiveMQ
// claims Native through the group, ordered nothing under
// MetadataPortableEnvelope, or under MetadataAdaptive once a repeated header
// made it envelope. The key still comes back from the envelope.
func TestMappingEnvelopeKeepsTheKeysGroup(t *testing.T) {
	for _, mode := range []mq.MetadataMode{mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			m := mapperFor(t, mode)
			src := &mq.Message{Body: []byte("payload"), Key: []byte("order-42")}
			// A repeated key has no native form, so adaptive envelopes too.
			src.Headers.Add("k", []byte("1"))
			src.Headers.Add("k", []byte("2"))
			out, err := m.Encode(aq.NewBuf(), src, nil)
			if err != nil {
				t.Fatalf("Encode in %v: %v", mode, err)
			}
			if out.Properties == nil || out.Properties.GroupID == nil || *out.Properties.GroupID != "order-42" {
				t.Fatalf("the enveloped message went out with group-id %v, want the key %q: a broker orders a "+
					"group by that property and cannot read the envelope", out.Properties, src.Key)
			}
			var got mq.Message
			if _, err := m.Decode(&got, out); err != nil {
				t.Fatalf("Decode in %v: %v", mode, err)
			}
			if !bytes.Equal(got.Key, src.Key) || len(got.Headers.Values("k")) != 2 {
				t.Fatalf("the enveloped message came back as key %q and headers %v", got.Key, got.Headers.Values("k"))
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

// TestMappingHoldsAHeaderKeyToTheProfileBound is [aq.Profile.MaxHeaderKeyBytes]
// in each metadata mode: a key longer than the bound is refused before any I/O
// with MessageTooLarge under MetadataNativeStrict, as an application property
// or as a message annotation, carried in the envelope by MetadataAdaptive and
// MetadataPortableEnvelope, and dropped and counted by MetadataLenientDrop. A
// key at the bound travels natively, and a profile with no bound carries any
// key.
func TestMappingHoldsAHeaderKeyToTheProfileBound(t *testing.T) {
	p := testProfile()
	p.MaxHeaderKeyBytes = 16
	mapper := func(mode mq.MetadataMode) *aq.Mapper { return keyBoundMapperFor(t, mode, p.MaxHeaderKeyBytes) }
	fits, long, annotated := strings.Repeat("k", 16), strings.Repeat("k", 17), "x-opt-"+strings.Repeat("a", 11)
	message := func(keys ...string) *mq.Message {
		m := &mq.Message{Body: []byte("x")}
		for _, k := range keys {
			m.Headers.Add(k, []byte("v"))
		}
		return m
	}

	strict := mapper(mq.MetadataNativeStrict)
	switch out, err := strict.Encode(aq.NewBuf(), message(fits), nil); {
	case err != nil:
		t.Errorf("a key at the bound under NativeStrict: %v, want it carried natively", err)
	case out.ApplicationProperties[fits] != "v":
		t.Errorf("a key at the bound under NativeStrict travelled as %v, want an application property", out.ApplicationProperties)
	}
	for _, k := range []string{long, annotated} {
		_, err := strict.Encode(aq.NewBuf(), message(k), nil)
		if mq.CodeOf(err) != mq.CodeMessageTooLarge {
			t.Errorf("a %d-byte key under NativeStrict = %v (code %v), want CodeMessageTooLarge", len(k), err, mq.CodeOf(err))
		}
		if err := aq.Representable(p, message(k)); mq.CodeOf(err) != mq.CodeMessageTooLarge {
			t.Errorf("Representable of a %d-byte key = %v, want CodeMessageTooLarge", len(k), err)
		}
	}

	for _, mode := range []mq.MetadataMode{mq.MetadataAdaptive, mq.MetadataPortableEnvelope} {
		m := mapper(mode)
		out, err := m.Encode(aq.NewBuf(), message(long), nil)
		if err != nil {
			t.Fatalf("a key over the bound under %v: %v, want it carried in the envelope", mode, err)
		}
		if _, native := out.ApplicationProperties[long]; native {
			t.Errorf("under %v the key over the bound travelled as an application property", mode)
		}
		var got mq.Message
		if _, err := m.Decode(&got, out); err != nil {
			t.Fatalf("decoding the enveloped message under %v: %v", mode, err)
		}
		if v, ok := got.Headers.Get(long); !ok || string(v) != "v" {
			t.Errorf("under %v the key over the bound came back as %q (present %v)", mode, v, ok)
		}
	}

	lenient := mapper(mq.MetadataLenientDrop)
	buf := aq.NewBuf()
	out, err := lenient.Encode(buf, message(fits, long, annotated), nil)
	if err != nil {
		t.Fatalf("LenientDrop: %v", err)
	}
	if _, kept := out.ApplicationProperties[long]; kept || out.ApplicationProperties[fits] != "v" || buf.Dropped() != 2 {
		t.Errorf("LenientDrop kept the long key %v, the fitting one %v, and counted %d drops; want only the fitting "+
			"one kept and 2 drops counted", kept, out.ApplicationProperties[fits], buf.Dropped())
	}

	unbounded := mapperFor(t, mq.MetadataNativeStrict)
	if _, err := unbounded.Encode(aq.NewBuf(), message(strings.Repeat("k", 300)), nil); err != nil {
		t.Errorf("a 300-byte key on a profile with no bound: %v, want it carried", err)
	}
	p.MaxHeaderKeyBytes = -1
	if err := p.Validate(); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("a negative MaxHeaderKeyBytes validated as %v, want CodeInvalidConfig", err)
	}
}

// TestMappingKeepsTheBrokersOwnNamesOut is [aq.Profile.BrokerHeader] in each
// metadata mode and on the way back: a caller's header under a name the
// broker reads is refused before any I/O with InvalidArgument under
// MetadataNativeStrict, as an application property or as a message
// annotation, carried in the envelope by MetadataAdaptive and
// MetadataPortableEnvelope, and dropped and counted by MetadataLenientDrop; a
// delivery's own entry under one is left out of its headers, uncounted. The
// profile's own controls still go out under the names it reserves, and a
// profile that reserves nothing carries and hands back every name. A predicate
// that matches the driver's own iqmq- names is not asked about them.
func TestMappingKeepsTheBrokersOwnNamesOut(t *testing.T) {
	p := testProfile()
	p.BrokerHeader = func(k string) bool {
		return strings.HasPrefix(k, "JMS") || k == "x-opt-delivery-delay" || strings.HasPrefix(k, mq.ReservedHeaderPrefix)
	}
	mapper := func(p aq.Profile, mode mq.MetadataMode) *aq.Mapper {
		caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
		if err != nil {
			t.Fatalf("building the capability table: %v", err)
		}
		m, err := aq.MapperForTest(p, mode, caps, true)
		if err != nil {
			t.Fatalf("building the mapping: %v", err)
		}
		return m
	}
	message := func(keys ...string) *mq.Message {
		m := &mq.Message{Body: []byte("x")}
		for _, k := range keys {
			m.Headers.Add(k, []byte("1"))
		}
		return m
	}

	strict := mapper(p, mq.MetadataNativeStrict)
	for _, k := range []string{"JMSExpiration", "x-opt-delivery-delay"} {
		if _, err := strict.Encode(aq.NewBuf(), message("tenant", k), nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("a caller's %q under NativeStrict = %v, want CodeInvalidArgument before any I/O", k, err)
		}
		if err := aq.Representable(p, message(k)); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("Representable of %q = %v, want CodeInvalidArgument", k, err)
		}
	}
	out, err := strict.Encode(aq.NewBuf(), message("tenant", "x-opt-other"), &mq.PublishOptions{Delay: time.Minute})
	switch {
	case err != nil:
		t.Fatalf("ordinary headers and the profile's own delay under NativeStrict: %v", err)
	case out.ApplicationProperties["tenant"] != "1" || out.Annotations["x-opt-other"] != "1":
		t.Errorf("the ordinary headers travelled as %v and %v", out.ApplicationProperties, out.Annotations)
	case out.Annotations["x-opt-delivery-delay"] != int64(60_000):
		t.Errorf("the profile's own delay travelled as %v, want it under the name it reserves", out.Annotations)
	}
	own := &mq.Message{Body: []byte("x"), Key: []byte("\x00\xff"), Properties: mq.Properties{CausationID: "cause"}}
	own.Headers.Add(aq.HeaderSubject, []byte("s"))
	if out, err := strict.Encode(aq.NewBuf(), own, nil); err != nil {
		t.Errorf("the driver's own iqmq- names under NativeStrict: %v", err)
	} else {
		var got mq.Message
		_, err := strict.Decode(&got, out)
		subject, _ := got.Headers.Get(aq.HeaderSubject)
		if err != nil || !bytes.Equal(got.Key, own.Key) || got.Properties.CausationID != "cause" || string(subject) != "s" {
			t.Errorf("the driver's own names came back as key %q, causation %q, subject %q (%v)", got.Key,
				got.Properties.CausationID, subject, err)
		}
	}

	for _, mode := range []mq.MetadataMode{mq.MetadataAdaptive, mq.MetadataPortableEnvelope} {
		m := mapper(p, mode)
		out, err := m.Encode(aq.NewBuf(), message("tenant", "JMSExpiration", "x-opt-delivery-delay"), nil)
		if err != nil {
			t.Fatalf("a caller's broker names under %v: %v, want them carried in the envelope", mode, err)
		}
		if len(out.ApplicationProperties) != 0 || len(out.Annotations) != 0 || !bytes.HasPrefix(out.Data[0], []byte("IQMQ")) {
			t.Errorf("under %v the message travelled as properties %v and annotations %v, want it enveloped", mode,
				out.ApplicationProperties, out.Annotations)
		}
		var got mq.Message
		if _, err := m.Decode(&got, out); err != nil {
			t.Fatalf("decoding the enveloped message under %v: %v", mode, err)
		}
		for _, k := range []string{"tenant", "JMSExpiration", "x-opt-delivery-delay"} {
			if v, _ := got.Headers.Get(k); string(v) != "1" {
				t.Errorf("under %v %q came back from the envelope as %q", mode, k, v)
			}
		}
	}

	buf := aq.NewBuf()
	out, err = mapper(p, mq.MetadataLenientDrop).Encode(buf, message("tenant", "JMSExpiration", "x-opt-delivery-delay"), nil)
	switch {
	case err != nil:
		t.Fatalf("LenientDrop: %v", err)
	case len(out.ApplicationProperties) != 1 || out.ApplicationProperties["tenant"] != "1" || len(out.Annotations) != 0:
		t.Errorf("LenientDrop sent properties %v and annotations %v, want only tenant", out.ApplicationProperties,
			out.Annotations)
	case buf.Dropped() != 2:
		t.Errorf("LenientDrop counted %d drops, want 2", buf.Dropped())
	}

	delivery := &amqp.Message{
		Data:                  [][]byte{[]byte("x")},
		ApplicationProperties: map[string]any{"JMSXDeliveryCount": "", "tenant": "acme"},
		Annotations:           amqp.Annotations{"x-opt-delivery-delay": int64(300), "x-opt-other": "v"},
	}
	for name, want := range map[string]int{"reserving": 2, "reserving nothing": 4} {
		q := p
		if want == 4 {
			q.BrokerHeader = nil
		}
		var got mq.Message
		lossy, err := mapper(q, mq.MetadataNativeStrict).Decode(&got, delivery)
		if err != nil || lossy != 0 || got.Headers.Len() != want {
			t.Errorf("a profile %s decoded %d headers (%v, lossy %d, %v), want %d", name, got.Headers.Len(),
				headerKeys(&got), lossy, err, want)
		}
	}
	if _, err := mapper(func() aq.Profile { q := p; q.BrokerHeader = nil; return q }(), mq.MetadataNativeStrict).
		Encode(aq.NewBuf(), message("JMSExpiration"), nil); err != nil {
		t.Errorf("a profile that reserves nothing refused %q: %v", "JMSExpiration", err)
	}
}

// headerKeys is a message's header keys, for a failure message.
func headerKeys(m *mq.Message) []string {
	var keys []string
	for k := range m.Headers.All() {
		keys = append(keys, k)
	}
	return keys
}

// TestAdaptiveEnvelopesNothingElseForTheKeyBound holds the second envelope
// under MetadataAdaptive, the one for a key over [aq.Profile.MaxHeaderKeyBytes],
// to that key. A message the kit's adaptive Wrap hands back for another reason
// goes natively, as it did before the bound: an empty header key travels as an
// application property named "", on a profile with the bound and on one
// without. The first version of the bound enveloped it, on drivers/activemq
// too. A key over the bound in the same message is what envelopes it.
func TestAdaptiveEnvelopesNothingElseForTheKeyBound(t *testing.T) {
	long := strings.Repeat("k", 17)
	message := func(keys ...string) *mq.Message {
		m := &mq.Message{Body: []byte("x")}
		for _, k := range keys {
			m.Headers.Add(k, []byte("v"))
		}
		return m
	}
	enveloped := func(out *amqp.Message) bool {
		return len(out.ApplicationProperties) == 0 && len(out.Data) > 0 && bytes.HasPrefix(out.Data[0], []byte("IQMQ"))
	}
	for name, m := range map[string]*aq.Mapper{
		"a bound of 16": keyBoundMapperFor(t, mq.MetadataAdaptive, 16),
		"no bound":      mapperFor(t, mq.MetadataAdaptive),
	} {
		out, err := m.Encode(aq.NewBuf(), message(""), nil)
		if err != nil {
			t.Fatalf("%s: an empty key under Adaptive: %v", name, err)
		}
		if enveloped(out) || out.ApplicationProperties[""] != "v" {
			t.Errorf("%s: an empty key under Adaptive travelled enveloped %v, as properties %v; want it natively, "+
				"as an application property named \"\"", name, enveloped(out), out.ApplicationProperties)
		}
	}
	out, err := keyBoundMapperFor(t, mq.MetadataAdaptive, 16).Encode(aq.NewBuf(), message("", long), nil)
	if err != nil {
		t.Fatalf("an empty key and one over the bound under Adaptive: %v", err)
	}
	if !enveloped(out) {
		t.Errorf("a message with a key over the bound travelled natively, as properties %v", out.ApplicationProperties)
	}
}

// TestAKeyThatIsNotUTF8IsTheCallersMistakeInEveryMode: an AMQP
// application-property name is a string, and so is an envelope's header key,
// so a header key that is not valid UTF-8 has nowhere to go in any mode.
// MetadataNativeStrict and MetadataLenientDrop refuse it with
// CodeInvalidArgument before any I/O. MetadataAdaptive and
// MetadataPortableEnvelope refused it too, with the envelope check's
// CodeCorruptMessage, which says received data could not be read: on
// RabbitMQ 4.3.5 a Publish failed "CorruptMessage: wire: invalid UTF-8 in
// string field" for the caller's own message. Every mode gives the same
// CodeInvalidArgument, on a profile with a key bound and on one without
// (drivers/activemq has none).
func TestAKeyThatIsNotUTF8IsTheCallersMistakeInEveryMode(t *testing.T) {
	const key = "k\xff"
	message := func() *mq.Message {
		m := &mq.Message{Body: []byte("x")}
		m.Headers.Add(key, []byte("v"))
		return m
	}
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataLenientDrop, mq.MetadataAdaptive, mq.MetadataPortableEnvelope,
	} {
		for name, m := range map[string]*aq.Mapper{
			"a bound of 16": keyBoundMapperFor(t, mode, 16),
			"no bound":      mapperFor(t, mode),
		} {
			if _, err := m.Encode(aq.NewBuf(), message(), nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Errorf("%v, %s: a key that is not UTF-8 = %v (code %v), want CodeInvalidArgument before any I/O: "+
					"the message is the caller's, and no section can carry the key", mode, name, err, mq.CodeOf(err))
			}
		}
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

// TestMappingRestatesAHoldInTheFormTheRuleTakes holds both spellings of a
// scheduled publication against both kinds of delay rule, through a sink whose
// backend reads a stopped clock.
//
// A rule reads either a delay or an instant, and the caller may name either.
// The mapping used to write the field the rule reads and ignore the other: a
// DeliverAt on a relative rule went out as a delay of zero and was delivered at
// once, and a Delay on an absolute rule went out as the zero time, year one.
// Both publications succeeded. The stopped clock is years away from the wall's,
// so a restatement read off the wall clock instead of [aq.Config.Clock] fails
// here too.
func TestMappingRestatesAHoldInTheFormTheRuleTakes(t *testing.T) {
	now := time.Date(2031, 3, 4, 5, 6, 7, 0, time.UTC)
	relative := testProfile()
	absolute := testProfile()
	absolute.Delay = aq.DelayRule{Slot: aq.SlotMessageAnnotation, Key: "x-opt-delivery-time", Absolute: true}
	for _, tc := range []struct {
		name string
		p    aq.Profile
		key  string
		opts mq.PublishOptions
		// want is the value under key; nil means no hold is written at all.
		want any
	}{
		{
			"a DeliverAt on a relative rule is the delay left until it", relative, "x-opt-delivery-delay",
			mq.PublishOptions{DeliverAt: now.Add(90 * time.Second)},
			int64(90_000),
		},
		{
			"a DeliverAt already past on a relative rule is due now", relative, "x-opt-delivery-delay",
			mq.PublishOptions{DeliverAt: now.Add(-time.Minute)},
			nil,
		},
		{
			"a Delay on a relative rule is the delay", relative, "x-opt-delivery-delay",
			mq.PublishOptions{Delay: 90 * time.Second},
			int64(90_000),
		},
		{
			"a Delay on an absolute rule is the instant it ends", absolute, "x-opt-delivery-time",
			mq.PublishOptions{Delay: 90 * time.Second},
			now.Add(90 * time.Second).UnixMilli(),
		},
		{
			"a DeliverAt on an absolute rule is the caller's instant", absolute, "x-opt-delivery-time",
			mq.PublishOptions{DeliverAt: now.Add(time.Hour)},
			now.Add(time.Hour).UnixMilli(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := open(t, tc.p, aq.Config{Clock: stoppedClock{now: now}})
			snk := f.sink(mq.SinkConfig{Topic: "topic"})
			if err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("x")}, &tc.opts); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			sent := f.sender(budget).Sent()
			if len(sent) != 1 {
				t.Fatalf("the peer received %d transfer(s), want 1", len(sent))
			}
			got, ok := sent[0].Annotations[tc.key]
			if tc.want == nil {
				if ok {
					t.Fatalf("a hold that is already over went out as %s=%v (%T); an explicit zero is a "+
						"scheduled publication on some brokers, so none is written", tc.key, got, got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("the hold went out as %s=%v (%T), want %v (%T)", tc.key, got, got, tc.want, tc.want)
			}
		})
	}
}

// TestMappingRestatesAHoldAgainstTheWallClockOutsideASink is the same
// restatement through [aq.NewMapper], which has no backend and so no injected
// clock to read.
func TestMappingRestatesAHoldAgainstTheWallClockOutsideASink(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	before := time.Now()
	out, err := m.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{DeliverAt: before.Add(time.Hour)})
	elapsed := time.Since(before)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	ms, ok := out.Annotations["x-opt-delivery-delay"].(int64)
	if !ok || ms > time.Hour.Milliseconds() || ms < (time.Hour-elapsed).Milliseconds() {
		t.Fatalf("a DeliverAt an hour ahead went out as x-opt-delivery-delay=%v, want the %v left until it",
			out.Annotations["x-opt-delivery-delay"], time.Hour-elapsed)
	}
}

// stoppedClock reads one instant for ever and hands out real timers, so that a
// test can say exactly what a hold restated against it is while the backend's
// own waits still end.
type stoppedClock struct{ now time.Time }

func (c stoppedClock) Now() time.Time { return c.now }

func (stoppedClock) NewTimer(d time.Duration) mq.Timer { return stoppedClockTimer{t: time.NewTimer(d)} }

// stoppedClockTimer adapts *time.Timer to mq.Timer.
type stoppedClockTimer struct{ t *time.Timer }

func (s stoppedClockTimer) C() <-chan time.Time        { return s.t.C }
func (s stoppedClockTimer) Stop() bool                 { return s.t.Stop() }
func (s stoppedClockTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }

// TestMappingRefusesADelayBeyondTheProfilesCeiling holds the profile's own Max,
// which the specification makes an advertised limit and makes a refusal before I/O.
func TestMappingRefusesADelayBeyondTheProfilesCeiling(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	_, err := m.Encode(aq.NewBuf(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{Delay: 2 * time.Hour})
	assertCode(t, err, mq.CodeInvalidArgument, "a delay beyond the profile's ceiling")
}

// TestMappingRefusesADeliverAtBeyondTheProfilesCeiling is the same ceiling for
// the same hold stated as an instant, measured on the backend's clock.
//
// Only a Delay used to be checked, so a DeliverAt two hours ahead on a profile
// whose broker accepts one hour went to the broker anyway. The clock is
// stopped years from the wall's, so the instant half an hour ahead of it is
// accepted only if it is measured on [aq.Config.Clock].
func TestMappingRefusesADeliverAtBeyondTheProfilesCeiling(t *testing.T) {
	now := time.Date(2031, 3, 4, 5, 6, 7, 0, time.UTC)
	f := open(t, testProfile(), aq.Config{Clock: stoppedClock{now: now}})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{DeliverAt: now.Add(2 * time.Hour)})
	assertCode(t, err, mq.CodeInvalidArgument, "a delivery time beyond the profile's ceiling")
	if err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("x")},
		&mq.PublishOptions{DeliverAt: now.Add(30 * time.Minute)}); err != nil {
		t.Fatalf("a delivery time inside the profile's ceiling was refused: %v", err)
	}
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
