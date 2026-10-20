package streams_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/wire"
)

// sameMessage reports whether two messages are the same message as the field
// layout is required to preserve them, and says what differs when they are
// not.
//
// The nil-versus-empty distinction of Key and Body is part of the comparison,
// because the message contract makes it observable and Message.Clone preserves
// it: a layout that turned an empty body into a nil one would change what a
// handler sees.
//
// CreatedAt is compared as an instant, not as a time.Time. An entry carries
// no location, so a time set in a named zone comes back as the same instant
// in UTC; that is the one thing the round trip does not preserve, and it is
// documented on [streams.PropCreatedAt] rather than hidden by a loose
// comparison of everything else.
func sameMessage(want, got *mq.Message) string {
	if (want.Body == nil) != (got.Body == nil) {
		return "body nil-ness: want nil=" + strconv.FormatBool(want.Body == nil) +
			", got nil=" + strconv.FormatBool(got.Body == nil)
	}
	if !bytes.Equal(want.Body, got.Body) {
		return "body: " + strconv.Itoa(len(want.Body)) + " bytes vs " + strconv.Itoa(len(got.Body))
	}
	if (want.Key == nil) != (got.Key == nil) {
		return "key nil-ness: want nil=" + strconv.FormatBool(want.Key == nil) +
			", got nil=" + strconv.FormatBool(got.Key == nil)
	}
	if !bytes.Equal(want.Key, got.Key) {
		return "key: " + strconv.Itoa(len(want.Key)) + " bytes vs " + strconv.Itoa(len(got.Key))
	}
	if want.Topic != got.Topic {
		return "topic: " + strconv.Quote(want.Topic) + " vs " + strconv.Quote(got.Topic)
	}
	if want.ID != got.ID {
		return "id: " + strconv.Quote(want.ID) + " vs " + strconv.Quote(got.ID)
	}
	if want.Headers.Len() != got.Headers.Len() {
		return "header count: " + strconv.Itoa(want.Headers.Len()) + " vs " + strconv.Itoa(got.Headers.Len())
	}
	wantKeys, wantVals := headerPairs(want)
	gotKeys, gotVals := headerPairs(got)
	for i := range wantKeys {
		if wantKeys[i] != gotKeys[i] {
			return "header " + strconv.Itoa(i) + " key: " + strconv.Quote(wantKeys[i]) +
				" vs " + strconv.Quote(gotKeys[i])
		}
		if !bytes.Equal(wantVals[i], gotVals[i]) {
			return "header " + strconv.Itoa(i) + " value: " +
				strconv.Itoa(len(wantVals[i])) + " bytes vs " + strconv.Itoa(len(gotVals[i]))
		}
	}
	w, g := want.Properties, got.Properties
	switch {
	case w.ContentType != g.ContentType:
		return "content type"
	case w.ContentEncoding != g.ContentEncoding:
		return "content encoding"
	case w.CorrelationID != g.CorrelationID:
		return "correlation id"
	case w.CausationID != g.CausationID:
		return "causation id"
	case w.ReplyTo != g.ReplyTo:
		return "reply to"
	case !w.CreatedAt.Equal(g.CreatedAt):
		return "created at: " + w.CreatedAt.String() + " vs " + g.CreatedAt.String()
	}
	return ""
}

func headerPairs(m *mq.Message) ([]string, [][]byte) {
	keys := make([]string, 0, m.Headers.Len())
	vals := make([][]byte, 0, m.Headers.Len())
	for k, v := range m.Headers.All() {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}

// roundTrip encodes m, decodes what it produced and returns the result.
func roundTrip(t testing.TB, m *mq.Message) *mq.Message {
	t.Helper()
	fields := streams.Encode(nil, m)
	if len(fields)%2 != 0 {
		t.Fatalf("Encode produced %d fields, which is not name/value pairs", len(fields))
	}
	var out mq.Message
	if err := streams.Decode(&out, fields); err != nil {
		t.Fatalf("Decode of a message this layout encoded: %v (fields=%d)", err, len(fields))
	}
	return &out
}

// TestFieldLayoutRoundTrip is the layout's central promise: a message this
// driver accepted is a message this driver can decode.
//
// The cases are the ones that break a field-value encoding: a header key
// containing the framing's own separator, a binary value, a duplicate header
// key, an empty value, an empty header key, the nil-versus-empty distinction
// of key and body, and a property at the limit.
func TestFieldLayoutRoundTrip(t *testing.T) {
	withHeaders := func(pairs ...string) mq.Headers {
		var h mq.Headers
		for i := 0; i+1 < len(pairs); i += 2 {
			h.Add(pairs[i], []byte(pairs[i+1]))
		}
		return h
	}
	cases := []struct {
		name string
		msg  mq.Message
	}{
		{"empty", mq.Message{}},
		{"body-only", mq.Message{Body: []byte("hello")}},
		{"empty-body-is-not-nil", mq.Message{Body: []byte{}}},
		{"empty-key-is-not-nil", mq.Message{Key: []byte{}, Body: []byte("x")}},
		{"binary-key", mq.Message{Key: []byte{0, 1, 0xff, '\n'}, Body: []byte("x")}},
		{"topic", mq.Message{Topic: "acme.orders", Body: []byte("x")}},
		{"header-key-with-colon", mq.Message{
			Headers: withHeaders("a:b:c", "v"), Body: []byte("x"),
		}},
		{"header-value-with-nul", mq.Message{
			Headers: func() mq.Headers {
				var h mq.Headers
				h.Add("k", []byte{0, ':', 0, 'x'})
				return h
			}(), Body: []byte("x"),
		}},
		{"duplicate-header-keys-in-order", mq.Message{
			Headers: withHeaders("dup", "first", "other", "o", "dup", "second"),
			Body:    []byte("x"),
		}},
		{"empty-header-key-and-value", mq.Message{
			Headers: withHeaders("", ""), Body: []byte("x"),
		}},
		{"eleven-headers", mq.Message{
			Headers: func() mq.Headers {
				var h mq.Headers
				for i := range 11 {
					h.Add("h"+strconv.Itoa(i), []byte{byte(i)})
				}
				return h
			}(), Body: []byte("x"),
		}},
		{"identifier", mq.Message{ID: "01JABCDEF", Body: []byte("x")}},
		{"properties", mq.Message{
			Body: []byte("x"),
			Properties: mq.Properties{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationID:   "corr",
				CausationID:     "cause",
				ReplyTo:         "acme.replies",
				CreatedAt:       time.Unix(1767225600, 123456789).UTC(),
			},
		}},
		{"created-at-before-the-epoch", mq.Message{
			Body:       []byte("x"),
			Properties: mq.Properties{CreatedAt: time.Unix(-6795364578, 871345152).UTC()},
		}},
		{"created-at-beyond-unixnano", mq.Message{
			// 2300, which a single unix-nanosecond count cannot hold. It is
			// the reason PropCreatedAt is seconds and nanoseconds.
			Body:       []byte("x"),
			Properties: mq.Properties{CreatedAt: time.Date(2300, 6, 1, 12, 0, 0, 1, time.UTC)},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.msg
			got := roundTrip(t, &msg)
			if diff := sameMessage(&msg, got); diff != "" {
				t.Fatalf("the field layout did not round-trip the message: %s", diff)
			}
		})
	}
}

// TestFieldLayoutLimitsMatchTheEnvelope is the finding held in place for
// this driver.
//
// driverkit.Envelope encodes anything and decodes under wire.Limits, so a
// message accepted above those bounds is a record that is published
// successfully and can then never be decoded — and therefore never settled.
// The bound this package checks has to be the SAME bound, field for field, or
// the hole is open again at whichever field drifted.
func TestFieldLayoutLimitsMatchTheEnvelope(t *testing.T) {
	lim := streams.DefaultLimits
	for _, tc := range []struct {
		name      string
		got, want int
	}{
		{"MaxKey", lim.MaxKey, wire.DefaultLimits().MaxKey},
		{"MaxBody", lim.MaxBody, wire.DefaultLimits().MaxBody},
		{"MaxID", lim.MaxID, wire.DefaultLimits().MaxID},
		{"MaxHeaders", lim.MaxHeaders, wire.DefaultLimits().MaxHeaders},
		{"MaxHeaderKey", lim.MaxHeaderKey, wire.DefaultLimits().MaxHeaderKey},
		{"MaxHeaderValue", lim.MaxHeaderValue, wire.DefaultLimits().MaxHeaderValue},
		{"MaxProp", lim.MaxProp, wire.DefaultLimits().MaxProp},
	} {
		if tc.got != tc.want {
			t.Errorf("streams.DefaultLimits.%s = %d, wire.DefaultLimits.%s = %d: "+
				"a message admitted above the envelope's decode limit is an unsettleable record ",
				tc.name, tc.got, tc.name, tc.want)
		}
	}
}

// TestCheckRefusesWhatTheEnvelopeCannotDecode drives each limit one byte over
// and asserts the refusal, and then asserts that the envelope really would
// have failed on it — which is what makes the limit a fact about the driver
// rather than a number in a struct.
func TestCheckRefusesWhatTheEnvelopeCannotDecode(t *testing.T) {
	lim := streams.DefaultLimits
	over := []struct {
		name string
		msg  func() *mq.Message
	}{
		{"header-key", func() *mq.Message {
			m := &mq.Message{Body: []byte("x")}
			m.Headers.Add(strings.Repeat("k", lim.MaxHeaderKey+1), []byte("v"))
			return m
		}},
		{"header-value", func() *mq.Message {
			m := &mq.Message{Body: []byte("x")}
			m.Headers.Add("k", bytes.Repeat([]byte("v"), lim.MaxHeaderValue+1))
			return m
		}},
		{"identifier", func() *mq.Message {
			return &mq.Message{Body: []byte("x"), ID: strings.Repeat("i", lim.MaxID+1)}
		}},
		{"key", func() *mq.Message {
			return &mq.Message{Body: []byte("x"), Key: bytes.Repeat([]byte("k"), lim.MaxKey+1)}
		}},
		{"property", func() *mq.Message {
			return &mq.Message{
				Body:       []byte("x"),
				Properties: mq.Properties{ContentType: strings.Repeat("c", lim.MaxProp+1)},
			}
		}},
		{"header-count", func() *mq.Message {
			m := &mq.Message{Body: []byte("x")}
			for i := range lim.MaxHeaders + 1 {
				m.Headers.Add("h"+strconv.Itoa(i), []byte("v"))
			}
			return m
		}},
	}
	layout := mustLayout(t, wire.Auto)
	for _, tc := range over {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.msg()
			if err := lim.Check(m); err == nil {
				t.Fatalf("Check accepted a message one byte over %s; it would be published and then undecodable", tc.name)
			}
			// The other half: the envelope really does refuse it. A limit
			// set that refused messages the envelope would have carried
			// would be a limit nobody needed, and a check that passed
			// because the envelope was lenient would be no check at all.
			body, err := layout.Encode(nil, m)
			if err != nil {
				return // refused at encode, which is also a refusal
			}
			var out mq.Message
			src := mq.Message{Body: body}
			if derr := layout.Decode(src.Body, &out, nil); derr == nil {
				t.Fatalf("the portable envelope round-tripped a message one byte over %s, "+
					"so this limit refuses something nothing needed refusing", tc.name)
			}
		})
	}
}

// TestDecodeRefusesACorruptEntry pins the strict half of the decoder. Each
// case is a field list a correct encoder never writes, and each one would
// silently change the message if it were accepted.
func TestDecodeRefusesACorruptEntry(t *testing.T) {
	cases := []struct {
		name   string
		fields []string
	}{
		{"odd-length", []string{"b"}},
		{"duplicate-body", []string{"b", "one", "b", "two"}},
		{"duplicate-key", []string{"k", "one", "k", "two"}},
		{"duplicate-topic", []string{"t", "one", "t", "two"}},
		{"duplicate-property", []string{"p:0", "one", "p:0", "two"}},
		{"header-gap", []string{"h:0", "1:kv", "h:2", "1:kv"}},
		{"header-repeat", []string{"h:0", "1:kv", "h:0", "1:kv"}},
		{"header-index-not-decimal", []string{"h:x", "1:kv"}},
		{"header-index-leading-zero", []string{"h:01", "1:kv"}},
		{"header-index-empty", []string{"h:", "1:kv"}},
		{"header-no-length", []string{"h:0", "kv"}},
		{"header-empty-packed", []string{"h:0", ""}},
		{"header-length-overruns", []string{"h:0", "9:k"}},
		{"header-negative-length", []string{"h:0", "-1:k"}},
		{"created-at-no-comma", []string{"p:6", "123"}},
		{"created-at-bad-nanos", []string{"p:6", "1,1000000000"}},
		{"created-at-negative-nanos", []string{"p:6", "1,-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m mq.Message
			if err := streams.Decode(&m, tc.fields); err == nil {
				t.Fatalf("Decode accepted a corrupt entry (%v); accepting it delivers a message "+
					"that is not the one that was published", tc.fields)
			}
		})
	}
}

// TestDecodeIgnoresBookkeepingAndUnknownFields is the lenient half: the
// driver's own "x:" fields and a field a future version added are not message
// content and must not become one.
func TestDecodeIgnoresBookkeepingAndUnknownFields(t *testing.T) {
	fields := []string{
		"b", "body",
		"x:a", "7",
		"x:d", "100,0",
		"zzz-from-the-future", "whatever",
		"p:99", "a property this version does not know",
	}
	var m mq.Message
	if err := streams.Decode(&m, fields); err != nil {
		t.Fatalf("Decode of an entry carrying bookkeeping fields: %v", err)
	}
	if string(m.Body) != "body" || m.Headers.Len() != 0 || m.ID != "" {
		t.Fatalf("bookkeeping fields leaked into the message: %+v", m)
	}
	if got := streams.Attempt(fields); got != 7 {
		t.Fatalf("Attempt = %d, want 7", got)
	}
	if got := streams.DeliverAt(fields); got.Unix() != 100 {
		t.Fatalf("DeliverAt = %v, want unix 100", got)
	}
}

func mustLayout(tb testing.TB, k wire.Kind) wire.Layout {
	tb.Helper()
	l, err := wire.New(k)
	if err != nil {
		tb.Fatalf("wire.New(%v): %v", k, err)
	}
	return l
}
