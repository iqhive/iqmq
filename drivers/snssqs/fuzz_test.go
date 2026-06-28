package snssqs

import (
	"bytes"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzSNSAttributes is the attribute fuzz for this driver: the
// publication mapping, the SNS/SQS attribute translation and the round trip
// raw message delivery closes.
//
// The target is deliberately PURE — no servers, no goroutines, no clock — and
// what it covers is the whole path a message takes between the two SDKs:
//
//	message --Encode--> SQS attributes --snsAttrs--> SNS attributes
//	        --raw delivery--> SQS attributes --Decode--> message
//
// The middle step is the one only this driver has, and the last is the SQS
// driver's own decoder: raw message delivery maps SNS's attributes onto SQS's
// unchanged, so the composition has to be the identity on everything promise. `snsfake.RawAttributes` performs the delivery step, so the
// function the fake delivers with is the function this target checks — if the
// model were wrong, `conformance/suites` against LocalStack would disagree
// with the unit tests rather than both being wrong together.
//
// What it asserts, for every input and in each of the four modes:
//
//   - the encoding either refuses the message with a refusal that carries a
//     portable code, or produces something SNS would accept: at most ten
//     attributes, legal attribute names, a non-empty body inside the character
//     set, no more than the configured size, and NO delay or group — SNS has
//     no per-message timer and a standard topic has no groups;
//   - the two attribute translations lose nothing, field for field;
//   - what comes out decodes back to the message that went in, byte for byte,
//     including the nil-versus-empty distinction;
//   - and the NON-raw shape loses the metadata, which is the refusal's reason
//     stated as a property rather than as a comment.
func FuzzSNSAttributes(f *testing.F) {
	for _, seed := range attributeSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := Config{
			Region:          "fake-region-1",
			AccountID:       "000000000000",
			Body:            sqs.BodyEncoding(r.Choice(3)),
			MaxMessageBytes: int64(r.Int(1024, MaxMessageBytes)),
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("the generated configuration is invalid: %v", err)
		}
		m := genMessage(r)
		for _, mode := range [...]mq.MetadataMode{
			mq.MetadataNativeStrict,
			mq.MetadataPortableEnvelope,
			mq.MetadataAdaptive,
			mq.MetadataLenientDrop,
		} {
			enc := mustEncoder(t, cfg, mode)
			pub, err := encodeForTest(enc, m)
			if err != nil {
				// The specification: every refusal carries a code a caller can switch
				// on. An error that reached here with CodeUnknown is a
				// refusal a program cannot act on.
				if code := mq.CodeOf(err); code == mq.CodeUnknown {
					t.Fatalf("mode %v: the mapping refused a message with an error carrying no portable code: %v", mode, err)
				}
				continue
			}
			assertSNSWouldAccept(t, cfg, mode, pub)

			// The two translations, and the assertion that neither loses a
			// field. It is the reason they are loops over a struct's fields
			// rather than an unsafe cast between two identical types.
			snsMap := snsAttrs(pub.Attrs)
			assertSameAttributes(t, mode, triplesOfSQS(pub.Attrs), triplesOfSNS(snsMap))
			delivered := snsfake.RawAttributes(snsMap)
			assertSameAttributes(t, mode, triplesOfSQS(delivered), triplesOfSNS(snsMap))
			// And the driver's own inverse of the same translation, which
			// is what models raw delivery inside the fuzz target's reach.
			assertSameAttributes(t, mode, triplesOfSQS(sqsAttrs(snsMap)), triplesOfSNS(snsMap))

			var got mq.Message
			if err := enc.Decode(rawMessage(pub.Body, delivered), "t", &got); err != nil {
				t.Fatalf("mode %v: decoding what raw delivery produces from what encode produced: %v", mode, err)
			}
			if mode == mq.MetadataLenientDrop {
				// The lenient mode is the one that is allowed to lose
				// headers, so the round trip is only checked for what it
				// kept.
				assertKeptSubset(t, m, &got)
			} else {
				assertRoundTrip(t, mode, m, &got)
			}

			// And the shape a subscription WITHOUT raw delivery produces.
			// Every iqmq message attribute is inside the body, so a
			// consumer that read it would see no key, no identifier and no
			// headers — which is why the driver refuses such a subscription
			// rather than trying to unwrap one.
			assertNonRawLosesTheMetadata(t, mode, enc, pub, snsMap, m)
		}
	})
}

// encodeForTest is the sink's own encode step without a sink: the option
// refusals SNS forces, then the shared mapping.
func encodeForTest(enc *sqs.Encoder, m *mq.Message) (sqs.Publication, error) {
	if err := checkPublishOptions(&mq.PublishOptions{}); err != nil {
		return sqs.Publication{}, err
	}
	return enc.Encode(m, &mq.PublishOptions{})
}

// mustEncoder builds the shared mapping for one mode or fails.
func mustEncoder(t *testing.T, cfg Config, mode mq.MetadataMode) *sqs.Encoder {
	t.Helper()
	caps, err := capabilities(cfg.resolve())
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	enc, err := sqs.NewEncoder(cfg.sqsConfig(), caps, mode)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	return enc
}

// assertSNSWouldAccept holds the encoded publication to SNS's own limits,
// which is the specification's "limits before I/O" seen from the other side.
//
// The name and body grammars are restated here rather than shared with the
// code that produced them, and that is the point: a checker that called the
// same function as the encoder would agree with it by construction, and the
// two differences from the queue service — 256 KiB, and no timer — are exactly
// what a shared checker would have missed.
func assertSNSWouldAccept(t *testing.T, cfg Config, mode mq.MetadataMode, pub sqs.Publication) {
	t.Helper()
	switch {
	case len(pub.Attrs) > MaxAttributes:
		t.Fatalf("mode %v: %d attributes, over the service's %d", mode, len(pub.Attrs), MaxAttributes)
	case pub.Body == "":
		t.Fatalf("mode %v: an empty Message, which SNS's Publish refuses outright", mode)
	case !xmlSafeText(pub.Body):
		t.Fatalf("mode %v: a body outside the services' character set: %q", mode, pub.Body)
	case pub.Bytes > int(cfg.resolve().MaxMessageBytes):
		t.Fatalf("mode %v: %d bytes, over the configured %d", mode, pub.Bytes, cfg.resolve().MaxMessageBytes)
	case pub.Delay != 0:
		t.Fatalf("mode %v: a delay of %d second(s) reached a service with no per-message timer", mode, pub.Delay)
	case pub.GroupID != "" || pub.DedupID != "":
		t.Fatalf("mode %v: a standard topic got a group %q / deduplication identifier %q", mode, pub.GroupID, pub.DedupID)
	}
	for name := range pub.Attrs {
		if !snsAttributeNameOK(name) {
			t.Fatalf("mode %v: attribute name %q, which the service refuses", mode, name)
		}
	}
}

// snsAttributeNameOK is the service's message-attribute name grammar,
// restated: alphanumerics, hyphens, underscores and periods; no leading or
// trailing period; no two periods in succession; at most 256 characters; and
// not beginning with "aws." or "amazon." in any case.
func snsAttributeNameOK(k string) bool {
	if k == "" || len(k) > 256 {
		return false
	}
	if k[0] == '.' || k[len(k)-1] == '.' {
		return false
	}
	lower := make([]byte, 0, 8)
	for i := 0; i < len(k) && i < 8; i++ {
		c := k[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		lower = append(lower, c)
	}
	for _, reserved := range []string{"aws.", "amazon."} {
		if len(lower) >= len(reserved) && string(lower[:len(reserved)]) == reserved {
			return false
		}
	}
	for i := range len(k) {
		c := k[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_':
		case c == '.':
			if i+1 < len(k) && k[i+1] == '.' {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// xmlSafeText is the character set both services' text fields accept, restated
// from the pinned SDKs' documentation: #x9 | #xA | #xD | #x20-#xD7FF |
// #xE000-#xFFFD | #x10000-#x10FFFF.
func xmlSafeText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		switch {
		case r == 0x9 || r == 0xA || r == 0xD:
		case r >= 0x20 && r <= 0xD7FF:
		case r >= 0xE000 && r <= 0xFFFD:
		case r >= 0x10000 && r <= 0x10FFFF:
		default:
			return false
		}
	}
	return true
}

// assertSameAttributes holds one of the two translations to being lossless.
//
// Both SDKs generate the same three fields under two names, so the comparison
// is over a neutral triple rather than over either struct: a comparison
// written against one of them would stop compiling — rather than start failing
// — if the other grew a field, and stopping compiling is the outcome this
// whole loop exists to get instead of an unsafe cast.
func assertSameAttributes(t *testing.T, mode mq.MetadataMode, a, b map[string]attrTriple) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("mode %v: %d attribute(s) became %d", mode, len(a), len(b))
	}
	for name, av := range a {
		bv, ok := b[name]
		if !ok {
			t.Fatalf("mode %v: the attribute %q was lost in translation", mode, name)
		}
		if av.dataType != bv.dataType {
			t.Fatalf("mode %v: attribute %q changed type from %q to %q", mode, name, av.dataType, bv.dataType)
		}
		if av.stringValue != bv.stringValue {
			t.Fatalf("mode %v: attribute %q's string value changed from %q to %q", mode, name, av.stringValue, bv.stringValue)
		}
		if !bytes.Equal(av.binary, bv.binary) {
			t.Fatalf("mode %v: attribute %q's binary value changed", mode, name)
		}
	}
}

// attrTriple is the shape the two SDKs' attribute structs share.
type attrTriple struct {
	dataType    string
	stringValue string
	binary      []byte
}

// triplesOfSQS and triplesOfSNS project the two generated maps onto it.
func triplesOfSQS(in map[string]sqstypes.MessageAttributeValue) map[string]attrTriple {
	out := make(map[string]attrTriple, len(in))
	for k, v := range in {
		out[k] = attrTriple{aws.ToString(v.DataType), aws.ToString(v.StringValue), v.BinaryValue}
	}
	return out
}

func triplesOfSNS(in map[string]snstypes.MessageAttributeValue) map[string]attrTriple {
	out := make(map[string]attrTriple, len(in))
	for k, v := range in {
		out[k] = attrTriple{aws.ToString(v.DataType), aws.ToString(v.StringValue), v.BinaryValue}
	}
	return out
}

// assertNonRawLosesTheMetadata states the refusal's reason as a property.
//
// SNS without raw delivery wraps the body in a JSON notification and puts
// every message attribute INSIDE it, so the SQS message a consumer receives
// has no attributes at all: no key, no identifier, no properties, no headers,
// and a body that is the notification rather than the payload. A driver that
// accepted such a subscription would hand every application that, and the specification
// makes silently changing a payload a disqualifier.
func assertNonRawLosesTheMetadata(t *testing.T, mode mq.MetadataMode, enc *sqs.Encoder, pub sqs.Publication, snsMap map[string]snstypes.MessageAttributeValue, want *mq.Message) {
	t.Helper()
	body := snsfake.Notification("arn:aws:sns:r:1:t", "sns-1", pub.Body, snsMap)
	var got mq.Message
	if err := enc.Decode(rawMessage(body, nil), "t", &got); err != nil {
		// A notification the decoder refuses is a safe outcome: the
		// application sees a failure rather than a wrong payload. What is
		// not acceptable is a SUCCESSFUL decode that agrees with the
		// publication, which the checks below rule out.
		return
	}
	if string(got.Body) == pub.Body && pub.Body != body {
		t.Fatalf("mode %v: the non-raw notification decoded to the published body, which would make the raw-delivery refusal pointless", mode)
	}
	if len(want.Key) > 0 && bytes.Equal(got.Key, want.Key) {
		t.Fatalf("mode %v: the non-raw notification carried the key through; it cannot, because the attributes are inside the body", mode)
	}
	if want.Headers.Len() > 0 && got.Headers.Len() == want.Headers.Len() && mode != mq.MetadataPortableEnvelope {
		t.Fatalf("mode %v: the non-raw notification carried %d header(s) through", mode, got.Headers.Len())
	}
}

// rawMessage is the SQS message a raw-delivery subscription produces from a
// publication.
func rawMessage(body string, attrs map[string]sqstypes.MessageAttributeValue) *sqstypes.Message {
	return &sqstypes.Message{
		MessageId:         aws.String("m-1"),
		ReceiptHandle:     aws.String("r-1"),
		Body:              aws.String(body),
		MessageAttributes: attrs,
		Attributes:        map[string]string{},
	}
}

// assertRoundTrip is the round trip promise.
func assertRoundTrip(t *testing.T, mode mq.MetadataMode, want, got *mq.Message) {
	t.Helper()
	if (want.Body == nil) != (got.Body == nil) {
		t.Fatalf("mode %v: a %s body came back %s; the specification distinguishes nil from empty",
			mode, nilOrNot(want.Body), nilOrNot(got.Body))
	}
	if !bytes.Equal(want.Body, got.Body) {
		t.Fatalf("mode %v: body %q came back %q", mode, want.Body, got.Body)
	}
	if !bytes.Equal(want.Key, got.Key) && (len(want.Key) != 0 || len(got.Key) != 0) {
		t.Fatalf("mode %v: key %q came back %q", mode, want.Key, got.Key)
	}
	if want.ID != got.ID {
		t.Fatalf("mode %v: identifier %q came back %q", mode, want.ID, got.ID)
	}
	if want.Properties != got.Properties {
		t.Fatalf("mode %v: properties %+v came back %+v", mode, want.Properties, got.Properties)
	}
	if want.Headers.Len() != got.Headers.Len() {
		t.Fatalf("mode %v: %d header(s) came back as %d", mode, want.Headers.Len(), got.Headers.Len())
	}
	seen := map[string]bool{}
	for k := range want.Headers.All() {
		if seen[k] {
			continue
		}
		seen[k] = true
		a, b := want.Headers.Values(k), got.Headers.Values(k)
		if len(a) != len(b) {
			t.Fatalf("mode %v: header %q had %d value(s) and came back with %d", mode, k, len(a), len(b))
		}
		for i := range a {
			if !bytes.Equal(a[i], b[i]) {
				t.Fatalf("mode %v: header %q value %d came back %q, want %q", mode, k, i, b[i], a[i])
			}
		}
	}
}

// assertKeptSubset is the lenient mode's weaker promise: everything that came
// back was published, and nothing but the headers was lost.
func assertKeptSubset(t *testing.T, want, got *mq.Message) {
	t.Helper()
	if !bytes.Equal(want.Body, got.Body) {
		t.Fatalf("the lenient mode changed the body: %q came back %q", want.Body, got.Body)
	}
	if want.ID != got.ID {
		t.Fatalf("the lenient mode dropped the identifier %q", want.ID)
	}
	if want.Properties != got.Properties {
		t.Fatalf("the lenient mode dropped a property: %+v came back %+v", want.Properties, got.Properties)
	}
	for k, v := range got.Headers.All() {
		found := false
		for _, cand := range want.Headers.Values(k) {
			if bytes.Equal(cand, v) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("the lenient mode invented a header %q=%q", k, v)
		}
	}
}

// nilOrNot names a body for a failure message.
func nilOrNot(b []byte) string {
	if b == nil {
		return "nil"
	}
	return "non-nil"
}

// genMessage builds a message from the fuzz input, steering it towards the
// shapes this driver's mapping has to decide about: a header count around the
// ten-attribute budget, keys that are and are not legal attribute names,
// repeated keys, binary values, and a reserved name.
func genMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	switch r.Choice(3) {
	case 0:
		m.Body = nil
	case 1:
		m.Body = []byte{}
	default:
		m.Body = r.Bytes(64)
	}
	if r.Bool() {
		m.Key = r.Bytes(24)
	}
	if r.Bool() {
		m.ID = string(r.Bytes(12))
	}
	if r.Bool() {
		m.Properties.ContentType = string(r.Bytes(8))
	}
	if r.Bool() {
		m.Properties.ContentEncoding = string(r.Bytes(8))
	}
	if r.Bool() {
		m.Properties.CorrelationID = string(r.Bytes(8))
	}
	if r.Bool() {
		m.Properties.CausationID = string(r.Bytes(8))
	}
	if r.Bool() {
		m.Properties.ReplyTo = string(r.Bytes(8))
	}
	if r.Bool() {
		// Whole seconds so that the RFC 3339 rendering round-trips; a
		// property whose precision the encoding cannot carry is a
		// different bug from the ones this target is about.
		m.Properties.CreatedAt = time.Unix(int64(r.Int(0, 1<<31)), 0).UTC()
	}
	for n := r.Int(0, 14); n > 0; n-- {
		m.Headers.Add(genHeaderKey(r), r.Bytes(16))
	}
	return m
}

// genHeaderKey picks a header key out of the pool the mapping has to decide
// about. The reserved names are the SQS driver's own, because that is the
// driver whose mapping this one shares.
func genHeaderKey(r *fuzzutil.Reader) string {
	pool := []string{
		"content-type", "x-trace-id", "a", "A", "a.b", "a_b",
		"a/b", "a b", "a..b", ".a", "a.", "aws.x", "AWS.x", "amazon.y", "",
		"iqmq-k", "iqmq-id", "iqmq-enc", "iqmq-env", "iqmq-ts",
		"repeated", "repeated",
	}
	if r.Choice(6) == 0 {
		return string(r.Bytes(8))
	}
	return pool[r.Choice(len(pool))]
}

// attributeSeeds are the committed inputs: one per body policy, which is the
// decision that changes what reaches the wire.
func attributeSeeds() [][]byte {
	out := make([][]byte, 0, 3)
	for body := range 3 {
		w := fuzzutil.NewWriter()
		w.Choice(3, body)
		w.Int(1024, MaxMessageBytes, MaxMessageBytes)
		w.Choice(3, 2)
		w.Bytes(64, []byte("payload"))
		w.Bool(true)
		w.Bytes(24, []byte("k"))
		for range 7 {
			w.Bool(false)
		}
		w.Int(0, 14, 2)
		out = append(out, w.Data())
	}
	return out
}
