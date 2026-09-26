package sqs

import (
	"bytes"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/wire"
)

// FuzzSQSAttributes is the attribute fuzz for this driver: round trip,
// limits, deterministic duplicate collapse and reserved-field collisions
// rejected.
//
// A message is generated from the input and put through the encoder in every
// one of the four modes. What the target asserts is the same in all four,
// and is the contract the whole mapping exists for:
//
//   - the encoding either refuses the message with one of this driver's OWN
//     refusals, or produces something the service would accept: at most ten
//     attributes, a body in the service's character set, a non-empty body,
//     and no more than the configured message size;
//   - whatever it produces decodes back to the message that went in, byte for
//     byte, including the nil-versus-empty distinction;
//   - a refusal in the strict mode is an ACCEPTANCE in the adaptive one,
//     except for the reserved-name collision, which every mode refuses.
func FuzzSQSAttributes(f *testing.F) {
	for _, seed := range attributeSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := Config{
			Body:            BodyEncoding(r.Choice(int(bodyEncodingCount))),
			FIFO:            r.Bool(),
			MaxMessageBytes: int64(r.Int(1024, MaxMessageBytes)),
		}
		m := genMessage(r)
		strictRefused := error(nil)
		for _, mode := range [...]mq.MetadataMode{
			mq.MetadataNativeStrict,
			mq.MetadataPortableEnvelope,
			mq.MetadataAdaptive,
			mq.MetadataLenientDrop,
		} {
			enc := mustEncoder(t, cfg, mode)
			out, err := enc.encode(m, &mq.PublishOptions{})
			if err != nil {
				assertKnownRefusal(t, mode, err)
				if mode == mq.MetadataNativeStrict {
					strictRefused = err
				}
				continue
			}
			assertServiceWouldAccept(t, cfg, out)
			var got mq.Message
			if derr := enc.decode(wireOf(out), "t", &got); derr != nil {
				t.Fatalf("mode %v: decode of what encode produced: %v", mode, derr)
			}
			if mode == mq.MetadataLenientDrop {
				// The lenient mode is the one that is allowed to lose
				// headers, so the round trip is only checked for what it
				// kept.
				assertKeptSubset(t, m, &got)
				continue
			}
			assertRoundTrip(t, mode, m, &got)
		}
		if strictRefused != nil && errors.Is(strictRefused, errReservedHeader) {
			// Every mode refuses a collision with a reserved attribute
			// name: enveloping it would work and would come back as a
			// header beside the field it collided with.
			for _, mode := range [...]mq.MetadataMode{mq.MetadataAdaptive, mq.MetadataPortableEnvelope} {
				if _, err := mustEncoder(t, cfg, mode).encode(m, &mq.PublishOptions{}); !errors.Is(err, errReservedHeader) {
					t.Fatalf("mode %v accepted a header colliding with a reserved attribute name: %v", mode, err)
				}
			}
		}
	})
}

// mustEncoder builds an encoder or fails.
func mustEncoder(t *testing.T, cfg Config, mode mq.MetadataMode) *encoder {
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

// assertKnownRefusal holds the encoder to the rule that a refusal says
// what was refused: an error nothing here names is one a caller cannot act on.
func assertKnownRefusal(t *testing.T, mode mq.MetadataMode, err error) {
	t.Helper()
	for _, known := range []error{
		errReservedHeader, errDuplicateHeader, errHeaderName, errTooManyAttributes,
		errBodyNotText, mq.ErrMessageTooLarge, errNoTTL, errNoPriority,
		errNoPartition, errNoDedup, errFIFODelay, errDelayTooLong,
		// The kit's own refusal: an envelope its Unwrap could not read
		// back. FuzzSQSAttributes found that the encoder accepted one
		// , and Envelope.Wrap now refuses it.
		wire.ErrUTF8,
	} {
		if errors.Is(err, known) {
			return
		}
	}
	t.Fatalf("mode %v: the encoder refused a message with an error it does not name: %v", mode, err)
}

// assertServiceWouldAccept holds the encoded message to the service's own
// limits, which is the specification's "limits before I/O" seen from the other side.
func assertServiceWouldAccept(t *testing.T, cfg Config, out encoded) {
	t.Helper()
	switch {
	case len(out.Attrs) > MaxAttributes:
		t.Fatalf("%d attributes, over the service's %d", len(out.Attrs), MaxAttributes)
	case out.Body == "":
		t.Fatal("an empty MessageBody, which the service refuses outright")
	case !sqsSafeText([]byte(out.Body)):
		t.Fatalf("a body outside the service's character set: %q", out.Body)
	case out.Bytes > int(cfg.resolve().MaxMessageBytes):
		t.Fatalf("%d bytes, over the configured %d", out.Bytes, cfg.resolve().MaxMessageBytes)
	}
	for name := range out.Attrs {
		if !attributeNameOK(name) {
			t.Fatalf("attribute name %q, which the service refuses", name)
		}
	}
	if cfg.FIFO && out.GroupID == "" {
		t.Fatal("a FIFO publication with no MessageGroupId, which the service refuses")
	}
	if len(out.GroupID) > maxGroupIDLen {
		t.Fatalf("a MessageGroupId of %d characters, over the service's %d", len(out.GroupID), maxGroupIDLen)
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

// wireOf turns an encoded publication into the message a receive produces.
func wireOf(out encoded) *wireMessage { return newWireMessage(out) }

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
// about.
func genHeaderKey(r *fuzzutil.Reader) string {
	pool := []string{
		"content-type", "x-trace-id", "a", "A", "a.b", "a_b",
		"a/b", "a b", "a..b", ".a", "a.", "aws.x", "AWS.x", "amazon.y", "",
		attrKey, attrID, attrEncoding, attrEnvelope, attrCreated,
		"repeated", "repeated",
	}
	if r.Choice(6) == 0 {
		return string(r.Bytes(8))
	}
	return pool[r.Choice(len(pool))]
}

// attributeSeeds are the committed inputs: one per metadata decision this
// driver has to make.
func attributeSeeds() [][]byte {
	out := make([][]byte, 0, 4)
	for _, body := range []BodyEncoding{BodyAuto, BodyText, BodyBase64} {
		w := fuzzutil.NewWriter()
		w.Choice(int(bodyEncodingCount), int(body))
		w.Bool(false)
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
