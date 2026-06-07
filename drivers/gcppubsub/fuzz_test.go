package gcppubsub

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/wire"
)

// FuzzPubSubAttributes is the attribute fuzz for this driver: round trip,
// limits, deterministic duplicate collapse and reserved-field collisions
// rejected.
//
// A message is generated from the input and put through the encoder in every
// one of the four modes. What the target asserts is the same in all four,
// and is the contract the whole mapping exists for:
//
//   - the encoding either refuses the message with one of this driver's OWN
//     refusals, or produces something the SERVICE would accept: at most a
//     hundred attributes, every attribute name a legal one, every attribute
//     value a string inside the service's 1024 bytes, an ordering key inside
//     its 1024 bytes, and no more than ten megabytes in total;
//   - whatever it produces decodes back to the message that went in, byte for
//     byte, including the nil-versus-empty distinction;
//   - a reserved-name collision is refused in EVERY mode, because enveloping
//     it would work and would then come back as a header beside the field it
//     collided with.
func FuzzPubSubAttributes(f *testing.F) {
	for _, seed := range attributeSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := Config{
			ProjectID:   "p",
			Ordering:    r.Bool(),
			ExactlyOnce: r.Bool(),
		}
		m := genMessage(r)
		// Whether the message collides with a reserved attribute name is
		// decided STRUCTURALLY and not from the error, because the specification makes
		// errors.Is match by CODE: every InvalidArgument refusal this driver
		// builds satisfies errors.Is(err, errReservedAttribute), so reading
		// the reason out of the error identified a whole class of refusals as
		// one of them. FuzzPubSubAttributes found that in the check itself.
		reserved := hasReservedHeader(m)
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
				continue
			}
			if reserved {
				t.Fatalf("mode %v accepted a header colliding with a reserved attribute name; the specification rejects a conflict and no metadata mode makes one acceptable", mode)
			}
			assertServiceWouldAccept(t, out)
			var got mq.Message
			if derr := enc.decode(incomingOf(out), "t", &got); derr != nil {
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
	})
}

// FuzzPubSubErrorMapping holds the rule table to its own contract: every
// status code maps to a portable code, the native error stays reachable
// through errors.As, and nothing panics on an error the table has no rule for.
func FuzzPubSubErrorMapping(f *testing.F) {
	for _, seed := range errorSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		mapper := newErrorMapper()
		ops := [...]string{
			mq.OpPublish, mq.OpFetch, mq.OpAck, mq.OpNack,
			mq.OpExtend, mq.OpOpen, mq.OpClose, mq.OpAdmin,
		}
		op := ops[r.Choice(len(ops))]
		native := genError(r)
		mapped := mapper.Map(op, r.String(16), r.String(16), native)
		if native == nil {
			if mapped != nil {
				t.Fatalf("a nil error mapped to %v", mapped)
			}
			return
		}
		if mapped == nil {
			t.Fatal("a non-nil error mapped to nil")
		}
		// The specification: the native error stays reachable, and the portable code is
		// never the zero value by accident — an error that carries no
		// judgement is CodeUnknown and is not retryable, which is the honest
		// answer rather than a silent retry.
		if !errors.Is(mapped, native) {
			t.Fatalf("the mapped error no longer wraps the native one: %v", mapped)
		}
		if mq.CodeOf(mapped) == mq.CodeUnknown && mq.IsRetryable(mapped) {
			t.Fatalf("an unclassified error is retryable: %v", mapped)
		}
		// An ambiguous outcome is only ever reported for a publication: a
		// fetch that failed delivered nothing and a settlement that failed
		// left the message where it was.
		if mq.IsAmbiguous(mapped) && op != mq.OpPublish {
			t.Fatalf("op %s reported an ambiguous outcome: %v", op, mapped)
		}
	})
}

// hasReservedHeader reports whether the message carries a header key in a
// reserved namespace: this driver's "iqmq-" or the service's "goog".
func hasReservedHeader(m *mq.Message) bool {
	for k := range m.Headers.All() {
		if reservedName(k) {
			return true
		}
	}
	return false
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

// genMessage builds a message from the fuzz input, reaching every shape the
// mapping has a branch for.
func genMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	switch r.Choice(4) {
	case 0: // nil body
	case 1:
		m.Body = []byte{}
	default:
		m.Body = r.Bytes(64)
	}
	switch r.Choice(4) {
	case 0: // nil key
	case 1:
		m.Key = []byte{}
	default:
		m.Key = r.Bytes(48)
	}
	if r.Bool() {
		m.ID = r.String(32)
	}
	if r.Bool() {
		m.Properties = mq.Properties{
			ContentType:     r.String(16),
			ContentEncoding: r.String(8),
			CorrelationID:   r.String(16),
			CausationID:     r.String(16),
			ReplyTo:         r.String(16),
		}
		if r.Bool() {
			m.Properties.CreatedAt = time.Unix(int64(r.Uint32()), 0).UTC()
		}
	}
	for n := r.Int(0, 6); n > 0; n-- {
		switch r.Choice(6) {
		case 0:
			m.Headers.Add("iqmq-"+r.String(8), r.Bytes(8))
		case 1:
			m.Headers.Add(ReservedAttributePrefix+r.String(8), r.Bytes(8))
		case 2:
			m.Headers.Add(strings.Repeat("k", r.Int(1, 400)), r.Bytes(8))
		case 3:
			m.Headers.Add("dup", r.Bytes(8))
		case 4:
			m.Headers.Add(r.String(16), bytes.Repeat([]byte{0xff}, r.Int(1, 8)))
		default:
			m.Headers.Add(r.String(16), r.Bytes(16))
		}
	}
	return m
}

// genError builds a native error from the fuzz input.
func genError(r *fuzzutil.Reader) error {
	switch r.Choice(4) {
	case 0:
		return nil
	case 1:
		return statusError(r)
	case 2:
		return errors.New(r.String(24))
	default:
		return sentinelError(r)
	}
}

// assertKnownRefusal holds the encoder to the rule that a refusal says
// what was refused: an error nothing here names is one a caller cannot act on.
func assertKnownRefusal(t *testing.T, mode mq.MetadataMode, err error) {
	t.Helper()
	for _, known := range []error{
		errReservedAttribute, errAttributeKeyInvalid, errAttributeKeyTooLong,
		errAttributeValueTooLong, errAttributeValueBinary, errTooManyAttributes,
		errEnvelopeNeeded, errMessageTooLarge, errOrderingKeyTooLong,
		errNoScheduledPublish, errNoTTL, errNoPriority, errNoPartition, errNoDedup,
		errNilMessage,
		// The kit's own refusals: an envelope its Unwrap could not read
		// back. Envelope.Check is what refuses one and the driver's admission bound asks it.
		wire.ErrUTF8, mq.ErrMessageTooLarge,
	} {
		if errors.Is(err, known) {
			return
		}
	}
	t.Fatalf("mode %v: the encoder refused a message with an error it does not name: %v", mode, err)
}

// assertServiceWouldAccept holds the encoded message to the service's own
// limits, which is the specification's "limits before I/O" seen from the other side.
func assertServiceWouldAccept(t *testing.T, out encoded) {
	t.Helper()
	if n := len(out.Out.Attributes); n > MaxAttributes {
		t.Fatalf("%d attributes, over the service's %d", n, MaxAttributes)
	}
	if out.Bytes > MaxMessageBytes {
		t.Fatalf("%d bytes, over the service's %d", out.Bytes, MaxMessageBytes)
	}
	if n := len(out.Out.OrderingKey); n > MaxOrderingKeyBytes {
		t.Fatalf("an ordering key of %d bytes, over the service's %d", n, MaxOrderingKeyBytes)
	}
	if !utf8.ValidString(out.Out.OrderingKey) {
		t.Fatalf("an ordering key that is not a string: %q", out.Out.OrderingKey)
	}
	for name, value := range out.Out.Attributes {
		switch {
		case !legalAttributeName(name):
			t.Fatalf("attribute name %q, which the service refuses", name)
		case len(value) > MaxAttributeValueBytes:
			t.Fatalf("attribute %q has a %d-byte value, over the service's %d", name, len(value), MaxAttributeValueBytes)
		case !utf8.ValidString(value):
			t.Fatalf("attribute %q has a value that is not a string: %q", name, value)
		}
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
		for _, candidate := range want.Headers.Values(k) {
			if bytes.Equal(candidate, v) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("the lenient mode produced a header nobody published: %q=%q", k, v)
		}
	}
}

// nilOrNot renders the distinction draws.
func nilOrNot(b []byte) string {
	if b == nil {
		return "nil"
	}
	return "empty-or-set"
}

// incomingOf turns an encoded publication back into what a delivery of it
// looks like, which is what closes the round trip without a broker.
func incomingOf(out encoded) *psapi.Incoming {
	return &psapi.Incoming{
		ID:          "m1",
		Data:        out.Out.Data,
		Attributes:  out.Out.Attributes,
		OrderingKey: out.Out.OrderingKey,
	}
}
