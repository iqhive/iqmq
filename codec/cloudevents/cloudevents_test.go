package cloudevents_test

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/iqhive/iqmq/codec"
	ce "github.com/iqhive/iqmq/codec/cloudevents"
	"github.com/iqhive/iqmq/codec/codectest"
	"github.com/iqhive/iqmq/internal/errcode"
)

// sample builds a complete, valid CloudEvent.
func sample(t *testing.T) *event.Event {
	t.Helper()
	e := event.New(ce.SpecVersion)
	e.SetID("id-1")
	e.SetSource("https://example.test/orders")
	e.SetType("test.order.created")
	e.SetSubject("order/42")
	e.SetTime(time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC))
	e.SetDataSchema("https://example.test/schemas/order.json")
	if err := e.SetData("application/json", map[string]any{"id": "a", "qty": 2}); err != nil {
		t.Fatal(err)
	}
	ce.SetPartitionKey(&e, "cust-42")
	e.SetExtension("traceparent", "00-aaaa-bbbb-01")
	return &e
}

func TestContract(t *testing.T) {
	want := sample(t)
	incomplete := event.New(ce.SpecVersion)
	incomplete.SetID("x")

	codectest.Suite{
		Codec:       ce.New(),
		ContentType: ce.ContentType,
		Cases: []codectest.Case{{
			Name:      "event",
			Value:     want,
			NewTarget: func() any { return new(event.Event) },
			Check: func(target any) error {
				got := target.(*event.Event)
				if got.ID() != want.ID() || got.Type() != want.Type() || got.Source() != want.Source() ||
					got.Subject() != want.Subject() || got.DataSchema() != want.DataSchema() ||
					!got.Time().Equal(want.Time()) || !bytes.Equal(got.Data(), want.Data()) {
					return errors.New("round trip lost an attribute: " + got.String())
				}
				if k, ok := ce.PartitionKey(got); !ok || k != "cust-42" {
					return errors.New("round trip lost the partition key")
				}
				return nil
			},
		}, {
			Name: "minimal event, no data",
			Value: func() *event.Event {
				e := event.New(ce.SpecVersion)
				e.SetID("id-2")
				e.SetSource("/svc")
				e.SetType("t")
				return &e
			}(),
			NewTarget: func() any { return new(event.Event) },
			Check: func(target any) error {
				got := target.(*event.Event)
				if got.ID() != "id-2" || got.Type() != "t" || len(got.Data()) != 0 {
					return errors.New("round trip changed a minimal event: " + got.String())
				}
				return nil
			},
		}},
		Unencodable: []any{
			make(chan int),
			"a string is not an event",
			// An event missing `source` and `type`: not a CloudEvent, so not
			// something this codec will put on a broker.
			&incomplete,
		},
		Corrupt: [][]byte{
			{},
			[]byte("{"),
			[]byte(`{}`),
			[]byte(`{"specversion":"0.3","id":"a","source":"/s","type":"t"}`),
			[]byte(`[{"specversion":"1.0","id":"a","source":"/s","type":"t"}]`),
			[]byte(`{"specversion":"1.0","id":"a"}`),
		},
	}.Run(t)
}

// TestTheCodecInterfaceCannotCarryABinding is the specification stated as a
// test: the structured codec is a codec.Codec, and the binary binding is not
// and cannot be, because codec.Codec has no way to return headers.
func TestTheCodecInterfaceCannotCarryABinding(t *testing.T) {
	var structured any = ce.New()
	if _, ok := structured.(codec.Codec); !ok {
		t.Fatal("the structured codec is not a codec.Codec")
	}
	var binding any = ce.NewBinding()
	if _, ok := binding.(codec.Codec); ok {
		t.Fatal("Binding satisfies codec.Codec, so it could be registered as one -- and then half of what it " +
			"does (the headers) would be silently dropped by every caller that used it through the interface")
	}
	// The concrete reason, spelled out: the interface's Marshal returns one
	// value and a binary-mode message is two.
	e := sample(t)
	body, headers, err := ce.NewBinding().Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) == 0 {
		t.Fatal("binary mode produced no headers, so there would be nothing for the interface to lose")
	}
	if !bytes.Equal(body, e.Data()) {
		t.Fatalf("binary mode wrapped the data: %s", body)
	}
}

// TestBinaryModeHeaderMapping pins the attribute-to-header mapping the package
// comment documents. It is the binding's contract and the thing another
// CloudEvents implementation has to agree with.
func TestBinaryModeHeaderMapping(t *testing.T) {
	e := sample(t)
	body, headers, err := ce.NewBinding().Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, h := range headers {
		if _, dup := got[h.Name]; dup {
			t.Fatalf("header %q appears twice", h.Name)
		}
		got[h.Name] = h.Value
	}
	want := map[string]string{
		"ce-specversion":     "1.0",
		"ce-id":              "id-1",
		"ce-source":          "https://example.test/orders",
		"ce-type":            "test.order.created",
		"ce-subject":         "order/42",
		"ce-time":            "2026-09-15T10:00:00Z",
		"ce-dataschema":      "https://example.test/schemas/order.json",
		"ce-datacontenttype": "application/json",
		"ce-partitionkey":    "cust-42",
		"ce-traceparent":     "00-aaaa-bbbb-01",
		// the portable content-type header carries the DATA's type in
		// binary mode, not application/cloudevents+json.
		codec.HeaderContentType: "application/json",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("header %q = %q, want %q (all: %v)", k, got[k], v, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("binary mode produced %d headers, want %d: %v", len(got), len(want), got)
	}
	// schema-id is deliberately NOT populated from dataschema.
	if _, ok := got[codec.HeaderSchemaID]; ok {
		t.Fatalf("binary mode set %s from dataschema; the package comment says it does not", codec.HeaderSchemaID)
	}

	// And it round-trips.
	var back event.Event
	if err := ce.NewBinding().Decode(body, headers, &back); err != nil {
		t.Fatal(err)
	}
	if back.ID() != e.ID() || back.Type() != e.Type() || back.Source() != e.Source() ||
		back.Subject() != e.Subject() || back.DataSchema() != e.DataSchema() ||
		back.DataContentType() != e.DataContentType() || !back.Time().Equal(e.Time()) ||
		!bytes.Equal(back.Data(), e.Data()) {
		t.Fatalf("binary round trip lost an attribute:\n got %s\nwant %s", back.String(), e.String())
	}
	if k, ok := ce.PartitionKey(&back); !ok || k != "cust-42" {
		t.Fatalf("binary round trip lost the partition key: %q %v", k, ok)
	}
	if back.Extensions()["traceparent"] != "00-aaaa-bbbb-01" {
		t.Fatalf("binary round trip lost an extension: %v", back.Extensions())
	}
}

// TestBinaryHeadersAreDeterministic: the specification requires deterministic encoding, and
// Go's map iteration is not one. Extensions are sorted.
func TestBinaryHeadersAreDeterministic(t *testing.T) {
	e := sample(t)
	for i := range 16 {
		e.SetExtension("x"+strconv.Itoa(i), strconv.Itoa(i))
	}
	b := ce.NewBinding()
	_, first, err := b.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	render := func(hs []ce.Header) string {
		var sb strings.Builder
		for _, h := range hs {
			sb.WriteString(h.Name)
			sb.WriteByte('=')
			sb.WriteString(h.Value)
			sb.WriteByte(';')
		}
		return sb.String()
	}
	want := render(first)
	for range 200 {
		_, again, err := b.Encode(e)
		if err != nil {
			t.Fatal(err)
		}
		if render(again) != want {
			t.Fatalf("the header block is not a function of the event:\n%s\n%s", render(again), want)
		}
	}
}

// TestAttributeCountIsBoundedBeforeAnythingIsBuilt is the count limit for
// the binding. The delivery is otherwise VALID -- it decodes when the limit
// permits it -- so what the test observes is the limit and not a bad message.
func TestAttributeCountIsBoundedBeforeAnythingIsBuilt(t *testing.T) {
	e := sample(t)
	for i := range 200 {
		e.SetExtension("x"+strconv.Itoa(i), "v")
	}
	body, headers, err := ce.NewBinding().Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the delivery carries %d headers", len(headers))

	var got event.Event
	tight := ce.NewBindingWithConfig(ce.Config{MaxAttributes: 16})
	derr := tight.Decode(body, headers, &got)
	if derr != ce.ErrTooManyAttributes { //nolint:errorlint // identity, see reasonIs
		t.Fatalf("a %d-header delivery at MaxAttributes=16 = %v, want ErrTooManyAttributes", len(headers), derr)
	}
	if got.ID() != "" {
		t.Fatal("the decoder ran anyway")
	}
	if code := errcode.CodeOf(derr); code != errcode.MessageTooLarge {
		t.Fatalf("ErrTooManyAttributes carries %v, want MessageTooLarge", code)
	}

	roomy := ce.NewBindingWithConfig(ce.Config{MaxAttributes: 1024})
	if err := roomy.Decode(body, headers, &got); err != nil {
		t.Fatalf("the same delivery at MaxAttributes=1024: %v", err)
	}
	if len(got.Extensions()) != 202 {
		t.Fatalf("the roomy decode kept %d extensions, want 202 (200 added, plus partitionkey and traceparent)", len(got.Extensions()))
	}
}

// TestBodyOverTheLimitIsRejectedBeforeTheDecoder is the byte limit, on both
// content modes, with a body that decodes when the limit permits it.
func TestBodyOverTheLimitIsRejectedBeforeTheDecoder(t *testing.T) {
	e := event.New(ce.SpecVersion)
	e.SetID("id")
	e.SetSource("/s")
	e.SetType("t")
	if err := e.SetData("text/plain", strings.Repeat("x", 4096)); err != nil {
		t.Fatal(err)
	}

	structured, err := ce.New().Marshal(&e)
	if err != nil {
		t.Fatal(err)
	}
	var got event.Event
	if derr := ce.NewWithConfig(ce.Config{MaxBytes: 1024}).Unmarshal(structured, &got); derr != ce.ErrTooLarge { //nolint:errorlint // identity
		t.Fatalf("a %d-byte structured body at MaxBytes=1024 = %v, want ErrTooLarge", len(structured), derr)
	}
	if got.ID() != "" {
		t.Fatal("the structured decoder ran anyway")
	}
	if derr := ce.NewWithConfig(ce.Config{MaxBytes: 1 << 16}).Unmarshal(structured, &got); derr != nil || got.ID() != "id" {
		t.Fatalf("the same body at MaxBytes=65536 = %v, %v", got.ID(), derr)
	}

	body, headers, err := ce.NewBinding().Encode(&e)
	if err != nil {
		t.Fatal(err)
	}
	var bgot event.Event
	if derr := ce.NewBindingWithConfig(ce.Config{MaxBytes: 1024}).Decode(body, headers, &bgot); derr != ce.ErrTooLarge { //nolint:errorlint // identity
		t.Fatalf("a %d-byte binary body at MaxBytes=1024 = %v, want ErrTooLarge", len(body), derr)
	}
	if bgot.ID() != "" {
		t.Fatal("the binary decoder ran anyway")
	}
	if derr := ce.NewBindingWithConfig(ce.Config{MaxBytes: 1 << 16}).Decode(body, headers, &bgot); derr != nil {
		t.Fatalf("the same delivery at MaxBytes=65536: %v", derr)
	}
}

// TestContentModesAreNotAutoDetected is the specification's "envelopes are never
// auto-detected unless configured", in both directions.
func TestContentModesAreNotAutoDetected(t *testing.T) {
	e := sample(t)
	structured, err := ce.New().Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	body, headers, err := ce.NewBinding().Encode(e)
	if err != nil {
		t.Fatal(err)
	}

	// A binary-mode body (the bare data) handed to the structured codec: it
	// is not a CloudEvent envelope, so it is refused.
	var got event.Event
	if derr := ce.New().Unmarshal(body, &got); derr == nil {
		t.Fatalf("the structured codec accepted a binary-mode body %s as %s", body, got.String())
	}
	// A structured body handed to the binding with no attribute headers:
	// ErrNoAttributes, not an empty event built from a JSON blob.
	if derr := ce.NewBinding().Decode(structured, nil, &got); !reasonIs(derr, ce.ErrNoAttributes) {
		t.Fatalf("the binding accepted a structured body with no headers: %v", derr)
	}
	// Each reads its own.
	if derr := ce.New().Unmarshal(structured, &got); derr != nil {
		t.Fatal(derr)
	}
	if derr := ce.NewBinding().Decode(body, headers, &got); derr != nil {
		t.Fatal(derr)
	}
}

// TestBatchBodyIsRefusedByName: a JSON batch is a different media type and one
// iqmq message is one event.
func TestBatchBodyIsRefusedByName(t *testing.T) {
	e := sample(t)
	one, err := ce.New().Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	batch := append(append([]byte("[  "), one...), ']')
	var got event.Event
	derr := ce.New().Unmarshal(batch, &got)
	if !reasonIs(derr, ce.ErrBatch) {
		t.Fatalf("a batch body = %v, want ErrBatch", derr)
	}
	if ce.ContentTypeBatch == ce.ContentType {
		t.Fatal("the batch media type is not distinct")
	}
}

// TestSpecVersionIsNotNegotiated: only 1.0 is read or written, and a body that
// says otherwise is refused rather than coerced.
func TestSpecVersionIsNotNegotiated(t *testing.T) {
	var got event.Event
	for _, body := range []string{
		`{"specversion":"0.3","id":"a","source":"/s","type":"t"}`,
		`{"specversion":"","id":"a","source":"/s","type":"t"}`,
		`{"specversion":"2.0","id":"a","source":"/s","type":"t"}`,
	} {
		derr := ce.New().Unmarshal([]byte(body), &got)
		if derr == nil {
			t.Fatalf("Unmarshal(%s) succeeded", body)
		}
		var de *codec.DecodeError
		if !errors.As(derr, &de) {
			t.Fatalf("Unmarshal(%s) = %v, want a *codec.DecodeError", body, derr)
		}
	}
	// Binary mode too.
	e := sample(t)
	body, headers, err := ce.NewBinding().Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for i := range headers {
		if headers[i].Name == "ce-specversion" {
			headers[i].Value = "0.3"
		}
	}
	if derr := ce.NewBinding().Decode(body, headers, &got); !reasonIs(derr, ce.ErrSpecVersion) {
		t.Fatalf("a 0.3 delivery = %v, want ErrSpecVersion", derr)
	}
}

// TestInvalidEventsAreRefusedOnBothSides: something that is not a CloudEvent
// is not put on a broker and is not handed to a handler.
func TestInvalidEventsAreRefusedOnBothSides(t *testing.T) {
	e := event.New(ce.SpecVersion)
	e.SetID("only-an-id")
	if _, err := ce.New().Marshal(&e); err == nil {
		t.Fatal("Marshal published an event with no source and no type")
	}
	if _, _, err := ce.NewBinding().Encode(&e); err == nil {
		t.Fatal("Binding.Encode published an event with no source and no type")
	}
	// With validation off it goes through, which is what SkipValidation is
	// for -- and the option has to do something or it is decoration.
	if _, err := ce.NewWithConfig(ce.Config{SkipValidation: true}).Marshal(&e); err != nil {
		t.Fatalf("SkipValidation did not skip validation: %v", err)
	}
	// Decode side: a body that parses but is not valid.
	var got event.Event
	body := []byte(`{"specversion":"1.0","id":"a"}`)
	if derr := ce.New().Unmarshal(body, &got); !reasonIs(derr, ce.ErrInvalidEvent) {
		t.Fatalf("an event with no source or type = %v, want ErrInvalidEvent", derr)
	}
	if derr := ce.NewWithConfig(ce.Config{SkipValidation: true}).Unmarshal(body, &got); derr != nil {
		t.Fatalf("SkipValidation did not skip validation on decode: %v", derr)
	}
}

// TestKafkaPrefix: the Kafka binding uses ce_ and the prefix is the caller's
// choice, because the codec does not know the driver.
func TestKafkaPrefix(t *testing.T) {
	b := ce.NewBindingWithConfig(ce.Config{HeaderPrefix: ce.KafkaHeaderPrefix})
	if b.Prefix() != "ce_" {
		t.Fatalf("Prefix() = %q", b.Prefix())
	}
	e := sample(t)
	body, headers, err := b.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range headers {
		if h.Name == codec.HeaderContentType {
			continue
		}
		if !strings.HasPrefix(h.Name, "ce_") {
			t.Fatalf("header %q does not use the configured prefix", h.Name)
		}
	}
	var got event.Event
	if err := b.Decode(body, headers, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID() != e.ID() {
		t.Fatalf("round trip under the Kafka prefix lost the id")
	}
	// And the default binding does not read them: the prefix is part of the
	// contract, not a guess.
	if err := ce.NewBinding().Decode(body, headers, &got); !reasonIs(err, ce.ErrNoAttributes) {
		t.Fatalf("the ce- binding read ce_ headers: %v", err)
	}
}

func TestRegisterAndTyped(t *testing.T) {
	r := codec.NewRegistry()
	if err := ce.Register(r); err != nil {
		t.Fatal(err)
	}
	// application/cloudevents+json has a structured syntax suffix, so a
	// registry that holds a JSON codec must not shadow it.
	json, err := codec.New(codec.JSONv1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Register(json); err != nil {
		t.Fatal(err)
	}
	got, err := r.For(ce.ContentType)
	if err != nil || got.ContentType() != ce.ContentType {
		t.Fatalf("For(%q) = %v, %v", ce.ContentType, got, err)
	}

	tc := ce.Typed()
	e := sample(t)
	b, err := tc.Encode(*e)
	if err != nil {
		t.Fatal(err)
	}
	v, err := tc.Decode(b)
	if err != nil || v.ID() != e.ID() {
		t.Fatalf("typed round trip = %v, %v", v.String(), err)
	}
}

// reasonIs reports whether err is a *codec.DecodeError whose own error IS
// want, by identity.
//
// errors.Is cannot answer this: *codec.DecodeError.Is matches any target
// carrying the same portable code, and five of this package's sentinels carry
// errcode.CorruptMessage, so errors.Is(<any corrupt body>, ErrBatch) is TRUE
// for every corrupt body.
func reasonIs(err, want error) bool {
	return codec.Reason(err) == want //nolint:errorlint // identity is the question; errors.Is answers a different one
}

// TestBodyMustBeAJSONObject and TestReservedExtensionNamesAreRefused pin the
// two rules this package adds to the specification's own validation, both
// found by FuzzCloudEvents.
//
// The failing input was, verbatim from
// testdata/fuzz/FuzzCloudEvents/20db7e884834a9dd:
//
//	,"specversion":"1.0","id":"0","source":"00","type":"0","00":"0000000","dAtA":"0"}
//
// No opening brace, and a member called `dAtA`. The SDK accepted both: it
// lower-cased `dAtA` into an extension called `data`, left DataEncoded nil,
// and then marshalled the extension as the event's data -- so the body
// re-encoded to something with the members in a different order and a
// different meaning, which is what the round-trip property caught.
func TestBodyMustBeAJSONObject(t *testing.T) {
	c := ce.New()
	inner := `"specversion":"1.0","id":"0","source":"/s","type":"t"`
	for _, body := range []string{
		`,` + inner + `}`,
		inner + `}`,
		`"a string"`,
		`42`,
		`true`,
		`   `,
	} {
		var got event.Event
		derr := c.Unmarshal([]byte(body), &got)
		if !reasonIs(derr, ce.ErrNotObject) {
			t.Fatalf("Unmarshal(%q) = %v, want ErrNotObject", body, derr)
		}
	}
	// The SDK takes the first of them, which is why the check is here.
	var lib event.Event
	if err := lib.UnmarshalJSON([]byte(`,` + inner + `}`)); err != nil {
		t.Fatalf("the SDK now rejects a body with no opening brace itself (%v), so ErrNotObject is dead code", err)
	}
	// And a real object still decodes.
	var ok event.Event
	if err := c.Unmarshal([]byte(`{`+inner+`}`), &ok); err != nil || ok.ID() != "0" {
		t.Fatalf("a JSON object was rejected: %v", err)
	}
}

func TestReservedExtensionNamesAreRefused(t *testing.T) {
	c := ce.New()
	body := []byte(`{"specversion":"1.0","id":"0","source":"/s","type":"t","00":"x","dAtA":"0"}`)

	// The SDK accepts it and turns dAtA into an extension called data.
	var lib event.Event
	if err := lib.UnmarshalJSON(body); err != nil {
		t.Fatalf("the SDK now rejects the collision itself (%v), so ErrReservedExtension is dead code", err)
	}
	if _, collided := lib.Extensions()["data"]; !collided {
		t.Fatalf("the SDK no longer lower-cases dAtA onto data (extensions %v), so this test does not show "+
			"what it claims", lib.Extensions())
	}
	// ... and the event it produces re-encodes to a different body, which is
	// the harm.
	once, err := lib.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var round event.Event
	if err := round.UnmarshalJSON(once); err != nil {
		t.Fatal(err)
	}
	twice, err := round.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(once, twice) {
		t.Fatalf("the collision no longer destabilises the round trip (%s), so the rule is decoration", once)
	}
	t.Logf("SDK round trip 1: %s", once)
	t.Logf("SDK round trip 2: %s", twice)

	var got event.Event
	if derr := c.Unmarshal(body, &got); !reasonIs(derr, ce.ErrReservedExtension) {
		t.Fatalf("Unmarshal of a reserved-name collision = %v, want ErrReservedExtension", derr)
	}
	// SkipValidation does not skip this one: a body that does not mean one
	// thing is not a strictness preference.
	lenient := ce.NewWithConfig(ce.Config{SkipValidation: true})
	if derr := lenient.Unmarshal(body, &got); !reasonIs(derr, ce.ErrReservedExtension) {
		t.Fatalf("SkipValidation skipped the reserved-name check: %v", derr)
	}

	// Encode side, both content modes.
	e := sample(t)
	e.SetExtension("data", "x")
	if _, err := c.Marshal(e); err == nil {
		t.Fatal("Marshal published an event with an extension called data")
	}
	if _, _, err := ce.NewBinding().Encode(e); err == nil {
		t.Fatal("Binding.Encode published an event with an extension called data")
	}
	// Binary decode: a ce-data header is a sender that does not understand
	// binary mode, not an extension.
	clean := sample(t)
	body2, headers, err := ce.NewBinding().Encode(clean)
	if err != nil {
		t.Fatal(err)
	}
	headers = append(headers, ce.Header{Name: "ce-data", Value: "x"})
	if derr := ce.NewBinding().Decode(body2, headers, &got); !reasonIs(derr, ce.ErrReservedExtension) {
		t.Fatalf("a ce-data header = %v, want ErrReservedExtension", derr)
	}
}

// TestBinaryModeRefusesWhatAHeaderCannotCarry is an
// attribute value with leading or trailing whitespace travels in structured
// mode and cannot travel in a header, so the binding refuses it rather than
// trimming the event on the way past.
//
// The failing input was, verbatim from
// testdata/fuzz/FuzzCloudEvents/b6c8ca96fcb3f1c1:
//
//	{"specversion":"1.0","id":"0","source":"0","type":"0 "}
//
// The structured codec carried `type: "0 "` faithfully and the binding
// returned `type: "0"`, because the SDK's SetType trims while its JSON
// decoder does not.
func TestBinaryModeRefusesWhatAHeaderCannotCarry(t *testing.T) {
	body := []byte(`{"specversion":"1.0","id":"0","source":"0","type":"0 "}`)
	c := ce.New()
	var e event.Event
	if err := c.Unmarshal(body, &e); err != nil {
		t.Fatalf("the structured codec refused a trailing space in type (%v); this test's premise is gone", err)
	}
	if e.Type() != "0 " {
		t.Fatalf("the structured codec now trims type (%q), so the two modes no longer differ and the "+
			"binding's refusal is dead code", e.Type())
	}
	if _, _, err := ce.NewBinding().Encode(&e); err == nil {
		t.Fatal("Binding.Encode accepted an attribute value a header cannot carry")
	} else if code := errcode.CodeOf(err); code != errcode.InvalidArgument {
		t.Fatalf("Binding.Encode = %v (%v), want InvalidArgument", err, code)
	}

	// The SDK's own asymmetry, pinned: SetType trims and the JSON decoder
	// does not.
	var lib event.Event
	if err := lib.UnmarshalJSON(body); err != nil {
		t.Fatal(err)
	}
	if lib.Type() != "0 " {
		t.Fatalf("the SDK's JSON decoder now trims type (%q)", lib.Type())
	}
	lib.SetType("0 ")
	if lib.Type() != "0" {
		t.Fatalf("the SDK's SetType no longer trims (%q), so a header round trip would preserve the space "+
			"and the binding's refusal is dead code", lib.Type())
	}

	// An extension value is checked too.
	clean := sample(t)
	clean.SetExtension("spaced", "v ")
	if _, _, err := ce.NewBinding().Encode(clean); err == nil {
		t.Fatal("Binding.Encode accepted an extension value with a trailing space")
	}

	// And a well-formed event is unaffected.
	if _, _, err := ce.NewBinding().Encode(sample(t)); err != nil {
		t.Fatalf("the check refused a clean event: %v", err)
	}
}
