package cloudevents_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"

	"bitbucket.org/iqhive/iqmq/codec"
	ce "bitbucket.org/iqhive/iqmq/codec/cloudevents"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// FuzzCloudEvents drives BOTH content modes from one input and holds them to
// the properties that matter for an untrusted delivery (the security checks,
// ):
//
//  1. nothing panics, in either mode;
//  2. every rejection is a limit (ErrTooLarge, ErrTooManyAttributes) or a
//     *codec.DecodeError carrying CorruptMessage, and nothing else;
//  3. anything the structured codec accepts is a VALID CloudEvent of
//     specversion 1.0 -- the decoder's job is to refuse what is not one, and
//     this is the property that would break if validation were dropped;
//  4. an accepted event re-encodes, and the second round trip is identical to
//     the first;
//  5. the two content modes agree: an event that came out of the structured
//     codec survives a trip through the binary binding with its attributes
//     intact. A binding whose two modes disagree is worse than one mode.
//
// the specification names FuzzProto and FuzzMsgpack. This target exists because
// CloudEvents has the largest untrusted surface of the four -- a JSON
// envelope, an attribute set the sender chooses, and a header block -- and
// "why not" had no answer.
func FuzzCloudEvents(f *testing.F) {
	seeds := []string{
		"", "{", "{}", "[]", "null", "0",
		`{"specversion":"1.0","id":"a","source":"/s","type":"t"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","data":{"x":1}}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","data_base64":"AAH/"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","data_base64":"not base64"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","time":"not a time"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","time":"2026-09-15T10:00:00Z"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","datacontenttype":"application/json","data":"x"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","partitionkey":"k"}`,
		`{"specversion":"1.0","id":"a","source":"/s","type":"t","EXT":"x"}`,
		`{"specversion":"0.3","id":"a","source":"/s","type":"t"}`,
		`{"specversion":"1.0","id":"a","source":"/s"}`,
		`[{"specversion":"1.0","id":"a","source":"/s","type":"t"}]`,
		`{"specversion":"1.0","id":"` + strings.Repeat("a", 4096) + `","source":"/s","type":"t"}`,
		"\ufeff{}",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	structured := ce.New()
	binding := ce.NewBinding()
	f.Fuzz(func(t *testing.T, data []byte) {
		var e event.Event
		if err := structured.Unmarshal(data, &e); err != nil {
			classify(t, "structured", data, err)
			// The binding must not panic on the same bytes, with the bytes
			// also offered as a header value.
			var be event.Event
			hs := []ce.Header{{Name: "ce-id", Value: string(data)}}
			if berr := binding.Decode(data, hs, &be); berr != nil {
				classify(t, "binary", data, berr)
			}
			return
		}

		// 3. Anything accepted is a valid 1.0 event.
		if e.SpecVersion() != ce.SpecVersion {
			t.Fatalf("accepted an event of specversion %q (body %q)", e.SpecVersion(), data)
		}
		if verr := e.Validate(); verr != nil {
			t.Fatalf("accepted an event that does not validate: %v (body %q)", verr, data)
		}

		// 4. Stable re-encode.
		first, err := structured.Marshal(&e)
		if err != nil {
			t.Fatalf("a decoded event does not re-encode: %v (body %q)", err, data)
		}
		var again event.Event
		if err := structured.Unmarshal(first, &again); err != nil {
			t.Fatalf("the codec cannot decode its own output %s: %v", first, err)
		}
		second, err := structured.Marshal(&again)
		if err != nil {
			t.Fatalf("re-encoding the second decode failed: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("the second round trip differs:\n%s\n%s", second, first)
		}
		ap, err := structured.(codec.Appender).AppendMarshal([]byte("P"), &e)
		if err != nil || !bytes.Equal(ap, append([]byte("P"), first...)) {
			t.Fatalf("AppendMarshal = %s, want %s (%v)", ap, append([]byte("P"), first...), err)
		}

		// 5. The two content modes agree.
		body, headers, err := binding.Encode(&e)
		if err != nil {
			// The one event structured mode carries and binary mode cannot:
			// an attribute value with leading or trailing whitespace, which a
			// transport header cannot hold unchanged. It is refused rather
			// than trimmed, and this is the property that found it.
			if errcode.CodeOf(err) != errcode.InvalidArgument {
				t.Fatalf("binary Encode of an event the structured codec accepted: %v (body %q)", err, data)
			}
			if !hasUntrimmedAttribute(&e) {
				t.Fatalf("binary Encode refused an event with no untrimmed attribute: %v (body %q)", err, data)
			}
			continueAfterUnrepresentable(t, &e)
			return
		}
		var viaBinary event.Event
		if err := binding.Decode(body, headers, &viaBinary); err != nil {
			t.Fatalf("binary Decode of its own Encode: %v (body %q)", err, data)
		}
		if viaBinary.ID() != e.ID() || viaBinary.Type() != e.Type() || viaBinary.Source() != e.Source() ||
			viaBinary.Subject() != e.Subject() || viaBinary.DataSchema() != e.DataSchema() ||
			!bytes.Equal(viaBinary.Data(), e.Data()) {
			t.Fatalf("the two content modes disagree:\n structured %s\n binary     %s", e.String(), viaBinary.String())
		}
		if !viaBinary.Time().Equal(e.Time()) {
			t.Fatalf("binary mode changed the time: %v vs %v", viaBinary.Time(), e.Time())
		}
	})
}

// hasUntrimmedAttribute reports whether any attribute or extension value of e
// has leading or trailing whitespace. It is the only reason Binding.Encode may
// refuse an event the structured codec accepted, so the fuzz target checks the
// reason rather than taking the error's word for it.
func hasUntrimmedAttribute(e *event.Event) bool {
	vals := []string{e.ID(), e.Source(), e.Type(), e.Subject(), e.DataSchema(), e.DataContentType()}
	for name, v := range e.Extensions() {
		_ = name
		if s, ok := v.(string); ok {
			vals = append(vals, s)
		}
	}
	for _, v := range vals {
		if v != "" && strings.TrimSpace(v) != v {
			return true
		}
	}
	return false
}

// continueAfterUnrepresentable checks that the structured mode is unaffected:
// the event still round-trips through the codec even though the binding
// refused it.
func continueAfterUnrepresentable(t *testing.T, e *event.Event) {
	t.Helper()
	c := ce.New()
	b, err := c.Marshal(e)
	if err != nil {
		t.Fatalf("structured Marshal refused an event only binary mode cannot carry: %v", err)
	}
	var back event.Event
	if err := c.Unmarshal(b, &back); err != nil {
		t.Fatalf("structured Unmarshal of its own output: %v", err)
	}
	if back.Type() != e.Type() {
		t.Fatalf("structured mode changed the type: %q vs %q", back.Type(), e.Type())
	}
}

// classify holds a rejection to the error model.
func classify(t *testing.T, mode string, data []byte, err error) {
	t.Helper()
	if sameErr(err, ce.ErrTooLarge, ce.ErrTooManyAttributes) {
		if code := errcode.CodeOf(err); code != errcode.MessageTooLarge {
			t.Fatalf("%s: %v carries %v, want MessageTooLarge", mode, err, code)
		}
		return
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("%s: %v (%T) is neither a limit nor a *codec.DecodeError (body %q)", mode, err, err, data)
	}
	if de.Err == nil || de.ContentType != ce.ContentType {
		t.Fatalf("%s: incomplete *codec.DecodeError %+v", mode, de)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%s: %v carries %v, want CorruptMessage", mode, err, code)
	}
}

// sameErr reports whether err IS one of targets, by identity. See reasonIs.
func sameErr(err error, targets ...error) bool {
	for _, target := range targets {
		if err == target { //nolint:errorlint // identity is the question
			return true
		}
	}
	return false
}
