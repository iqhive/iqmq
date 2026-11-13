//go:build go1.27 && goexperiment.jsonv2

package codec

import (
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"sync"
)

// jsonv2Available reports that this build contains encoding/json/v2.
const jsonv2Available = true

// jsonV2 is the encoding/json/v2 codec (Go 1.27 and later). What it promises
// a user is on the JSONv2 constant; this is why it is configured as it is.
//
// Deterministic is enabled, so that a body is a function of the value alone
// (dedup keys and content hashes depend on it) exactly as encoding/json's
// sorted map output is. So are the options that give a Go type the JSON form
// v1 gives it where v2's default refused the type outright or wrote a form v1
// reads back as a different value: FormatDurationAsNano (v2 has no default for
// a time.Duration), StringifyWithLegacySemantics (v2 refuses the `string`
// option on a bool or a string), and FormatNilSliceAsNull and
// FormatNilMapAsNull (v2 writes [] and {}, which v1 reads back as empty, not
// nil). Without them a consumer on v2 refused, as a corrupt message, what a
// publisher on v1 sent, and jsonv2.Register replaces application/json, so the
// two are mixed in one deployment by design. None of the four makes a body
// one version of this codec writes unreadable to another: an earlier JSONv2
// could neither write nor read a Duration or a `string`-option bool or
// string, and every reader takes both null and []/{} for a nil slice or map.
//
// FormatByteArrayAsArray and FormatBytesWithLegacySemantics are NOT enabled,
// though v1 and v2 differ there too, because each changes what v2 itself
// reads as well as writes: a JSONv2 consumer on an earlier version would
// refuse the new form of either, and with FormatByteArrayAsArray an old
// base64 byte array, stored or in flight, would no longer decode. So [N]byte
// and slices of a named byte type are the documented exceptions to v1/v2
// interop (TestJSONKindsDoNotCrossForTwoByteTypes).
//
// Everything else keeps v2's defaults, which differ from v1 in ways that are
// the point of the package: duplicate member names and invalid UTF-8 are
// rejected rather than silently repaired, struct field names match
// case-sensitively, HTML-sensitive runes are not escaped, and omitempty omits
// a field only when its JSON value is empty, so a zero number or false is
// written. None of them changes the Go value a body decodes to; the bytes
// differ where those defaults differ, which the model test pins.
type jsonV2 struct {
	// writers keeps the writer off the heap on the append path; the writer
	// itself would otherwise escape into the io.Writer interface on every
	// call.
	writers sync.Pool
}

// v2Options is built once: joining options per call would allocate.
var v2Options = jsonv2.JoinOptions(
	jsonv2.Deterministic(true),
	jsonv1.FormatDurationAsNano(true),
	jsonv2.FormatNilSliceAsNull(true),
	jsonv2.FormatNilMapAsNull(true),
	jsonv1.StringifyWithLegacySemantics(true),
)

// newJSONv2 returns the encoding/json/v2 codec.
func newJSONv2() (Codec, error) { return &jsonV2{}, nil }

// ContentType implements Codec.
func (*jsonV2) ContentType() string { return ContentTypeJSON }

// Marshal implements Codec. A value it cannot encode returns no body:
// encoding/json/v2 returns what it wrote before the failure alongside the
// error, where encoding/json and every other codec here return nothing.
func (*jsonV2) Marshal(v any) ([]byte, error) {
	b, err := jsonv2.Marshal(v, v2Options)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Unmarshal implements Codec. A body it cannot decode is a *DecodeError.
func (*jsonV2) Unmarshal(data []byte, v any) error {
	if err := checkTarget(v); err != nil {
		return err
	}
	if err := jsonv2.Unmarshal(data, v, v2Options); err != nil {
		return &DecodeError{ContentType: ContentTypeJSON, Err: err}
	}
	return nil
}

// AppendMarshal implements Appender by encoding straight into dst: v2 can
// write to an io.Writer, so no intermediate buffer is allocated or copied.
func (c *jsonV2) AppendMarshal(dst []byte, v any) ([]byte, error) {
	w, _ := c.writers.Get().(*appendWriter)
	if w == nil {
		w = new(appendWriter)
	}
	w.b = dst
	err := jsonv2.MarshalWrite(w, v, v2Options)
	out := w.b
	w.b = nil
	c.writers.Put(w)
	if err != nil {
		return dst, err
	}
	// MarshalWrite terminates the value with a newline; the body is the value.
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}

// appendWriter is an io.Writer that appends to a caller-owned slice.
type appendWriter struct{ b []byte }

// Write implements io.Writer.
func (w *appendWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}
