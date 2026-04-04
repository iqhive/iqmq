//go:build go1.27

package codec

import (
	jsonv2 "encoding/json/v2"
	"sync"
)

// jsonv2Available reports that this build contains encoding/json/v2.
const jsonv2Available = true

// jsonV2 is the encoding/json/v2 codec (Go 1.27 and later).
//
// The option set is deliberately minimal: only Deterministic is enabled, so
// that a body is a function of the value alone (dedup keys and content hashes
// depend on it) exactly as encoding/json's sorted map output is. Everything
// else keeps v2's defaults, which differ from v1 in ways that are the point of
// the package: duplicate member names and invalid UTF-8 are rejected rather
// than silently repaired, struct field names match case-sensitively, and
// HTML-sensitive runes are not escaped. Values encoded by either kind decode
// to the same Go value under the other; the bytes differ where those defaults
// differ, which the model test pins.
type jsonV2 struct{}

// v2Options is built once: joining options per call would allocate.
var v2Options = jsonv2.JoinOptions(jsonv2.Deterministic(true))

// newJSONv2 returns the encoding/json/v2 codec.
func newJSONv2() Codec { return jsonV2{} }

// ContentType implements Codec.
func (jsonV2) ContentType() string { return ContentTypeJSON }

// Marshal implements Codec.
func (jsonV2) Marshal(v any) ([]byte, error) { return jsonv2.Marshal(v, v2Options) }

// Unmarshal implements Codec. A body it cannot decode is a *DecodeError.
func (jsonV2) Unmarshal(data []byte, v any) error {
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
func (jsonV2) AppendMarshal(dst []byte, v any) ([]byte, error) {
	w, _ := appendWriters.Get().(*appendWriter)
	w.b = dst
	err := jsonv2.MarshalWrite(w, v, v2Options)
	out := w.b
	w.b = nil
	appendWriters.Put(w)
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

// appendWriters keeps the writer off the heap on the append path; the writer
// itself would otherwise escape into the io.Writer interface on every call.
var appendWriters = sync.Pool{New: func() any { return new(appendWriter) }}
