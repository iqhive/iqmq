package codec

import "encoding/json"

// jsonV1 is the encoding/json codec: the reference implementation of family
// The specification and the kind every build contains.
//
// Semantics are the ones encoding/json has always had: HTML-sensitive runes
// are escaped, object members are sorted when the value is a map, duplicate
// member names in the input are accepted (the last wins), invalid UTF-8 is
// replaced with U+FFFD, and struct field names match case-insensitively.
type jsonV1 struct{}

// ContentType implements Codec.
func (jsonV1) ContentType() string { return ContentTypeJSON }

// Marshal implements Codec.
func (jsonV1) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements Codec. A body it cannot decode is a *DecodeError.
func (jsonV1) Unmarshal(data []byte, v any) error {
	if err := checkTarget(v); err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return &DecodeError{ContentType: ContentTypeJSON, Err: err}
	}
	return nil
}

// AppendMarshal implements Appender.
//
// encoding/json has no append entry point, so the encoding is built in a
// fresh buffer and copied into dst. That copy is exactly the cost the
// bake-off weighs against encoding/json/v2, which can encode straight into
// the caller's buffer.
func (c jsonV1) AppendMarshal(dst []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}
