package codec

import "encoding"

// Raw returns the pass-through codec: bodies are bytes already and the codec
// only moves them.
//
// Marshal accepts, in order: an AppendMarshaler, a Marshaler, a []byte or
// *[]byte, a string or *string, an encoding.BinaryAppender and an
// encoding.BinaryMarshaler. A nil value encodes as an empty body. Anything
// else is ErrNotRawValue.
//
// Ownership: Marshal of a []byte returns that very slice, so the caller must
// not mutate it until the publication completes (this is the zero-copy path
// the memory engine's Owned mode is built for). Unmarshal into a *[]byte
// copies, reusing the target's capacity, so a decoded body never aliases the
// driver's receive buffer.
//
// Raw is not a kind of the family: it is not a JSON implementation and
// takes no part in that bake-off.
func Raw() Codec { return rawCodec{} }

// rawCodec is stateless; the zero value is ready to use.
type rawCodec struct{}

// ContentType implements Codec.
func (rawCodec) ContentType() string { return ContentTypeOctetStream }

// Marshal implements Codec.
func (rawCodec) Marshal(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case AppendMarshaler:
		return t.AppendMessage(nil)
	case Marshaler:
		return t.MarshalMessage()
	case []byte:
		return t, nil
	case *[]byte:
		if t == nil {
			return nil, ErrNilTarget
		}
		return *t, nil
	case string:
		return []byte(t), nil
	case *string:
		if t == nil {
			return nil, ErrNilTarget
		}
		return []byte(*t), nil
	case encoding.BinaryAppender:
		return t.AppendBinary(nil)
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	default:
		return nil, ErrNotRawValue
	}
}

// AppendMarshal implements Appender.
func (c rawCodec) AppendMarshal(dst []byte, v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return dst, nil
	case AppendMarshaler:
		return t.AppendMessage(dst)
	case []byte:
		return append(dst, t...), nil
	case *[]byte:
		if t == nil {
			return dst, ErrNilTarget
		}
		return append(dst, (*t)...), nil
	case string:
		return append(dst, t...), nil
	case *string:
		if t == nil {
			return dst, ErrNilTarget
		}
		return append(dst, (*t)...), nil
	case encoding.BinaryAppender:
		return t.AppendBinary(dst)
	}
	b, err := c.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// Unmarshal implements Codec. The target must be a pointer, an Unmarshaler or
// an encoding.BinaryUnmarshaler.
func (rawCodec) Unmarshal(data []byte, v any) error {
	switch t := v.(type) {
	case nil:
		return ErrNilTarget
	case Unmarshaler:
		return t.UnmarshalMessage(data)
	case *[]byte:
		if t == nil {
			return ErrNilTarget
		}
		*t = append((*t)[:0], data...)
		return nil
	case *string:
		if t == nil {
			return ErrNilTarget
		}
		*t = string(data)
		return nil
	case encoding.BinaryUnmarshaler:
		return t.UnmarshalBinary(data)
	default:
		return ErrNotRawValue
	}
}
