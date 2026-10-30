package codec

import (
	"encoding"
	"reflect"
)

// Raw returns the pass-through codec: bodies are bytes already and the codec
// only moves them.
//
// Marshal accepts, in order: an AppendMarshaler, a Marshaler, a []byte or
// *[]byte, a string or *string, an encoding.BinaryAppender and an
// encoding.BinaryMarshaler. A nil value encodes as an empty body. Anything
// else is ErrNotRawValue.
//
// The indirection Adapt adds is followed, as [Indirect] and [Target] describe:
// a pointer to a pointer, and a *any, are what they point at. So
// Adapt[*[]byte](Raw()) and Adapt[*T](Raw()) for a message type work as their
// element types do, and Adapt[any](Raw()) encodes any value above. Its Decode
// returns the body as a []byte, copied, unless the *any holds a non-nil
// pointer, which is decoded into. At most eight levels are followed, so a
// value that points at itself is ErrNotRawValue rather than a stack overflow,
// and Unmarshal allocates through a nil pointer only for a type it decodes
// into, so a refused target is left as it was.
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
//
// A nil pointer whose type is one of the method-carrying shapes is
// ErrNilTarget, as a nil *[]byte is: its method would be called on a nil
// receiver, and the panic, not an error, is what the caller got.
func (c rawCodec) Marshal(v any) ([]byte, error) { return c.marshal(v, 0) }

// marshal is Marshal for a value depth levels of indirection below the one
// the caller passed.
func (c rawCodec) marshal(v any, depth int) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case AppendMarshaler:
		if nilPointer(t) {
			return nil, ErrNilTarget
		}
		return t.AppendMessage(nil)
	case Marshaler:
		if nilPointer(t) {
			return nil, ErrNilTarget
		}
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
	case *any:
		if t == nil {
			return nil, ErrNilTarget
		}
		if depth >= maxIndirection {
			return nil, ErrNotRawValue
		}
		return c.marshal(*t, depth+1)
	case encoding.BinaryAppender:
		if nilPointer(t) {
			return nil, ErrNilTarget
		}
		return t.AppendBinary(nil)
	case encoding.BinaryMarshaler:
		if nilPointer(t) {
			return nil, ErrNilTarget
		}
		return t.MarshalBinary()
	default:
		if depth < maxIndirection && pointerToPointer(v) {
			return c.marshal(reflect.ValueOf(v).Elem().Interface(), depth+1)
		}
		return nil, ErrNotRawValue
	}
}

// AppendMarshal implements Appender.
func (c rawCodec) AppendMarshal(dst []byte, v any) ([]byte, error) {
	return c.appendMarshal(dst, v, 0)
}

// appendMarshal is AppendMarshal for a value depth levels of indirection
// below the one the caller passed.
func (c rawCodec) appendMarshal(dst []byte, v any, depth int) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return dst, nil
	case AppendMarshaler:
		if nilPointer(t) {
			return dst, ErrNilTarget
		}
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
	case *any:
		if t == nil {
			return dst, ErrNilTarget
		}
		if depth >= maxIndirection {
			return dst, ErrNotRawValue
		}
		return c.appendMarshal(dst, *t, depth+1)
	case encoding.BinaryAppender:
		if nilPointer(t) {
			return dst, ErrNilTarget
		}
		return t.AppendBinary(dst)
	}
	if depth < maxIndirection && pointerToPointer(v) {
		return c.appendMarshal(dst, reflect.ValueOf(v).Elem().Interface(), depth+1)
	}
	b, err := c.marshal(v, depth)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// Unmarshal implements Codec. The target must be a pointer, an Unmarshaler or
// an encoding.BinaryUnmarshaler.
func (c rawCodec) Unmarshal(data []byte, v any) error { return c.unmarshal(data, v, 0) }

// unmarshal is Unmarshal into a target depth levels of indirection below the
// one the caller passed.
func (c rawCodec) unmarshal(data []byte, v any, depth int) error {
	switch t := v.(type) {
	case nil:
		return ErrNilTarget
	case Unmarshaler:
		if nilPointer(t) {
			return ErrNilTarget
		}
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
	case *any:
		if t == nil {
			return ErrNilTarget
		}
		if rv := reflect.ValueOf(*t); rv.Kind() == reflect.Pointer && !rv.IsNil() {
			if depth >= maxIndirection {
				return ErrNotRawValue
			}
			return c.unmarshal(data, *t, depth+1)
		}
		*t = append([]byte(nil), data...)
		return nil
	case encoding.BinaryUnmarshaler:
		if nilPointer(t) {
			return ErrNilTarget
		}
		return t.UnmarshalBinary(data)
	default:
		if depth >= maxIndirection || !pointerToPointer(v) {
			return ErrNotRawValue
		}
		elem := reflect.ValueOf(v).Elem()
		if elem.IsNil() {
			// Allocated only for a type this codec decodes into, so a
			// refusal leaves the caller's pointer nil.
			if !decodesInto(elem.Type(), depth+1) {
				return ErrNotRawValue
			}
			elem.Set(reflect.New(elem.Type().Elem()))
		}
		return c.unmarshal(data, elem.Interface(), depth+1)
	}
}

// pointerToPointer reports whether v is a non-nil pointer to a pointer, the
// indirection Adapt adds to a T that is a pointer type.
func pointerToPointer(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && !rv.IsNil() && rv.Elem().Kind() == reflect.Pointer
}

// decodesInto reports whether a pointer of type t, allocated at depth,
// reaches a target Unmarshal decodes into within maxIndirection levels.
func decodesInto(t reflect.Type, depth int) bool {
	for ; depth <= maxIndirection; depth++ {
		switch {
		case t == reflect.TypeFor[*[]byte](), t == reflect.TypeFor[*string](), t == reflect.TypeFor[*any](),
			t.Implements(reflect.TypeFor[Unmarshaler]()), t.Implements(reflect.TypeFor[encoding.BinaryUnmarshaler]()):
			return true
		case t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Pointer:
			t = t.Elem()
		default:
			return false
		}
	}
	return false
}

// nilPointer reports whether v is a typed nil pointer. Its own method is
// what an Unmarshaler is decoded through, and one with a pointer receiver
// dereferences nil there: the panic, not ErrNilTarget, is what the caller
// got, where a nil *[]byte has always been ErrNilTarget.
func nilPointer(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}
