package codec

import "reflect"

// Indirect returns the value a codec should encode, following the pointer and
// interface indirection the generic adapter adds.
//
// It exists because Adapt lifts a Codec into Typed[T] by taking the address of
// its argument: Typed[T].Encode(v) calls Marshal(&v), so a codec bound to
// T = *pb.Order is handed a **pb.Order, and a codec bound to T = any is handed
// a *any. A reflective codec never notices — encoding/json follows both — but a
// codec that type-asserts its argument to a concrete interface does: a
// **pb.Order does not implement proto.Message, and asserting on it is how a
// perfectly good value becomes "not a message".
//
// The rule is narrow on purpose. A pointer whose element is itself a pointer
// or an interface is indirection the adapter added, so it is followed; a
// pointer to anything else is the value the caller meant, so it is returned
// as it stands — *pb.Order stays *pb.Order and *[]byte stays *[]byte, because
// those are what their codecs want. A nil pointer stops the walk and is
// returned as the nil pointer it is, so the codec reports its own error rather
// than a panic.
//
// It follows at most eight pointers. A pointer type defined in terms of
// itself (`type P *P`) can point at itself, and the walk looped for ever on
// one; it now returns the pointer it stopped at, which the codec refuses.
//
// Hot path: reflect.ValueOf does not allocate (v is boxed already) and the
// common case — a pointer to a struct — returns after one Kind comparison.
func Indirect(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	for n := 0; n < maxIndirection && rv.Kind() == reflect.Pointer && !rv.IsNil(); n++ {
		switch e := rv.Elem(); e.Kind() {
		case reflect.Pointer:
			rv = e
		case reflect.Interface:
			if e.IsNil() {
				return rv.Interface()
			}
			return e.Elem().Interface()
		default:
			return rv.Interface()
		}
	}
	return rv.Interface()
}

// Target returns the pointer a codec should decode into, allocating through
// the nil pointers Adapt's Decode leaves behind, and reports ErrNilTarget for
// anything that is not a non-nil pointer.
//
// Typed[T].Decode declares `var v T` and calls Unmarshal(data, &v). For
// T = *pb.Order that is a **pb.Order whose element is nil, so a codec that
// merely followed the indirection would decode into a nil *pb.Order. Target
// allocates the element instead and returns the innermost pointer, which is
// the value a decoder can use.
//
// A *any whose element is nil is returned unchanged: there is no type to
// allocate, so the codec reports what it can and cannot decode into itself.
//
// Like Indirect it follows at most eight pointers and interfaces, so a value
// that points at itself, through a `type P *P` or a *any holding its own
// address, returns the pointer it stopped at rather than looping for ever.
func Target(v any) (any, error) {
	if err := checkTarget(v); err != nil {
		return nil, err
	}
	rv := reflect.ValueOf(v)
	for range maxIndirection {
		e := rv.Elem()
		switch e.Kind() {
		case reflect.Pointer:
			if e.IsNil() {
				if !e.CanSet() {
					return rv.Interface(), nil
				}
				e.Set(reflect.New(e.Type().Elem()))
			}
			rv = e
		case reflect.Interface:
			if e.IsNil() || e.Elem().Kind() != reflect.Pointer || e.Elem().IsNil() {
				return rv.Interface(), nil
			}
			rv = e.Elem()
		default:
			return rv.Interface(), nil
		}
	}
	return rv.Interface(), nil
}

// maxIndirection bounds how many pointers and interfaces Indirect, Target and
// Raw follow. Adapt adds one level; eight leaves room for a T that is itself
// a pointer type, and stops a value that points at itself.
const maxIndirection = 8
