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
// than a panic. The value an interface holds is walked by the same rule, as
// Target walks it: Adapt[any] handed a **pb.Order passes a *any holding it,
// and the walk used to stop at the **pb.Order, which the proto codec refused
// as not a message although it decoded into the same value.
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
			rv = e.Elem()
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
// A target that leads back to itself through an interface, such as a *any
// holding its own address (directly, or through other pointers and
// interfaces), is refused with an InvalidArgument error, as a decoder that
// decodes into what an interface's pointer points at follows it for ever:
// encoding/json until the stack overflows, or, built with
// GOEXPERIMENT=nojsonv2, by decoding a one-step loop into the interface and
// spinning for ever on a longer one. Only the target is checked. A loop
// inside the value, such as a struct field holding its own address, is not,
// and encoding/json and vmihailenco/msgpack still overflow the stack on it
// (encoding/json without the jsonv2 experiment spins for ever on one of two
// or more steps). Like Indirect it follows at most eight pointers and
// interfaces, so a `type P *P` pointing at itself returns the pointer it
// stopped at rather than looping for ever.
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

// loopsThroughAnInterface reports whether following the pointer p the way
// Target and the decoders do (into the pointer it points at, and into the
// pointer an interface it points at holds) comes back to where it has been
// through an interface. A loop of pointers alone needs a pointer type defined
// in terms of itself, which is left to the bound. It is Floyd's two-speed
// walk, so it allocates nothing and ends on a loop of any length; for a
// pointer to anything but a pointer or an interface it takes one step.
func loopsThroughAnInterface(p reflect.Value) bool {
	slow, fast := p, p
	for {
		var ok bool
		if fast, _, ok = nextTarget(fast); !ok {
			return false
		}
		if fast, _, ok = nextTarget(fast); !ok {
			return false
		}
		slow, _, _ = nextTarget(slow)
		if sameTarget(slow, fast) {
			break
		}
	}
	// fast is on the loop: go round it once.
	for at := fast; ; {
		next, through, _ := nextTarget(at)
		if through {
			return true
		}
		if sameTarget(next, fast) {
			return false
		}
		at = next
	}
}

// nextTarget is one step on from the non-nil pointer p: the pointer p points
// at, or the pointer held by the interface p points at. through is set for a
// step through an interface, and ok is false where the chain ends.
func nextTarget(p reflect.Value) (next reflect.Value, through, ok bool) {
	switch e := p.Elem(); e.Kind() {
	case reflect.Pointer:
		return e, false, !e.IsNil()
	case reflect.Interface:
		h := e.Elem()
		return h, true, h.Kind() == reflect.Pointer && !h.IsNil()
	default:
		return reflect.Value{}, false, false
	}
}

// sameTarget reports whether the pointers a and b are the same step: the
// same address and type.
func sameTarget(a, b reflect.Value) bool {
	return a.UnsafePointer() == b.UnsafePointer() && a.Type() == b.Type()
}
