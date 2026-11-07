package avro

import (
	"math/big"
	"reflect"
	"strings"

	avro "github.com/hamba/avro/v2"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The Go values hamba/avro decodes into by writing a pointer over them.
//
// hamba/avro v2.31.0 has two decoders that take the address of a value for
// the address of a pointer to it, and write a pointer over the value's first
// word. Nothing returns an error.
//
//   - A union decoded into a STRUCT. The library builds the decoder of a
//     pointer to the struct and runs it on the struct's own address
//     (codec_union.go, the reflect.Struct case of createDecoderOfUnion). A null
//     writes nil over the first word, a record writes a freshly allocated
//     pointer over it when it is zero, and when it is not zero the record is
//     decoded into whatever address the word holds. Watched: a nullable record
//     decoded into a Typed[*T] came back with its int64 field holding a heap
//     address and the rest of the record lost, a null came back as a zero
//     record, and the same body decoded into a *T that already held a value
//     faulted at address 0x1. A time.Time under a union is a struct too.
//   - A bytes decimal decoded into a big.Rat VALUE (codec_native.go,
//     bytesDecimalCodec). Watched: 123.45 came back as 0, as a field and
//     under a nullable union whose Go field was a *big.Rat. A decimal decodes
//     into a *big.Rat, and a nullable one into a **big.Rat. At the top level of
//     a schema no Typed shape gives the library that: codec.Adapt's
//     indirection is followed one pointer further than the decimal decoder
//     needs, so a decimal belongs in a record.
//
// Two things in Unmarshal follow from it:
//
//   - A schema whose top level is a union is handed the pointer to the
//     pointer (unionTarget), so Typed[*T] gives the library the **T its
//     nullable-union decoder takes: a null sets the *T to nil and a value
//     allocates it, or decodes into the T it already points at.
//   - A Go type that puts such a value where the library would write over it,
//     at the top or anywhere inside, is refused with an InvalidArgument error
//     before the library builds anything for it (refuseOverwrites), and the
//     error says what decodes there. The answer is worked out once per type
//     and kept.

// unionTarget is codec.Target for a schema whose top level is a union: it
// follows and allocates the same pointers and interfaces, but stops at the
// last pointer to a pointer rather than allocating through it, so the library
// can set that pointer to nil for a null or allocate it for a value.
//
// Like codec.Target it follows at most eight levels, so a value that points at
// itself returns the pointer it stopped at.
func unionTarget(v any) (any, error) {
	if v == nil {
		return nil, codec.ErrNilTarget
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, codec.ErrNilTarget
	}
	for range maxIndirection {
		e := rv.Elem()
		switch e.Kind() {
		case reflect.Pointer:
			if e.Type().Elem().Kind() != reflect.Pointer {
				// e is the nullable pointer itself: rv is what the
				// library takes.
				return rv.Interface(), nil
			}
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

// maxIndirection is codec's bound on the pointers and interfaces a target walk
// follows. overwriteIn follows no more pointers than this in a row either, so
// a pointer type defined in terms of itself (`type P *P`) cannot loop it.
const maxIndirection = 8

// refuseOverwrites reports an InvalidArgument error when decoding into a
// value of type t (what the library is handed; t is a pointer) would have the
// library write a pointer over a struct or a big.Rat, and nil when it would
// not. The answer is computed once per type.
func (c *avroCodec) refuseOverwrites(t reflect.Type) error {
	if t == nil || t.Kind() != reflect.Pointer {
		return nil
	}
	if got, ok := c.overwriteChecked.Load(t); ok {
		err, _ := got.(error)
		return err
	}
	var err error
	if path, why := overwriteIn(c.schema, t.Elem(), c.tagKey, map[schemaType]bool{}, 0); why != "" {
		at := "the top level"
		if len(path) > 0 {
			at = "field " + strings.Join(path, ".")
		}
		err = errcode.New(errcode.InvalidArgument, "codec/avro: cannot decode into "+t.String()+" at "+at+": "+why)
	}
	c.overwriteChecked.Store(t, err)
	return err
}

// schemaType is one schema and Go type pair overwriteIn has visited, which is
// what stops it on a recursive schema.
type schemaType struct {
	s avro.Schema
	t reflect.Type
}

var (
	// unionConverter is hamba/avro's own union hook, which the library asks
	// a pointer type for before it treats it as a nullable union.
	unionConverter = reflect.TypeFor[avro.UnionConverter]()
	// ratType is the type the library decodes a bytes decimal into.
	ratType = reflect.TypeFor[big.Rat]()
)

// overwriteIn reports where t, decoded under s, would have the library write a
// pointer over a value: the path of field names to it, outermost first, and
// why, which says what decodes there instead. why is "" when nowhere would.
//
// It follows the types the library's decoders follow for each schema and
// nothing else: a type the library refuses for a schema is refused there with
// the library's own error, and an interface is decoded into fresh values of
// the library's choosing. ptrs counts the pointers followed in a row.
func overwriteIn(s avro.Schema, t reflect.Type, tagKey string, seen map[schemaType]bool, ptrs int) (path []string, why string) {
	if r, ok := s.(*avro.RefSchema); ok {
		s = r.Schema()
	}
	if t.Kind() == reflect.Pointer && ptrs >= maxIndirection {
		return nil, ""
	}
	switch s := s.(type) {
	case *avro.UnionSchema:
		switch t.Kind() {
		case reflect.Struct:
			if _, i := s.Indices(); s.Nullable() && isRat(t) && isBytesDecimal(s.Types()[i]) {
				return nil, ratWhy(t)
			}
			why := t.String() + " is a struct where the schema has a union, and hamba/avro decodes a union " +
				"into a struct by writing a pointer over it: "
			if s.Nullable() {
				return nil, why + "use a *" + t.String() + " there"
			}
			return nil, why + "a union other than [\"null\", T] decodes into an any, a map[string]any, " +
				"or a pointer to a type that implements avro.UnionConverter"
		case reflect.Pointer:
			if t.Implements(unionConverter) || !s.Nullable() {
				return nil, ""
			}
			_, i := s.Indices()
			return overwriteIn(s.Types()[i], t.Elem(), tagKey, seen, ptrs+1)
		case reflect.Slice:
			if !s.Nullable() {
				return nil, ""
			}
			_, i := s.Indices()
			return overwriteIn(s.Types()[i], t, tagKey, seen, 0)
		default:
			// An interface or a map[string]any is decoded into fresh values
			// of the library's choosing; anything else it refuses itself.
			return nil, ""
		}
	case *avro.PrimitiveSchema:
		if isBytesDecimal(s) && isRat(t) {
			return nil, ratWhy(t)
		}
	case *avro.RecordSchema:
		if t.Kind() == reflect.Pointer {
			return overwriteIn(s, t.Elem(), tagKey, seen, ptrs+1)
		}
		if t.Kind() != reflect.Struct || seen[schemaType{s, t}] {
			return nil, ""
		}
		seen[schemaType{s, t}] = true
		for _, f := range s.Fields() {
			ft, ok := fieldOf(t, tagKey, f)
			if !ok {
				continue
			}
			if path, why := overwriteIn(f.Type(), ft, tagKey, seen, 0); why != "" {
				return append([]string{f.Name()}, path...), why
			}
		}
	case *avro.ArraySchema:
		if t.Kind() == reflect.Slice {
			return overwriteIn(s.Items(), t.Elem(), tagKey, seen, 0)
		}
	case *avro.MapSchema:
		if t.Kind() == reflect.Map {
			return overwriteIn(s.Values(), t.Elem(), tagKey, seen, 0)
		}
	}
	return nil, ""
}

// isRat reports whether t is a big.Rat value, or a struct the library converts
// to one, which is what its decimal decoder writes a pointer over.
func isRat(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t.ConvertibleTo(ratType)
}

// isBytesDecimal reports whether s is a bytes schema with the decimal logical
// type. A fixed decimal decoded into a big.Rat value is refused by the library
// itself, with an error.
func isBytesDecimal(s avro.Schema) bool {
	if r, ok := s.(*avro.RefSchema); ok {
		s = r.Schema()
	}
	p, ok := s.(*avro.PrimitiveSchema)
	return ok && p.Type() == avro.Bytes && p.Logical() != nil && p.Logical().Type() == avro.Decimal
}

// ratWhy is the reason a big.Rat value is refused where the schema has a bytes
// decimal.
func ratWhy(t reflect.Type) string {
	return t.String() + " is a big.Rat value where the schema has a bytes decimal, and hamba/avro decodes a " +
		"decimal into one by writing a pointer over it: a decimal decodes into a *big.Rat field and a nullable " +
		"one into a **big.Rat field, and no Typed shape decodes one at the top level of a schema, so put it in a record"
}

// fieldOf returns the type of the struct field the library decodes the schema
// field f into, matching f's name and then its aliases as the library does.
func fieldOf(t reflect.Type, tagKey string, f *avro.Field) (reflect.Type, bool) {
	if ft, ok := fieldNamed(t, tagKey, f.Name()); ok {
		return ft, true
	}
	for _, alias := range f.Aliases() {
		if ft, ok := fieldNamed(t, tagKey, alias); ok {
			return ft, true
		}
	}
	return nil, false
}

// fieldNamed finds a struct field the way hamba/avro's describeStruct lists
// them: breadth first through embedded structs (and pointers to them), each
// struct visited once, unexported fields skipped, a field named by its tag up
// to any comma or else by its Go name, and the first match in that order is
// the one decoded into.
func fieldNamed(t reflect.Type, tagKey, name string) (reflect.Type, bool) {
	visited := map[reflect.Type]bool{}
	for next := []reflect.Type{t}; len(next) > 0; {
		curr := next
		next = nil
		for _, st := range curr {
			if visited[st] {
				continue
			}
			visited[st] = true
			for i := range st.NumField() {
				sf := st.Field(i)
				if sf.Anonymous {
					et := sf.Type
					if et.Kind() == reflect.Pointer {
						et = et.Elem()
					}
					if et.Kind() == reflect.Struct {
						next = append(next, et)
					}
					continue
				}
				if !sf.IsExported() {
					continue
				}
				n := sf.Name
				if tag, ok := sf.Tag.Lookup(tagKey); ok {
					n, _, _ = strings.Cut(tag, ",")
				}
				if n == name {
					return sf.Type, true
				}
			}
		}
	}
	return nil, false
}
