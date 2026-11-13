package avro

import (
	"math/big"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

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
// Two more leave a slice the caller reused wrong (codec_union.go and
// codec_array.go). A null under a nullable union decoded into a SLICE writes
// nil over the slice's data word only, so the slice reads as nil but keeps its
// old length and capacity: index 0 panicked and an index past the first page
// was a fatal fault the process cannot recover from, two messages apart. And
// an empty array decoded into a slice that is not nil leaves it as it was, so
// the message read as the previous message's list.
//
// Four things in Unmarshal follow:
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
//   - An interface the library decodes into is the one place the Go type does
//     not say what the library is handed. When it already holds a pointer,
//     the library does not build a value of its own: it decodes into that
//     pointer with the pointer's type (codec_dynamic.go, efaceDecoder, under
//     any schema but a union; codec_record.go, recordIfaceDecoder, for an
//     interface with methods under a record), and for a nil pointer it
//     decodes into a fresh value of the pointer's element type. Watched: a
//     struct under a union reached that way came back with a heap address in
//     its long, and with a value already there the decode faulted at address
//     0x1. So the check of a type that has such an interface in it also notes
//     where (a heldNode plan), and Unmarshal walks the value along that plan
//     to every such interface that holds a pointer and checks the pointer's
//     type the same way.
//   - The same plan notes the slices the library decodes into in place. Once
//     the body has passed every check, Unmarshal sets each slice an array is
//     decoded into to length 0, keeping its capacity, so an empty array is
//     empty and a longer one still reuses the memory, and takes the capacity
//     from each nullable slice that is not a list (the library never decodes
//     into its memory); after the decode it sets each slice a null left
//     without data to nil, unless the target is the zero value or the walk
//     before the decode met no nullable slice, and no record in the plan has
//     two schema fields decoding into one Go field (see prepare for why that
//     is enough). Each step of the plan knows which kinds of site lie under
//     it, so each walk skips what has nothing for it, and a type with no such
//     interface and no such slice costs nothing more.
//
// Every check here follows at most eight pointers in a row, where the library
// follows any number, so a type with more is refused (overwriteIn).

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

// isNullableUnion reports whether s is a union of null and one other type.
func isNullableUnion(s avro.Schema) bool {
	u, ok := s.(*avro.UnionSchema)
	return ok && u.Nullable()
}

// maxIndirection is codec's bound on the pointers and interfaces a target walk
// follows. overwriteIn follows no more pointers than this in a row either, and
// refuses a type with more: past the bound it could not say where the library
// writes, and a pointer type defined in terms of itself (`type P *P`), which
// sent hamba/avro's own decoder construction into a stack overflow, never
// ends.
const maxIndirection = 8

// refuseOverwrites reports an InvalidArgument error when decoding into target
// (what the library is handed; a pointer) would have the library write a
// pointer over a struct or a big.Rat, and nil when it would not. What the type
// decides is computed once per type; the pointers held by the interfaces the
// library decodes into are checked on every call, as they are the value's.
// It returns the type's answer, whose plan Unmarshal walks again around the
// decode (see prepare and repair); for a target that is not a pointer, which
// codec.Target never returns, one with nothing to walk.
func (c *avroCodec) refuseOverwrites(target any) (*typeCheck, error) {
	t := reflect.TypeOf(target)
	if t == nil || t.Kind() != reflect.Pointer {
		return &typeCheck{}, nil
	}
	got, ok := c.overwriteChecked.Load(t)
	if !ok {
		path, why, plan := overwriteIn(c.schema, t.Elem(), c.tagKey, map[schemaType]*heldNode{}, 0)
		chk := &typeCheck{plan: sitesOf(plan)}
		if why != "" {
			chk.err = overwriteError(t, path, why)
		}
		got = chk
		c.overwriteChecked.Store(t, chk)
	}
	chk, _ := got.(*typeCheck)
	if chk.err != nil || chk.plan == nil || chk.plan.sites&siteHeld == 0 {
		return chk, chk.err
	}
	w := heldWalk{c: c, mode: walkCheck}
	if r := w.value(reflect.ValueOf(target).Elem(), chk.plan); r != nil {
		at := "the top level"
		if len(r.at) > 0 {
			at = "field " + strings.Join(r.at, ".")
		}
		return chk, overwriteError(t, append(r.at, r.path...), "the interface at "+at+" holds a "+r.held.String()+
			", and hamba/avro decodes into what such a pointer points at: "+r.why)
	}
	return chk, nil
}

// prepare sets every slice the library will decode an array into, in target,
// to length 0, keeping its capacity: the library leaves a slice that is not nil
// as it was for an empty array. It sets every other nullable slice (bytes, or
// the caller's own text type) to length and capacity 0, keeping its data word.
// The library never decodes into that memory, and with no capacity left a
// caller's UnmarshalText can no longer reuse it either, so a null that meets it
// later, in the target or in a copy a growing list leaves behind, has nothing
// to break. Unmarshal calls it once the body has passed every check, so a refused
// body leaves the target as it was.
//
// It reports whether repair must run after the decode: when the walk met a
// nullable slice in the target, or the plan, or the plan of a pointer an
// interface in the target holds, has a record two of whose schema fields
// decode into one Go field (an alias naming another field). When the target
// is the zero value and the plan has no such record, it walks nothing and
// reports false.
//
// Why that is enough: a null leaves a broken slice only where the slice has
// capacity when the null meets it. The walk follows every pointer, interface
// and slice (to its capacity) of the target that the plan leads through, so a
// nullable slice it did not meet is in memory the library allocated during
// this decode. That starts with no capacity and, as the library never shares
// what it allocates, is decoded once, unless two schema fields decode into
// it. Any other route to a slice twice in one decode (an any or a pointer to
// the target's own memory) goes through memory the caller supplied, where the
// walk meets the nullable slice; a zero target holds no such memory. Code of
// the caller's that the library calls (UnmarshalText, avro.UnionConverter) is
// taken to write only the value it is given. This deliberately does not ask
// which writes can give a slice capacity: hamba/avro has several (an array,
// a nullable union's value, bytes into a []byte, the caller's own
// UnmarshalText), and a rule that names them has missed one before.
func (c *avroCodec) prepare(target any, chk *typeCheck) (needRepair bool) {
	p := chk.plan
	if p == nil {
		return false
	}
	// A plan with no nullable slice and no interface never needs repair, and
	// its walk resets what lists it meets; the zero check is for the rest.
	if p.sites&siteShared == 0 && p.sites&(siteNull|siteHeld) != 0 && isZeroValue(target) {
		return false
	}
	w := heldWalk{c: c, mode: walkPrepare, sawShared: p.sites&siteShared != 0}
	w.value(reflect.ValueOf(target).Elem(), p)
	return w.needRepair()
}

// isZeroValue reports whether the pointer target points at the zero value of
// its type: every byte zero. Nonzero padding is read as not zero, which only
// costs a walk. reflect.Value.IsZero gives the same answer field by field,
// which on a struct holding slices costs more than the walk it saves.
func isZeroValue(target any) bool {
	v := reflect.ValueOf(target)
	size := v.Type().Elem().Size()
	p := v.UnsafePointer()
	if size%8 == 0 && uintptr(p)%8 == 0 {
		//nolint:gosec // G103: reads the size bytes of the value target points at, as words; it is aligned
		for _, w := range unsafe.Slice((*uint64)(p), size/8) {
			if w != 0 {
				return false
			}
		}
		return true
	}
	//nolint:gosec // G103: reads the size bytes of the value target points at
	for _, b := range unsafe.Slice((*byte)(p), size) {
		if b != 0 {
			return false
		}
	}
	return true
}

// repair sets every slice in target that a null left with no data, but with
// its old length and capacity, to nil. Unmarshal calls it after every decode
// prepare says can need it, one that failed part of the way included.
func (c *avroCodec) repair(target any, chk *typeCheck) {
	w := heldWalk{c: c, mode: walkRepair}
	w.value(reflect.ValueOf(target).Elem(), chk.plan)
}

// typeCheck is refuseOverwrites' answer for one target type: the error when
// the type itself is refused, and otherwise the plan of the places in a value
// of it that the library decodes into in place (nil when there are none).
type typeCheck struct {
	err  error
	plan *heldNode
}

// The kinds of site a plan node reaches.
const (
	// siteHeld is an interface the library decodes into: what it holds is
	// checked, and what that points at may have sites of any kind.
	siteHeld uint8 = 1 << iota
	// siteArray is a slice the library decodes an array into in place.
	siteArray
	// siteNull is a slice a nullable union decodes into, whose data word a
	// null sets to nil.
	siteNull
	// siteShared is a record two of whose schema fields decode into one Go
	// field, as a field's alias may name another field: that field is
	// decoded twice in one decode.
	siteShared
)

// walkMode is what a heldWalk does at the sites it reaches.
type walkMode uint8

const (
	// walkCheck refuses what an interface holds that the library must not
	// decode into, and changes nothing.
	walkCheck walkMode = iota
	// walkPrepare sets each siteArray slice to length 0, before the decode.
	walkPrepare
	// walkRepair sets each siteNull slice a null left with no data to nil,
	// after the decode.
	walkRepair
)

// overwriteError is the InvalidArgument error for decoding into a value of
// type t, at path, refused for why.
func overwriteError(t reflect.Type, path []string, why string) error {
	at := "the top level"
	if len(path) > 0 {
		at = "field " + strings.Join(path, ".")
	}
	return errcode.New(errcode.InvalidArgument, "codec/avro: cannot decode into "+t.String()+" at "+at+": "+why)
}

// heldNode is one step of the plan overwriteIn builds for a Go type, from a
// value of it to the interfaces and the slices the library decodes into in
// place: a value of type t decoded under the schema s. t's kind says which
// step it is: a struct looks in fields, a pointer (when it is not nil) and a
// slice (every element up to its capacity, which the library reuses) look in
// elem, and an interface is a place to check. A slice is a site itself when
// array or nullable is set. sites is every kind of site reachable from the
// node, itself included, so a walk skips a step with nothing for it.
type heldNode struct {
	t      reflect.Type
	s      avro.Schema
	fields []heldField
	elem   *heldNode
	// array is set for a slice the library decodes an array into in place,
	// nullable for one a nullable union decodes into, and shared for a
	// record two of whose schema fields decode into one Go field.
	array    bool
	nullable bool
	shared   bool
	sites    uint8
	// checked holds, for an interface, the answer for each pointer type
	// found in it (a *heldCheck per reflect.Type), so that a walk reads it
	// without building a key.
	checked sync.Map
}

// heldField is a struct field a heldNode looks in: the schema field's name,
// the Go field's index path, and the step for its value.
type heldField struct {
	name  string
	index []int
	node  *heldNode
}

// heldCheck is the answer for a pointer type an interface holds: why the
// library would write over a value in what it points at, and where, or the
// plan for the sites inside it.
type heldCheck struct {
	path []string
	why  string
	next *heldNode
}

// check returns the answer for the pointer type p held by the interface n:
// the library decodes into the value p points at, under n.s. The answer is
// the codec's for that schema and type, so every interface that meets the
// type shares one plan for it, and a walk that meets it again knows it.
func (w *heldWalk) check(n *heldNode, p reflect.Type) *heldCheck {
	if got, ok := n.checked.Load(p); ok {
		chk, _ := got.(*heldCheck)
		return chk
	}
	key := schemaType{n.s, p}
	got, ok := w.c.heldChecked.Load(key)
	if !ok {
		path, why, next := overwriteIn(n.s, p.Elem(), w.c.tagKey, map[schemaType]*heldNode{}, 0)
		got, _ = w.c.heldChecked.LoadOrStore(key, &heldCheck{path: path, why: why, next: sitesOf(next)})
	}
	chk, _ := got.(*heldCheck)
	n.checked.Store(p, chk)
	return chk
}

// sitesOf sets each node's sites in the plan n and returns n, or nil when no
// site can be reached from it: a recursive type's plan holds steps back to
// itself, and one with no site anywhere must cost Unmarshal nothing. The nodes
// are the plan's own, built by one overwriteIn call and not yet shared, so
// they are written before anything can read them.
func sitesOf(n *heldNode) *heldNode {
	var nodes []*heldNode
	seen := map[*heldNode]bool{}
	var collect func(*heldNode)
	collect = func(n *heldNode) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		nodes = append(nodes, n)
		n.sites = 0
		if n.t.Kind() == reflect.Interface {
			n.sites |= siteHeld
		}
		if n.array {
			n.sites |= siteArray
		}
		if n.nullable {
			n.sites |= siteNull
		}
		if n.shared {
			n.sites |= siteShared
		}
		collect(n.elem)
		for _, f := range n.fields {
			collect(f.node)
		}
	}
	collect(n)
	// A step back to a node makes a cycle, so what each node reaches is
	// gathered until nothing changes.
	for changed := true; changed; {
		changed = false
		for _, m := range nodes {
			got := m.sites
			if m.elem != nil {
				got |= m.elem.sites
			}
			for _, f := range m.fields {
				got |= f.node.sites
			}
			if got != m.sites {
				m.sites, changed = got, true
			}
		}
	}
	if n == nil || n.sites == 0 {
		return nil
	}
	return n
}

// heldWalk walks a value along a heldNode plan for the codec c, doing what
// mode says at each site. It remembers each pointer and slice it has followed,
// with the type and schema it was followed to, so a value that reaches itself
// is walked once: the first few in small, which costs no allocation, and the
// rest in visited. sawNull and sawShared are what prepare learns (see
// prepare).
type heldWalk struct {
	c         *avroCodec
	mode      walkMode
	sawNull   bool
	sawShared bool
	small     [8]heldVisit
	nsmall    int
	visited   map[heldVisit]bool
}

// wants reports whether the step n has anything for the walk's mode: an
// interface to check, or a slice to prepare or repair.
func (w *heldWalk) wants(n *heldNode) bool {
	switch w.mode {
	case walkCheck:
		return n.sites&siteHeld != 0
	case walkRepair:
		return n.sites&(siteNull|siteHeld) != 0
	default:
		// prepare looks at every nullable slice too: each one that is not a
		// list gives up its capacity.
		return n.sites&(siteArray|siteHeld|siteNull) != 0
	}
}

// needRepair is prepare's answer (see prepare).
func (w *heldWalk) needRepair() bool { return w.sawNull || w.sawShared }

// heldVisit is one pointer or slice followed to a value of type t under the
// schema s. A slice is told apart by its capacity too, as the elements walked
// depend on it.
type heldVisit struct {
	p   unsafe.Pointer
	cap int
	t   reflect.Type
	s   avro.Schema
}

// first reports whether the pointer or slice v has not been followed to the
// step n before, and marks it followed.
func (w *heldWalk) first(v reflect.Value, n *heldNode) bool {
	k := heldVisit{p: v.UnsafePointer(), t: n.t, s: n.s}
	if v.Kind() == reflect.Slice {
		k.cap = v.Cap()
	}
	for _, seen := range w.small[:w.nsmall] {
		if seen == k {
			return false
		}
	}
	if w.visited[k] {
		return false
	}
	if w.nsmall < len(w.small) {
		w.small[w.nsmall] = k
		w.nsmall++
		return true
	}
	if w.visited == nil {
		w.visited = map[heldVisit]bool{}
	}
	w.visited[k] = true
	return true
}

// heldRefusal is where a heldWalk found an interface holding a pointer the
// library must not decode into: the field path to the interface, the
// pointer's type, the path on from what it points at, and why.
type heldRefusal struct {
	at   []string
	held reflect.Type
	path []string
	why  string
}

// value walks v, a value of the type n was built for, along the plan. Under
// walkCheck it stops at the first interface holding a pointer the library must
// not decode into and reports it; it reports nothing under the other modes.
// Nothing is allocated unless something is refused or more than len(w.small)
// pointers and slices are followed.
func (w *heldWalk) value(v reflect.Value, n *heldNode) *heldRefusal {
	if !w.wants(n) {
		return nil
	}
	switch v.Kind() {
	case reflect.Interface:
		e := v.Elem()
		if e.Kind() != reflect.Pointer {
			// Nil, or not a pointer: the library builds a value of its own.
			return nil
		}
		chk := w.check(n, e.Type())
		if chk.why != "" {
			if w.mode != walkCheck {
				// Only a value the library built itself gets here, after
				// the check walk passed: a *big.Rat from a decimal.
				return nil
			}
			return &heldRefusal{held: e.Type(), path: chk.path, why: chk.why}
		}
		if w.mode == walkPrepare && chk.next != nil && chk.next.sites&siteShared != 0 {
			// Nil or not, what the pointer leads to is decoded with a
			// record two of whose fields share one Go field.
			w.sawShared = true
		}
		if !e.IsNil() && chk.next != nil && w.first(e, chk.next) {
			// An interface found inside what this pointer points at is
			// reported at its own place: the fields on the way to it
			// prepend themselves as the walk unwinds.
			return w.value(e.Elem(), chk.next)
		}
	case reflect.Pointer:
		if !v.IsNil() && n.elem != nil && w.first(v, n.elem) {
			return w.value(v.Elem(), n.elem)
		}
	case reflect.Slice:
		switch {
		case w.mode == walkPrepare && n.array && v.Len() != 0 && v.CanSet():
			v.SetLen(0)
		case w.mode == walkPrepare && n.nullable && !n.array && v.Cap() != 0 && v.CanSet():
			// A nullable slice that is not a list (bytes, or the caller's
			// own text type): the library never decodes into its memory, so
			// it gives up its capacity, keeping its data word so that a value
			// is still decoded in place without a new header. A null then
			// finds nothing to leave behind, here or in a copy of it that a
			// list growing past its capacity strands.
			v.SetLen(0)
			v.SetCap(0)
		case w.mode == walkRepair && n.nullable && v.IsNil() && v.Cap() != 0 && v.CanSet():
			// A null wrote nil over the data word and left the rest.
			v.SetZero()
		}
		if w.mode == walkPrepare && n.nullable {
			w.sawNull = true
		}
		if n.elem != nil && v.Cap() > 0 && w.first(v, n) {
			// Every element up to the capacity can be decoded into, so
			// the check and the preparation look at all of them; only
			// the elements decoded can need repair.
			end := v.Cap()
			if w.mode == walkRepair {
				end = v.Len()
			}
			v = v.Slice(0, end)
			for i := range end {
				if r := w.value(v.Index(i), n.elem); r != nil {
					return r
				}
			}
		}
	case reflect.Struct:
		for _, f := range n.fields {
			fv, err := v.FieldByIndexErr(f.index)
			if err != nil {
				// Through a nil embedded pointer, which the library
				// allocates afresh.
				continue
			}
			if r := w.value(fv, f.node); r != nil {
				r.at = append([]string{f.name}, r.at...)
				return r
			}
		}
	default:
		// No plan has a step of any other kind.
	}
	return nil
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
// why, which says what decodes there instead. why is "" when nowhere would,
// and then held is the plan to the interfaces and the slices in a value of t
// that the library decodes into in place (nil when it has none).
//
// It follows the types the library's decoders follow for each schema and
// nothing else: a type the library refuses for a schema is refused there with
// the library's own error, and an interface under a union is decoded into
// fresh values of the library's choosing. Elsewhere an interface is decoded
// into what it holds, which only the value says (heldNode). ptrs counts the
// pointers followed in a row. seen holds each record and struct pair being
// or already walked, with its plan.
func overwriteIn(s avro.Schema, t reflect.Type, tagKey string, seen map[schemaType]*heldNode, ptrs int) (path []string, why string, held *heldNode) {
	if r, ok := s.(*avro.RefSchema); ok {
		s = r.Schema()
	}
	if t.Kind() == reflect.Pointer && ptrs >= maxIndirection {
		// The library follows any number of pointers, so past the bound
		// this check could not say where it writes.
		return nil, "more than eight pointers in a row lead to " + t.String() + ", and this codec checks no further " +
			"where hamba/avro writes (a pointer type defined in terms of itself, such as type P *P, never ends): " +
			"use fewer pointers", nil
	}
	if t.Kind() == reflect.Interface {
		// efaceDecoder takes an any under any schema but a union, and the
		// record decoder an interface with methods; the library refuses an
		// interface with methods anywhere else.
		_, union := s.(*avro.UnionSchema)
		_, record := s.(*avro.RecordSchema)
		if !union && (t.NumMethod() == 0 || record) {
			return nil, "", &heldNode{t: t, s: s}
		}
		return nil, "", nil
	}
	switch s := s.(type) {
	case *avro.UnionSchema:
		switch t.Kind() {
		case reflect.Struct:
			if _, i := s.Indices(); s.Nullable() && isRat(t) && isBytesDecimal(s.Types()[i]) {
				return nil, ratWhy(t), nil
			}
			why := t.String() + " is a struct where the schema has a union, and hamba/avro decodes a union " +
				"into a struct by writing a pointer over it: "
			if s.Nullable() {
				return nil, why + "use a *" + t.String() + " there", nil
			}
			return nil, why + "a union other than [\"null\", T] decodes into an any, a map[string]any, " +
				"or a pointer to a type that implements avro.UnionConverter", nil
		case reflect.Pointer:
			if t.Implements(unionConverter) || !s.Nullable() {
				return nil, "", nil
			}
			_, i := s.Indices()
			path, why, held := overwriteIn(s.Types()[i], t.Elem(), tagKey, seen, ptrs+1)
			return path, why, through(t, s, held)
		case reflect.Slice:
			if !s.Nullable() {
				return nil, "", nil
			}
			_, i := s.Indices()
			path, why, held := overwriteIn(s.Types()[i], t, tagKey, seen, 0)
			if why != "" {
				return path, why, nil
			}
			// The node is this call's own (an array's, or none for bytes),
			// so it can be marked.
			if held == nil {
				held = &heldNode{t: t, s: s}
			}
			held.nullable = true
			return nil, "", held
		default:
			// A map[string]any is decoded into fresh values of the
			// library's choosing; anything else it refuses itself.
			return nil, "", nil
		}
	case *avro.PrimitiveSchema:
		if isBytesDecimal(s) && isRat(t) {
			return nil, ratWhy(t), nil
		}
	case *avro.RecordSchema:
		if t.Kind() == reflect.Pointer {
			path, why, held := overwriteIn(s, t.Elem(), tagKey, seen, ptrs+1)
			return path, why, through(t, s, held)
		}
		if t.Kind() != reflect.Struct {
			return nil, "", nil
		}
		key := schemaType{s, t}
		if n, ok := seen[key]; ok {
			return nil, "", n
		}
		n := &heldNode{t: t, s: s}
		seen[key] = n
		var decoded [][]int
		for _, f := range s.Fields() {
			ft, index, ok := fieldOf(t, tagKey, f)
			if !ok {
				continue
			}
			// A field whose alias names another field decodes into that
			// field's Go field as well, as the library matches an alias
			// when no Go field has the name itself.
			if slices.ContainsFunc(decoded, func(d []int) bool { return slices.Equal(d, index) }) {
				n.shared = true
			}
			decoded = append(decoded, index)
			path, why, held := overwriteIn(f.Type(), ft, tagKey, seen, 0)
			if why != "" {
				return append([]string{f.Name()}, path...), why, nil
			}
			if held != nil {
				n.fields = append(n.fields, heldField{name: f.Name(), index: index, node: held})
			}
		}
		if len(n.fields) == 0 && !n.shared {
			// A step back to n taken while walking it stays, and sitesOf
			// drops it when nothing else reaches a site.
			seen[key] = nil
			return nil, "", nil
		}
		return nil, "", n
	case *avro.ArraySchema:
		if t.Kind() == reflect.Slice {
			path, why, held := overwriteIn(s.Items(), t.Elem(), tagKey, seen, 0)
			if why != "" {
				return path, why, nil
			}
			return nil, "", &heldNode{t: t, s: s, elem: held, array: true}
		}
	case *avro.MapSchema:
		if t.Kind() == reflect.Map {
			// Each value is decoded into a fresh element, so no interface in
			// one holds a pointer the caller put there.
			path, why, _ := overwriteIn(s.Values(), t.Elem(), tagKey, seen, 0)
			return path, why, nil
		}
	}
	return nil, "", nil
}

// through is the step through a pointer to held, or nil when there is nothing
// to look for.
func through(t reflect.Type, s avro.Schema, held *heldNode) *heldNode {
	if held == nil {
		return nil
	}
	return &heldNode{t: t, s: s, elem: held}
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

// fieldOf returns the type and the index path of the struct field the library
// decodes the schema field f into, matching f's name and then its aliases as
// the library does.
func fieldOf(t reflect.Type, tagKey string, f *avro.Field) (reflect.Type, []int, bool) {
	if ft, index, ok := fieldNamed(t, tagKey, f.Name()); ok {
		return ft, index, true
	}
	for _, alias := range f.Aliases() {
		if ft, index, ok := fieldNamed(t, tagKey, alias); ok {
			return ft, index, true
		}
	}
	return nil, nil, false
}

// fieldNamed finds a struct field the way hamba/avro's describeStruct lists
// them: breadth first through embedded structs (and pointers to them), each
// struct visited once, unexported fields skipped, a field named by its tag up
// to any comma or else by its Go name, and the first match in that order is
// the one decoded into. index is its path for reflect.Value.FieldByIndex.
func fieldNamed(t reflect.Type, tagKey, name string) (reflect.Type, []int, bool) {
	type embedded struct {
		t     reflect.Type
		index []int
	}
	visited := map[reflect.Type]bool{}
	for next := []embedded{{t: t}}; len(next) > 0; {
		curr := next
		next = nil
		for _, st := range curr {
			if visited[st.t] {
				continue
			}
			visited[st.t] = true
			for i := range st.t.NumField() {
				sf := st.t.Field(i)
				if sf.Anonymous {
					et := sf.Type
					if et.Kind() == reflect.Pointer {
						et = et.Elem()
					}
					if et.Kind() == reflect.Struct {
						next = append(next, embedded{t: et, index: append(slices.Clip(st.index), i)})
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
					return sf.Type, append(slices.Clip(st.index), i), true
				}
			}
		}
	}
	return nil, nil, false
}
