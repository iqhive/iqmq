package avro

import (
	"math"
	"slices"
	"strconv"
	"strings"

	avro "github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/pkg/crc64"
)

// maxDecimalPrecision is the library's bound on the precision of any decimal,
// from the release after iqhive/avro v2.36.0 (which had none).
const maxDecimalPrecision = 1000

// droppedDecimal describes the first schema in s, at the record field path
// given, whose text gives a well-formed decimal (a bytes or a fixed, a
// precision of at least 1, a scale of 0 to the precision) that the library
// parsed as the plain type because it is past one of the library's own
// bounds: the digits a fixed of its size holds, or maxDecimalPrecision. It
// returns "" when there is none. The library drops such a decimal with no
// error, and a *big.Rat does not decode from the plain type, so every message
// would be refused as a corrupt body. A decimal the specification calls
// invalid (no precision, a scale past it, a decimal on a long) is left alone:
// the specification says to ignore it and use the type under it, as v1.1.3
// did.
func droppedDecimal(s avro.Schema, path []string) string {
	switch s := s.(type) {
	case *avro.RefSchema:
		// The schema text defines it elsewhere, where it is walked.
		return ""
	case *avro.RecordSchema:
		for _, f := range s.Fields() {
			if why := droppedDecimal(f.Type(), append(slices.Clip(path), f.Name())); why != "" {
				return why
			}
		}
		return ""
	case *avro.ArraySchema:
		return droppedDecimal(s.Items(), path)
	case *avro.MapSchema:
		return droppedDecimal(s.Values(), path)
	case *avro.UnionSchema:
		for _, t := range s.Types() {
			if why := droppedDecimal(t, path); why != "" {
				return why
			}
		}
		return ""
	default:
		// A primitive, a fixed or an enum, which carry their properties.
	}
	p, ok := s.(avro.PropertySchema)
	if !ok || p.Prop("logicalType") != "decimal" || isDecimal(s) {
		return ""
	}
	f, fixed := s.(*avro.FixedSchema)
	if !fixed && s.Type() != avro.Bytes {
		return ""
	}
	prec, ok := intProp(p, "precision")
	if !ok || prec < 1 {
		return ""
	}
	if p.Prop("scale") != nil {
		if scale, ok := intProp(p, "scale"); !ok || scale < 0 || scale > prec {
			return ""
		}
	}
	var rule string
	switch {
	case fixed && f.Size() > 0 && prec > fixedDigits(f.Size()):
		rule = "a decimal on a fixed of size " + strconv.Itoa(f.Size()) + " takes a precision of 1 to " +
			strconv.Itoa(fixedDigits(f.Size()))
	case prec > maxDecimalPrecision:
		rule = "a decimal takes a precision of 1 to " + strconv.Itoa(maxDecimalPrecision)
	default:
		// Dropped for some other reason, which the specification's rule for an
		// invalid logical type covers.
		return ""
	}
	at := "the top level"
	if len(path) > 0 {
		at = "field " + strings.Join(path, ".")
	}
	return at + ": " + rule + ", the library parses this one (precision " + strconv.Itoa(prec) + ") as the plain " +
		string(s.Type()) + ", and a *big.Rat does not decode from that: lower the precision, written as a whole " +
		"number with no fraction part, which changes the SchemaID, or remove the logicalType to decode the plain " +
		string(s.Type())
}

// fixedDigits is how many decimal digits a signed value of size bytes holds,
// floor(log10(2^(8*size-1)-1)), as the library and the specification work it
// out (hamba/avro allowed a digit more for some sizes).
func fixedDigits(size int) int {
	return int(math.Floor(math.Log10(2) * float64(8*size-1)))
}

// intProp returns the property name of p as an int, and false when it is not
// a whole number of at most 31 bits. The library keeps the numbers of a
// schema's properties as float64.
func intProp(p avro.PropertySchema, name string) (int, bool) {
	v, ok := p.Prop(name).(float64)
	if !ok || v != math.Trunc(v) || math.Abs(v) > math.MaxInt32 {
		return 0, false
	}
	return int(v), true
}

// canonicalFingerprint is the little-endian CRC-64-AVRO fingerprint of s's
// Parsing Canonical Form as Java's SchemaNormalization computes it, which is
// what Java writes in a single-object header. SchemaID's fingerprint is taken
// over the library's canonical form instead, which keeps logical types; for a
// schema with none, the two are the same. Python's avro and goavro write a
// primitive with a logical type as {"type":"long"} in the form, so for such
// a schema theirs is a third fingerprint.
func canonicalFingerprint(s avro.Schema) []byte {
	var b strings.Builder
	writeCanonical(&b, s, map[string]bool{})
	h := crc64.SumWithByteOrder([]byte(b.String()), crc64.LittleEndian)
	return h[:]
}

// writeCanonical writes the Parsing Canonical Form of s to b as Java does:
// only name, type, fields, symbols, items, values and size, in that order, a
// primitive by its name alone (a logical type is not part of the form), and a
// named schema by its full name after its first appearance, seen.
func writeCanonical(b *strings.Builder, s avro.Schema, seen map[string]bool) {
	if r, ok := s.(*avro.RefSchema); ok {
		s = r.Schema()
	}
	if n, ok := s.(avro.NamedSchema); ok {
		if seen[n.FullName()] {
			b.WriteString(`"` + n.FullName() + `"`)
			return
		}
		seen[n.FullName()] = true
	}
	switch s := s.(type) {
	case *avro.RecordSchema:
		typ := "record"
		if s.IsError() {
			typ = "error"
		}
		b.WriteString(`{"name":"` + s.FullName() + `","type":"` + typ + `","fields":[`)
		for i, f := range s.Fields() {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`{"name":"` + f.Name() + `","type":`)
			writeCanonical(b, f.Type(), seen)
			b.WriteByte('}')
		}
		b.WriteString(`]}`)
	case *avro.EnumSchema:
		b.WriteString(`{"name":"` + s.FullName() + `","type":"enum","symbols":[`)
		for i, sym := range s.Symbols() {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`"` + sym + `"`)
		}
		b.WriteString(`]}`)
	case *avro.FixedSchema:
		b.WriteString(`{"name":"` + s.FullName() + `","type":"fixed","size":` + strconv.Itoa(s.Size()) + `}`)
	case *avro.ArraySchema:
		b.WriteString(`{"type":"array","items":`)
		writeCanonical(b, s.Items(), seen)
		b.WriteByte('}')
	case *avro.MapSchema:
		b.WriteString(`{"type":"map","values":`)
		writeCanonical(b, s.Values(), seen)
		b.WriteByte('}')
	case *avro.UnionSchema:
		b.WriteByte('[')
		for i, t := range s.Types() {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, t, seen)
		}
		b.WriteByte(']')
	default:
		// A primitive or null.
		b.WriteString(`"` + string(s.Type()) + `"`)
	}
}
