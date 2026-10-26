package avro

import (
	"math"

	avro "github.com/hamba/avro/v2"

	"github.com/iqhive/iqmq/internal/errcode"
)

// maxRecordDepth bounds how many records one datum may nest inside each
// other.
//
// Records are the only thing counted, because they are the only way a body
// can nest deeper than its schema: a type can refer to itself only by name, a
// record is the only named type that holds other types, so every cycle in a
// schema passes through one. Arrays, maps and unions between two records nest
// no deeper than the schema text does. Without this bound a recursive schema
// nested as deep as MaxBytes allowed (one byte per level, so 8 Mi levels at the
// default), which is more stack than a goroutine has.
//
// 10,000 is the bound encoding/json and protobuf-go settled on for the same
// problem. It is far deeper than any message a person designs, and at it the
// walk and hamba/avro's decoders together use a few megabytes of stack, so a
// hostile body costs a few megabytes rather than the process.
const maxRecordDepth = 10_000

// The walk's reasons. Each is CorruptMessage like every other refusal of a
// body; they are separate values so that codec.Reason says which rule a body
// broke.
var (
	errVarint = errcode.New(errcode.CorruptMessage,
		"codec/avro: a varint is longer than its type allows or its value does not fit")
	errValue = errcode.New(errcode.CorruptMessage,
		"codec/avro: a union index, enum index or boolean is out of range")
	errLength = errcode.New(errcode.CorruptMessage,
		"codec/avro: a string or bytes length is negative")
	errStringLimit = errcode.New(errcode.CorruptMessage,
		"codec/avro: a string or bytes value is longer than MaxStringBytes")
	errBlock = errcode.New(errcode.CorruptMessage,
		"codec/avro: an array or map block's count or byte size is invalid")
	errElements = errcode.New(errcode.CorruptMessage,
		"codec/avro: an array or map declares more elements than MaxSliceElements")
	errZeroWidth = errcode.New(errcode.CorruptMessage,
		"codec/avro: the body declares more zero-byte array elements than MaxSliceElements")
	errDepth = errcode.New(errcode.CorruptMessage,
		"codec/avro: records nest deeper than the codec allows")
)

// walkKind is a schema node's type, narrowed to what the walk does with it.
// string and bytes are one kind because they encode the same way, and a
// logical type is its underlying type because that is how it encodes.
type walkKind uint8

const (
	walkNull walkKind = iota
	walkBoolean
	walkInt
	walkLong
	walkFloat
	walkDouble
	walkBytes
	walkFixed
	walkEnum
	walkRecord
	walkArray
	walkMap
	walkUnion
	// walkUnknown is a type this walk was not written for. It is refused rather
	// than handed to the library unchecked; hamba/avro v2.31.0 parses no such
	// type, so it is never reached.
	walkUnknown
)

// walkNode is one schema node compiled for the walk.
type walkNode struct {
	kind walkKind
	// zeroWidth is true when every encoding of the type is zero bytes: null, a
	// fixed of size 0, and a record whose fields are all zero-width. Those are
	// the only elements MaxBytes does not bound, because a body can declare
	// any number of them for the price of one block count.
	zeroWidth bool
	// size is a fixed's byte size or an enum's symbol count.
	size int
	// kids index walkPlan.nodes: a record's fields in order, an array's items,
	// a map's values or a union's branches.
	kids []int
}

// walkPlan is the codec's schema compiled once, at New, for the walk. A named
// type is compiled once and referred to by index, so a recursive schema is a
// cycle of indices rather than an infinite tree. nodes[0] is the root.
type walkPlan struct {
	nodes []walkNode
}

// compileWalk compiles s for the walk.
func compileWalk(s avro.Schema) walkPlan {
	var p walkPlan
	p.add(s, map[string]int{})
	return p
}

// add compiles s and returns its index. named holds the named types compiled
// or being compiled, by full name.
func (p *walkPlan) add(s avro.Schema, named map[string]int) int {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	ns, isNamed := s.(avro.NamedSchema)
	if isNamed {
		if i, ok := named[ns.FullName()]; ok {
			return i
		}
	}
	i := len(p.nodes)
	p.nodes = append(p.nodes, walkNode{kind: kindOf(s.Type())})
	if isNamed {
		// Registered before the fields are compiled, so a field of the record's
		// own type finds this node. Its zeroWidth is still false at that point,
		// which is the answer for a type in the middle of being computed: a
		// record that contains itself through fields alone has no finite
		// encoding, and one that does so through a union, an array or a map
		// costs at least a byte per level.
		named[ns.FullName()] = i
	}
	var kids []int
	switch s := s.(type) {
	case *avro.RecordSchema:
		for _, f := range s.Fields() {
			kids = append(kids, p.add(f.Type(), named))
		}
	case *avro.ArraySchema:
		kids = []int{p.add(s.Items(), named)}
	case *avro.MapSchema:
		kids = []int{p.add(s.Values(), named)}
	case *avro.UnionSchema:
		for _, t := range s.Types() {
			kids = append(kids, p.add(t, named))
		}
	case *avro.EnumSchema:
		p.nodes[i].size = len(s.Symbols())
	case *avro.FixedSchema:
		p.nodes[i].size = s.Size()
	}
	p.nodes[i].kids = kids
	p.nodes[i].zeroWidth = p.zeroWidth(i)
	return i
}

// zeroWidth decides node i's zeroWidth from its kids, which are compiled.
func (p *walkPlan) zeroWidth(i int) bool {
	n := p.nodes[i]
	switch n.kind {
	case walkNull:
		return true
	case walkFixed:
		return n.size == 0
	case walkRecord:
		for _, k := range n.kids {
			if !p.nodes[k].zeroWidth {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func kindOf(t avro.Type) walkKind {
	switch t {
	case avro.Null:
		return walkNull
	case avro.Boolean:
		return walkBoolean
	case avro.Int:
		return walkInt
	case avro.Long:
		return walkLong
	case avro.Float:
		return walkFloat
	case avro.Double:
		return walkDouble
	case avro.String, avro.Bytes:
		return walkBytes
	case avro.Fixed:
		return walkFixed
	case avro.Enum:
		return walkEnum
	case avro.Record, avro.Error:
		return walkRecord
	case avro.Array:
		return walkArray
	case avro.Map:
		return walkMap
	case avro.Union:
		return walkUnion
	default:
		return walkUnknown
	}
}

// datumWalk is one walk of one body against the codec's schema, run before
// hamba/avro is handed the body. It reads the bytes the way the library will
// and allocates nothing, so a body the library would turn into a negative
// slice length, a 100 GB allocation or 15 seconds of decoding is refused for
// the cost of reading it once. It lives on Unmarshal's stack.
type datumWalk struct {
	plan *walkPlan
	body []byte
	pos  int
	// elems is MaxSliceElements as New resolved it, 1<<48 included.
	elems int64
	// strMax is MaxStringBytes as New resolved it. Zero or less is no limit,
	// which is what hamba/avro makes of it.
	strMax int64
	// zeroLeft is how many more zero-width array elements the body may
	// declare. It starts at elems and is shared by every array in the body,
	// because a per-array limit multiplies with nesting.
	zeroLeft int64
}

// datum walks the whole body as one datum of the root type.
func (w *datumWalk) datum() error {
	if err := w.walk(0, 0); err != nil {
		return err
	}
	if w.pos != len(w.body) {
		return ErrTrailingBytes
	}
	return nil
}

// walk reads one value of node i. depth is how many records enclose it.
func (w *datumWalk) walk(i, depth int) error {
	n := &w.plan.nodes[i]
	if n.zeroWidth {
		return nil
	}
	switch n.kind {
	case walkBoolean:
		b, err := w.take(1)
		if err != nil {
			return err
		}
		if b[0] > 1 {
			return errValue
		}
		return nil
	case walkInt:
		_, err := w.int()
		return err
	case walkLong:
		_, err := w.long()
		return err
	case walkFloat:
		_, err := w.take(4)
		return err
	case walkDouble:
		_, err := w.take(8)
		return err
	case walkBytes:
		return w.bytes()
	case walkFixed:
		_, err := w.take(n.size)
		return err
	case walkEnum:
		return w.index(n.size)
	case walkRecord:
		if depth >= maxRecordDepth {
			return errDepth
		}
		for _, k := range n.kids {
			if err := w.walk(k, depth+1); err != nil {
				return err
			}
		}
		return nil
	case walkUnion:
		idx, err := w.int()
		if err != nil {
			return err
		}
		if idx < 0 || int(idx) >= len(n.kids) {
			return errValue
		}
		return w.walk(n.kids[idx], depth)
	case walkArray, walkMap:
		return w.blocks(n, depth)
	default:
		return errValue
	}
}

// blocks reads an array's or a map's blocks.
//
// A block whose count is negative carries its byte size, and that size must be
// exactly what its items take. hamba/avro ignores it when decoding but its skip
// decoder (a schema field the Go type has no field for) jumps by it, so a size
// that disagreed with the items would have the library read bytes this walk
// never checked.
func (w *datumWalk) blocks(n *walkNode, depth int) error {
	item := n.kids[0]
	countOnly := n.kind == walkArray && w.plan.nodes[item].zeroWidth
	var total int64
	for {
		count, err := w.long()
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		size := int64(-1)
		if count < 0 {
			// -MinInt64 is MinInt64, which hamba/avro adds to its running
			// total unchecked: GO-2026-5047's negative slice length.
			if count == math.MinInt64 {
				return errBlock
			}
			count = -count
			if size, err = w.long(); err != nil {
				return err
			}
			if size < 0 {
				return errBlock
			}
			if size > int64(len(w.body)-w.pos) {
				return ErrTruncated
			}
		}
		// Compared before it is added, so neither a huge count nor a run of
		// them can wrap the total.
		if count > w.elems-total {
			return errElements
		}
		total += count
		start := w.pos
		switch {
		case countOnly:
			// A zero-width element has nothing to read, so the block is counted
			// rather than walked, which keeps the walk linear in the body.
			if count > w.zeroLeft {
				return errZeroWidth
			}
			w.zeroLeft -= count
		default:
			// Every element here costs at least a byte or fails, so this loop
			// ends within the body however large count is.
			for range count {
				if n.kind == walkMap {
					if err := w.bytes(); err != nil {
						return err
					}
				}
				if err := w.walk(item, depth); err != nil {
					return err
				}
			}
		}
		if size >= 0 && int64(w.pos-start) != size {
			return errBlock
		}
	}
}

// bytes reads a string or bytes value: a length, then that many bytes.
func (w *datumWalk) bytes() error {
	l, err := w.long()
	if err != nil {
		return err
	}
	switch {
	case l < 0:
		return errLength
	case w.strMax > 0 && l > w.strMax:
		return errStringLimit
	case l > int64(len(w.body)-w.pos):
		return ErrTruncated
	}
	w.pos += int(l)
	return nil
}

// index reads an enum index and checks it names one of n symbols.
func (w *datumWalk) index(n int) error {
	idx, err := w.int()
	if err != nil {
		return err
	}
	if idx < 0 || int(idx) >= n {
		return errValue
	}
	return nil
}

// take returns the next n bytes.
func (w *datumWalk) take(n int) ([]byte, error) {
	if n > len(w.body)-w.pos {
		return nil, ErrTruncated
	}
	b := w.body[w.pos : w.pos+n]
	w.pos += n
	return b, nil
}

// int reads a zigzag varint that must fit in 32 bits: at most five bytes, the
// fifth carrying only the top four bits. hamba/avro drops the excess bits of an
// over-long int silently; this refuses it.
func (w *datumWalk) int() (int32, error) {
	u, err := w.varint(5, 0x0f)
	return int32(u>>1) ^ -int32(u&1), err
}

// long reads a zigzag varint that must fit in 64 bits: at most ten bytes, the
// tenth carrying only the top bit.
func (w *datumWalk) long() (int64, error) {
	u, err := w.varint(10, 0x01)
	return int64(u>>1) ^ -int64(u&1), err
}

// varint reads an unsigned little-endian base-128 varint of at most maxLen
// bytes, whose last byte may be at most last.
func (w *datumWalk) varint(maxLen int, last byte) (uint64, error) {
	var u uint64
	for i := range maxLen {
		if w.pos >= len(w.body) {
			return 0, ErrTruncated
		}
		b := w.body[w.pos]
		w.pos++
		if i == maxLen-1 && b > last {
			return 0, errVarint
		}
		u |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return u, nil
		}
	}
	return 0, errVarint
}
