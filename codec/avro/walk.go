package avro

import (
	"math"

	avro "github.com/iqhive/avro/v2"

	"github.com/iqhive/iqmq/internal/errcode"
)

// maxDepth bounds how deeply one datum's records, arrays and maps may nest,
// each value of one of them a level, unions and everything else none.
//
// Only a record lets a body nest deeper than its schema (a type refers to
// itself only by name, and a record is the only named type that holds others),
// but the arrays and maps between two records are levels too: each is a step
// of recursion in the library's decoders, and from the release after
// iqhive/avro v2.36.0 the library counts them against its Config.MaxDepth,
// which New sets to this same bound. The walk counts the same levels the same
// way, so it refuses with errDepth every body the library would refuse for its
// depth, before the library sees it. (The library does not count the array of a
// slice of scalars, which it decodes without a step of recursion, so at the
// bound the walk can refuse a body the library would decode into one.) Without
// a bound a recursive schema nested as deep as MaxBytes allowed (one byte per
// level, so 8 Mi levels at the default), which is more stack than a goroutine
// has.
//
// 10,000 is the bound encoding/json and protobuf-go settled on for the same
// problem, and the library's default. It is far deeper than any message a
// person designs, and at it the walk and the library's decoders together use a
// few megabytes of stack, so a hostile body costs a few megabytes rather than
// the process.
const maxDepth = 10_000

// The library's own bound on array elements that take no bytes (from the
// release after iqhive/avro v2.36.0), which the walk applies as the library
// does so that it refuses such a body first: each byte of the outermost value
// read before a block pays for zeroWidthPerByte of its elements, and the body
// may declare at most zeroWidthSlack more, or MaxSliceElements when that is
// smaller. The library's own encoder writes such arrays in blocks of at most
// zeroWidthPerByte, so what it writes always stays within the bound.
const (
	zeroWidthPerByte = 8
	zeroWidthSlack   = 1024
)

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
	errZeroWidthBytes = errcode.New(errcode.CorruptMessage,
		"codec/avro: the body declares more zero-byte array elements than its bytes pay for")
	errDepth = errcode.New(errcode.CorruptMessage,
		"codec/avro: records, arrays and maps nest deeper than the codec allows")
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
	// than handed to the library unchecked; the library parses no such type,
	// so it is never reached.
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
	// levels is, for a zero-width node, how many records nest in a value of
	// it, each a level the library enters though the walk reads nothing.
	levels int
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
	if p.nodes[i].zeroWidth && p.nodes[i].kind == walkRecord {
		levels := 0
		for _, k := range kids {
			levels = max(levels, p.nodes[k].levels)
		}
		p.nodes[i].levels = levels + 1
	}
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
// the library is handed the body. It reads the bytes the way the library will
// and allocates nothing, so a body hamba/avro turned into a negative slice
// length, a 100 GB allocation or 15 seconds of decoding is refused for the
// cost of reading it once. It lives on Unmarshal's stack.
type datumWalk struct {
	plan *walkPlan
	body []byte
	pos  int
	// elems is MaxSliceElements as New resolved it, 1<<48 included.
	elems int64
	// strMax is MaxStringBytes as New resolved it. Zero or less is no limit,
	// which is what the library makes of a negative one.
	strMax int64
	// zeroLeft is how many more zero-width array elements the body may
	// declare. It starts at elems and is shared by every array in the body,
	// because a per-array limit multiplies with nesting.
	zeroLeft int64
	// valueStart is where the outermost record, array or map began, paid how
	// many zero-width array elements its bytes have paid for, and slackLeft
	// how many more than that the body may declare: the library's bound (see
	// zeroWidthPerByte).
	valueStart int
	paid       int64
	slackLeft  int64
}

// newDatumWalk returns the walk of body under the plan p with the limits New
// resolved.
func newDatumWalk(p *walkPlan, body []byte, elems, strMax int) datumWalk {
	return datumWalk{
		plan: p, body: body, elems: int64(elems), strMax: int64(strMax), zeroLeft: int64(elems),
		slackLeft: int64(min(zeroWidthSlack, elems)),
	}
}

// enter starts the outermost value at the walk's position when depth says
// there is none yet, as the library's Reader does.
func (w *datumWalk) enter(depth int) {
	if depth == 0 {
		w.valueStart = w.pos
		w.paid = 0
	}
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

// walk reads one value of node i. depth is how many levels (see maxDepth)
// enclose it.
func (w *datumWalk) walk(i, depth int) error {
	n := &w.plan.nodes[i]
	if n.zeroWidth {
		if depth+n.levels > maxDepth {
			return errDepth
		}
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
		if depth >= maxDepth {
			return errDepth
		}
		w.enter(depth)
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
		if depth >= maxDepth {
			return errDepth
		}
		w.enter(depth)
		return w.blocks(n, depth+1)
	default:
		return errValue
	}
}

// blocks reads an array's or a map's blocks. depth counts the array or the
// map itself.
//
// A block whose count is negative carries its byte size, and that size must be
// exactly what its items take. The library ignores it when decoding but its skip
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
			// -MinInt64 is MinInt64, which hamba/avro added to its running
			// total unchecked: GO-2026-5047's negative slice length.
			// iqhive/avro refuses it too.
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
			// rather than walked, which keeps the walk linear in the body; the
			// records in one are levels all the same.
			if count > w.zeroLeft {
				return errZeroWidth
			}
			if depth+w.plan.nodes[item].levels > maxDepth {
				return errDepth
			}
			// What the value's bytes so far pay for, then the body's slack.
			credit := max(0, zeroWidthPerByte*int64(w.pos-w.valueStart)-w.paid)
			paid := min(count, credit)
			if count-paid > w.slackLeft {
				return errZeroWidthBytes
			}
			w.paid += paid
			w.slackLeft -= count - paid
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
// fifth carrying only the top four bits. hamba/avro dropped the excess bits of
// an over-long int silently; this refuses it, and so does iqhive/avro.
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
