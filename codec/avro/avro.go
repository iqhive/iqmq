// Package avro encodes message bodies as Avro binary.
//
// # Where the schema comes from, and what was rejected
//
// Avro is the one codec in this repository that cannot work without a schema:
// the binary encoding carries no field names, no types and no tags, so a
// datum is uninterpretable without the schema that wrote it. Somebody has to
// supply one, and which somebody is the design decision.
//
// **The decision: the caller supplies exactly one schema at construction, and
// it is both the writer schema and the reader schema.** New takes the schema
// text, parses it once, and returns a codec bound to it. There is no registry
// client, no lookup, no negotiation and no schema resolution: a body written
// by a different schema decodes as whatever that schema's bytes happen to mean
// under this one, which is Avro's own behaviour and is why FramingSingleObject
// exists below.
//
// Three alternatives were considered and rejected, and the reasons are here
// rather than in a commit message because each of them is what a reader will
// expect to find:
//
//   - **Confluent Schema Registry framing** (a zero magic byte, a four-byte
//     registry id, then the datum; the id resolved over HTTP with a cache).
//     Rejected for this increment, not for ever. It puts an HTTP client and a
//     cache inside a codec and makes DECODING depend on a service the message
//     broker does not control: a registry outage turns every delivery into an
//     undecodable body, so the receive path acquires a second availability
//     dependency that the error model has no code for. Registry ids are
//     also per-deployment, so the same bytes mean different things in two
//     clusters — which makes a body non-portable in exactly the way a schema
//     is supposed to prevent. It belongs in an `x/` module with its own
//     failure policy.
//   - **Inferring the schema from the Go type** (iqhive/avro can generate one
//     by reflection). Rejected outright. An Avro schema carries decisions a
//     Go struct does not: union member order, logical types, defaults,
//     namespaces, aliases. An inferred schema is a contract nobody reviewed
//     that changes silently when a Go field is renamed — and the reason to
//     choose Avro over JSON is that the schema is the contract.
//   - **Object Container File framing**, which embeds the writer schema in the
//     body and is therefore self-describing. Rejected for a MESSAGE body: the
//     schema JSON is usually far larger than the datum (hundreds of bytes
//     against tens), and OCF exists to amortise that over the many records of
//     a file. Paying it per message is the wrong trade, and iqmq bodies are
//     one record each.
//
// # Framing: configured, never detected
//
// Two framings ship, chosen at construction. The specification forbids auto-detection ("
// envelopes are never auto-detected unless configured"), so the decoder reads
// a body as the framing it was configured for and never guesses from the first
// two bytes:
//
//   - FramingDatum (the default) is a bare Avro binary datum. Smallest, and
//     nothing in the body identifies the schema -- including whether it is a
//     datum at all. A body that starts C3 01 is decoded as a datum, because
//     a datum can: the zigzag long -98 is C3 01, so an array or a map of 98
//     entries, a record whose first field is the long -98 and any float or
//     fixed that happens to begin with those bytes all encode that way. A
//     datum codec that refused them refused its own output, which is what
//     FuzzAvro found. A single-object body handed to a
//     datum codec is therefore misread rather than refused, exactly as a
//     datum from a different schema is; single-object framing is the answer
//     to both.
//   - FramingSingleObject is Avro's own single-object encoding: the two-byte
//     C3 01 marker, the eight-byte little-endian CRC-64-AVRO fingerprint of
//     the schema's parsing-canonical form, then the datum. Ten bytes of
//     overhead buys the one thing datum framing cannot do — the decoder
//     verifies the fingerprint and reports ErrSchemaMismatch, so a sender that
//     changed its schema is an error instead of silent corruption.
//
// Either way the codec implements codec.Schemer, so a publisher stamps
// `schema-id` with the fingerprint and a receiver can detect a mismatch from
// the header even under datum framing. That is the one place the root Codec
// contract already had a hook for a schema-bearing codec, and this is its
// first user.
//
// # What this package does NOT do
//
//   - No schema resolution. Avro's reader-vs-writer resolution rules (field
//     defaults, promotions, aliases) are implemented by iqhive/avro when it is
//     given TWO schemas; this codec has one, so a body is decoded under the
//     schema it was constructed with and nothing is resolved.
//   - No registry, no id allocation, no compatibility checking.
//   - No canonical form of the DATUM. Avro's canonical form is defined for
//     schemas, not for encoded data, and a map's entries are encoded in the
//     order Go iterates the map, which changes from one encode to the next,
//     so a body holding a map is not a function of the value. Sorting the
//     keys cannot fix that (a Go map does not keep the order it was filled
//     in), so there is no option for it. Where a dedup key or a content hash
//     is derived from the body, model the map as an array of key/value
//     records, which encodes the same way every time.
//   - No JSON encoding of Avro data.
//
// # A union decodes into a pointer, and a decimal into a *big.Rat
//
// Wherever the schema has a union, the Go value there must never be a struct.
// A nullable union (["null", T]) decodes into a pointer, or a slice for a
// nullable array (a nil slice encodes as the null branch, at the top level as
// in a record field; at the top level a pointer to a nil slice does too, as
// Marshal(&s) cannot be told from Typed[[]T], where in a record field it is an
// empty array); any union decodes into an interface, a map[string]any, or a
// pointer to a type that implements avro.UnionConverter. hamba/avro decodes a
// union into a struct by writing a pointer over the struct's first word, with
// no error: watched, a nullable record came back with its long field holding
// a heap address, a null came back as a zero record, and a
// decode into a struct that already held a value faulted at address 0x1. It
// does the same to a big.Rat value under a bytes decimal, union or not, and
// 123.45 came back as 0.
//
// So Unmarshal refuses either, at the top level or inside a record, an array
// or a map, with an InvalidArgument error before the library sees the body,
// and the value is left as it was. An interface the target holds counts too:
// where the library decodes into an interface that already holds a pointer
// (an any anywhere but under a union, an interface with methods under a
// record), it decodes into what the pointer points at, with the pointer's
// type, and for a nil pointer into a fresh value of that type; so the pointer's
// type is checked in the same way, on every call, as it is the value's. The
// error says what decodes there:
//
//   - A nullable record is a *T: Typed[*T] over the schema ["null", {record}]
//     decodes a null to a nil *T and a record to a *T that points at it. A
//     nullable timestamp is a *time.Time. A union with more than one non-null
//     branch is an any, a map[string]any or a *T whose type implements
//     avro.UnionConverter.
//   - A bytes decimal is a *big.Rat field, and a nullable one a **big.Rat
//     field. No Typed shape decodes a decimal at the top level of a schema
//     (codec.Adapt's indirection is followed one pointer further than the
//     library's decimal decoder needs), so a decimal belongs in a record.
//
// The check follows at most eight pointers in a row, and a target type with
// more is refused with an InvalidArgument error: the library follows any
// number, so past the bound nothing could be said about where it writes, and
// a pointer type defined in terms of itself (type P *P) overflowed the stack
// in the library.
//
// Two more targets are refused with an InvalidArgument error, as the library
// cannot decode into them and does not say so. One is an interface that
// leads back to itself through pointers and interfaces (an any holding its
// own address), which the library follows until the stack overflows. The
// other is a value under a string or an enum that the library decodes by
// calling UnmarshalText on it, when it holds nothing: an interface type with
// that method that is nil or holds a nil pointer, or a map (or a struct of
// one pointer) whose UnmarshalText has a value receiver and that is nil or
// zero. The library panics on those. A value the library allocates itself
// always holds nothing, so a nil pointer to such a value is refused whatever
// the body holds, a null under a nullable union included, and so is a list
// or a map of them, whatever the body holds, as the body says how many
// elements there are. Such a list decoded when the body fitted its capacity,
// and such a pointer when the body was null; both are refused now. Use a type
// whose UnmarshalText has a pointer receiver.
//
// # Decoding into a reused target
//
// A slice the target reuses holds what the body says: a null leaves it nil
// and an empty array empty, keeping its memory for a list that fits (the
// library left a slice a null met with its old length and no data, and an
// empty array met as it was). A map the target reuses is not reset: it keeps
// the entries the body does not name and takes the ones it does, as with
// encoding/json, so clear it first if it must hold only this message's.
//
// A pointer to an avro.UnionConverter under a union with a null is set to nil
// before the decode, so a null leaves it nil; the library left the previous
// message's value there, and allocates a fresh one for every other branch.
// An any under a decimal that holds a *big.Rat, as the library leaves one
// there, is emptied before the decode, so the library builds a fresh
// *big.Rat and the one the any held is not written.
//
// Keep no slice or pointer taken from a reused target across decodes: the
// next decode writes that memory, and a list it outgrows is left behind in a
// state that must not be read.
//
// The caller's own decoding code that the library calls, UnmarshalText (for
// a type decoded from a string or an enum) and an avro.UnionConverter's
// FromAny, is taken to write only the value it is given. Code that writes
// elsewhere in the target, through a pointer kept outside it, can leave a
// slice there with no data and its old length.
//
// union.go has the detail.
//
// # Untrusted input (the security checks)
//
// This package was written against hamba/avro v2.31.0, which was archived in
// January 2026. After v1.1.3 it moved to the fork github.com/iqhive/avro/v2,
// which fixes some of what follows (noted below). Every check stays: they are
// this package's own, and the fork does not fix them all.
//
// hamba/avro's DefaultConfig left MaxSliceAllocSize at 1<<48, and an Avro
// array's block count is an attacker-chosen zigzag varint. Watched, on
// v2.31.0, with a SIX-byte body claiming 2^40 array elements:
//
//	runtime: out of memory: cannot allocate 8796093022208-byte block
//	fatal error: out of memory
//
// An 8.8-terabyte allocation from six bytes, and an unrecoverable fatal error
// rather than an error value. So this package NEVER uses DefaultConfig: it
// sets MaxSliceAllocSize and MaxByteSliceSize from Config on every API it
// builds, and checks MaxBytes against len(data) before the decoder is handed
// anything.
//
// iqhive/avro grows a slice as its elements arrive instead, so an element
// that takes a byte can no longer be allocated ahead of the body.
//
// The second problem in this format is not a limit at all. Hamba/avro
// reported a TRUNCATED body as a SUCCESSFUL decode: frozenConfig.Unmarshal
// returned nil when the reader's error was io.EOF, and every field that ran
// off the end was left at its zero value (iqhive/avro reports
// io.ErrUnexpectedEOF). Measured on v2.31.0:
//
//	record in=<empty>  err=<nil>  v={ID: Qty:0}
//	array  in=80808001 err=<nil>  len=1048576 cap=1048576
//
// Four bytes became a million zeroes with no error -- silent data fabrication
// AND a 256x allocation amplifier in one. So this package does not use
// API.Unmarshal: it drives avro.Reader directly and treats io.EOF as
// ErrTruncated, then Peeks to refuse a body with bytes left over. A truncated
// message is a corrupt message, not a message of zeroes.
//
// Those limits are not enough on their own. govulncheck reports three
// advisories in hamba/avro v2.31.0 with no fixed version, and each of them
// got past every limit above. iqhive/avro refuses GO-2026-5047's counts; the
// other two, elements that take no bytes and nesting, are still the walk's:
//
//   - GO-2026-5047, an integer overflow. The array decoder negates a negative
//     block count and adds it to a running total unchecked, so a count of
//     MinInt64 (its own negation), or a block of one then a block of
//     MaxInt64, wraps the total negative. MaxSliceAllocSize passes it, and a
//     12-byte body decoded without an error into an []int64 whose len was
//     MinInt64.
//   - GO-2026-5048, unbounded allocation. MaxSliceElements bounds one array,
//     so nesting multiplies it: array<array<null>> decoded into [][]any
//     allocated 100 MB from a 259-byte body, and 256 KiB of body asks for
//     about 100 GB. A map had no element bound at all, because the library
//     checks MaxSliceAllocSize for arrays only.
//   - GO-2026-5046, CPU exhaustion. The library visits an element that costs
//     no bytes like any other, so array<array<record{}>> made 268 million
//     decode steps from 16 KiB of body (0.94 s), and 256 KiB is about 15 s a
//     message.
//
// So Unmarshal walks the body against the schema before the library is handed
// it (walk.go). The walk reads the body the way the library will, allocates
// nothing, and refuses with a CorruptMessage error: a malformed varint; a
// union index, enum index or boolean out of range; a string or bytes length
// that is negative, over MaxStringBytes or past the end; a block count of
// MinInt64; a block byte size that is negative, past the end or not what the
// block's items take; an array or a map whose count, summed over its blocks,
// exceeds MaxSliceElements; more zero-byte array elements in the whole body
// than MaxSliceElements; and records nested more than 10,000 deep. An element
// that costs a byte is already bounded by MaxBytes, so only zero-byte elements
// (null, a fixed of size 0, a record of nothing else) need a budget of their
// own, and the walk counts those rather than visiting them, which keeps it
// linear in the body.
//
// The depth bound closes a gap this comment used to state and leave open. A
// RECURSIVE schema (a record with a field of its own type) had no nesting
// bound in hamba/avro, and MaxBytes alone allowed one level per byte: 8 Mi
// levels at the default, more stack than a goroutine has. (iqhive/avro
// bounds it at 10,000 levels by default.)
//
// govulncheck reported all three for as long as this package linked
// hamba/avro, because it reports a vulnerable module this package calls, not
// whether this package contains the advisory. It has none for iqhive/avro.
//
// All values returned by this package are stateless and safe for concurrent
// use by multiple goroutines.
package avro

import (
	"encoding/hex"
	"errors"
	"io"
	"math"
	"reflect"
	"sync"

	avro "github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/soe"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// ContentType is the media type this package produces under both framings.
//
// It is the same string for a datum and for a single-object body, which is a
// limitation stated rather than worked around: Avro registers no media type
// for single-object encoding, and the framing has to be agreed out of band
// anyway because the SCHEMA does. A receiver that needs to tell them apart
// reads `schema-id` (codec.HeaderSchemaID), which this codec populates through
// codec.Schemer.
const ContentType = "avro/binary"

// Defaults for Config.
const (
	// DefaultMaxBytes is the largest body Unmarshal will decode.
	DefaultMaxBytes = 8 << 20
	// DefaultMaxSliceElements bounds how many elements one array or map may
	// declare, and how many zero-byte array elements one body may declare in
	// all. hamba/avro's own default is 1<<48, and it is a count rather than
	// the byte size its name suggests, so a large default multiplies by the
	// element size.
	DefaultMaxSliceElements = 1 << 16
	// DefaultMaxStringBytes bounds one `bytes` or `string` value. hamba/avro's
	// own default is 1 MiB, which is kept.
	DefaultMaxStringBytes = 1 << 20
)

// Framing selects how a datum is wrapped. It is configured, never detected.
type Framing uint8

// The two framings.
const (
	// FramingDatum is a bare Avro binary datum and is the default.
	FramingDatum Framing = iota
	// FramingSingleObject is Avro's single-object encoding: C3 01, the
	// eight-byte CRC-64-AVRO fingerprint, then the datum.
	FramingSingleObject
)

// String returns the framing's slug.
func (f Framing) String() string {
	if f == FramingSingleObject {
		return "sobj"
	}
	return "datum"
}

// Errors this package reports.
var (
	// ErrTooLarge (MessageTooLarge) reports a body over Config.MaxBytes,
	// before anything is allocated from it.
	ErrTooLarge = errcode.New(errcode.MessageTooLarge, "codec/avro: body exceeds the configured maximum")
	// ErrSchemaMismatch (CorruptMessage) reports a single-object body whose
	// fingerprint is not this codec's schema. It is the one thing datum
	// framing cannot tell you.
	ErrSchemaMismatch = errcode.New(errcode.CorruptMessage, "codec/avro: body was written by a different schema")
	// ErrFraming (CorruptMessage) reports a body that does not carry the
	// single-object header its codec is configured for: a missing or wrong
	// marker, or a body too short to hold the header. A datum codec never
	// reports it, because nothing in a datum says it is one.
	ErrFraming = errcode.New(errcode.CorruptMessage, "codec/avro: body framing does not match the configured framing")
	// ErrTruncated (CorruptMessage) reports a body that ended before the
	// schema did. hamba/avro reports this as a successful decode with zero
	// values, which is why this package does not use its Unmarshal.
	ErrTruncated = errcode.New(errcode.CorruptMessage, "codec/avro: body ended before the schema did")
	// ErrTrailingBytes (CorruptMessage) reports bytes after the datum. One
	// body is one datum; a second one is a framing error, not a value.
	ErrTrailingBytes = errcode.New(errcode.CorruptMessage, "codec/avro: bytes remain after the datum")
	// ErrNoSchema (InvalidConfig) reports a Config with no schema.
	ErrNoSchema = errcode.New(errcode.InvalidConfig, "codec/avro: no schema")
)

// Config configures the codec.
type Config struct {
	// Schema is the Avro schema text, as JSON. It is required, and it is both
	// the writer schema and the reader schema.
	Schema string
	// Framing selects the body framing. The zero value is FramingDatum.
	Framing Framing
	// MaxBytes is the largest body Unmarshal decodes. Zero means
	// DefaultMaxBytes; negative removes the limit.
	MaxBytes int
	// MaxSliceElements bounds how many elements one array or one map may
	// declare, summed over its blocks, and how many zero-byte array elements
	// (null, a fixed of size 0, a record of nothing else) one body may declare
	// across all of its arrays. Zero means DefaultMaxSliceElements; a negative
	// value restores hamba/avro's own 1<<48 for all three, which is the setting
	// the package comment's 8.8-terabyte allocation was measured under.
	//
	// The body-wide budget is there because a zero-byte element costs nothing
	// against MaxBytes, so a bound on each array multiplies with nesting: 64
	// arrays of 65,536 nulls is 259 bytes of body. Every other element costs
	// at least a byte, and MaxBytes bounds those.
	//
	// It is a COUNT and not a byte size, which is worth saying because the
	// library's own field is called MaxSliceAllocSize and documented as "the
	// maximum size that the decoder will allocate" while being compared
	// against the accumulated element count. So the bytes an array may cost
	// are this count times the element size: 65,536 elements of a 1 KiB
	// record is 64 MiB, and a caller whose records are large lowers this or
	// lowers MaxBytes.
	MaxSliceElements int
	// MaxStringBytes bounds one `bytes` or `string` value. Zero means
	// DefaultMaxStringBytes.
	MaxStringBytes int
	// TagKey is the struct tag key. Empty means hamba/avro's own "avro".
	TagKey string
}

// New returns the codec for c's schema, or an error when the schema is missing
// or will not parse.
func New(c Config) (codec.Codec, error) {
	if c.Schema == "" {
		return nil, ErrNoSchema
	}
	schema, err := avro.Parse(c.Schema)
	if err != nil {
		return nil, errcode.New(errcode.InvalidConfig, "codec/avro: "+err.Error())
	}
	fp, err := soe.ComputeFingerprint(schema)
	if err != nil {
		return nil, errcode.New(errcode.InvalidConfig, "codec/avro: "+err.Error())
	}
	header, err := soe.BuildHeaderForFingerprint(fp)
	if err != nil {
		return nil, errcode.New(errcode.InvalidConfig, "codec/avro: "+err.Error())
	}
	limit := c.MaxBytes
	if limit == 0 {
		limit = DefaultMaxBytes
	}
	elems := c.MaxSliceElements
	switch {
	case elems == 0:
		elems = DefaultMaxSliceElements
	case elems < 0:
		// hamba/avro's own default, restored deliberately: 1<<48, or
		// math.MaxInt on a 32-bit platform, where 1<<48 is not an int.
		elems = min(1<<48, math.MaxInt)
	}
	strMax := c.MaxStringBytes
	if strMax == 0 {
		strMax = DefaultMaxStringBytes
	}
	// The limits go into the frozen API, which is the ONLY way they are
	// applied: hamba/avro's DefaultConfig has no bound worth the name and is
	// never used here.
	api := avro.Config{
		TagKey:            c.TagKey,
		MaxSliceAllocSize: elems,
		MaxByteSliceSize:  strMax,
	}.Freeze()
	ac := &avroCodec{
		api:          api,
		schema:       schema,
		topContainer: schema.Type() == avro.Array || schema.Type() == avro.Map,
		topUnion:     schema.Type() == avro.Union,
		topNullable:  isNullableUnion(schema),
		tagKey:       tagKeyOf(c.TagKey),
		framing:      c.Framing,
		header:       header,
		fingerprint:  hex.EncodeToString(fp),
		limit:        limit,
		walk:         compileWalk(schema),
		elems:        elems,
		strMax:       strMax,
	}
	ac.encoders.New = func() any {
		st := new(encState)
		st.e = api.NewEncoder(schema, &st.w)
		return st
	}
	ac.readers.New = func() any {
		// bufSize 0: Reset binds the reader straight to the body, so the
		// buffer is never used.
		return avro.NewReader(nil, 0, avro.WithReaderConfig(api))
	}
	return ac, nil
}

// tagKeyOf is the struct tag key hamba/avro uses for Config.TagKey.
func tagKeyOf(k string) string {
	if k == "" {
		return "avro"
	}
	return k
}

// Typed returns the codec bound to T, the generic shape.
func Typed[T any](c Config) (codec.Typed[T], error) {
	got, err := New(c)
	if err != nil {
		return nil, err
	}
	return codec.Adapt[T](got), nil
}

// Register installs the codec in r under ContentType.
func Register(r *codec.Registry, c Config) error {
	got, err := New(c)
	if err != nil {
		return err
	}
	return r.Register(got)
}

// avroCodec is immutable after construction; the pools are safe for
// concurrent use.
type avroCodec struct {
	api         avro.API
	schema      avro.Schema
	framing     Framing
	header      []byte // the ten-byte single-object header, precomputed
	fingerprint string
	limit       int
	// topContainer is true when the schema's top level is an array or a map.
	// hamba/avro encodes a *struct but refuses a *slice and a *map --
	// "avro: *[]int64 is unsupported for Avro array" -- while codec.Adapt
	// hands a codec the address of its argument, so Typed[[]int64] would be
	// unusable without this. It is decided from the SCHEMA rather than from
	// the value, because a nullable union of an array legitimately wants the
	// pointer and only the schema knows which case it is.
	topContainer bool
	// topUnion is true when the schema's top level is a union, which is
	// decoded through unionTarget rather than codec.Target (union.go), and
	// topNullable when that union is nullable. hamba/avro's nullable-union
	// encoder takes the non-null branch for any pointer that is not nil, so
	// the pointer codec.Adapt adds to a Typed[[]T] made a nil slice an empty
	// array; it encodes the slice itself as null when it is nil, as it does
	// in a record field.
	topUnion    bool
	topNullable bool
	// tagKey is the struct tag key the library matches fields by,
	// overwriteChecked holds refuseOverwrites' answer for each target type,
	// and heldChecked its answer for each pointer type an interface it
	// decodes into may hold, under the schema there (union.go).
	tagKey           string
	overwriteChecked sync.Map
	heldChecked      sync.Map
	// walk is the schema compiled for the walk Unmarshal runs before the
	// library sees a body, and elems and strMax are the resolved limits it
	// applies: the same values the frozen API was given.
	walk     walkPlan
	elems    int
	strMax   int
	encoders sync.Pool
	readers  sync.Pool
}

// ContentType implements codec.Codec.
func (*avroCodec) ContentType() string { return ContentType }

// SchemaID implements codec.Schemer: the CRC-64-AVRO fingerprint of the
// schema's parsing-canonical form, in hex. A publisher stamps it into
// codec.HeaderSchemaID, so a receiver can detect a schema mismatch from the
// header even when the body is a bare datum.
func (c *avroCodec) SchemaID() string { return c.fingerprint }

// Schema returns the schema text in its parsed, canonical form. It is here so
// that an operator can see what a codec is actually bound to.
func (c *avroCodec) Schema() string { return c.schema.String() }

// Marshal implements codec.Codec.
func (c *avroCodec) Marshal(v any) ([]byte, error) { return c.AppendMarshal(nil, v) }

// AppendMarshal implements codec.Appender by encoding straight into the
// caller's buffer: hamba/avro's Encoder writes to an io.Writer.
//
// the pinned failure contract is honoured by returning the caller's own
// slice header on failure. The encoder writes as it goes, so a value rejected
// half-way through (a union member the schema does not contain, a field of the
// wrong type) leaves bytes past the caller's length; those are garbage the
// caller cannot see and the next append overwrites.
func (c *avroCodec) AppendMarshal(dst []byte, v any) ([]byte, error) {
	st, _ := c.encoders.Get().(*encState)
	st.w.b = dst
	if c.framing == FramingSingleObject {
		st.w.b = append(st.w.b, c.header...)
	}
	st.e.Reset(&st.w)
	err := st.e.Encode(c.encodeValue(v))
	out := st.w.b
	st.w.b = nil
	if err != nil {
		// The encoder is DISCARDED rather than pooled. hamba/avro's Writer
		// keeps its error, and Encoder.Reset does not clear it, so a pooled
		// encoder that has once failed reports that failure for ever --
		// watched as the caller's next good value coming back as
		// `ID: avro: chan int is unsupported for avro record`, with one more
		// `ID:` prefix per subsequent call.
		return dst, err
	}
	c.encoders.Put(st)
	return out, nil
}

// encodeValue resolves what to hand the encoder: the indirection codec.Adapt
// adds, and -- for a schema whose top level is an array or a map -- one more
// level, because hamba/avro refuses a pointer to a slice or a map. Under a
// top-level nullable union a pointer to a slice is followed too, so that a nil
// slice is the null branch (topNullable).
func (c *avroCodec) encodeValue(v any) any {
	out := codec.Indirect(v)
	if !c.topContainer && !c.topNullable {
		return out
	}
	rv := reflect.ValueOf(out)
	for rv.Kind() == reflect.Pointer && !rv.IsNil() {
		switch k := rv.Elem().Kind(); {
		case k == reflect.Slice, c.topContainer && (k == reflect.Map || k == reflect.Array):
			rv = rv.Elem()
		default:
			return out
		}
	}
	if !rv.IsValid() {
		return out
	}
	return rv.Interface()
}

// Unmarshal implements codec.Codec. The size limit is checked and the body is
// walked against the schema before the decoder is handed anything, and the
// framing is verified rather than detected.
//
// The target is checked first, so a target that is the caller's error is
// reported as one (codec.ErrNilTarget, or InvalidArgument for a struct where
// the schema has a union or a big.Rat value where it has a bytes decimal,
// including inside what a pointer held by an interface in the target points
// at, for an interface that leads back to itself, and for a value under a
// string or an enum that the library calls UnmarshalText on and that holds
// nothing, see the package comment) whatever the body holds. A schema whose
// top level is a union decodes into a pointer: Typed[*T] decodes a null to a
// nil *T.
func (c *avroCodec) Unmarshal(data []byte, v any) error {
	var t any
	var err error
	if c.topUnion {
		t, err = unionTarget(v)
	} else {
		t, err = codec.Target(v)
	}
	if err != nil {
		return err
	}
	chk, err := c.refuseOverwrites(t)
	if err != nil {
		return err
	}
	if c.limit >= 0 && len(data) > c.limit {
		return ErrTooLarge
	}
	if c.framing == FramingSingleObject {
		fp, rest, err := soe.ParseHeader(data)
		if err != nil {
			return &codec.DecodeError{ContentType: ContentType, Err: ErrFraming}
		}
		if hex.EncodeToString(fp) != c.fingerprint {
			return &codec.DecodeError{ContentType: ContentType, Err: ErrSchemaMismatch}
		}
		data = rest
	}
	// Under datum framing the body is a datum, whatever its first two bytes
	// are. It used to be refused when they were the single-object marker, and C3 01 is also the zigzag long -98 -- so a 98-entry array encoded by
	// this very codec came back as ErrFraming.
	//
	// The walk refuses what the library's own limits let through (the package
	// comment's three advisories) before the library allocates anything.
	w := datumWalk{plan: &c.walk, body: data, elems: int64(c.elems), strMax: int64(c.strMax), zeroLeft: int64(c.elems)}
	if err := w.datum(); err != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: err}
	}
	// avro.Reader is driven directly rather than through API.Unmarshal,
	// which returns nil when the reader's error is io.EOF -- so a truncated
	// body decodes "successfully" into zero values.
	// A slice the target reuses is set up for the decode, and put right
	// after it, whatever the decode did, when it can need that (union.go).
	needRepair := c.prepare(t, chk)
	r, _ := c.readers.Get().(*avro.Reader)
	r.Reset(data)
	r.ReadVal(c.schema, t)
	if needRepair {
		c.repair(t, chk)
	}
	derr := r.Error
	if derr == nil {
		// One body is one datum. Peek sets the reader's error to io.EOF when
		// nothing is left, so a nil error here means there are bytes over.
		r.Peek()
		if r.Error == nil {
			derr = ErrTrailingBytes
		}
	} else if errors.Is(derr, io.EOF) {
		derr = ErrTruncated
	}
	// The reader is Reset to nil before it is pooled: Reset binds it straight
	// to the caller's body, so a pooled reader still holding that slice would
	// retain a delivery's bytes for as long as the pool does.
	r.Error = nil
	r.Reset(nil)
	c.readers.Put(r)
	if derr != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: derr}
	}
	return nil
}
