// Package id holds the message-identifier generators the opt-in `AutoID` sources a publisher can use to
// stamp every message with a unique, time-ordered identifier.
//
// All four kinds share one shape: an identifier is a fixed number of
// big-endian bytes whose leading bits are a timestamp, so the binary form
// of two identifiers from the same generator compares in creation order,
// and every kind is strictly monotonic per generator — a sequence counter
// separates identifiers created within one clock tick, and a counter that
// overflows borrows from the next tick rather than repeating or going
// backwards. Generators are safe for concurrent use and allocate nothing
// per identifier when the caller appends into its own buffer.
//
// Time and randomness are injected (Config.Clock, Config.Seed), so a test
// can reproduce a generator exactly; the default seed comes from
// crypto/rand and the default clock from the wall clock.
package id

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Kind identifies an identifier format. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Snowflake is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// UUIDv7 is the RFC 9562 version 7 UUID: 48-bit Unix millisecond
	// timestamp, a 12-bit sequence in rand_a and 62 random bits, rendered
	// as the canonical 36-character lower-case hyphenated form.
	UUIDv7
	// ULID is a 128-bit identifier: 48-bit Unix millisecond timestamp,
	// 16-bit sequence and 64 random bits, rendered as 26 characters of
	// upper-case Crockford base32.
	ULID
	// XID is a 96-bit identifier in the layout of rs/xid: 32-bit Unix
	// seconds, a 5-byte generator identity and a 24-bit counter, rendered
	// as 20 characters of lower-case base32hex.
	XID
	// Snowflake is a 64-bit identifier: 41-bit millisecond timestamp since
	// Epoch, a 10-bit node number and a 12-bit sequence, rendered as
	// decimal digits. It carries no randomness, so two generators that may
	// run at the same time must be given different Config.Node numbers or
	// they will produce the same identifiers. It is the reference.
	Snowflake

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "uuidv7", "ulid", "xid", "snowflake"}

var kindIdents = [kindCount]string{"Auto", "UUIDv7", "ULID", "XID", "Snowflake"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := UUIDv7; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Size returns the number of bytes in the binary form.
func (k Kind) Size() int {
	switch k.Resolve() {
	case UUIDv7, ULID:
		return 16
	case XID:
		return 12
	case Snowflake:
		return 8
	default:
		return 0
	}
}

// TextLen returns the exact length of the text form, or 0 for a kind whose
// text form is variable-length (Snowflake's decimal digits).
func (k Kind) TextLen() int {
	switch k.Resolve() {
	case UUIDv7:
		return 36
	case ULID:
		return 26
	case XID:
		return 20
	default:
		return 0
	}
}

// MaxTextLen returns the longest text form the kind can produce.
func (k Kind) MaxTextLen() int {
	if n := k.TextLen(); n != 0 {
		return n
	}
	return 19 // math.MaxInt64 has 19 digits
}

// Sortable reports whether the text form sorts chronologically under plain
// byte comparison. Snowflake's decimal rendering does not (a shorter
// number sorts before a longer one only by luck), though its binary form
// and its numeric value do.
func (k Kind) Sortable() bool { return k.Resolve() != Snowflake }

// Resolution reports the smallest time difference the timestamp field can
// distinguish.
func (k Kind) Resolution() time.Duration {
	if k.Resolve() == XID {
		return time.Second
	}
	return time.Millisecond
}

// Epoch is the Snowflake epoch: identifiers count milliseconds from here,
// which keeps the 41-bit field valid until 2089.
var Epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// ErrInvalidID (InvalidArgument) is returned by Parse for text that is not
// the canonical form of the requested kind.
var ErrInvalidID = errcode.New(errcode.InvalidArgument, "id: invalid identifier")

// Generator produces identifiers of one kind. Implementations are safe for
// concurrent use and never allocate in Append.
type Generator interface {
	// Kind returns the concrete kind (never Auto).
	Kind() Kind
	// Append appends the text form of a new identifier to dst.
	Append(dst []byte) []byte
	// Next returns the text form of a new identifier.
	Next() string
	// NextValue returns a new identifier in binary form.
	NextValue() Value
}

// Config configures a generator. The zero Config is valid: the wall clock,
// a seed from crypto/rand and node 0.
type Config struct {
	// Clock is the time source; nil means clock.Real.
	Clock clock.Clock
	// Seed seeds the generator's random stream and its identity bytes.
	// Zero means a seed drawn from crypto/rand, which is what production
	// uses; a non-zero seed makes the generator reproducible.
	Seed uint64
	// Node is the node number of a Snowflake generator (0…1023, higher
	// bits ignored) and part of the identity bytes of an XID generator.
	// Two generators that may run at the same time need different nodes.
	Node uint16
}

// New returns a generator of kind k with the default configuration.
func New(k Kind) (Generator, error) { return NewWith(k, Config{}) }

// NewWith returns a generator of kind k, or fails on an invalid kind.
func NewWith(k Kind, cfg Config) (Generator, error) {
	k = k.Resolve()
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Seed == 0 {
		cfg.Seed = randomSeed()
	}
	switch k {
	case UUIDv7:
		g := &uuidGen{}
		g.init(cfg)
		return g, nil
	case ULID:
		g := &ulidGen{}
		g.init(cfg)
		return g, nil
	case XID:
		g := &xidGen{}
		g.init(cfg)
		g.identity = identityBytes(cfg.Seed, cfg.Node)
		return g, nil
	case Snowflake:
		g := &snowflakeGen{}
		g.init(cfg)
		return g, nil
	default:
		return nil, errors.New("id: invalid kind")
	}
}

// randomSeed draws a seed from crypto/rand. A failure is impossible on the
// platforms Go supports (crypto/rand panics itself on a broken source), so
// the value is used directly.
func randomSeed() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	s := binary.LittleEndian.Uint64(b[:])
	if s == 0 {
		s = 0x9E3779B97F4A7C15
	}
	return s
}

// identityBytes derives the five machine-and-process bytes of an XID from
// the seed and the node number. Unlike rs/xid, iqmq never puts the host
// name or the process id into an identifier: an identifier that travels
// with a message must not describe the machine that produced it.
func identityBytes(seed uint64, node uint16) [5]byte {
	h := mix(seed ^ uint64(node)<<48)
	return [5]byte{byte(h), byte(h >> 8), byte(h >> 16), byte(node), byte(node >> 8)}
}

// splitRng is a lock-free random stream: a shared additive counter through
// a bijective mixer. Two calls never return the same value within a
// generator's 2^64 identifiers, which is what makes the collision bound of
// the random-tailed kinds exact rather than probabilistic.
type splitRng struct {
	_     [64]byte
	state atomic.Uint64
	_     [64]byte
}

func (r *splitRng) next() uint64 { return mix(r.state.Add(0x9E3779B97F4A7C15)) }

// mix is the SplitMix64 finaliser.
func mix(z uint64) uint64 {
	z ^= z >> 30
	z *= 0xBF58476D1CE4E5B9
	z ^= z >> 27
	z *= 0x94D049BB133111EB
	return z ^ z>>31
}

// base is the state every generator shares.
type base struct {
	clk  clock.Clock
	rng  splitRng
	node uint16
	// tick packs the last used timestamp and sequence into one word so a
	// single CAS keeps the pair monotonic under concurrency.
	tick atomic.Uint64
}

// init installs the configuration in a freshly allocated generator.
func (b *base) init(cfg Config) {
	b.clk = cfg.Clock
	b.node = cfg.Node
	b.rng.state.Store(cfg.Seed)
}

// advance returns the next (unit, seq) pair for a timestamp in units of
// the kind's resolution and a sequence of seqBits bits. The pair is
// strictly greater than the previous one: a sequence that overflows
// borrows the next unit, so identifiers never repeat and never go
// backwards, even when the clock does.
func (b *base) advance(now uint64, seqBits uint) (uint64, uint64) {
	mask := uint64(1)<<seqBits - 1
	for {
		old := b.tick.Load()
		oldUnit, oldSeq := old>>seqBits, old&mask
		unit, seq := now, uint64(0)
		if now <= oldUnit {
			unit, seq = oldUnit, oldSeq+1
			if seq > mask {
				unit, seq = oldUnit+1, 0
			}
		}
		if b.tick.CompareAndSwap(old, unit<<seqBits|seq) {
			return unit, seq
		}
	}
}

// Value is the binary form of an identifier.
type Value struct {
	kind Kind
	b    [16]byte
}

// MakeValue builds a Value of kind k from its big-endian bytes, or fails
// when len(b) is not k.Size().
func MakeValue(k Kind, b []byte) (Value, error) {
	k = k.Resolve()
	if len(b) != k.Size() {
		return Value{}, errors.New("id: value of " + strconv.Itoa(len(b)) + " bytes, want " + strconv.Itoa(k.Size()))
	}
	var v Value
	v.kind = k
	copy(v.b[:], b)
	return v, nil
}

// Kind returns the value's kind. The zero Value has kind Auto and no
// bytes; it renders as the empty string and is never produced by a
// generator.
func (v Value) Kind() Kind { return v.kind }

// IsZero reports whether v is the zero Value.
func (v Value) IsZero() bool { return v == Value{} }

// size is the value's byte count; the zero Value has none (its kind is
// Auto, which as a Kind resolves to the bound default but as a Value's
// kind means "no identifier").
func (v Value) size() int {
	if v.kind == Auto {
		return 0
	}
	return v.kind.Size()
}

// Raw returns the big-endian bytes of the identifier.
func (v Value) Raw() []byte { return v.b[:v.size()] }

// AppendBinary appends the big-endian bytes to dst.
func (v Value) AppendBinary(dst []byte) []byte { return append(dst, v.b[:v.size()]...) }

// Compare orders two values of the same kind by creation time, then by
// sequence and random tail: it is a plain comparison of the binary form.
// Values of different kinds are ordered by kind so that a mixed slice has
// a total order.
func (v Value) Compare(o Value) int {
	if v.kind != o.kind {
		if v.kind < o.kind {
			return -1
		}
		return 1
	}
	for i := range v.size() {
		if v.b[i] != o.b[i] {
			if v.b[i] < o.b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Time returns the instant encoded in the identifier, truncated to the
// kind's resolution. The zero Value returns the zero time.
func (v Value) Time() time.Time {
	switch v.kind {
	case UUIDv7, ULID:
		ms := uint64(v.b[0])<<40 | uint64(v.b[1])<<32 | uint64(v.b[2])<<24 |
			uint64(v.b[3])<<16 | uint64(v.b[4])<<8 | uint64(v.b[5])
		return time.UnixMilli(int64(ms)).UTC()
	case XID:
		return time.Unix(int64(binary.BigEndian.Uint32(v.b[:4])), 0).UTC()
	case Snowflake:
		ms := binary.BigEndian.Uint64(v.b[:8]) >> 22
		return Epoch.Add(time.Duration(ms) * time.Millisecond).UTC()
	default:
		return time.Time{}
	}
}

// Sequence returns the per-tick sequence number encoded in the identifier
// (the 24-bit counter for XID).
func (v Value) Sequence() uint64 {
	switch v.kind {
	case UUIDv7:
		return uint64(v.b[6]&0x0f)<<8 | uint64(v.b[7])
	case ULID:
		return uint64(v.b[6])<<8 | uint64(v.b[7])
	case XID:
		return uint64(v.b[9])<<16 | uint64(v.b[10])<<8 | uint64(v.b[11])
	case Snowflake:
		return binary.BigEndian.Uint64(v.b[:8]) & 0xfff
	default:
		return 0
	}
}

// Node returns the node number encoded in a Snowflake identifier and 0 for
// every other kind.
func (v Value) Node() uint64 {
	if v.kind == Snowflake {
		return binary.BigEndian.Uint64(v.b[:8]) >> 12 & 0x3ff
	}
	return 0
}

// AppendText appends the canonical text form of v to dst.
func (v Value) AppendText(dst []byte) []byte {
	switch v.kind {
	case UUIDv7:
		return appendUUID(dst, &v.b)
	case ULID:
		return appendULID(dst, &v.b)
	case XID:
		return appendXID(dst, &v.b)
	case Snowflake:
		return strconv.AppendUint(dst, binary.BigEndian.Uint64(v.b[:8]), 10)
	default:
		return dst
	}
}

// String returns the canonical text form.
func (v Value) String() string {
	var buf [36]byte
	return string(v.AppendText(buf[:0]))
}

// text is the type set of the parse entry points: one generic body serves
// strings and byte slices, so neither caller pays a conversion.
type text interface{ ~string | ~[]byte }

// Parse decodes the canonical text form of kind k. Anything else — wrong
// length, wrong case, a character outside the alphabet, a value that does
// not fit the field widths, or a non-canonical rendering such as a leading
// zero — is rejected with ErrInvalidID. Parse never allocates.
func Parse(k Kind, s string) (Value, error) { return parse(k, s) }

// ParseBytes is Parse over a byte slice.
func ParseBytes(k Kind, b []byte) (Value, error) { return parse(k, b) }

func parse[T text](k Kind, s T) (Value, error) {
	k = k.Resolve()
	var v Value
	v.kind = k
	var ok bool
	switch k {
	case UUIDv7:
		ok = parseUUID(s, &v.b)
	case ULID:
		ok = parseULID(s, &v.b)
	case XID:
		ok = parseXID(s, &v.b)
	case Snowflake:
		ok = parseSnowflake(s, &v.b)
	default: // an invalid kind parses nothing
	}
	if !ok {
		return Value{}, ErrInvalidID
	}
	return v, nil
}
