package streams

import (
	"strconv"
	"strings"
)

// ID is a stream entry identifier: a millisecond timestamp and a sequence
// number within that millisecond.
//
// It is a struct of two uint64s rather than the "1700000000000-0" string
// Redis prints, because the driver compares identifiers on the hot path —
// which pending entry is the one we were handed, which is the high-water mark
// of a shard — and comparing decimal strings gets the answer wrong as soon as
// the millisecond count changes width. "9-0" sorts after "10-0" as a string
// and before it as an identifier.
type ID struct {
	// Ms is the entry's millisecond timestamp.
	Ms uint64
	// Seq is the sequence within that millisecond.
	Seq uint64
}

// The identifiers Redis spells rather than numbers.
const (
	// IDAuto is XADD's "*": the server assigns the identifier.
	IDAuto = "*"
	// IDNew is XREADGROUP's ">": deliver entries never delivered to this
	// group, rather than this consumer's pending ones.
	IDNew = ">"
	// IDMin is a range's open lower bound.
	IDMin = "-"
	// IDMax is a range's open upper bound.
	IDMax = "+"
	// IDZero is the beginning of a stream, which XGROUP CREATE takes to mean
	// "replay everything".
	IDZero = "0-0"
	// IDLast is XGROUP CREATE's "$": start at the entries added from now on.
	IDLast = "$"
)

// ParseID parses Redis's "<ms>-<seq>" spelling.
//
// A bare "<ms>" is accepted and means sequence 0, which is what Redis accepts
// in a range bound; an empty string, a missing separator, a sign, or a
// component that is not decimal is refused. It is strict on purpose: an
// identifier is what a settlement names, so an identifier this driver
// mis-parsed would settle the wrong entry or none.
func ParseID(s string) (ID, error) {
	if s == "" {
		return ID{}, ErrBadID
	}
	msPart, seqPart := s, ""
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		msPart, seqPart = s[:dash], s[dash+1:]
		if seqPart == "" {
			return ID{}, ErrBadID
		}
	}
	ms, err := strconv.ParseUint(msPart, 10, 64)
	if err != nil {
		return ID{}, ErrBadID
	}
	var seq uint64
	if seqPart != "" {
		if seq, err = strconv.ParseUint(seqPart, 10, 64); err != nil {
			return ID{}, ErrBadID
		}
	}
	return ID{Ms: ms, Seq: seq}, nil
}

// ValidID reports whether s is what [ParseID] accepts, without building the
// [ID].
//
// It exists for the callers that discard the identifier -- a publisher
// checking only that XADD's reply IS an identifier -- and for them alone: the
// batch publish path checked a hundred replies per pipeline through ParseID
// and threw every one of them away, which is two strconv.ParseUint calls per
// message spent on a number nobody reads.
//
// The two functions must agree on EVERY string. A reply ParseID refuses and
// this accepts is a corrupt reply reported as a successful publication, so the
// agreement is held by FuzzCheckXAddMatchesParseXAdd and
// FuzzValidIDOnDecimalStrings running them against each other rather than by a
// table of cases somebody has to remember to extend.
//
// The structure below is deliberately ParseID's -- same split on the first
// separator, same refusal of an empty sequence, same per-part test -- so that a
// reader can see the correspondence and an editor of one is looking at the
// other. It differs in one way only, and lint asked for it: the sequence half
// is scoped to the branch that has one, where ParseID needs it afterwards to
// build the number.
func ValidID(s string) bool {
	if s == "" {
		return false
	}
	msPart := s
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		seqPart := s[dash+1:]
		msPart = s[:dash]
		if seqPart == "" || !validUint64(seqPart) {
			return false
		}
	}
	return validUint64(msPart)
}

// maxUint64Dec is 1<<64 - 1 in decimal: the bound [validUint64] compares
// against. strconv.ParseUint refuses what overflows a uint64, so a validator
// that only checked for digits would accept a thirty-digit reply ParseID calls
// bad -- and the whole claim being made is that the two agree.
const maxUint64Dec = "18446744073709551615"

// validUint64 reports whether s is what strconv.ParseUint(s, 10, 64) accepts:
// a non-empty run of decimal digits whose value fits in a uint64.
//
// Leading zeros are skipped FIRST because ParseUint accepts them -- "007" is 7
// and "00000000000000000000001" is 1 -- so the length comparison that catches
// overflow has to be against the significant digits and not against len(s).
// Underscore separators, a sign and a base prefix are all refused by the digit
// test, which is also what ParseUint does at base 10.
func validUint64(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	for i < len(s) && s[i] == '0' {
		i++
	}
	d := s[i:]
	for j := range len(d) {
		if d[j] < '0' || d[j] > '9' {
			return false
		}
	}
	if len(d) != len(maxUint64Dec) {
		return len(d) < len(maxUint64Dec)
	}
	// Same number of significant digits: the decimal spellings compare
	// lexically exactly as the numbers do.
	return d <= maxUint64Dec
}

// String returns Redis's spelling, "<ms>-<seq>".
func (id ID) String() string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// Append appends Redis's spelling to dst, for a caller building a command
// without allocating a string per identifier.
func (id ID) Append(dst []byte) []byte {
	dst = strconv.AppendUint(dst, id.Ms, 10)
	dst = append(dst, '-')
	return strconv.AppendUint(dst, id.Seq, 10)
}

// IsZero reports whether id is the beginning of a stream.
func (id ID) IsZero() bool { return id.Ms == 0 && id.Seq == 0 }

// Compare orders two identifiers: -1, 0 or 1.
func (id ID) Compare(other ID) int {
	switch {
	case id.Ms < other.Ms:
		return -1
	case id.Ms > other.Ms:
		return 1
	case id.Seq < other.Seq:
		return -1
	case id.Seq > other.Seq:
		return 1
	}
	return 0
}

// Before reports whether id sorts before other.
func (id ID) Before(other ID) bool { return id.Compare(other) < 0 }

// Next returns the smallest identifier greater than id, which is what an
// exclusive range bound is written as on a server that has no exclusive
// bounds.
func (id ID) Next() ID {
	if id.Seq == ^uint64(0) {
		return ID{Ms: id.Ms + 1}
	}
	return ID{Ms: id.Ms, Seq: id.Seq + 1}
}

// SeqBits is how many low bits of a packed sequence hold the within-
// millisecond sequence.
//
// A stream identifier is two 64-bit numbers and the API's
// DeliveryInfo.Sequence is one, so something has to give. Twenty bits is what
// gives least: a millisecond timestamp needs 41 bits until the year 71,000,
// which leaves 23 to spare, and 2^20 is 1,048,575 entries in a single
// millisecond — a billion publications a second. Above that the sequence
// SATURATES rather than wrapping, so a packed value stays monotone and a
// resume from it starts slightly early (a redelivery) rather than slightly
// late (a loss).
const SeqBits = 20

// Pack folds an identifier into one monotone unsigned integer, which is what
// DeliveryInfo.Sequence carries and what StartAtSequence takes back.
//
// Monotone is the property that matters: a caller comparing two deliveries'
// sequences has to get the publication order, and a packing that put the
// sequence in the high bits would not. Exactness is the second: the
// millisecond half alone would make a resume replay up to a millisecond's
// worth of entries, which on a busy stream is thousands of duplicates.
func (id ID) Pack() uint64 {
	seq := id.Seq
	if seq >= 1<<SeqBits {
		seq = 1<<SeqBits - 1
	}
	return id.Ms<<SeqBits | seq
}

// Unpack reverses [ID.Pack].
func Unpack(v uint64) ID {
	return ID{Ms: v >> SeqBits, Seq: v & (1<<SeqBits - 1)}
}

// FromMillis returns the first identifier of a millisecond, which is how a
// start-at-time position is expressed: Redis has no "seek by time", and a
// stream identifier's high half IS the publication time in milliseconds, so
// "the first entry at or after time t" is the range bound "<t-ms>-0".
func FromMillis(ms int64) ID {
	if ms < 0 {
		return ID{}
	}
	return ID{Ms: uint64(ms)}
}

// ErrBadID is a stream identifier this driver cannot parse.
var ErrBadID = newErr("iqmq/streams: the stream entry identifier is not <ms>-<seq>")
