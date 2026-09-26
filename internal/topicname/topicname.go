// Package topicname validates portable topic names.
//
// The portable grammar (the message contract) is
//
//	name    = segment *( "/" segment )
//	segment = 1*( ALPHA / DIGIT / "." / "_" / "-" )
//
// with a total length of at most MaxLen bytes. The optional "/" separator
// is the hierarchy marker for backends that have one (Pulsar tenants and
// namespaces, MQTT-style trees); flat backends see it as an ordinary byte
// of the name and map it through their MapTopic function.
//
// Two rules go beyond the one-line grammar and are deliberate:
//
//   - Empty segments ("a//b"), leading and trailing separators are rejected
//     because hierarchical backends disagree on what they denote.
//   - Segments consisting solely of dots ("." and "..") are rejected because
//     several brokers expose topics through path-shaped admin URLs and a
//     dot-only segment is a path-traversal hazard there.
//
// Validation is pure, never panics, never allocates and does one pass over
// the name. Check reports the first problem in left-to-right order (after
// the whole-name checks Empty and TooLong) together with its byte offset,
// so callers can point at the offending byte; Validate reduces that to the
// errcode.ErrInvalidTopic sentinel for hot paths.
package topicname

import (
	"iter"

	"github.com/iqhive/iqmq/internal/errcode"
)

const (
	// MaxLen is the maximum length of a portable topic name in bytes.
	MaxLen = 255
	// Separator is the hierarchy separator.
	Separator = '/'
)

// ProblemKind classifies why a name is invalid.
type ProblemKind uint8

// Problem kinds, in the order Check reports them for a given name.
const (
	// OK means the name is valid.
	OK ProblemKind = iota
	// Empty means the name has no bytes.
	Empty
	// TooLong means the name exceeds MaxLen bytes.
	TooLong
	// InvalidByte means the byte at Pos is outside the segment alphabet.
	InvalidByte
	// LeadingSeparator means the name starts with a separator.
	LeadingSeparator
	// TrailingSeparator means the name ends with a separator.
	TrailingSeparator
	// EmptySegment means two separators are adjacent; Pos is the second.
	EmptySegment
	// DotSegment means the segment starting at Pos consists only of dots.
	DotSegment

	problemKindCount
)

var problemKindNames = [...]string{
	OK:                "OK",
	Empty:             "Empty",
	TooLong:           "TooLong",
	InvalidByte:       "InvalidByte",
	LeadingSeparator:  "LeadingSeparator",
	TrailingSeparator: "TrailingSeparator",
	EmptySegment:      "EmptySegment",
	DotSegment:        "DotSegment",
}

// String returns the name of the kind.
func (k ProblemKind) String() string {
	if k < problemKindCount {
		return problemKindNames[k]
	}
	return "ProblemKind(?)"
}

// Problem is the outcome of Check: a kind and, where meaningful, the byte
// offset it refers to.
type Problem struct {
	// Kind classifies the problem; OK means none.
	Kind ProblemKind
	// Pos is the byte offset the problem refers to: the invalid byte, the
	// second of two adjacent separators, the start of a dot-only segment,
	// the trailing separator, or MaxLen for TooLong. It is 0 otherwise.
	Pos int
}

// OK reports whether the name was valid.
func (p Problem) OK() bool { return p.Kind == OK }

// Err returns nil for a valid name and errcode.ErrInvalidTopic otherwise.
func (p Problem) Err() error {
	if p.Kind == OK {
		return nil
	}
	return errcode.ErrInvalidTopic
}

// segmentByte is the segment alphabet as a lookup table.
var segmentByte = func() (t [256]bool) {
	for c := 'A'; c <= 'Z'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	t['.'], t['_'], t['-'] = true, true, true
	return t
}()

// IsSegmentByte reports whether c may appear inside a segment.
func IsSegmentByte(c byte) bool { return segmentByte[c] }

// check is the shared implementation for string and []byte names.
func check[S ~string | ~[]byte](name S) Problem {
	n := len(name)
	if n == 0 {
		return Problem{Kind: Empty}
	}
	if n > MaxLen {
		return Problem{Kind: TooLong, Pos: MaxLen}
	}
	if name[0] == Separator {
		return Problem{Kind: LeadingSeparator}
	}
	if name[n-1] == Separator {
		return Problem{Kind: TrailingSeparator, Pos: n - 1}
	}
	segStart := 0
	allDots := true
	for i := range n {
		c := name[i]
		switch {
		case c == Separator:
			if i == segStart {
				return Problem{Kind: EmptySegment, Pos: i}
			}
			if allDots {
				return Problem{Kind: DotSegment, Pos: segStart}
			}
			segStart = i + 1
			allDots = true
		case segmentByte[c]:
			if c != '.' {
				allDots = false
			}
		default:
			return Problem{Kind: InvalidByte, Pos: i}
		}
	}
	if allDots {
		return Problem{Kind: DotSegment, Pos: segStart}
	}
	return Problem{}
}

// Check validates name and describes the first problem found.
func Check(name string) Problem { return check(name) }

// CheckBytes is Check for a name held in a byte slice, for decoders that
// validate before converting.
func CheckBytes(name []byte) Problem { return check(name) }

// Validate returns nil when name is a portable topic name and
// errcode.ErrInvalidTopic otherwise. It never allocates.
func Validate(name string) error { return check(name).Err() }

// ValidateBytes is Validate for a name held in a byte slice.
func ValidateBytes(name []byte) error { return check(name).Err() }

// Valid reports whether name is a portable topic name.
func Valid(name string) bool { return check(name).Kind == OK }

// Hierarchical reports whether name contains a separator. It does not
// validate name.
func Hierarchical(name string) bool {
	for i := range len(name) {
		if name[i] == Separator {
			return true
		}
	}
	return false
}

// Depth returns the number of segments of a valid name (one for a flat
// name). For an invalid name it counts separators plus one.
func Depth(name string) int {
	d := 1
	for i := range len(name) {
		if name[i] == Separator {
			d++
		}
	}
	return d
}

// Segments iterates over the segments of name in order without allocating.
// It is intended for valid names: a leading, trailing or doubled separator
// yields empty segments exactly where Check would report a problem, so a
// caller that has validated the name never sees one.
func Segments(name string) iter.Seq[string] {
	return func(yield func(string) bool) {
		start := 0
		for i := range len(name) {
			if name[i] == Separator {
				if !yield(name[start:i]) {
					return
				}
				start = i + 1
			}
		}
		yield(name[start:])
	}
}
