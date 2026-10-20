// Package subject matches hierarchical subjects against wildcard patterns.
//
// A subject is a sequence of non-empty tokens joined by a separator; a
// pattern is a subject in which a token may be one of two wildcards: the
// token "*" (WildcardOne) matches exactly one token, and the token ">"
// (WildcardTail) matches one or more tokens and must be the last token.
//
// These are the NATS wildcards. The separator is configurable through Sep
// because iqmq topic names use "/" as their hierarchy marker while NATS
// subjects use "."; Dot is the default and Slash is the topic-name
// separator. Everything else in a token is literal: a token that mixes a
// wildcard character with other bytes ("a*", "**", ">>") is invalid, a
// token may not be empty, and bytes at or below 0x20 (control characters
// and space) and 0x7F are rejected in both subjects and patterns.
//
// Match reports false for a subject that is not itself valid (empty token,
// wildcard character, control byte, over MaxLen), so a pattern never
// matches something that could not have been published.
//
// Matching is pure, never panics and never allocates. Compile allocates the
// token table once; Sep.Match matches a pattern in one pass without
// compiling. Both agree with each other and with the naive matcher the fuzz
// target keeps as an oracle.
package subject

import (
	"github.com/iqhive/iqmq/internal/errcode"
)

const (
	// MaxLen is the maximum length in bytes of a subject or a pattern.
	MaxLen = 255
	// WildcardOne is the token that matches exactly one token.
	WildcardOne = '*'
	// WildcardTail is the token that matches one or more trailing tokens.
	WildcardTail = '>'
)

// Sentinel errors carried by Compile and the validators.
var (
	// ErrInvalidPattern (InvalidArgument) reports a malformed pattern.
	ErrInvalidPattern = errcode.New(errcode.InvalidArgument, "iqmq: invalid subject pattern")
	// ErrInvalidSubject (InvalidArgument) reports a malformed literal subject.
	ErrInvalidSubject = errcode.New(errcode.InvalidArgument, "iqmq: invalid subject")
)

// Sep is the token separator of a subject grammar. Its methods are the
// grammar's operations; the package-level functions use Dot.
type Sep byte

// The two separators iqmq uses.
const (
	// Dot is the NATS separator and the default.
	Dot Sep = '.'
	// Slash is the topic-name hierarchy separator.
	Slash Sep = '/'
)

// valid reports whether s is a usable separator: a printable ASCII byte
// that is not a wildcard.
func (s Sep) valid() bool {
	c := byte(s)
	return c > 0x20 && c < 0x7f && c != WildcardOne && c != WildcardTail
}

// tokenByte reports whether c may appear inside a literal token.
func tokenByte(c, sep byte) bool {
	return c > 0x20 && c != 0x7f && c != WildcardOne && c != WildcardTail && c != sep
}

// checkSubject reports whether subj is a valid literal subject: 1..MaxLen
// bytes of non-empty tokens made of token bytes.
func checkSubject[S ~string | ~[]byte](subj S, sep byte) bool {
	n := len(subj)
	if n == 0 || n > MaxLen {
		return false
	}
	return validTokens(subj, sep)
}

// validTokens reports whether s is one or more non-empty tokens of token
// bytes. It is the tail-wildcard acceptance test and does not check MaxLen.
func validTokens[S ~string | ~[]byte](s S, sep byte) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	start := 0
	for i := range n {
		c := s[i]
		if c == sep {
			if i == start {
				return false
			}
			start = i + 1
			continue
		}
		if !tokenByte(c, sep) {
			return false
		}
	}
	return start < n
}

// checkPattern reports whether pat is a valid pattern for sep.
func checkPattern[S ~string | ~[]byte](pat S, sep byte) bool {
	n := len(pat)
	if n == 0 || n > MaxLen {
		return false
	}
	start := 0
	for i := 0; i <= n; i++ {
		if i < n && pat[i] != sep {
			continue
		}
		l := i - start
		switch {
		case l == 0:
			return false
		case l == 1 && pat[start] == WildcardOne:
			// one-token wildcard
		case l == 1 && pat[start] == WildcardTail:
			if i != n {
				return false
			}
		default:
			for j := start; j < i; j++ {
				if !tokenByte(pat[j], sep) {
					return false
				}
			}
		}
		start = i + 1
	}
	return true
}

// equalTok compares a literal token with a subject slice byte by byte.
func equalTok[P, S ~string | ~[]byte](lit P, s S) bool {
	if len(lit) != len(s) {
		return false
	}
	for j := range len(lit) {
		if lit[j] != s[j] {
			return false
		}
	}
	return true
}

// matchRaw matches a validated pattern against subj in one pass.
func matchRaw[P, S ~string | ~[]byte](pat P, subj S, sep byte) bool {
	pn, sn := len(pat), len(subj)
	if sn == 0 || sn > MaxLen {
		return false
	}
	pi, si := 0, 0
	for {
		pe := pi
		for pe < pn && pat[pe] != sep {
			pe++
		}
		if pe-pi == 1 && pat[pi] == WildcardTail {
			return validTokens(subj[si:], sep)
		}
		if si >= sn {
			return false
		}
		se := si
		for se < sn && subj[se] != sep {
			if !tokenByte(subj[se], sep) {
				return false
			}
			se++
		}
		if se == si {
			return false
		}
		if (pe-pi != 1 || pat[pi] != WildcardOne) && !equalTok(pat[pi:pe], subj[si:se]) {
			return false
		}
		patEnd, subEnd := pe == pn, se == sn
		if patEnd || subEnd {
			return patEnd && subEnd
		}
		pi, si = pe+1, se+1
		if si == sn {
			return false
		}
	}
}

// Validate returns nil when pattern is a valid pattern for s and
// ErrInvalidPattern otherwise. It never allocates.
func (s Sep) Validate(pattern string) error {
	if !s.valid() || !checkPattern(pattern, byte(s)) {
		return ErrInvalidPattern
	}
	return nil
}

// ValidateSubject returns nil when subject is a valid literal subject for s
// (no wildcards) and ErrInvalidSubject otherwise. It never allocates.
func (s Sep) ValidateSubject(subject string) error {
	if !s.valid() || !checkSubject(subject, byte(s)) {
		return ErrInvalidSubject
	}
	return nil
}

// ValidateSubjectBytes is ValidateSubject for a subject held in a byte slice.
func (s Sep) ValidateSubjectBytes(subject []byte) error {
	if !s.valid() || !checkSubject(subject, byte(s)) {
		return ErrInvalidSubject
	}
	return nil
}

// Match reports whether pattern matches subject without compiling. It
// returns false when pattern is invalid. It never allocates.
func (s Sep) Match(pattern, subject string) bool {
	return s.valid() && checkPattern(pattern, byte(s)) && matchRaw(pattern, subject, byte(s))
}

// MatchBytes is Match for a subject held in a byte slice.
func (s Sep) MatchBytes(pattern string, subject []byte) bool {
	return s.valid() && checkPattern(pattern, byte(s)) && matchRaw(pattern, subject, byte(s))
}

// tokKind classifies a compiled pattern token.
type tokKind uint8

const (
	tokLiteral tokKind = iota
	tokOne
	tokTail
)

// Pattern is a compiled pattern. It is immutable and safe for concurrent
// use.
type Pattern struct {
	raw     string
	toks    []string
	kinds   []tokKind
	sep     Sep
	literal bool
	tail    bool
}

// Compile parses pattern for s. The returned Pattern shares pattern's
// storage and allocates its token table once.
func (s Sep) Compile(pattern string) (*Pattern, error) {
	if !s.valid() || !checkPattern(pattern, byte(s)) {
		return nil, ErrInvalidPattern
	}
	n := 1
	for i := range len(pattern) {
		if pattern[i] == byte(s) {
			n++
		}
	}
	p := &Pattern{
		raw:     pattern,
		toks:    make([]string, 0, n),
		kinds:   make([]tokKind, 0, n),
		sep:     s,
		literal: true,
	}
	start := 0
	for i := 0; i <= len(pattern); i++ {
		if i < len(pattern) && pattern[i] != byte(s) {
			continue
		}
		tok := pattern[start:i]
		kind := tokLiteral
		switch {
		case len(tok) == 1 && tok[0] == WildcardOne:
			kind, p.literal = tokOne, false
		case len(tok) == 1 && tok[0] == WildcardTail:
			kind, p.literal, p.tail = tokTail, false, true
		}
		p.toks = append(p.toks, tok)
		p.kinds = append(p.kinds, kind)
		start = i + 1
	}
	return p, nil
}

// match walks the compiled tokens against subj.
func match[S ~string | ~[]byte](p *Pattern, subj S) bool {
	n := len(subj)
	if n == 0 || n > MaxLen {
		return false
	}
	sep := byte(p.sep)
	pos := 0
	last := len(p.kinds) - 1
	for i, kind := range p.kinds {
		if kind == tokTail {
			return validTokens(subj[pos:], sep)
		}
		if pos >= n {
			return false
		}
		end := pos
		for end < n && subj[end] != sep {
			if !tokenByte(subj[end], sep) {
				return false
			}
			end++
		}
		if end == pos {
			return false
		}
		if kind == tokLiteral && !equalTok(p.toks[i], subj[pos:end]) {
			return false
		}
		if end == n {
			return i == last
		}
		pos = end + 1
		if pos == n {
			return false
		}
	}
	return false
}

// Match reports whether p matches subject. It never allocates.
func (p *Pattern) Match(subject string) bool { return match(p, subject) }

// MatchBytes is Match for a subject held in a byte slice.
func (p *Pattern) MatchBytes(subject []byte) bool { return match(p, subject) }

// String returns the pattern text.
func (p *Pattern) String() string { return p.raw }

// Sep returns the separator the pattern was compiled for.
func (p *Pattern) Sep() Sep { return p.sep }

// Literal reports whether the pattern has no wildcard: it then matches
// exactly one subject, its own text.
func (p *Pattern) Literal() bool { return p.literal }

// Tail reports whether the pattern ends in the tail wildcard.
func (p *Pattern) Tail() bool { return p.tail }

// NumTokens returns the number of tokens in the pattern.
func (p *Pattern) NumTokens() int { return len(p.toks) }

// Token returns the i'th token: the literal text, "*" or ">".
func (p *Pattern) Token(i int) string { return p.toks[i] }

// Validate is Dot.Validate.
func Validate(pattern string) error { return Dot.Validate(pattern) }

// ValidateSubject is Dot.ValidateSubject.
func ValidateSubject(subject string) error { return Dot.ValidateSubject(subject) }

// Compile is Dot.Compile.
func Compile(pattern string) (*Pattern, error) { return Dot.Compile(pattern) }

// Match is Dot.Match.
func Match(pattern, subject string) bool { return Dot.Match(pattern, subject) }
