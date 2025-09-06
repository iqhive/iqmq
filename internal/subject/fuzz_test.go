package subject

import (
	"errors"
	"strings"
	"testing"
)

// Naive oracle: split on the separator and match token lists recursively.

func oracleTokens(s string, sep byte) ([]string, bool) {
	if len(s) == 0 || len(s) > MaxLen {
		return nil, false
	}
	toks := strings.Split(s, string(sep))
	for _, tok := range toks {
		if tok == "" {
			return nil, false
		}
		for i := range len(tok) {
			if c := tok[i]; c <= 0x20 || c == 0x7f {
				return nil, false
			}
		}
	}
	return toks, true
}

func oracleValidSubject(s string, sep byte) bool {
	toks, ok := oracleTokens(s, sep)
	if !ok {
		return false
	}
	for _, tok := range toks {
		if strings.ContainsAny(tok, "*>") {
			return false
		}
	}
	return true
}

func oracleValidPattern(p string, sep byte) bool {
	toks, ok := oracleTokens(p, sep)
	if !ok {
		return false
	}
	for i, tok := range toks {
		switch tok {
		case "*":
		case ">":
			if i != len(toks)-1 {
				return false
			}
		default:
			if strings.ContainsAny(tok, "*>") {
				return false
			}
		}
	}
	return true
}

func oracleMatch(pt, st []string) bool {
	if len(pt) == 0 {
		return len(st) == 0
	}
	if pt[0] == ">" {
		return len(st) >= 1
	}
	if len(st) == 0 {
		return false
	}
	if pt[0] == "*" || pt[0] == st[0] {
		return oracleMatch(pt[1:], st[1:])
	}
	return false
}

var fuzzSeps = []Sep{Dot, Slash}

// FuzzSubjectMatch compares the compiled matcher, the one-shot matcher and
// the byte entry points against the naive oracle for both separators, and
// checks the validators agree with the oracle grammar.
func FuzzSubjectMatch(f *testing.F) {
	seeds := []struct {
		pattern, subject string
		sep              byte
	}{
		{"orders.*", "orders.created", 0},
		{"orders.>", "orders.eu.created", 0},
		{"*.*.*", "a.b.c", 0},
		{">", "a", 0},
		{"a.b", "a.b", 0},
		{"a.*.c", "a.b.c", 0},
		{"a.>", "a", 0},
		{"orders/*/v1.2/>", "orders/eu/v1.2/created", 1},
		{"a.b/*", "a.b/c", 1},
		{"a..b", "a.b", 0},
		{"a*", "ab", 0},
		{">.a", "b.a", 0},
		{"*", "*", 0},
		{"", "", 0},
		{"a.*", "a.", 0},
		{"é.*", "é.ü", 0},
		{strings.Repeat("a", MaxLen), strings.Repeat("a", MaxLen), 0},
		{"*", strings.Repeat("a", MaxLen+1), 0},
	}
	for _, s := range seeds {
		f.Add(s.pattern, s.subject, s.sep)
	}
	f.Fuzz(func(t *testing.T, pattern, subject string, sepIdx byte) {
		sep := fuzzSeps[int(sepIdx)%len(fuzzSeps)]
		sb := byte(sep)

		// Validators agree with the oracle grammar.
		wantPattern := oracleValidPattern(pattern, sb)
		if err := sep.Validate(pattern); (err == nil) != wantPattern {
			t.Fatalf("Validate(%q) = %v, oracle %v", pattern, err, wantPattern)
		} else if err != nil && !errors.Is(err, ErrInvalidPattern) {
			t.Fatalf("Validate(%q) = %v: wrong sentinel", pattern, err)
		}
		wantSubject := oracleValidSubject(subject, sb)
		if err := sep.ValidateSubject(subject); (err == nil) != wantSubject {
			t.Fatalf("ValidateSubject(%q) = %v, oracle %v", subject, err, wantSubject)
		} else if err != nil && !errors.Is(err, ErrInvalidSubject) {
			t.Fatalf("ValidateSubject(%q) = %v: wrong sentinel", subject, err)
		}
		if err := sep.ValidateSubjectBytes([]byte(subject)); (err == nil) != wantSubject {
			t.Fatalf("ValidateSubjectBytes(%q) = %v, oracle %v", subject, err, wantSubject)
		}

		p, err := sep.Compile(pattern)
		if (err == nil) != wantPattern {
			t.Fatalf("Compile(%q) = %v, oracle valid=%v", pattern, err, wantPattern)
		}
		if !wantPattern {
			if p != nil || sep.Match(pattern, subject) || sep.MatchBytes(pattern, []byte(subject)) {
				t.Fatalf("invalid pattern %q produced %v / matched", pattern, p)
			}
			return
		}

		// Every entry point agrees with the oracle.
		pt, _ := oracleTokens(pattern, sb)
		want := false
		if wantSubject {
			st, _ := oracleTokens(subject, sb)
			want = oracleMatch(pt, st)
		}
		if got := p.Match(subject); got != want {
			t.Fatalf("Pattern(%q).Match(%q) = %v, oracle %v", pattern, subject, got, want)
		}
		if got := p.MatchBytes([]byte(subject)); got != want {
			t.Fatalf("Pattern(%q).MatchBytes(%q) = %v, oracle %v", pattern, subject, got, want)
		}
		if got := sep.Match(pattern, subject); got != want {
			t.Fatalf("Sep.Match(%q, %q) = %v, oracle %v", pattern, subject, got, want)
		}
		if got := sep.MatchBytes(pattern, []byte(subject)); got != want {
			t.Fatalf("Sep.MatchBytes(%q, %q) = %v, oracle %v", pattern, subject, got, want)
		}

		// Accessors describe the token list.
		if p.String() != pattern || p.Sep() != sep || p.NumTokens() != len(pt) {
			t.Fatalf("accessors of %q: %q %v %d", pattern, p.String(), p.Sep(), p.NumTokens())
		}
		literal := true
		for i, tok := range pt {
			if p.Token(i) != tok {
				t.Fatalf("Token(%d) = %q, want %q", i, p.Token(i), tok)
			}
			if tok == "*" || tok == ">" {
				literal = false
			}
		}
		if p.Literal() != literal || p.Tail() != (pt[len(pt)-1] == ">") {
			t.Fatalf("Literal=%v Tail=%v for %q", p.Literal(), p.Tail(), pattern)
		}
		if literal && want != (subject == pattern) {
			t.Fatalf("literal %q matched %q = %v", pattern, subject, want)
		}
		if literal && sep.ValidateSubject(pattern) != nil {
			t.Fatalf("literal pattern %q is not a valid subject", pattern)
		}

		// Generalising a literal token to * keeps a match; a matching
		// subject also matches the tail wildcard of any prefix.
		if want {
			for i, tok := range pt {
				if tok == "*" || tok == ">" {
					continue
				}
				gen := append(append(append([]string{}, pt[:i]...), "*"), pt[i+1:]...)
				if !sep.Match(strings.Join(gen, string(sep)), subject) {
					t.Fatalf("generalised %q no longer matches %q", strings.Join(gen, string(sep)), subject)
				}
			}
			for i := range pt {
				tail := append(append([]string{}, pt[:i]...), ">")
				if !sep.Match(strings.Join(tail, string(sep)), subject) {
					t.Fatalf("tail pattern %q does not match %q", strings.Join(tail, string(sep)), subject)
				}
			}
			if !p.Tail() && p.NumTokens() != strings.Count(subject, string(sep))+1 {
				t.Fatalf("%q matched %q with a different token count", pattern, subject)
			}
		}
	})
}
