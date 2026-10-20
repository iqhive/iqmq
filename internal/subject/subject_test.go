package subject

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
)

func TestValidatePattern(t *testing.T) {
	long := strings.Repeat("a", MaxLen)
	valid := []string{
		"a", "a.b", "a.b.c", "*", ">", "a.*", "*.a", "a.*.c", "a.>", "*.>", "a.*.>", "*.*.*",
		"orders-created_v2", "a/b", "a:b", "é.ü", "a\x80b", long, long[:200] + "." + long[:54],
		"tenant/ns/topic", "a..b.c"[:1] + ".b",
	}
	invalid := []string{
		"", ".", "..", ".a", "a.", "a..b", "a*", "*a", "**", ">>", "a>", ">a", ">.a", ">.>", "a.>.b",
		"a b", "a\tb", "a\x00b", "a\x7fb", "a.*b", long + "a", long + ".a",
	}
	for _, p := range valid {
		if err := Validate(p); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", p, err)
		}
		if _, err := Compile(p); err != nil {
			t.Errorf("Compile(%q) = %v, want nil", p, err)
		}
	}
	for _, p := range invalid {
		err := Validate(p)
		if err == nil {
			t.Errorf("Validate(%q) = nil, want error", p)
			continue
		}
		if !errors.Is(err, ErrInvalidPattern) || errcode.CodeOf(err) != errcode.InvalidArgument {
			t.Errorf("Validate(%q) = %v: wrong sentinel", p, err)
		}
		if cp, cerr := Compile(p); cp != nil || !errors.Is(cerr, ErrInvalidPattern) {
			t.Errorf("Compile(%q) = %v, %v", p, cp, cerr)
		}
		if Match(p, "a") || Match(p, p) {
			t.Errorf("Match with invalid pattern %q reported true", p)
		}
	}
}

func TestValidateSubject(t *testing.T) {
	valid := []string{"a", "a.b", "orders.created", "a/b", strings.Repeat("a", MaxLen), "é"}
	invalid := []string{"", ".", "a.", ".a", "a..b", "*", ">", "a.*", "a.>", "a*", "a b", "a\x00", strings.Repeat("a", MaxLen+1)}
	for _, s := range valid {
		if err := ValidateSubject(s); err != nil {
			t.Errorf("ValidateSubject(%q) = %v", s, err)
		}
		if err := Dot.ValidateSubjectBytes([]byte(s)); err != nil {
			t.Errorf("ValidateSubjectBytes(%q) = %v", s, err)
		}
		// A literal subject is also a literal pattern matching exactly itself.
		p := mustCompile(t, s)
		if !p.Literal() || !p.Match(s) {
			t.Errorf("literal %q: Literal=%v Match=%v", s, p.Literal(), p.Match(s))
		}
	}
	for _, s := range invalid {
		err := ValidateSubject(s)
		if err == nil {
			t.Errorf("ValidateSubject(%q) = nil", s)
			continue
		}
		if !errors.Is(err, ErrInvalidSubject) || errcode.CodeOf(err) != errcode.InvalidArgument {
			t.Errorf("ValidateSubject(%q) = %v: wrong sentinel", s, err)
		}
		if eb := Dot.ValidateSubjectBytes([]byte(s)); !errors.Is(eb, ErrInvalidSubject) {
			t.Errorf("ValidateSubjectBytes(%q) = %v", s, eb)
		}
		// No pattern matches an invalid subject, not even the tail wildcard.
		if mustCompile(t, ">").Match(s) || Match(">", s) || Match("*", s) {
			t.Errorf("invalid subject %q matched", s)
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"a", "a", true},
		{"a", "b", false},
		{"a", "a.b", false},
		{"a.b", "a", false},
		{"a.b", "a.b", true},
		{"a.b", "a.b.c", false},
		{"*", "a", true},
		{"*", "a.b", false},
		{"*", "", false},
		{"a.*", "a.b", true},
		{"a.*", "a", false},
		{"a.*", "a.b.c", false},
		{"a.*", "b.c", false},
		{"*.b", "a.b", true},
		{"*.b", "a.c", false},
		{"*.*", "a.b", true},
		{"*.*", "a", false},
		{"*.*", "a.b.c", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
		{"a.*.c", "a.b.b.c", false},
		{">", "a", true},
		{">", "a.b.c", true},
		{">", "", false},
		{"a.>", "a", false},
		{"a.>", "a.b", true},
		{"a.>", "a.b.c.d", true},
		{"a.>", "b.c", false},
		{"*.>", "a.b", true},
		{"*.>", "a", false},
		{"a.*.>", "a.b.c", true},
		{"a.*.>", "a.b", false},
		// Wildcard characters in the subject are literal-less: never match.
		{"*", "*", false},
		{">", ">", false},
		{"a.*", "a.*", false},
		{"a.>", "a.>", false},
		{"a.>", "a.b.*", false},
		// Malformed subjects never match.
		{">", ".a", false},
		{">", "a.", false},
		{">", "a..b", false},
		{"a.*", "a.", false},
		{"a.*", "a..", false},
		{"*.b", ".b", false},
		{"a.b", "a.b.", false},
		{">", "a b", false},
		{"*", "a\x00", false},
		// Non-ASCII literals.
		{"é.*", "é.ü", true},
		{"*.ü", "é.ü", true},
	}
	for _, c := range cases {
		p := mustCompile(t, c.pattern)
		if got := p.Match(c.subject); got != c.want {
			t.Errorf("Pattern(%q).Match(%q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
		if got := p.MatchBytes([]byte(c.subject)); got != c.want {
			t.Errorf("Pattern(%q).MatchBytes(%q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
		if got := Match(c.pattern, c.subject); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
		if got := Dot.MatchBytes(c.pattern, []byte(c.subject)); got != c.want {
			t.Errorf("Dot.MatchBytes(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestSlashSeparator(t *testing.T) {
	// With Slash, dots are ordinary bytes and slashes separate tokens.
	p, err := Slash.Compile("orders/*/v1.2/>")
	if err != nil {
		t.Fatal(err)
	}
	if p.Sep() != Slash || p.NumTokens() != 4 || p.Literal() || !p.Tail() {
		t.Fatalf("unexpected pattern shape: %+v", p)
	}
	for i, want := range []string{"orders", "*", "v1.2", ">"} {
		if p.Token(i) != want {
			t.Errorf("Token(%d) = %q, want %q", i, p.Token(i), want)
		}
	}
	yes := []string{"orders/eu/v1.2/created", "orders/us/v1.2/a/b/c"}
	no := []string{"orders/eu/v1.3/created", "orders/eu/v1.2", "orders.eu.v1.2.created", "orders//v1.2/x", "orders/eu/v1.2/"}
	for _, s := range yes {
		if !p.Match(s) || !Slash.Match("orders/*/v1.2/>", s) {
			t.Errorf("%q should match", s)
		}
	}
	for _, s := range no {
		if p.Match(s) || Slash.Match("orders/*/v1.2/>", s) {
			t.Errorf("%q should not match", s)
		}
	}
	// A dotted name is a single token under Slash and a literal pattern.
	if q := mustCompileSep(t, Slash, "a.b.c"); !q.Literal() || q.NumTokens() != 1 || !q.Match("a.b.c") || q.Match("a.b.d") {
		t.Error("dotted literal under Slash misbehaves")
	}
	if err := Slash.Validate("a.b/*"); err != nil {
		t.Errorf("Slash.Validate(a.b/*) = %v", err)
	}
	if err := Slash.ValidateSubject("a.b/c"); err != nil {
		t.Errorf("Slash.ValidateSubject(a.b/c) = %v", err)
	}
	if err := Slash.Validate("a/b/*c"); !errors.Is(err, ErrInvalidPattern) {
		t.Errorf("Slash.Validate(a/b/*c) = %v", err)
	}
}

func TestInvalidSeparator(t *testing.T) {
	for _, s := range []Sep{'*', '>', ' ', 0, 0x7f, 0x80} {
		if err := s.Validate("a"); !errors.Is(err, ErrInvalidPattern) {
			t.Errorf("Sep(%q).Validate = %v", byte(s), err)
		}
		if err := s.ValidateSubject("a"); !errors.Is(err, ErrInvalidSubject) {
			t.Errorf("Sep(%q).ValidateSubject = %v", byte(s), err)
		}
		if err := s.ValidateSubjectBytes([]byte("a")); !errors.Is(err, ErrInvalidSubject) {
			t.Errorf("Sep(%q).ValidateSubjectBytes = %v", byte(s), err)
		}
		if p, err := s.Compile("a"); p != nil || err == nil {
			t.Errorf("Sep(%q).Compile = %v, %v", byte(s), p, err)
		}
		if s.Match("a", "a") || s.MatchBytes("a", []byte("a")) {
			t.Errorf("Sep(%q).Match reported true", byte(s))
		}
	}
}

func TestPatternAccessors(t *testing.T) {
	p := mustCompile(t, "a.*.>")
	if p.String() != "a.*.>" || p.Sep() != Dot || p.Literal() || !p.Tail() || p.NumTokens() != 3 {
		t.Errorf("accessors: %q %v %v %v %d", p.String(), p.Sep(), p.Literal(), p.Tail(), p.NumTokens())
	}
	if p.Token(0) != "a" || p.Token(1) != "*" || p.Token(2) != ">" {
		t.Errorf("tokens: %q %q %q", p.Token(0), p.Token(1), p.Token(2))
	}
	lit := mustCompile(t, "orders.created")
	if !lit.Literal() || lit.Tail() || lit.NumTokens() != 2 {
		t.Errorf("literal accessors: %v %v %d", lit.Literal(), lit.Tail(), lit.NumTokens())
	}
	if _, err := Compile("a..b"); err == nil {
		t.Error("Compile of an invalid pattern did not fail")
	}
}

func mustCompile(tb testing.TB, pattern string) *Pattern {
	tb.Helper()
	return mustCompileSep(tb, Dot, pattern)
}

func mustCompileSep(tb testing.TB, s Sep, pattern string) *Pattern {
	tb.Helper()
	p, err := s.Compile(pattern)
	if err != nil {
		tb.Fatalf("Compile(%q): %v", pattern, err)
	}
	return p
}

func TestConcurrentMatch(t *testing.T) {
	p := mustCompile(t, "orders.*.>")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if !p.Match("orders.eu.created") || p.Match("orders.eu") {
					t.Error("concurrent match diverged")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestAllocs(t *testing.T) {
	p := mustCompile(t, "orders.*.created.>")
	subj := "orders.eu.created.v2.final"
	subjBytes := []byte(subj)
	var sink int
	checks := []struct {
		label string
		fn    func()
	}{
		{"Pattern.Match", func() {
			if p.Match(subj) {
				sink++
			}
		}},
		{"Pattern.MatchBytes", func() {
			if p.MatchBytes(subjBytes) {
				sink++
			}
		}},
		{"Match", func() {
			if Match("orders.*.created.>", subj) {
				sink++
			}
		}},
		{"Sep.MatchBytes", func() {
			if Dot.MatchBytes("orders.*.created.>", subjBytes) {
				sink++
			}
		}},
		{"Validate", func() {
			if Validate("orders.*.created.>") == nil {
				sink++
			}
		}},
		{"ValidateSubject", func() {
			if ValidateSubject(subj) == nil {
				sink++
			}
		}},
		{"ValidateSubjectBytes", func() {
			if Dot.ValidateSubjectBytes(subjBytes) == nil {
				sink++
			}
		}},
		{"ValidateBad", func() {
			if Validate("a..b") != nil {
				sink++
			}
		}},
	}
	for _, c := range checks {
		if n := testing.AllocsPerRun(200, c.fn); n != 0 {
			t.Errorf("%s allocated %v times per call", c.label, n)
		}
	}
	// Compile allocates the Pattern and its two token tables, nothing else.
	if n := testing.AllocsPerRun(200, func() {
		if _, err := Compile("orders.*.created.>"); err != nil {
			sink++
		}
	}); n > 3 {
		t.Errorf("Compile allocated %v times per call, want at most 3", n)
	}
	_ = sink
}
