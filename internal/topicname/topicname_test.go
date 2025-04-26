package topicname

import (
	"errors"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

func TestCheck(t *testing.T) {
	long := strings.Repeat("a", MaxLen)
	cases := []struct {
		name string
		want Problem
	}{
		{"a", Problem{}},
		{"orders", Problem{}},
		{"orders/created", Problem{}},
		{"tenant/ns/topic", Problem{}},
		{"a.b.c", Problem{}},
		{"a-b_c.d/e-f_g.h", Problem{}},
		{"...a", Problem{}},
		{"a...", Problem{}},
		{".a.", Problem{}},
		{"A0", Problem{}},
		{long, Problem{}},
		{long[:200] + "/" + long[:54], Problem{}},
		{"", Problem{Kind: Empty}},
		{long + "a", Problem{Kind: TooLong, Pos: MaxLen}},
		{long + "/", Problem{Kind: TooLong, Pos: MaxLen}},
		{"/a", Problem{Kind: LeadingSeparator}},
		{"/", Problem{Kind: LeadingSeparator}},
		{"a/", Problem{Kind: TrailingSeparator, Pos: 1}},
		{"a//b", Problem{Kind: EmptySegment, Pos: 2}},
		{"a///b", Problem{Kind: EmptySegment, Pos: 2}},
		{"ab/c//d", Problem{Kind: EmptySegment, Pos: 5}},
		{".", Problem{Kind: DotSegment}},
		{"..", Problem{Kind: DotSegment}},
		{"a/./b", Problem{Kind: DotSegment, Pos: 2}},
		{"a/../b", Problem{Kind: DotSegment, Pos: 2}},
		{"a/b/..", Problem{Kind: DotSegment, Pos: 4}},
		{"a b", Problem{Kind: InvalidByte, Pos: 1}},
		{"a*b", Problem{Kind: InvalidByte, Pos: 1}},
		{"a>b", Problem{Kind: InvalidByte, Pos: 1}},
		{"a:b", Problem{Kind: InvalidByte, Pos: 1}},
		{"é", Problem{Kind: InvalidByte, Pos: 0}},
		{"a\x00", Problem{Kind: InvalidByte, Pos: 1}},
		{"a\\b", Problem{Kind: InvalidByte, Pos: 1}},
		{"a/b c", Problem{Kind: InvalidByte, Pos: 3}},
		// The first problem in position order wins.
		{"a/ /..", Problem{Kind: InvalidByte, Pos: 2}},
		{"a//..", Problem{Kind: EmptySegment, Pos: 2}},
		{"../ ", Problem{Kind: DotSegment, Pos: 0}},
	}
	for _, c := range cases {
		got := Check(c.name)
		if got != c.want {
			t.Errorf("Check(%q) = %+v, want %+v", c.name, got, c.want)
		}
		if gb := CheckBytes([]byte(c.name)); gb != got {
			t.Errorf("CheckBytes(%q) = %+v, Check = %+v", c.name, gb, got)
		}
		if Valid(c.name) != c.want.OK() {
			t.Errorf("Valid(%q) = %v, want %v", c.name, Valid(c.name), c.want.OK())
		}
		err := Validate(c.name)
		if (err == nil) != c.want.OK() {
			t.Errorf("Validate(%q) = %v, want ok=%v", c.name, err, c.want.OK())
		}
		if err != nil {
			if !errors.Is(err, errcode.ErrInvalidTopic) {
				t.Errorf("Validate(%q) = %v, not ErrInvalidTopic", c.name, err)
			}
			if errcode.CodeOf(err) != errcode.InvalidArgument {
				t.Errorf("Validate(%q) code = %v", c.name, errcode.CodeOf(err))
			}
		}
		if eb := ValidateBytes([]byte(c.name)); (eb == nil) != (err == nil) {
			t.Errorf("ValidateBytes(%q) = %v, Validate = %v", c.name, eb, err)
		}
		if pe := got.Err(); (pe == nil) != (err == nil) || (err != nil && !errors.Is(pe, err)) {
			t.Errorf("Check(%q).Err() = %v, Validate = %v", c.name, pe, err)
		}
	}
}

func TestProblemKindString(t *testing.T) {
	want := []string{"OK", "Empty", "TooLong", "InvalidByte", "LeadingSeparator", "TrailingSeparator", "EmptySegment", "DotSegment"}
	for i, w := range want {
		if got := ProblemKind(i).String(); got != w {
			t.Errorf("ProblemKind(%d).String() = %q, want %q", i, got, w)
		}
	}
	if got := ProblemKind(200).String(); got != "ProblemKind(?)" {
		t.Errorf("out-of-range String() = %q", got)
	}
}

func TestIsSegmentByte(t *testing.T) {
	for c := range 256 {
		b := byte(c)
		want := (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '.' || b == '_' || b == '-'
		if IsSegmentByte(b) != want {
			t.Errorf("IsSegmentByte(%q) = %v, want %v", b, !want, want)
		}
	}
}

func TestSegments(t *testing.T) {
	cases := map[string][]string{
		"a":               {"a"},
		"orders/created":  {"orders", "created"},
		"tenant/ns/topic": {"tenant", "ns", "topic"},
		"a.b.c":           {"a.b.c"},
		// Invalid names still split predictably.
		"":     {""},
		"/a":   {"", "a"},
		"a/":   {"a", ""},
		"a//b": {"a", "", "b"},
	}
	for name, want := range cases {
		var got []string
		for seg := range Segments(name) {
			got = append(got, seg)
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("Segments(%q) = %q, want %q", name, got, want)
		}
		if d := Depth(name); d != len(want) {
			t.Errorf("Depth(%q) = %d, want %d", name, d, len(want))
		}
		if h := Hierarchical(name); h != strings.Contains(name, "/") {
			t.Errorf("Hierarchical(%q) = %v", name, h)
		}
	}
	// Early exit stops the iteration.
	n := 0
	for range Segments("a/b/c") {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Errorf("break after 2 segments iterated %d", n)
	}
}

func TestAllocs(t *testing.T) {
	name := "tenant/ns/orders.created-v2_final"
	bad := "tenant//ns"
	nameBytes := []byte(name)
	var sink int
	checks := []struct {
		label string
		fn    func()
	}{
		{"Validate", func() {
			if Validate(name) != nil {
				sink++
			}
		}},
		{"ValidateBad", func() {
			if Validate(bad) == nil {
				sink++
			}
		}},
		{"ValidateBytes", func() {
			if ValidateBytes(nameBytes) != nil {
				sink++
			}
		}},
		{"Check", func() { sink += Check(name).Pos }},
		{"CheckBad", func() { sink += Check(bad).Pos }},
		{"Valid", func() {
			if Valid(name) {
				sink++
			}
		}},
		{"Segments", func() {
			for seg := range Segments(name) {
				sink += len(seg)
			}
		}},
		{"Depth", func() { sink += Depth(name) }},
	}
	for _, c := range checks {
		if n := testing.AllocsPerRun(200, c.fn); n != 0 {
			t.Errorf("%s allocated %v times per call", c.label, n)
		}
	}
	_ = sink
}
