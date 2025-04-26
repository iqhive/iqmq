package topicname

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// grammar is the one-line portable grammar from the message contract as a
// regular expression; together with the length bound and the dot-only
// segment rule it forms the oracle Check is compared against.
var grammar = regexp.MustCompile(`^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$`)

// oracle is the naive validator.
func oracle(name string) bool {
	if len(name) == 0 || len(name) > MaxLen || !grammar.MatchString(name) {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if strings.Trim(seg, ".") == "" {
			return false
		}
	}
	return true
}

// FuzzTopicValidate checks grammar agreement between Check and the regexp
// oracle, agreement between the string and byte entry points, and that the
// reported problem kind and position describe the name.
func FuzzTopicValidate(f *testing.F) {
	for _, s := range []string{
		"a", "orders", "orders/created", "tenant/ns/topic", "a.b.c", "a-b_c",
		strings.Repeat("a", MaxLen), strings.Repeat("a", MaxLen+1),
		"", "/", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "...", "a...",
		"a b", "a*b", "a>b", "é", "a\x00b", "a\\b", "a:b", "A/B/C/D/E/F/G",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		p := Check(name)
		if pb := CheckBytes([]byte(name)); pb != p {
			t.Fatalf("CheckBytes = %+v, Check = %+v", pb, p)
		}
		want := oracle(name)
		if p.OK() != want {
			t.Fatalf("Check(%q) = %+v, oracle valid=%v", name, p, want)
		}
		err := Validate(name)
		if (err == nil) != want || Valid(name) != want {
			t.Fatalf("Validate(%q) = %v, Valid = %v, oracle %v", name, err, Valid(name), want)
		}
		if err != nil && (!errors.Is(err, errcode.ErrInvalidTopic) || errcode.CodeOf(err) != errcode.InvalidArgument) {
			t.Fatalf("Validate(%q) = %v: wrong sentinel", name, err)
		}
		if (ValidateBytes([]byte(name)) == nil) != want {
			t.Fatalf("ValidateBytes(%q) disagrees", name)
		}
		if p.Pos < 0 || p.Pos > len(name) {
			t.Fatalf("Check(%q) = %+v: position out of range", name, p)
		}
		switch p.Kind {
		case OK:
			if p.Pos != 0 {
				t.Fatalf("OK with Pos %d", p.Pos)
			}
			var segs []string
			for seg := range Segments(name) {
				if seg == "" || strings.Trim(seg, ".") == "" || !grammar.MatchString(seg) || strings.Contains(seg, "/") {
					t.Fatalf("Segments(%q) yielded %q", name, seg)
				}
				segs = append(segs, seg)
			}
			if strings.Join(segs, "/") != name {
				t.Fatalf("Segments(%q) = %q does not rebuild the name", name, segs)
			}
			if Depth(name) != len(segs) {
				t.Fatalf("Depth(%q) = %d, %d segments", name, Depth(name), len(segs))
			}
			if Hierarchical(name) != (len(segs) > 1) {
				t.Fatalf("Hierarchical(%q) = %v with %d segments", name, Hierarchical(name), len(segs))
			}
		case Empty:
			if name != "" {
				t.Fatalf("Empty for %q", name)
			}
		case TooLong:
			if len(name) <= MaxLen || p.Pos != MaxLen {
				t.Fatalf("TooLong for %d bytes, Pos %d", len(name), p.Pos)
			}
		case InvalidByte:
			if p.Pos >= len(name) || IsSegmentByte(name[p.Pos]) || name[p.Pos] == Separator {
				t.Fatalf("InvalidByte at %d of %q", p.Pos, name)
			}
		case LeadingSeparator:
			if name[0] != Separator || p.Pos != 0 {
				t.Fatalf("LeadingSeparator for %q Pos %d", name, p.Pos)
			}
		case TrailingSeparator:
			if name[len(name)-1] != Separator || p.Pos != len(name)-1 {
				t.Fatalf("TrailingSeparator for %q Pos %d", name, p.Pos)
			}
		case EmptySegment:
			if p.Pos == 0 || name[p.Pos] != Separator || name[p.Pos-1] != Separator {
				t.Fatalf("EmptySegment at %d of %q", p.Pos, name)
			}
		case DotSegment:
			rest := name[p.Pos:]
			if i := strings.IndexByte(rest, Separator); i >= 0 {
				rest = rest[:i]
			}
			if rest == "" || strings.Trim(rest, ".") != "" || (p.Pos > 0 && name[p.Pos-1] != Separator) {
				t.Fatalf("DotSegment at %d of %q", p.Pos, name)
			}
		default:
			t.Fatalf("unknown kind %v", p.Kind)
		}
	})
}
