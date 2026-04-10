package iqmq

import "testing"

// framableReason is what keeps the dead-letter route publishable on a
// broker that frames a header as a line of text. The table
// is the whole of its contract, including the two answers that are easy to get
// wrong: a reason with nothing printable renders EMPTY, which
// deadLetterMessage turns into an absent header rather than an empty one, and
// a multi-byte rune is never touched.
func TestFramableReason(t *testing.T) {
	for _, c := range []struct{ name, in, want string }{
		{"nothing to do", "schema mismatch", "schema mismatch"},
		{"empty", "", ""},
		{"a newline", "a\nb", "a b"},
		{"a header injection", "a\r\nInjected: yes", "a  Injected: yes"},
		{"a tab inside", "a\tb", "a b"},
		{"a NUL", "a\x00b", "a b"},
		{"a DEL", "a\x7fb", "a b"},
		{"leading and trailing space", "  padded  ", "padded"},
		{"leading tab", "\tpadded", "padded"},
		{"a control character at each end", "\nboom\n", "boom"},
		{"nothing printable", "\n\r\t\x00", ""},
		{"only spaces", "   ", ""},
		{"a rune above ASCII survives", "échec de décodage", "échec de décodage"},
		{"a rune beside a control character", "é\né", "é é"},
		{"every byte printable at the boundary", " \x1f ", ""},
	} {
		if got := framableReason(c.in); got != c.want {
			t.Errorf("%s: framableReason(%q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

// TestFramableReasonProducesAFramableValue is the property the table is a
// sample of, restated as the rule drivers/nats and drivers/jetstream apply:
// no control character anywhere, and no leading or trailing space or tab.
func TestFramableReasonProducesAFramableValue(t *testing.T) {
	for _, in := range []string{
		"", "\n", "\x00\x01\x02", "a\rb", " \t ", "plain",
		"a very\nlong\nmulti-line\nerror\r\nwith headers: 1",
		"\x1funprintable at the front", "unprintable at the back\x1f",
	} {
		got := framableReason(in)
		for i := range got {
			if c := got[i]; c < 0x20 || c == 0x7f {
				t.Errorf("framableReason(%q) = %q: byte %d is %#x", in, got, i, c)
			}
		}
		if len(got) > 0 && (got[0] == ' ' || got[0] == '\t' || got[len(got)-1] == ' ' || got[len(got)-1] == '\t') {
			t.Errorf("framableReason(%q) = %q: it is padded", in, got)
		}
	}
}
