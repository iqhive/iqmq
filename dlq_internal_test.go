package iqmq

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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

// TestFramableReasonIsBoundedAndValidUTF8 is the half of the contract that
// drivers/gcppubsub needs, and every driver gets: the dead-letter publisher
// publishes in MetadataNativeStrict, and a Pub/Sub attribute value is at most
// 1,024 bytes of valid UTF-8, so a longer handler error, or one that is not
// UTF-8, failed the route there and the message was redelivered for ever. An
// invalid sequence becomes U+FFFD, one for each run of invalid bytes, and a
// longer reason is cut at a character boundary and ends in "...", the marker
// counted inside the 1,024.
func TestFramableReasonIsBoundedAndValidUTF8(t *testing.T) {
	a := func(n int) string { return strings.Repeat("a", n) }
	for _, c := range []struct{ name, in, want string }{
		{"exactly the bound", a(1024), a(1024)},
		{"one byte over", a(1025), a(1021) + "..."},
		{"long text", a(5000), a(1021) + "..."},
		{"long text padded and with a newline", "  " + a(1030) + "\n", a(1021) + "..."},
		{"invalid byte", "a\xffb", "a�b"},
		{"a run of invalid bytes is one replacement", "a\xff\xfe\xfdb", "a�b"},
		{"invalid at the end", "failed\xc3", "failed�"},
		{"a valid replacement character stays", "a�b", "a�b"},
		// The cut lands inside a three-byte rune and backs off to its start.
		{"cut inside a rune", a(1020) + "€zzzz", a(1020) + "..."},
		{"cut right after a rune", a(1018) + "€zzzz", a(1018) + "€..."},
		{"invalid bytes past the bound", a(1030) + "\xff", a(1021) + "..."},
		{"replacements that grow it past the bound", a(1022) + "\xff", a(1021) + "..."},
	} {
		if got := framableReason(c.in); got != c.want {
			t.Errorf("%s: framableReason(%d bytes) = %q (%d bytes), want %q (%d bytes)",
				c.name, len(c.in), got, len(got), c.want, len(c.want))
		}
	}
}

// TestFramableReasonAllocatesNothingForAShortValidReason: the route renders
// the handler's error on every dead-lettering, and a reason that is already
// short, printable and valid UTF-8 is handed back as it is.
func TestFramableReasonAllocatesNothingForAShortValidReason(t *testing.T) {
	for _, in := range []string{"schema mismatch", "  padded  ", "échec de décodage", strings.Repeat("a", 1024)} {
		var got string
		if n := testing.AllocsPerRun(100, func() { got = framableReason(in) }); n != 0 {
			t.Errorf("framableReason(%q) allocated %v times, want 0", in, n)
		}
		if got == "" {
			t.Errorf("framableReason(%q) = %q", in, got)
		}
	}
}

// TestFramableReasonProducesAFramableValue is the property the table is a
// sample of, restated as the rule drivers/nats and drivers/jetstream apply:
// no control character anywhere, and no leading or trailing space or tab;
// and as the rule drivers/gcppubsub applies: valid UTF-8, at most 1,024 bytes.
func TestFramableReasonProducesAFramableValue(t *testing.T) {
	for _, in := range []string{
		"", "\n", "\x00\x01\x02", "a\rb", " \t ", "plain",
		"a very\nlong\nmulti-line\nerror\r\nwith headers: 1",
		"\x1funprintable at the front", "unprintable at the back\x1f",
		strings.Repeat("é\n\xff", 700), strings.Repeat("€", 400), strings.Repeat("\xff ", 600),
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
		if !utf8.ValidString(got) || len(got) > 1024 {
			t.Errorf("framableReason(%d bytes) = %d bytes, valid UTF-8 = %v: want at most 1,024 bytes of valid UTF-8",
				len(in), len(got), utf8.ValidString(got))
		}
	}
}
