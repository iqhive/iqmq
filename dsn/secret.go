package dsn

import (
	"cmp"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
)

// Redacted is what a credential renders as everywhere iqmq prints one. It is a
// fixed string and never a length hint: "xxxx" for a four-character password
// tells an attacker the length, which is information the log did not have to
// give away.
const Redacted = "redacted"

// Secret is a credential that never renders itself.
//
// the security checks forbids credentials in logs and the specification forbids them in error
// strings, and both rules are easy to break by accident: a Config printed with
// %v, a DSN put into an error for context, a struct handed to slog. A Secret
// answers all three at once, because the only ways a program prints a value —
// fmt's Formatter and Stringer, slog's LogValuer, encoding/json's Marshaler —
// are the ones this type implements, and each of them answers with [Redacted].
//
// Reveal is the single, greppable way to get the bytes back. A reviewer looking
// for the places a password can escape searches for that one name.
type Secret string

// String returns the redaction, never the secret. An empty Secret renders as
// the empty string: "the DSN carried no password" is not a credential, and
// rendering it as one would make every password-less DSN look like it had one.
func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return Redacted
}

// GoString returns the redaction, so %#v does not defeat String.
func (s Secret) GoString() string {
	if s == "" {
		return `dsn.Secret("")`
	}
	return `dsn.Secret("` + Redacted + `")`
}

// Format writes the redaction under every verb fmt hands to a method.
//
// String alone was not enough. fmt asks a Stringer only under the string verbs
// (%v %s %q %x %X), and under any other it reports a bad verb with the value
// beside it, printed without asking any method: "%!d(dsn.Secret=s3cr3t)". A
// Formatter is asked first under every verb, so the redaction goes back to fmt
// as a plain string and a width, a %q or a bad verb renders as it would for
// that string. %p is the exception no method can close: fmt refuses it for a
// value that is not a pointer before it looks for one.
func (s Secret) Format(f fmt.State, verb rune) {
	if verb == 'v' && f.Flag('#') {
		_, _ = io.WriteString(f, s.GoString())
		return
	}
	_, _ = fmt.Fprintf(f, fmt.FormatString(f, verb), s.String())
}

// LogValue returns the redaction, so slog does not print the secret.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

// MarshalText returns the redaction, so encoding/json and every other
// text-marshalling encoder writes the redaction rather than the credential.
// A Secret is never round-tripped through a marshaller: it is an input.
func (s Secret) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Reveal returns the credential itself. It is the only way to read one, and it
// is named so that a review can find every place a secret leaves the type.
func (s Secret) Reveal() string { return string(s) }

// Scrub replaces every occurrence of each secret in msg with the redaction, so
// that an error or a log line built from a native client's message cannot carry
// a credential the client embedded in it. An empty secret is skipped: replacing
// the empty string would redact the gaps between every character.
//
// Every byte of every occurrence is redacted, whatever order the secrets come
// in, and occurrences that overlap are one redaction. Replacing the secrets one
// at a time let a password that is part of a longer token go first and leave
// the rest of the token in the message, and let a later secret match inside a
// redaction already made. So every occurrence is found in msg as it was
// given, before anything is replaced.
func Scrub(msg string, secrets ...Secret) string {
	type span struct{ start, end int }
	var all []span
	for _, s := range secrets {
		if s == "" {
			continue
		}
		// Every occurrence, overlapping ones included: "aaa" holds "aa"
		// twice, and the second covers a byte the first does not.
		for i := 0; ; {
			j := strings.Index(msg[i:], string(s))
			if j < 0 {
				break
			}
			all = append(all, span{start: i + j, end: i + j + len(s)})
			i += j + 1
		}
	}
	if len(all) == 0 {
		return msg
	}
	// Occurrences that overlap become one run and one redaction; ones that
	// only touch stay two, as two ReplaceAll matches would.
	slices.SortFunc(all, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	runs := all[:0]
	for _, sp := range all {
		if n := len(runs); n > 0 && sp.start < runs[n-1].end {
			runs[n-1].end = max(runs[n-1].end, sp.end)
			continue
		}
		runs = append(runs, sp)
	}
	var b strings.Builder
	at := 0
	for _, r := range runs {
		b.WriteString(msg[at:r.start])
		b.WriteString(Redacted)
		at = r.end
	}
	b.WriteString(msg[at:])
	return b.String()
}
