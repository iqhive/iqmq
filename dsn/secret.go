package dsn

import (
	"log/slog"
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
// fmt's Stringer, slog's LogValuer, encoding/json's Marshaler — are the three
// this type implements, and each of them answers with [Redacted].
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
func Scrub(msg string, secrets ...Secret) string {
	for _, s := range secrets {
		if s == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, string(s), Redacted)
	}
	return msg
}
