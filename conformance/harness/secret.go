package harness

import "log/slog"

// redacted is what a Secret renders as. It is the same fixed string
// github.com/iqhive/iqmq/dsn uses, and never a length hint.
const redacted = "redacted"

// Secret is a broker password that never renders itself: fmt's %v and %s,
// %#v, slog's text and JSON handlers and encoding/json all print "redacted".
// Reveal is the one way to read it.
//
// It is a copy of github.com/iqhive/iqmq/dsn.Secret's renderings rather than
// that type, and the reason is a rule this package does not own:
// benchmarks/native-baselines measures each raw client with this harness
// starting its containers, and its TestNoRootImport requires that the harness
// import nothing from the root module, so that a baseline cannot link iqmq
// code and measure it. Using dsn.Secret here was watched
// failing exactly that test:
//
//	nativebench_test.go:246: ../../../conformance/harness/harness.go imports github.com/iqhive/iqmq/dsn
//
// A driver configuration that wants a dsn.Secret converts -- the underlying
// types are the same, so `dsn.Secret(b.Auth.Password)` copies the credential
// without revealing it anywhere.
type Secret string

// String returns the redaction, or "" for an empty Secret: "no password" is
// not a credential, and rendering it as one would make every password-less
// broker look like it had one.
func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return redacted
}

// GoString returns the redaction, so %#v does not defeat String.
func (s Secret) GoString() string {
	if s == "" {
		return `harness.Secret("")`
	}
	return `harness.Secret("` + redacted + `")`
}

// LogValue returns the redaction, so slog does not print the secret.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

// MarshalText returns the redaction, so encoding/json writes it rather than the
// password.
func (s Secret) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Reveal returns the password itself. It is the only way to read one.
func (s Secret) Reveal() string { return string(s) }
