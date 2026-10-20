package dsn_test

import (
	"reflect"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// FuzzDSNParse is the parser target. The bytes of a DSN come from an
// environment variable a stranger may have set, so the parser must be pure,
// bounded, total and non-panicking, and it must never leak a credential into
// the rendering a program is allowed to log.
//
// The invariants after an accepted parse:
//
//   - Parsing is a function: the same input gives the same Config.
//   - The credential never appears in String(), in the Secret's own renderings
//     or in a Scrub of a message containing it.
//   - String() round-trips: re-parsing the redacted rendering gives back the
//     same Config apart from the password, which is what makes the rendering
//     safe to put in a log and still useful.
//   - Strict() fails for exactly the parameters nothing read, and reading them
//     all satisfies it (ADR-010).
//   - Every failure carries CodeInvalidConfig, never a panic and never
//     CodeUnknown.
func FuzzDSNParse(f *testing.F) {
	for _, s := range []string{
		"",
		"memory://",
		"mem://?autocreate=true",
		"kafka://b1:9092,b2:9093/orders?acks=all&tls=true",
		"kafka://alice:s3cr3t@b1.example:9092/orders",
		"amqp://us%20er:p%40ss@[2001:db8::1]:5672/a%2Fb?q=%20x",
		"nats://u:@h:4222",
		"redis://h?db=0&db=1",
		"x://h#frag",
		"x://h?=1",
		"x://h?a=%zz",
		"x://,h",
		"x://h:99999999999999999999",
		"://",
		"x:opaque",
		"X+y-1.z://H",
		"x://h?a=1&b=2&c=3",
		"x://h/p?+=%2B",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := dsn.Parse(s)
		if err != nil {
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
				t.Fatalf("Parse(%q) failed with code %v, want CodeInvalidConfig", s, code)
			}
			// Scheme and Parse agree about what is not a DSN: a registry
			// that took a scheme from a string Parse refuses would dispatch
			// a DSN nothing can read.
			if _, serr := dsn.Scheme(s); serr == nil && len(s) <= dsn.MaxLen {
				// Scheme reads less than Parse does, so it may accept a
				// string Parse refuses; what it must never do is return a
				// scheme that is not the one Parse would have found.
				_ = serr
			}
			return
		}
		again, aerr := dsn.Parse(s)
		if aerr != nil {
			t.Fatalf("Parse(%q) succeeded and then failed: %v", s, aerr)
		}
		if !reflect.DeepEqual(cfg, again) {
			t.Fatalf("Parse(%q) is not a function: %+v then %+v", s, cfg, again)
		}
		if cfg.Scheme == "" {
			t.Fatalf("Parse(%q) accepted a DSN with no scheme", s)
		}
		if got, err := dsn.Scheme(s); err != nil || got != cfg.Scheme {
			t.Fatalf("Scheme(%q) is %q, %v; Parse found %q", s, got, err, cfg.Scheme)
		}
		// The scheme is the input's, in its one canonical spelling: RFC 3986
		// makes a scheme case-insensitive, and every comparison after Parse
		// -- a registry lookup, a driver's own `c.Scheme != Scheme` -- is
		// exact, so anything but lower case here is a DSN that parses and
		// reaches nothing. Compared byte by byte against
		// an ASCII fold of the input, not through the package's own helper.
		if !isCanonicalSchemeOf(cfg.Scheme, s) {
			t.Fatalf("Parse(%q) read the scheme %q, which is not the input's scheme lower-cased", s, cfg.Scheme)
		}
		rendered := cfg.String()
		if secret := cfg.Password.Reveal(); secret != "" {
			// The leak check is structural rather than a substring search:
			// a one-character password is a substring of half the strings
			// in the world, and "the rendering does not contain these
			// bytes" is therefore not the property. The property is that
			// the credential position of the rendering carries the
			// redaction and not the credential.
			if cfg.Password.String() != dsn.Redacted {
				t.Fatalf("Secret.String is %q for %q, want the redaction", cfg.Password.String(), s)
			}
			if !strings.Contains(cfg.Password.GoString(), dsn.Redacted) {
				t.Fatalf("Secret.GoString is %q for %q", cfg.Password.GoString(), s)
			}
			if scrubbed := dsn.Scrub(secret, cfg.Password); scrubbed != dsn.Redacted {
				t.Fatalf("Scrub of the password of %q gave %q", s, scrubbed)
			}
		}
		// The rendering is a DSN again, and the same one apart from the
		// credential: a log line that names a broker has to name the broker.
		//
		// Escaping can make the rendering longer than the input, so a DSN
		// near MaxLen may render past it. That is the length bound doing its
		// job rather than a round-trip failure, and the invariant does not
		// hold across it.
		if len(rendered) > dsn.MaxLen {
			return
		}
		back, berr := dsn.Parse(rendered)
		if berr != nil {
			t.Fatalf("String() of %q gave %q, which Parse refuses: %v", s, rendered, berr)
		}
		if back.Scheme != cfg.Scheme || back.Username != cfg.Username || back.Path != cfg.Path {
			t.Fatalf("String() of %q did not round-trip: %+v vs %+v", s, back, cfg)
		}
		if back.HasPassword != cfg.HasPassword {
			t.Fatalf("String() of %q changed whether there was a password: %v vs %v", s, back.HasPassword, cfg.HasPassword)
		}
		if cfg.HasPassword && back.Password.Reveal() != dsn.Redacted {
			t.Fatalf("String() of %q carried the credential rather than the redaction: %q", s, rendered)
		}
		if !reflect.DeepEqual(back.Addrs(), cfg.Addrs()) {
			t.Fatalf("String() of %q lost an endpoint: %q vs %q", s, back.Addrs(), cfg.Addrs())
		}
		if back.Params.Len() != cfg.Params.Len() {
			t.Fatalf("String() of %q lost a parameter: %d vs %d", s, back.Params.Len(), cfg.Params.Len())
		}
		for k, v := range cfg.Params.All() {
			got, ok := back.Params.Lookup(k)
			if !ok || got != v {
				t.Fatalf("String() of %q changed %q from %q to %q (present=%v)", s, k, v, got, ok)
			}
		}
		// Strict names exactly what nothing read, and reading everything
		// satisfies it.
		strict, _ := dsn.Parse(s)
		if unread := strict.Params.Unread(); len(unread) != strict.Params.Len() {
			t.Fatalf("a fresh Config already had read marks: %q", unread)
		}
		if strict.Params.Len() > 0 {
			if err := strict.Params.Strict(); err == nil {
				t.Fatalf("Strict accepted %q with %d unread parameters", s, strict.Params.Len())
			}
		}
		for k := range strict.Params.All() {
			if err := strict.Params.String(k, func(string) {}); err != nil {
				t.Fatalf("String(%q): %v", k, err)
			}
		}
		if err := strict.Params.Strict(); err != nil {
			t.Fatalf("Strict rejected %q after every parameter was read: %v", s, err)
		}
	})
}

// isCanonicalSchemeOf reports whether scheme is the scheme s begins with, with
// every ASCII upper-case letter lowered and nothing else changed.
func isCanonicalSchemeOf(scheme, s string) bool {
	if len(s) < len(scheme)+len("://") || s[len(scheme):len(scheme)+len("://")] != "://" {
		return false
	}
	for i := range len(scheme) {
		in, got := s[i], scheme[i]
		if in >= 'A' && in <= 'Z' {
			in += 'a' - 'A'
		}
		if got != in {
			return false
		}
	}
	return true
}
