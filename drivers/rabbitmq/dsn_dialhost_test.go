package rabbitmq_test

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// TestADSNHostCannotRedirectTheDial holds ParseDSN to refusing a host that
// decodes to a byte the AMQP URL it builds would give a meaning to.
//
// The shared parser percent-decodes a host and Config.url writes the result
// into "amqp[s]://[user[:password]@]<host>/<vhost>", which amqp091-go's
// ParseURI reads with net/url. Measured against that library:
//
//	rabbitmq://tok%40real-host:5672/v  dialled real-host as the user "tok"
//	rabbitmq://a%2Fb:5672/v            dialled a
//	rabbitmq://a%3Fb:5672/v            dialled a AND lost the vhost
//	rabbitmq://a%23b:5672/v            dialled a AND lost the vhost
//	rabbitmq://u:p@tok%40real-host/v   dialled real-host with the password "p@tok"
//
// and a backslash, a space or a control byte made ParseURI refuse the URL, so
// the configuration failed at the dial with an error about a string the
// operator never wrote. A host name or an IP literal holds none of them.
//
// A comma is NOT in the set: amqp091-go reads one server URL, not a list, and
// the measurement above kept "a,b" as the host. It is the separator this
// driver's own DSN rendering joins endpoints with, which is the renderer's
// business (TestTheRenderingParsesBack), not the dialler's.
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"rabbitmq://tok%40real-host:5672/v",
		"rabbitmq://u:p@tok%40real-host:5672/v",
		"rabbitmq://a%2Fb:5672/v",
		"rabbitmq://a%3Fb:5672/v",
		"rabbitmq://a%23b:5672/v",
		"rabbitmq://a%5Cb:5672/v",
		"rabbitmq://a%20b:5672/v",
		"rabbitmq://a%09b:5672/v",
		"rabbitmq://a%7Fb:5672/v",
		"rabbitmq://h:5672,tok%40real-host:5672/v",
		// The scheme itself: "a%3A%2F%2Fb" decodes to "a://b", and
		// Config.url reads the first "://" of an address as ITS scheme, so
		// the dial would leave AMQP entirely. The "/" refusal covers it.
		"rabbitmq://a%3A%2F%2Fb:5672/v",
		// A double-encoded host puts the escape back: net/url rejects every
		// "%XY" in a host but "%25", so these failed at the DIAL rather than
		// at construction.
		"rabbitmq://tok%2540real-host:5672/v",
		"rabbitmq://a%25b:5672/v",
	} {
		_, err := rq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: its host decodes to a byte the AMQP URL gives a meaning to, so the "+
				"dial would go somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
	// What a host legitimately holds still parses: an IPv6 literal, one with
	// a zone, a name, and a comma -- which this driver does not split on.
	for in, want := range map[string]string{
		"rabbitmq://[::1]:5672/v":            "[::1]:5672",
		"rabbitmq://[fe80::1%25eth0]:5672/v": "[fe80::1%25eth0]:5672",
		"rabbitmq://broker-1.example:5672/v": "broker-1.example:5672",
		"rabbitmq://a%2Cb:5672/v":            "a,b:5672",
	} {
		cfg, err := rq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if len(cfg.Addrs) != 1 || cfg.Addrs[0] != want {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.Addrs, want)
		}
	}
}

// TestACredentialCannotRedirectTheDial is the host refusal's twin one
// component to the left: the USER and the PASSWORD are written into the same
// AMQP URL, and net/url ends the authority at the first literal "/", "?" or
// "#".
//
// Before the fix, measured against amqp091-go v1.14.0's own ParseURI:
//
//	rabbitmq://u%2Fv:p@good-host:5672/prod  ->  amqp://u/v:p@good-host:5672/prod
//	   which ParseURI reads as host "u", user "guest", password "guest" and
//	   vhost "v:p@good-host:5672/prod" -- so the dial went to a host the DSN
//	   does not name AND the configured password travelled to it inside the
//	   Connection.Open frame.
//	rabbitmq://u:aB%2FcD@good-host:5672/prod -> "invalid port \":aB\" after host"
//	   -- a base64 password, which routinely holds a "/", could not dial at all.
//
// The fix escapes rather than refuses, because both positions are free text as
// far as AMQP is concerned; refusing bytes in the user alone would have left
// the password broken. The assertion is on the dialled URL read back through
// the client library's own parser, which is the only thing that settles it.
func TestACredentialCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		dsn, user, pass string
	}{
		{"rabbitmq://u%2Fv:p@good-host:5672/prod", "u/v", "p"},
		{"rabbitmq://u%3Fv:p@good-host:5672/prod", "u?v", "p"},
		{"rabbitmq://u%23v:p@good-host:5672/prod", "u#v", "p"},
		{"rabbitmq://u%40v:p@good-host:5672/prod", "u@v", "p"},
		{"rabbitmq://u:aB%2FcD@good-host:5672/prod", "u", "aB/cD"},
		{"rabbitmq://u:a%3Fb@good-host:5672/prod", "u", "a?b"},
		{"rabbitmq://u:a%23b@good-host:5672/prod", "u", "a#b"},
		{"rabbitmq://u:a%25b@good-host:5672/prod", "u", "a%b"},
		{"rabbitmq://alice:pw@good-host:5672/prod", "alice", "pw"},
	} {
		cfg, err := rq.ParseDSN(row.dsn)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", row.dsn, err)
			continue
		}
		urls := rq.URLsForTest(cfg)
		if len(urls) != 1 {
			t.Errorf("ParseDSN(%q) built %d dial URLs, want 1", row.dsn, len(urls))
			continue
		}
		u, perr := amqp.ParseURI(urls[0])
		if perr != nil {
			t.Errorf("ParseDSN(%q) dials %q, which amqp091-go refuses: %v", row.dsn, urls[0], perr)
			continue
		}
		if u.Host != "good-host" || u.Port != 5672 {
			t.Errorf("ParseDSN(%q) dials %q, which reaches %s:%d -- the DSN names good-host:5672",
				row.dsn, urls[0], u.Host, u.Port)
		}
		if u.Username != row.user || u.Password != row.pass {
			t.Errorf("ParseDSN(%q) dials %q, which authenticates as %q/%q, want %q/%q",
				row.dsn, urls[0], u.Username, u.Password, row.user, row.pass)
		}
		if u.Vhost != "prod" {
			t.Errorf("ParseDSN(%q) dials %q, whose vhost is %q, want %q -- a credential must not end up in it",
				row.dsn, urls[0], u.Vhost, "prod")
		}
	}
}

// TestATypedAddressCannotRedirectTheDial is the typed twin of the test above,
// which nothing had: `dialableHost` ran only inside ParseDSN, so writing the
// struct instead of the string carried a host straight past the guard the DSN
// refuses.
func TestATypedAddressCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{
		"tok@real-host:5672", "a/b:5672", "a?b:5672", "a#b:5672",
		"a\\b:5672", "a b:5672", "a\tb:5672", "a\x7fb:5672", "a%b:5672",
		"amqp://a/b:5672", "amqps://tok@real-host:5671",
	} {
		cfg := rq.Config{Addrs: []string{addr}}
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate accepted the address %q: the AMQP URL built from it dials somewhere the "+
				"configuration does not name", addr)
		}
	}
	// What an address legitimately holds still validates.
	for _, addr := range []string{
		"h:5672", "amqp://h:5672", "amqps://h:5671", "[::1]:5672",
		"[fe80::1%25eth0]:5672", "a,b:5672", "broker-1.example:5672",
	} {
		ok := rq.Config{Addrs: []string{addr}}
		if err := ok.Validate(); err != nil {
			t.Errorf("Validate refused the address %q: %v", addr, err)
		}
	}
}

// TestAVhostSurvivesTheDialURL holds the escaping one component further right
// than reached: the vhost is the AMQP URL's PATH, and a
// space, a TAB or a DEL in it made amqp091-go refuse the whole URI -- "URI
// must not contain whitespace" / "net/url: invalid control character in URL"
// -- so the configuration was accepted and could not dial at all.
func TestAVhostSurvivesTheDialURL(t *testing.T) {
	t.Parallel()
	for _, vhost := range []string{"acme", "a b", "a\tb", "a\x7fb", "a/b", "a?b", "a#b", "a%b", "/"} {
		cfg := rq.Config{Addrs: []string{"h:5672"}, Vhost: vhost}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate refused the vhost %q: %v", vhost, err)
			continue
		}
		urls := rq.URLsForTest(cfg)
		u, perr := amqp.ParseURI(urls[0])
		if perr != nil {
			t.Errorf("the vhost %q builds the dial URL %q, which amqp091-go refuses: %v", vhost, urls[0], perr)
			continue
		}
		if u.Vhost != vhost {
			t.Errorf("the vhost %q builds %q, whose vhost reads back as %q", vhost, urls[0], u.Vhost)
		}
	}
}
