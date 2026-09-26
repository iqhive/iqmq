package amqp10_test

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
)

// TestADSNHostCannotRedirectTheDial holds ParseInto to refusing a host that
// decodes to a byte the AMQP URL it builds would give a meaning to.
//
// The shared parser percent-decodes a host and ParseInto writes the result
// into "amqp[s]://<host>:<port>", which go-amqp reads with net/url. Measured
// against go-amqp v1.7.0's own dialConn (conn.go, url.Parse then u.Hostname(),
// u.Port() and u.User):
//
//	amqp10://tok%40real-host:5672  dialled real-host AND, because a userinfo
//	                               in the URL becomes SASLTypePlain, sent SASL
//	                               PLAIN as "tok" over a cleartext connection
//	amqp10://a%2Fb:5672            dialled a, on port 5672, with "/b:5672" a path
//	amqp10://a%3Fb:5672            dialled a, on port 5672
//	amqp10://a%23b:5672            dialled a, on port 5672
//	amqp10://a%5Cb:5672            made url.Parse refuse the address at the dial
//
// The last one is the shape the refusal exists for: the configuration was
// accepted and failed later with an error about a string the operator never
// wrote. A host name or an IP literal holds none of these bytes.
//
// This is ParseInto's refusal, so it is also drivers/activemq's and
// drivers/rabbitmq/amqp10's -- they read their endpoints through it.
//
// A comma is NOT in the set: go-amqp dials one address, and the measurement
// kept "a,b" as net/url's host. That is where this driver differs from the
// NATS ones, whose client splits a server string on ",".
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"amqp10://tok%40real-host:5672",
		"amqp10://u:p@tok%40real-host:5672?tls=true",
		"amqp10://a%2Fb:5672",
		"amqp10://a%3Fb:5672",
		"amqp10://a%23b:5672",
		"amqp10://a%5Cb:5672",
		"amqp10://a%20b:5672",
		"amqp10://a%09b:5672",
		"amqp10://a%7Fb:5672",
		"amqp10://h:5672,tok%40real-host:5672",
		// "a%3A%2F%2Fb" decodes to "a://b", which the parser renders as a
		// bracketed literal and net/url then refuses. The "/" refusal covers
		// it at construction instead.
		"amqp10://a%3A%2F%2Fb:5672",
		// A double-encoded host puts the escape back.
		"amqp10://tok%2540real-host:5672",
		"amqp10://a%25b:5672",
	} {
		_, err := aq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: its host decodes to a byte the AMQP URL gives a meaning to, so the "+
				"dial would go somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
		// The refusal must be ParseInto's own, which names the DSN HOST --
		// not checkAddr's, which describes an Addrs entry. Both catch these,
		// so removing the ParseInto guard alone left every test green (review
		// round 2's mutation audit), and a DSN's author would then be told
		// about a field they did not write.
		if !strings.Contains(err.Error(), "a DSN host decodes to") {
			t.Errorf("ParseDSN(%q) = %v; a DSN host's refusal should name the DSN host", in, err)
		}
	}
	// What a host legitimately holds still parses: an IPv6 literal, one with
	// a zone, a name, and a comma -- which this driver does not split on.
	for in, want := range map[string]string{
		"amqp10://[::1]:5672":            "amqp://[::1]:5672",
		"amqp10://[fe80::1%25eth0]:5672": "amqp://[fe80::1%25eth0]:5672",
		"amqp10://broker-1.example:5672": "amqp://broker-1.example:5672",
		"amqp10://a%2Cb:5672":            "amqp://a,b:5672",
	} {
		cfg, err := aq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if len(cfg.Addrs) != 1 || cfg.Addrs[0] != want {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.Addrs, want)
		}
	}
}

// TestATypedAddressCannotRedirectTheDial pins `checkAddr`'s typed twins, which
// the DSN rows above cannot reach: the specification's "%" rule and
// the byte set both live there, and nothing watched either.
func TestATypedAddressCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{
		"amqp://a%b:5672", "a%b:5672", "amqps://tok%40real-host:5671",
		"amqp://tok@real-host:5672", "amqp://a/b:5672", "amqp://a?b:5672",
		"amqp://a#b:5672", "amqp://a\\b:5672",
	} {
		if err := (aq.Config{Addrs: []string{addr}}).Validate(); err == nil {
			t.Errorf("Validate accepted the address %q: go-amqp reads it with net/url, which either sends the "+
				"connection elsewhere or refuses the address at the dial", addr)
		}
	}
	for _, addr := range []string{
		"amqp://h:5672", "amqps://h:5671", "h:5672",
		"amqp://[::1]:5672", "amqp://[fe80::1%25eth0]:5672", "amqp://a,b:5672",
	} {
		if err := (aq.Config{Addrs: []string{addr}}).Validate(); err != nil {
			t.Errorf("Validate refused the address %q: %v", addr, err)
		}
	}
}

// TestAnAddressIsDialledWithAScheme pins the half that
// nothing watched: [aq.Config.resolve] writes a scheme into an address that
// has none.
//
// go-amqp refuses a bare "host:port" before it opens anything -- measured
// against v1.7.0, `Dial(ctx, "127.0.0.1:5672", ...)` returns
// `parse "127.0.0.1:5672": first path segment in URL cannot contain colon` --
// and `checkAddr` accepts one, so such an address failed at the dial with an
// error about a string the operator never wrote. The scheme also has to be
// the RIGHT one, because that is what the library picks the transport from.
func TestAnAddressIsDialledWithAScheme(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name string
		cfg  aq.Config
		want string
	}{
		{"bare address", aq.Config{Addrs: []string{"h:5672"}}, "amqp://h:5672"},
		{"bare address with a TLS config", aq.Config{
			Addrs: []string{"h:5671"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		}, "amqps://h:5671"},
		{"an address that states its own scheme keeps it", aq.Config{
			Addrs: []string{"amqps://h:5671"},
		}, "amqps://h:5671"},
		{"no address at all", aq.Config{}, aq.DefaultAddr},
	} {
		got := aq.ResolvedForTest(row.cfg).Addrs
		if len(got) != 1 || got[0] != row.want {
			t.Errorf("%s: resolves to %v, want [%s] -- go-amqp picks the transport from the scheme and "+
				"refuses an address that has none", row.name, got, row.want)
		}
	}
}
