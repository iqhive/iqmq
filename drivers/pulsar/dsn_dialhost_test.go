package pulsar_test

import (
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	pq "github.com/iqhive/iqmq/drivers/pulsar"
)

// TestADSNHostCannotRedirectTheDial holds ParseDSN to refusing a host that
// decodes to a byte the service URL it builds would give a meaning to.
//
// The shared parser percent-decodes a host and ParseDSN joins the results into
// "<scheme>://<host>,<host>" as [pq.Config.ServiceURL], which
// pulsar-client-go parses with its own service-URI grammar
// (pulsar/internal/service_uri.go). Measured against v0.19.0 by reading the
// client's own "Connecting to broker" lines and by counting the TCP
// connections that reached a local listener:
//
//	pulsar://tok%40real-host:6650   connected to pulsar://real-host:6650
//	pulsar://a%2Fb:6650             connected to pulsar://a:6650 (port lost)
//	pulsar://a%3Fb:6650             connected to pulsar://a:6650 (port lost)
//	pulsar://a%23b:6650             connected to pulsar://a:6650 (port lost)
//	pulsar://a%5Cb:6650             "Invalid service URL: InvalidConfiguration"
//	pulsar://nowhere.invalid%2C127.0.0.1:P   TWO brokers; 127.0.0.1:P was
//	                                         connected to, twice
//	pulsar://nowhere.invalid%3B127.0.0.1:P   the same, twice
//
// The last two are why this driver's set is not the set drivers/rabbitmq,
// drivers/amqp10, drivers/sqs and drivers/snssqs use: pulsar-client-go splits
// its host list on "," AND on ";" (splitURI in service_uri.go), and those four
// libraries split on neither. The separator that matters is the client
// library's.
//
// A host name or an IP literal holds none of these bytes, so such a host is
// refused rather than escaped: there is no spelling of it that dials what the
// DSN names.
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"pulsar://tok%40real-host:6650",
		"pulsar://u:p@tok%40real-host:6650",
		"pulsar://a%2Fb:6650",
		"pulsar://a%3Fb:6650",
		"pulsar://a%23b:6650",
		"pulsar://a%5Cb:6650",
		"pulsar://a%20b:6650",
		"pulsar://a%09b:6650",
		"pulsar://a%7Fb:6650",
		"pulsar://a%2Cb:6650",
		"pulsar://a%3Bb:6650",
		"pulsar://nowhere%2C127.0.0.1:6650",
		"pulsar://nowhere%3B127.0.0.1:6650",
		"pulsar://h:6650,tok%40real-host:6650",
		// "a%3A%2F%2Fb" decodes to "a://b", a second scheme inside the
		// service URL. The "/" refusal covers it.
		"pulsar://a%3A%2F%2Fb:6650",
		// A double-encoded host puts the escape back.
		"pulsar://tok%2540real-host:6650",
		"pulsar://a%25b:6650",
	} {
		_, err := pq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: its host decodes to a byte the service URL splits on or gives a "+
				"meaning to, so the dial would go somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
	// What a host legitimately holds still parses: an IPv6 literal, one with
	// a zone, and a name.
	for in, want := range map[string]string{
		"pulsar://[::1]:6650":            "pulsar://[::1]:6650",
		"pulsar://[fe80::1%25eth0]:6650": "pulsar://[fe80::1%25eth0]:6650",
		"pulsar://broker-1.example:6650": "pulsar://broker-1.example:6650",
		"pulsar://a.example,b.example":   "pulsar://a.example:6650,b.example:6650",
	} {
		cfg, err := pq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if cfg.ServiceURL != want {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.ServiceURL, want)
		}
	}
}

// TestALeaseGuardMustBeShorterThanTheEffectiveAckTimeout pins, which nothing watched: reverting it to the round-1 expression
// (`c.AckTimeout > 0 && c.LeaseGuard >= c.AckTimeout`) left the whole pulsar
// suite green.
//
// Validate read AckTimeout's zero as "no lease" while resolve reads it as
// DefaultAckTimeout, so `leaseguard=1h` alone resolved to AckTimeout=1m0s with
// LeaseGuard=1h0m0s -- precisely what errLeaseGuardTooLong describes -- and
// was ACCEPTED, while the same resolved pair written out in full was refused.
// Neither field is rendered, so the numeric axis structurally cannot reach
// this; the consequence is a lifecycle one, a guarded deadline already in the
// past for every delivery.
func TestALeaseGuardMustBeShorterThanTheEffectiveAckTimeout(t *testing.T) {
	t.Parallel()
	// Refused: the guard is at or past the EFFECTIVE ack timeout.
	for _, in := range []string{
		"pulsar://h:6650/t/n?leaseguard=1h",
		"pulsar://h:6650/t/n?leaseguard=1m",
		"pulsar://h:6650/t/n?leaseguard=5m&acktimeout=1m",
		"pulsar://h:6650/t/n?leaseguard=2h&acktimeout=2h",
	} {
		if _, err := pq.ParseDSN(in); err == nil {
			t.Errorf("ParseDSN(%q) was accepted: the resolved LeaseGuard is at or past the resolved AckTimeout, "+
				"so no delivery would ever be vouched for", in)
		}
	}
	// Accepted: the guard is shorter than the effective ack timeout.
	for _, in := range []string{
		"pulsar://h:6650/t/n?leaseguard=30s",
		"pulsar://h:6650/t/n?leaseguard=1h&acktimeout=2h",
		"pulsar://h:6650/t/n",
	} {
		if _, err := pq.ParseDSN(in); err != nil {
			t.Errorf("ParseDSN(%q) was refused: %v", in, err)
		}
	}
	// The typed path is the same configuration and must answer the same way.
	if err := (pq.Config{ServiceURL: "pulsar://h:6650", LeaseGuard: 5 * time.Minute}).Validate(); err == nil {
		t.Error("a typed LeaseGuard of 5m with AckTimeout unset validated; it resolves to AckTimeout=1m0s, " +
			"so every delivery's guarded deadline is already in the past")
	}
	if err := (pq.Config{ServiceURL: "pulsar://h:6650", LeaseGuard: 30 * time.Second}).Validate(); err != nil {
		t.Errorf("a typed LeaseGuard of 30s was refused: %v", err)
	}
}

// TestATypedServiceURLCannotRedirectTheDial is the typed twin of the test
// above, which nothing had: `dialableHost` ran only inside ParseDSN, so a
// typed ServiceURL carried a host straight past it.
func TestATypedServiceURLCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, url := range []string{
		"pulsar://tok@real-host:6650",
		"pulsar://a b:6650",
		"pulsar://a\tb:6650",
		"pulsar://a%b:6650",
		"pulsar://a\\b:6650",
		"pulsar://good:6650,tok@real-host:6650",
		"pulsar://good:6650;tok@real-host:6650",
	} {
		cfg := pq.Config{ServiceURL: url}
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate accepted the service URL %q: pulsar-client-go reads it with its own grammar, so "+
				"the dial would go somewhere the configuration does not name", url)
		}
	}
	for _, url := range []string{
		"pulsar://h:6650", "pulsar+ssl://h:6651", "pulsar://a:6650,b:6650",
		"pulsar://a:6650;b:6650", "pulsar://[::1]:6650", "pulsar://[fe80::1%25eth0]:6650",
	} {
		cfg := pq.Config{ServiceURL: url}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate refused the service URL %q: %v", url, err)
		}
	}
}
