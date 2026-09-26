package amqp10_test

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/dsn"
)

// TestValidateRefusals is the specification's "validate config before starting background
// work" held to the whole table, with no I/O.
//
// Two of the rows are the ones this profile exists to explain — Declare and
// KeyAsRoutingKey are fields the AMQP 0-9-1 driver has and this protocol cannot
// honour — and both are refused rather than accepted and ignored, because the specification
// forbids silently dropping an unsupported option. The rest are the ordinary
// ones, and they are here because a configuration refused at the first frame
// instead of at construction is a refusal an operator reads as a broker
// problem.
func TestValidateRefusals(t *testing.T) {
	ok := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("the minimal configuration does not validate: %v", err)
	}
	for name, cfg := range map[string]rmq10.Config{
		"no endpoint":      {},
		"a blank endpoint": {Addrs: []string{"   "}},
		"a password and no user": {
			Addrs:    []string{"amqp://h:5672"},
			Password: dsn.Secret("s3cret"),
		},
		"skipping verification without TLS": {
			Addrs:              []string{"amqp://h:5672"},
			InsecureSkipVerify: true,
		},
		"a queue type this profile has no claims for": {
			Addrs:     []string{"amqp://h:5672"},
			QueueType: rmq10.QueueType(7),
		},
		"an exchange type RabbitMQ does not have": {
			Addrs:        []string{"amqp://h:5672"},
			ExchangeType: "topics",
		},
		"a declaration this protocol cannot make": {
			Addrs:   []string{"amqp://h:5672"},
			Declare: true,
		},
		"a per-message routing key an AMQP 1.0 link cannot carry": {
			Addrs:           []string{"amqp://h:5672"},
			KeyAsRoutingKey: true,
		},
		"a virtual host and a hostname, which write the same field": {
			Addrs:    []string{"amqp://h:5672"},
			Vhost:    "acme",
			HostName: "rabbit-1",
		},
		"a reserved topology prefix": {
			Addrs:  []string{"amqp://h:5672"},
			Prefix: "amq.",
		},
		"a control character in the virtual host": {
			Addrs: []string{"amqp://h:5672"},
			Vhost: "ac\nme",
		},
		"a negative credit ceiling": {
			Addrs:  []string{"amqp://h:5672"},
			Credit: -1,
		},
		"a negative dial timeout": {
			Addrs:       []string{"amqp://h:5672"},
			DialTimeout: -time.Second,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", name)
			}
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig && code != mq.CodeInvalidArgument {
				t.Errorf("the refusal of %s carries code %v; a configuration that cannot be used is "+
					"CodeInvalidConfig ", name, code)
			}
		})
	}
}

// TestDeclareRefusalNamesTheWayOut is the one refusal an operator is most
// likely to meet, so the message has to be useful rather than correct.
//
// A caller who moves an application from `drivers/rabbitmq` will have Declare
// set, and "unsupported" would leave them with a topology that does not exist
// and no idea whose job it is. The message says what the broker cannot do and
// names the three things that can do it.
func TestDeclareRefusalNamesTheWayOut(t *testing.T) {
	err := rmq10.Config{Addrs: []string{"amqp://h:5672"}, Declare: true}.Validate()
	if err == nil {
		t.Fatal("Config.Declare was accepted")
	}
	msg := err.Error()
	for _, want := range [...]string{"amqp:not-found", "drivers/rabbitmq", "management"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not mention %q: %q", want, msg)
		}
	}
}

// TestCoreConfigConversion is the module's real API, checked one translation at
// a time.
//
// Three of these are the reason a typed RabbitMQ configuration exists at all
// rather than a bare `core.Config` with a profile beside it, and each would be
// silently wrong if it were left to the caller: a virtual host written into the
// wrong field lands in the default vhost, an endpoint with no scheme is not an
// AMQP URL, and a credit ceiling is where the prefetch went.
func TestCoreConfigConversion(t *testing.T) {
	cfg := rmq10.Config{
		// Every address is plaintext: the conversion under test here is the
		// scheme-and-port normalisation, and a list that MIXES transports is
		// refused by the core  because a DSN carries one
		// `tls=` for the whole list. The mixed case has its own row below.
		Addrs:             []string{"rabbit-1", "rabbit-2:5673", "amqp://rabbit-3:5673", "[::1]:5672", "[fe80::1]"},
		Username:          "iqmq",
		Password:          dsn.Secret("s3cret"),
		Vhost:             "acme",
		ContainerID:       "orders-worker-7",
		Credit:            256,
		ReceiverSettle:    core.ReceiverSettleSecond,
		SASL:              core.SASLPlain,
		InsecurePlainSASL: true,
	}
	cc, err := cfg.CoreConfig()
	if err != nil {
		t.Fatalf("converting the configuration: %v", err)
	}
	if want := "vhost:acme"; cc.HostName != want {
		t.Errorf("the core's HostName is %q, want %q. AMQP 1.0 has no virtual-host field, so RabbitMQ 4 selects one "+
			"from the OPEN frame's hostname; a configuration whose vhost went anywhere else lands in the default "+
			"virtual host and publishes into the wrong topology", cc.HostName, want)
	}
	// The last two are the shape that decides where the host ends and the
	// port begins: an IPv6 literal's own colons are not port separators, so a
	// bracketed endpoint with no port still gets the protocol's default.
	want := []string{
		"amqp://rabbit-1:5672", "amqp://rabbit-2:5673", "amqp://rabbit-3:5673",
		"amqp://[::1]:5672", "amqp://[fe80::1]:5672",
	}
	if len(cc.Addrs) != len(want) {
		t.Fatalf("the core got %d endpoints, want %d: %v", len(cc.Addrs), len(want), cc.Addrs)
	}
	for i := range want {
		if cc.Addrs[i] != want[i] {
			t.Errorf("endpoint %d is %q, want %q", i, cc.Addrs[i], want[i])
		}
	}
	if cc.MaxCredit != 256 {
		t.Errorf("the core's MaxCredit is %d, want 256: credit is this protocol's answer to basic.qos and the only "+
			"window it has", cc.MaxCredit)
	}
	if cc.Password.Reveal() != "s3cret" || cc.Username != "iqmq" {
		t.Error("the credentials did not reach the core, so the connection would be anonymous")
	}
	if cc.ReceiverSettle != core.ReceiverSettleSecond {
		t.Error("the receiver settle mode did not reach the core, which decides whether a failed settlement is a lost " +
			"lease or an unknown outcome")
	}
	if err := cc.Validate(); err != nil {
		t.Errorf("the converted configuration does not satisfy the core: %v", err)
	}
}

// TestTLSEndpointGetsTheProtectedPort holds the other half of the endpoint
// conversion: a configured *tls.Config changes both the scheme and the default
// port, because 5671 and 5672 are the protocol's own and an operator who
// configured TLS did not also mean to name the plaintext port.
func TestTLSEndpointGetsTheProtectedPort(t *testing.T) {
	cfg := rmq10.Config{
		Addrs: []string{"rabbit-1"},
		TLS:   &tls.Config{MinVersion: tls.VersionTLS12},
	}
	cc, err := cfg.CoreConfig()
	if err != nil {
		t.Fatalf("converting the configuration: %v", err)
	}
	if want := "amqps://rabbit-1:5671"; cc.Addrs[0] != want {
		t.Errorf("the endpoint became %q, want %q", cc.Addrs[0], want)
	}
}

// TestRenderingsCarryNoCredential is, and it is checked on all three
// renderings because a structured logger reaches the fields directly rather
// than through String: a credential left in one of them reaches the log
// whatever the others do.
func TestRenderingsCarryNoCredential(t *testing.T) {
	const secret = "correct-horse-battery-staple"
	cfg := rmq10.Config{
		Addrs:    []string{"amqp://rabbit-1:5672"},
		Username: "iqmq",
		Password: dsn.Secret(secret),
		Vhost:    "acme",
	}
	for name, rendered := range map[string]string{
		"String": cfg.String(),
		"DSN":    cfg.DSN(),
		"LogValue": func() string {
			var b strings.Builder
			h := slog.NewTextHandler(&b, nil)
			slog.New(h).Info("cfg", slog.Any("config", cfg))
			return b.String()
		}(),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s carries the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, "acme") {
			t.Errorf("%s does not carry the virtual host, so the rendering cannot be read against the deployment: %s",
				name, rendered)
		}
	}
}

// TestZeroConfigIsNotUsable is the shape every driver in this repository has:
// the zero value names no endpoint and is refused, so there is no accidental
// default that reaches a broker somebody did not name.
func TestZeroConfigIsNotUsable(t *testing.T) {
	if err := (rmq10.Config{}).Validate(); err == nil {
		t.Fatal("the zero Config validates, so a caller who configured nothing would dial something")
	}
}

// TestDefaultsAreResolvedAndDocumented holds the two defaults this module owns
// rather than inherits: the exchange type a topology is assumed to have, and
// the credit window.
//
// They are checked through the conversion and the rendering because that is
// where a caller sees them: a default that never reached the core would leave
// the credit at the core's own number, and a caller reading the rendered
// configuration would be told something untrue about their own deployment.
func TestDefaultsAreResolvedAndDocumented(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://h:5672"}}
	cc, err := cfg.CoreConfig()
	if err != nil {
		t.Fatalf("converting the configuration: %v", err)
	}
	if cc.MaxCredit != rmq10.DefaultCredit {
		t.Errorf("the resolved credit is %d, want DefaultCredit (%d)", cc.MaxCredit, rmq10.DefaultCredit)
	}
	if got := cfg.String(); !strings.Contains(got, "exchange="+rmq10.DefaultExchangeType) {
		t.Errorf("the rendering does not show the resolved exchange type: %s", got)
	}
}

// TestConfigRedactsEverywhere is the per-driver half of the redaction
// promise, and this profile was the one driver package of sixteen without it.
//
// `make security-check`'s `configs` check proves the two renderings EXIST on
// all eighteen connection configurations; whether they REDACT is a per-driver
// test, because the check cannot know which field is the credential. Fifteen
// packages had one and this did not, so "the password never reaches a log"
// was an unproved claim here — true, as it turns out, because both renderings
// delegate to `DSN()` and that redacts, but unproved is the state this
// repository has learned not to leave a security claim in.
//
// The delegation is exactly why the test matters rather than why it is
// unnecessary: a future field added to `String` or `LogValue` directly, past
// the DSN, would leak and nothing would say so.
// sprintf is fmt.Sprintf, named so that the redaction test reads as prose.
func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }

func TestConfigRedactsEverywhere(t *testing.T) {
	const secret = "s3cr3t-do-not-print"
	cfg := rmq10.Config{
		Addrs:    []string{"amqps://host:5671"},
		Username: "user",
		Password: dsn.Secret(secret),
		Vhost:    "acme",
	}
	for name, rendered := range map[string]string{
		"String": cfg.String(),
		"DSN":    cfg.DSN(),
		"LogValue": func() string {
			var b strings.Builder
			slog.New(slog.NewTextHandler(&b, nil)).Info("cfg", "config", cfg)
			return b.String()
		}(),
		"the %v verb": func() string {
			// A caller who formats the struct with %v gets String(),
			// and a caller who reaches for %+v on a struct that has
			// one gets it too. It is the accident the interface
			// exists to prevent, so it is asserted rather than
			// assumed.
			return strings.Join([]string{
				strings.TrimSpace(sprintf("%v", cfg)),
				strings.TrimSpace(sprintf("%+v", cfg)),
			}, " ")
		}(),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s rendered the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, dsn.Redacted) {
			t.Errorf("%s did not render the redaction, so a reader cannot tell a password-less "+
				"configuration from one with a password: %s", name, rendered)
		}
	}

	// The compile-time surfaces, which are what keep a future field from
	// escaping through a formatting verb.
	var _ interface{ String() string } = cfg
	var _ interface{ LogValue() slog.Value } = cfg
}

// TestTheRenderedTransportMatchesTheDial holds this wrapper's own renderer to
// the rule the specification set for the core, which it did not inherit: it
// carries a `Config.DSN` of its own, and that one rendered `tls=` from
// `TLS != nil` while `renderAddr` stripped the scheme -- so an `amqps://`
// configuration rendered a PLAINTEXT DSN.
//
// The mixed-transport row is the other half: such a list has no rendering
// that describes both endpoints, so the core refuses it rather than letting
// the round trip downgrade the protected one.
func TestTheRenderedTransportMatchesTheDial(t *testing.T) {
	for _, row := range []struct {
		name  string
		addrs []string
		tls   bool
	}{
		{"amqps in the address", []string{"amqps://rabbit-1:5671"}, true},
		{"amqp+ssl in the address", []string{"amqp+ssl://rabbit-1:5671"}, true},
		{"plain address", []string{"amqp://rabbit-1:5672"}, false},
		{"bare address", []string{"rabbit-1:5672"}, false},
	} {
		cfg := rmq10.Config{Addrs: row.addrs}
		rendered := cfg.DSN()
		if got := strings.Contains(rendered, "tls=true"); got != row.tls {
			t.Errorf("%s: %v renders as %q, whose tls= is %v, want %v -- a rendering must not lose the "+
				"transport the address carries", row.name, row.addrs, rendered, got, row.tls)
		}
	}
	mixed := rmq10.Config{Addrs: []string{"amqps://a:5671", "amqp://b:5672"}}
	cc, err := mixed.CoreConfig()
	if err == nil {
		err = cc.Validate()
	}
	if err == nil {
		t.Errorf("a mixed-transport list was accepted; it renders as %q, which carries one tls= for both "+
			"endpoints and so describes one of them wrongly", mixed.DSN())
	}
}
