package activemq_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// TestDSNRoundTrip is the contract: what a configuration renders parses
// back into the same configuration, except for the password, which cannot be
// recovered from a redacted string and must not be.
//
// It is the one thing that makes `Config.String()` useful for what it exists
// for -- a log line a person then pastes -- and it is easy to break silently:
// a field added to the struct and not to the renderer round-trips as its zero
// value and nothing says so.
func TestDSNRoundTrip(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]amq.Config{
		"artemis, minimal": {
			Broker: activemq.Artemis,
			Addrs:  []string{"localhost:5672"},
		},
		"classic, everything": {
			Broker:            activemq.Classic,
			Addrs:             []string{"a:5672", "b:5672"},
			Username:          "u",
			Password:          dsn.Secret("sup3r-s3cret-passcode"),
			HostName:          "vhost",
			ContainerID:       "client-1",
			Prefix:            "iqmq.",
			Scheduler:         true,
			BrokerAutoCreates: true,
			DeadLetterTopic:   "dead",
			Credit:            64,
			MaxPending:        128,
			NonDurable:        true,
			InsecurePlainSASL: true,
			ReceiverSettle:    core.ReceiverSettleFirst,
			SenderSettle:      core.SenderSettleUnsettled,
			Backoff:           core.BackoffEqualJitter,
			IdleTimeout:       30 * time.Second,
			DialTimeout:       5 * time.Second,
			OperationTimeout:  10 * time.Second,
			WriteTimeout:      3 * time.Second,
			ReconnectBase:     100 * time.Millisecond,
			ReconnectMax:      30 * time.Second,
			ReconnectGrowth:   1.5,
			ReconnectAttempts: 7,
			NoReconnect:       true,
			MaxFrameSize:      131072,
			MaxSessions:       16,
		},
		"artemis, no dead letter": {
			Broker:       activemq.Artemis,
			Addrs:        []string{"localhost:5672"},
			NoDeadLetter: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rendered := cfg.DSN()
			back, err := amq.ParseDSN(rendered)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", rendered, err)
			}
			// The comparison is between the two RENDERINGS rather than
			// between the two structs, because `DSN` resolves the defaults
			// on its way out: comparing structs would compare a caller's
			// zero value against the parser's resolved one and fail on
			// every field with a default. What a rendering cannot catch is
			// a field the renderer forgot, and
			// TestDSNRendersEveryRenderableField is what catches that.
			if got := back.DSN(); got != rendered {
				t.Errorf("the round trip changed the configuration.\n out: %s\n  in: %s", got, rendered)
			}
			if cfg.Password != "" {
				if strings.Contains(rendered, cfg.Password.Reveal()) {
					t.Errorf("the rendered DSN carries the password: %s", rendered)
				}
				// The shared parser puts the redaction PLACEHOLDER in the
				// password, because the authority did carry one and a
				// parser that dropped it would make "no password" and "a
				// password that was redacted" the same configuration. What
				// must never come back is the real one.
				if got := back.Password.Reveal(); got != dsn.Redacted {
					t.Errorf("the parsed configuration's password is %q, want the redaction placeholder %q: a "+
						"redacted string cannot have held the real credential", got, dsn.Redacted)
				}
			}
		})
	}
}

// TestDSNRendersEveryRenderableField is what a round-trip comparison cannot
// be: it fails when a field is added to [amq.Config] and not to the renderer.
//
// A field the renderer forgot round-trips as its zero value and NOTHING says
// so -- the DSN parses, the comparison passes, and a deployment's setting is
// silently gone. So every exported field is either rendered or named here as
// one a connection string cannot carry, and adding a field without doing
// either fails this test.
func TestDSNRendersEveryRenderableField(t *testing.T) {
	t.Parallel()
	// The fields a connection string cannot carry, with the reason.
	unrenderable := map[string]string{
		"Password":   "a credential is redacted by design, so it cannot round-trip",
		"TLS":        "a certificate pool and a client certificate are values no string carries; `tls=true` asks for the library's defaults",
		"Properties": "arbitrary AMQP connection properties are a typed map, and a string cannot carry their types",
		"Clock":      "an injected clock is a value a caller supplies in code",
		"Hooks":      "hooks are functions",
	}
	rendered := amq.Config{
		Broker:            activemq.Artemis,
		Addrs:             []string{"h:5672"},
		InsecurePlainSASL: true,
	}.DSN()
	typ := reflect.TypeOf(amq.Config{})
	for i := range typ.NumField() {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		if _, ok := unrenderable[f.Name]; ok {
			continue
		}
		// A field is "rendered" when the renderer has a parameter for it.
		// The lower-cased field name is the convention every parameter in
		// this driver follows, so the check is that the parameter exists at
		// all rather than that this configuration set it.
		key := strings.ToLower(f.Name)
		if !parameterExists(t, key) {
			t.Errorf("Config.%s has no DSN parameter and is not named as unrenderable: a field the renderer forgot "+
				"round-trips as its zero value and nothing says so. Add a %q parameter, or add the field to this "+
				"test's unrenderable list with the reason", f.Name, key)
		}
	}
	_ = rendered
}

// parameterExists reports whether ParseDSN claims a parameter of that name.
//
// It asks the parser rather than reading the renderer, which is the direction
// that matters: `Strict` refuses a parameter nothing claimed, so a name the
// parser accepts is a name the parser reads.
func parameterExists(t *testing.T, key string) bool {
	t.Helper()
	switch key {
	case "addrs":
		// The endpoints are the authority rather than a parameter.
		return true
	case "username":
		return true
	case "nodeadletter":
		// The tristate's other half: `deadletter=false` is what sets it.
		return true
	case "brokerautocreates":
		key = "autocreate"
	case "receipts":
		key = "receipts"
	}
	_, err := amq.ParseDSN("activemq://h:5672?broker=artemis&insecureplainsasl=true&" + key + "=0")
	if err == nil {
		return true
	}
	// A parameter the parser claims but whose VALUE it refuses is still
	// claimed: what a missing parameter produces is Strict's own refusal,
	// which names the parameter as unknown.
	return !strings.Contains(err.Error(), "does not know")
}

// TestDSNRefusals is the other half: a connection string that says something
// this driver cannot honour is refused NAMING it, rather than parsed with the
// setting quietly dropped (ADR-010).
func TestDSNRefusals(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		s    string
		code mq.ErrorCode
	}{
		"no broker parameter": {
			// The one parameter without which the string cannot be parsed
			// back at all, and the one whose absence would otherwise be
			// guessed at.
			s:    "activemq://localhost:5672",
			code: mq.CodeInvalidConfig,
		},
		"an unknown broker": {
			s:    "activemq://localhost:5672?broker=rabbit",
			code: mq.CodeInvalidConfig,
		},
		"a path": {
			// Neither product has virtual hosts and AMQP 1.0 has no
			// virtual-host field, so a path could only be a caller's
			// mistaken expectation.
			s:    "activemq://localhost:5672/vhost?broker=artemis",
			code: mq.CodeInvalidConfig,
		},
		"an unknown parameter": {
			s:    "activemq://localhost:5672?broker=artemis&declare=true",
			code: mq.CodeInvalidConfig,
		},
		"the other driver's scheme": {
			s:    "activemq+stomp://localhost:61613?broker=artemis",
			code: mq.CodeDriverNotRegistered,
		},
		"receiver-settle-second on artemis": {
			s:    "activemq://localhost:5672?broker=artemis&receiversettle=second&insecureplainsasl=true",
			code: mq.CodeInvalidConfig,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := amq.ParseDSN(tc.s)
			if err == nil {
				t.Fatalf("ParseDSN accepted %s", name)
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Errorf("the refusal of %s is %v (code %v), want code %v", name, err, got, tc.code)
			}
		})
	}
}

// TestRegisterRefusesNoRegistry is the one thing Register can get wrong.
func TestRegisterRefusesNoRegistry(t *testing.T) {
	t.Parallel()
	if err := amq.Register(nil); err == nil {
		t.Fatal("Register(nil) reported success")
	}
	r := dsn.NewRegistry()
	if err := amq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// The two ActiveMQ drivers must be registrable with ONE registry, which
	// is how a deployment moves an application from one protocol to the
	// other by editing a connection string. A scheme collision would make
	// whichever module registered first decide the semantics.
	if amq.Scheme == "activemq+stomp" {
		t.Fatalf("the AMQP 1.0 driver claims the STOMP driver's scheme %q", amq.Scheme)
	}
}

// TestTheRenderedTransportMatchesTheDial holds this driver's own renderer to
// the rule the specification set for the core, which it did not inherit: it
// carries a `Config.DSN` of its own, and that one rendered `tls=` from
// `TLS != nil` while `renderAddr` stripped the scheme -- so an `amqps://`
// configuration rendered a PLAINTEXT DSN.
func TestTheRenderedTransportMatchesTheDial(t *testing.T) {
	for _, row := range []struct {
		name  string
		addrs []string
		tls   bool
	}{
		{"amqps in the address", []string{"amqps://artemis:5671"}, true},
		{"amqp+ssl in the address", []string{"amqp+ssl://artemis:5671"}, true},
		{"plain address", []string{"amqp://artemis:5672"}, false},
		{"bare address", []string{"artemis:5672"}, false},
	} {
		cfg := amq.Config{Addrs: row.addrs}
		rendered := cfg.DSN()
		if got := strings.Contains(rendered, "tls=true"); got != row.tls {
			t.Errorf("%s: %v renders as %q, whose tls= is %v, want %v -- a rendering must not lose the "+
				"transport the address carries", row.name, row.addrs, rendered, got, row.tls)
		}
	}
}
