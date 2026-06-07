package stomp_test

import (
	"crypto/tls"
	"reflect"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	stomp "bitbucket.org/iqhive/iqmq/drivers/activemq/stomp"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// roundTrippable strips the fields a round trip cannot carry, so that everything
// left is something the DSN really is responsible for.
//
// Two of them are worth naming. `Receipts` is a DERIVED field: the wire form
// is a tristate (absent, true, false) and the struct is two booleans, so
// `Config.resolve` is what computes `Receipts` from `NoReceipts` and
// `ParseDSN` deliberately leaves it alone — comparing it would assert that the
// parser does the resolver's job. `Clock` and `Hooks` hold functions, which no
// connection string carries and `reflect.DeepEqual` cannot compare anyway.
func roundTrippable(c stomp.Config) stomp.Config {
	c.Receipts = false
	c.Clock = nil
	c.Hooks = mq.Hooks{}
	// The passcode is compared separately and deliberately, because what the
	// round trip does with it is the one interesting thing about it: see
	// [TestDSNRoundTrip].
	c.Passcode = ""
	return c
}

// TestDSNRoundTrip is the round trip: what a configuration renders must
// parse back to the same configuration, with the passcode gone.
//
// The passcode is the whole reason the round trip is stated as "equal EXCEPT
// the secret". The specification forbids a credential in anything iqmq renders, so
// `Config.DSN` writes [dsn.Redacted] where the passcode was — which means the
// round trip is deliberately lossy in exactly one field, and a test that
// compared the structs outright would either fail or be evidence that the
// secret survived.
//
// What comes back in that field is the LITERAL redaction and not an empty
// string, and that is the repository's convention rather than this driver's
// choice: nothing in `dsn` strips `redacted` on the way in, and every driver
// that renders a credential behaves the same way. It is asserted rather than
// assumed, in both halves — the secret must not survive, and what replaced it
// must be the fixed redaction and never a length hint, because "xxxx" for a
// four-character password tells a reader the length.
func TestDSNRoundTrip(t *testing.T) {
	t.Parallel()
	defaults := func(c stomp.Config) stomp.Config {
		// The renderer resolves before it writes, so a configuration that
		// names none of these renders the defaults and parses them back.
		// Setting them here is what makes the comparison structural rather
		// than a re-implementation of `resolve`.
		c.Addrs = orDefault(c.Addrs, []string{"localhost:" + stomp.DefaultPort})
		c.Prefetch = stomp.DefaultPrefetch
		c.ConsumerWindow = stomp.DefaultConsumerWindow
		c.ReceiptTimeout = stomp.DefaultReceiptTimeout
		c.DialTimeout = stomp.DefaultDialTimeout
		return c
	}
	for name, cfg := range map[string]stomp.Config{
		"the least a configuration can say": defaults(stomp.Config{Broker: activemq.Artemis}),
		"classic":                           defaults(stomp.Config{Broker: activemq.Classic}),
		"a JMS selector, which carries = and ' and therefore exercises the escaping": defaults(stomp.Config{
			// This is the case the escaping exists for. A selector is a JMS
			// expression, so it carries "=" and "'" -- both of which mean
			// something to the shared parser -- and it is only valid on
			// Classic, because Artemis ignores a selector on a fully
			// qualified queue name and Config.Validate refuses one there.
			Broker:   activemq.Classic,
			Selector: "colour = 'red' AND size > 10",
		}),
		"a credential, redacted": defaults(stomp.Config{
			Broker:             activemq.Artemis,
			Login:              "artemis",
			Passcode:           dsn.Secret("hunter2"),
			InsecurePlainLogin: true,
		}),
		"a failover list": defaults(stomp.Config{
			Broker: activemq.Artemis,
			Addrs:  []string{"a.example:61613", "b.example:61613", "c.example"},
		}),
		"everything a connection string can express": defaults(stomp.Config{
			Broker:             activemq.Classic,
			Addrs:              []string{"broker.example:61614"},
			Login:              "app",
			Host:               "vhost.example",
			Prefix:             "tenant-a.",
			Selector:           "type = 'order'",
			Scheduler:          true,
			DeadLetterTopic:    "orders-dead",
			InsecureSkipVerify: true,
			InsecurePlainLogin: true,
			NoReconnect:        true,
			PrefetchBytes:      1 << 20,
			HeartbeatSend:      10 * time.Second,
			HeartbeatReceive:   20 * time.Second,
			ReconnectBase:      100 * time.Millisecond,
			ReconnectMax:       30 * time.Second,
			ReconnectGrowth:    1.75,
			ReconnectAttempts:  12,
			Metadata:           mq.MetadataPortableEnvelope,
			TLS:                &tls.Config{MinVersion: tls.VersionTLS12},
		}),
		"receipts turned off": defaults(stomp.Config{
			Broker:     activemq.Artemis,
			NoReceipts: true,
		}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rendered := cfg.DSN()
			if secret := cfg.Passcode.Reveal(); secret != "" && strings.Contains(rendered, secret) {
				t.Fatalf("the rendered DSN carries the passcode in clear: %q", rendered)
			}
			got, err := stomp.ParseDSN(rendered)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", rendered, err)
			}
			// The specification: the secret does not survive the rendering, which is the
			// one field the round trip is allowed to lose.
			secret := cfg.Passcode.Reveal()
			switch {
			case secret == "" && got.Passcode != "":
				t.Errorf("a configuration with no passcode round-tripped to one with %q; every password-less DSN "+
					"would then look like it had a credential", got.Passcode.Reveal())
			case secret != "" && got.Passcode.Reveal() == secret:
				t.Errorf("the passcode survived the round trip: %q reached a parsed Config, so a rendered DSN "+
					"stored or logged anywhere carries the credential", secret)
			case secret != "" && got.Passcode.Reveal() != dsn.Redacted:
				t.Errorf("the round trip left %q in the passcode, want the fixed redaction %q: a redaction that "+
					"varied with the secret would leak its length", got.Passcode.Reveal(), dsn.Redacted)
			}
			want := cfg
			if !reflect.DeepEqual(roundTrippable(got), roundTrippable(want)) {
				t.Errorf("the round trip through %q changed the configuration:\n got %+v\nwant %+v",
					rendered, roundTrippable(got), roundTrippable(want))
			}
			// And the rendering is stable: a DSN that parsed back to a
			// different configuration would also render differently, and a
			// caller who stored the string would open a different client
			// next time.
			if again := got.DSN(); again != rendered {
				t.Errorf("re-rendering is not stable:\n first %q\nsecond %q", rendered, again)
			}
		})
	}
}

// orDefault is the addresses or the resolved default.
func orDefault(got, def []string) []string {
	if len(got) == 0 {
		return def
	}
	return got
}

// TestDSNRefusals holds `ParseDSN` to the rule that a connection string is
// refused rather than partly understood.
//
// A parameter nobody reads is the case that matters most: a caller who wrote
// `prefetchsize=64` instead of `prefetch=64` has asked for something and a
// parser that ignored it would run with a different prefetch for ever. So the
// refusal has to NAME the parameter, which is what `dsn.Params.Strict` is for.
func TestDSNRefusals(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		in   string
		says string
	}{
		"an unknown parameter is refused, naming it": {
			in:   "activemq+stomp://localhost:61613?broker=artemis&prefetchsize=64",
			says: "prefetchsize",
		},
		"a path names nothing": {
			// Neither ActiveMQ has virtual hosts, and STOMP's own `host`
			// header is a parameter here because it is not a path either. A
			// parser that ignored the path would silently drop whatever the
			// caller meant by it.
			in:   "activemq+stomp://localhost:61613/vhost?broker=artemis",
			says: "path",
		},
		"a DSN with no broker is refused": {
			// The one place the required field really is required: "" does
			// not parse as a broker, so the DSN path refuses what the struct
			// path cannot (activemq.Artemis is the zero value of
			// activemq.Broker).
			in:   "activemq+stomp://localhost:61613",
			says: "broker=artemis or broker=classic",
		},
		"a broker that is neither product is refused": {
			in:   "activemq+stomp://localhost:61613?broker=rabbitmq",
			says: "neither",
		},
		"another driver's scheme is not this driver's": {
			// the specification's "never silently substituted": `activemq` is
			// `drivers/activemq`'s, over AMQP 1.0, and the two capability
			// tables really differ -- there is no native nack over STOMP and
			// no selector over AMQP 1.0.
			in: "activemq://localhost:5672?broker=artemis",
		},
		"a selector on Artemis is refused through the DSN too": {
			in:   "activemq+stomp://localhost:61613?broker=artemis&selector=colour%3D%27red%27",
			says: "fully qualified queue name",
		},
		"a passcode over a plaintext address is refused through the DSN too": {
			in:   "activemq+stomp://user:pass@localhost:61613?broker=artemis",
			says: "InsecurePlainLogin",
		},
		"a metadata mode outside the four is refused": {
			in:   "activemq+stomp://localhost:61613?broker=artemis&metadata=whatever",
			says: "",
		},
		"a negative reconnect growth is refused": {
			in:   "activemq+stomp://localhost:61613?broker=artemis&reconnectgrowth=-1",
			says: "non-negative",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := stomp.ParseDSN(tc.in)
			if err == nil {
				t.Fatalf("ParseDSN accepted %q and produced %+v", tc.in, cfg)
			}
			if tc.says != "" && !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal does not say %q, so a caller cannot tell what to change: %v", tc.says, err)
			}
		})
	}
}

// TestDSNAccepts is the other direction: a gate that refused everything would
// pass the table above and be useless.
func TestDSNAccepts(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		in    string
		check func(*testing.T, stomp.Config)
	}{
		"the shortest useful DSN": {
			in: "activemq+stomp://localhost:61613?broker=artemis",
			check: func(t *testing.T, c stomp.Config) {
				if c.Broker != activemq.Artemis {
					t.Errorf("broker is %v, want artemis", c.Broker)
				}
			},
		},
		"a selector on Classic, percent-encoded": {
			in: "activemq+stomp://localhost:61613?broker=classic&selector=colour+%3D+%27red%27",
			check: func(t *testing.T, c stomp.Config) {
				if c.Selector != "colour = 'red'" {
					t.Errorf("selector is %q, want %q", c.Selector, "colour = 'red'")
				}
			},
		},
		"receipts=false turns them off": {
			in: "activemq+stomp://localhost:61613?broker=artemis&receipts=false",
			check: func(t *testing.T, c stomp.Config) {
				if !c.NoReceipts {
					t.Error("receipts=false did not turn receipts off. The wire form is a tristate and the struct " +
						"is two booleans because the struct's default is ON: a DSN that says nothing must keep it, " +
						"and receipts=false has to turn it off")
				}
			},
		},
		"receipts=true is the default and says so": {
			in: "activemq+stomp://localhost:61613?broker=artemis&receipts=true",
			check: func(t *testing.T, c stomp.Config) {
				if c.NoReceipts {
					t.Error("receipts=true turned receipts off")
				}
			},
		},
		"tls=true asks for the library's defaults": {
			in: "activemq+stomp://localhost:61613?broker=artemis&tls=true",
			check: func(t *testing.T, c stomp.Config) {
				if c.TLS == nil {
					t.Fatal("tls=true produced no TLS configuration")
				}
				if c.TLS.MinVersion != tls.VersionTLS12 {
					t.Errorf("the TLS floor is %#x, want TLS 1.2 (%#x)", c.TLS.MinVersion, tls.VersionTLS12)
				}
			},
		},
		"a failover list": {
			in: "activemq+stomp://a.example:61613,b.example:61613?broker=artemis",
			check: func(t *testing.T, c stomp.Config) {
				if len(c.Addrs) != 2 {
					t.Errorf("the address list is %v, want two entries", c.Addrs)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, err := stomp.ParseDSN(tc.in)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.in, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestRegister is ADR-012's registration, and the refusal that goes with it.
func TestRegister(t *testing.T) {
	t.Parallel()
	if err := stomp.Register(nil); err == nil {
		t.Error("Register accepted no registry")
	}
	r := &recordingRegistrar{}
	if err := stomp.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if r.scheme != stomp.Scheme {
		t.Errorf("the driver registered under %q, want %q", r.scheme, stomp.Scheme)
	}
	if stomp.Scheme != "activemq+stomp" {
		t.Errorf("the scheme is %q; it must not be \"activemq\", which drivers/activemq claims over AMQP 1.0: a "+
			"registry in which one scheme could open either protocol would make a connection string ambiguous "+
			"about which capability table the caller gets, and the two tables really differ", stomp.Scheme)
	}
	if r.open == nil {
		t.Error("the driver registered no opener")
	}
}

// recordingRegistrar is a registry that remembers what was registered.
type recordingRegistrar struct {
	scheme string
	open   dsn.OpenFunc
}

// Register records the registration.
func (r *recordingRegistrar) Register(scheme string, open dsn.OpenFunc) error {
	r.scheme, r.open = scheme, open
	return nil
}
