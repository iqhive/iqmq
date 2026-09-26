package rabbitmq_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// TestParseDSN covers the grammar this driver adds to the shared parser.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name  string
		in    string
		check func(*testing.T, rq.Config)
	}{
		{
			"minimal", "rabbitmq://localhost",
			func(t *testing.T, c rq.Config) {
				if len(c.Addrs) != 1 || c.Addrs[0] != "localhost:5672" {
					t.Errorf("addrs = %v, want the default port applied", c.Addrs)
				}
			},
		},
		{
			"credentials and vhost", "rabbitmq://u:p@host:5673/prod",
			func(t *testing.T, c rq.Config) {
				if c.Username != "u" || c.Password.Reveal() != "p" || c.Vhost != "prod" {
					t.Errorf("user/password/vhost = %q/%q/%q", c.Username, c.Password.Reveal(), c.Vhost)
				}
			},
		},
		{
			"several endpoints", "rabbitmq://a,b:5673,c",
			func(t *testing.T, c rq.Config) {
				want := []string{"a:5672", "b:5673", "c:5672"}
				if len(c.Addrs) != 3 {
					t.Fatalf("addrs = %v, want %v", c.Addrs, want)
				}
				for i := range want {
					if c.Addrs[i] != want[i] {
						t.Errorf("addr %d = %q, want %q", i, c.Addrs[i], want[i])
					}
				}
			},
		},
		{
			"tls applies the tls port", "rabbitmq://host?tls=true",
			func(t *testing.T, c rq.Config) {
				if c.TLS == nil {
					t.Fatal("tls=true did not build a TLS configuration")
				}
				if c.Addrs[0] != "host:5671" {
					t.Errorf("addr = %q, want the AMQPS port", c.Addrs[0])
				}
			},
		},
		{
			"families", "rabbitmq://host?confirm=windowed&confirmwindow=8&delay=plugin",
			func(t *testing.T, c rq.Config) {
				if c.ConfirmKind != confirm.Windowed || c.ConfirmWindow != 8 {
					t.Errorf("confirm = (%v, %d)", c.ConfirmKind, c.ConfirmWindow)
				}
				if c.DelayKind != delay.Plugin {
					t.Errorf("delay = %v", c.DelayKind)
				}
			},
		},
		{
			"delay ladder", "rabbitmq://host?delayladder=1s;30s;5m",
			func(t *testing.T, c rq.Config) {
				want := []time.Duration{time.Second, 30 * time.Second, 5 * time.Minute}
				if len(c.DelayLadder) != len(want) {
					t.Fatalf("ladder = %v, want %v", c.DelayLadder, want)
				}
				for i := range want {
					if c.DelayLadder[i] != want[i] {
						t.Errorf("bucket %d = %v, want %v", i, c.DelayLadder[i], want[i])
					}
				}
			},
		},
		{
			"topology", "rabbitmq://host?declare=true&deadletter=true&queuetype=classic&prefix=iqmq.",
			func(t *testing.T, c rq.Config) {
				if !c.Declare || !c.DeadLetter || c.QueueType != rq.QueueClassic || c.Prefix != "iqmq." {
					t.Errorf("topology = %+v", c)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := rq.ParseDSN(tc.in)
			if err != nil {
				t.Fatalf("ParseDSN(%q) = %v", tc.in, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestParseDSNRefusals is the other direction, and the strict-parameter rule
// is the one that matters most: a parameter this driver does not have is a
// construction error naming it, not a setting nobody notices was ignored.
func TestParseDSNRefusals(t *testing.T) {
	for _, tc := range []struct{ name, in string }{
		{"wrong scheme", "amqp://host"},
		{"unknown parameter", "rabbitmq://host?nonesuch=1"},
		{"unknown confirm kind", "rabbitmq://host?confirm=nonesuch"},
		{"unknown delay kind", "rabbitmq://host?delay=nonesuch"},
		{"bad ladder", "rabbitmq://host?delayladder=1s;banana"},
		{"insecure without tls", "rabbitmq://host?insecureskipverify=true"},
		{"reserved prefix", "rabbitmq://host?prefix=amq."},
		{"absurd priority", "rabbitmq://host?maxpriority=300"},
		{"absurd channel max", "rabbitmq://host?channelmax=70000"},
		{"dead letter without declare", "rabbitmq://host?deadletter=true"},
		{"window with per-message confirms", "rabbitmq://host?confirm=permessage&confirmwindow=8"},
		{"no host", "rabbitmq://"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := rq.ParseDSN(tc.in)
			if err == nil {
				t.Fatalf("ParseDSN(%q) = %+v, want a refusal", tc.in, cfg)
			}
			switch code := mq.CodeOf(err); code {
			case mq.CodeInvalidConfig, mq.CodeInvalidArgument, mq.CodeDriverNotRegistered:
			default:
				t.Fatalf("ParseDSN(%q) refused with %v: %v", tc.in, code, err)
			}
		})
	}
}

// TestDSNRoundTripsRedacted is the round trip: everything but the password
// survives, and the password cannot.
//
// The password is the whole point of the asymmetry. A DSN that round-tripped a
// credential would be a credential in every log line that printed a
// configuration, which the specification forbids; one that dropped the user name too would
// not round-trip at all.
func TestDSNRoundTripsRedacted(t *testing.T) {
	const in = "rabbitmq://u:p@host:5673/prod?confirm=windowed&confirmwindow=8&declare=true&deadletter=true&delay=plugin&delayladder=1s;30s&heartbeat=15s&prefix=iqmq.&queuetype=classic&tls=true"
	first, err := rq.ParseDSN(in)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	rendered := first.DSN()
	if strings.Contains(rendered, ":p@") {
		t.Fatalf("the rendered DSN carries the password: %s", rendered)
	}
	if !strings.Contains(rendered, dsn.Redacted) {
		t.Fatalf("the rendered DSN does not mark the password as redacted: %s", rendered)
	}
	second, err := rq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("ParseDSN(%q) = %v: the rendered form must parse", rendered, err)
	}
	// Everything but the password is the same. The password is the redaction
	// marker, which is a value and not the original: a caller who wants to
	// re-open has to supply it again, which is the point.
	first.Password, second.Password = "", ""
	if got, want := second.DSN(), first.DSN(); got != want {
		t.Fatalf("the DSN does not round-trip:\n  first  %s\n  second %s", want, got)
	}
	if second.Password.Reveal() == "p" {
		t.Fatal("the password survived the redaction")
	}
}

// TestRegisterRefusesANilRegistry and the scheme itself.
func TestRegisterRefusesANilRegistry(t *testing.T) {
	if err := rq.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	if rq.Scheme != "rabbitmq" {
		t.Fatalf("Scheme = %q", rq.Scheme)
	}
	// "amqp" is deliberately not claimed: four brokers in this repository
	// speak it, so a registry entry under it would be a race between
	// whichever driver registered first.
	if _, err := rq.ParseDSN("amqp://host"); err == nil {
		t.Fatal("this driver answered the \"amqp\" scheme, which three other brokers also speak")
	}
}
