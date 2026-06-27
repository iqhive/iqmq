package amqp10_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	rmq10 "bitbucket.org/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestSchemeIsNeitherOfTheTwoObviousOnes is the specification's "never silently
// substituted" written down as a scheme.
//
// "rabbitmq" belongs to the AMQP 0-9-1 driver and "amqp10" to the shared
// core's generic profile, and a module that answered either would make a
// connection string ambiguous about which capability table the caller gets —
// which for this pair is not a theoretical difference: no request/reply, no
// delayed redelivery, no ordering by key, no wildcard subscription.
func TestSchemeIsNeitherOfTheTwoObviousOnes(t *testing.T) {
	if rmq10.Scheme != "rabbitmq+amqp10" {
		t.Fatalf("the scheme is %q", rmq10.Scheme)
	}
	for _, other := range [...]string{"rabbitmq", "amqp10", "amqp"} {
		if rmq10.Scheme == other {
			t.Errorf("this profile claims the scheme %q, which belongs to another module", other)
		}
		_, err := rmq10.ParseDSN(other + "://rabbit-1:5672")
		if !errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN accepted the scheme %q = %v, want ErrDriverNotRegistered", other, err)
		}
	}
}

// TestDSNRoundTrip is the round trip with the credential redacted.
//
// The rendering is not reversible for the password and must not be; everything
// else is, and the check is idempotence: what [rmq10.Config.DSN] renders,
// [rmq10.ParseDSN] reads, and rendering the result again produces the same
// string. That catches the failure a field-by-field comparison misses — a
// parameter that is rendered and not parsed, or parsed into a field the
// renderer does not read.
func TestDSNRoundTrip(t *testing.T) {
	cfg := rmq10.Config{
		Addrs:             []string{"rabbit-1:5672", "rabbit-2:5672"},
		Username:          "iqmq",
		Password:          dsn.Secret("s3cret"),
		Vhost:             "acme",
		ContainerID:       "orders-worker-7",
		Prefix:            "acme.",
		ExchangeType:      "topic",
		QueueType:         rmq10.QueueClassic,
		DeadLetter:        true,
		MaxPriority:       9,
		Credit:            256,
		MaxPending:        64,
		NonDurable:        true,
		ReceiverSettle:    core.ReceiverSettleSecond,
		SenderSettle:      core.SenderSettleUnsettled,
		SASL:              core.SASLPlain,
		InsecurePlainSASL: true,
		IdleTimeout:       30 * time.Second,
		MaxFrameSize:      128 << 10,
		MaxSessions:       32,
		DialTimeout:       5 * time.Second,
		OperationTimeout:  20 * time.Second,
		WriteTimeout:      15 * time.Second,
		Backoff:           core.BackoffEqualJitter,
		ReconnectBase:     250 * time.Millisecond,
		ReconnectMax:      20 * time.Second,
		ReconnectGrowth:   1.5,
		ReconnectAttempts: 7,
		NoReconnect:       false,
	}
	rendered := cfg.DSN()
	if strings.Contains(rendered, "s3cret") {
		t.Fatalf("the rendered DSN carries the password: %s", rendered)
	}
	if !strings.Contains(rendered, dsn.Redacted) {
		t.Errorf("the rendered DSN does not say a password was there: %s", rendered)
	}
	parsed, err := rmq10.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the rendered DSN does not parse back: %v\n%s", err, rendered)
	}
	if again := parsed.DSN(); again != rendered {
		t.Fatalf("the round trip is not stable.\nfirst:  %s\nsecond: %s", rendered, again)
	}
	// And the fields a caller would notice arrived.
	for name, cond := range map[string]bool{
		"the virtual host":       parsed.Vhost == "acme",
		"the topology prefix":    parsed.Prefix == "acme.",
		"the queue type":         parsed.QueueType == rmq10.QueueClassic,
		"the dead-letter route":  parsed.DeadLetter,
		"the priority ceiling":   parsed.MaxPriority == 9,
		"the credit window":      parsed.Credit == 256,
		"the receiver settle":    parsed.ReceiverSettle == core.ReceiverSettleSecond,
		"the sender settle":      parsed.SenderSettle == core.SenderSettleUnsettled,
		"the backoff policy":     parsed.Backoff == core.BackoffEqualJitter,
		"the reconnect attempts": parsed.ReconnectAttempts == 7,
		"the container id":       parsed.ContainerID == "orders-worker-7",
		"the two endpoints":      len(parsed.Addrs) == 2,
	} {
		if !cond {
			t.Errorf("%s did not survive the round trip: %s", name, rendered)
		}
	}
	// What comes back is the redaction MARKER and never the secret: the
	// marker is an ordinary string to the shared parser, which has no way to
	// know it stands for something it cannot recover. A caller who wants a
	// working configuration out of a rendered DSN supplies the password
	// again, which is the whole point of the rendering not being reversible.
	if got := parsed.Password.Reveal(); got != dsn.Redacted {
		t.Errorf("the password parsed back as %q; it must be the redaction marker and never the secret", got)
	}
}

// TestDSNRejectsAnUnknownParameter is ADR-010 a parameter this driver
// does not have is a construction error naming it, rather than a setting nobody
// notices was ignored.
//
// The two rows that matter are `declare` and `keyasroutingkey`. Both are
// parameters the AMQP 0-9-1 driver's DSN has, so a caller who copies a
// connection string across will carry them — and a driver that accepted and
// ignored them would leave that caller with a topology that does not exist and
// a key that is not the routing key, with nothing said.
func TestDSNRejectsAnUnknownParameter(t *testing.T) {
	for _, param := range [...]string{
		"declare=true",
		"keyasroutingkey=true",
		"confirm=windowed",
		"delay=ttldlx",
		"detectdelayplugin=true",
		"heartbeat=10s",
		"mandatory=true",
		"queuemaxlength=100",
		"consumertagprefix=iqmq",
		"nonsense=1",
	} {
		_, err := rmq10.ParseDSN("rabbitmq+amqp10://rabbit-1:5672?" + param)
		if err == nil {
			t.Errorf("the parameter %q was accepted; a setting a driver ignores is a setting nobody notices was "+
				"ignored", param)
			continue
		}
		key, _, _ := strings.Cut(param, "=")
		if !strings.Contains(err.Error(), key) {
			t.Errorf("the refusal of %q does not name it: %v", param, err)
		}
	}
}

// TestDSNRejectsAPath is the one place a DSN for this profile does not look
// like the other driver's, and refusing is the only honest answer.
//
// On AMQP 0-9-1 the URL's path IS the virtual host, so `rabbitmq://host/acme`
// is an idiom a caller will have. AMQP 1.0 has no virtual-host field at all, so
// there is nothing for a path to mean here; accepting and ignoring it would put
// the caller in the default virtual host with nothing said, which is the same
// class of mistake as accepting `declare`.
func TestDSNRejectsAPath(t *testing.T) {
	_, err := rmq10.ParseDSN("rabbitmq+amqp10://rabbit-1:5672/acme")
	if err == nil {
		t.Fatal("a DSN with a path was accepted, so a caller who moved a `rabbitmq://host/vhost` connection string " +
			"across would silently land in the default virtual host")
	}
	if !strings.Contains(err.Error(), "vhost") {
		t.Errorf("the refusal does not name the parameter that replaces the path: %v", err)
	}
	if _, err := rmq10.ParseDSN("rabbitmq+amqp10://rabbit-1:5672?vhost=acme"); err != nil {
		t.Fatalf("the parameter form was refused: %v", err)
	}
}

// TestDSNCarriesTheCoreParameters holds the split between the two parameter
// tables: the core reads its own and this profile reads its own, and Strict is
// called once at the end.
//
// A driver that called the core's own ParseDSN instead would have its own
// parameters rejected by the core's Strict, and one that re-implemented the
// core's table would have two tables to keep in step. So a DSN carrying one of
// each has to work.
func TestDSNCarriesTheCoreParameters(t *testing.T) {
	cfg, err := rmq10.ParseDSN("rabbitmq+amqp10://iqmq:pw@rabbit-1:5672" +
		"?vhost=acme&credit=32&receiversettle=second&sendersettle=mixed&backoff=decorrelated&insecureplainsasl=true&maxpending=8")
	if err != nil {
		t.Fatalf("parsing a DSN with parameters from both tables: %v", err)
	}
	if cfg.Vhost != "acme" || cfg.Credit != 32 {
		t.Errorf("this profile's parameters did not arrive: vhost=%q credit=%d", cfg.Vhost, cfg.Credit)
	}
	if cfg.ReceiverSettle != core.ReceiverSettleSecond || cfg.Backoff != core.BackoffDecorrelated || cfg.MaxPending != 8 {
		t.Errorf("the core's parameters did not arrive: receiversettle=%v backoff=%v maxpending=%d",
			cfg.ReceiverSettle, cfg.Backoff, cfg.MaxPending)
	}
}

// TestDSNRefusesAValueOutOfRange holds the one numeric parameter this profile
// narrows beyond the shared parser: `maxpriority` is a queue's
// `x-max-priority` and has to fit in a byte, and a clamp would hide a caller's
// mistake rather than answer it.
func TestDSNRefusesAValueOutOfRange(t *testing.T) {
	if _, err := rmq10.ParseDSN("rabbitmq+amqp10://h:5672?maxpriority=256"); err == nil {
		t.Fatal("maxpriority=256 was accepted; a priority ceiling is a byte and a clamp would hide the mistake")
	}
	if _, err := rmq10.ParseDSN("rabbitmq+amqp10://h:5672?maxpriority=255"); err != nil {
		t.Fatalf("maxpriority=255 was refused: %v", err)
	}
}

// TestRegisterWiresTheScheme is ADR-012's registry contract: a driver exposes
// Register and never reaches for a global registry of its own, so a library
// that composes two brokers registers each with a registry it owns.
func TestRegisterWiresTheScheme(t *testing.T) {
	r := dsn.NewRegistry()
	if err := rmq10.Register(r); err != nil {
		t.Fatalf("registering: %v", err)
	}
	if _, ok := r.Lookup(rmq10.Scheme); !ok {
		t.Fatalf("the scheme %q is not in the registry", rmq10.Scheme)
	}
	if err := rmq10.Register(r); err == nil {
		t.Error("registering twice was accepted, so a second registration could replace the first silently")
	}
	if err := rmq10.Register(nil); err == nil {
		t.Error("Register(nil) was accepted")
	}
	// The registry really opens through this driver: the DSN below names a
	// port nothing listens on, so what is checked is that the failure is a
	// DIAL failure rather than "no such scheme".
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := r.Open(ctx, "rabbitmq+amqp10://127.0.0.1:1?dialtimeout=200ms&sasl=none")
	if err == nil {
		t.Fatal("opening against a closed port succeeded")
	}
	if errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Errorf("the registry did not reach this driver: %v", err)
	}
}

// TestDSNAuthorityRoundTripsAByteThatIsAnEscape is the rendering hazard the
// shared core recorded and this one inherits.
//
// A host name that came out of a DSN's own percent-decoding can hold any byte
// at all, including the "%" that begins an escape. A rendering that emitted it
// raw would produce a connection string this driver's own parser refuses, which
// makes [rmq10.Config.DSN] useless for the one thing it exists for: a log line
// a person then pastes.
func TestDSNAuthorityRoundTripsAByteThatIsAnEscape(t *testing.T) {
	for _, host := range [...]string{"rab%bit", "rab@bit", "rab,bit", "::1", "[::1]"} {
		cfg := rmq10.Config{Addrs: []string{host + ":5672"}}
		if err := cfg.Validate(); err != nil {
			// A host this driver would not dial does not have to render: the
			// hazard is a host it WOULD dial whose rendering cannot be read
			// back.
			continue
		}
		rendered := cfg.DSN()
		if _, err := rmq10.ParseDSN(rendered); err != nil {
			t.Errorf("the DSN rendered for the host %q does not parse back: %v\n%s", host, err, rendered)
		}
	}
}

// TestDSNKeepsTheEndpointItWasGiven is the half of the authority round trip
// that "it parses back" does not cover: the DSN has to name the SAME endpoint.
//
// A dial address in [rmq10.Config.Addrs] is what
// [bitbucket.org/iqhive/iqmq/dsn.Host.String] produced, and `Config.DSN()` has to
// invert that bracketing exactly. It did not. For the host ":]" the dial
// address is `[:]]:5672`, the rendering split it at the FIRST "]" and then threw
// the remainder away, and the DSN said the host was ":" with no port — a log
// line naming a broker nobody configured, which rendered differently again on
// the next pass. FuzzRabbitAMQP10DSN found it as `f21062a831e024eb` (issue #50).
//
// The hosts here are the three shapes the inversion has to tell apart: one that
// the rendering brackets and whose bytes include the closing bracket, one whose
// own bytes are brackets that the rendering does NOT add (no colon, so no
// bracketing), and the ordinary IPv6 literal.
func TestDSNKeepsTheEndpointItWasGiven(t *testing.T) {
	for _, host := range [...]string{":]", "[a]b]", "::1", ":", "]0", "fe80::1%25eth0"} {
		want := "amqp://" + dsn.Host{Host: host, Port: "5672"}.String()
		cfg := rmq10.Config{Addrs: []string{want}}
		if err := cfg.Validate(); err != nil {
			continue
		}
		rendered := cfg.DSN()
		back, err := rmq10.ParseDSN(rendered)
		if err != nil {
			t.Errorf("the DSN rendered for the host %q does not parse back: %v\n%s", host, err, rendered)
			continue
		}
		if len(back.Addrs) != 1 || back.Addrs[0] != want {
			t.Errorf("the DSN rendered for the endpoint %q names the endpoints %q instead.\nrendered: %s",
				want, back.Addrs, rendered)
		}
		if second := back.DSN(); second != rendered {
			t.Errorf("the rendering for the endpoint %q is not stable.\nfirst:  %s\nsecond: %s", want, rendered, second)
		}
	}
}
