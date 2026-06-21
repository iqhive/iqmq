package activemq_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// The entry points, and the DSN escaping that makes a rendered configuration
// parse back.
//
// The Open family here is a thin shim over the AMQP core, and what it adds is
// the refusals: a nil connection, a configuration whose parts cannot be
// built, a DSN that does not parse. None of them ran.

// TestOpenWithNoConnectionIsRefused.
//
// Both entry points take a connection the CALLER owns, so there is nothing
// to dial and nothing to default: a nil is a programming error and has to be
// named as one rather than deferred to a nil dereference on first use.
//
// The refusal is asserted rather than located. The AMQP core refuses a nil
// connection too, so deleting the check here changes nothing a test can see
// -- both layers refusing is the point, since this shim is not the only way
// into that core.
func TestOpenWithNoConnectionIsRefused(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}}

	if _, err := amq.OpenWithClient(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("OpenWithClient accepted a nil connection")
	}
	if _, err := amq.BackendWithClient(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("BackendWithClient accepted a nil connection")
	}
}

// TestOpenRefusesAConfigurationItCannotBuildPartsFrom.
//
// The parts are the profile and the core's configuration, and building them
// is where this driver's own validation happens. A refusal here is a fast
// error naming the field; without it the core would be handed a profile
// nobody checked.
func TestOpenRefusesAConfigurationItCannotBuildPartsFrom(t *testing.T) {
	t.Parallel()
	bad := amq.Config{Broker: activemq.Broker(99), Addrs: []string{"localhost:5672"}}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Open", func() error { _, err := amq.Open(t.Context(), bad); return err }},
		{"OpenBackend", func() error { _, err := amq.OpenBackend(t.Context(), bad); return err }},
	} {
		if err := tc.call(); err == nil {
			t.Errorf("%s accepted a configuration naming no known broker", tc.name)
		}
	}
}

// TestOpenDSNRefusesWhatItCannotParse, before it dials anything.
func TestOpenDSNRefusesWhatItCannotParse(t *testing.T) {
	t.Parallel()
	for _, s := range []string{
		"",
		"not-a-dsn",
		"http://localhost:5672",         // the wrong scheme entirely
		"amqp://localhost:5672?broker=", // a broker parameter with no value
	} {
		if _, err := amq.OpenDSN(t.Context(), s); err == nil {
			t.Errorf("OpenDSN accepted %q", s)
		}
	}
}

// TestARenderedDSNParsesBackWithEveryCharacterThatMeansSomething.
//
// A DSN is rendered for a log line and for a conformance manifest, and it is
// parsed back by whoever reads them -- so a password containing an `@` or a
// `&` has to survive the round trip. Every character in the escape set is in
// this test, because the one that is missing is the one that will be in
// somebody's password.
func TestARenderedDSNParsesBackWithEveryCharacterThatMeansSomething(t *testing.T) {
	t.Parallel()
	const nasty = `p%+ @:/?#&=[],x`
	cfg := amq.Config{
		Broker:            activemq.Artemis,
		Addrs:             []string{"localhost:5672"},
		Username:          nasty,
		Password:          "s3cr3t",
		InsecurePlainSASL: true, // the rendered DSN has to parse back, and PLAIN over plaintext is refused otherwise
	}

	rendered := cfg.DSN()
	if strings.Contains(rendered, "@:/?#&") {
		t.Errorf("the rendered DSN carries the raw metacharacters: %q", rendered)
	}
	if !strings.Contains(rendered, "%") {
		t.Errorf("the rendered DSN escaped nothing: %q", rendered)
	}

	// The password is REDACTED and therefore does not survive, which is:
	// a DSN is rendered for a log line and a manifest, and neither is a place
	// for a secret. The round trip is lossy on purpose and this says so.
	if strings.Contains(rendered, "s3cr3t") {
		t.Errorf("the rendered DSN carries the password: %q", rendered)
	}

	back, err := amq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the DSN this driver rendered does not parse back: %v (%q)", err, rendered)
	}
	if back.Username != nasty {
		t.Errorf("the username survived the round trip as %q, want %q: a rendered DSN that does not parse "+
			"back is a log line nobody can act on", back.Username, nasty)
	}
	if back.Password.Reveal() == "s3cr3t" {
		t.Error("the password came back through the round trip; the rendering redacts it on purpose")
	}
}

// TestARenderedDSNLeavesAnOrdinaryValueAlone.
//
// The escape is skipped entirely when there is nothing to escape, which is
// the common case and the one a reader of a log line sees.
func TestARenderedDSNLeavesAnOrdinaryValueAlone(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{
		Broker:            activemq.Artemis,
		Addrs:             []string{"localhost:5672"},
		Username:          "artemis",
		InsecurePlainSASL: true,
	}

	rendered := cfg.DSN()
	if !strings.Contains(rendered, "artemis") {
		t.Errorf("the rendered DSN does not carry the username plainly: %q", rendered)
	}
	if strings.Contains(rendered, "%") {
		t.Errorf("the rendered DSN escaped a value that needs no escaping: %q", rendered)
	}
	back, err := amq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("parsing back: %v", err)
	}
	if back.Username != "artemis" {
		t.Errorf("the username came back as %q", back.Username)
	}
}

// TestClassicSuppliesNoAdminAndArtemisDoes.
//
// A nil Admin is load-bearing rather than lazy: the core asserts
// mq.AdminBackend once when the client is built, so a profile with no Admin
// makes `Client.Admin()` report ErrUnsupported instead of handing back a
// surface that refuses every call. Classic's management is JMX and a web
// console, neither of which is reachable over the connection the client has.
func TestClassicSuppliesNoAdminAndArtemisDoes(t *testing.T) {
	t.Parallel()
	classic := amq.Config{Broker: activemq.Classic, Addrs: []string{"localhost:5672"}}
	if got, err := amq.CapabilitiesForTest(classic, activemq.ServerClassic, "6.2.0"); err != nil {
		t.Fatalf("capabilities for Classic: %v", err)
	} else if got.Has(mq.FeatureAdmin) {
		t.Error("Classic claims FeatureAdmin; its management is JMX and a web console, neither reachable " +
			"over this connection")
	}

	artemis := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}}
	if got, err := amq.CapabilitiesForTest(artemis, activemq.ServerArtemis, "2.44.0"); err != nil {
		t.Fatalf("capabilities for Artemis: %v", err)
	} else if !got.Has(mq.FeatureAdmin) {
		t.Error("Artemis does not claim FeatureAdmin; its management address is reachable over the very " +
			"connection the client already has")
	}
}
