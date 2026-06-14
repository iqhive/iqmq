package rabbitmq_test

import (
	"strings"
	"testing"
	"time"

	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/internal/backoff"
)

// A rendered DSN has to parse back.
//
// It is rendered for a log line and for a conformance manifest, and parsed
// back by whoever reads them -- so every field the rendering emits has to
// survive the trip, and every character that means something to the parser
// has to be escaped on the way out.

// TestARenderedDSNParsesBackFieldForField.
//
// The configuration here sets the fields the renderer has a query parameter
// for, because a field it emits and cannot read back is worse than one it
// drops: the DSN looks complete and is wrong.
func TestARenderedDSNParsesBackFieldForField(t *testing.T) {
	t.Parallel()
	cfg := rq.Config{
		Addrs:         []string{"one:5672", "two:5672", "three:5672"},
		Username:      "operator",
		Vhost:         "/production",
		Prefix:        "app.",
		Declare:       true,
		Mandatory:     true,
		BackoffPolicy: backoff.Equal,
	}

	rendered := cfg.DSN()
	back, err := rq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the DSN this driver rendered does not parse back: %v (%q)", err, rendered)
	}

	if len(back.Addrs) != len(cfg.Addrs) {
		t.Errorf("the round trip kept %d of %d addresses: %v", len(back.Addrs), len(cfg.Addrs), back.Addrs)
	}
	for i := range back.Addrs {
		if i < len(cfg.Addrs) && back.Addrs[i] != cfg.Addrs[i] {
			t.Errorf("address %d came back as %q, want %q; the order is the reconnect order",
				i, back.Addrs[i], cfg.Addrs[i])
		}
	}
	if back.Username != cfg.Username {
		t.Errorf("the username came back as %q, want %q", back.Username, cfg.Username)
	}
	if back.Vhost != cfg.Vhost {
		t.Errorf("the vhost came back as %q, want %q", back.Vhost, cfg.Vhost)
	}
	if back.Prefix != cfg.Prefix {
		t.Errorf("the prefix came back as %q, want %q", back.Prefix, cfg.Prefix)
	}
	if !back.Declare {
		t.Error("Declare came back false; a deployment that turned declaration ON would find it off")
	}
	if !back.Mandatory {
		t.Error("Mandatory came back false")
	}
	if back.BackoffPolicy != cfg.BackoffPolicy {
		t.Errorf("the backoff policy came back as %v, want %v: a reconnect storm is diagnosed from this",
			back.BackoffPolicy, cfg.BackoffPolicy)
	}
}

// TestTheCharactersTheRenderingDOESEscapeSurviveTheTrip.
//
// The escaping here is deliberately minimal and config.go says why: "a name
// the broker accepts should read as itself in a log line". So the set is the
// characters that would END a parameter or a value -- `&=?#%` and a space in
// a query parameter, `/` and `%` in the vhost -- and nothing else.
//
// What that leaves is recorded in the maintenance notes rather than asserted
// here: the USERNAME is written raw, so one containing an `@` or a `?`
// produces a DSN that does not parse back. This test covers the set the
// rendering claims, so that shrinking it is a failure rather than a silent
// change.
func TestTheCharactersTheRenderingDOESEscapeSurviveTheTrip(t *testing.T) {
	t.Parallel()
	cfg := rq.Config{
		Addrs:  []string{"localhost:5672"},
		Vhost:  "tenant/one%two",
		Prefix: "a&b=c?d#e f",
	}

	rendered := cfg.DSN()
	back, err := rq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the rendered DSN does not parse back: %v (%q)", err, rendered)
	}
	if back.Vhost != cfg.Vhost {
		t.Errorf("the vhost came back as %q, want %q: \"/\" is the default vhost's whole name AND the path "+
			"separator, so it has to be encoded, and so does %% or the encoding is ambiguous",
			back.Vhost, cfg.Vhost)
	}
	if back.Prefix != cfg.Prefix {
		t.Errorf("the prefix came back as %q, want %q: every one of those characters would otherwise end "+
			"the parameter or its value", back.Prefix, cfg.Prefix)
	}
}

// TestTheRenderedDSNCarriesNoPassword is.
//
// A DSN is rendered for a log line and a manifest, and neither is a place for
// a secret -- so the round trip is lossy on purpose, and a test asserting the
// password came back would be asserting a leak.
func TestTheRenderedDSNCarriesNoPassword(t *testing.T) {
	t.Parallel()
	cfg := rq.Config{
		Addrs:    []string{"localhost:5672"},
		Username: "operator",
		Password: "s3cr3t-passphrase",
	}

	rendered := cfg.DSN()
	if strings.Contains(rendered, "s3cr3t-passphrase") {
		t.Errorf("the rendered DSN carries the password: %q", rendered)
	}
	back, err := rq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("parsing back: %v", err)
	}
	if back.Password.Reveal() == "s3cr3t-passphrase" {
		t.Error("the password survived the round trip; the rendering redacts it on purpose")
	}
}

// TestOpenDSNRefusesWhatItCannotParse, before it dials anything.
func TestOpenDSNRefusesWhatItCannotParse(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "not-a-dsn", "http://localhost:5672", "amqp://localhost:5672?prefetch=lots"} {
		if _, err := rq.OpenDSN(t.Context(), s); err == nil {
			t.Errorf("OpenDSN accepted %q", s)
		}
	}
}

// TestADurationIsRenderedSoThatItReadsBackAsItself.
//
// Every duration in this configuration is emitted and parsed by the same
// grammar, and a mismatch turns a thirty-second heartbeat into thirty
// nanoseconds -- which is a connection that drops immediately and an
// operator reading a DSN that looks right.
func TestADurationIsRenderedSoThatItReadsBackAsItself(t *testing.T) {
	t.Parallel()
	cfg := rq.Config{
		Addrs:       []string{"localhost:5672"},
		Heartbeat:   30 * time.Second,
		DialTimeout: 7 * time.Second,
	}

	back, err := rq.ParseDSN(cfg.DSN())
	if err != nil {
		t.Fatalf("parsing back: %v", err)
	}
	if back.Heartbeat != cfg.Heartbeat {
		t.Errorf("the heartbeat came back as %s, want %s", back.Heartbeat, cfg.Heartbeat)
	}
	if back.DialTimeout != cfg.DialTimeout {
		t.Errorf("the dial timeout came back as %s, want %s", back.DialTimeout, cfg.DialTimeout)
	}
}
