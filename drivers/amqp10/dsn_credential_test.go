package amqp10_test

import (
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
)

// TestACredentialInATypedAddressIsRefused holds Validate to refusing an
// address that carries user information.
//
// go-amqp turns a userinfo in the dial URL into SASLTypePlain, and renderAddr
// ESCAPES a host rather than redacting it, so "amqps://u:s3cret@h:5671" in
// Addrs travelled -- escaped -- into Config.DSN, which is also Config.String
// and Config.LogValue. The specification forbids a credential in any of them, and the
// documented Addrs forms are "amqp://host:port" and "amqps://host:port".
func TestACredentialInATypedAddressIsRefused(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-not-in-a-log"
	cfg := aq.Config{Addrs: []string{"amqps://u:" + secret + "@broker-1:5671"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate accepted an address carrying a credential; it renders as %q", cfg.DSN())
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
	}
	for _, addr := range []string{"broker-1:5672", "amqp://broker-1:5672", "amqps://broker-1:5671"} {
		ok := aq.Config{Addrs: []string{addr}}
		if err := ok.Validate(); err != nil {
			t.Errorf("Validate refused the plain address %q: %v", addr, err)
		}
	}
	// The configuration's own credential is redacted everywhere.
	withCred := aq.Config{
		Addrs: []string{"amqps://broker-1:5671"}, Username: "u", Password: secret,
		SASL: aq.SASLPlain, TLS: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if err := withCred.Validate(); err != nil {
		t.Fatalf("Validate refused a credential in its own fields: %v", err)
	}
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("cfg", "config", withCred)
	for name, got := range map[string]string{
		"DSN": withCred.DSN(), "String": withCred.String(), "LogValue": b.String(),
	} {
		if strings.Contains(got, secret) {
			t.Errorf("%s carries the secret: %s", name, got)
		}
	}
}
