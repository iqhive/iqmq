package rabbitmq_test

import (
	"log/slog"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// TestACredentialInATypedAddressIsRefused holds Validate to refusing an
// address that carries user information.
//
// A DSN carries ONE credential for every endpoint, so "amqp://u:s3cret@h:5672"
// in Addrs had no DSN spelling to come back from -- and it reached Config.DSN,
// Config.String and Config.LogValue as the bytes of the password, which the specification
// forbids. Every rendering surface is checked here, not just the one the fix
// went through: Config.url is what String and LogValue build from and
// Config.DSN is a second renderer, so a fix in either alone would have left
// the other leaking.
func TestACredentialInATypedAddressIsRefused(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-not-in-a-log"
	cfg := rq.Config{Addrs: []string{"amqp://u:" + secret + "@rabbit-1:5672"}, Vhost: "acme"}
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate accepted an address carrying a credential; it renders as %q", cfg.DSN())
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
	}
	// The refusal must be the one that NAMES the credential, not the generic
	// dial-host guard that also happens to catch an "@". Both refuse this
	// address, so removing the credential check alone left every test green
	// (review round 2's mutation audit) -- and the operator would then be
	// told their host holds a forbidden byte rather than where the password
	// goes.
	if !strings.Contains(err.Error(), "Username and Password") {
		t.Errorf("Validate = %v; the refusal should name where the credential goes, not describe the host", err)
	}
	// A plain address still validates, with and without a scheme.
	for _, addr := range []string{"rabbit-1:5672", "amqp://rabbit-1:5672", "amqps://rabbit-1:5671"} {
		ok := rq.Config{Addrs: []string{addr}}
		if err := ok.Validate(); err != nil {
			t.Errorf("Validate refused the plain address %q: %v", addr, err)
		}
	}
	// The configuration's own credential is redacted everywhere, which is
	// what makes the refusal above the right shape rather than the only one.
	withCred := rq.Config{Addrs: []string{"rabbit-1:5672"}, Username: "u", Password: secret}
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
