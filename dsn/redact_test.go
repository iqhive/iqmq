package dsn_test

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/dsn"
)

// credentialParams are the DSN parameter names the shipped drivers actually
// read a credential from, as of:
//
//	drivers/nats, drivers/jetstream   token
//	drivers/nsq                       authsecret
//	drivers/pulsar                    token, authparams
//	drivers/sqs, drivers/snssqs       sessiontoken
//
// The list is here to say that the hazard is real rather than hypothetical.
// The predicate under test is a SUBSTRING rule, so it covers a driver that
// adds "apisecret" or "producerpassword" tomorrow without this list changing,
// which is the failure mode a name-by-name list has.
var credentialParams = []string{"token", "authsecret", "authparams", "sessiontoken"}

// TestConfigStringRedactsCredentialParameters holds [dsn.Config.String] to
// what its own documentation claims: "safe to put in a log line or an error".
func TestConfigStringRedactsCredentialParameters(t *testing.T) {
	const secret = "s3cr3t-token-value"
	for _, name := range credentialParams {
		raw := "natsjs://user:pw@127.0.0.1:4222/ORDERS?" + name + "=" + secret + "&stream=ORDERS"
		cfg, err := dsn.Parse(raw)
		if err != nil {
			t.Fatalf("Parse(%q): %v", raw, err)
		}
		rendered := cfg.String()
		if strings.Contains(rendered, secret) {
			t.Errorf("Config.String() = %q, which carries the %s parameter's value", rendered, name)
		}
		if want := name + "=" + dsn.Redacted; !strings.Contains(rendered, want) {
			t.Errorf("Config.String() = %q, want it to contain %q", rendered, want)
		}
		// A non-credential parameter is still rendered: the redaction has to
		// be narrow enough that the rendering still says what was configured.
		if !strings.Contains(rendered, "stream=ORDERS") {
			t.Errorf("Config.String() = %q, which dropped a parameter that is not a credential", rendered)
		}
		if strings.Contains(rendered, "pw") {
			t.Errorf("Config.String() = %q, which carries the authority's password", rendered)
		}
	}
}

// TestConfigValueRendersItself is the other half of the same defect, and the
// sharper one.
//
// [dsn.Config.String] had a POINTER receiver, so a Config VALUE did not
// implement fmt.Stringer and `%v` of one printed the struct: the Password
// field redacts itself, and Params' fields are unexported, so fmt printed
// them as three bare slices -- `{[token] [s3cr3t] [false]}`. The credential
// reached the output through the very verb the Secret type's documentation
// names as the hazard it exists to answer ("a Config printed with %v").
func TestConfigValueRendersItself(t *testing.T) {
	const secret = "s3cr3t-token-value"
	cfg, err := dsn.Parse("natsjs://user:pw@127.0.0.1:4222?token=" + secret)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var value any = cfg
	if _, ok := value.(fmt.Stringer); !ok {
		t.Errorf("a dsn.Config VALUE does not implement fmt.Stringer, so %%v of one prints its fields")
	}
	if _, ok := value.(slog.LogValuer); !ok {
		t.Errorf("a dsn.Config VALUE does not implement slog.LogValuer; the specification requires a safe LogValue")
	}
	// `fmt.Sprintf("%s", cfg)` is deliberately NOT here: with a pointer
	// receiver it was a vet failure of its own --
	//
	//   dsn/redact_test.go:86:16: fmt.Sprintf format %s has arg cfg of wrong
	//   type bitbucket.org/iqhive/iqmq/dsn.Config
	//
	// -- which is vet naming the defect from the other direction, and it
	// stops the package's tests from building rather than running them. %v
	// takes the same path and reports the leak instead of refusing to compile.
	for _, s := range []string{
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%v", &cfg),
		fmt.Sprintf("%v", struct{ C dsn.Config }{cfg}),
	} {
		if strings.Contains(s, secret) {
			t.Errorf("a printed Config carries the credential: %q", s)
		}
	}
}

// TestIsCredentialNameIsTheOneList makes the predicate public so that the CLI,
// a driver and this package cannot disagree about which parameter names carry
// a secret. Two copies of a list like this is the drift defect, not a
// redundancy.
func TestIsCredentialNameIsTheOneList(t *testing.T) {
	for _, name := range append([]string{
		"password", "Passwd", "SASL_PASSWORD", "apisecret", "producerpassword",
		"privatekey", "passphrase", "nkey", "credential",
	}, credentialParams...) {
		if !dsn.IsCredentialName(name) {
			t.Errorf("IsCredentialName(%q) = false, and a DSN carrying it would be rendered in full", name)
		}
	}
	for _, name := range []string{"stream", "region", "name", "prefix", "tls", "keyprefix", "keyasroutingkey"} {
		if dsn.IsCredentialName(name) {
			t.Errorf("IsCredentialName(%q) = true, so a rendering would hide a setting that is not a secret", name)
		}
	}
}
