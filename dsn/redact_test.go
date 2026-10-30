package dsn_test

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/iqhive/iqmq/dsn"
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
	//   type github.com/iqhive/iqmq/dsn.Config
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

// everyVerb is the string verbs and a spread of the others, with a width, a
// precision and a flag, which is every path fmt takes to print a value.
var everyVerb = []string{
	"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%10s",
	"%d", "%t", "%c", "%b", "%o", "%U", "%e", "%08.3d",
}

// TestSecretAndConfigRedactUnderEveryVerb: fmt asks String only for the string
// verbs. Under %d, %t or %c it reports a bad verb and prints the value beside
// it without asking any method, so a Secret came out as
// "%!d(dsn.Secret=s3cr3t)" and a Config's parameters came out in full.
//
// %p and %w are not here because no method can answer them: fmt reports a bad
// verb for a value that is not a pointer, or not an error, before it looks for
// a method, and prints the value in the report.
// TestParamsHideTheirValuesFromTheVerbsNoMethodAnswers covers what can be
// hidden from them.
func TestSecretAndConfigRedactUnderEveryVerb(t *testing.T) {
	const secret = "s3cr3t-token-value"
	cfg, err := dsn.Parse("natsjs://user:" + secret + "@127.0.0.1:4222?token=" + secret + "&stream=ORDERS")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	args := map[string]any{
		"Secret":                    cfg.Password,
		"*Secret":                   &cfg.Password,
		"Config":                    cfg,
		"*Config":                   &cfg,
		"a struct holding a Secret": struct{ S dsn.Secret }{cfg.Password},
		"a struct holding a Config": struct{ C dsn.Config }{cfg},
	}
	hexed := hex.EncodeToString([]byte(secret))
	forms := []string{secret, hexed, strings.ToUpper(hexed)}
	for _, verb := range everyVerb {
		for name, arg := range args {
			got := fmt.Sprintf(verb, arg)
			for _, form := range forms {
				if strings.Contains(got, form) {
					t.Errorf("%s of a %s carries the credential: %q", verb, name, got)
				}
			}
		}
	}

	// The string verbs render what they did, so the redaction did not cost a
	// Config its rendering or a Secret its Go syntax.
	for verb, want := range map[string]string{
		"%v":   cfg.String(),
		"%s":   cfg.String(),
		"%q":   strconv.Quote(cfg.String()),
		"%#v":  cfg.String(),
		"%90s": fmt.Sprintf("%90s", cfg.String()),
	} {
		if got := fmt.Sprintf(verb, cfg); got != want {
			t.Errorf("%s of a Config is %q, want %q", verb, got, want)
		}
	}
	for verb, want := range map[string]string{
		"%v":   dsn.Redacted,
		"%s":   dsn.Redacted,
		"%q":   strconv.Quote(dsn.Redacted),
		"%#v":  cfg.Password.GoString(),
		"%10s": fmt.Sprintf("%10s", dsn.Redacted),
	} {
		if got := fmt.Sprintf(verb, cfg.Password); got != want {
			t.Errorf("%s of a Secret is %q, want %q", verb, got, want)
		}
	}
	if got := fmt.Sprintf("%s|%q", dsn.Secret(""), dsn.Secret("")); got != `|""` {
		t.Errorf("an empty Secret renders as %q under %%s and %%q, want it empty", got)
	}
}

// TestParamsHideTheirValuesFromTheVerbsNoMethodAnswers: under %w and %p fmt
// reports a bad verb and prints the value beside it with every method
// switched off, at every depth, so Params.Format is never asked. The values
// sit behind a pointer, which fmt prints below the top level as an address,
// so neither verb prints a parameter on its own or inside a Config. A Secret
// is a string, and nothing hides one from these two verbs; see Secret.Format.
func TestParamsHideTheirValuesFromTheVerbsNoMethodAnswers(t *testing.T) {
	const secret = "s3cr3t-token-value"
	cfg, err := dsn.Parse("natsjs://127.0.0.1:4222?token=" + secret + "&stream=ORDERS")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	args := map[string]any{
		"Params":                    cfg.Params,
		"*Params":                   &cfg.Params,
		"a struct holding a Params": struct{ P dsn.Params }{cfg.Params},
		"Config":                    cfg,
		"*Config":                   &cfg,
	}
	for _, verb := range []string{"%w", "%p"} {
		for name, arg := range args {
			if got := fmt.Sprintf(verb, arg); strings.Contains(got, secret) {
				t.Errorf("%s of a %s carries the credential: %q", verb, name, got)
			}
			if got := fmt.Errorf("open: "+verb, arg).Error(); strings.Contains(got, secret) {
				t.Errorf("Errorf %s of a %s carries the credential: %q", verb, name, got)
			}
		}
	}
	// The values are still there for the methods that are asked.
	if v, ok := cfg.Params.Lookup("stream"); !ok || v != "ORDERS" {
		t.Errorf("Lookup(stream) = %q, %v, want ORDERS", v, ok)
	}
	var none dsn.Params
	if none.Len() != 0 || none.Has("x") || len(none.Unread()) != 0 || none.Strict() != nil ||
		fmt.Sprint(none) != "" {
		t.Errorf("a zero Params is not empty: %d parameters, rendered %q", none.Len(), fmt.Sprint(none))
	}
}

// TestConfigStringEscapesWhatALogLineMustNotCarry: Config.String is documented
// as safe to put in a log line, and it wrote a decoded control byte, a rune
// that does not print or a byte that is not UTF-8 as it was. "%0A" in a
// username became a newline, so a Config logged as one line could forge the
// line after it. Each of those is percent-encoded now, which Parse reads back,
// and a printable rune that is not ASCII is still written as itself.
func TestConfigStringEscapesWhatALogLineMustNotCarry(t *testing.T) {
	raw := "nats://us%0Aer%1B%5B31m:pw@host%7F:4222/pa%E2%80%A8th%FF?stream=a%0Db%C2%85&name=%C3%BCber"
	cfg, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("Parse(%q): %v", raw, err)
	}
	rendered := cfg.String()
	if !utf8.ValidString(rendered) {
		t.Errorf("Config.String() = %q, which is not valid UTF-8", rendered)
	}
	for _, r := range rendered {
		if !unicode.IsPrint(r) {
			t.Errorf("Config.String() = %q, which carries %U unescaped", rendered, r)
		}
	}
	if !strings.Contains(rendered, "name=über") {
		t.Errorf("Config.String() = %q, which escaped a printable rune", rendered)
	}
	back, err := dsn.Parse(rendered)
	if err != nil {
		t.Fatalf("Parse refuses the rendering %q: %v", rendered, err)
	}
	if back.Username != cfg.Username || back.Path != cfg.Path || back.Addr() != cfg.Addr() {
		t.Errorf("the rendering %q reads back as user %q path %q host %q, want %q %q %q",
			rendered, back.Username, back.Path, back.Addr(), cfg.Username, cfg.Path, cfg.Addr())
	}
	for k, v := range cfg.Params.All() {
		if got, ok := back.Params.Lookup(k); !ok || got != v {
			t.Errorf("the rendering %q reads %q back as %q, want %q", rendered, k, got, v)
		}
	}
}

// TestParamsRedactUnderEveryVerb: a Config renders itself, but its Params
// field is exported and printed on its own as three bare slices, credential
// values among them: `{[token stream] [s3cr3t ORDERS] [false false]}`. Params
// has no String to answer %v, because that name is the typed accessor's.
func TestParamsRedactUnderEveryVerb(t *testing.T) {
	const secret = "s3cr3t-token-value"
	cfg, err := dsn.Parse("natsjs://127.0.0.1:4222?token=" + secret + "&stream=ORDERS")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	args := map[string]any{
		"Params":                    cfg.Params,
		"*Params":                   &cfg.Params,
		"a struct holding a Params": struct{ P dsn.Params }{cfg.Params},
	}
	hexed := hex.EncodeToString([]byte(secret))
	for _, verb := range everyVerb {
		for name, arg := range args {
			got := fmt.Sprintf(verb, arg)
			for _, form := range []string{secret, hexed, strings.ToUpper(hexed)} {
				if strings.Contains(got, form) {
					t.Errorf("%s of a %s carries the credential: %q", verb, name, got)
				}
			}
		}
	}
	// The rendering is Config.String's query, so it still says what was set.
	const want = "token=" + dsn.Redacted + "&stream=ORDERS"
	for _, verb := range []string{"%v", "%s", "%#v"} {
		if got := fmt.Sprintf(verb, cfg.Params); got != want {
			t.Errorf("%s of a Params is %q, want %q", verb, got, want)
		}
	}
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("opened", "params", cfg.Params)
	if strings.Contains(b.String(), secret) {
		t.Errorf("slog printed a Params' credential: %s", b.String())
	}
}
