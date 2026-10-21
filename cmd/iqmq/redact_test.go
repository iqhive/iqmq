package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/dsn"
)

// The credentials these tests use. They are real-shaped and distinctive on
// purpose: a test that looked for the absence of "secret" would pass against
// output that printed "s3cr3t", and a test that used a short value could match
// a substring of an unrelated word.
//
// the security checks is the subject: "secrets use redacting wrapper types",
// "Config implements safe String/LogValue", "bodies/headers never logged by
// default". The specification adds "native errors sanitised". A CLI is where all four
// become one question -- does a credential reach the terminal -- and there are
// four distinct places one can come from.
const (
	credPassword = "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY"
	credToken    = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9-THE-TOKEN"
	credEnv      = "AKIA-ENV-SECRET-VALUE-4f0c9d2e1b7a"
	credSession  = "FQoGZXIvYXdzEBYaDFNFU1NJT04tVE9LRU4"
)

// requireAbsent fails with the whole output when a credential appears in it.
//
// The failure message prints the offending output deliberately. A redaction
// test that said only "the credential leaked" would leave the next reader
// guessing which of four places it came from.
func requireAbsent(t *testing.T, what, cred, output string) {
	t.Helper()
	if strings.Contains(output, cred) {
		t.Fatalf("SECURITY: the %s reached the output.\n"+
			"credential: %q\nfull output:\n%s", what, cred, output)
	}
}

// TestRedactionPlace1_DSNUserinfo is the first place a credential can appear:
// the DSN's own userinfo.
func TestRedactionPlace1_DSNUserinfo(t *testing.T) {
	target := "sqs://AKIAEXAMPLE:" + credPassword + "@127.0.0.1:9324/prefix?region=elasticmq"

	rendered := redactTarget(target)
	requireAbsent(t, "DSN userinfo password", credPassword, rendered)
	if !strings.Contains(rendered, dsn.Redacted) {
		t.Errorf("redactTarget(%q) = %q, and it does not say the password was redacted", target, rendered)
	}
	if !strings.Contains(rendered, "AKIAEXAMPLE") {
		t.Errorf("redactTarget dropped the username: %q\n"+
			"the access key id is not a secret and is what says which principal the DSN was built for",
			rendered)
	}

	// And through the whole command, which is what a user actually sees: the
	// report, the parameter list and any failure message.
	h := newFullHarness(t)
	h.run("doctor", "--no-connect", target)
	requireAbsent(t, "DSN userinfo password in doctor's report", credPassword, h.stdout())
	requireAbsent(t, "DSN userinfo password in doctor's errors", credPassword, h.stderr())
}

// TestRedactionPlace2_DSNQueryParameter is the second place: a credential
// carried as a query parameter rather than in the authority.
//
// It is the one that catches the obvious implementation. dsn.Config.String
// redacted the password and ROUND-TRIPPED every parameter, so it rendered
// "?token=<secret>" verbatim to a terminal and a doctor built on it would have
// leaked every token-bearing DSN.
//
// The specification fixed that at the source -- dsn.Config.String redacts every
// credential-named parameter now -- and this test kept BOTH halves: the
// structural layer here still drops every parameter, because the CLI cannot
// know which of a driver's parameters is a credential, and the assertion below
// turned from a note into a check. Two layers, as the package comment says,
// because neither is enough on its own.
func TestRedactionPlace2_DSNQueryParameter(t *testing.T) {
	target := "natsjs://127.0.0.1:4222?token=" + credToken + "&stream=ORDERS"

	// The shared parser's own rendering no longer carries it, and that
	// is now asserted rather than noted: a regression in dsn.Config.String
	// would put the token back into every construction error naming a
	// parameter, which is not a place this package's scrub reaches.
	cfg, err := dsn.Parse(target)
	if err != nil {
		t.Fatalf("dsn.Parse: %v", err)
	}
	requireAbsent(t, "DSN query-parameter credential in dsn.Config.String", credToken, cfg.String())

	rendered := redactTarget(target)
	requireAbsent(t, "DSN query-parameter credential", credToken, rendered)

	names := strings.Join(paramNames(target), " ")
	requireAbsent(t, "DSN query-parameter credential in the parameter list", credToken, names)
	if !strings.Contains(names, "token") || !strings.Contains(names, "stream") {
		t.Errorf("paramNames = %q, and a reader cannot see which settings were given", names)
	}

	// The driver's own configuration rendering is the fourth place and is
	// tested below; here it matters that doctor's whole report is clean.
	h := newFullHarness(t)
	h.run("doctor", "--no-connect", target)
	requireAbsent(t, "DSN query-parameter credential in doctor's report", credToken, h.stdout())
	requireAbsent(t, "DSN query-parameter credential in doctor's errors", credToken, h.stderr())

	// And in JSON, which is the form a bug report attaches.
	h2 := newFullHarness(t)
	h2.run("doctor", "--no-connect", "-o", "json", target)
	requireAbsent(t, "DSN query-parameter credential in doctor's JSON", credToken, h2.stdout())
}

// TestRedactionPlace3_EnvironmentVariable is the third place: a credential in
// the environment, both as the DSN itself and as a variable a driver reads.
func TestRedactionPlace3_EnvironmentVariable(t *testing.T) {
	// The DSN comes from $IQMQ_DSN, and it carries a password.
	h := newFullHarness(t).withEnv(map[string]string{
		"IQMQ_DSN":              "rabbitmq://guest:" + credPassword + "@127.0.0.1:5672",
		"AWS_SECRET_ACCESS_KEY": credEnv,
		"AWS_SESSION_TOKEN":     credSession,
	})
	h.run("doctor", "--no-connect")
	out, errOut := h.stdout(), h.stderr()

	requireAbsent(t, "password from $IQMQ_DSN", credPassword, out)
	requireAbsent(t, "password from $IQMQ_DSN (stderr)", credPassword, errOut)
	requireAbsent(t, "$AWS_SECRET_ACCESS_KEY value", credEnv, out)
	requireAbsent(t, "$AWS_SESSION_TOKEN value", credSession, out)

	// The report has to SAY the variables are set, because "the credential is
	// in the environment" is a thing a reader needs to know. It must say it
	// without saying what they are.
	for _, want := range []string{"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "IQMQ_DSN"} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor did not report the environment variable %s at all:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "no value is ever printed") {
		t.Errorf("doctor's environment section does not say that values are never printed:\n%s", out)
	}

	// envReport has nowhere to put a value, which is the structural half of
	// the protection: a struct with no Value field cannot print one.
	h2 := newFullHarness(t).withEnv(map[string]string{"AWS_SECRET_ACCESS_KEY": credEnv})
	h2.run("doctor", "--no-connect", "-o", "json")
	requireAbsent(t, "$AWS_SECRET_ACCESS_KEY value in doctor's JSON", credEnv, h2.stdout())

	// And the structural layer ON ITS OWN, before the scrub can help.
	//
	// This is the half the first version of this test was missing. Adding a
	// Value field to envReport and printing it was watched leaving the test
	// GREEN, because the scrub caught the credential on the way out -- which
	// is defence in depth working exactly as intended and is also a test that
	// cannot tell the two layers apart. The report is therefore marshalled
	// directly here, with no writer and no scrub in the path, so a field that
	// carries a credential fails this whether the scrub would have caught it
	// or not.
	report := envOf(&cliEnv{getenv: func(k string) (string, bool) {
		if k == "AWS_SECRET_ACCESS_KEY" {
			return credEnv, true
		}
		if k == "AWS_SESSION_TOKEN" {
			return credSession, true
		}
		return "", false
	}})
	raw, merr := json.Marshal(report)
	if merr != nil {
		t.Fatalf("marshalling the environment report: %v", merr)
	}
	requireAbsent(t, "$AWS_SECRET_ACCESS_KEY value in the environment report itself",
		credEnv, string(raw))
	requireAbsent(t, "$AWS_SESSION_TOKEN value in the environment report itself",
		credSession, string(raw))
	set := 0
	for _, ev := range report {
		if ev.Set {
			set++
		}
	}
	if set != 2 {
		t.Errorf("the report says %d variables are set, want 2; it is not reading the "+
			"environment at all and this check proves nothing", set)
	}
}

// TestRedactionPlace4_ConfigStructField is the fourth place: the driver's own
// typed configuration, which is what the specification's "Config implements safe
// String/LogValue" is about.
//
// It goes through the CLI's describe path rather than calling the driver
// directly, because the question is whether the CLI can print a configuration
// safely -- and the answer has to be "it renders the driver's own String",
// never "it has its own renderer".
func TestRedactionPlace4_ConfigStructField(t *testing.T) {
	// A DSN whose credential lands in a dsn.Secret field of the driver's
	// Config: SecretAccessKey from the userinfo, SessionToken from a
	// parameter.
	target := "sqs://AKIAEXAMPLE:" + credPassword + "@127.0.0.1:9324" +
		"?region=elasticmq&sessiontoken=" + credSession

	entry, ok := entryForScheme("sqs")
	if !ok {
		t.Fatal("the sqs driver is not in the table")
	}
	if entry.describe == nil {
		t.Fatal("the sqs driver has no describe, so doctor cannot report its configuration at all")
	}
	rendered, err := entry.describe(target)
	if err != nil {
		t.Fatalf("describing %s: %v", redactTarget(target), err)
	}
	requireAbsent(t, "SecretAccessKey field in the rendered driver Config", credPassword, rendered)
	requireAbsent(t, "SessionToken field in the rendered driver Config", credSession, rendered)

	// The typed path directly, so that a driver whose Config.String regressed
	// is caught here and not only through the CLI.
	cfg, err := sqs.ParseDSN(target)
	if err != nil {
		t.Fatalf("sqs.ParseDSN: %v", err)
	}
	requireAbsent(t, "sqs.Config.String", credPassword, cfg.String())
	requireAbsent(t, "sqs.Config.String", credSession, cfg.String())
	if got := cfg.SecretAccessKey.Reveal(); got != credPassword {
		t.Errorf("the credential did not reach Config.SecretAccessKey (%q); "+
			"this test would then be proving nothing", got)
	}

	jsCfg, err := jetstream.ParseDSN("natsjs://127.0.0.1:4222?token=" + credToken)
	if err != nil {
		t.Fatalf("jetstream.ParseDSN: %v", err)
	}
	requireAbsent(t, "jetstream.Config.String", credToken, jsCfg.String())
	if got := jsCfg.Token.Reveal(); got != credToken {
		t.Errorf("the credential did not reach jetstream Config.Token (%q)", got)
	}

	// And through the command.
	h := newFullHarness(t)
	h.run("doctor", "--no-connect", target)
	requireAbsent(t, "driver Config field in doctor's report", credPassword, h.stdout())
	requireAbsent(t, "driver Config field in doctor's report", credSession, h.stdout())
}

// TestRedactionEveryDriverConfigRenderingIsSafe is the fourth place widened to
// every driver whose DSN can carry a credential.
//
// A per-driver table would be a list to forget to extend. This walks the
// driver table itself, builds a DSN with a credential in the userinfo, and
// holds whatever the driver's own String produced to not containing it. A
// driver whose DSN refuses a credential in the authority is skipped with its
// reason logged, so a skip is visible rather than silent.
func TestRedactionEveryDriverConfigRenderingIsSafe(t *testing.T) {
	checked := 0
	for _, d := range drivers() {
		if d.describe == nil {
			t.Logf("%s: no describe (its Config renders nothing); doctor reports its kinds instead", d.name)
			continue
		}
		scheme := d.schemes[0]
		for _, target := range []string{
			scheme + "://user:" + credPassword + "@127.0.0.1:9999",
			scheme + "://user:" + credPassword + "@127.0.0.1:9999/tenant/ns",
		} {
			rendered, err := d.describe(target)
			if err != nil {
				t.Logf("%s: %s refused this DSN shape (%v); not a redaction result", d.name, scheme, err)
				continue
			}
			requireAbsent(t, d.name+" Config.String", credPassword, rendered)
			checked++
			break
		}
	}
	if checked == 0 {
		t.Fatal("no driver's configuration rendering was exercised at all, so this test proves nothing")
	}
	t.Logf("checked %d driver configuration renderings", checked)
}

// TestScrubWriterHoldsASplitSecret is the scrubbing layer's own property.
//
// It is the one a naive implementation gets wrong: a writer that scrubbed each
// Write independently would pass a secret straight through when it arrives in
// two pieces, and a streaming report does arrive in pieces. The writer holds
// back a tail as long as its longest secret and emits it on Flush.
func TestScrubWriterHoldsASplitSecret(t *testing.T) {
	for _, split := range []int{1, 5, len(credPassword) - 1} {
		var buf bytes.Buffer
		w := newScrubWriter(&buf, dsn.Secret(credPassword))
		body := "before " + credPassword + " after"
		if _, err := w.Write([]byte(body[:split])); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := w.Write([]byte(body[split:])); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		got := buf.String()
		requireAbsent(t, "credential split across two Writes at "+itoa(split), credPassword, got)
		if !strings.Contains(got, dsn.Redacted) {
			t.Errorf("split at %d: the secret vanished without a redaction marker: %q", split, got)
		}
		if !strings.HasPrefix(got, "before ") || !strings.HasSuffix(got, " after") {
			t.Errorf("split at %d: the surrounding text was mangled: %q", split, got)
		}
	}
}

// TestScrubWriterFlushesWhatItHeld is the other half: the tail a writer holds
// back has to be emitted, or the last bytes of every report are lost.
func TestScrubWriterFlushesWhatItHeld(t *testing.T) {
	var buf bytes.Buffer
	w := newScrubWriter(&buf, dsn.Secret(credPassword))
	if _, err := w.Write([]byte("tail")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := buf.String(); got != "tail" {
		t.Errorf("Flush lost the held-back tail: got %q, want %q", got, "tail")
	}
}

// TestCredentialNameMatchingIsBySubstring pins the matching rule, because the
// alternative -- an exact list of parameter names -- is wrong the moment a
// driver adds one and its failure mode is a printed credential.
func TestCredentialNameMatchingIsBySubstring(t *testing.T) {
	credential := []string{
		"password", "passwd", "token", "sessiontoken", "authsecret", "authparams",
		"secret", "secretaccesskey", "apikey", "credentialsjson", "nkeyseed",
		// The names that do not exist yet and are covered anyway.
		"apisecret", "producerpassword", "refreshtoken", "clientsecret", "passphrase",
		// And the spellings a driver might use.
		"PASSWORD", "Token",
	}
	ordinary := []string{
		"region", "stream", "partitions", "capacity", "overflow", "fanout",
		"prefetch", "tracker", "nack", "settle", "confirm", "delay", "backoff",
		"project", "topicprefix", "clientid", "sasl", "tls", "creds", "trustcerts",
	}
	for _, n := range credential {
		if !isCredentialName(n) {
			t.Errorf("isCredentialName(%q) = false; a parameter of that name would be printed", n)
		}
	}
	for _, n := range ordinary {
		if isCredentialName(n) {
			t.Errorf("isCredentialName(%q) = true; an ordinary parameter would be redacted "+
				"and the report made less useful for no security gain", n)
		}
	}
}

// TestRedactTargetNeverEchoesAnUnparsableDSN covers the case where the bytes
// are least understood and most likely to be somebody's whole credential
// pasted into the wrong argument.
func TestRedactTargetNeverEchoesAnUnparsableDSN(t *testing.T) {
	for _, raw := range []string{
		credPassword,
		"not-a-dsn-" + credPassword,
		"kafka://host:99999999999999/" + credPassword + "#frag",
		"://" + credPassword,
		"kafka://host?bad=%zz&token=" + credPassword,
	} {
		got := redactTarget(raw)
		requireAbsent(t, "unparsable DSN echoed back", credPassword, got)
	}
}

// TestScrubCatchesALeakyDriverRendering is the scrubbing layer's reason to
// exist, and the fourth place's second line of defence.
//
// The structural layer -- render the configuration only through the driver's
// own String -- is a property of THIS package. It cannot help when the
// driver's own rendering is the thing that leaks, and doctor's output also
// carries driver error strings, native client messages and broker replies
// that this package did not write. So a driver whose configuration rendering
// carries a credential verbatim is registered here, and the whole report is
// still held to not printing it.
func TestScrubCatchesALeakyDriverRendering(t *testing.T) {
	target := "leaky://user:" + credPassword + "@127.0.0.1:9999?token=" + credToken

	// The leak, through the one seam a driver owns.
	rendered := leakyDescribe(target)
	if !strings.Contains(rendered, credPassword) {
		t.Fatal("the leaky rendering does not leak, so this test proves nothing")
	}

	var buf bytes.Buffer
	out := newScrubWriter(&buf, secretsOf(target)...)
	if _, err := out.Write([]byte("config " + rendered + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := out.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got := buf.String()
	requireAbsent(t, "credential from a driver's own leaky Config rendering", credPassword, got)
	requireAbsent(t, "token from a driver's own leaky Config rendering", credToken, got)
	if !strings.Contains(got, dsn.Redacted) {
		t.Errorf("the scrub removed the credential without leaving a marker: %q", got)
	}
}

// leakyDescribe is a driver rendering that carries its credentials verbatim,
// which is what a driver with no safe String would produce.
func leakyDescribe(target string) string {
	cfg, err := dsn.Parse(target)
	if err != nil {
		return ""
	}
	out := "user=" + cfg.Username + " password=" + cfg.Password.Reveal()
	for k, v := range cfg.Params.All() {
		out += " " + k + "=" + v
	}
	return out
}

// TestRedactionScrubsStderrToo is the gap the first version of this had, and
// the first version of THIS TEST could not fail either.
//
// It began by pointing the CLI at a DSN with an unknown parameter, on the
// theory that the driver's refusal would carry the credential. It does not:
// every parse failure in this repository is a static sentinel with no caller
// data in it, and the CLI's own message renders the target through
// redactTarget. So nothing ever put the credential on stderr, the scrub had
// nothing to catch, and disabling the scrub entirely left the test green --
// watched, with `errw := newScrubWriter(e.stderr)` in place.
//
// The hazard is real all the same, and it comes from the one place this
// package cannot control: a NATIVE error. A client library that puts the
// connection URL into a dial failure hands the CLI a string with the password
// in it, which is exactly what the specification's "native errors sanitised" is about --
// an obligation on the driver, not a property the CLI may assume. So the
// driver here is one whose Open fails the way a native client does.
func TestRedactionScrubsStderrToo(t *testing.T) {
	target := "leaky://user:" + credPassword + "@127.0.0.1:9999?token=" + credToken

	reg := dsn.NewRegistry()
	err := reg.Register("leaky", func(_ context.Context, raw string) (*mq.Client, error) {
		// A native client's dial failure, with the whole connection string in
		// it. Several real client libraries do precisely this.
		cfg, perr := dsn.Parse(raw)
		if perr != nil {
			return nil, perr
		}
		return nil, errors.New("dial tcp: cannot connect using amqp://" + cfg.Username +
			":" + cfg.Password.Reveal() + "@" + cfg.Addr() + ": connection refused")
	})
	if err != nil {
		t.Fatalf("registering: %v", err)
	}

	for _, args := range [][]string{
		{"pub", target, "orders", "body"},
		{"sub", target, "orders", "billing", "--max", "1"},
		{"tail", target, "orders"},
		{"bench", target, "orders", "--messages", "1"},
		{"doctor", target},
	} {
		h := harnessWith(t, reg, nil)
		code := h.run(args...)
		if code == exitOK {
			t.Fatalf("run(%v) succeeded against a driver whose Open always fails", args)
		}
		if h.stderr() == "" {
			t.Fatalf("run(%v) exited %d and wrote nothing to stderr", args, code)
		}
		requireAbsent(t, "credential from a native dial error on stderr ("+args[0]+")",
			credPassword, h.stderr())
		requireAbsent(t, "credential from a native dial error on stdout ("+args[0]+")",
			credPassword, h.stdout())
		// The redaction marker proves the scrub ran rather than the error
		// having been dropped.
		if !strings.Contains(h.stderr(), dsn.Redacted) {
			t.Errorf("run(%v): the error reached stderr with no redaction marker, so either "+
				"the credential was never there or the message was thrown away:\n%s",
				args, h.stderr())
		}
	}

	// The environment's credentials are scrubbed on stderr as well, and no
	// command has to remember to ask.
	h := harnessWith(t, reg, map[string]string{"AWS_SECRET_ACCESS_KEY": credEnv})
	h.env.registry = reg
	h.run("pub", "leaky://u:p@h?token="+credEnv, "orders", "body")
	requireAbsent(t, "$AWS_SECRET_ACCESS_KEY value on stderr", credEnv, h.stderr())
}

// TestScrubHoldsThePercentEncodedPassword: a password with a reserved
// character in it is written percent-encoded, and a native client that echoes
// its connection string prints that spelling, not the decoded one.
func TestScrubHoldsThePercentEncodedPassword(t *testing.T) {
	const decoded = "p@ss/w:rd+" + credPassword
	for _, written := range []string{
		"p%40ss%2Fw%3Ard%2B" + credPassword,
		"p%40ss%2fw%3ard%2b" + credPassword,
	} {
		target := "sqs://AKIAEXAMPLE:" + written + "@127.0.0.1:9324/prefix?region=elasticmq"
		var buf bytes.Buffer
		w := newScrubWriter(&buf, secretsOf(target)...)
		_, _ = w.Write([]byte("dial " + target + " failed; url " + url.QueryEscape(decoded) + "; raw " + decoded + "\n"))
		_ = w.Flush()
		for _, form := range []string{written, decoded, url.QueryEscape(decoded), url.PathEscape(decoded)} {
			requireAbsent(t, "percent-encoded DSN password", form, buf.String())
		}
	}
}
