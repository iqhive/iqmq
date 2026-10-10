package harness

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
)

// TestAuthRendersNoPassword holds the harness's login to the rule that a
// credential never reaches a log: every way a test prints a Broker or its
// Auth -- fmt's three struct verbs, slog's text handler and its JSON handler
// -- must print the user and not the password.
func TestAuthRendersNoPassword(t *testing.T) {
	const pw = "harness-p4ssw0rd-C06b"
	b := &Broker{Name: "artemis", Addr: "127.0.0.1:61616", Auth: Auth{User: "artemis", Password: pw}}

	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("broker", "auth", b.Auth, "broker", b)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("broker", "auth", b.Auth, "broker", b)

	renderings := map[string]string{
		"%v Auth":     fmt.Sprintf("%v", b.Auth),
		"%+v Auth":    fmt.Sprintf("%+v", b.Auth),
		"%#v Auth":    fmt.Sprintf("%#v", b.Auth),
		"%+v Broker":  fmt.Sprintf("%+v", *b),
		"%#v Broker":  fmt.Sprintf("%#v", *b),
		"slog (both)": logs.String(),
	}
	for name, got := range renderings {
		if strings.Contains(got, pw) {
			t.Errorf("%s renders the password: %s", name, got)
		}
		if !strings.Contains(got, "artemis") {
			t.Errorf("%s does not render the user, so this test proves nothing about it: %s", name, got)
		}
	}
}

// TestPingSTOMPSendsThePasswordNotItsRedaction is the other half: redacting
// every rendering must not redact the one place the harness SENDS the
// password. PingSTOMP writes it into the CONNECT frame, and a rendering used
// there instead of Reveal -- `fmt.Sprint(auth.Password)`, or a `%s` -- logs in
// as "redacted" and fails every Artemis and ActiveMQ ping. TestPingSTOMPFake
// could not tell: its user and password are both "artemis", and it reads only
// the login header.
func TestPingSTOMPSendsThePasswordNotItsRedaction(t *testing.T) {
	const pw = "harness-p4ssw0rd-C06b"
	var gotPasscode string
	addr := fakeServer(t, func(c net.Conn) {
		frame, _ := bufio.NewReader(c).ReadString(0)
		for line := range strings.SplitSeq(frame, "\n") {
			if v, ok := strings.CutPrefix(line, "passcode:"); ok {
				gotPasscode = v
			}
		}
		_, _ = io.WriteString(c, "CONNECTED\nversion:1.2\n\n\x00")
	})
	b := &Broker{Name: "artemis", Addrs: map[string]string{"stomp": addr}, Auth: Auth{User: "artemis", Password: pw}}
	if err := Ping(testCtx(t), b); err != nil {
		t.Fatal(err)
	}
	if gotPasscode != pw {
		t.Fatalf("passcode header = %q, want the password itself", gotPasscode)
	}
}

// TestSecretRendersAsTheRootModulesDoes pins the harness's local Secret to the
// renderings of github.com/iqhive/iqmq/dsn.Secret, which it copies because
// this package may not import the root module : the fixed
// redaction and never a length hint, the empty string for no password, and
// the same answers through %#v, slog and encoding/json.
func TestSecretRendersAsTheRootModulesDoes(t *testing.T) {
	const pw = "harness-p4ssw0rd-C06b"
	s := Secret(pw)
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("x", "pw", s)
	js, err := json.Marshal(struct{ P Secret }{s})
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct{ got, want string }{
		"String":    {s.String(), "redacted"},
		"%v":        {fmt.Sprint(s), "redacted"},
		"%#v":       {fmt.Sprintf("%#v", s), `harness.Secret("redacted")`},
		"LogValue":  {s.LogValue().String(), "redacted"},
		"json":      {string(js), `{"P":"redacted"}`},
		"empty":     {Secret("").String(), ""},
		"empty %#v": {fmt.Sprintf("%#v", Secret("")), `harness.Secret("")`},
		"Reveal":    {s.Reveal(), pw},
	} {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}
	if !strings.Contains(logs.String(), `"pw":"redacted"`) || strings.Contains(logs.String(), pw) {
		t.Errorf("slog's JSON handler rendered %s", logs.String())
	}
}
