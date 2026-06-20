package redisstore_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"bitbucket.org/iqhive/iqmq/x/idempotency/redisstore"
)

// storePassword is distinct from every other string in these tests, so that a
// rendering which contains it contains the credential and nothing else.
const storePassword = "redisstore-p4ssw0rd-C06b"

// TestOptionsRenderNoPassword holds Options to the rule that a credential
// never reaches a log. Options has no String method of its own, so every way a
// caller prints one -- fmt's three struct verbs, slog's text handler and its
// JSON handler -- renders each field through that field's own type, and the
// password's type is what decides.
func TestOptionsRenderNoPassword(t *testing.T) {
	opts := redisstore.Options{Addrs: []string{"redis.internal:6379"}, Username: "svc-idem", Password: storePassword}

	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("store", "opts", opts)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("store", "opts", opts)

	for name, got := range map[string]string{
		"%v":          fmt.Sprintf("%v", opts),
		"%+v":         fmt.Sprintf("%+v", opts),
		"%#v":         fmt.Sprintf("%#v", opts),
		"%+v pointer": fmt.Sprintf("%+v", &opts),
		"slog (both)": logs.String(),
	} {
		if strings.Contains(got, storePassword) {
			t.Errorf("%s renders the password: %s", name, got)
		}
		// The user name is not a credential and must survive: a rendering
		// that printed nothing at all would pass the check above and help
		// no operator.
		if !strings.Contains(got, "svc-idem") {
			t.Errorf("%s does not render the user, so it proves nothing about the password beside it: %s", name, got)
		}
	}
}

// TestOpenAuthenticatesWithThePasswordNotItsRedaction is the other half:
// redacting every rendering must not redact the one place the password is
// SENT. Open hands it to rueidis, and a rendering used there instead of the
// credential logs in as "redacted".
//
// The wrong-password arm is what makes the right-password arm mean something:
// without it, a server that did not check passwords would pass both.
func TestOpenAuthenticatesWithThePasswordNotItsRedaction(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.RequireUserAuth("svc-idem", storePassword)

	s, err := redisstore.Open(t.Context(), redisstore.Options{
		Addrs: []string{srv.Addr()}, Username: "svc-idem", Password: storePassword, DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open with the right password: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.Claim(t.Context(), "k", time.Minute); err != nil {
		t.Fatalf("Claim over the authenticated connection: %v", err)
	}

	const wrong = "wrong-p4ssw0rd-C06b"
	bad, err := redisstore.Open(t.Context(), redisstore.Options{
		Addrs: []string{srv.Addr()}, Username: "svc-idem", Password: wrong, DialTimeout: 5 * time.Second,
	})
	if err == nil {
		_ = bad.Close()
		t.Fatal("Open with a wrong password succeeded, so this server does not check passwords and the arm above proves nothing")
	}
	// Open wraps the client's refusal; the password it was given must not
	// be in it. rueidis answers WRONGPASS without echoing the credential
	// today, and this is what notices the day a client version does.
	if strings.Contains(err.Error(), wrong) {
		t.Errorf("the refusal renders the password it was given: %v", err)
	}
}
