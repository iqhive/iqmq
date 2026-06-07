package gcppubsub_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestConfigValidateIsPure holds [gps.Config.Validate] it refuses
// what it can refuse and it reads nothing — no clock, no environment, no
// network.
func TestConfigValidateIsPure(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  gps.Config
		want mq.ErrorCode
	}{
		{"no-project", gps.Config{}, mq.CodeInvalidConfig},
		{"project-is-a-path", gps.Config{ProjectID: "projects/p"}, mq.CodeInvalidConfig},
		{"negative-deadline", gps.Config{ProjectID: "p", AckDeadline: -time.Second}, mq.CodeInvalidConfig},
		{"deadline-too-long", gps.Config{ProjectID: "p", AckDeadline: time.Hour}, mq.CodeInvalidConfig},
		{
			"strict-deadline-too-short",
			gps.Config{ProjectID: "p", AckDeadline: time.Second, StrictAckDeadline: true},
			mq.CodeInvalidConfig,
		},
		{"extension-too-long", gps.Config{ProjectID: "p", MaxExtension: 2 * time.Hour}, mq.CodeInvalidConfig},
		{"retention-too-short", gps.Config{ProjectID: "p", Retention: time.Minute}, mq.CodeInvalidConfig},
		{"retention-too-long", gps.Config{ProjectID: "p", Retention: 365 * 24 * time.Hour}, mq.CodeInvalidConfig},
		{
			"dead-letter-attempts-below-the-service-minimum",
			gps.Config{ProjectID: "p", DeadLetterTopic: "dlq", MaxDeliveryAttempts: 2},
			mq.CodeInvalidConfig,
		},
		{
			"two-credentials",
			gps.Config{ProjectID: "p", CredentialsJSON: "{}", CredentialsFile: "/k.json"},
			mq.CodeInvalidConfig,
		},
		{
			"insecure-without-an-endpoint",
			gps.Config{ProjectID: "p", InsecureTransport: true},
			mq.CodeInvalidConfig,
		},
		{"prefix-outside-the-grammar", gps.Config{ProjectID: "p", TopicPrefix: "a/b"}, mq.CodeInvalidConfig},
		{"negative-prefetch", gps.Config{ProjectID: "p", Prefetch: -1}, mq.CodeInvalidConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatal("the configuration was accepted")
			}
			if got := mq.CodeOf(err); got != tc.want {
				t.Fatalf("the refusal reported %v, want %v: %v", got, tc.want, err)
			}
		})
	}
	// And a usable one is accepted, including the deadline the emulator takes
	// and the managed service does not: StrictAckDeadline is what refuses it
	// locally, and it is off by default because several scenarios ask
	// for a sub-second lease and the emulator grants it.
	ok := gps.Config{ProjectID: "p", AckDeadline: 500 * time.Millisecond}
	if err := ok.Validate(); err != nil {
		t.Fatalf("a sub-second deadline was refused without StrictAckDeadline: %v", err)
	}
}

// TestConfigNeverRendersACredential is a configuration is safe
// in a log line, in an error and under %#v.
func TestConfigNeverRendersACredential(t *testing.T) {
	const secret = "-----BEGIN PRIVATE KEY-----abcdef"
	cfg := gps.Config{
		ProjectID:       "p",
		CredentialsJSON: dsn.Secret(secret),
		CredentialsFile: "/etc/keys/sa.json",
	}
	for name, rendered := range map[string]string{
		"String":   cfg.String(),
		"GoString": cfg.GoString(),
		"LogValue": cfg.LogValue().String(),
		"DSN":      cfg.DSN(),
		"slog":     slogRender(cfg),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s rendered the credential: %s", name, rendered)
		}
		if !strings.Contains(rendered, "p") {
			t.Errorf("%s rendered nothing useful: %s", name, rendered)
		}
	}
	// The secret's own rendering is dsn.Secret's contract and is proved
	// there; this is the driver honouring it.
	if got := cfg.CredentialsJSON.String(); strings.Contains(got, secret) {
		t.Errorf("dsn.Secret rendered itself: %s", got)
	}
	if cfg.CredentialsJSON.Reveal() != secret {
		t.Error("Reveal does not return the credential")
	}
}

// slogRender renders the configuration through log/slog, which is the path a
// structured logger takes and the one a String() method alone does not cover.
func slogRender(cfg gps.Config) string {
	var b strings.Builder
	h := slog.NewTextHandler(&b, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.New(h).Info("config", "cfg", cfg)
	return b.String()
}

// TestConfigResolveFillsEveryDefault is what makes one set of numbers visible
// to every reader of a Config: a zero field that meant "the library's default"
// in one place and "this driver's" in another is how two code paths disagree
// about the same configuration.
func TestConfigResolveFillsEveryDefault(t *testing.T) {
	c, _ := scripted(t)
	caps := c.Capabilities()
	// The resolved MaxExtension is what MaxDelay is advertised as, which is
	// the one place a resolved default is observable from outside.
	if got := caps.Limit(mq.LimitMaxDelay); !got.Known || got.Value != int64(gps.DefaultMaxExtension) {
		t.Fatalf("a zero MaxExtension resolved to %v, want %v", got, gps.DefaultMaxExtension)
	}
}
