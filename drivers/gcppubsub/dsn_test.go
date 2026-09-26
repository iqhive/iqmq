package gcppubsub_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSN covers the two authority shapes and every parameter, because a
// parameter nobody parses is a setting that silently did nothing.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name  string
		in    string
		check func(*testing.T, gps.Config)
	}{
		{
			"project-in-the-authority",
			"gcppubsub://my-project",
			func(t *testing.T, c gps.Config) {
				if c.ProjectID != "my-project" || c.Endpoint != "" {
					t.Fatalf("project %q endpoint %q", c.ProjectID, c.Endpoint)
				}
			},
		},
		{
			"endpoint-and-project",
			"gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true",
			func(t *testing.T, c gps.Config) {
				if c.ProjectID != "my-project" || c.Endpoint != "127.0.0.1:8085" || !c.InsecureTransport {
					t.Fatalf("%+v", c)
				}
			},
		},
		{
			"every-flag",
			"gcppubsub://p?ordering=true&exactlyonce=1&retainacked=yes&autocreate=on&strictackdeadline=true&compression=true",
			func(t *testing.T, c gps.Config) {
				for name, got := range map[string]bool{
					"ordering": c.Ordering, "exactlyonce": c.ExactlyOnce,
					"retainacked": c.RetainAcked, "autocreate": c.AutoCreate,
					"strictackdeadline": c.StrictAckDeadline, "compression": c.Compression,
				} {
					if !got {
						t.Errorf("%s was not parsed", name)
					}
				}
			},
		},
		{
			"durations-and-counts",
			"gcppubsub://p?ackdeadline=30s&maxextension=5m&retention=10m&apitimeout=15s&publishdelay=5ms&publishtimeout=45s&prefetch=32&publishcount=50&completionworkers=8&completionqueue=256",
			func(t *testing.T, c gps.Config) {
				if c.AckDeadline != 30*time.Second || c.MaxExtension != 5*time.Minute ||
					c.Retention != 10*time.Minute || c.APITimeout != 15*time.Second ||
					c.PublishDelayThreshold != 5*time.Millisecond || c.PublishTimeout != 45*time.Second ||
					c.Prefetch != 32 || c.PublishCountThreshold != 50 ||
					c.CompletionWorkers != 8 || c.CompletionQueue != 256 {
					t.Fatalf("%+v", c)
				}
			},
		},
		{
			"dead-letter-and-filter",
			`gcppubsub://p?deadlettertopic=dlq&maxdeliveryattempts=7&filter=attributes.k%20%3D%20%22v%22`,
			func(t *testing.T, c gps.Config) {
				if c.DeadLetterTopic != "dlq" || c.MaxDeliveryAttempts != 7 {
					t.Fatalf("%+v", c)
				}
				if c.Filter != `attributes.k = "v"` {
					t.Fatalf("filter %q", c.Filter)
				}
			},
		},
		{
			"prefixes-and-credentials-file",
			"gcppubsub://p?topicprefix=dev-&subscriptionprefix=dev-&credentialsfile=/etc/keys/sa.json",
			func(t *testing.T, c gps.Config) {
				if c.TopicPrefix != "dev-" || c.SubscriptionPrefix != "dev-" ||
					c.CredentialsFile != "/etc/keys/sa.json" {
					t.Fatalf("%+v", c)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := gps.ParseDSN(tc.in)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.in, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestParseDSNRefusals is the other half: a DSN this driver cannot honour is
// an error rather than a partially applied configuration.
func TestParseDSNRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"wrong-scheme", "sqs://p"},
		{"no-project", "gcppubsub://"},
		{"unknown-parameter", "gcppubsub://p?nonsuch=1"},
		{"bad-duration", "gcppubsub://p?ackdeadline=soon"},
		{"bad-number", "gcppubsub://p?prefetch=lots"},
		{"bad-flag", "gcppubsub://p?ordering=perhaps"},
		{"insecure-without-an-endpoint", "gcppubsub://p?insecuretransport=true"},
		{"deadline-out-of-range", "gcppubsub://p?ackdeadline=2h"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := gps.ParseDSN(tc.in); err == nil {
				t.Fatalf("ParseDSN(%q) was accepted", tc.in)
			}
		})
	}
}

// TestDSNRoundTripsWithoutTheCredential is the rule: the rendering is not a
// round trip, and what it loses is the secret.
func TestDSNRoundTripsWithoutTheCredential(t *testing.T) {
	const in = "gcppubsub://127.0.0.1:8085/p?insecuretransport=true&ordering=true&ackdeadline=30s&prefetch=32"
	cfg, err := gps.ParseDSN(in)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	rendered := cfg.DSN()
	again, err := gps.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the rendered DSN does not parse: %q: %v", rendered, err)
	}
	if again.ProjectID != cfg.ProjectID || again.Endpoint != cfg.Endpoint ||
		again.Ordering != cfg.Ordering || again.AckDeadline != cfg.AckDeadline ||
		again.Prefetch != cfg.Prefetch {
		t.Fatalf("the round trip lost a setting:\n%+v\n%+v", cfg, again)
	}
	// And a credential set in Go is redacted rather than round-tripped.
	cfg.CredentialsJSON = dsn.Secret("secret-key")
	if r := cfg.DSN(); strings.Contains(r, "secret-key") {
		t.Fatalf("the rendered DSN carries the credential: %s", r)
	} else if !strings.Contains(r, dsn.Redacted) {
		t.Fatalf("the rendered DSN does not say a credential was withheld: %s", r)
	}
}

// TestRegisterUsesTheCallersRegistry is ADR-012: a driver never reaches for a
// global registry, so a library that composes two brokers registers each of
// them with a registry it owns.
func TestRegisterUsesTheCallersRegistry(t *testing.T) {
	var r dsn.Registry
	if err := gps.Register(&r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := gps.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	if !contains(r.Schemes(), gps.Scheme) {
		t.Fatalf("the registry does not hold %q: %v", gps.Scheme, r.Schemes())
	}
	// Registering twice is a conflict rather than a silent replacement.
	if err := gps.Register(&r); err == nil {
		t.Fatal("registering the same scheme twice was accepted")
	}
	_ = mq.ErrClosed
}

// contains reports whether the slice holds v.
func contains(all []string, v string) bool {
	for _, s := range all {
		if s == v {
			return true
		}
	}
	return false
}
