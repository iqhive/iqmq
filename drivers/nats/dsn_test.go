package nats_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSN holds the DSN to the shape and to ADR-010 the specification's
// rule: every parameter this driver has is read, and one it does not have is a
// construction error naming it rather than a setting nobody notices.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name, in string
		check    func(*testing.T, nq.Config)
		wantErr  bool
	}{
		{
			name: "bare host takes the default port",
			in:   "nats://broker",
			check: func(t *testing.T, c nq.Config) {
				if len(c.Addrs) != 1 || c.Addrs[0] != "nats://broker:4222" {
					t.Fatalf("addrs = %v", c.Addrs)
				}
			},
		},
		{
			name: "several endpoints keep their order",
			in:   "nats://a:4222,b:4223",
			check: func(t *testing.T, c nq.Config) {
				if len(c.Addrs) != 2 || c.Addrs[0] != "nats://a:4222" || c.Addrs[1] != "nats://b:4223" {
					t.Fatalf("addrs = %v", c.Addrs)
				}
			},
		},
		{
			name: "the path is the subject prefix",
			in:   "nats://host/acme",
			check: func(t *testing.T, c nq.Config) {
				if c.SubjectPrefix != "acme" {
					t.Fatalf("subject prefix = %q", c.SubjectPrefix)
				}
			},
		},
		{
			name: "credentials are secrets",
			in:   "nats://user:s3cret@host",
			check: func(t *testing.T, c nq.Config) {
				if c.Username != "user" || c.Password.Reveal() != "s3cret" {
					t.Fatalf("credentials = %q/%q", c.Username, c.Password.Reveal())
				}
				if got := c.Password.String(); got != dsn.Redacted {
					t.Fatalf("the password renders as %q", got)
				}
			},
		},
		{
			name: "every parameter is read",
			in: "nats://host?name=svc&creds=/etc/n.creds&pendingmessages=1024&pendingbytes=2048" +
				"&connecttimeout=3s&flushtimeout=4s&reconnectwait=5s&maxreconnects=-1" +
				"&pinginterval=6s&maxpingsout=7&draintimeout=8s&noecho=true",
			check: func(t *testing.T, c nq.Config) {
				for name, pair := range map[string][2]any{
					"name":            {c.Name, "svc"},
					"creds":           {c.CredentialsFile, "/etc/n.creds"},
					"pendingmessages": {c.PendingMessages, 1024},
					"pendingbytes":    {c.PendingBytes, 2048},
					"connecttimeout":  {c.ConnectTimeout, 3 * time.Second},
					"flushtimeout":    {c.FlushTimeout, 4 * time.Second},
					"reconnectwait":   {c.ReconnectWait, 5 * time.Second},
					"maxreconnects":   {c.MaxReconnects, -1},
					"pinginterval":    {c.PingInterval, 6 * time.Second},
					"maxpingsout":     {c.MaxPingsOut, 7},
					"draintimeout":    {c.DrainTimeout, 8 * time.Second},
					"noecho":          {c.NoEcho, true},
				} {
					if pair[0] != pair[1] {
						t.Errorf("%s = %v, want %v", name, pair[0], pair[1])
					}
				}
			},
		},
		{
			name: "tls turns on TLS",
			in:   "nats://host?tls=true",
			check: func(t *testing.T, c nq.Config) {
				if c.TLS == nil {
					t.Fatal("tls=true produced no TLS configuration")
				}
				if c.TLS.InsecureSkipVerify {
					t.Fatal("tls=true skipped verification")
				}
			},
		},
		{name: "an unknown parameter is refused", in: "nats://host?capacty=100", wantErr: true},
		{name: "a repeated parameter is refused", in: "nats://host?name=a&name=b", wantErr: true},
		{name: "a bad boolean is refused", in: "nats://host?noecho=1", wantErr: true},
		{name: "another driver's scheme is refused", in: "memory://", wantErr: true},
		{name: "the JetStream scheme is not this driver's", in: "natsjs://host?stream=ORDERS", wantErr: true},
		{name: "skipping verification without TLS is refused", in: "nats://host?insecureskipverify=true", wantErr: true},
		{name: "a prefix that is not a subject is refused", in: "nats://host/a b", wantErr: true},
		{name: "a fragment is refused", in: "nats://host#frag", wantErr: true},
		{name: "not a DSN", in: "nats", wantErr: true},
		{name: "longer than the bound", in: "nats://host?name=" + strings.Repeat("x", 9000), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := nq.ParseDSN(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseDSN(%q) was accepted: %+v", tc.in, cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.in, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestUnknownParameterNamesItself is the half of ADR-010 that makes the
// refusal useful: a caller who typo'd a parameter is told which one.
func TestUnknownParameterNamesItself(t *testing.T) {
	_, err := nq.ParseDSN("nats://host?pendingmesages=10")
	if err == nil {
		t.Fatal("a typo'd parameter was accepted")
	}
	if !strings.Contains(err.Error(), "pendingmesages") {
		t.Fatalf("the refusal does not name the parameter: %v", err)
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidConfig {
		t.Fatalf("code = %v, want InvalidConfig", code)
	}
}

// TestConfigRenderingRedacts is a configuration is safe to log.
func TestConfigRenderingRedacts(t *testing.T) {
	cfg, err := nq.ParseDSN("nats://user:s3cret@host:4222/acme?tls=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	cfg.Token = dsn.Secret("t0ken")
	for name, rendered := range map[string]string{
		"DSN":      cfg.DSN(),
		"String":   cfg.String(),
		"LogValue": cfg.LogValue().String(),
	} {
		if strings.Contains(rendered, "s3cret") {
			t.Errorf("%s carries the password: %s", name, rendered)
		}
		if strings.Contains(rendered, "t0ken") {
			t.Errorf("%s carries the token: %s", name, rendered)
		}
		if !strings.Contains(rendered, "host:4222") || !strings.Contains(rendered, "acme") {
			t.Errorf("%s does not name the broker: %s", name, rendered)
		}
	}
}

// TestRegister is ADR-012: the driver claims its own scheme on a registry the
// caller owns, and a second registration of the same scheme is a conflict
// rather than a silent replacement.
func TestRegister(t *testing.T) {
	r := dsn.NewRegistry()
	if err := nq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if schemes := r.Schemes(); len(schemes) != 1 || schemes[0] != "nats" {
		t.Fatalf("registered schemes = %v, want [nats]", schemes)
	}
	if err := nq.Register(r); err == nil {
		t.Fatal("registering twice was accepted")
	}
	if err := nq.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	// A scheme nobody claimed is ErrDriverNotRegistered, and one that is
	// claimed reaches this driver.
	if _, err := r.Open(t.Context(), "kafka://b1:9092"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Fatalf("opening an unregistered scheme = %v", err)
	}
	url := serverURL(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c, err := r.Open(ctx, url)
	if err != nil {
		t.Fatalf("opening %q through the registry: %v", url, err)
	}
	defer func() { _ = c.Close() }()
	if c.Info().Name != "nats" {
		t.Fatalf("the registry opened %q", c.Info().Name)
	}
}
