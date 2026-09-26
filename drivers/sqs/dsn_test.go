package sqs_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSN is the parameter table, and the two things about it that are
// not obvious: a DSN with no host is a regional endpoint, and a DSN with one
// is an explicit endpoint whose scheme is decided by insecuretransport.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dsn   string
		check func(*testing.T, sq.Config)
	}{
		{"regional, no host", "sqs://AKIA:secret@aws?region=eu-west-1", func(t *testing.T, c sq.Config) {
			if c.Region != "eu-west-1" {
				t.Errorf("region = %q", c.Region)
			}
			if c.Endpoint != "" {
				t.Errorf("endpoint = %q, want the SDK's own regional endpoint", c.Endpoint)
			}
			if c.AccessKeyID != "AKIA" || c.SecretAccessKey.Reveal() != "secret" {
				t.Errorf("credentials = %q / %q", c.AccessKeyID, c.SecretAccessKey.Reveal())
			}
		}},
		{"explicit endpoint", "sqs://k:s@127.0.0.1:9324?region=elasticmq&insecuretransport=true", func(t *testing.T, c sq.Config) {
			if c.Endpoint != "http://127.0.0.1:9324" {
				t.Errorf("endpoint = %q", c.Endpoint)
			}
			if !c.InsecureTransport {
				t.Error("insecuretransport was not carried")
			}
		}},
		{"https endpoint", "sqs://k:s@sqs.example.com?region=eu-west-1", func(t *testing.T, c sq.Config) {
			if c.Endpoint != "https://sqs.example.com" {
				t.Errorf("endpoint = %q, want https by default", c.Endpoint)
			}
		}},
		{"prefix in the path", "sqs://aws?region=r", func(t *testing.T, c sq.Config) {
			if c.QueuePrefix != "" {
				t.Errorf("prefix = %q", c.QueuePrefix)
			}
		}},
		{"regional host with credentials", "sqs://k:s@aws?region=eu-west-1", func(t *testing.T, c sq.Config) {
			if c.Endpoint != "" {
				t.Errorf("endpoint = %q, want the regional one", c.Endpoint)
			}
			if c.AccessKeyID != "k" {
				t.Errorf("the credentials were lost: %q", c.AccessKeyID)
			}
		}},
		{"prefix", "sqs://k:s@host/dev-?region=r", func(t *testing.T, c sq.Config) {
			if c.QueuePrefix != "dev-" {
				t.Errorf("prefix = %q", c.QueuePrefix)
			}
		}},
		{"fifo profile", "sqs://k:s@aws?region=r&fifo=true&contentbaseddedup=true", func(t *testing.T, c sq.Config) {
			if !c.FIFO || !c.ContentBasedDeduplication {
				t.Errorf("fifo = %v, dedup = %v", c.FIFO, c.ContentBasedDeduplication)
			}
			if c.Profile() != "fifo" {
				t.Errorf("profile = %q", c.Profile())
			}
		}},
		{"settlement policy", "sqs://k:s@aws?region=r&settle=hybrid&settlemaxitems=5&settleinterval=100ms", func(t *testing.T, c sq.Config) {
			if c.Settle != flush.Hybrid {
				t.Errorf("settle = %v", c.Settle)
			}
			if c.SettleMaxItems != 5 || c.SettleInterval != 100*time.Millisecond {
				t.Errorf("items = %d, interval = %v", c.SettleMaxItems, c.SettleInterval)
			}
		}},
		{"body policy", "sqs://k:s@aws?region=r&body=base64", func(t *testing.T, c sq.Config) {
			if c.Body != sq.BodyBase64 {
				t.Errorf("body = %v", c.Body)
			}
		}},
		{"consume shape", "sqs://k:s@aws?region=r&pollers=4&maxmessages=10&waittime=20s&ackdeadline=90s&leaseguard=2s", func(t *testing.T, c sq.Config) {
			if c.Pollers != 4 || c.MaxMessages != 10 {
				t.Errorf("pollers = %d, maxmessages = %d", c.Pollers, c.MaxMessages)
			}
			if c.WaitTime != 20*time.Second || c.AckDeadline != 90*time.Second || c.LeaseGuard != 2*time.Second {
				t.Errorf("waittime = %v, ackdeadline = %v, leaseguard = %v", c.WaitTime, c.AckDeadline, c.LeaseGuard)
			}
		}},
		{"redrive", "sqs://k:s@aws?region=r&deadletterqueue=dead&maxreceivecount=3", func(t *testing.T, c sq.Config) {
			if c.DeadLetterQueue != "dead" || c.MaxReceiveCount != 3 {
				t.Errorf("dlq = %q, maxreceivecount = %d", c.DeadLetterQueue, c.MaxReceiveCount)
			}
		}},
		{"session token", "sqs://k:s@aws?region=r&sessiontoken=tok", func(t *testing.T, c sq.Config) {
			if c.SessionToken.Reveal() != "tok" {
				t.Errorf("token = %q", c.SessionToken.Reveal())
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := sq.ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.dsn, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestParseDSNRefusals holds the parser to the rule that an unknown
// parameter is a construction error naming it, and to this driver's own
// refusals.
func TestParseDSNRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
	}{
		{"wrong scheme", "kafka://host"},
		{"unknown parameter", "sqs://k:s@aws?region=r&nonesuch=1"},
		{"unknown settlement policy", "sqs://k:s@aws?region=r&settle=eventually"},
		{"unknown body policy", "sqs://k:s@aws?region=r&body=gzip"},
		{"two endpoints", "sqs://k:s@a.example,b.example?region=r"},
		{"http without insecuretransport", "sqs://k:s@host?region=r&insecuretransport=false&maxmessages=11"},
		{"maxmessages over the service limit", "sqs://k:s@aws?region=r&maxmessages=11"},
		{"waittime over the service limit", "sqs://k:s@aws?region=r&waittime=25s"},
		{"ackdeadline over the service limit", "sqs://k:s@aws?region=r&ackdeadline=13h"},
		{"secret without a key", "sqs://:s@aws?region=r"},
		{"key without a secret", "sqs://k@aws?region=r"},
		{"maxreceivecount without a queue", "sqs://k:s@aws?region=r&maxreceivecount=3"},
		{"content-based dedup outside FIFO", "sqs://k:s@aws?region=r&contentbaseddedup=true"},
		{"a port on the regional host", "sqs://k:s@aws:9324?region=r"},
		{"insecure transport on the regional host", "sqs://k:s@aws?region=r&insecuretransport=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := sq.ParseDSN(tc.dsn)
			if err == nil {
				t.Fatalf("ParseDSN(%q) accepted it: %v", tc.dsn, cfg)
			}
		})
	}
}

// TestDSNRedacts is a configuration's rendering carries no credential,
// through every surface it can be rendered by.
func TestDSNRedacts(t *testing.T) {
	const secret = "s3cr3t-not-real"
	const token = "tok3n-not-real"
	cfg := sq.Config{
		Region:          "eu-west-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: dsn.Secret(secret),
		SessionToken:    dsn.Secret(token),
	}
	for name, rendered := range map[string]string{
		"DSN":      cfg.DSN(),
		"String":   cfg.String(),
		"LogValue": cfg.LogValue().String(),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s carries the secret access key: %s", name, rendered)
		}
		if strings.Contains(rendered, token) {
			t.Errorf("%s carries the session token: %s", name, rendered)
		}
		if !strings.Contains(rendered, dsn.Redacted) {
			t.Errorf("%s does not mark the redaction: %s", name, rendered)
		}
		// The access key ID is NOT a secret and stays, because a redacted
		// DSN that did not say which principal it was built for would be
		// useless in a log.
		if !strings.Contains(rendered, "AKIAEXAMPLE") {
			t.Errorf("%s dropped the access key ID: %s", name, rendered)
		}
	}
}

// TestDSNRoundTripsWhatItRenders checks that everything the rendering keeps
// parses back to the same value, so that a DSN in a log is a DSN an operator
// can use (with the credential replaced).
func TestDSNRoundTripsWhatItRenders(t *testing.T) {
	in := sq.Config{
		Region:          "eu-west-1",
		Endpoint:        "http://127.0.0.1:9324",
		AccessKeyID:     "AKIA",
		SecretAccessKey: "s",
		QueuePrefix:     "dev-",
		FIFO:            true,
		Pollers:         3,
		MaxMessages:     7,
		WaitTime:        10 * time.Second,
		AckDeadline:     2 * time.Minute,
		LeaseGuard:      time.Second,
		Settle:          flush.Time,
		SettleMaxItems:  4,
		SettleInterval:  75 * time.Millisecond,
		Body:            sq.BodyText,
		MaxMessageBytes: 1 << 18,
		DeadLetterQueue: "dead.fifo",
		MaxReceiveCount: 5,
		Retention:       48 * time.Hour,
		APITimeout:      15 * time.Second,
		AccountID:       "000000000000",
		// InsecureTransport has to be set for the http:// endpoint, which
		// is the refusal TestParseDSNRefusals covers from the other side.
		InsecureTransport:         true,
		ContentBasedDeduplication: true,
		AutoCreate:                true,
	}
	rendered := in.DSN()
	// The credential is gone by design, so it is put back before parsing.
	rendered = strings.Replace(rendered, "AKIA:"+dsn.Redacted, "AKIA:s", 1)
	out, err := sq.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", rendered, err)
	}
	out.SecretAccessKey = in.SecretAccessKey
	out.SessionToken = in.SessionToken
	// Config holds a function field and a map, so it is not comparable;
	// reflect.DeepEqual is the comparison, and it is an equality over the
	// whole struct rather than a field list so that a field added without a
	// DSN parameter fails here.
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("the rendering did not round-trip:\n rendered %s\n  in  %+v\n  out %+v", rendered, in, out)
	}
}

// TestRegister holds the registration surface to ADR-012: a driver registers
// with a registry the caller owns and never reaches for a global one.
func TestRegister(t *testing.T) {
	r := &dsn.Registry{}
	if err := sq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sq.Register(nil); err == nil {
		t.Fatal("Register(nil) was accepted")
	}
	if _, err := r.Open(t.Context(), "sqs://k:s@?region=r&nonesuch=1"); err == nil {
		t.Fatal("the registry opened a DSN with an unknown parameter")
	}
	if _, err := r.Open(t.Context(), "nosuch://host"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Fatalf("opening an unregistered scheme = %v", err)
	}
}
