package snssqs_test

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	sns "bitbucket.org/iqhive/iqmq/drivers/snssqs"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestValidateRefusals holds every configuration refusal to its code, which is
// what a caller switching on the model needs.
//
// The last four rows are the point of the whole test: Validate ends by
// validating the configuration the INNER SQS driver will be built with, so
// every refusal the specification wrote is this driver's refusal too. Without that line a
// poll longer than the service allows, or an http:// endpoint with no
// InsecureTransport, would be accepted here and refused inside OpenBackend —
// after the capability table had been built and the clients created.
func TestValidateRefusals(t *testing.T) {
	base := func() sns.Config {
		return sns.Config{Region: "r", AccountID: "1"}
	}
	for _, tc := range []struct {
		name string
		cfg  func(*sns.Config)
		code mq.ErrorCode
		says string
	}{
		{"body encoding", func(c *sns.Config) { c.Body = sq.BodyEncoding(9) }, mq.CodeInvalidConfig, "auto, text or base64"},
		{"settle kind", func(c *sns.Config) { c.Settle = flush.Kind(99) }, mq.CodeInvalidConfig, "A32"},
		{"filter scope", func(c *sns.Config) { c.FilterPolicyScope = sns.FilterScope(7) }, mq.CodeInvalidConfig, "FilterPolicyScope"},
		{"message bytes over SNS's limit", func(c *sns.Config) { c.MaxMessageBytes = sns.MaxMessageBytes + 1 }, mq.CodeInvalidConfig, "256 KiB"},
		{"negative message bytes", func(c *sns.Config) { c.MaxMessageBytes = -1 }, mq.CodeInvalidConfig, "256 KiB"},
		{"control character in the region", func(c *sns.Config) { c.Region = "eu-\x00west" }, mq.CodeInvalidConfig, "control character"},
		{"topic prefix", func(c *sns.Config) { c.TopicPrefix = "no spaces" }, mq.CodeInvalidConfig, "TopicPrefix"},
		{"queue prefix", func(c *sns.Config) { c.QueuePrefix = "no spaces" }, mq.CodeInvalidConfig, "QueuePrefix"},
		{"topic ARN that is not one", func(c *sns.Config) { c.TopicARNs = map[string]string{"t": "arn:aws:sqs:r:1:q"} }, mq.CodeInvalidConfig, "SNS topic ARN"},
		{"filter policy that is not JSON", func(c *sns.Config) { c.FilterPolicies = map[string]string{"s": "region=eu"} }, mq.CodeInvalidConfig, "JSON object"},
		{"filter policy with no conditions", func(c *sns.Config) { c.FilterPolicies = map[string]string{"s": "{}"} }, mq.CodeInvalidConfig, "matches everything"},
		{"filter policy on an unmappable subscription", func(c *sns.Config) { c.FilterPolicies = map[string]string{"a b": `{"x":["y"]}`} }, mq.CodeInvalidConfig, "subscription name"},

		// The inner driver's refusals, reached through the composition.
		{"poll longer than the service allows", func(c *sns.Config) { c.WaitTime = 30 * time.Second }, mq.CodeInvalidConfig, "twenty-second long poll"},
		{"more than ten messages per receive", func(c *sns.Config) { c.MaxMessages = 11 }, mq.CodeInvalidConfig, "ten messages"},
		{"plaintext endpoint without the flag", func(c *sns.Config) { c.Endpoint = "http://127.0.0.1:4566" }, mq.CodeInvalidConfig, "InsecureTransport"},
		{"redrive count with no queue", func(c *sns.Config) { c.MaxReceiveCount = 3 }, mq.CodeInvalidConfig, "DeadLetterQueue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.cfg(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("the configuration was accepted")
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Fatalf("code = %v (%v), want %v", got, err, tc.code)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal does not mention %q: %v", tc.says, err)
			}
		})
	}
}

// TestValidateAcceptsAWorkingConfiguration is the other direction: a
// refusal-only test would pass for a Validate that refused everything.
func TestValidateAcceptsAWorkingConfiguration(t *testing.T) {
	cfg := sns.Config{
		Region:            "eu-west-1",
		Endpoint:          "http://127.0.0.1:4566",
		InsecureTransport: true,
		AccessKeyID:       "AKIA",
		SecretAccessKey:   "secret",
		AccountID:         "000000000000",
		TopicPrefix:       "acme",
		QueuePrefix:       "acme",
		AutoCreate:        true,
		FilterPolicies:    map[string]string{"eu": `{"region":["eu"]}`},
		FilterPolicyScope: sns.FilterScopeBody,
		Pollers:           4,
		MaxMessages:       10,
		WaitTime:          20 * time.Second,
		AckDeadline:       90 * time.Second,
		Settle:            flush.Hybrid,
		SettleMaxItems:    10,
		SettleInterval:    50 * time.Millisecond,
		Body:              sq.BodyBase64,
		MaxMessageBytes:   128 << 10,
		DeadLetterQueue:   "acme-dlq",
		MaxReceiveCount:   5,
		Retention:         48 * time.Hour,
		APITimeout:        10 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a complete, working configuration was refused: %v", err)
	}
}

// TestConfigRedactsItsCredentials is for all three rendering surfaces the
// specification asks a configuration to redact through.
func TestConfigRedactsItsCredentials(t *testing.T) {
	const secret = "not-a-real-secret-4f2a"
	const token = "not-a-real-token-99bd"
	cfg := sns.Config{
		Region:          "eu-west-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: dsn.Secret(secret),
		SessionToken:    dsn.Secret(token),
		AccountID:       "000000000000",
	}
	rendered := []string{cfg.DSN(), cfg.String(), cfg.LogValue().String()}
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logger.Info("config", "cfg", cfg)
	rendered = append(rendered, buf.String())
	// An error built from the configuration is the fourth surface, and the
	// one cares about: a refusal a caller logs must not carry a
	// credential.
	bad := cfg
	bad.MaxMessageBytes = -1
	if err := bad.Validate(); err != nil {
		rendered = append(rendered, err.Error())
	}
	for i, s := range rendered {
		if strings.Contains(s, secret) {
			t.Fatalf("rendering %d carries the secret access key: %s", i, s)
		}
		if strings.Contains(s, token) {
			t.Fatalf("rendering %d carries the session token: %s", i, s)
		}
	}
	// The access key ID is NOT a secret and is rendered, which is what
	// makes a redacted DSN still say which principal it was built for.
	if !strings.Contains(cfg.DSN(), "AKIAEXAMPLE") {
		t.Fatalf("the redacted DSN does not say which principal it was built for: %s", cfg.DSN())
	}
	if !strings.Contains(cfg.DSN(), dsn.Redacted) {
		t.Fatalf("the redacted DSN carries no redaction marker: %s", cfg.DSN())
	}
}

// TestParseDSN is the parameter table, and the two things about it that are
// not obvious: a DSN with no host is the services' regional endpoints, and a
// DSN with one names an endpoint whose scheme is decided by
// insecuretransport.
func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dsn   string
		check func(*testing.T, sns.Config)
	}{
		{"regional host with credentials", "snssqs://AKIA:secret@aws?region=eu-west-1&accountid=000000000000", func(t *testing.T, c sns.Config) {
			if c.Region != "eu-west-1" {
				t.Errorf("region = %q", c.Region)
			}
			if c.Endpoint != "" {
				t.Errorf("endpoint = %q, want the SDKs' own regional endpoints", c.Endpoint)
			}
			if c.AccessKeyID != "AKIA" || c.SecretAccessKey.Reveal() != "secret" {
				t.Errorf("credentials = %q / %q", c.AccessKeyID, c.SecretAccessKey.Reveal())
			}
		}},
		{"explicit endpoint", "snssqs://k:s@127.0.0.1:4566?region=us-east-1&insecuretransport=true", func(t *testing.T, c sns.Config) {
			if c.Endpoint != "http://127.0.0.1:4566" {
				t.Errorf("endpoint = %q", c.Endpoint)
			}
			if !c.InsecureTransport {
				t.Error("insecuretransport was not carried")
			}
		}},
		{"https endpoint", "snssqs://k:s@sns.example.com?region=eu-west-1", func(t *testing.T, c sns.Config) {
			if c.Endpoint != "https://sns.example.com" {
				t.Errorf("endpoint = %q, want https by default", c.Endpoint)
			}
		}},
		{"topic prefix in the path", "snssqs://k:s@aws/acme?region=r", func(t *testing.T, c sns.Config) {
			if c.TopicPrefix != "acme" {
				t.Errorf("topic prefix = %q", c.TopicPrefix)
			}
		}},
		{"queue prefix", "snssqs://k:s@aws?region=r&queueprefix=dev", func(t *testing.T, c sns.Config) {
			if c.QueuePrefix != "dev" {
				t.Errorf("queue prefix = %q", c.QueuePrefix)
			}
		}},
		{"filter scope", "snssqs://k:s@aws?region=r&filterscope=body", func(t *testing.T, c sns.Config) {
			if c.FilterPolicyScope != sns.FilterScopeBody {
				t.Errorf("filter scope = %v", c.FilterPolicyScope)
			}
		}},
		{"provisioning", "snssqs://k:s@aws?region=r&autocreate=true&noqueuepolicy=true", func(t *testing.T, c sns.Config) {
			if !c.AutoCreate || !c.NoQueuePolicy {
				t.Errorf("autocreate = %v, noqueuepolicy = %v", c.AutoCreate, c.NoQueuePolicy)
			}
		}},
		{"settlement policy", "snssqs://k:s@aws?region=r&settle=hybrid&settlemaxitems=5&settleinterval=100ms", func(t *testing.T, c sns.Config) {
			if c.Settle != flush.Hybrid {
				t.Errorf("settle = %v", c.Settle)
			}
			if c.SettleMaxItems != 5 || c.SettleInterval != 100*time.Millisecond {
				t.Errorf("items = %d, interval = %v", c.SettleMaxItems, c.SettleInterval)
			}
		}},
		{"body policy", "snssqs://k:s@aws?region=r&body=base64", func(t *testing.T, c sns.Config) {
			if c.Body != sq.BodyBase64 {
				t.Errorf("body = %v", c.Body)
			}
		}},
		{"consume shape", "snssqs://k:s@aws?region=r&pollers=4&maxmessages=10&waittime=20s&ackdeadline=90s&leaseguard=2s", func(t *testing.T, c sns.Config) {
			if c.Pollers != 4 || c.MaxMessages != 10 {
				t.Errorf("pollers = %d, maxmessages = %d", c.Pollers, c.MaxMessages)
			}
			if c.WaitTime != 20*time.Second || c.AckDeadline != 90*time.Second || c.LeaseGuard != 2*time.Second {
				t.Errorf("waittime = %v, ackdeadline = %v, leaseguard = %v", c.WaitTime, c.AckDeadline, c.LeaseGuard)
			}
		}},
		{"redrive", "snssqs://k:s@aws?region=r&deadletterqueue=acme-dlq&maxreceivecount=4&retention=48h", func(t *testing.T, c sns.Config) {
			if c.DeadLetterQueue != "acme-dlq" || c.MaxReceiveCount != 4 || c.Retention != 48*time.Hour {
				t.Errorf("dlq = %q, count = %d, retention = %v", c.DeadLetterQueue, c.MaxReceiveCount, c.Retention)
			}
		}},
		{"session token", "snssqs://k:s@aws?region=r&sessiontoken=tok", func(t *testing.T, c sns.Config) {
			if c.SessionToken.Reveal() != "tok" {
				t.Errorf("session token = %q", c.SessionToken.Reveal())
			}
		}},
		{"partition", "snssqs://k:s@aws?region=cn-north-1&partition=aws-cn&accountid=1", func(t *testing.T, c sns.Config) {
			if c.Partition != "aws-cn" {
				t.Errorf("partition = %q", c.Partition)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := sns.ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.dsn, err)
			}
			tc.check(t, cfg)
		})
	}
}

// TestParseDSNRefusals holds the grammar to what it refuses.
//
// The `strict` row is the one that matters most: a parameter this driver does
// not have is a construction error naming it rather than a setting nobody
// notices was ignored (ADR-010). A DSN that carried
// `filterpolicy=...` — which this driver deliberately does not offer, because
// a JSON object in a DSN is unreadable either encoded or raw (the specification) — has to say so.
func TestParseDSNRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		says string
	}{
		{"wrong scheme", "sqs://k:s@aws?region=r", ""},
		{"unknown parameter", "snssqs://k:s@aws?region=r&nonsense=1", "nonsense"},
		{"a filter policy is not a DSN parameter", "snssqs://k:s@aws?region=r&filterpolicy=%7B%7D", "filterpolicy"},
		{"two hosts", "snssqs://k:s@a.example.com,b.example.com?region=r", "one endpoint"},
		{"regional host with a port", "snssqs://k:s@aws:4566?region=r", "takes no port"},
		{"regional host with insecure transport", "snssqs://k:s@aws?region=r&insecuretransport=true", "no meaning without an endpoint"},
		{"unknown settle kind", "snssqs://k:s@aws?region=r&settle=nonsense", "A32"},
		{"unknown body policy", "snssqs://k:s@aws?region=r&body=nonsense", "auto, text or base64"},
		{"unknown filter scope", "snssqs://k:s@aws?region=r&filterscope=nonsense", "FilterPolicyScope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sns.ParseDSN(tc.dsn)
			if err == nil {
				t.Fatalf("ParseDSN(%q) was accepted", tc.dsn)
			}
			if tc.says != "" && !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal does not mention %q: %v", tc.says, err)
			}
		})
	}
}

// TestRegisterAndOpenDSN holds ADR-012: a driver registers with a registry the
// caller owns and never reaches for a global one.
func TestRegisterAndOpenDSN(t *testing.T) {
	var reg dsn.Registry
	if err := sns.Register(&reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sns.Register(nil); err == nil {
		t.Fatalf("Register(nil) was accepted")
	}
	// Opening performs no I/O, so this really does build a client against a
	// local endpoint nothing is listening on.
	c, err := reg.Open(t.Context(), "snssqs://k:s@127.0.0.1:4566?region=r&insecuretransport=true")
	if err != nil {
		t.Fatalf("opening through the registry: %v", err)
	}
	if got := c.Info().Name; got != "snssqs" {
		t.Fatalf("the client names the driver %q", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := reg.Open(t.Context(), "nosuch://host"); !errors.Is(err, mq.ErrDriverNotRegistered) {
		t.Fatalf("opening an unregistered scheme reported %v", err)
	}
}
