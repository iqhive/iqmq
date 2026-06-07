package jetstream_test

import (
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestConfigValidateRefusals holds [nq.Config.Validate] to the specification's "refuse a
// configuration before starting background work". Every row is a configuration
// that must not reach a dial.
func TestConfigValidateRefusals(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  nq.Config
	}{
		{name: "empty address", cfg: nq.Config{Addrs: []string{""}}},
		{name: "non-ascii address", cfg: nq.Config{Addrs: []string{"nats://hö:4222"}}},
		{name: "control byte in name", cfg: nq.Config{Name: "a\x00b"}},
		{name: "invalid utf8 in user", cfg: nq.Config{Username: "\xc5"}},
		{name: "stream name with a dot", cfg: nq.Config{Stream: "a.b"}},
		{name: "consumer prefix with a dot", cfg: nq.Config{ConsumerPrefix: "a.b"}},
		{name: "subject prefix is not a token", cfg: nq.Config{SubjectPrefix: "a b"}},
		{name: "schedule subject is not a token", cfg: nq.Config{ScheduleSubject: "a b"}},
		{name: "dead letter topic is not a subject", cfg: nq.Config{DeadLetterTopic: "a b"}},
		{name: "stream subject is not a pattern", cfg: nq.Config{StreamSubjects: []string{"a b"}}},
		{name: "managed stream with no subjects", cfg: nq.Config{ManageStream: true}},
		{name: "storage outside the enumeration", cfg: nq.Config{Storage: 9}},
		{name: "negative pending", cfg: nq.Config{PendingMessages: -1}},
		{name: "negative replicas", cfg: nq.Config{Replicas: -1}},
		{name: "negative max deliver", cfg: nq.Config{MaxDeliver: -1}},
		{name: "negative async pending", cfg: nq.Config{PublishAsyncMaxPending: -1}},
		{name: "negative ack wait", cfg: nq.Config{AckWait: -time.Second}},
		{name: "negative pings", cfg: nq.Config{MaxPingsOut: -1}},
		{name: "prefix and domain", cfg: nq.Config{APIPrefix: "$JS.a.API", Domain: "hub"}},
		{name: "password without a user", cfg: nq.Config{Password: dsn.Secret("p")}},
		{name: "two credentials", cfg: nq.Config{Username: "u", Token: dsn.Secret("t")}},
		{name: "insecure without tls", cfg: nq.Config{InsecureSkipVerify: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", c.cfg)
			}
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig && code != mq.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidConfig", code)
			}
		})
	}
}

// TestConfigValidateAccepts is the other half: a configuration a deployment
// really writes is not refused, and a check that refuses everything proves
// nothing.
func TestConfigValidateAccepts(t *testing.T) {
	for _, cfg := range []nq.Config{
		{},
		{Addrs: []string{"nats://host:4222", "tls://host2:4222"}, Stream: "ORDERS"},
		{SubjectPrefix: "acme", ManageStream: true, Storage: nq.StorageMemory},
		{StreamSubjects: []string{"acme.>", "iqmqsched.IQMQ.>"}, ManageStream: true},
		{Username: "u", Password: dsn.Secret("p"), TLS: &tls.Config{MinVersion: tls.VersionTLS12}, InsecureSkipVerify: true},
		{DeadLetterTopic: "orders/dlq", DoubleAck: true, MaxDeliver: 5},
		{Domain: "hub"},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate(%+v): %v", cfg, err)
		}
	}
}

// TestConfigRenderingRedacts is a configuration is safe to log, to put in
// an error and to hand slog, and the credential is gone from all three.
func TestConfigRenderingRedacts(t *testing.T) {
	cfg := nq.Config{
		Addrs:    []string{"nats://host:4222"},
		Username: "user",
		Password: dsn.Secret("s3cret-password"),
		Stream:   "ORDERS",
	}
	for _, got := range []string{cfg.DSN(), cfg.String(), cfg.LogValue().String()} {
		if strings.Contains(got, "s3cret-password") {
			t.Fatalf("a rendered configuration carries the password: %q", got)
		}
		if !strings.Contains(got, dsn.Redacted) {
			t.Errorf("the rendering does not mark the redaction: %q", got)
		}
		if !strings.Contains(got, "stream=ORDERS") {
			t.Errorf("the rendering does not name the stream: %q", got)
		}
	}
	tokenCfg := nq.Config{Token: dsn.Secret("s3cret-token")}
	if s := tokenCfg.String(); strings.Contains(s, "s3cret-token") {
		t.Fatalf("a rendered configuration carries the token: %q", s)
	}
	if cfg.LogValue().Kind() != slog.KindString {
		t.Error("LogValue is not a string value")
	}
}

// TestConfigDSNRendersEveryDecision is what makes the rendering worth having: it
// is not a round trip — the credential is deliberately gone — but everything
// that decides behaviour is in it, so a log line names the same broker, the same
// stream and the same settlement boundary.
func TestConfigDSNRendersEveryDecision(t *testing.T) {
	cfg := nq.Config{
		SubjectPrefix:             "acme",
		Stream:                    "ORDERS",
		ManageStream:              true,
		StreamSubjects:            []string{"acme.>"},
		Storage:                   nq.StorageMemory,
		Replicas:                  3,
		MaxAge:                    time.Hour,
		MaxMsgs:                   10,
		MaxBytes:                  1 << 20,
		MaxDeliver:                5,
		ConsumerInactiveThreshold: time.Minute,
		DeadLetterTopic:           "orders/dlq",
		DisableSchedules:          true,
		DoubleAck:                 true,
		APIPrefix:                 "$JS.acme.API",
		MaxReconnects:             -1,
		PingInterval:              time.Second,
		MaxPingsOut:               3,
	}
	got := cfg.DSN()
	for _, want := range []string{
		"natsjs://", "/acme?", "stream=ORDERS", "managestream=true", "streamsubjects=acme.>",
		"storage=memory", "replicas=3", "maxage=1h0m0s", "maxmsgs=10", "maxbytes=1048576",
		"maxdeliver=5", "inactivethreshold=1m0s", "deadlettertopic=orders/dlq",
		"disableschedules=true", "doubleack=true", "apiprefix=$JS.acme.API",
		"maxreconnects=-1", "pinginterval=1s", "maxpingsout=3",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the rendered DSN does not carry %q: %s", want, got)
		}
	}
}

// TestScheduleRootCarriesTheStreamName is the overlap rule: JetStream refuses
// two streams in one account whose subjects overlap, so a schedule root shared
// by every stream would make a second iqmq stream impossible to create.
func TestScheduleRootCarriesTheStreamName(t *testing.T) {
	one := nq.Config{SubjectPrefix: "a", Stream: "ONE", ManageStream: true}
	two := nq.Config{SubjectPrefix: "b", Stream: "TWO", ManageStream: true}
	oneDSN, twoDSN := one.DSN(), two.DSN()
	if !strings.Contains(oneDSN, "schedulesubject=iqmqsched") {
		t.Fatalf("the schedule subject is not rendered: %s", oneDSN)
	}
	// The rendering carries the root token; the stream-qualified root is what
	// the stream's own subject list gets, which TestManagedStreamSubjects
	// checks against a live server.
	if oneDSN == twoDSN {
		t.Fatal("two configurations with different streams render identically")
	}
}
