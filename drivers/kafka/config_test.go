package kafka_test

import (
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/dsn"
)

// TestConfigValidate covers every refusal, because the specification requires a driver to
// refuse a configuration before it starts background work and a validation
// that dialled would turn a typo into a connection timeout.
func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  kq.Config
		ok   bool
	}{
		{"zero is usable", kq.Config{}, true},
		{"brokers", kq.Config{Brokers: []string{"127.0.0.1:9092", "kafka-2:9092"}}, true},
		{"empty broker", kq.Config{Brokers: []string{""}}, false},
		{"broker with a space", kq.Config{Brokers: []string{"kafka 1:9092"}}, false},
		{"broker with a control byte", kq.Config{Brokers: []string{"kafka\x00:9092"}}, false},
		{"broker with a high byte", kq.Config{Brokers: []string{"kafka\xc5:9092"}}, false},
		{"client id with a newline", kq.Config{ClientID: "a\nb"}, false},
		{"prefix with a slash", kq.Config{TopicPrefix: "acme/"}, false},
		{"prefix", kq.Config{TopicPrefix: "acme."}, true},
		{"sasl without a user", kq.Config{SASL: kq.SASLPlain}, false},
		{"user without sasl", kq.Config{Username: "u"}, false},
		{"sasl with a user", kq.Config{SASL: kq.SASLScramSHA512, Username: "u", Password: "p"}, true},
		{"sasl outside the enumeration", kq.Config{SASL: kq.SASLMechanism(9), Username: "u"}, false},
		{"insecure without tls", kq.Config{InsecureSkipVerify: true}, false},
		{"insecure with tls", kq.Config{TLS: &tls.Config{MinVersion: tls.VersionTLS12}, InsecureSkipVerify: true}, true},
		{"negative timeout", kq.Config{SessionTimeout: -time.Second}, false},
		{"negative bound", kq.Config{FetchMaxBytes: -1}, false},
		{"negative partitions", kq.Config{Partitions: -2}, false},
		{"cluster default partitions", kq.Config{Partitions: -1, ReplicationFactor: -1}, true},
		{"tracker outside the family", kq.Config{Tracker: 99}, false},
		{"custom nack with no function", kq.Config{Nack: nack.Custom}, false},
		{"dead-letter topic with a slash", kq.Config{NackConfig: nack.Config{DeadLetterTopic: "a/b"}}, false},
		{"dead-letter topic", kq.Config{NackConfig: nack.Config{DeadLetterTopic: "orders.dlq"}}, true},
		{"negative linger is no linger", kq.Config{Linger: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("Validate = %v (code %v), want CodeInvalidConfig", err, mq.CodeOf(err))
			}
		})
	}
}

// TestConfigRenderingRedacts is the rule: a configuration is safe to log,
// and every one of the three rendering surfaces has to be.
func TestConfigRenderingRedacts(t *testing.T) {
	const secret = "s3cr3t-do-not-print"
	cfg := kq.Config{
		Brokers:  []string{"kafka-1:9092"},
		Username: "svc",
		Password: dsn.Secret(secret),
		SASL:     kq.SASLScramSHA256,
	}
	for name, rendered := range map[string]string{
		"DSN":      cfg.DSN(),
		"String":   cfg.String(),
		"LogValue": cfg.LogValue().String(),
		"slog":     slogText(cfg),
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("%s rendered the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, dsn.Redacted) {
			t.Fatalf("%s does not say the password was redacted: %s", name, rendered)
		}
		if !strings.Contains(rendered, "svc") || !strings.Contains(rendered, "kafka-1:9092") {
			t.Fatalf("%s lost the part that names the broker: %s", name, rendered)
		}
	}
	// The reveal is the one route, and it is explicit.
	if cfg.Password.Reveal() != secret {
		t.Fatal("Reveal did not return the password")
	}
}

// slogText renders a configuration through a slog handler, which is the
// surface an application actually logs through.
func slogText(cfg kq.Config) string {
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("open", "config", cfg)
	return b.String()
}

// TestConfigDSNRoundTripsTheBehaviour: the rendering is not a credential
// round trip and is not meant to be one, but everything else has to survive,
// or a rendered configuration would name a different broker from the one in
// use.
func TestConfigDSNRoundTripsTheBehaviour(t *testing.T) {
	cfg := kq.Config{
		Brokers:           []string{"kafka-1:9092", "kafka-2:9092"},
		ClientID:          "orders-svc",
		TopicPrefix:       "acme.",
		CommitInterval:    time.Second,
		RevokeWait:        3 * time.Second,
		Partitions:        6,
		ReplicationFactor: 3,
		AutoCreateTopics:  true,
		ReadCommitted:     true,
		NackConfig:        nack.Config{DeadLetterTopic: "orders.dlq", MaxAttempts: 5, Retries: 2, Delay: 250 * time.Millisecond},
	}
	back, err := kq.ParseDSN(cfg.DSN())
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", cfg.DSN(), err)
	}
	for _, tc := range []struct{ name, want, got string }{
		{"client id", cfg.ClientID, back.ClientID},
		{"prefix", cfg.TopicPrefix, back.TopicPrefix},
		{"dead-letter topic", cfg.NackConfig.DeadLetterTopic, back.NackConfig.DeadLetterTopic},
	} {
		if tc.got != tc.want {
			t.Errorf("%s came back as %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if back.CommitInterval != cfg.CommitInterval || back.RevokeWait != cfg.RevokeWait {
		t.Errorf("timings came back as %v/%v", back.CommitInterval, back.RevokeWait)
	}
	if back.Partitions != cfg.Partitions || back.ReplicationFactor != cfg.ReplicationFactor {
		t.Errorf("topology came back as %d/%d", back.Partitions, back.ReplicationFactor)
	}
	if !back.AutoCreateTopics || !back.ReadCommitted {
		t.Errorf("the booleans came back as %v/%v", back.AutoCreateTopics, back.ReadCommitted)
	}
	if back.NackConfig.MaxAttempts != 5 || back.NackConfig.Retries != 2 || back.NackConfig.Delay != 250*time.Millisecond {
		t.Errorf("the nack configuration came back as %+v", back.NackConfig)
	}
	if len(back.Brokers) != 2 || back.Brokers[0] != "kafka-1:9092" {
		t.Errorf("brokers came back as %v", back.Brokers)
	}
}

// TestSASLNames keeps the DSN spelling and the enumeration in step: a name a
// DSN accepts has to be the name the configuration renders.
func TestSASLNames(t *testing.T) {
	names := kq.SASLNames()
	if len(names) != 4 {
		t.Fatalf("SASLNames() = %v", names)
	}
	for i, n := range names {
		if got := kq.SASLMechanism(i).String(); got != n {
			t.Fatalf("SASLMechanism(%d) = %q, want %q", i, got, n)
		}
	}
	if kq.SASLMechanism(4).Valid() || kq.SASLMechanism(4).String() != "SASLMechanism(?)" {
		t.Fatal("a mechanism outside the enumeration is reported as valid")
	}
}
