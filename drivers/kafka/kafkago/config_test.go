package kafkago_test

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	"bitbucket.org/iqhive/iqmq/driverkit/offset"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestValidateRefusesBeforeAnyIO is a configuration is refused at
// construction and without a dial, so a typo is an immediate answer rather
// than a connection timeout.
func TestValidateRefusesBeforeAnyIO(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  kq.Config
	}{
		{"empty-broker", kq.Config{Brokers: []string{""}}},
		{"non-ascii-broker", kq.Config{Brokers: []string{"hos\xfft:9092"}}},
		{"control-char-broker", kq.Config{Brokers: []string{"host:9092\n"}}},
		{"control-char-client-id", kq.Config{ClientID: "a\nb"}},
		{"bad-prefix", kq.Config{TopicPrefix: "a/b"}},
		{"sasl-without-user", kq.Config{SASL: kq.SASLPlain}},
		{"user-without-sasl", kq.Config{Username: "u"}},
		{"bad-sasl", kq.Config{Username: "u", SASL: kq.SASLMechanism(99)}},
		{"insecure-without-tls", kq.Config{InsecureSkipVerify: true}},
		{"bad-balancer", kq.Config{Balancer: kq.GroupBalancer(9)}},
		{"negative-duration", kq.Config{SessionTimeout: -time.Second}},
		{"negative-metadata-ttl", kq.Config{MetadataTTL: -time.Second}},
		{"negative-bound", kq.Config{FetchMaxBytes: -1}},
		{"negative-attempts", kq.Config{MaxAttempts: -1}},
		{"bad-topology", kq.Config{Partitions: -2}},
		{"fetch-bounds-inverted", kq.Config{FetchMinBytes: 1024, FetchMaxBytes: 16}},
		{"bad-tracker", kq.Config{Tracker: offset.Kind(99)}},
		{"bad-dead-letter", kq.Config{NackConfig: nack.Config{DeadLetterTopic: "a/b"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", tc.cfg)
			}
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig && code != mq.CodeInvalidArgument {
				t.Errorf("Validate refused with %v, want an invalid-configuration code", code)
			}
		})
	}
	// And the zero value is usable, which is what "every default is a bound"
	// means: nothing in the resolved configuration is unlimited.
	if err := (kq.Config{}).Validate(); err != nil {
		t.Fatalf("the zero Config is refused: %v", err)
	}
}

// TestFetchBoundsAreRefusedRatherThanPanicked is a refusal this driver makes
// because the client does not.
//
// kafka-go's NewReader PANICS on MinBytes above MaxBytes, and a driver builds
// its readers on a goroutine of a consumer group generation — so the panic
// would take the process down from inside a background loop, long after the
// configuration was accepted. The specification puts the refusal at construction.
func TestFetchBoundsAreRefusedRatherThanPanicked(t *testing.T) {
	cfg := kq.Config{FetchMinBytes: 1 << 20, FetchMaxBytes: 1024}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted FetchMinBytes above FetchMaxBytes; kafka-go panics on that inside NewReader, from a goroutine this driver started")
	}
	if _, err := kq.OpenBackend(t.Context(), cfg); err == nil {
		t.Fatal("OpenBackend accepted it")
	}
}

// TestConfigRedactsTheCredential is neither String, nor LogValue, nor an
// error built from a configuration may carry the password.
func TestConfigRedactsTheCredential(t *testing.T) {
	cfg := kq.Config{
		Brokers:  []string{"host:9092"},
		Username: "u",
		Password: "s3cr3t",
		SASL:     kq.SASLScramSHA512,
		TLS:      &tls.Config{MinVersion: tls.VersionTLS13},
	}
	for name, s := range map[string]string{
		"String":   cfg.String(),
		"DSN":      cfg.DSN(),
		"LogValue": cfg.LogValue().String(),
	} {
		if strings.Contains(s, "s3cr3t") {
			t.Errorf("%s carries the password: %s", name, s)
		}
		if !strings.Contains(s, "u") {
			t.Errorf("%s does not carry the user name, which is not a secret: %s", name, s)
		}
	}
}

// TestConfigDSNRendersTheResolvedConfiguration is what makes a rendered
// configuration useful in a log: it shows the values the driver will actually
// use, not the zeros the caller left.
func TestConfigDSNRendersTheResolvedConfiguration(t *testing.T) {
	s := (kq.Config{}).DSN()
	if !strings.HasPrefix(s, "kafkago://") {
		t.Errorf("DSN %q does not use this driver's own scheme; a kafka:// DSN is the franz-go driver's", s)
	}
	for _, want := range []string{
		"127.0.0.1:9092",
		"clientid=iqmq",
		"balancer=range",
		"batchmaxrecords=1",
		"metadatattl=1s",
		"commitinterval=250ms",
		"revokewait=5s",
		"maxattempts=3",
		"tracker=auto",
		"nack=auto",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("the rendered zero configuration does not mention %q: %s", want, s)
		}
	}
}

// TestSASLAndBalancerNames pins the enumeration names a DSN spells, because
// they are a public grammar: a name that changed would break a connection
// string somebody wrote down.
func TestSASLAndBalancerNames(t *testing.T) {
	if got, want := kq.SASLNames(), []string{"none", "plain", "scram-sha-256", "scram-sha-512"}; !equalStrings(got, want) {
		t.Errorf("SASLNames = %q, want %q", got, want)
	}
	if got, want := kq.GroupBalancerNames(), []string{"range", "roundrobin"}; !equalStrings(got, want) {
		t.Errorf("GroupBalancerNames = %q, want %q", got, want)
	}
	if s := kq.SASLMechanism(99).String(); !strings.Contains(s, "?") {
		t.Errorf("an out-of-range mechanism renders as %q", s)
	}
	if s := kq.GroupBalancer(99).String(); !strings.Contains(s, "?") {
		t.Errorf("an out-of-range assignor renders as %q", s)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
