package sqs_test

import (
	"testing"
	"time"

	sq "github.com/iqhive/iqmq/drivers/sqs"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
)

// TestValidateIsPure holds the specification's "validate config before starting background
// work": every refusal below is reachable without a request, and the whole
// table is checked for the code the specification asks a configuration failure to carry.
func TestValidateIsPure(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  sq.Config
	}{
		{"body policy out of range", sq.Config{Body: sq.BodyEncoding(9)}},
		{"settlement policy out of range", sq.Config{Settle: flush.Kind(9)}},
		{"negative pollers", sq.Config{Pollers: -1}},
		{"maxmessages over the service limit", sq.Config{MaxMessages: 11}},
		{"settlemaxitems over the service limit", sq.Config{SettleMaxItems: 11}},
		{"negative duration", sq.Config{WaitTime: -time.Second}},
		{"waittime over the service limit", sq.Config{WaitTime: 21 * time.Second}},
		{"ackdeadline over the service limit", sq.Config{AckDeadline: 13 * time.Hour}},
		{"message bytes over the service limit", sq.Config{MaxMessageBytes: 1 << 21}},
		{"control byte in the region", sq.Config{Region: "eu\x00west"}},
		{"secret without a key", sq.Config{SecretAccessKey: "s"}},
		{"key without a secret", sq.Config{AccessKeyID: "k"}},
		{"endpoint without a scheme", sq.Config{Endpoint: "127.0.0.1:9324", InsecureTransport: true}},
		{"http endpoint without the insecure flag", sq.Config{Endpoint: "http://127.0.0.1:9324"}},
		{"insecure without an endpoint", sq.Config{InsecureTransport: true}},
		{"prefix outside the name grammar", sq.Config{QueuePrefix: "dev/"}},
		{"dead-letter queue outside the name grammar", sq.Config{DeadLetterQueue: "dead letter"}},
		{"maxreceivecount without a queue", sq.Config{MaxReceiveCount: 2}},
		{"content-based dedup outside FIFO", sq.Config{ContentBasedDeduplication: true}},
		{"empty topic in QueueURLs", sq.Config{QueueURLs: map[string]string{"": "http://x"}}},
		{"empty URL in QueueURLs", sq.Config{QueueURLs: map[string]string{"t": ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatal("Validate accepted it")
			}
			if got := mq.CodeOf(err); got != mq.CodeInvalidConfig {
				t.Fatalf("code = %v, want CodeInvalidConfig: the specification makes a configuration failure comparable by code", got)
			}
		})
	}
}

// TestValidateAcceptsTheUsefulShapes is the other direction: a table of
// refusals that refused everything would pass the test above and ship a
// driver nobody can configure.
func TestValidateAcceptsTheUsefulShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  sq.Config
	}{
		{"zero", sq.Config{}},
		{"regional with credentials", sq.Config{Region: "eu-west-1", AccessKeyID: "k", SecretAccessKey: "s"}},
		{"emulator", sq.Config{Endpoint: "http://127.0.0.1:9324", InsecureTransport: true, AccessKeyID: "x", SecretAccessKey: "x"}},
		{"fifo with dedup", sq.Config{FIFO: true, ContentBasedDeduplication: true}},
		{"redrive", sq.Config{DeadLetterQueue: "dead", MaxReceiveCount: 3}},
		{"service maxima", sq.Config{
			MaxMessages: 10, SettleMaxItems: 10, WaitTime: 20 * time.Second,
			AckDeadline: 12 * time.Hour, MaxMessageBytes: 1 << 20,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

// TestProfileIsNeverEmpty is for this driver: the two profiles differ in
// seven capability claims, so DriverInfo.Profile has to say which one a caller
// is holding.
func TestProfileIsNeverEmpty(t *testing.T) {
	if got := (sq.Config{}).Profile(); got != "standard" {
		t.Fatalf("the default profile is %q", got)
	}
	if got := (sq.Config{FIFO: true}).Profile(); got != "fifo" {
		t.Fatalf("the FIFO profile is %q", got)
	}
}

// TestOpenRefusesBeforeAnyRequest checks that a bad configuration is refused
// at construction, and that opening with a nil client is refused too.
func TestOpenRefusesBeforeAnyRequest(t *testing.T) {
	if _, err := sq.Open(t.Context(), sq.Config{MaxMessages: 11}); err == nil {
		t.Fatal("Open accepted a configuration Validate refuses")
	}
	if _, err := sq.OpenWithClient(t.Context(), nil, 0, sq.Config{}); err == nil {
		t.Fatal("OpenWithClient accepted a nil client")
	}
	if _, err := sq.BackendWithAPI(t.Context(), nil, nil, 0, sq.Config{}); err == nil {
		t.Fatal("BackendWithAPI accepted a nil API")
	}
	// An ownership value outside the enumeration is refused before any
	// background work, which is the rule and driverkit's CheckOwnership.
	if _, err := sq.BackendWithAPI(t.Context(), nil, nil, 99, sq.Config{}); err == nil {
		t.Fatal("BackendWithAPI accepted an ownership value outside the enumeration")
	}
}
