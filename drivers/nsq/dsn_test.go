package nsq_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestParseDSN holds the connection string for this driver, and the
// two decisions in it that are not obvious: a repeatable parameter really is
// repeatable, and `max_in_flight` is not a parameter at all.
func TestParseDSN(t *testing.T) {
	cfg, err := nq.ParseDSN("nsq://nsqd-a:4150,nsqd-b/acme." +
		"?lookupd=http://lookupd-a:4161,http://lookupd-b:4161" +
		"&admin=nsqd-a:4151&metadata=adaptive&ensuretopology=true" +
		"&ackdeadline=45s&leaseguard=3s&maxmessagebytes=524288&snappy=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if got := strings.Join(cfg.Addrs, ","); got != "nsqd-a:4150,nsqd-b:4150" {
		t.Errorf("the endpoints are %q; a host with no port takes nsqd's TCP port", got)
	}
	if len(cfg.LookupdAddrs) != 2 {
		t.Errorf("%d lookupd address(es) survived: a comma-separated list must keep every value, "+
			"or a DSN naming two nsqlookupd instances silently follows one", len(cfg.LookupdAddrs))
	}
	if cfg.TopicPrefix != "acme." {
		t.Errorf("the prefix is %q", cfg.TopicPrefix)
	}
	if cfg.Metadata != mq.MetadataAdaptive {
		t.Errorf("the metadata mode is %v", cfg.Metadata)
	}
	if !cfg.EnsureTopology || !cfg.Snappy {
		t.Errorf("a boolean did not survive: ensuretopology=%v snappy=%v", cfg.EnsureTopology, cfg.Snappy)
	}
	if cfg.AckDeadline != 45*time.Second || cfg.LeaseGuard != 3*time.Second {
		t.Errorf("a duration did not survive: %v %v", cfg.AckDeadline, cfg.LeaseGuard)
	}
	if cfg.MaxMessageBytes != 524288 {
		t.Errorf("the message limit is %d", cfg.MaxMessageBytes)
	}
}

// TestParseDSNRefusals holds ADR-010 the strict-unknown rule and the
// rest of the grammar.
func TestParseDSNRefusals(t *testing.T) {
	for name, s := range map[string]string{
		"another driver's scheme":                 "nats://host:4222",
		"an unknown parameter":                    "nsq://host:4150?max_in_flight=200",
		"an unknown metadata mode":                "nsq://host:4150?metadata=envelope",
		"a malformed duration":                    "nsq://host:4150?ackdeadline=soon",
		"a malformed boolean":                     "nsq://host:4150?snappy=maybe",
		"skipping verification without TLS":       "nsq://host:4150?insecureskipverify=true",
		"a topic prefix that is not a legal name": "nsq://host:4150/a%2Fb",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := nq.ParseDSN(s); err == nil {
				t.Fatalf("ParseDSN(%q) was accepted", s)
			}
		})
	}
}

// TestMaxInFlightIsNotADSNParameter is, written as a test
// because the example DSN shows the parameter and this driver deliberately
// does not have it.
//
// Prefetch is the core's: it is resolved from the consumer configuration and
// handed to the driver in SourceConfig.Prefetch. A DSN that set it would be a
// second owner of one number, and the driver would then have to choose between
// them — silently, on every subscription.
func TestMaxInFlightIsNotADSNParameter(t *testing.T) {
	for _, spelling := range []string{"max_in_flight", "maxinflight", "prefetch"} {
		if _, err := nq.ParseDSN("nsq://host:4150?" + spelling + "=200"); err == nil {
			t.Fatalf("%q was accepted as a DSN parameter; prefetch is the core's ", spelling)
		}
	}
}

// TestTheRenderedDSNParsesBackToTheSameBehaviour is what makes Config.DSN
// useful in a log line: everything but the credential survives the round trip.
func TestTheRenderedDSNParsesBackToTheSameBehaviour(t *testing.T) {
	want := nq.Config{
		Addrs:           []string{"nsqd:4150"},
		LookupdAddrs:    []string{"http://lookupd:4161"},
		AdminAddrs:      []string{"nsqd:4151"},
		TopicPrefix:     "acme.",
		Metadata:        mq.MetadataPortableEnvelope,
		EnsureTopology:  true,
		MaxMessageBytes: 1 << 19,
		AckDeadline:     45 * time.Second,
		ClientID:        "worker-1",
	}
	got, err := nq.ParseDSN(want.DSN())
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", want.DSN(), err)
	}
	if got.DSN() != want.DSN() {
		t.Fatalf("the rendering is not stable:\n first %s\nsecond %s", want.DSN(), got.DSN())
	}
}
