package nsq_test

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/dsn"
)

// TestValidateRefusesWhatCannotWork holds the specification's "validate config before
// starting background work": every refusal here is pure, so a typo is an
// immediate answer rather than a dial that times out.
func TestValidateRefusesWhatCannotWork(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  nq.Config
		code mq.ErrorCode
	}{
		"no address at all":       {nq.Config{}, mq.CodeInvalidConfig},
		"an address with a space": {nq.Config{Addrs: []string{"host :4150"}}, mq.CodeInvalidConfig},
		"a non-ASCII address":     {nq.Config{Addrs: []string{"h\xc5st:4150"}}, mq.CodeInvalidConfig},
		"a lookupd with a control byte": {
			nq.Config{Addrs: []string{"h:4150"}, LookupdAddrs: []string{"http://h\x00/"}}, mq.CodeInvalidConfig,
		},
		"a topic prefix that is not a legal name": {
			nq.Config{Addrs: []string{"h:4150"}, TopicPrefix: "a/b"}, mq.CodeInvalidConfig,
		},
		"a client identifier with a control byte": {
			nq.Config{Addrs: []string{"h:4150"}, ClientID: "a\x01b"}, mq.CodeInvalidConfig,
		},
		"a negative size limit": {
			nq.Config{Addrs: []string{"h:4150"}, MaxMessageBytes: -1}, mq.CodeInvalidConfig,
		},
		"a body limit smaller than the message limit": {
			nq.Config{Addrs: []string{"h:4150"}, MaxMessageBytes: 1 << 20, MaxBodyBytes: 1 << 10}, mq.CodeInvalidConfig,
		},
		"a negative duration": {
			nq.Config{Addrs: []string{"h:4150"}, AckDeadline: -time.Second}, mq.CodeInvalidConfig,
		},
		"a heartbeat at the read deadline": {
			nq.Config{Addrs: []string{"h:4150"}, ReadTimeout: 10 * time.Second, HeartbeatInterval: 10 * time.Second}, mq.CodeInvalidConfig,
		},
		"a lease guard past the deadline": {
			nq.Config{Addrs: []string{"h:4150"}, AckDeadline: time.Second, LeaseGuard: time.Minute}, mq.CodeInvalidConfig,
		},
		"an ack deadline past nsqd's maximum": {
			nq.Config{Addrs: []string{"h:4150"}, AckDeadline: 2 * nq.DefaultMaxAckDeadline}, mq.CodeInvalidConfig,
		},
		"both compressions": {
			nq.Config{Addrs: []string{"h:4150"}, Deflate: true, Snappy: true}, mq.CodeInvalidConfig,
		},
		"a deflate level outside 1..9": {
			nq.Config{Addrs: []string{"h:4150"}, Deflate: true, DeflateLevel: 12}, mq.CodeInvalidConfig,
		},
		"an unknown metadata mode": {
			nq.Config{Addrs: []string{"h:4150"}, Metadata: mq.MetadataMode(9)}, mq.CodeInvalidConfig,
		},
		"skipping verification without TLS": {
			nq.Config{Addrs: []string{"h:4150"}, InsecureSkipVerify: true}, mq.CodeInvalidConfig,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if mq.CodeOf(err) != tc.code {
				t.Fatalf("Validate answered %v (%v), want %v", mq.CodeOf(err), err, tc.code)
			}
		})
	}
	ok := nq.Config{
		Addrs:              []string{"127.0.0.1:4150"},
		LookupdAddrs:       []string{"http://127.0.0.1:4161"},
		TLS:                &tls.Config{MinVersion: tls.VersionTLS12},
		InsecureSkipVerify: true,
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("a workable configuration was refused: %v", err)
	}
	// A configuration with only lookupd addresses can consume and not
	// publish, which Validate accepts and the producer refuses.
	consumeOnly := nq.Config{LookupdAddrs: []string{"http://127.0.0.1:4161"}}
	if err := consumeOnly.Validate(); err != nil {
		t.Fatalf("a consume-only configuration was refused: %v", err)
	}
}

// TestARenderedConfigCarriesNoCredential is neither String, nor
// LogValue, nor DSN may carry the secret, and slog prefers LogValue over
// String so both have to be right.
func TestARenderedConfigCarriesNoCredential(t *testing.T) {
	const secret = "iqmq-nsq-secret-not-a-real-credential"
	cfg := nq.Config{Addrs: []string{"127.0.0.1:4150"}, AuthSecret: dsn.Secret(secret)}
	for name, got := range map[string]string{
		"DSN":      cfg.DSN(),
		"String":   cfg.String(),
		"LogValue": cfg.LogValue().String(),
	} {
		if strings.Contains(got, secret) {
			t.Errorf("%s carries the credential: %s", name, got)
		}
		if !strings.Contains(got, dsn.Redacted) {
			t.Errorf("%s does not mark the credential as redacted: %s", name, got)
		}
	}
	// And through slog, which is how it actually reaches a log pipeline.
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("config", "cfg", cfg)
	if strings.Contains(b.String(), secret) {
		t.Errorf("a slog record carries the credential: %s", b.String())
	}
}

// TestTheDefaultsMirrorNSQdsOwnFlags is the one class of default this driver
// cannot discover: nsqd reports none of its limits over either protocol, so
// each of these is a MIRROR of a flag and the driver page says so.
func TestTheDefaultsMirrorNSQdsOwnFlags(t *testing.T) {
	for name, tc := range map[string]struct{ got, want int64 }{
		"--max-msg-size":    {nq.DefaultMaxMessageBytes, 1 << 20},
		"--max-body-size":   {nq.DefaultMaxBodyBytes, 5 << 20},
		"--max-req-timeout": {int64(nq.DefaultMaxRequeueDelay), int64(time.Hour)},
		"--msg-timeout":     {int64(nq.DefaultAckDeadline), int64(time.Minute)},
		"--max-msg-timeout": {int64(nq.DefaultMaxAckDeadline), int64(15 * time.Minute)},
	} {
		if tc.got != tc.want {
			t.Errorf("the default for %s is %d, want nsqd's own %d", name, tc.got, tc.want)
		}
	}
}

// TestRegisterNeedsARegistry pins ADR-012's rule that a driver never reaches
// for a global registry of its own.
func TestRegisterNeedsARegistry(t *testing.T) {
	if err := nq.Register(nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Register(nil) answered %v", err)
	}
	r := dsn.NewRegistry()
	if err := nq.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := r.Lookup(nq.Scheme); !ok {
		t.Fatalf("the registry does not know %q", nq.Scheme)
	}
	if err := nq.Register(r); err == nil {
		t.Fatal("a second registration of one scheme was accepted")
	}
	_ = errors.Is(nil, nil)
}
