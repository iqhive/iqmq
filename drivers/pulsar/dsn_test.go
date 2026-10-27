package pulsar_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSN holds the connection string's grammar and its parameter table.
func TestParseDSN(t *testing.T) {
	for name, tc := range map[string]struct {
		in    string
		check func(*testing.T, pd.Config)
		fail  bool
	}{
		"the minimum": {
			in: "pulsar://broker",
			check: func(t *testing.T, c pd.Config) {
				if c.ServiceURL != "pulsar://broker:6650" {
					t.Fatalf("ServiceURL is %q; a DSN endpoint that names no port takes the binary "+
						"protocol's, not the administrative API's", c.ServiceURL)
				}
			},
		},
		"several brokers": {
			in: "pulsar://a:6650,b:6650",
			check: func(t *testing.T, c pd.Config) {
				if c.ServiceURL != "pulsar://a:6650,b:6650" {
					t.Fatalf("ServiceURL is %q", c.ServiceURL)
				}
			},
		},
		"the TLS scheme": {
			in: "pulsar+ssl://broker",
			check: func(t *testing.T, c pd.Config) {
				if c.ServiceURL != "pulsar+ssl://broker:6651" {
					t.Fatalf("ServiceURL is %q; the TLS scheme's default port is 6651", c.ServiceURL)
				}
				// The scheme is what turns TLS on inside pulsar-client-go;
				// a *tls.Config here would make it ignore the DSN's
				// trustcerts, insecureskipverify and authname=tls.
				// TestAPulsarSSLDSNVerifiesWithItsTrustStore
				// is the handshake that proves the connection is TLS.
				if c.TLS != nil {
					t.Fatal("the TLS scheme produced a *tls.Config, which pulsar-client-go uses as given, " +
						"ignoring the DSN's trust store, insecure flag and TLS client certificate")
				}
			},
		},
		"the path is the namespace": {
			in: "pulsar://broker/acme/billing",
			check: func(t *testing.T, c pd.Config) {
				if c.Tenant != "acme" || c.Namespace != "billing" {
					t.Fatalf("the path gave tenant %q namespace %q", c.Tenant, c.Namespace)
				}
			},
		},
		"the third path segment is the prefix": {
			in: "pulsar://broker/acme/billing/iqmq-",
			check: func(t *testing.T, c pd.Config) {
				if c.TopicPrefix != "iqmq-" {
					t.Fatalf("the path gave prefix %q", c.TopicPrefix)
				}
			},
		},
		"a fourth path segment": {in: "pulsar://broker/a/b/c/d", fail: true},
		"the parameters": {
			in: "pulsar://broker/acme/ns?admin=http://broker:8080&subscriptiontype=keyshared" +
				"&metadata=adaptive&acktimeout=5s&leaseguard=1s&nackredeliverydelay=250ms" +
				"&deadlettertopic=orders-dlq&maxattempts=4&compression=lz4&hashing=murmur3" +
				"&receiverqueuesize=64&retryenable=true&retrytopic=orders-retry&readcompacted=true",
			check: func(t *testing.T, c pd.Config) {
				if c.SubscriptionType != pd.SubscriptionKeyShared {
					t.Fatalf("subscriptiontype gave %v", c.SubscriptionType)
				}
				if c.Metadata != mq.MetadataAdaptive {
					t.Fatalf("metadata gave %v", c.Metadata)
				}
				if c.AckTimeout != 5*time.Second || c.LeaseGuard != time.Second {
					t.Fatalf("the lease came out as %v/%v", c.AckTimeout, c.LeaseGuard)
				}
				if c.MaxAttempts != 4 || c.DeadLetterTopic != "orders-dlq" {
					t.Fatalf("the dead-letter policy came out as %d/%q", c.MaxAttempts, c.DeadLetterTopic)
				}
				if c.Compression != pd.CompressionLZ4 || c.Hashing != pd.HashingMurmur3 {
					t.Fatalf("compression/hashing came out as %v/%v", c.Compression, c.Hashing)
				}
				if !c.RetryEnable || c.RetryTopic != "orders-retry" || !c.ReadCompacted {
					t.Fatal("the retry-topic and compaction parameters were not read")
				}
			},
		},
		"the token is a secret": {
			in: "pulsar://broker?token=abcdef",
			check: func(t *testing.T, c pd.Config) {
				if c.Token.Reveal() != "abcdef" {
					t.Fatalf("the token came out as %q", c.Token.Reveal())
				}
				if strings.Contains(c.String(), "abcdef") {
					t.Fatal("the rendered configuration carries the token")
				}
			},
		},
		// The shared parser's own rule, which is the point of using it:
		// a parameter this driver does not have is a construction error
		// naming it, not a setting nobody notices was ignored.
		"an unknown parameter":      {in: "pulsar://broker?prefetch=100", fail: true},
		"a bad enumeration":         {in: "pulsar://broker?subscriptiontype=roundrobin", fail: true},
		"a bad duration":            {in: "pulsar://broker?acktimeout=soon", fail: true},
		"the wrong scheme":          {in: "kafka://broker", fail: true},
		"insecure without TLS":      {in: "pulsar://broker?insecureskipverify=true", fail: true},
		"insecure with TLS":         {in: "pulsar+ssl://broker?insecureskipverify=true"},
		"a negative attempt bound":  {in: "pulsar://broker?maxattempts=-1&deadlettertopic=d", fail: true},
		"batch-index without admin": {in: "pulsar://broker?enablebatchindexack=true", fail: true},
		// Past uint32 the conversion wrapped, so 4294967297 became 1.
		"an attempt bound past uint32": {in: "pulsar://broker?maxattempts=4294967297&deadlettertopic=d", fail: true},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := pd.ParseDSN(tc.in)
			if tc.fail {
				if err == nil {
					t.Fatalf("ParseDSN(%q) was accepted", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.in, err)
			}
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

// TestDSNRenderingIsRedactedAndNotARoundTrip states the property deliberately:
// a rendering that round-tripped would be a rendering that carried the
// credential.
func TestDSNRenderingIsRedactedAndNotARoundTrip(t *testing.T) {
	cfg, err := pd.ParseDSN("pulsar://broker:6650/acme/ns?token=secret-token&admin=http://broker:8080")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	rendered := cfg.DSN()
	if strings.Contains(rendered, "secret-token") {
		t.Fatalf("the rendered DSN carries the token: %q", rendered)
	}
	if !strings.Contains(rendered, dsn.Redacted) {
		t.Fatalf("the rendered DSN does not say a credential was redacted: %q", rendered)
	}
	back, err := pd.ParseDSN(rendered)
	if err != nil {
		t.Fatalf("the redacted DSN does not parse, which makes it useless in a log: %v", err)
	}
	if back.Token.Reveal() == cfg.Token.Reveal() {
		t.Fatal("the redacted DSN round-tripped the credential")
	}
}

// TestRegisterTakesBothSchemes is why there are two: `pulsar+ssl://` is what
// Pulsar's own tooling and service URLs use.
func TestRegisterTakesBothSchemes(t *testing.T) {
	r := dsn.NewRegistry()
	if err := pd.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, scheme := range []string{pd.Scheme, pd.SchemeTLS} {
		if _, ok := r.Lookup(scheme); !ok {
			t.Errorf("the registry has no %q", scheme)
		}
	}
}
