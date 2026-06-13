package pulsar_test

import (
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestConfigValidate holds every refusal the specification asks for before any I/O.
func TestConfigValidate(t *testing.T) {
	ok := pd.Config{ServiceURL: "pulsar://broker:6650"}
	for name, tc := range map[string]struct {
		mutate func(*pd.Config)
		code   mq.ErrorCode
	}{
		"the default endpoint":       {mutate: func(c *pd.Config) { c.ServiceURL = "" }},
		"a TLS endpoint":             {mutate: func(c *pd.Config) { c.ServiceURL = "pulsar+ssl://broker:6651" }},
		"an http endpoint":           {mutate: func(c *pd.Config) { c.ServiceURL = "http://broker:8080" }, code: mq.CodeInvalidConfig},
		"an endpoint with no host":   {mutate: func(c *pd.Config) { c.ServiceURL = "pulsar://" }, code: mq.CodeInvalidConfig},
		"a bad admin endpoint":       {mutate: func(c *pd.Config) { c.AdminURL = "broker:8080" }, code: mq.CodeInvalidConfig},
		"an admin endpoint":          {mutate: func(c *pd.Config) { c.AdminURL = "http://broker:8080" }},
		"insecure without TLS":       {mutate: func(c *pd.Config) { c.InsecureSkipVerify = true }, code: mq.CodeInvalidConfig},
		"two authentication kinds":   {mutate: func(c *pd.Config) { c.Token, c.AuthName = "t", "tls" }, code: mq.CodeInvalidConfig},
		"a bad domain":               {mutate: func(c *pd.Config) { c.Domain = "ephemeral" }, code: mq.CodeInvalidConfig},
		"a bad tenant":               {mutate: func(c *pd.Config) { c.Tenant = "a/b" }, code: mq.CodeInvalidArgument},
		"a bad subscription type":    {mutate: func(c *pd.Config) { c.SubscriptionType = 9 }, code: mq.CodeInvalidConfig},
		"a bad compression":          {mutate: func(c *pd.Config) { c.Compression = 9 }, code: mq.CodeInvalidConfig},
		"a bad metadata mode":        {mutate: func(c *pd.Config) { c.Metadata = 9 }, code: mq.CodeInvalidConfig},
		"a negative timeout":         {mutate: func(c *pd.Config) { c.OperationTimeout = -1 }, code: mq.CodeInvalidConfig},
		"a negative bound":           {mutate: func(c *pd.Config) { c.ReceiverQueueSize = -1 }, code: mq.CodeInvalidConfig},
		"an unbounded client buffer": {mutate: func(c *pd.Config) { c.MemoryLimitBytes = -1 }, code: mq.CodeInvalidConfig},
		"out-of-order without Key_Shared": {
			mutate: func(c *pd.Config) { c.AllowOutOfOrderDelivery = true }, code: mq.CodeInvalidConfig,
		},
		"out-of-order with Key_Shared": {
			mutate: func(c *pd.Config) {
				c.AllowOutOfOrderDelivery = true
				c.SubscriptionType = pd.SubscriptionKeyShared
			},
		},
		"a lease guard longer than the lease": {
			mutate: func(c *pd.Config) { c.AckTimeout, c.LeaseGuard = time.Second, 2*time.Second },
			code:   mq.CodeInvalidConfig,
		},
		"a lease both ways": {
			mutate: func(c *pd.Config) { c.DisableAckTimeout, c.AckTimeout = true, time.Second },
			code:   mq.CodeInvalidConfig,
		},
		"attempts with no destination": {
			mutate: func(c *pd.Config) { c.MaxAttempts = 3 }, code: mq.CodeInvalidConfig,
		},
		"attempts with a destination": {
			mutate: func(c *pd.Config) { c.MaxAttempts, c.DeadLetterTopic = 3, "orders-dlq" },
		},
		"a retry topic without the retry topic enabled": {
			mutate: func(c *pd.Config) { c.RetryTopic = "orders-retry" }, code: mq.CodeInvalidConfig,
		},
		// The guard: the flag needs an administrative endpoint,
		// because the only way to learn whether the broker honours an
		// acknowledgement set is to ask it.
		"batch-index acknowledgement without an admin endpoint": {
			mutate: func(c *pd.Config) { c.EnableBatchIndexAck = true }, code: mq.CodeInvalidConfig,
		},
		"batch-index acknowledgement with one": {
			mutate: func(c *pd.Config) {
				c.EnableBatchIndexAck, c.AdminURL = true, "http://broker:8080"
			},
		},
		"a dead-letter topic that is not a legal name": {
			mutate: func(c *pd.Config) { c.DeadLetterTopic = "a/b" }, code: mq.CodeInvalidArgument,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ok
			tc.mutate(&cfg)
			err := cfg.Validate()
			switch {
			case tc.code == mq.CodeUnknown && err != nil:
				t.Fatalf("Validate refused a legal configuration: %v", err)
			case tc.code == mq.CodeUnknown:
				return
			case err == nil:
				t.Fatalf("Validate accepted %s", name)
			case mq.CodeOf(err) != tc.code:
				t.Fatalf("Validate reported %v (code %v), want %v", err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

// TestConfigDefaultsGrantALease is Pulsar's own behaviour is
// that an unsettled delivery is given back on a disconnection and at no other
// time, and the rest of this API models a lease that comes back.
func TestConfigDefaultsGrantALease(t *testing.T) {
	b, _ := newRig(t, func(c *pd.Config) { c.AckTimeout, c.LeaseGuard = 0, 0 })
	if !b.Capabilities().Has(mq.FeatureExtendLease) {
		t.Fatal("a default configuration advertises no lease extension, and the default AckTimeout is " +
			"a minute")
	}
	off, _ := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = 0, 0
		c.DisableAckTimeout = true
	})
	if off.Capabilities().Has(mq.FeatureExtendLease) {
		t.Fatal("DisableAckTimeout removes the emulated lease and the capability table still claims it")
	}
	src := sourceOf(t, off, "noleases", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, off, "noleases", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})
	d := fetchOne(t, src)
	if !d.Info.LeaseExpires.IsZero() {
		t.Fatalf("a delivery from a client with no lease reports LeaseExpires %v; the specification makes the zero "+
			"time \"the broker grants no lease\"", d.Info.LeaseExpires)
	}
}

// TestSecretsNeverRendered is in the one place the conformance kit cannot
// reach: every rendering the driver offers of its own configuration.
//
// It is the check had to add in its own module because the kit's
// `secrets/redaction` scenario could pass a driver that rendered its whole
// configuration into every error.
func TestSecretsNeverRendered(t *testing.T) {
	const token = "eyJ-not-a-real-token-but-it-looks-like-one"
	const params = "privateKey:file:///not/a/real/key"
	cfg := pd.Config{
		ServiceURL: "pulsar://broker:6650", AdminURL: "http://broker:8080",
		Token: dsn.Secret(token),
	}
	auth := pd.Config{ServiceURL: "pulsar://broker:6650", AuthName: "tls", AuthParams: dsn.Secret(params)}
	rendered := []string{
		cfg.String(), cfg.DSN(), cfg.LogValue().String(),
		auth.String(), auth.DSN(), auth.LogValue().String(),
	}
	for i, s := range rendered {
		for _, secret := range []string{token, params} {
			if strings.Contains(s, secret) {
				t.Fatalf("rendering %d carries a credential: %q", i, s)
			}
		}
		if !strings.Contains(s, "broker:6650") {
			t.Fatalf("rendering %d does not name the endpoint at all, so it is not a useful rendering: %q", i, s)
		}
	}
	if got := cfg.Secrets(); len(got) != 1 || got[0] != token {
		t.Fatalf("Config.Secrets reported %d credential(s), want the one token", len(got))
	}
}
