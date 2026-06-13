package nsq_test

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
)

// The configuration refusals that involve more than one field.
//
// The single-field ones are held elsewhere. These are the ones where two
// settings are individually fine and wrong together -- which is the kind a
// deployment actually produces, because each was set by somebody reasonable
// at a different time.

// TestTwoSettingsThatAreFineAloneAndWrongTogetherAreRefused.
//
// Each of these is a real failure mode with a confusing symptom, which is
// why the refusal is worth having at configuration time:
//
//   - a heartbeat at or past the read deadline makes every idle connection
//     look dead, and reads as a broker that keeps dropping out;
//   - an acknowledgement deadline past the maximum is refused by nsqd in the
//     IDENTIFY handshake, so the alternative to refusing here is a
//     connection that will not open and an error about a parameter;
//   - a body bound smaller than the message bound is a pair that can never
//     both be satisfied.
func TestTwoSettingsThatAreFineAloneAndWrongTogetherAreRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		shape func(*nq.Config)
	}{
		{"a heartbeat at the read deadline", func(c *nq.Config) {
			c.HeartbeatInterval = 5 * time.Second
			c.ReadTimeout = 5 * time.Second
		}},
		{"a heartbeat past the read deadline", func(c *nq.Config) {
			c.HeartbeatInterval = 10 * time.Second
			c.ReadTimeout = 5 * time.Second
		}},
		{"an acknowledgement deadline past the maximum", func(c *nq.Config) {
			c.AckDeadline = 2 * time.Hour
			c.MaxAckDeadline = time.Minute
		}},
		{"a body bound smaller than the message bound", func(c *nq.Config) {
			c.MaxMessageBytes = 1 << 20
			c.MaxBodyBytes = 1 << 10
		}},
		{"an output buffer timeout below the floor", func(c *nq.Config) {
			c.OutputBufferTimeout = time.Nanosecond
		}},
		{"an acknowledgement deadline below the floor", func(c *nq.Config) {
			// The guard is lowered with it, or the LeaseGuard rule fires
			// first and this case would pass with the floor check deleted.
			c.AckDeadline = 500 * time.Millisecond
			c.LeaseGuard = time.Millisecond
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := nq.Config{Addrs: []string{"127.0.0.1:4150"}}
			tc.shape(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate accepted it")
			}
			if got := mq.CodeOf(err); got != mq.CodeInvalidConfig {
				t.Errorf("Validate answered %v (code %v), want CodeInvalidConfig", err, got)
			}
		})
	}
}

// TestAPrefixThatMakesEveryTopicIllegalIsRefused.
//
// The prefix is prepended to every topic, so one carrying a character nsqd
// will not take makes every publication fail -- at publish time, naming a
// topic the caller never wrote. Refusing at configuration time names the
// setting instead.
func TestAPrefixThatMakesEveryTopicIllegalIsRefused(t *testing.T) {
	t.Parallel()
	cfg := nq.Config{Addrs: []string{"127.0.0.1:4150"}, TopicPrefix: "bad prefix "}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate accepted a topic prefix carrying a space; every publication would fail at publish " +
			"time naming a topic the caller never wrote")
	}
}

// TestAConfigurationNamingNoBrokerAtAllIsRefused.
//
// This driver reaches nsqd directly or through nsqlookupd, and it needs one
// of the two. The DEFAULT address lives in `resolve`, which runs after this
// -- so an empty configuration is a refusal here rather than a silent
// localhost, and the error names both ways of fixing it.
func TestAConfigurationNamingNoBrokerAtAllIsRefused(t *testing.T) {
	t.Parallel()
	cfg := nq.Config{}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted a configuration naming neither an nsqd nor an nsqlookupd")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidConfig {
		t.Errorf("Validate answered %v (code %v), want CodeInvalidConfig", err, got)
	}

	// A lookupd address alone is enough: the two are alternatives, not a
	// pair, and a check that demanded both would refuse the deployment shape
	// nsqlookupd exists for.
	viaLookup := nq.Config{LookupdAddrs: []string{"127.0.0.1:4161"}}
	if err := viaLookup.Validate(); err != nil {
		t.Errorf("Validate with an nsqlookupd address and no nsqd is %v; the two are alternatives", err)
	}
}
