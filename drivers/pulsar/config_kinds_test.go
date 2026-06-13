package pulsar_test

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
)

// The typed enumerations and the configuration refusals.
//
// Each enumeration is a value a caller sets and the driver hands to the
// client library, and each carries three methods nothing exercised: the
// name, the validity, and the library's own constant. The last one is the
// one that matters -- a wrong mapping sends every message with the wrong
// compression, or every key to the wrong partition, and nothing fails.

// TestEverySubscriptionKindMapsToItsOwnPulsarType.
//
// They are four different delivery contracts -- Shared competes, Key_Shared
// competes within a key, Exclusive is one consumer, Failover is one active
// and the rest waiting -- so a mapping that collapsed two of them would
// change what the driver guarantees without changing what it claims.
func TestEverySubscriptionKindMapsToItsOwnPulsarType(t *testing.T) {
	t.Parallel()
	seen := map[pulsar.SubscriptionType]pd.SubscriptionKind{}
	for _, k := range []pd.SubscriptionKind{
		pd.SubscriptionShared, pd.SubscriptionKeyShared,
		pd.SubscriptionExclusive, pd.SubscriptionFailover,
	} {
		if !k.Valid() {
			t.Errorf("%v reports itself invalid", k)
		}
		if s := k.String(); s == "" || strings.Contains(s, "(?)") {
			t.Errorf("%d has no name: %q", k, s)
		}
		native := k.Native()
		if other, dup := seen[native]; dup {
			t.Errorf("%v and %v both map to %v; they are different delivery contracts", k, other, native)
		}
		seen[native] = k
	}
}

// TestEveryCompressionKindMapsToItsOwnCodec.
func TestEveryCompressionKindMapsToItsOwnCodec(t *testing.T) {
	t.Parallel()
	seen := map[pulsar.CompressionType]pd.CompressionKind{}
	for _, k := range []pd.CompressionKind{
		pd.CompressionNone, pd.CompressionLZ4, pd.CompressionZLib,
		pd.CompressionZSTD, pd.CompressionSnappy,
	} {
		if !k.Valid() {
			t.Errorf("%v reports itself invalid", k)
		}
		if s := k.String(); s == "" || strings.Contains(s, "(?)") {
			t.Errorf("%d has no name: %q", k, s)
		}
		native := k.Native()
		if other, dup := seen[native]; dup {
			t.Errorf("%v and %v both map to %v; a collapsed codec sends every message compressed the "+
				"wrong way and nothing fails", k, other, native)
		}
		seen[native] = k
	}
}

// TestAValueThatIsNotOneOfTheKindsNamesItselfRatherThanPanicking.
//
// The name is read by a log line and by a failure message, so a value from a
// stale build or a bad cast has to render as something a reader can act on.
func TestAValueThatIsNotOneOfTheKindsNamesItselfRatherThanPanicking(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		got   string
		valid bool
	}{
		{"SubscriptionKind", pd.SubscriptionKind(99).String(), pd.SubscriptionKind(99).Valid()},
		{"CompressionKind", pd.CompressionKind(99).String(), pd.CompressionKind(99).Valid()},
		{"HashingKind", pd.HashingKind(99).String(), pd.HashingKind(99).Valid()},
	} {
		if !strings.Contains(tc.got, "(?)") {
			t.Errorf("%s(99) renders as %q; an out-of-range value has to say so", tc.name, tc.got)
		}
		if tc.valid {
			t.Errorf("%s(99) reports itself valid", tc.name)
		}
	}
}

// TestEveryConfigurationRefusalIsReached.
//
// Each is a field a deployment sets, and each refusal runs before anything is
// dialled -- which is what makes a misconfiguration an InvalidConfig naming
// the field rather than a timeout naming a host.
func TestEveryConfigurationRefusalIsReached(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		shape func(*pd.Config)
	}{
		{"a subscription type that is not one", func(c *pd.Config) { c.SubscriptionType = pd.SubscriptionKind(99) }},
		{"a compression that is not one", func(c *pd.Config) { c.Compression = pd.CompressionKind(99) }},
		{"a hashing scheme that is not one", func(c *pd.Config) { c.Hashing = pd.HashingKind(99) }},
		{"a metadata mode that is not one", func(c *pd.Config) { c.Metadata = mq.MetadataMode(99) }},
		{"a negative connection timeout", func(c *pd.Config) { c.ConnectionTimeout = -time.Second }},
		{"a negative acknowledgement grouping", func(c *pd.Config) { c.AckGroupingMaxTime = -time.Second }},
		{"a negative acknowledgement timeout", func(c *pd.Config) { c.AckTimeout = -time.Second }},
		{"a negative connection bound", func(c *pd.Config) { c.MaxConnectionsPerBroker = -1 }},
		{"a negative batching bound", func(c *pd.Config) { c.BatchingMaxMessages = -1 }},
		{"a negative partition count", func(c *pd.Config) { c.Partitions = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := pd.Config{ServiceURL: "pulsar://127.0.0.1:1"}
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
