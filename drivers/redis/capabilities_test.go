package redis_test

import (
	"context"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
)

// capsFor builds a backend's table for one server shape and configuration.
func capsFor(t testing.TB, opts fakestreams.Options, cfg rd.Config) mq.Capabilities {
	t.Helper()
	srv := fakestreams.New(opts)
	cfg.Clock = fakeClock{srv}
	b, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b.Capabilities()
}

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows.
//
// A feature nobody mentioned renders as unsupported, which is the safe answer
// — and is also indistinguishable from a feature the driver forgot. The
// builder's Missing() is what tells the two apart, and `capabilities` refuses
// to produce a table with a gap; this is the test that would fail if a
// feature were added to the inventory and not to this driver.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	caps := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true}, rd.Config{})
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		// Support returns the zero value for an unadvertised feature, so this
		// cannot distinguish "considered and absent" from "forgotten" — that
		// is Missing()'s job inside `capabilities`, which refuses to build.
		// What this asserts is that the table was built at all, which means
		// Missing() was empty.
		_ = caps.Support(f)
	}
	if !caps.Has(mq.FeatureAtLeastOnce) {
		t.Fatal("the table does not claim at-least-once delivery, which is what this driver is for")
	}
}

// TestEveryConditionalClaimStatesItsCondition is the rule, held over every
// configuration that changes the table.
//
// A conditional support with no note tells a program that something might
// work under conditions nobody wrote down, and an emulation with no note does
// not say what the emulation is. driverkit.CapabilityBuilder refuses both, so
// this test is a second line — but it is the line that catches a note that
// was made EMPTY by a configuration rather than one that was never written.
func TestEveryConditionalClaimStatesItsCondition(t *testing.T) {
	for _, kind := range streams.Kinds() {
		for _, partitions := range []int{1, 4} {
			caps := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true},
				rd.Config{Nack: kind, Partitions: partitions})
			for i := range mq.FeatureCount {
				f := mq.Feature(i)
				sup := caps.Support(f)
				switch sup.Level {
				case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
					if sup.Note == "" {
						t.Fatalf("nack=%v partitions=%d: %v is %v with no note ",
							kind, partitions, mq.Feature(i), sup.Level)
					}
				case mq.Native, mq.Unsupported:
				}
			}
		}
	}
}

// TestCapabilityNotesMatchTheStrategy holds the table to the strategy it
// describes.
//
// The table is built before a source exists, so it cannot ask a
// [streams.Strategy] what it does; it uses a pair of switches instead. This
// is what keeps those switches in step with the real strategies: a kind whose
// Delays or Reorders changed would otherwise leave the capability table
// saying the old thing, and a caller reading "DelayedRedelivery: emulated
// durable" would configure a delay the driver refuses.
func TestCapabilityNotesMatchTheStrategy(t *testing.T) {
	keys, err := streams.NewKeys("t", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	for _, kind := range streams.Kinds() {
		strategy, serr := streams.NewNack(kind, streams.NackConfig{
			Keys: keys, Group: "g", Consumer: "c", AckDeadline: testLease,
		})
		if serr != nil {
			t.Fatalf("NewNack(%v): %v", kind, serr)
		}
		caps := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true}, rd.Config{Nack: kind})
		delaySup := caps.Support(mq.FeatureDelayedRedelivery)
		if strategy.Delays() {
			if delaySup.Level != mq.EmulatedDurable {
				t.Fatalf("%v delays, and the table advertises DelayedRedelivery as %v, not an emulation",
					kind, delaySup.Level)
			}
		} else if delaySup.Level == mq.EmulatedDurable {
			t.Fatalf("%v cannot delay a redelivery and the table advertises DelayedRedelivery as an "+
				"emulation: a caller reading it would configure a delay this driver refuses", kind)
		}
		orderSup := caps.Support(mq.FeatureOrderedByPartition)
		if orderSup.Note == "" {
			t.Fatalf("%v: the ordering claim has no note", kind)
		}
		wantReorder := strategy.Reorders()
		gotReorder := containsStr(orderSup.Note, "which DOES re-add")
		if wantReorder != gotReorder {
			t.Fatalf("%v reorders=%v but the ordering note says reorders=%v; a note that describes "+
				"the wrong strategy is worse than no note", kind, wantReorder, gotReorder)
		}
	}
}

func containsStr(s, sub string) bool { return contains(s, sub) }

// TestDurableConfirmFlipsWithTheServer is the version matrix's claim, proved
// against the fake so that `make ci` covers it and the container matrix only
// has to confirm it.
//
// It is the clearest of the three flips: WAITAOF arrived in 7.0, so a 6.2
// server has no durable boundary to define at all, and a 7.x server with
// `appendonly no` has the command and nothing for it to wait on.
func TestDurableConfirmFlipsWithTheServer(t *testing.T) {
	cases := []struct {
		version string
		aof     bool
		want    bool
	}{
		{"6.2.14", true, false},
		{"6.2.14", false, false},
		{"7.0.15", true, true},
		{"7.4.2", true, true},
		{"7.4.2", false, false},
		{"8.2.9", true, true},
		// An unparseable version is not "at least" anything, so the durable
		// boundary is refused — the safe direction, and what the specification requires
		// of a fact the driver does not know.
		{"unstable", true, false},
	}
	for _, tc := range cases {
		name := tc.version
		if name == "" {
			name = "unknown"
		}
		if tc.aof {
			name += "-aof"
		} else {
			name += "-noaof"
		}
		t.Run(name, func(t *testing.T) {
			caps := capsFor(t, fakestreams.Options{Version: tc.version, AOF: tc.aof}, rd.Config{})
			if got := caps.Has(mq.FeatureDurableConfirm); got != tc.want {
				t.Fatalf("server %q aof=%v: DurableConfirm = %v, want %v",
					tc.version, tc.aof, got, tc.want)
			}
		})
	}
}

// TestOrderedGloballyFlipsWithThePartitionCount: a topic with one shard has a
// total order and a topic with four does not.
//
// Claiming otherwise is the mistake a partitioned driver makes most easily,
// and it is not a small one: a caller who required global ordering and got
// four shards would see messages out of order with nothing failing.
func TestOrderedGloballyFlipsWithThePartitionCount(t *testing.T) {
	one := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true}, rd.Config{Partitions: 1})
	if !one.Has(mq.FeatureOrderedGlobally) {
		t.Fatal("a one-shard topic has a total order and the table does not claim it")
	}
	four := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true}, rd.Config{Partitions: 4})
	if four.Has(mq.FeatureOrderedGlobally) {
		t.Fatal("a four-shard topic has no total order and the table claims one")
	}
	if !four.Has(mq.FeatureOrderedByKey) {
		t.Fatal("a four-shard topic orders by key and the table does not claim it")
	}
}

// TestLimitsAreOnlyWhatTheDriverKnows is the rule about limits: an
// unadvertised limit is never an overloaded zero.
//
// The two that are advertised are the entry layout's, which are the portable
// envelope's — a message over them would be accepted and then be undecodable,
// so the core has to be able to refuse it before any I/O. Everything else is
// left unknown rather than guessed: Redis's proto-max-bulk-len is a server
// setting this driver does not read, and advertising its default as a fact is
// exactly what the specification forbids.
func TestLimitsAreOnlyWhatTheDriverKnows(t *testing.T) {
	caps := capsFor(t, fakestreams.Options{Version: "7.4.2", AOF: true}, rd.Config{})
	size := caps.Limit(mq.LimitMaxMessageBytes)
	if !size.Known {
		t.Fatal("the message-size limit is unknown, so the core cannot refuse an oversized publication before any I/O")
	}
	if size.Value != int64(streams.DefaultLimits.MaxBody) {
		t.Fatalf("the advertised message size is %d, the entry layout's bound is %d: a caller "+
			"trusting the advertised figure could publish a message the driver refuses",
			size.Value, streams.DefaultLimits.MaxBody)
	}
	headers := caps.Limit(mq.LimitMaxHeaderCount)
	if !headers.Known || headers.Value != int64(streams.DefaultLimits.MaxHeaders) {
		t.Fatalf("the advertised header count is %v, the layout's bound is %d",
			headers, streams.DefaultLimits.MaxHeaders)
	}
	for _, key := range []mq.LimitKey{
		mq.LimitMaxBatchMessages,
		mq.LimitMaxBatchBytes,
		mq.LimitMaxHeaderBytes,
		mq.LimitMaxDelay,
	} {
		if lim := caps.Limit(key); lim.Known {
			t.Fatalf("%v is advertised as %d; this driver does not know it, and the specification forbids "+
				"advertising a default as a fact", key, lim.Value)
		}
	}
}
