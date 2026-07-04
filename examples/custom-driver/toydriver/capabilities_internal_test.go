package toydriver

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestEveryFeatureIsConsidered holds the capability table to the specification's
// inventory.
//
// It is the one test in this package that is NOT in the external test package,
// because the question it asks is about the table's construction rather than
// about the driver's behaviour. What it buys: a feature added after
// this driver was written makes this test fail, instead of silently becoming
// an unconsidered claim of "no". CapabilityBuilder.Missing is not an error on
// its own — a table that says nothing about a feature says "unsupported",
// which is the safe answer — so the assertion has to be somebody's, and a
// driver author's own test is the right somebody.
func TestEveryFeatureIsConsidered(t *testing.T) {
	missing := newCapabilityBuilder(Config{}).Missing()
	if len(missing) == 0 {
		return
	}
	for _, f := range missing {
		t.Errorf("the capability table never mentions %v: add it to buildCapabilities, as a claim or as Unsupported", f)
	}
}

// TestCapabilityTableIsHonest is the builder's own validation, which refuses a
// conditional support with no condition and an emulation with no note.
func TestCapabilityTableIsHonest(t *testing.T) {
	caps, err := buildCapabilities(Config{})
	if err != nil {
		t.Fatalf("buildCapabilities: %v", err)
	}
	if !caps.Has(mq.FeatureAtLeastOnce) {
		t.Error("the table does not advertise at-least-once delivery, which this broker does provide")
	}
	if caps.Has(mq.FeatureDurableStorage) {
		t.Error("the table advertises durable storage, which an in-memory broker does not have")
	}
	if lim := caps.Limit(mq.LimitMaxMessageBytes); !lim.Known || lim.Value != DefaultMaxMessageBytes {
		t.Errorf("the advertised maximum message size is %v, want %d", lim, DefaultMaxMessageBytes)
	}
}
