package nsq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows: an unadvertised feature renders as unsupported, which
// is the safe answer and is indistinguishable from one the driver forgot.
//
// The construction itself is the assertion — capabilities() refuses a table
// that does not mention every feature — and the rest holds the rule that a
// Conditional or an emulated claim states its condition.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	caps := newRig(t).client.Capabilities()
	for f := range mq.FeatureCount {
		s := caps.Support(mq.Feature(f))
		if !s.Level.Valid() {
			t.Fatalf("feature %v has level %v", mq.Feature(f), s.Level)
		}
		switch s.Level {
		case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note; the specification requires the condition or the emulation to be stated",
					mq.Feature(f), s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
	}
}

// TestNoOrderingIsClaimedAtAll is the specification's "ordering never claimed", read
// literally.
//
// nsqd makes no promise about the order in which a channel's messages are
// delivered — not for one producer, one consumer and no failures: an in-memory
// queue and a disk-backed queue are drained independently and a requeue puts a
// message back wherever there is room. A Conditional claim with a note would be
// the wrong answer, because there is no condition under which it holds.
func TestNoOrderingIsClaimedAtAll(t *testing.T) {
	caps := newRig(t).client.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureOrderedByKey, mq.FeatureOrderedByPartition, mq.FeatureOrderedGlobally,
	} {
		if s := caps.Support(f); s.Level != mq.Unsupported {
			t.Errorf("%v is claimed at %v; the specification says ordering is never claimed for this broker", f, s.Level)
		}
	}
}

// TestTheHeaderClaimFollowsTheConfiguredMetadataMode is the reason this driver
// is in the plan, and the one place its capability table is not constant across
// configurations.
//
// NSQ carries a body and nothing else, so with no envelope configured all
// three header features are unsupported. With one configured they are
// EmulatedDurable: the header-less emulation, switched on deliberately, and
// the headers really do travel. Advertising them as unsupported while the
// envelope carried them would be the mirror image of the silent emulation the specification
// forbids — a caller planning against the table would be told a message cannot
// carry a binary header when on this client it can.
func TestTheHeaderClaimFollowsTheConfiguredMetadataMode(t *testing.T) {
	headers := []mq.Feature{mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders}
	t.Run("with no envelope", func(t *testing.T) {
		caps := newRig(t).client.Capabilities()
		for _, f := range headers {
			if caps.Has(f) {
				t.Errorf("%v is claimed with no envelope configured; NSQ messages have no headers at all", f)
			}
		}
	})
	for name, mode := range map[string]mq.MetadataMode{
		"adaptive": mq.MetadataAdaptive,
		"portable": mq.MetadataPortableEnvelope,
	} {
		t.Run("with the "+name+" envelope", func(t *testing.T) {
			caps := newRig(t, func(c *nq.Config) { c.Metadata = mode }).client.Capabilities()
			for _, f := range headers {
				s := caps.Support(f)
				if s.Level != mq.EmulatedDurable {
					t.Errorf("%v is %v, want EmulatedDurable", f, s.Level)
				}
				if s.Note == "" {
					t.Errorf("%v is emulated with no note; the specification requires the emulation to be stated", f)
				}
				// the specification's own rule: an emulated claim does not satisfy a
				// requirement for a native one.
				if err := caps.Require(mq.RequireAtLeast(f, mq.Native)); err == nil {
					t.Errorf("%v at %v satisfied a requirement for a NATIVE header", f, s.Level)
				}
			}
		})
	}
	// The lenient mode drops metadata rather than carrying it, so it claims
	// nothing: a table that said otherwise would advertise headers a
	// publication discards.
	t.Run("with the lenient mode", func(t *testing.T) {
		caps := newRig(t, func(c *nq.Config) { c.Metadata = mq.MetadataLenientDrop }).client.Capabilities()
		for _, f := range headers {
			if caps.Has(f) {
				t.Errorf("%v is claimed in a mode that DROPS it", f)
			}
		}
	})
}

// TestNoDeadLetterRouteIsClaimed is.
//
// FeatureDeadLetter means the DRIVER knows where a dead letter goes and can get
// it there in one native settlement, which nsqd has no verb for. The specification's "ED"
// for this broker is the CORE's publish-then-acknowledge emulation, and the
// core needs no capability from the driver to perform it.
func TestNoDeadLetterRouteIsClaimed(t *testing.T) {
	if newRig(t).client.Capabilities().Has(mq.FeatureDeadLetter) {
		t.Error("FeatureDeadLetter is claimed; nsqd has no dead-letter verb, and the runner's " +
			"publish-then-acknowledge emulation is the core's rather than this driver's")
	}
}

// TestTheAdvertisedLimitsAreTheOnesThisDriverEnforces is the rule that an
// advertised limit is a claim.
//
// Every one of these mirrors an nsqd flag rather than a value nsqd reports —
// nsqd publishes none of its limits over either protocol — so what makes them
// true is that the driver enforces exactly them, before any I/O
// The unknowns are honestly unknown: the specification forbids reading
// an unadvertised limit as zero.
func TestTheAdvertisedLimitsAreTheOnesThisDriverEnforces(t *testing.T) {
	h := newRig(t, func(c *nq.Config) {
		c.MaxMessageBytes = 512 << 10
		c.MaxBodyBytes = 4 << 20
		c.MaxRequeueDelay = 30 * time.Minute
		c.MaxAckDeadline = 10 * time.Minute
	})
	caps := h.client.Capabilities()
	for name, tc := range map[string]struct {
		key  mq.LimitKey
		want int64
	}{
		"the message size": {mq.LimitMaxMessageBytes, 512 << 10},
		"the batch size":   {mq.LimitMaxBatchBytes, 4 << 20},
		"the delay":        {mq.LimitMaxDelay, int64(30 * time.Minute)},
		"the lease":        {mq.LimitMaxLease, int64(10 * time.Minute)},
	} {
		lim := caps.Limit(tc.key)
		if !lim.Known {
			t.Errorf("%s is not advertised; the conformance kit fails a container-backed driver that "+
				"advertises no limit at all", name)
			continue
		}
		if lim.Value != tc.want {
			t.Errorf("%s is advertised as %d, want the configured %d", name, lim.Value, tc.want)
		}
	}
	for name, key := range map[string]mq.LimitKey{
		"a batch message count": mq.LimitMaxBatchMessages,
		"a header count":        mq.LimitMaxHeaderCount,
		"a header size":         mq.LimitMaxHeaderBytes,
	} {
		if lim := caps.Limit(key); lim.Known {
			t.Errorf("%s is advertised as %d; nsqd bounds no such thing and the specification forbids guessing",
				name, lim.Value)
		}
	}
}

// TestInfoNamesTheClientLibrary is every field is either true or empty,
// and an empty string is "the driver did not say" rather than a claim.
func TestInfoNamesTheClientLibrary(t *testing.T) {
	info := newRig(t).client.Info()
	if info.Name != "nsq" {
		t.Errorf("the driver name is %q", info.Name)
	}
	if info.ClientName != "github.com/nsqio/go-nsq" || info.ClientVersion == "" {
		t.Errorf("the client library is %q %q", info.ClientName, info.ClientVersion)
	}
	if info.BrokerVersion != "" {
		t.Errorf("the broker version is %q; nsqd reports it on an HTTP endpoint this driver does not "+
			"call at open, so the specification makes the honest answer the empty string", info.BrokerVersion)
	}
}
