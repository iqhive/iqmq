package activemq_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
)

// TestCapabilityTableIsComplete is the specification's own requirement: every feature of
// the inventory has an answer.
//
// `driverkit.CapabilityBuilder` refuses an incomplete table, so this could
// only fail as a build error -- which is why the test asks WHICH row is
// missing rather than whether the build succeeded.
func TestCapabilityTableIsComplete(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		cfg := amq.Config{Broker: broker, Addrs: []string{"localhost:5672"}, InsecurePlainSASL: true}
		missing, err := amq.MissingFeaturesForTest(cfg)
		if err != nil {
			t.Fatalf("%v: building the table: %v", broker, err)
		}
		if len(missing) > 0 {
			t.Errorf("%v leaves %d features unanswered: %v", broker, len(missing), missing)
		}
	}
}

// TestCapabilityNotesAreStated is the evidence rule as a check: a
// conditional support states its condition and an emulation says what the
// emulation is, or the conformance kit's `capability/<F>/claim` scenario fails
// against a real broker.
func TestCapabilityNotesAreStated(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		for _, scheduler := range []bool{false, true} {
			cfg := amq.Config{
				Broker: broker, Addrs: []string{"localhost:5672"},
				InsecurePlainSASL: true, Scheduler: scheduler,
			}
			caps, err := amq.CapabilitiesForTest(cfg, productFor(broker), "2.44.0")
			if err != nil {
				t.Fatalf("%v scheduler=%v: %v", broker, scheduler, err)
			}
			for i := range mq.FeatureCount {
				f := mq.Feature(i)
				s := caps.Support(f)
				switch s.Level {
				case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
					if strings.TrimSpace(s.Note) == "" {
						t.Errorf("%v scheduler=%v: %v is %v with no note; the specification requires the condition or the "+
							"emulation to be stated and the conformance kit's claim scenario checks it",
							broker, scheduler, f, s.Level)
					}
				case mq.Native, mq.Unsupported:
					// A bare native support needs no note, and an
					// unsupported one MAY carry the measurement that
					// established the absence -- which this driver's table
					// leans on heavily -- but the specification does not require one.
				}
			}
		}
	}
}

// TestClaimsNarrowTheProtocolAndNeverWiden holds this driver to the seam's own
// rule: the protocol's table is the ceiling and the driver's answer may be
// lower but never higher.
//
// A table that claimed more than the protocol supports would be a promise the
// wire cannot keep, and it is the easy mistake -- a driver author who knows
// the broker has a feature claims it without checking that the protocol can
// reach it. `FeatureServerFilter` is the case here: Artemis and Classic both
// have JMS selectors, and go-amqp exposes no way to set a link source filter,
// so the AMQP arm cannot have them whatever the broker can do.
func TestClaimsNarrowTheProtocolAndNeverWiden(t *testing.T) {
	t.Parallel()
	p, err := protocol.New(protocol.AMQP10)
	if err != nil {
		t.Fatalf("protocol: %v", err)
	}
	for _, broker := range activemq.Brokers() {
		cfg := amq.Config{
			Broker: broker, Addrs: []string{"localhost:5672"},
			InsecurePlainSASL: true, Scheduler: true,
		}
		caps, err := amq.CapabilitiesForTest(cfg, productFor(broker), "2.44.0")
		if err != nil {
			t.Fatalf("%v: %v", broker, err)
		}
		for i := range mq.FeatureCount {
			f := mq.Feature(i)
			got, want := caps.Support(f), p.Support(broker, f)
			if got.Level.Rank() <= want.Level.Rank() {
				continue
			}
			// The one way a driver may exceed the protocol is by EMULATING
			// the feature, which is what the two emulated levels are
			// for: the protocol does not have it and the driver provides
			// it anyway, with the emulation stated. Anything else is a
			// promise the wire cannot keep.
			switch got.Level {
			case mq.EmulatedVolatile, mq.EmulatedDurable:
				if strings.TrimSpace(got.Note) == "" {
					t.Errorf("%v: the driver emulates %v and says nothing about the emulation; the specification requires it",
						broker, f)
				}
			default:
				t.Errorf("%v: the driver claims %v at %v and the protocol supports it at %v; a driver narrows a row "+
					"for what its own code does not implement, and the only way it may exceed the protocol is by "+
					"emulating the feature and saying so",
					broker, f, got.Level, want.Level)
			}
		}
	}
}

// TestTheTwoProductsDifferInTheClaimsThatMatter is the increment's own output
// as a check.
//
// Four broker-by-protocol combinations exist because the claims differ, and a
// table that had converged on the union of them would be wrong about both
// products. These are the rows that differ between the two products over
// AMQP 1.0, and each of them was measured.
func TestTheTwoProductsDifferInTheClaimsThatMatter(t *testing.T) {
	t.Parallel()
	artemis := capsFor(t, activemq.Artemis, true)
	classic := capsFor(t, activemq.Classic, true)

	for _, tc := range []struct {
		feature mq.Feature
		artemis mq.Level
		classic mq.Level
		why     string
	}{
		{
			feature: mq.FeatureDeduplication,
			artemis: mq.Native, classic: mq.Unsupported,
			why: "Artemis discards a second message carrying an `_AMQ_DUPL_ID` it has seen; Classic has no " +
				"duplicate detection at all",
		},
		{
			feature: mq.FeatureAdmin,
			artemis: mq.Conditional, classic: mq.Unsupported,
			why: "Artemis has a management address reachable over AMQP 1.0; Classic's management is JMX and its " +
				"web console, and neither is",
		},
	} {
		if got := artemis.Support(tc.feature).Level; got != tc.artemis {
			t.Errorf("Artemis claims %v at %v, want %v (%s)", tc.feature, got, tc.artemis, tc.why)
		}
		if got := classic.Support(tc.feature).Level; got != tc.classic {
			t.Errorf("Classic claims %v at %v, want %v (%s)", tc.feature, got, tc.classic, tc.why)
		}
	}

	// And the row a DEPLOYMENT can take away, which is the one a caller is
	// most likely to be surprised by: Classic's scheduler is off unless the
	// broker is configured with schedulerSupport="true", and measured, with
	// it off the broker delivers a scheduled message at once and passes the
	// property through to the consumer.
	off := capsFor(t, activemq.Classic, false)
	if got := off.Support(mq.FeatureScheduledPublish).Level; got != mq.Unsupported {
		t.Errorf("Classic with no scheduler claims scheduled publication at %v; measured, the broker then delivers "+
			"the message immediately with AMQ_SCHEDULED_DELAY passed through to the consumer, so the claim would be "+
			"a promise the broker discards", got)
	}
	if got := capsFor(t, activemq.Classic, true).Support(mq.FeatureScheduledPublish).Level; got == mq.Unsupported {
		t.Error("Classic WITH the scheduler claims no scheduled publication, so Config.Scheduler does nothing and " +
			"the withdrawal above cannot be told from a driver that never supported it")
	}
	if got := capsFor(t, activemq.Artemis, false).Support(mq.FeatureScheduledPublish).Level; got == mq.Unsupported {
		t.Error("Artemis with Config.Scheduler unset claims no scheduled publication; Artemis's scheduler is " +
			"always available and the field is Classic's")
	}
}

// TestFeaturesTheProtocolCannotHaveAreWithdrawn is the other direction of the
// same honesty: a feature the PROTOCOL cannot reach is refused whatever the
// broker can do, and each of these was checked against the library.
func TestFeaturesTheProtocolCannotHaveAreWithdrawn(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		caps := capsFor(t, broker, true)
		for _, tc := range []struct {
			feature mq.Feature
			why     string
		}{
			{mq.FeatureDelayedRedelivery, "measured: a `modified` disposition carrying x-opt-delivery-delay is redelivered immediately with delivery-count 1, so the annotation is honoured on a publication and ignored on a disposition"},
			{mq.FeatureServerFilter, "a JMS selector is an AMQP 1.0 link source filter and go-amqp exposes no way to set one, whatever the broker supports"},
			{mq.FeatureWildcardSubscribe, "a topic maps to its own destination, so a pattern over topic names would have to span destinations and a receiving link attaches to one"},
			{mq.FeatureExtendLease, "AMQP 1.0 grants no lease: an unsettled delivery is the receiver's for as long as the link lives"},
			{mq.FeatureExactlyOnceDelivery, "AMQP 1.0's `received` state resumes a partial transfer and does not de-duplicate a redelivery"},
			{mq.FeatureIdempotentPublish, "neither product has a producer epoch or a sequence number, so a retried publication is a second message; what Artemis has is caller-supplied de-duplication, which is a different feature and needs the caller to name a key"},
			{mq.FeatureReplay, "a receiving link attaches to a destination and reads what is there rather than a history"},
			{mq.FeatureTransactions, "go-amqp implements no transactional resource"},
		} {
			if got := caps.Support(tc.feature).Level; got != mq.Unsupported {
				t.Errorf("%v claims %v at %v: %s", broker, tc.feature, got, tc.why)
			}
		}
	}
}

// TestTheDriverAdvertisesALimitItReadsFromSomewhere is `limits/advertised`'s
// requirement, brought forward to a test that needs no container.
//
// The conformance kit FAILS a driver that advertises none of the seven
// limits against container evidence, and the reason is worth restating: every
// scenario that skips on an unknown limit is blind to the lookup having
// failed, so a driver with no limits at all is a driver whose limit handling
// nothing checks.
func TestTheDriverAdvertisesALimitItReadsFromSomewhere(t *testing.T) {
	t.Parallel()
	caps := capsFor(t, activemq.Artemis, true)
	known := 0
	for k := range mq.LimitKeyCount {
		if caps.Limit(mq.LimitKey(k)).Known {
			known++
		}
	}
	if known == 0 {
		t.Fatalf("the driver advertises none of the %d limits; the conformance kit fails that against container "+
			"evidence, because every scenario that skips on an unknown limit is then blind to the lookup having "+
			"failed", mq.LimitKeyCount)
	}
	// And the one it does advertise is a real ceiling rather than a zero:
	// the rule is that an unknown limit is never an overloaded zero, and
	// a limit of zero advertised as known would refuse every header.
	if lim := caps.Limit(mq.LimitMaxHeaderBytes); lim.Known && lim.Value <= 0 {
		t.Errorf("MaxHeaderBytes is advertised as %d, which refuses every header", lim.Value)
	}
}

// capsFor is the table one configuration publishes.
func capsFor(t *testing.T, broker activemq.Broker, scheduler bool) mq.Capabilities {
	t.Helper()
	cfg := amq.Config{
		Broker: broker, Addrs: []string{"localhost:5672"},
		InsecurePlainSASL: true, Scheduler: scheduler,
	}
	caps, err := amq.CapabilitiesForTest(cfg, productFor(broker), "2.44.0")
	if err != nil {
		t.Fatalf("capabilities for %v: %v", broker, err)
	}
	return caps
}

// productFor is what the broker calls itself, which for Classic is nothing at
// all: measured, its AMQP 1.0 open performative carries no product.
func productFor(b activemq.Broker) string {
	if b == activemq.Classic {
		return ""
	}
	return activemq.ProductArtemis
}

// TestCoreCeilingIsRespected is the seam's own guard, exercised from the
// outside: the core refuses a profile that claims more than it has code for,
// and this is what proves the refusal is reachable rather than theoretical.
func TestCoreCeilingIsRespected(t *testing.T) {
	t.Parallel()
	// FeatureExtendLease is the row the core's own documentation flags for
	// this driver by name: Artemis really does extend a message's lease and
	// this driver has no Extend to offer, so a profile that claimed it would
	// publish a capability the core answers ErrUnsupported to.
	if got := core.CoreCeiling(mq.FeatureExtendLease); got != mq.Unsupported {
		t.Fatalf("the core's ceiling for ExtendLease is %v; this test is about the row the core refuses", got)
	}
	if got := capsFor(t, activemq.Artemis, true).Support(mq.FeatureExtendLease).Level; got != mq.Unsupported {
		t.Errorf("the driver claims ExtendLease at %v", got)
	}
}
