package rabbitmq_test

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
)

// TestDriverNarrowsAndNeverWidensTheProtocol is the rule the capability table
// is built on.
//
// The table starts from `internal/rabbit/protocol`'s answer for AMQP 0-9-1 —
// the PROTOCOL's feature subset, which the specification measures and which
// the AMQP 1.0 profile shares — and narrows it for what this client's own
// configuration cannot do. A driver that WIDENED one would be claiming
// something no code implements, and the whole value is that a claim is
// actionable.
//
// The rule is NOT "the driver's level is never higher", and the case that
// showed why is worth stating. The protocol's row for FeatureDelayedRedelivery
// is Conditional with the note "neither is a protocol feature", and the
// driver's is EmulatedDurable: the driver really does build a delayed
// redelivery, out of a TTL queue and a dead-letter route. That is a refinement
// rather than a widening — the levels are not one ordered scale, because
// Conditional is about a CONDITION and an emulation is about WHO does the
// work.
//
// So the invariant is the one that actually matters to a caller: a driver may
// never claim the BROKER does something natively that its protocol does not
// have, and may never claim a feature its protocol does not have at all. It
// may claim to EMULATE one, because that is a claim about the driver's own
// code and the specification requires the emulation to be named.
func TestDriverNarrowsAndNeverWidensTheProtocol(t *testing.T) {
	proto, err := protocol.New(protocol.AMQP091)
	if err != nil {
		t.Fatal(err)
	}
	// The widest configuration this driver has: everything that can be
	// turned on is on, so nothing is narrowed for a reason of the
	// configuration's.
	caps, err := rq.CapabilitiesForFactsForTest("4.3.5", true, true, true, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	refined := 0
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		got, want := caps.Support(f), proto.Support(f)
		switch {
		case got.Level == mq.Native && want.Level != mq.Native:
			t.Errorf("%v: the driver claims the broker does this NATIVELY and the protocol offers %v; a driver may emulate a feature and may not invent one",
				f, want.Level)
		case want.Level == mq.Unsupported && got.Level != mq.Unsupported &&
			got.Level != mq.EmulatedVolatile && got.Level != mq.EmulatedDurable:
			// An EMULATION over an absent protocol feature is the one
			// widening that is legitimate, and it is the whole reason the specification
			// has emulated levels: FeatureBatchPublish is not an AMQP 0-9-1
			// frame and a batch still works, because the core loops and this
			// driver confirms each publication. What is forbidden is a
			// CONDITIONAL or a NATIVE claim over an absent feature, which
			// says the broker does something it cannot.
			t.Errorf("%v: the driver claims %v for a feature the protocol does not have at all, and %v is not an emulation: only an emulation is the driver's own to add",
				f, got.Level, got.Level)
		case got.Level != want.Level:
			// A refinement: the driver narrowed the claim, or turned the
			// protocol's "not a protocol feature" into a named emulation.
			// Both are legitimate and both have to carry a note.
			refined++
			if got.Level != mq.Unsupported && got.Level != mq.Native && got.Note == "" {
				t.Errorf("%v: the driver refined %v to %v and said nothing about it, which the message contract forbids",
					f, want.Level, got.Level)
			}
		}
	}
	if refined == 0 {
		// The widest configuration still refines several rows -- the
		// dead-letter route, the delayed redelivery, the scheduled
		// publication -- so a run that refined nothing means the narrowing
		// is not happening at all and every other assertion here is
		// vacuous.
		t.Error("the driver's table is the protocol's table unchanged: the narrowing is not being applied")
	}
}

// rank orders the support levels from the weakest claim to the strongest, so
// that "narrower" is a comparison rather than a case analysis.
//
// the specification's own ordering is Unsupported < EmulatedVolatile < Conditional <
// EmulatedDurable < Native for the purpose of "can this satisfy a
// requirement", and what matters here is only that a driver's answer is never
// a stronger claim than its protocol's.
func rank(l mq.Level) int {
	switch l {
	case mq.Unsupported:
		return 0
	case mq.EmulatedVolatile:
		return 1
	case mq.Conditional:
		return 2
	case mq.EmulatedDurable:
		return 3
	case mq.Native:
		return 4
	default:
		return 0
	}
}

// TestCapabilityClaimsFollowTheFacts holds every conditional claim to the fact
// it depends on, in BOTH directions.
//
// One direction alone proves nothing: a table that reported everything
// Unsupported would pass a test that only checked the absences, and one that
// reported everything Native would pass a test that only checked the
// presences. Each row below therefore names a fact, and asserts what the claim
// is when the fact holds and what it is when it does not.
func TestCapabilityClaimsFollowTheFacts(t *testing.T) {
	type facts struct {
		version                                         string
		quorum, declare, deadLetter, delayPlugin, keyRK bool
		priority                                        uint8
	}
	widest := facts{version: "4.3.5", quorum: true, declare: true, deadLetter: true, delayPlugin: true, keyRK: true}

	build := func(t *testing.T, f facts) mq.Capabilities {
		t.Helper()
		caps, err := rq.CapabilitiesForFactsForTest(f.version, f.quorum, f.declare, f.deadLetter, f.delayPlugin, f.priority, f.keyRK)
		if err != nil {
			t.Fatal(err)
		}
		return caps
	}

	for _, tc := range []struct {
		name    string
		feature mq.Feature
		with    facts
		without facts
		// present is the weakest level the claim may have when the fact
		// holds.
		present mq.Level
	}{
		{
			"a dead-letter route needs the declaration", mq.FeatureDeadLetter,
			widest,
			facts{version: "4.3.5", quorum: true, declare: true, keyRK: true},
			mq.Native,
		},
		{
			"a delayed redelivery needs topology", mq.FeatureDelayedRedelivery,
			widest,
			facts{version: "4.3.5", quorum: true},
			mq.EmulatedDurable,
		},
		{
			"a scheduled publication needs the plugin", mq.FeatureScheduledPublish,
			widest,
			facts{version: "4.3.5", quorum: true, declare: true, deadLetter: true},
			mq.Conditional,
		},
		{
			"a priority needs x-max-priority", mq.FeaturePriority,
			facts{version: "4.3.5", declare: true, priority: 9},
			widest,
			mq.Conditional,
		},
		{
			"ordering by key needs the key as the routing key", mq.FeatureOrderedByKey,
			widest,
			facts{version: "4.3.5", quorum: true, declare: true, deadLetter: true, delayPlugin: true},
			mq.Conditional,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			with := build(t, tc.with).Support(tc.feature)
			if rank(with.Level) < rank(tc.present) {
				t.Errorf("with the fact, %v is %v: want at least %v", tc.feature, with.Level, tc.present)
			}
			if with.Level == mq.Conditional && with.Note == "" {
				t.Errorf("%v is conditional with no condition stated, which the message contract forbids", tc.feature)
			}
			without := build(t, tc.without).Support(tc.feature)
			if without.Level != mq.Unsupported {
				t.Errorf("without the fact, %v is %v: want Unsupported, or the claim does not follow from the fact", tc.feature, without.Level)
			}
		})
	}
}

// TestClassicQueueLosesTheAttemptCount is the specification's own `(verify)`, held to both
// answers.
//
// `x-delivery-count` is a quorum-queue field. A classic queue does not carry
// it, so Attempt is always zero there and MaxAttempts cannot be
// enforced from the broker's own count. The level does not change — a classic
// queue still delivers at least once and still settles explicitly — so what
// has to change is the NOTE, and a table that said nothing would leave a
// caller planning on a count that is always one.
func TestClassicQueueLosesTheAttemptCount(t *testing.T) {
	quorum, err := rq.CapabilitiesForFactsForTest("4.3.5", true, true, true, false, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	classic, err := rq.CapabilitiesForFactsForTest("4.3.5", false, true, true, false, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []mq.Feature{mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack} {
		q, c := quorum.Support(f), classic.Support(f)
		if q.Level != c.Level {
			t.Errorf("%v is %v on a quorum queue and %v on a classic one: the LEVEL is the same and the note is what differs", f, q.Level, c.Level)
		}
		if c.Note == "" {
			t.Errorf("%v says nothing about the missing x-delivery-count on a classic queue", f)
		}
		if q.Note == c.Note {
			t.Errorf("%v carries the same note on both queue types, so the difference is not stated", f)
		}
	}
}

// TestUnsupportedClaimsAreHonest is the negative half of the table: the things
// this broker really does not have.
func TestUnsupportedClaimsAreHonest(t *testing.T) {
	caps, err := rq.CapabilitiesForFactsForTest("4.3.5", true, true, true, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []mq.Feature{
		// A queue has no history, so nothing can be replayed or sought.
		mq.FeatureReplay, mq.FeatureSeekByTime, mq.FeatureSeekBySequence,
		// RabbitMQ de-duplicates nothing and has no exactly-once delivery.
		mq.FeatureExactlyOnceDelivery, mq.FeatureIdempotentPublish, mq.FeatureDeduplication,
		// AMQP 0-9-1 has no batch frame, so an ATOMIC batch is unreachable;
		// the non-atomic one is an emulation and is in the list below.
		mq.FeatureAtomicBatchPublish,
		// `basic.ack{multiple}` exists and this driver does not use it.
		mq.FeatureBatchSettle,
		// A delivery's lease is the channel's lifetime: nothing to extend.
		mq.FeatureExtendLease,
		// Neither a duplicate header key nor a global order is expressible.
		mq.FeatureDuplicateHeaders, mq.FeatureOrderedGlobally,
		// `tx.select` exists in the protocol and this driver does not use it.
		mq.FeatureTransactions,
		// A topic exchange HAS wildcards and this driver's topology cannot
		// use them for what FeatureWildcardSubscribe names: a topic
		// maps to its own exchange, and a pattern over topic names would
		// have to bind one queue to every matching exchange, including
		// exchanges created later. See `narrow` for the alternative that was
		// rejected.
		mq.FeatureWildcardSubscribe,
	} {
		if s := caps.Support(f); s.Level != mq.Unsupported {
			t.Errorf("%v is claimed at %v: this broker does not have it", f, s.Level)
		}
	}
	// And the things it does have, so the test is not satisfied by a table
	// that claims nothing.
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage, mq.FeatureAtLeastOnce, mq.FeatureExplicitAck,
		mq.FeatureNack, mq.FeatureFanOut, mq.FeatureConsumerGroups,
		mq.FeatureDurableSubscriptions, mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders,
		mq.FeaturePublisherConfirm, mq.FeatureRequestReply,
		mq.FeatureMessageTTL, mq.FeatureBatchReceive, mq.FeatureAdmin,
		// Emulated rather than native, and a caller can still do it: the
		// core loops and this driver confirms each publication.
		mq.FeatureBatchPublish,
	} {
		if s := caps.Support(f); s.Level == mq.Unsupported {
			t.Errorf("%v is unsupported: this broker has it", f)
		}
	}
}

// TestEveryFeatureIsConsidered is the difference between a feature a driver
// decided it does not have and one nobody looked at.
func TestEveryFeatureIsConsidered(t *testing.T) {
	caps, err := rq.CapabilitiesForFactsForTest("4.3.5", true, true, true, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	// The builder refuses a table that mentions a feature twice and one
	// whose conditional rows carry no condition, so reaching this point
	// means the whole inventory was set; what is checked here is that
	// nothing is left at the zero value by omission, which the inventory's
	// own length is what proves.
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		if s.Level == mq.Native && s.Scope == mq.ScopeNone {
			t.Errorf("%v is native with no scope: a feature that holds over nothing is not a feature", f)
		}
		if s.Level != mq.Unsupported && s.Level != mq.Native && s.Note == "" {
			t.Errorf("%v is %v with no note", f, s.Level)
		}
	}
	if l := caps.Limits(); !l.MaxHeaderBytes.Known {
		t.Error("the header ceiling is not advertised, and it is the protocol's own: a field-table key is a shortstr")
	}
	if l := caps.Limits(); l.MaxMessageBytes.Known {
		t.Error("a message-size ceiling is advertised: RabbitMQ's max_message_size is a server configuration this driver cannot read, so the specification makes the honest answer an unadvertised limit")
	}
}
