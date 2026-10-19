package amqp10_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
)

// update rewrites the golden file rather than comparing against it.
//
// A golden file nobody has watched change records whatever the code did on the
// day it was written, so the flag logs the path it wrote and the diff is what a
// reviewer reads.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// goldenConfig is the deployment the golden table is taken from: a durable
// quorum queue with a dead-letter route and a priority ceiling.
//
// It is deliberately the RICHEST deployment rather than the zero Config,
// because the golden file is what a reviewer reads a capability change out of
// and the zero Config withdraws four rows before the table is even built.
func goldenConfig() rmq10.Config {
	return rmq10.Config{
		Addrs:       []string{"amqp://127.0.0.1:5672"},
		Prefix:      "acme.",
		QueueType:   rmq10.QueueQuorum,
		DeadLetter:  true,
		MaxPriority: 9,
	}
}

// TestCapabilitiesGolden compares the rendered table against a committed file.
//
// A capability claim that cannot be read is a claim on trust. The generated
// The specification matrix is built from a conformance run, which needs a broker; this is
// the half that needs nothing, so a change to any claim or any note is a diff a
// reviewer sees rather than a cell nobody regenerated.
//
// Run with -update to rewrite the golden file, and read the diff before you do.
func TestCapabilitiesGolden(t *testing.T) {
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	got := core.DescribeCapabilities(caps)
	path := filepath.Join("testdata", "capabilities.golden")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("writing the golden file: %v", err)
		}
		t.Logf("wrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the golden file: %v (run with -update to create it)", err)
	}
	if got != string(want) {
		t.Fatalf("the capability table has changed.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// TestCapabilitiesConsiderEveryFeature turns "this profile decided it does not
// have that" into something a test can tell apart from "nobody looked".
//
// The specification makes the absent half of a capability table as load-bearing as the
// present half: a caller reads it to decide whether it may rely on a
// behaviour, and a feature nobody mentioned renders as unsupported — the safe
// answer, and indistinguishable from one the profile forgot. This is the check
// that fires when the inventory grows.
func TestCapabilitiesConsiderEveryFeature(t *testing.T) {
	for name, cfg := range map[string]rmq10.Config{
		"the richest deployment": goldenConfig(),
		"the barest one":         {Addrs: []string{"amqp://127.0.0.1:5672"}},
	} {
		missing, err := rmq10.MissingFeaturesForTest(cfg)
		if err != nil {
			t.Fatalf("%s: building the table: %v", name, err)
		}
		if len(missing) > 0 {
			t.Errorf("%s never mentions %v of the inventory", name, missing)
		}
	}
}

// TestEveryConditionalClaimStatesItsCondition is the rule about claims
// themselves.
//
// A conditional support whose condition nobody wrote down tells a caller
// nothing they can act on, and an emulation that does not say what the
// emulation is tells them less. The same check runs inside the conformance kit
// against a live broker; this is the half that needs none.
func TestEveryConditionalClaimStatesItsCondition(t *testing.T) {
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		switch s.Level {
		case mq.Conditional, mq.EmulatedDurable, mq.EmulatedVolatile:
			if s.Note == "" {
				t.Errorf("%v is advertised at %v with no condition stated", f, s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
	}
}

// TestEveryWithdrawalSaysWhat holds the other direction of the same rule, which
// The specification does not require and this profile does.
//
// Eight of this table's withdrawals are features the AMQP 0-9-1 driver really
// has — request/reply, a delayed redelivery, a scheduled publication, ordering
// by key, a wildcard subscription, a topology API, server-side filtering and
// at-most-once delivery — and "unsupported" with no reason would read as an
// omission by whoever wrote the table rather than as the difference the specification says
// the two profiles have. So each one states what the broker or this protocol
// lacks.
func TestEveryWithdrawalSaysWhat(t *testing.T) {
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	for _, f := range [...]mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureOrderedByKey,
		mq.FeatureWildcardSubscribe,
		mq.FeatureServerFilter,
		mq.FeatureAdmin,
		mq.FeatureRequestReply,
	} {
		s := caps.Support(f)
		if s.Level != mq.Unsupported {
			t.Errorf("%v is advertised at %v; this profile does not have it", f, s.Level)
			continue
		}
		if s.Note == "" {
			t.Errorf("%v is withdrawn with no reason. The AMQP 0-9-1 driver has this feature, so a bare "+
				"\"unsupported\" reads as an omission rather than as the difference the driver documentation describes", f)
		}
	}
}

// TestEveryConditionalClaimMovesWithItsFact is the both-directions test the specification
// asks for: a claim that depends on a fact has to change when the fact does.
//
// A table whose rows never moved would be a constant wearing a condition. Each
// row below names the configuration fact it depends on, and the test builds the
// table twice — with the fact and without it — and fails if the level is the
// same both times. That is the check that catches a claim which states a
// condition and then ignores it.
func TestEveryConditionalClaimMovesWithItsFact(t *testing.T) {
	base := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	for _, tc := range []struct {
		fact    string
		feature mq.Feature
		with    func(rmq10.Config) rmq10.Config
		without func(rmq10.Config) rmq10.Config
	}{
		{
			fact:    "the queues have a dead-letter exchange (Config.DeadLetter)",
			feature: mq.FeatureDeadLetter,
			with:    func(c rmq10.Config) rmq10.Config { c.DeadLetter = true; return c },
			without: func(c rmq10.Config) rmq10.Config { c.DeadLetter = false; return c },
		},
		{
			fact:    "the queues were declared with x-max-priority (Config.MaxPriority)",
			feature: mq.FeaturePriority,
			with:    func(c rmq10.Config) rmq10.Config { c.MaxPriority = 9; return c },
			without: func(c rmq10.Config) rmq10.Config { c.MaxPriority = 0; return c },
		},
		{
			fact:    "the subscription's queue is a quorum queue (Config.QueueType)",
			feature: mq.FeatureDurableConfirm,
			with:    func(c rmq10.Config) rmq10.Config { c.QueueType = rmq10.QueueQuorum; return c },
			without: func(c rmq10.Config) rmq10.Config { c.QueueType = rmq10.QueueClassic; return c },
		},
		{
			fact:    "the message is published durable (Config.NonDurable)",
			feature: mq.FeatureDurableStorage,
			with:    func(c rmq10.Config) rmq10.Config { c.NonDurable = false; return c },
			without: func(c rmq10.Config) rmq10.Config { c.NonDurable = true; return c },
		},
	} {
		t.Run(tc.feature.String(), func(t *testing.T) {
			withCaps, err := rmq10.CapabilitiesForTest(tc.with(base), "RabbitMQ", "4.1.0")
			if err != nil {
				t.Fatalf("building the table with the fact: %v", err)
			}
			withoutCaps, err := rmq10.CapabilitiesForTest(tc.without(base), "RabbitMQ", "4.1.0")
			if err != nil {
				t.Fatalf("building the table without the fact: %v", err)
			}
			strong, weak := withCaps.Support(tc.feature), withoutCaps.Support(tc.feature)
			if strong.Level.Rank() <= weak.Level.Rank() {
				t.Fatalf("%v is advertised at %v with %s and at %v without it. A claim that does not move with the "+
					"fact it names is a constant wearing a condition",
					tc.feature, strong.Level, tc.fact, weak.Level)
			}
			if weak.Note == "" && weak.Level == mq.Unsupported {
				t.Errorf("%v is withdrawn without %s and says nothing about why", tc.feature, tc.fact)
			}
		})
	}
}

// TestDriverNarrowsTheProtocolAndNeverWidensIt is the rule every driver in this
// repository is held to, and for this pair it is what keeps the two RabbitMQ
// tables from drifting in the rows that are NOT the subject.
//
// `internal/rabbit/protocol` owns the PROTOCOL's feature subset and is shared
// by both drivers. A driver narrows a row for what its own code does not
// implement and never widens one: a driver that claimed more than its protocol
// offers would be claiming something no code implements, and the shared table
// would stop being the authority both halves derive from.
func TestDriverNarrowsTheProtocolAndNeverWidensIt(t *testing.T) {
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		_, proto, err := rmq10.ProtocolSupportForTest(f)
		if err != nil {
			t.Fatalf("reading the protocol's table: %v", err)
		}
		driver := caps.Support(f)
		emulation := driver.Level == mq.EmulatedVolatile || driver.Level == mq.EmulatedDurable
		if driver.Level.Rank() > proto.Level.Rank() && !emulation {
			t.Errorf("this driver advertises %v at %v and the AMQP 1.0 protocol arm offers only %v: a driver may "+
				"narrow the protocol's answer and never widen it", f, driver.Level, proto.Level)
		}
		if emulation && driver.Note == "" {
			// The one widening that is allowed has a condition: the specification makes
			// an emulated claim say what the emulation IS, because "the
			// protocol does not have this and it works anyway" is a sentence
			// a caller has to be able to read.
			t.Errorf("this driver emulates %v and says nothing about how, which the message contract forbids", f)
		}
	}
}

// TestTheTwoProfilesAreNotTheSame is the whole subject
// asserted rather than described.
//
// The specification says the AMQP 1.0 profile is "never silently substituted", and the only
// reason that sentence is in the specification is that the two profiles differ.
// So the two tables have to differ, and this test fails if they are identical:
// a change that quietly equalised them would take the substitution rule's
// justification away while leaving the rule in place.
//
// The comparison is against `internal/rabbit/protocol`'s two arms rather than
// against the AMQP 0-9-1 DRIVER's table, and the reason is a module boundary:
// importing `drivers/rabbitmq` here would make this module — whose whole point
// is that it speaks a different protocol — depend on amqp091-go. The shared
// arms are the authority both drivers derive from, so a difference there is a
// difference in both tables; `conformance/matrix`, which already depends on
// both driver modules, is where the two FINAL tables are compared.
func TestTheTwoProfilesAreNotTheSame(t *testing.T) {
	differences := 0
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		older, newer, err := rmq10.ProtocolSupportForTest(f)
		if err != nil {
			t.Fatalf("reading the protocols' tables: %v", err)
		}
		if older.Level != newer.Level || older.Note != newer.Note {
			differences++
		}
	}
	if differences == 0 {
		t.Fatal("the two arms answer identically. The \"never silently substituted\" exists because " +
			"they do not: if the two tables are the same, either a difference has been equalised by mistake or the " +
			"rule has lost its justification")
	}
	// And the differences have to be the ones the specification names, in this driver's own
	// table: a table that differed only in its notes would pass the count
	// above while claiming the same features.
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building this driver's table: %v", err)
	}
	for _, f := range [...]mq.Feature{
		mq.FeatureRequestReply,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureOrderedByKey,
		mq.FeatureWildcardSubscribe,
	} {
		older, _, err := rmq10.ProtocolSupportForTest(f)
		if err != nil {
			t.Fatalf("reading the protocols' tables: %v", err)
		}
		if older.Level == mq.Unsupported {
			t.Errorf("the AMQP 0-9-1 arm does not claim %v either, so this profile's withdrawal is not the "+
				"difference the specification describes and the list in capabilities.go is out of date", f)
		}
		if got := caps.Support(f).Level; got != mq.Unsupported {
			t.Errorf("this profile advertises %v at %v; the feature-subset criterion for the specification names it as "+
				"one of the features AMQP 1.0 on RabbitMQ does not have", f, got)
		}
	}
}

// TestTheAttemptCountSaysWhatItDependsOn is one of the four (verify) items
// held to the only honest answer available without a broker.
//
// The specification marks `x-delivery-count → Attempt (quorum queues)` **(verify)**. On this
// protocol the count is the AMQP header's `delivery-count` — a protocol field
// rather than a header extension — and whether a CLASSIC queue's redeliveries
// reach it is not something this repository can check without a broker. The specification
// makes an Attempt of zero "the broker does not count attempts", which is a
// different answer from "this is the first delivery", so the claim states the
// condition rather than asserting a count that may never move.
func TestTheAttemptCountSaysWhatItDependsOn(t *testing.T) {
	base := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	quorum, err := rmq10.CapabilitiesForTest(base, "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the quorum table: %v", err)
	}
	classicCfg := base
	classicCfg.QueueType = rmq10.QueueClassic
	classic, err := rmq10.CapabilitiesForTest(classicCfg, "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the classic table: %v", err)
	}
	qNote := quorum.Support(mq.FeatureAtLeastOnce).Note
	cNote := classic.Support(mq.FeatureAtLeastOnce).Note
	if !strings.Contains(qNote, "delivery-count") {
		t.Errorf("the at-least-once note does not say where the attempt count comes from: %q", qNote)
	}
	if !strings.Contains(qNote, "x-delivery-count") {
		t.Errorf("the note does not say that the count is a protocol field rather than RabbitMQ's header "+
			"extension, which is the difference a caller who moves profiles sees: %q", qNote)
	}
	if cNote == qNote {
		t.Error("the at-least-once note is the same for a classic queue and a quorum queue. Whether a classic " +
			"queue's redeliveries reach the AMQP 1.0 header's delivery-count is not verified here, and a note that " +
			"did not say so would be claiming a count that may be 1 on every delivery")
	}
	if !strings.Contains(cNote, "not verified") {
		t.Errorf("the classic-queue note does not say the count is unverified: %q", cNote)
	}
}

// TestOnlyTheCrossProfileCeilingIsAdvertised is the rule about the numeric
// half of the table: an unadvertised limit is distinct from one advertised as
// zero, and an advertised one is something a caller may RELY on.
//
// Exactly one number here is not a guess. An AMQP 0-9-1 field-table key is a
// 255-byte shortstr and an AMQP 1.0 application-property key is a string with
// no such bound — and no RabbitMQ deployment has only the 1.0 endpoint, so a
// key longer than 255 bytes published through this profile cannot be
// represented to a 0-9-1 consumer of the same queue. "Stay under this and both
// profiles can read it" is what a caller relies on and it is true, which is
// what makes it advertisable.
//
// Everything else stays unknown, and the message ceiling is the one worth
// naming: RabbitMQ's `max_message_size` is a SERVER setting rather than a
// connection property, so the only message limit this driver can know is the
// one the peer declares on a link, which the CORE reads and this profile does
// not invent. The scripted peer declares none, so it is unknown here.
func TestOnlyTheCrossProfileCeilingIsAdvertised(t *testing.T) {
	caps, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	if v := caps.Limit(mq.LimitMaxHeaderBytes); !v.Known || v.Value != 255 {
		t.Errorf("MaxHeaderBytes is %v (known %v), want 255: it is the AMQP 0-9-1 field-table key's own ceiling "+
			"and the one limit a caller of BOTH profiles has to respect", v.Value, v.Known)
	}
	for i := range mq.LimitKeyCount {
		k := mq.LimitKey(i)
		if k == mq.LimitMaxHeaderBytes {
			continue
		}
		if v := caps.Limit(k); v.Known {
			t.Errorf("the table advertises %v = %d. Nothing in this profile knows that limit before the peer "+
				"declares one on a link, so an advertised number here came from a guess", k, v.Value)
		}
	}
}

// TestCapabilityTableIsConstantForTheLifeOfTheBackend is the other rule
// about the table: it is built once, at open, from facts that are already
// known.
//
// The version is the fact a reader would expect to move it, and above the gate
// it deliberately does not: [rmq10.CheckBrokerForTest] has already refused
// everything below RabbitMQ 4.0, so there is no claim left that a 4.1 broker
// has and a 4.0 broker does not. Stating that as a test is what stops a future
// version-dependent claim from being added without the evidence to support it.
func TestCapabilityTableIsConstantForTheLifeOfTheBackend(t *testing.T) {
	first, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", "4.0.0")
	if err != nil {
		t.Fatalf("building the table for 4.0.0: %v", err)
	}
	for _, version := range [...]string{"4.1.0", "4.2.9", "5.0.0"} {
		other, err := rmq10.CapabilitiesForTest(goldenConfig(), "RabbitMQ", version)
		if err != nil {
			t.Fatalf("building the table for %s: %v", version, err)
		}
		if a, b := core.DescribeCapabilities(first), core.DescribeCapabilities(other); a != b {
			t.Errorf("the table for 4.0.0 and the table for %s differ. A version-dependent claim is allowed and has "+
				"to be recorded in capabilities.go with the evidence for it, and there is none", version)
		}
	}
}

// TestTheClaimedBoundaryIsTheProfilesOwn is a consistency check between two
// places the same sentence has to appear.
//
// The specification requires the exact durable confirmation boundary to be stated, and it is
// stated twice — in the profile's durable rule, which is what the core refuses
// a ConfirmDurable publisher against, and in the capability table, which is
// what a caller reads. Two copies of a sentence drift; this is what stops them.
func TestTheClaimedBoundaryIsTheProfilesOwn(t *testing.T) {
	cfg := goldenConfig()
	profile, err := cfg.Profile()
	if err != nil {
		t.Fatalf("building the profile: %v", err)
	}
	caps, err := rmq10.CapabilitiesForTest(cfg, "RabbitMQ", "4.1.0")
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	note := caps.Support(mq.FeatureDurableConfirm).Note
	if note != profile.Durable.Note {
		t.Errorf("the capability table states the durable boundary as %q and the profile's own rule states it as %q",
			note, profile.Durable.Note)
	}
	if !strings.Contains(note, "no equivalent") {
		t.Errorf("the durable boundary does not mention that there is no confirm strategy to select: %q. The "+
			"AMQP 0-9-1 driver chooses between per-message, windowed or asynchronous confirms, and a "+
			"caller who moved profiles looking for that choice needs to be told it has no equivalent", note)
	}
}
