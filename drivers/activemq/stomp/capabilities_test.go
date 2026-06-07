package stomp_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	stomp "bitbucket.org/iqhive/iqmq/drivers/activemq/stomp"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
)

// capsOf is the table this configuration would publish.
func capsOf(t *testing.T, cfg stomp.Config) mq.Capabilities {
	t.Helper()
	caps, err := stomp.CapabilitiesForTest(cfg)
	if err != nil {
		t.Fatalf("CapabilitiesForTest(%v): %v", cfg.Broker, err)
	}
	return caps
}

// stompProtocol is the shared mapping's own half of the table, which is what
// the driver's is compared against.
func stompProtocol(t *testing.T) protocol.Protocol {
	t.Helper()
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		t.Fatalf("protocol.New(STOMP): %v", err)
	}
	return p
}

// combinations is the table the specification makes this driver: two brokers
// times whether the client configured a selector.
//
// The Artemis-with-a-selector row is not reachable through `Config.Validate`
// — the selector is refused there, because Artemis IGNORES one on a fully
// qualified queue name — and it is in the table anyway, because the
// capability function is reached by the conformance kit and by
// `CapabilitiesForTest` without validation and must not claim a filter it
// does not have.
//
// `Receipts: true` is set explicitly in every row, which is now redundant
// rather than load-bearing: [stomp.CapabilitiesForTest] resolves the
// configuration, so the derived `Receipts` field is computed for a row that
// leaves it unset. It stays because it says what each row is: a client with
// receipts on. Before the accessor resolved, an unresolved Config read as
// NoReceipts and the table withdrew three rows a real client has.
func combinations() map[string]stomp.Config {
	return map[string]stomp.Config{
		"artemis, no selector": {Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}, Receipts: true},
		"artemis, a selector configured anyway": {
			Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}, Receipts: true,
			Selector: "colour = 'red'",
		},
		"classic, no selector": {Broker: activemq.Classic, Addrs: []string{"localhost:61613"}, Receipts: true},
		"classic, a selector": {
			Broker: activemq.Classic, Addrs: []string{"localhost:61613"}, Receipts: true,
			Selector: "colour = 'red'",
		},
		// The two modes that ENVELOPE, because the table moves with
		// them: a STOMP frame header is text and cannot carry a binary value
		// or a repeated name, and under these two the driver carries both in
		// iqmq's own envelope inside the body. A combination table that only
		// held the strict default would leave the claim the conformance kit
		// actually exercises -- the harness configures MetadataAdaptive --
		// entirely unchecked.
		"artemis, the adaptive envelope": {
			Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}, Receipts: true,
			Metadata: mq.MetadataAdaptive,
		},
		"classic, the portable envelope": {
			Broker: activemq.Classic, Addrs: []string{"localhost:61613"}, Receipts: true,
			Metadata: mq.MetadataPortableEnvelope,
		},
	}
}

// envelopes reports whether a configuration asks for the envelope, which is
// what makes two header rows the driver's own mechanism rather than the
// protocol's.
func envelopes(cfg stomp.Config) bool {
	return cfg.Metadata == mq.MetadataPortableEnvelope || cfg.Metadata == mq.MetadataAdaptive
}

// TestDriverNarrowsAndNeverWidensTheProtocol is the rule `capabilities.go`
// states about itself, held to every row of every combination.
//
// The driver's table starts from the shared protocol's answer for this broker
// — which is the PROTOCOL's half, measured against the pinned images and held
// by the specification's own tests — and narrows it for what this driver's code does
// not implement. A row that claimed MORE than the protocol supports would be
// a promise the wire cannot keep, and the generated capability matrix would
// publish it.
//
// Three rows are exempt, and each is exempt because the driver adds a
// MECHANISM of its own rather than discovering something about the protocol.
// They are asserted separately and explicitly in
// [TestTheThreeRowsTheDriverAddsItself]; an exemption nobody pinned is an
// exemption that grows.
func TestDriverNarrowsAndNeverWidensTheProtocol(t *testing.T) {
	t.Parallel()
	p := stompProtocol(t)
	for name, cfg := range combinations() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caps := capsOf(t, cfg)
			for i := range mq.FeatureCount {
				f := mq.Feature(i)
				if ownMechanism(cfg, f) {
					continue
				}
				driver, proto := caps.Support(f), p.Support(cfg.Broker, f)
				if driver.Level.Rank() > proto.Level.Rank() {
					t.Errorf("%v: the driver claims %v and the protocol supports only %v. A table that claimed "+
						"more than the wire can do is a promise nothing can keep, and the generated capability "+
						"matrix publishes it.\n driver note: %s\nprotocol note: %s",
						f, driver.Level, proto.Level, driver.Note, proto.Note)
				}
			}
		})
	}
}

// ownMechanism reports whether a row is one the driver decides on its own
// rather than narrowing from the protocol.
func ownMechanism(cfg stomp.Config, f mq.Feature) bool {
	switch f {
	case mq.FeatureNack, mq.FeatureBatchPublish:
		return true
	case mq.FeatureDeadLetter:
		// Only with a route of the driver's own: with none, the row really
		// is the protocol's answer narrowed.
		return cfg.DeadLetterTopic != ""
	case mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders:
		// Only under the enveloping modes, where the mechanism is iqmq's
		// own envelope inside the body and not anything a frame header does.
		// Under the strict default the row really is the protocol's
		// Unsupported, and the narrowing rule covers it.
		return envelopes(cfg)
	default:
		return false
	}
}

// TestTheThreeRowsTheDriverAddsItself pins the exemptions, with their levels
// and their notes.
//
// Each is an EMULATION and the specification makes an emulation say what the emulation is,
// because "emulated" with no note tells a program that something works by
// magic. `driverkit.CapabilityBuilder` refuses a table that does not, so the
// note is enforced at construction; what this pins is the LEVEL, which the
// builder cannot check and which decides whether a `RequireAtLeast` passes.
func TestTheThreeRowsTheDriverAddsItself(t *testing.T) {
	t.Parallel()
	p := stompProtocol(t)
	for name, cfg := range map[string]stomp.Config{
		"artemis": {Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}, DeadLetterTopic: "dead"},
		"classic": {Broker: activemq.Classic, Addrs: []string{"localhost:61613"}, DeadLetterTopic: "dead"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caps := capsOf(t, cfg)

			// FeatureNack. The protocol's answer is Unsupported, and it is
			// Unsupported for a MEASURED reason: a NACK frame returns no
			// message on either broker. The driver claims EmulatedDurable
			// because it has a mechanism the protocol does not describe --
			// a subscription reset, after which the broker redelivers from
			// its own durable storage. Durable and not volatile precisely
			// because nothing is held in this process.
			nack := caps.Support(mq.FeatureNack)
			if got := p.Support(cfg.Broker, mq.FeatureNack).Level; got != mq.Unsupported {
				t.Errorf("the protocol now claims %v for FeatureNack; this test is about the driver WIDENING an "+
					"Unsupported row and would stop being about that", got)
			}
			if nack.Level != mq.EmulatedDurable {
				t.Errorf("FeatureNack is %v, want %v: the mechanism is the broker's own redelivery of an "+
					"unacknowledged client-individual delivery when its subscription goes away, so nothing is "+
					"held in this process and a crash loses nothing", nack.Level, mq.EmulatedDurable)
			}
			if nack.Scope != mq.ScopeGroup {
				t.Errorf("FeatureNack's scope is %v, want %v: the unit is the SUBSCRIPTION and not the delivery -- "+
					"nacking one returns them all", nack.Scope, mq.ScopeGroup)
			}
			for _, must := range []string{"NACK", "ErrLeaseLost"} {
				if !strings.Contains(nack.Note, must) {
					t.Errorf("FeatureNack's note does not mention %q, and a caller reading the table is the only "+
						"one who can find out what a requeue costs here: %q", must, nack.Note)
				}
			}

			// FeatureDeadLetter with a route: the driver PUBLISHES and then
			// acknowledges, which is the order, and the publication is
			// to a destination the broker stores -- hence durable.
			dl := caps.Support(mq.FeatureDeadLetter)
			if dl.Level != mq.EmulatedDurable {
				t.Errorf("FeatureDeadLetter with a configured route is %v, want %v", dl.Level, mq.EmulatedDurable)
			}
			if !strings.Contains(dl.Note, "dead") {
				t.Errorf("FeatureDeadLetter's note does not name the route it publishes to: %q", dl.Note)
			}
			if !strings.Contains(dl.Note, "") {
				t.Errorf("FeatureDeadLetter's note does not say that the original is acknowledged only once the "+
					"dead-letter publication is confirmed, which is the whole of the emulation: %q", dl.Note)
			}

			// FeatureBatchPublish: one SEND frame carries one message and
			// the library has no batched write, so a batch is a loop --
			// which is volatile, because a failure part-way through leaves
			// what it had already written published.
			batch := caps.Support(mq.FeatureBatchPublish)
			if batch.Level != mq.EmulatedVolatile {
				t.Errorf("FeatureBatchPublish is %v, want %v: a loop's failure leaves what it had already sent "+
					"published, which is not atomic and must not be claimed as durable", batch.Level,
					mq.EmulatedVolatile)
			}
			if !strings.Contains(batch.Note, "loop") {
				t.Errorf("FeatureBatchPublish's note does not say the batch is a loop: %q", batch.Note)
			}
			if caps.Has(mq.FeatureAtomicBatchPublish) {
				t.Error("FeatureAtomicBatchPublish is claimed. There is no batch frame in STOMP, so nothing can " +
					"make N publications one outcome, and a caller who required atomicity would get a loop")
			}
		})
	}
}

// TestTheEnvelopeRowsMoveWithTheConfiguration is the specification's own rule that a
// capability table is a property of the CLIENT, applied to the two rows a
// STOMP frame header cannot carry at all.
//
// A frame header is a text name and a text value, so a value that is not valid
// UTF-8 cannot travel and a repeated name is put on the wire and then
// discarded -- STOMP 1.2 says the first occurrence wins, and nothing reports
// the loss. Under the strict default the honest answer is a refusal, and under
// the enveloping modes the driver carries both inside iqmq's own envelope
// in the BODY. Both answers are honest and they are different, so the test has
// to be two-sided: a table that claimed the emulation regardless would promise
// a caller in strict mode something the wire refuses, and one that refused
// regardless would hide the route the promise is actually kept by.
func TestTheEnvelopeRowsMoveWithTheConfiguration(t *testing.T) {
	t.Parallel()
	rows := [...]mq.Feature{mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders}
	for name, cfg := range combinations() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caps := capsOf(t, cfg)
			for _, f := range rows {
				got := caps.Support(f)
				if !envelopes(cfg) {
					if got.Level != mq.Unsupported {
						t.Errorf("%v is %v under %v. A STOMP frame header is text, so neither a binary value nor "+
							"a repeated name can travel -- and a claim here would be a promise the wire refuses "+
							"before any I/O", f, got.Level, cfg.Metadata)
					}
					continue
				}
				if got.Level != mq.EmulatedDurable {
					t.Errorf("%v is %v under %v, want %v: the envelope is in the message the BROKER stores, so "+
						"nothing is held in this process and a crash loses nothing -- which is what separates the "+
						"durable emulation from the volatile one", f, got.Level, cfg.Metadata, mq.EmulatedDurable)
				}
				if !strings.Contains(got.Note, "envelope") {
					t.Errorf("%v's note does not say that the mechanism is the envelope: %q", f, got.Note)
				}
				if !strings.Contains(got.Note, "non-iqmq") {
					t.Errorf("%v's note does not say what the envelope COSTS -- the bytes on the wire are not "+
						"what a non-iqmq consumer of that destination would read, which is the one thing a "+
						"deployment choosing the mode needs to know: %q", f, got.Note)
				}
			}
		})
	}
}

// TestEveryQualifiedRowStatesItsCondition is the evidence rule, which the
// conformance kit also checks: a Conditional support states its condition and
// an emulation says what the emulation is.
//
// "Conditional" with no note tells a program that something might work, under
// conditions nobody wrote down. The builder refuses one at construction, so
// this would fail as a construction error rather than a bad cell — which is
// exactly where it should fail, and is why the assertion is worth having
// anyway: a driver that changed a row to Conditional and forgot the note
// discovers it here rather than in the published matrix.
func TestEveryQualifiedRowStatesItsCondition(t *testing.T) {
	t.Parallel()
	for name, cfg := range combinations() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caps := capsOf(t, cfg)
			for i := range mq.FeatureCount {
				f := mq.Feature(i)
				s := caps.Support(f)
				switch s.Level {
				case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
					if strings.TrimSpace(s.Note) == "" {
						t.Errorf("%v is %v with no note. The specification requires the condition or the emulation to be "+
							"stated, and the conformance kit publishes this cell", f, s.Level)
					}
				case mq.Unsupported, mq.Native:
				}
			}
		})
	}
}

// TestAdminIsAbsentAndTheInterfaceProvesIt is one claim checked twice, because
// the two halves can disagree.
//
// STOMP has no topology frame: there is no frame that creates a destination,
// deletes one, purges one or reports its depth, in any version of the
// specification. So the table says Unsupported — and the BACKEND must also
// not satisfy [mq.AdminBackend], because the core asserts the interface once
// when the client is built and records the answer. A backend that satisfied it
// and then refused every call would make `Client.Admin()` available and
// FeatureAdmin look present to anyone who asked the client rather than the
// table, which the specification forbids.
func TestAdminIsAbsentAndTheInterfaceProvesIt(t *testing.T) {
	t.Parallel()
	for name, cfg := range combinations() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caps := capsOf(t, cfg)
			s := caps.Support(mq.FeatureAdmin)
			if s.Level != mq.Unsupported {
				t.Errorf("FeatureAdmin is %v; STOMP has no topology frame at all", s.Level)
			}
			if strings.TrimSpace(s.Note) == "" {
				t.Error("FeatureAdmin is Unsupported with no note. The specification does not require one, and an operator " +
					"reading the matrix needs to be told what DOES create the topology -- drivers/activemq's " +
					"Admin against Artemis, or a SUBSCRIBE on Classic")
			}
		})
	}
	var b any = (*stomp.Backend)(nil)
	if _, carries := b.(mq.AdminBackend); carries {
		t.Error("*stomp.Backend satisfies mq.AdminBackend. The core asserts the interface once when the client is " +
			"built, so a backend that carries the surface and refuses every call makes Client.Admin() available " +
			"and FeatureAdmin look present to a caller who asked the client rather than the table")
	}
}

// TestTheDriverWithdrawsWhatTheDeploymentDoesNotHave is the other three rows
// `capabilities.go` decides on its own, all of which NARROW.
//
// Each is a fact about the deployment rather than about the protocol, and each
// would otherwise be a claim the broker silently does not honour — which is
// worse than an absence, because a caller acts on it.
func TestTheDriverWithdrawsWhatTheDeploymentDoesNotHave(t *testing.T) {
	t.Parallel()
	classic := stomp.Config{Broker: activemq.Classic, Addrs: []string{"localhost:61613"}}
	artemis := stomp.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}

	// A scheduled publication on Classic. Measured: with the broker's
	// scheduler OFF, a message carrying AMQ_SCHEDULED_DELAY is delivered
	// IMMEDIATELY and the header travels through to the consumer -- so a
	// driver that claimed the feature regardless would promise a delay the
	// broker discards, with nothing reporting it.
	if got := capsOf(t, classic).Support(mq.FeatureScheduledPublish); got.Level != mq.Unsupported {
		t.Errorf("FeatureScheduledPublish on Classic with no scheduler is %v, want Unsupported: measured, with "+
			"schedulerSupport off the broker delivers the message at once and passes the header through",
			got.Level)
	} else if !strings.Contains(got.Note, "schedulerSupport") {
		t.Errorf("the withdrawal does not name the broker setting that would restore it: %q", got.Note)
	}
	withScheduler := classic
	withScheduler.Scheduler = true
	if got := capsOf(t, withScheduler).Support(mq.FeatureScheduledPublish); got.Level != mq.Native {
		t.Errorf("FeatureScheduledPublish on Classic WITH the scheduler is %v, want Native: a withdrawal that "+
			"could not be reversed would make Config.Scheduler pointless", got.Level)
	}
	// Artemis's scheduler is always available, so the field is ignored there.
	if got := capsOf(t, artemis).Support(mq.FeatureScheduledPublish); got.Level != mq.Native {
		t.Errorf("FeatureScheduledPublish on Artemis is %v, want Native", got.Level)
	}

	// Receipts. Without them a STOMP publication is fire-and-forget: the
	// frame is written and the broker says nothing back, so the specification's
	// ConfirmAccepted has no boundary at all.
	artemis.Receipts = true
	classic.Receipts = true
	noReceipts := artemis
	noReceipts.Receipts, noReceipts.NoReceipts = false, true
	for _, f := range [...]mq.Feature{mq.FeatureReceipts} {
		if got := capsOf(t, noReceipts).Support(f); got.Level != mq.Unsupported {
			t.Errorf("%v under NoReceipts is %v, want Unsupported", f, got.Level)
		}
	}
	if got := capsOf(t, artemis).Support(mq.FeatureReceipts); got.Level != mq.Native {
		t.Errorf("FeatureReceipts with receipts on is %v, want Native", got.Level)
	}

	// The server-side filter, which is the one row this arm WINS and wins on
	// one broker. Measured: a `selector` header filters on Classic and is
	// ignored on Artemis when the subscription is an FQQN.
	classicSelector := classic
	classicSelector.Selector = "colour = 'red'"
	if got := capsOf(t, classicSelector).Support(mq.FeatureServerFilter); got.Level != mq.Native {
		t.Errorf("FeatureServerFilter on Classic with a selector is %v, want Native: it is the row that makes "+
			"The specification end \"STOMP kept for Classic\"", got.Level)
	}
	if got := capsOf(t, classic).Support(mq.FeatureServerFilter); got.Level != mq.Conditional {
		t.Errorf("FeatureServerFilter on Classic with NO selector is %v, want Conditional: the evidence rule "+
			"is that a claim which cannot be exercised does not render, and a selector nobody configured cannot "+
			"be", got.Level)
	}
	artemisSelector := artemis
	artemisSelector.Selector = "colour = 'red'"
	if got := capsOf(t, artemisSelector).Support(mq.FeatureServerFilter); got.Level != mq.Unsupported {
		t.Errorf("FeatureServerFilter on Artemis with a selector is %v, want Unsupported: measured, the header is "+
			"IGNORED on a fully qualified queue name, which is the only shape this driver uses there -- so a "+
			"claim here would be a filter that silently passes everything", got.Level)
	}
}

// TestLimitsAreOnlyWhatTheDriverKnows is the rule that an unadvertised
// limit is never an overloaded zero.
//
// Five of the seven are deliberately left unknown, and each absence is a
// decision: neither broker documents a ceiling on a scheduled delivery, STOMP
// grants no lease so there is nothing to bound, and a batch here is a loop
// with no protocol-level bound at all. A driver that published zero for any of
// them would be telling a caller that nothing is allowed.
func TestLimitsAreOnlyWhatTheDriverKnows(t *testing.T) {
	t.Parallel()
	lim := capsOf(t, stomp.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:61613"}}).Limits()
	if lim.MaxHeaderBytes.Value != activemq.MaxNameLen || !lim.MaxHeaderBytes.Known {
		t.Errorf("MaxHeaderBytes is %+v, want the known value %d: it is the longest header name or value either "+
			"broker carries, which is internal/activemq's own name ceiling", lim.MaxHeaderBytes, activemq.MaxNameLen)
	}
	// The invariant, which holds whichever way the protocol's own answer
	// goes: a limit is advertised only when it is known, and a limit that is
	// known is not zero.
	if lim.MaxMessageBytes.Known != (lim.MaxMessageBytes.Value > 0) {
		t.Errorf("MaxMessageBytes is %+v: a known limit of zero would tell a caller that no message is allowed, "+
			"and an unknown one must not be an overloaded zero", lim.MaxMessageBytes)
	}
	for name, l := range map[string]bool{
		"MaxDelay":         lim.MaxDelay.Known,
		"MaxLease":         lim.MaxLease.Known,
		"MaxBatchMessages": lim.MaxBatchMessages.Known,
		"MaxBatchBytes":    lim.MaxBatchBytes.Known,
		"MaxHeaderCount":   lim.MaxHeaderCount.Known,
	} {
		if l {
			t.Errorf("%s is advertised. Nothing measured a ceiling for it: neither broker documents one for a "+
				"scheduled delivery, STOMP grants no lease, and a batch is a loop -- so advertising a number "+
				"would be a guess a caller would size itself against", name)
		}
	}
}

// TestDriverInfoNamesTheProtocolAsWellAsTheProduct is, and it is a test
// because a report keyed on the wrong name is a report that overwrites
// another driver's.
func TestDriverInfoNamesTheProtocolAsWellAsTheProduct(t *testing.T) {
	t.Parallel()
	if stomp.DriverName != "activemq" {
		t.Errorf("the driver name is %q, want \"activemq\": it is the same driver as drivers/activemq -- one "+
			"broker family, the \"protocol choice explicit\" -- and the PROFILE is which product over which "+
			"protocol", stomp.DriverName)
	}
	for _, profile := range [...]string{stomp.ProfileArtemis, stomp.ProfileClassic} {
		if !strings.Contains(profile, "stomp") {
			t.Errorf("the profile %q does not name the protocol. drivers/activemq uses the bare product names, "+
				"and two conformance reports cannot share a key", profile)
		}
	}
	if stomp.ProfileArtemis == stomp.ProfileClassic {
		t.Error("both profiles have the same name")
	}
	if stomp.ClientModule != "github.com/go-stomp/stomp/v3" {
		t.Errorf("the client module is %q", stomp.ClientModule)
	}
	if !strings.HasPrefix(stomp.ClientVersion, "v3.") {
		t.Errorf("the client version is %q, which is not the v3 line this driver is written against",
			stomp.ClientVersion)
	}
}
