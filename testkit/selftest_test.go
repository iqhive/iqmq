package testkit_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/testkit"
)

// brokenHarness builds a memory-driver harness whose backend is wrapped in the
// faults the caller names.
//
// The memory driver passes the whole conformance suite, which is what makes it
// the right subject for this test: every failure below is caused by exactly one
// injected defect, so a scenario that does not fail is a scenario that cannot.
func brokenHarness(faults testkit.Faults) testkit.Harness {
	return tunedHarness(faults, testkit.EvidenceInProcess, nil, 0)
}

// selftestSecret is the credential the redaction self-tests look for. It is
// not a real one, and it is spelled distinctively so that a match cannot be a
// coincidence.
const selftestSecret = "selftest-p4ssw0rd-not-a-real-credential"

// tunedHarness is brokenHarness with the three things a self-test sometimes
// has to change: what kind of evidence the run claims to be, which credentials
// the harness declares, and whether the broker advertises a message-size
// limit.
//
// Evidence is a fixture here rather than a fact -- these runs are all in
// process -- and it has to be settable because two scenarios branch on it:
// `limits/advertised` only fails a driver whose evidence is a real broker, and
// a self-test that could not say "container" could not watch it fail.
func tunedHarness(faults testkit.Faults, evidence testkit.Evidence, secrets []string, maxBytes int) testkit.Harness {
	var seq atomic.Uint64
	return testkit.Harness{
		Driver:   "memory-broken",
		Evidence: evidence,
		Secrets:  secrets,
		Timeout:  10 * time.Second,
		Fidelity: 64,
		TopicName: func(purpose string) string {
			return "selftest." + strconv.FormatUint(seq.Add(1), 36) + "." + strings.Map(func(r rune) rune {
				if r == '/' {
					return '-'
				}
				return r
			}, purpose)
		},
		Open: func(_ context.Context, n testkit.Need) (*mq.Client, error) {
			size := n.MaxMessageSize
			if size == 0 {
				size = maxBytes
			}
			topo := memory.TopologyConfig{
				Partitions:       n.Partitions,
				AckDeadline:      n.AckDeadline,
				Priority:         n.Priority,
				DedupWindow:      n.DedupWindow,
				CapacityMessages: n.Capacity,
				MaxMessageSize:   size,
			}
			if n.Retain {
				// The same mapping memory/conformance_test.go makes,
				// and it has to be here too: replay and the two seek
				// positions are conditional on a stream topology with a
				// retention bound, so a harness that ignored Need.Retain
				// made the memory driver refuse StartEarliest for every
				// one of them. Three capability probes could then only
				// ever report a refusal, which is the answer a self-test
				// about refusals must not get for free.
				topo.Kind = memory.TopologyStream
				topo.Retention = memory.Retention{MaxMessages: 4096}
			}
			cfg := memory.Config{
				AutoCreate:      true,
				Clock:           memory.NewTestClock(time.Now()),
				DefaultTopology: topo,
			}
			b, err := memory.NewBroker(cfg)
			if err != nil {
				return nil, err
			}
			return mq.NewClient(testkit.Wrap(b, faults)), nil
		},
		Advance: func(_ context.Context, c *mq.Client, d time.Duration) error {
			b, ok := c.Native().(*memory.Broker)
			if !ok {
				return mq.ErrUnsupported
			}
			return b.Advance(d)
		},
	}
}

// TestTheKitRejectsADriverItShouldReject is the check that makes every other
// check in this package worth running.
//
// A conformance kit that has never rejected anything is assumed not to work, and
// six checks in this repository turned out not to. So this takes the one driver
// that passes the whole suite, breaks a single thing it claims, and asserts that
// the scenario responsible for that claim fails — and that it passes again with
// the defect removed, so the failure is caused by the defect and not by the
// fixture.
func TestTheKitRejectsADriverItShouldReject(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		scenario string
		faults   testkit.Faults
		wants    string
		// control is what the same scenario reports for the unbroken
		// driver. It is usually a pass; for a scenario gated on the
		// driver NOT claiming the feature it is a skip, because the
		// unbroken driver does claim it and the gate does not select it.
		control testkit.Outcome
	}{
		{
			// The driver claims it cannot extend a lease while its
			// settler happily does. A negative claim nobody checks is
			// how a caller ends up relying on behaviour a driver is
			// free to remove.
			name:     "a withdrawn claim the driver still honours",
			scenario: "capability/ExtendLease/refused",
			faults:   testkit.Faults{Capabilities: testkit.WithdrawFeature(mq.FeatureExtendLease)},
			wants:    "the feature worked anyway",
			control:  testkit.OutcomeSkip,
		},
		{
			name:     "a claimed lease extension the adapter dropped",
			scenario: "capability/ExtendLease/works",
			faults:   testkit.Faults{HideExtender: true},
			// The failure NAMES the call now. It used to read "the
			// feature did not work", because the positive direction
			// had one message for every error; a driver that
			// advertises a feature and refuses its own call is a
			// different defect from one that accepts the call and
			// does nothing, and the report says which.
			wants:   "refused Delivery.Extend",
			control: testkit.OutcomePass,
		},
		{
			// The driver advertises a lease ceiling and its Extend
			// grants past it. `DeadlineExtender.Extend` returns only
			// an error, so the shorter grant is invisible: the lease
			// scheduler goes on assuming the documented minimum and
			// the first sign is a redelivery underneath a handler
			// that is still working.
			name:     "a lease ceiling the driver grants past",
			scenario: "capability/ExtendLease/works",
			faults:   testkit.Faults{Capabilities: testkit.AdvertiseMaxLease(2 * time.Second)},
			wants:    "longer than the advertised MaxLease",
			control:  testkit.OutcomePass,
		},
		{
			// The driver claims delayed redelivery and redelivers
			// immediately, which makes every backoff invisible.
			name:     "a delay the driver ignores",
			scenario: "capability/DelayedRedelivery/works",
			faults:   testkit.Faults{DropNackDelay: true},
			wants:    "redelivered the message immediately",
			control:  testkit.OutcomePass,
		},
		{
			// The acknowledgement reports success and settles
			// nothing, so the message comes back when the lease
			// expires. The whole point.
			name:     "an acknowledgement that settles nothing",
			scenario: "settle/ack-prevents-redelivery",
			faults:   testkit.Faults{SwallowAck: true},
			wants:    "was redelivered",
			control:  testkit.OutcomePass,
		},
		{
			// The ordering half of the guarantee machinery. A
			// driver that claims OrderedByKey and hands out each
			// batch backwards is refuted by one counter-example,
			// which is the one direction a guarantee CAN be
			// settled in by a single run.
			name:     "an ordering the driver claims and its fetch reverses",
			scenario: "capability/OrderedByKey/works",
			faults:   testkit.Faults{ReorderFetch: true},
			wants:    "this run observed the guarantee break",
			control:  testkit.OutcomePass,
		},
		{
			// Item 3 of this increment: the delivery half of
			// exactly-once, which the de-duplication probe cannot
			// see. The driver claims the feature and redelivers a
			// delivery whose lease expired, which is what every
			// at-least-once broker does.
			name:     "exactly-once delivery claimed by an at-least-once broker",
			scenario: "capability/ExactlyOnceDelivery/works",
			faults: testkit.Faults{Capabilities: testkit.ClaimFeature(
				mq.FeatureExactlyOnceDelivery,
				mq.Support{Level: mq.EmulatedVolatile, Scope: mq.ScopeTopic, Note: "in-process, inside DedupWindow"},
			)},
			wants: "was delivered a second time once its lease expired",
			// The unbroken driver does not claim the feature, so
			// the positive scenario's gate does not select it.
			control: testkit.OutcomeSkip,
		},
		{
			// The specification requires a conditional support to state its
			// condition. One that does not tells a caller nothing
			// they can act on.
			name:     "a conditional claim with no condition",
			scenario: "capability/Priority/claim",
			faults: testkit.Faults{Capabilities: testkit.ClaimFeature(
				mq.FeaturePriority, mq.Support{Level: mq.Conditional, Scope: mq.ScopeTopic},
			)},
			wants:   "requires a conditional support to state its condition",
			control: testkit.OutcomePass,
		},
		{
			// Volatile emulation never satisfies a durability
			// requirement, whatever the ordinal says. A driver that
			// advertised durable storage as a volatile emulation would
			// otherwise pass a caller's RequireAtLeast.
			name:     "a claim the requirement machinery must refuse",
			scenario: "capability/DurableStorage/claim",
			faults: testkit.Faults{Capabilities: testkit.ClaimFeature(
				mq.FeatureDurableStorage, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic},
			)},
			wants: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wants == "" {
				t.Skip("this row is the control for a claim with no behavioural probe; the pair below covers it")
			}
			broken, err := testkit.RunOne(ctx, brokenHarness(tc.faults), tc.scenario)
			if err != nil {
				t.Fatalf("RunOne: %v", err)
			}
			if broken.Outcome != testkit.OutcomeFail {
				t.Fatalf("the kit reported %q for a driver with %s; a conformance kit that passes a driver it should fail is worse than no kit at all",
					broken.Outcome, tc.name)
			}
			if !strings.Contains(broken.Note, tc.wants) {
				t.Errorf("the failure says %q, want it to mention %q", broken.Note, tc.wants)
			}
			t.Logf("%s -> %s: %s", tc.scenario, broken.Outcome, broken.Note)

			// And the same scenario passes with the defect removed,
			// so the failure is the defect's and not the fixture's.
			healthy, err := testkit.RunOne(ctx, brokenHarness(testkit.Faults{}), tc.scenario)
			if err != nil {
				t.Fatalf("RunOne: %v", err)
			}
			if healthy.Outcome != tc.control {
				t.Fatalf("the unbroken driver reported %q for %s, want %q: %s", healthy.Outcome, tc.scenario, tc.control, healthy.Note)
			}
		})
	}
}

// TestTheKitRejectsAClaimTheMachineryRefutes is the capability-table half of the
// same rule, where there is no behaviour to break: the claim itself is checked
// against the specification's own rules.
func TestTheKitRejectsAClaimTheMachineryRefutes(t *testing.T) {
	ctx := context.Background()
	// A driver advertising durable storage as a volatile emulation with no
	// note fails twice over: the specification requires an emulation to say what it is,
	// and volatile emulation never satisfies a durable requirement.
	h := brokenHarness(testkit.Faults{Capabilities: testkit.ClaimFeature(
		mq.FeatureDurableStorage, mq.Support{Level: mq.EmulatedVolatile, Scope: mq.ScopeTopic},
	)})
	res, err := testkit.RunOne(ctx, h, "capability/DurableStorage/claim")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("the kit accepted a volatile emulation with no note: %q", res.Outcome)
	}
	if !strings.Contains(res.Note, "no note saying what the emulation is") {
		t.Errorf("the failure says %q", res.Note)
	}
	t.Logf("capability/DurableStorage/claim -> %s: %s", res.Outcome, res.Note)
}

// TestSkippedScenariosDoNotProveAClaim pins the rule that makes a skip honest: a
// claim whose scenario skipped is not verified, and the generated matrix renders
// it as unsupported rather than as a claim nobody checked.
func TestSkippedScenariosDoNotProveAClaim(t *testing.T) {
	rep := &testkit.Report{
		Schema: testkit.Schema, Driver: "x", Evidence: testkit.EvidenceInProcess,
		Date: time.Now().UTC().Format(time.RFC3339),
		Results: []testkit.Result{
			{Scenario: "a", Feature: "Transactions", Claimed: testkit.LevelNative, Positive: true, Outcome: testkit.OutcomeSkip},
			{Scenario: "b", Feature: "Transactions", Claimed: testkit.LevelNative, Outcome: testkit.OutcomePass},
			{Scenario: "c", Feature: "Nack", Claimed: testkit.LevelNative, Positive: true, Outcome: testkit.OutcomePass},
			{Scenario: "d", Feature: "Nack", Claimed: testkit.LevelNative, Outcome: testkit.OutcomePass},
			{Scenario: "e", Feature: "Replay", Claimed: testkit.LevelNative, Positive: true, Outcome: testkit.OutcomePass},
			{Scenario: "f", Feature: "Replay", Claimed: testkit.LevelNative, Outcome: testkit.OutcomeFail},
		},
	}
	want := map[string]bool{"Transactions": false, "Nack": true, "Replay": false}
	for _, v := range rep.Verify() {
		if got := v.Proven(); got != want[v.Feature] {
			t.Errorf("%s: Proven is %v, want %v", v.Feature, got, want[v.Feature])
		}
	}
}

// `capability/<X>/refused` reads "the driver says it cannot do this; prove it
// refuses". That is right for a feature — a delay, a lease extension, a seek
// all have a call to refuse — and wrong for a GUARANTEE, which has none. A
// standard SQS queue does not promise order, its driver claims none, and the
// kit published thirty-two keyed messages and FAILED the driver when they
// arrived in order, which one consumer at that volume nearly always makes them
// do. OrderedByKey, OrderedByPartition and OrderedGlobally were all documented
// as inapplicable to get past it, and all three denials were true.
//
// The two rows below are the whole rule: a denial nothing contradicted is
// UNREFUTED, and a denial something contradicted is confirmed by that
// observation.
func TestADeniedGuaranteeIsUnrefutedRatherThanFailed(t *testing.T) {
	ctx := context.Background()
	withdrawn := testkit.Faults{Capabilities: testkit.WithdrawFeature(mq.FeatureOrderedByKey)}

	// The driver denies the ordering and this run kept it, which is what an
	// in-order broker and a quiet standard queue both do.
	quiet, err := testkit.RunOne(ctx, brokenHarness(withdrawn), "capability/OrderedByKey/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if quiet.Outcome != testkit.OutcomeSkip {
		t.Fatalf("a truthful denial of an ordering reported %q: %s\nthe claim is unfalsifiable in one run and the kit may not fail a driver for it",
			quiet.Outcome, quiet.Note)
	}
	if !strings.Contains(quiet.Note, "ABSENCE is not observable in one run") {
		t.Errorf("the skip says %q", quiet.Note)
	}
	if quiet.Kind != testkit.KindGuarantee {
		t.Errorf("the result's kind is %q, want %q: the report has to say which claims carry one-sided evidence", quiet.Kind, testkit.KindGuarantee)
	}
	t.Logf("unrefuted -> %s: %s", quiet.Outcome, quiet.Note)

	// The same denial, against a broker that really does reorder. Now
	// there is evidence, and the denial is confirmed by it.
	withdrawn.ReorderFetch = true
	confirmed, err := testkit.RunOne(ctx, brokenHarness(withdrawn), "capability/OrderedByKey/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if confirmed.Outcome != testkit.OutcomePass {
		t.Fatalf("a denial confirmed by an observed reordering reported %q: %s", confirmed.Outcome, confirmed.Note)
	}
	if !strings.Contains(confirmed.Note, "the denial is confirmed by observation") {
		t.Errorf("the pass says %q, and a one-sided pass has to say what it saw", confirmed.Note)
	}
	t.Logf("confirmed -> %s: %s", confirmed.Outcome, confirmed.Note)

	// And a run of the whole suite renders the two states differently:
	// Unrefuted is "one run cannot check this", which is not the same
	// unverified state as "nobody checked this".
	rep, err := testkit.Run(ctx, brokenHarness(testkit.Faults{Capabilities: testkit.WithdrawFeature(mq.FeatureOrderedByKey)}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var found bool
	for _, v := range rep.Verify() {
		if v.Feature != mq.FeatureOrderedByKey.String() {
			continue
		}
		found = true
		if !v.Guarantee || !v.Unrefuted() || v.Proven() || v.Failed {
			t.Errorf("OrderedByKey is %+v; want a guarantee that is unrefuted, unproven and not failed", v)
		}
	}
	if !found {
		t.Error("the report says nothing about OrderedByKey")
	}
}

// TestExactlyOnceDeliveryAndDeduplicationAreTwoClaims is item 3: a driver with
// publication de-duplication and at-least-once delivery has to be able to DENY
// exactly-once delivery, which one shared probe made impossible.
//
// `exercises()` mapped both features to dedupProbe — publish the same DedupKey
// twice, expect one delivery — so the probe proved the de-duplication and
// thereby refuted a denial that was true. SQS FIFO and JetStream both hit it
// and both documented the negative scenario away.
func TestExactlyOnceDeliveryAndDeduplicationAreTwoClaims(t *testing.T) {
	ctx := context.Background()
	h := brokenHarness(testkit.Faults{})

	// The memory driver is exactly the shape the item describes: it
	// de-duplicates a publication inside DedupWindow and it redelivers a
	// delivery whose lease expired.
	dedup, err := testkit.RunOne(ctx, h, "capability/Deduplication/works")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if dedup.Outcome != testkit.OutcomePass {
		t.Fatalf("capability/Deduplication/works reported %q: %s", dedup.Outcome, dedup.Note)
	}
	eod, err := testkit.RunOne(ctx, h, "capability/ExactlyOnceDelivery/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if eod.Outcome != testkit.OutcomePass {
		t.Fatalf("a driver with publication de-duplication and at-least-once delivery reported %q for its exactly-once DENIAL: %s\nthe two features need two probes",
			eod.Outcome, eod.Note)
	}
	if !strings.Contains(eod.Note, "delivered a second time") {
		t.Errorf("the denial's evidence is %q, want the observed redelivery", eod.Note)
	}
	t.Logf("Deduplication/works -> %s; ExactlyOnceDelivery/refused -> %s: %s", dedup.Outcome, eod.Outcome, eod.Note)
}

// TestRedactionCatchesADriverThatRendersItsCredential is item 7.
func TestRedactionCatchesADriverThatRendersItsCredential(t *testing.T) {
	ctx := context.Background()
	secrets := []string{selftestSecret}

	leaky := tunedHarness(testkit.Faults{LeakSecret: selftestSecret}, testkit.EvidenceInProcess, secrets, 1<<20)
	res, err := testkit.RunOne(ctx, leaky, "secrets/redaction")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("secrets/redaction reported %q for a driver that renders its credential into every error it builds: %s\na redaction check that cannot fail proves nothing",
			res.Outcome, res.Note)
	}
	if !strings.Contains(res.Note, "a credential appeared") {
		t.Errorf("the failure says %q", res.Note)
	}
	t.Logf("secrets/redaction -> %s: %s", res.Outcome, res.Note)

	// The same harness without the defect passes, so the failure is the
	// defect's and not the fixture's.
	clean := tunedHarness(testkit.Faults{}, testkit.EvidenceInProcess, secrets, 1<<20)
	ok, err := testkit.RunOne(ctx, clean, "secrets/redaction")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if ok.Outcome != testkit.OutcomePass {
		t.Fatalf("secrets/redaction reported %q for a driver that redacts: %s", ok.Outcome, ok.Note)
	}
	if !strings.Contains(ok.Note, "driver-built error") {
		t.Errorf("the pass says %q; it has to say how many DRIVER-built errors it actually reached, or a run where none was reached looks the same", ok.Note)
	}

	// And a harness that declares a credential no error can carry is a
	// skip rather than a pass: the scenario says so when nothing it
	// probed produced a driver error at all.
	t.Logf("redacting driver -> %s: %s", ok.Outcome, ok.Note)
}

// TestLimitsAdvertisedCatchesABrokenLimitLookup is item 6, and the hand-off.
//
// the specification's `message.max.bytes` lookup read a DescribeConfigs resource that kfake
// answers generously and Apache Kafka answers with the dynamic cluster-wide
// configuration only. The real broker said nothing, the driver advertised no
// MaxMessageBytes for four months, and nothing noticed: the specification makes "unknown" a
// legitimate answer, so `publish/oversized-payload` SKIPS, and a skip is
// indistinguishable from a pass.
func TestLimitsAdvertisedCatchesABrokenLimitLookup(t *testing.T) {
	ctx := context.Background()

	// A driver whose limit lookup returned nothing, against a real broker.
	blind := tunedHarness(testkit.Faults{Capabilities: testkit.WithdrawLimits()}, testkit.EvidenceContainer, nil, 1<<20)
	res, err := testkit.RunOne(ctx, blind, "limits/advertised")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("limits/advertised reported %q for a driver that advertises no limit at all against container evidence: %s",
			res.Outcome, res.Note)
	}
	if !strings.Contains(res.Note, "advertises none of the 7 limits") {
		t.Errorf("the failure says %q", res.Note)
	}
	t.Logf("limits/advertised -> %s: %s", res.Outcome, res.Note)

	// The same driver with its lookup working passes, and says what it saw:
	// a pass that recorded nothing is indistinguishable from a pass that
	// checked nothing, which is the whole defect.
	seeing := tunedHarness(testkit.Faults{}, testkit.EvidenceContainer, nil, 1<<20)
	ok, err := testkit.RunOne(ctx, seeing, "limits/advertised")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if ok.Outcome != testkit.OutcomePass {
		t.Fatalf("limits/advertised reported %q for a driver advertising a maximum message size: %s", ok.Outcome, ok.Note)
	}
	if !strings.Contains(ok.Note, "MaxMessageBytes=1048576") {
		t.Errorf("the pass says %q, and it has to name the limits it saw", ok.Note)
	}
	t.Logf("limits/advertised -> %s: %s", ok.Outcome, ok.Note)

	// The same emptiness against an emulator or in process is a skip, not
	// a failure: the specification makes "unknown" legitimate and the specification's own alternatives
	// entry rejected punishing a broker that genuinely bounds nothing.
	emulated := tunedHarness(testkit.Faults{Capabilities: testkit.WithdrawLimits()}, testkit.EvidenceEmulator, nil, 1<<20)
	soft, err := testkit.RunOne(ctx, emulated, "limits/advertised")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if soft.Outcome != testkit.OutcomeSkip {
		t.Errorf("limits/advertised reported %q against emulator evidence: %s", soft.Outcome, soft.Note)
	}

	// And the report carries the numbers, so that a reviewer can see a
	// lookup that went quiet without re-running anything.
	rep, err := testkit.Run(ctx, seeing)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := rep.Limits["MaxMessageBytes"]; got != 1<<20 {
		t.Errorf("the report's limits are %v, want MaxMessageBytes to be there", rep.Limits)
	}
}

// TestFidelityNamesThePublisherThatRefused is item 5: a publisher-side refusal
// has to be reported as one, and at once.
//
// The scenario publishes in a goroutine and collects in the caller, and the
// publisher's error used to sit in a local until after wg.Wait() — which runs
// after collectAck has spent the WHOLE scenario timeout waiting for messages
// the publisher gave up on. The failure was correct and useful and arrived
// twenty seconds late under the name `publish/fidelity`, so the first thing a
// reader concluded was that byte fidelity had broken.
func TestFidelityNamesThePublisherThatRefused(t *testing.T) {
	var mu sync.Mutex
	sent := 0
	refuse := errors.New("the subscription queue is full")
	faults := testkit.Faults{Publish: func(*mq.Message) error {
		mu.Lock()
		defer mu.Unlock()
		sent++
		if sent > 8 {
			return refuse
		}
		return nil
	}}
	h := brokenHarness(faults)
	res, err := testkit.RunOne(context.Background(), h, "publish/fidelity")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("publish/fidelity reported %q for a broker that refused the ninth publication: %s", res.Outcome, res.Note)
	}
	if !strings.Contains(res.Note, "the PUBLISHER refused message 8 of 64") {
		t.Errorf("the failure says %q; it has to name the publisher, or the first reading is that byte fidelity broke", res.Note)
	}
	if !strings.Contains(res.Note, refuse.Error()) {
		t.Errorf("the failure does not carry the broker's own reason: %q", res.Note)
	}
	// The timing is the half that matters. The harness bounds a scenario at
	// ten seconds and the collector is released by the publisher's error
	// path, so this is milliseconds rather than the whole budget.
	if res.Duration > 5*time.Second {
		t.Errorf("the failure took %v of the scenario's %v budget: the collector was not released when the publisher gave up",
			res.Duration, 10*time.Second)
	}
	t.Logf("publish/fidelity -> %s in %v: %s", res.Outcome, res.Duration, res.Note)
}

// TestTheFidelityScenariosDoNotAssumeAnOrderTheyDoNotClaim is the check for
// the last of the order-correlating scenarios.
//
// Each case is its own round trip now, and a broker that reverses every batch
// it hands out has to pass anyway.
func TestTheFidelityScenariosDoNotAssumeAnOrderTheyDoNotClaim(t *testing.T) {
	ctx := context.Background()
	reordering := brokenHarness(testkit.Faults{ReorderFetch: true})
	for _, name := range []string{"publish/roundtrip-bodies", "publish/key-roundtrip"} {
		res, err := testkit.RunOne(ctx, reordering, name)
		if err != nil {
			t.Fatalf("RunOne(%s): %v", name, err)
		}
		if res.Outcome != testkit.OutcomePass {
			t.Errorf("%s reported %q against a broker that reverses every batch: %s\nthis scenario is about the fidelity of one message and claims nothing about order",
				name, res.Outcome, res.Note)
			continue
		}
		t.Logf("%s -> %s", name, res.Outcome)
	}
	// And they still fail a broker that really loses the payload, so the
	// change did not make them unfalsifiable: a body that comes back
	// changed is caught whichever order it arrives in.
	res, err := testkit.RunOne(ctx, brokenHarness(testkit.Faults{}), "publish/roundtrip-bodies")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomePass {
		t.Fatalf("publish/roundtrip-bodies reported %q for an unbroken driver: %s", res.Outcome, res.Note)
	}
}

// TestResultKeepsTheFeatureWhenTheScenarioNeverGotAClient pins the repair for
// the two paths that return before the kit has a client: a scenario the driver
// documents as inapplicable, and a topology the driver refuses. Both used to
// return a Result with no Feature at all, so 70 results of the committed
// conformance corpus are filed against no capability and their reason -- the
// only record of WHY that claim is unverified -- was dropped on the floor.
func TestResultKeepsTheFeatureWhenTheScenarioNeverGotAClient(t *testing.T) {
	const scenario = "capability/Priority/works"
	t.Run("documented skip", func(t *testing.T) {
		h := brokenHarness(testkit.Faults{})
		h.Skip = map[string]string{scenario: "this broker has no priority ceiling"}
		res, err := testkit.RunOne(context.Background(), h, scenario)
		if err != nil {
			t.Fatal(err)
		}
		if res.Outcome != testkit.OutcomeSkip {
			t.Fatalf("outcome %q, want skip", res.Outcome)
		}
		if res.Feature != "Priority" {
			t.Errorf("feature %q, want Priority: the skip's reason %q is then filed against no claim", res.Feature, res.Note)
		}
	})
	t.Run("topology refused", func(t *testing.T) {
		h := brokenHarness(testkit.Faults{})
		open := h.Open
		h.Open = func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if strings.Contains(n.Topic, "capability-Priority-works") {
				return nil, mq.ErrUnsupported
			}
			return open(ctx, n)
		}
		res, err := testkit.RunOne(context.Background(), h, scenario)
		if err != nil {
			t.Fatal(err)
		}
		if res.Outcome != testkit.OutcomeSkip {
			t.Fatalf("outcome %q (%s), want skip", res.Outcome, res.Note)
		}
		if res.Feature != "Priority" {
			t.Errorf("feature %q, want Priority: the refusal %q is then filed against no claim", res.Feature, res.Note)
		}
	})
}

// TestARefusalFromSomewhereElseIsNotEvidence is the false-PASS half of the
// defect this increment closes.
//
// `capability/<F>/refused` reads "the driver says it cannot do this; prove it
// refuses", and it used to read the WHOLE probe's error: any ErrUnsupported
// from any step counted as the refusal. A probe is five or six portable calls
// — open a consumer, publish, receive, settle — and only one of them is the
// feature's. A driver that cannot open a consumer at all therefore had every
// one of its negative claims recorded as "refuted by observation" without the
// feature's own call ever being made, which is a claim nobody checked
// rendering as the strongest thing a negative claim can be.
func TestARefusalFromSomewhereElseIsNotEvidence(t *testing.T) {
	ctx := context.Background()
	h := brokenHarness(testkit.Faults{
		Capabilities:     testkit.WithdrawFeature(mq.FeatureDelayedRedelivery),
		RefuseOpenSource: mq.ErrUnsupported,
	})
	res, err := testkit.RunOne(ctx, h, "capability/DelayedRedelivery/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome == testkit.OutcomePass {
		t.Fatalf("a refusal from OPENING A CONSUMER was read as proof that the driver refuses a delayed nack: %s\nthe feature's own call was never made, so nothing about the claim was observed",
			res.Note)
	}
	if res.Outcome != testkit.OutcomeSkip {
		t.Fatalf("outcome %q, want a skip: %s", res.Outcome, res.Note)
	}
	if !strings.Contains(res.Note, "did not reach the call") {
		t.Errorf("the skip says %q, and it has to say which call was never reached", res.Note)
	}
	t.Logf("unattributed refusal -> %s: %s", res.Outcome, res.Note)
}

// TestADriverThatAnswersSomethingOtherThanARefusalFails is the FAIL this
// increment adds, and the one the recorded defect is named for.
//
// The specification gives a driver that does not support a call exactly one answer:
// ErrUnsupported (or, for a settlement on a source with no settler, the specification's
// ErrNotSettleable). A driver that answers the feature's own call with an
// internal failure has done something OTHER than refuse, and the kit now says
// so against the call it made rather than reporting a generic "want
// ErrUnsupported" about whatever error surfaced last.
func TestADriverThatAnswersSomethingOtherThanARefusalFails(t *testing.T) {
	ctx := context.Background()
	h := brokenHarness(testkit.Faults{
		Capabilities: testkit.WithdrawFeature(mq.FeatureDelayedRedelivery),
		NackError: func(o mq.NackOptions) error {
			if o.Delay <= 0 {
				return nil
			}
			return &mq.OpError{
				Operation: "nack", Code: mq.CodeUnavailable,
				Err: errors.New("the broker lost its lease table"),
			}
		},
	})
	res, err := testkit.RunOne(ctx, h, "capability/DelayedRedelivery/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("a driver that answered the delayed nack with an unrelated failure reported %q: %s\nthat is neither the feature working nor a refusal, and the specification allows only the second",
			res.Outcome, res.Note)
	}
	if !strings.Contains(res.Note, "neither the feature working nor a refusal") {
		t.Errorf("the failure says %q, and it has to distinguish this from a refusal", res.Note)
	}
	if !strings.Contains(res.Note, "Delivery.Nack") {
		t.Errorf("the failure says %q, and it has to name the call the driver answered", res.Note)
	}
	t.Logf("wrong-answer -> %s: %s", res.Outcome, res.Note)
}

// TestADivergingDriverIsUnrefutedRatherThanFailed is the false-FAIL half, and
// it is the one that cost three scenarios in drivers/sqs.
//
// `capability/FanOut/refused` opens two subscription names and reads the
// refusal. A queue broker accepts both names, refuses nothing, and delivers
// the message to whichever reader asked first: the two names are ONE
// subscription and the readers compete. The probe then failed a driver whose
// denial of fan-out was true, on "no delivery arrived", and
// `capability/FanOut/refused`, `capability/Replay/refused` and three ordering
// scenarios were documented away to get past it.
//
// The honest answer is the one a guarantee already gets: the driver has no
// call to refuse here, so one run cannot establish the absence, and the claim
// is UNREFUTED rather than refuted or false.
func TestADivergingDriverIsUnrefutedRatherThanFailed(t *testing.T) {
	ctx := context.Background()
	queue := testkit.Faults{ShareSubscriptions: true}

	t.Run("a true denial is not failed", func(t *testing.T) {
		f := queue
		f.Capabilities = testkit.WithdrawFeature(mq.FeatureFanOut)
		res, err := testkit.RunOne(ctx, brokenHarness(f), "capability/FanOut/refused")
		if err != nil {
			t.Fatalf("RunOne: %v", err)
		}
		if res.Outcome == testkit.OutcomeFail {
			t.Fatalf("a queue broker's TRUE denial of fan-out was failed: %s\nthe driver refused nothing and delivered to one of the two readers, which is what denying fan-out looks like",
				res.Note)
		}
		if res.Outcome == testkit.OutcomePass {
			t.Fatalf("a driver that did something DIFFERENT rather than refusing passed the refusal probe: %s", res.Note)
		}
		if res.Outcome != testkit.OutcomeSkip {
			t.Fatalf("outcome %q: %s", res.Outcome, res.Note)
		}
		if res.Kind != testkit.KindGuarantee {
			t.Errorf("the result's kind is %q, want %q: a claim one run cannot settle is one-sided evidence and the report has to say so, or a reader cannot tell it from a claim nobody probed",
				res.Kind, testkit.KindGuarantee)
		}
		if !strings.Contains(res.Note, "COMPETE") {
			t.Errorf("the skip says %q, and it has to say what the driver did instead of refusing", res.Note)
		}
		t.Logf("diverged -> %s (kind %q): %s", res.Outcome, res.Kind, res.Note)
	})

	t.Run("the same divergence fails a driver that claims it", func(t *testing.T) {
		// The memory driver DOES claim fan-out, so the positive
		// direction runs, and the same observation is a failure there:
		// a driver that advertises fan-out and competes has broken its
		// claim. The two readings of one observation are the whole
		// point of naming it rather than returning a bare error.
		res, err := testkit.RunOne(ctx, brokenHarness(queue), "capability/FanOut/works")
		if err != nil {
			t.Fatalf("RunOne: %v", err)
		}
		if res.Outcome != testkit.OutcomeFail {
			t.Fatalf("a driver advertising fan-out whose two subscriptions compete reported %q: %s", res.Outcome, res.Note)
		}
		if !strings.Contains(res.Note, "did something else instead") {
			t.Errorf("the failure says %q, and it has to name what happened instead of the feature", res.Note)
		}
		t.Logf("claimed and diverged -> %s: %s", res.Outcome, res.Note)
	})

	t.Run("replay is the same shape", func(t *testing.T) {
		f := queue
		f.Capabilities = testkit.WithdrawFeature(mq.FeatureReplay)
		res, err := testkit.RunOne(ctx, brokenHarness(f), "capability/Replay/refused")
		if err != nil {
			t.Fatalf("RunOne: %v", err)
		}
		if res.Outcome != testkit.OutcomeSkip || res.Kind != testkit.KindGuarantee {
			t.Fatalf("a settled message a second reader cannot find reported %q (kind %q): %s",
				res.Outcome, res.Kind, res.Note)
		}
		t.Logf("replay diverged -> %s (kind %q): %s", res.Outcome, res.Kind, res.Note)
	})
}

// TestADivergenceRendersAsUnrefutedRatherThanRefuted is the report half: the
// promoted kind has to reach the artefacts, or the distinction exists only in
// a note nobody parses.
//
// conformance/report's State() already partitions the space -- refuted,
// unrefuted, unverified, advertised, verified, failed -- and derives
// "unrefuted" from the result's kind. A divergence that skipped without
// promoting the kind would render as `unverified`, which says "nobody checked
// this" about a run that checked it and found the driver has nothing to
// refuse.
func TestADivergenceRendersAsUnrefutedRatherThanRefuted(t *testing.T) {
	ctx := context.Background()
	f := testkit.Faults{ShareSubscriptions: true, Capabilities: testkit.WithdrawFeature(mq.FeatureFanOut)}
	res, err := testkit.RunOne(ctx, brokenHarness(f), "capability/FanOut/refused")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	rep := &testkit.Report{
		Schema: testkit.Schema, Driver: "queue-broker", Evidence: testkit.EvidenceInProcess,
		Date: time.Now().UTC().Format(time.RFC3339), Results: []testkit.Result{res},
	}
	if err := rep.Validate(); err != nil {
		t.Fatalf("the report carrying a diverged result is not a valid report: %v", err)
	}
	var found bool
	for _, v := range rep.Verify() {
		if v.Feature != mq.FeatureFanOut.String() {
			continue
		}
		found = true
		if !v.Unrefuted() {
			t.Errorf("FanOut is %+v, want unrefuted: a denial a run found nothing to refuse is not the same state as one nobody probed", v)
		}
		if v.Proven() || v.Failed {
			t.Errorf("FanOut is %+v, want neither proven nor failed", v)
		}
	}
	if !found {
		t.Error("the report says nothing about FanOut")
	}
}

// TestAFailingScenarioOnAHealthyDriverAddsNoHealthNote is the end-to-end half
// of the health note a failing scenario now carries.
//
// A failing scenario appends the driver's health so that a deadline failure
// carries its cause: `conformance (kafka)` was red for a week with nothing but
// "no delivery arrived: context deadline exceeded", while the reason -- every
// non-fatal fetch error, `kgo.ErrDataLoss` included, going to
// `health.Degraded` and the fetch returning nil -- was recorded and dropped.
//
// What this pins is the quiet half, which is the half that would otherwise
// break every other failure in the suite: the memory driver stays Ready while
// an injected fault fails the scenario, and the note must say nothing about
// health. Appending "driver health is ready" to a wrong-body failure would be
// noise on every one of the suite's 157 scenarios.
//
// The Degraded branch is covered at the unit level by TestHealthNote, because
// degrading a driver's health from here would need a new `Faults` field and the
// driver plumbing to honour it -- more surface than this note is worth.
func TestAFailingScenarioOnAHealthyDriverAddsNoHealthNote(t *testing.T) {
	res, err := testkit.RunOne(context.Background(),
		brokenHarness(testkit.Faults{HideExtender: true}), "capability/ExtendLease/works")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomeFail {
		t.Fatalf("the fixture did not fail the scenario, so this proves nothing: %s (%s)", res.Outcome, res.Note)
	}
	if strings.Contains(res.Note, "driver health") {
		t.Errorf("a failure on a Ready driver gained a health note, which would be noise on every scenario in the suite: %q", res.Note)
	}
}
