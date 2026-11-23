package protocol

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/testguard"
)

// TestProtocolKindsList records the family's kinds in the test log, which is
// what the bake-off's own lint reads to check that every kind it measures
// exists.
//
// It is named for the family rather than `TestKindsList` because
// names it in KINDS_TEST: `internal/activemq` is one
// package tree and the protocol family is one of the things in it, so the
// generic `./internal/...` walk is not what should find these kinds.
func TestProtocolKindsList(t *testing.T) {
	parts := make([]string, 0, kindCount)
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
	if len(parts) != int(kindCount)-1 {
		t.Fatalf("Kinds() listed %d of %d concrete kinds", len(parts), kindCount-1)
	}
}

// TestKindsList is the generic entry point `bakeoff lint`'s walk over
// `./internal/...` looks for, and it exists as well as [TestProtocolKindsList]
// rather than instead of it.
//
// The two do different jobs and both are wanted. names
// KINDS_TEST=TestProtocolKindsList, which is what the campaign and the
// declaration check run; the walk, though, runs `-run '^TestKindsList$'` and
// SKIPS a package that has none — and with it the two checks that only the
// walk performs: that every declared kind has a `TestModel/<slug>` subtest,
// and that every declared kind appears in a benchmark name. The specification's
// lesson was that a check which does not run is worth nothing (see
// the contributor notes, "Variant-family package shape"), so this package answers
// to both names.
func TestKindsList(t *testing.T) { TestProtocolKindsList(t) }

// TestAutoBindingMatchesADR checks that when an ADR binds this family
// (front-matter `auto: {activemqprotocol: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../../..", "activemqprotocol")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds activemqprotocol yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds activemqprotocol to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestKindNames holds the slug and identifier tables to the enumeration.
func TestKindNames(t *testing.T) {
	seen := map[string]Kind{}
	for k := Auto; k < kindCount; k++ {
		slug, ident := k.String(), k.Ident()
		if slug == "" || ident == "" {
			t.Fatalf("kind %d has slug %q and identifier %q", k, slug, ident)
		}
		if prev, dup := seen[slug]; dup {
			t.Fatalf("kinds %v and %v share the slug %q", prev, k, slug)
		}
		seen[slug] = k
		got, ok := ParseKind(slug)
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) = (%v, %v), want (%v, true)", slug, got, ok, k)
		}
	}
	if _, ok := ParseKind("nonesuch"); ok {
		t.Fatal("ParseKind accepted a slug no kind has")
	}
	if k := kindCount; k.Valid() || k.String() != "kind(?)" || k.Ident() != "" {
		t.Fatalf("a kind outside the enumeration reports valid=%v slug=%q ident=%q", k.Valid(), k.String(), k.Ident())
	}
	if _, err := New(kindCount); err == nil {
		t.Fatal("New accepted a kind outside the enumeration")
	}
	if got := Slugs(); len(got) != int(kindCount)-1 {
		t.Fatalf("Slugs() listed %v", got)
	}
	// The value enumerations have the same shape, because a benchmark name, a
	// scorecard and a failure message all read them.
	if v := valueKindCount; v.Valid() || v.String() != "ValueKind(?)" {
		t.Errorf("a value kind outside the enumeration reports valid=%v name=%q", v.Valid(), v.String())
	}
	if s := delaySlotCount; s.Valid() || s.String() != "DelaySlot(?)" {
		t.Errorf("a delay slot outside the enumeration reports valid=%v name=%q", s.Valid(), s.String())
	}
	for v := Text; v < valueKindCount; v++ {
		if v.String() == "" || !v.Valid() {
			t.Errorf("value kind %d has name %q valid=%v", v, v.String(), v.Valid())
		}
	}
}

// TestFeatureCoverageDiffers is "feature coverage score"
// criterion as a check rather than as prose.
//
// The whole point is that the four combinations do NOT offer the
// same thing, and two tables written in one package are one careless edit away
// from being identical. This is what would fail if that edit were made: the
// rows that must differ are named individually, with the measurement that
// makes each of them differ, so a change to any of them is deliberate.
func TestFeatureCoverageDiffers(t *testing.T) {
	amqp, err := New(AMQP10)
	if err != nil {
		t.Fatal(err)
	}
	st, err := New(STOMP)
	if err != nil {
		t.Fatal(err)
	}

	// (1) The four counts are four different numbers. If any two coincided,
	// the row's criterion would have fewer terms than the row states, and
	// the scorecard's `features` metric could not separate them.
	counts := map[string]int{}
	for _, p := range []Protocol{amqp, st} {
		for _, b := range activemq.Brokers() {
			key := p.Kind().String() + "/" + b.String()
			counts[key] = p.Features(b)
		}
	}
	t.Logf("feature coverage: %v", counts)
	seen := map[int]string{}
	for key, n := range counts {
		if prev, dup := seen[n]; dup {
			t.Errorf("%s and %s both score %d natively: coverage score is read on Artemis AND Classic for BOTH protocols, and four combinations that are not four numbers cannot answer it", prev, key, n)
		}
		seen[n] = key
	}
	// And the order is the one the row's expected outcome asserts: AMQP 1.0
	// is more complete than STOMP on Artemis, which is what "feature
	// completeness on Artemis, then perf -> AMQP 1.0 default" decides on.
	if amqp.Features(activemq.Artemis) <= st.Features(activemq.Artemis) {
		t.Errorf("on Artemis the AMQP 1.0 arm scores %d and the STOMP arm %d: the specification is decided on feature completeness on Artemis FIRST, so a default chosen on perf would contradict the row",
			amqp.Features(activemq.Artemis), st.Features(activemq.Artemis))
	}

	// (2) The rows that must differ between the two PROTOCOLS, on both
	// brokers, each with the measurement that makes it differ.
	for _, tc := range []struct {
		feature mq.Feature
		why     string
		want    [2]mq.Level // amqp10, stomp
	}{
		{
			mq.FeatureNack,
			"a `modified{delivery-failed}` disposition returns the delivery on both brokers; a STOMP NACK frame returns nothing on either and is treated as an ACK on Artemis",
			[2]mq.Level{mq.Native, mq.Unsupported},
		},
		{
			mq.FeatureBinaryHeaders,
			"AMQP 1.0 application properties are typed; a STOMP frame header is text",
			[2]mq.Level{mq.Native, mq.Unsupported},
		},
		{
			mq.FeatureOrderedByKey,
			"the exclusive assignment of a group to one consumer was measured over AMQP 1.0; over STOMP Artemis echoes the group as _AMQ_GROUP_ID and Classic echoes nothing",
			[2]mq.Level{mq.Native, mq.Conditional},
		},
		{
			mq.FeatureBatchReceive,
			"an AMQP 1.0 receive window is the link's credit; STOMP's is a broker-specific SUBSCRIBE header",
			[2]mq.Level{mq.Native, mq.Conditional},
		},
	} {
		for _, b := range activemq.Brokers() {
			got := [2]mq.Level{amqp.Support(b, tc.feature).Level, st.Support(b, tc.feature).Level}
			if got != tc.want {
				t.Errorf("%v on %v is (amqp10=%v, stomp=%v), want (%v, %v): %s",
					tc.feature, b, got[0], got[1], tc.want[0], tc.want[1], tc.why)
			}
		}
	}

	// (3) The one row the STOMP arm WINS, and it wins it on one broker only.
	// It is the reason ends "STOMP kept for Classic", so a
	// change that equalised it would remove the family's whole second half.
	if lvl := st.Support(activemq.Classic, mq.FeatureServerFilter).Level; lvl != mq.Native {
		t.Errorf("the STOMP arm advertises FeatureServerFilter on Classic as %v, want native: measured, a `colour = 'red'` selector kept a blue message out of the subscription, and it is the only row this arm wins", lvl)
	}
	if lvl := st.Support(activemq.Artemis, mq.FeatureServerFilter).Level; lvl != mq.Unsupported {
		t.Errorf("the STOMP arm advertises FeatureServerFilter on Artemis as %v, want unsupported: measured, the selector is IGNORED under a fully qualified queue name subscription, which is the only shape this driver uses there", lvl)
	}
	if lvl := amqp.Support(activemq.Classic, mq.FeatureServerFilter).Level; lvl != mq.Unsupported {
		t.Errorf("the AMQP 1.0 arm advertises FeatureServerFilter as %v, want unsupported: a subscription filter is a link source filter that go-amqp does not expose", lvl)
	}

	// (4) The rows that must differ between the two BROKERS, over both
	// protocols. This is the half a table written per protocol would miss.
	for _, tc := range []struct {
		feature mq.Feature
		why     string
		want    [2]mq.Level // artemis, classic
	}{
		{
			mq.FeatureDeduplication,
			"Artemis reads _AMQ_DUPL_ID and Classic has no broker-side de-duplication at all",
			[2]mq.Level{mq.Native, mq.Unsupported},
		},
		{
			mq.FeatureFanOut,
			"Artemis fans out through a multicast address; Classic's fan-out is the virtual-topic convention and copies only into the consumer queues that already exist",
			[2]mq.Level{mq.Native, mq.Conditional},
		},
		{
			mq.FeaturePriority,
			"Artemis honours a message's priority by default; Classic reorders only when its policy entry sets prioritizedMessages",
			[2]mq.Level{mq.Native, mq.Conditional},
		},
		{
			mq.FeatureAdmin,
			"Artemis has the activemq.management address; Classic has no management destination either protocol can address",
			[2]mq.Level{mq.Conditional, mq.Unsupported},
		},
	} {
		for _, p := range []Protocol{amqp, st} {
			got := [2]mq.Level{
				p.Support(activemq.Artemis, tc.feature).Level,
				p.Support(activemq.Classic, tc.feature).Level,
			}
			if got != tc.want {
				t.Errorf("%v over %v is (artemis=%v, classic=%v), want (%v, %v): %s",
					tc.feature, p.Kind(), got[0], got[1], tc.want[0], tc.want[1], tc.why)
			}
		}
	}

	// (5) And the rows that are the same everywhere really are, because that
	// is the other way a table drifts: a row given a per-combination answer
	// it has no evidence for.
	for _, f := range absentFromAllFour {
		for _, p := range []Protocol{amqp, st} {
			for _, b := range activemq.Brokers() {
				if s := p.Support(b, f); s.Level != mq.Unsupported || s.Note != "" || s.Scope != mq.ScopeNone {
					t.Errorf("%v is declared absent from all four combinations and %v/%v answers %+v; the reason belongs in absentFromAllFour's doc comment, not in a per-row note", f, p.Kind(), b, s)
				}
			}
		}
	}
}

// TestEveryFeatureIsConsidered is what keeps a new feature from being silently
// unsupported on all four combinations.
//
// A feature nobody considered reads as Unsupported, which is the safe answer
// and not the informative one, and the evidence rule is about what a driver
// CLAIMS rather than about what it forgot. So each arm's switch ends in a
// default that says so, and this is what fails on it: adding a feature to the
// inventory fails here rather than shipping as an absence in four tables.
func TestEveryFeatureIsConsidered(t *testing.T) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range activemq.Brokers() {
			for f := range mq.FeatureCount {
				if s := p.Support(b, mq.Feature(f)); s.Note == notConsidered {
					t.Errorf("%v/%v: %v reached the table's default: answer it, or add it to absentFromAllFour with the reason", k, b, mq.Feature(f))
				}
			}
		}
	}
}

// TestSupportTableCoversTheInventory holds the whole table to a plausible
// shape, in both directions.
//
// More than two thirds unsupported would mean the table had stopped being
// filled in; fewer than a handful would mean it had started claiming things.
// Both are implausible for these brokers, and the bound is here so that a
// table nobody maintained is noticed.
func TestSupportTableCoversTheInventory(t *testing.T) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range activemq.Brokers() {
			table := SupportTable(p, b)
			if len(table) != mq.FeatureCount {
				t.Fatalf("%v/%v: the table has %d rows and the inventory has %d", k, b, len(table), mq.FeatureCount)
			}
			absent := 0
			for _, s := range table {
				if s.Level == mq.Unsupported {
					absent++
				}
			}
			if absent > 2*mq.FeatureCount/3 {
				t.Errorf("%v/%v reports %d of %d features unsupported: the table looks unfilled", k, b, absent, mq.FeatureCount)
			}
			if absent < 3 {
				t.Errorf("%v/%v reports only %d features unsupported: neither broker has replay, global ordering or exactly-once delivery, so a table that claims nearly everything is wrong", k, b, absent)
			}
		}
	}
}

// TestAutoIsTheMoreCompleteProtocolOnArtemis is the family's decision written
// as a check.
//
// expected outcome is "feature completeness on Artemis, then
// perf -> AMQP 1.0 default; STOMP kept for Classic". Auto is what a caller
// gets by NOT choosing, so it has to be the arm the criterion's first term
// names — and a campaign that measured the STOMP mapping faster must therefore
// not move AutoKind. This says so, in the criterion's own order.
func TestAutoIsTheMoreCompleteProtocolOnArtemis(t *testing.T) {
	p, err := New(Auto)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != AMQP10 {
		t.Fatalf("the Auto protocol is %v: the default a caller gets by not choosing is decided by FIRST criterion term, which is feature completeness on Artemis", p.Kind())
	}
	other, err := New(STOMP)
	if err != nil {
		t.Fatal(err)
	}
	if p.Features(activemq.Artemis) <= other.Features(activemq.Artemis) {
		t.Fatalf("the default protocol covers %d features on Artemis and the other covers %d: the default is supposed to be the MORE complete one there",
			p.Features(activemq.Artemis), other.Features(activemq.Artemis))
	}
	// Both arms ship, which is what the specification asks for: "protocol choice
	// explicit". So the STOMP arm must still be constructible and must still
	// be the arm that has the selector on Classic, or "STOMP kept for
	// Classic" would be a sentence with nothing behind it.
	if other.Support(activemq.Classic, mq.FeatureServerFilter).Level != mq.Native {
		t.Error("the non-default arm no longer has the row it is kept for")
	}
}

// TestDelaySlotKeys pins the four measured slots' keys, because getting one
// wrong is not an error a deployment reports.
//
// A publication that names the wrong key, or the right key in the wrong
// section, is DELIVERED IMMEDIATELY: measured, an `AMQ_SCHEDULED_DELAY`
// annotation on Artemis and an `x-opt-delivery-delay` annotation on Classic
// both went out at once. Nothing in a log says so, so the constants are
// checked here.
func TestDelaySlotKeys(t *testing.T) {
	for _, tc := range []struct {
		slot          DelaySlot
		wantD, wantAt string
		name          string
	}{
		{NoDelay, "", "", "none"},
		{DelayAnnotation, "x-opt-delivery-delay", "x-opt-delivery-time", "annotation"},
		{DelayProperty, "AMQ_SCHEDULED_DELAY", "AMQ_SCHEDULED_TIME", "property"},
		{DelayHeader, "AMQ_SCHEDULED_DELAY", "", "header"},
	} {
		d, at := tc.slot.Keys()
		if d != tc.wantD || at != tc.wantAt {
			t.Errorf("%v.Keys() = (%q, %q), want (%q, %q)", tc.slot, d, at, tc.wantD, tc.wantAt)
		}
		if tc.slot.String() != tc.name {
			t.Errorf("%d.String() = %q, want %q", tc.slot, tc.slot.String(), tc.name)
		}
	}
	// STOMP has NO absolute form, and the empty second key is what says so.
	// A slot that grew one would let a driver send a header neither broker
	// reads over that protocol.
	if _, at := DelayHeader.Keys(); at != "" {
		t.Errorf("the STOMP slot names the absolute key %q; STOMP has no absolute scheduled-delivery header", at)
	}
}

// TestReservedFrameHeader pins the list that both refuses a caller's header
// and filters a broker's.
//
// The measured ones are the point. A Classic STOMP delivery was seen carrying
// `destination`, `message-id`, `subscription`, `ack`, `timestamp`, `expires`,
// `priority`, `persistent`, `redelivered`, `content-length` and
// `scheduledJobId`; Artemis echoes `_AMQ_GROUP_ID` and `_AMQ_SCHED_DELIVERY`.
// A name that fell off this list would reach an application as a header nobody
// published, and a caller's header of the same name would be shadowed.
func TestReservedFrameHeader(t *testing.T) {
	for _, key := range []string{
		"destination", "content-type", "content-length", "receipt", "receipt-id",
		"message-id", "subscription", "ack", "redelivered", "priority",
		"expires", "persistent", "timestamp", "reply-to", "correlation-id",
		PropertyGroupID, PropertyScheduledDelay, PropertyScheduledTime,
		PropertyClassicScheduledJob, PropertyDuplicateID, PropertyArtemisGroupID,
		PropertyArtemisScheduled, "_AMQ_SOMETHING_A_FUTURE_RELEASE_ADDS", "selector",
		"JMS_AMQP_NATIVE",
	} {
		if !ReservedFrameHeader(key) {
			t.Errorf("ReservedFrameHeader(%q) = false: it would be shadowed on publication and handed back as the application's on delivery", key)
		}
	}
	// And an ordinary header is not reserved, or every publication would be
	// refused.
	for _, key := range []string{"trace", "tenant", "x-request-id", "amq-ish", "AMQP", "scheduled", ""} {
		if ReservedFrameHeader(key) {
			t.Errorf("ReservedFrameHeader(%q) = true: an application's own header is not the protocol's", key)
		}
	}
	// And the part that clashes on BOTH arms is the message properties a
	// broker acts on over either protocol, not STOMP's frame headers. The
	// distinction is what keeps the AMQP 1.0 arm from refusing a header it can
	// carry: `destination` is a frame header and nobody's property, and over
	// AMQP 1.0 `JMSXGroupID` and Artemis's other `_AMQ_` names did nothing,
	// while `_AMQ_DUPL_ID` really is a de-duplication key on Artemis.
	for _, key := range []string{
		PropertyScheduledDelay, PropertyScheduledTime, "AMQ_SCHEDULED_REPEAT",
		PropertyClassicScheduledJob, PropertyDuplicateID, PropertyJMSExpiration,
	} {
		if !ReservedProperty(key) {
			t.Errorf("ReservedProperty(%q) = false: the broker acts on it whichever protocol carried it", key)
		}
	}
	for _, key := range []string{
		"destination", "content-type", "content-length", "receipt", "receipt-id",
		"message-id", "subscription", "ack", "redelivered", "priority",
		"expires", "persistent", "timestamp", "reply-to", "correlation-id",
		"selector", "destination-type", "transaction", "trace", "",
		PropertyGroupID, PropertyArtemisGroupID, PropertyArtemisScheduled, "JMS_AMQP_NATIVE", "JMSType",
	} {
		if ReservedProperty(key) {
			t.Errorf("ReservedProperty(%q) = true: only the STOMP arm reserves it, so the AMQP 1.0 arm would refuse a header it can carry", key)
		}
	}
	// And Artemis's annotations are the AMQP 1.0 arm's own half: over STOMP
	// they are nobody's.
	for _, key := range []string{AnnotationDeliveryDelay, AnnotationDeliveryTime, "x-opt-jms-dest", "x-opt-jms-msg-type"} {
		if !ReservedAnnotation(key) || ReservedFrameHeader(key) {
			t.Errorf("ReservedAnnotation(%q) = %v and ReservedFrameHeader = %v, want true and false", key,
				ReservedAnnotation(key), ReservedFrameHeader(key))
		}
	}
	for _, key := range []string{"x-opt-something", "x-opt-jms", "x-opt-", "trace", ""} {
		if ReservedAnnotation(key) {
			t.Errorf("ReservedAnnotation(%q) = true: it is the caller's annotation", key)
		}
	}
	// And two JMS properties are reserved over AMQP 1.0 only, where the name
	// has to decide: over STOMP the value does, on Classic.
	for _, key := range []string{"JMSDeliveryMode", "JMSXGroupSeq"} {
		if !ReservedOverAMQP(key) || ReservedFrameHeader(key) || ReservedProperty(key) {
			t.Errorf("%q: ReservedOverAMQP %v, ReservedFrameHeader %v, ReservedProperty %v; want true, false, false",
				key, ReservedOverAMQP(key), ReservedFrameHeader(key), ReservedProperty(key))
		}
	}
	for _, key := range []string{"JMSXDeliveryCount", "JMSRedelivered", PropertyJMSExpiration} {
		if !ReservedProperty(key) || !ReservedOverAMQP(key) || !ReservedFrameHeader(key) {
			t.Errorf("%q is not reserved on both arms", key)
		}
	}
	// The `x-iqmq-` prefix is a separate namespace and a header merely under
	// "x-" is not in it: both brokers' own extensions use that prefix.
	if !ReservedHeader(KeyHeader) || !ReservedHeader(CausationHeader) || !ReservedHeader(EncodingHeader) || !ReservedHeader(IDHeader) {
		t.Error("one of these drivers' own entries is not recognised as reserved")
	}
	if ReservedHeader("x-something") || ReservedHeader("x-iqmq") {
		t.Error("a header outside the reserved prefix was treated as reserved")
	}
}

// TestTheNamesTheBrokersActOnAreNotCarried is the reserved lists measured
// family by family, through both drivers, against
// apache/activemq-classic:6.2.0 with its scheduler on and
// apache/activemq-artemis:2.44.0-alpine, for what each broker DID with the
// header and not only for whether it came back. Each reserved name here was
// carried as an ordinary header up to v1.1.3, and each did harm:
//
//   - Classic's scheduler reads every `AMQ_SCHEDULED_` name on both arms: with
//     `AMQ_SCHEDULED_REPEAT: 3` and `AMQ_SCHEDULED_PERIOD: 200` one publication
//     was delivered four times, and `AMQ_SCHEDULED_CRON` held one back until
//     its schedule. A `scheduledJobId` made it skip the driver's own delay.
//   - Classic sets the expiry from `JMSExpiration` on both arms, and `1`
//     expired the message on arrival; and the delivery count and redelivered
//     flag from `JMSXDeliveryCount` and `JMSRedelivered`, so a first delivery
//     arrived as attempt 5, or 2.
//   - Over STOMP, Artemis routed a message with `destination-type: ANYCAST`
//     past every subscription of a fan-out topic, `transaction: tx1` was
//     receipted and never delivered on Artemis and closed the connection on
//     Classic, and a plain body with `JMS_AMQP_NATIVE: true` failed every AMQP
//     1.0 consumer of the subscription on Classic.
//   - Over AMQP 1.0, Artemis refused a message whose `x-opt-jms-dest`
//     annotation was text and handed back the `x-opt-delivery-delay` of a
//     message the driver had delayed itself; Classic lost a message with
//     `JMSDeliveryMode: 1` when it restarted; and Artemis removed every STOMP
//     consumer a message with `JMSXGroupSeq: abc` was delivered to.
//
// The values after them are refused over STOMP on Classic only: its JMS
// setters close the connection on a value they cannot read. The names after
// those are carried as they were before, on the arm the case names: over STOMP
// `JMSType`, `JMSCorrelationID`, `JMSReplyTo`, `JMSTimestamp` and a valid
// `JMSPriority` or `JMSXGroupSeq` set the JMS field they are named after; over
// AMQP 1.0 none of the carried ones changed anything on either product; and
// Classic does not apply `transformation` to a frame with a `content-length`,
// which every SEND has.
func TestTheNamesTheBrokersActOnAreNotCarried(t *testing.T) {
	both := []string{
		"AMQ_SCHEDULED_REPEAT", "AMQ_SCHEDULED_PERIOD", "AMQ_SCHEDULED_CRON", "scheduledJobId",
		"JMSExpiration", "_AMQ_DUPL_ID", "JMSXDeliveryCount", "JMSRedelivered",
	}
	reserved := map[Kind][]string{
		STOMP: slices.Concat([]string{"destination-type", "transaction", "JMS_AMQP_NATIVE", "JMSXGroupID", "_AMQ_GROUP_ID"}, both),
		AMQP10: slices.Concat([]string{
			"x-opt-delivery-delay", "x-opt-delivery-time", "x-opt-jms-dest", "x-opt-jms-msg-type", "x-opt-jms-reply-to",
			"JMSDeliveryMode", "JMSXGroupSeq",
		}, both),
	}
	type kv struct{ k, v string }
	// Refused over STOMP on Classic, by value.
	refusedValues := []kv{
		{"JMSPriority", "high"},
		{"JMSPriority", " 7"},
		{"JMSPriority", "7.5"},
		{"JMSPriority", "2147483648"},
		{"JMSXGroupSeq", "abc"},
		{"JMSXGroupSeq", "1.5"},
		{"JMSTimestamp", "abc"},
		{"JMSTimestamp", "9223372036854775808"},
		{"JMSDeliveryMode", "true"},
		{"JMSDeliveryMode", "abc"},
		{"JMSReplyTo", ""},
		{"JMSReplyTo", "queue://"},
		{"JMSReplyTo", "a,queue://"},
	}
	// Carried on both arms, as before.
	carriedValues := []kv{
		{"JMSPriority", "0"},
		{"JMSPriority", "9"},
		{"JMSPriority", "10"},
		{"JMSPriority", "-1"},
		{"JMSPriority", "+7"},
		{"JMSTimestamp", "1"},
		{"JMSReplyTo", "/queue/replies"},
		{"JMSType", "OrderCreated"},
		{"JMSCorrelationID", "c-1"},
		{"JMSXUserID", "producer"},
		{"JMS_vendor", "v"},
		{"JMS_AMQP_MA_x-opt-delivery-delay", "60000"},
		{"JMSDeliveryTime", "1"},
		{"type", "t1"},
		{"transformation", "jms-map-json"},
		{"jms", "1"},
		{"AMQ_SCHEDULE", "1"},
		{"tenant", "acme"},
	}
	carried := map[Kind][]kv{
		STOMP: slices.Concat([]kv{
			{"x-opt-delivery-delay", "1"},
			{"x-opt-jms-dest", "1"},
			{"JMSXGroupSeq", "-1"},
			{"JMSDeliveryMode", "1"},
			{"JMSDeliveryMode", "2"},
		}, carriedValues),
		AMQP10: slices.Concat([]kv{
			{"destination-type", "ANYCAST"},
			{"transaction", "tx1"},
			{"x-opt-something", "1"},
			{"JMSXGroupID", "g"},
			{"_AMQ_GROUP_ID", "g"},
			{"_AMQ_SCHED_DELIVERY", "1"},
			{"JMS_AMQP_NATIVE", "true"},
			{"JMSPriority", "high"},
			{"JMSTimestamp", "abc"},
			{"JMSReplyTo", "queue://"},
		}, carriedValues),
	}
	refused := func(t *testing.T, p Protocol, k Kind, b activemq.Broker, name, value string) {
		t.Helper()
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Add(name, []byte(value))
		var f Fields
		o := Options{Broker: b, KeyHeader: KeyHeader}
		if err := p.Carry(&f, m, o); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("%v on %v: a caller's %q = %q header was carried under MetadataNativeStrict (%v), want a "+
				"refusal before any I/O", k, b, name, value, err)
		}
		o.Metadata = mq.MetadataLenientDrop
		if err := p.Carry(&f, m, o); err != nil || f.Dropped != 1 || len(f.Props) != 0 {
			t.Errorf("%v on %v: MetadataLenientDrop with a caller's %q = %q header = %v, %d dropped, %d entries; "+
				"want it dropped and counted", k, b, name, value, err, f.Dropped, len(f.Props))
		}
	}
	for _, k := range []Kind{STOMP, AMQP10} {
		p := newProtocol(t, k)
		for _, b := range activemq.Brokers() {
			for _, name := range reserved[k] {
				refused(t, p, k, b, name, "1")
				f := Fields{Props: []Entry{{Key: name, Text: "1", Kind: Text}}}
				var got mq.Message
				if err := p.Restore(&got, &f, Options{Broker: b}); err != nil || got.Headers.Len() != 0 {
					t.Errorf("%v on %v: a delivery's %q was handed to the caller (%v, %d headers): an application "+
						"that forwarded its headers would publish it", k, b, name, err, got.Headers.Len())
				}
			}
			values := carried[k]
			switch {
			case k == STOMP && b == activemq.Classic:
				for _, c := range refusedValues {
					refused(t, p, k, b, c.k, c.v)
				}
			case k == STOMP:
				values = slices.Concat(values, refusedValues)
			}
			for _, c := range values {
				m := &mq.Message{Body: []byte("b")}
				m.Headers.Add(c.k, []byte(c.v))
				var f Fields
				if err := p.Carry(&f, m, Options{Broker: b, KeyHeader: KeyHeader}); err != nil {
					t.Errorf("%v on %v: a caller's %q = %q header was refused (%v); it was carried before and did "+
						"no harm", k, b, c.k, c.v, err)
					continue
				}
				var got mq.Message
				err := p.Restore(&got, &f, Options{Broker: b})
				if v, ok := got.Headers.Get(c.k); err != nil || !ok || string(v) != c.v {
					t.Errorf("%v on %v: %q came back as %q, %v (%v)", k, b, c.k, v, ok, err)
				}
			}
		}
	}
}

// TestClassicReplyToGrammar holds the STOMP arm's `JMSReplyTo` rule to what
// ActiveMQ Classic 6.2.0 was measured reading, value by value: each refused
// value closed the connection, and each carried one was read as a
// destination. Classic strips a `queue://`, `topic://`, `temp-queue://` or
// `temp-topic://` prefix and refuses a name that is empty once trimmed, part
// by part in a comma-separated one. A value with a `?` is refused: Classic
// reads `a?x` and closed the connection on `a?x=%zz`.
func TestClassicReplyToGrammar(t *testing.T) {
	for _, tc := range []struct {
		value string
		read  bool
	}{
		{"", false},
		{" ", false},
		{"queue://", false},
		{"topic://", false},
		{"temp-queue://", false},
		{"temp-topic://", false},
		{"queue:// ", false},
		{"queue://a,topic://", false},
		{"a,queue://", false},
		{"queue://a, topic://", false},
		{"a, queue://", false},
		{"/queue/x,queue:// ", false},
		{"a?x=%zz", false},
		{"a?x", false},
		{"?x", false},
		{"abc", true},
		{"/queue/", true},
		{"/topic/", true},
		{"/temp-queue/", true},
		{"QUEUE://", true},
		{",", true},
		{"a,b", true},
		{"a,,b", true},
		{"a, ", true},
		{"queue://,", true},
		{" x", true},
		{"temp-queue://x", true},
		{"queue://topic://", true},
		{"ID:x", true},
		{"/queue/replies", true},
	} {
		if got := classicDestination(tc.value); got != tc.read {
			t.Errorf("classicDestination(%q) = %v, want %v as Classic 6.2.0 read it", tc.value, got, tc.read)
		}
	}
}

// TestAllocs holds the shared mapping to zero allocations in both directions.
//
// It is the check that justifies putting one mapping between two drivers at
// all, and it matters more here than in the RabbitMQ pair: the STOMP arm's
// `Carry` and `Restore` ARE its driver's mapping, so an allocation here is an
// allocation per publication in `drivers/activemq/stomp`.
//
// Zero is reachable in both directions because of three decisions [Fields]
// records: the group BORROWS the message key rather than converting it to a
// string; every application header is a Binary entry whose bytes are the
// caller's, so `Restore` hands `mq.Headers.Add` the stored slice and converts
// nothing; and every number the brokers want as text — the delay, the expiry,
// the priority — stays a typed field, so the DRIVER renders it into a frame
// header or a typed section and the shared mapping renders nothing.
//
// The third one is the interesting one, and it is where this family differs
// from RabbitMQ's AMQP 0-9-1 `expiration` property is a decimal STRING,
// so `internal/rabbit/protocol` allocates once per publication with a TTL and
// pins exactly that. ActiveMQ's equivalents are a duration, an instant and a
// whole-millisecond integer, so there is nothing to render here and zero is
// reachable rather than nearly reachable.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range activemq.Brokers() {
			o := Options{Broker: b, KeyHeader: KeyHeader, KeyAsGroup: true, Expiry: modelExpiry}
			m := &mq.Message{Key: []byte("k"), Body: []byte("b"), ID: "id"}
			for _, key := range []string{"a", "b", "c", "d"} {
				m.Headers.Add(key, []byte("1"))
			}
			var f Fields
			var dst mq.Message
			// Warm the buffer's slice, the duplicate set's storage and the
			// destination's header storage; the steady state is what the specification
			// budgets.
			for range 4 {
				if err := p.Carry(&f, m, o); err != nil {
					t.Fatal(err)
				}
				dst.Reset()
				if err := p.Restore(&dst, &f, o); err != nil {
					t.Fatal(err)
				}
			}
			got := testing.AllocsPerRun(200, func() {
				if err := p.Carry(&f, m, o); err != nil {
					t.Fatal(err)
				}
			})
			// Zero even WITH headers, which is not obvious: a message with
			// headers needs a duplicate-detection set, and the map cannot be
			// reused across calls because `Carry` is safe for concurrent use
			// over one protocol value while `Fields` is the caller's. It
			// costs nothing at these counts because the map does not escape
			// `carry` and the compiler therefore stack-allocates it. That
			// holds at the header counts the specification budgets and stops holding
			// further up -- see the 32-header block below, which pins the
			// number rather than pretending it is still zero.
			if got != 0 {
				t.Errorf("%v/%v: Carry allocated %.0f times per call with four headers, want 0: the duplicate-detection set does not escape carry and is stack-allocated at this count", k, b, got)
			}
			bare := &mq.Message{Key: []byte("k"), Body: []byte("b"), ID: "id"}
			got = testing.AllocsPerRun(200, func() {
				if err := p.Carry(&f, bare, o); err != nil {
					t.Fatal(err)
				}
			})
			if got != 0 {
				t.Errorf("%v/%v: Carry allocated %.0f times for a message with no headers, want 0: the group borrows the key and the reserved entries are strings", k, b, got)
			}
			// A publication with a TTL, a priority, a delay and a
			// de-duplication key -- the widest mapping either arm performs --
			// still allocates nothing, because every value goes into a typed
			// field or into an entry that holds a string or borrowed bytes.
			wide := o
			wide.Publish.TTL = time.Minute
			wide.Publish.Priority, wide.Publish.PrioritySet = 4, true
			wide.Publish.Delay = 2500 * time.Millisecond
			wide.Delay = 2500 * time.Millisecond
			if b == activemq.Artemis {
				wide.Publish.DedupKey = "d-1"
			}
			for range 4 {
				if err := p.Carry(&f, bare, wide); err != nil {
					t.Fatal(err)
				}
			}
			got = testing.AllocsPerRun(200, func() {
				if err := p.Carry(&f, bare, wide); err != nil {
					t.Fatal(err)
				}
			})
			if got != 0 {
				t.Errorf("%v/%v: Carry allocated %.0f times for the widest publication, want 0: a TTL is a typed field on one arm and an instant on the other, and neither is a rendered string", k, b, got)
			}

			// And the receive direction, which is the half a driver runs on
			// every delivery.
			if err := p.Carry(&f, m, o); err != nil {
				t.Fatal(err)
			}
			got = testing.AllocsPerRun(200, func() {
				dst.Reset()
				if err := p.Restore(&dst, &f, o); err != nil {
					t.Fatal(err)
				}
			})
			if got != 0 {
				t.Errorf("%v/%v: Restore allocated %.0f times per delivery, want 0: every application header is a Binary entry whose bytes are already the caller's, so Restore converts nothing", k, b, got)
			}

			// The one place a DELIVERY costs an allocation, pinned as a
			// number: an application header whose value the driver handed
			// over as a Go STRING rather than as borrowed bytes.
			// [Entry.value] has to convert it, which is one allocation per
			// such header per delivery.
			//
			// It is not on any driver's path today and it is pinned anyway,
			// because the shape is easy to reach by accident: both drivers
			// build `Fields.Props` from a wire delivery, go-stomp hands a
			// frame's headers over as strings, and a driver that passed them
			// through as `Kind: Text` would pay this on every header of every
			// delivery. Borrowed bytes are what it should pass instead, and
			// this is the number that says why.
			textDelivery := Fields{Props: []Entry{
				{Key: "trace", Text: "abc", Kind: Text},
				{Key: "tenant", Text: "t-9", Kind: Text},
			}}
			for range 4 {
				dst.Reset()
				if err := p.Restore(&dst, &textDelivery, o); err != nil {
					t.Fatal(err)
				}
			}
			got = testing.AllocsPerRun(200, func() {
				dst.Reset()
				if err := p.Restore(&dst, &textDelivery, o); err != nil {
					t.Fatal(err)
				}
			})
			if got != 2 {
				t.Errorf("%v/%v: Restore allocated %.0f times for a delivery whose two header values are Go strings, want exactly 2 (one conversion each): a driver should hand a delivery's header values over as borrowed bytes, and this number is what says what passing strings costs", k, b, got)
			}

			// The one place zero is NOT reachable, pinned as a number rather
			// than left to be discovered: a message with 32 headers makes
			// the duplicate-detection set big enough to escape to the heap,
			// and the map's own growth costs three allocations. The specification's
			// header dimension stops at 8 and this benchmark's at 4, so it
			// is not on the budgeted path -- but a driver publishing wide
			// messages pays it, and a future change that made it worse
			// should fail here rather than in somebody's profile.
			wide32 := &mq.Message{Key: []byte("k"), Body: []byte("b"), ID: "id"}
			for i := range 32 {
				wide32.Headers.Add("h"+strconv.Itoa(i), []byte("1"))
			}
			for range 4 {
				if err := p.Carry(&f, wide32, o); err != nil {
					t.Fatal(err)
				}
			}
			got = testing.AllocsPerRun(200, func() {
				if err := p.Carry(&f, wide32, o); err != nil {
					t.Fatal(err)
				}
			})
			if got > 3 {
				t.Errorf("%v/%v: Carry allocated %.0f times for a 32-header message, want at most 3 (the duplicate-detection map escaping and growing)", k, b, got)
			}
		}
	}
}
