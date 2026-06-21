package activemq_test

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// The allocation counts this PROFILE is responsible for (rule 1,
// the specification's "AllocsPerRun for mapping").
//
// The mapping itself is the shared core's and the core pins its own counts
// exactly -- five for a message with a key, two headers and four properties,
// two of them per header. What is pinned here is the DELTA this profile adds,
// because that is the part a change to these rules can move and a benchmark of
// the core cannot see: a delay whose value function boxed, a de-duplication key
// converted per publication, an annotation key built per message. Each of those
// would cost one allocation in every deployment and would be invisible
// upstream.
//
// The counts are exact and the reason is the playbook's: a change to the rules
// is then a diff in this file rather than a number drifting upwards.

// TestAllocsProfileAddsNothingToABareMessage is the base case: with no
// per-message control, this profile's rules must cost nothing at all.
func TestAllocsProfileAddsNothingToABareMessage(t *testing.T) {
	for _, broker := range activemq.Brokers() {
		mapper, buf := mapperFor(t, broker)
		m := &mq.Message{Body: []byte("payload")}
		o := &mq.PublishOptions{TTL: time.Minute, Priority: 3, PrioritySet: true}
		warmBuf(t, mapper, buf, m, o)
		if got := allocs(t, mapper, buf, m, o); got != 0 {
			t.Errorf("%v: encoding a bare payload with a TTL and a priority allocates %.1f times; both are AMQP "+
				"header fields and this profile's rules touch neither", broker, got)
		}
	}
}

// TestAllocsADelayCostsWhatItsSectionCosts is the profile's own hot-path cost,
// and the measured asymmetry between the two products is the finding.
//
// A delayed publication costs TWO allocations on Artemis and ONE on Classic,
// and the difference is the protocol's rather than the driver's: an AMQP 1.0
// MESSAGE-ANNOTATIONS map is keyed by `any` (`amqp.Annotations` is
// `map[any]any`), so an annotation boxes its KEY as well as its value, while
// APPLICATION-PROPERTIES is `map[string]any` and boxes only the value. Artemis
// reads the delay from the annotation section and Classic from the properties
// section, so Artemis's scheduled publication is one allocation dearer for a
// reason no amount of work in this driver can remove.
//
// The counts are exact because the point is that a change to the rules is a
// diff in this file: a rule whose `Value` function built a string, or one that
// formatted its key per message, would be three or four and nothing else would
// say so.
func TestAllocsADelayCostsWhatItsSectionCosts(t *testing.T) {
	want := map[activemq.Broker]float64{
		// key + value, both boxed into a map[any]any.
		activemq.Artemis: 2,
		// value only, into a map[string]any.
		activemq.Classic: 1,
	}
	for _, broker := range activemq.Brokers() {
		mapper, buf := mapperFor(t, broker)
		m := &mq.Message{Body: []byte("payload")}
		plain := &mq.PublishOptions{}
		delayed := &mq.PublishOptions{Delay: 2 * time.Second}
		warmBuf(t, mapper, buf, m, plain)
		base := allocs(t, mapper, buf, m, plain)
		warmBuf(t, mapper, buf, m, delayed)
		with := allocs(t, mapper, buf, m, delayed)
		if delta := with - base; delta != want[broker] {
			t.Errorf("%v: a delayed publication costs %.1f allocations more than an undelayed one (%.1f against "+
				"%.1f), want exactly %.0f. Artemis reads the delay from the message-annotations section, which is "+
				"keyed by `any` and therefore boxes the key as well as the value; Classic reads it from "+
				"application-properties, which is keyed by string",
				broker, delta, with, base, want[broker])
		}
	}
}

// TestAllocsADedupKeyCostsOneBoxedValue is the same accounting for the other
// control this profile owns, on the product that has it.
func TestAllocsADedupKeyCostsOneBoxedValue(t *testing.T) {
	mapper, buf := mapperFor(t, activemq.Artemis)
	m := &mq.Message{Body: []byte("payload")}
	plain := &mq.PublishOptions{}
	deduped := &mq.PublishOptions{DedupKey: "order-42"}
	warmBuf(t, mapper, buf, m, plain)
	base := allocs(t, mapper, buf, m, plain)
	warmBuf(t, mapper, buf, m, deduped)
	with := allocs(t, mapper, buf, m, deduped)
	if delta := with - base; delta != 1 {
		t.Errorf("a de-duplicated publication costs %.1f allocations more than a plain one (%.1f against %.1f), "+
			"want exactly 1: the key is already a string and boxing it into the application-properties map is the "+
			"one allocation", delta, with, base)
	}
}

// TestAllocsTheTwoProductsDifferByExactlyOne is the whole publication's
// version of the same accounting, so that a reader can see the asymmetry once
// and not wonder whether it compounds.
//
// It does not compound: the annotation section's extra boxing is paid once per
// ANNOTATION and the only annotation either product's rules write is the
// scheduled delivery. Everything else -- the key, the headers, the properties
// -- is the core's and is identical on both.
func TestAllocsTheTwoProductsDifferByExactlyOne(t *testing.T) {
	counts := map[activemq.Broker]float64{}
	for _, broker := range activemq.Brokers() {
		mapper, buf := mapperFor(t, broker)
		m := &mq.Message{Key: []byte("order-42"), Body: []byte("payload")}
		m.Headers.Add("one", []byte("1"))
		o := &mq.PublishOptions{Delay: 2 * time.Second, TTL: time.Minute}
		warmBuf(t, mapper, buf, m, o)
		counts[broker] = allocs(t, mapper, buf, m, o)
	}
	if delta := counts[activemq.Artemis] - counts[activemq.Classic]; delta != 1 {
		t.Errorf("the same publication costs %.1f allocations on Artemis and %.1f on Classic, a difference of %.1f, "+
			"want exactly 1: the annotation section's `any` key is paid once per annotation and the scheduled "+
			"delivery is the only annotation either product's rules write, so the asymmetry must not compound",
			counts[activemq.Artemis], counts[activemq.Classic], delta)
	}
}

// mapperFor builds this profile's mapper and a reusable buffer.
func mapperFor(t *testing.T, broker activemq.Broker) (*core.Mapper, *core.Buf) {
	t.Helper()
	cfg := amq.Config{
		Broker: broker, Addrs: []string{"localhost:5672"},
		InsecurePlainSASL: true, Scheduler: true,
	}
	profile, err := cfg.Profile()
	if err != nil {
		t.Fatalf("%v: Profile: %v", broker, err)
	}
	caps, err := amq.CapabilitiesForTest(cfg, productFor(broker), "2.44.0")
	if err != nil {
		t.Fatalf("%v: capabilities: %v", broker, err)
	}
	mapper, err := core.MapperForTest(profile, mq.MetadataNativeStrict, caps, true)
	if err != nil {
		t.Fatalf("%v: mapper: %v", broker, err)
	}
	return mapper, core.NewBuf()
}

// warmBuf grows the buffer's maps and slabs, which happens once rather than per
// message: what the specification forbids is an allocation PER MESSAGE.
func warmBuf(t *testing.T, m *core.Mapper, b *core.Buf, msg *mq.Message, o *mq.PublishOptions) {
	t.Helper()
	for range 6 {
		if _, err := m.Encode(b, msg, o); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
}

// allocs is one encode's allocation count.
func allocs(t *testing.T, m *core.Mapper, b *core.Buf, msg *mq.Message, o *mq.PublishOptions) float64 {
	t.Helper()
	return testing.AllocsPerRun(200, func() {
		if _, err := m.Encode(b, msg, o); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	})
}
