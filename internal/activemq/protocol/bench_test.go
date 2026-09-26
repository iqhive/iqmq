package protocol

import (
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq"
)

// The benchmarks.
//
// The decision column is "on Artemis and Classic; feature coverage score
// (delay, groups, priority, TTL)", whose expected outcome is "feature
// completeness on Artemis, then perf". Only the SECOND of those terms is a
// measurement. The first two are facts about each protocol on each broker that
// the package computes and this benchmark REPORTS, so that the scorecard
// carries the whole criterion rather than the half of it that has a clock
// attached:
//
//	features the coverage score, as the number of the inventory the
//	              protocol supports natively ON THIS BROKER. It is computed
//	              from Protocol.Support, not measured, and the four conformance
//	              suites are what prove the table in both directions. It is
//	              reported per broker precisely because the row's criterion is
//	              read "on Artemis and Classic": 19 and 16 for the AMQP 1.0
//	              arm, 15 and 13 for the STOMP one.
//	min-broker    supported-version compatibility, as the MAJOR version this
//	              protocol needs on this broker. It is four numbers rather
//	              than one, and the reason is per broker as well as per
//	              protocol: Artemis 2.7 for both arms (the subscription is an
//	              FQQN), Classic 5.9 for AMQP 1.0 and 5.6 for STOMP.
//	ns/op         perf, for the part of it that is honestly measurable here:
//	              the property mapping each protocol's message model demands,
//	              on the hot path of every publication and every delivery.
//	              It is not the same kind of number for the two arms and the
//	              file's own comment on reportProtocol says why.
//	allocs/op     the same, as allocations. It is the sharper half: the STOMP
//	              arm's mapping IS its driver's, so a mapping that allocated
//	              per message would put that driver on the wrong side. TestAllocs pins both arms at zero.
//
//	BenchmarkActiveMQProtocol/<kind>/<broker>/carry/headers=<0|4>
//	BenchmarkActiveMQProtocol/<kind>/<broker>/restore/headers=<0|4>
//
// The BROKER is a benchmark dimension and not only a metric, because the
// mapping really does different work on the two products: the delay goes in a
// different section over AMQP 1.0, and a de-duplication key is carried on one
// and refused on the other. A family measured on one broker would report a
// number for a criterion written about two.
//
// What is NOT measured here is the two protocols' throughput against the real
// brokers. That needs two brokers and two client libraries and is recorded as
// this family's criterion gap the four conformance
// suites are where the arms are compared against a real Artemis and a real
// Classic.

// benchHeaders is the header counts the mapping is measured at. Zero is the
// allocation-free path a driver publishing a bare message takes, and four is the specification's own header dimension.
var benchHeaders = []int{0, 4}

// BenchmarkActiveMQProtocol measures both arms, on both brokers.
func BenchmarkActiveMQProtocol(b *testing.B) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(k.String(), func(b *testing.B) {
			for _, broker := range activemq.Brokers() {
				b.Run(broker.String(), func(b *testing.B) {
					for _, n := range benchHeaders {
						b.Run("carry/headers="+strconv.Itoa(n), func(b *testing.B) {
							benchCarry(b, p, broker, n)
						})
						b.Run("restore/headers="+strconv.Itoa(n), func(b *testing.B) {
							benchRestore(b, p, broker, n)
						})
					}
				})
			}
		})
	}
}

// benchCarry measures the publish direction.
func benchCarry(b *testing.B, p Protocol, broker activemq.Broker, headers int) {
	m := benchMessage(headers)
	o := benchOptions(broker)
	var f Fields
	if err := p.Carry(&f, m, o); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Carry(&f, m, o); err != nil {
			b.Fatal(err)
		}
	}
	reportProtocol(b, p, broker)
}

// benchRestore measures the receive direction.
//
// The destination message is reset rather than reallocated, which is what a
// driver's fetch buffer does: the specification makes the deliveries in a fetch buffer valid
// only until the next Fetch, so the driver reuses the storage and the mapping
// has to be measured against that reuse.
func benchRestore(b *testing.B, p Protocol, broker activemq.Broker, headers int) {
	m := benchMessage(headers)
	o := benchOptions(broker)
	var f Fields
	if err := p.Carry(&f, m, o); err != nil {
		b.Fatal(err)
	}
	// A real delivery carries the broker's own headers as well as the
	// application's, and filtering them is part of what Restore costs: a
	// Classic STOMP delivery was measured carrying eleven of them.
	//
	// Their values are BINARY entries borrowing the caller's bytes, because
	// that is what a driver parsing a delivery produces -- the bytes are the
	// client library's buffer, and `Kind: Text` is for the values this
	// MAPPING sources from a Go string. Getting that wrong made this row
	// dishonest in the first draft: a Text-valued `destination` costs one
	// allocation to hand back, the AMQP 1.0 arm hands it back because a frame
	// header is nobody's message property, the STOMP arm filters it, and the
	// benchmark therefore charged the arm that carried MORE two allocations
	// per delivery for doing so. allocs/op is one of this family's decision
	// metrics, so a row that penalised the more capable arm would have
	// reached the scorecard.
	f.Props = append(f.Props,
		Entry{Key: "destination", Bin: []byte("/queue/Consumer.workers.VirtualTopic.orders"), Kind: Binary},
		Entry{Key: "subscription", Bin: []byte("sub-0"), Kind: Binary},
		Entry{Key: PropertyArtemisGroupID, Text: "order-42", Kind: Text},
	)
	var dst mq.Message
	if err := p.Restore(&dst, &f, o); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		dst.Reset()
		if err := p.Restore(&dst, &f, o); err != nil {
			b.Fatal(err)
		}
	}
	reportProtocol(b, p, broker)
}

// reportProtocol adds the two criterion terms that are facts rather than
// measurements, so that every row of the scorecard carries the whole
// the criterion and not the half of it with a clock attached.
//
// Both are per BROKER, which is the point of the row's "on Artemis and
// Classic": one number for a family whose criterion names two products would
// be a number for a different question.
//
// A warning for whoever reads the ns/op beside them: the two arms' mappings do
// not do the same amount of work, and the difference is the protocols' and not
// the code's. The STOMP arm validates every header value as UTF-8 and checks
// every header name against the frame's own, because its mapping is what
// actually builds the frame; the AMQP 1.0 arm does neither, because its typed
// sections need neither. So a faster STOMP row would not be evidence that
// STOMP is the better arm — it would be evidence that it checked less — and the specification puts feature completeness before perf for exactly that reason.
func reportProtocol(b *testing.B, p Protocol, broker activemq.Broker) {
	b.ReportMetric(float64(p.Features(broker)), "features")
	b.ReportMetric(float64(p.MinBroker(broker).Major), "min-broker")
}

// benchMessage is the message both directions are measured on.
//
// The header values are text, because a STOMP frame header cannot carry
// anything else and a benchmark that fed one arm a value the other refuses
// would be measuring a refusal against a mapping.
func benchMessage(headers int) *mq.Message {
	m := &mq.Message{
		Key:  []byte("order-42"),
		Body: make([]byte, 1024),
		ID:   "01JQ2K8V5N7R3T4Y6W8X9Z0A1B",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "identity",
			CorrelationID:   "01JQ2K8V5N7R3T4Y6W8X9Z0A1C",
			CausationID:     "01JQ2K8V5N7R3T4Y6W8X9Z0A1D",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1_700_000_000, 0).UTC(),
		},
	}
	for i := range headers {
		m.Headers.Add("h"+strconv.Itoa(i), []byte("v"+strconv.Itoa(i)))
	}
	return m
}

// benchOptions is the option set both directions are measured with: the key in
// its entry and as the group, a TTL with the expiry a driver resolved for it,
// a priority and a scheduled delay. That is the widest mapping a publication
// asks for, and it is the same on both brokers so that the two broker rows
// differ only by what the PROTOCOL does differently on them.
func benchOptions(broker activemq.Broker) Options {
	o := Options{
		Broker:     broker,
		Persistent: true,
		KeyHeader:  KeyHeader,
		KeyAsGroup: true,
		Receipt:    "r-1",
		Expiry:     time.Unix(1_700_000_060, 0).UTC(),
		Delay:      2500 * time.Millisecond,
	}
	o.Publish.TTL = time.Minute
	o.Publish.Priority, o.Publish.PrioritySet = 4, true
	o.Publish.Delay = 2500 * time.Millisecond
	return o
}
