package protocol

import (
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// The benchmarks.
//
// The decision column is "supported-version compatibility, feature subset,
// perf", and only the third of those is a measurement. The first two are
// facts about each protocol that the package computes and this benchmark
// REPORTS, so that the scorecard carries the whole criterion rather than the
// third of it that has a clock attached:
//
//	features the feature subset, as the number of the inventory the
//	              protocol supports natively. It is computed from
//	              Protocol.Support, not measured, and the conformance suites
//	              are what prove the table in both directions.
//	min-broker    supported-version compatibility, as the major version this
//	              protocol needs. The AMQP 1.0 profile refuses anything below
//	              4, which is what settles the family's default before any
//	              measurement.
//	ns/op         perf, for the part of it that is honestly measurable here:
//	              the property mapping each protocol's message model demands,
//	              on the hot path of every publication and every delivery.
//	allocs/op     the same, as allocations. It is the sharper half: the
//	              mapping is shared by two drivers, so a mapping that
//	              allocated per message would put both of them on the wrong
//	              side.
//
//	BenchmarkRabbitProtocol/<kind>/carry/headers=<0|4>
//	BenchmarkRabbitProtocol/<kind>/restore/headers=<0|4>
//
// What is NOT measured here is the two protocols' throughput against a broker.
// That needs a broker and two client libraries and is recorded as this family's
// criterion gap the conformance suites and the
// version matrix are where the two arms are compared against a real RabbitMQ.

// benchHeaders is the header counts the mapping is measured at. Zero is the
// allocation-free path a driver publishing a bare message takes, and four is the specification's own header dimension.
var benchHeaders = []int{0, 4}

// BenchmarkRabbitProtocol measures both arms.
func BenchmarkRabbitProtocol(b *testing.B) {
	for _, k := range Kinds() {
		p, err := New(k)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(k.String(), func(b *testing.B) {
			for _, n := range benchHeaders {
				b.Run("carry/headers="+strconv.Itoa(n), func(b *testing.B) { benchCarry(b, p, n) })
				b.Run("restore/headers="+strconv.Itoa(n), func(b *testing.B) { benchRestore(b, p, n) })
			}
		})
	}
}

// benchCarry measures the publish direction.
func benchCarry(b *testing.B, p Protocol, headers int) {
	m := benchMessage(headers)
	o := benchOptions()
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
	reportProtocol(b, p)
}

// benchRestore measures the receive direction.
//
// The destination message is reset rather than reallocated, which is what a
// driver's fetch buffer does: the specification makes the deliveries in a fetch buffer valid
// only until the next Fetch, so the driver reuses the storage and the mapping
// has to be measured against that reuse.
func benchRestore(b *testing.B, p Protocol, headers int) {
	m := benchMessage(headers)
	o := benchOptions()
	var f Fields
	if err := p.Carry(&f, m, o); err != nil {
		b.Fatal(err)
	}
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
	reportProtocol(b, p)
}

// reportProtocol adds the two criterion terms that are facts rather than
// measurements, so that every row of the scorecard carries the whole of the specification's
// criterion.
func reportProtocol(b *testing.B, p Protocol) {
	b.ReportMetric(float64(p.Features()), "features")
	b.ReportMetric(float64(p.MinBroker().Major), "min-broker")
}

// benchMessage is the message both directions are measured on.
func benchMessage(headers int) *mq.Message {
	m := &mq.Message{
		Key:  []byte("order-42"),
		Body: make([]byte, 1024),
		ID:   "01JQ2K8V5N7R3T4Y6W8X9Z0A1B",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "identity",
			CorrelationID:   "01JQ2K8V5N7R3T4Y6W8X9Z0A1C",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1_700_000_000, 0).UTC(),
		},
	}
	for i := range headers {
		m.Headers.Add("h"+strconv.Itoa(i), []byte("v"+strconv.Itoa(i)))
	}
	return m
}

// benchOptions is the option set both directions are measured with: a key in
// the table, a TTL and a priority, which is the widest mapping a publication
// asks for.
func benchOptions() Options {
	o := Options{KeyHeader: ReservedHeaderPrefix + "key", Persistent: true}
	o.Publish.TTL = time.Minute
	o.Publish.Priority, o.Publish.PrioritySet = 4, true
	return o
}
