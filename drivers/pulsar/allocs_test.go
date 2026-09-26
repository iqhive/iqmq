package pulsar_test

import (
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// TestAllocsMapping holds the pure mapping's allocation budget.
//
// The encoder is on the publication path and the decoder is on the receive
// path, so the specification's "no allocation per message that the payload did not already
// need" applies to both. The property map is caller storage and reused, which
// is what the budget is about: without it every publication would allocate a
// map, and the sink's own pool would be pointless.
//
// It skips under -race, which perturbs allocation counts, and `make allocs` is
// where it actually runs.
func TestAllocsMapping(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector perturbs allocation counts")
	}
	m := &mq.Message{Key: []byte("customer-7"), Body: []byte("payload"), ID: "id-9"}
	m.Headers.Set("kind", []byte("order"))
	m.Properties.ContentType = "application/json"
	m.Properties.CreatedAt = time.Unix(1700000000, 0)
	var pm pulsar.ProducerMessage
	props := make(map[string]string, 8)
	// Warm the map so that its growth is not counted: the sink reuses one
	// per pooled publication, so the steady state is what matters.
	pd.Encode(&pm, m, props)

	if n := testing.AllocsPerRun(200, func() { pd.Encode(&pm, m, props) }); n > 2 {
		t.Fatalf("Encode allocates %.0f times per publication; the property map is caller storage and "+
			"reused, so the steady state is the key's and the properties' string conversions alone", n)
	}
	if n := testing.AllocsPerRun(200, func() { _ = pd.Representable(m) }); n > 1 {
		t.Fatalf("Representable allocates %.0f times per publication; it is the admission check "+
			"and runs before every one", n)
	}
	if n := testing.AllocsPerRun(200, func() { _, _ = pd.TopicOf(pd.Config{}, "orders") }); n > 2 {
		t.Fatalf("TopicOf allocates %.0f times; it runs once per handle, not per message, but a mapper "+
			"that allocated per call would be doing something other than concatenation", n)
	}
	pos := pd.BatchPosition{Ledger: 1, Entry: 2, Partition: 0, Index: 1, Size: 4}
	rules := pd.AckRules{Tracked: true}
	if n := testing.AllocsPerRun(200, func() { _ = rules.Check(pos) }); n != 0 {
		t.Fatalf("the batch-acknowledgement guard allocates %.0f times per settlement; it runs before "+
			"every one and its answer is a preallocated error", n)
	}
	if n := testing.AllocsPerRun(200, func() { _ = rules.Scope(pos) }); n != 0 {
		t.Fatalf("AckRules.Scope allocates %.0f times", n)
	}
}
