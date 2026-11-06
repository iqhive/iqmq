package pulsar_test

import (
	"context"
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

// TestAllocsPublishUnderBlockIfQueueFull holds a publication's allocation
// budget under BlockIfQueueFull, where the driver's wait for room is on the
// path, to the budget without it.
//
// The wait's first form cost a publication three allocations more: the
// client's answer was read for a refusal even when there was no error, which
// moves the reading's target to the heap, and the hand-off kept its two flags
// on its own stack, captured by a callback of their own. The flags now live in
// the pooled scratch publication and the answer is read only when it is an
// error.
func TestAllocsPublishUnderBlockIfQueueFull(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector perturbs allocation counts")
	}
	budget := map[bool]map[string]float64{}
	for _, block := range []bool{false, true} {
		b, _ := newRig(t, func(c *pd.Config) { c.BlockIfQueueFull = block })
		snk := sinkOf(t, b, "allocs", mq.MetadataNativeStrict)
		m := &mq.Message{Body: []byte("x")}
		ctx := context.Background()
		done := func(error) {}
		budget[block] = map[string]float64{
			"Publish": testing.AllocsPerRun(200, func() { _ = snk.Publish(ctx, m, nil) }),
			"PublishAsync": testing.AllocsPerRun(200, func() {
				_ = snk.(mq.AsyncSink).PublishAsync(ctx, m, nil, done)
			}),
		}
	}
	for _, path := range []string{"Publish", "PublishAsync"} {
		if on, off := budget[true][path], budget[false][path]; on > off {
			t.Errorf("%s allocates %.0f times per publication under BlockIfQueueFull and %.0f without it; "+
				"the driver's wait for room should cost none", path, on, off)
		}
	}
}
