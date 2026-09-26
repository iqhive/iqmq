package redis_test

import (
	"testing"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
)

// TestAllocsMapping holds the pure mapping to an allocation budget.
//
// Every function here is on the publish path or the delivery path of every
// message, so a change that made one allocate per header — a string
// conversion that escaped, a slice grown one entry at a time — would cost
// exactly where the budgets are stated.
//
// The budgets are not zero, and the reason is worth stating rather than
// hiding behind a generous number: a Redis command is a list of STRINGS, and
// a message's key, body and header values are []byte, so the conversion is
// where the copy has to happen. It happens once per field, which is also what
// makes it safe for a caller to reuse its buffers the instant Publish returns
// What the budget catches is growth beyond one allocation per field.
func TestAllocsMapping(t *testing.T) {
	m := &mq.Message{
		Topic:      "acme.orders",
		Key:        []byte("order-4711"),
		Body:       []byte("payload"),
		ID:         "01JQZ8V3K5T2WQ9X7YB6C4D2E1",
		Properties: mq.Properties{ContentType: "application/json"},
	}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Add("tag", []byte("a"))
	m.Headers.Add("tag", []byte("b"))

	// ShardOf hashes the key. It must allocate nothing: it runs on every
	// publication to a partitioned topic.
	if got := testing.AllocsPerRun(200, func() {
		_ = rd.ShardOf(m.Key, 8)
	}); got != 0 {
		t.Errorf("ShardOf allocates %.0f times, want 0: it runs on every publication to a "+
			"partitioned topic", got)
	}

	// The limit check reads lengths and validates UTF-8. It must allocate
	// nothing: it runs before every publication, in every metadata mode.
	lim := streams.DefaultLimits
	if got := testing.AllocsPerRun(200, func() {
		if err := lim.Check(m); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("Limits.Check allocates %.0f times, want 0: it runs before every publication", got)
	}

	// The entry layout. Eight fields for this message (body, key, topic,
	// three headers, identifier, content type), each one string conversion,
	// plus the header framing's builder. The budget is the field count and a
	// little, not a round number.
	dst := make([]string, 0, 32)
	if got := testing.AllocsPerRun(200, func() {
		dst = streams.Encode(dst[:0], m)
	}); got > 8 {
		t.Errorf("Encode allocates %.0f times for a three-header message, want at most 8 "+
			"(one per field that carries bytes, plus the header framing)", got)
	}

	// The key naming. Shard 0 is the bare tag, which is already built, so it
	// must allocate nothing; a numbered shard is one concatenation.
	keys, err := streams.NewKeys("acme.orders", 8)
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(200, func() { _ = keys.Stream(0) }); got != 0 {
		t.Errorf("Keys.Stream(0) allocates %.0f times, want 0", got)
	}
	if got := testing.AllocsPerRun(200, func() { _ = keys.Stream(3) }); got > 1 {
		t.Errorf("Keys.Stream(3) allocates %.0f times, want at most 1", got)
	}

	// The reclaimer's bookkeeping, which every delivery and every settlement
	// goes through. It is a map, so a first insertion may grow it; a steady
	// state must not allocate.
	rec := streams.NewReclaimer()
	key := streams.EntryKey{Shard: 1, ID: streams.ID{Ms: 1767225600000, Seq: 1}}
	gen, _, _ := rec.Hand(key)
	if got := testing.AllocsPerRun(200, func() {
		if err := rec.Check(key, gen); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("Reclaimer.Check allocates %.0f times, want 0: every settlement goes through it", got)
	}
}
