package pulsar_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzPulsarNaming holds the topic mapper to the three properties the specification's
// target 3 asks of one: it never panics, everything it accepts round-trips, and
// it is injective.
//
// The injectivity is the one that costs correctness. A mapper that folded two
// portable topics onto one Pulsar topic would make one subscription receive
// another's messages, which is why an illegal name is REFUSED here rather than
// escaped.
func FuzzPulsarNaming(f *testing.F) {
	seedNaming(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := pd.Config{
			Domain:      []string{"", pd.DomainPersistent, pd.DomainNonPersistent}[r.Int(0, 2)],
			Tenant:      []string{"", "public", "acme", "a.b"}[r.Int(0, 3)],
			Namespace:   []string{"", "default", "billing"}[r.Int(0, 2)],
			TopicPrefix: []string{"", "iqmq-", "p."}[r.Int(0, 2)],
		}
		seen := map[string]string{}
		for range r.Int(1, 6) {
			topic := r.String(40)
			got, err := pd.TopicOf(cfg, topic)
			if err != nil {
				// A refusal is always allowed; what is not allowed
				// is a name that is accepted and then unusable.
				if mq.CodeOf(err) == mq.CodeUnknown {
					t.Fatalf("TopicOf(%q) refused with an unclassified error: %v", topic, err)
				}
				continue
			}
			name := got.String()
			back, perr := pd.ParseTopic(name)
			if perr != nil {
				t.Fatalf("TopicOf(%q) produced %q, which ParseTopic refuses: %v", topic, name, perr)
			}
			if back.String() != name {
				t.Fatalf("%q round-tripped to %q", name, back.String())
			}
			if !strings.Contains(topic, pd.Separator) {
				// A short name is composed with the configured
				// namespace and the prefix; a fully qualified one
				// is taken as it stands, which is how a caller
				// reaches another tenant.
				if back.Local != cfg.TopicPrefix+topic {
					t.Fatalf("the local name came back as %q, want the prefix plus %q", back.Local, topic)
				}
			}
			if pd.HasPattern(topic) {
				t.Fatalf("TopicOf accepted the pattern %q; a pattern can be subscribed to and not "+
					"published to", topic)
			}
			if prev, dup := seen[name]; dup && prev != topic {
				t.Fatalf("%q and %q both map to %q; a mapper that is not injective makes one "+
					"subscription receive another's messages", prev, topic, name)
			}
			seen[name] = topic
			// And the pattern form of the same name is a legal
			// pattern for the same namespace.
			pat, ns, perr := pd.PatternOf(cfg, topic)
			if perr != nil {
				t.Fatalf("PatternOf(%q) refused a topic TopicOf accepted: %v", topic, perr)
			}
			if ns != back.Namespaced() {
				t.Fatalf("PatternOf reported namespace %q, want %q", ns, back.Namespaced())
			}
			if !strings.Contains(topic, pd.Separator) && pat != name {
				t.Fatalf("PatternOf produced %q for a topic that is not a pattern, want %q", pat, name)
			}
		}
		// A subscription name takes the same grammar, because Pulsar
		// builds the retry topic's name out of it.
		sub := r.String(40)
		if got, err := pd.SubscriptionOf(sub); err == nil {
			if got != sub {
				t.Fatalf("SubscriptionOf(%q) produced %q; the mapping is the identity", sub, got)
			}
			if err := pd.CheckName(got); err != nil {
				t.Fatalf("SubscriptionOf accepted %q, which CheckName refuses: %v", got, err)
			}
		}
	})
}

// seedNaming commits the shapes that matter: the ordinary name, the empty one,
// a name with a slash, a pattern, a partition suffix and a fully qualified
// name.
func seedNaming(f *testing.F) {
	f.Helper()
	for _, topic := range []string{
		"orders", "", "a/b", "orders.*", "orders-partition-3",
		"persistent://acme/ns/orders", "..", "ORDERS", "a b",
	} {
		w := fuzzutil.NewWriter()
		w.Int(0, 2, 1)
		w.Int(0, 3, 0)
		w.Int(0, 2, 0)
		w.Int(0, 2, 0)
		w.Int(1, 6, 1)
		w.String(40, topic)
		w.String(40, "workers")
		f.Add(w.Data())
	}
}

// FuzzPulsarAckRules is the disqualifier as a property.
//
// The rule the driver must never break is one sentence: NO acknowledgement
// settles a message the application did not acknowledge, unless every message
// of its entry was acknowledged. Five conditions decide what one
// acknowledgement settles — whether the entry is a batch, whether the
// identifier carries the client's bitmap, whether the client sends an
// acknowledgement set, whether the broker honours one — and two of the
// combinations lose messages silently. This walks all of them.
func FuzzPulsarAckRules(f *testing.F) {
	seedAckRules(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		rules := pd.AckRules{
			Tracked:             r.Bool(),
			ClientBatchIndexAck: r.Bool(),
			BrokerBatchIndexAck: r.Bool(),
		}
		size := int32(r.Int(1, 8))
		acks := pd.NewEntryAcks(size)
		requested := make([]bool, size)
		settled := make([]bool, size)
		refusals := 0
		for range r.Int(0, 16) {
			i := int32(r.Int(0, int(size)-1))
			p := pd.BatchPosition{Ledger: 1, Entry: 2, Partition: -1, Index: -1, Size: size}
			if size > 1 {
				p.Index = i
			}
			if !p.Valid() {
				t.Fatalf("the model built an invalid position %v", p)
			}
			if err := rules.Check(p); err != nil {
				// The driver refuses and writes nothing, which is
				// what leaves the message to be redelivered
				// rather than destroying its siblings.
				refusals++
				if mq.CodeOf(err) != mq.CodeNotSettleable && mq.CodeOf(err) != mq.CodeInvalidArgument {
					t.Fatalf("the refusal reported %v (code %v)", err, mq.CodeOf(err))
				}
				continue
			}
			changed, complete := acks.Ack(p.Index)
			if changed != !requested[i] {
				t.Fatalf("EntryAcks reported changed=%v for index %d whose requested state is %v",
					changed, i, requested[i])
			}
			requested[i] = true
			if complete != allTrue(requested) {
				t.Fatalf("EntryAcks reported complete=%v with %v acknowledged", complete, requested)
			}
			switch rules.Scope(p) {
			case pd.AckScopeMessage:
				if rules.ClientBatchIndexAck && p.Batched() {
					settled[i] = true
					continue
				}
				// The client holds the frame back until every
				// index is clear, and then sends one for the
				// entry.
				if complete {
					for j := range settled {
						settled[j] = true
					}
				}
			case pd.AckScopeEntry:
				for j := range settled {
					settled[j] = true
				}
			}
		}
		// THE INVARIANT.
		for i := range settled {
			if settled[i] && !requested[i] {
				t.Fatalf("message %d of a %d-message entry was settled and nobody acknowledged it, "+
					"under rules %+v: the performance budgets forbids losing an accepted message "+
					"outside a documented failure boundary", i, size, rules)
			}
		}
		// And the bookkeeping's own consistency.
		if acks.Settled()+acks.Pending() != int(acks.Size()) {
			t.Fatalf("EntryAcks reports %d settled and %d pending of %d", acks.Settled(), acks.Pending(), acks.Size())
		}
		if acks.Complete() != allTrue(requested) {
			t.Fatalf("EntryAcks reports complete=%v with %v acknowledged", acks.Complete(), requested)
		}
		// A position the entry does not have is never settleable.
		bad := pd.BatchPosition{Ledger: 1, Entry: 2, Partition: -1, Index: size, Size: size}
		if bad.Valid() {
			t.Fatalf("a position whose index equals the batch size reports itself valid: %v", bad)
		}
		if err := rules.Check(bad); err == nil {
			t.Fatal("an invalid position was accepted for settlement")
		}
		// An unsafe combination is refused for every batched position, and
		// never for an unbatched one.
		single := pd.BatchPosition{Ledger: 1, Entry: 2, Partition: -1, Index: -1, Size: 1}
		if err := rules.Check(single); err != nil {
			t.Fatalf("an entry holding one message was refused: %v; there is no sibling to lose", err)
		}
		// And an unsafe combination is refused for every batched position
		// that was attempted at all.
		if size > 1 && (!rules.Tracked || (rules.ClientBatchIndexAck && !rules.BrokerBatchIndexAck)) {
			for i := range requested {
				if requested[i] {
					t.Fatalf("index %d was acknowledged under rules %+v, which cannot name one "+
						"message of a batch: every such settlement must be refused", i, rules)
				}
			}
		}
		_ = refusals
	})
}

// allTrue reports whether every entry is true.
func allTrue(b []bool) bool {
	for _, v := range b {
		if !v {
			return false
		}
	}
	return true
}

// seedAckRules commits the five rows of the truth table, each with a batch and
// with a single message.
func seedAckRules(f *testing.F) {
	f.Helper()
	add := func(tracked, client, broker bool, size, ops int, idx []int) {
		w := fuzzutil.NewWriter()
		w.Bool(tracked)
		w.Bool(client)
		w.Bool(broker)
		w.Int(1, 8, size)
		w.Int(0, 16, ops)
		for _, i := range idx {
			w.Int(0, size-1, i)
		}
		f.Add(w.Data())
	}
	// The client holds the frame back: every index acknowledged.
	add(true, false, false, 4, 4, []int{0, 1, 2, 3})
	// Partly acknowledged, which must settle nothing.
	add(true, false, false, 4, 2, []int{1, 2})
	// The broker honours the set.
	add(true, true, true, 4, 1, []int{1})
	// The broker IGNORES the set, which is the combination that loses
	// messages.
	add(true, true, false, 4, 1, []int{1})
	// The identifier lost the client's bitmap.
	add(false, false, false, 4, 1, []int{1})
	// And the unbatched entry, where none of it applies.
	add(false, true, false, 1, 1, []int{0})
	add(true, false, true, 1, 1, []int{0})
}
