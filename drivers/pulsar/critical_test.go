package pulsar_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
	"bitbucket.org/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// fakeAdmin serves the one administrative endpoint this driver reads at open:
// the broker's runtime configuration.
//
// It exists because the batch-index acknowledgement guard is a question about
// the BROKER — `acknowledgmentAtBatchIndexLevelEnabled` — and the answer is not
// on the binary protocol at all. Without it the guard could only be tested
// against a container, and the failure it prevents is silent.
func fakeAdmin(t *testing.T, batchIndexAck bool) string {
	t.Helper()
	flag := "false"
	if batchIndexAck {
		flag = "true"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v2/brokers/configuration/runtime", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"brokerVersion":"4.2.4","maxMessageSize":"5242880",` +
			`"acknowledgmentAtBatchIndexLevelEnabled":"` + flag + `"}`))
	})
	mux.HandleFunc("/admin/v2/brokers/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestCriticalBatchIndexAckDoesNotSettleASibling is the disqualifier in
// Pulsar's clothing, and the reason FuzzPulsarAckRules exists.
//
// Pulsar's producer batches by default, so one broker ENTRY holds several
// messages and an acknowledgement names an ENTRY. Three of the five ways that
// can go — an identifier that lost the client's per-entry bitmap, an
// acknowledgement set sent to a broker that ignores one, and a cumulative
// acknowledgement used for throughput — all settle messages the application
// never completed, which is "can lose an accepted message outside a documented
// failure boundary". None of them reports an error anywhere.
func TestCriticalBatchIndexAckDoesNotSettleASibling(t *testing.T) {
	const topic = "persistent://public/default/batched"

	t.Run("the client holds the frame back", func(t *testing.T) {
		// With batch-index acknowledgement off — this driver's default —
		// pulsar-client-go keeps a bitmap in the identifier and sends the
		// entry's frame only when every index is clear. Acknowledging one
		// message of a four-message batch must therefore reach the broker
		// as NOTHING, and must certainly not settle the other three.
		b, server := newRig(t, nil, pulsarfake.WithBatchSize(4))
		src := sourceOf(t, b, "batched", "workers", mq.MetadataNativeStrict, 8)
		snk := sinkOf(t, b, "batched", mq.MetadataNativeStrict)
		for i := range 4 {
			publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
		}
		if n := server.Entries(topic); n != 1 {
			t.Fatalf("the producer wrote %d entries for four messages; this test needs them in ONE batch", n)
		}
		ds := fetchN(t, src, 4)
		if err := src.Settler().Ack(context.Background(), &ds[1]); err != nil {
			t.Fatalf("Ack of the second message of the batch: %v", err)
		}
		st := server.Stats(topic, "workers")
		if st.CrossSettled != 0 {
			t.Fatalf("acknowledging one message of a four-message batch settled %d message(s) nobody "+
				"completed: the performance budgets forbids losing an accepted message outside a "+
				"documented failure boundary", st.CrossSettled)
		}
		if st.Acked != 0 {
			t.Fatalf("the broker acknowledged %d message(s) after one of four was settled; with "+
				"batch-index acknowledgement off the client holds the frame back until every index "+
				"is clear", st.Acked)
		}
		for _, i := range []int{0, 2, 3} {
			if err := src.Settler().Ack(context.Background(), &ds[i]); err != nil {
				t.Fatalf("Ack of message %d: %v", i, err)
			}
		}
		st = server.Stats(topic, "workers")
		if st.Acked != 4 || st.CrossSettled != 0 {
			t.Fatalf("once every index was settled the broker acknowledged %d and cross-settled %d, "+
				"want 4 and 0", st.Acked, st.CrossSettled)
		}
	})

	t.Run("an identifier with no batch bookkeeping is refused", func(t *testing.T) {
		// The hazard that cannot be seen: an identifier put through
		// Serialize and DeserializeMessageID carries the same ledger,
		// entry, batch index and batch size, and acknowledging one
		// message of a batch with it settles every sibling. The driver
		// refuses instead, which leaves the message to be redelivered.
		b, server := newRig(t, nil, pulsarfake.WithBatchSize(4), pulsarfake.WithUntrackedIDs())
		src := sourceOf(t, b, "batched", "workers", mq.MetadataNativeStrict, 8)
		snk := sinkOf(t, b, "batched", mq.MetadataNativeStrict)
		for i := range 4 {
			publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
		}
		ds := fetchN(t, src, 4)
		err := src.Settler().Ack(context.Background(), &ds[1])
		if err == nil {
			t.Fatal("the driver acknowledged one message of a batched entry through an identifier that " +
				"cannot name one message of it; that acknowledgement settles every sibling in the entry")
		}
		if !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("the refusal reported %v (code %v), want ErrNotSettleable", err, mq.CodeOf(err))
		}
		if st := server.Stats(topic, "workers"); st.CrossSettled != 0 || st.Acked != 0 {
			t.Fatalf("the broker saw %d acknowledgement(s) and %d cross-settlement(s) from a refused "+
				"settlement; a refusal must write nothing", st.Acked, st.CrossSettled)
		}
	})

	t.Run("a broker that ignores an acknowledgement set is refused at open", func(t *testing.T) {
		// The combination that loses messages and that no error reports:
		// the client puts an acknowledgement set in the frame and the
		// broker's acknowledgmentAtBatchIndexLevelEnabled is false, so
		// the broker settles the whole entry. The driver asks the
		// administrative API at open and refuses the configuration.
		server := pulsarfake.New(pulsarfake.WithBatchSize(4), pulsarfake.WithBrokerBatchIndexAck(false))
		cfg := rigConfig(server)
		cfg.AdminURL = fakeAdmin(t, false)
		cfg.EnableBatchIndexAck = true
		b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
		if err == nil {
			_ = b.Close()
			t.Fatal("the driver opened a client that will send an acknowledgement set to a broker that " +
				"ignores one; every individual acknowledgement of a batched entry then settles the " +
				"whole entry and every sibling in it, with no error anywhere ")
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Fatalf("the refusal reported %v (code %v), want InvalidConfig", err, mq.CodeOf(err))
		}
	})

	t.Run("a broker that honours an acknowledgement set settles one index", func(t *testing.T) {
		server := pulsarfake.New(pulsarfake.WithBatchSize(4), pulsarfake.WithBrokerBatchIndexAck(true))
		cfg := rigConfig(server)
		cfg.AdminURL = fakeAdmin(t, true)
		cfg.EnableBatchIndexAck = true
		b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		src := sourceOf(t, b, "batched", "workers", mq.MetadataNativeStrict, 8)
		snk := sinkOf(t, b, "batched", mq.MetadataNativeStrict)
		for i := range 4 {
			publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
		}
		ds := fetchN(t, src, 4)
		if err := src.Settler().Ack(context.Background(), &ds[1]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		st := server.Stats(topic, "workers")
		if st.Acked != 1 || st.CrossSettled != 0 {
			t.Fatalf("with the broker honouring an acknowledgement set, settling one message of four "+
				"acknowledged %d and cross-settled %d, want 1 and 0", st.Acked, st.CrossSettled)
		}
	})
}

// TestCriticalSubscriptionModes is the specification's "Pulsar subscription modes": each of
// the four types is a different broker, and the capability table has to say so.
//
// It is the increment's subject. A driver that claimed the strongest ordering
// for all four would be right about one of them, and a caller planning against
// the table would build a keyed pipeline on a Shared subscription that reorders
// by construction.
func TestCriticalSubscriptionModes(t *testing.T) {
	for _, tc := range []struct {
		kind pd.SubscriptionKind
		// second says what a SECOND consumer of one subscription is.
		second string
		// key, partition and global are the three claims.
		key, partition, global mq.Level
		groups                 mq.Level
		scheduled              mq.Level
	}{
		{
			kind: pd.SubscriptionShared, second: "competitor",
			key: mq.Unsupported, partition: mq.Unsupported, global: mq.Unsupported,
			groups: mq.Native, scheduled: mq.Native,
		},
		{
			kind: pd.SubscriptionKeyShared, second: "competitor",
			key: mq.Conditional, partition: mq.Unsupported, global: mq.Unsupported,
			groups: mq.Native, scheduled: mq.Native,
		},
		{
			kind: pd.SubscriptionExclusive, second: "refused",
			key: mq.Conditional, partition: mq.Native, global: mq.Conditional,
			groups: mq.Unsupported, scheduled: mq.Unsupported,
		},
		{
			kind: pd.SubscriptionFailover, second: "standby",
			key: mq.Conditional, partition: mq.Native, global: mq.Conditional,
			groups: mq.Conditional, scheduled: mq.Unsupported,
		},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			b, _ := newRig(t, func(c *pd.Config) { c.SubscriptionType = tc.kind })
			caps := b.Capabilities()
			for _, claim := range []struct {
				f    mq.Feature
				want mq.Level
			}{
				{mq.FeatureOrderedByKey, tc.key},
				{mq.FeatureOrderedByPartition, tc.partition},
				{mq.FeatureOrderedGlobally, tc.global},
				{mq.FeatureConsumerGroups, tc.groups},
				{mq.FeatureScheduledPublish, tc.scheduled},
			} {
				got := caps.Support(claim.f)
				if got.Level != claim.want {
					t.Errorf("a %s subscription advertises %v at %v, want %v",
						tc.kind, claim.f, got.Level, claim.want)
				}
				if got.Level == mq.Conditional && got.Note == "" {
					t.Errorf("%v is conditional on a %s subscription with no condition stated: "+
						"The specification requires one, and a conditional claim with no condition tells a "+
						"caller nothing they can act on", claim.f, tc.kind)
				}
			}
			// A delayed publication is Pulsar's DISPATCHER's, and the
			// dispatcher that has one belongs to a Shared or Key_Shared
			// subscription. A client configured for the other two
			// refuses the schedule rather than making one that does not
			// happen.
			snk := sinkOf(t, b, "sched", mq.MetadataNativeStrict)
			err := snk.Publish(context.Background(), &mq.Message{Body: []byte("later")},
				&mq.PublishOptions{Delay: time.Second})
			switch tc.scheduled {
			case mq.Native:
				if err != nil {
					t.Errorf("a delayed publication on a %s subscription's client: %v", tc.kind, err)
				}
			default:
				if !errors.Is(err, mq.ErrUnsupported) {
					t.Errorf("a delayed publication from a client configured for a %s subscription "+
						"reported %v (code %v), want ErrUnsupported: Pulsar's delayed-delivery "+
						"tracker belongs to the Shared and Key_Shared dispatchers, so such a "+
						"message would be delivered at once", tc.kind, err, mq.CodeOf(err))
				}
			}
			// And what a second consumer of one subscription IS.
			checkSecondConsumer(t, b, tc.kind, tc.second)
		})
	}
}

// checkSecondConsumer opens two consumers on one subscription and checks what
// the second one is: a competitor, a standby, or a refusal.
func checkSecondConsumer(t *testing.T, b *pd.Backend, kind pd.SubscriptionKind, want string) {
	t.Helper()
	first := sourceOf(t, b, "pair", "workers", mq.MetadataNativeStrict, 8)
	second, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "pair", Subscription: "workers", Prefetch: 8,
		Metadata: mq.MetadataNativeStrict, Start: mq.StartEarliest(),
	})
	if want == "refused" {
		if err == nil {
			_ = second.Close()
			t.Fatalf("a second consumer of one %s subscription was accepted; Pulsar refuses it with "+
				"ConsumerBusy, which is why FeatureConsumerGroups is not claimed", kind)
		}
		if mq.CodeOf(err) != mq.CodeConflict {
			t.Fatalf("the refusal of a second %s consumer reported %v (code %v), want Conflict",
				kind, err, mq.CodeOf(err))
		}
		return
	}
	if err != nil {
		t.Fatalf("a second consumer of one %s subscription: %v", kind, err)
	}
	t.Cleanup(func() { _ = second.Close() })
	snk := sinkOf(t, b, "pair", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Key: []byte("k"), Body: []byte("one")})
	// Exactly one of the two receives it, whichever kind of second consumer
	// this is: that is what the specification asks of a group, and a standby satisfies it
	// for a different reason than a competitor does.
	seen := 0
	for _, src := range []mq.Source{first, second} {
		dst := make([]mq.DriverDelivery, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		n, _ := src.Fetch(ctx, dst)
		cancel()
		if n == 1 {
			seen++
			if err := src.Settler().Ack(context.Background(), &dst[0]); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("%d of two consumers of one %s subscription received the message, want exactly one", seen, kind)
	}
	if want == "standby" {
		// A Failover subscription's second consumer receives nothing
		// while the first holds, which is why its ConsumerGroups claim
		// is conditional rather than native.
		dst := make([]mq.DriverDelivery, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		n, _ := second.Fetch(ctx, dst)
		cancel()
		if n != 0 {
			t.Fatalf("the standby consumer of a %s subscription received a message while the active "+
				"one held the subscription", kind)
		}
	}
}

// TestCriticalPartitionExpansion is the specification's "Pulsar partition expansion": a
// topic gains partitions while a producer and a consumer are attached.
//
// Two things must hold and one must NOT be claimed. The producer has to
// discover the new count and publish to the new partitions; the consumer has to
// receive from them; and the per-key ordering claim must not be Native, because
// adding partitions rehashes every key — a key's older messages are in one
// partition and its newer ones in another, with no order between them.
func TestCriticalPartitionExpansion(t *testing.T) {
	const topic = "persistent://public/default/expanding"
	b, server := newRig(t, func(c *pd.Config) { c.SubscriptionType = pd.SubscriptionKeyShared })
	server.AddPartitions(topic, 4)
	src := sourceOf(t, b, "expanding", "workers", mq.MetadataNativeStrict, 64)
	snk := sinkOf(t, b, "expanding", mq.MetadataNativeStrict)

	// A key whose partition CHANGES when the count doubles, found rather
	// than assumed: with four partitions and eight, most keys move and some
	// do not, and a test that hard-coded one would be asserting arithmetic.
	key := keyThatMoves(t, 4, 8)

	for i := range 3 {
		publish(t, snk, &mq.Message{Key: []byte(key), Body: []byte{byte('a' + i)}})
	}
	before := fetchN(t, src, 3)
	first := before[0].Info
	if !first.HasPartition {
		t.Fatal("a delivery from a partitioned topic does not say which partition it came from: the specification " +
			"pairs every optional field with a Has flag precisely so that partition zero and " +
			"\"the broker did not say\" are different answers")
	}
	for i := range before {
		if before[i].Info.Partition != first.Partition {
			t.Fatalf("delivery %d of one key came from partition %d and the first from %d; before an "+
				"expansion a key is in ONE partition", i, before[i].Info.Partition, first.Partition)
		}
		if err := src.Settler().Ack(context.Background(), &before[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	// The expansion.
	server.AddPartitions(topic, 8)
	for i := range 3 {
		publish(t, snk, &mq.Message{Key: []byte(key), Body: []byte{byte('x' + i)}})
	}
	after := fetchN(t, src, 3)
	moved := false
	for i := range after {
		if after[i].Info.Partition != first.Partition {
			moved = true
		}
		if err := src.Settler().Ack(context.Background(), &after[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if !moved {
		t.Fatalf("after the partition count went from four to eight, key %q was still published to "+
			"partition %d: the producer did not discover the new partitions, so two thirds of the "+
			"topic's capacity is idle and the keys that DID move are being reordered silently",
			key, first.Partition)
	}
	if server.Partitions(topic) != 8 {
		t.Fatalf("the topic reports %d partitions after the expansion", server.Partitions(topic))
	}
	// And the claim. This is the half a test can assert deterministically:
	// the broker's reordering across an expansion is real and arbitrary, and
	// what the driver owes a caller is a table that does not promise
	// otherwise.
	if lvl := b.Capabilities().Support(mq.FeatureOrderedByKey).Level; lvl == mq.Native {
		t.Fatalf("the driver advertises per-key ordering at %v while a partition expansion rehashes "+
			"every key; the claim has to be conditional and the condition has to name the "+
			"partition count", lvl)
	}
}

// TestCriticalKeySharedOrdering is the specification's "Pulsar Key_Shared ordering", and
// the finding it carries is about the PRODUCER rather than the consumer.
//
// Pulsar dispatches an ENTRY to one consumer. With the default batcher an entry
// holds whatever arrived within BatchingMaxPublishDelay of it — several keys —
// so a Key_Shared subscription would have to send one consumer an entry holding
// another consumer's keys, and per-key ordering would be false for every key
// that shared an entry. The key-based batcher is what makes the claim true.
func TestCriticalKeySharedOrdering(t *testing.T) {
	const topic = "persistent://public/default/keyed"
	b, server := newRig(t,
		func(c *pd.Config) { c.SubscriptionType = pd.SubscriptionKeyShared },
		pulsarfake.WithBatchSize(4))

	snk := sinkOf(t, b, "keyed", mq.MetadataNativeStrict)
	if got := server.Batcher(topic); got != pulsar.KeyBasedBatchBuilder {
		t.Fatalf("a producer on a client configured for a Key_Shared subscription asked for batcher %v, "+
			"want the key-based one: Pulsar dispatches an ENTRY to one consumer, so an entry holding "+
			"two keys is an entry that cannot be routed by key and the per-key ordering claim is "+
			"false for both of them", got)
	}

	// Two consumers, and two keys chosen to land in DIFFERENT hash ranges so
	// that they belong to different consumers.
	keyA, keyB := keysInDifferentRanges(t, 2)
	first := sourceOf(t, b, "keyed", "workers", mq.MetadataNativeStrict, 64)
	second, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "keyed", Subscription: "workers", Prefetch: 64,
		Metadata: mq.MetadataNativeStrict, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("a second Key_Shared consumer: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	const each = 8
	for i := range each {
		publish(t, snk, &mq.Message{Key: []byte(keyA), Body: []byte{byte('0' + i)}})
		publish(t, snk, &mq.Message{Key: []byte(keyB), Body: []byte{byte('0' + i)}})
	}
	// The producer's last batch of each key is still open, and an open batch
	// has not been sent: BatchingMaxPublishDelay elapsing is what closes it.
	server.Flush()
	// Every message of one key goes to ONE consumer, and in publication
	// order. Collect what each consumer got and check both halves.
	byKey := map[string][]byte{}
	owner := map[string]int{}
	for idx, src := range []mq.Source{first, second} {
		for {
			dst := make([]mq.DriverDelivery, 16)
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			n, _ := src.Fetch(ctx, dst)
			cancel()
			if n == 0 {
				break
			}
			for i := range n {
				key := string(dst[i].Message.Key)
				if prev, ok := owner[key]; ok && prev != idx {
					t.Fatalf("key %q was delivered to consumer %d and to consumer %d; a Key_Shared "+
						"subscription sends one key to one consumer, and per-key order is not a "+
						"claim otherwise", key, prev, idx)
				}
				owner[key] = idx
				byKey[key] = append(byKey[key], dst[i].Message.Body[0])
				if err := src.Settler().Ack(context.Background(), &dst[i]); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
		}
	}
	for key, got := range byKey {
		for i := range got {
			if got[i] != byte('0'+i) {
				t.Fatalf("key %q arrived as %q: a Key_Shared subscription delivers one key's messages "+
					"in publication order while the consumer set is stable", key, got)
			}
		}
	}
	if len(byKey) != 2 {
		t.Fatalf("the two keys arrived as %d key(s); both were published", len(byKey))
	}
	// And the claim: conditional, never native, with the condition stated.
	s := b.Capabilities().Support(mq.FeatureOrderedByKey)
	if s.Level != mq.Conditional {
		t.Fatalf("a Key_Shared subscription advertises per-key ordering at %v, want conditional: the "+
			"promise ends when a consumer joins or leaves and when the partition count changes, "+
			"and neither is under the driver's control", s.Level)
	}
	if s.Note == "" {
		t.Fatal("the conditional per-key ordering claim states no condition")
	}
}
