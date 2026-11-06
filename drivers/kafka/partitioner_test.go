package kafka_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestPartitionerMatchesKafkasFormula is the whole reason this driver has a
// partitioner of its own.
//
// Kafka's default partitioner is `(murmur2(key) & 0x7fffffff) % numPartitions`,
// and a Go producer that hashed differently would split one key's records
// across two partitions — which breaks the per-key ordering for every
// consumer of the topic, silently, and only in a fleet that also has a Java
// or librdkafka producer. The expectations below are the formula, computed
// from the murmur2 implementation the specification already holds known-answer tests
// for.
func TestPartitionerMatchesKafkasFormula(t *testing.T) {
	p := kq.Partitioner().ForTopic("orders")
	for _, n := range []int{1, 2, 3, 4, 12, 16, 64, 1000} {
		for i := range 200 {
			key := []byte("user-" + strconv.Itoa(i))
			rec := &kgo.Record{Key: key}
			got := p.Partition(rec, n)
			want := int((murmur2(key) & 0x7fffffff) % uint32(n))
			if got != want {
				t.Fatalf("Partition(%q, %d) = %d, Kafka's formula says %d", key, n, got, want)
			}
		}
	}
}

// TestPartitionerIsStableAcrossPartitioners: two producers must agree, which
// is what "Kafka-compatible" means in practice.
func TestPartitionerIsStableAcrossPartitioners(t *testing.T) {
	a := kq.Partitioner().ForTopic("orders")
	b := kq.Partitioner().ForTopic("orders")
	for i := range 100 {
		rec := &kgo.Record{Key: []byte("k" + strconv.Itoa(i))}
		if a.Partition(rec, 8) != b.Partition(rec, 8) {
			t.Fatalf("two partitioners disagreed about %q", rec.Key)
		}
	}
}

// TestPartitionerConsistency: a keyed record must always hash to the same
// partition and a keyless one need not, which is what lets franz-go move a
// keyless record to a healthier leader and forbids it moving a keyed one.
func TestPartitionerConsistency(t *testing.T) {
	p := kq.Partitioner().ForTopic("orders")
	if !p.RequiresConsistency(&kgo.Record{Key: []byte("k")}) {
		t.Fatal("a keyed record does not require consistency: its key's order would be split across partitions")
	}
	if p.RequiresConsistency(&kgo.Record{}) {
		t.Fatal("a keyless record requires consistency: nothing orders it, so nothing is lost by moving it")
	}
	// An empty non-nil key is still a key: Kafka hashes it, and it is a
	// different partition from "no key at all".
	if !p.RequiresConsistency(&kgo.Record{Key: []byte{}}) {
		t.Fatal("an empty non-nil key is a key")
	}
}

// TestPartitionerIsSticky: a keyless record goes to one partition until the
// producer starts a new batch, which is what keeps a keyless stream's batches
// full. Round-robin across sixteen partitions makes sixteen batches of a
// sixteenth the size, which is sixteen produce requests instead of one.
func TestPartitionerIsSticky(t *testing.T) {
	tp, ok := kq.Partitioner().ForTopic("orders").(*kq.TopicPartitioner)
	if !ok {
		t.Fatal("ForTopic did not return the driver's own partitioner")
	}
	first := tp.Partition(&kgo.Record{}, 16)
	for range 100 {
		if got := tp.Partition(&kgo.Record{}, 16); got != first {
			t.Fatalf("keyless records went to %d and %d within one batch", first, got)
		}
	}
	// A new batch ends the run. It may land on the same partition by
	// chance, so the check is over many batches rather than one.
	moved := false
	for range 200 {
		tp.OnNewBatch()
		if tp.Partition(&kgo.Record{}, 16) != first {
			moved = true
			break
		}
	}
	if !moved {
		t.Fatal("two hundred new batches all chose the same partition: the sticky partition is never re-chosen")
	}
}

// TestPartitionerHandlesShrinkingTopics: a partition count that falls below
// the sticky choice must not be returned, because a produce to a partition
// that no longer exists is an error the caller cannot act on.
func TestPartitionerHandlesShrinkingTopics(t *testing.T) {
	tp := &kq.TopicPartitioner{}
	tp.OnNewBatch()
	if got := tp.Partition(&kgo.Record{}, 16); got < 0 || got >= 16 {
		t.Fatalf("Partition out of range: %d", got)
	}
	for range 50 {
		if got := tp.Partition(&kgo.Record{}, 1); got != 0 {
			t.Fatalf("Partition(·, 1) = %d, want the only partition there is", got)
		}
	}
	if got := tp.Partition(&kgo.Record{Key: []byte("k")}, 0); got != 0 {
		t.Fatalf("Partition(·, 0) = %d, want 0 rather than a panic", got)
	}
}

// murmur2 is Kafka's own hash, written out here so that the partitioner is
// checked against the formula rather than against the implementation it calls.
func murmur2(data []byte) uint32 {
	const (
		seed = uint32(0x9747b28c)
		m    = uint32(0x5bd1e995)
		r    = 24
	)
	h := seed ^ uint32(len(data))
	n := len(data) & ^3
	for i := 0; i < n; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch len(data) - n {
	case 3:
		h ^= uint32(data[n+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[n+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[n])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// TestEveryPublicationPathRoutesWhereItWasAsked: a synchronous Publish and
// PublishBatch carry the requested partition in their record's placement,
// and the other paths in RouteToPartition's context value. Every record lands
// on the partition it asked for, keyed or not; an unrouted key lands where the
// other path puts it; and a partition the topic does not have is refused.
func TestEveryPublicationPathRoutesWhereItWasAsked(t *testing.T) {
	topic := newTopic("rv-route")
	cl := cluster(t, 3, []string{topic})
	_, b := openPair(t, cl)
	syncSink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	none, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	hw := func() [3]int64 {
		var out [3]int64
		for p := range int32(3) {
			out[p] = cl.PartitionInfo(topic, p).HighWatermark
		}
		return out
	}
	expect := func(what string, before, want [3]int64) {
		t.Helper()
		var got [3]int64
		deadline := time.Now().Add(5 * time.Second)
		for {
			now := hw()
			for i := range 3 {
				got[i] = now[i] - before[i]
			}
			if got == want || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if got != want {
			t.Errorf("%s: records added per partition = %v, want %v", what, got, want)
		}
	}
	for want := range int32(3) {
		for _, keyed := range []bool{false, true} {
			before := hw()
			m := &mq.Message{Body: []byte("x")}
			if keyed {
				m.Key = []byte("k-not-for-this-partition")
			}
			o := &mq.PublishOptions{Partition: want, PartitionSet: true}
			if err := syncSink.Publish(t.Context(), m, o); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			var w [3]int64
			w[want] = 1
			expect("sync Publish routed", before, w)

			before = hw()
			if err := syncSink.(mq.BatchSink).PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a"), Key: m.Key}, {Body: []byte("b")}, {Body: []byte("c")}}, o); err != nil {
				t.Fatalf("PublishBatch: %v", err)
			}
			w[want] = 3
			expect("sync PublishBatch routed", before, w)

			before = hw()
			if err := none.Publish(t.Context(), m, o); err != nil {
				t.Fatalf("ConfirmNone Publish: %v", err)
			}
			w[want] = 1
			expect("ConfirmNone Publish routed", before, w)

			before = hw()
			done := make(chan error, 1)
			if err := syncSink.(mq.AsyncSink).PublishAsync(context.Background(), m, o, func(err error) { done <- err }); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			expect("PublishAsync routed", before, w)
		}
	}
	// A keyed record unrouted: the sync path (placement) and the async path
	// (no placement) put the same key on the same partition, every time.
	for _, key := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		before := hw()
		if err := syncSink.Publish(t.Context(), &mq.Message{Key: []byte(key), Body: []byte("s")}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		var syncAt [3]int64
		time.Sleep(50 * time.Millisecond)
		now := hw()
		for i := range 3 {
			syncAt[i] = now[i] - before[i]
		}
		before = hw()
		if err := none.Publish(t.Context(), &mq.Message{Key: []byte(key), Body: []byte("n")}, nil); err != nil {
			t.Fatalf("ConfirmNone Publish: %v", err)
		}
		expect("keyed "+key+" on the ConfirmNone path where the sync path put it", before, syncAt)
	}
	// Keyless sync records spread to valid partitions only, and none is refused.
	for range 30 {
		if err := syncSink.Publish(t.Context(), &mq.Message{Body: []byte("keyless")}, nil); err != nil {
			t.Fatalf("keyless Publish: %v", err)
		}
	}
	// Out of range is still franz-go's refusal.
	if err := syncSink.Publish(t.Context(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{Partition: 7, PartitionSet: true}); err == nil {
		t.Error("a sync Publish to partition 7 of 3 succeeded")
	}
}
