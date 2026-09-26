package kafka_test

import (
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

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
