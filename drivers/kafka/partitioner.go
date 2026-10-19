package kafka

import (
	"context"
	"math/rand/v2"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iqhive/iqmq/internal/hash"
)

// Partitioner returns a [kgo.Partitioner] that places records where Kafka's
// Java client would.
//
// Two halves, and both matter:
//
//   - A record WITH a key goes to `murmur2(key) & 0x7fffffff % partitions`,
//     which is byte for byte what the Java client's default partitioner
//     computes. That is what makes a key's records land on the same partition
//     as records the rest of the fleet produced — a Go producer that hashed
//     differently would split one key's order across two partitions and break
//     the per-key ordering for every consumer of that topic, silently and
//     only in a mixed-language deployment. The hash is the specification's
//     [github.com/iqhive/iqmq/internal/hash.KafkaPartition], which is already
//     measured, already checked against Kafka's own known-answer tests, and
//     bound whatever the bake-off decides for the engine's own lane hash:
//     The specification says murmur2 is "required in the driver regardless".
//   - A record WITHOUT a key is sticky: it goes to one partition until the
//     producer starts a new batch, and then to another chosen at random. That
//     is what keeps a keyless stream's batches full — round-robin across
//     sixteen partitions makes sixteen batches of a sixteenth the size, which
//     is sixteen produce requests instead of one.
//
// franz-go's own StickyKeyPartitioner would do the same thing given the same
// hash, and this is that: the partitioner is written out here rather than
// delegated so that the hash is visibly iqmq's own measured one and so that
// [TopicPartitioner] can be unit-tested against Kafka's formula without a
// broker.
func Partitioner() kgo.Partitioner { return partitioner{} }

// partitioner is the [kgo.Partitioner] half: one TopicPartitioner per topic,
// because the sticky partition is per topic.
type partitioner struct{}

// ForTopic returns a partitioner for one topic.
func (partitioner) ForTopic(string) kgo.TopicPartitioner { return &TopicPartitioner{} }

// TopicPartitioner is the murmur2 sticky partitioner for one topic.
//
// It is exported so that the partition a key lands on can be computed and
// asserted without a client: [TopicPartitioner.Partition] is a pure function
// of the key and the partition count for every keyed record.
//
// It is not safe for concurrent use; franz-go owns one per topic and drives it
// from the record-buffering path, which is serialised per topic.
type TopicPartitioner struct {
	sticky int32
	// have reports whether sticky has been chosen, so that the zero value
	// of an exported type does not mean "stick to partition zero".
	have bool
}

// compile-time proof that the partitioner implements both halves franz-go
// asks for: the partition decision and the new-batch notification the
// stickiness depends on.
var (
	_ kgo.Partitioner                = partitioner{}
	_ kgo.TopicPartitioner           = (*TopicPartitioner)(nil)
	_ kgo.TopicPartitionerOnNewBatch = (*TopicPartitioner)(nil)
)

// RequiresConsistency reports that a keyed record must always hash to the same
// partition, which is what stops franz-go moving it to a partition whose
// leader is healthier.
//
// A keyless record has no order to keep, so it may be moved: that is what
// makes the sticky half useful rather than merely arbitrary. A record the
// caller routed explicitly may not be moved either — the caller named the
// partition, and moving it would silently ignore the instruction.
func (p *TopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	if _, ok := requestedPartition(r.Context); ok {
		return true
	}
	return r.Key != nil
}

// OnNewBatch ends the current sticky run, so the next keyless record picks a
// new partition.
func (p *TopicPartitioner) OnNewBatch() { p.have = false }

// partitionKey is the key [RouteToPartition] carries an explicitly requested
// partition under.
type partitionKey struct{}

// RouteToPartition marks a record for one partition, which is what the specification's
// per-call Partition control asks for.
//
// It travels in the record's Context and NOT in its Partition field, which
// looks like the obvious place and is not: franz-go writes the chosen
// partition into that field before it buffers the record (`pr.Partition =
// recBuf.partition` in sink.go's bufferRecord), so by the time a partitioner
// is asked a second time — which happens on the batch boundary
// TopicPartitionerOnNewBatch exists for — the field no longer says what the
// CALLER asked for, it says what the partitioner picked. A driver that read it
// back could not tell "partition 0 because the caller said so" from
// "partition 0 because I chose it", and a keyless record would stop rotating
// its sticky partition. The record's Context is documented as the field "used
// for enriching records" and to "propagate record enrichment across producer
// hooks", which is exactly this.
//
// ctx is the publication's own context, which franz-go uses for the record's
// cancellation, so the value is added to it rather than to a fresh one.
func RouteToPartition(ctx context.Context, rec *kgo.Record, partition int32) {
	rec.Context = context.WithValue(ctx, partitionKey{}, partition)
}

// requestedPartition reads back what [RouteToPartition] marked, if anything.
func requestedPartition(ctx context.Context) (int32, bool) {
	if ctx == nil {
		return 0, false
	}
	p, ok := ctx.Value(partitionKey{}).(int32)
	return p, ok
}

// Partition returns the partition for r out of n.
//
// A caller that named a partition gets it. Otherwise a keyed record goes to
// Kafka's own formula and a keyless one to the sticky partition, chosen once
// per batch.
func (p *TopicPartitioner) Partition(r *kgo.Record, n int) int {
	if n <= 0 {
		return 0
	}
	if want, ok := requestedPartition(r.Context); ok {
		// Out of range is franz-go's own refusal ("invalid record
		// partitioning choice"), which is the right answer: the caller
		// named a partition the topic does not have.
		return int(want)
	}
	if r.Key != nil {
		return hash.KafkaPartition(r.Key, n)
	}
	if !p.have || int(p.sticky) >= n {
		// rand/v2's generator is per-process and seeded by the runtime, so
		// two producers do not start on the same partition. The choice is
		// made once per batch, not once per record.
		p.sticky, p.have = int32(rand.IntN(n)), true //nolint:gosec // partition choice, not a security decision
	}
	return int(p.sticky)
}
