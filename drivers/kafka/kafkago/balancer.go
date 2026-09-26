package kafkago

import (
	"sync/atomic"

	kgo "github.com/segmentio/kafka-go"

	"github.com/iqhive/iqmq/internal/hash"
)

// Partitioner returns the murmur2 partitioner this driver produces with, with
// keyless records held on one partition for run records at a time.
//
// Three halves, and each matters for a different reason:
//
//   - A record WITH A KEY goes to `murmur2(key) & 0x7fffffff % partitions`,
//     which is byte for byte what the Java client's default partitioner
//     computes and what the franz-go driver computes. That is what makes a
//     key's records land on the same partition as records the rest of the
//     fleet produced — a Go producer that hashed differently would split one
//     key's order across two partitions and break the per-key ordering for
//     every consumer of that topic, silently and only in a mixed-client
//     deployment. The hash is the specification's
//     [github.com/iqhive/iqmq/internal/hash.KafkaPartition], which is already
//     measured and already checked against Kafka's own known-answer tests, and
//     is bound whatever the bake-off decides for the engine's own lane
//     hash: the specification says murmur2 is "required in the driver regardless".
//     kafka-go ships a [kgo.Murmur2Balancer] of its own and this driver does
//     not use it, so that BOTH Kafka drivers compute the partition with the
//     one measured implementation and TestCrossDriverPartitioning can prove
//     they agree.
//   - A record WITHOUT A KEY goes to one partition for `run` records and then
//     to the next, which keeps a keyless stream's batches full. franz-go's
//     partitioner is told when a batch ends
//     ([kgo.TopicPartitionerOnNewBatch]) and re-rolls then; kafka-go's
//     [kgo.Balancer] has no such notification at all, so the run length is
//     the only thing a balancer here can key stickiness on, and the honest
//     choice is the writer's own batch size. At run=1 —
//     which is [DefaultBatchMaxRecords] — this is plain round-robin, which is
//     kafka-go's own default.
//   - A record the CALLER ROUTED goes where the caller said, read from the
//     message's WriterData. See [Route].
//
// The counter is a single atomic and is shared across topics: it decides only
// which partition a keyless record goes to, and a keyless record has no order
// to keep. [kgo.Writer] calls Balance from every goroutine that publishes, so
// a balancer that took a lock would serialise the publish path.
func Partitioner(run int) kgo.Balancer {
	if run < 1 {
		run = 1
	}
	return &TopicPartitioner{run: uint64(run)}
}

// TopicPartitioner is the murmur2 partitioner, exported so that the partition
// a key lands on can be computed and asserted without a broker.
//
// It is safe for concurrent use, which [kgo.Balancer] requires of any balancer
// shared by a writer: kafka-go calls Balance from the goroutine of every
// WriteMessages.
type TopicPartitioner struct {
	next atomic.Uint64
	run  uint64
}

// compile-time proof that the partitioner is what kafka-go's writer takes.
var _ kgo.Balancer = (*TopicPartitioner)(nil)

// Balance returns the partition msg goes to, out of the partitions the topic
// has.
//
// The return value is a partition ID from the list rather than an index into
// it: kafka-go passes the topic's partition IDs and uses the answer as an ID,
// so a balancer that returned an index would be right only for a topic whose
// partitions are numbered 0..n-1 — which is every ordinary topic and not a
// topic whose partitions have been reassigned.
func (p *TopicPartitioner) Balance(msg kgo.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	if want, ok := requestedPartition(msg.WriterData); ok {
		// The caller named a partition. It is returned as given, even when
		// the topic does not have it: the broker's own
		// UNKNOWN_TOPIC_OR_PARTITION is the refusal, and silently routing
		// elsewhere would ignore the instruction the specification made enforceable.
		return int(want)
	}
	if msg.Key != nil {
		return partitions[hash.KafkaPartition(msg.Key, len(partitions))]
	}
	i := p.next.Add(1) - 1
	return partitions[(i/p.run)%uint64(len(partitions))]
}

// PartitionFor is the partition a keyed record goes to, out of n.
//
// It is the pure form of [TopicPartitioner.Balance]'s keyed half, exported so
// that a test can assert the two Kafka drivers agree without a broker and
// without either importing the other.
func PartitionFor(key []byte, n int) int {
	if n <= 0 {
		return 0
	}
	return hash.KafkaPartition(key, n)
}
