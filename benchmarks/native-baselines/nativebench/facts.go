package nativebench

// Fact is one fact — "a factual assertion about a third-party
// system that must be checked against upstream documentation before
// implementation" — that was verified while building the baselines.
//
// Claim is recorded verbatim in docs/drivers/<Driver>.md; TestFactsRecorded
// fails when a page drifts from the table, so the driver pages cannot claim
// something the baselines did not check. Source says where the check was
// made: a file and line in the pinned client (the strongest evidence, since
// `go test` runs against exactly that code), the broker's own documentation,
// or the harness image pin.
type Fact struct {
	// Driver is the docs/drivers/<Driver>.md page that records the fact.
	Driver string
	// Subject is the short label the page renders in bold.
	Subject string
	// Claim is the verified statement, recorded verbatim in the page.
	Claim string
	// Source is where it was verified.
	Source string
}

// Facts returns every fact verified for the native baselines,
// grouped by driver page in page order.
func Facts() []Fact {
	return []Fact{
		// ---- Kafka -----------------------------------------------------
		{
			Driver: "kafka", Subject: "franz-go defaults",
			Claim:  "franz-go produces with acks=all and an idempotent producer by default; idempotency is only turned off by DisableIdempotentWrite, and its default partitioner (UniformBytesPartitioner) hashes keys with murmur2, so partition assignment matches the Java client.",
			Source: "franz-go v1.21.6 pkg/kgo/config.go:663,672 and pkg/kgo/partitioner.go:254,278",
		},
		{
			Driver: "kafka", Subject: "Sarama defaults",
			Claim:  "Sarama's NewConfig defaults are weaker than franz-go's: Producer.RequiredAcks is WaitForLocal (acks=1) and Producer.MaxMessageBytes is 1 MiB; an idempotent producer additionally requires RequiredAcks=WaitForAll, Net.MaxOpenRequests=1 and Retry.Max>=1.",
			Source: "Sarama v1.60.2 config.go:569-570,811-824",
		},
		{
			Driver: "kafka", Subject: "Sarama partitioner compatibility",
			Claim:  "Sarama's default hash partitioner uses FNV-1a, not Kafka's murmur2, and NewReferenceHashPartitioner only fixes the sign handling; matching the Java client exactly needs NewCustomPartitioner with a murmur2 hash function.",
			Source: "Sarama v1.60.2 partitioner.go:170-195,250-264; franz-go pkg/kgo/partitioner.go:540-560",
		},
		{
			Driver: "kafka", Subject: "kafka-go maintenance and acks",
			Claim:  "segmentio/kafka-go carries no maintenance-mode or deprecation notice for the project, but kafka.NewWriter and kafka.WriterConfig are deprecated in favour of the Writer struct — and a zero-valued Writer.RequiredAcks means RequireNone, because only the deprecated constructor rewrites 0 to RequireAll.",
			Source: "kafka-go v0.4.51 README.md and writer.go:499-511",
		},
		{
			Driver: "kafka", Subject: "message size",
			Claim:  "The broker's default message.max.bytes is 1 MiB, so a 100-message batch of 64 KiB payloads must be split; Sarama splits a produce set when a partition's buffer would reach Producer.MaxMessageBytes, and franz-go splits at ProducerBatchMaxBytes.",
			Source: "Sarama v1.60.2 produce_set.go:316-336; Kafka broker default",
		},

		// ---- NATS core -------------------------------------------------
		{
			Driver: "nats", Subject: "delivery guarantee",
			Claim:  "Core NATS is at-most-once with no store and no per-message acknowledgement: a publish is only known to have reached the server after a round trip (Conn.Flush), and a subscription that is not connected when a message is published never sees it.",
			Source: "nats.go v1.53.1 nats.go Conn.Flush; NATS core documentation",
		},
		{
			Driver: "nats", Subject: "max payload",
			Claim:  "The server advertises its max_payload in the INFO it sends on connect and the client rejects larger publishes locally with ErrMaxPayload; the default is 1 MiB, so payload limits are a server setting, not a client constant.",
			Source: "nats.go v1.53.1 nats.go (Conn.MaxPayload, ErrMaxPayload); nats-server default",
		},

		// ---- JetStream -------------------------------------------------
		{
			Driver: "jetstream", Subject: "per-message TTL and schedules",
			Claim:  "The client exposes per-message TTL as the Nats-TTL header (jetstream.MsgTTLHeader) and message schedules as a schedule header taking an @at timestamp, an @every duration or a cron expression, gated by StreamConfig.AllowMsgSchedules; TTL needs a 2.11+ server and schedules a 2.12+ server, and the harness pins nats 2.14.6.",
			Source: "nats.go v1.53.1 jetstream/message.go:210,220-232 and jetstream/stream_config.go:211; conformance/harness/images.go",
		},
		{
			Driver: "jetstream", Subject: "deduplication and progress",
			Claim:  "Publish deduplication uses the Nats-Msg-Id header (jetstream.MsgIDHeader) against the stream's duplicate window, and a consumer extends its ack wait with Msg.InProgress rather than a lease API.",
			Source: "nats.go v1.53.1 jetstream/message.go:169,378",
		},

		// ---- NSQ -------------------------------------------------------
		{
			Driver: "nsq", Subject: "size limits",
			Claim:  "nsqd's defaults cap a single message at 1 MiB (--max-msg-size) and a whole command body at 5 MiB (--max-body-size), so an MPUB of 100 64 KiB messages (6.4 MiB) must be split into smaller chunks.",
			Source: "nsqd v1.3.0 flag defaults; verified by the MPUB chunking in the nsq baseline",
		},
		{
			Driver: "nsq", Subject: "deferred publish and discovery",
			Claim:  "go-nsq exposes per-message delays through Producer.DeferredPublish (DPUB) and discovers nsqd instances through nsqlookupd, which requires nsqd to broadcast an address the client can reach.",
			Source: "go-nsq v1.1.0 producer.go:233; conformance/harness/brokers.go NSQ",
		},
		{
			Driver: "nsq", Subject: "output buffering",
			Claim:  "nsqd buffers writes to each client and flushes them on a timeout the client sets in IDENTIFY: go-nsq defaults to output_buffer_size 16 KiB and output_buffer_timeout 250 ms, so a single small message can take a quarter of a second to arrive on an otherwise idle connection while a 64 KiB message fills the buffer and leaves at once. A latency-sensitive consumer must lower the timeout, and the client warns that going below 25 ms costs nsqd noticeable CPU.",
			Source: "go-nsq v1.1.0 config.go:165-172; visible in BenchmarkNative/nsq/EndToEnd, where 64 B and 1 KiB round trips take ~250 ms and 64 KiB takes ~380 us",
		},
		{
			Driver: "nsq", Subject: "channels",
			Claim:  "A topic with no channel discards what is published to it, so a channel must exist (SUB, or the /channel/create HTTP endpoint) before the first publish or messages are lost.",
			Source: "nsqd v1.3.0 behaviour; verified by the nsq baseline's provisioning step",
		},

		// ---- Redis -----------------------------------------------------
		{
			Driver: "redis", Subject: "reclaim support",
			Claim:  "XAUTOCLAIM, which a stream driver needs to reclaim entries from dead consumers without scanning the pending-entries list, requires Redis 6.2 or newer; the harness pins redis 7.4 and offers valkey 8.1 as a matrix image.",
			Source: "Redis command documentation (XAUTOCLAIM, since 6.2.0); conformance/harness/images.go",
		},
		{
			Driver: "redis", Subject: "rueidis argument model",
			Claim:  "rueidis builds commands from string arguments, so a []byte payload must go through rueidis.BinaryString — the documented conversion that does not copy — and its auto-pipelining lets one connection carry a whole batch through DoMulti.",
			Source: "rueidis v1.0.77 binary.go:17 and client.go:89",
		},
		{
			Driver: "redis", Subject: "value size",
			Claim:  "A Redis string value, and therefore a stream field value, is limited to 512 MiB; the practical limit for a message payload is set by memory and by proto-max-bulk-len (512 MiB by default), not by the client.",
			Source: "Redis data-type documentation; redis.conf default",
		},

		// ---- RabbitMQ --------------------------------------------------
		{
			Driver: "rabbitmq", Subject: "client",
			Claim:  "github.com/rabbitmq/amqp091-go is the AMQP 0-9-1 client maintained by the RabbitMQ core team, and it is the client the baseline measures; RabbitMQ 4 also speaks AMQP 1.0 natively, for which github.com/Azure/go-amqp is the Go client.",
			Source: "amqp091-go v1.14.0 README.md; RabbitMQ 4 release notes",
		},
		{
			Driver: "rabbitmq", Subject: "quorum queues",
			Claim:  "Quorum queues count redeliveries in the x-delivery-count header and fsync before returning a publisher confirm, so a confirmed publish to a quorum queue is a much stronger boundary than a memory-durability publish and must not be compared with one.",
			Source: "RabbitMQ quorum-queue documentation; verified by the amqp091 baseline's declaration",
		},
		{
			Driver: "rabbitmq", Subject: "delayed messages",
			Claim:  "Delayed publishing needs the rabbitmq_delayed_message_exchange plugin, which is not part of the official image: the harness builds hack/images/rabbitmq-delayed for it, and a driver must detect the exchange type rather than assume it.",
			Source: "hack/images/rabbitmq-delayed/Dockerfile; conformance/harness/brokers.go RabbitMQDelayed",
		},

		{
			Driver: "activemq", Subject: "clients",
			Claim:  "github.com/Azure/go-amqp is an actively maintained MIT-licensed AMQP 1.0 client and is what this driver uses for Artemis and for ActiveMQ Classic's AMQP transport; github.com/go-stomp/stomp/v3 is not a fallback for it but the client of a separate driver module (drivers/activemq/stomp) which a caller selects deliberately, and the one thing that module has and this one does not is a server-side selector, measured to filter on Classic and to be ignored on Artemis.",
			Source: "go-amqp v1.7.0 README.md and LICENSE; internal/activemq/protocol/stomp.go's measured FeatureServerFilter row",
		},
		{
			Driver: "activemq", Subject: "routing type",
			Claim:  "Artemis decides the routing type of an auto-created address from the link's capabilities: a sender or receiver that declares the \"queue\" capability gets an ANYCAST address and queue, and without it the broker falls back to its default-address-routing-type. That is how the goamqp baseline gets a queue to measure and it is not how this driver works: this driver creates no topology at all, expects auto-creation to be off, and states the routing type explicitly, because a multicast address with a durable queue per subscription is what fan-out needs and an anycast address is not.",
			Source: "Artemis AMQP documentation; verified by the goamqp baseline's link options; drivers/activemq/admin.go's queueConfiguration",
		},
		{
			Driver: "activemq", Subject: "delays",
			Claim:  "Scheduled delivery is broker-specific and section-specific, and each product ignores the other's spelling: over AMQP 1.0 Artemis reads the x-opt-delivery-delay message annotation and ActiveMQ Classic reads the AMQ_SCHEDULED_DELAY application property, while over STOMP both brokers read AMQ_SCHEDULED_DELAY as a frame header; Classic additionally needs schedulerSupport=\"true\", which the pinned image does not set. An FQQN (address::queue) is how a consumer names an existing queue on an Artemis address.",
			Source: "apache/activemq-artemis:2.44.0-alpine and apache/activemq-classic:6.2.0, both directions; Artemis and ActiveMQ Classic scheduled-message documentation",
		},

		// ---- Google Cloud Pub/Sub --------------------------------------
		{
			Driver: "gcppubsub", Subject: "client generation",
			Claim:  "cloud.google.com/go/pubsub/v2 replaces the v1 Topic and Subscription types with Publisher and Subscriber handles obtained from the client, and exposes the generated admin surface as client.TopicAdminClient and client.SubscriptionAdminClient.",
			Source: "pubsub/v2 v2.7.0 doc.go, publisher.go:146, subscriber.go:58, pubsub.go:51-59",
		},
		{
			Driver: "gcppubsub", Subject: "settlement",
			Claim:  "Ack and Nack must be called inside the Receive callback or flow control is not respected, and the exactly-once path is AckWithResult/NackWithResult, which return an AckResult carrying an AcknowledgeStatus instead of failing silently.",
			Source: "pubsub/v2 v2.7.0 doc.go and message.go:29-44,102-120",
		},
		{
			Driver: "gcppubsub", Subject: "delivery attempts and ordering",
			Claim:  "Message.DeliveryAttempt is only populated when the subscription has a dead-letter policy, and publishing with an ordering key fails unless Publisher.EnableMessageOrdering is set.",
			Source: "pubsub/v2 v2.7.0 message.go:84-95 and publisher.go:74-75,194,218",
		},
		{
			Driver: "gcppubsub", Subject: "emulator evidence",
			Claim:  "The emulator is not evidence for managed Pub/Sub: it does not implement exactly-once delivery, and its latency and quota behaviour are unrelated to the service, so emulator-based claims are marked as such and managed numbers are measured separately.",
			Source: "conformance/harness/brokers.go PubSub; the performance budgets layer 4",
		},

		// ---- Amazon SQS ------------------------------------------------
		{
			Driver: "sqs", Subject: "message and batch size",
			Claim:  "The maximum SQS message size is 1 MiB (1,048,576 bytes) after the 2025 increase from 256 KiB, SendMessageBatch takes at most 10 entries, and the same 1 MiB limit applies to the sum of a batch's payloads.",
			Source: "aws-sdk-go-v2 service/sqs v1.51.0 api_op_SendMessageBatch.go:13-25; AWS SQS quotas documentation",
		},
		{
			Driver: "sqs", Subject: "body character set",
			Claim:  "An SQS body may contain only XML, JSON or unformatted text (#x9, #xA, #xD, #x20-#xD7FF, #xE000-#xFFFD, #x10000-#x10FFFF), so binary payloads must be encoded — the baseline base64-encodes them and pays the 4/3 inflation against the size limits.",
			Source: "aws-sdk-go-v2 service/sqs v1.51.0 api_op_SendMessageBatch.go:27-34",
		},
		{
			Driver: "sqs", Subject: "timers and visibility",
			Claim:  "A message timer (per-message delay) is at most 15 minutes and is not supported on FIFO queues, which only take a queue-level delay; the visibility timeout defaults to 30 seconds and cannot exceed 12 hours.",
			Source: "AWS SQS quotas documentation (message timer, visibility timeout); AWS FIFO-queue documentation",
		},
		{
			Driver: "sqs", Subject: "emulator evidence",
			Claim:  "ElasticMQ and LocalStack are functional stand-ins, not performance evidence: baseline numbers taken against them describe the client and the container, never the managed service.",
			Source: "conformance/harness/brokers.go ElasticMQ, LocalStack; the performance budgets layer 4",
		},

		// ---- SNS + SQS -------------------------------------------------
		{
			Driver: "snssqs", Subject: "fan-out shape",
			Claim:  "SNS-to-SQS fan-out is measured through the SQS side: the publish boundary is the SNS Publish response, the delivery boundary is the subscribing queue, and a subscription filter policy is evaluated by SNS on message attributes (or on the body with FilterPolicyScope MessageBody), so a driver that maps headers to attributes changes what the filter can see.",
			Source: "AWS SNS message-filtering documentation; conformance/harness/brokers.go LocalStack",
		},

		// ---- Pulsar ----------------------------------------------------
		{
			Driver: "pulsar", Subject: "producer batching",
			Claim:  "The Go client batches by default — 10 ms BatchingMaxPublishDelay, 1000 messages, 128 KiB — so a synchronous Send measures the batch delay unless batching is disabled; the baseline disables it for the per-message benchmarks and keeps it for the batched ones.",
			Source: "pulsar-client-go v0.21.0 pulsar/producer.go:141-165",
		},
		{
			Driver: "pulsar", Subject: "message size",
			Claim:  "The broker advertises its maximum message size during the connection handshake and the client falls back to the 5 MiB default when it does not, so payload limits are negotiated per connection rather than fixed by the client.",
			Source: "pulsar-client-go v0.21.0 pulsar/internal/connection.go:337-342",
		},
		{
			Driver: "pulsar", Subject: "retry topics and transactions",
			Claim:  "Dead-letter and retry topics are a client-side policy (ConsumerOptions.DLQ, DLQPolicy), not a broker feature, and transactions are supported by the Go client but must be enabled on both the client and the broker.",
			Source: "pulsar-client-go v0.21.0 pulsar/consumer.go:69-101,183-185; pulsar/client.go ClientOptions.EnableTransaction",
		},
		{
			Driver: "pulsar", Subject: "ordering",
			Claim:  "Key_Shared subscriptions keep per-key order only while the consumer set is stable: a new consumer takes over hash ranges, and messages already delivered to the previous owner may be redelivered out of order relative to the new owner's, so ordering claims must be scoped to a stable subscription.",
			Source: "Pulsar Key_Shared documentation",
		},
	}
}

// FactsFor returns the facts recorded on one driver page.
func FactsFor(driver string) []Fact {
	var out []Fact
	for _, f := range Facts() {
		if f.Driver == driver {
			out = append(out, f)
		}
	}
	return out
}
