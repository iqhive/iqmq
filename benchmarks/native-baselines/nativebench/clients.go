package nativebench

import "strings"

// Client describes one native-client baseline: which client library at which
// pinned version is measured, against which harness broker, and where the
// facts behind it are recorded.
type Client struct {
	// Slug is the benchmark path segment and the package directory name.
	Slug string
	// Module is the Go module path of the native client.
	Module string
	// Version is the version pinned in go.mod. TestPinnedVersions keeps the
	// two in step: a baseline number is only meaningful next to the client
	// version that produced it (native comparison rules).
	Version string
	// Broker is the conformance/harness broker name the baseline runs
	// against; it must be a key of harness.Images.
	Broker string
	// Driver is the docs/drivers/<Driver>.md page that records this client's
	// version policy and the facts verified for it.
	Driver string
	// Confirmation is the publish confirmation boundary the Publish and
	// PublishBatch benchmarks wait for. Never compare a baseline against a
	// driver measured at a weaker boundary.
	Confirmation string
}

// Clients returns every baseline in this module, in benchmark order.
func Clients() []Client {
	return []Client{
		{
			Slug: "franzgo", Module: "github.com/twmb/franz-go", Version: "v1.22.1",
			Broker: "redpanda", Driver: "kafka",
			Confirmation: "acks=all on an idempotent producer (client default), ProduceSync",
		},
		{
			Slug: "kafkago", Module: "github.com/segmentio/kafka-go", Version: "v0.4.51",
			Broker: "redpanda", Driver: "kafka",
			Confirmation: "RequiredAcks=RequireAll set explicitly, synchronous WriteMessages",
		},
		{
			Slug: "sarama", Module: "github.com/IBM/sarama", Version: "v1.61.1",
			Broker: "redpanda", Driver: "kafka",
			Confirmation: "Producer.RequiredAcks=WaitForAll with Idempotent=true, SyncProducer",
		},
		{
			Slug: "nats", Module: "github.com/nats-io/nats.go", Version: "v1.54.0",
			Broker: "nats", Driver: "nats",
			Confirmation: "core NATS at-most-once: server round-trip via Flush, no store",
		},
		{
			Slug: "jetstream", Module: "github.com/nats-io/nats.go", Version: "v1.54.0",
			Broker: "nats", Driver: "jetstream",
			Confirmation: "JetStream PubAck from a file-backed stream (R1)",
		},
		{
			Slug: "nsq", Module: "github.com/nsqio/go-nsq", Version: "v1.1.0",
			Broker: "nsq", Driver: "nsq",
			Confirmation: "nsqd OK response to PUB/MPUB (memory-backed until mem-queue-size)",
		},
		{
			Slug: "rueidis", Module: "github.com/redis/rueidis", Version: "v1.0.78",
			Broker: "redis", Driver: "redis",
			Confirmation: "XADD reply from the server (appendfsync everysec, no WAIT)",
		},
		{
			Slug: "goredis", Module: "github.com/redis/go-redis/v9", Version: "v9.23.0",
			Broker: "redis", Driver: "redis",
			Confirmation: "XADD reply from the server (appendfsync everysec, no WAIT)",
		},
		{
			Slug: "amqp091", Module: "github.com/rabbitmq/amqp091-go", Version: "v1.15.0",
			Broker: "rabbitmq", Driver: "rabbitmq",
			Confirmation: "publisher confirm (basic.ack) on a durable quorum queue",
		},
		{
			Slug: "goamqp", Module: "github.com/Azure/go-amqp", Version: "v1.7.0",
			Broker: "artemis", Driver: "activemq",
			Confirmation: "AMQP 1.0 settled disposition from the broker (Sender.Send)",
		},
		{
			Slug: "pubsub", Module: "cloud.google.com/go/pubsub/v2", Version: "v2.7.0",
			Broker: "pubsub", Driver: "gcppubsub",
			Confirmation: "PublishResult.Get: server-assigned message ID (emulator, not evidence for managed Pub/Sub)",
		},
		{
			Slug: "sqs", Module: "github.com/aws/aws-sdk-go-v2/service/sqs", Version: "v1.52.2",
			Broker: "elasticmq", Driver: "sqs",
			Confirmation: "SendMessage/SendMessageBatch response (ElasticMQ, not evidence for managed SQS)",
		},
		{
			Slug: "pulsar", Module: "github.com/apache/pulsar-client-go", Version: "v0.21.0",
			Broker: "pulsar", Driver: "pulsar",
			Confirmation: "broker send receipt (Producer.Send), persistent topic",
		},
	}
}

// ClientBySlug returns the baseline with the given slug.
func ClientBySlug(slug string) (Client, bool) {
	for _, c := range Clients() {
		if c.Slug == slug {
			return c, true
		}
	}
	return Client{}, false
}

// Pin renders the client as "module@version", the form the driver pages and
// the benchmark reports record.
func (c Client) Pin() string { return c.Module + "@" + c.Version }

// List renders the registry as "slug=module@version,..." for TestClientsList
// and the bench-native report header.
func List() string {
	var sb strings.Builder
	for i, c := range Clients() {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(c.Slug)
		sb.WriteByte('=')
		sb.WriteString(c.Pin())
	}
	return sb.String()
}
