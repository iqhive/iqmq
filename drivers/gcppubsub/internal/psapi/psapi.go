// Package psapi is the slice of a Google Cloud Pub/Sub client library this
// driver is built over.
//
// It exists because the specification is a bake-off between two
// CLIENT GENERATIONS — `cloud.google.com/go/pubsub/v2` and the deprecated v1
// API behind `drivers/gcppubsub/v1compat` — and a bake-off needs both arms
// behind one contract. Everything above this interface (the capability table,
// the metadata mapping, the completion pool, the push adapter, the settlement
// state machine, the admin surface) is written once and measured twice.
//
// The shapes here are the driver's, not the library's. A published `*Incoming`
// carries what need and nothing else, with the library's own
// message reachable through [Incoming.Native] for
// [github.com/iqhive/iqmq.DriverDelivery.Native]. That is deliberate: the two
// generations happen to alias the same message type today, and a contract that
// leaned on the coincidence would break on the day one of them stops.
package psapi

import (
	"context"
	"errors"
	"time"
)

// Client is one client generation's whole surface, as this driver uses it.
//
// Close releases what the client owns. Whether the driver calls it at all is
// [github.com/iqhive/iqmq/driverkit.Ownership]'s decision, not this
// interface's.
type Client interface {
	// Project is the Google Cloud project the client is bound to. It is
	// reported through DriverInfo and is what a fully qualified topic name is
	// built from.
	Project() string
	// Generation names the client library this implementation is, which is the arm ("v2" or "v1compat") and DriverInfo.Profile.
	Generation() string
	// ClientModule and ClientVersion are the native client's module path and
	// pinned version, for DriverInfo.
	ClientModule() string
	ClientVersion() string
	// Publisher returns a publisher for one topic. It performs no I/O: the
	// library's publisher is a handle, and the first RPC is the first
	// publication.
	Publisher(cfg PublisherConfig) (Publisher, error)
	// Subscription returns a subscription handle. It performs no I/O either.
	Subscription(cfg SubscriptionConfig) (Subscription, error)
	// Admin is the topic and subscription administration surface.
	Admin() Admin
	// Native is what Backend.Native answers: the library's own client.
	Native() any
	Close() error
}

// PublisherConfig is everything a publisher is built with. The zero value of
// each threshold means the library's own default.
type PublisherConfig struct {
	// Topic is the topic ID, not the fully qualified name; the
	// implementation qualifies it with the project.
	Topic string
	// Ordering is the library's EnableMessageOrdering. It is a construction
	// flag rather than a per-publication one, which is the whole reason
	// FeatureOrderedByKey is CONDITIONAL for this driver: a publisher built
	// without it refuses every publication that carries an ordering key.
	Ordering bool

	CountThreshold int
	ByteThreshold  int
	DelayThreshold time.Duration
	// Timeout bounds one publish RPC inside the library's bundler.
	Timeout time.Duration
	// NumGoroutines is how many bundler goroutines the library runs.
	NumGoroutines int
	// FlowMaxOutstandingMessages and FlowMaxOutstandingBytes are the
	// library's publish-side flow control. Zero means the library's default;
	// a negative value means unbounded, which this driver never asks for.
	FlowMaxOutstandingMessages int
	FlowMaxOutstandingBytes    int
	// Compression is the library's EnableCompression and its threshold.
	Compression          bool
	CompressionThreshold int
}

// Publisher is one topic's publication handle.
//
// Publish NEVER blocks on the server: the library bundles, and the
// [Result] is what the publication's outcome arrives through.
type Publisher interface {
	Publish(ctx context.Context, m *Outgoing) Result
	// ResumePublish re-enables publication for one ordering key after a
	// failure.
	//
	// It is the most consequential call in this interface. When a publication
	// carrying an ordering key fails, the library puts that key into a
	// permanently failed state and every LATER publication of the same key is
	// rejected with an error that names the pause — for the life of the
	// publisher, silently as far as a caller reading only the first failure is
	// concerned. That is the "message loss under documented
	// configuration" in slow motion, so this driver resumes the key on every
	// ordered publication that fails.
	ResumePublish(orderingKey string)
	// Flush waits for every buffered publication to complete.
	Flush()
	// Stop flushes and releases the publisher's goroutines. A publisher is
	// unusable afterwards.
	Stop()
}

// Outgoing is one publication in the shape the service takes it.
type Outgoing struct {
	// Data is the message body. Pub/Sub carries it as bytes, so there is no
	// binary-body policy on this driver.
	Data []byte
	// Attributes are the message attributes. Pub/Sub attribute values are
	// STRINGS, which is what makes a binary header value need the envelope.
	Attributes map[string]string
	// OrderingKey is the publication's ordering key, empty for an unordered
	// publication.
	OrderingKey string
}

// Result is one pending publication.
//
// Get blocks until the service has answered, ctx is done, or the library has
// given up. The server-assigned message ID it returns is the broker
// identifier and is what this driver's receipt carries.
type Result interface {
	Get(ctx context.Context) (serverID string, err error)
}

// SubscriptionConfig is everything a subscription handle is built with.
type SubscriptionConfig struct {
	// Subscription is the subscription ID, not the fully qualified name.
	Subscription string
	// MaxOutstandingMessages is the library's message-count flow control and
	// is the driver's prefetch. MaxOutstandingBytes is the byte half.
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	// NumGoroutines is how many StreamingPull streams the library opens. This
	// driver asks for ONE: a second stream is a second source of
	// out-of-order delivery for an ordering key, and the bounded ring behind
	// the callback is what bounds concurrency instead.
	NumGoroutines int
	// MaxExtension is how long the library will keep extending a message's
	// ack deadline automatically. It is this driver's whole lease-extension
	// story, because the ack ID a ModifyAckDeadline needs is not reachable
	// from the library's message type.
	MaxExtension time.Duration
	// MaxDurationPerAckExtension and MinDurationPerAckExtension bound one
	// automatic extension. Zero means the library's default.
	MaxDurationPerAckExtension time.Duration
	MinDurationPerAckExtension time.Duration
}

// Subscription is one subscription's consumption handle.
type Subscription interface {
	// Receive delivers by callback until ctx is done or the service returns a
	// non-retryable error. It is what [github.com/iqhive/iqmq/driverkit.PushAdapter]
	// turns into the SPI's pull Fetch.
	//
	// The callback's RETURN is the library's flow-control release, not the
	// settlement: a callback that blocks holds a flow-control slot and the
	// stream stops asking for more, which is the only backpressure a
	// streaming pull has.
	Receive(ctx context.Context, fn func(context.Context, *Incoming)) error
}

// Incoming is one received message.
type Incoming struct {
	// ID is the service-assigned message ID.
	ID string
	// Data is the body.
	Data []byte
	// Attributes are the message attributes.
	Attributes map[string]string
	// PublishTime is when the service accepted the publication.
	PublishTime time.Time
	// OrderingKey is the publication's ordering key, empty when it had none.
	OrderingKey string
	// Attempt is DeliveryAttempt, and HasAttempt says whether the service
	// reported one at all: Pub/Sub populates it ONLY on a subscription with a
	// dead-letter policy, and the specification forbids reading an absent value as zero.
	Attempt    int
	HasAttempt bool
	// Native is the library's own message, for DriverDelivery.Native.
	Native any
	// Settle is the four settlement verbs for this message.
	Settle Settlement
}

// Settlement is one message's settlement, which is per-message on this broker
// rather than per-source: the library holds the ack ID inside the message.
//
// The two "WithResult" halves are the exactly-once path. On a subscription
// without exactly-once delivery enabled the library readies them immediately
// with success, which is why this driver only calls them when the
// subscription really has it: a success that means "the RPC was queued" and a
// success that means "the service will never redeliver this" are not the same
// answer and must not be reported as one.
type Settlement interface {
	Ack()
	Nack()
	AckWithResult(ctx context.Context) error
	NackWithResult(ctx context.Context) error
}

// Admin is the topic and subscription administration this driver needs.
//
// It is deliberately narrow. The specification keeps the portable admin to the intersection
// and leaves everything else to the driver's own typed surface, and every
// method here is one the portable [github.com/iqhive/iqmq.Admin] or one of this
// driver's conditional capabilities needs.
type Admin interface {
	CreateTopic(ctx context.Context, topic string) error
	DeleteTopic(ctx context.Context, topic string) error
	TopicExists(ctx context.Context, topic string) (bool, error)
	// ListTopics calls fn for each topic ID until fn returns false.
	ListTopics(ctx context.Context, fn func(topic string) bool) error
	CreateSubscription(ctx context.Context, spec SubscriptionSpec) error
	DeleteSubscription(ctx context.Context, sub string) error
	// GetSubscription reads a subscription's configuration back. It is what
	// answers "does this subscription have exactly-once delivery" and "does
	// it have a dead-letter policy", both of which decide a capability claim
	// and neither of which a driver may assume.
	GetSubscription(ctx context.Context, sub string) (SubscriptionInfo, error)
	// Seek moves a subscription's acknowledgement state to a timestamp.
	Seek(ctx context.Context, sub string, to time.Time) error
	// Purge discards everything a subscription has not acknowledged, which on
	// this broker is a Seek to now.
	Purge(ctx context.Context, sub string) error
}

// TopicRetainer is a topic's message retention: creating a topic with it and
// reading it back. Config.TopicRetention, StartEarliest on a new subscription
// and StartAt without RetainAcked need it.
//
// It is an interface of its own, beside [Admin] rather than in it, for the
// compatibility of a published module and for no other reason.
// drivers/gcppubsub/v1compat implements [Admin] from another module, and its
// releases up to v1.1.1 have neither method: had they been added to [Admin],
// a deployment that upgraded drivers/gcppubsub and kept such a v1compat would
// stop compiling inside v1compat. The driver asserts this interface once, at
// open, and refuses what needs it, naming the v1compat upgrade, when the arm
// does not implement it. The v2 arm, the scripted fake and v1compat from the
// release that ships with it all do.
type TopicRetainer interface {
	// CreateRetainedTopic creates one topic with messageRetentionDuration set
	// to retention, so that the topic keeps every message for that long
	// whatever its subscriptions acknowledge, and a subscription created
	// later can seek back to messages published before it existed.
	CreateRetainedTopic(ctx context.Context, topic string, retention time.Duration) error
	// TopicRetention reads a topic's messageRetentionDuration back, zero
	// when the topic has none. A topic that does not exist is ErrNotFound.
	// It is read from the service rather than from the configuration because
	// a topic provisioned elsewhere may have it.
	TopicRetention(ctx context.Context, topic string) (time.Duration, error)
}

// SubscriptionSpec is a subscription to create.
type SubscriptionSpec struct {
	Subscription string
	Topic        string
	// AckDeadline is ackDeadlineSeconds. The service takes whole seconds.
	AckDeadline time.Duration
	// Filter is a server-side filter expression, empty for none.
	Filter string
	// ExactlyOnce is enableExactlyOnceDelivery.
	ExactlyOnce bool
	// Ordering is enableMessageOrdering, which a subscription needs before the
	// service will deliver an ordered key in order.
	Ordering bool
	// RetainAcked and Retention are retainAckedMessages and
	// messageRetentionDuration, which is what a replay or a seek reads.
	RetainAcked bool
	Retention   time.Duration
	// DeadLetterTopic and MaxDeliveryAttempts are the dead-letter policy.
	// MaxDeliveryAttempts is also what makes DeliveryAttempt present on a
	// delivery at all.
	DeadLetterTopic     string
	MaxDeliveryAttempts int
}

// SubscriptionInfo is a subscription's configuration as the service reports it.
type SubscriptionInfo struct {
	Exists              bool
	Topic               string
	AckDeadline         time.Duration
	Filter              string
	ExactlyOnce         bool
	Ordering            bool
	RetainAcked         bool
	Retention           time.Duration
	DeadLetterTopic     string
	MaxDeliveryAttempts int
}

// ErrNotFound is what an implementation returns for a topic or subscription
// that does not exist, so that the driver's error mapping does not have to
// know one library's status codes from another's.
var ErrNotFound = errors.New("psapi: not found")

// ErrAlreadyExists is the converse, which makes an Ensure idempotent without
// a read first.
var ErrAlreadyExists = errors.New("psapi: already exists")

// ErrOrderingPaused is what a publication reports when the library has paused
// an ordering key after an earlier failure. It is mapped to a retryable error
// and the key is resumed, because the alternative is a publisher that has
// silently stopped carrying that key.
var ErrOrderingPaused = errors.New("psapi: publishing is paused for this ordering key")

// ErrAckIDNotReachable is what an implementation returns from a settlement the
// service rejected because the ack ID is no longer valid — an expired lease, or
// a message another delivery now owns.
var ErrAckIDNotReachable = errors.New("psapi: the acknowledgement identifier is no longer valid")
