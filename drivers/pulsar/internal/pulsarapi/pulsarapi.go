// Package pulsarapi is the narrow slice of pulsar-client-go this driver uses
// (the specification's `internal/client/{iface,real,fake}.go`).
//
// It is thinner than the other drivers' seams because pulsar-client-go's own
// surface is already interfaces: [github.com/apache/pulsar-client-go/pulsar.Producer],
// [github.com/apache/pulsar-client-go/pulsar.Consumer],
// [github.com/apache/pulsar-client-go/pulsar.Message] and
// [github.com/apache/pulsar-client-go/pulsar.MessageID] are all interface
// types, so an in-process model can be substituted for any of them without a
// wrapper. What this package adds is the one type that is NOT an interface in
// the library — the client — plus the narrowing: this driver never creates a
// reader or a table view, so a fake does not have to pretend to.
//
// # Why a fake at all
//
//   - a partitioned topic that GAINS partitions while a consumer is attached;
//   - a Key_Shared subscription reassigning hash ranges as consumers join;
//   - an entry holding a BATCH of messages, of which the application settles
//     one — the case where an acknowledgement can settle a sibling message;
//   - a message redelivered while the previous delivery's handle is still held.
//
// The fake in [bitbucket.org/iqhive/iqmq/drivers/pulsar/pulsarfake] produces all
// four in microseconds. It reproduces the client library's own
// batch-acknowledgement bookkeeping as well as the broker's, because that
// bookkeeping is where the third item lives: pulsar-client-go tracks which
// indices of a batched entry have been acknowledged inside the MessageID it
// handed out, and an acknowledgement carrying an identifier that lost that
// tracker acknowledges the WHOLE entry. A fake at the socket could not see it.
package pulsarapi

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Client is the publishing and consuming factory: pulsar.Client without the
// reader and table-view surfaces this driver does not use.
//
// The real *pulsar.Client satisfies it unchanged, which is the point: the
// escape hatch a caller reaches for is the library's own type.
type Client interface {
	// CreateProducer opens a producer on one topic.
	CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error)
	// Subscribe opens a consumer on one topic, a list of topics or a
	// pattern.
	Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error)
	// TopicPartitions is how many partitions a topic has, as the partition
	// topic names. It is a lookup and not a subscription, which is what
	// makes it safe for a liveness probe (the API).
	TopicPartitions(topic string) ([]string, error)
	// NewTransaction begins a transaction, for x/pulsartx. It fails unless
	// the client was built with transactions enabled.
	NewTransaction(duration time.Duration) (pulsar.Transaction, error)
	// Close releases the client and every producer and consumer it made.
	Close()
}

// The library types this driver passes around, aliased so that the driver reads
// as one vocabulary and a reader can see at a glance which library types cross
// the seam.
type (
	// Producer is one topic's producer.
	Producer = pulsar.Producer
	// Consumer is one subscription.
	Consumer = pulsar.Consumer
	// Message is one received message.
	Message = pulsar.Message
	// MessageID is a message's position. It is the SETTLEMENT HANDLE, and
	// the identity of the value matters: see [pulsarapi.Client]'s package
	// documentation and the driver's own acks.go.
	MessageID = pulsar.MessageID
	// ConsumerMessage is what a consumer's channel carries.
	ConsumerMessage = pulsar.ConsumerMessage
	// ProducerMessage is one publication.
	ProducerMessage = pulsar.ProducerMessage
	// Transaction is one Pulsar transaction, for x/pulsartx.
	Transaction = pulsar.Transaction
)

// BatchTracker is optionally implemented by a [Consumer] that can say whether
// the message identifiers it hands out still carry the client library's
// per-entry batch bookkeeping.
//
// pulsar-client-go v0.21's own consumer hands out `*trackingMessageID` values,
// which do carry it, and implements nothing here — so a driver that finds this
// interface absent reads "tracked", which is a statement about the pinned
// client version rather than an assumption about every client.
//
// It exists because the ALTERNATIVE is unobservable and the consequence is a
// lost message. An identifier that has been through Serialize and
// DeserializeMessageID, or built with NewMessageID, looks identical through the
// MessageID interface — same ledger, same entry, same batch index, same batch
// size — and acknowledging one message of a batch through it settles every
// sibling in the entry. There is no way to ask the
// library which kind of value it gave you, so the rule cannot be tested against
// the real client at all; this is the seam
// [bitbucket.org/iqhive/iqmq/drivers/pulsar/pulsarfake] uses to make the driver's
// refusal happen on purpose.
type BatchTracker interface {
	// BatchTracked reports whether this consumer's identifiers carry the
	// batch bookkeeping.
	BatchTracked() bool
}

// Compile-time proof that the library's client is this seam. A method added to
// pulsar.Client is not a build failure here — the narrowing is deliberate — but
// a method REMOVED from it, or changed, is.
var _ Client = pulsar.Client(nil)
