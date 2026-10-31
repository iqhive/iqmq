// Package client is the seam between the RabbitMQ driver and
// github.com/rabbitmq/amqp091-go.
//
// It exists so that the driver's behaviour can be tested without a broker, and
// its shape is chosen for one reason: what the driver has to get right is not
// the AMQP 0-9-1 wire format — the library does that — but the way the LIBRARY
// hands the protocol over. The specification recorded the general form of that lesson: a
// driver's rules are usually tested by feeding it what a SERVER sends, and what
// reaches the driver is what a CLIENT LIBRARY hands over. On this library that
// difference is large and it is where the driver's hardest cases live:
//
//   - **Cumulative confirms are expanded.** `basic.ack{multiple=true}` never
//     reaches the driver: `confirms.Multiple` turns it into one
//     [amqp091.Confirmation] per delivery tag, RESEQUENCED into tag order, on
//     the `NotifyPublish` channel. So the driver sees per-tag confirms, in
//     order, and `internal/rabbit/confirm`'s cumulative path is exercised by
//     the AMQP 1.0 profile's disposition ranges rather than by this driver.
//   - **A channel close NACKS what was outstanding.**
//     `deferredConfirmations.Close` resolves every pending confirmation with
//     `ack=false`, and the `NotifyPublish` channel is CLOSED with
//     out-of-sequence confirmations discarded. A driver that read either of
//     those as a definite negative would report a failure for a publication
//     the broker may have taken, which is the ambiguity and the specification's
//     settlement disqualifier. TestCriticalChannelCloseDuringConfirm is the
//     evidence.
//   - **A notification can be DROPPED.** `notifyAll` abandons any send that
//     blocks for more than five seconds, so a driver that is slow to read its
//     return channel loses the `basic.return` and reports an unroutable
//     publication as delivered. That is why the driver's confirm loop does
//     bookkeeping only.
//   - **`basic.return` precedes `basic.ack` on the wire and arrives on a
//     different Go channel.** The library's reader dispatches frames in wire
//     order and blocks on the send, so an UNBUFFERED return channel is what
//     makes the ordering observable; with both channels buffered a `select`
//     may take the ack first and report a returned message as confirmed.
//
// The interfaces carry the library's own value types — [amqp091.Publishing],
// [amqp091.Delivery], [amqp091.Return], [amqp091.Confirmation],
// [amqp091.Table] — deliberately. They are the protocol's own shapes and
// re-declaring them would add a translation layer whose only effect would be
// to let the fake and the real client disagree about the thing the fake exists
// to reproduce. What is abstracted is BEHAVIOUR: dialling, a channel, and the
// notification streams.
package client

import (
	"context"
	"crypto/tls"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Dialer opens a connection. It is the only entry point the driver uses, so a
// test supplies a fake dialler and gets a whole client with no broker.
type Dialer interface {
	// Dial opens a connection to one of the URLs, trying them in order, and
	// returns the first that answers.
	Dial(ctx context.Context, urls []string, cfg DialConfig) (Conn, error)
}

// DialConfig is what a dial needs beyond the URL.
type DialConfig struct {
	// Vhost overrides the virtual host in the URL. Empty keeps the URL's.
	Vhost string
	// Heartbeat is the AMQP heartbeat interval. Zero uses the server's.
	Heartbeat time.Duration
	// Timeout bounds one dial attempt.
	Timeout time.Duration
	// ChannelMax and FrameSize are the connection tuning parameters.
	ChannelMax uint16
	FrameSize  int
	// TLS is the client TLS configuration, or nil for a plaintext dial.
	TLS *tls.Config
	// Properties is the client-properties table the connection advertises.
	Properties amqp.Table
}

// Conn is one AMQP connection.
type Conn interface {
	// Channel opens a channel on this connection.
	Channel() (Channel, error)
	// NotifyClose registers a listener for the connection's own close.
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	// NotifyBlocked registers a listener for `connection.blocked` and
	// `connection.unblocked`, which is how RabbitMQ tells a publisher that
	// the broker is under resource pressure (NotifyBlocked).
	NotifyBlocked(c chan amqp.Blocking) chan amqp.Blocking
	// IsClosed reports whether the connection has closed.
	IsClosed() bool
	// Close closes the connection and every channel on it. It waits for the
	// broker's `connection.close-ok` with no bound.
	Close() error
	// CloseDeadline is Close with the close handshake bounded by deadline,
	// which is the library's socket deadline. A broker under a resource
	// alarm has stopped reading the connection and never answers, so this is
	// the close that returns. The connection is closed whatever it reports.
	CloseDeadline(deadline time.Time) error
	// ServerProperties is the `connection.start` table, which is where the
	// broker's version comes from.
	ServerProperties() amqp.Table
}

// ChannelAlias is [Channel] under a second name, for a driver file that needs
// the type in a struct field without importing the package twice under two
// spellings. It is an alias and not a type: they are the same interface.
type ChannelAlias = Channel

// Channel is one AMQP channel.
//
// Every method is the library's own, with the context-taking form preferred
// where the library has one: a publication or a consume that ignored the
// caller's context would be an operation the specification cannot cancel.
type Channel interface {
	// Confirm puts the channel into publisher-confirm mode.
	Confirm(noWait bool) error
	// Qos sets the prefetch (Qos(prefetch)).
	Qos(prefetchCount, prefetchSize int, global bool) error

	// ExchangeDeclare declares an exchange.
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	// ExchangeDeclarePassive asserts that an exchange exists without
	// creating it, which is how the specification's "no hidden creation" is made
	// checkable for a publisher: an exchange nobody declared is a topic this
	// client cannot use, and saying so at open is the only way a
	// ConfirmNone publication ever learns it.
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	// ExchangeDelete deletes one.
	ExchangeDelete(name string, ifUnused, noWait bool) error
	// QueueDeclare declares a queue and reports its message and consumer
	// counts.
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	// QueueDeclarePassive asserts that a queue exists without creating it,
	// which is the specification's "no hidden creation" made checkable.
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	// QueueBind binds a queue to an exchange.
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	// QueueDelete deletes a queue and reports how many messages went with
	// it.
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	// QueuePurge empties a queue.
	QueuePurge(name string, noWait bool) (int, error)

	// PublishWithContext publishes one message.
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	// GetNextPublishSeqNo is the delivery tag the next publication on this
	// channel will be given.
	//
	// The driver does not use it to assign tags -- `confirm.Tracker` does
	// that, under the lock that also serialises the publish -- and it is on
	// the seam so that a test can assert the two counters agree. They can
	// only disagree if something published on this channel outside the
	// driver's lock, which would mis-attribute every confirm after it.
	GetNextPublishSeqNo() uint64

	// ConsumeWithContext starts a consumer and returns its delivery stream.
	ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	// Cancel stops a consumer by tag.
	Cancel(consumer string, noWait bool) error

	// Ack, Nack and Reject are the three settlement frames.
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Reject(tag uint64, requeue bool) error

	// NotifyClose registers a listener for this channel's close, which is
	// where a channel exception arrives.
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	// NotifyReturn registers a listener for `basic.return`: a `mandatory`
	// publication no queue was bound for. The channel should be UNBUFFERED;
	// see the package comment.
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	// NotifyCancel registers a listener for a consumer the BROKER cancelled,
	// which happens when the queue it was consuming is deleted.
	NotifyCancel(c chan string) chan string
	// NotifyPublish registers a listener for publisher confirms, expanded
	// per tag and resequenced by the library.
	NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation
	// NotifyFlow registers a listener for `channel.flow`, RabbitMQ's
	// per-channel back-pressure signal.
	NotifyFlow(c chan bool) chan bool

	// Close closes the channel.
	Close() error
	// IsClosed reports whether the channel has closed.
	IsClosed() bool
}
