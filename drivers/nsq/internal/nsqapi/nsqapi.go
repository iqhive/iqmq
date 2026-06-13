// Package nsqapi is the narrow slice of go-nsq this driver uses (the specification's `internal/client/{iface,real,fake}.go`).
//
// Two halves are shaped differently on purpose. The Producer interface takes a
// context, which go-nsq's `*nsq.Producer` does not: the driver's sink has a
// context and go-nsq has an asynchronous form, so turning one into the other is
// the real client's job and not every caller's. The Consumer interface is
// go-nsq's own shape, because `*nsq.Message` is the settlement handle — `FIN`,
// `REQ` and `TOUCH` are methods on it, dispatched through a
// `nsq.MessageDelegate` — and that delegate is precisely the seam a fake needs.
package nsqapi

import (
	"context"
	"time"

	gonsq "github.com/nsqio/go-nsq"
)

// Message is one delivery. It is go-nsq's own type, because the settlement
// verbs are methods on it.
type Message = gonsq.Message

// Handler is what a consumer calls for each delivery. It is go-nsq's own
// interface so that a real `*nsq.Consumer` accepts the driver's handler
// unchanged.
type Handler = gonsq.Handler

// Producer is the publishing half: every verb this driver sends to nsqd, with a
// context bounding each one.
//
// The three publishing verbs are separate rather than one call with options,
// because they are three separate protocol commands with three separate
// limits — nsqd bounds a single PUB body by `--max-msg-size` and a whole MPUB
// by `--max-body-size`, and DPUB additionally by `--max-req-timeout`.
type Producer interface {
	// Publish sends one PUB and waits for nsqd's answer.
	//
	// A context that is done before the answer arrives makes the outcome
	// AMBIGUOUS and not a failure: the frame is already on the wire, and
	// nsqd may well have taken it.
	Publish(ctx context.Context, topic string, body []byte) error
	// MultiPublish sends one MPUB carrying every body and waits for nsqd's
	// answer. It has the same ambiguity on a cancelled context.
	MultiPublish(ctx context.Context, topic string, bodies [][]byte) error
	// DeferredPublish sends one DPUB: nsqd holds the message at the channel
	// level until delay has passed.
	DeferredPublish(ctx context.Context, topic string, delay time.Duration, body []byte) error
	// Ping connects if necessary and sends a NOP, which is the cheap
	// liveness check: it never publishes and never creates a channel.
	Ping(ctx context.Context) error
	// Close stops the producer and waits for its goroutines.
	Close()
	// Addr is the nsqd this producer publishes to, for diagnostics.
	Addr() string
}

// Consumer is the consuming half: one nsqd channel subscription.
//
// It is built already bound to a topic and a channel, because go-nsq's
// `NewConsumer` is, and it panics if handlers are added after it has connected —
// so the driver's order of operations (add handlers, then connect) is part of
// the contract rather than an accident.
type Consumer interface {
	// AddConcurrentHandlers starts n handler goroutines. It must be called
	// before any Connect.
	AddConcurrentHandlers(h Handler, n int)
	// ConnectToNSQLookupds discovers nsqd instances through nsqlookupd and
	// keeps following it.
	ConnectToNSQLookupds(addrs []string) error
	// ConnectToNSQDs connects directly to the named nsqd instances.
	ConnectToNSQDs(addrs []string) error
	// Stop begins a graceful stop; StopChan is closed when it finishes.
	Stop()
	// StopChan is closed once the consumer has stopped.
	StopChan() <-chan int
}

// ConsumerFactory is optionally implemented by a [Producer] that can also
// provide the consuming half.
//
// A caller who hands this driver a client of their own through
// `OpenWithClient` has replaced the transport, not merely the publishing side:
// an instrumented client, a proxy, or the in-process model this driver's tests
// run against all speak both halves. When the producer implements this, the
// backend takes its consumers from here too; when it does not, the backend
// builds real go-nsq consumers against the configured addresses.
//
// It is an optional interface rather than a second parameter because the
// common case — a real `*nsq.Producer` — implements only the publishing half,
// and a constructor that demanded both would make the ordinary escape hatch
// harder to use than the unusual one.
type ConsumerFactory interface {
	// Consumer returns a consumer bound to one topic and channel.
	Consumer(topic, channel string, maxInFlight int, deadline time.Duration) Consumer
}
