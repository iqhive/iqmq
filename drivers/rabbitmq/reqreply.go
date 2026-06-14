package rabbitmq

import (
	"context"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
)

// ReplyAddress is RabbitMQ's direct reply-to pseudo-queue (the specification's "direct
// reply-to for native request/reply").
//
// A client consumes from this name with `no-ack` and publishes a request whose
// `reply_to` is this name; the broker substitutes a per-request address of its
// own, the responder publishes the reply to THAT address through the default
// exchange, and the broker routes it back over the requester's own
// consumer. There is no queue, no declaration and no cleanup: the whole
// exchange costs one consumer per connection and nothing per request, which is
// what makes it worth having over the portable emulation.
const ReplyAddress = "amq.rabbitmq.reply-to"

// IsReplyAddress reports whether a topic is a direct reply-to address.
//
// It exists because a reply address is a TOPIC as far as the core is
// concerned — a responder publishes to `Delivery.Properties.ReplyTo` — and it
// is not a topic at all on this broker: it is a routing key on the default
// exchange, under the reserved `amq.` prefix that `internal/rabbit` refuses to
// derive a name from. So the sink recognises it and routes around the whole
// naming layer, which is the one special case in this driver's publish path
// and is documented on the driver page.
func IsReplyAddress(topic string) bool {
	if topic == ReplyAddress {
		return true
	}
	const prefix = ReplyAddress + "."
	return len(topic) > len(prefix) && topic[:len(prefix)] == prefix
}

// requester is this driver's direct reply-to endpoint: one channel, one
// consumer, one correlation table, any number of concurrent requests.
type requester struct {
	b *Backend

	mu sync.Mutex
	// ch is the reply consumer's channel, opened on the first request and
	// closed with the backend.
	ch client.ChannelAlias
	// waiting maps a correlation identifier to the request waiting for its
	// reply.
	waiting map[string]chan *mq.Message
	// started reports whether the consumer is up, so that a second request
	// does not open a second one.
	started bool
	seq     uint64
}

// RequestNative sends m to topic and waits for the reply, over direct
// reply-to.
//
// `reqreply.New` finds this by type-asserting what `mq.Client.Native` returns,
// which is the escape hatch ADR-003 already provides: the root package does
// not grow an SPI method for a feature four drivers have and eight do not.
//
// A native implementation owns the whole exchange, including the reply
// topology, the correlation and the timeout — so nothing in `reqreply`'s own
// correlation table is used on this path, and no reply topic or subscription
// is created. That is the whole reason the specification has a native path: the portable
// emulation creates a reply topic and a subscription on it, which on a managed
// broker is a resource with a cost, a quota and a lifetime.
func (b *Backend) RequestNative(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errNilRequest
	}
	rq, err := b.requesterFor()
	if err != nil {
		return nil, err
	}
	return rq.request(ctx, topic, m)
}

// requesterFor returns the backend's reply endpoint, starting it on first use.
//
// It is lazy because most clients never make a request, and a consumer on
// `amq.rabbitmq.reply-to` that nobody uses is a consumer in every broker's
// management view for no reason.
func (b *Backend) requesterFor() (*requester, error) {
	b.rqOnce.Do(func() { b.rq = &requester{b: b, waiting: map[string]chan *mq.Message{}} })
	if err := b.rq.start(); err != nil {
		return nil, err
	}
	return b.rq, nil
}

// start opens the reply consumer if it is not already up.
func (r *requester) start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	ch, err := r.b.channel()
	if err != nil {
		return err
	}
	// `no-ack` is mandatory: the broker refuses a consumer on the direct
	// reply-to pseudo-queue that asks for explicit settlement, because there
	// is no queue to hold an unacknowledged reply in. A reply is therefore
	// at-most-once, which is the right guarantee for one — a reply nobody is
	// waiting for any more is a reply nobody wants.
	// The backend's lifetime context, for [source.attach]'s reason: the
	// reply consumer outlives the request that started it and serves every
	// later request on this client.
	replies, err := ch.ConsumeWithContext(r.b.ctx, ReplyAddress, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return r.b.errs.Map(mq.OpFetch, ReplyAddress, "", err)
	}
	r.ch, r.started = ch, true
	r.b.wg.Add(1)
	go r.pump(replies)
	return nil
}

// pump dispatches replies to the requests waiting for them.
func (r *requester) pump(replies <-chan amqp.Delivery) {
	defer r.b.wg.Done()
	var (
		msg    mq.Message
		fields protocol.Fields
	)
	for {
		select {
		case d, ok := <-replies:
			if !ok {
				r.stop()
				return
			}
			// The reply's storage is copied out, because the waiter reads it
			// after this loop has moved on and the library's buffer is
			// reused: the borrowing rule runs out at the end of this
			// iteration, and there is no settlement to extend it.
			if _, err := decode(r.b.proto, &msg, &fields, &d, r.b.options(mq.MetadataNativeStrict), "", false, r.b.clock.Now()); err != nil {
				continue
			}
			r.deliver(d.CorrelationId, msg.Clone())
		case <-r.b.ctx.Done():
			return
		}
	}
}

// deliver hands one reply to its waiter, or drops it.
//
// A reply with no waiter is DROPPED, and silently: it is a reply whose
// requester gave up, which is the ordinary outcome of a timeout, and there is
// nobody left to report it to.
func (r *requester) deliver(id string, m *mq.Message) {
	r.mu.Lock()
	wait := r.waiting[id]
	delete(r.waiting, id)
	r.mu.Unlock()
	if wait == nil {
		return
	}
	select {
	case wait <- m:
	default:
	}
}

// stop tears the endpoint down, so that a lost channel is re-opened on the
// next request rather than leaving every request to time out.
func (r *requester) stop() {
	r.mu.Lock()
	r.started = false
	r.ch = nil
	waiters := make([]chan *mq.Message, 0, len(r.waiting))
	for id, w := range r.waiting {
		waiters = append(waiters, w)
		delete(r.waiting, id)
	}
	r.mu.Unlock()
	for _, w := range waiters {
		// A nil reply is how a waiter is told the endpoint went: it reports
		// CodeUnavailable rather than waiting for its own deadline, which is
		// the difference between a request that failed in a second and one
		// that failed in thirty.
		select {
		case w <- nil:
		default:
		}
	}
}

// request publishes one request and waits for its reply.
func (r *requester) request(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	r.mu.Lock()
	r.seq++
	id := "rr-" + strconv.FormatUint(r.seq, 36)
	wait := make(chan *mq.Message, 1)
	r.waiting[id] = wait
	ch := r.ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.waiting, id)
		r.mu.Unlock()
	}()
	if ch == nil {
		// The reply endpoint's channel, which a channel exception takes with
		// the connection up: see [errNoChannel].
		return nil, errNoChannel
	}

	names, err := publishRoute(r.b, topic)
	if err != nil {
		return nil, err
	}
	var buf publishing
	opts := r.b.options(mq.MetadataNativeStrict)
	if err := buf.encode(r.b.proto, m, opts, ""); err != nil {
		return nil, r.b.errs.Map(mq.OpPublish, topic, "", err)
	}
	// The correlation identifier and the reply address are the driver's, not
	// the caller's: a caller who set CorrelationID on the request would have
	// it overwritten, which is why the specification gives the native path the whole
	// exchange. The driver page says so.
	buf.pub.CorrelationId = id
	buf.pub.ReplyTo = ReplyAddress
	if err := ch.PublishWithContext(ctx, names.exchange, names.key, true, false, buf.pub); err != nil {
		return nil, r.b.errs.MapNet(mq.OpPublish, topic, "", err, true)
	}

	ctx, cancel := r.b.waitDeadline(ctx)
	defer cancel()
	select {
	case reply := <-wait:
		if reply == nil {
			// The endpoint's channel went while this request was outstanding.
			return nil, errNoChannel
		}
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// route is where a request or a reply is published to.
type route struct{ exchange, key string }

// publishRoute is the exchange and routing key a topic is published to,
// including the direct reply-to special case.
func publishRoute(b *Backend, topic string) (route, error) {
	if IsReplyAddress(topic) {
		// The default exchange routes by name, which is what a reply address
		// is: a routing key the broker itself minted. Nothing is declared and
		// nothing is derived.
		return route{exchange: "", key: topic}, nil
	}
	n, err := b.names(topic, "")
	if err != nil {
		return route{}, err
	}
	return route{exchange: n.Exchange, key: topic}, nil
}

// errNilRequest is a request with no message.
var errNilRequest = &mq.OpError{
	Driver: DriverName, Operation: mq.OpPublish,
	Code: mq.CodeInvalidArgument,
}
