package rabbitmq

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
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
//
// It recognises the address as the broker wrote it and as a delivery carries
// it in `Properties.ReplyTo`, which is escaped to be a valid topic name (see
// [replyTopic]).
func IsReplyAddress(topic string) bool {
	if topic == ReplyAddress {
		return true
	}
	return len(topic) > len(replyPrefix) && topic[:len(replyPrefix)] == replyPrefix
}

// replyPrefix is what every address the broker mints for direct reply-to
// starts with.
const replyPrefix = ReplyAddress + "."

// replyEscape introduces an escaped byte in a reply address, and marks an
// escaped address when it is the first byte after [replyPrefix]. The broker
// writes its addresses in standard base64 and '.', which has no '_', so an
// address it wrote never starts that way.
const replyEscape = '_'

// replyTopic is the topic a responder publishes its reply to, for a request
// whose `reply_to` is addr: addr itself when it is already a valid topic name,
// and otherwise addr escaped into one.
//
// RabbitMQ replaces a request's `reply_to` with an address of its own,
// `amq.rabbitmq.reply-to.<base64>.<base64>`, and standard base64 has '+', '/'
// and the '=' padding: the second part encodes 16 bytes, so it always ends
// "==". A responder publishes its reply to `Delivery.Properties.ReplyTo`
// through mq.Client.Publish (reqreply.Respond does), and the core's topic
// check allows only [A-Za-z0-9._-] and the '/' separator, so the reply was
// refused before it reached this driver and native request/reply never
// worked on a real broker. The core is where a fix belongs, because the
// address is the broker's and not a topic; until the core lets a driver vouch
// for one, the driver hands the responder a name the check accepts, and
// [replyKey] turns it back into the broker's address whenever a message is
// published: to the reply address, and in `reply_to` too, so that a relay
// that forwards a request's Properties sends the broker's own address on.
//
// The escaped form is [replyPrefix], then [replyEscape], then the rest of addr
// with every byte other than [A-Za-z0-9.-] written as [replyEscape] and two
// upper-case hex digits. An address with nothing to escape keeps its own
// spelling, and since the broker never writes [replyEscape] the two forms
// cannot be confused.
func replyTopic(addr string) string {
	if !IsReplyAddress(addr) || addr == ReplyAddress {
		return addr
	}
	rest := addr[len(replyPrefix):]
	n := 0
	for i := range len(rest) {
		if !replyByte(rest[i]) {
			n++
		}
	}
	if n == 0 {
		return addr
	}
	const hex = "0123456789ABCDEF"
	out := make([]byte, 0, len(addr)+1+2*n)
	out = append(out, replyPrefix...)
	out = append(out, replyEscape)
	for i := range len(rest) {
		c := rest[i]
		if replyByte(c) {
			out = append(out, c)
			continue
		}
		out = append(out, replyEscape, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

// replyKey is the routing key a reply to topic is published with: the address
// the broker wrote, which [replyTopic] escaped. A topic that is not an escaped
// reply address, or that does not unescape, is its own routing key.
func replyKey(topic string) string {
	if len(topic) <= len(replyPrefix)+1 || topic[:len(replyPrefix)] != replyPrefix ||
		topic[len(replyPrefix)] != replyEscape {
		return topic
	}
	rest := topic[len(replyPrefix)+1:]
	out := make([]byte, 0, len(topic))
	out = append(out, replyPrefix...)
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if c != replyEscape {
			out = append(out, c)
			continue
		}
		if i+2 >= len(rest) {
			return topic
		}
		hi, okHi := unhex(rest[i+1])
		lo, okLo := unhex(rest[i+2])
		if !okHi || !okLo {
			return topic
		}
		out = append(out, hi<<4|lo)
		i += 2
	}
	return string(out)
}

// replyByte reports whether a byte of a reply address needs no escaping.
func replyByte(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-'
}

// unhex is one upper-case hex digit's value.
func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

// replyShare is what two reply publishers have to agree on to share a
// channel: the confirmation mode the channel is opened in and the settings a
// publication is encoded with.
type replyShare struct {
	mode     mq.ConfirmationMode
	metadata mq.MetadataMode
	receipts bool
}

// openReplyPublisher opens a publisher of replies to one direct reply-to
// address, cfg.Topic, on the channel every reply address of this client with
// the same [replyShare] publishes on.
//
// RabbitMQ takes a reply to a direct reply-to address on any channel, and
// every requester connection has an address of its own. `Client.Publish`
// keeps a publisher for every topic it has published to until the client
// closes, so a publisher with a channel of its own per address left a
// responder one channel and one goroutine per requester connection it had ever
// answered: a responder answering requesters that live for one request, a CLI
// or a job, reached the connection's channel_max, after which it could open no
// channel at all, its consumers' repairs included
// (TestRepliesToManyRequestersShareOneChannel). What an address costs now is
// the core's publisher and a [replyPublisher].
func (b *Backend) openReplyPublisher(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	mode := cfg.Confirmation.Resolve()
	if mode == mq.ConfirmDurable && !b.fx.quorum {
		// [Backend.OpenSink]'s refusal, for its reason.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoDurableBoundary)
	}
	share := replyShare{mode: mode, metadata: cfg.Metadata, receipts: cfg.Receipts}
	if _, err := b.replySink(ctx, share); err != nil {
		return nil, err
	}
	return &replyPublisher{b: b, share: share, key: replyKey(cfg.Topic)}, nil
}

// replySink is the publisher every reply address with share publishes on. It
// is opened when there is none, and opened again when the one there is has
// nothing coming to attach it ([replySinkStranded]).
//
// The shared publisher is an ordinary publisher, registered with the backend
// like any other, so a reconnection re-attaches it, the teardown closes it, and
// a channel exception gets its one repair. A publisher whose repair failed
// waits for the next reconnection, which is the rule for one topic's publisher;
// this one carries every reply, and `Client.Publish` never opens another for
// an address it has cached, so after one failed repair no reply to any
// requester could be published until the connection was replaced
// (TestASharedReplyPublisherWhoseRepairFailedIsReplaced). The driver owns it,
// so it replaces it, and every [replyPublisher] looks it up on each
// publication.
func (b *Backend) replySink(ctx context.Context, share replyShare) (*sink, error) {
	if s := b.currentReplySink(share); s != nil && !replySinkStranded(s) {
		return s, nil
	}
	// Opening one is I/O, so it is serialised by a lock of its own and the
	// map's lock is not held across it: a Close and a publication on a
	// publisher that works do not wait for a broker that is slow to open it.
	b.replyMu.Lock()
	defer b.replyMu.Unlock()
	old := b.currentReplySink(share)
	if old != nil && !replySinkStranded(old) {
		return old, nil
	}
	if b.ctx.Err() != nil {
		return nil, mq.ErrClosed
	}
	s, err := b.newSink(ctx, mq.SinkConfig{
		Topic: ReplyAddress, Confirmation: share.mode, Metadata: share.metadata, Receipts: share.receipts,
	}, rabbit.Names{}, share.mode)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = s.Close()
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	b.mu.Unlock()
	b.replySinksMu.Lock()
	if b.replySinks == nil {
		b.replySinks = map[replyShare]*sink{}
	}
	b.replySinks[share] = s
	b.replySinksMu.Unlock()
	if old != nil {
		// It has no channel, so its close is no round trip.
		_ = old.Close()
	}
	return s, nil
}

// currentReplySink is the shared reply publisher for share, or nil.
func (b *Backend) currentReplySink(share replyShare) *sink {
	b.replySinksMu.Lock()
	defer b.replySinksMu.Unlock()
	return b.replySinks[share]
}

// replySinkStranded reports whether a shared reply publisher is closed, or has
// no channel and nothing that will attach one: no repair in flight and no
// reconnection coming.
func replySinkStranded(s *sink) bool {
	return s.isClosed() || s.detached() && !s.repairing.Load() && !s.b.reconnectPending()
}

// replyPublisher publishes replies to one direct reply-to address, key, as
// the broker wrote it, on the publisher its [replyShare] shares (see
// [Backend.openReplyPublisher]), which it looks up on each publication
// ([Backend.replySink]). Its Close answers its own publications still waiting
// ([sink.abandon]) and leaves the shared publisher open: the teardown closes
// that.
type replyPublisher struct {
	b      *Backend
	share  replyShare
	key    string
	closed atomic.Bool
}

var (
	_ mq.Sink      = (*replyPublisher)(nil)
	_ mq.AsyncSink = (*replyPublisher)(nil)
	_ mq.FlushSink = (*replyPublisher)(nil)
	_ mq.OwnedSink = (*replyPublisher)(nil)
)

// Publish publishes one reply. See [sink.Publish].
func (p *replyPublisher) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	s, err := p.sink(ctx)
	if err != nil {
		return err
	}
	return s.publish(ctx, p, m, o, nil, nil)
}

// PublishAsync publishes one reply and reports its outcome to done. See
// [sink.PublishAsync].
func (p *replyPublisher) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	s, err := p.sink(ctx)
	if err != nil {
		return err
	}
	return s.publish(ctx, p, m, o, done, nil)
}

// PublishOwned publishes one reply it owns. See [sink.PublishOwned].
func (p *replyPublisher) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	s, err := p.sink(ctx)
	if err != nil {
		// The release is this sink's obligation on every path, a refusal
		// included.
		if om.Message != nil && om.Release != nil {
			om.Release(om.Message)
		}
		return err
	}
	return s.publishOwned(ctx, p, om, o)
}

// Flush waits for the replies the shared publisher has admitted, this one's
// among them. See [sink.Flush].
func (p *replyPublisher) Flush(ctx context.Context) error {
	s, err := p.sink(ctx)
	if err != nil {
		return err
	}
	return s.Flush(ctx)
}

// Close closes this publisher: its publications still waiting for their
// confirm are answered as ambiguous, and the shared publisher stays open.
func (p *replyPublisher) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s := p.b.currentReplySink(p.share); s != nil {
		s.abandon(p)
	}
	return nil
}

// sink is the shared publisher to publish on, or why there is none.
func (p *replyPublisher) sink(ctx context.Context) (*sink, error) {
	if p.closed.Load() {
		return nil, mq.ErrClosed
	}
	return p.b.replySink(ctx, p.share)
}

// requester is this driver's direct reply-to endpoint: one channel, one
// consumer, one correlation table, any number of concurrent requests.
type requester struct {
	b *Backend

	mu sync.Mutex
	// ch is the reply consumer's channel, opened on the first request and
	// closed with the backend, and chConn is the connection it is on.
	ch     client.ChannelAlias
	chConn client.Conn
	// waiting maps a correlation identifier to the request waiting for its
	// reply.
	waiting map[string]chan *mq.Message
	// started reports whether the consumer is up, so that a second request
	// does not open a second one.
	started bool
	seq     uint64
	// routed holds the topics whose exchange [requester.routable] has found
	// since ch was opened. It is emptied with ch, so that a topic whose
	// exchange went away, and took ch with it, is asked about again.
	routed map[string]struct{}
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
	rq, err := b.requesterFor(ctx)
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
func (b *Backend) requesterFor(ctx context.Context) (*requester, error) {
	rq := b.endpoint()
	if err := rq.start(ctx); err != nil {
		return nil, err
	}
	return rq, nil
}

// endpoint is the backend's reply endpoint, started or not.
func (b *Backend) endpoint() *requester {
	b.rqOnce.Do(func() {
		b.rq = &requester{b: b, waiting: map[string]chan *mq.Message{}, routed: map[string]struct{}{}}
	})
	return b.rq
}

// start opens the reply consumer if it is not already up.
//
// The channel is opened and the consumer started OUTSIDE the lock, and the
// channel open is bounded by ctx (see [Backend.channelOnWithin]). Both used to
// run under the lock with no bound, so a broker slow to answer
// `channel.open` held the first request past its context, and held Close as
// well: the teardown's [requester.stop] takes the lock, and the connection
// close that would have ended the parked open comes after it
// (TestARequestAndCloseDoNotWaitForTheReplyChannelOpen). `basic.consume` is
// still a round trip the library gives no deadline, as every declaration is.
//
// The install is under the lock, and it refuses once the backend is closing,
// so that the teardown's stop either closes a channel this opened or runs
// first and this installs none. Two first requests at once may each open a
// channel; the one that installs second closes its own and uses the other.
func (r *requester) start(ctx context.Context) error {
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if started {
		return nil
	}
	if r.b.ctx.Err() != nil {
		return mq.ErrClosed
	}
	cn := r.b.conn()
	ch, err := r.b.channelOnWithin(ctx, cn)
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
	//nolint:contextcheck // the consumer outlives the request that starts it; the backend's context is what bounds it
	replies, err := ch.ConsumeWithContext(r.b.ctx, ReplyAddress, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return r.b.errs.Map(mq.OpFetch, ReplyAddress, "", err)
	}
	r.mu.Lock()
	if r.started || r.b.ctx.Err() != nil {
		closing := r.b.ctx.Err() != nil
		r.mu.Unlock()
		_ = r.b.dropChannel(ch, cn)
		if closing {
			return mq.ErrClosed
		}
		return nil
	}
	r.ch, r.chConn, r.started = ch, cn, true
	r.b.wg.Add(1)
	r.mu.Unlock()
	go r.pump(ch, replies)
	return nil
}

// pump dispatches replies to the requests waiting for them.
func (r *requester) pump(ch client.ChannelAlias, replies <-chan amqp.Delivery) {
	defer r.b.wg.Done()
	var (
		msg    mq.Message
		fields protocol.Fields
	)
	for {
		select {
		case d, ok := <-replies:
			if !ok {
				r.stop(ch)
				return
			}
			// The reply's storage is copied out, because the waiter reads it
			// after this loop has moved on and the library's buffer is
			// reused: the borrowing rule runs out at the end of this
			// iteration, and there is no settlement to extend it.
			if _, err := decode(r.b.proto, &msg, &fields, &d, r.b.options(mq.MetadataNativeStrict), "", "", false, r.b.clock.Now()); err != nil {
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
// next request rather than leaving every request to time out. The pump calls
// it with the channel it was reading, and stops nothing if that channel has
// already been replaced; the backend's teardown calls it with nil, which is
// whatever channel is installed.
//
// It closes the channel. One whose consumer the broker cancelled is still
// open, and under ClientBorrowed the connection it is on stays
// open after Close, so a channel left to the connection's close stayed on the
// caller's connection after the client had gone. Closing a channel that has
// already closed is answered at once. The teardown leaves a channel on the
// driver's own connection to that connection's close (see
// [Backend.dropChannel]).
func (r *requester) stop(only client.ChannelAlias) {
	r.mu.Lock()
	if only != nil && r.ch != only {
		r.mu.Unlock()
		return
	}
	ch, cn := r.ch, r.chConn
	r.started = false
	r.ch, r.chConn = nil, nil
	clear(r.routed)
	waiters := make([]chan *mq.Message, 0, len(r.waiting))
	for id, w := range r.waiting {
		waiters = append(waiters, w)
		delete(r.waiting, id)
	}
	r.mu.Unlock()
	if ch != nil {
		_ = r.b.dropChannel(ch, cn)
	}
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

// routable makes sure the exchange a request to topic goes to exists, before
// the request is published on the reply channel.
//
// Every request goes out on that one channel, because the broker routes a
// reply to the channel its request was published on. A request to a topic
// with no exchange is a `basic.publish` to a missing exchange, which the
// broker answers with a 404 channel exception, and that closed the reply
// channel under every request in flight: a request to a healthy topic, already
// answered, failed as if the endpoint had gone, and the bad request was not
// told NotFound (TestARequestToAMissingTopicFailsAlone). So the first request
// to each topic asks first, on a channel of its own, as a publisher's attach
// does ([Backend.ensure]): with Config.Declare it declares the exchange, and
// without it asserts it. The answer holds until the reply channel is
// replaced, so a request costs nothing more after the first; an exchange
// deleted after that still closes the reply channel once, and the next request
// asks again.
func (r *requester) routable(ctx context.Context, topic string, n rabbit.Names) error {
	r.mu.Lock()
	_, ok := r.routed[topic]
	r.mu.Unlock()
	if ok {
		return nil
	}
	if !r.b.cfg.Declare || !r.b.topo.known(decl{kind: declExchange, name: n.Exchange}) {
		dch, err := r.b.channelOnWithin(ctx, r.b.conn())
		if err != nil {
			return err
		}
		if err := r.b.ensure(ctx, dch, n, false); err != nil {
			_ = dch.Close()
			return err
		}
		if err := dch.Close(); err != nil {
			return r.b.errs.Map(mq.OpOpen, topic, "", err)
		}
	}
	r.mu.Lock()
	if r.started {
		r.routed[topic] = struct{}{}
	}
	r.mu.Unlock()
	return nil
}

// request publishes one request and waits for its reply.
func (r *requester) request(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	rt, err := publishRoute(r.b, topic)
	if err != nil {
		return nil, err
	}
	if !IsReplyAddress(topic) {
		// A reply address is on the default exchange, which always exists.
		n, err := r.b.names(topic, "")
		if err != nil {
			return nil, err
		}
		if err := r.routable(ctx, topic, n); err != nil {
			return nil, err
		}
	}

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

	var buf publishing
	opts := r.b.options(mq.MetadataNativeStrict)
	// A request is routed by its topic, so under KeyAsRoutingKey its key goes
	// in [KeyHeader], as a publisher's does.
	if err := buf.encode(r.b.proto, m, opts, "", r.b.cfg.KeyAsRoutingKey, m.Key); err != nil {
		return nil, r.b.errs.Map(mq.OpPublish, topic, "", err)
	}
	buf.identify(r.b.cfg.AppID, r.b.cfg.PublishType)
	// The correlation identifier and the reply address are the driver's, not
	// the caller's: a caller who set CorrelationID on the request would have
	// it overwritten, which is why the specification gives the native path the whole
	// exchange. The driver page says so.
	buf.pub.CorrelationId = id
	buf.pub.ReplyTo = ReplyAddress
	if err := ch.PublishWithContext(ctx, rt.exchange, rt.key, true, false, buf.pub); err != nil {
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
		// is: a routing key the broker itself minted, spelt as the broker
		// wrote it ([replyKey]). Nothing is declared and nothing is derived.
		return route{exchange: "", key: replyKey(topic)}, nil
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
