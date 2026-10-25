package activemq

import (
	"context"
	"encoding/json"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Topology, and why only one of the two products has any.
//
// The specification makes implicit topology creation a defect, so a sink or a source in
// this driver ATTACHES and never creates. Something has to create, and on
// Artemis that something is the broker's own management address, reachable
// over the very connection the client already has. On ActiveMQ Classic there
// is nothing to reach: its management is JMX and its web console, so the
// profile supplies no Admin, `Client.Admin()` answers ErrUnsupported, and its
// destinations come into existence when a link attaches to them because the
// broker cannot be told not to.
//
// # Artemis's management protocol, as measured
//
// It is not the AMQP management specification. A request is a message sent to
// the `activemq.management` address with
//
//	_AMQ_ResourceName   the resource ("broker", "queue.<name>", "address.<name>")
//	_AMQ_OperationName  the operation ("createAddress", "createQueue", ...)
//
// in its application properties and an AMQP **value** body holding a JSON
// ARRAY of the operation's arguments, as a STRING. Three body encodings were
// tried against the pinned image before that one worked: a data section
// carrying the JSON bytes and an AMQP value holding a real list both fail with
// "Error reading in simpleString", because the broker decodes the body with
// its own SimpleString codec. The reply arrives on a dynamic receiver named by
// the request's `reply-to` and carries `_AMQ_OperationSucceeded` and a JSON
// array body of its own.
//
// # The management link is not the driver's
//
// Every request opens its own session, sender and dynamic receiver and closes
// them again. That costs three frames per operation and it is the right price:
// a long-lived management link would be a fourth handle the reconnect
// supervisor has to invalidate, and the operations are rare by
// construction. The session comes from the core's `Backend.Native()`, which is
// the `*amqp.Conn` — a session of the driver's own would be one a caller could
// close underneath a sink.

// admin returns the profile's topology surface, or nil for a product that has
// none.
//
// A nil answer is load-bearing rather than lazy: the core asserts
// [mq.AdminBackend] once when the client is built, so a profile with no Admin
// produces a backend that does not satisfy the interface at all, and
// `Client.Admin()` therefore reports ErrUnsupported instead of returning a
// surface that refuses every call. The core's own `checkClaim` refuses a
// profile that claims [mq.FeatureAdmin] without supplying this.
func (c Config) admin() func(context.Context, *core.Backend) (mq.Admin, error) {
	if c.Broker != activemq.Artemis {
		return nil
	}
	return func(_ context.Context, b *core.Backend) (mq.Admin, error) {
		if b == nil {
			return nil, errNoBackend
		}
		return &artemisAdmin{cfg: c, b: b}, nil
	}
}

// artemisAdmin is Artemis's management address behind the portable
// surface.
type artemisAdmin struct {
	cfg Config
	b   *core.Backend
}

var _ mq.Admin = (*artemisAdmin)(nil)

// managementAddress is the address Artemis answers management requests on.
const managementAddress = "activemq.management"

// The application-property names Artemis's management protocol uses.
const (
	propResource  = "_AMQ_ResourceName"
	propOperation = "_AMQ_OperationName"
	propSucceeded = "_AMQ_OperationSucceeded"
)

// EnsureTopic creates the topic's multicast address if it does not exist.
//
// "If it does not exist" is the broker's answer and not a read-then-write:
// `createAddress` on an address that already exists fails with AMQ229019's
// sibling, and asking first would be a race with every other client doing the
// same thing. The specification requires the operation to be idempotent, so an
// already-exists refusal is success.
//
// Under [AddressQueue] the topic IS a JMS queue, so this creates an ANYCAST
// address and the queue of the same name on it: a JMS queue is both, and an
// address alone would accept a publication and route it nowhere.
func (a *artemisAdmin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	n, err := a.cfg.Names(spec.Name, "")
	if err != nil {
		return err
	}
	_, err = a.call(ctx, "broker", "createAddress", n.Address, routingType(a.cfg.Addressing))
	if err != nil && !isExists(err) {
		return err
	}
	if a.cfg.Addressing != AddressQueue {
		return nil
	}
	return a.createQueue(ctx, n)
}

// createQueue creates the queue a name set names, treating one that is already
// there as success for the reason EnsureTopic gives.
func (a *artemisAdmin) createQueue(ctx context.Context, n activemq.Names) error {
	body, err := json.Marshal(queueConfiguration(n, a.cfg.Addressing))
	if err != nil {
		return a.wrap(err)
	}
	_, err = a.call(ctx, "broker", "createQueue", string(body))
	if isExists(err) {
		return nil
	}
	return err
}

// routingType is the Artemis routing type an addressing mode's addresses and
// queues are created with.
func routingType(a Addressing) string {
	if a == AddressQueue {
		return "ANYCAST"
	}
	return "MULTICAST"
}

// DeleteTopic removes the address and every queue on it.
//
// The queues go first and the address second, because Artemis refuses to
// delete an address that still has queues bound to it — so the order is the
// operation rather than tidiness.
//
// Under [AddressQueue] the one queue on the address is the topic's own, so it
// is deleted by name, with its messages, before the address.
func (a *artemisAdmin) DeleteTopic(ctx context.Context, topic string) error {
	n, err := a.cfg.Names(topic, "")
	if err != nil {
		return err
	}
	if a.cfg.Addressing == AddressQueue {
		if err := a.destroyQueue(ctx, n.Queue); err != nil {
			return err
		}
		return a.deleteAddress(ctx, n.Address)
	}
	names, err := a.queueNames(ctx)
	if err != nil {
		return err
	}
	for _, q := range names {
		t, sub, ok := activemq.ParseQueue(activemq.Artemis, a.cfg.Prefix, q)
		if !ok || t != topic {
			continue
		}
		if err := a.DeleteSubscription(ctx, t, sub); err != nil && !isMissing(err) {
			return err
		}
	}
	return a.deleteAddress(ctx, n.Address)
}

// deleteAddress removes an address, treating one that is already gone as
// success so that a Delete is idempotent.
func (a *artemisAdmin) deleteAddress(ctx context.Context, address string) error {
	_, err := a.call(ctx, "broker", "deleteAddress", address)
	if isMissing(err) {
		return nil
	}
	return err
}

// destroyQueue removes a queue and its messages, treating one that is already
// gone as success for the same reason.
func (a *artemisAdmin) destroyQueue(ctx context.Context, queue string) error {
	_, err := a.call(ctx, "broker", "destroyQueue", queue, true, true)
	if isMissing(err) {
		return nil
	}
	return err
}

// ListTopics iterates the topics this driver's own queues belong to.
//
// It reports the topics with at least one SUBSCRIPTION rather than every
// address on the broker, and that is a deliberate narrowing: a broker's
// address list holds `activemq.management`, `DLQ`, `ExpiryQueue`, every MQTT
// session store and every temporary reply address, and a driver that reported
// those as topics would be reporting the broker's furniture. What it can say
// truthfully is which of the names it would itself have derived are there.
//
// Under [AddressQueue] every queue is a topic, so it reports every queue whose
// name carries the prefix, without it. With no prefix that includes the
// broker's own DLQ and ExpiryQueue: they are JMS queues too, and nothing in a
// queue's name says which ones a deployment meant.
func (a *artemisAdmin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		names, err := a.queueNames(ctx)
		if err != nil {
			yield("", err)
			return
		}
		seen := make(map[string]struct{}, len(names))
		for _, q := range names {
			topic, ok := a.topicOf(q)
			if !ok {
				continue
			}
			if _, dup := seen[topic]; dup {
				continue
			}
			seen[topic] = struct{}{}
			if !yield(topic, nil) {
				return
			}
		}
	}
}

// topicOf reads the topic a queue belongs to, or reports that the queue is not
// one of this configuration's.
func (a *artemisAdmin) topicOf(queue string) (string, bool) {
	if a.cfg.Addressing != AddressQueue {
		topic, _, ok := activemq.ParseQueue(activemq.Artemis, a.cfg.Prefix, queue)
		return topic, ok
	}
	topic, ok := strings.CutPrefix(queue, a.cfg.Prefix)
	return topic, ok && topic != ""
}

// EnsureSubscription creates the subscription's durable queue on the topic's
// address.
//
// The queue is MULTICAST, durable, shared (`max-consumers: -1`) and not
// purged when its last consumer leaves, which is the four-part definition of the durable shared subscription. `purge-on-no-consumers: true` would make
// it a non-durable one and is the default for a queue Artemis auto-creates
// from a JMS topic subscriber, which is why every field is stated.
//
// Under [AddressQueue] a subscription names nothing of its own, so this is
// EnsureTopic: the JMS queue every consumer shares.
func (a *artemisAdmin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	if a.cfg.Addressing == AddressQueue {
		return a.EnsureTopic(ctx, mq.TopicSpec{Name: spec.Topic})
	}
	n, err := a.cfg.Names(spec.Topic, spec.Name)
	if err != nil {
		return err
	}
	if err := a.EnsureTopic(ctx, mq.TopicSpec{Name: spec.Topic}); err != nil {
		return err
	}
	return a.createQueue(ctx, n)
}

// queueConfiguration is what makes a subscription a DURABLE SHARED one.
//
// Every field is stated rather than left to the broker's default, and the
// reason is that Artemis's default for an auto-created topic subscriber's
// queue is the opposite of what the specification asks for: `purge-on-no-consumers: true`
// makes it a NON-durable subscription, so the queue and everything in it go
// when the last consumer leaves. `max-consumers: -1` is the "two consumers of
// one subscription compete" half, and `auto-create-address: false` is the specification:
// a create that made its own address would be the implicit topology this
// driver refuses.
//
// Under [AddressQueue] the same fields make a durable JMS queue: ANYCAST
// rather than MULTICAST, and otherwise unchanged, because a queue other
// applications share must survive its consumers leaving just as a durable
// subscription must.
func queueConfiguration(n activemq.Names, addressing Addressing) map[string]any {
	return map[string]any{
		"name":                  n.Queue,
		"address":               n.Address,
		"routing-type":          routingType(addressing),
		"durable":               true,
		"max-consumers":         -1,
		"purge-on-no-consumers": false,
		"auto-create-address":   false,
		"auto-delete":           false,
	}
}

// DeleteSubscription removes the subscription's queue.
//
// Under [AddressQueue] it removes nothing and succeeds. The subscription names
// no queue of its own, and the queue it would otherwise resolve to is the JMS
// queue every other consumer and producer shares: deleting it because one
// consumer's subscription was deleted would take it from all of them.
// DeleteTopic is how the queue itself goes.
func (a *artemisAdmin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	n, err := a.cfg.Names(topic, sub)
	if err != nil {
		return err
	}
	if a.cfg.Addressing == AddressQueue {
		return nil
	}
	return a.destroyQueue(ctx, n.Queue)
}

// Purge discards everything the subscription has not consumed.
func (a *artemisAdmin) Purge(ctx context.Context, topic, sub string) error {
	n, err := a.cfg.Names(topic, sub)
	if err != nil {
		return err
	}
	_, err = a.call(ctx, "queue."+n.Queue, "removeAllMessages")
	return err
}

// Stats reports the subscription's depth and in-flight count.
//
// Lag is left UNKNOWN rather than computed, and that is the rule about an
// unknown never being an overloaded zero. Artemis reports a queue's message
// count and its delivering count, and "lag" in the sense the specification means — how far
// behind the subscription is — is the same number as the depth for a queue: a
// broker with no log has no position to be behind. Reporting the depth twice
// would make two different questions look answered.
func (a *artemisAdmin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	n, err := a.cfg.Names(topic, sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	depth, err := a.callInt(ctx, "queue."+n.Queue, "countMessages")
	if err != nil {
		return mq.QueueStats{}, err
	}
	inflight, err := a.callInt(ctx, "queue."+n.Queue, "getDeliveringCount")
	if err != nil {
		return mq.QueueStats{}, err
	}
	return mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: depth, Known: true},
		InFlight: mq.Limit[int64]{Value: inflight, Known: true},
	}, nil
}

// queueNames asks the broker for every queue it has.
func (a *artemisAdmin) queueNames(ctx context.Context) ([]string, error) {
	body, err := a.call(ctx, "broker", "getQueueNames")
	if err != nil {
		return nil, err
	}
	// The reply is a JSON array holding one array of names.
	var outer []json.RawMessage
	if err := json.Unmarshal([]byte(body), &outer); err != nil || len(outer) == 0 {
		return nil, a.wrap(errBadManagementReply)
	}
	var names []string
	if err := json.Unmarshal(outer[0], &names); err != nil {
		return nil, a.wrap(errBadManagementReply)
	}
	return names, nil
}

// callInt runs an operation whose reply is a single number.
func (a *artemisAdmin) callInt(ctx context.Context, resource, op string) (int64, error) {
	body, err := a.call(ctx, resource, op)
	if err != nil {
		return 0, err
	}
	var outer []json.RawMessage
	if err := json.Unmarshal([]byte(body), &outer); err != nil || len(outer) == 0 {
		return 0, a.wrap(errBadManagementReply)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(outer[0])), 10, 64)
	if err != nil {
		return 0, a.wrap(errBadManagementReply)
	}
	return n, nil
}

// managementRequest builds one request message.
//
// It is a function of its arguments and nothing else, so that the encoding the
// broker turned out to need — an AMQP value body holding the JSON array as a
// STRING — is pinned by a test rather than only by a container.
func managementRequest(resource, op, replyTo string, args ...any) (*amqp.Message, error) {
	if args == nil {
		args = []any{}
	}
	body, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	reply := replyTo
	return &amqp.Message{
		Value:      string(body),
		Properties: &amqp.MessageProperties{ReplyTo: &reply},
		ApplicationProperties: map[string]any{
			propResource:  resource,
			propOperation: op,
		},
	}, nil
}

// managementOK reads a reply and returns its body, or the broker's own
// refusal.
//
// A refused operation arrives as a SUCCESSFUL delivery whose
// `_AMQ_OperationSucceeded` is false and whose body is the reason, which is
// why this is a separate step from receiving: a caller that only checked for a
// transport error would read "Queue X already exists" as success.
func managementOK(m *amqp.Message) (string, error) {
	if m == nil {
		return "", errBadManagementReply
	}
	body, _ := m.Value.(string)
	if body == "" && len(m.GetData()) > 0 {
		body = string(m.GetData())
	}
	ok, present := m.ApplicationProperties[propSucceeded].(bool)
	if !present {
		return body, errBadManagementReply
	}
	if !ok {
		return body, &ManagementError{Reason: body}
	}
	return body, nil
}

// call runs one management operation and returns its reply body.
func (a *artemisAdmin) call(ctx context.Context, resource, op string, args ...any) (string, error) {
	conn, ok := a.b.Native().(*amqp.Conn)
	if !ok || conn == nil {
		return "", a.wrap(errNoNativeConn)
	}
	ctx, cancel := a.bound(ctx)
	defer cancel()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return "", a.wrap(err)
	}
	// The close outlives the call's own deadline, which may be what ended
	// the call, but it is bounded afresh: WithoutCancel drops the deadline
	// along with the cancellation, and a broker that never answers the END
	// would otherwise hold this Admin call for ever.
	defer func() {
		cctx, ccancel := a.bound(context.WithoutCancel(ctx))
		defer ccancel()
		_ = sess.Close(cctx)
	}()
	snd, err := sess.NewSender(ctx, managementAddress, nil)
	if err != nil {
		return "", a.wrap(err)
	}
	rcv, err := sess.NewReceiver(ctx, "", &amqp.ReceiverOptions{Credit: 1, DynamicAddress: true})
	if err != nil {
		return "", a.wrap(err)
	}
	req, err := managementRequest(resource, op, rcv.Address(), args...)
	if err != nil {
		return "", a.wrap(err)
	}
	if err := snd.Send(ctx, req, nil); err != nil {
		return "", a.wrap(err)
	}
	reply, err := rcv.Receive(ctx, nil)
	if err != nil {
		return "", a.wrap(err)
	}
	// The reply is accepted before it is read: the operation has happened
	// whatever the body says, and leaving it unsettled would hold a credit
	// on a link that is about to close.
	if err := rcv.AcceptMessage(ctx, reply); err != nil {
		return "", a.wrap(err)
	}
	body, err := managementOK(reply)
	if err != nil {
		return body, a.wrap(err)
	}
	return body, nil
}

// bound gives a management operation a deadline when the caller gave none.
//
// the operations reach a broker that may be busy compacting a journal, so
// a call with no deadline would park for ever on a context the caller thought
// was fine. The driver's own OperationTimeout is the bound, and a caller's own
// deadline always wins.
func (a *artemisAdmin) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	d := a.cfg.OperationTimeout
	if d <= 0 {
		d = defaultAdminTimeout
	}
	return context.WithTimeout(ctx, d)
}

// defaultAdminTimeout bounds a management operation whose caller set none.
const defaultAdminTimeout = 30 * time.Second

// wrap puts a management failure through the driver's error table, so that
// Artemis's own AMQ229019 becomes CodeConflict and a dead connection becomes a
// retryable CodeUnavailable.
func (a *artemisAdmin) wrap(err error) error {
	if err == nil {
		return nil
	}
	return core.MapForTest(a.b, mq.OpAdmin, err)
}

// ManagementError is an operation the broker refused, with its own reason.
type ManagementError struct {
	// Reason is the broker's answer, which carries its AMQ code.
	Reason string
}

// Error implements error.
func (e *ManagementError) Error() string {
	return "iqmq/activemq: the broker refused the management operation: " + e.Reason
}

// isExists reports whether err is Artemis saying the thing is already there.
//
// The specification requires Ensure to be idempotent, and Artemis answers a second
// `createQueue` with AMQ229019 and a second `createAddress` with AMQP229204's
// sibling rather than with success — so this is what makes the operation
// idempotent. It matches on the CODE and not on the sentence, because the
// sentence names the address and the code does not change.
func isExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "AMQ229019") || // Queue X already exists
		strings.Contains(s, "AMQ229204") && strings.Contains(s, "already exists") ||
		strings.Contains(s, "already exists")
}

// isMissing reports whether err is Artemis saying the thing is not there,
// which makes a Delete idempotent for the same reason.
func isMissing(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "AMQ229017") || // Queue X does not exist
		strings.Contains(s, "AMQ229203") || // Address does not exist
		strings.Contains(s, "does not exist")
}

// Admin's own refusals.
var (
	errNoBackend = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the profile's Admin was given no backend")
	errNoNativeConn = errcode.New(errcode.Unavailable,
		"iqmq/activemq: the connection is not an AMQP 1.0 connection, so Artemis's management address cannot be reached")
	errBadManagementReply = errcode.New(errcode.CorruptMessage,
		"iqmq/activemq: the broker's management reply is not the shape Artemis documents")
)
