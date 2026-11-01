package stomp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
)

// The question Config.VerifyQueueAddress asks Artemis before a subscription.
//
// Artemis's management address answers over STOMP as it does over AMQP 1.0:
// a SEND to `activemq.management` names the resource and the operation in
// `_AMQ_ResourceName` and `_AMQ_OperationName`, carries a JSON array of the
// operation's arguments as its body, and is answered on the queue its
// `reply-to` names with `_AMQ_OperationSucceeded` and a JSON array body.
// Measured on the pinned image, with auto-creation off:
//
//   - The body must be a TEXT message, so the SEND carries no
//     `content-length` (go-stomp's SendOpt.NoContentLength). With one, the
//     broker answers `AMQ339011 Error sending message` and closes the
//     connection.
//   - `getAddress` on `queue.<name>` answers `["<address>"]`, and for a name
//     the broker has no queue of, `_AMQ_OperationSucceeded: false` with
//     `["AMQ229067: Cannot find resource with name queue.<name>"]`.
//   - STOMP has no temporary queue, so the reply queue is made with a
//     `createQueue` of its own (ANYCAST, on an address of its own name, which
//     the create makes), which needs no auto-creation.
//   - The broker does not remove a reply queue left behind. Its address is
//     never reaped (the image's configuration sets `auto-delete-addresses`
//     false for `#`, and `activemq.management.#` does not override it) and
//     it survives a restart; the queue is reaped about a minute after its
//     consumer goes, and one that never had a consumer is not reaped at all.
//     So every reply queue is destroyed, queue and address, by this driver
//     ([dropReplies]).
//   - An account without the `manage` permission has its first SEND refused
//     with `AMQ339011` and no cause, and the connection closes.
//
// The exchange took about 7 ms against a local broker.

// managementAddress is the address Artemis answers management requests on.
const managementAddress = "activemq.management"

// replyQueuePrefix starts the name of the queue an answer comes back on. It is
// under the management address's own name, as the reply queues of Artemis's
// own management clients are, which the broker's default configuration gives
// an address setting of its own.
const replyQueuePrefix = managementAddress + ".iqmq."

// maxStranded bounds the reply queues a backend remembers to drop. A question
// cut short strands at most one, and the next question to the same node drops
// them all, so the list is longer than one only while every question to a
// node is cut short; past the bound the oldest is forgotten and stays on the
// broker.
const maxStranded = 16

// strandedReply is a reply queue a question was cut short after making, and
// the node it was made on.
type strandedReply struct{ addr, name string }

// queueAddress asks the Artemis broker at addr which address the queue called
// queue is bound to, and reports found false when it has no queue of that
// name.
//
// It runs over a connection of its own, which it dials and closes. A refused
// management request is an ERROR frame, and an ERROR frame closes the
// connection it arrives on: on the source's own connection, a refusal would
// have been that connection's death as well as the answer.
//
// The whole exchange is bounded by Config.ReceiptTimeout after the dial, as
// any round trip of this driver's is, and by ctx. go-stomp's own waits take no
// context, so when ctx ends first the connection is closed under them, which
// ends each of them, and the function waits for that close before it returns.
//
// # The reply queue is dropped, and so is one a question before left
//
// A question that ends normally drops its reply queue and waits for the
// receipt on the drop's last frame, which says the broker has processed it.
// One cut short after its createQueue went out, by ctx or by the bound, or
// whose drop gets no receipt, cannot wait for anything, so the backend
// remembers the reply queue as stranded. The next question to the same node
// drops the reply queues stranded there before it makes its own, as many as
// its write queue has room for, and its createQueue's receipt says those drops
// were processed, so they are forgotten then; Close drops the ones on the node
// the backend's own connection reaches before its DISCONNECT, as many as that
// connection's write queue has room for. No goroutine does either, and
// neither waits for room.
func (b *Backend) queueAddress(ctx context.Context, addr, queue string) (bound string, found bool, err error) {
	conn, _, err := b.dialOne(ctx, addr, func(error) {})
	if err != nil {
		return "", false, err
	}
	ctx, cancel := context.WithTimeoutCause(ctx, b.cfg.ReceiptTimeout, errNoQueueAnswer)
	defer cancel()
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(finished)
		_ = conn.MustDisconnect()
	})
	defer func() {
		if !stop() {
			<-finished
		}
		_ = conn.MustDisconnect()
	}()

	dropped := dropReplies(conn, b.strandedAt(addr))
	reply := replyQueuePrefix + rand.Text()
	// With a receipt, so that a refusal reaches this call with the broker's
	// sentence. Without one the ERROR frame could arrive before the
	// subscription exists, and go-stomp would drop it.
	opts := append(managementOpts("broker", "createQueue", ""), gostomp.SendOpt.Receipt)
	if err := conn.Send(managementAddress, "", replyQueueArgs(reply), opts...); err != nil {
		if mayHaveMade(err) {
			b.strand(addr, reply)
		}
		return "", false, settled(ctx, managementRefusal(err))
	}
	// The receipt says the broker has processed every frame before the
	// createQueue on this connection, the drops among them.
	b.unstrand(addr, dropped)
	m, err := ask(ctx, conn, reply, queue)
	if err == nil && ctx.Err() == nil && dropReply(conn, reply) == nil {
		_ = conn.Disconnect()
		return readAnswer(m)
	}
	b.strand(addr, reply)
	if err != nil {
		return "", false, err
	}
	return readAnswer(m)
}

// ask subscribes to the reply queue, asks `getAddress` of queue and waits for
// the answer, which it unsubscribes from once it has it.
//
// It unsubscribes only then. Every other way out ends with the connection
// closed, which ends the subscription as well, and an Unsubscribe on a
// connection already closed is a race inside go-stomp v3.2.1: it closes the
// subscription's channel from the caller's goroutine while the library's own
// reader may still be sending the answer on it, which the race detector
// reported on the pinned Artemis and which panics when the send comes second.
// With the answer read, nothing else is sent on that channel.
func ask(ctx context.Context, conn *gostomp.Conn, reply, queue string) (*gostomp.Message, error) {
	sub, err := conn.Subscribe(reply, gostomp.AckAuto, gostomp.SubscribeOpt.Header("subscription-type", "ANYCAST"))
	if err != nil {
		return nil, settled(ctx, err)
	}
	if err := conn.Send(managementAddress, "", []byte("[]"), managementOpts("queue."+queue, "getAddress", reply)...); err != nil {
		return nil, settled(ctx, err)
	}
	select {
	case m, ok := <-sub.C:
		switch {
		case !ok:
			return nil, settled(ctx, gostomp.ErrClosedUnexpectedly)
		case m.Err != nil:
			return nil, settled(ctx, managementRefusal(m.Err))
		}
		_ = sub.Unsubscribe()
		return m, nil
	case <-ctx.Done():
		return nil, settled(ctx, nil)
	}
}

// dropReplies asks the broker to destroy each reply queue and then its
// address, and returns the replies it sent the drops of. It waits for no
// answer: a failed drop, such as one of a queue the broker reaped or never
// had, answers nobody and changes nothing (measured on the pinned image). Each
// is ordered before the frames that follow it on conn, so a later receipt on
// conn says it was processed.
//
// It sends a drop only while conn's write queue has room ([hasRoom]) and
// never waits for room, so that a broker that has stopped reading costs a
// question or Close nothing here; the drops it did not send stay stranded.
func dropReplies(conn *gostomp.Conn, replies []string) []string {
	var sent []string
	for _, reply := range replies {
		if !hasRoom(conn) || sendDrop(conn, reply) != nil {
			return sent
		}
		sent = append(sent, reply)
	}
	return sent
}

// dropReply drops one reply queue and waits for the broker to say it has
// processed the drop: a receipt on its last frame. A receipted Send reports
// the connection's end as a failure, where go-stomp's Disconnect returns nil
// once the connection has closed under it, which said nothing about the drop.
func dropReply(conn *gostomp.Conn, reply string) error {
	if !hasRoom(conn) {
		return errWriteQueueFull
	}
	return sendDrop(conn, reply, gostomp.SendOpt.Receipt)
}

// sendDrop sends a reply queue's destroyQueue and then its deleteAddress,
// which carries opts.
func sendDrop(conn *gostomp.Conn, reply string, opts ...func(*frame.Frame) error) error {
	name := strconv.Quote(reply)
	err := conn.Send(managementAddress, "", []byte("["+name+",true,true]"), managementOpts("broker", "destroyQueue", "")...)
	if err != nil {
		return err
	}
	return conn.Send(managementAddress, "", []byte("["+name+"]"),
		append(managementOpts("broker", "deleteAddress", ""), opts...)...)
}

// mayHaveMade reports whether a createQueue whose Send failed with err may
// have made the reply queue all the same. The broker's ERROR frame answering
// it says it did not, and so does a frame go-stomp never queued, on a
// connection already closed or with no slot within MsgSendTimeout. Anything
// else, a connection that ended while the receipt was awaited, may have.
func mayHaveMade(err error) bool {
	var refusal gostomp.Error
	refused := errors.As(err, &refusal) && refusal.Frame != nil && refusal.Frame.Command == frame.ERROR && !isConnGone(err)
	unsent := errors.Is(err, gostomp.ErrAlreadyClosed) || errors.Is(err, gostomp.ErrMsgSendTimeout)
	return !refused && !unsent
}

// strand remembers a reply queue a question was cut short after making.
func (b *Backend) strand(addr, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.stranded) == maxStranded {
		b.stranded = slices.Delete(b.stranded, 0, 1)
	}
	b.stranded = append(b.stranded, strandedReply{addr: addr, name: name})
}

// strandedAt is the reply queues stranded on the node at addr.
func (b *Backend) strandedAt(addr string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, r := range b.stranded {
		if r.addr == addr {
			out = append(out, r.name)
		}
	}
	return out
}

// unstrand forgets reply queues on the node at addr, which the broker has
// dropped.
func (b *Backend) unstrand(addr string, names []string) {
	if len(names) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stranded = slices.DeleteFunc(b.stranded, func(r strandedReply) bool {
		return r.addr == addr && slices.Contains(names, r.name)
	})
}

// settled is the error an exchange step reports. Once ctx has ended, the
// connection's end is [Backend.queueAddress]'s own doing and says nothing
// about the broker, so the step reports why ctx ended: errNoQueueAnswer when
// the exchange's own bound ran out, and the caller's context error otherwise.
func settled(ctx context.Context, err error) error {
	switch {
	case ctx.Err() == nil:
		return err
	case errors.Is(context.Cause(ctx), errNoQueueAnswer):
		return errNoQueueAnswer
	}
	return ctx.Err()
}

// managementRefusal is a refusal of a management SEND, said as what it most
// likely is: Artemis's sentence for it names no cause.
func managementRefusal(err error) error {
	if strings.Contains(err.Error(), "AMQ339011") {
		return fmt.Errorf("%w: %w", errManagementRefused, err)
	}
	return err
}

// managementOpts are the headers of a management request. replyTo names
// where the answer goes, and an empty one asks for no answer.
func managementOpts(resource, operation, replyTo string) []func(*frame.Frame) error {
	opts := []func(*frame.Frame) error{
		gostomp.SendOpt.Header("_AMQ_ResourceName", resource),
		gostomp.SendOpt.Header("_AMQ_OperationName", operation),
		gostomp.SendOpt.NoContentLength,
	}
	if replyTo != "" {
		opts = append(opts, gostomp.SendOpt.Header("reply-to", replyTo))
	}
	return opts
}

// replyQueueArgs is `createQueue`'s argument for the reply queue: an ANYCAST
// queue on an address of its own name, which the create makes. The
// `auto-delete` fields let the broker reap the queue once a consumer has come
// and gone; they do nothing for its address or for a queue that never had a
// consumer, which is why [dropReplies] exists.
func replyQueueArgs(reply string) []byte {
	conf, _ := json.Marshal(map[string]any{
		"name":                      reply,
		"address":                   reply,
		"routing-type":              "ANYCAST",
		"durable":                   false,
		"auto-create-address":       true,
		"auto-delete":               true,
		"auto-delete-delay":         0,
		"auto-delete-message-count": -1,
		"purge-on-no-consumers":     true,
	})
	return []byte("[" + strconv.Quote(string(conf)) + "]")
}

// readAnswer reads `getAddress`'s answer.
func readAnswer(m *gostomp.Message) (bound string, found bool, err error) {
	var body []string
	if json.Unmarshal(m.Body, &body) != nil || len(body) != 1 {
		return "", false, fmt.Errorf("%w: %q", errBadQueueAnswer, m.Body)
	}
	switch m.Header.Get("_AMQ_OperationSucceeded") {
	case "true":
		return body[0], true, nil
	case "false":
		if strings.Contains(body[0], "AMQ229067") {
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: %s", errQueueQuestionFailed, body[0])
	}
	return "", false, fmt.Errorf("%w: no _AMQ_OperationSucceeded header", errBadQueueAnswer)
}

// verifyQueue asks the broker, when Config.VerifyQueueAddress says to, whether
// this source's queue is bound to its address, before the first SUBSCRIBE on
// conn. A queue bound to another address ends the source for good, as a
// delivery from one does ([Source.refuseForeign]): every subscription it could
// make attaches to that queue. A queue the broker does not have is left to the
// SUBSCRIBE, which reports it or creates it as the broker is configured to.
//
// It asks once per connection, so a requeue that keeps its connection does not
// ask again. It runs within the caller's context, like the dial before it, so
// Close does not cut a fetch's question short: the question ends at
// Config.ReceiptTimeout or the fetch's deadline. The caller holds subMu.
func (s *Source) verifyQueue(ctx context.Context, conn *gostomp.Conn) error {
	if !s.b.cfg.VerifyQueueAddress || !s.b.brokerIsArtemis() {
		return nil
	}
	s.mu.Lock()
	asked, at := s.verified == conn, s.connAt
	s.mu.Unlock()
	if asked {
		return nil
	}
	bound, found, err := s.b.queueAddress(ctx, s.b.cfg.Addrs[at], s.queue)
	if err != nil {
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, err, false)
	}
	if found && bound != s.address {
		refusal := fmt.Errorf("%w: Artemis says the queue %q is bound to the address %q, and this subscription's "+
			"address is %q", errForeignQueue, s.queue, bound, s.address)
		s.mu.Lock()
		s.refusal = refusal
		s.mu.Unlock()
		return s.b.errs.Map(mq.OpOpen, s.topic, s.sub, refusal)
	}
	s.mu.Lock()
	s.verified = conn
	s.mu.Unlock()
	return nil
}
