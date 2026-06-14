package rabbitfake

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Restart closes every connection and then drops every queue, every
// client-declared exchange and every binding, which is the one failure a
// topology REPLAY exists for.
//
// A broker that came back without the topology it had is the failure it
// models, and nothing else in this package can produce it. A durable exchange
// and a quorum queue survive an ordinary restart, so a driver that never
// replayed would usually seem to work — and the delay queues
// do NOT survive, because `x-expires` deletes an unused one, so a client that
// did not replay would come back unable to delay a redelivery at all. Without
// this the replay is unobservable: every declaration the driver repeats is
// idempotent, so a test could not tell a driver that replayed from one that
// did not.
//
// The broker's OWN exchanges are kept, because a real broker does not lose
// them: the default exchange and `amq.*` are the server's and a client may
// neither declare nor delete them. The observation logs are kept too — the
// arrival log, the settlements, the dead-letter hops and the drops are the
// past, and a test asking "what happened before the outage" is asking about
// something the outage did not undo.
func (b *Broker) Restart(err *amqp.Error) {
	// The connections go FIRST and the topology second, and the order is the
	// whole reason this function exists rather than a test calling the two
	// itself.
	//
	// A driver notices the close and starts its dial loop at once, so a test
	// that closed and then forgot would be racing its own driver: if the
	// replay reached the broker before the forget, the replay's declarations
	// would be the ones deleted and nothing would ever recreate them --
	// which is a test that fails for a reason the driver had no part in.
	// Doing both before any dial can succeed is what makes the replay
	// deterministically observable.
	//
	// Closing first also keeps `Forget`'s consumer cancellations off the
	// wire: a `basic.cancel` for a queue that is going away tells the driver
	// "your subscription is gone" rather than "the connection went", and the
	// two are answered differently. A restarting broker sends
	// neither -- the socket simply closes.
	// The broker stops LISTENING first, which is what makes the sequence
	// deterministic rather than merely ordered: the driver's dial loop starts
	// the instant the close lands, so without this a dial could succeed and
	// replay the topology between the close and the forget -- and the
	// forget would then delete the replay's own declarations, leaving the
	// driver attached to a broker with no queues and nothing to notice it.
	// A restarting broker is not listening either, so this is the faithful
	// model as well as the deterministic one.
	b.SetDown(true)
	for _, h := range b.Connections() {
		h.CloseWith(err)
	}
	b.Forget()
	b.SetDown(false)
}

// Forget drops every queue, every client-declared exchange and every binding.
// See the package's own [Broker.Restart], which is what a test that also wants
// the connections closed should call instead.
func (b *Broker) Forget() {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := make(map[string]*exchange, len(b.exchanges))
	order := make([]string, 0, len(b.exOrder))
	for _, name := range b.exOrder {
		ex, ok := b.exchanges[name]
		if !ok || !ex.predeclared {
			continue
		}
		// A kept exchange loses its bindings and whatever it was holding:
		// the bindings named queues that are gone, and a delayed-message
		// exchange's held messages went with the queues they were bound for.
		ex.bindings = nil
		ex.held = nil
		kept[name] = ex
		order = append(order, name)
	}
	b.exchanges, b.exOrder = kept, order

	// The queues go through the broker's own drop path rather than being
	// deleted from the map, and the difference is not tidiness. That path
	// records a Drop for every message it destroys, cancels the consumers
	// that were reading the queue, and marks the queue deleted so that a
	// settlement for a delivery out of it is refused rather than applied to
	// an orphan. Emptying the map directly was the first version of this
	// function, and it made three things wrong at once: the destroyed
	// messages were invisible to Dropped(), so FuzzRabbitFake's "nothing is
	// lost" invariant could not see them; no `basic.cancel` reached the
	// driver, so a consumer went on believing it had one; and a later ack for
	// a tag issued before the outage decremented the unacknowledged count on
	// a queue nothing could reach.
	for _, name := range append([]string(nil), b.qOrder...) {
		if q, ok := b.queues[name]; ok {
			b.dropQueueLocked(q, DropQueueDeleted)
		}
	}
	b.queues, b.qOrder = map[string]*queue{}, nil
}

// Bind and Unbind add and remove one binding from outside the client, which is
// what makes a publication from ONE publisher routable and then not.
//
// They exist for the `basic.return` attribution: a return carries the returned
// message and not its delivery tag, so a driver with more than one publication
// outstanding needs a correlation token to know which one came back — and the
// only way to put a routable and an unroutable publication on ONE channel,
// with several outstanding at once, is to change the binding between them. A
// second publisher would give each sink its own confirm tracker with one
// publication outstanding, which is the case the token is NOT needed for.
//
// `queue.bind` and `queue.unbind` are real AMQP 0-9-1 methods; these are the
// same operations performed by the test rather than by the client, because the
// client under test is the thing whose behaviour is the subject.
func (b *Broker) Bind(exchange, queue, key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ex, ok := b.exchanges[exchange]
	if !ok {
		return false
	}
	for _, bd := range ex.bindings {
		if bd.queue == queue && bd.key == key {
			return true
		}
	}
	ex.bindings = append(ex.bindings, binding{queue: queue, key: key})
	return true
}

// Unbind removes one binding, reporting whether it was there.
func (b *Broker) Unbind(exchange, queue, key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ex, ok := b.exchanges[exchange]
	if !ok {
		return false
	}
	for i, bd := range ex.bindings {
		if bd.queue == queue && bd.key == key {
			ex.bindings = append(ex.bindings[:i], ex.bindings[i+1:]...)
			return true
		}
	}
	return false
}
