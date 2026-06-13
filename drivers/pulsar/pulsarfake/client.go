package pulsarfake

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
)

// The compile-time proof that the model is the driver's client seam.
var (
	_ pulsarapi.Client       = (*fclient)(nil)
	_ pulsar.Producer        = (*fproducer)(nil)
	_ pulsar.Consumer        = (*fconsumer)(nil)
	_ pulsarapi.BatchTracker = (*fconsumer)(nil)
)

// fclient is the model's client.
type fclient struct {
	s      *Server
	closed bool
	mu     sync.Mutex
}

// CreateProducer opens a producer on one topic.
func (c *fclient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, resultErr(pulsar.AlreadyClosedError, "client closed")
	}
	c.mu.Unlock()
	if o.Topic == "" {
		return nil, resultErr(pulsar.InvalidTopicName, "no topic")
	}
	c.s.mu.Lock()
	t := c.s.topic(o.Topic)
	t.batcher = o.BatcherBuilderType
	c.s.mu.Unlock()
	return &fproducer{s: c.s, topic: o.Topic, name: o.Name, opts: o}, nil
}

// Subscribe opens a consumer on one topic.
//
// A pattern subscription is refused: this model is one topic per consumer, and
// pretending otherwise would let a test believe a pattern subscription had been
// exercised when nothing had. The pattern path is the conformance suite's, and
// the driver refuses a seek on it for the same reason a real client does.
func (c *fclient) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, resultErr(pulsar.AlreadyClosedError, "client closed")
	}
	c.mu.Unlock()
	switch {
	case o.TopicsPattern != "" || len(o.Topics) > 0:
		return nil, resultErr(pulsar.OperationNotSupported, "this model subscribes to one topic at a time")
	case o.Topic == "":
		return nil, resultErr(pulsar.InvalidTopicName, "no topic")
	case o.SubscriptionName == "":
		return nil, resultErr(pulsar.InvalidConfiguration, "no subscription name")
	case o.MessageChannel == nil:
		return nil, resultErr(pulsar.InvalidConfiguration, "no message channel")
	}
	c.s.mu.Lock()
	t := c.s.topic(o.Topic)
	fs := t.subs[o.SubscriptionName]
	if fs == nil {
		fs = newSub(o)
		fs.initStart(t, o.SubscriptionInitialPosition == pulsar.SubscriptionPositionEarliest)
		t.subs[o.SubscriptionName] = fs
	}
	if fs.typ == pulsar.Exclusive && len(fs.consumers) > 0 {
		c.s.mu.Unlock()
		// Pulsar's own answer, and the reason FeatureConsumerGroups is
		// not claimed for an Exclusive subscription.
		return nil, resultErr(pulsar.ConsumerBusy, "exclusive subscription already has a consumer")
	}
	cons := &fconsumer{
		s: c.s, t: t, fs: fs, ch: o.MessageChannel, name: o.Name,
		batchIndexAck: o.EnableBatchIndexAcknowledgment,
		dlq:           o.DLQ, retry: o.RetryEnable,
	}
	fs.consumers = append(fs.consumers, cons)
	fs.reassign()
	c.s.mu.Unlock()
	c.s.pumpAll()
	return cons, nil
}

// TopicPartitions is the topic's partition names, which is a metadata lookup
// and creates nothing.
func (c *fclient) TopicPartitions(topic string) ([]string, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	t := c.s.topics[topic]
	if t == nil {
		return []string{topic}, nil
	}
	if t.count <= 0 {
		return []string{topic}, nil
	}
	out := make([]string, 0, t.count)
	for i := range t.count {
		out = append(out, topic+"-partition-"+strconv.Itoa(i))
	}
	return out, nil
}

// NewTransaction is the transaction coordinator, which this model does not
// have: x/pulsartx is tested against its own fake.
func (c *fclient) NewTransaction(time.Duration) (pulsar.Transaction, error) {
	return nil, resultErr(pulsar.TransactionCoordinatorNotEnabled, "no transaction coordinator")
}

// Close closes the client and everything it made.
func (c *fclient) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// fproducer is the model's producer, bound to one topic.
type fproducer struct {
	s     *Server
	topic string
	name  string
	opts  pulsar.ProducerOptions

	mu     sync.Mutex
	seq    int64
	closed bool
}

// Topic is the producer's topic.
func (p *fproducer) Topic() string { return p.topic }

// Name is the producer's name.
func (p *fproducer) Name() string { return p.name }

// Send publishes one message and returns the broker's send receipt.
func (p *fproducer) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return p.store(m)
}

// SendAsync publishes one message and reports the outcome to the callback,
// which is the native asynchronous path.
func (p *fproducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	if err := ctxErr(ctx); err != nil {
		if cb != nil {
			cb(nil, m, err)
		}
		return
	}
	id, err := p.store(m)
	if cb != nil {
		cb(id, m, err)
	}
}

// store puts one message in the topic, batching it with the entry the producer
// is filling.
func (p *fproducer) store(m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, resultErr(pulsar.ProducerClosed, "producer closed")
	}
	p.seq++
	p.mu.Unlock()
	p.s.mu.Lock()
	full := p.s.topic(p.topic).queueFull
	p.s.mu.Unlock()
	if full {
		// The message was NEVER admitted, which is what makes this
		// backpressure rather than an ambiguous publication.
		return nil, resultErr(pulsar.ProducerQueueIsFull, "queue full")
	}

	p.s.mu.Lock()
	t := p.s.topic(p.topic)
	props := make(map[string]string, len(m.Properties))
	for k, v := range m.Properties {
		props[k] = v
	}
	// A nil payload and an empty one are two different messages, and
	// `append([]byte(nil), empty...)` is nil — which would make the model
	// lose the one distinction Pulsar keeps natively.
	var payload []byte
	if m.Payload != nil {
		payload = append(make([]byte, 0, len(m.Payload)), m.Payload...)
	}
	msg := &fmsg{
		payload:     payload,
		nullValue:   m.Payload == nil && m.Value == nil,
		key:         m.Key,
		orderingKey: m.OrderingKey,
		props:       props,
		eventTime:   m.EventTime,
		publishTime: p.s.now,
		producer:    p.name,
	}
	part := p.partitionFor(t, m.Key)
	due := time.Time{}
	switch {
	case !m.DeliverAt.IsZero():
		due = m.DeliverAt
	case m.DeliverAfter > 0:
		due = p.s.now.Add(m.DeliverAfter)
	}
	lg := t.logOf(part)
	idx, batchIdx, size := p.append(t, lg, msg, due)
	id := &msgID{
		topic: p.topic, ledger: 1, entry: int64(idx),
		partition: int32(part), batchIdx: batchIdx, batchSize: size,
		sl: slot{part: part, entry: idx, idx: int(max32(batchIdx, 0))},
	}
	if t.count <= 0 {
		id.partition = -1
	}
	p.s.mu.Unlock()
	p.s.pumpAll()
	return id, nil
}

// append puts the message in the partition's log, in the entry the producer is
// filling, and returns the entry index and the message's place in it.
//
// The batching rule is the driver's own configuration made observable: with the
// key-based batcher one entry holds one KEY, and with the default batcher it
// holds whatever arrives until the batch is full. It is what the Key_Shared
// ordering test reads — an entry is dispatched to one consumer, so an entry
// holding two keys is an entry Pulsar cannot route by key.
func (p *fproducer) append(t *ftopic, lg *plog, msg *fmsg, due time.Time) (entryIdx int, batchIdx, size int32) {
	batch := p.s.opt.batchSize
	if p.opts.DisableBatching || batch <= 1 {
		lg.entries = append(lg.entries, &fentry{msgs: []*fmsg{msg}, due: due, closed: true})
		return len(lg.entries) - 1, -1, 1
	}
	keyed := t.batcher == pulsar.KeyBasedBatchBuilder
	if n := len(lg.entries); n > 0 {
		e := lg.entries[n-1]
		sameKey := !keyed || (len(e.msgs) > 0 && e.msgs[0].key == msg.key)
		if !e.closed && len(e.msgs) < batch && e.due.Equal(due) && sameKey {
			e.msgs = append(e.msgs, msg)
			if len(e.msgs) >= batch {
				e.closed = true
			}
			return n - 1, int32(len(e.msgs) - 1), int32(len(e.msgs))
		}
		// A new batch is started, which SENDS the one before it: that is
		// what a batcher does when the next message cannot join.
		e.closed = true
	}
	lg.entries = append(lg.entries, &fentry{msgs: []*fmsg{msg}, due: due})
	return len(lg.entries) - 1, 0, 1
}

// partitionFor is where a message goes, which is the producer's router.
//
// It reads the topic's CURRENT partition count, which is what makes partition
// expansion observable: the same key lands in a different partition once the
// count grows, and the key's older and newer messages are then in two logs with
// no order between them.
func (p *fproducer) partitionFor(t *ftopic, key string) int {
	n := t.partitionCount()
	if n == 1 {
		return 0
	}
	if key == "" {
		t.rr++
		return t.rr % n
	}
	return int(javaStringHash(key) % uint32(n))
}

// LastSequenceID is the last sequence the producer used.
func (p *fproducer) LastSequenceID() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seq
}

// Flush sends what the producer is still batching.
func (p *fproducer) Flush() error { p.s.Flush(); return nil }

// FlushWithCtx sends what the producer is still batching.
func (p *fproducer) FlushWithCtx(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	p.s.Flush()
	return nil
}

// Close closes the producer, sending what it was batching.
func (p *fproducer) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.s.Flush()
}

// max32 is the larger of two int32 values.
func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
