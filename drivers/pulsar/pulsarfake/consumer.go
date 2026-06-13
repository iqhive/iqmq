package pulsarfake

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// fconsumer is one consumer of one subscription.
type fconsumer struct {
	s    *Server
	t    *ftopic
	fs   *fsub
	ch   chan pulsar.ConsumerMessage
	name string

	batchIndexAck bool
	dlq           *pulsar.DLQPolicy
	retry         bool

	mu     sync.Mutex
	closed bool
}

// BatchTracked reports whether this consumer's identifiers carry the client's
// per-entry batch bookkeeping, which is
// [bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi.BatchTracker].
//
// It is the whole reason that interface exists: a test can make this false and
// watch the driver refuse a settlement it cannot make safely, which no test
// against the real client could arrange.
func (c *fconsumer) BatchTracked() bool { return !c.s.opt.untracked }

// Subscription is the subscription's name.
func (c *fconsumer) Subscription() string {
	for name, fs := range c.t.subs {
		if fs == c.fs {
			return name
		}
	}
	return ""
}

// Name is the consumer's name.
func (c *fconsumer) Name() string { return c.name }

// Chan is the message channel.
func (c *fconsumer) Chan() <-chan pulsar.ConsumerMessage { return c.ch }

// Receive reads one message.
func (c *fconsumer) Receive(ctx context.Context) (pulsar.Message, error) {
	select {
	case cm := <-c.ch:
		return cm.Message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Ack settles a message positively.
func (c *fconsumer) Ack(m pulsar.Message) error { return c.AckID(m.ID()) }

// AckID settles one identifier positively.
//
// This is where the client's batch bookkeeping and the broker's
// acknowledgement-set support meet, and the five outcomes are the truth table
// [bitbucket.org/iqhive/iqmq/drivers/pulsar.AckRules] states.
func (c *fconsumer) AckID(id pulsar.MessageID) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return resultErr(pulsar.ConsumerClosed, "consumer closed")
	}
	c.mu.Unlock()
	fid, ok := id.(*msgID)
	if !ok {
		return resultErr(pulsar.InvalidMessage, "identifier is not this model's")
	}
	c.s.mu.Lock()
	fs := c.fs
	sl := fid.sl
	if fs.acked[sl] {
		c.s.mu.Unlock()
		return nil
	}
	if g, ok := fs.gen[sl]; ok && fid.gen != g && fs.inflight[sl] {
		// A settlement for a delivery a later one superseded. A real
		// broker applies it — an acknowledgement names a position — so
		// this model applies it too, and counts it: a driver that let one
		// through has settled work it never completed.
		fs.stats.StaleSettled++
	}
	batched := fid.batchIdx >= 0 && fid.batchSize > 1
	settleEntry := true
	switch {
	case !batched:
		// The entry holds this message alone.
	case fid.tracker == nil:
		// The identifier lost the client's batch bookkeeping, so
		// `trackingMessageID.ack()` answers "the whole batch is done".
	default:
		changed, complete := fid.tracker.ack(int(fid.batchIdx))
		if !changed {
			c.s.mu.Unlock()
			return nil
		}
		switch {
		case c.batchIndexAck && c.s.opt.brokerBatchAck:
			// The frame carries an acknowledgement set and the broker
			// honours it: the named index alone is settled.
			settleEntry = false
		case c.batchIndexAck:
			// The frame carries a set the broker IGNORES, so the
			// broker settles the entry. This is the combination that
			// loses messages.
		case complete:
			// Every index is clear, so the client sends the entry's
			// frame now.
		default:
			// The client holds the frame back. Nothing reaches the
			// broker at all.
			c.s.mu.Unlock()
			return nil
		}
	}
	if settleEntry {
		fs.settleEntry(c.t, sl, fid.tracker)
	} else {
		fs.settleOne(sl, true)
	}
	fs.unblock()
	c.s.mu.Unlock()
	c.s.pumpAll()
	return nil
}

// AckIDList settles many identifiers in one frame.
func (c *fconsumer) AckIDList(ids []pulsar.MessageID) error {
	for _, id := range ids {
		if err := c.AckID(id); err != nil {
			return err
		}
	}
	return nil
}

// AckWithTxn settles a message inside a transaction, which this model does not
// have.
func (c *fconsumer) AckWithTxn(pulsar.Message, pulsar.Transaction) error {
	return resultErr(pulsar.TransactionCoordinatorNotEnabled, "no transaction coordinator")
}

// AckCumulative settles every message up to and including this one.
func (c *fconsumer) AckCumulative(m pulsar.Message) error { return c.AckIDCumulative(m.ID()) }

// AckIDCumulative settles every message up to and including this identifier.
//
// The closed check is the same one AckID makes, and it was missing here: a
// cumulative acknowledgement through a closed consumer returned nil while the
// single-message one returned ConsumerClosed. The library refuses both, and
// the asymmetry mattered in exactly the shutdown race a driver meets -- a
// handler settling while the consumer closes would have been told its work
// was acknowledged when a real broker would redeliver it.
func (c *fconsumer) AckIDCumulative(id pulsar.MessageID) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return resultErr(pulsar.ConsumerClosed, "consumer closed")
	}
	c.mu.Unlock()
	fid, ok := id.(*msgID)
	if !ok {
		return resultErr(pulsar.InvalidMessage, "identifier is not this model's")
	}
	c.s.mu.Lock()
	lg := c.t.parts[fid.sl.part]
	for i := 0; i <= fid.sl.entry && i < len(lg.entries); i++ {
		for k := range lg.entries[i].msgs {
			if i == fid.sl.entry && k > fid.sl.idx {
				continue
			}
			c.fs.settleOne(slot{part: fid.sl.part, entry: i, idx: k}, true)
		}
	}
	c.fs.unblock()
	c.s.mu.Unlock()
	c.s.pumpAll()
	return nil
}

// Nack asks for a message to be redelivered.
func (c *fconsumer) Nack(m pulsar.Message) { c.NackID(m.ID()) }

// NackID asks for one identifier's message to be redelivered after the
// consumer's NackRedeliveryDelay, which is the client's own hold.
func (c *fconsumer) NackID(id pulsar.MessageID) {
	fid, ok := id.(*msgID)
	if !ok {
		return
	}
	c.s.mu.Lock()
	fs := c.fs
	sl := fid.sl
	if !fs.acked[sl] {
		fs.stats.Nacked++
		delete(fs.inflight, sl)
		delete(fs.holder, sl)
		fs.nackDue[sl] = c.s.now.Add(fs.nackDelay)
	}
	fs.unblock()
	c.s.mu.Unlock()
	c.s.pumpAll()
}

// ReconsumeLater republishes the message to the retry topic with a delay, or to
// the dead-letter topic when its reconsume count has passed the policy's
// maximum — and acknowledges the original only once the republication is
// stored, which is pulsar-client-go's own rule.
func (c *fconsumer) ReconsumeLater(m pulsar.Message, delay time.Duration) {
	c.ReconsumeLaterWithCustomProperties(m, map[string]string{}, delay)
}

// ReconsumeLaterWithCustomProperties is [fconsumer.ReconsumeLater] with
// properties the caller adds, which is how the driver forces the dead-letter
// route for an explicit Reject.
func (c *fconsumer) ReconsumeLaterWithCustomProperties(m pulsar.Message, custom map[string]string, delay time.Duration) {
	fid, ok := m.ID().(*msgID)
	if !ok {
		return
	}
	props := map[string]string{}
	for k, v := range m.Properties() {
		props[k] = v
	}
	for k, v := range custom {
		props[k] = v
	}
	times := 1
	if s, ok := props[pulsar.SysPropertyReconsumeTimes]; ok {
		if n, err := parseInt(s); err == nil {
			times = n + 1
		}
	} else {
		props[pulsar.SysPropertyRealTopic] = m.Topic()
		props[pulsar.PropertyOriginMessageID] = m.ID().String()
	}
	props[pulsar.SysPropertyReconsumeTimes] = itoa(times)

	var dest string
	isDLQ := false
	switch {
	case c.dlq == nil:
		// No policy at all: pulsar-client-go would block on a nil
		// channel here. The model refuses instead, which is a defect
		// this driver's own guard prevents.
		return
	case uint32(times) > c.dlq.MaxDeliveries:
		dest, isDLQ = c.dlq.DeadLetterTopic, true
	case c.retry:
		dest = c.dlq.RetryLetterTopic
	default:
		return
	}
	if dest == "" {
		return
	}
	c.s.mu.Lock()
	dst := c.s.topic(dest)
	due := time.Time{}
	if !isDLQ && delay > 0 {
		due = c.s.now.Add(delay)
	}
	msg := &fmsg{
		payload: m.Payload(), nullValue: m.Payload() == nil, key: m.Key(),
		orderingKey: m.OrderingKey(), props: props, eventTime: m.EventTime(),
		publishTime: c.s.now, producer: "retry",
	}
	lg := dst.logOf(0)
	// Closed, because the router's producer publishes one message and
	// flushes: a retry or dead-letter publication is not batched with
	// anything and must be deliverable at once.
	lg.entries = append(lg.entries, &fentry{msgs: []*fmsg{msg}, due: due, closed: true})
	// Only now is the original acknowledged, which is the router's rule and
	// what makes a failed republication never lose the message.
	c.fs.settleEntry(c.t, fid.sl, fid.tracker)
	if isDLQ {
		c.fs.stats.DeadLettered++
	} else {
		c.fs.stats.Retried++
	}
	c.fs.unblock()
	c.s.mu.Unlock()
	c.s.pumpAll()
}

// Seek moves the subscription's cursor to an identifier.
func (c *fconsumer) Seek(id pulsar.MessageID) error {
	fid, ok := id.(*msgID)
	if !ok {
		return resultErr(pulsar.SeekFailed, "identifier is not this model's")
	}
	c.drain()
	c.s.mu.Lock()
	c.fs.rewind(fid.sl.part, fid.sl.entry)
	c.s.mu.Unlock()
	c.s.pumpAll()
	return nil
}

// drain empties the consumer's message channel, which pulsar-client-go does
// before a seek.
//
// It is not tidiness: a consumer that had already prefetched the messages it
// was about to seek past would hand them to the application after the seek, and
// the seek would look as if it had not happened. The real client drains the
// channel for the same reason.
func (c *fconsumer) drain() {
	for {
		select {
		case <-c.ch:
		default:
			return
		}
	}
}

// SeekByTime moves every partition's cursor to the first entry published at or
// after t, which is the only seek Pulsar applies across a partitioned topic.
func (c *fconsumer) SeekByTime(at time.Time) error {
	c.drain()
	c.s.mu.Lock()
	for part, lg := range c.t.parts {
		target := len(lg.entries)
		for i, e := range lg.entries {
			if len(e.msgs) > 0 && !e.msgs[0].publishTime.Before(at) {
				target = i
				break
			}
		}
		c.fs.rewind(part, target)
	}
	c.s.mu.Unlock()
	c.s.pumpAll()
	return nil
}

// GetLastMessageIDs is the last identifier of each partition.
func (c *fconsumer) GetLastMessageIDs() ([]pulsar.TopicMessageID, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	out := make([]pulsar.TopicMessageID, 0, len(c.t.parts))
	for part, lg := range c.t.parts {
		if len(lg.entries) == 0 {
			continue
		}
		i := len(lg.entries) - 1
		out = append(out, &msgID{
			topic: c.t.name, ledger: 1, entry: int64(i), partition: int32(part),
			batchIdx: -1, batchSize: int32(len(lg.entries[i].msgs)),
			sl: slot{part: part, entry: i},
		})
	}
	return out, nil
}

// Unsubscribe removes the subscription.
func (c *fconsumer) Unsubscribe() error {
	c.s.mu.Lock()
	delete(c.t.subs, c.Subscription())
	c.s.mu.Unlock()
	return nil
}

// UnsubscribeForce removes the subscription, whatever else is attached.
func (c *fconsumer) UnsubscribeForce() error { return c.Unsubscribe() }

// Close closes the consumer and makes everything it held available again, which
// is what Pulsar does on a disconnection — and is how this driver's Close gives
// work back.
func (c *fconsumer) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	c.s.mu.Lock()
	fs := c.fs
	kept := fs.consumers[:0]
	for _, other := range fs.consumers {
		if other != c {
			kept = append(kept, other)
		}
	}
	fs.consumers = kept
	fs.releaseHeldBy(c)
	fs.reassign()
	c.s.mu.Unlock()
	c.s.pumpAll()
}

// settleOne marks one message acknowledged and counts it.
func (fs *fsub) settleOne(sl slot, requested bool) {
	if fs.acked[sl] {
		return
	}
	fs.acked[sl] = true
	delete(fs.inflight, sl)
	delete(fs.holder, sl)
	delete(fs.nackDue, sl)
	if requested {
		fs.stats.Acked++
		return
	}
	// Nobody acknowledged this message and the broker marked it consumed
	// anyway, because an acknowledgement settled the whole entry it was in.
	fs.stats.CrossSettled++
}

// settleEntry marks every message of an entry acknowledged, counting the ones
// nobody asked for.
//
// The tracker is what tells the two apart. When the client held the frame back
// until every index was clear, every message in the entry WAS acknowledged by
// the application and the entry frame is the sum of those acknowledgements.
// When the identifier had no tracker, or the frame carried a set the broker
// ignored, the siblings were never acknowledged by anybody — and those are the
// cross-settled ones the specification is about.
func (fs *fsub) settleEntry(t *ftopic, sl slot, tracker *entryAcks) {
	if sl.part >= len(t.parts) || sl.entry >= len(t.parts[sl.part].entries) {
		return
	}
	e := t.parts[sl.part].entries[sl.entry]
	fs.settleOne(sl, true)
	for k := range e.msgs {
		requested := tracker != nil && tracker.cleared(k)
		fs.settleOne(slot{part: sl.part, entry: sl.entry, idx: k}, requested)
	}
}

// rewind moves one partition's cursor to an entry index and forgets what it had
// settled from there on, which is what a seek does.
//
// It also takes back everything the partition had HANDED OUT, whatever its
// index: a seek resets the subscription's cursor, so a delivery that was in
// flight before it is a delivery nobody owns afterwards. Leaving them in flight
// made a seek past an already-prefetched message look as if it had not
// happened.
func (fs *fsub) rewind(part, entry int) {
	fs.start[part] = entry
	for sl := range fs.acked {
		if sl.part == part && sl.entry >= entry {
			delete(fs.acked, sl)
		}
	}
	for sl := range fs.inflight {
		if sl.part == part {
			delete(fs.inflight, sl)
			delete(fs.holder, sl)
		}
	}
	for sl := range fs.nackDue {
		if sl.part == part {
			delete(fs.nackDue, sl)
		}
	}
}

// parseInt is strconv.Atoi, wrapped so that consumer.go does not import strconv
// for one call.
func parseInt(s string) (int, error) { return atoi(s) }
