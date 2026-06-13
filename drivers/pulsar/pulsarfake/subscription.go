package pulsarfake

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// fsub is one Pulsar subscription: a cursor per partition, the consumers
// sharing it, and the settlement bookkeeping.
type fsub struct {
	typ             pulsar.SubscriptionType
	allowOutOfOrder bool
	nackDelay       time.Duration

	consumers []*fconsumer

	// start is the first entry index this subscription dispatches per
	// partition, which is what SubscriptionInitialPosition decides. A
	// partition that appeared after the subscription was created is not in
	// the map and starts at its beginning, which is what a real broker
	// does with a new partition.
	start map[int]int

	acked    map[slot]bool
	inflight map[slot]bool
	holder   map[slot]*fconsumer
	// gen is the delivery generation per slot, which is what lets a
	// settlement for a SUPERSEDED delivery be recognised. The broker cannot
	// recognise one — an acknowledgement names a position — so this is the
	// model's instrumentation rather than the broker's behaviour.
	gen map[slot]uint32
	// redeliveries is how many times a slot has been handed out, which is
	// Pulsar's RedeliveryCount.
	redeliveries map[slot]uint32
	// nackDue is when a negatively acknowledged message becomes deliverable
	// again. It is the CLIENT's hold (NackRedeliveryDelay) and applies to
	// every subscription type, unlike the broker's delayed delivery.
	nackDue map[slot]time.Time

	// owner and blocked are Key_Shared's hash-range assignment.
	owner   map[int]*fconsumer
	blocked map[int]bool

	rr    int
	stats Stats

	// topicOf is the topic this subscription belongs to, kept so that the
	// hash-range bookkeeping can read a slot's key.
	topicOf *ftopic
}

// newSub builds a subscription from the options the consumer was opened with.
func newSub(o pulsar.ConsumerOptions) *fsub {
	fs := &fsub{
		typ:          o.Type,
		nackDelay:    o.NackRedeliveryDelay,
		start:        map[int]int{},
		acked:        map[slot]bool{},
		inflight:     map[slot]bool{},
		holder:       map[slot]*fconsumer{},
		gen:          map[slot]uint32{},
		redeliveries: map[slot]uint32{},
		nackDue:      map[slot]time.Time{},
		owner:        map[int]*fconsumer{},
		blocked:      map[int]bool{},
	}
	if o.KeySharedPolicy != nil {
		fs.allowOutOfOrder = o.KeySharedPolicy.AllowOutOfOrderDelivery
	}
	if fs.nackDelay <= 0 {
		fs.nackDelay = time.Second
	}
	return fs
}

// initStart records where the subscription begins, which is only read the first
// time a consumer of that name is opened.
func (fs *fsub) initStart(t *ftopic, earliest bool) {
	if len(fs.start) > 0 {
		return
	}
	for i, p := range t.parts {
		if earliest {
			fs.start[i] = 0
			continue
		}
		fs.start[i] = len(p.entries)
	}
}

// startOf is the first entry index this subscription dispatches from a
// partition.
func (fs *fsub) startOf(part int) int {
	if v, ok := fs.start[part]; ok {
		return v
	}
	return 0
}

// entryDone reports whether every message of an entry has been acknowledged.
func (fs *fsub) entryDone(t *ftopic, sl slot) bool {
	e := t.parts[sl.part].entries[sl.entry]
	for k := range e.msgs {
		if !fs.acked[slot{part: sl.part, entry: sl.entry, idx: k}] {
			return false
		}
	}
	return true
}

// entryInflight reports whether any message of an entry is out with a consumer.
func (fs *fsub) entryInflight(t *ftopic, part, entry int) bool {
	e := t.parts[part].entries[entry]
	for k := range e.msgs {
		if fs.inflight[slot{part: part, entry: entry, idx: k}] {
			return true
		}
	}
	return false
}

// deliverable reports whether an entry may be dispatched now.
//
// Two different holds, and the difference is Pulsar's: the BROKER's delayed
// delivery (DeliverAfter, DeliverAt) belongs to the Shared and Key_Shared
// dispatchers and an Exclusive or Failover subscription receives such a message
// at once, while the CLIENT's negative-acknowledgement hold applies to every
// subscription type because it lives in the client's own tracker.
func (fs *fsub) deliverable(t *ftopic, part, entry int, now time.Time) bool {
	e := t.parts[part].entries[entry]
	if !e.closed {
		// The producer is still filling this batch, so the broker does
		// not have it.
		return false
	}
	if !e.due.IsZero() && e.due.After(now) {
		switch fs.typ {
		case pulsar.Exclusive, pulsar.Failover:
			// Delivered at once, which is the honest model of
			// Pulsar's restriction and the reason the driver refuses
			// a delayed publication from a client configured for
			// these types.
		default:
			return false
		}
	}
	for k := range e.msgs {
		sl := slot{part: part, entry: entry, idx: k}
		if fs.acked[sl] {
			continue
		}
		if due, ok := fs.nackDue[sl]; ok && due.After(now) {
			return false
		}
	}
	return true
}

// reassign recomputes the Key_Shared hash-range ownership and blocks every
// range whose owner changed while the previous owner still holds messages from
// it.
//
// The blocking IS AllowOutOfOrderDelivery false: Pulsar would rather stop
// delivering a range than hand it to a new consumer while the old one is still
// working on it, because handing it over is what breaks per-key order.
func (fs *fsub) reassign() {
	if fs.typ != pulsar.KeyShared {
		return
	}
	prev := fs.owner
	fs.owner = map[int]*fconsumer{}
	if len(fs.consumers) == 0 {
		return
	}
	for r := range HashRanges {
		fs.owner[r] = fs.consumers[r%len(fs.consumers)]
		if fs.allowOutOfOrder {
			continue
		}
		if old := prev[r]; old != nil && old != fs.owner[r] && fs.rangeHeldBy(r, old) {
			fs.blocked[r] = true
		}
	}
}

// rangeHeldBy reports whether a consumer still holds an unsettled message in a
// hash range.
func (fs *fsub) rangeHeldBy(r int, c *fconsumer) bool {
	for sl, holder := range fs.holder {
		if holder != c || !fs.inflight[sl] {
			continue
		}
		if fs.rangeOfSlot(sl) == r {
			return true
		}
	}
	return false
}

// rangeOfSlot is the hash range of the message at a slot.
func (fs *fsub) rangeOfSlot(sl slot) int {
	if fs.topicOf == nil {
		return 0
	}
	t := fs.topicOf
	if sl.part >= len(t.parts) || sl.entry >= len(t.parts[sl.part].entries) {
		return 0
	}
	e := t.parts[sl.part].entries[sl.entry]
	if sl.idx >= len(e.msgs) {
		return 0
	}
	return rangeOf(keyOf(e.msgs[sl.idx]))
}

// unblock clears every range that no longer has an unsettled message from its
// previous owner.
func (fs *fsub) unblock() {
	if fs.typ != pulsar.KeyShared {
		return
	}
	for r := range fs.blocked {
		held := false
		for sl := range fs.inflight {
			if fs.inflight[sl] && fs.rangeOfSlot(sl) == r && fs.holder[sl] != fs.owner[r] {
				held = true
				break
			}
		}
		if !held {
			delete(fs.blocked, r)
		}
	}
}

// keyOf is the key a message is routed by: the ordering key when it has one,
// and the partition key otherwise, which is Pulsar's own precedence.
func keyOf(m *fmsg) string {
	if m.orderingKey != "" {
		return m.orderingKey
	}
	return m.key
}

// pick is the consumer an entry is dispatched to, which is the whole of the
// subscription type's behaviour.
func (fs *fsub) pick(t *ftopic, part, entry int) *fconsumer {
	if len(fs.consumers) == 0 {
		return nil
	}
	switch fs.typ {
	case pulsar.Exclusive, pulsar.Failover:
		// One ACTIVE consumer. A Failover subscription's other consumers
		// are standbys and receive nothing until it goes away, which is
		// why FeatureConsumerGroups is Conditional for it rather than
		// Native.
		return fs.consumers[0]
	case pulsar.KeyShared:
		e := t.parts[part].entries[entry]
		if len(e.msgs) == 0 {
			return nil
		}
		r := rangeOf(keyOf(e.msgs[0]))
		if fs.blocked[r] {
			return nil
		}
		return fs.owner[r]
	default:
		// Shared: round robin, and therefore no ordering of any kind.
		fs.rr++
		return fs.consumers[fs.rr%len(fs.consumers)]
	}
}

// dispatch hands out every entry that is deliverable. It is called with the
// server's mutex held.
func (fs *fsub) dispatch(t *ftopic, now time.Time) {
	fs.topicOf = t
	for part := range t.parts {
		lg := t.parts[part]
		for i := fs.startOf(part); i < len(lg.entries); i++ {
			if fs.entryDone(t, slot{part: part, entry: i}) {
				if i == fs.startOf(part) {
					fs.start[part] = i + 1
				}
				continue
			}
			if fs.entryInflight(t, part, i) || !fs.deliverable(t, part, i, now) {
				continue
			}
			c := fs.pick(t, part, i)
			if c == nil {
				continue
			}
			fs.deliver(t, part, i, c, now)
		}
	}
}

// deliver hands one entry's unsettled messages to one consumer, sharing one
// batch tracker between them the way pulsar-client-go does.
func (fs *fsub) deliver(t *ftopic, part, entry int, c *fconsumer, now time.Time) {
	e := t.parts[part].entries[entry]
	pending := make([]int, 0, len(e.msgs))
	for k := range e.msgs {
		if !fs.acked[slot{part: part, entry: entry, idx: k}] {
			pending = append(pending, k)
		}
	}
	if len(pending) == 0 {
		return
	}
	if cap(c.ch)-len(c.ch) < len(pending) {
		// No room. The channel's capacity is the prefetch, so this is
		// the protocol's flow control: the entry stays in the backlog.
		return
	}
	tracker := newEntryAcks(len(e.msgs), func(i int) bool {
		return fs.acked[slot{part: part, entry: entry, idx: i}]
	})
	size := int32(len(e.msgs))
	for _, k := range pending {
		sl := slot{part: part, entry: entry, idx: k}
		fs.inflight[sl] = true
		fs.holder[sl] = c
		fs.gen[sl]++
		fs.redeliveries[sl]++
		fs.stats.Delivered++
		if fs.redeliveries[sl] > 1 {
			fs.stats.Redelivered++
		}
		id := &msgID{
			topic: t.name, ledger: 1, entry: int64(entry),
			partition: int32(part), batchIdx: -1, batchSize: size,
			sl: sl, gen: fs.gen[sl],
		}
		if size > 1 {
			id.batchIdx = int32(k)
		}
		if t.count <= 0 {
			id.partition = -1
		}
		if !c.s.opt.untracked {
			id.tracker = tracker
		}
		msg := &fmessage{t: t, m: e.msgs[k], id: id, redeliveries: fs.redeliveries[sl] - 1}
		c.ch <- pulsar.ConsumerMessage{Consumer: c, Message: msg}
	}
	_ = now
}

// releaseAll makes every unsettled message available again, which is what a
// consumer's disconnection does.
func (fs *fsub) releaseAll() {
	for sl := range fs.inflight {
		delete(fs.inflight, sl)
		delete(fs.holder, sl)
		delete(fs.nackDue, sl)
	}
	fs.unblock()
}

// releaseHeldBy makes one consumer's unsettled messages available again.
func (fs *fsub) releaseHeldBy(c *fconsumer) {
	for sl, holder := range fs.holder {
		if holder != c {
			continue
		}
		delete(fs.inflight, sl)
		delete(fs.holder, sl)
		delete(fs.nackDue, sl)
	}
	fs.unblock()
}
