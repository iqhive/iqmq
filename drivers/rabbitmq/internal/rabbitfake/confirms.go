package rabbitfake

import (
	"sort"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// notifyAll is this package's copy of the library's own notifyAll, and it is a
// copy on purpose.
//
// `amqp091.notifyAll` abandons any individual send that blocks for longer than
// five seconds, so a driver that is slow to read a notification channel does
// not lose the frame that arrives while it is slow -- it loses the
// notification entirely, and every later one it manages to read arrives out of
// step with the broker. The driver's `basic.return` handling is built around
// exactly that, so the fake has to be able to produce it.
//
// The timeout is a parameter rather than a constant for one reason. Five
// seconds is a time nobody wants in a unit test, and a fake whose only
// abandonment was "wait five seconds" would make the one test that needs the
// behaviour the slowest in the suite. So there are three regimes, and the
// default is the one no real library has:
//
//   - timeout == 0 blocks until the listener reads. Nothing is ever dropped,
//     so an assertion about what a listener received is an assertion about
//     what the broker sent. This is the default because a test that is not
//     about abandonment should not have to think about it.
//   - timeout < 0 abandons immediately: a listener that is not already parked
//     on a receive loses the notification. This is the deterministic form of
//     the library's behaviour and the one the abandonment test uses -- there
//     is no clock in it at all, so it cannot be flaky.
//   - timeout > 0 abandons after that long, with a REAL timer. It is the
//     literal reproduction, for a test that wants to watch a listener race the
//     timeout, and it is the one place in this package that reads a wall
//     clock. A timer on the injected [bitbucket.org/iqhive/iqmq.Clock] was the
//     alternative and it is worse than useless here: under the frozen clock
//     this package defaults to, "abandon after five seconds" would mean "never
//     abandon", which is the regime timeout == 0 already provides.
//
// abort releases every form of the send, so that tearing the broker down never
// leaves the dispatcher parked on a listener nobody is reading.
func notifyAll[T any](listeners []chan T, v T, timeout time.Duration, abort <-chan struct{}) {
	for _, c := range listeners {
		switch {
		case timeout < 0:
			select {
			case c <- v:
			default:
			}
		case timeout == 0:
			select {
			case c <- v:
			case <-abort:
			}
		default:
			t := time.NewTimer(timeout)
			select {
			case c <- v:
			case <-t.C:
			case <-abort:
			}
			t.Stop()
		}
	}
}

// ConfirmStyle is how a broker frames the publisher confirms for one channel.
//
// It exists because the confirm the DRIVER sees is not the confirm the broker
// sent. `basic.ack{multiple=true}` never reaches a driver at all:
// `confirms.Multiple` expands it into one [amqp091.Confirmation] per delivery
// tag, and `confirms.One` holds a tag that arrives early in a sequencer until
// the gap in front of it is filled. Both of those are the library's, both are
// invisible from above, and a driver that was only ever tested against a
// broker that confirmed one tag at a time in tag order would never run either
// path. So the framing is a knob.
type ConfirmStyle uint8

// The confirm framings a channel can be given.
const (
	// ConfirmPerTag sends one `basic.ack{multiple=false}` per publication, in
	// tag order, as soon as the publication is routed. It is the default
	// because it is what a lightly loaded RabbitMQ does.
	ConfirmPerTag ConfirmStyle = iota
	// ConfirmCumulative holds [ConfirmScript.Batch] publications and then
	// confirms them with ONE `basic.ack{multiple=true}` naming the highest
	// tag, which is what a loaded RabbitMQ does. What the driver then sees is
	// the library's expansion.
	ConfirmCumulative
	// ConfirmReversed holds [ConfirmScript.Batch] publications and then
	// confirms them per tag in DESCENDING tag order, which is what makes the
	// library's sequencer do its work: every tag but the last is parked until
	// the lowest arrives, and then all of them are released in tag order.
	ConfirmReversed
	// ConfirmManual sends nothing at all. The test drives the confirms itself
	// through [ChannelHandle.AckPublication] and its three siblings, which is
	// what an interleaving no policy expresses is written with.
	ConfirmManual
)

// ConfirmScript is the confirm framing for one channel.
type ConfirmScript struct {
	// Style is the framing.
	Style ConfirmStyle
	// Batch is how many publications [ConfirmCumulative] and
	// [ConfirmReversed] hold before they confirm. Zero means two, which is
	// the smallest batch that can be cumulative at all.
	Batch int
}

// batch is the group size, with zero read as two.
func (s ConfirmScript) batch() int {
	if s.Batch <= 0 {
		return 2
	}
	return s.Batch
}

// ConfirmOutcome is how one publication's confirm was resolved.
//
// It is observation rather than protocol: a driver on this seam cannot see it,
// because the seam has no `PublishWithDeferredConfirm` and the library resolves
// a deferred confirmation by closing a channel nobody above it holds. It is
// here so that a test can assert the thing turns on -- what a channel
// close did to a publication whose confirm had not arrived -- without having
// to infer it from a `NotifyPublish` channel that the same close closed.
type ConfirmOutcome uint8

// The ways a publication's confirm is resolved.
const (
	// OutcomeAcked is a `basic.ack` from the broker.
	OutcomeAcked ConfirmOutcome = iota
	// OutcomeNacked is a `basic.nack` from the broker.
	OutcomeNacked
	// OutcomeClosed is `deferredConfirmations.Close`: the channel closed with
	// the publication outstanding and the library resolved it with
	// `ack=false`. The specification calls this AMBIGUOUS and the library calls it a
	// negative, which is the difference the driver exists to absorb.
	OutcomeClosed
	// OutcomeUnpublished is `confirms.unpublish`: the publish itself failed,
	// so the tag was handed back and no confirm will ever name it.
	OutcomeUnpublished
)

// String names the outcome for a failure message.
func (o ConfirmOutcome) String() string {
	switch o {
	case OutcomeAcked:
		return "acked"
	case OutcomeNacked:
		return "nacked"
	case OutcomeClosed:
		return "closed"
	case OutcomeUnpublished:
		return "unpublished"
	default:
		return "unknown"
	}
}

// Resolution is one publication's tag and what became of it.
type Resolution struct {
	// Tag is the delivery tag the publication was given.
	Tag uint64
	// Outcome is how it was resolved.
	Outcome ConfirmOutcome
}

// confirms is the fake's reproduction of `amqp091.confirms`, field for field
// and method for method.
//
// It is a reproduction and not an approximation because it is the single thing
// this package exists for. Everything else in here is a broker; this is the
// LIBRARY, sitting between a broker's `basic.ack` frames and the driver's
// `NotifyPublish` channel, and the three behaviours it interposes -- the
// expansion of a cumulative ack, the resequencing of an out-of-order one, and
// what a close does to what is outstanding -- are behaviours no broker can be
// made to produce and no test above the seam can otherwise reach. Writing it
// again from the library's own source is the only way the fake and the real
// client can be held to one answer; a simplification that dropped the
// sequencer would let the driver's confirm bookkeeping pass against a fake
// that never reordered anything.
type confirms struct {
	m         sync.Mutex
	listeners []chan amqp.Confirmation
	sequencer map[uint64]amqp.Confirmation

	// deferredMut guards deferred and resolved, and it is a THIRD lock
	// because the library has a third: `deferredConfirmations` is its own
	// type with its own mutex, and the two sets it holds are reached from
	// both the publishing goroutine (which takes publishedMut) and the
	// connection's dispatcher (which takes m). Merging them under either one
	// of the existing locks is what the first version of this file did, and
	// `go test -race` found it in one run: `confirms.publish` writing the set
	// while `confirms.resolveThrough` deleted from it.
	//
	// It is always the INNERMOST lock. Nothing holds it across a call that
	// takes m or publishedMut, so there is no ordering to keep in mind.
	deferredMut sync.Mutex
	// deferred is `deferredConfirmations.confirmations` reduced to its
	// observable content: the set of tags published and not yet resolved.
	// The library holds a *DeferredConfirmation per tag so that a caller can
	// wait on one; this seam has no way to ask for one, so the fake keeps the
	// SET and records what each tag was resolved to instead.
	deferred map[uint64]struct{}
	resolved []Resolution

	publishedMut sync.Mutex
	published    uint64

	expecting uint64
	closed    bool
}

// newConfirms allocates the confirm bookkeeping for one channel.
func newConfirms() *confirms {
	return &confirms{
		sequencer: map[uint64]amqp.Confirmation{},
		deferred:  map[uint64]struct{}{},
		expecting: 1,
	}
}

// listen registers a NotifyPublish listener.
func (c *confirms) listen(l chan amqp.Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()
	c.listeners = append(c.listeners, l)
}

// publish takes the next delivery tag, which is the library's `confirms.publish`.
func (c *confirms) publish() uint64 {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	c.published++
	c.deferredMut.Lock()
	c.deferred[c.published] = struct{}{}
	c.deferredMut.Unlock()
	return c.published
}

// unpublish hands the last tag back, which is what the library does when the
// publish it had just counted failed to reach the socket.
//
// It is reproduced because it is the one place the library's counter can move
// BACKWARDS, and a driver that assigns its own tags under its own lock -- which
// this one does -- has to agree with it. `GetNextPublishSeqNo` is on the seam
// so that a test can assert the two counters still agree after a failed
// publish; without the decrement here that assertion could not fail.
func (c *confirms) unpublish() {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	if c.published == 0 {
		return
	}
	c.deferredMut.Lock()
	if _, ok := c.deferred[c.published]; ok {
		delete(c.deferred, c.published)
		c.resolved = append(c.resolved, Resolution{Tag: c.published, Outcome: OutcomeUnpublished})
	}
	c.deferredMut.Unlock()
	c.published--
}

// nextSeqNo is `GetNextPublishSeqNo`: the tag the next publication will get.
func (c *confirms) nextSeqNo() uint64 {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	return c.published + 1
}

// confirm delivers one confirmation and advances the expected tag. It is the
// library's `confirms.confirm` and is called with c.m held.
func (c *confirms) confirm(conf amqp.Confirmation, timeout time.Duration, abort <-chan struct{}) {
	delete(c.sequencer, c.expecting)
	c.expecting++
	notifyAll(c.listeners, conf, timeout, abort)
}

// resequence releases every confirmation the sequencer is holding that is now
// in order. It is the library's `confirms.resequence`.
func (c *confirms) resequence(timeout time.Duration, abort <-chan struct{}) {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	for c.expecting <= c.published {
		sequenced, found := c.sequencer[c.expecting]
		if !found {
			return
		}
		c.confirm(sequenced, timeout, abort)
	}
}

// one is the library's `confirms.One`: `basic.ack{multiple=false}`.
func (c *confirms) one(conf amqp.Confirmation, timeout time.Duration, abort <-chan struct{}) {
	c.m.Lock()
	defer c.m.Unlock()
	if c.closed {
		return
	}
	c.resolve(conf.DeliveryTag, conf.Ack)
	if c.expecting == conf.DeliveryTag {
		c.confirm(conf, timeout, abort)
	} else {
		c.sequencer[conf.DeliveryTag] = conf
	}
	c.resequence(timeout, abort)
}

// multiple is the library's `confirms.Multiple`: `basic.ack{multiple=true}`,
// expanded into one confirmation per tag up to and including the one named.
func (c *confirms) multiple(conf amqp.Confirmation, timeout time.Duration, abort <-chan struct{}) {
	c.m.Lock()
	defer c.m.Unlock()
	if c.closed {
		return
	}
	c.resolveThrough(conf.DeliveryTag, conf.Ack)
	for c.expecting <= conf.DeliveryTag {
		c.confirm(amqp.Confirmation{DeliveryTag: c.expecting, Ack: conf.Ack}, timeout, abort)
	}
	c.resequence(timeout, abort)
}

// close is the library's `confirms.Close` together with
// `deferredConfirmations.Close`.
//
// Both halves matter and they say different things. The deferred half resolves
// every outstanding publication with `ack=false` -- a DEFINITE negative for a
// publication the broker may well have taken, which is the ambiguity
// arriving at a driver wearing the wrong label. The listener half closes the
// `NotifyPublish` channel and never delivers what the sequencer was holding,
// so a driver that had a tag parked behind a gap learns nothing about it at
// all. A driver that read either as "the broker refused this" would report a
// failure for a message that is in a queue.
func (c *confirms) close() {
	c.m.Lock()
	defer c.m.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	// deferredConfirmations.Close, in tag order so that the record a test
	// reads is the same on every run. The library iterates a map here, which
	// it can afford because nothing observes the order.
	c.deferredMut.Lock()
	tags := make([]uint64, 0, len(c.deferred))
	for tag := range c.deferred {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	for _, tag := range tags {
		delete(c.deferred, tag)
		c.resolved = append(c.resolved, Resolution{Tag: tag, Outcome: OutcomeClosed})
	}
	c.deferredMut.Unlock()
	for _, l := range c.listeners {
		close(l)
	}
	c.listeners = nil
	// The sequencer is dropped rather than drained, which is the library's
	// "discarding any out of sequence confirmations".
	c.sequencer = map[uint64]amqp.Confirmation{}
}

// resolve records the outcome of one tag.
func (c *confirms) resolve(tag uint64, ack bool) {
	c.deferredMut.Lock()
	defer c.deferredMut.Unlock()
	if _, ok := c.deferred[tag]; !ok {
		// The library's own comment: "We should never receive a confirmation
		// for a tag that hasn't been published, but a test causes this to
		// happen." It ignores it, and so does this.
		return
	}
	delete(c.deferred, tag)
	c.resolved = append(c.resolved, Resolution{Tag: tag, Outcome: outcomeOf(ack)})
}

// resolveThrough records the outcome of every outstanding tag at or below one,
// in tag order.
func (c *confirms) resolveThrough(tag uint64, ack bool) {
	c.deferredMut.Lock()
	defer c.deferredMut.Unlock()
	pending := make([]uint64, 0, len(c.deferred))
	for t := range c.deferred {
		if t <= tag {
			pending = append(pending, t)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i] < pending[j] })
	for _, t := range pending {
		delete(c.deferred, t)
		c.resolved = append(c.resolved, Resolution{Tag: t, Outcome: outcomeOf(ack)})
	}
}

// outcomeOf maps an ack flag to its outcome.
func outcomeOf(ack bool) ConfirmOutcome {
	if ack {
		return OutcomeAcked
	}
	return OutcomeNacked
}

// pending is the outstanding tags, in order.
func (c *confirms) pending() []uint64 {
	c.deferredMut.Lock()
	defer c.deferredMut.Unlock()
	tags := make([]uint64, 0, len(c.deferred))
	for tag := range c.deferred {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

// resolutions is the record of how each publication's confirm was resolved, in
// the order the resolutions happened.
func (c *confirms) resolutions() []Resolution {
	c.deferredMut.Lock()
	defer c.deferredMut.Unlock()
	return append([]Resolution(nil), c.resolved...)
}
