package stomp

import (
	"sync"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

// overflow is the queue between go-stomp's subscription channel and the
// source's bounded buffer, and it exists so that the channel is ALWAYS read.
//
// # Why the channel has to be read whatever the caller is doing
//
// go-stomp runs a connection on one process loop, and that loop does three
// jobs from one `select`: it hands each MESSAGE to its subscription with a
// blocking send on an unbuffered channel, it routes every RECEIPT, and it
// writes the client's heart-beat when the write timer fires. The subscription
// forwards into the sixteen-deep `Subscription.C`, so a reader that stops for
// more than sixteen frames stops the loop, and with it the heart-beats. The
// source's pump used to be that reader, and it stopped whenever the buffer
// was full: after the window `Config.HeartbeatSend` negotiated the broker
// dropped the connection and redelivered everything the subscription had not
// acknowledged, and the ACKs a caller wrote meanwhile sat in go-stomp's write
// queue and were lost with it. Measured against the pinned Artemis with the
// default configuration and a consumer that held its 32 deliveries for eight
// seconds: 64 duplicates in 120 messages, twenty acknowledgements that
// returned nil and never reached the broker, and twelve more that failed with
// `msg send timeout` once the write queue's twenty slots were full.
//
// So the subscription is read by two goroutines rather than one. The DRAIN
// ([Source.drain]) reads the channel into this queue and does nothing else;
// the PUMP ([Source.pump]) takes from it and does everything the single pump
// used to do -- decode, claim a token, push into the bounded buffer, and wait
// on any of them. A full buffer now parks the pump and not the loop.
//
// # What bounds it is the broker's own window
//
// A queue that took whatever arrived would be the unbounded push-to-pull
// adapter `driverkit.PushAdapter` exists to prevent. It does not need a bound
// of its own, because each broker already stops sending once the subscription
// holds a window's worth of unacknowledged deliveries -- and everything in
// this queue is unacknowledged, because nothing in it has been handed to
// anyone yet. Read from the pinned images' sources and measured against them:
//
//	Classic   a COUNT. `activemq.prefetchSize` is the consumer's prefetch,
//	          `PrefetchSubscription.isFull` is "dispatched minus the prefetch
//	          extension has reached it", and a client-individual ACK removes
//	          one delivery from `dispatched`. The extension grows only on a
//	          DELIVERED or transacted acknowledgement and on a pull, none of
//	          which this driver sends. Measured: exactly the prefetch arrives,
//	          5 of 5 and 32 of 32 at 10 KB a message, whatever the size.
//	Artemis   frame BYTES. `StompSession.sendMessage` charges each MESSAGE
//	          its whole encoded frame (`StompFrame.getEncodedSize`: command,
//	          headers, body and terminator), the consumer sends while its
//	          credit is positive, and a client-individual ACK refunds the
//	          charge. Measured with a 1 MiB window and one-byte bodies: 4038
//	          frames, which [frameSize] puts at 1,044,701 bytes against the
//	          broker's 1,048,576 -- the estimate is a lower bound, as it has to
//	          be.
//
// The window is enforced HERE as well, and a put at the bound waits, so that
// the queue is bounded by the configuration by construction and not by trust
// in the broker. Every frame in the queue was in the broker's unacknowledged
// set when the next one was sent, so a broker that honours its window never
// reaches the bound: Classic sends only below the count, and Artemis only
// while the charge, which [frameSize] never exceeds, is below the window. The
// bound is reached only by a peer that ignored the window it was asked for,
// and what that costs is what every full buffer used to cost -- the drain
// stops, and so do the heart-beats -- rather than memory without limit.
//
// # A terminal message goes to the front
//
// The pump takes a terminal error in its place in the stream ([Source.pump]
// says why the order matters), but the MESSAGEs queued ahead of one are
// dropped when it arrives. For the live subscription a terminal message means
// go-stomp has ended the connection -- an ERROR frame, which it closes the
// socket after; a read or write failure; its own heart-beat read timeout --
// so every delivery queued ahead of it is on a socket that is gone, and
// [Source.fail] drops the buffered ones for the same reason. Handing them out
// would cost a caller one lost lease each before the error that explains them,
// and a full Artemis window is thousands. For a superseded subscription, the
// pump drops every MESSAGE anyway.
//
// # One writer and one reader
//
// The drain is the only caller of [overflow.put] and [overflow.end], and the
// pump the only caller of [overflow.take] and [overflow.finish]. That is what
// lets each wake-up be a one-slot channel: a kick sent while nobody waits stays
// in the slot, so a waiter that checked the queue, released the lock and then
// started waiting still wakes, and a stale kick costs one more look.
type overflow struct {
	// maxCount and maxBytes are the broker's window in its own unit. Zero is
	// no bound in that unit.
	maxCount int
	maxBytes int64

	mu    sync.Mutex
	items []queued
	// count and bytes are the MESSAGEs queued and their frame bytes. A
	// terminal message is neither: it carries no delivery and the broker
	// charges nothing for it.
	count int
	bytes int64
	// ended is the drain gone: nothing more will be put.
	ended bool
	// gone is the pump gone: nothing more will be taken.
	gone bool

	// ready is kicked when something is put or the drain ends; room when
	// something is taken. One slot each, never more.
	ready chan struct{}
	room  chan struct{}
	// finished is closed when the pump stops, which is what lets the drain
	// stop reading a channel whose deliveries nobody will take.
	finished   chan struct{}
	finishOnce sync.Once
}

// queued is one message and the frame bytes it was counted at, kept so that
// taking it subtracts exactly what putting it added.
type queued struct {
	msg  *gostomp.Message
	size int64
}

// newOverflow returns an empty queue bounded by a broker window of maxCount
// messages or maxBytes frame bytes, zero being no bound in that unit.
func newOverflow(maxCount int, maxBytes int64) *overflow {
	return &overflow{
		maxCount: maxCount,
		maxBytes: maxBytes,
		ready:    make(chan struct{}, 1),
		room:     make(chan struct{}, 1),
		finished: make(chan struct{}),
	}
}

// put queues one message, waiting only while the queue holds the broker's
// whole window, and reports false once the pump has finished or stop is
// closed, either of which ends the drain.
func (q *overflow) put(m *gostomp.Message, stop <-chan struct{}) bool {
	var size int64
	if m.Err == nil {
		size = frameSize(m)
	}
	for {
		q.mu.Lock()
		switch {
		case q.gone:
			q.mu.Unlock()
			return false
		case m.Err != nil:
			q.dropMessagesLocked()
			q.items = append(q.items, queued{msg: m})
			q.mu.Unlock()
			kick(q.ready)
			return true
		case q.roomLocked():
			q.items = append(q.items, queued{msg: m, size: size})
			q.count++
			q.bytes += size
			q.mu.Unlock()
			kick(q.ready)
			return true
		}
		q.mu.Unlock()
		select {
		case <-q.room:
		case <-q.finished:
			return false
		case <-stop:
			return false
		}
	}
}

// roomLocked reports whether the queue is below the broker's window. An empty
// queue always is, so one frame larger than the window is still taken rather
// than waiting for room that cannot appear. The caller holds q.mu.
func (q *overflow) roomLocked() bool {
	if q.maxCount > 0 && q.count >= q.maxCount {
		return false
	}
	return q.maxBytes <= 0 || q.bytes < q.maxBytes
}

// dropMessagesLocked drops every queued MESSAGE and keeps any terminal
// message, so that a terminal error reaches the pump next. The caller holds
// q.mu.
func (q *overflow) dropMessagesLocked() {
	kept := q.items[:0]
	for _, e := range q.items {
		if e.msg.Err != nil {
			kept = append(kept, e)
		}
	}
	clear(q.items[len(kept):])
	q.items = kept
	q.count, q.bytes = 0, 0
}

// take returns the oldest queued message, waiting while there is none. It
// reports false once the drain has ended and the queue is empty, or when stop
// is closed.
func (q *overflow) take(stop <-chan struct{}) (*gostomp.Message, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			e := q.items[0]
			q.items[0] = queued{}
			q.items = q.items[1:]
			if e.msg.Err == nil {
				q.count--
				q.bytes -= e.size
			}
			q.mu.Unlock()
			kick(q.room)
			return e.msg, true
		}
		ended := q.ended
		q.mu.Unlock()
		if ended {
			return nil, false
		}
		select {
		case <-q.ready:
		case <-stop:
			return nil, false
		}
	}
}

// end records that the drain has stopped. What is queued is still taken.
func (q *overflow) end() {
	q.mu.Lock()
	q.ended = true
	q.mu.Unlock()
	kick(q.ready)
}

// finish records that the pump has stopped and wakes a drain waiting for room.
// What is still queued is left where it is: nothing will take it, and the
// queue goes with the drain, which stops at once.
func (q *overflow) finish() {
	q.finishOnce.Do(func() {
		q.mu.Lock()
		q.gone = true
		q.mu.Unlock()
		close(q.finished)
	})
}

// done is closed once the pump has stopped.
func (q *overflow) done() <-chan struct{} { return q.finished }

// kick wakes a waiter on a one-slot channel without ever blocking: a kick that
// finds the slot full is one the waiter has not consumed yet.
func kick(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// frameSize is a LOWER bound on the bytes a MESSAGE frame occupied on the wire,
// which is what Artemis charges a consumer's window for it.
//
// Lower is the direction that matters: the queue's byte bound has to be one an
// honest broker cannot reach, so the estimate may undercount and must never
// overcount. go-stomp hands over the header after unescaping it and counts
// each line ending as one byte, where the wire has the escapes and may have
// CR LF, and the terminator is counted as the NUL alone where Artemis writes a
// NUL and a newline. Every header the frame carried is counted, `content-length`
// included, because go-stomp keeps it in the header it parsed.
func frameSize(m *gostomp.Message) int64 {
	// The command and its line, the blank line before the body, the body and
	// the NUL.
	n := int64(len(frame.MESSAGE)) + 1 + 1 + int64(len(m.Body)) + 1
	if m.Header == nil {
		return n
	}
	for i := range m.Header.Len() {
		k, v := m.Header.GetAt(i)
		n += int64(len(k)) + 1 + int64(len(v)) + 1
	}
	return n
}
