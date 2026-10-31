package kafkago

import (
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
)

// route is what a publication carries in [kgo.Message.WriterData]: the
// partition the caller asked for, and where the broker's answer goes.
//
// WriterData is the field kafka-go documents as "arbitrary data you wish to
// include, so it will be available when handle it on the Writer's `Completion`
// method", and it is the only per-message channel this client offers in both
// directions: the balancer is handed the whole message on the way out and the
// completion is handed it back on the way in.
//
// It is NOT Message.Partition, which looks like the obvious place and is
// documented "read-only and MUST NOT be set when writing messages" — and is in
// fact overwritten by the writer with the partition it chose before the
// completion runs, so a driver that used it could not tell "partition 0
// because the caller said so" from "partition 0 because the balancer picked
// it". That is the same trap the franz-go driver avoids by using the record's
// Context ; this is its kafka-go counterpart.
type route struct {
	// partition is the partition the per-call control asked for.
	partition int32
	// hasPartition reports whether it was asked for, so that partition 0 is
	// not indistinguishable from "the caller said nothing".
	hasPartition bool
	// msg is the caller's message, whose Receipt slot [route.fill] fills.
	// It is nil when the sink was not configured for receipts.
	msg *mq.Message
	// confirm is the confirmation mode the filled receipt records.
	confirm mq.ConfirmationMode
	// at, offset and time are the broker's answer for the record, which the
	// completion records here and not in the caller's receipt: kafka-go
	// answers a synchronous publication whose ctx ended with ctx.Err() and
	// carries on, so the completion can run after the caller was told the
	// publication failed and has its message back. answered is set once they
	// are written, and is atomic because a writer whose Async is true returns
	// from WriteMessages before its completion runs, so a Publish on it reads
	// it while the completion may be writing.
	at       int32
	offset   int64
	time     time.Time
	answered atomic.Bool
	// buf is the pooled buffer this publication was built in, so that the
	// completion can drop its reference to it. See [msgbuf.release].
	buf *msgbuf
	// report is the publication's terminal accounting — an asynchronous
	// callback, an owned message's release, the admitted-work count — and it
	// runs EXACTLY ONCE whichever of the completion and the submitting
	// goroutine reaches it first. It is nil on a synchronous path, where the
	// caller's own return value is the report.
	report func(err error)
	// reported is what makes `report` run once. It is not a sync.Once
	// because the two callers are not cooperating: the submitter calls
	// settle only when WriteMessages hands it an error, and on this client
	// such an error may mean "the record was never submitted" (so the
	// completion will never run) or "the record was submitted and the wait
	// ended" (so it will, later). Neither knows which.
	reported atomic.Bool
}

// settle performs the publication's terminal accounting once, and reports
// whether THIS call was the one that did it.
//
// The return value is what keeps `PublishAsync` free of a captured flag: the
// submitting goroutine has to know whether the callback has already been given
// an outcome, and a plain bool written by whichever goroutine reported would be
// a data race on every path where both of them can report. Reading it out of
// the same atomic that decided is the only answer that needs no reasoning
// about which of kafka-go's error returns mean "never submitted".
func (r *route) settle(err error) bool {
	if r.reported.Swap(true) {
		return false
	}
	if r.report != nil {
		r.report(err)
	}
	return true
}

// answer records the broker's answer for the record the writer hands back.
func (r *route) answer(msg *kgo.Message) {
	// A Kafka partition is an int32 on the wire.
	r.at, r.offset, r.time = int32(msg.Partition), msg.Offset, msg.Time
	r.answered.Store(true)
}

// fill copies the broker's answer into the caller's receipt, and is called
// only by whoever is about to tell the caller its publication succeeded: the
// submitting goroutine of a synchronous one after WriteMessages returned, or
// the report of an asynchronous one before it hands the message back. It
// does nothing for a record the broker has not answered.
func (r *route) fill() {
	if !r.answered.Load() || r.msg == nil || r.msg.Receipt == nil {
		return
	}
	r.msg.Receipt.Partition = r.at
	r.msg.Receipt.Offset = r.offset
	r.msg.Receipt.Confirmation = r.confirm
	r.msg.Receipt.ConfirmedAt = r.time
	// Kafka assigns no message identifier: the coordinates ARE the
	// identifier, and the specification makes an empty MessageID "the broker
	// assigns none" rather than a claim.
}

// reset clears a pooled route so that nothing of the previous publication
// survives into the next one — in particular `report`, which would otherwise
// report a stale outcome, and `msg`, which would otherwise keep a caller's
// message alive for as long as the pool holds the buffer.
func (r *route) reset() { *r = route{} }

// Route marks a message for one partition, which is what the per-call
// Partition control asks for.
//
// It replaces the message's WriterData, so a caller using this directly on a
// writer of its own loses whatever it had there. Inside this driver the field
// is the driver's own.
func Route(msg *kgo.Message, partition int32) {
	r, ok := msg.WriterData.(*route)
	if !ok {
		r = &route{}
		msg.WriterData = r
	}
	r.partition, r.hasPartition = partition, true
}

// requestedPartition reads back what [Route] marked, if anything.
func requestedPartition(data any) (int32, bool) {
	r, ok := data.(*route)
	if !ok || !r.hasPartition {
		return 0, false
	}
	return r.partition, true
}
