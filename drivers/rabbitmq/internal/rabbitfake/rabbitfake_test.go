package rabbitfake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// The fake is the seam every unit test of the RabbitMQ driver will run
// through, so what these tests hold it to is not "does it look like a broker"
// but "does it do what amqp091-go does". A fake that is wrong is worse than no
// fake: it certifies a driver against its own mistake, and the mistakes that
// matter here are the four the seam's package comment lists, none of which a
// container can be made to produce on demand.

// newBroker returns a broker that is closed when the test ends.
func newBroker(t *testing.T, cfg rabbitfake.Config) *rabbitfake.Broker {
	t.Helper()
	b := rabbitfake.New(cfg)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// dial opens a connection.
func dial(t *testing.T, b *rabbitfake.Broker) client.Conn {
	t.Helper()
	conn, err := b.Dialer().Dial(context.Background(), []string{"amqp://fake/"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("dialling the fake: %v", err)
	}
	return conn
}

// openChannel opens a channel.
func openChannel(t *testing.T, conn client.Conn) client.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	return ch
}

// setup is the arrangement almost every test starts from: a broker, a
// connection and a channel with one durable queue reachable through the
// default exchange by its own name.
func setup(t *testing.T, cfg rabbitfake.Config) (*rabbitfake.Broker, client.Channel) {
	t.Helper()
	b := newBroker(t, cfg)
	ch := openChannel(t, dial(t, b))
	if _, err := ch.QueueDeclare("q", true, false, false, false, nil); err != nil {
		t.Fatalf("declaring q: %v", err)
	}
	return b, ch
}

// pub is a publication with a body and a correlating header, which is what a
// `basic.return` is matched back to its publication by.
func pub(body string) amqp.Publishing {
	return amqp.Publishing{
		Headers: amqp.Table{"iqmq-id": body},
		Body:    []byte(body),
	}
}

// publish publishes and fails the test on an error the library itself would
// have returned.
func publish(t *testing.T, ch client.Channel, exchange, key string, mandatory bool, body string) {
	t.Helper()
	if err := ch.PublishWithContext(context.Background(), exchange, key, mandatory, false, pub(body)); err != nil {
		t.Fatalf("publishing %q to %q/%q: %v", body, exchange, key, err)
	}
}

// consume starts a consumer with the background context, so that no watcher
// goroutine is started for a context that can never be done.
func consume(t *testing.T, ch client.Channel, queue, tag string, autoAck bool) <-chan amqp.Delivery {
	t.Helper()
	d, err := ch.ConsumeWithContext(context.Background(), queue, tag, autoAck, false, false, false, nil)
	if err != nil {
		t.Fatalf("consuming %q: %v", queue, err)
	}
	return d
}

// syncWithin drains the broker's dispatchers and fails if they do not drain.
//
// A bound is needed rather than a plain Sync because the interesting failure
// in this package is a notification send that was NOT abandoned: that parks
// the dispatcher for ever, and an unbounded Sync would hang instead of
// reporting it.
func syncWithin(t *testing.T, b *rabbitfake.Broker, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		b.Sync()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("the dispatcher did not drain within %v: a notification send is parked, "+
			"which means it was never abandoned", d)
	}
}

// recv reads one delivery, or fails.
func recv(t *testing.T, deliveries <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatalf("the delivery channel closed with nothing on it")
		}
		return d
	case <-time.After(2 * time.Second):
		t.Fatalf("no delivery arrived within two seconds")
		return amqp.Delivery{}
	}
}

// drainConfirms reads whatever is already on a confirm channel.
func drainConfirms(confs <-chan amqp.Confirmation) []amqp.Confirmation {
	var out []amqp.Confirmation
	for {
		select {
		case c, ok := <-confs:
			if !ok {
				return out
			}
			out = append(out, c)
		default:
			return out
		}
	}
}

// tagsOf is the delivery tags of a run of confirmations, in arrival order.
func tagsOf(confs []amqp.Confirmation) []uint64 {
	out := make([]uint64, 0, len(confs))
	for _, c := range confs {
		out = append(out, c.DeliveryTag)
	}
	return out
}

// equalTags compares two tag sequences.
func equalTags(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCumulativeConfirmsAreExpandedPerTag is the first of the seam's four
// behaviours: `basic.ack{multiple=true}` never reaches a driver.
//
// The broker sends ONE frame naming tag 3. What arrives on `NotifyPublish` is
// three confirmations, one per tag, in tag order, because `confirms.Multiple`
// expands it. A driver that had only ever been tested against a broker
// confirming one tag at a time would never run that path, and a driver that
// expected the cumulative frame would wait for ever for confirms 1 and 2.
func TestCumulativeConfirmsAreExpandedPerTag(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{
		Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmCumulative, Batch: 3},
	})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confs := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	for _, body := range []string{"a", "b", "c"} {
		publish(t, ch, "", "q", false, body)
	}
	syncWithin(t, b, 2*time.Second)

	got := drainConfirms(confs)
	if want := []uint64{1, 2, 3}; !equalTags(tagsOf(got), want) {
		t.Fatalf("the confirms arrived as %v, want %v: a cumulative basic.ack has to be "+
			"expanded into one confirmation per tag", tagsOf(got), want)
	}
	for _, c := range got {
		if !c.Ack {
			t.Fatalf("tag %d arrived as a nack; the broker acked the batch", c.DeliveryTag)
		}
	}
	if next := ch.GetNextPublishSeqNo(); next != 4 {
		t.Fatalf("GetNextPublishSeqNo is %d after three publications, want 4", next)
	}
}

// TestOutOfOrderConfirmsAreResequenced is the second half of the same
// behaviour: a confirm that arrives before the tag in front of it is HELD.
//
// Tags 3 and 2 are confirmed first and nothing reaches the driver, because
// `confirms.One` parks them in its sequencer. Confirming tag 1 releases all
// three, in tag order. A driver reading `NotifyPublish` therefore never sees a
// gap and never sees a tag twice, which is what lets it treat the stream as a
// contiguous sequence -- and is a property of the LIBRARY, not of any broker.
func TestOutOfOrderConfirmsAreResequenced(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{
		Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual},
	})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confs := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	for _, body := range []string{"a", "b", "c"} {
		publish(t, ch, "", "q", false, body)
	}
	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	h.AckPublication(3)
	h.AckPublication(2)
	syncWithin(t, b, 2*time.Second)
	if early := drainConfirms(confs); len(early) != 0 {
		t.Fatalf("tags %v reached the listener while tag 1 was still outstanding; the "+
			"sequencer has to hold a confirm until the gap in front of it is filled", tagsOf(early))
	}

	h.AckPublication(1)
	syncWithin(t, b, 2*time.Second)
	got := drainConfirms(confs)
	if want := []uint64{1, 2, 3}; !equalTags(tagsOf(got), want) {
		t.Fatalf("the confirms arrived as %v, want %v", tagsOf(got), want)
	}
}

// TestReversedConfirmsArriveInTagOrder is the same property driven by a
// broker-side script rather than by hand, which is the shape a driver's own
// test will use: nothing in the arrangement mentions a tag.
func TestReversedConfirmsArriveInTagOrder(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{
		Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmReversed, Batch: 4},
	})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confs := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	for _, body := range []string{"a", "b", "c", "d"} {
		publish(t, ch, "", "q", false, body)
	}
	syncWithin(t, b, 2*time.Second)
	got := drainConfirms(confs)
	if want := []uint64{1, 2, 3, 4}; !equalTags(tagsOf(got), want) {
		t.Fatalf("the confirms arrived as %v, want %v: the broker sent them 4,3,2,1 and the "+
			"library resequences", tagsOf(got), want)
	}
}

// TestMandatoryReturnPrecedesItsAck is the seam's fourth behaviour, and the
// ordering is the whole of it.
//
// An unroutable `mandatory` publication produces a `basic.return` and then a
// `basic.ack` -- in that order on the wire, on two different Go channels. The
// library's reader dispatches frames in wire order and blocks on each send, so
// with an UNBUFFERED return channel the return is the only thing ready when
// the driver first looks. A driver that selected over both with the return
// buffered could take the ack first and report a message nobody received as
// delivered.
func TestMandatoryReturnPrecedesItsAck(t *testing.T) {
	_, ch := setup(t, rabbitfake.Config{})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return))
	confs := ch.NotifyPublish(make(chan amqp.Confirmation))

	publish(t, ch, "", "nobody-is-bound-here", true, "a")

	var order []string
	var ret amqp.Return
	for range 2 {
		select {
		case r := <-returns:
			order, ret = append(order, "return"), r
		case <-confs:
			order = append(order, "ack")
		case <-time.After(2 * time.Second):
			t.Fatalf("only %v arrived within two seconds", order)
		}
	}
	if len(order) != 2 || order[0] != "return" || order[1] != "ack" {
		t.Fatalf("the frames arrived as %v, want [return ack]: basic.return precedes "+
			"basic.ack on the wire and the reader dispatches in wire order", order)
	}
	if ret.ReplyCode != amqp.NoRoute {
		t.Fatalf("the return's reply code is %d, want %d (NO_ROUTE)", ret.ReplyCode, amqp.NoRoute)
	}
	if got := ret.Headers["iqmq-id"]; got != "a" {
		t.Fatalf("the return carries iqmq-id %v, want \"a\": a return has no delivery tag, so "+
			"a header is the only thing a driver can correlate it by", got)
	}
}

// TestChannelCloseNacksWhatWasOutstanding is the seam's second behaviour, and
// the one turn on.
//
// Three publications; tag 2 is confirmed and parked by the sequencer; then the
// channel closes with a 406. What the library does is resolve every
// outstanding publication with `ack=false` -- a DEFINITE negative for messages
// the broker may well have taken -- CLOSE `NotifyPublish`, and discard the
// confirmation the sequencer was holding, so tag 2's ack is never delivered
// either. A driver that read any of that as "the broker refused these" would
// report a failure for a message that is in a queue.
func TestChannelCloseNacksWhatWasOutstanding(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{
		Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual},
	})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confs := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	for _, body := range []string{"a", "b", "c"} {
		publish(t, ch, "", "q", false, body)
	}
	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	h.AckPublication(2)
	syncWithin(t, b, 2*time.Second)

	cause := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - test", Server: true, Recover: true}
	h.CloseWith(cause)

	select {
	case got := <-closes:
		if got == nil || got.Code != amqp.PreconditionFailed {
			t.Fatalf("NotifyClose received %v, want a 406", got)
		}
	default:
		t.Fatal("NotifyClose received nothing; the close error is broadcast before the " +
			"listener channels are closed")
	}
	if left := drainConfirms(confs); len(left) != 0 {
		t.Fatalf("tags %v reached the listener; tag 2's confirmation was in the sequencer "+
			"behind tag 1 and a close DISCARDS what the sequencer holds", tagsOf(left))
	}
	select {
	case _, ok := <-confs:
		if ok {
			t.Fatal("NotifyPublish delivered a value after the close")
		}
	default:
		t.Fatal("NotifyPublish was not closed by the channel close")
	}
	want := []rabbitfake.Resolution{
		{Tag: 2, Outcome: rabbitfake.OutcomeAcked},
		{Tag: 1, Outcome: rabbitfake.OutcomeClosed},
		{Tag: 3, Outcome: rabbitfake.OutcomeClosed},
	}
	got := h.ConfirmResolutions()
	if len(got) != len(want) {
		t.Fatalf("the confirms resolved as %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolution %d is %v, want %v: a close nacks what is outstanding, which "+
				"is a definite negative for a publication the broker may have taken", i, got[i], want[i])
		}
	}
	if !ch.IsClosed() {
		t.Fatal("the channel does not report itself closed after a channel exception")
	}
}

// TestSlowListenerLosesTheReturn is the seam's third behaviour: a notification
// can be DROPPED.
//
// `notifyAll` abandons a send that blocks for longer than five seconds, so a
// driver that is slow to read its return channel loses the `basic.return`
// entirely -- and still receives the `basic.ack`, so it reports an unroutable
// publication as delivered. That is why the driver's confirm loop does
// bookkeeping only and never anything that could block. The timeout here is
// the immediate-abandonment form, which needs no clock at all.
func TestSlowListenerLosesTheReturn(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{NotifyTimeout: -1})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	// Nobody is parked on a receive for this one, which is what "slow" means.
	returns := ch.NotifyReturn(make(chan amqp.Return))
	confs := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	publish(t, ch, "", "nobody-is-bound-here", true, "a")
	syncWithin(t, b, 2*time.Second)

	select {
	case r := <-returns:
		t.Fatalf("the return for %q arrived; a notification whose send blocks is ABANDONED, "+
			"and that is why a driver may not do work in its return handler", r.RoutingKey)
	default:
	}
	got := drainConfirms(confs)
	if len(got) != 1 || !got[0].Ack {
		t.Fatalf("the confirms are %v, want one ack: the ack is not abandoned, so the driver "+
			"is told the publication succeeded and never told it was returned", got)
	}
}

// TestRequeuePutsTheMessageBackAtTheHead holds the fake to what the specification's
// immediate nack means.
//
// Two messages, a prefetch of one, and the first delivery nacked with
// requeue=true. The next delivery has to be the SAME message, redelivered --
// not the second one. A broker that put it at the tail would turn a retry into
// a deferral of unbounded length on any queue with a backlog, and the
// difference is invisible with a queue of one.
func TestRequeuePutsTheMessageBackAtTheHead(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	publish(t, ch, "", "q", false, "first")
	publish(t, ch, "", "q", false, "second")
	deliveries := consume(t, ch, "q", "c", false)

	first := recv(t, deliveries)
	if string(first.Body) != "first" {
		t.Fatalf("the first delivery is %q, want %q: a queue is FIFO", first.Body, "first")
	}
	if first.Redelivered {
		t.Fatal("the first delivery is marked redelivered")
	}
	if err := ch.Nack(first.DeliveryTag, false, true); err != nil {
		t.Fatalf("basic.nack: %v", err)
	}

	again := recv(t, deliveries)
	if string(again.Body) != "first" {
		t.Fatalf("the delivery after the requeue is %q, want %q: a requeued message goes back "+
			"to the HEAD of its queue", again.Body, "first")
	}
	if !again.Redelivered {
		t.Fatal("the redelivery is not marked redelivered, so a consumer cannot tell it has " +
			"seen this message before")
	}
	var requeued bool
	for _, s := range b.Settlements() {
		if s.Kind == rabbitfake.SettleNack && s.Requeue {
			requeued = true
		}
	}
	if !requeued {
		t.Fatalf("no requeueing settlement was recorded; the settlements are %v", b.Settlements())
	}
}

// TestDeliveryCountIsQuorumOnly is the specification's `(verify)`, and the driver's
// capability table rests on it.
//
// A quorum queue exposes `x-delivery-count` on a redelivery; a classic queue
// does not have the counter at all, so the header is absent. A driver that
// read Attempt off a classic queue would report every delivery as the first,
// and a driver that refused to work without the header would refuse a topology
// somebody declared classic. Both halves need a test, so both halves are here.
func TestDeliveryCountIsQuorumOnly(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	for name, kind := range map[string]string{"quorum": amqp.QueueTypeQuorum, "classic": amqp.QueueTypeClassic} {
		if _, err := ch.QueueDeclare(name, true, false, false, false,
			amqp.Table{amqp.QueueTypeArg: kind}); err != nil {
			t.Fatalf("declaring %s: %v", name, err)
		}
	}
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	for _, name := range []string{"quorum", "classic"} {
		publish(t, ch, "", name, false, name)
	}

	for _, tc := range []struct {
		queue string
		want  bool
	}{{"quorum", true}, {"classic", false}} {
		deliveries := consume(t, ch, tc.queue, "c-"+tc.queue, false)
		first := recv(t, deliveries)
		if _, present := first.Headers[rabbitfake.DeliveryCountHeader]; present {
			t.Fatalf("%s: the FIRST delivery carries %s; a real broker omits it until there "+
				"has been a failed attempt", tc.queue, rabbitfake.DeliveryCountHeader)
		}
		if err := ch.Nack(first.DeliveryTag, false, true); err != nil {
			t.Fatalf("%s: basic.nack: %v", tc.queue, err)
		}
		again := recv(t, deliveries)
		got, present := again.Headers[rabbitfake.DeliveryCountHeader]
		switch {
		case tc.want && !present:
			t.Fatalf("quorum: the redelivery has no %s header, so Attempt has nothing "+
				"to be read from", rabbitfake.DeliveryCountHeader)
		case tc.want && got != int64(1):
			t.Fatalf("quorum: %s is %v, want 1 after one failed attempt",
				rabbitfake.DeliveryCountHeader, got)
		case !tc.want && present:
			t.Fatalf("classic: the redelivery carries %s = %v; a classic queue does not have "+
				"the counter and the driver's capability table says so",
				rabbitfake.DeliveryCountHeader, got)
		}
		if err := ch.Ack(again.DeliveryTag, false); err != nil {
			t.Fatalf("%s: basic.ack: %v", tc.queue, err)
		}
	}
}

// TestMismatchedRedeclareIsAPreconditionFailure is what makes
// `internal/rabbit`'s validate-before-the-first-frame rule worth having.
//
// Re-declaring a queue with different arguments is a 406 PRECONDITION_FAILED,
// and it CLOSES THE CHANNEL. On a publisher-confirm channel that takes every
// publication whose confirm has not yet arrived with it, which is why the
// driver refuses a bad name before it writes anything.
func TestMismatchedRedeclareIsAPreconditionFailure(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	args := amqp.Table{amqp.QueueMessageTTLArg: 1000}
	if _, err := ch.QueueDeclare("q", true, false, false, false, args); err != nil {
		t.Fatalf("declaring q: %v", err)
	}
	// An int32 where the first declaration used an int is the SAME frame, so
	// this one has to be accepted; only a different value is a mismatch.
	if _, err := ch.QueueDeclare("q", true, false, false, false,
		amqp.Table{amqp.QueueMessageTTLArg: int32(1000)}); err != nil {
		t.Fatalf("re-declaring q with an equal argument of another width: %v", err)
	}

	_, err := ch.QueueDeclare("q", true, false, false, false,
		amqp.Table{amqp.QueueMessageTTLArg: 2000})
	var aerr *amqp.Error
	if !errors.As(err, &aerr) || aerr.Code != amqp.PreconditionFailed {
		t.Fatalf("re-declaring q with a different TTL returned %v, want a 406", err)
	}
	if !ch.IsClosed() {
		t.Fatal("the channel is still open after a 406; a channel exception closes the channel, " +
			"and that is why the driver validates a name before its first frame")
	}
	select {
	case got := <-closes:
		if got == nil || got.Code != amqp.PreconditionFailed {
			t.Fatalf("NotifyClose received %v, want a 406", got)
		}
	default:
		t.Fatal("NotifyClose received nothing")
	}
	if _, err := b.Queue("q"); !err {
		t.Fatal("the refused re-declaration removed the queue")
	}
}

// TestTTLDeadLettersOnlyAfterTheClockMoves is the specification's `ttldlx` mechanism
// with no broker in it.
//
// A message parked in a queue whose `x-message-ttl` is a second is still there
// at 999 milliseconds and dead-lettered at 1000, through the queue's
// `x-dead-letter-exchange` with the queue's `x-dead-letter-routing-key`. The
// clock is the test's, which is the whole point: against a container the same
// assertion is a real sleep, and a real sleep in a test about a
// thirty-second delay is a test nobody runs.
func TestTTLDeadLettersOnlyAfterTheClockMoves(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	if err := ch.ExchangeDeclare("q.dlx", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring the dead-letter exchange: %v", err)
	}
	if _, err := ch.QueueDeclare("q.dlq", true, false, false, false, nil); err != nil {
		t.Fatalf("declaring the dead-letter queue: %v", err)
	}
	if err := ch.QueueBind("q.dlq", "dlq", "q.dlx", false, nil); err != nil {
		t.Fatalf("binding the dead-letter queue: %v", err)
	}
	if _, err := ch.QueueDeclare("q.delay", true, false, false, false, amqp.Table{
		amqp.QueueMessageTTLArg:            1000,
		rabbitfake.DeadLetterExchangeArg:   "q.dlx",
		rabbitfake.DeadLetterRoutingKeyArg: "dlq",
	}); err != nil {
		t.Fatalf("declaring the delay queue: %v", err)
	}
	publish(t, ch, "", "q.delay", false, "parked")

	b.Advance(999 * time.Millisecond)
	if q, ok := b.Queue("q.delay"); !ok || q.Depth != 1 {
		t.Fatalf("the delay queue holds %v at 999ms, want one message: a TTL is a MINIMUM and "+
			"must not fire early", q)
	}
	if dl := b.DeadLettered(); len(dl) != 0 {
		t.Fatalf("%d dead-letter hops happened before the TTL elapsed: %v", len(dl), dl)
	}

	b.Advance(1 * time.Millisecond)
	dl := b.DeadLettered()
	if len(dl) != 1 {
		t.Fatalf("%d dead-letter hops at 1000ms, want one: %v", len(dl), dl)
	}
	if dl[0].Reason != rabbitfake.DeathExpired || dl[0].Exchange != "q.dlx" || dl[0].RoutingKey != "dlq" {
		t.Fatalf("the hop is %+v, want expired via q.dlx with the key dlq: a queue's "+
			"x-dead-letter-routing-key REPLACES the message's own key", dl[0])
	}
	if q, ok := b.Queue("q.dlq"); !ok || q.Depth != 1 {
		t.Fatalf("the dead-letter queue holds %v, want one message", q)
	}
	if q, ok := b.Queue("q.delay"); !ok || q.Depth != 0 {
		t.Fatalf("the delay queue still holds %v", q)
	}
}

// TestPrefetchBoundsUnacknowledgedDeliveries is the specification's `Qos(prefetch)`.
//
// With a prefetch of two, a consumer holds two unacknowledged deliveries and
// no more, however many messages are ready; acknowledging one releases exactly
// one more. A broker that ignored the prefetch would hand a driver its whole
// backlog at once, which is the difference between bounded memory and none.
func TestPrefetchBoundsUnacknowledgedDeliveries(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	if err := ch.Qos(2, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	for _, body := range []string{"a", "b", "c", "d", "e"} {
		publish(t, ch, "", "q", false, body)
	}
	deliveries := consume(t, ch, "q", "c", false)
	syncWithin(t, b, 2*time.Second)

	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	if got := h.Unacked(); got != 2 {
		t.Fatalf("the channel holds %d unacknowledged deliveries, want 2: the prefetch bounds "+
			"how many one consumer may hold", got)
	}
	if q, ok := b.Queue("q"); !ok || q.Depth != 3 {
		t.Fatalf("the queue holds %v, want three ready messages behind the prefetch", q)
	}

	first := recv(t, deliveries)
	if err := ch.Ack(first.DeliveryTag, false); err != nil {
		t.Fatalf("basic.ack: %v", err)
	}
	syncWithin(t, b, 2*time.Second)
	if got := h.Unacked(); got != 2 {
		t.Fatalf("the channel holds %d unacknowledged deliveries after one ack, want 2: an "+
			"acknowledgement releases exactly one more", got)
	}
	states := h.ConsumerStates()
	if len(states) != 1 || states[0].Outstanding > states[0].Prefetch {
		t.Fatalf("the consumer holds %v, which breaches its own prefetch", states)
	}
}

// TestBothPrefetchReadingsAreEnforcedIndependently is the two readings of
// `Qos(prefetch)` held at the same time.
//
// rabbitmq.com/docs/consumer-prefetch, "Multiple Consumers Sharing the
// Limit": "RabbitMQ interprets this as meaning that the two prefetch limits
// should be enforced independently of each other; consumers will only receive
// new messages when neither limit on unacknowledged messages has been
// reached." So a global qos does NOT retire a per-consumer bound that is
// already in force, and it does not widen it either.
//
// The first half is crasher #75's own script -- qos(1, global=false),
// consume, publish three, qos(3, global=true) -- which this fake answered by
// handing the consumer a second delivery, because both readings shared one
// field and a global qos overwrote the per-consumer one. The second half is
// the opposite direction: the channel-wide reading binds the total even when
// every consumer has room under its own.
func TestBothPrefetchReadingsAreEnforcedIndependently(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("basic.qos per-consumer: %v", err)
	}
	deliveries := consume(t, ch, "q", "c", false)
	for _, body := range []string{"a", "b", "c"} {
		publish(t, ch, "", "q", false, body)
	}
	if err := ch.Qos(3, 0, true); err != nil {
		t.Fatalf("basic.qos channel-wide: %v", err)
	}
	syncWithin(t, b, 2*time.Second)

	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	states := h.ConsumerStates()
	if len(states) != 1 {
		t.Fatalf("the channel reports %d consumers, want 1", len(states))
	}
	if states[0].Prefetch != 1 {
		t.Errorf("the consumer's prefetch is %d after a global qos, want 1: a global qos sets "+
			"the OTHER reading and leaves this consumer's snapshot alone", states[0].Prefetch)
	}
	if states[0].Outstanding != 1 {
		t.Fatalf("the consumer holds %d unacknowledged deliveries with a per-consumer prefetch "+
			"of 1 and a channel-wide prefetch of 3, want 1: both limits are enforced",
			states[0].Outstanding)
	}
	if con, chWide := h.Prefetch(); con != 1 || chWide != 3 {
		t.Errorf("the channel reports prefetch readings (%d, %d), want (1, 3)", con, chWide)
	}
	first := recv(t, deliveries)
	if err := ch.Ack(first.DeliveryTag, false); err != nil {
		t.Fatalf("basic.ack: %v", err)
	}
	syncWithin(t, b, 2*time.Second)
	if got := h.ConsumerStates()[0].Outstanding; got != 1 {
		t.Fatalf("the consumer holds %d after one ack, want 1: an acknowledgement releases "+
			"exactly one more and the per-consumer bound is still 1", got)
	}

	// The other direction: three consumers that each have room under a
	// per-consumer bound of two, behind a channel-wide bound of three.
	b2, ch2 := setup(t, rabbitfake.Config{})
	if err := ch2.Qos(2, 0, false); err != nil {
		t.Fatalf("basic.qos per-consumer: %v", err)
	}
	if err := ch2.Qos(3, 0, true); err != nil {
		t.Fatalf("basic.qos channel-wide: %v", err)
	}
	for _, body := range []string{"a", "b", "c", "d", "e", "f"} {
		publish(t, ch2, "", "q", false, body)
	}
	consume(t, ch2, "q", "c0", false)
	consume(t, ch2, "q", "c1", false)
	consume(t, ch2, "q", "c2", false)
	syncWithin(t, b2, 2*time.Second)
	h2 := b2.LastChannel()
	if got := h2.Unacked(); got != 3 {
		t.Fatalf("the channel holds %d unacknowledged deliveries with a channel-wide prefetch "+
			"of 3, want 3: the channel-wide limit bounds the total however much room each "+
			"consumer has under its own", got)
	}
	for _, st := range h2.ConsumerStates() {
		if st.Outstanding > st.Prefetch {
			t.Errorf("consumer %q holds %d with a prefetch of %d", st.Tag, st.Outstanding, st.Prefetch)
		}
	}
}

// TestPassiveDeclareOfAMissingQueueIs404 is the specification's "no hidden creation" made
// checkable: a passive declare asserts rather than creates, and its refusal
// closes the channel.
func TestPassiveDeclareOfAMissingQueueIs404(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	_, err := ch.QueueDeclarePassive("absent", true, false, false, false, nil)
	var aerr *amqp.Error
	if !errors.As(err, &aerr) || aerr.Code != amqp.NotFound {
		t.Fatalf("a passive declare of a queue that does not exist returned %v, want a 404", err)
	}
	if !ch.IsClosed() {
		t.Fatal("the channel is still open after a 404")
	}
	if _, ok := b.Queue("absent"); ok {
		t.Fatal("the passive declare CREATED the queue, which is the hidden creation the specification forbids")
	}
}

// TestReservedPrefixIsAccessRefused is the refusal `internal/rabbit`'s
// `checkDeclarable` exists to keep a caller's prefix out of.
func TestReservedPrefixIsAccessRefused(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	_, err := ch.QueueDeclare("amq.mine", true, false, false, false, nil)
	var aerr *amqp.Error
	if !errors.As(err, &aerr) || aerr.Code != amqp.AccessRefused {
		t.Fatalf("declaring a queue under the reserved prefix returned %v, want a 403", err)
	}
	if !ch.IsClosed() {
		t.Fatal("the channel is still open after a 403")
	}
}

// TestDelayedMessageExchangeNeedsThePlugin is the detection.
//
// `rabbitmq_delayed_message_exchange` is not in the official image, so a driver
// that assumed it would fail at open against a stock broker. The refusal is a
// 503 COMMAND_INVALID -- an unknown exchange TYPE -- and with the plugin
// installed the same declaration is accepted and the delay is honoured off the
// injected clock.
func TestDelayedMessageExchangeNeedsThePlugin(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	args := amqp.Table{rabbitfake.DelayedTypeArg: amqp.ExchangeDirect}
	err := ch.ExchangeDeclare("d", rabbitfake.ExchangeDelayed, true, false, false, false, args)
	var aerr *amqp.Error
	if !errors.As(err, &aerr) || aerr.Code != amqp.PreconditionFailed {
		t.Fatalf("declaring an %s exchange without the plugin returned %v, want a 503",
			rabbitfake.ExchangeDelayed, err)
	}

	b.SetDelayedMessagePlugin(true)
	ch = openChannel(t, dial(t, b))
	if err := ch.ExchangeDeclare("d", rabbitfake.ExchangeDelayed, true, false, false, false, args); err != nil {
		t.Fatalf("declaring an %s exchange with the plugin installed: %v", rabbitfake.ExchangeDelayed, err)
	}
	if _, err := ch.QueueDeclare("target", true, false, false, false, nil); err != nil {
		t.Fatalf("declaring the target: %v", err)
	}
	if err := ch.QueueBind("target", "k", "d", false, nil); err != nil {
		t.Fatalf("binding the target: %v", err)
	}
	msg := pub("later")
	msg.Headers[rabbitfake.DelayHeader] = int32(500)
	if err := ch.PublishWithContext(context.Background(), "d", "k", false, false, msg); err != nil {
		t.Fatalf("publishing a delayed message: %v", err)
	}
	if held := b.Delayed(); len(held) != 1 {
		t.Fatalf("the exchange holds %d delayed publications, want one", len(held))
	}
	if q, _ := b.Queue("target"); q.Depth != 0 {
		t.Fatalf("the target already holds %d messages; the plugin holds the message inside "+
			"the exchange until the delay elapses", q.Depth)
	}
	b.Advance(500 * time.Millisecond)
	if q, _ := b.Queue("target"); q.Depth != 1 {
		t.Fatalf("the target holds %d messages after the delay, want one", q.Depth)
	}
}

// TestQueueDeleteCancelsItsConsumers is the one thing that makes a broker send
// `basic.cancel`, and the reason `NotifyCancel` is on the seam at all.
//
// A driver that treated a cancelled consumer as a transient read error would
// spin against a queue that no longer exists.
func TestQueueDeleteCancelsItsConsumers(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	cancels := ch.NotifyCancel(make(chan string, 1))
	deliveries := consume(t, ch, "q", "doomed", false)
	if _, err := ch.QueueDelete("q", false, false, false); err != nil {
		t.Fatalf("deleting q: %v", err)
	}
	syncWithin(t, b, 2*time.Second)
	select {
	case tag := <-cancels:
		if tag != "doomed" {
			t.Fatalf("NotifyCancel named %q, want %q", tag, "doomed")
		}
	default:
		t.Fatal("deleting a queue did not cancel its consumer, so a driver would never learn " +
			"that the queue it is consuming is gone")
	}
	select {
	case _, ok := <-deliveries:
		if ok {
			t.Fatal("the delivery channel yielded a value after the consumer was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the delivery channel was not closed by the cancel")
	}
}

// TestSettlingTwiceIsAPreconditionFailure holds the fake to the awkward shape
// of the real answer.
//
// A tag the channel never issued and a tag it has already settled are the same
// thing to a broker, and its only way to object is a channel exception --
// `basic.ack` has no reply, so the CALL returns nil and the channel dies
// afterwards. A driver that checked only the error return would think the
// second ack succeeded.
func TestSettlingTwiceIsAPreconditionFailure(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	publish(t, ch, "", "q", false, "a")
	deliveries := consume(t, ch, "q", "c", false)
	d := recv(t, deliveries)
	if err := ch.Ack(d.DeliveryTag, false); err != nil {
		t.Fatalf("the first basic.ack: %v", err)
	}
	if err := ch.Ack(d.DeliveryTag, false); err != nil {
		t.Fatalf("the second basic.ack returned %v; basic.ack has no reply, so the call cannot "+
			"report the broker's objection", err)
	}
	syncWithin(t, b, 2*time.Second)
	if !ch.IsClosed() {
		t.Fatal("settling the same delivery twice left the channel open")
	}
	select {
	case got := <-closes:
		if got == nil || got.Code != amqp.PreconditionFailed {
			t.Fatalf("NotifyClose received %v, want a 406 for the unknown delivery tag", got)
		}
	default:
		t.Fatal("NotifyClose received nothing")
	}
	if err := ch.Ack(d.DeliveryTag, false); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("a settlement on the now-closed channel returned %v, want ErrClosed", err)
	}
}

// TestTopicRoutingUsesTheSharedMatcher is the specification's "does this binding really
// reach that topic", answered by the same matcher the driver answers it with.
func TestTopicRoutingUsesTheSharedMatcher(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	if err := ch.ExchangeDeclare("t", amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring the topic exchange: %v", err)
	}
	for _, q := range []struct{ name, key string }{
		{"eu", "orders.eu.*"},
		{"all", "orders.#"},
		{"none", "invoices.*"},
	} {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, nil); err != nil {
			t.Fatalf("declaring %s: %v", q.name, err)
		}
		if err := ch.QueueBind(q.name, q.key, "t", false, nil); err != nil {
			t.Fatalf("binding %s: %v", q.name, err)
		}
	}
	publish(t, ch, "t", "orders.eu.created", false, "m")
	for _, tc := range []struct {
		queue string
		want  int
	}{{"eu", 1}, {"all", 1}, {"none", 0}} {
		if got := len(b.Publications(tc.queue)); got != tc.want {
			t.Fatalf("%s received %d publications, want %d", tc.queue, got, tc.want)
		}
	}
}

// TestServerPropertiesReportTheConfiguredVersion is what the four
// version-gated claims are tested through. A gate nobody has watched refuse is
// assumed not to work.
func TestServerPropertiesReportTheConfiguredVersion(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{Product: "RabbitMQ", Version: "3.13.7"})
	conn := dial(t, b)
	props := conn.ServerProperties()
	if got := props["product"]; got != "RabbitMQ" {
		t.Fatalf("server_properties reports the product %v, want RabbitMQ", got)
	}
	if got := props["version"]; got != "3.13.7" {
		t.Fatalf("server_properties reports the version %v, want 3.13.7", got)
	}
	b.SetServerVersion("RabbitMQ", "4.3.1")
	if got := dial(t, b).ServerProperties()["version"]; got != "4.3.1" {
		t.Fatalf("a connection opened after SetServerVersion reports %v, want 4.3.1", got)
	}
}

// TestFailedPublishHandsTheDeliveryTagBack is `confirms.unpublish`, the one
// place the library's publication counter moves BACKWARDS.
//
// The driver assigns its own tags under the lock that serialises the publish,
// so it has to move backwards with the library or every confirm after this one
// is attributed to the wrong publication. `GetNextPublishSeqNo` is on the seam
// so that the agreement is assertable, and this is the arrangement in which it
// could fail.
func TestFailedPublishHandsTheDeliveryTagBack(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{
		Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual},
	})
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	publish(t, ch, "", "q", false, "a")
	if next := ch.GetNextPublishSeqNo(); next != 2 {
		t.Fatalf("GetNextPublishSeqNo is %d after one publication, want 2", next)
	}
	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	sentinel := errors.New("the write did not leave")
	h.FailNextPublishes(1, sentinel)
	if err := ch.PublishWithContext(context.Background(), "", "q", false, false, pub("b")); !errors.Is(err, sentinel) {
		t.Fatalf("the failed publish returned %v, want the injected error", err)
	}
	if next := ch.GetNextPublishSeqNo(); next != 2 {
		t.Fatalf("GetNextPublishSeqNo is %d after a publish that failed, want 2: the library "+
			"counts the publication and then hands the tag back", next)
	}
	if pending := h.PendingConfirms(); len(pending) != 1 || pending[0] != 1 {
		t.Fatalf("the outstanding tags are %v, want just tag 1: the failed publication's tag "+
			"was withdrawn", pending)
	}
	publish(t, ch, "", "q", false, "c")
	if next := ch.GetNextPublishSeqNo(); next != 3 {
		t.Fatalf("GetNextPublishSeqNo is %d after the next successful publication, want 3", next)
	}
}

// TestBrokerSideCancelReachesNotifyCancel is the fault a driver meets when an
// operator deletes a queue under it, reached directly.
func TestBrokerSideCancelReachesNotifyCancel(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	cancels := ch.NotifyCancel(make(chan string, 1))
	consume(t, ch, "q", "c", false)
	h := b.LastChannel()
	if h == nil {
		t.Fatal("the broker reports no open channel")
	}
	if !h.CancelConsumer("c") {
		t.Fatal("CancelConsumer reported no such consumer")
	}
	select {
	case tag := <-cancels:
		if tag != "c" {
			t.Fatalf("NotifyCancel named %q, want %q", tag, "c")
		}
	default:
		t.Fatal("a broker-side basic.cancel did not reach NotifyCancel")
	}
	if got := h.Consumers(); len(got) != 0 {
		t.Fatalf("the channel still reports the consumers %v", got)
	}
}

// TestConnectionCloseRequeuesEveryUnsettledDelivery is the disqualifier
// arranged deliberately: after this, every delivery tag the driver holds names
// a message somebody else may now have.
func TestConnectionCloseRequeuesEveryUnsettledDelivery(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	conn := b.LastConnection()
	if conn == nil {
		t.Fatal("the broker reports no connection")
	}
	if err := ch.Qos(2, 0, false); err != nil {
		t.Fatalf("basic.qos: %v", err)
	}
	publish(t, ch, "", "q", false, "a")
	publish(t, ch, "", "q", false, "b")
	consume(t, ch, "q", "c", false)
	syncWithin(t, b, 2*time.Second)
	if got := len(b.InFlight()); got != 2 {
		t.Fatalf("%d deliveries are in flight, want 2", got)
	}

	conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "CONNECTION_FORCED - test", Server: true})
	if got := len(b.InFlight()); got != 0 {
		t.Fatalf("%d deliveries are still in flight after the connection closed", got)
	}
	if q, ok := b.Queue("q"); !ok || q.Depth != 2 {
		t.Fatalf("the queue holds %v, want both messages back: a connection close gives every "+
			"unsettled delivery back to its queue", q)
	}
	ready := b.Ready("q")
	if len(ready) != 2 || string(ready[0].Body) != "a" || !ready[0].Redelivered {
		t.Fatalf("the queue holds %v, want a then b, both redelivered", ready)
	}
	if !ch.IsClosed() {
		t.Fatal("a channel on a closed connection does not report itself closed")
	}
}

// TestConnectionBlockedReachesItsListener is NotifyBlocked: how
// RabbitMQ tells a publisher that the broker is under resource pressure.
func TestConnectionBlockedReachesItsListener(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	conn := dial(t, b)
	blockings := conn.NotifyBlocked(make(chan amqp.Blocking, 2))
	h := b.LastConnection()
	h.Block("low on disk")
	h.Unblock()
	got := make([]amqp.Blocking, 0, 2)
	for range 2 {
		select {
		case v := <-blockings:
			got = append(got, v)
		default:
			t.Fatalf("only %v arrived", got)
		}
	}
	if !got[0].Active || got[0].Reason != "low on disk" || got[1].Active {
		t.Fatalf("the blockings are %v, want an active one with a reason and then an inactive one", got)
	}
}

// TestChannelFlowReachesItsListener is RabbitMQ's per-channel back-pressure
// signal. The fake does not enforce it, deliberately: what a driver has to do
// with `channel.flow` is stop publishing, and a broker that refused the
// publications anyway would make a driver that ignored the signal look correct.
func TestChannelFlowReachesItsListener(t *testing.T) {
	b, ch := setup(t, rabbitfake.Config{})
	flows := ch.NotifyFlow(make(chan bool, 2))
	h := b.LastChannel()
	h.Flow(false)
	h.Flow(true)
	select {
	case v := <-flows:
		if v {
			t.Fatal("the first channel.flow is active, want inactive")
		}
	default:
		t.Fatal("channel.flow did not reach its listener")
	}
	select {
	case v := <-flows:
		if !v {
			t.Fatal("the second channel.flow is inactive, want active")
		}
	default:
		t.Fatal("the second channel.flow did not arrive")
	}
}

// TestDeterministicObservations is the determinism claim, checked rather than
// asserted in a comment: the same script twice produces the same observations.
func TestDeterministicObservations(t *testing.T) {
	run := func() string {
		b := rabbitfake.New(rabbitfake.Config{})
		defer func() { _ = b.Close() }()
		ch := openChannel(t, dial(t, b))
		if err := ch.ExchangeDeclare("t", amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
			t.Fatalf("declaring: %v", err)
		}
		for _, name := range []string{"c", "a", "b"} {
			if _, err := ch.QueueDeclare(name, true, false, false, false,
				amqp.Table{rabbitfake.DeadLetterExchangeArg: "t"}); err != nil {
				t.Fatalf("declaring %s: %v", name, err)
			}
			if err := ch.QueueBind(name, "k.#", "t", false, nil); err != nil {
				t.Fatalf("binding %s: %v", name, err)
			}
		}
		publish(t, ch, "t", "k.one", false, "m1")
		publish(t, ch, "t", "k.two", false, "m2")
		deliveries := consume(t, ch, "a", "ca", false)
		d := recv(t, deliveries)
		if err := ch.Reject(d.DeliveryTag, false); err != nil {
			t.Fatalf("basic.reject: %v", err)
		}
		b.Sync()
		out := ""
		for _, q := range b.Queues() {
			out += q.Name + ":" + itoa(q.Depth) + "/" + itoa(q.Unacked) + ";"
		}
		for _, s := range b.Settlements() {
			out += string(s.Kind) + ":" + q64(s.Tag) + ";"
		}
		for _, dl := range b.DeadLettered() {
			out += string(dl.Reason) + "->" + dl.Exchange + "/" + dl.RoutingKey + ";"
		}
		return out
	}
	first, second := run(), run()
	if first != second {
		t.Fatalf("two runs of one script observed different states:\n  %s\n  %s", first, second)
	}

	// The two-run comparison is necessary and not sufficient: a ranged map
	// with three keys agrees with itself about a third of the time, so a
	// fake that had gone back to ranging one would pass this test most runs
	// and fail it occasionally, which is the worst of both. So the ordering
	// is asserted directly as well -- every collection whose order is
	// observable comes back sorted.
	b := newBroker(t, rabbitfake.Config{})
	ch := openChannel(t, dial(t, b))
	for _, name := range []string{"zulu", "alpha", "mike"} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			t.Fatalf("declaring %s: %v", name, err)
		}
		if err := ch.ExchangeDeclare("x-"+name, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			t.Fatalf("declaring x-%s: %v", name, err)
		}
	}
	queues := b.Queues()
	for i := 1; i < len(queues); i++ {
		if queues[i-1].Name > queues[i].Name {
			t.Fatalf("Broker.Queues returned %q before %q; an observable order that comes "+
				"out of a map iteration is a test that disagrees with itself between runs",
				queues[i-1].Name, queues[i].Name)
		}
	}
	exchanges := b.Exchanges()
	for i := 1; i < len(exchanges); i++ {
		if exchanges[i-1].Name > exchanges[i].Name {
			t.Fatalf("Broker.Exchanges returned %q before %q", exchanges[i-1].Name, exchanges[i].Name)
		}
	}
}

// itoa and q64 keep the determinism test's fingerprint free of fmt, whose
// map-printing order is its own question.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func q64(n uint64) string { return itoa(int(n)) }
