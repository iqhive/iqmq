package rabbitmq_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzRabbitTagSettlement is the settlement disqualifier as a fuzz target,
// and it is the one this driver most needs.
//
// A delivery tag is per CHANNEL and restarts at 1 on a new one. So every time
// a channel is replaced — a channel exception, a reconnection, a
// broker-side consumer cancel — every tag this driver has handed out names, on
// the replacement channel, whatever delivery holds that tag there. Sending one
// of them would acknowledge a message the caller never saw and never
// processed, and nothing in a deployment would report it: the caller's own
// message would then be redelivered and the other one would be gone.
//
// The invariant is therefore not "no crash" but the one that matters: **every
// settlement the driver sends names the delivery the caller settled, and a
// settlement the driver refuses sends nothing at all.** The fake records every
// settlement with the MESSAGE it settled, so the check is exact rather than a
// count: a settlement that landed on another delivery is a settlement whose
// recorded body is not the one the caller settled.
//
// The second invariant is the stickiness: a settlement reported as
// AlreadySettled, LeaseLost or SettlementUnknown must not have reached the
// broker, and a delivery whose outcome was unknown must never later report
// success.
func FuzzRabbitTagSettlement(f *testing.F) {
	seed := func(steps ...string) {
		w := fuzzutil.NewWriter()
		for _, s := range steps {
			tagScript.WriteStep(f, w, s)
		}
		f.Add(w.Data())
	}
	seed("publish", "fetch", "ack")
	seed("publish", "publish", "fetch", "closechannel", "ack")
	seed("publish", "fetch", "closechannel", "fetch", "ack", "ack")
	seed("publish", "fetch", "ack", "ack")
	seed("publish", "fetch", "nack", "fetch", "ack")
	seed("publish", "fetch", "closeconn", "ack")
	seed("publish", "publish", "fetch", "deadletter", "drop")
	seed("publish", "fetch", "closechannel", "closechannel", "ack")
	seed("publish", "fetch", "cancel", "ack")

	f.Fuzz(func(t *testing.T, data []byte) {
		s := newTagSUT(t)
		tagScript.Run(t, data, s)
		s.finish()
	})
}

// tagSUT is one client over one fake broker, with the model of what it holds.
type tagSUT struct {
	t      *testing.T
	broker *rabbitfake.Broker
	client *mq.Client
	cons   *mq.ConsumerHandle
	pub    *mq.PublisherHandle

	// held is the deliveries the caller has fetched and not settled, keyed by
	// body -- which is the only identity that survives a redelivery on a new
	// channel, and therefore the only one a settlement can be checked
	// against.
	held map[string]heldDelivery
	// epoch counts the disruptions: a channel close, a connection loss, a
	// broker-side cancel. A delivery fetched in an earlier epoch is one whose
	// channel has been replaced, so its tag names a different delivery now
	// and its settlement MUST be refused. It is what turns "no crash" into
	// the claim.
	epoch int
	// published counts the publications, which is what makes each body
	// unique.
	published int
	// buf is the fetch buffer, reused so that a fetch allocates nothing.
	buf []mq.Delivery
}

// newTagSUT opens a client whose topology exists and whose prefetch is small
// enough that the ring fills.
func newTagSUT(t *testing.T) *tagSUT {
	t.Helper()
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		// A short first delay, so a reconnection inside a fuzz iteration
		// costs microseconds rather than a hundred milliseconds.
		c.ReconnectBase = time.Microsecond
	})
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 4 })
	return &tagSUT{
		t:      t,
		broker: broker,
		client: client,
		cons:   cons,
		pub:    publishOn(t, client, "orders"),
		held:   map[string]heldDelivery{},
		buf:    make([]mq.Delivery, 4),
	}
}

// heldDelivery is one delivery the caller is holding and the epoch it was
// fetched in.
type heldDelivery struct {
	d     mq.Delivery
	epoch int
}

// tagScript is the operation script.
var tagScript = fuzzutil.Script[*tagSUT]{
	MaxSteps: 64,
	Steps: []fuzzutil.Step[*tagSUT]{
		{Name: "publish", Weight: 5, Run: opTagPublish},
		{Name: "fetch", Weight: 5, Run: opTagFetch},
		{Name: "ack", Weight: 5, Run: opTagAck},
		{Name: "nack", Weight: 3, Run: opTagNack},
		{Name: "drop", Weight: 1, Run: opTagDrop},
		{Name: "deadletter", Weight: 1, Run: opTagDeadLetter},
		{Name: "closechannel", Weight: 2, Run: opTagCloseChannel},
		{Name: "closeconn", Weight: 1, Run: opTagCloseConn},
		{Name: "cancel", Weight: 1, Run: opTagCancel},
	},
}

// opTagPublish publishes one message with a body nothing else has.
func opTagPublish(t testing.TB, _ *fuzzutil.Reader, s *tagSUT) {
	s.published++
	body := "m" + strconv.Itoa(s.published)
	err := s.pub.Publish(s.t.Context(), &mq.Message{Body: []byte(body)})
	if err == nil {
		return
	}
	// A publication may fail while the connection is being re-established,
	// and it may be ambiguous. Neither is a defect; what would be is a
	// definite failure for a publication the broker may have taken.
	var oe *mq.OpError
	if errors.As(err, &oe) && (oe.Ambiguous || oe.Retryable) {
		return
	}
	t.Fatalf("publishing %s: %v", body, err)
}

// opTagFetch receives whatever is available and records it.
func opTagFetch(t testing.TB, _ *fuzzutil.Reader, s *tagSUT) {
	ctx, cancel := contextWithTimeoutTB(s.t, 50*time.Millisecond)
	defer cancel()
	n, err := s.cons.Receive(ctx, s.buf)
	if err != nil {
		// A receive may time out (nothing there), report the queue gone (a
		// cancel) or report closed. None is a defect.
		return
	}
	for i := range n {
		body := string(s.buf[i].Body)
		if prev, dup := s.held[body]; dup && prev.epoch == s.epoch {
			_ = prev
			// A redelivery in the SAME epoch is a defect: nothing has
			// invalidated the first delivery, so the broker has handed the
			// same message out twice while it was still settleable.
			t.Fatalf("the driver handed out %s twice in one epoch without a settlement in between", body)
		}
		s.held[body] = heldDelivery{d: s.buf[i], epoch: s.epoch}
	}
}

// opTagAck, opTagNack, opTagDrop and opTagDeadLetter settle one held delivery.
func opTagAck(t testing.TB, r *fuzzutil.Reader, s *tagSUT) {
	s.settle(t, r, "ack", func(d mq.Delivery) error { return d.Ack() })
}

func opTagNack(t testing.TB, r *fuzzutil.Reader, s *tagSUT) {
	s.settle(t, r, "nack", func(d mq.Delivery) error { return d.Nack() })
}

func opTagDrop(t testing.TB, r *fuzzutil.Reader, s *tagSUT) {
	s.settle(t, r, "drop", func(d mq.Delivery) error { return d.Nack(mq.NackDrop()) })
}

func opTagDeadLetter(t testing.TB, r *fuzzutil.Reader, s *tagSUT) {
	s.settle(t, r, "deadletter", func(d mq.Delivery) error { return d.Nack(mq.NackDeadLetter()) })
}

// settle applies one settlement and checks the invariants.
//
// Three things are forbidden, and each of them is a way to settle the wrong
// message:
//
//  1. A successful settlement whose recorded frame names ANOTHER body. That is
//     the disqualifier: a tag from a replaced channel names a different
//     delivery there.
//  2. A refused settlement that still sent a frame. A driver that sent the
//     frame and mapped the broker's refusal would, on a broker that did not
//     refuse it, have settled somebody else's delivery.
//  3. A SUCCESSFUL settlement of a delivery fetched in an earlier epoch. Its
//     channel has been replaced, so it cannot be settled at all, and a driver
//     that reported success either lied or settled something else.
func (s *tagSUT) settle(t testing.TB, r *fuzzutil.Reader, what string, act func(mq.Delivery) error) {
	body, h, ok := s.pick(r)
	if !ok {
		return
	}
	before := s.settlements()
	err := act(h.d)
	after := s.settlements()
	newly := after[len(before):]

	if err == nil {
		for i := range newly {
			if got := string(newly[i].Message.Body); got != body {
				t.Fatalf("%s of %s settled the delivery %q instead: a tag from a replaced channel names a different delivery there, and sending it acknowledges a message the caller never saw ",
					what, body, got)
			}
		}
		if len(newly) == 0 {
			t.Fatalf("%s of %s reported success and sent nothing to the broker", what, body)
		}
		if h.epoch != s.epoch {
			t.Fatalf("%s of %s succeeded although its channel had been replaced %d time(s) since it was fetched: its tag names a different delivery on the new channel",
				what, body, s.epoch-h.epoch)
		}
		delete(s.held, body)
		return
	}

	if len(newly) != 0 {
		t.Fatalf("%s of %s was refused (%v) and still sent %d frame(s) to the broker: %+v", what, body, err, len(newly), newly)
	}
	switch {
	case errors.Is(err, mq.ErrLeaseLost), errors.Is(err, mq.ErrAlreadySettled),
		errors.Is(err, mq.ErrSettlementUnknown), errors.Is(err, mq.ErrUnsupported),
		errors.Is(err, mq.ErrClosed), errors.Is(err, mq.ErrSettlementInProgress):
	default:
		switch mq.CodeOf(err) {
		case mq.CodeSettlementUnknown, mq.CodeUnavailable, mq.CodeNotFound:
		default:
			t.Fatalf("%s of %s failed with an unexpected error: %v (code %v)", what, body, err, mq.CodeOf(err))
		}
	}
	// A refused settlement leaves the caller holding nothing it can act on:
	// the delivery is gone, and the broker will redeliver it.
	delete(s.held, body)
}

// pick chooses one held delivery.
func (s *tagSUT) pick(r *fuzzutil.Reader) (string, heldDelivery, bool) {
	if len(s.held) == 0 {
		return "", heldDelivery{}, false
	}
	bodies := make([]string, 0, len(s.held))
	for body := range s.held {
		bodies = append(bodies, body)
	}
	// The map's iteration order is not stable, so the bodies are sorted
	// before the choice: a fuzz target whose own choice depended on map order
	// would not reproduce from a corpus entry.
	sortStrings(bodies)
	body := bodies[r.Choice(len(bodies))]
	return body, s.held[body], true
}

// opTagCloseChannel closes whatever channel the consumer is using, which is a
// channel exception.
func opTagCloseChannel(_ testing.TB, _ *fuzzutil.Reader, s *tagSUT) {
	for _, ch := range s.broker.Channels() {
		if len(ch.Consumers()) > 0 && !ch.IsClosed() {
			ch.CloseWith(&amqp.Error{Code: amqp.ResourceError, Reason: "pressure", Server: true})
			s.disrupt()
			return
		}
	}
}

// opTagCloseConn closes the connection, which is the reconnection path.
func opTagCloseConn(_ testing.TB, _ *fuzzutil.Reader, s *tagSUT) {
	if cn := s.broker.LastConnection(); cn != nil && !cn.IsClosed() {
		cn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		s.disrupt()
	}
}

// opTagCancel makes the broker cancel the consumer, which is what a
// `queue.delete` under a live consumer produces.
func opTagCancel(_ testing.TB, _ *fuzzutil.Reader, s *tagSUT) {
	for _, ch := range s.broker.Channels() {
		for _, tag := range ch.Consumers() {
			ch.CancelConsumer(tag)
		}
	}
	s.disrupt()
}

// disrupt records that every delivery handed out so far is now unsettleable,
// and waits until the DRIVER has noticed.
//
// The wait is the part that makes the epoch mean something. A fault is armed
// on the broker, and the driver learns about it through a notification that
// travels through the fake's dispatcher and then through the driver's own
// pump; until it has, the driver is still handing out deliveries from the dead
// channel, and a model that advanced its epoch at the moment the fault was
// armed would call the first redelivery after the re-attach a duplicate. The
// source's channel GENERATION is the observable that says the driver has
// invalidated its tokens, drained its ring and re-attached, so the wait is on
// that.
func (s *tagSUT) disrupt() {
	b := backendOf(s.t, s.client)
	before := rq.GenerationsForTest(b)
	s.broker.Sync()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		// EITHER condition is the driver having noticed. The generation
		// moving is the re-attach; no source having a channel at all is the
		// post-cancel steady state, in which the generation moves once and
		// then never again -- so a wait on the move alone would spend the
		// whole budget on every later disruption, and a single fuzz input
		// took a minute before this was here.
		if moved(before, rq.GenerationsForTest(b)) || rq.AttachedForTest(b) == 0 {
			s.epoch++
			return
		}
		time.Sleep(20 * time.Microsecond)
	}
	// A close during a close does nothing at all, and the epoch still
	// advances: every delivery handed out before now is unsettleable either
	// way.
	s.epoch++
}

// moved reports whether every source's generation has advanced, which is how
// "the driver re-attached" becomes an observation.
func moved(before, after []uint32) bool {
	if len(after) == 0 || len(before) != len(after) {
		return true
	}
	for i := range after {
		if after[i] <= before[i] {
			return false
		}
	}
	return true
}

// settlements is the settlements the CLIENT asked for, which is what a
// staleness check reads: a channel close settles what it was holding too, and
// those are the broker's own.
func (s *tagSUT) settlements() []rabbitfake.Settlement {
	all := s.broker.Settlements()
	out := make([]rabbitfake.Settlement, 0, len(all))
	for i := range all {
		if all[i].Kind != rabbitfake.SettleChannelClosed {
			out = append(out, all[i])
		}
	}
	return out
}

// finish is the end-of-script reconciliation.
//
// The per-step checks are where the disqualifier lives; this is the one
// property that can only be read at the end: every settlement the broker
// recorded names a message this script published, so the driver never settled
// a delivery out of thin air.
func (s *tagSUT) finish() {
	published := map[string]struct{}{}
	for i := 1; i <= s.published; i++ {
		published["m"+strconv.Itoa(i)] = struct{}{}
	}
	settled := s.settlements()
	for i := range settled {
		body := string(settled[i].Message.Body)
		if _, ok := published[body]; !ok {
			s.t.Fatalf("the broker recorded a settlement of %q, which this script never published: %+v", body, settled[i])
		}
	}
	if err := s.cons.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
		s.t.Fatalf("closing the consumer: %v", err)
	}
}
