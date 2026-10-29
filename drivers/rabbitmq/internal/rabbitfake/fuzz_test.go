package rabbitfake_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// The pools the script draws names from. They are small on purpose: a fuzzer
// that invented a fresh queue name for every publication would explore a
// topology of singletons, and every interesting case in a broker -- two
// consumers competing, a binding that matches two queues, a dead-letter route
// that points back at its own queue -- needs two operations to name the same
// thing.
var (
	fuzzQueues    = []string{"q0", "q1", "q2", "q3"}
	fuzzExchanges = []string{"", "x0", "x1", "x2"}
	fuzzKeys      = []string{"a", "a.b", "a.b.c", "b", "#", "a.#", "a.*"}
	fuzzKinds     = []string{amqp.ExchangeDirect, amqp.ExchangeTopic, amqp.ExchangeFanout}
)

// fuzzState is the system under test plus the bookkeeping the invariants are
// checked against.
//
// There is no reference MODEL here, which is the difference between this target
// and most of the op-script targets in this repository. The fake IS the
// reference implementation of a RabbitMQ for the driver's tests, so there is
// nothing above it to differentially compare against; what can be checked is
// the set of statements no AMQP broker may falsify whatever it is asked to do,
// and those are the five in [fuzzState.check].
type fuzzState struct {
	b     *rabbitfake.Broker
	conn  client.Conn
	chans []*fuzzChan

	// published is every message id the script successfully published, so
	// that "no message is lost" has a left-hand side.
	published []string
	nextID    int
}

// fuzzChan is one channel and what the script knows about it.
type fuzzChan struct {
	ch client.Channel
	h  *rabbitfake.ChannelHandle

	confirming  bool
	confs       <-chan amqp.Confirmation
	lastConfirm uint64

	consumers []*fuzzConsumer
	held      []amqp.Delivery

	// globalFloor is how many deliveries the channel was already holding when
	// its global prefetch was last set. A qos does not recall what is already
	// out, so the bound the broker owes is max(prefetch, floor) rather than
	// the prefetch alone -- see [client.Channel.Qos]'s own note.
	globalFloor int
}

// fuzzConsumer is one consumer and the queue it reads.
type fuzzConsumer struct {
	tag        string
	queue      string
	deliveries <-chan amqp.Delivery
	// auto records a `no-ack` consumer, whose deliveries are settled the
	// moment they are handed out. The script must NOT hold one to settle
	// later: the broker has already forgotten the tag, so settling it is a
	// 406 -- correct behaviour, and not what this target is looking for.
	auto   bool
	closed bool
	// lastTag is the last delivery tag THIS consumer was given. One
	// consumer's deliveries arrive in the order the channel issued them; two
	// consumers' do not, because they arrive on two Go channels.
	lastTag uint64
}

// FuzzRabbitFake drives an operation script at the fake and checks the
// statements no AMQP broker may break.
//
// The target exists for the same reason `FuzzFakeStreams` does: this fake is
// the seam every unit test of the RabbitMQ driver runs through, so a defect in
// it is a defect in the driver's whole suite -- and worse than a crash, a
// SILENT one certifies the driver against the fake's own mistake. The five
// invariants are chosen to be things a careless broker model gets wrong rather
// than things that are obviously true:
//
//  1. **No message is delivered twice without being given back first.** Each
//     arrival of a message into a queue is worth one delivery, and each
//     requeue is worth one more. A broker that handed the same message to two
//     consumers, or redelivered it while it was still unsettled, breaks.
//  2. **No message is lost.** Every publication is in a queue, in flight,
//     settled, dead-lettered, held by a delayed exchange, or DROPPED WITH A
//     RECORD. The last one is the reason [rabbitfake.DropReason] exists: a
//     broker may discard a message, and a fake that discarded it silently
//     would make this statement uncheckable.
//  3. **A confirm is issued for every publication on a confirm channel, at
//     most once.** No delivery tag is resolved twice, and the confirmations
//     that reach a listener are in strictly ascending tag order -- which is
//     the LIBRARY's resequencing, not the broker's ordering, and the property
//     the driver's confirm bookkeeping is written against.
//  4. **Delivery tags on one channel are consecutive from 1.** A gap means
//     the broker issued a tag nobody was told about, and a driver keyed on
//     tags would then attribute a settlement to the wrong delivery.
//  5. **The unacknowledged count never exceeds the prefetch.** Per CONSUMER,
//     which is what `basic.qos` bounds.
//
// It also asserts what every fuzz target asserts: it never panics, whatever
// the bytes say. The fake reads arguments out of tables the caller controls --
// a TTL, a maximum length, a priority, a delay -- and an index or a conversion
// computed from one of those is exactly where a fake crashes on input a
// hand-written case never produces.
func FuzzRabbitFake(f *testing.F) {
	for _, seed := range fuzzSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		st := newFuzzState(t)
		defer st.close()
		rabbitScript.Run(t, data, st)
		st.check(t)
	})
}

// rabbitScript is the operation script. It is a package-level value so that
// the seed corpus can be authored against the same step table the fuzzer
// decodes with; a seed written against a different table selects different
// operations and stops being a seed for anything.
var rabbitScript = fuzzutil.Script[*fuzzState]{
	MaxSteps: 96,
	Check:    func(t testing.TB, s *fuzzState) { s.check(t) },
	Steps: []fuzzutil.Step[*fuzzState]{
		{Name: "declare-queue", Weight: 4, Run: stepDeclareQueue},
		{Name: "declare-exchange", Weight: 2, Run: stepDeclareExchange},
		{Name: "bind", Weight: 4, Run: stepBind},
		{Name: "publish", Weight: 8, Run: stepPublish},
		{Name: "confirm-select", Weight: 2, Run: stepConfirmSelect},
		{Name: "qos", Weight: 2, Run: stepQos},
		{Name: "consume", Weight: 3, Run: stepConsume},
		{Name: "receive", Weight: 6, Run: stepReceive},
		{Name: "settle", Weight: 6, Run: stepSettle},
		{Name: "drain-confirms", Weight: 3, Run: stepDrainConfirms},
		{Name: "advance", Weight: 2, Run: stepAdvance},
		{Name: "purge", Weight: 1, Run: stepPurge},
		{Name: "delete-queue", Weight: 1, Run: stepDeleteQueue},
		{Name: "cancel", Weight: 1, Run: stepCancel},
		{Name: "close-channel", Weight: 1, Run: stepCloseChannel},
		{Name: "open-channel", Weight: 2, Run: stepOpenChannel},
		{Name: "confirm-script", Weight: 2, Run: stepConfirmScript},
	},
}

// newFuzzState opens a broker and one channel.
func newFuzzState(t testing.TB) *fuzzState {
	b := rabbitfake.New(rabbitfake.Config{DelayedMessagePlugin: true})
	conn, err := b.Dialer().Dial(context.Background(), []string{"amqp://fake/"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("dialling the fake: %v", err)
	}
	st := &fuzzState{b: b, conn: conn}
	st.open()
	return st
}

// close tears the broker down.
func (s *fuzzState) close() {
	_ = s.conn.Close()
	_ = s.b.Close()
}

// open adds a channel.
func (s *fuzzState) open() *fuzzChan {
	ch, err := s.conn.Channel()
	if err != nil {
		return nil
	}
	fc := &fuzzChan{ch: ch, h: s.b.LastChannel()}
	s.chans = append(s.chans, fc)
	return fc
}

// pick chooses a channel that is still open, opening one when none is.
//
// The reopening is what keeps the script productive. A mismatched re-declare
// or a settlement of an unknown tag closes the channel, and a script that then
// no-opped for the rest of its steps would spend most of its input doing
// nothing -- so the fuzzer would explore the first channel exception over and
// over and never reach what happens after one.
func (s *fuzzState) pick(r *fuzzutil.Reader) *fuzzChan {
	live := make([]*fuzzChan, 0, len(s.chans))
	for _, fc := range s.chans {
		if !fc.ch.IsClosed() {
			live = append(live, fc)
		}
	}
	if len(live) == 0 {
		return s.open()
	}
	return live[r.Choice(len(live))]
}

// usable reports that a channel handle exists, which it does for every channel
// the fake opened; a nil one would mean the connection had gone.
func (fc *fuzzChan) usable() bool { return fc != nil && fc.h != nil }

// nextBody is the next message id, which travels in a header so that every
// observation can be tied back to the publication that made it.
func (s *fuzzState) nextBody() string {
	s.nextID++
	return "m" + strconv.Itoa(s.nextID)
}

// idOf reads a message id back out of a header table.
func idOf(h amqp.Table) string {
	id, _ := h["fz"].(string)
	return id
}

func stepDeclareQueue(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	name := fuzzQueues[r.Choice(len(fuzzQueues))]
	args := amqp.Table{}
	if r.Bool() {
		args[amqp.QueueTypeArg] = amqp.QueueTypeQuorum
	}
	if r.Bool() {
		args[amqp.QueueMessageTTLArg] = int64(r.Int(0, 200))
	}
	if r.Bool() {
		args[rabbitfake.DeadLetterExchangeArg] = fuzzExchanges[r.Choice(len(fuzzExchanges))]
		if r.Bool() {
			args[rabbitfake.DeadLetterRoutingKeyArg] = fuzzKeys[r.Choice(len(fuzzKeys))]
		}
	}
	if r.Bool() {
		args[amqp.QueueMaxLenArg] = int64(r.Int(0, 3))
	}
	if r.Bool() {
		args["x-max-priority"] = int64(r.Int(0, 9))
	}
	// The declaration's own error is not asserted on: a re-declare that
	// disagrees with an existing queue is SUPPOSED to be a 406 that closes the
	// channel, and that is one of the states worth reaching.
	_, _ = fc.ch.QueueDeclare(name, r.Bool(), r.Bool(), false, false, args)
}

func stepDeclareExchange(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	name := fuzzExchanges[r.Choice(len(fuzzExchanges))]
	kind := fuzzKinds[r.Choice(len(fuzzKinds))]
	args := amqp.Table{}
	if r.Bool() {
		kind = rabbitfake.ExchangeDelayed
		args[rabbitfake.DelayedTypeArg] = fuzzKinds[r.Choice(len(fuzzKinds))]
	}
	_ = fc.ch.ExchangeDeclare(name, kind, r.Bool(), false, false, false, args)
}

func stepBind(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	_ = fc.ch.QueueBind(
		fuzzQueues[r.Choice(len(fuzzQueues))],
		fuzzKeys[r.Choice(len(fuzzKeys))],
		fuzzExchanges[r.Choice(len(fuzzExchanges))],
		false, nil,
	)
}

func stepPublish(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	id := s.nextBody()
	msg := amqp.Publishing{
		Headers:  amqp.Table{"fz": id},
		Body:     []byte(id),
		Priority: uint8(r.Int(0, 9)),
	}
	if r.Bool() {
		msg.Expiration = strconv.Itoa(r.Int(0, 200))
	}
	if r.Bool() {
		msg.Headers[rabbitfake.DelayHeader] = int64(r.Int(0, 200))
	}
	exchange := fuzzExchanges[r.Choice(len(fuzzExchanges))]
	key := fuzzKeys[r.Choice(len(fuzzKeys))]
	if r.Bool() {
		// The default exchange routes by queue name, so half the publications
		// aim at a queue rather than at a pattern; otherwise almost everything
		// would be unroutable and the settlement half of the script would
		// never run.
		exchange, key = "", fuzzQueues[r.Choice(len(fuzzQueues))]
	}
	if err := fc.ch.PublishWithContext(context.Background(), exchange, key, r.Bool(), false, msg); err == nil {
		s.published = append(s.published, id)
	}
}

func stepConfirmSelect(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() || fc.confirming {
		return
	}
	if err := fc.ch.Confirm(false); err != nil {
		return
	}
	fc.confirming = true
	fc.confs = fc.ch.NotifyPublish(make(chan amqp.Confirmation, 256))
}

func stepQos(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	count, global := r.Int(0, 4), r.Bool()
	if global {
		fc.globalFloor = fc.h.Unacked()
	}
	_ = fc.ch.Qos(count, 0, global)
}

func stepConsume(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	queue := fuzzQueues[r.Choice(len(fuzzQueues))]
	tag := "c" + strconv.Itoa(len(fc.consumers)) + "-" + strconv.Itoa(fc.h.ID())
	// The background context, deliberately: a cancellable one would start a
	// watcher goroutine per consumer and the target opens thousands of them.
	auto := r.Bool()
	deliveries, err := fc.ch.ConsumeWithContext(context.Background(), queue, tag, auto, false, false, false, nil)
	if err != nil {
		return
	}
	fc.consumers = append(fc.consumers, &fuzzConsumer{
		tag: tag, queue: queue, deliveries: deliveries, auto: auto,
	})
}

func stepReceive(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() || len(fc.consumers) == 0 {
		return
	}
	// A barrier first, so that "nothing arrived" means the broker sent
	// nothing rather than that the dispatcher had not got there yet.
	s.b.Sync()
	con := fc.consumers[r.Choice(len(fc.consumers))]
	if con.closed {
		return
	}
	select {
	case d, ok := <-con.deliveries:
		if !ok {
			con.closed = true
			return
		}
		// Invariant 4's per-consumer half: one consumer's deliveries arrive
		// in the order the channel issued them, and no tag ever exceeds what
		// the channel says it has issued. The consecutive-from-1 half is
		// checked against the broker in [fuzzState.checkDeliveryTags],
		// because two consumers on one channel receive interleaved tags on
		// two Go channels and the read order says nothing about the issue
		// order.
		if d.DeliveryTag <= con.lastTag {
			t.Fatalf("consumer %q was given delivery tag %d after %d: one consumer's "+
				"deliveries arrive in the order the channel issued them",
				con.tag, d.DeliveryTag, con.lastTag)
		}
		if issued := fc.h.DeliveryTagsIssued(); d.DeliveryTag > issued {
			t.Fatalf("consumer %q was given delivery tag %d but channel %d has issued only %d",
				con.tag, d.DeliveryTag, fc.h.ID(), issued)
		}
		con.lastTag = d.DeliveryTag
		if !con.auto {
			fc.held = append(fc.held, d)
		}
	default:
	}
}

func stepSettle(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() || len(fc.held) == 0 {
		return
	}
	i := r.Choice(len(fc.held))
	d := fc.held[i]
	fc.held = append(fc.held[:i], fc.held[i+1:]...)
	switch r.Choice(3) {
	case 0:
		_ = fc.ch.Ack(d.DeliveryTag, false)
	case 1:
		_ = fc.ch.Nack(d.DeliveryTag, false, r.Bool())
	default:
		_ = fc.ch.Reject(d.DeliveryTag, r.Bool())
	}
}

func stepDrainConfirms(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() || fc.confs == nil {
		return
	}
	s.b.Sync()
	for {
		select {
		case c, ok := <-fc.confs:
			if !ok {
				fc.confs = nil
				return
			}
			// Invariant 3's ordering half: what reaches a listener is in
			// strictly ascending tag order, whatever order the broker's
			// frames arrived in. That is the library's sequencer and it is
			// the property a driver's bookkeeping is written against.
			if c.DeliveryTag <= fc.lastConfirm {
				t.Fatalf("channel %d confirmed tag %d after tag %d: confirmations reach a "+
					"listener resequenced into tag order", fc.h.ID(), c.DeliveryTag, fc.lastConfirm)
			}
			fc.lastConfirm = c.DeliveryTag
		default:
			return
		}
	}
}

func stepAdvance(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	s.b.Advance(time.Duration(r.Int(0, 300)) * time.Millisecond)
}

func stepPurge(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	_, _ = fc.ch.QueuePurge(fuzzQueues[r.Choice(len(fuzzQueues))], false)
}

func stepDeleteQueue(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	_, _ = fc.ch.QueueDelete(fuzzQueues[r.Choice(len(fuzzQueues))], r.Bool(), r.Bool(), false)
}

func stepCancel(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() || len(fc.consumers) == 0 {
		return
	}
	con := fc.consumers[r.Choice(len(fc.consumers))]
	if r.Bool() {
		_ = fc.ch.Cancel(con.tag, false)
	} else {
		fc.h.CancelConsumer(con.tag)
	}
}

func stepCloseChannel(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	if r.Bool() {
		_ = fc.ch.Close()
	} else {
		fc.h.CloseWith(&amqp.Error{
			Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - fuzz", Server: true, Recover: true,
		})
	}
	fc.held = nil
}

func stepOpenChannel(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	if len(s.chans) >= 4 {
		return
	}
	s.open()
}

func stepConfirmScript(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
	fc := s.pick(r)
	if !fc.usable() {
		return
	}
	styles := []rabbitfake.ConfirmStyle{
		rabbitfake.ConfirmPerTag,
		rabbitfake.ConfirmCumulative,
		rabbitfake.ConfirmReversed,
	}
	fc.h.SetConfirmScript(rabbitfake.ConfirmScript{
		Style: styles[r.Choice(len(styles))],
		Batch: r.Int(1, 4),
	})
}

// check asserts the invariants that hold whatever the script did.
func (s *fuzzState) check(t testing.TB) {
	s.b.Sync()
	s.checkNoDoubleDelivery(t)
	s.checkNothingLost(t)
	s.checkConfirmsResolvedOnce(t)
	s.checkDeliveryTags(t)
	s.checkPrefetch(t)
}

// checkDeliveryTags is invariant 4, asked of the broker rather than of the
// order the script happened to read its consumers in.
//
// Every tag a channel has issued is accounted for exactly once: it is either
// unsettled or it has a settlement, and together they have to be 1..issued
// with no gap and no repeat. A gap means the broker issued a tag it told
// nobody about; a repeat means two deliveries share a tag, and a driver keyed
// on tags would then settle the wrong one.
func (s *fuzzState) checkDeliveryTags(t testing.TB) {
	for _, fc := range s.chans {
		issued := fc.h.DeliveryTagsIssued()
		seen := make(map[uint64]int, issued)
		for _, tag := range fc.h.UnackedTags() {
			seen[tag]++
		}
		settlements := s.b.Settlements()
		for i := range settlements {
			if settlements[i].Channel == fc.h.ID() {
				seen[settlements[i].Tag]++
			}
		}
		for tag := uint64(1); tag <= issued; tag++ {
			switch seen[tag] {
			case 1:
			case 0:
				t.Fatalf("channel %d issued %d delivery tags and tag %d is neither unsettled "+
					"nor settled: a tag the broker issued and told nobody about",
					fc.h.ID(), issued, tag)
			default:
				t.Fatalf("channel %d accounts for delivery tag %d %d times: two deliveries "+
					"share a tag", fc.h.ID(), tag, seen[tag])
			}
			delete(seen, tag)
		}
		for tag := range seen {
			t.Fatalf("channel %d accounts for delivery tag %d, which is above the %d it has "+
				"issued", fc.h.ID(), tag, issued)
		}
	}
}

// checkNoDoubleDelivery is invariant 1.
//
// Each arrival of a message into a queue is worth one delivery and each
// requeue is worth one more, so the deliveries of one message out of one queue
// can never exceed the sum. A broker that handed one message to two consumers,
// or redelivered one that was still unsettled, breaks it -- and so does one
// whose prefetch accounting lets the pump hand out the same ready message
// twice.
func (s *fuzzState) checkNoDoubleDelivery(t testing.TB) {
	type key struct {
		queue, id string
	}
	arrivals := map[key]int{}
	// ArrivalQueues rather than Queues: the arrival log outlives the queue,
	// and it has to. A queue that was deleted still has settlements recorded
	// against it, so counting arrivals only for the queues that still exist
	// makes every delivery out of a deleted queue look like one the broker
	// invented -- which is what this check reported the first time it ran.
	for _, name := range s.b.ArrivalQueues() {
		pubs := s.b.Publications(name)
		for i := range pubs {
			arrivals[key{name, idOf(pubs[i].Headers)}]++
		}
	}
	requeues := map[key]int{}
	delivered := map[key]int{}
	settlements := s.b.Settlements()
	for i := range settlements {
		k := key{settlements[i].Queue, idOf(settlements[i].Message.Headers)}
		delivered[k]++
		if settlements[i].Requeue {
			requeues[k]++
		}
	}
	inflight := s.b.InFlight()
	for i := range inflight {
		delivered[key{inflight[i].Queue, idOf(inflight[i].Message.Headers)}]++
	}
	for k, n := range delivered {
		if allowed := arrivals[k] + requeues[k]; n > allowed {
			t.Fatalf("%q was delivered %d times out of queue %q, which arrived %d times and "+
				"was requeued %d times: a message may not be delivered again until it has "+
				"been given back", k.id, n, k.queue, arrivals[k], requeues[k])
		}
	}
}

// checkNothingLost is invariant 2.
//
// Every publication the broker accepted is somewhere: ready in a queue, in
// flight to a consumer, settled, dead-lettered, held by a delayed exchange, or
// dropped with a recorded reason. A message that is in none of those was lost,
// which is the one failure a queue may never have.
func (s *fuzzState) checkNothingLost(t testing.TB) {
	seen := map[string]struct{}{}
	note := func(h amqp.Table) {
		if id := idOf(h); id != "" {
			seen[id] = struct{}{}
		}
	}
	for _, q := range s.b.Queues() {
		ready := s.b.Ready(q.Name)
		for i := range ready {
			note(ready[i].Headers)
		}
	}
	inflight := s.b.InFlight()
	for i := range inflight {
		note(inflight[i].Message.Headers)
	}
	settlements := s.b.Settlements()
	for i := range settlements {
		note(settlements[i].Message.Headers)
	}
	dead := s.b.DeadLettered()
	for i := range dead {
		note(dead[i].Message.Headers)
	}
	dropped := s.b.Dropped()
	for i := range dropped {
		note(dropped[i].Message.Headers)
	}
	delayed := s.b.Delayed()
	for i := range delayed {
		note(delayed[i].Headers)
	}
	for _, id := range s.published {
		if _, ok := seen[id]; !ok {
			t.Fatalf("%q was published and is in no queue, in flight, settled, dead-lettered, "+
				"held or dropped: a broker may discard a message, but never without saying so", id)
		}
	}
}

// checkConfirmsResolvedOnce is invariant 3's exactly-once half.
//
// Every publication on a confirm channel is resolved at most once -- acked,
// nacked, withdrawn or resolved by a close -- and a channel that has CLOSED has
// resolved all of them, because `deferredConfirmations.Close` leaves nothing
// outstanding. A tag resolved twice would mean a driver is told two different
// things about one message.
func (s *fuzzState) checkConfirmsResolvedOnce(t testing.TB) {
	for _, fc := range s.chans {
		if !fc.confirming {
			continue
		}
		seen := map[uint64]rabbitfake.ConfirmOutcome{}
		for _, res := range fc.h.ConfirmResolutions() {
			if res.Outcome == rabbitfake.OutcomeUnpublished {
				// A withdrawn tag was handed back and will be reissued to
				// the next publication, so it is neither a duplicate nor a
				// tag that was never issued.
				delete(seen, res.Tag)
				continue
			}
			if prev, dup := seen[res.Tag]; dup {
				t.Fatalf("channel %d resolved delivery tag %d twice, as %v and then as %v",
					fc.h.ID(), res.Tag, prev, res.Outcome)
			}
			seen[res.Tag] = res.Outcome
		}
		published := fc.h.Published()
		for tag := range seen {
			if tag == 0 || tag > published {
				t.Fatalf("channel %d resolved delivery tag %d, which it never issued (it has "+
					"published %d)", fc.h.ID(), tag, published)
			}
		}
		if !fc.ch.IsClosed() {
			continue
		}
		for tag := uint64(1); tag <= published; tag++ {
			if _, ok := seen[tag]; !ok {
				t.Fatalf("channel %d closed with delivery tag %d unresolved: a close resolves "+
					"every outstanding publication, which is what makes it ambiguous rather "+
					"than pending", fc.h.ID(), tag)
			}
		}
	}
}

// checkPrefetch is invariant 5. It asserts BOTH of the readings, because
// RabbitMQ enforces both independently: the per-consumer one over each
// consumer's own snapshot, and the channel-wide one over the channel's total.
//
// The per-consumer assertion needs no floor, where the channel-wide one does.
// A consumer's bound is the snapshot its `basic.consume` took and nothing
// moves it afterwards -- a later qos(global=false) is for NEW consumers and a
// later qos(global=true) sets the other reading -- so it can never be lowered
// under deliveries that are already out. The channel-wide reading can be, and
// [fuzzChan.globalFloor] is what that costs.
func (s *fuzzState) checkPrefetch(t testing.TB) {
	for _, fc := range s.chans {
		_, chCount := fc.h.Prefetch()
		total := 0
		for _, con := range fc.h.ConsumerStates() {
			total += con.Outstanding
			if con.AutoAck || con.Prefetch == 0 {
				continue
			}
			if con.Outstanding > con.Prefetch {
				t.Fatalf("consumer %q holds %d unacknowledged deliveries with a prefetch of %d",
					con.Tag, con.Outstanding, con.Prefetch)
			}
		}
		bound := chCount
		if fc.globalFloor > bound {
			bound = fc.globalFloor
		}
		if chCount > 0 && total > bound {
			t.Fatalf("channel %d holds %d unacknowledged deliveries with a global prefetch of "+
				"%d (it was already holding %d when the prefetch was set)",
				fc.h.ID(), total, chCount, fc.globalFloor)
		}
	}
}

// fuzzSeeds are the readable seeds, authored against the same step table the
// fuzzer decodes with.
//
// They exist so that every committed input reaches the paths a corpus file
// cannot be read for: the confirm scripts, a TTL that elapses on a clock
// advance, a channel close with a publication outstanding, and a settlement of
// a delivery a `no-ack` consumer received. `make fuzz-seeds` runs all of them
// under -race in every gate, so they are the part of this target that protects
// the package on a branch nobody fuzzed.
func fuzzSeeds(tb testing.TB) [][]byte {
	seeds := make([][]byte, 0, 6)

	// A queue, a publication, a consumer, a delivery and an acknowledgement:
	// the shortest script that settles anything at all.
	w := fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 0, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 0, false, false, true, 0)
	rabbitScript.WriteStep(tb, w, "qos")
	w.Choice(1, 0)
	w.Int(0, 4, 1)
	w.Bool(false)
	rabbitScript.WriteStep(tb, w, "consume")
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), 0)
	w.Bool(false)
	rabbitScript.WriteStep(tb, w, "receive")
	w.Choice(1, 0)
	w.Choice(1, 0)
	rabbitScript.WriteStep(tb, w, "settle")
	w.Choice(1, 0)
	w.Choice(1, 0)
	w.Choice(3, 0)
	seeds = append(seeds, w.Data())

	// Confirm mode with a cumulative script: three publications answered by
	// one frame, which the library has to expand.
	w = fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 0, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "confirm-select")
	w.Choice(1, 0)
	rabbitScript.WriteStep(tb, w, "confirm-script")
	w.Choice(1, 0)
	w.Choice(3, 1)
	w.Int(1, 4, 3)
	for range 3 {
		rabbitScript.WriteStep(tb, w, "publish")
		writePublish(w, 0, false, false, true, 0)
	}
	rabbitScript.WriteStep(tb, w, "drain-confirms")
	w.Choice(1, 0)
	seeds = append(seeds, w.Data())

	// A TTL that elapses, dead-lettering into a topic exchange that is bound
	// back to another queue: the specification's `ttldlx` shape.
	w = fuzzutil.NewWriter()
	writeExchangeDecl(tb, w, 1, 1)
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDeclTTL(w, 0, 10, 1)
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 1, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "bind")
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), 1)
	w.Choice(len(fuzzKeys), 4)
	w.Choice(len(fuzzExchanges), 1)
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 0, false, false, true, 0)
	rabbitScript.WriteStep(tb, w, "advance")
	w.Int(0, 300, 50)
	seeds = append(seeds, w.Data())

	// A channel closed with a publication outstanding, which is.
	w = fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 0, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "confirm-select")
	w.Choice(1, 0)
	rabbitScript.WriteStep(tb, w, "confirm-script")
	w.Choice(1, 0)
	w.Choice(3, 1)
	w.Int(1, 4, 4)
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 0, false, false, true, 0)
	rabbitScript.WriteStep(tb, w, "close-channel")
	w.Choice(1, 0)
	w.Bool(false)
	seeds = append(seeds, w.Data())

	// A `no-ack` consumer, whose deliveries are settled the moment they are
	// handed out and must never be settled again.
	w = fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 2, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 0, false, false, true, 2)
	rabbitScript.WriteStep(tb, w, "consume")
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), 2)
	w.Bool(true)
	rabbitScript.WriteStep(tb, w, "receive")
	w.Choice(1, 0)
	w.Choice(1, 0)
	seeds = append(seeds, w.Data())

	// A prefetch of one against a backlog of three, which is the only shape
	// in which invariant 5 can be breached at all: with one message ready a
	// broker that ignored the prefetch entirely would still look correct.
	w = fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "declare-queue")
	writeQueueDecl(w, 0, false, false, false, false, false)
	rabbitScript.WriteStep(tb, w, "qos")
	w.Choice(1, 0)
	w.Int(0, 4, 1)
	w.Bool(false)
	rabbitScript.WriteStep(tb, w, "consume")
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), 0)
	w.Bool(false)
	for range 3 {
		rabbitScript.WriteStep(tb, w, "publish")
		writePublish(w, 0, false, false, true, 0)
	}
	for range 3 {
		rabbitScript.WriteStep(tb, w, "receive")
		w.Choice(1, 0)
		w.Choice(1, 0)
	}
	seeds = append(seeds, w.Data())

	// An unroutable mandatory publication through a topic exchange nothing is
	// bound to, which produces a return followed by an ack.
	w = fuzzutil.NewWriter()
	writeExchangeDecl(tb, w, 2, 1)
	rabbitScript.WriteStep(tb, w, "confirm-select")
	w.Choice(1, 0)
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 2, true, false, false, 0)
	rabbitScript.WriteStep(tb, w, "drain-confirms")
	w.Choice(1, 0)
	seeds = append(seeds, w.Data())

	// A publication to an exchange nobody declared, which the broker answers
	// with a 404 that closes the channel -- and which discards the message
	// with no reply to the publisher at all. The fuzzer found this in under a
	// second the first time it ran, because the fake was discarding the
	// message without recording it: see [rabbitfake.DropRefused].
	w = fuzzutil.NewWriter()
	rabbitScript.WriteStep(tb, w, "publish")
	writePublish(w, 2, false, false, false, 0)
	seeds = append(seeds, w.Data())

	return seeds
}

// writeExchangeDecl encodes a declare-exchange step for a durable exchange of
// a core AMQP type.
func writeExchangeDecl(tb testing.TB, w *fuzzutil.Writer, exchange, kind int) {
	rabbitScript.WriteStep(tb, w, "declare-exchange")
	w.Choice(1, 0)
	w.Choice(len(fuzzExchanges), exchange)
	w.Choice(len(fuzzKinds), kind)
	w.Bool(false) // not a delayed-message exchange
	w.Bool(true)  // durable
}

// writeQueueDecl encodes a declare-queue step with no interpreted arguments.
func writeQueueDecl(w *fuzzutil.Writer, queue int, quorum, ttl, dlx, maxLen, priority bool) {
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), queue)
	w.Bool(quorum)
	w.Bool(ttl)
	w.Bool(dlx)
	w.Bool(maxLen)
	w.Bool(priority)
	w.Bool(true)  // durable
	w.Bool(false) // autoDelete
}

// writeQueueDeclTTL encodes a declare-queue step with a TTL and a dead-letter
// exchange, which is the delay-queue shape.
func writeQueueDeclTTL(w *fuzzutil.Writer, queue, ttlMillis, dlx int) {
	w.Choice(1, 0)
	w.Choice(len(fuzzQueues), queue)
	w.Bool(false) // quorum
	w.Bool(true)  // ttl
	w.Int(0, 200, ttlMillis)
	w.Bool(true) // dlx
	w.Choice(len(fuzzExchanges), dlx)
	w.Bool(false) // no dead-letter routing key: the message keeps its own
	w.Bool(false) // maxLen
	w.Bool(false) // priority
	w.Bool(true)  // durable
	w.Bool(false) // autoDelete
}

// writePublish encodes a publish step.
func writePublish(w *fuzzutil.Writer, exchange int, mandatory, delayed, toQueue bool, queue int) {
	w.Choice(1, 0)
	w.Int(0, 9, 0) // priority
	w.Bool(false)  // no per-message expiration
	w.Bool(delayed)
	if delayed {
		w.Int(0, 200, 50)
	}
	w.Choice(len(fuzzExchanges), exchange)
	w.Choice(len(fuzzKeys), 0)
	w.Bool(toQueue)
	if toQueue {
		w.Choice(len(fuzzQueues), queue)
	}
	w.Bool(mandatory)
}
