package pulsar_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// TestAckIsIdempotentAndTerminal is the specification's "a program that settles twice has a
// bug": exactly one terminal settlement reaches the driver and a second is
// refused.
func TestAckIsIdempotentAndTerminal(t *testing.T) {
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "settle", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "settle", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("once")})
	d := fetchOne(t, src)
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a second Ack reported %v (code %v), want ErrAlreadySettled", err, mq.CodeOf(err))
	}
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{}); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a Nack after an Ack reported %v, want ErrAlreadySettled", err)
	}
	if st := server.Stats("persistent://public/default/settle", "workers"); st.Acked != 1 || st.Nacked != 0 {
		t.Fatalf("the broker saw %d acknowledgement(s) and %d negative one(s) for one message",
			st.Acked, st.Nacked)
	}
}

// TestConcurrentSettlementsSerialise is the specification's "exactly one settlement wins",
// under -race and from eight goroutines.
func TestConcurrentSettlementsSerialise(t *testing.T) {
	b, _ := newRig(t, nil)
	src := sourceOf(t, b, "raced", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "raced", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("raced")})
	d := fetchOne(t, src)

	const racers = 8
	errs := make([]error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				errs[i] = src.Settler().Ack(context.Background(), &d)
				return
			}
			errs[i] = src.Settler().Nack(context.Background(), &d, mq.NackOptions{})
		}()
	}
	close(start)
	wg.Wait()
	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, mq.ErrAlreadySettled), errors.Is(err, mq.ErrLeaseLost),
			errors.Is(err, mq.ErrSettlementInProgress):
		default:
			t.Fatalf("racer %d got %v (code %v), which is not one of the settlement outcomes",
				i, err, mq.CodeOf(err))
		}
	}
	if winners != 1 {
		t.Fatalf("%d of %d concurrent settlements succeeded, want exactly one", winners, racers)
	}
}

// TestStaleSettlementIsRefused is the disqualifier: Pulsar redelivers a
// message under the SAME identifier, so a settlement written for an abandoned
// delivery would land on the work that replaced it.
func TestStaleSettlementIsRefused(t *testing.T) {
	const topic = "persistent://public/default/stale"
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "stale", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "stale", mq.MetadataNativeStrict)
	const messages = 4
	for i := range messages {
		publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	first := fetchN(t, src, messages)
	server.Redeliver(topic, "workers")
	second := fetchN(t, src, messages)

	for i := range first {
		if err := src.Settler().Ack(context.Background(), &first[i]); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("the stale Ack of delivery %d answered %v (code %v), want ErrLeaseLost",
				i, err, mq.CodeOf(err))
		}
	}
	for i := range second {
		if err := src.Settler().Ack(context.Background(), &second[i]); err != nil {
			t.Fatalf("the live Ack of delivery %d: %v", i, err)
		}
	}
	st := server.Stats(topic, "workers")
	if st.StaleSettled != 0 {
		t.Fatalf("%d settlement(s) landed on a later delivery than the one that sent them: the specification makes "+
			"\"lets a stale ack settle a later redelivery\" a disqualifier", st.StaleSettled)
	}
	if st.Acked != messages {
		t.Fatalf("the broker acknowledged %d of %d messages", st.Acked, messages)
	}
	if n := b.Stale(); n < messages {
		t.Fatalf("the driver counted %d stale settlements for %d superseded deliveries; a Stale of zero "+
			"under a workload that supersedes deliveries means the check is not running", n, messages)
	}
}

// TestEmulatedLeaseGivesTheMessageBack is the whole point.
//
// Pulsar's broker redelivers an unsettled message when the consumer
// disconnects and on no timer, and pulsar-client-go v0.21 has no client-side
// acknowledgement timeout — so without the driver's own deadline a message
// abandoned by a handler sits unsettled until the process ends.
func TestEmulatedLeaseGivesTheMessageBack(t *testing.T) {
	const topic = "persistent://public/default/leased"
	b, server := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = 2*time.Second, 500*time.Millisecond
	})
	src := sourceOf(t, b, "leased", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "leased", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("abandoned")})
	d := fetchOne(t, src)
	if d.Info.LeaseExpires.IsZero() {
		t.Fatal("the delivery carries no lease")
	}
	// Deliberately unsettled. The sweeper gives it back one guard before the
	// deadline, and the negative acknowledgement's own delay then applies.
	server.Advance(2 * time.Second)
	waitFor(t, "the lease sweeper to give the delivery back", func() bool { return b.LeaseLost() > 0 })
	server.Advance(time.Second)
	again := fetchOne(t, src)
	if string(again.Message.Body) != "abandoned" {
		t.Fatalf("the redelivery carries %q", again.Message.Body)
	}
	if again.Info.Attempt < 2 {
		t.Fatalf("the redelivery reports attempt %d, want at least 2", again.Info.Attempt)
	}
	// And the abandoned delivery's settlement is refused rather than applied
	// to the work that replaced it.
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the expired delivery's Ack answered %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
	if err := src.Settler().Ack(context.Background(), &again); err != nil {
		t.Fatalf("the live Ack: %v", err)
	}
	if n := b.LeaseLost(); n == 0 {
		t.Fatal("the driver's lease sweeper gave nothing back, so the lease is a diagnosis rather than " +
			"a prevention")
	}
	if st := server.Stats(topic, "workers"); st.Nacked == 0 {
		t.Fatal("the sweeper wrote no negative acknowledgement, so the broker was never told to " +
			"redeliver and the message came back only because the model redelivered it")
	}
}

// racingClient is the in-process model with one change to its consumers: the
// first negative acknowledgement runs race before it is written, which is how
// a test puts a settlement between the sweeper collecting the deliveries whose
// lease is running out and the sweeper handling each of them.
type racingClient struct {
	pulsarapi.Client
	race func(pulsar.MessageID)
	// raced is a flag rather than a sync.Once because race settles a
	// delivery, and that settlement writes a negative acknowledgement of
	// its own through this consumer: a Once would deadlock on itself.
	raced atomic.Bool
}

func (c *racingClient) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	cons, err := c.Client.Subscribe(o)
	if err != nil {
		return nil, err
	}
	return &racingConsumer{Consumer: cons, c: c}, nil
}

type racingConsumer struct {
	pulsar.Consumer
	c *racingClient
}

func (r *racingConsumer) NackID(id pulsar.MessageID) {
	if r.c.raced.CompareAndSwap(false, true) {
		r.c.race(id)
	}
	r.Consumer.NackID(id)
}

// TestTheSweeperCountsOnlyWhatItGaveBack is a delivery the sweeper collected
// as expiring whose own late settlement got to it first. The settlement is
// refused, gives the message back and counts it as stale; the sweeper used to
// count it as a lost lease as well, before finding it had nothing left to do.
func TestTheSweeperCountsOnlyWhatItGaveBack(t *testing.T) {
	server := pulsarfake.New()
	cl := &racingClient{Client: server.Client()}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := sourceOf(t, b, "racing", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "racing", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("one")})
	publish(t, snk, &mq.Message{Body: []byte("two")})
	ds := fetchN(t, src, 2)
	// Whichever delivery the sweeper gives back first, the other one is
	// settled by its handler before the sweeper reaches it.
	cl.race = func(id pulsar.MessageID) {
		for i := range ds {
			if ds[i].Native.(pulsar.Message).ID().String() != id.String() {
				_ = src.Settler().Ack(context.Background(), &ds[i])
			}
		}
	}
	server.Advance(9500 * time.Millisecond)
	waitFor(t, "both deliveries to be given back", func() bool {
		return server.Stats("persistent://public/default/racing", "workers").Nacked == 2
	})
	// Close waits for the sweeper, so the counts below are final.
	_ = src.Close()
	if lost, stale := b.LeaseLost(), b.Stale(); lost != 1 || stale != 1 {
		t.Fatalf("the sweeper gave one delivery back and a late settlement the other, but LeaseLost is %d "+
			"and Stale is %d, want 1 and 1", lost, stale)
	}
}

// stalledSweep is the model's clock with a sweeper that never runs, so that a
// settlement is what meets an expired lease first.
type stalledSweep struct{ *pulsarfake.Server }

// NewTimer returns a timer that never fires.
func (stalledSweep) NewTimer(time.Duration) mq.Timer { return stalledTimer{} }

// stalledTimer is a timer that never fires.
type stalledTimer struct{}

func (stalledTimer) C() <-chan time.Time      { return nil }
func (stalledTimer) Stop() bool               { return true }
func (stalledTimer) Reset(time.Duration) bool { return false }

// TestSettlementPastTheGuardGivesTheMessageBack is the case the sweeper does
// not cover. A settlement that arrives after the guard marks the delivery lost
// and is refused; the sweeper gives back only an ACTIVE delivery, so if the
// settlement did not give it back nothing did, and the message sat unsettled
// until the consumer disconnected.
func TestSettlementPastTheGuardGivesTheMessageBack(t *testing.T) {
	const topic = "persistent://public/default/late"
	b, server := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = 2*time.Second, 500*time.Millisecond
		if s, ok := c.Clock.(*pulsarfake.Server); ok {
			c.Clock = stalledSweep{s}
		}
	})
	src := sourceOf(t, b, "late", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "late", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("late")})
	d := fetchOne(t, src)
	server.Advance(1600 * time.Millisecond)
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack past the guard answered %v (code %v), want ErrLeaseLost", err, mq.CodeOf(err))
	}
	if st := server.Stats(topic, "workers"); st.Nacked == 0 {
		t.Fatal("the refused settlement gave nothing back, so the broker was never told to redeliver")
	}
	server.Advance(time.Second)
	if again := fetchOne(t, src); string(again.Message.Body) != "late" {
		t.Fatalf("the redelivery carries %q", again.Message.Body)
	}
}

// TestExtendMovesTheLocalDeadline is the emulated lease's other half, and its
// honest limit: the extension buys time against the DRIVER's sweeper and
// nothing else.
func TestExtendMovesTheLocalDeadline(t *testing.T) {
	b, server := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = 2*time.Second, 500*time.Millisecond
	})
	src := sourceOf(t, b, "extended", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "extended", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("slow")})
	d := fetchOne(t, src)
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler is not a DeadlineExtender, and this client grants a lease")
	}
	if err := ext.Extend(context.Background(), &d, 0); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Extend(0) reported %v (code %v), want InvalidArgument", err, mq.CodeOf(err))
	}
	if err := ext.Extend(context.Background(), &d, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	// Past the original deadline, and the delivery is still ours.
	server.Advance(5 * time.Second)
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("an extended delivery's Ack: %v", err)
	}
	// And on a client with no lease, there is nothing to extend.
	off, _ := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = 0, 0
		c.DisableAckTimeout = true
	})
	osrc := sourceOf(t, off, "noext", "workers", mq.MetadataNativeStrict, 4)
	osnk := sinkOf(t, off, "noext", mq.MetadataNativeStrict)
	publish(t, osnk, &mq.Message{Body: []byte("x")})
	od := fetchOne(t, osrc)
	oext, ok := osrc.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler stopped being a DeadlineExtender, which the core asserts once at open")
	}
	if err := oext.Extend(context.Background(), &od, time.Minute); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("extending a delivery with no lease reported %v, want ErrUnsupported", err)
	}
	_ = osrc.Settler().Ack(context.Background(), &od)
}

// TestExtendNeverShortensTheLease is an extension shorter than what is left of
// the lease. DeadlineExtender moves the deadline by at least by, and Extend
// used to store now+by whatever the deadline was, so an Extend of 1.5s on a
// fresh 10s lease brought it inside the guard and the next settlement was
// refused with ErrLeaseLost.
func TestExtendNeverShortensTheLease(t *testing.T) {
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "shorten", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "shorten", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})
	d := fetchOne(t, src)
	ext := src.Settler().(mq.DeadlineExtender)
	if err := ext.Extend(context.Background(), &d, 1500*time.Millisecond); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	server.Advance(600 * time.Millisecond)
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("the Ack 0.6s into a 10s lease extended by 1.5s answered %v (code %v), want nil",
			err, mq.CodeOf(err))
	}
}

// TestADeliveryIsVouchedForUntilTheLeaseItReports is a settlement just before
// DeliveryInfo.LeaseExpires, and one at it.
//
// The driver stops vouching for a delivery one LeaseGuard before its
// AckTimeout runs out, and it used to report the whole AckTimeout as
// LeaseExpires: a handler that measured its work against the lease it was
// given had its settlement refused, and the message given back, for the last
// LeaseGuard of it.
func TestADeliveryIsVouchedForUntilTheLeaseItReports(t *testing.T) {
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "vouched", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "vouched", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("in time")})
	publish(t, snk, &mq.Message{Body: []byte("too late")})
	ds := fetchN(t, src, 2)
	server.Advance(ds[0].Info.LeaseExpires.Sub(server.Now()) - time.Millisecond)
	if err := src.Settler().Ack(context.Background(), &ds[0]); err != nil {
		t.Fatalf("the Ack 1ms before the LeaseExpires the delivery reports answered %v (code %v), want nil",
			err, mq.CodeOf(err))
	}
	server.Advance(ds[1].Info.LeaseExpires.Sub(server.Now()))
	if err := src.Settler().Ack(context.Background(), &ds[1]); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack at the LeaseExpires the delivery reports answered %v (code %v), want ErrLeaseLost",
			err, mq.CodeOf(err))
	}
}

// TestALeaseSchedulerWhoseLeadIsInsideTheGuardKeepsTheDelivery is the
// extension the core's lease scheduler sends (mq.WithLeaseExtension): lead
// before the deadline, asking for LeaseExpires-ReceivedAt again, and the next
// one lead before the deadline that extension set.
//
// With the whole AckTimeout reported as LeaseExpires and the driver acting one
// LeaseGuard earlier, a lead no longer than the guard had every extension
// refused with ErrLeaseLost and the delivery given back while the runner was
// keeping it, so a slow handler's work was always wasted. drivers/sqs fixed the
// same class in its own terms.
func TestALeaseSchedulerWhoseLeadIsInsideTheGuardKeepsTheDelivery(t *testing.T) {
	const lead = 500 * time.Millisecond // inside rigConfig's 1s LeaseGuard
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "extend-lead", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "extend-lead", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("slow")})
	d := fetchOne(t, src)
	by := d.Info.LeaseExpires.Sub(d.Info.ReceivedAt)
	ext := src.Settler().(mq.DeadlineExtender)
	due := d.Info.LeaseExpires.Add(-lead)
	for i := range 3 {
		server.Advance(due.Sub(server.Now()))
		if err := ext.Extend(context.Background(), &d, by); err != nil {
			t.Fatalf("extension %d, sent %v before the deadline (the delivery reported a lease of %v and the "+
				"LeaseGuard is 1s), was refused with %v (code %v)", i+1, lead, by, err, mq.CodeOf(err))
		}
		due = due.Add(by - lead)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("the Ack of a delivery the scheduler kept answered %v (code %v), want nil", err, mq.CodeOf(err))
	}
	if n := b.LeaseLost(); n != 0 {
		t.Fatalf("the sweeper gave back %d delivery(s) the scheduler was keeping", n)
	}
}

// TestNackRoutes holds the three negative actions, and the two routes a
// delayed requeue can take.
func TestNackRoutes(t *testing.T) {
	t.Run("an immediate requeue", func(t *testing.T) {
		const topic = "persistent://public/default/nacked"
		b, server := newRig(t, nil)
		src := sourceOf(t, b, "nacked", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "nacked", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("again")})
		d := fetchOne(t, src)
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{}); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		fetchNothing(t, src, "an undelayed negative settlement still waits Pulsar's NackRedeliveryDelay")
		server.Advance(time.Second)
		again := fetchOne(t, src)
		if again.Info.Attempt != 2 || !again.Info.Redelivered {
			t.Fatalf("the redelivery reports attempt %d redelivered %v", again.Info.Attempt, again.Info.Redelivered)
		}
		if err := src.Settler().Ack(context.Background(), &again); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		_ = server.Stats(topic, "workers")
	})

	t.Run("a delayed requeue held in this process", func(t *testing.T) {
		// EmulatedVolatile delay: with no retry topic the delay
		// has to be held somewhere, and the only place left is here. The
		// capability table declares it as an emulation and says what is
		// lost with the process.
		b, server := newRig(t, nil)
		src := sourceOf(t, b, "held", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "held", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("later")})
		d := fetchOne(t, src)
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: 5 * time.Second}); err != nil {
			t.Fatalf("Nack with a delay: %v", err)
		}
		if n := b.HeldDelayedNacks(); n != 1 {
			t.Fatalf("the driver holds %d delayed negative settlement(s), want 1", n)
		}
		// The delivery is terminal the moment the delayed settlement was
		// accepted: what is still owed is the FRAME, not the decision. A
		// second settlement that reached the broker beside the hold would
		// acknowledge a message this driver has promised to give back.
		if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("acknowledging a delivery whose delayed negative settlement is held reported "+
				"%v (code %v), want ErrAlreadySettled", err, mq.CodeOf(err))
		}
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: time.Second}); !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("a second delayed negative settlement reported %v, want ErrAlreadySettled", err)
		}
		server.Advance(time.Second)
		fetchNothing(t, src, "a delayed negative settlement came back early")
		server.Advance(5 * time.Second)
		waitFor(t, "the scheduler to pay the held negative settlement", func() bool {
			return b.HeldDelayedNacks() == 0
		})
		// With a fake clock, waiting IS advancing: the counter above is the
		// driver's and drops when the scheduler releases the hold, which is
		// before the redelivery is queued on the server. A single Advance
		// therefore moves time for a message that is not there yet, and the
		// fetch then waits out its whole budget at a fake time nothing ever
		// reaches -- "fetched 0 of 1 deliveries before the deadline" after
		// five seconds of retrying, on a loaded host.
		again, ok := fetchAdvancing(t, src, server, time.Second)
		if !ok {
			t.Fatal("the delayed redelivery never arrived, however far the clock was advanced")
		}
		if string(again.Message.Body) != "later" {
			t.Fatalf("the delayed redelivery carries %q", again.Message.Body)
		}
		if err := src.Settler().Ack(context.Background(), &again); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})

	t.Run("a delayed requeue through the retry topic", func(t *testing.T) {
		const topic = "persistent://public/default/retried"
		b, server := newRig(t, func(c *pd.Config) {
			c.RetryEnable = true
			c.DeadLetterTopic = "retried-dlq"
			c.MaxAttempts = 4
		})
		src := sourceOf(t, b, "retried", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "retried", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("retry me")})
		d := fetchOne(t, src)
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: 2 * time.Second}); err != nil {
			t.Fatalf("Nack with a delay: %v", err)
		}
		if n := b.HeldDelayedNacks(); n != 0 {
			t.Fatalf("the driver held %d delayed negative settlement(s) in process while the retry "+
				"topic was enabled; the delay is the broker's there", n)
		}
		st := server.Stats(topic, "workers")
		if st.Retried != 1 {
			t.Fatalf("the retry router moved %d message(s), want 1", st.Retried)
		}
		if st.Acked != 1 {
			t.Fatalf("the original was acknowledged %d time(s); the router acknowledges it once the "+
				"republication is stored", st.Acked)
		}
	})

	t.Run("drop is destructive and says so", func(t *testing.T) {
		const topic = "persistent://public/default/dropped"
		b, server := newRig(t, nil)
		src := sourceOf(t, b, "dropped", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "dropped", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("gone")})
		d := fetchOne(t, src)
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDrop}); err != nil {
			t.Fatalf("Nack(drop): %v", err)
		}
		server.Advance(5 * time.Second)
		fetchNothing(t, src, "a dropped message came back")
		if st := server.Stats(topic, "workers"); st.Acked != 1 {
			t.Fatalf("the broker saw %d acknowledgement(s); Pulsar's only way to make a message go "+
				"away is to mark it consumed", st.Acked)
		}
	})

	t.Run("a dead-letter route needs a destination", func(t *testing.T) {
		b, _ := newRig(t, nil)
		src := sourceOf(t, b, "nowhere", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "nowhere", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("poison")})
		d := fetchOne(t, src)
		err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("a dead-letter settlement with no destination reported %v (code %v), want "+
				"ErrUnsupported: the specification forbids one that destroys the message instead", err, mq.CodeOf(err))
		}
		// And it left the delivery settleable, which is the half that
		// keeps the message from being lost.
		if err := src.Settler().Ack(context.Background(), &d); err != nil {
			t.Fatalf("a refused settlement did not leave the delivery settleable: %v", err)
		}
	})

	t.Run("a dead-letter route with one", func(t *testing.T) {
		const topic = "persistent://public/default/poisoned"
		b, server := newRig(t, func(c *pd.Config) {
			c.DeadLetterTopic = "poisoned-dlq"
			c.MaxAttempts = 4
		})
		src := sourceOf(t, b, "poisoned", "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, "poisoned", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("poison")})
		d := fetchOne(t, src)
		if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
			t.Fatalf("Reject: %v", err)
		}
		st := server.Stats(topic, "workers")
		if st.DeadLettered != 1 {
			t.Fatalf("the dead-letter router moved %d message(s), want 1: an explicit Reject has to "+
				"reach the same route a message exhausted by redelivery would ",
				st.DeadLettered)
		}
		if b.DeadLettered() != 1 {
			t.Fatalf("the driver counted %d explicit dead-letter routes", b.DeadLettered())
		}
		// And the message is on the dead-letter topic, readable.
		dlq := sourceOf(t, b, "poisoned-dlq", "auditors", mq.MetadataNativeStrict, 4)
		got := fetchOne(t, dlq)
		if string(got.Message.Body) != "poison" {
			t.Fatalf("the dead-letter topic holds %q", got.Message.Body)
		}
		if err := dlq.Settler().Ack(context.Background(), &got); err != nil {
			t.Fatalf("Ack on the dead-letter topic: %v", err)
		}
	})
}

// TestAHeldNackDoesNotGiveBackItsRedelivery is a delayed negative settlement
// held in this process while the consumer reconnects. The broker redelivers the
// message under the same identifier and a handler starts on it; the hold used
// to fire on that identifier anyway, so the broker took the live redelivery
// back, sent it a third time and the handler's Ack was refused.
func TestAHeldNackDoesNotGiveBackItsRedelivery(t *testing.T) {
	const topic = "persistent://public/default/heldredeliver"
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "heldredeliver", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "heldredeliver", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})
	first := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &first, mq.NackOptions{Delay: 3 * time.Second}); err != nil {
		t.Fatalf("delayed Nack: %v", err)
	}
	server.Redeliver(topic, "workers")
	live := fetchOne(t, src)
	server.Advance(3500 * time.Millisecond)
	waitFor(t, "the hold to come due", func() bool { return b.HeldDelayedNacks() == 0 })
	if n := server.Stats(topic, "workers").Nacked; n != 0 {
		t.Fatalf("the hold wrote %d negative acknowledgement(s) for a message whose redelivery is being "+
			"worked on, want 0", n)
	}
	// Past NackRedeliveryDelay, and well inside the redelivery's lease.
	server.Advance(time.Second)
	fetchNothing(t, src, "the message came back while its redelivery was being worked on")
	if err := src.Settler().Ack(context.Background(), &live); err != nil {
		t.Fatalf("the redelivery's Ack answered %v (code %v), want nil", err, mq.CodeOf(err))
	}
}

// TestBatchSettlementIsIndexAligned holds the batched settlement and its
// index-aligned result.
func TestBatchSettlementIsIndexAligned(t *testing.T) {
	const topic = "persistent://public/default/batchsettle"
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "batchsettle", "workers", mq.MetadataNativeStrict, 16)
	snk := sinkOf(t, b, "batchsettle", mq.MetadataNativeStrict)
	const messages = 6
	for i := range messages {
		publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	ds := fetchN(t, src, messages)
	bs, ok := src.Settler().(mq.BatchSettler)
	if !ok {
		t.Fatal("the settler is not a BatchSettler")
	}
	refs := make([]*mq.DriverDelivery, messages)
	for i := range ds {
		refs[i] = &ds[i]
	}
	if err := bs.AckBatch(context.Background(), refs); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if st := server.Stats(topic, "workers"); st.Acked != messages {
		t.Fatalf("the broker acknowledged %d of %d", st.Acked, messages)
	}
	// A second AckBatch is a batch of refusals, index-aligned.
	err := bs.AckBatch(context.Background(), refs)
	var berr *mq.BatchError
	if !errors.As(err, &berr) {
		if !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("a second AckBatch reported %v (code %v)", err, mq.CodeOf(err))
		}
		return
	}
	if len(berr.Errs) != messages {
		t.Fatalf("the batch error carries %d outcomes for %d deliveries; a result aligned with "+
			"anything but the input reports one delivery's outcome for another", len(berr.Errs), messages)
	}
}

// TestAFailedBatchSettlementIsReported is why AckBatch does not use
// Consumer.AckIDList: pulsar-client-go v0.21 discards a partition consumer's
// error there and answers nil, and the deliveries were then marked settled
// although nothing reached the broker.
func TestAFailedBatchSettlementIsReported(t *testing.T) {
	const topic = "persistent://public/default/batchfail"
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "batchfail", "workers", mq.MetadataNativeStrict, 16)
	snk := sinkOf(t, b, "batchfail", mq.MetadataNativeStrict)
	const messages = 3
	for i := range messages {
		publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	ds := fetchN(t, src, messages)
	refs := make([]*mq.DriverDelivery, messages)
	for i := range ds {
		refs[i] = &ds[i]
	}
	pd.CloseNativeConsumerForTest(src)
	if err := src.Settler().(mq.BatchSettler).AckBatch(context.Background(), refs); err == nil {
		t.Fatal("AckBatch on a closed consumer reported success")
	}
	if st := server.Stats(topic, "workers"); st.Acked != 0 {
		t.Fatalf("the broker acknowledged %d of %d through a closed consumer", st.Acked, messages)
	}
}

// TestSettlingAnotherSourcesDeliveryIsRefused is the structural half of the
// batch guard: a caller cannot hand this settler a handle of its own making.
func TestSettlingAnotherSourcesDeliveryIsRefused(t *testing.T) {
	b, _ := newRig(t, nil, pulsarfake.WithBatchSize(1))
	one := sourceOf(t, b, "a", "workers", mq.MetadataNativeStrict, 4)
	two := sourceOf(t, b, "b", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "a", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})
	d := fetchOne(t, one)
	if err := two.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("settling one source's delivery through another reported %v (code %v), want "+
			"ErrNotSettleable", err, mq.CodeOf(err))
	}
	if err := two.Settler().Ack(context.Background(), &mq.DriverDelivery{}); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("settling a zero DriverDelivery reported %v, want ErrNotSettleable", err)
	}
	// A tampered generation is a delivery this source cannot vouch for.
	tampered := d
	tampered.Gen++
	if err := one.Settler().Ack(context.Background(), &tampered); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("settling a delivery whose generation was changed reported %v, want ErrLeaseLost", err)
	}
	if err := one.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
