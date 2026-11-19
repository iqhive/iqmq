package rabbitmq_test

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// TestADelayedNackThatWouldComeBackTooLargeIsRefused is a delayed nack of a
// delivery whose parked copy would come back from the delay in a frame its
// consumer cannot read, on both arms.
//
// The copy comes back larger than the delivery: by the driver's attempt
// header and confirm token, by `x-delay` on the plugin arm, and on the TTL
// arm by the death headers the delay queue's dead-lettering adds, about 470
// octets. A delivery near the frame bound came back too large for the
// consumer's client library, which closed the connection; the delivery went
// back to its queue and closed the next connection too. So the nack is
// refused before anything is written, with Unsupported, which Run answers
// with a nack without the delay, and the delivery is still the caller's.
//
// The largest delivery a delayed nack takes comes back in a frame of exactly
// the frame size less its 8 octets and the 52 kept for a redelivery's count
// headers, so the check is exact: one octet more is refused.
func TestADelayedNackThatWouldComeBackTooLargeIsRefused(t *testing.T) {
	const frameMax = 4096
	for _, arm := range []struct {
		name string
		kind delay.Kind
	}{{"ttldlx", delay.TTLDLX}, {"plugin", delay.Plugin}} {
		t.Run(arm.name, func(t *testing.T) {
			bcfg := rabbitfake.Config{FrameMax: frameMax, DelayedMessagePlugin: arm.kind == delay.Plugin}
			broker, cl := openFakeWith(t, bcfg, func(c *rq.Config) {
				c.Declare = true
				c.DelayKind = arm.kind
			})
			cons := consumeOn(t, cl, "orders", "workers")
			raw := fakeChannel(t, broker)
			published := func() int {
				n := len(broker.Delayed())
				for _, q := range broker.ArrivalQueues() {
					n += len(broker.Publications(q))
				}
				return n
			}
			// nack publishes a delivery with a header of n octets, as another
			// publisher would, and nacks it with a delay. It reports whether
			// the nack took it and, if it did, the frame its copy came back
			// in.
			nack := func(n int) (bool, int) {
				t.Helper()
				m := amqp.Publishing{Body: []byte("b"), Headers: amqp.Table{"h": make([]byte, n)}}
				if err := raw.PublishWithContext(t.Context(), "", "orders.workers", false, false, m); err != nil {
					t.Fatalf("publishing a %d-octet header: %v", n, err)
				}
				d := receiveOne(t, cons)
				before, settled := published(), len(broker.Settlements())
				err := d.Nack(mq.NackDelay(3 * time.Second))
				broker.Sync()
				if err != nil {
					if !errors.Is(err, mq.ErrUnsupported) || mq.CodeOf(err) != mq.CodeUnsupported || mq.IsAmbiguous(err) {
						t.Fatalf("a delayed nack of a %d-octet header = %v (code %v): want it taken or refused "+
							"with Unsupported", n, err, mq.CodeOf(err))
					}
					if got := published(); got != before {
						t.Fatalf("a refused delayed nack of a %d-octet header published %d messages", n, got-before)
					}
					if got := len(broker.Settlements()); got != settled {
						t.Fatalf("a refused delayed nack of a %d-octet header settled the delivery: %v", n,
							broker.Settlements()[settled:])
					}
					if err := d.Nack(); err != nil {
						t.Fatalf("the delivery whose delayed nack was refused could not be nacked: %v", err)
					}
					again := receiveOne(t, cons)
					if err := again.Ack(); err != nil {
						t.Fatal(err)
					}
					return false, 0
				}
				broker.Advance(4 * time.Second)
				back := receiveOne(t, cons)
				arrived := broker.Publications("orders.workers")
				frame := arrived[len(arrived)-1].HeaderFrame
				if err := back.Ack(); err != nil {
					t.Fatal(err)
				}
				return true, frame
			}
			fits, over := 1000, frameMax
			if ok, _ := nack(fits); !ok {
				t.Fatalf("a delayed nack of a %d-octet header was refused at a frame size of %d", fits, frameMax)
			}
			for over-fits > 1 {
				mid := (fits + over) / 2
				if ok, _ := nack(mid); ok {
					fits = mid
				} else {
					over = mid
				}
			}
			ok, frame := nack(fits)
			if !ok {
				t.Fatalf("the largest header a delayed nack took, %d octets, was refused the second time", fits)
			}
			if want := frameMax - 8 - 52; frame != want {
				t.Errorf("the largest delivery a delayed nack takes came back in a %d-octet frame, want %d: the "+
					"frame size less its 8 octets and the 52 kept for a redelivery", frame, want)
			}
			if ok, _ := nack(fits + 1); ok {
				t.Errorf("a delayed nack of a header one octet larger than the largest taken, %d, was taken", fits+1)
			}
		})
	}
}

// TestAnUnmappableDeliveryThatWouldComeBackTooLargeIsNotParked is the other
// park, of a delivery the consumer cannot map, which goes to the tail of its
// own queue with the driver's attempt header and confirm token
// (TestUnmappableDeliveriesDoNotStopTheConsumer). One near the frame bound came
// back in a frame its consumer could not read, which closed the connection on
// every delivery. It is now held unsettled, as a park that fails leaves it,
// and the broker takes it back with the channel; one further from the bound is
// parked and comes back within it.
func TestAnUnmappableDeliveryThatWouldComeBackTooLargeIsNotParked(t *testing.T) {
	const frameMax = 4096
	broker, cl := openFakeWith(t, rabbitfake.Config{FrameMax: frameMax}, declaring)
	b, _ := rq.BackendForTest(cl)
	cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) {
		c.Metadata = mq.MetadataPortableEnvelope
	})
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	restores := rq.RestoresForTest(b)
	// A plain body is a delivery this consumer cannot map. The first is
	// within the 38 octets of the bound the park adds; the second is not.
	for _, n := range []int{frameMax - 8 - 52 - 25 - 30, frameMax - 8 - 52 - 25 - 100} {
		if _, err := broker.Publish("", "orders.workers", amqp.Publishing{
			Body: []byte("not an envelope"), Headers: amqp.Table{"h": make([]byte, n)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if string(d.Body) != "after" {
		t.Fatalf("the delivery behind the unmappable ones is %q, want %q", d.Body, "after")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	// A park waits for its confirm on a goroutine of its own, so the test
	// waits for the one that fits to land, and then a little longer for one
	// that should not. The copy is as unmappable as its original, so it is
	// parked again each time it comes back, which is the design: it goes
	// round until a dead-letter route or an operator takes it.
	near, far := frameMax-8-52-25-30, frameMax-8-52-25-100
	parked := func() []rabbitfake.Publication {
		broker.Sync()
		var out []rabbitfake.Publication
		for _, p := range broker.Publications("orders.workers") {
			if _, ok := p.Headers[rq.AttemptHeader]; ok {
				out = append(out, p)
			}
		}
		return out
	}
	for deadline := time.Now().Add(5 * time.Second); len(parked()) == 0 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	copies := parked()
	if len(copies) == 0 {
		t.Fatal("no unmappable delivery was parked, want the one further from the bound")
	}
	for _, p := range copies {
		if got := len(p.Headers["h"].([]byte)); got != far {
			t.Fatalf("the unmappable delivery with a %d-octet header was parked, want only the one of %d", got, far)
		}
	}
	if got, want := copies[0].HeaderFrame, far+25+38; got != want {
		t.Errorf("the parked copy arrived in a %d-octet frame, want %d", got, want)
	}
	held := false
	for _, f := range broker.InFlight() {
		if h, _ := f.Message.Headers["h"].([]byte); len(h) == near {
			held = true
		}
	}
	if !held {
		t.Errorf("the unmappable delivery within the park's reach of the bound is not held unsettled: %v",
			broker.InFlight())
	}
	if got := rq.RestoresForTest(b); got != restores {
		t.Errorf("the client reconnected %d times", got-restores)
	}
}

// fakeChannel is a channel on a connection of the test's own to the fake
// broker, as another client would have.
func fakeChannel(t *testing.T, b *rabbitfake.Broker) client.Channel {
	t.Helper()
	conn, err := b.Dialer().Dial(t.Context(), []string{"amqp://fake/"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("dialling the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	return ch
}
