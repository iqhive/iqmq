package nsq_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestCloseRacesTheHandlerForThousandsOfRounds is this repository's recurring
// defect class, in the shape it takes on a push-adapted driver.
//
// Three increments in a row shipped the same bug: a record put somewhere the
// engine holds it WHILE the thing holding it is closing, so the placement
// lands after the drain and the record is never released. Here the record is a
// delivery, the place is the push adapter's ring, and the cost of losing the
// race is a message nsqd holds in flight until its per-message timeout expires
// — which for the DEFAULT deadline is a minute of a message nobody is working
// on, and which makes go-nsq's own teardown wait thirty seconds for a response
// that will never come.
//
// The known instances of this class appeared at five to twenty failures per two
// or three thousand rounds, so the round count is what makes the test mean
// anything. What it asserts is the conservation law: after Close, every message
// the broker took is either finished or back in the channel, and NONE is still
// in flight.
func TestCloseRacesTheHandlerForThousandsOfRounds(t *testing.T) {
	const rounds = 3000
	const messages = 8
	for round := range rounds {
		b, server := newBackendRig(t)
		topic := "race-" + strconv.Itoa(round)
		src := sourceOf(t, b, topic, "workers", mq.MetadataNativeStrict, 4)
		snk := sinkOf(t, b, topic, mq.MetadataNativeStrict)
		for i := range messages {
			if err := snk.Publish(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, nil); err != nil {
				t.Fatalf("round %d: Publish: %v", round, err)
			}
		}

		// One goroutine fetches and settles while another closes, so the
		// close lands at an unpredictable point in the handler's own path:
		// between the supersession and the push, inside a parked push, or
		// after a delivery has been handed out and not yet settled.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			buf := make([]mq.DriverDelivery, 4)
			taken := 0
			for {
				n, err := src.Fetch(ctx, buf)
				if err != nil {
					return
				}
				for i := range n {
					d := buf[i]
					taken++
					if taken%2 == 0 {
						// Every other delivery is left UNSETTLED on
						// purpose. Without that the fetcher settles
						// everything the moment it arrives and Close
						// has nothing outstanding to put back — which
						// makes the test one that cannot fail, because
						// the obligation the specification puts on Close is precisely
						// about what it has handed out and nobody
						// settled.
						continue
					}
					// A settlement racing the close may be refused, and
					// a refusal is not a failure: what must not happen
					// is a settlement that lands on somebody else's
					// delivery.
					if err := src.Settler().Ack(context.Background(), &d); err != nil &&
						!errors.Is(err, mq.ErrLeaseLost) && !errors.Is(err, mq.ErrAlreadySettled) {
						t.Errorf("round %d: Ack answered %v", round, err)
						return
					}
				}
			}
		}()

		// Close only once the pipeline is actually running. Without this
		// the whole round finishes before the broker has delivered
		// anything, and the test becomes one that cannot fail: the ring is
		// empty at Close, so a Close that drained before it closed — the
		// defect this test exists for — loses nothing. Watching the test
		// fail under exactly that mutation is what says it works.
		deadline := time.Now().Add(2 * time.Second)
		for server.Stats(topic, "workers").Delivered < 2 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		if err := src.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
		wg.Wait()

		st := server.Stats(topic, "workers")
		if st.InFlight != 0 {
			t.Fatalf("round %d: %d message(s) are still in flight after Close; the placement landed "+
				"after the drain, so nsqd holds the message until its timeout expires", round, st.InFlight)
		}
		if st.Finished+st.Depth+st.Deferred != messages {
			t.Fatalf("round %d: the broker accounts for %d finished, %d pending and %d deferred of %d "+
				"published: a delivery was neither settled nor requeued",
				round, st.Finished, st.Depth, st.Deferred, messages)
		}
		if st.CrossSettled != 0 {
			t.Fatalf("round %d: %d settlement(s) landed on a later delivery", round, st.CrossSettled)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("round %d: backend Close: %v", round, err)
		}
	}
}

// TestPushBackpressureHoldsTheBrokerOff is the property the push adapter exists
// for, asserted rather than assumed.
//
// The ring is the bound between a broker that delivers by calling the driver
// and an SPI that asks the driver to be called. A driver whose ring never
// blocks is not applying backpressure, it is buffering — and the specification forbids the
// unbounded buffer that is.
func TestPushBackpressureHoldsTheBrokerOff(t *testing.T) {
	b, server := newBackendRig(t)
	// A prefetch of one, so the ring holds one delivery and the model's
	// delivery loop parks on the second.
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 1)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	const messages = 12
	for i := range messages {
		if err := snk.Publish(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// The broker cannot get ahead: at most the ring's capacity plus what one
	// handler is carrying is ever out of the channel.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := server.Stats("orders", "workers")
		if st.InFlight > 2 {
			t.Fatalf("%d messages are in flight with a prefetch of one: the ring is buffering rather "+
				"than applying backpressure", st.InFlight)
		}
		if st.Depth+st.InFlight+st.Finished == messages && st.InFlight > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// And it drains: every message arrives once the consumer takes them.
	seen := 0
	for seen < messages {
		d := fetchOne(t, src)
		if err := src.Settler().Ack(context.Background(), &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		seen++
	}
	if st := server.Stats("orders", "workers"); st.Finished != messages {
		t.Fatalf("the broker finished %d of %d messages", st.Finished, messages)
	}
}

// TestASourceBuiltAndNeverConnectedStillStops is a goroutine leak that is
// invisible until it is not.
//
// go-nsq's consumer runs an RDY loop from the moment it is constructed, and its
// Stop with no connections closes the incoming-message channel and then waits
// for the handler count to fall to zero. With no handlers that count is already
// zero, so nothing ever calls the exit that closes the stop channel, and the
// RDY loop runs for ever. A source that is built and then closed without
// connecting is not hypothetical — OpenSource closes one when the backend
// closed underneath it — so the handler pool is added at construction rather
// than at connect. TestMain's goleak is the assertion.
func TestASourceBuiltAndNeverConnectedStillStops(t *testing.T) {
	b, server := newBackendRig(t)
	for range 8 {
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{
			Topic: "never-connected", Subscription: "workers", Prefetch: 4,
			AckDeadline: 30 * time.Second,
		})
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// And a second Close changes nothing: the specification makes it idempotent.
		if err := src.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
	if st := server.Stats("never-connected", "workers"); st.InFlight != 0 {
		t.Fatalf("the broker holds %d in flight for a subscription that never consumed", st.InFlight)
	}
}
