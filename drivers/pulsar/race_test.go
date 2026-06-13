package pulsar_test

import (
	"context"
	"sync"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
)

// TestHeldNackRacesClose is this repository's recurring defect class, in the one
// route this driver adds to it.
//
// the contributor notes records three increments shipping the same bug: a record put
// into a lane, a store or a retained log while its owner is closing. The
// placing goroutine checks the closed flag, Close drains, and the placement
// lands after the drain — so the obligation attached to the placement never
// runs.
//
// This driver's version is a DELAYED NEGATIVE SETTLEMENT. The scheduler's list
// is the place a record is put, and the obligation is a message Pulsar is still
// waiting for: a hold that Close drained past is a delivery nothing will ever
// give back, so the message sits unsettled until the consumer disconnects — and
// with a long-lived client that is the process's lifetime.
//
// The known instances of this class appeared at five to twenty failures per two
// or three thousand rounds, so the round count is what makes the test worth
// having. What it asserts is the invariant: once Close has returned, the driver
// holds nothing.
func TestHeldNackRacesClose(t *testing.T) {
	const rounds = 3000
	for round := range rounds {
		b, _ := newRig(t, nil)
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{
			Topic: "raceclose", Subscription: "workers", Prefetch: 8,
			Metadata: mq.MetadataNativeStrict, Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("round %d: OpenSource: %v", round, err)
		}
		snk := sinkOf(t, b, "raceclose", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("x")})
		d := fetchOne(t, src)

		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			// The placement: a delayed negative settlement goes into
			// the scheduler's list.
			_ = src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: time.Hour})
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = src.Close()
		}()
		close(start)
		wg.Wait()
		if held := b.HeldDelayedNacks(); held != 0 {
			t.Fatalf("round %d: the driver still holds %d delayed negative settlement(s) after Close "+
				"returned; each of them is a message Pulsar is waiting for and a scheduler that "+
				"has stopped existing", round, held)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}
}

// TestConcurrentPublishAndClose races a publication against the sink's own
// Close, for the other half of the same class: an asynchronous publication
// whose callback still holds the caller's message.
func TestConcurrentPublishAndClose(t *testing.T) {
	const rounds = 500
	for round := range rounds {
		b, _ := newRig(t, nil)
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "raceclose"})
		if err != nil {
			t.Fatalf("round %d: OpenSink: %v", round, err)
		}
		as := snk.(mq.AsyncSink)
		var calls int64
		var mu sync.Mutex
		var wg sync.WaitGroup
		const publications = 8
		wg.Add(publications + 1)
		start := make(chan struct{})
		for range publications {
			go func() {
				defer wg.Done()
				<-start
				m := &mq.Message{Body: []byte("x")}
				if err := as.PublishAsync(context.Background(), m, nil, func(error) {
					mu.Lock()
					calls++
					mu.Unlock()
				}); err != nil {
					// A publication refused because the sink
					// closed first is correct, and its
					// callback must NOT have run.
					return
				}
				mu.Lock()
				calls += 1 << 20 // admitted; the callback owes one
				mu.Unlock()
			}()
		}
		go func() {
			defer wg.Done()
			<-start
			_ = snk.Close()
		}()
		close(start)
		wg.Wait()
		mu.Lock()
		admitted := calls >> 20
		ran := calls & (1<<20 - 1)
		mu.Unlock()
		if ran != admitted {
			t.Fatalf("round %d: %d publications were admitted and %d callbacks ran; the specification promises "+
				"exactly one per admitted publication, close included", round, admitted, ran)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}
}

// TestSettleRacesTheLeaseSweeper races a caller's settlement against the
// driver's own sweeper, which is the pair of goroutines that both want to take
// a delivery terminal.
//
// Exactly one of them may win, and the loser's answer has to be one of the specification:
// a settlement that raced the sweeper and was told "already settled" would tell
// a caller its work was accounted for when the message had been given back.
func TestSettleRacesTheLeaseSweeper(t *testing.T) {
	const rounds = 2000
	b, server := newRig(t, func(c *pd.Config) {
		c.AckTimeout, c.LeaseGuard = time.Second, 100*time.Millisecond
	})
	src := sourceOf(t, b, "racesweep", "workers", mq.MetadataNativeStrict, 64)
	snk := sinkOf(t, b, "racesweep", mq.MetadataNativeStrict)
	for round := range rounds {
		publish(t, snk, &mq.Message{Body: []byte("x")})
		d := fetchOne(t, src)
		var wg sync.WaitGroup
		wg.Add(2)
		var settleErr error
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			settleErr = src.Settler().Ack(context.Background(), &d)
		}()
		go func() {
			defer wg.Done()
			<-start
			// Past the deadline, so the sweeper wants the delivery
			// too.
			server.Advance(2 * time.Second)
		}()
		close(start)
		wg.Wait()
		switch {
		case settleErr == nil:
		case mq.CodeOf(settleErr) == mq.CodeLeaseLost:
		default:
			t.Fatalf("round %d: a settlement that raced the lease sweeper reported %v (code %v); the "+
				"only honest answers are success and ErrLeaseLost — ErrAlreadySettled would tell a "+
				"caller its work was accounted for when the message had been given back",
				round, settleErr, mq.CodeOf(settleErr))
		}
	}
}
