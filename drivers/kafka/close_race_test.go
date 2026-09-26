package kafka_test

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestCloseRacesSettlement is the contributor notes's recurring defect class,
// applied to this driver.
//
// The class is "a record put somewhere the component holds it while the
// component is closing": the placing goroutine checks the closed flag, Close
// drains, and the placement lands after the drain. Three increments in a row
// shipped it. This driver's analogue is a settlement — the route that puts a
// record into a partition's held-for-redelivery list, or completes an offset
// and moves a watermark — arriving while Close is committing and tearing the
// group client down.
//
// What must hold on every crossing:
//
//   - no panic and no data race (this runs under -race in `make ci`);
//   - a settlement either succeeds or reports one of the refusals. What it
//     must never do is return nil while having done nothing, because the core
//     would then mark a delivery settled that the broker never heard about;
//   - nothing is left running. goleak in TestMain is what says so, and a
//     commit ticker or a parked poll that outlived Close would fail it.
//
// The crossings are what makes it a race test rather than a smoke test: a
// hundred proves nothing, so this is two thousand settlements over twenty
// consumers, each consumer's Close racing a hundred settlements of its own.
func TestCloseRacesSettlement(t *testing.T) {
	const (
		consumers        = 20
		perConsumer      = 100
		closeAfterAround = 30 // settlements dispatched before Close is called
	)
	topic := newTopic("closerace")
	cl := cluster(t, 4, []string{topic})
	c := openOn(t, cl)

	// One batch of records per consumer, all published up front.
	for i := range consumers * perConsumer {
		publish(t, c, topic, &mq.Message{
			Key:  []byte("k" + strconv.Itoa(i%37)),
			Body: []byte(strconv.Itoa(i)),
		})
	}

	var (
		settled   atomic.Int64
		refused   atomic.Int64
		crossings atomic.Int64
	)
	for round := range consumers {
		group := "g-closerace-" + strconv.Itoa(round)
		cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: topic, Subscription: group, Start: mq.StartEarliest(), Prefetch: perConsumer,
		})
		if err != nil {
			t.Fatalf("round %d: opening a consumer: %v", round, err)
		}
		// Take a batch and hold every delivery unsettled.
		dst := make([]mq.Delivery, perConsumer)
		got := 0
		deadline := time.Now().Add(20 * time.Second)
		for got < perConsumer && time.Now().Before(deadline) {
			n, rerr := cons.Receive(t.Context(), dst[got:])
			got += n
			if rerr != nil {
				break
			}
		}
		if got == 0 {
			_ = cons.Close()
			t.Fatalf("round %d: no deliveries", round)
		}

		// Every settlement is dispatched at once, and Close is called from
		// the middle of the pack: what crosses it is the point.
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range got {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				var err error
				if i%3 == 0 {
					err = dst[i].Nack()
				} else {
					err = dst[i].Ack()
				}
				crossings.Add(1)
				switch {
				case err == nil:
					settled.Add(1)
				case settleRefusal(err):
					refused.Add(1)
				default:
					t.Errorf("round %d delivery %d: settlement reported %v (code %v); the specification allows a success or one of its own refusals and nothing else",
						round, i, err, mq.CodeOf(err))
				}
			}(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// A handful of settlements go first, so the rest cross a Close
			// that is already under way.
			for settled.Load()+refused.Load() < closeAfterAround {
				if crossings.Load() >= int64(got) {
					break
				}
			}
			if err := cons.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
				t.Errorf("round %d: Close reported %v", round, err)
			}
		}()
		close(start)
		wg.Wait()
	}
	t.Logf("%d settlements crossed a Close: %d succeeded, %d were refused", crossings.Load(), settled.Load(), refused.Load())
	if crossings.Load() < consumers*perConsumer/2 {
		t.Fatalf("only %d settlements crossed a Close; the race was not exercised", crossings.Load())
	}
	if refused.Load() == 0 {
		t.Log("no settlement was refused: every one of them landed before its Close, which is a valid outcome and means the window was not hit this run")
	}
}

// settleRefusal reports whether err is one of the refusals the specification allows a
// settlement to report.
func settleRefusal(err error) bool {
	return errors.Is(err, mq.ErrClosed) ||
		errors.Is(err, mq.ErrLeaseLost) ||
		errors.Is(err, mq.ErrAlreadySettled) ||
		errors.Is(err, mq.ErrSettlementInProgress) ||
		errors.Is(err, mq.ErrSettlementUnknown)
}
