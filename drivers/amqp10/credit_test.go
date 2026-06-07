package amqp10_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The credit accounting. The invariant is
//
//	issued + held <= prefetch
//
// and every test here is one way to break it.

// TestCreditNeverExceedsThePrefetch is the bound made real: "Prefetch is how
// many messages the source may hold unclaimed".
//
// A driver that issued credit per fetch rather than per free slot would let the
// peer send more unsettled deliveries than the prefetch, and the token table
// would then be the only thing between the caller and a settlement that names
// somebody else's delivery.
func TestCreditNeverExceedsThePrefetch(t *testing.T) {
	for _, prefetch := range []int{1, 2, 8} {
		t.Run("prefetch="+itoa(prefetch), func(t *testing.T) {
			f := open(t, testProfile(), aq.Config{})
			src := f.source(mq.SourceConfig{Prefetch: prefetch})
			set := src.Settler()
			rcv, first := deliverAndFetch(t, f, src, "tag-0", "x")
			held := []mq.DriverDelivery{first}
			// Queue far more than the prefetch and fetch as much as the window
			// allows.
			for i := 1; i <= prefetch*4; i++ {
				rcv.Deliver(message("tag-"+itoa(i), "x"))
			}
			for len(held) < prefetch {
				held = append(held, fetchOne(t, src, budget))
			}
			stats := src.CreditStats()
			if stats.Held != prefetch || stats.Issued != 0 {
				t.Fatalf("the window is %+v, want %d held and no credit outstanding", stats, prefetch)
			}
			if got := rcv.Credit(); got != 0 {
				t.Fatalf("the peer holds %d credit with the window full", got)
			}
			if got := rcv.Granted(); int(got) != prefetch {
				t.Fatalf("the source issued %d credit in total for a prefetch of %d; every credit has to be matched "+
					"by a free slot", got, prefetch)
			}
			// Settle them all and the credit comes back, one per settlement.
			for i := range held {
				if err := set.Ack(context.Background(), &held[i]); err != nil {
					t.Fatalf("Ack %d: %v", i, err)
				}
			}
			if got := src.CreditStats(); got.Held != 0 {
				t.Fatalf("the window still holds %d deliver(ies) after settling everything: %+v", got.Held, got)
			}
			next := fetchOne(t, src, budget)
			if got := rcv.Granted(); int(got) <= prefetch {
				t.Fatalf("the source has issued only %d credit in total after settling %d deliveries; a settlement "+
					"has to return the credit or the window shrinks for ever", got, prefetch)
			}
			if err := set.Ack(context.Background(), &next); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestCreditReturnsCreditItCouldNotIssue is the one place the invariant can
// drift silently: credit the peer does not hold must not stay counted, or the
// window shrinks by that much for ever.
//
// It was written for a world in which a detached link was never replaced, and
// asserted that the count did not GROW past what the dead peer held. The specification
// changed the behaviour it was pinning: a link that dies is now dropped and
// re-attached, so the credit the dead link held is credit NOBODY holds and the
// window is reset with the link — which is what
// [Source.connectionLost]'s own documentation has always said credit is for
// ("credit belongs to a link and the link is gone"). The invariant is the
// same and the observable is stronger: the reattached link must be granted the
// whole prefetch, which is false the moment the window keeps a dead link's
// numbers.
func TestCreditReturnsCreditItCouldNotIssue(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	const prefetch = 4
	src := f.source(mq.SourceConfig{Prefetch: prefetch})
	rcv, first := deliverAndFetch(t, f, src, "tag-0", "x")
	if err := src.Settler().Ack(context.Background(), &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	before := src.CreditStats()
	if before.Issued == 0 {
		t.Fatalf("the window counts no outstanding credit (%+v), so this test has nothing to measure", before)
	}
	rcv.Detach(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if _, err := src.Fetch(ctx, dst); err == nil {
		t.Fatal("a fetch on a detached link succeeded")
	}
	if got := src.CreditStats(); got.Issued != 0 || got.Held != 0 {
		t.Fatalf("the window still counts %d issued and %d held credit after the link it belonged to died (%+v): "+
			"credit belongs to a link, so what a dead link held is credit nobody holds", got.Issued, got.Held, got)
	}
	// The reattached link is the observable half. A window that had kept the
	// dead link's numbers would issue the difference and no more, so the new
	// peer would be granted less than the prefetch and the consumer would be
	// permanently slower with nothing saying why.
	next, second := deliverAndFetch(t, f, src, "tag-1", "y")
	if next == rcv {
		t.Fatal("the source is still using the link that was detached")
	}
	if got := next.Granted(); int(got) != prefetch {
		t.Errorf("the reattached link was granted %d credit in total, want the whole prefetch of %d: a window that "+
			"kept a dead link's issued count issues the difference and the consumer is slower for ever", got, prefetch)
	}
	if err := src.Settler().Ack(context.Background(), &second); err != nil {
		t.Fatalf("Ack on the reattached link: %v", err)
	}
}

// TestCreditUnknownOutcomesShrinkTheWindowVisibly is the stickiness reaching
// the throughput a deployment sees.
//
// A delivery whose settlement outcome nobody knows keeps its slot for ever, so
// the effective prefetch falls. That is CORRECT — a slot reused under a token
// whose fate nobody knows is how an unknown settlement becomes a successful one
// by accident — and the point of the counter is that the condition has a name
// and a number rather than being an unexplained loss of throughput.
func TestCreditUnknownOutcomesShrinkTheWindowVisibly(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleSecond})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	rcv, first := deliverAndFetch(t, f, src, "tag-0", "x")
	rcv.Deliver(message("tag-1", "x"))
	second := fetchOne(t, src, budget)

	// Both settlements fail with a link that simply closed, which under
	// `second` is an unknown outcome.
	rcv.Detach(nil)
	for i, d := range []*mq.DriverDelivery{&first, &second} {
		err := src.Settler().Ack(context.Background(), d)
		assertCode(t, err, mq.CodeSettlementUnknown, "settlement "+itoa(i))
	}
	stats := src.CreditStats()
	if stats.Unknown != 2 || stats.Held != 2 {
		t.Fatalf("the window is %+v, want both slots held by an unknown outcome", stats)
	}
	// Every slot is now held by a delivery nothing will ever free, so a fetch
	// reports the condition rather than waiting for ever. That is a diagnosis
	// and not a hang, and it is not a spin either: the state is terminal for
	// this source.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	_, err := src.Fetch(ctx, dst)
	assertCode(t, err, mq.CodeResourceExhausted, "a fetch whose whole window is held by unknown outcomes")
}

// TestCreditWindowResetsOnAReconnection is the other half of the stickiness: a
// delivery whose settlement was ambiguous on a link that no longer EXISTS has
// been returned to the node by the detach, so its outcome is no longer unknown —
// it is a redelivery.
//
// A window that kept the old numbers would issue less than the prefetch on the
// new link and would go on doing so for the client's whole life.
func TestCreditWindowResetsOnAReconnection(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleSecond})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	rcv, first := deliverAndFetch(t, f, src, "tag-0", "x")
	rcv.Detach(nil)
	assertCode(t, src.Settler().Ack(context.Background(), &first), mq.CodeSettlementUnknown, "the settlement")
	if got := src.CreditStats(); got.Unknown != 1 {
		t.Fatalf("the window records %d unknown outcome(s), want 1", got.Unknown)
	}

	gen := f.b.Generation()
	f.peer.LastConn().Kill(nil)
	if !aq.AwaitGenerationForTest(f.b, gen, budget) {
		t.Fatal("the generation did not move")
	}
	if !aq.AwaitReconnectForTest(f.b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection")
	}
	stats := src.CreditStats()
	if stats.Unknown != 0 || stats.Held != 0 || stats.Issued != 0 {
		t.Fatalf("the window is %+v after a reconnection, want every number reset: the credit the peer was holding "+
			"died with the link, and a delivery returned to the node by the detach is a redelivery rather than an "+
			"unknown outcome", stats)
	}
	// And the new link gets the whole prefetch.
	_, fresh := deliverAndFetch(t, f, src, "tag-1", "y")
	newRcv := f.receiver(budget)
	if got := newRcv.Granted(); int(got) != 2 {
		t.Fatalf("the new link was issued %d credit, want the whole prefetch of 2", got)
	}
	if err := src.Settler().Ack(context.Background(), &fresh); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestCreditFetchWaitsOnDeterministicTime is the parked-fetch wait under
// testing/synctest, where the bubble's clock makes "it really did block" an
// assertion rather than a sleep.
//
// synctest is what the specification asks for wherever time matters: the bubble advances
// time only when every goroutine in it is blocked, so a fetch that returned
// early would show up as a bubble that never became idle.
func TestCreditFetchWaitsOnDeterministicTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := open(t, testProfile(), aq.Config{})
		src := f.source(mq.SourceConfig{Prefetch: 1})
		set := src.Settler()
		_, held := deliverAndFetch(t, f, src, "tag-0", "x")

		type result struct {
			n   int
			err error
		}
		parked := make(chan result, 1)
		go func() {
			dst := make([]mq.DriverDelivery, 1)
			n, err := src.Fetch(context.Background(), dst)
			parked <- result{n, err}
		}()
		// Every goroutine in the bubble is now blocked, which is what
		// synctest.Wait waits for — and a fetch that had returned would have
		// sent on the channel instead.
		synctest.Wait()
		select {
		case r := <-parked:
			t.Fatalf("the Fetch returned (%d, %v) with the whole window held", r.n, r.err)
		default:
		}
		// One settlement, and the delivery the peer was holding back arrives.
		f.receiver(budget).Deliver(message("tag-1", "y"))
		if err := set.Ack(context.Background(), &held); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		synctest.Wait()
		select {
		case r := <-parked:
			if r.err != nil || r.n != 1 {
				t.Fatalf("the woken Fetch returned (%d, %v)", r.n, r.err)
			}
		default:
			t.Fatal("a settlement did not wake the parked Fetch")
		}
	})
}
