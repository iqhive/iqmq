package stomp_test

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// TestNoDeliveryIsDroppedWhenTheTokenTableFills pins the one bound in this
// driver that used to answer exhaustion by throwing a message away.
//
// A delivery claims its token BEFORE it is pushed into the bounded buffer, and
// a delivery the caller holds has left the buffer and still holds its token.
// So the live token count reaches the buffer's capacity PLUS whatever the
// caller has not settled, and the table is sized at twice the prefetch to give
// that room. The specification chose the factor after a table the same size as
// the buffer lost 144 of 400 messages.
//
// The factor is a margin and never a guarantee: what a caller holds unsettled
// is bounded by nothing. When the table did fill, the pump DROPPED the
// delivery -- and a dropped delivery is not "left with the broker" in any
// useful sense, because STOMP has no per-delivery requeue on either broker and
// it comes back only when the subscription is reset. A long-lived consumer
// never resets, so it is a lost message. Measured against a real Artemis with
// `prefetch=8` and two competing consumers: six of forty lost, the last six,
// with both consumers then idle for the whole deadline because the broker
// considered them dispatched.
//
// This test creates the condition deliberately rather than waiting for a race
// to produce it: hold a full table's worth of deliveries unsettled, deliver
// more, and require every one to arrive once the settlements free the slots.
func TestNoDeliveryIsDroppedWhenTheTokenTableFills(t *testing.T) {
	const (
		prefetch = 4
		// Twice the prefetch is the table, so 2*prefetch deliveries can be
		// live at once. Three times that leaves the pump no choice but to
		// wait for a settlement, which is the path under test.
		total = 6 * prefetch
	)
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", prefetch)

	msgs := make([]stompsrv.Message, 0, total)
	for i := range total {
		msgs = append(msgs, stompsrv.Message{Body: []byte(strconv.Itoa(i))})
	}
	deliver(t, srv, consume, msgs...)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	seen := map[string]int{}
	held := make([]mq.DriverDelivery, 0, 2*prefetch)
	dst := make([]mq.DriverDelivery, prefetch)
	settler := src.Settler()
	fetch := func() {
		n, err := src.Fetch(ctx, dst)
		if err != nil {
			t.Fatalf("Fetch after %d of %d deliveries: %v. A delivery the pump dropped never arrives, "+
				"because STOMP redelivers only on a subscription reset and this subscription is still "+
				"live -- so the symptom of the defect is exactly this: a fetch that waits out its "+
				"deadline with the broker believing it has handed the message over", len(seen), total, err)
		}
		for i := range n {
			seen[string(dst[i].Message.Body)]++
			held = append(held, dst[i])
		}
	}

	// FORCE the wait rather than hope for it. Holding a whole table's worth
	// unsettled leaves the pump no slot to claim while messages are still
	// arriving, so it must be waiting -- there is no interleaving in which
	// it is not, which is what makes the assertion below deterministic.
	//
	// The first version of this test settled in waves and then asserted
	// that a wait had HAPPENED, which is a different thing: on a machine
	// whose settlements win the race the pump never blocks, and the guard
	// fired on GitHub's two-core runner and turned `main` red. An assertion
	// about a race is only safe when the race has been removed.
	for len(held) < 2*prefetch {
		fetch()
	}
	await(t, "the pump to wait for a token slot", func() bool { return src.TokenWaits() > 0 })

	for _, d := range held {
		if err := settler.Ack(ctx, &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	held = held[:0]
	for len(seen) < total {
		fetch()
		if len(held) >= 2*prefetch {
			for _, d := range held {
				if err := settler.Ack(ctx, &d); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
			held = held[:0]
		}
	}
	for _, d := range held {
		_ = settler.Ack(ctx, &d)
	}

	if len(seen) != total {
		var missing []string
		for i := range total {
			if _, ok := seen[strconv.Itoa(i)]; !ok {
				missing = append(missing, strconv.Itoa(i))
			}
		}
		t.Fatalf("%d of %d deliveries arrived; %v never did", len(seen), total, missing)
	}
	for body, n := range seen {
		if n != 1 {
			t.Errorf("delivery %q arrived %d times", body, n)
		}
	}
}

// The two tests below are what makes [stomp.Source]'s slot wait an instrument
// that has been watched fail. The wait used to be a 500-microsecond poll, so
// every reason to stop waiting was answered by the NEXT tick and nothing had to
// be selected on; it now parks in
// `driverkit.TokenTable.ClaimOrWait` on its release broadcast, and the whole question is
// whether that channel is closed on every path that ends the wait.
//
// `drivers/rabbitmq` answers a dead channel with a SECOND `NotifyClose`
// listener and a closing source with its own `closed` channel, because there
// the pump is the only reader of the close notification and the pump is the
// goroutine parked in the wait. A stomp source owns its own socket
//  and the socket is watched independently, so here every
// one of those paths invalidates the token table -- and `InvalidateAll` closes
// the broadcast. That is a claim about three other functions rather than about
// `claimSlot`, which is exactly why it needs a test: a wait that ignored the
// broadcast makes both of these fail and nothing else in the package
// notice.
//
// The observable is the PARKED GOROUTINE and not a counter, because no counter
// says the difference. `TokenWaits` counts waits ENTERED, a reset's
// redeliveries re-park a fresh pump within microseconds so counting waiters is
// a race, and `Close` can legitimately take thirty seconds waiting out
// go-stomp's unsubscribe receipt (`closeSource`), so "Close returned" is not a
// bound this suite may assert on. Identifying the goroutines by id and
// requiring THOSE to leave is the only form of the assertion that is neither
// racy nor timing-dependent.

// slotWaiters is the set of goroutine ids currently parked in
// `Source.claimSlot`, read from the runtime's own stacks.
func slotWaiters() map[string]bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]bool{}
	// Every chunk starts with the goroutine id once the buffer is given the
	// separator its first goroutine does not have in front of it.
	for _, g := range strings.Split("\n\ngoroutine "+string(buf), "\n\ngoroutine ") {
		if !strings.Contains(g, "(*Source).claimSlot(") {
			continue
		}
		if i := strings.IndexByte(g, ' '); i > 0 {
			out[g[:i]] = true
		}
	}
	return out
}

// awaitSlotWaitersGone waits until none of `who` is still parked in the slot
// wait.
//
// It polls at ten milliseconds rather than at `await`'s one, because
// `runtime.Stack` over every goroutine stops the world and this host runs about
// fifty other containers.
func awaitSlotWaitersGone(t *testing.T, who map[string]bool, what string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		still := slotWaiters()
		left := make([]string, 0, len(who))
		for id := range who {
			if still[id] {
				left = append(left, id)
			}
		}
		if len(left) == 0 {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s: goroutine(s) %v are still parked in Source.claimSlot after %v. The wait no longer "+
				"polls, so the ONLY thing that ends it is the release broadcast TokenTable.ClaimOrWait parks "+
				"on -- and every path that supersedes a subscription or closes a source ends in "+
				"InvalidateAll, which closes it. A waiter still here means that channel is not being "+
				"selected on, and the delivery it holds waits for ever rather than for 500 microseconds",
				what, left, budget)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// parkThePumpOnAFullTable leaves the pump waiting for a token slot, with the
// caller holding a full table's worth of deliveries unsettled, and returns
// them.
//
// It FORCES the wait rather than hoping for it, for
// `TestNoDeliveryIsDroppedWhenTheTokenTableFills`'s reason: an assertion about
// a race is only safe once the race has been removed.
func parkThePumpOnAFullTable(t *testing.T, srv *stompsrv.Server, src *stomp.Source,
	consume string, prefetch int,
) ([]mq.DriverDelivery, map[string]bool) {
	t.Helper()
	// Twice the prefetch is the table, so one more than that is a delivery the
	// pump cannot claim a slot for until the caller settles something.
	total := 2*prefetch + 1
	msgs := make([]stompsrv.Message, 0, total)
	for i := range total {
		msgs = append(msgs, stompsrv.Message{Body: []byte(strconv.Itoa(i))})
	}
	deliver(t, srv, consume, msgs...)
	held := fetchN(t, src, 2*prefetch)
	await(t, "the pump to wait for a token slot", func() bool { return src.TokenWaits() > 0 })
	var waiting map[string]bool
	await(t, "the waiting pump to appear in the runtime's stacks", func() bool {
		waiting = slotWaiters()
		return len(waiting) > 0
	})
	return held, waiting
}

// TestASlotWaitEndsWhenTheSourceCloses is the teardown exit.
//
// `Source.Close` sets `closed`, drains the buffer -- which releases every token
// still in it -- and then invalidates the table, and each of the last two
// closes the broadcast. Without one of them `Close` would wait for a pump that
// never returns: the pump is in `pumpsWG` and `Close` ends with
// `pumpsWG.Wait()`, so the symptom is a Close that never comes back at all.
func TestASlotWaitEndsWhenTheSourceCloses(t *testing.T) {
	const prefetch = 4
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", prefetch)

	_, waiting := parkThePumpOnAFullTable(t, srv, src, consume, prefetch)

	// Close runs on its own goroutine ONLY so that the assertion below is
	// about the pump rather than about go-stomp's unsubscribe receipt, which
	// `closeSource` documents as a thirty-second library race. The cleanup
	// joins it, so a Close that never returns is still reported.
	closed := make(chan error, 1)
	go func() { closed <- src.Close() }()
	t.Cleanup(func() {
		select {
		case err := <-closed:
			if err != nil && !strings.Contains(err.Error(), "unsubscribe receipt timeout") {
				t.Errorf("closing the source: %v", err)
			}
		case <-time.After(35 * time.Second):
			t.Errorf("Source.Close never returned. It ends with pumpsWG.Wait(), so a pump still parked in " +
				"the slot wait is a Close that hangs for the life of the process")
		}
	})

	awaitSlotWaitersGone(t, waiting, "closing the source")
}

// TestASlotWaitEndsWhenTheSubscriptionIsReset is the supersede exit.
//
// A requeue is this driver's whole answer to Nack and it is an
// unsubscribe: `Source.reset` bumps `subGen`, invalidates the table and
// re-subscribes. The pump parked in the wait belongs to the generation that
// has just been superseded, so it must stop waiting -- and it is the
// invalidation, not the bump, that wakes it.
//
// The new subscription's redeliveries park a FRESH pump within microseconds, so
// the assertion is that the original goroutines left and not that nothing is
// waiting.
func TestASlotWaitEndsWhenTheSubscriptionIsReset(t *testing.T) {
	const prefetch = 4
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", prefetch)

	held, waiting := parkThePumpOnAFullTable(t, srv, src, consume, prefetch)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &held[0], nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack with requeue: %v", err)
	}
	await(t, "the subscription to be reset", func() bool { return src.Resets() > 0 })

	awaitSlotWaitersGone(t, waiting, "resetting the subscription")
}
