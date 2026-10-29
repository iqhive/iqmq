package stomp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestAPumpReportsTheBrokersSentenceWhetherOrNotItIsStillTheLiveSubscription
// pins, which is one ordering inside [Source.pump]: the
// terminal-error branch above the supersession check rather than below it.
//
// The supersession check asks "am I still the current subscription?", and that
// is the right question about a MESSAGE -- a delivery from a subscription the
// broker has already taken back would be handed to a caller who cannot settle
// it. Asked about an ERROR it discards the only diagnosis STOMP offers, and it
// gets asked, because the pump's authority to speak is revocable by the OTHER
// observer of the same event: both brokers write the ERROR frame and then close
// the socket, `watchedConn` fires [Source.socketDied] inline on go-stomp's
// reader goroutine one read later, and `socketDied` clears `sub2` -- so the next
// fetch re-subscribes, [Source.subscribe] bumps `subGen`, and the pump carrying
// the sentence four goroutine hops behind has already been superseded.
// `TestCriticalRefusedSubscribeReportsNotFound` is what that cost: about one run
// in twenty-five reported its own `DeadlineExceeded` at the full budget with
// `lastErr` nil, while an instrumented run named the sentence in the counter
// that had dropped it.
//
// Both arms are asserted because they are two different rules and the fix is
// the LINE BETWEEN them, not either half: a superseded pump must report and must
// NOT repair, because the connection, the subscription and the buffer now belong
// to a later generation -- dropping them is and the specification's
// damage one hop along, and swapping the buffer strands whatever the live
// subscription has already buffered, which on this protocol comes back only on
// a reset. The third claim is the one the record-but-do-not-
// repair split makes necessary: only the pump that repaired the source may stop
// READING, because go-stomp can put a message after a terminal one and a pump
// that stopped would block `processLoop` for every subscription on the socket.
//
// It calls the drain and then the pump directly and feeds `Subscription.C`
// itself, so there is no clock, no socket and no second goroutine in it: what
// the pump leaves unread, on the channel or in its overflow, is a function of
// `which` against `subGen` and nothing else. That is deliberate -- the end-to-end
// version of this claim is the flake this fix exists to remove, and the specification records why it cannot be made to wait on an event.
func TestAPumpReportsTheBrokersSentenceWhetherOrNotItIsStillTheLiveSubscription(t *testing.T) {
	t.Parallel()
	const refusal = "AMQ339001 Destination does not exist orders::orders.workers"

	t.Run("superseded", func(t *testing.T) {
		t.Parallel()
		s := probeSource(t, 2)
		buffer, live := s.pending, s.sub2
		if left := runPump(t, s, 1, refusal); left != 0 {
			t.Errorf("a superseded pump stopped reading with %d message(s) left on its channel. go-stomp does not "+
				"always close `Subscription.C` after a terminal message -- `Unsubscribe`'s receipt timeout pushes "+
				"one and leaves it open -- and a pump that stopped would fill that channel and block `processLoop` "+
				"on its unbuffered handoff, stalling every subscription on the connection", left)
		}

		if got := s.takeErr(); got == nil || got.Error() != refusal {
			t.Fatalf("a superseded pump recorded %v as this source's terminal error, want %q. The ERROR frame's "+
				"`message` header is the whole of what STOMP says about a refusal, so a pump that drops it leaves "+
				"the fetch to report its own deadline about nothing in particular", got, refusal)
		}
		if s.buffer() != buffer {
			t.Errorf("a superseded pump replaced the live buffer. It belongs to a LATER subscription, and closing " +
				"it drops deliveries that come back only on a reset -- STOMP has no per-delivery redelivery " +
				"")
		}
		// `sub2` is the observable for the connection too, and deliberately so
		// rather than asserting `conn == nil` -- which this fixture starts at
		// nil, so that check could not fail. `dropConnection` clears `conn` and
		// `sub2` together under one lock, so a surviving subscription is proof
		// that the repair did not run.
		if s.sub2 != live {
			t.Errorf("a superseded pump retired the live subscription and, with it, the live connection: " +
				"`dropConnection` clears both. Neither is the one that died")
		}
	})

	t.Run("current", func(t *testing.T) {
		t.Parallel()
		s := probeSource(t, 1)
		buffer := s.pending
		if left := runPump(t, s, 1, refusal); left != 1 {
			t.Errorf("the live subscription's pump read on past the terminal error, leaving %d message(s) instead "+
				"of 1. It has just dropped the connection those messages arrived on, so anything after the error "+
				"belongs to a subscription that no longer exists", left)
		}

		if got := s.takeErr(); got == nil || got.Error() != refusal {
			t.Fatalf("the live subscription's pump recorded %v, want %q", got, refusal)
		}
		if s.buffer() == buffer {
			t.Errorf("the live subscription's pump left the buffer in place. Closing it is the ONLY thing that " +
				"wakes a parked fetch -- driverkit.PushAdapter has no other signal ")
		}
		if s.sub2 != nil {
			t.Errorf("the live subscription's pump left `sub2` set, so the next fetch would not re-subscribe")
		}
	})
}

// probeSource is the smallest Source a pump can run against: no backend
// connection, no socket, and a subscription generation the test chooses.
func probeSource(t *testing.T, subGen uint64) *Source {
	t.Helper()
	pending, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	table, err := driverkit.NewTokenTable(2)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	return &Source{
		b:        &Backend{},
		prefetch: 1,
		pending:  pending,
		table:    table,
		subGen:   subGen,
		// A non-nil subscription is what the superseded arm asserts is left
		// alone and the live arm asserts is retired.
		sub2:    &gostomp.Subscription{},
		closing: make(chan struct{}),
	}
}

// runPump hands a pump belonging to subscription generation `which` one
// terminal error followed by one ordinary message, waits for it to exit, and
// returns how much of the subscription it left unread.
//
// The trailing message is what makes "did it stop reading?" observable, and it
// is a real shape rather than a contrivance: `Subscription.Unsubscribe` pushes a
// terminal message on its receipt timeout WITHOUT closing the channel, so a
// message after a terminal error is something go-stomp can produce. The channel
// is closed so that the pump ends either way and the test cannot hang.
//
// The drain runs FIRST and to completion, so the whole channel is in the
// overflow before the pump takes anything: what is left unread is then the
// pump's decision alone, counted on the channel and in the queue together,
// with no interleaving between the two goroutines to make it a race.
func runPump(t *testing.T, s *Source, which uint64, message string) int {
	t.Helper()
	ch := make(chan *gostomp.Message, 2)
	ch <- &gostomp.Message{Err: errors.New(message)}
	ch <- &gostomp.Message{}
	close(ch)
	q := newOverflow(0, 0)
	s.pumpsWG.Add(2)
	s.drain(&gostomp.Subscription{C: ch}, q)
	s.pump(context.Background(), nil, q, which, 1)
	s.pumpsWG.Wait()
	return len(ch) + len(q.items)
}

// TestTheFirstFetchOnASourceThatWillNotRedialReportsTheBrokersReason is a
// broker's ERROR frame on a NoReconnect source, read by the fetch after it.
//
// The broker writes the frame and then closes the socket, and the two reach
// the source on different goroutines: the socket's death notice
// ([Source.socketDied]) marks the connection lost at once, and the pump reads
// the frame a few hops later and records the sentence. A fetch between the two
// found the source refusing to re-dial, read no recorded error, and reported
// "Config.NoReconnect makes that terminal" in place of the broker's reason,
// in 79 runs of 100 under the race detector. The fetch now waits for the dead
// subscription's pump before it answers.
//
// The source is built by hand and its pump fed by hand, so the fetch is in that
// gap for certain: the death has been noticed and the pump has read nothing.
func TestTheFirstFetchOnASourceThatWillNotRedialReportsTheBrokersReason(t *testing.T) {
	t.Parallel()
	const refusal = "AMQ339001 Destination does not exist orders::orders.workers"
	s, ch := deadSource(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fetched := make(chan error, 1)
	go func() {
		_, err := s.Fetch(ctx, make([]mq.DriverDelivery, 1))
		fetched <- err
	}()
	select {
	case err := <-fetched:
		t.Fatalf("the fetch answered %v before the pump of the subscription that died had read anything. The "+
			"broker's sentence is behind the death notice, so a fetch that answers here reports the source's own "+
			"refusal and the reason is lost", err)
	case <-time.After(50 * time.Millisecond):
	}
	ch <- &gostomp.Message{Err: errors.New(refusal)}
	err := <-fetched
	if got := mq.CodeOf(err); got != mq.CodeNotFound || !strings.Contains(err.Error(), "AMQ339001") {
		t.Fatalf("the first fetch reports %v (code %v), want NotFound with the broker's sentence", err, got)
	}

	next, cancelNext := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelNext()
	if _, err := s.Fetch(next, make([]mq.DriverDelivery, 1)); !refusesToRedial(err) {
		t.Errorf("the fetch after it reports %v, want the source's own non-retryable refusal: the sentence is "+
			"reported once", err)
	}
}

// deadSource is a NoReconnect source whose connection has died and whose
// pump, fed through the returned channel, has read nothing yet.
func deadSource(t *testing.T) (*Source, chan<- *gostomp.Message) {
	t.Helper()
	cfg := Config{Broker: activemq.Artemis, Addrs: []string{"127.0.0.1:1"}, NoReconnect: true}.resolve()
	b, err := newBackend(cfg, nil, driverkit.ClientOwned)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	s := probeSource(t, 1)
	s.b = b
	s.sub2 = nil
	s.dialled, s.lost = true, true
	s.stopPumps = make(chan struct{})

	ch := make(chan *gostomp.Message, 1)
	q := newOverflow(0, 0)
	done := make(chan struct{})
	s.pumpDone = done
	s.pumpsWG.Add(2)
	go s.drain(&gostomp.Subscription{C: ch}, q)
	go func() {
		defer close(done)
		s.pump(context.Background(), nil, q, 1, 0)
	}()
	t.Cleanup(func() {
		close(s.stopPumps)
		s.pumpsWG.Wait()
	})
	return s, ch
}

// TestAFetchWaitingForAPumpParkedOnAFullBufferIsNotHeld is the same wait when
// the pump of the subscription that died is parked pushing into a full buffer,
// which nothing fetches from while the source refuses to re-dial.
//
// The deliveries in that buffer came over the connection that died and can no
// longer be settled, so the fetch closes it. Without that the pump stayed
// parked and never read the subscription's terminal message: every fetch
// reported the source's own refusal, and a fetch that waited for the pump would
// have waited out its whole deadline instead.
func TestAFetchWaitingForAPumpParkedOnAFullBufferIsNotHeld(t *testing.T) {
	t.Parallel()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	b, err := OpenBackend(ctx, Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: time.Second,
		DialTimeout:    10 * time.Second,
		NoReconnect:    true,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	s, ok := opened.(*Source)
	if !ok {
		t.Fatalf("OpenSource returned %T", opened)
	}
	if err := srv.WaitSubscribed(s.Destination(), 1, 10*time.Second); err != nil {
		t.Fatalf("the driver never subscribed: %v", err)
	}
	// One delivery for the caller, one for the buffer, and one the pump holds
	// while it waits: first for a token, and once the death frees every token,
	// for room in the buffer.
	if _, err := srv.Deliver(s.Destination(),
		stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")},
		stompsrv.Message{Body: []byte("three")},
	); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if n, err := s.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 1 || err != nil {
		t.Fatalf("Fetch = %d, %v; want one delivery", n, err)
	}
	for deadline := time.Now().Add(10 * time.Second); s.TokenWaits() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the pump never waited for a token, so this fixture does not park it")
		}
	}
	// The server drops the source's socket, the newest one, so the source's own
	// death notice is what sees it.
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		s.mu.Lock()
		gone := s.conn == nil
		s.mu.Unlock()
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the source never noticed its socket's death")
		}
	}

	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer fcancel()
	start := time.Now()
	_, err = s.Fetch(fctx, make([]mq.DriverDelivery, 1))
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the fetch took %v: it waited for a pump nothing would unpark", took)
	}
	if err == nil || refusesToRedial(err) {
		t.Errorf("the first fetch after the socket died reports %v, want what the pump read when the connection "+
			"went, which the source's own refusal displaced", err)
	}
}

// refusesToRedial reports whether err is a source's own refusal to re-dial
// under NoReconnect. It reads the sentence, because errors.Is matches any
// error of the same code.
func refusesToRedial(err error) bool {
	return err != nil && !mq.IsRetryable(err) && strings.Contains(err.Error(), errSourceNoReconnect.Error())
}
