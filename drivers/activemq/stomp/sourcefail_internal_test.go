package stomp

import (
	"context"
	"errors"
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"

	"github.com/iqhive/iqmq/driverkit"
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
// It calls the pump directly and feeds `Subscription.C` itself, so there is no
// clock, no socket and no second goroutine in it: the outcome is a function of
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
// returns how much of the channel it left unread.
//
// The trailing message is what makes "did it stop reading?" observable, and it
// is a real shape rather than a contrivance: `Subscription.Unsubscribe` pushes a
// terminal message on its receipt timeout WITHOUT closing the channel, so a
// message after a terminal error is something go-stomp can produce. The channel
// is closed so that the pump ends either way and the test cannot hang.
func runPump(t *testing.T, s *Source, which uint64, message string) int {
	t.Helper()
	ch := make(chan *gostomp.Message, 2)
	ch <- &gostomp.Message{Err: errors.New(message)}
	ch <- &gostomp.Message{}
	close(ch)
	s.pumpsWG.Add(1)
	s.pump(context.Background(), nil, &gostomp.Subscription{C: ch}, which, 1)
	s.pumpsWG.Wait()
	return len(ch)
}
