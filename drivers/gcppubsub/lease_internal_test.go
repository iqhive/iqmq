package gcppubsub

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestAMessageThatWaitedInTheLibraryKeepsTheLibrarysLease holds the lease of a
// message the client library pulled while the callback was blocked on a full
// ring.
//
// The library starts a message's extension budget when the streaming pull
// returns it, and hands it to the callback only once its flow control lets it,
// which is when a blocked callback returns. Its lease used to start when the
// callback was handed it, so a message that waited four seconds under a
// three-second budget was handed out with a full three seconds left, after the
// library had stopped extending it and the service could have given it to
// another consumer, whose work this delivery's Ack would then have
// acknowledged. Here the three messages arrive together: the first fills the
// ring, the second's callback blocks behind it, the third waits in the library,
// and nobody fetches for four seconds.
func TestAMessageThatWaitedInTheLibraryKeepsTheLibrarysLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		src := leaseSource(t, 1, 3*time.Second)
		pulled := time.Now()
		msgs := []*psapi.Incoming{src.incoming(t, "1"), src.incoming(t, "2"), src.incoming(t, "3")}
		src.admit(ctx, msgs[0])
		blocked := make(chan struct{})
		go func() {
			defer close(blocked)
			src.admit(ctx, msgs[1])
		}()
		// The library's flow control lets the third message through once the
		// second callback returns.
		go func() {
			<-blocked
			src.admit(ctx, msgs[2])
		}()
		synctest.Wait()
		if returned(blocked) {
			t.Fatal("the second callback returned with the ring full")
		}
		time.Sleep(4 * time.Second)

		live := src.fetchLive(t)
		for _, d := range live {
			if d.Info.LeaseExpires.After(pulled.Add(src.budget)) {
				t.Errorf("message %s is handed out with its lease ending %v after the pull, past the %v the "+
					"library extends it for", d.Ref.(*delivery).id, d.Info.LeaseExpires.Sub(pulled), src.budget)
			}
		}
		if len(live) != 0 {
			t.Errorf("%d delivery(ies) handed out, want none: every message's budget ended a second ago", len(live))
		}
		// The first two reached the callback at the pull, so the library let
		// them go when their budget ended and the service may have given them
		// to another consumer already. As far as the driver can tell, the
		// third may have been pulled just before its callback, so the library
		// may still be extending it, and it goes back to the library.
		for i, want := range []int32{0, 0, 1} {
			if got := msgs[i].Settle.(*recordedSettlement).nacks.Load(); got != want {
				t.Errorf("message %s was nacked %d time(s), want %d", msgs[i].ID, got, want)
			}
		}
	})
}

// TestAMessagePulledAfterTheLibraryWasFreeHasItsWholeLease is the other side:
// the wait a lease is charged with ends where the library stops waiting. A
// message the library pulls once nothing has held it back for a while reached
// the callback straight away, and charging it with the wait of the messages
// before it would shorten its lease for nothing.
func TestAMessagePulledAfterTheLibraryWasFreeHasItsWholeLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		src := leaseSource(t, 1, 3*time.Second)
		src.admit(ctx, src.incoming(t, "1"))
		go src.admit(ctx, src.incoming(t, "2"))
		synctest.Wait()
		time.Sleep(2 * time.Second)
		dst := make([]mq.DriverDelivery, 1)
		for range 2 {
			if n, err := src.ring.Fetch(ctx, dst); n != 1 {
				t.Fatalf("taking a delivery out of the ring: %d, %v", n, err)
			}
			synctest.Wait()
		}
		time.Sleep(idleGap)
		pulled := time.Now()
		src.admit(ctx, src.incoming(t, "3"))
		live := src.fetchLive(t)
		if len(live) != 1 {
			t.Fatalf("%d delivery(ies) handed out, want the third message", len(live))
		}
		if got, want := live[0].Info.LeaseExpires, pulled.Add(src.budget); !got.Equal(want) {
			t.Errorf("the message pulled after the library was free reports a lease of %v, want %v",
				got.Sub(pulled), src.budget)
		}
	})
}

// TestASteadyStreamUnderASmallByteBudgetKeepsItsWholeLease holds the arrivals
// account to the flow control the client library has when
// Config.MaxOutstandingBytes is no larger than the largest message the service
// takes. The library counts a message's own data against the budget, so a
// small message has room while another is in flight. The account used to ask
// whether there was room for the largest message instead, which under such a
// budget is any data at all: every callback that returned looked like the flow
// control getting room back, and the next message, pulled 40 ms later, was
// charged the gap. Over the window those gaps added up to the whole budget, and
// from then on every message was dropped as expired and nacked, a hot loop that
// spends a dead-letter attempt each time.
func TestASteadyStreamUnderASmallByteBudgetKeepsItsWholeLease(t *testing.T) {
	for _, tc := range []struct {
		prefetch int
		budget   time.Duration
		messages int
	}{
		{64, 10 * time.Second, 600},
		{1000, time.Minute, 5000},
	} {
		t.Run(fmt.Sprintf("prefetch-%d", tc.prefetch), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				src := leaseSource(t, tc.prefetch, tc.budget)
				src.arrivals.bytes = 1 << 20
				ctx := context.Background()
				// A done context makes Fetch take what the ring holds and
				// return, so the ring is drained after every message.
				done, cancel := context.WithCancel(ctx)
				cancel()
				dst := make([]mq.DriverDelivery, tc.prefetch)
				var short time.Duration
				dropped := 0
				for i := range tc.messages {
					in := src.incoming(t, strconv.Itoa(i))
					pulled := time.Now()
					src.admit(ctx, in)
					if in.Settle.(*recordedSettlement).nacks.Load() > 0 {
						dropped++
					}
					n, _ := src.Fetch(done, dst)
					for _, d := range dst[:n] {
						short = max(short, pulled.Add(tc.budget).Sub(d.Info.LeaseExpires))
					}
					time.Sleep(40 * time.Millisecond)
				}
				if short > 0 || dropped > 0 {
					t.Errorf("a library that held nothing back was charged: a lease came out %v short of the %v "+
						"budget, and %d of %d messages were dropped as expired and nacked", short, tc.budget, dropped,
						tc.messages)
				}
			})
		})
	}
}

// TestTheWaitChargedToALeaseCoversTheWindowAndNoMore holds the arrivals
// account to its two bounds while the library's flow control stays full.
// Every gap among the last window callbacks is charged, so a message as far
// into a pull as the window reaches is not handed out with a lease the library
// has stopped keeping. Nothing older than twice the window is, so a ring that
// stays full under a service sending more than the prefetch does not make
// every later message look as old as the first.
func TestTheWaitChargedToALeaseCoversTheWindowAndNoMore(t *testing.T) {
	const window, gap = 4, time.Second
	a := arrivals{window: window, slots: 1, bytes: libraryOutstandingBytes}
	start := time.Unix(1_000_000, 0)
	a.enter(start, 0)
	for i := 1; i <= 5*window; i++ {
		now := start.Add(time.Duration(i) * gap)
		wait := now.Sub(a.enter(now, 0))
		if low := time.Duration(min(i, window)) * gap; wait < low {
			t.Errorf("callback %d is charged %v, less than the last %d gaps", i, wait, min(i, window))
		}
		if wait > 2*window*gap {
			t.Errorf("callback %d is charged %v, more than twice the window of %d gaps", i, wait, window)
		}
	}
}

// TestALeaseIsChargedOnlyWhileTheLibrarysFlowControlIsFull holds the arrivals
// account to the one state in which the client library can be holding a
// message back. A callback blocked on a full ring that leaves room in the flow
// control holds nothing back, so the message the library hands over next was
// pulled for it and keeps its whole lease. The flow control is full when every
// slot is held, when the data held leaves no room for the message handed over,
// counted as the library counts it, by its own data and never more than the
// whole budget, and while a callback with an ordering key is blocked, because
// the later messages of its key wait behind it in slots of their own.
func TestALeaseIsChargedOnlyWhileTheLibrarysFlowControlIsFull(t *testing.T) {
	const gap = time.Second
	for _, tc := range []struct {
		name   string
		slots  int
		bytes  int64
		size   int64
		next   int64
		keyed  bool
		charge bool
	}{
		{"a slot is left", 4, libraryOutstandingBytes, 10, 10, false, false},
		{"every slot is held", 1, libraryOutstandingBytes, 10, 10, false, true},
		{"the data held leaves room", 4, 2 * MaxMessageBytes, MaxMessageBytes - 1, MaxMessageBytes, false, false},
		{"the data held leaves no room", 4, 2 * MaxMessageBytes, MaxMessageBytes + 1, MaxMessageBytes, false, true},
		{"a small budget leaves room for a small message", 4, 1 << 20, 10, 10, false, false},
		{"a message larger than the budget needs all of it", 4, 1 << 20, 10, 2 << 20, false, true},
		{"a callback with an ordering key is blocked", 4, libraryOutstandingBytes, 10, 10, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := arrivals{window: 2 * tc.slots, slots: tc.slots, bytes: tc.bytes}
			start := time.Unix(1_000_000, 0)
			// One callback is handed its message and blocks on the ring.
			a.enter(start, tc.size)
			if tc.keyed {
				a.park()
			}
			now := start.Add(gap)
			wait := now.Sub(a.enter(now, tc.next))
			switch {
			case tc.charge && wait < gap:
				t.Errorf("the next message is charged %v, want the %v the library can have held it back for", wait, gap)
			case !tc.charge && wait != 0:
				t.Errorf("the next message is charged %v, want nothing: the library had room for it", wait)
			}
		})
	}
}

// TestAMessageHandedOverBehindOneTheLibraryHeldBackIsChargedWithIt holds the
// other side of counting room by each message's own data. The library hands
// messages over in order, so a small message with room of its own still waited
// behind a large one the flow control held back, and is handed over straight
// after it. It is charged with the large one's wait. One handed over more than
// idleGap later came from a pull of its own and keeps its whole lease. Several
// callbacks returning at once, as a Fetch that drains a full ring makes them
// do, are counted by the first, which is the one that gave the library room:
// the ones after it return to a flow control that already has some.
//
// The messages one return lets through run on goroutines of their own, so
// they reach the account in any order, and the small one often comes first.
// Judged by its own size it had room, started the sum again, and left the
// large one, and itself, charged nothing for the seconds both had waited. That
// holds for a large message larger than any the account has seen as well.
func TestAMessageHandedOverBehindOneTheLibraryHeldBackIsChargedWithIt(t *testing.T) {
	const gap = time.Second
	start := time.Unix(1_000_000, 0)
	for _, tc := range []struct {
		name                  string
		leaving, held, behind int64
	}{
		// The one returning is the largest the account has seen.
		{"handed over together", 669473, 385761, 165914},
		// The one held back is larger than any the account has seen.
		{"handed over together, the one held back larger than any seen", 300 << 10, 800 << 10, 3000},
	} {
		for _, smallFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, small first %v", tc.name, smallFirst), func(t *testing.T) {
				a := arrivals{window: 8, slots: 4, bytes: 1 << 20}
				a.enter(start, tc.leaving)
				at := start.Add(gap)
				a.leave(at, tc.leaving)
				var big, small time.Duration
				if smallFirst {
					small = at.Sub(a.enter(at, tc.behind))
					big = at.Sub(a.enter(at, tc.held))
				} else {
					big = at.Sub(a.enter(at, tc.held))
					small = at.Sub(a.enter(at, tc.behind))
				}
				if big < gap || small < gap {
					t.Errorf("the message the flow control held back is charged %v and the one behind it %v, want "+
						"the %v both waited", big, small, gap)
				}
			})
		}
	}
	t.Run("a small message behind a large one", func(t *testing.T) {
		a := arrivals{window: 8, slots: 4, bytes: 1 << 20}
		a.enter(start, 900<<10)
		a.leave(start.Add(gap), 900<<10)
		at := start.Add(gap)
		if wait := at.Sub(a.enter(at, 200<<10)); wait < gap {
			t.Errorf("the message the library had no room for is charged %v, want the %v it waited", wait, gap)
		}
		at = at.Add(time.Millisecond)
		if wait := at.Sub(a.enter(at, 10)); wait < gap {
			t.Errorf("the small message handed over behind it is charged %v, want the %v it waited too", wait, gap)
		}
		at = at.Add(idleGap)
		if wait := at.Sub(a.enter(at, 10)); wait != 0 {
			t.Errorf("a message handed over %v later is charged %v, want nothing: it came from a pull of its own",
				idleGap, wait)
		}
	})
	t.Run("callbacks returning together", func(t *testing.T) {
		a := arrivals{window: 8, slots: 4, bytes: libraryOutstandingBytes}
		for range 4 {
			a.enter(start, 10)
		}
		at := start.Add(gap)
		for i := range 4 {
			a.leave(at.Add(time.Duration(i)*time.Microsecond), 10)
		}
		at = at.Add(time.Millisecond)
		if wait := at.Sub(a.enter(at, 10)); wait < gap {
			t.Errorf("the message the first return let through is charged %v, want the %v every slot was held", wait, gap)
		}
	})
}

// TestAStreamOfMessagesTooLargeToShareTheByteBudgetIsChargedItsGaps pins the
// limit the arrivals account documents. When two messages of the recent
// largest size do not fit in MaxOutstandingBytes together, a callback that
// returns leaves no room for the next one, and the account cannot tell a
// message the library held back from one pulled after: a stream of them closer
// together than idleGap is charged every gap. A stream a little further apart
// is not, and neither is one whose messages fit two at a time. The test fails
// when that changes, so that the documentation changes with it.
func TestAStreamOfMessagesTooLargeToShareTheByteBudgetIsChargedItsGaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int64
		every  time.Duration
		charge bool
	}{
		{"more than half the budget, closer together than idleGap", 600 << 10, 40 * time.Millisecond, true},
		{"more than half the budget, further apart than idleGap", 600 << 10, 60 * time.Millisecond, false},
		{"less than half the budget, closer together than idleGap", 400 << 10, 40 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := arrivals{window: 128, slots: 64, bytes: 1 << 20}
			at := time.Unix(1_000_000, 0)
			var wait time.Duration
			for range 10 {
				wait = at.Sub(a.enter(at, tc.size))
				a.leave(at, tc.size)
				at = at.Add(tc.every)
			}
			switch {
			case tc.charge && wait < 9*tc.every:
				t.Errorf("the tenth message is charged %v, want the nine gaps before it, %v", wait, 9*tc.every)
			case !tc.charge && wait != 0:
				t.Errorf("the tenth message is charged %v, want nothing", wait)
			}
		})
	}
}

// TestTheReturnsKeptAreTheMostHeldWithinIdleGap holds the record arrivals keeps
// of the flow control's returns to a plain one: the most it held immediately
// before any return within idleGap. The record drops a return a later one
// outlasts, and moves its head past the expired ones rather than moving the
// returns themselves, which made a long run of returns, each holding less than
// the one before, cost every return the length of the run.
func TestTheReturnsKeptAreTheMostHeldWithinIdleGap(t *testing.T) {
	type held struct {
		at    time.Time
		bytes int64
	}
	rng := rand.New(rand.NewPCG(1, 2))
	a := arrivals{window: 64, slots: 1 << 20, bytes: libraryOutstandingBytes}
	var all []held
	at := time.Unix(1_000_000, 0)
	for i := range 20_000 {
		at = at.Add(time.Duration(rng.Int64N(int64(2 * time.Millisecond))))
		if rng.IntN(3) > 0 {
			a.data = rng.Int64N(1 << 20)
			all = append(all, held{at, a.data})
			a.room(at)
			continue
		}
		var want int64
		for _, h := range all {
			if at.Sub(h.at) < idleGap {
				want = max(want, h.bytes)
			}
		}
		if got := a.returned(at); got != want {
			t.Fatalf("step %d: the record says the flow control held %d within idleGap, want %d", i, got, want)
		}
	}
}

// leaseSource is a source over a ring of the given capacity whose deliveries
// have the given budget, with no receiver: the test is the client library and
// calls admit itself.
func leaseSource(t *testing.T, prefetch int, budget time.Duration) *source {
	t.Helper()
	ring, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: prefetch})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	src := &source{
		b:        &Backend{cfg: Config{ProjectID: "p"}, errs: newErrorMapper()},
		cfg:      mq.SourceConfig{Topic: "t", Subscription: "s"},
		enc:      mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataPortableEnvelope),
		ring:     ring,
		deadline: time.Second,
		budget:   budget,
		inflight: map[string]*delivery{},
		holds:    map[*delivery]mq.Timer{},
		stop:     make(chan struct{}),
		arrivals: arrivals{window: 2 * prefetch, slots: prefetch, bytes: libraryOutstandingBytes},
	}
	src.set = newSettler(src)
	return src
}

// incoming is one decodable message as the client library hands it over.
func (s *source) incoming(t *testing.T, id string) *psapi.Incoming {
	t.Helper()
	out, err := s.enc.encode(&mq.Message{Body: []byte(id)}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return &psapi.Incoming{ID: id, Data: out.Out.Data, Attributes: out.Out.Attributes, Settle: &recordedSettlement{}}
}

// fetchLive is the application fetching until nothing more can arrive: what
// Fetch hands out, once every callback in the bubble has pushed or is blocked
// and the ring is empty.
func (s *source) fetchLive(t *testing.T) []mq.DriverDelivery {
	t.Helper()
	// A done context makes Fetch take what the ring holds and return rather
	// than wait for more.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var live []mq.DriverDelivery
	dst := make([]mq.DriverDelivery, 4)
	for {
		synctest.Wait()
		if s.ring.Len() == 0 {
			return live
		}
		n, _ := s.Fetch(ctx, dst)
		live = append(live, dst[:n]...)
	}
}

// returned reports whether done is closed, without waiting.
func returned(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
