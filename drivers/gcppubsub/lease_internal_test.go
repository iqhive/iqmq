package gcppubsub

import (
	"context"
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
// slot is held, when the data held leaves no room for a message as large as
// the service takes, and while a callback with an ordering key is blocked,
// because the later messages of its key wait behind it in slots of their own.
func TestALeaseIsChargedOnlyWhileTheLibrarysFlowControlIsFull(t *testing.T) {
	const gap = time.Second
	for _, tc := range []struct {
		name   string
		slots  int
		bytes  int64
		size   int64
		keyed  bool
		charge bool
	}{
		{"a slot is left", 4, libraryOutstandingBytes, 10, false, false},
		{"every slot is held", 1, libraryOutstandingBytes, 10, false, true},
		{"the data held leaves room", 4, 2 * MaxMessageBytes, MaxMessageBytes - 1, false, false},
		{"the data held leaves no room", 4, 2 * MaxMessageBytes, MaxMessageBytes + 1, false, true},
		{"a callback with an ordering key is blocked", 4, libraryOutstandingBytes, 10, true, true},
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
			wait := now.Sub(a.enter(now, 10))
			switch {
			case tc.charge && wait < gap:
				t.Errorf("the next message is charged %v, want the %v the library can have held it back for", wait, gap)
			case !tc.charge && wait != 0:
				t.Errorf("the next message is charged %v, want nothing: the library had room for it", wait)
			}
		})
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
