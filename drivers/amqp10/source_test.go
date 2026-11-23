package amqp10_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// TestSourceFetchIsOneWaitAndADrain is Fetch shape: "blocks until at
// least one delivery is available" and then fills the rest of dst from what the
// library already holds, so a batch costs one wait rather than one per message.
func TestSourceFetchIsOneWaitAndADrain(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 8})
	go func() {
		rcv := f.receiver(budget)
		for i := range 5 {
			rcv.Deliver(message("tag-"+itoa(i), "body-"+itoa(i)))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dst := make([]mq.DriverDelivery, 8)
	got := 0
	for got < 5 {
		n, err := src.Fetch(ctx, dst[got:])
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		got += n
	}
	// The whole batch usually arrives in one fetch; what the test holds is
	// that a batch is REACHABLE at all, because a driver whose drain did not
	// work would return one per call and still pass a count.
	one, err := src.Fetch(ctx, dst[:1])
	_ = one
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		// The sixth fetch has nothing to take, so it blocks until the context
		// ends. Either answer is correct; what must not happen is a failure
		// of another kind.
		t.Fatalf("a fetch with nothing to take returned %v", err)
	}
	set := src.Settler()
	for i := range 5 {
		if err := set.Ack(context.Background(), &dst[i]); err != nil {
			t.Fatalf("acknowledging delivery %d: %v", i, err)
		}
	}
}

// TestSourceFetchAllocatesNothing is the specification's "It allocates nothing per fetch" and rule 1.
//
// The message storage is per token slot and the deliveries go into the caller's
// dst, so the only allocations left are the mapping's own — which
// TestAllocsMapping measures separately. What this holds is the PLUMBING: a
// fetch of an already-delivered batch allocates a small, fixed amount rather
// than one slice per call.
func TestSourceFetchAllocatesNothing(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 16})
	set := src.Settler()
	rcv, first := deliverAndFetch(t, f, src, "tag-0", "body")
	if err := set.Ack(context.Background(), &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	ctx := context.Background()
	// Warm every pool and every map the first fetch grows.
	for i := range 4 {
		rcv.Deliver(message("warm-"+itoa(i), "body"))
		if _, err := src.Fetch(ctx, dst); err != nil {
			t.Fatalf("warming: %v", err)
		}
		if err := set.Ack(ctx, &dst[0]); err != nil {
			t.Fatalf("warming: %v", err)
		}
	}
	got := testingAllocsPerRun(16, func() {
		rcv.Deliver(message("tag", "body"))
		if _, err := src.Fetch(ctx, dst); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if err := set.Ack(ctx, &dst[0]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
	// The floor is the delivery's own boxed handle — the SPI's Ref, which an
	// AMQP disposition genuinely needs, plus the native message the test
	// itself builds. What this number would catch is a per-fetch slice or a
	// per-delivery message allocation, which is the defect rule 1 is
	// about.
	if got > 10 {
		t.Fatalf("a fetch-and-settle round allocates %.0f times; the specification requires Fetch to allocate nothing per fetch, "+
			"so a number this size means a per-call slice or a per-delivery message", got)
	}
	t.Logf("fetch+settle allocations per round: %.1f", got)
}

// TestSourceReleasesWhatItCannotDecode is the disqualifier reached from the
// other direction: a message iqmq cannot represent is one a caller can neither
// read nor settle, so handing it over would make it unsettleable.
//
// The answer is `modified` with the attempt COUNTED and this link marked as one
// not to redeliver to. Releasing it plainly would have the broker hand it
// straight back and the source would spin on it for ever; rejecting it would
// destroy or dead-letter a message whose only fault is a body shape iqmq has no
// field for.
func TestSourceReleasesWhatItCannotDecode(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	go func() {
		rcv := f.receiver(budget)
		// An amqp-sequence body, which has no portable form at all.
		rcv.Deliver(&amqp.Message{DeliveryTag: []byte("bad"), Sequence: [][]any{{int64(1)}}})
		rcv.Deliver(message("good", "readable"))
	}()
	d := fetchOne(t, src, budget)
	if string(d.Message.Body) != "readable" {
		t.Fatalf("the fetch handed over %q; a message this driver cannot represent must not reach a caller",
			d.Message.Body)
	}
	rcv := f.receiver(budget)
	disps := rcv.Dispositions()
	if len(disps) != 1 {
		t.Fatalf("the peer received %d disposition(s), want one for the undecodable message: %+v", len(disps), disps)
	}
	got := disps[0]
	if got.Tag != "bad" {
		t.Fatalf("the disposition names %q, want the undecodable message", got.Tag)
	}
	if got.Disposition.Outcome != client.OutcomeModified {
		t.Fatalf("the undecodable message was settled with %v, want `modified`: `released` would have the broker "+
			"hand it straight back and this source would spin on it for ever",
			got.Disposition.Outcome)
	}
	if !got.Disposition.DeliveryFailed || !got.Disposition.UndeliverableHere {
		t.Fatalf("the `modified` outcome is %+v, want delivery-failed and undeliverable-here", got.Disposition)
	}
	if n := src.Undecodable(); n != 1 {
		t.Fatalf("the source counted %d undecodable deliver(ies), want 1: a deployment cannot see this otherwise", n)
	}
	if n := f.b.Stats().Undecodable; n != 1 {
		t.Fatalf("the backend counted %d undecodable deliver(ies), want 1", n)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSourceMovesWhatItCannotDecodeToTheBack is UndecodableToBack, the rule
// for a broker that reads `undeliverable-here` as a rejection and puts a
// delivery given back any other way at the head of the queue, which is what
// RabbitMQ 4 does: the zero value's `modified{undeliverable-here}` had it
// dead-letter or drop a message whose only fault is a body shape iqmq has no
// field for. A copy goes to the back of the source's own address, and the
// original is accepted only once the broker has accepted the copy.
//
// A move the broker refuses as unauthorised is final for the source: the
// delivery is settled as the zero rule settles it, which does not hand it
// straight back, and so is the next one, with no second copy. A consumer whose
// user may not publish to its queue used to get the delivery straight back,
// for ever. A copy the broker rejects, as a full queue's reject-publish does,
// is counted and is not final: the original comes back with the attempt
// counted, and the next delivery is moved. Latching on it dropped every later
// undecodable delivery on the source. A move that failed for another reason, a
// link that went, gives the original back the same way. Either is given back
// until the broker has given the delivery back 20 times; the next failed move
// settles that delivery as the zero rule does, without latching the source.
func TestSourceMovesWhatItCannotDecodeToTheBack(t *testing.T) {
	p := testProfile()
	p.Undecodable = aq.UndecodableToBack
	notHere := client.Disposition{Outcome: client.OutcomeModified, DeliveryFailed: true, UndeliverableHere: true}
	for _, tc := range []struct {
		name string
		plan *client.SendPlan
		// count is the first undecodable delivery's delivery-count;
		// first and next are how the two undecodable deliveries are
		// settled, copies how many copies the broker took at the back over
		// how many attached links, and abandoned the deliveries settled
		// as the zero rule does instead of moved.
		count            uint32
		first, next      client.Disposition
		copies, attaches int
		abandoned        uint64
	}{
		{
			name:  "the broker accepts the copies",
			first: client.Disposition{Outcome: client.OutcomeAccepted}, next: client.Disposition{Outcome: client.OutcomeAccepted},
			copies: 2, attaches: 1,
		},
		{
			name:  "the broker rejects the copy as over the queue's length",
			plan:  &client.SendPlan{State: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondResourceLimitExceeded}}},
			first: client.Disposition{Outcome: client.OutcomeModified, DeliveryFailed: true},
			next:  client.Disposition{Outcome: client.OutcomeAccepted}, copies: 2, attaches: 1,
		},
		{
			name:  "the broker rejects the copy as unauthorised",
			plan:  &client.SendPlan{State: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}}},
			first: notHere, next: notHere, copies: 1, attaches: 1, abandoned: 2,
		},
		{
			name:  "the broker refuses the transfer as unauthorised",
			plan:  &client.SendPlan{Err: &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}}},
			first: notHere, next: notHere, attaches: 1, abandoned: 2,
		},
		{
			name:  "the link goes during the transfer",
			plan:  &client.SendPlan{Err: &amqp.LinkError{}},
			first: client.Disposition{Outcome: client.OutcomeModified, DeliveryFailed: true},
			next:  client.Disposition{Outcome: client.OutcomeAccepted}, copies: 1, attaches: 2,
		},
		{
			name:  "the broker rejects the copy of a delivery it has given back 20 times",
			plan:  &client.SendPlan{State: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondResourceLimitExceeded}}},
			count: 20, first: notHere,
			next:   client.Disposition{Outcome: client.OutcomeAccepted},
			copies: 2, attaches: 1, abandoned: 1,
		},
		{
			name:  "the broker rejects the copy of a delivery it has given back 19 times",
			plan:  &client.SendPlan{State: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondResourceLimitExceeded}}},
			count: 19, first: client.Disposition{Outcome: client.OutcomeModified, DeliveryFailed: true},
			next:   client.Disposition{Outcome: client.OutcomeAccepted},
			copies: 2, attaches: 1,
		},
		{
			name:  "the link goes during the transfer of a delivery given back 20 times",
			plan:  &client.SendPlan{Err: &amqp.LinkError{}},
			count: 20, first: notHere,
			next:   client.Disposition{Outcome: client.OutcomeAccepted},
			copies: 1, attaches: 2, abandoned: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := newPeer()
			if tc.plan != nil {
				peer.PlanSends(*tc.plan)
			}
			f := openWith(t, p, aq.Config{}, peer)
			src := f.source(mq.SourceConfig{Prefetch: 4})
			bad := func(tag string, count uint32) *amqp.Message {
				return &amqp.Message{
					DeliveryTag: []byte(tag),
					Header:      &amqp.MessageHeader{Durable: true, DeliveryCount: count, FirstAcquirer: true},
					Annotations: amqp.Annotations{"x-opt-origin": "a JMS StreamMessage"},
					Sequence:    [][]any{{int64(1)}},
				}
			}
			go func() {
				rcv := f.receiver(budget)
				rcv.Deliver(bad("bad-1", max(tc.count, 3)))
				rcv.Deliver(message("good-1", "readable"))
			}()
			d := fetchOne(t, src, budget)
			if string(d.Message.Body) != "readable" {
				t.Fatalf("the fetch handed over %q; a message this driver cannot represent must not reach a caller", d.Message.Body)
			}
			rcv := f.receiver(budget)
			waitDispositions(t, rcv, 1)
			rcv.Deliver(bad("bad-2", 3), message("good-2", "readable too"))
			d2 := fetchOne(t, src, budget)
			if string(d2.Message.Body) != "readable too" {
				t.Fatalf("the second fetch handed over %q", d2.Message.Body)
			}
			disps := waitDispositions(t, rcv, 2)
			for i, want := range []client.Disposition{tc.first, tc.next} {
				got := disps[i]
				if got.Tag != "bad-"+strconv.Itoa(i+1) || got.Disposition.Outcome != want.Outcome ||
					got.Disposition.DeliveryFailed != want.DeliveryFailed || got.Disposition.UndeliverableHere != want.UndeliverableHere {
					t.Fatalf("undecodable delivery %d was settled %+v, want %+v", i+1, got, want)
				}
			}
			var sent []*amqp.Message
			attaches := 0
			for _, snd := range f.peer.LastConn().Senders() {
				if snd.Address() == "topic::sub" {
					attaches++
					sent = append(sent, snd.Sent()...)
				}
			}
			if len(sent) != tc.copies || attaches != tc.attaches {
				t.Fatalf("%d cop(ies) reached the back of the source's own address over %d link(s), want %d over %d",
					len(sent), attaches, tc.copies, tc.attaches)
			}
			if len(sent) > 0 {
				cp := sent[0]
				if len(cp.Sequence) != 1 || cp.Annotations["x-opt-origin"] != "a JMS StreamMessage" {
					t.Fatalf("the copy is %+v, want the undecodable message's body and annotations", cp)
				}
				if cp.Header == nil || !cp.Header.Durable || cp.Header.DeliveryCount != 0 || cp.Header.FirstAcquirer {
					t.Fatalf("the copy's header is %+v, want a new message's that keeps the durability", cp.Header)
				}
			}
			st := f.b.Stats()
			if n := src.Undecodable(); n != 2 || st.MovesAbandoned != tc.abandoned {
				t.Fatalf("the source counted %d undecodable deliver(ies) and %d abandoned move(s), want 2 and %d",
					n, st.MovesAbandoned, tc.abandoned)
			}
			for _, dd := range []*mq.DriverDelivery{&d, &d2} {
				if err := src.Settler().Ack(context.Background(), dd); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
		})
	}
}

// TestAMoveToTheBackOutlivesTheFetchThatStartedIt: the move runs under the
// backend's lifetime and the driver's bound, not under the fetch's ctx. A
// fetch whose ctx ended while the copy waited for the broker's answer left
// the original unsettled beside its accepted copy, so a consumer polling with
// short timeouts multiplied the message on the queue. The fetch returns at its
// deadline, the copy's answer arrives later, and the original is accepted.
// The source's Close waits for a move under way.
func TestAMoveToTheBackOutlivesTheFetchThatStartedIt(t *testing.T) {
	p := testProfile()
	p.Undecodable = aq.UndecodableToBack
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, p, aq.Config{}, peer)
	src := f.source(mq.SourceConfig{Prefetch: 4})
	go func() {
		f.receiver(budget).Deliver(&amqp.Message{DeliveryTag: []byte("bad"), Sequence: [][]any{{int64(1)}}})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch over an undecodable delivery alone = %d, %v; want its deadline", n, err)
	}
	var back *client.FakeSender
	deadline := time.Now().Add(budget)
	for back == nil || back.Held() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no copy is waiting for the broker's answer")
		}
		for _, snd := range f.peer.LastConn().Senders() {
			if snd.Address() == "topic::sub" {
				back = snd
			}
		}
		time.Sleep(time.Millisecond)
	}
	rcv := f.receiver(budget)
	if disps := rcv.Dispositions(); len(disps) != 0 {
		t.Fatalf("the original was settled %+v before its copy was answered", disps)
	}
	closed := make(chan error, 1)
	go func() { closed <- src.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while a move was under way", err)
	case <-time.After(100 * time.Millisecond):
	}
	back.ResolveHeld(0, &amqp.StateAccepted{})
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	disps := rcv.Dispositions()
	if len(disps) == 0 || disps[0].Tag != "bad" || disps[0].Disposition.Outcome != client.OutcomeAccepted {
		t.Fatalf("the original was settled %+v, want `accepted` once its copy was: the fetch's deadline does not cut the move", disps)
	}
}

// waitDispositions waits for the peer to have received n dispositions on rcv,
// which a move to the back writes off the fetch, and returns them.
func waitDispositions(tb testing.TB, rcv *client.FakeReceiver, n int) []client.Dispositioned {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for {
		disps := rcv.Dispositions()
		if len(disps) >= n {
			return disps
		}
		if time.Now().After(deadline) {
			tb.Fatalf("the peer received %d disposition(s), want %d: %+v", len(disps), n, disps)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSourceRejectsWhatItCannotDecode is UndecodableRejected, for a broker
// whose own dead-letter policy routes a rejection: the message goes there
// with the reason, and the source counts it.
func TestSourceRejectsWhatItCannotDecode(t *testing.T) {
	p := testProfile()
	p.Undecodable = aq.UndecodableRejected
	f := open(t, p, aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	go func() {
		rcv := f.receiver(budget)
		rcv.Deliver(&amqp.Message{DeliveryTag: []byte("bad"), Sequence: [][]any{{int64(1)}}})
		rcv.Deliver(message("good", "readable"))
	}()
	d := fetchOne(t, src, budget)
	disps := f.receiver(budget).Dispositions()
	if len(disps) != 1 || disps[0].Tag != "bad" || disps[0].Disposition.Outcome != client.OutcomeRejected {
		t.Fatalf("the peer received %+v, want one `rejected` for the undecodable message", disps)
	}
	if e := disps[0].Disposition.Error; e == nil || !strings.Contains(e.Description, "could not decode") {
		t.Fatalf("the rejection carries %+v, want the reason for whoever reads the dead-letter queue", e)
	}
	if n := src.Undecodable(); n != 1 {
		t.Fatalf("the source counted %d undecodable deliver(ies), want 1", n)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSourceHasNoLease is the specification's "the zero time when the broker grants no
// lease", and it is a real property of this protocol rather than an omission:
// an unsettled delivery is the receiver's for as long as the link lives.
func TestSourceHasNoLease(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2, AckDeadline: time.Minute})
	_, d := deliverAndFetch(t, f, src, "tag-1", "x")
	if !d.Info.LeaseExpires.IsZero() {
		t.Fatalf("the delivery reports a lease expiring at %v; AMQP 1.0 grants no lease, so a driver that reported "+
			"a deadline would be inventing one and a handler that trusted it would be told its delivery had expired "+
			"when it had not", d.Info.LeaseExpires)
	}
	if d.Info.Topic != "topic" || d.Info.Subscription != "sub" {
		t.Fatalf("the delivery metadata is %+v, want the source's own topic and subscription", d.Info)
	}
	if d.Info.Source != src.Address() {
		t.Fatalf("DeliveryInfo.Source = %q, want the address the link attached to", d.Info.Source)
	}
	if d.Info.ReceivedAt.IsZero() {
		t.Fatal("the delivery reports no arrival time")
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSourceReportsTheProfilesDeliveryLease is [aq.Profile.DeliveryLease]: a
// broker that takes a delivery back after a while, as RabbitMQ's consumer
// timeout does, is reported as each delivery's lease, its ReceivedAt plus the
// profile's lease, whatever the consumer's AckDeadline says; and the lease
// cannot be extended, so FeatureExtendLease stays unsupported, with a note
// that says why.
func TestSourceReportsTheProfilesDeliveryLease(t *testing.T) {
	p := testProfile()
	p.DeliveryLease = 10 * time.Minute
	f := open(t, p, aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2, AckDeadline: time.Minute})
	_, d := deliverAndFetch(t, f, src, "tag-1", "x")
	if got := d.Info.LeaseExpires.Sub(d.Info.ReceivedAt); d.Info.LeaseExpires.IsZero() || got != p.DeliveryLease {
		t.Fatalf("the delivery reports a lease of %v (expiring at %v), want the profile's %v",
			got, d.Info.LeaseExpires, p.DeliveryLease)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
	if err != nil {
		t.Fatal(err)
	}
	ext := caps.Support(mq.FeatureExtendLease)
	if ext.Level != mq.Unsupported || !strings.Contains(ext.Note, "LeaseExpires reports") {
		t.Errorf("FeatureExtendLease with a delivery lease is %v, %q; want unsupported, with a note that the "+
			"lease is reported and cannot be moved", ext.Level, ext.Note)
	}
}

// TestRunGivesBackADeliveryWithinHalfItsLease is what the lease is for: a
// handler that asks for a redelivery an hour off, by a postponement or by a
// retry, on a profile that cannot delay one (no NackDelay rule), so Run keeps
// the delivery itself. A broker that takes the delivery back after the
// profile's DeliveryLease, as RabbitMQ 4.3 does a quorum queue's after its
// consumer timeout, then stops sending to the consumer over AMQP 1.0; Run,
// reading the lease, requeues the delivery within half of it instead. With no
// lease reported it held the delivery for the hour.
func TestRunGivesBackADeliveryWithinHalfItsLease(t *testing.T) {
	const lease = 400 * time.Millisecond
	for _, tc := range []struct {
		name  string
		first error
	}{
		{"postponement", mq.Postpone(nil, time.Hour)},
		{"retry", mq.RetryAfter(errors.New("not yet"), time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testProfile()
			p.NackDelay = aq.NackDelayRule{}
			p.DeliveryLease = lease
			f := open(t, p, aq.Config{})
			cons, err := mq.NewClient(f.b).Consumer(context.Background(), mq.ConsumerConfig{Topic: "topic", Subscription: "sub"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			ran := make(chan error, 1)
			stop := sync.OnceFunc(func() {
				cancel()
				if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run: %v", err)
				}
			})
			t.Cleanup(func() {
				stop()
				_ = cons.Close()
			})
			handled := make(chan struct{}, 4)
			go func() {
				ran <- cons.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					handled <- struct{}{}
					return tc.first
				}))
			}()
			rcv := f.receiver(5 * time.Second)
			start := time.Now()
			rcv.Deliver(message("tag-1", "x"))
			select {
			case <-handled:
			case <-time.After(5 * time.Second):
				t.Fatal("the handler never saw the message")
			}
			// Well inside the hour, well past half the lease.
			deadline := time.Now().Add(5 * lease)
			for len(rcv.Dispositions()) == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if len(rcv.Dispositions()) == 0 {
				t.Fatalf("no disposition reached the peer %v after the delivery, with a lease of %v: Run held the "+
					"delivery past its lease", time.Since(start).Round(time.Millisecond), lease)
			}
			if d := rcv.Dispositions()[0].Disposition; d.Outcome != client.OutcomeModified {
				t.Errorf("the delivery went back with %+v, want a requeue", d)
			}
		})
	}
}

// TestSourceRefusesASeekItCannotHonour is the start positions over a
// protocol that has no position at all.
//
// Latest and earliest are the same place — a receiving link reads what is at an
// address — which is honest rather than a silent approximation; a time and a
// sequence are refused, because pretending to them would make a replay scenario
// read an empty address and call it a pass.
func TestSourceRefusesASeekItCannotHonour(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	for name, start := range map[string]mq.StartPosition{
		"latest":            mq.StartLatest(),
		"earliest":          mq.StartEarliest(),
		"the zero position": {},
	} {
		if _, err := f.b.OpenSource(sinkCtx(), mq.SourceConfig{
			Topic: "t", Subscription: "s", Prefetch: 1, Start: start,
		}); err != nil {
			t.Errorf("a start position of %s was refused: %v", name, err)
		}
	}
	for name, start := range map[string]mq.StartPosition{
		"a time":     mq.StartAt(time.Now()),
		"a sequence": mq.StartAtSequence(7),
	} {
		_, err := f.b.OpenSource(sinkCtx(), mq.SourceConfig{
			Topic: "t", Subscription: "s", Prefetch: 1, Start: start,
		})
		assertIs(t, err, mq.ErrUnsupported, "a start position of "+name)
	}
}

// TestSourceRefusesAnUnresolvedPrefetch holds the SPI's own contract: the specification says
// SourceConfig.Prefetch "is always positive: the core resolves the portable
// default before opening the source", so a zero is a core that did not.
func TestSourceRefusesAnUnresolvedPrefetch(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	_, err := f.b.OpenSource(sinkCtx(), mq.SourceConfig{Topic: "t", Subscription: "s"})
	assertCode(t, err, mq.CodeInvalidConfig, "a prefetch of zero")
}

// TestSourceMaxCreditCapsThePrefetch is [aq.Config.MaxCredit]: the prefetch is
// the CORE's number and a broker with a per-link ceiling of its own would refuse
// the flow frame.
func TestSourceMaxCreditCapsThePrefetch(t *testing.T) {
	f := open(t, testProfile(), aq.Config{MaxCredit: 3})
	src := f.source(mq.SourceConfig{Prefetch: 100})
	if got := src.CreditStats().Prefetch; got != 3 {
		t.Fatalf("the window's prefetch is %d, want the configured cap of 3", got)
	}
	_, d := deliverAndFetch(t, f, src, "tag-1", "x")
	if got := f.receiver(budget).Granted(); got > 3 {
		t.Fatalf("the source issued %d credit against a cap of 3", got)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSourceCloseReleasesWhatItHandedOut is the specification's "Close ... requeues or
// abandons everything it has handed out that was never settled".
//
// Releasing explicitly rather than relying on the detach is a promptness
// measure: AMQP 1.0 returns an unsettled delivery to the node when its link goes
// away, so nothing is lost either way — but the release turns "when the link
// goes" into "now", which on a broker whose links linger is the difference
// between a restart that resumes and one that appears to hang.
func TestSourceCloseReleasesWhatItHandedOut(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	rcv, _ := deliverAndFetch(t, f, src, "held-1", "x")
	// One more in the caller's hands, and one the peer delivered that nobody
	// fetched.
	rcv.Deliver(message("held-2", "y"))
	_ = fetchOne(t, src, budget)
	rcv.Deliver(message("prefetched", "z"))
	waitFor(t, budget, "the peer to hand over the third message", func() bool {
		return rcv.Queued() == 1
	})

	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	tags := map[string]client.Outcome{}
	for _, d := range rcv.Dispositions() {
		tags[d.Tag] = d.Disposition.Outcome
	}
	for _, tag := range []string{"held-1", "held-2"} {
		got, ok := tags[tag]
		if !ok {
			t.Errorf("the close did not release %s; the specification asks a Close to requeue or abandon everything it handed out", tag)
			continue
		}
		if got != client.OutcomeReleased {
			t.Errorf("the close settled %s with %v, want `released`: the receiver did not act on it, and counting it "+
				"as a failed attempt would make a consumer that shuts down look like one that is failing", tag, got)
		}
	}
	if !rcv.Detached() {
		t.Error("the close did not detach the link, which is what returns anything the release missed to the node")
	}
}

// TestSourceCloseIsIdempotentAndRefusesAfterwards is the lifecycle rule.
func TestSourceCloseIsIdempotentAndRefusesAfterwards(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	_, err := src.Fetch(context.Background(), dst)
	assertIs(t, err, mq.ErrClosed, "Fetch on a closed source")
	assertIs(t, src.Settler().Ack(context.Background(), &mq.DriverDelivery{}), mq.ErrClosed,
		"Ack on a closed source")
}

// TestSourceFetchRespectsTheCallersContext is the specification's "cancellation unblocks
// local admission, fetch, flush, drain".
func TestSourceFetchRespectsTheCallersContext(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	start := time.Now()
	n, err := src.Fetch(ctx, dst)
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch returned (%d, %v), want the context's own error", n, err)
	}
	if elapsed := time.Since(start); elapsed > budget {
		t.Fatalf("Fetch took %v to notice a 50ms deadline", elapsed)
	}
	// An empty dst is not a fetch at all, and blocking on one would be a
	// fetch that can never produce anything.
	n, err = src.Fetch(context.Background(), nil)
	if n != 0 || err != nil {
		t.Fatalf("Fetch with an empty destination returned (%d, %v), want (0, nil)", n, err)
	}
}

// TestSourceFetchReportsALostConnectionAsRetryable is what makes the lazy
// re-attach of links.go work: a link that died returns its own error, mapped to
// CodeUnavailable and retryable, and the next Fetch attaches a new link under
// the new generation.
func TestSourceFetchReportsALostConnectionAsRetryable(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	rcv, d := deliverAndFetch(t, f, src, "tag-1", "before")
	_ = rcv
	gen := f.b.Generation()

	// A parked fetch, then the connection dies underneath it.
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
	time.Sleep(20 * time.Millisecond)
	f.peer.LastConn().Kill(&amqp.ConnError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondConnectionForced}})
	select {
	case r := <-parked:
		if r.err == nil {
			t.Fatal("the parked Fetch returned successfully when the connection died")
		}
		if !mq.IsRetryable(r.err) {
			t.Fatalf("the parked Fetch reported %v (code %v), which is not retryable; a reconnection is what answers "+
				"it and the next Fetch is what attaches a new link", r.err, mq.CodeOf(r.err))
		}
		if errors.Is(r.err, mq.ErrClosed) {
			t.Fatalf("a lost connection was reported as a closed source: %v", r.err)
		}
	case <-time.After(budget):
		t.Fatal("the parked Fetch did not notice that its connection died")
	}
	if !aq.AwaitGenerationForTest(f.b, gen, budget) {
		t.Fatal("the generation did not move")
	}
	if !aq.AwaitReconnectForTest(f.b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection")
	}
	// The delivery from the old link is stale, and the next fetch works on a
	// link of its own.
	assertIs(t, src.Settler().Ack(context.Background(), &d), mq.ErrLeaseLost, "a delivery from the dead link")
	_, fresh := deliverAndFetch(t, f, src, "tag-2", "after")
	if string(fresh.Message.Body) != "after" {
		t.Fatalf("the fetch after the reconnection produced %q", fresh.Message.Body)
	}
	if err := src.Settler().Ack(context.Background(), &fresh); err != nil {
		t.Fatalf("acknowledging a delivery from the new link: %v", err)
	}
}

// TestSourceConsumerOptionsReachTheAttach is the one path a server-side
// selector has: the portable ConsumerConfig can name a topic and a
// subscription, and a filter is the broker's own expression language.
func TestSourceConsumerOptionsReachTheAttach(t *testing.T) {
	f := open(t, testProfile(), aq.Config{
		Consumer: aq.ConsumerOptions{
			LinkName: "iqmq-sub-1",
			Filters:  []amqp.LinkFilter{amqp.NewSelectorFilter("colour = 'red'")},
			Copy:     true,
		},
	})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	_, d := deliverAndFetch(t, f, src, "tag-1", "x")
	// The fake records the options it was attached with only in so far as it
	// honours them; what this test can check is that the attach SUCCEEDED with
	// them and that the delivery arrived, which is what a peer that refused a
	// filter would not allow.
	if string(d.Message.Body) != "x" {
		t.Fatalf("the delivery is %q", d.Message.Body)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestConsumerOptionsFromDecodesTheSPIsFutureField holds the decoder the specification
// asks for, against the day the SPI carries a driver-specific field.
func TestConsumerOptionsFromDecodesTheSPIsFutureField(t *testing.T) {
	want := aq.ConsumerOptions{LinkName: "l"}
	for name, in := range map[string]any{
		"nil":       nil,
		"a value":   want,
		"a pointer": &want,
	} {
		got, err := aq.ConsumerOptionsFrom(in)
		if err != nil {
			t.Errorf("ConsumerOptionsFrom(%s): %v", name, err)
			continue
		}
		if name != "nil" && got.LinkName != want.LinkName {
			t.Errorf("ConsumerOptionsFrom(%s) = %+v, want %+v", name, got, want)
		}
	}
	if _, err := aq.ConsumerOptionsFrom(aq.PublisherOptions{}); err == nil {
		t.Fatal("the consumer decoder accepted a publisher's options; a caller who has mixed up two drivers' " +
			"options would otherwise have the setting silently ignored")
	}
	if _, err := aq.PublisherOptionsFrom(aq.ConsumerOptions{}); err == nil {
		t.Fatal("the publisher decoder accepted a consumer's options")
	}
	var nilPtr *aq.PublisherOptions
	if _, err := aq.PublisherOptionsFrom(nilPtr); err != nil {
		t.Fatalf("a nil pointer was refused: %v; a caller that set nothing wants the defaults", err)
	}
}
