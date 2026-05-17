package reqreply_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
	"bitbucket.org/iqhive/iqmq/reqreply"
)

// rig is a memory broker with a request topic, a reply topic, a request/reply
// client and a responder running over the request topic.
type rig struct {
	t   *testing.T
	cl  *mq.Client
	b   *memory.Broker
	rr  *reqreply.Client
	req *mq.ConsumerHandle
	// stop cancels the responder.
	stop   context.CancelFunc
	served chan struct{}
}

// newRig opens everything and starts the responder with the given answer.
func newRig(t *testing.T, cfg reqreply.Config, answer reqreply.Answer) *rig {
	t.Helper()
	cl, b := memory.NewForTest(t, memory.TestTopics("rpc", "replies"))
	if cfg.ReplyTopic == "" {
		cfg.ReplyTopic = "replies"
	}
	rr, err := reqreply.New(context.Background(), cl, cfg, reqreply.WithEmulatedRequestReply())
	if err != nil {
		t.Fatalf("opening the request/reply client: %v", err)
	}
	t.Cleanup(func() { _ = rr.Close() })

	rc, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "rpc", Subscription: "svc"})
	if err != nil {
		t.Fatalf("opening the request consumer: %v", err)
	}
	t.Cleanup(func() { _ = rc.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := &rig{t: t, cl: cl, b: b, rr: rr, req: rc, stop: cancel, served: make(chan struct{}, 64)}
	if answer != nil {
		go func() {
			_ = rc.Run(ctx, reqreply.Serve(cl, func(c context.Context, d *mq.Delivery) (*mq.Message, error) {
				m, err := answer(c, d)
				select {
				case r.served <- struct{}{}:
				default:
				}
				return m, err
			}))
		}()
	}
	return r
}

func echo(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
	return &mq.Message{Body: append([]byte("re: "), d.Body...)}, nil
}

// noReply is the reqreply.Answer that handles a request and does not answer
// it, which is what a one-way message on a request topic is. The typed nil is
// deliberate: a bare "return nil, nil" is a shape the linters refuse, and the
// contract here really is "no message, no failure".
var noReply *mq.Message

func TestRequestGetsItsReply(t *testing.T) {
	r := newRig(t, reqreply.Config{}, echo)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("Request = %v, want nil", err)
	}
	if string(reply.Body) != "re: ping" {
		t.Fatalf("reply = %q, want %q", reply.Body, "re: ping")
	}
	if n := r.rr.Pending(); n != 0 {
		t.Fatalf("%d requests outstanding after a completed one, want 0", n)
	}
}

func TestConcurrentRequestsEachGetTheirOwnReply(t *testing.T) {
	// The whole point of a correlation table: one reply consumer, many
	// requests, and no request ever receiving another's answer.
	r := newRig(t, reqreply.Config{}, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}, nil
	})
	const n = 64
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte(strconv.Itoa(i))})
			errs[i] = err
			if err == nil {
				got[i] = string(reply.Body)
			}
		}()
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d = %v", i, errs[i])
		}
		if want := "re: " + strconv.Itoa(i); got[i] != want {
			t.Fatalf("request %d received %q, want %q: a reply reached the wrong waiter", i, got[i], want)
		}
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d requests outstanding, want 0", p)
	}
}

func TestATimedOutRequestReleasesItsSlot(t *testing.T) {
	// The leak this design can have: a request whose reply never arrives
	// must give its correlation entry back. A client that kept one per
	// timeout would grow without bound on exactly the workload -- an
	// unreliable responder -- that makes timeouts common.
	r := newRig(t, reqreply.Config{}, func(context.Context, *mq.Delivery) (*mq.Message, error) {
		return noReply, nil // handled, never answered
	})
	const n = 32
	for i := range n {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("request %d = %v, want context.DeadlineExceeded", i, err)
		}
		if p := r.rr.Pending(); p != 0 {
			t.Fatalf("after request %d timed out, %d entries are still held; the slot must be released before Request returns", i, p)
		}
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d correlation entries survived %d timed-out requests, want 0", p, n)
	}
}

func TestARequestThatCouldNotBePublishedReleasesItsSlot(t *testing.T) {
	r := newRig(t, reqreply.Config{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A topic the broker does not have and will not create.
	_, err := r.rr.Request(ctx, "", &mq.Message{Body: []byte("ping")})
	if err == nil {
		t.Fatal("Request = nil for a publication that cannot succeed")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Request = %v, want the publication's own failure", err)
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d entries held after a request that was never published, want 0", p)
	}
}

func TestCapacityBoundsTheOutstandingRequests(t *testing.T) {
	// The specification: bounded local state. A responder that never replies must not be
	// able to grow the client's memory without limit.
	r := newRig(t, reqreply.Config{Capacity: 4}, func(context.Context, *mq.Delivery) (*mq.Message, error) {
		return noReply, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var refused atomic.Int64
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, rcancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer rcancel()
			_, err := r.rr.Request(rctx, "rpc", &mq.Message{Body: []byte("ping")})
			if errors.Is(err, reqreply.ErrTooManyPending) {
				refused.Add(1)
			}
		}()
	}
	wg.Wait()
	if refused.Load() == 0 {
		t.Fatal("no request was refused with ErrTooManyPending; the correlation table is unbounded")
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d entries held after every request ended, want 0", p)
	}
}

func TestCloseWakesEveryWaiter(t *testing.T) {
	r := newRig(t, reqreply.Config{}, func(context.Context, *mq.Delivery) (*mq.Message, error) {
		return noReply, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 8
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
			errs <- err
		}()
	}
	// Wait until the requests are outstanding, then close under them.
	deadline := time.Now().Add(5 * time.Second)
	for r.rr.Pending() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := r.rr.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	for i := range n {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatalf("waiter %d got a reply from a closed client", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d was not woken by Close; it is waiting for a reply that can never come", i)
		}
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d entries survived Close, want 0", p)
	}
	if err := r.rr.Close(); err != nil {
		t.Fatalf("a second Close = %v, want nil: Close is idempotent", err)
	}
	if _, err := r.rr.Request(ctx, "rpc", &mq.Message{}); !errors.Is(err, reqreply.ErrClosed) {
		t.Fatalf("Request on a closed client = %v, want ErrClosed", err)
	}
}

func TestAStrayReplyIsSettledAndMatchesNothing(t *testing.T) {
	// A reply carrying a correlation id this client never issued -- another
	// client's on a shared reply topic, or a responder's invention -- must
	// not be mapped onto one of our ids, and must still be settled: an
	// unsettled delivery holds a lease and is redelivered for ever.
	var unmatched atomic.Int64
	r := newRig(t, reqreply.Config{
		OnUnmatched: func(context.Context, *mq.Delivery) { unmatched.Add(1) },
	}, echo)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []string{"", "1", "not-ours.7", "....", "\x00"} {
		m := &mq.Message{Body: []byte("stray"), Properties: mq.Properties{CorrelationID: id}}
		if err := r.cl.Publish(ctx, "replies", m); err != nil {
			t.Fatalf("publishing a stray reply: %v", err)
		}
	}
	waitFor(t, "the stray replies to be settled", func() bool { return unmatched.Load() == 5 })
	s, err := r.b.Inspect("replies", "replies-rr")
	if err != nil {
		t.Fatalf("inspecting the reply subscription: %v", err)
	}
	if s.InFlight != 0 || s.Acked != 5 {
		t.Fatalf("reply subscription = %+v, want 5 acked and nothing in flight", s)
	}
	// And a real request still works afterwards.
	reply, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("Request after the strays = %v, want nil", err)
	}
	if string(reply.Body) != "re: ping" {
		t.Fatalf("reply = %q", reply.Body)
	}
}

func TestAnotherClientsCorrelationIDNeverMatchesAnOutstandingRequest(t *testing.T) {
	// Ids come from a counter, so two clients sharing a reply topic would
	// issue the same small integers: client A's reply to request 1 would
	// satisfy client B's request 1. That is a wrong answer delivered as a
	// right one, and nothing about it is observable from the outside -- which
	// is why the id carries a per-client instance prefix and why this test
	// publishes a reply whose COUNTER matches the outstanding request's.
	var unmatched atomic.Int64
	r := newRig(t, reqreply.Config{
		OnUnmatched: func(context.Context, *mq.Delivery) { unmatched.Add(1) },
	}, func(context.Context, *mq.Delivery) (*mq.Message, error) {
		return noReply, nil // the real responder never answers
	})
	done := make(chan struct {
		body string
		err  error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		reply, err := r.rr.Request(ctx, "rpc", &mq.Message{Body: []byte("ping")})
		body := ""
		if reply != nil {
			body = string(reply.Body)
		}
		done <- struct {
			body string
			err  error
		}{body, err}
	}()
	waitFor(t, "the request to be outstanding", func() bool { return r.rr.Pending() == 1 })

	// This client's first request is counter 1; another client's would be
	// too, under its own instance prefix.
	foreign := &mq.Message{
		Body:       []byte("another client's answer"),
		Properties: mq.Properties{CorrelationID: "zzzzzz.1"},
	}
	if err := r.cl.Publish(context.Background(), "replies", foreign); err != nil {
		t.Fatalf("publishing the foreign reply: %v", err)
	}
	waitFor(t, "the foreign reply to be settled", func() bool { return unmatched.Load() == 1 })

	got := <-done
	if got.err == nil {
		t.Fatalf("Request received %q, want a timeout: another client's correlation id must not match ours", got.body)
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("Request = %v, want context.DeadlineExceeded", got.err)
	}
	if p := r.rr.Pending(); p != 0 {
		t.Fatalf("%d entries outstanding, want 0", p)
	}
}

func TestEmulationIsOptIn(t *testing.T) {
	// The specification: emulation creates topology, so it is never taken silently.
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	_, err := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"})
	if !errors.Is(err, reqreply.ErrNotEnabled) {
		t.Fatalf("New without WithEmulatedRequestReply = %v, want ErrNotEnabled", err)
	}
	if mq.CodeOf(err) != mq.CodeUnsupported {
		t.Fatalf("CodeOf = %v, want CodeUnsupported", mq.CodeOf(err))
	}
	rr, err := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"},
		reqreply.WithEmulatedRequestReply())
	if err != nil {
		t.Fatalf("New with the opt-in = %v, want nil", err)
	}
	if err := rr.Close(); err != nil {
		t.Fatal(err)
	}
}

// nativeStub is a NativeRequester that answers without any topology.
type nativeStub struct {
	calls atomic.Int64
	err   error
}

func (n *nativeStub) RequestNative(_ context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	n.calls.Add(1)
	if n.err != nil {
		return nil, n.err
	}
	return &mq.Message{Body: append([]byte(topic+": "), m.Body...)}, nil
}

func TestNativeRequesterIsUsedAndCreatesNoTopology(t *testing.T) {
	cl, b := memory.NewForTest(t)
	native := &nativeStub{}
	rr, err := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"},
		reqreply.WithNativeRequester(native))
	if err != nil {
		t.Fatalf("New = %v, want nil: a native requester needs no opt-in", err)
	}
	defer func() { _ = rr.Close() }()
	if !rr.Native() {
		t.Fatal("Native() = false for a client built over a native requester")
	}
	reply, err := rr.Request(context.Background(), "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("Request = %v, want nil", err)
	}
	if string(reply.Body) != "rpc: ping" {
		t.Fatalf("reply = %q, want the native requester's own", reply.Body)
	}
	if native.calls.Load() != 1 {
		t.Fatalf("the native requester was called %d times, want 1", native.calls.Load())
	}
	if _, err := b.Inspect("replies", "replies-rr"); err == nil {
		t.Fatal("the native path created the emulated reply subscription; the specification says it must not")
	}
	if p := rr.Pending(); p != 0 {
		t.Fatalf("Pending() = %d on the native path, want 0: the driver owns the correlation", p)
	}
}

func TestNativeCanBeRefused(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	native := &nativeStub{}
	rr, err := reqreply.New(context.Background(), cl, reqreply.Config{ReplyTopic: "replies"},
		reqreply.WithNativeRequester(native),
		reqreply.WithoutNativeRequestReply(),
		reqreply.WithEmulatedRequestReply())
	if err != nil {
		t.Fatalf("New = %v, want nil", err)
	}
	defer func() { _ = rr.Close() }()
	if rr.Native() {
		t.Fatal("Native() = true after WithoutNativeRequestReply")
	}
}

func TestNewRefusesAnUnusableConfiguration(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	cases := []struct {
		name string
		cl   *mq.Client
		cfg  reqreply.Config
	}{
		{"no client", nil, reqreply.Config{ReplyTopic: "replies"}},
		{"no reply topic", cl, reqreply.Config{}},
		{"negative timeout", cl, reqreply.Config{ReplyTopic: "replies", Timeout: -time.Second}},
		{"negative capacity", cl, reqreply.Config{ReplyTopic: "replies", Capacity: -1}},
		{"negative prefetch", cl, reqreply.Config{ReplyTopic: "replies", Prefetch: -1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := reqreply.New(context.Background(), c.cl, c.cfg, reqreply.WithEmulatedRequestReply())
			if !errors.Is(err, reqreply.ErrInvalidConfig) {
				t.Fatalf("New = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

// ---------------------------------------------------------------- Responder

func TestRespondAddressesTheReplyAndKeepsTheCorrelationID(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("replies"))
	watch, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "replies", Subscription: "watch"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()

	d := &mq.Delivery{Message: mq.Message{Topic: "rpc", Body: []byte("ping")}}
	d.Properties.ReplyTo = "replies"
	d.Properties.CorrelationID = "corr-1"
	if err := reqreply.Respond(context.Background(), cl, d, &mq.Message{Body: []byte("pong")}); err != nil {
		t.Fatalf("Respond = %v, want nil", err)
	}
	waitFor(t, "the reply to arrive", func() bool {
		s, err := b.Inspect("replies", "watch")
		return err == nil && s.Ready == 1
	})
	buf := make([]mq.Delivery, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := watch.Receive(ctx, buf)
	if err != nil || n != 1 {
		t.Fatalf("Receive = %d, %v", n, err)
	}
	got := &buf[0]
	if string(got.Body) != "pong" {
		t.Fatalf("reply body = %q", got.Body)
	}
	if got.Properties.CorrelationID != "corr-1" {
		t.Fatalf("reply correlation id = %q, want the request's own: a responder that invents one produces a reply no waiter matches", got.Properties.CorrelationID)
	}
	if err := got.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestRespondReadsTheReplyToHeaderToo(t *testing.T) {
	d := &mq.Delivery{Message: mq.Message{Topic: "rpc"}}
	d.Headers.Set(reqreply.HeaderReplyTo, []byte("replies"))
	if got := reqreply.ReplyToOf(d); got != "replies" {
		t.Fatalf("ReplyToOf = %q, want the header's own (the specification maps the property to a header on most backends)", got)
	}
}

func TestRespondRefusesARequestWithNowhereToReply(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	d := &mq.Delivery{Message: mq.Message{Topic: "rpc"}}
	if err := reqreply.Respond(context.Background(), cl, d, &mq.Message{}); !errors.Is(err, reqreply.ErrNoReplyTo) {
		t.Fatalf("Respond = %v, want ErrNoReplyTo", err)
	}
}

func TestServeHandlesAOneWayMessageWithoutFailingIt(t *testing.T) {
	// A publisher that sent a message to a request topic without asking for
	// an answer has done nothing wrong; failing such a delivery would
	// dead-letter perfectly good traffic.
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	var called atomic.Int64
	h := reqreply.Serve(cl, func(context.Context, *mq.Delivery) (*mq.Message, error) {
		called.Add(1)
		return &mq.Message{Body: []byte("pong")}, nil
	})
	d := &mq.Delivery{Message: mq.Message{Topic: "rpc", Body: []byte("ping")}}
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil for a request with no reply topic", err)
	}
	if called.Load() != 1 {
		t.Fatal("the answer was not called")
	}
}

func TestServeReturnsTheAnswersFailureUnchanged(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("replies"))
	want := errors.New("the answer failed")
	h := reqreply.Serve(cl, func(context.Context, *mq.Delivery) (*mq.Message, error) { return noReply, want })
	d := &mq.Delivery{Message: mq.Message{Topic: "rpc"}}
	d.Properties.ReplyTo = "replies"
	if err := h.Handle(context.Background(), d); !errors.Is(err, want) {
		t.Fatalf("Handle = %v, want the answer's own error so the runner's failure policy decides", err)
	}
}

func TestServeRefusesNilArguments(t *testing.T) {
	cl, _ := memory.NewForTest(t)
	for _, c := range []struct {
		name string
		call func()
	}{
		{"nil publisher", func() { reqreply.Serve(nil, echo) }},
		{"nil answer", func() { reqreply.Serve(cl, nil) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic: a construction error must not become a per-delivery failure")
				}
			}()
			c.call()
		})
	}
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
