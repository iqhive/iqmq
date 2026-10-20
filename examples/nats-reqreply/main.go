// Command nats-reqreply is the API's request/reply over
// github.com/iqhive/iqmq/drivers/nats: one responder, one requester, and the
// four answers a caller can get back.
//
// # Why NATS is the broker this story is told on
//
// The specification has two paths and they are not equivalent. A broker with request/reply
// of its own — NATS's inbox, RabbitMQ's direct reply-to — answers a request
// through machinery the server owns: no reply topic is created, no reply
// consumer is opened, and the server can tell the requester that NOBODY IS
// LISTENING. Everywhere else request/reply is emulated over two ordinary
// topics and a correlation id, which works, costs a subscription, and cannot
// distinguish a responder that is slow from a responder that does not exist.
//
// This program runs both paths against the same broker, in that order, so the
// difference is a printed line rather than a claim. The four outcomes are:
//
//	a reply                     the ordinary case
//	context.DeadlineExceeded    a responder that will not answer this request
//	context.Canceled            a request the caller gave up on
//	iqmq code NotFound          nobody is subscribed at all (native path only)
//
// # The gap this example found
//
// [github.com/iqhive/iqmq/reqreply.New] takes the native path when
// mq.Client.Native() satisfies [github.com/iqhive/iqmq/reqreply.NativeRequester].
// This driver has the method — [github.com/iqhive/iqmq/drivers/nats.Backend.Request],
// with request/reply's exact signature — but its Native() answers the
// *nats.Conn, so nothing in the repository satisfies that interface and the
// portable client would refuse the native path and offer to emulate it.
// nativeRequester below is the four-line adapter that closes the gap from
// outside, which is what the example can do and what a reader needs; making
// the driver expose it is the follow-up.
//
// # Settlement
//
// NATS Core has no settlement: a delivery is complete the moment the handler
// has it, and Ack reports mq.ErrNotSettleable rather than pretending. The
// responder below therefore acknowledges nothing, and the request it answers
// is not redelivered if the reply is lost. That is the guarantee this broker
// gives and the reason a request/reply service on NATS Core is a CALL and not
// a workflow: the timeout is the only failure handling there is.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/reqreply"
)

// The three subjects this program uses. They are ordinary iqmq topics: the
// NATS driver maps a topic to a subject and back, so "rpc.price" is the
// subject "rpc.price" and nothing about request/reply is special about it.
const (
	topicRequests = "rpc.price"
	topicReplies  = "rpc.replies"
	// topicNobody has no responder on purpose. It is the whole point of the
	// third and sixth lines of output.
	topicNobody = "rpc.nobody"
)

// bodySilent is the request the responder handles and does not answer.
//
// reqreply.Serve reads an Answer's (nil, nil) as "no reply": the request was
// handled and nothing is published, which is what a one-way message on a
// request topic is. It is also how a responder that is up, subscribed and
// working produces a TIMEOUT, which is a different failure from the two below
// and is the one a caller most often meets in production.
const bodySilent = "SILENT"

// requestTimeout is the deadline every ordinary request in this program
// carries.
//
// reqreply.Config.Timeout is a FALLBACK and not a cap: the specification makes it the
// deadline a request gets when its context has none, and this program's
// context has one (main's two minutes, the test's sixty seconds). A request
// that leaned on Config.Timeout here would therefore wait for the OUTER
// deadline, which is how a responder that answers nothing turns a one-second
// failure into a sixty-second one. Giving each call its own deadline is the
// habit; it is also what makes this example fail fast when it is wrong.
const requestTimeout = 3 * time.Second

// prices is the service. A request body is a SKU and a reply body is a price
// in cents.
var prices = map[string]int64{"SKU-1": 1299, "SKU-2": 1499}

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body as a function that returns, so that os.Exit is reached
// after the deferred shutdown has run rather than instead of it.
func demo() error {
	addr := os.Getenv("IQMQ_NATS_ADDR")
	if addr == "" {
		// The harness convention of conformance/harness: IQMQ_<BROKER>_ADDR
		// names an externally managed broker as host:port. With nothing set
		// this is the default port on this machine, so `go run .` works
		// against `nats-server` started by hand.
		addr = "127.0.0.1:4222"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, backend, err := open(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()
	return run(ctx, os.Stdout, client, backend)
}

// open dials addr and returns both halves.
//
// It opens the BACKEND and wraps it, rather than calling nq.Open, because the
// driver's own request/reply is a method on *nq.Backend and mq.Client.Native()
// answers the native *nats.Conn instead — there is no route from a client back
// to the driver's type, which is deliberate in the SPI (ADR-003). A program
// that needs both opens both, exactly as drivers/kafka's own tests do.
//
// addr is host:port or a nats:// URL; both are accepted, because the harness
// convention is the bare address and a reader who has a URL should not have to
// know that.
func open(ctx context.Context, addr string) (*mq.Client, *nq.Backend, error) {
	url := addr
	if !hasScheme(url) {
		url = "nats://" + url
	}
	backend, err := nq.OpenBackend(ctx, nq.Config{Addrs: []string{url}})
	if err != nil {
		return nil, nil, err
	}
	// The client owns the backend it was built over: Shutdown closes both.
	return mq.NewClient(backend), backend, nil
}

// hasScheme reports whether s already names a scheme.
func hasScheme(s string) bool {
	for i := range len(s) {
		switch {
		case s[i] == ':':
			return i+2 < len(s) && s[i+1] == '/' && s[i+2] == '/'
		case s[i] >= 'a' && s[i] <= 'z', s[i] >= 'A' && s[i] <= 'Z',
			s[i] >= '0' && s[i] <= '9', s[i] == '+', s[i] == '-', s[i] == '.':
		default:
			return false
		}
	}
	return false
}

// nativeRequester plumbs this driver's own request/reply into the portable
// interface reqreply.New looks for.
//
// The signature is already identical — reqreply.NativeRequester asks for
// RequestNative(ctx, topic, *mq.Message) (*mq.Message, error) and
// nq.Backend.Request is exactly that under a different name — so the whole
// adapter is the rename. That it has to be written at all is the finding:
// reqreply's native path is implemented, the driver's half is implemented, and
// nothing joins them.
type nativeRequester struct{ b *nq.Backend }

// RequestNative sends a native NATS request and waits for one reply. The
// server does the correlation: the requester subscribes to a private inbox,
// the request carries it as the reply subject, and a responder that publishes
// to Properties.ReplyTo answers it.
func (n nativeRequester) RequestNative(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	return n.b.Request(ctx, topic, m)
}

// run is the whole program: a responder, a native requester, an emulated
// requester, and the four outcomes.
//
// out is an io.Writer rather than the log package so that the test can read
// what this printed and compare it byte for byte. A service would use
// log/slog.
func run(ctx context.Context, out io.Writer, cl *mq.Client, backend *nq.Backend) error {
	// silentSeen counts the unanswerable requests the responder has actually
	// received. It is what makes the cancellation case below deterministic:
	// the request is cancelled after the responder has it, not before it has
	// left, so the error really is a cancelled REQUEST and not a cancelled
	// publication.
	silentSeen := newSightings()

	responder, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topicRequests, Subscription: "pricing"})
	if err != nil {
		return err
	}
	runCtx, stopResponder := context.WithCancel(ctx)
	defer stopResponder()
	// One responder serves BOTH paths, and that is not a coincidence:
	// reqreply.Serve answers Properties.ReplyTo and stamps the request's
	// correlation id on the reply. On the native path the reply subject is the
	// requester's private inbox and the id is ignored; on the emulated path it
	// is the shared reply topic and the id is the only thing that matches the
	// answer to its waiter. A responder written once is correct on both.
	done := make(chan error, 1)
	serve, err := reqreply.Serve(cl, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
		if string(d.Body) == bodySilent {
			// Handled, not answered, and not failed: see bodySilent.
			silentSeen.saw()
			//nolint:nilnil // (nil, nil) is reqreply.Answer's documented "no reply": the request is handled and nothing is published
			return nil, nil
		}
		price, ok := prices[string(d.Body)]
		if !ok {
			// A reply is still a reply. Returning an error here would
			// hand the delivery to the runner's failure policy, and on a
			// broker with no settlement that means a retry the requester
			// is no longer waiting for.
			return &mq.Message{Body: []byte("unknown sku")}, nil
		}
		return &mq.Message{Body: strconv.AppendInt(nil, price, 10)}, nil
	})
	if err != nil {
		return err
	}
	go func() {
		done <- responder.Run(runCtx, serve, mq.WithConcurrency(4))
	}()
	defer func() {
		stopResponder()
		<-done
	}()

	if err := nativePath(ctx, out, cl, backend, silentSeen); err != nil {
		return err
	}
	return emulatedPath(ctx, out, cl)
}

// nativePath runs the four outcomes through the driver's own request/reply.
func nativePath(ctx context.Context, out io.Writer, cl *mq.Client, backend *nq.Backend, silentSeen *sightings) error {
	// Config.Timeout is named rather than left to DefaultTimeout (30 s)
	// because a request whose deadline is thirty seconds is a request that
	// holds a caller's goroutine for thirty seconds when the responder is
	// down. Every call below narrows it further: see requestTimeout.
	rr, err := reqreply.New(ctx, cl, reqreply.Config{Timeout: requestTimeout},
		reqreply.WithNativeRequester(nativeRequester{b: backend}))
	if err != nil {
		return err
	}
	//nolint:contextcheck // the specification gives Close no context on purpose: a prompt close is not an operation a caller cancels, and this client has nothing admitted to drain
	defer func() { _ = rr.Close() }()
	_, _ = fmt.Fprintf(out, "native requester: native=%v, reply topology=none\n", rr.Native())

	// 1. The ordinary case.
	octx, ocancel := context.WithTimeout(ctx, requestTimeout)
	reply, err := rr.Request(octx, topicRequests, &mq.Message{Body: []byte("SKU-1")})
	ocancel()
	if err != nil {
		return fmt.Errorf("requesting SKU-1: %w", err)
	}
	_, _ = fmt.Fprintf(out, "native reply: SKU-1 costs %s cents\n", reply.Body)

	// 2. A responder that will not answer THIS request. The deadline is the
	//    caller's and nothing else can end the wait.
	tctx, tcancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err = rr.Request(tctx, topicRequests, &mq.Message{Body: []byte(bodySilent)})
	tcancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("a request nobody answered reported %w, want context.DeadlineExceeded", err)
	}
	if err := silentSeen.waitFor(ctx, 1); err != nil {
		return err
	}
	// The ERROR TEXT is not printed. It is the driver's wording wrapped
	// around the context's, and an example whose expected output depends on
	// another package's error string breaks the next time that package
	// rewords it. What is printed is what a caller can branch on: errors.Is
	// against context.DeadlineExceeded, which the check above already made,
	// and the portable code.
	_, _ = fmt.Fprintf(out, "native timeout: a subscribed responder that does not answer -> the caller's deadline (iqmq code %v)\n", mq.CodeOf(err))

	// 3. A request the caller gives up on. The cancellation happens after the
	//    responder has the request, so this is a cancelled request and not a
	//    publication that never left.
	cctx, ccancel := context.WithCancel(ctx)
	type result struct{ err error }
	res := make(chan result, 1)
	go func() {
		_, err := rr.Request(cctx, topicRequests, &mq.Message{Body: []byte(bodySilent)})
		res <- result{err}
	}()
	if err := silentSeen.waitFor(ctx, 2); err != nil {
		// The request goroutine is this function's to wait for.
		ccancel()
		<-res
		return err
	}
	ccancel()
	if err := (<-res).err; !errors.Is(err, context.Canceled) {
		return fmt.Errorf("a cancelled request reported %w, want context.Canceled", err)
	}
	_, _ = fmt.Fprintln(out, "native cancelled: a request the caller gave up on -> context canceled")

	// 4. Nobody is listening. This is the answer only a native path can give,
	//    and the deadline is deliberately generous: if the server did not
	//    volunteer "no responders" this line would take requestTimeout and
	//    report a timeout instead.
	nctx, ncancel := context.WithTimeout(ctx, requestTimeout)
	_, err = rr.Request(nctx, topicNobody, &mq.Message{Body: []byte("SKU-1")})
	ncancel()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return errors.New("a request to an unsubscribed subject timed out; the native path is supposed to report NotFound at once")
	case mq.CodeOf(err) != mq.CodeNotFound:
		return fmt.Errorf("a request to an unsubscribed subject reported code %v, want NotFound: %w", mq.CodeOf(err), err)
	}
	_, _ = fmt.Fprintf(out, "native nobody: no subscriber on %s -> iqmq code %v, before the deadline\n", topicNobody, mq.CodeOf(err))

	// Pending is always zero on the native path — the driver owns the
	// correlation — and it is checked anyway, because the ONE failure the
	// emulated design can have is a leaked slot and a reader comparing the
	// two lines should see the same number from both.
	if n := rr.Pending(); n != 0 {
		return fmt.Errorf("the native requester holds %d correlation slots, want 0", n)
	}
	return nil
}

// emulatedPath runs the same requests through the portable emulation over
// two ordinary topics.
func emulatedPath(ctx context.Context, out io.Writer, cl *mq.Client) error {
	em, err := reqreply.New(ctx, cl, reqreply.Config{
		ReplyTopic: topicReplies,
		Timeout:    requestTimeout,
		// OnUnmatched is called for a reply that matches no waiter: a late
		// answer to a request that has already timed out, a duplicate, or
		// another process's reply on a shared reply topic. It is a counter in
		// a service and it is here because a reply topic with no OnUnmatched
		// is a reply topic whose surprises are invisible.
		OnUnmatched: func(_ context.Context, d *mq.Delivery) {
			_, _ = fmt.Fprintf(out, "emulated unmatched reply: correlation id %q\n", d.Properties.CorrelationID)
		},
	},
		// WithoutNativeRequestReply is not needed today — this backend's
		// Native() is a *nats.Conn and satisfies nothing — and it is passed
		// anyway so that this half keeps measuring the emulated path on the
		// day the driver starts exposing its own.
		reqreply.WithoutNativeRequestReply(),
		// Emulation is opt-in because it CREATES TOPOLOGY: a reply topic and
		// a subscription on it. On NATS that is free and on a managed broker
		// it is a resource with a quota, which is why the specification refuses to
		// provision one silently.
		reqreply.WithEmulatedRequestReply())
	if err != nil {
		return err
	}
	//nolint:contextcheck // the specification gives Close no context on purpose; Close drains the correlation table, which needs no deadline of the caller's
	defer func() { _ = em.Close() }()
	_, _ = fmt.Fprintf(out, "emulated requester: native=%v, reply topology=%s\n", em.Native(), topicReplies)

	octx, ocancel := context.WithTimeout(ctx, requestTimeout)
	reply, err := em.Request(octx, topicRequests, &mq.Message{Body: []byte("SKU-2")})
	ocancel()
	if err != nil {
		return fmt.Errorf("requesting SKU-2: %w", err)
	}
	_, _ = fmt.Fprintf(out, "emulated reply: SKU-2 costs %s cents\n", reply.Body)

	// The difference the whole example exists to show: with no subscriber on
	// the request topic, the emulated path has nothing to distinguish "nobody
	// is there" from "nobody has answered yet". The publication succeeds —
	// NATS Core drops a message no subscriber wants — and the caller waits for
	// its own deadline.
	nctx, ncancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err = em.Request(nctx, topicNobody, &mq.Message{Body: []byte("SKU-1")})
	ncancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("the emulated path reported %w for an unsubscribed topic, want context.DeadlineExceeded", err)
	}
	_, _ = fmt.Fprintf(out, "emulated nobody: no subscriber on %s -> the caller's deadline (iqmq code %v), indistinguishable from slow\n", topicNobody, mq.CodeOf(err))

	// The leak this design can have, and the reason Pending is exported: a
	// request whose reply never arrives releases its correlation slot on the
	// requesting goroutine before it returns. A client that leaked one per
	// timeout would grow without bound on exactly the workload — an
	// unreliable responder — that makes timeouts common.
	if n := em.Pending(); n != 0 {
		return fmt.Errorf("after a timed-out request the emulated requester holds %d correlation slots, want 0", n)
	}
	_, _ = fmt.Fprintln(out, "emulated pending after the timeout: 0 correlation slots")
	return nil
}

// sightings counts the unanswerable requests the responder has received.
//
// The count is guarded by a mutex rather than carried as tokens on a
// channel, and more only wakes a waiter: the responder's handler must not
// block on a waiter that is not there.
type sightings struct {
	mu   sync.Mutex
	n    int
	more chan struct{}
}

func newSightings() *sightings { return &sightings{more: make(chan struct{}, 1)} }

// saw records one sighting.
func (s *sightings) saw() {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	select {
	case s.more <- struct{}{}:
	default:
	}
}

// waitFor waits until the responder has received n requests in all, or ctx
// ends.
func (s *sightings) waitFor(ctx context.Context, n int) error {
	for !s.reached(n) {
		select {
		case <-s.more:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// reached reports whether the count has reached n.
func (s *sightings) reached(n int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n >= n
}
