package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/reqreply"
)

// Native request/reply over direct reply-to.
//
// reqreply.go was 3.1 % covered -- after `make ci` AND after every conformance
// suite, because the kit has no request/reply scenario and `reqreply`'s own
// tests drive a stub rather than this driver. A headline feature
// therefore shipped with its implementation exercised by nothing.
//
// The fake grew the pseudo-queue for these : a consumer on
// `amq.rabbitmq.reply-to` conjures it, under an address the broker mints with
// the real broker's shape ('+', '/' and "=="), the request's `reply_to` is
// replaced with that address, the default exchange already binds every queue
// by its own name, and the reply routes back with no further help.

// responder answers requests on a topic until the test ends.
//
// It is the other half of a request: the driver's own path stops at
// "published, now waiting", and without something to reply, every test here
// would be a timeout test.
type responder struct {
	t      *testing.T
	client *mq.Client
	stop   chan struct{}
	wg     sync.WaitGroup

	mu    sync.Mutex
	seen  []string
	reply func(req mq.Delivery) *mq.Message
}

// respondOn consumes a topic and replies to whatever arrives.
func respondOn(t *testing.T, c *mq.Client, topic, sub string, reply func(mq.Delivery) *mq.Message) *responder {
	t.Helper()
	r := &responder{t: t, client: c, stop: make(chan struct{}), reply: reply}
	cons := consumeOn(t, c, topic, sub)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		dst := make([]mq.Delivery, 1)
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			rctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			n, err := cons.Receive(rctx, dst)
			cancel()
			if err != nil || n == 0 {
				select {
				case <-r.stop:
					return
				default:
					continue
				}
			}
			d := dst[0]
			r.mu.Lock()
			r.seen = append(r.seen, d.Properties.CorrelationID)
			r.mu.Unlock()
			if m := r.reply(d); m != nil {
				// The reply goes to the address the REQUEST named, over the
				// default exchange: that is what a responder does and the
				// whole reason the sink recognises a reply address. It goes
				// through Client.Publish, as reqreply.Respond sends it, so
				// the address meets the core's topic check.
				m.Properties.CorrelationID = d.Properties.CorrelationID
				if got := d.Properties.ReplyTo; !rq.IsReplyAddress(got) || got == rq.ReplyAddress {
					r.t.Errorf("the request's reply_to is %q, want the address the broker put there", got)
				}
				// A context of its own: the receive's has already been
				// cancelled, and publishing the reply on it would cancel
				// every reply this responder ever sends.
				pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := r.client.Publish(pctx, d.Properties.ReplyTo, m)
				pcancel()
				if err != nil {
					r.t.Errorf("publishing the reply: %v", err)
				}
			}
			_ = d.Ack()
		}
	}()
	t.Cleanup(r.close)
	return r
}

func (r *responder) close() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	r.wg.Wait()
}

// TestAReplyReachesTheAddressTheBrokerSubstitutes is reqreply.New, which picks
// this driver's RequestNative, against reqreply.Serve, end to end and through
// nothing but the portable API.
//
// RabbitMQ replaces a request's `reply_to` with an address of its own,
// `amq.rabbitmq.reply-to.<base64>.<base64>`: either part can hold '+' and '/',
// and the second always ends "==". The responder publishes its reply there
// through mq.Client.Publish, whose topic check allows none of those bytes, so
// every native request timed out on a real broker, and the responder's runner
// retried and then dead-lettered every request. The fake left `reply_to`
// alone, which is why nothing here saw it.
func TestAReplyReachesTheAddressTheBrokerSubstitutes(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	requestThroughReqreply(t, client)
}

// requestThroughReqreply serves "rpc" with reqreply.Serve under a runner and
// makes two requests with reqreply.New, which is the whole of what a caller of
// native request/reply writes.
func requestThroughReqreply(t *testing.T, client *mq.Client) {
	t.Helper()
	cons := consumeOn(t, client, "rpc", "workers")
	var seen sync.Map
	h, err := reqreply.Serve(client, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
		seen.Store(d.Properties.ReplyTo, true)
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan error, 8)
	runCtx, stopRun := context.WithCancel(t.Context())
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		_ = cons.Run(runCtx, mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			err := h.Handle(ctx, d)
			select {
			case handled <- err:
			default:
			}
			return err
		}))
	}()
	t.Cleanup(func() { stopRun(); <-ran })

	rr, err := reqreply.New(t.Context(), client, reqreply.Config{})
	if err != nil {
		t.Fatalf("reqreply.New: %v", err)
	}
	t.Cleanup(func() { _ = rr.Close() })
	for _, body := range []string{"ping", "pong"} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		reply, rerr := rr.Request(ctx, "rpc", &mq.Message{Body: []byte(body)})
		cancel()
		var herr error
		select {
		case herr = <-handled:
		default:
		}
		if rerr != nil {
			t.Fatalf("the request %q failed with %v; the responder's reply failed with %v (invalid topic: %t)",
				body, rerr, herr, errors.Is(herr, mq.ErrInvalidTopic))
		}
		if got := string(reply.Body); got != "re: "+body {
			t.Fatalf("the reply to %q is %q", body, got)
		}
	}
	seen.Range(func(k, _ any) bool {
		addr, _ := k.(string)
		t.Logf("the reply address the responder saw: %q", addr)
		if !rq.IsReplyAddress(addr) || mq.ValidateTopic(addr) != nil {
			t.Errorf("the responder saw the reply address %q, which is not a reply address and a valid topic", addr)
		}
		return true
	})
}

// TestARequestGetsItsOwnReply is the whole exchange, end to end.
func TestARequestGetsItsOwnReply(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	respondOn(t, client, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}
	})

	b := backendOf(t, client)
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	reply, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err != nil {
		t.Fatalf("RequestNative: %v", err)
	}
	if got := string(reply.Body); got != "re: ping" {
		t.Errorf("the reply's body is %q, want %q", got, "re: ping")
	}
}

// TestARequestAndItsReplyKeepTheirKeysUnderKeyAsRoutingKey holds the message
// key across both legs with Config.KeyAsRoutingKey. A request is routed by its
// topic and a reply by the reply address, so neither routing key is the key.
// A keyed request used to reach the responder with the topic as its key, a
// keyless one with the topic too, and every reply reached the requester with
// the reply address as its key.
func TestARequestAndItsReplyKeepTheirKeysUnderKeyAsRoutingKey(t *testing.T) {
	_, client := openFake(t, func(c *rq.Config) { c.Declare, c.KeyAsRoutingKey = true, true })
	var (
		mu   sync.Mutex
		seen []string
	)
	respondOn(t, client, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		mu.Lock()
		seen = append(seen, keyOf(d.Key))
		mu.Unlock()
		if string(d.Body) == "keyless" {
			return &mq.Message{Body: []byte("re")}
		}
		return &mq.Message{Key: []byte("reply-key"), Body: []byte("re")}
	})
	b := backendOf(t, client)
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	keyed, err := b.RequestNative(ctx, "rpc", &mq.Message{Key: []byte("customer-42"), Body: []byte("keyed")})
	if err != nil {
		t.Fatalf("the keyed request: %v", err)
	}
	keyless, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("keyless")})
	if err != nil {
		t.Fatalf("the keyless request: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != "customer-42" || seen[1] != "<none>" {
		t.Errorf("the responder saw request keys %q, want [customer-42 <none>]", seen)
	}
	if got := keyOf(keyed.Key); got != "reply-key" {
		t.Errorf("a keyed reply reached the requester with key %s, want reply-key", got)
	}
	if got := keyOf(keyless.Key); got != "<none>" {
		t.Errorf("a keyless reply reached the requester with key %s, want none", got)
	}
}

// keyOf is a message key for a failure message, with no key spelled out.
func keyOf(k []byte) string {
	if k == nil {
		return "<none>"
	}
	return string(k)
}

// TestConcurrentRequestsEachGetTheirOwnReply is what the correlation table is
// FOR.
//
// One channel and one consumer serve every request on a client, so the only
// thing that keeps two callers' replies apart is the correlation identifier.
// A driver that used a single waiter, or that matched on anything the caller
// controls, would pass a one-request test and cross the wires here.
func TestConcurrentRequestsEachGetTheirOwnReply(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	respondOn(t, client, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		// Echo the body, so a crossed reply is visible as the wrong one and
		// not merely as a missing one.
		return &mq.Message{Body: append([]byte(nil), d.Body...)}
	})

	b := backendOf(t, client)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := contextWithTimeout(t, 10*time.Second)
			defer cancel()
			body := "req-" + strconv.Itoa(i)
			reply, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte(body)})
			if err != nil {
				errs[i] = err
				return
			}
			got[i] = string(reply.Body)
		}()
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if want := "req-" + strconv.Itoa(i); got[i] != want {
			t.Errorf("request %d was answered with %q, want %q: the replies crossed, which is what the "+
				"correlation table exists to prevent", i, got[i], want)
		}
	}
}

// TestTheDriverOwnsTheCorrelationIdentifier.
//
// The specification gives the native path the whole exchange, including the correlation.
// A caller who sets CorrelationID has it OVERWRITTEN, because the driver's
// own table is keyed on it and a caller who reused a value would collect
// somebody else's reply. The driver page says so; this is what says so in a
// way that fails.
func TestTheDriverOwnsTheCorrelationIdentifier(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	r := respondOn(t, client, "rpc", "workers", func(mq.Delivery) *mq.Message {
		return &mq.Message{Body: []byte("pong")}
	})

	b := backendOf(t, client)
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	_, err := b.RequestNative(ctx, "rpc", &mq.Message{
		Body:       []byte("ping"),
		Properties: mq.Properties{CorrelationID: "the-caller's-own"},
	})
	if err != nil {
		t.Fatalf("RequestNative: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seen) == 0 {
		t.Fatal("the responder saw no request")
	}
	if r.seen[0] == "the-caller's-own" {
		t.Error("the caller's correlation identifier reached the wire; the driver's table is keyed on it, so " +
			"two callers who both set the same one would collect each other's replies")
	}
	if !strings.HasPrefix(r.seen[0], "rr-") {
		t.Errorf("the request's correlation identifier is %q, want the driver's own", r.seen[0])
	}
}

// TestTheRequestNamesTheReplyAddress: without `reply_to` there is nothing for
// a responder to answer, and the broker mints the per-request address from it.
func TestTheRequestNamesTheReplyAddress(t *testing.T) {
	t.Parallel()
	broker, client := openFake(t, declaring)
	respondOn(t, client, "rpc", "workers", func(mq.Delivery) *mq.Message {
		return &mq.Message{Body: []byte("pong")}
	})

	b := backendOf(t, client)
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	if _, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")}); err != nil {
		t.Fatalf("RequestNative: %v", err)
	}
	if !rq.IsReplyAddress(rq.ReplyAddress) {
		t.Error("IsReplyAddress does not recognise the address the driver itself publishes")
	}
	_ = broker
}

// TestARequestWithNoMessageIsRefused, rather than publishing an empty one.
func TestARequestWithNoMessageIsRefused(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	b := backendOf(t, client)

	_, err := b.RequestNative(t.Context(), "rpc", nil)
	if err == nil {
		t.Fatal("RequestNative with no message returned a reply")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidArgument {
		t.Errorf("RequestNative with no message is code %v, want CodeInvalidArgument", got)
	}
}

// TestARequestOnAClosedClientIsRefused.
func TestARequestOnAClosedClientIsRefused(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	b := backendOf(t, client)
	if err := client.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}

	if _, err := b.RequestNative(t.Context(), "rpc", &mq.Message{Body: []byte("ping")}); err == nil {
		t.Error("RequestNative on a closed client returned a reply; there is no channel to have sent it on")
	}
}

// TestARequestNobodyAnswersEndsAtTheDeadlineAndNotLater.
//
// The waiter is removed from the correlation table when the request gives up.
// A driver that left it there would leak an entry per timed-out request, and
// the reply that arrived late would be handed to nobody while still occupying
// the table.
func TestARequestNobodyAnswersEndsAtTheDeadlineAndNotLater(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	// A consumer exists so the request is routable, but nothing replies.
	consumeOn(t, client, "rpc", "workers")
	b := backendOf(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	if err == nil {
		t.Fatal("RequestNative returned a reply that nobody sent")
	}
	if !errors.Is(err, context.DeadlineExceeded) && mq.CodeOf(err) != mq.CodeDeadlineExceeded {
		t.Errorf("a request nobody answered failed with %v, want the deadline", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the request took %s to give up on a 150ms deadline", took)
	}
}

// TestARequestToATopicTheNamingRefusesFailsBeforeAnythingIsSent.
//
// A wildcard is not a topic on this broker -- a topic maps to its own
// exchange -- and the refusal has to happen on the request path too, not only
// on the publish path, or a request would be sent to an exchange named
// `orders.*`.
func TestARequestToATopicTheNamingRefusesFailsBeforeAnythingIsSent(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	b := backendOf(t, client)

	_, err := b.RequestNative(t.Context(), "rpc.*", &mq.Message{Body: []byte("ping")})
	if err == nil {
		t.Fatal("RequestNative to a wildcard topic returned a reply")
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("RequestNative to a wildcard topic is code %v, want CodeUnsupported", got)
	}
}

// TestIsReplyAddressRecognisesWhatTheBrokerMints.
//
// The broker substitutes a per-request address BENEATH the pseudo-queue's
// name, and the responder publishes to that. The sink has to route it around
// the naming layer, which refuses the reserved `amq.` prefix outright -- so a
// recogniser that matched only the bare name would make every reply fail to
// publish.
func TestIsReplyAddressRecognisesWhatTheBrokerMints(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{rq.ReplyAddress, true},
		{rq.ReplyAddress + ".g1AAAAB", true},
		{rq.ReplyAddress + ".", false}, // the prefix and nothing after it is not an address
		{"amq.rabbitmq.reply", false},
		{"orders", false},
		{"", false},
	} {
		if got := rq.IsReplyAddress(tc.topic); got != tc.want {
			t.Errorf("IsReplyAddress(%q) is %t, want %t", tc.topic, got, tc.want)
		}
	}
}

// TestAGivenUpRequestLeavesNothingInTheCorrelationTable.
//
// A waiter removed only when its reply ARRIVES leaks one entry per timed-out
// request, and a long-running client accumulates them for as long as it
// lives. Nothing else observes this: the request returns its deadline error
// either way, which is why the count is exported for tests at all.
func TestAGivenUpRequestLeavesNothingInTheCorrelationTable(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	consumeOn(t, client, "rpc", "workers") // routable, and nothing replies
	b := backendOf(t, client)

	for range 4 {
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
		_, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
		cancel()
		if err == nil {
			t.Fatal("RequestNative returned a reply that nobody sent")
		}
	}
	if got := b.WaitingRequestsForTest(); got != 0 {
		t.Errorf("the correlation table still holds %d waiters after four requests gave up, want 0: a client "+
			"that leaks one per timeout accumulates them for as long as it lives", got)
	}
}

// TestARequestToAReplyAddressGoesRoundTheNamingLayer.
//
// `internal/rabbit` refuses to derive a name from the reserved `amq.` prefix,
// so a request addressed to a reply address has to be RECOGNISED and routed
// onto the default exchange -- which routes by queue name, and a reply
// address is a name the broker minted. Without the special case the request
// is refused before anything is sent -- with InvalidArgument, naming the
// reserved prefix.
//
// What comes back is the request itself: the default exchange routes it to
// the queue of that name, which is the requester's own reply consumer. That
// is the faithful outcome of addressing yourself and it is not the point --
// the point is which failure does NOT happen.
func TestARequestToAReplyAddressGoesRoundTheNamingLayer(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	b := backendOf(t, client)

	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	_, err := b.RequestNative(ctx, rq.ReplyAddress, &mq.Message{Body: []byte("ping")})
	if err != nil && strings.Contains(err.Error(), "reserved prefix") {
		t.Errorf("a request to a reply address was refused by the naming layer (%v); the address has to be "+
			"recognised and routed onto the default exchange, which routes by queue name", err)
	}
}

// TestARequestToAMissingTopicFailsAlone is RequestNative publishing every
// request on the one direct reply-to channel, which it has to: the broker
// routes a reply to the channel the request was published on.
//
// A request to a topic whose exchange does not exist is a `basic.publish` to
// a missing exchange, which the broker answers with a 404 channel exception.
// That closed the reply consumer's channel, so every request in flight was
// told the endpoint had gone: a request to a healthy topic, already delivered
// to its responder, failed with "this handle has no channel", not retryable,
// and its reply was lost. The bad request was told the same instead of
// NotFound.
func TestARequestToAMissingTopicFailsAlone(t *testing.T) {
	t.Parallel()
	// An operator's client provisions "rpc"; the requester declares nothing,
	// so a topic nobody provisioned has no exchange.
	broker, ops := openFake(t, declaring)
	admin, err := ops.Admin()
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "rpc"}); err != nil {
		t.Fatal(err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "rpc", Name: "workers"}); err != nil {
		t.Fatal(err)
	}
	client, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), base())
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	respondOn(t, client, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		time.Sleep(300 * time.Millisecond)
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}
	})
	requestBesideAMissingTopic(t, backendOf(t, client))
}

// requestBesideAMissingTopic makes a request to "rpc" and, while it is in
// flight, one to a topic with no exchange.
func requestBesideAMissingTopic(t *testing.T, b *rq.Backend) {
	t.Helper()
	// The first request starts the endpoint, so both below share its channel.
	ctx, cancel := contextWithTimeout(t, 10*time.Second)
	defer cancel()
	if _, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("warm")}); err != nil {
		t.Fatalf("the first request: %v", err)
	}
	good := make(chan error, 1)
	go func() {
		reply, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
		if err == nil && string(reply.Body) != "re: ping" {
			err = errors.New("the reply is " + strconv.Quote(string(reply.Body)))
		}
		good <- err
	}()
	time.Sleep(50 * time.Millisecond)

	bctx, bcancel := contextWithTimeout(t, 2*time.Second)
	defer bcancel()
	_, badErr := b.RequestNative(bctx, "no-such-topic", &mq.Message{Body: []byte("x")})
	goodErr := <-good
	t.Logf("the request to the missing topic: %v (code %v)", badErr, mq.CodeOf(badErr))
	t.Logf("the request to the healthy topic, in flight at the same time: %v", goodErr)
	if goodErr != nil {
		t.Fatalf("a request to a healthy topic failed with %v because another request named a topic whose "+
			"exchange does not exist: both share the direct reply-to channel, the 404 closed it, and every "+
			"request in flight was told the endpoint had gone", goodErr)
	}
	if got := mq.CodeOf(badErr); got != mq.CodeNotFound {
		t.Errorf("the request to the missing topic failed with %v (code %v), want CodeNotFound", badErr, got)
	}
	// And the endpoint still serves.
	if _, err := b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("after")}); err != nil {
		t.Errorf("a request after the refused one: %v", err)
	}
}

// TestARequestAndCloseDoNotWaitForTheReplyChannelOpen is the reply endpoint's
// start under a broker slow to answer `channel.open`, the hazard
// [rq.Backend]'s bounded channel open exists for.
//
// requester.start held its lock across an unbounded channel open and
// `basic.consume`. RequestNative did not select on its context anywhere on
// that path, so a request with a 200 ms deadline waited for the broker; and
// since a borrowed client's Close stops the endpoint (which takes the same
// lock) before it closes the connection, which is the one thing that would
// end the parked open, Client.Close waited for the broker as well.
func TestARequestAndCloseDoNotWaitForTheReplyChannelOpen(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	b := backendOf(t, cl)
	defer dialer.release()

	dialer.arm()
	reqCtx, reqCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer reqCancel()
	requested := make(chan error, 1)
	go func() {
		_, err := b.RequestNative(reqCtx, "rpc", &mq.Message{Body: []byte("ping")})
		requested <- err
	}()
	dialer.waitReached(t)

	select {
	case err := <-requested:
		t.Logf("RequestNative returned %v while the channel open was parked", err)
		if err == nil {
			t.Error("RequestNative reported a reply while the reply channel was still opening")
		}
	case <-time.After(time.Second):
		t.Errorf("RequestNative with a 200ms context was still waiting 1s later: the reply endpoint's start " +
			"waits for channel.open with no bound and does not select on the caller's context")
	}

	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v", err)
	case <-time.After(2 * time.Second):
		dialer.release()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 2s later, and returned %v once the open was answered: the "+
			"teardown stops the reply endpoint, which waited for the lock its start holds across the parked "+
			"channel.open", err)
	}
}

// TestARelayedRequestKeepsTheBrokersReplyAddress is a relay: a consumer that
// forwards a request to a second topic with its Properties, as a gateway does,
// and the responder on that topic answers.
//
// A delivery carries the broker's reply address escaped, so that a responder
// can publish to it through the core. The escaped form went out on the wire
// when the relay published the Properties again: a responder that is not iqmq
// replied to `amq.rabbitmq.reply-to._...`, an address the broker does not
// know, and an iqmq responder escaped it a second time, which the driver
// undid only once. Either way the reply went nowhere.
func TestARelayedRequestKeepsTheBrokersReplyAddress(t *testing.T) {
	for _, native := range []bool{true, false} {
		name := "iqmq responder"
		if native {
			name = "amqp091 responder"
		}
		t.Run(name, func(t *testing.T) {
			_, client := openFake(t, declaring)
			relayRequests(t, client, native)
		})
	}
}

// relayRequests relays "rpc" to "rpc2" with each request's Properties, has
// "rpc2" answered by a responder of the kind native names, and makes a request
// to "rpc".
func relayRequests(t *testing.T, client *mq.Client, native bool) {
	t.Helper()
	publishOn(t, client, "rpc2") // declares the exchange the relay forwards to
	relay := consumeOn(t, client, "rpc", "relay")
	runCtx, stopRun := context.WithCancel(t.Context())
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		_ = relay.Run(runCtx, mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			return client.Publish(ctx, "rpc2", &mq.Message{Body: d.Body, Properties: d.Properties})
		}))
	}()
	t.Cleanup(func() { stopRun(); <-ran })

	var seen sync.Map
	if native {
		answerNatively(t, backendOf(t, client), &seen)
	} else {
		respondOn(t, client, "rpc2", "workers", func(d mq.Delivery) *mq.Message {
			seen.Store(d.Properties.ReplyTo, true)
			return &mq.Message{Body: append([]byte("re: "), d.Body...)}
		})
	}

	ctx, cancel := contextWithTimeout(t, 3*time.Second)
	defer cancel()
	reply, err := backendOf(t, client).RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	seen.Range(func(k, _ any) bool {
		t.Logf("the responder saw reply_to %q", k)
		return true
	})
	if err != nil {
		t.Fatalf("a request relayed with its Properties got no reply: %v", err)
	}
	if got := string(reply.Body); got != "re: ping" {
		t.Fatalf("the reply is %q", got)
	}
}

// answerNatively answers "rpc2" with amqp091 alone, the way a responder that
// is not iqmq does: it publishes to the `reply_to` it reads off the wire.
func answerNatively(t *testing.T, b *rq.Backend, seen *sync.Map) {
	t.Helper()
	ch, err := b.Conn().Channel()
	if err != nil {
		t.Fatal(err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	exchange, err := b.Names("rpc2", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(q.Name, "#", exchange.Exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	// Not the test's context: amqp091 cancels a consumer whose context ends
	// with a round trip of its own, and the test's context ends just before
	// the cleanup's channel close, a second round trip on the same channel
	// at the same moment, which the library does not support.
	deliveries, err := ch.ConsumeWithContext(context.Background(), q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		for d := range deliveries {
			seen.Store(d.ReplyTo, true)
			_ = ch.PublishWithContext(context.Background(), "", d.ReplyTo, false, false, amqp.Publishing{
				CorrelationId: d.CorrelationId, Body: append([]byte("re: "), d.Body...),
			})
		}
	}()
	t.Cleanup(func() { _ = ch.Close(); <-answered })
}

// TestRepliesToManyRequestersShareOneChannel is a responder answering
// requester clients that each live for one request, a CLI or a job.
//
// Every requester connection has its own reply address, and `Client.Publish`
// keeps a publisher for every topic it has published to until the client
// closes. Each reply address used to cost the responder a publisher with an
// AMQP channel and a goroutine of its own, so a responder answering such
// requesters gathered channels until the connection's channel_max, after which
// it could open none, its consumers' repairs included. Replies to every
// address now go out on one channel.
func TestRepliesToManyRequestersShareOneChannel(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	responder, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = responder.Close() })
	conn := broker.LastConnection()
	respondOn(t, responder, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		return &mq.Message{Body: d.Body}
	})
	request := func(i int) {
		t.Helper()
		requester, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		_, err = backendOf(t, requester).RequestNative(ctx, "rpc", &mq.Message{Body: []byte{byte(i)}})
		cancel()
		_ = requester.Close()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	// The first answer opens what the responder needs to answer at all: its
	// consumer's channel and whatever it replies on.
	request(0)
	before := len(conn.Channels())
	const requesters = 10
	for i := range requesters {
		request(i + 1)
	}
	after := len(conn.Channels())
	t.Logf("the responder had %d channels after its first answer, and %d after answering %d more requester "+
		"clients", before, after, requesters)
	if after > before {
		t.Fatalf("the responder holds %d channels after answering %d more requester clients that have all "+
			"closed, %d more than after the first: one per reply address, until it reaches channel_max", after,
			requesters, after-before)
	}
}

// TestASharedReplyPublisherWhoseRepairFailedIsReplaced is the shared reply
// channel meeting a channel exception whose one repair attempt fails.
//
// The shared publisher is an ordinary publisher: one repair, and after a
// failed one nothing attaches it until the connection is replaced. It used to
// be replaced only once closed, so from then on every reply to every
// requester failed, new requesters included, and the error told the
// responder to open a new publisher, which `Client.Publish`'s cache never
// does. Here the exception lands during an alarm, so the repair's
// `channel.open` times out.
func TestASharedReplyPublisherWhoseRepairFailedIsReplaced(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cfg.DialTimeout = 300 * time.Millisecond
	responder, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeBounded(t, responder)
	rb := backendOf(t, responder)
	conn := broker.LastConnection()
	respondOn(t, responder, "rpc", "workers", func(d mq.Delivery) *mq.Message { return &mq.Message{Body: d.Body} })

	request := func(body string) error {
		t.Helper()
		requester, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = requester.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err = backendOf(t, requester).RequestNative(ctx, "rpc", &mq.Message{Body: []byte(body)})
		return err
	}
	if err := request("A"); err != nil {
		t.Fatalf("request A: %v", err)
	}
	var shared *rabbitfake.ChannelHandle
	for _, ch := range conn.Channels() {
		if len(ch.Consumers()) == 0 && ch.IsConfirming() && ch.Published() > 0 {
			shared = ch
		}
	}
	if shared == nil {
		t.Fatal("the responder has no reply channel")
	}

	other := publishOn(t, responder, "other")
	conn.Alarm("low on disk")
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = other.Publish(pctx, &mq.Message{Body: []byte("x")})
	pcancel()
	waitFor(t, 5*time.Second, rb.Blocked)
	// A reply the broker refuses, an oversized one say, closes the shared
	// channel, and the repair's channel.open meets the alarm.
	closedShared := make(chan struct{})
	go func() {
		defer close(closedShared)
		shared.CloseWith(&amqp.Error{
			Code: amqp.PreconditionFailed, Server: true,
			Reason: "PRECONDITION_FAILED - message size 20000000 is larger than configured max size 16777216",
		})
	}()
	<-closedShared
	time.Sleep(time.Second)
	conn.ClearAlarm()
	waitFor(t, 5*time.Second, func() bool { return !rb.Blocked() })

	if err := request("B"); err != nil {
		t.Fatalf("after one failed repair of the shared reply channel, a new requester got no reply: %v", err)
	}
}

// TestClosingAReplyPublisherLetsGoOfItsReplies is a reply publisher closed
// with an asynchronous reply still waiting for its confirm.
//
// Every reply address shares one publisher, so a reply publisher's Close
// closes nothing. Its replies still waiting were left to the shared publisher,
// which went on to run their callbacks, and with Receipts to fill their
// receipts, after the core had handed the messages back to the caller: a
// write into a message the driver no longer borrowed. Close now answers them,
// as ambiguous, as the close of any other publisher does.
func TestClosingAReplyPublisherLetsGoOfItsReplies(t *testing.T) {
	broker, client := openFakeWith(t, rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		declaring)
	b := backendOf(t, client)
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: rq.ReplyAddress + ".gone", Receipts: true})
	if err != nil {
		t.Fatal(err)
	}
	async, ok := sink.(mq.AsyncSink)
	if !ok {
		t.Fatal("a reply publisher is not an AsyncSink")
	}
	var receipt mq.PublishReceipt
	m := &mq.Message{Body: []byte("re: ping"), Receipt: &receipt}
	var (
		calls atomic.Int32
		first atomic.Value
	)
	if err := async.PublishAsync(t.Context(), m, &mq.PublishOptions{}, func(err error) {
		if calls.Add(1) == 1 {
			first.Store(fmt.Sprint(err))
		}
	}); err != nil {
		t.Fatal(err)
	}
	ch := broker.LastChannel()
	waitFor(t, 5*time.Second, func() bool { return len(ch.PendingConfirms()) == 1 })

	if err := sink.Close(); err != nil {
		t.Fatalf("closing the reply publisher: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("the reply's callback ran %d times by the time Close returned, want once", got)
	}
	t.Logf("the reply's outcome at Close: %v", first.Load())
	for _, tag := range ch.PendingConfirms() {
		ch.AckPublication(tag)
	}
	broker.Sync()
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Errorf("the reply's callback ran %d times, want once", got)
	}
	if receipt != (mq.PublishReceipt{}) {
		t.Errorf("the broker's confirm after Close filled the receipt of a message handed back to its caller: %+v",
			receipt)
	}
}
