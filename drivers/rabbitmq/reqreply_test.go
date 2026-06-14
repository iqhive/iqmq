package rabbitmq_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// Native request/reply over direct reply-to.
//
// reqreply.go was 3.1 % covered -- after `make ci` AND after every conformance
// suite, because the kit has no request/reply scenario and `reqreply`'s own
// tests drive a stub rather than this driver. A headline feature
// therefore shipped with its implementation exercised by nothing.
//
// The fake grew the pseudo-queue for these : a consumer on
// `amq.rabbitmq.reply-to` conjures it, the default exchange already binds
// every queue by its own name, and the reply routes back with no further
// help.

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
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: rq.ReplyAddress})
	if err != nil {
		t.Fatalf("opening the reply publisher: %v", err)
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { _ = pub.Close() }()
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
				// whole reason the sink recognises a reply address.
				m.Properties.CorrelationID = d.Properties.CorrelationID
				if got := d.Properties.ReplyTo; got != rq.ReplyAddress {
					r.t.Errorf("the request's reply_to is %q, want the direct reply-to address", got)
				}
				// A context of its own: the receive's has already been
				// cancelled, and publishing the reply on it would cancel
				// every reply this responder ever sends.
				pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := pub.Publish(pctx, m)
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
