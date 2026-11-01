package stomp_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// Config.VerifyQueueAddress, against the in-process server.
//
// A source checks every delivery's address (foreignqueue_test.go), so a
// subscription attached to another address's queue hands nothing of it out.
// What that check cannot see is such a queue while it is empty: the attach
// succeeds and the source receives nothing at all. With VerifyQueueAddress
// the source asks Artemis's management address which address its queue is
// bound to before it subscribes. The server does not answer management
// requests on its own, so each test here answers the question the way the
// pinned Artemis does (management.go has the measurement): a MESSAGE on the
// reply queue the request names, with `_AMQ_OperationSucceeded` and a JSON
// array body. The real-broker twin is in collision_test.go.

// managementAddress is where the driver sends a management request.
const managementAddress = "activemq.management"

// question is one `getAddress` request the driver sent: the queue it asks
// about and the reply queue it waits on.
type question struct {
	queue, reply string
}

// awaitQuestion waits for the n-th `getAddress` request the server receives.
func awaitQuestion(srv *stompsrv.Server, n int) (question, error) {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		seen := 0
		for _, f := range srv.FramesOf(frame.SEND) {
			if f.Get(frame.Destination) != managementAddress || f.Get("_AMQ_OperationName") != "getAddress" {
				continue
			}
			seen++
			if seen == n {
				q := question{queue: strings.TrimPrefix(f.Get("_AMQ_ResourceName"), "queue."), reply: f.Get("reply-to")}
				return q, srv.WaitSubscribed(q.reply, 1, budget)
			}
		}
		time.Sleep(time.Millisecond)
	}
	return question{}, errors.New("the driver asked no getAddress question within the budget")
}

// questions is how many `getAddress` requests the server has received.
func questions(srv *stompsrv.Server) int {
	n := 0
	for _, f := range srv.FramesOf(frame.SEND) {
		if f.Get(frame.Destination) == managementAddress && f.Get("_AMQ_OperationName") == "getAddress" {
			n++
		}
	}
	return n
}

// answer is the reply Artemis sends to a `getAddress` request.
func answer(succeeded bool, body string) stompsrv.Message {
	return stompsrv.Message{
		Body:    []byte(body),
		Headers: [][2]string{{"_AMQ_OperationSucceeded", strconv.FormatBool(succeeded)}},
	}
}

// opened is what OpenSource returned on the goroutine a test opened it on.
type opened struct {
	src mq.Source
	err error
}

// openAnswered opens a source on a goroutine of its own, answers the n-th
// question the driver asks with reply, and returns what OpenSource returned
// and the question. The goroutine is waited for on every path.
func openAnswered(t *testing.T, b *stomp.Backend, srv *stompsrv.Server, topic, sub string, n int,
	reply stompsrv.Message,
) (opened, question) {
	t.Helper()
	done := make(chan opened, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: topic, Subscription: sub, Prefetch: 4})
		done <- opened{src: src, err: err}
	}()
	q, err := awaitQuestion(srv, n)
	if err == nil {
		_, err = srv.Deliver(q.reply, reply)
	}
	got := <-done
	if got.src != nil {
		t.Cleanup(func() { closeSource(t, got.src) })
	}
	if err != nil {
		t.Fatalf("answering the driver's question: %v (OpenSource returned %v)\nframes:%s", err, got.err, frameLog(srv))
	}
	return got, q
}

// verifying is an Artemis configuration that asks before it subscribes.
func verifying(srv *stompsrv.Server) stomp.Config {
	cfg := artemisConfig(srv)
	cfg.VerifyQueueAddress = true
	return cfg
}

// TestAVerifiedSourceRefusesAnEmptyQueueOfAnotherAddress is the case the
// delivery check cannot see. The fan-out subscription "orders"/"billing"
// SUBSCRIBEd to `orders::orders.billing`, Artemis attached it to another
// application's EMPTY JMS queue `orders.billing`, and the source received
// nothing, its own topic's publications included, with no error until that
// queue's first message. With VerifyQueueAddress, OpenSource asks first and
// refuses with a Conflict that is not retryable, and it never subscribes.
func TestAVerifiedSourceRefusesAnEmptyQueueOfAnotherAddress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, topic, sub, bound string
		addressing              stomp.Addressing
	}{
		{name: "fan-out over a JMS queue", topic: "orders", sub: "billing", bound: "orders.billing"},
		{name: "fan-out named like its topic over a JMS queue", topic: "orders", sub: "orders", bound: "orders.orders"},
		{
			name: "AddressQueue over a subscription's queue", topic: "orders.billing", sub: "x", bound: "orders",
			addressing: stomp.AddressQueue,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			cfg := verifying(srv)
			cfg.Addressing = tc.addressing
			b := openBackend(t, cfg)
			got, q := openAnswered(t, b, srv, tc.topic, tc.sub, 1, answer(true, `["`+tc.bound+`"]`))
			if code := mq.CodeOf(got.err); code != mq.CodeConflict || mq.IsRetryable(got.err) {
				t.Fatalf("OpenSource(%s, %s) over a queue bound to %s returned %v (code %v), want a Conflict that "+
					"is not retryable\nframes:%s", tc.topic, tc.sub, tc.bound, got.err, code, frameLog(srv))
			}
			if !strings.Contains(got.err.Error(), `"`+tc.bound+`"`) {
				t.Errorf("the refusal %v does not name %s, the address that holds the queue", got.err, tc.bound)
			}
			_, consume := destinations(t, cfg, tc.topic, tc.sub)
			if n := srv.Subscribes(consume); n != 0 {
				t.Errorf("the source sent %d SUBSCRIBE frames to %s after the broker said its queue is "+
					"another address's\nframes:%s", n, consume, frameLog(srv))
			}
			want := tc.topic + "." + tc.sub
			if tc.addressing == stomp.AddressQueue {
				want = tc.topic
			}
			if q.queue != want {
				t.Errorf("the driver asked about the queue %q, want %q", q.queue, want)
			}
			assertReplyQueueGone(t, srv, q.reply)
		})
	}

	t.Run("without the flag the attach succeeds", func(t *testing.T) {
		// What a deployment that has not set it gets: the SUBSCRIBE goes out
		// and nothing is asked. The delivery check still refuses the first
		// message from the other address.
		t.Parallel()
		srv := artemisServer(t)
		cfg := artemisConfig(srv)
		b := openBackend(t, cfg)
		openSource(t, b, cfg, srv, "orders", "billing", 4)
		if n := len(srv.FramesOf(frame.SEND)); n != 0 {
			t.Errorf("a source with VerifyQueueAddress off sent %d SEND frames\nframes:%s", n, frameLog(srv))
		}
	})
}

// assertReplyQueueGone checks that the driver gave its reply queue back: it
// unsubscribed from it and asked the broker to destroy it.
func assertReplyQueueGone(t *testing.T, srv *stompsrv.Server, reply string) {
	t.Helper()
	await(t, "the reply queue's destroyQueue request", func() bool {
		for _, f := range srv.FramesOf(frame.SEND) {
			if f.Get("_AMQ_OperationName") == "destroyQueue" && strings.Contains(string(f.Body), `"`+reply+`"`) {
				return true
			}
		}
		return false
	})
	if n := srv.Live(reply); n != 0 {
		t.Errorf("the driver left %d live subscriptions on its reply queue %s", n, reply)
	}
	for _, f := range srv.FramesOf(frame.SEND) {
		if f.Get(frame.Destination) == managementAddress && f.Has(frame.ContentLength) {
			t.Errorf("a management request carried content-length, which Artemis refuses with AMQ339011: %s", f)
		}
	}
}

// TestAVerifiedSourceSubscribesToItsOwnQueue is the other side: a queue bound
// to the subscription's own address, and a queue the broker does not have
// yet, which the SUBSCRIBE reports or creates as the broker is configured to.
// A requeue keeps its connection and does not ask again.
func TestAVerifiedSourceSubscribesToItsOwnQueue(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		reply stompsrv.Message
	}{
		{name: "the subscription's own queue", reply: answer(true, `["orders"]`)},
		{
			name:  "no queue of that name yet",
			reply: answer(false, `["AMQ229067: Cannot find resource with name queue.orders.billing"]`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			cfg := verifying(srv)
			b := openBackend(t, cfg)
			got, q := openAnswered(t, b, srv, "orders", "billing", 1, tc.reply)
			if got.err != nil {
				t.Fatalf("OpenSource: %v\nframes:%s", got.err, frameLog(srv))
			}
			_, consume := destinations(t, cfg, "orders", "billing")
			if err := srv.WaitSubscribed(consume, 1, budget); err != nil {
				t.Fatalf("the source never subscribed to %s: %v", consume, err)
			}
			assertReplyQueueGone(t, srv, q.reply)
			deliver(t, srv, consume, body("an order"))
			d := fetchN(t, got.src, 1)[0]
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			if err := got.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err != nil {
				t.Fatalf("Nack(requeue): %v", err)
			}
			if err := srv.WaitSubscribed(consume, 2, budget); err != nil {
				t.Fatalf("the requeue never subscribed again: %v", err)
			}
			if n := questions(srv); n != 1 {
				t.Errorf("the driver asked %d questions, want 1: a requeue that keeps its connection does not ask "+
					"again", n)
			}
		})
	}
}

// TestAVerifiedSourceAsksAgainOnANewConnection is a source whose connection
// died and whose queue, on the connection it dials next, is another
// address's: a broker restored from another node's topology, or a queue
// recreated by another application while this one was away. The fetch that
// re-dials asks again, refuses with the Conflict, and the source never
// subscribes again.
func TestAVerifiedSourceAsksAgainOnANewConnection(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(verifying(srv))
	b := openBackend(t, cfg)
	got, _ := openAnswered(t, b, srv, "orders", "billing", 1, answer(true, `["orders"]`))
	if got.err != nil {
		t.Fatalf("OpenSource: %v", got.err)
	}
	_, consume := destinations(t, cfg, "orders", "billing")
	if err := srv.WaitSubscribed(consume, 1, budget); err != nil {
		t.Fatalf("the source never subscribed: %v", err)
	}
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}

	// The fetch that re-dials asks, on a goroutine, while this one answers.
	fetched := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		dst := make([]mq.DriverDelivery, 1)
		for {
			_, err := got.src.Fetch(ctx, dst)
			if err == nil || mq.IsRetryable(err) && ctx.Err() == nil {
				time.Sleep(time.Millisecond)
				continue
			}
			fetched <- err
			return
		}
	}()
	q, err := awaitQuestion(srv, 2)
	if err == nil {
		_, err = srv.Deliver(q.reply, answer(true, `["orders.billing"]`))
	}
	ferr := <-fetched
	if err != nil {
		t.Fatalf("answering the second question: %v (the fetch returned %v)\nframes:%s", err, ferr, frameLog(srv))
	}
	if code := mq.CodeOf(ferr); code != mq.CodeConflict || mq.IsRetryable(ferr) {
		t.Fatalf("the fetch after the re-dial returned %v (code %v), want a Conflict that is not retryable",
			ferr, code)
	}
	if n := srv.Subscribes(consume); n != 1 {
		t.Errorf("the source sent %d SUBSCRIBE frames to %s, want only the first\nframes:%s", n, consume, frameLog(srv))
	}
	again := fetchError(t, got.src, 100*time.Millisecond)
	if mq.CodeOf(again) != mq.CodeConflict {
		t.Errorf("the next fetch returned %v, want the same Conflict", again)
	}
	if n := questions(srv); n != 2 {
		t.Errorf("the driver asked %d questions, want 2: a refused source asks nothing more", n)
	}
}

// TestAVerifiedSourceIsRefusedWhenTheAccountCannotManage is an account
// without the `manage` permission. Measured on the pinned Artemis, its first
// management request is answered with an ERROR frame whose sentence names no
// cause, and the connection closes. OpenSource fails with Forbidden, not
// retryable, and the source's own connection, which the question did not
// use, sent no SUBSCRIBE.
func TestAVerifiedSourceIsRefusedWhenTheAccountCannotManage(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := verifying(srv)
	b := openBackend(t, cfg)
	srv.FailSend(managementAddress, "AMQ339011 Error sending message CoreMessage[messageID=378, durable=false, "+
		"address=activemq.management, properties=TypedProperties[destination=activemq.management, "+
		"_AMQ_OperationName=createQueue, _AMQ_ResourceName=broker]]@593323435")
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
	if src != nil {
		closeSource(t, src)
	}
	if code := mq.CodeOf(err); code != mq.CodeForbidden || mq.IsRetryable(err) {
		t.Fatalf("OpenSource with a refused management request returned %v (code %v), want Forbidden and not "+
			"retryable", err, code)
	}
	if !strings.Contains(err.Error(), "manage permission") {
		t.Errorf("the refusal %v does not say what the broker's sentence most likely means", err)
	}
	_, consume := destinations(t, cfg, "orders", "billing")
	if n := srv.Subscribes(consume); n != 0 {
		t.Errorf("the source subscribed %d times without an answer\nframes:%s", n, frameLog(srv))
	}
}

// TestAVerifiedSourceThatGetsNoAnswerMayRetry is a broker that takes the
// question and never answers. The question is a round trip like any other of
// this driver's, so it ends at Config.ReceiptTimeout with a retryable
// Unavailable: nothing was subscribed, and a retry can be answered.
func TestAVerifiedSourceThatGetsNoAnswerMayRetry(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := verifying(srv)
	b := openBackend(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
	took := time.Since(start)
	if src != nil {
		closeSource(t, src)
	}
	if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) {
		t.Fatalf("OpenSource with an unanswered question returned %v (code %v), want a retryable Unavailable",
			err, code)
	}
	if took > budget/2 {
		t.Errorf("OpenSource took %v with a ReceiptTimeout of %v", took, cfg.ReceiptTimeout)
	}
	if n := questions(srv); n != 1 {
		t.Errorf("the driver asked %d questions, want 1", n)
	}
}
