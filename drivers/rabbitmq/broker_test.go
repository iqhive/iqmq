package rabbitmq_test

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
)

// The tests in this file run against a real RabbitMQ, for the behaviour the
// fake was taught from one: the reply address the broker writes into a
// request, and a connection the broker stops reading under a resource alarm.
//
// They are env-gated, as the AMQP 1.0 profile's suite is: IQMQ_RABBITMQ_ADDR
// is host:port, IQMQ_RABBITMQ_USER and IQMQ_RABBITMQ_PASSWORD the credentials
// (guest and guest when unset), and the alarm tests also need
// IQMQ_RABBITMQ_CONTAINER, the docker container the broker runs in, to raise
// and clear the alarm with rabbitmqctl. With nothing set they skip. The alarm
// is broker-wide, so those tests are not parallel and clear it on the way
// out.

// brokerConfig is a Config for the broker the environment names, with a
// prefix of its own so that a run leaves its topology apart from any other's,
// or it skips the test.
func brokerConfig(t *testing.T) rq.Config {
	t.Helper()
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR (host:port) to run this against a RabbitMQ broker")
	}
	cfg := base()
	cfg.Addrs = strings.Split(addr, ",")
	cfg.Username, cfg.Password = "guest", dsn.Secret("guest")
	if u := os.Getenv("IQMQ_RABBITMQ_USER"); u != "" {
		cfg.Username, cfg.Password = u, dsn.Secret(os.Getenv("IQMQ_RABBITMQ_PASSWORD"))
	}
	cfg.Declare = true
	cfg.Prefix = "iqmq-test-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
	return cfg
}

// openBroker opens a client on the broker brokerConfig names.
func openBroker(t *testing.T, cfg rq.Config) *mq.Client {
	t.Helper()
	cl, err := rq.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("opening a client on the broker: %v", err)
	}
	return cl
}

// TestNativeRequestReplyOnABroker is
// TestAReplyReachesTheAddressTheBrokerSubstitutes on RabbitMQ, where the
// address in `reply_to` is the broker's own.
func TestNativeRequestReplyOnABroker(t *testing.T) {
	cl := openBroker(t, brokerConfig(t))
	t.Cleanup(func() { _ = cl.Close() })
	requestThroughReqreply(t, cl)
}

// TestARelayedRequestKeepsTheBrokersReplyAddressOnABroker is
// TestARelayedRequestKeepsTheBrokersReplyAddress on RabbitMQ, with an amqp091
// responder answering the relayed request.
func TestARelayedRequestKeepsTheBrokersReplyAddressOnABroker(t *testing.T) {
	cl := openBroker(t, brokerConfig(t))
	t.Cleanup(func() { _ = cl.Close() })
	relayRequests(t, cl, true)
}

// TestRepliesToManyRequestersShareOneChannelOnABroker is
// TestRepliesToManyRequestersShareOneChannel on RabbitMQ, which takes a reply
// to a direct reply-to address on any channel. One requester gives up before
// its reply is sent, so the shared channel also publishes to an address whose
// channel has gone, and the requesters after it must still be answered.
func TestRepliesToManyRequestersShareOneChannelOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	responder := openBroker(t, cfg)
	t.Cleanup(func() { _ = responder.Close() })
	respondOn(t, responder, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		if string(d.Body) == "slow" {
			time.Sleep(500 * time.Millisecond)
		}
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}
	})
	request := func(body string, within time.Duration) error {
		t.Helper()
		requester := openBroker(t, cfg)
		defer func() { _ = requester.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), within)
		defer cancel()
		reply, err := backendOf(t, requester).RequestNative(ctx, "rpc", &mq.Message{Body: []byte(body)})
		if err == nil && string(reply.Body) != "re: "+body {
			t.Errorf("the reply to %q is %q", body, reply.Body)
		}
		return err
	}
	for i := range 3 {
		if err := request("ping"+strconv.Itoa(i), 5*time.Second); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if err := request("slow", 100*time.Millisecond); err == nil {
		t.Fatal("the slow request was answered within 100ms, so its reply did not go to an address that had gone")
	}
	time.Sleep(time.Second) // the reply to the requester that has gone
	for i := range 3 {
		if err := request("after"+strconv.Itoa(i), 5*time.Second); err != nil {
			t.Fatalf("a request after a reply to an address that had gone: %v", err)
		}
	}
}

// TestARequestToAMissingTopicFailsAloneOnABroker is
// TestARequestToAMissingTopicFailsAlone on RabbitMQ, with a requester that
// declares nothing.
func TestARequestToAMissingTopicFailsAloneOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	ops := openBroker(t, cfg)
	t.Cleanup(func() { _ = ops.Close() })
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = admin.DeleteSubscription(ctx, "rpc", "workers")
		_ = admin.DeleteTopic(ctx, "rpc")
	})
	cfg.Declare = false
	client := openBroker(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	respondOn(t, client, "rpc", "workers", func(d mq.Delivery) *mq.Message {
		time.Sleep(300 * time.Millisecond)
		return &mq.Message{Body: append([]byte("re: "), d.Body...)}
	})
	requestBesideAMissingTopic(t, backendOf(t, client))
}

// TestCloseReturnsUnderAResourceAlarmOnABroker is
// TestCloseReturnsUnderAResourceAlarm under a real memory alarm.
func TestCloseReturnsUnderAResourceAlarmOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	cl := openBroker(t, cfg)
	b := backendOf(t, cl)
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "held" {
		t.Fatalf("received %q", d.Body)
	}

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	t.Cleanup(func() { rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6") })
	// The publication is what makes the broker block this connection.
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	perr := pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	t.Logf("the publication the alarm caught: %v", perr)
	waitFor(t, 20*time.Second, b.Blocked)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v after %v", err, time.Since(start))
	case <-time.After(10 * time.Second):
		rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		err := <-closed
		t.Fatalf("Client.Close was still waiting 10s into the alarm, and returned %v only %v after it began, "+
			"once the alarm cleared", err, time.Since(start))
	}

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
	check := openBroker(t, cfg)
	t.Cleanup(func() { _ = check.Close() })
	again := consumeOn(t, check, "orders", "workers")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var got []string
	dst := make([]mq.Delivery, 4)
	for len(got) < 2 {
		n, err := again.Receive(ctx, dst)
		if err != nil {
			break
		}
		for i := range n {
			got = append(got, string(dst[i].Body))
			_ = dst[i].Ack()
		}
	}
	t.Logf("in the queue after the alarm: %q", got)
	for _, want := range []string{"held", "caught"} {
		if !containsString(got, want) {
			t.Errorf("%q is not in the queue after the alarm: %q", want, got)
		}
	}
	admin, err := check.Admin()
	if err == nil {
		_ = admin.DeleteSubscription(t.Context(), "orders", "workers")
		_ = admin.DeleteTopic(t.Context(), "orders")
	}
}

// TestAFlappingAlarmLeavesTheDriverReadingOnABroker is
// TestCloseReturnsUnderAFlappingAlarm on RabbitMQ: an alarm that comes back
// before the broker has read what the client wrote when it first cleared.
//
// With the supervision loop closing a consumer's channel when the first alarm
// cleared, the loop waited in that round trip through the second alarm, the
// library abandoned the second `connection.unblocked` after five seconds, and
// the driver read the connection as blocked for good with no alarm on the
// broker: a consumer closed after that kept its channel and the message it
// held. The hook, which hears `connection.unblocked`, raises the second alarm
// inside that window every time.
func TestAFlappingAlarmLeavesTheDriverReadingOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	// The hook runs on the driver's goroutine, where a test may not stop, so
	// a failure there is only reported.
	ctl := func(args ...string) {
		out, err := exec.Command("docker", append([]string{"exec", container, "rabbitmqctl"}, args...)...).CombinedOutput()
		if err != nil {
			t.Errorf("rabbitmqctl %v: %v: %s", args, err, out)
		}
	}
	var (
		armed  atomic.Bool
		pubP   atomic.Pointer[mq.PublisherHandle]
		hooked = make(chan struct{})
		again  sync.WaitGroup
	)
	t.Cleanup(again.Wait)
	cfg.Hooks.OnReconnect = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt != 0 || ev.Err != nil || !armed.CompareAndSwap(true, false) {
			return
		}
		ctl("set_vm_memory_high_watermark", "absolute", "1")
		again.Go(func() {
			pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pubP.Load().Publish(pctx, &mq.Message{Body: []byte("caught2")})
		})
		time.Sleep(1500 * time.Millisecond)
		close(hooked)
	}
	cl := openBroker(t, cfg)
	b := backendOf(t, cl)
	t.Cleanup(func() { _ = cl.Close() })
	t.Cleanup(func() { ctl("set_vm_memory_high_watermark", "0.6") })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	pubP.Store(pub)
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)

	ctl("set_vm_memory_high_watermark", "absolute", "1")
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)
	if err := cons.Close(); err != nil {
		t.Fatalf("closing the consumer during the first alarm: %v", err)
	}
	armed.Store(true)
	ctl("set_vm_memory_high_watermark", "0.6")
	select {
	case <-hooked:
	case <-time.After(20 * time.Second):
		t.Fatal("the hook never heard connection.unblocked")
	}
	time.Sleep(time.Second)
	ctl("set_vm_memory_high_watermark", "0.6")
	// The library's five-second abandonment, and some.
	waitFor(t, 15*time.Second, func() bool { return !b.Blocked() })
	if got := cl.Health().State; got != mq.HealthReady {
		t.Errorf("health is %v with no alarm on the broker, want ready", got)
	}

	// What a stale "blocked" did to a consumer closed after the alarms.
	c2 := consumeOn(t, cl, "orders", "workers")
	dctx, dcancel := context.WithTimeout(t.Context(), 2*time.Second)
	dst := make([]mq.Delivery, 8)
	for {
		n, err := c2.Receive(dctx, dst)
		if err != nil {
			break
		}
		for i := range n {
			_ = dst[i].Ack()
		}
	}
	dcancel()
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, c2); string(d.Body) != "after" {
		t.Fatalf("received %q", d.Body)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("closing the second consumer: %v", err)
	}
	c3 := consumeOn(t, cl, "orders", "workers")
	rctx, rcancel := context.WithTimeout(t.Context(), 5*time.Second)
	n, err := c3.Receive(rctx, dst)
	rcancel()
	if err != nil || n == 0 || string(dst[0].Body) != "after" {
		t.Fatalf("a consumer opened after the second one closed received %d (%v): the second one's channel, "+
			"and the message it held, were kept", n, err)
	}
	_ = dst[0].Ack()
}

// rabbitmqctl runs rabbitmqctl in the broker's container.
func rabbitmqctl(t *testing.T, container string, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"exec", container, "rabbitmqctl"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("rabbitmqctl %v: %v: %s", args, err, out)
	}
}
