package rabbitmq_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
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

// TestAnAlarmThatBeginsDuringAReconnectionIsHeardOnABroker is
// TestAnAlarmThatBeginsDuringAReconnectionIsHeard on RabbitMQ: a memory alarm
// is in effect, the broker closes the client's connection, and a publisher
// goes on publishing while the dial loop restores the client.
func TestAnAlarmThatBeginsDuringAReconnectionIsHeardOnABroker(t *testing.T) {
	for _, clear := range []bool{false, true} {
		name := "Close"
		if clear {
			name = "the alarm clears"
		}
		t.Run(name, func(t *testing.T) { alarmDuringAReconnectionOnABroker(t, clear) })
	}
}

func alarmDuringAReconnectionOnABroker(t *testing.T, clear bool) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	var failedDials atomic.Int32
	cfg.DialTimeout = time.Second
	cfg.Hooks.OnReconnect = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt > 0 && ev.Err != nil {
			failedDials.Add(1)
		}
	}
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)
	stop := make(chan struct{})
	var publisher sync.WaitGroup
	t.Cleanup(publisher.Wait)
	t.Cleanup(func() { close(stop) })
	orig := b.Conn()
	publisher.Go(func() {
		// The dial loop installs the new connection before it re-attaches
		// the publisher, which publishes from then on.
		for cn := b.Conn(); cn == nil || cn == orig; cn = b.Conn() {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_ = pub.Publish(ctx, &mq.Message{Body: []byte("during the reconnection")})
			cancel()
		}
	})
	rabbitmqctl(t, container, "close_all_connections", "an alarm during a reconnection")
	for deadline := time.Now().Add(15 * time.Second); !b.Blocked(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the broker blocked the connection the dial loop was restoring and the driver never heard it "+
				"(health %v)", cl.Health().State)
		}
	}
	alarmed := b.Conn()
	t.Logf("the alarm heard: health %v", cl.Health().State)

	if clear {
		time.Sleep(4 * cfg.DialTimeout)
		if got := failedDials.Load(); got != 0 || b.Conn() != alarmed {
			t.Fatalf("four DialTimeouts into the alarm %d attempts had failed and the connection was replaced: %v",
				got, b.Conn() != alarmed)
		}
		clearAlarm()
		waitFor(t, 20*time.Second, func() bool { return !b.Blocked() && cl.Health().State == mq.HealthReady })
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 16)
		for seen := false; !seen; {
			n, err := cons.Receive(ctx, dst)
			if err != nil {
				t.Fatalf("the consumer received nothing written during the reconnection once the alarm cleared: %v", err)
			}
			for i := range n {
				seen = seen || string(dst[i].Body) == "during the reconnection"
				_ = dst[i].Ack()
			}
		}
	}
	if !clear {
		start := time.Now()
		closed := make(chan error, 1)
		go func() { closed <- cl.Close() }()
		select {
		case err := <-closed:
			t.Logf("Client.Close returned %v after %v", err, time.Since(start))
		case <-time.After(10 * time.Second):
			clearAlarm()
			err := <-closed
			t.Fatalf("Client.Close was still waiting 10s into the alarm, and returned %v only once it cleared, %v "+
				"after it began", err, time.Since(start))
		}
	}
	clearAlarm()
	deleteOrders(t, cfg)
}

// TestAConsumerClosedBehindAFullSocketReturnsOnABroker is
// TestAConsumerClosedBehindAFullSocketReturns under a real memory alarm, with
// publications of 8 MiB to fill the socket's buffers.
func TestAConsumerClosedBehindAFullSocketReturnsOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	for _, body := range []string{"held", "unread"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	if d := receiveOne(t, cons); string(d.Body) != "held" {
		t.Fatalf("received %q", d.Body)
	}

	var pubs sync.WaitGroup
	t.Cleanup(pubs.Wait)
	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)
	big := make([]byte, 8<<20)
	for range 4 {
		pubs.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			_ = pub.Publish(ctx, &mq.Message{Body: big})
		})
	}
	time.Sleep(3 * time.Second)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		t.Logf("the consumer's Close returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		clearAlarm()
		err := <-closed
		t.Fatalf("the consumer's Close was still waiting 5s into the alarm behind publications that filled the "+
			"socket, and returned %v only once it cleared, %v after it began", err, time.Since(start))
	}
	clearAlarm()
	pubs.Wait()
	deleteOrders(t, cfg)
}

// TestADelayedNackDuringAnAlarmIsRefusedAtOnceOnABroker is
// TestADelayedNackDuringAnAlarmIsRefusedAtOnce under a real memory alarm, on a
// delay queue with `x-expires`, which a delayed nack declares again before it
// parks the copy.
func TestADelayedNackDuringAnAlarmIsRefusedAtOnceOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	cfg.DelayKind = delay.TTLDLX
	cfg.DelayQueueTTL = 2 * time.Minute
	cl := openBroker(t, cfg)
	b := backendOf(t, cl)
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	t.Cleanup(func() { rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6") })
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)

	start := time.Now()
	nacked := make(chan error, 1)
	go func() { nacked <- d.Nack(mq.NackDelay(time.Second)) }()
	var err error
	select {
	case err = <-nacked:
		t.Logf("the delayed nack during the alarm returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		err = <-nacked
		t.Fatalf("the delayed nack was still waiting 5s into the alarm, and returned %v %v after it began, "+
			"once the alarm cleared", err, time.Since(start))
	}
	if mq.CodeOf(err) != mq.CodeBackpressure || mq.IsAmbiguous(err) {
		t.Fatalf("the delayed nack during the alarm reported %v, want a definite Backpressure", err)
	}

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
	waitFor(t, 20*time.Second, func() bool { return !b.Blocked() })
	if err := d.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("the delayed nack once the alarm cleared: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var got []string
	dst := make([]mq.Delivery, 4)
	for !containsString(got, "held") || !containsString(got, "caught") {
		n, err := cons.Receive(ctx, dst)
		if err != nil {
			t.Fatalf("received %q, want the parked copy of %q and %q: %v", got, "held", "caught", err)
		}
		for i := range n {
			got = append(got, string(dst[i].Body))
			_ = dst[i].Ack()
		}
	}
	t.Logf("received after the alarm: %q", got)
	if n := countOf(got, "held"); n != 1 {
		t.Fatalf("%q came back %d times, want once", "held", n)
	}
	deleteOrders(t, cfg)
}

// TestARefusedDelayQueueDeclarationLeavesTheConsumersOtherDeliveriesOnABroker
// is TestARefusedDelayQueueDeclarationLeavesTheConsumersOtherDeliveries on
// RabbitMQ, which refuses a queue declared again with another `x-expires`.
func TestARefusedDelayQueueDeclarationLeavesTheConsumersOtherDeliveriesOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	cfg.DelayKind = delay.TTLDLX
	otherCfg := cfg
	otherCfg.DelayQueueTTL = 3 * time.Minute
	other := openBroker(t, otherCfg)
	t.Cleanup(func() { _ = other.Close() })
	otherCons := consumeOn(t, other, "orders", "workers")
	if err := publishOn(t, other, "orders").Publish(t.Context(), &mq.Message{Body: []byte("other")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, otherCons); d.Nack(mq.NackDelay(3*time.Second)) != nil {
		t.Fatal("the other client's delayed nack failed")
	}
	if err := otherCons.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.DelayQueueTTL = 2 * time.Minute
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 8 })
	pub := publishOn(t, cl, "orders")
	for _, body := range []string{"X", "Y"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var x, y mq.Delivery
	dst := make([]mq.Delivery, 8)
	for x.Body == nil || y.Body == nil {
		n, err := cons.Receive(ctx, dst)
		if err != nil {
			t.Fatalf("receiving X and Y: %v", err)
		}
		for i := range n {
			switch string(dst[i].Body) {
			case "X":
				x = dst[i]
			case "Y":
				y = dst[i]
			default:
				_ = dst[i].Ack()
			}
		}
	}

	nerr := x.Nack(mq.NackDelay(3 * time.Second))
	code, _ := rq.ReplyCode(nerr)
	t.Logf("X's delayed nack: %v (reply code %d, portable code %v)", nerr, code, mq.CodeOf(nerr))
	if code != amqp.PreconditionFailed || mq.CodeOf(nerr) != mq.CodeConflict || mq.IsAmbiguous(nerr) {
		t.Fatalf("X's delayed nack reported %v, want the broker's 406 as a definite Conflict", nerr)
	}
	if err := y.Ack(); err != nil {
		t.Fatalf("Y's Ack after X's refused delayed nack: %v", err)
	}
	if err := x.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("a delayed nack of X after the refusal: %v", err)
	}
	var got []string
	for !containsString(got, "X") {
		n, err := cons.Receive(ctx, dst)
		if err != nil {
			t.Fatalf("X's parked copy never came back (%v); received %q", err, got)
		}
		for i := range n {
			got = append(got, string(dst[i].Body))
			_ = dst[i].Ack()
		}
	}
	t.Logf("received after the refusal: %q", got)
	if containsString(got, "Y") {
		t.Fatalf("Y was delivered again after its Ack: %q", got)
	}
	deleteOrders(t, cfg)
}

// TestAClosedBorrowedClientLeavesNoAlarmListenerOnABroker is
// TestAClosedBorrowedClientLeavesNoAlarmListenerOnTheLentConnection on
// RabbitMQ, where the client library holds each notification up to five
// seconds for a listener nobody reads.
func TestAClosedBorrowedClientLeavesNoAlarmListenerOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	conn, err := amqp.Dial("amqp://" + cfg.Username + ":" + cfg.Password.Reveal() + "@" + cfg.Addrs[0] + "/")
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cl, err := rq.OpenWithClient(t.Context(), conn, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("closing the borrowed client: %v", err)
	}
	callers := conn.NotifyBlocked(make(chan amqp.Blocking, 4))
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	for round := 1; round <= 2; round++ {
		rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
		pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
		_ = ch.PublishWithContext(pctx, "", "iqmq-test-nowhere", false, false, amqp.Publishing{Body: []byte("x")})
		pcancel()
		select {
		case blk := <-callers:
			if !blk.Active {
				t.Fatalf("round %d: the first notification is %+v, want connection.blocked", round, blk)
			}
		case <-time.After(20 * time.Second):
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
			t.Fatalf("round %d: connection.blocked never reached the caller's listener", round)
		}
		cleared := time.Now()
		rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		select {
		case blk := <-callers:
			took := time.Since(cleared)
			t.Logf("round %d: connection.unblocked reached the caller's listener %v after the alarm cleared", round, took)
			if blk.Active || took > 4*time.Second {
				t.Fatalf("round %d: %+v reached the caller's listener %v after the alarm cleared: the closed "+
					"client's listener, which nobody reads, held it", round, blk, took)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: connection.unblocked never reached the caller's listener", round)
		}
	}
}

// TestClientCloseEndsAPublicationParkedInAFullSocketOnABroker is
// TestClientCloseEndsAPublicationParkedInAFullSocket and
// TestClientCloseEndsAReplyParkedInAFullSocket under a real memory alarm,
// with publications of 8 MiB to fill the socket's buffers. The reply goes to
// the address RabbitMQ wrote into a real request.
func TestClientCloseEndsAPublicationParkedInAFullSocketOnABroker(t *testing.T) {
	for _, reply := range []bool{false, true} {
		name := "a publication"
		if reply {
			name = "a reply"
		}
		t.Run(name, func(t *testing.T) { closeBehindAFullSocketOnABroker(t, reply) })
	}
}

func closeBehindAFullSocketOnABroker(t *testing.T, reply bool) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	if v, old := rootModuleBefore(t, "v1.1.2"); old {
		t.Skipf("this build uses root module %s, whose Client.Close does not tell the backend it is closing", v)
	}
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	pub := publishOn(t, cl, "orders")
	publish := func(ctx context.Context, body []byte) error {
		return pub.Publish(ctx, &mq.Message{Body: body})
	}
	if reply {
		requester := openBroker(t, cfg)
		t.Cleanup(func() { _ = requester.Close() })
		requests := consumeOn(t, cl, "rpc", "workers")
		rb := backendOf(t, requester)
		var asked sync.WaitGroup
		asked.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = rb.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
		})
		d := receiveOne(t, requests)
		address := d.Properties.ReplyTo
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
		asked.Wait()
		t.Cleanup(func() {
			if admin, err := requester.Admin(); err == nil {
				_ = admin.DeleteSubscription(context.Background(), "rpc", "workers")
				_ = admin.DeleteTopic(context.Background(), "rpc")
			}
		})
		publish = func(ctx context.Context, body []byte) error {
			return cl.Publish(ctx, address, &mq.Message{Body: body})
		}
	}
	// Once before the alarm, so that the publisher has its channel.
	if err := publish(t.Context(), []byte("before")); err != nil {
		t.Fatalf("publishing before the alarm: %v", err)
	}

	var pubs sync.WaitGroup
	t.Cleanup(pubs.Wait)
	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)
	big := make([]byte, 8<<20)
	outcomes := make(chan error, 4)
	for range 4 {
		pubs.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			outcomes <- publish(ctx, big)
		})
	}
	time.Sleep(3 * time.Second)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Client.Close returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		clearAlarm()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 5s into the alarm behind publications that filled the socket, "+
			"and returned %v only once it cleared, %v after it began", err, time.Since(start))
	}
	pubs.Wait()
	close(outcomes)
	for err := range outcomes {
		t.Logf("a parked publication: %v (ambiguous %v, retryable %v)", err, mq.IsAmbiguous(err), mq.IsRetryable(err))
		switch {
		case err == nil:
			t.Error("a publication parked in the socket reported success after Close")
		case mq.CodeOf(err) == mq.CodeDeadlineExceeded && !mq.IsAmbiguous(err):
			t.Errorf("the write Close failed with the socket's deadline reported %v, a definite failure: part of "+
				"it may have reached the broker", err)
		case mq.IsRetryable(err):
			t.Errorf("a publication the closing client refused reported %v, retryable: nothing will take a "+
				"retry once the client has closed", err)
		}
	}
	clearAlarm()
	deleteOrders(t, cfg)
}

// TestAnOlderTopicTypedDelayedExchangeLosesNothingOnABroker is
// TestAnOlderTopicTypedDelayedExchangeIsLeftAlone on the delayed-message image
// (`make image-rabbitmq-delayed`), whose plugin routes nothing through a
// topic-typed delayed exchange. IQMQ_RABBITMQ_DELAYED_ADDR is its host:port.
//
// An earlier client left `orders.delayed` declared with the topic's type and
// bound to the subscription's queue. Parking there, as the driver did when it
// declared that name with `direct` and the broker kept `topic`, the nack
// acknowledged the original and the copy never came back.
func TestAnOlderTopicTypedDelayedExchangeLosesNothingOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_DELAYED_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_DELAYED_ADDR (host:port) to a broker with the delayed-message plugin " +
			"(make image-rabbitmq-delayed)")
	}
	cfg := base()
	cfg.Addrs = []string{addr}
	cfg.Username, cfg.Password = "guest", dsn.Secret("guest")
	cfg.Declare, cfg.DetectDelayPlugin = true, true
	cfg.Prefix = "iqmq-test-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, older := cfg.Prefix+"orders.workers", cfg.Prefix+"orders.delayed"
	t.Cleanup(func() {
		for _, x := range []string{older, older + ".direct", cfg.Prefix + "iqmq.delay-probe.delayed.direct"} {
			_ = raw.ExchangeDelete(x, false, false)
		}
	})
	if _, err := raw.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatal(err)
	}
	if err := raw.ExchangeDeclare(older, "x-delayed-message", true, false, false, false,
		amqp.Table{"x-delayed-type": "topic"}); err != nil {
		t.Fatalf("declaring the older client's delayed exchange: %v", err)
	}
	if err := raw.QueueBind(queue, queue, older, false, nil); err != nil {
		t.Fatal(err)
	}

	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	if facts := rq.FactsForTest(backendOf(t, cl)); !contains(facts, "delaykind=plugin") {
		t.Fatalf("the client chose %s, want the plugin arm, or this test proves nothing", facts)
	}
	cons := consumeOn(t, cl, "orders", "workers")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("later")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Nack(mq.NackDelay(time.Second)) != nil {
		t.Fatal("the delayed nack failed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if n != 1 || string(dst[0].Body) != "later" {
		t.Fatalf("the delayed nack acknowledged the original and its parked copy never came back (%v): it was "+
			"parked in the older topic-typed exchange, which routes nothing", err)
	}
	_ = dst[0].Ack()
	deleteOrders(t, cfg)
}

// TestAMissingDelayedExchangeFailsTheConsumerOpenOnABroker is
// TestAMissingDelayedExchangeFailsTheConsumerOpen on the delayed-message
// image: Config.Declare off, the plugin arm, and the subscription provisioned
// without `orders.delayed.direct`.
func TestAMissingDelayedExchangeFailsTheConsumerOpenOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_DELAYED_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_DELAYED_ADDR (host:port) to a broker with the delayed-message plugin " +
			"(make image-rabbitmq-delayed)")
	}
	prefix := "iqmq-test-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	exchange, queue := prefix+"orders", prefix+"orders.workers"
	t.Cleanup(func() {
		_, _ = raw.QueueDelete(queue, false, false, false)
		_ = raw.ExchangeDelete(exchange, false, false)
	})
	if err := raw.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueueBind(queue, "orders", exchange, false, nil); err != nil {
		t.Fatal(err)
	}

	cfg := base()
	cfg.Addrs = []string{addr}
	cfg.Username, cfg.Password = "guest", dsn.Secret("guest")
	cfg.Prefix = prefix
	cfg.DelayKind = delay.Plugin
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	_, err = cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	t.Logf("opening the consumer: %v", err)
	if mq.CodeOf(err) != mq.CodeNotFound || !contains(err.Error(), prefix+"orders.delayed.direct") {
		t.Fatalf("opening a consumer with %sorders.delayed.direct missing: %v, want NotFound naming it", prefix, err)
	}
}

// TestALossAfterBeginCloseLeavesCloseCleanOnABroker is
// TestALossAfterBeginCloseLeavesCloseClean on RabbitMQ: BeginClose with no
// alarm, the broker closing every connection, then Client.Close, beside the
// control without BeginClose. Both return nil.
func TestALossAfterBeginCloseLeavesCloseCleanOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to close its connections with rabbitmqctl")
	}
	for _, begin := range []bool{false, true} {
		cl := openBroker(t, cfg)
		b := backendOf(t, cl)
		cons := consumeOn(t, cl, "orders", "workers")
		if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		receiveOne(t, cons)
		if begin {
			b.BeginClose()
		}
		rabbitmqctl(t, container, "close_all_connections", "a test closing every connection")
		time.Sleep(time.Second)
		err := cl.Close()
		t.Logf("BeginClose first: %v; Client.Close after the broker closed the connection: %v", begin, err)
		if err != nil {
			t.Errorf("BeginClose first %v: Client.Close returned %v, want nil", begin, err)
		}
	}
	deleteOrders(t, cfg)
}

// TestAnAlarmHeardAfterBeginCloseEndsAPublicationParkedInAFullSocketOnABroker
// is TestAnAlarmHeardAfterBeginCloseEndsAPublicationParkedInAFullSocket under
// a real memory alarm raised after BeginClose, with publications of 8 MiB to
// fill the socket's buffers while the test holds the alarm's report to the
// hook, before the driver acts on it.
func TestAnAlarmHeardAfterBeginCloseEndsAPublicationParkedInAFullSocketOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	hook, arm, release := holdTheAlarmReport()
	defer release()
	cfg.Hooks.OnReconnect = hook
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the alarm: %v", err)
	}
	b.BeginClose()

	var pubs sync.WaitGroup
	t.Cleanup(pubs.Wait)
	arm()
	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)
	big := make([]byte, 8<<20)
	outcomes := make(chan error, 4)
	for range 4 {
		pubs.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			outcomes <- pub.Publish(ctx, &mq.Message{Body: big})
		})
	}
	time.Sleep(3 * time.Second)

	closed := make(chan error, 1)
	go func() { closed <- pub.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("the publisher's Close returned %v behind publications that filled the socket, before the driver "+
			"acted on the alarm, so this test proves nothing", err)
	case <-time.After(500 * time.Millisecond):
	}
	start := time.Now()
	release()
	select {
	case err := <-closed:
		t.Logf("the publisher's Close returned %v %v after the driver heard the alarm", err, time.Since(start))
	case <-time.After(5 * time.Second):
		clearAlarm()
		err := <-closed
		t.Fatalf("the publisher's Close after BeginClose was still waiting 5s into an alarm heard after it, "+
			"and returned %v only once the alarm cleared, %v after the driver heard it", err, time.Since(start))
	}
	pubs.Wait()
	close(outcomes)
	ambiguous := 0
	for err := range outcomes {
		t.Logf("a publication written under the alarm: %v (ambiguous %v, retryable %v)", err, mq.IsAmbiguous(err),
			mq.IsRetryable(err))
		if err == nil {
			t.Error("a publication written under the alarm reported success, with the connection closed under it")
		}
		if mq.IsAmbiguous(err) {
			ambiguous++
		}
	}
	if ambiguous == 0 {
		t.Error("no publication was parked in the socket when the connection closed, so this test proves nothing")
	}
	clearAlarm()
	deleteOrders(t, cfg)
}

// TestAConsumerTheBrokerCancelledDuringAnAlarmClosesAtOnceOnABroker is
// TestAConsumerTheBrokerCancelledDuringAnAlarmClosesAtOnce on RabbitMQ: a
// memory alarm, then `rabbitmqctl delete_queue` of the consumer's queue, which
// makes the broker send `basic.cancel`, then the consumer's Close.
func TestAConsumerTheBrokerCancelledDuringAnAlarmClosesAtOnceOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to raise an alarm with rabbitmqctl")
	}
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)
	names, err := b.Names("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 2*time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 20*time.Second, b.Blocked)

	rabbitmqctl(t, container, "delete_queue", names.Queue)
	// The driver gives up the cancelled consumer's channel as it hears the
	// cancel, and learns whether the queue has gone only once the broker
	// reads again.
	waitFor(t, 10*time.Second, func() bool { return rq.AttachedForTest(b) == 0 })

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		t.Logf("the consumer's Close returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		clearAlarm()
		err := <-closed
		t.Fatalf("the consumer the broker cancelled was still closing 5s into the alarm, and its Close returned "+
			"%v only once the alarm cleared, %v after it began", err, time.Since(start))
	}
	clearAlarm()
	deleteOrders(t, cfg)
}

// TestAConsumerTimeoutDoesNotEndTheConsumerOnABroker holds a delivery past its
// queue's consumer timeout, a `consumer-timeout` policy of 2 s, and then
// receives again, for a quorum queue with Config.Declare on and off and for a
// classic queue.
//
// RabbitMQ 4.3 then cancels a quorum queue's consumer with `basic.cancel`,
// keeps the queue and the channel, and puts the delivery back; 3.13 to 4.2
// close the channel with 406 PRECONDITION_FAILED instead. The driver read the
// cancel as the queue deleted, so on 4.3 one slow handler ended the consumer
// for good with CodeNotFound. Either way the consumer must go on and receive
// the message again, and the held delivery's settlement must report
// ErrLeaseLost. On 4.3 no consumer timeout reaches a classic queue's
// consumer, so that case is skipped there.
func TestAConsumerTimeoutDoesNotEndTheConsumerOnABroker(t *testing.T) {
	base := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to set a policy with rabbitmqctl")
	}
	for i, tc := range []struct {
		name    string
		queue   rq.QueueType
		declare bool
	}{
		{"quorum", rq.QueueQuorum, true},
		{"quorum, Declare off", rq.QueueQuorum, false},
		{"classic", rq.QueueClassic, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := base
			cfg.Prefix += strconv.Itoa(i) + "."
			cfg.QueueType = tc.queue
			cl, b := openSubscription(t, cfg, tc.declare)
			if tc.queue == rq.QueueClassic && brokerAtLeast(t, cl, 4, 3) {
				t.Skip("RabbitMQ 4.3 times out no classic queue's consumer: it refuses x-consumer-timeout there, and the policy does not reach one")
			}
			names, err := b.Names("orders", "workers")
			if err != nil {
				t.Fatal(err)
			}
			policy := "iqmq-test-consumer-timeout-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(i)
			rabbitmqctl(t, container, "set_policy", policy, "^"+regexp.QuoteMeta(names.Queue)+"$",
				`{"consumer-timeout":2000}`, "--apply-to", "queues")
			t.Cleanup(func() { rabbitmqctl(t, container, "clear_policy", policy) })
			cons := consumeOn(t, cl, "orders", "workers")
			pub := publishOn(t, cl, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("slow")}); err != nil {
				t.Fatal(err)
			}
			held := receiveOne(t, cons)
			restores := rq.RestoresForTest(b)

			// RabbitMQ 3.13 to 4.2 check the timeout about once a minute.
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			dst := make([]mq.Delivery, 1)
			n, err := cons.Receive(ctx, dst)
			if err != nil || n != 1 {
				t.Fatalf("Receive after the consumer timeout: %d, %v (code %v), want the delivery back: the consumer ended although its queue is still there",
					n, err, mq.CodeOf(err))
			}
			if string(dst[0].Body) != "slow" {
				t.Fatalf("the consumer received %q, want the delivery the broker put back", dst[0].Body)
			}
			if err := held.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
				t.Errorf("acknowledging the delivery that outlived its consumer timeout = %v, want ErrLeaseLost", err)
			}
			if err := dst[0].Ack(); err != nil {
				t.Fatalf("acknowledging the redelivery: %v", err)
			}
			if got := rq.RestoresForTest(b); got != restores {
				t.Errorf("the consumer timeout cost a reconnection (restores %d to %d)", restores, got)
			}
			dropSubscription(t, cfg, names)
		})
	}
}

// TestAConsumerClosedWhileItWaitsToSubscribeAgainReturnsUnderAnAlarmOnABroker
// is TestAConsumerClosedWhileItWaitsToSubscribeAgainReturnsUnderAnAlarm on
// RabbitMQ 4.3: a 2 s consumer-timeout policy cancels the consumer holding a
// delivery, a memory alarm is already raised (the connection has not
// published since, so the broker still reads it), and a publication made
// during the driver's 3 s wait before it subscribes the consumer again makes
// the broker block the connection. The consumer's Close must not wait for the
// alarm.
func TestAConsumerClosedWhileItWaitsToSubscribeAgainReturnsUnderAnAlarmOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to set a policy and raise an alarm with rabbitmqctl")
	}
	cl, b := openSubscription(t, cfg, true)
	if !brokerAtLeast(t, cl, 4, 3) {
		t.Skip("needs RabbitMQ 4.3 or later, which cancels a consumer that times out; earlier versions close its channel")
	}
	names, err := b.Names("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	policy := "iqmq-test-resubscribe-alarm-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rabbitmqctl(t, container, "set_policy", policy, "^"+regexp.QuoteMeta(names.Queue)+"$",
		`{"consumer-timeout":2000}`, "--apply-to", "queues")
	t.Cleanup(func() { rabbitmqctl(t, container, "clear_policy", policy) })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("slow")}); err != nil {
		t.Fatal(err)
	}
	_ = receiveOne(t, cons) // held past the consumer timeout

	rabbitmqctl(t, container, "set_vm_memory_high_watermark", "absolute", "1")
	cleared := false
	clearAlarm := func() {
		if !cleared {
			cleared = true
			rabbitmqctl(t, container, "set_vm_memory_high_watermark", "0.6")
		}
	}
	t.Cleanup(clearAlarm)

	// The broker's cancel: the driver gives the channel up and starts its wait.
	waitFor(t, 30*time.Second, func() bool { return rq.AttachedForTest(b) == 0 })
	cancelled := time.Now()
	time.Sleep(100 * time.Millisecond)
	pctx, pcancel := context.WithTimeout(t.Context(), time.Second)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 2*time.Second, b.Blocked)
	if since := time.Since(cancelled); since > 2500*time.Millisecond {
		t.Skipf("the broker blocked the connection %v after the cancel, too close to the end of the driver's 3 s wait", since)
	}

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		t.Logf("the consumer's Close returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		clearAlarm()
		err := <-closed
		t.Fatalf("a consumer closed during the alarm, in the driver's wait to subscribe it again after a "+
			"consumer-timeout cancel, was still closing 5s into the alarm; its Close returned %v only "+
			"%v after it began, once the alarm cleared", err, time.Since(start))
	}
	clearAlarm()
	dropSubscription(t, cfg, names)
}

// TestDeleteSubscriptionDeletesItsDeadLettersOnABroker is
// TestDeleteSubscriptionDeletesItsDeadLetters on RabbitMQ: after
// DeleteSubscription with Config.DeadLetter on, a passive declaration finds
// neither `<queue>.dlq` nor `<queue>.dlx`, and a second DeleteSubscription,
// with nothing left to delete, succeeds.
func TestDeleteSubscriptionDeletesItsDeadLettersOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	cfg.DeadLetter = true
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	admin, err := cl.Admin()
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	names, err := backendOf(t, cl).Names("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ch := rawChannel(t, cfg)
		_, _ = ch.QueueDelete(names.DeadLetterQueue, false, false, false)
		_ = ch.ExchangeDelete(names.DeadLetterExchange, false, false)
		dropSubscription(t, cfg, names)
	})
	if _, err := rawChannel(t, cfg).QueueDeclarePassive(names.DeadLetterQueue, true, false, false, false, nil); err != nil {
		t.Fatalf("EnsureSubscription declared no dead-letter queue: %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	for _, q := range []string{names.Queue, names.DeadLetterQueue} {
		_, err := rawChannel(t, cfg).QueueDeclarePassive(q, true, false, false, false, nil)
		if code, _ := rq.ReplyCode(err); code != amqp.NotFound {
			t.Errorf("a passive declare of the queue %s after DeleteSubscription: %v, want 404", q, err)
		}
	}
	err = rawChannel(t, cfg).ExchangeDeclarePassive(names.DeadLetterExchange, amqp.ExchangeDirect, true, false, false, false, nil)
	if code, _ := rq.ReplyCode(err); code != amqp.NotFound {
		t.Errorf("a passive declare of the exchange %s after DeleteSubscription: %v, want 404", names.DeadLetterExchange, err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Errorf("DeleteSubscription of a subscription already deleted: %v, want nil", err)
	}
}

// TestAHeaderSetPastTheFrameSizeIsRefusedBeforeAnyIOOnABroker is
// TestAHeaderSetPastTheFrameSizeIsRefusedBeforeAnyIO on RabbitMQ: three header
// values of 50 KB make a content header frame larger than the default frame
// size of 131072 bytes, which RabbitMQ answers with 501 FRAME_ERROR and the
// connection's close. The driver refuses the publication with MessageTooLarge
// before any I/O, the connection stays up, and two such values go through.
func TestAHeaderSetPastTheFrameSizeIsRefusedBeforeAnyIOOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	cl, b := openSubscription(t, cfg, true)
	names, err := b.Names("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropSubscription(t, cfg, names) })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	values := func(n int) *mq.Message {
		m := &mq.Message{Body: []byte("b")}
		for i := range n {
			m.Headers.Add("big-"+strconv.Itoa(i), []byte(strings.Repeat("x", 50_000)))
		}
		return m
	}
	restores := rq.RestoresForTest(b)
	err = pub.Publish(t.Context(), values(3))
	if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) {
		t.Fatalf("publishing three 50 KB headers = %v (code %v), want CodeMessageTooLarge before any I/O", err, mq.CodeOf(err))
	}
	if err := pub.Publish(t.Context(), values(2)); err != nil {
		t.Fatalf("publishing two 50 KB headers after the refusal: %v", err)
	}
	if d := receiveOne(t, cons); d.Headers.Len() != 2 {
		t.Errorf("the publication that fitted arrived with %d headers, want 2", d.Headers.Len())
	}
	if got := rq.RestoresForTest(b); got != restores {
		t.Errorf("the refusal cost a reconnection: restores %d to %d", restores, got)
	}
}

// TestARequeuedPublicationAtTheBoundIsDeliveredOnABroker publishes, through
// the driver, the largest header set the frame check takes into a quorum
// queue on RabbitMQ 4.3, and has it come back twice: after a requeueing nack,
// which adds `x-acquired-count`, and after its consumer closed holding it,
// which adds `x-delivery-count` too. Each redelivery has to stay readable. The
// check used to allow the whole frame size less 8, and a message published at
// that bound came back, after one requeue, in a frame the consumer's client
// library refused with 501, which closed the consumer's connection on every
// redelivery.
func TestARequeuedPublicationAtTheBoundIsDeliveredOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	cl, b := openSubscription(t, cfg, true)
	if !brokerAtLeast(t, cl, 4, 3) {
		t.Skip("needs RabbitMQ 4.3 or later, which adds both count headers to a redelivery")
	}
	names, err := b.Names("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropSubscription(t, cfg, names) })
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	pub := publishOn(t, cl, "orders")
	// Three headers, because the envelope's own bound holds one value to
	// 64 KiB; the third's size is searched.
	sized := func(n int) *mq.Message {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Add("a", make([]byte, 64_000))
		m.Headers.Add("b", make([]byte, 64_000))
		m.Headers.Add("h", make([]byte, n))
		return m
	}
	fits, over := 0, 8_000
	for over-fits > 1 {
		mid := (fits + over) / 2
		switch err := pub.Publish(t.Context(), sized(mid)); {
		case err == nil:
			fits = mid
		case mq.CodeOf(err) == mq.CodeMessageTooLarge:
			over = mid
		default:
			t.Fatalf("publishing a %d-byte header: %v", mid, err)
		}
	}
	admin, err := cl.Admin()
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Purge(t.Context(), "orders", "workers"); err != nil {
		t.Fatal(err)
	}
	restores := rq.RestoresForTest(b)
	if err := pub.Publish(t.Context(), sized(fits)); err != nil {
		t.Fatalf("publishing the largest header set the check takes, a second header of %d bytes: %v", fits, err)
	}
	first := receiveOne(t, cons)
	if err := first.Nack(); err != nil {
		t.Fatalf("nacking the first delivery: %v", err)
	}
	second := receiveOne(t, cons)
	if second.Attempt != 2 {
		t.Errorf("the redelivery after a requeueing nack reports Attempt %d, want 2", second.Attempt)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("closing the consumer holding the redelivery: %v", err)
	}
	again := consumeOn(t, cl, "orders", "workers")
	third := receiveOne(t, again)
	if got, _ := third.Headers.Get("h"); len(got) != fits {
		t.Errorf("the third delivery's header is %d bytes, want %d", len(got), fits)
	}
	if err := third.Ack(); err != nil {
		t.Fatalf("acknowledging the third delivery: %v", err)
	}
	if got := rq.RestoresForTest(b); got != restores {
		t.Errorf("a redelivery cost a reconnection (restores %d to %d): a frame the client could not read", restores, got)
	}
	t.Logf("headers of 64000, 64000 and %d bytes, requeued twice, delivered three times; attempt %d", fits, third.Attempt)
}

// TestOpenEndsAtTheCallersDeadlineAgainstAPausedBrokerOnABroker is
// TestOpenEndsAtTheCallersDeadline against RabbitMQ's container paused with
// `docker pause`: the kernel still completes the TCP handshake and nothing
// answers the AMQP one. Open has to return at the caller's 1 s deadline with
// the context's error, not at Config.DialTimeout's 15 s.
func TestOpenEndsAtTheCallersDeadlineAgainstAPausedBrokerOnABroker(t *testing.T) {
	cfg := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to pause it")
	}
	if out, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("docker pause: %v: %s", err, out)
	}
	unpause := func() { _ = exec.Command("docker", "unpause", container).Run() }
	t.Cleanup(unpause)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	start := time.Now()
	cl, err := rq.Open(ctx, cfg)
	took := time.Since(start)
	unpause()
	if cl != nil {
		_ = cl.Close()
	}
	if took > 3*time.Second || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open against a paused broker with a 1s context = %v (code %v) after %v; want "+
			"context.DeadlineExceeded at the deadline", err, mq.CodeOf(err), took)
	}
	t.Logf("Open returned %v after %v", err, took)
}

// TestADeletedQueueEndsItsConsumerWithoutAReconnectionOnABroker deletes a
// subscription's queue under a live consumer from another connection, for a
// quorum and a classic queue, Config.Declare on and off, with the consumer
// idle and holding a delivery.
//
// RabbitMQ 4.2 and 4.3 cancel a quorum queue's consumers before they have
// finished deleting it, and answer a `basic.consume` in between with a 541
// CONNECTION exception. The driver used to subscribe every cancelled consumer
// again at once, so a deletion closed the client's connection, and with
// Declare on the topology replay declared the deleted queue again and
// subscribed the consumer to it. The consumer must report CodeNotFound without
// a reconnection, and the queue must stay deleted.
func TestADeletedQueueEndsItsConsumerWithoutAReconnectionOnABroker(t *testing.T) {
	base := brokerConfig(t)
	i := 0
	for _, queue := range []rq.QueueType{rq.QueueQuorum, rq.QueueClassic} {
		for _, declare := range []bool{true, false} {
			for _, holding := range []bool{false, true} {
				i++
				name := queue.String() + "/declare " + strconv.FormatBool(declare) + "/holding " + strconv.FormatBool(holding)
				prefix := base.Prefix + strconv.Itoa(i) + "."
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					cfg := base
					cfg.Prefix = prefix
					cfg.QueueType = queue
					cl, b := openSubscription(t, cfg, declare)
					names, err := b.Names("orders", "workers")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { dropSubscription(t, cfg, names) })
					cons := consumeOn(t, cl, "orders", "workers")
					pub := publishOn(t, cl, "orders")
					if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
						t.Fatal(err)
					}
					d := receiveOne(t, cons)
					if !holding {
						if err := d.Ack(); err != nil {
							t.Fatal(err)
						}
					}
					restores := rq.RestoresForTest(b)

					raw := rawChannel(t, cfg)
					start := time.Now()
					if _, err := raw.QueueDelete(names.Queue, false, false, false); err != nil {
						t.Fatalf("deleting the queue: %v", err)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					_, rerr := cons.Receive(ctx, make([]mq.Delivery, 1))
					t.Logf("Receive after the deletion: %v after %v", mq.CodeOf(rerr), time.Since(start))
					if mq.CodeOf(rerr) != mq.CodeNotFound {
						t.Fatalf("Receive after the queue's deletion reported %v, want CodeNotFound (restores %d to %d)",
							rerr, restores, rq.RestoresForTest(b))
					}
					// Past the driver's 3 s wait before a resubscription, so
					// a late consume would have happened.
					time.Sleep(3500 * time.Millisecond)
					if got := rq.RestoresForTest(b); got != restores {
						t.Fatalf("the deletion of one queue cost the client a reconnection (restores %d to %d)", restores, got)
					}
					if _, err := rawChannel(t, cfg).QueueDeclarePassive(names.Queue, true, false, false, false, nil); err == nil {
						t.Fatal("the deleted queue exists again")
					}
					if snap := cl.Health(); snap.State != mq.HealthReady {
						t.Fatalf("the client reports %v (%v), want ready", snap.State, snap.Err)
					}
				})
			}
		}
	}
}

// openSubscription opens a client on the broker cfg names, with the
// subscription orders/workers in place: declared by the client itself with
// declare on, and by a client of its own, closed again, with declare off.
func openSubscription(t *testing.T, cfg rq.Config, declare bool) (*mq.Client, *rq.Backend) {
	t.Helper()
	if !declare {
		setup := openBroker(t, cfg)
		c, err := setup.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
		if err != nil {
			t.Fatalf("declaring the subscription: %v", err)
		}
		_ = c.Close()
		_ = setup.Close()
		cfg.Declare = false
	}
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	return cl, backendOf(t, cl)
}

// rawChannel is a channel on a connection of the test's own, closed with the
// test.
func rawChannel(t *testing.T, cfg rq.Config) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial("amqp://" + cfg.Username + ":" + cfg.Password.Reveal() + "@" + cfg.Addrs[0] + "/")
	if err != nil {
		t.Fatalf("dialling the broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	return ch
}

// dropSubscription deletes the subscription's queue and its topic's exchange.
func dropSubscription(t *testing.T, cfg rq.Config, n rabbit.Names) {
	t.Helper()
	ch := rawChannel(t, cfg)
	_, _ = ch.QueueDelete(n.Queue, false, false, false)
	_ = ch.ExchangeDelete(n.Exchange, false, false)
}

// brokerAtLeast reports whether the broker cl is connected to is
// major.minor or later.
func brokerAtLeast(t *testing.T, cl *mq.Client, major, minor int) bool {
	t.Helper()
	v := cl.Info().BrokerVersion
	maj, rest, _ := strings.Cut(v, ".")
	mnr, _, _ := strings.Cut(rest, ".")
	a, aerr := strconv.Atoi(maj)
	b, berr := strconv.Atoi(mnr)
	if aerr != nil || berr != nil {
		t.Fatalf("the broker reported the version %q", v)
	}
	return a > major || a == major && b >= minor
}

// TestALongTopicOpensOnTheTTLArmOnABroker is TestALongTopicOpensOffThePluginArm
// and TestTheTTLArmDeclaresNoDelayedExchange on the delayed-message image
// (`make image-rabbitmq-delayed`, IQMQ_RABBITMQ_DELAYED_ADDR): a client that
// found the plugin and chose the ttldlx arm opens a publisher and a consumer on
// a topic whose exchange name, prefix included, is 247 bytes long. The broker
// refuses a longer name than 255 bytes, which `<exchange>.delayed.direct`
// would be.
func TestALongTopicOpensOnTheTTLArmOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_DELAYED_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_DELAYED_ADDR (host:port) to a broker with the delayed-message plugin " +
			"(make image-rabbitmq-delayed)")
	}
	cfg := base()
	cfg.Addrs = []string{addr}
	cfg.Username, cfg.Password = "guest", dsn.Secret("guest")
	cfg.Declare, cfg.DetectDelayPlugin, cfg.DelayKind = true, true, delay.TTLDLX
	cfg.Prefix = "iqmq-test-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
	topic := strings.Repeat("t", 247-len(cfg.Prefix))
	cl := openBroker(t, cfg)
	t.Cleanup(func() { _ = cl.Close() })
	t.Cleanup(func() {
		if admin, err := cl.Admin(); err == nil {
			_ = admin.DeleteSubscription(context.Background(), topic, "w")
			_ = admin.DeleteTopic(context.Background(), topic)
		}
		conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if raw, err := conn.Channel(); err == nil {
			_ = raw.ExchangeDelete(cfg.Prefix+"iqmq.delay-probe.delayed.direct", false, false)
		}
	})
	if facts := rq.FactsForTest(backendOf(t, cl)); !contains(facts, "delayplugin=true delaykind=ttldlx") {
		t.Fatalf("the client is on %s, want the ttldlx arm with the plugin found, or this test proves nothing", facts)
	}
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("a publisher on a topic whose exchange name is 247 bytes, on the ttldlx arm: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "w"})
	if err != nil {
		t.Fatalf("a consumer on a topic whose exchange name is 247 bytes, on the ttldlx arm: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "x" {
		t.Fatalf("received %q", d.Body)
	} else if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

// deleteOrders deletes the topology a test made under cfg's prefix, on a
// client of its own.
func deleteOrders(t *testing.T, cfg rq.Config) {
	t.Helper()
	check := openBroker(t, cfg)
	t.Cleanup(func() { _ = check.Close() })
	if admin, err := check.Admin(); err == nil {
		_ = admin.DeleteSubscription(t.Context(), "orders", "workers")
		_ = admin.DeleteTopic(t.Context(), "orders")
	}
}

// rabbitmqctl runs rabbitmqctl in the broker's container.
func rabbitmqctl(t *testing.T, container string, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"exec", container, "rabbitmqctl"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("rabbitmqctl %v: %v: %s", args, err, out)
	}
}

// TestADelayedNackThatWouldComeBackTooLargeIsRefusedOnABroker is
// TestADelayedNackThatWouldComeBackTooLargeIsRefused on RabbitMQ: on the TTL
// arm with a quorum and a classic queue (IQMQ_RABBITMQ_ADDR), and on the
// plugin arm (IQMQ_RABBITMQ_DELAYED_ADDR), at the review's sizes, a delivery
// at the publication bound and 120, 400 and 700 octets under it, nacked with a
// 500 ms delay.
//
// Measured before the check, the copy came back in a frame the client library
// refused with 501, so the connection closed and the client reconnected: on
// RabbitMQ 4.3.5 about 21 times on a quorum queue, until its delivery limit
// dropped the message, and 2,459 times in 15 s on a classic queue, for ever;
// on 4.2.9's plugin arm the copy of a delivery at the bound looped the same
// way. Now a delayed nack is either refused with Unsupported, sending and
// settling nothing, and the delivery is still the caller's to nack without the
// delay, or taken, and its copy comes back. Nothing reconnects. The largest
// delivery a delayed nack takes, found by search, comes back and is delivered
// again after two requeues, which add RabbitMQ 4.3's two count headers.
func TestADelayedNackThatWouldComeBackTooLargeIsRefusedOnABroker(t *testing.T) {
	for _, arm := range []string{"quorum", "classic", "plugin"} {
		t.Run(arm, func(t *testing.T) {
			var cfg rq.Config
			switch arm {
			case "plugin":
				addr := os.Getenv("IQMQ_RABBITMQ_DELAYED_ADDR")
				if addr == "" {
					t.Skip("set IQMQ_RABBITMQ_DELAYED_ADDR (host:port) to a broker with the delayed-message plugin " +
						"(make image-rabbitmq-delayed)")
				}
				cfg = base()
				cfg.Addrs = []string{addr}
				cfg.Username, cfg.Password = "guest", dsn.Secret("guest")
				cfg.Declare, cfg.DetectDelayPlugin = true, true
				cfg.Prefix = "iqmq-test-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "."
			case "classic":
				cfg = brokerConfig(t)
				cfg.QueueType = rq.QueueClassic
			default:
				cfg = brokerConfig(t)
			}
			cl, b := openSubscription(t, cfg, true)
			names, err := b.Names("orders", "workers")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { dropSubscription(t, cfg, names) })
			cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			if err != nil {
				t.Fatal(err)
			}
			pub := publishOn(t, cl, "orders")
			// Three headers, because the envelope's own bound holds one value
			// to 64 KiB; the third's size is searched.
			sized := func(n int) *mq.Message {
				m := &mq.Message{Body: []byte("b")}
				m.Headers.Add("a", make([]byte, 64_000))
				m.Headers.Add("b", make([]byte, 64_000))
				m.Headers.Add("h", make([]byte, n))
				return m
			}
			fits, over := 0, 8_000
			for over-fits > 1 {
				mid := (fits + over) / 2
				switch err := pub.Publish(t.Context(), sized(mid)); {
				case err == nil:
					fits = mid
				case mq.CodeOf(err) == mq.CodeMessageTooLarge:
					over = mid
				default:
					t.Fatalf("publishing a %d-byte header: %v", mid, err)
				}
			}
			// Each park takes a confirm token, two characters long for the
			// first 35, as the search below needs them to stay; a publication
			// takes one only under Config.Mandatory.
			admin, err := cl.Admin()
			if err != nil {
				t.Fatal(err)
			}
			if err := admin.Purge(t.Context(), "orders", "workers"); err != nil {
				t.Fatal(err)
			}
			// nack publishes a delivery with a third header of n bytes and
			// nacks it with a delay, reporting whether the nack took it. A
			// copy that comes back is requeued requeues times before it is
			// acknowledged.
			nack := func(n, requeues int) bool {
				t.Helper()
				restores := rq.RestoresForTest(b)
				defer func() {
					if got := rq.RestoresForTest(b); got != restores {
						t.Errorf("a delayed nack of a delivery %d bytes under the bound: the client reconnected "+
							"%d times", fits-n, got-restores)
					}
				}()
				if err := pub.Publish(t.Context(), sized(n)); err != nil {
					t.Fatalf("publishing a %d-byte header: %v", n, err)
				}
				d := receiveOne(t, cons)
				if err := d.Nack(mq.NackDelay(500 * time.Millisecond)); err != nil {
					if !errors.Is(err, mq.ErrUnsupported) || mq.CodeOf(err) != mq.CodeUnsupported || mq.IsAmbiguous(err) {
						t.Fatalf("a delayed nack %d bytes under the bound = %v (code %v): want it taken or "+
							"refused with Unsupported", fits-n, err, mq.CodeOf(err))
					}
					if err := d.Nack(); err != nil {
						t.Fatalf("the delivery whose delayed nack was refused could not be nacked: %v", err)
					}
					again := receiveOne(t, cons)
					if err := again.Ack(); err != nil {
						t.Fatal(err)
					}
					return false
				}
				back := receiveOne(t, cons)
				for i := range requeues {
					if err := back.Nack(); err != nil {
						t.Fatalf("requeueing the copy that came back, %d: %v", i+1, err)
					}
					back = receiveOne(t, cons)
				}
				if err := back.Ack(); err != nil {
					t.Fatal(err)
				}
				return true
			}
			for _, margin := range []int{0, 120, 400, 700} {
				took := nack(fits-margin, 0)
				t.Logf("%s: a delayed nack %d bytes under the bound taken %v", arm, margin, took)
				switch {
				case margin == 0 && took:
					t.Errorf("a delayed nack of a delivery at the publication bound was taken")
				case margin == 700 && !took:
					t.Errorf("a delayed nack of a delivery 700 bytes under the publication bound was refused")
				case arm == "plugin" && margin > 0 && !took:
					t.Errorf("a delayed nack on the plugin arm %d bytes under the publication bound was refused",
						margin)
				}
			}
			lo, hi := fits-700, fits
			for hi-lo > 1 {
				mid := (lo + hi) / 2
				if nack(mid, 0) {
					lo = mid
				} else {
					hi = mid
				}
			}
			if !nack(lo, 2) {
				t.Fatalf("the largest delivery a delayed nack took, %d bytes under the bound, was refused the "+
					"second time", fits-lo)
			}
			t.Logf("%s (%s): the largest delivery a delayed nack takes is %d bytes under the publication bound",
				arm, rq.FactsForTest(b), fits-lo)
		})
	}
}

// TestAHoldIsCutInsideTheConsumerTimeoutOnABroker is
// TestAHoldOnAQuorumQueueIsCutInsideTheConsumerTimeout on RabbitMQ 4.3, with
// Config.Declare off as in production, so that the delayed nack is refused: a
// `consumer-timeout` policy of 10 s on the subscription's quorum queue,
// Config.ConsumerTimeout to match, and a handler that asks for 60 s, by a
// postponement or by a failure under a 60 s backoff.
//
// Measured before deliveries reported the timeout as their lease: Run held the
// delivery for the 60 s, the broker cancelled the consumer at the timeout, the
// driver subscribed it again about 3 s later and the message came back about
// 13 s after it was first handled. Now Run gives it back within half the lease,
// the redelivery comes about 5 s after the first call or sooner, the consumer
// is never cancelled (its source keeps its channel generation), and a message
// published afterwards is handled.
func TestAHoldIsCutInsideTheConsumerTimeoutOnABroker(t *testing.T) {
	base := brokerConfig(t)
	container := os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if container == "" {
		t.Skip("set IQMQ_RABBITMQ_CONTAINER to the broker's docker container, to set a policy with rabbitmqctl")
	}
	const timeout = 10 * time.Second
	for i, tc := range []struct {
		name  string
		first func() error
	}{
		{"postponement", func() error { return mq.Postpone(nil, 60*time.Second) }},
		{"retry the driver cannot delay", func() error { return errors.New("handler failed") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Prefix += "hold" + strconv.Itoa(i) + "."
			cfg.ConsumerTimeout = timeout
			cl, b := openSubscription(t, cfg, false)
			if !brokerAtLeast(t, cl, 4, 3) {
				t.Skip("needs RabbitMQ 4.3, whose consumer timeout cancels a quorum queue's consumer within a second")
			}
			names, err := b.Names("orders", "workers")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { dropSubscription(t, cfg, names) })
			policy := "iqmq-test-hold-timeout-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			rabbitmqctl(t, container, "set_policy", policy, "^"+regexp.QuoteMeta(names.Queue)+"$",
				`{"consumer-timeout":10000}`, "--apply-to", "queues")
			t.Cleanup(func() { rabbitmqctl(t, container, "clear_policy", policy) })
			cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			if err != nil {
				t.Fatal(err)
			}
			pub := publishOn(t, cl, "orders")
			var mu sync.Mutex
			var calls []time.Time
			called := make(chan struct{}, 8)
			handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				mu.Lock()
				calls = append(calls, time.Now())
				n := len(calls)
				mu.Unlock()
				called <- struct{}{}
				if n == 1 {
					return tc.first()
				}
				return nil
			})
			ctx, cancel := context.WithCancel(t.Context())
			ran := make(chan error, 1)
			go func() {
				ran <- cons.Run(ctx, handler, mq.WithRetryBackoff(mq.ConstantBackoff(60*time.Second)))
			}()
			defer func() {
				cancel()
				if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run: %v", err)
				}
			}()
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
				t.Fatal(err)
			}
			waitCallWithin(t, called, 10*time.Second)
			gens := rq.GenerationsForTest(b)
			restores := rq.RestoresForTest(b)
			waitCallWithin(t, called, 30*time.Second)
			mu.Lock()
			back := calls[1].Sub(calls[0])
			mu.Unlock()
			t.Logf("the held delivery came back %v after it was first handled", back.Round(10*time.Millisecond))
			if back > timeout/2+3*time.Second {
				t.Errorf("the held delivery came back %v after it was first handled, want within half the %v "+
					"consumer timeout: Run held it until the broker took it back", back, timeout)
			}
			if got := rq.GenerationsForTest(b); !slices.Equal(got, gens) {
				t.Errorf("the consumer's channel generation moved from %v to %v: the broker cancelled it", gens, got)
			}
			if got := rq.RestoresForTest(b); got != restores {
				t.Errorf("the client reconnected %d times", got-restores)
			}
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
				t.Fatal(err)
			}
			waitCallWithin(t, called, 10*time.Second)
		})
	}
}

// waitCallWithin waits for one handler call for at most within.
func waitCallWithin(t *testing.T, called <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-called:
	case <-time.After(within):
		t.Fatalf("the handler was not called within %v", within)
	}
}
