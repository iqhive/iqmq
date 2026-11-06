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

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
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
	if v, old := rootModuleBefore(t, "v1.2.0"); old {
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
	rctx, rcancel := context.WithTimeout(t.Context(), 10*time.Second)
	_, rerr := cons.Receive(rctx, make([]mq.Delivery, 1))
	rcancel()
	if mq.CodeOf(rerr) != mq.CodeNotFound {
		t.Fatalf("Receive after the queue's deletion: %v, want NotFound: the cancel has not reached the consumer", rerr)
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
		t.Fatalf("the consumer the broker cancelled was still closing 5s into the alarm, and its Close returned "+
			"%v only once the alarm cleared, %v after it began", err, time.Since(start))
	}
	clearAlarm()
	deleteOrders(t, cfg)
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
