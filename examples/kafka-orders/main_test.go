package main

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// wantOutput is what the example prints, byte for byte.
//
// Nothing in it depends on which partition a key landed on or on the order the
// cluster handed the partitions over: those are the two things that differ
// between runs, and an example whose expected output contained either would
// fail at random. What is left is the invariants — one key in one partition,
// the events in order, the attempt count, the dead letter's provenance, and a
// shutdown with nothing in flight.
const wantOutput = "topology: orders with 3 partition(s), dead letters to orders.dlq\n" +
	"key ord-1001: created,paid,shipped in one partition, in order\n" +
	"key ord-1002: created,paid,shipped in one partition, in order\n" +
	"key ord-1003: created,paid,shipped in one partition, in order\n" +
	"key ord-bad: 3 handler attempts of created, every one failed (the broker's own attempt counter stayed at 1: the retry never left this process)\n" +
	"the handler acked 9 deliveries itself; the runner overruled none\n" +
	"dead-lettered ord-bad/created, with origin topic orders, subscription fulfilment, partition and offset\n" +
	"...and with neither iqmq-attempts nor iqmq-failure: Nack(NackDeadLetter()) carries no attempt count and no reason \n" +
	"commit: offsets committed explicitly, so the at-least-once window is zero here (1 commit(s) in this run)\n" +
	"shutdown: run drained cleanly; received 10, acked 9, dead-lettered 1, settle failures 0, in flight 0\n"

// TestRunInProcess is the arm that never skips.
//
// kfake is franz-go's own in-process Kafka broker: the real protocol, real
// record batches, real consumer groups and real rebalances, which is why this
// driver's whole unit suite needs no container either. It is what lets
// `make ci` on a machine with no Docker still hold this example to its output,
// and it is the reason the container arm below is allowed to skip.
func TestRunInProcess(t *testing.T) {
	addr := kfakeCluster(t)
	reportArm(t, armInProcess, statusRan, "kfake in-process cluster")
	runExample(t, addr)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_KAFKA_ADDR.
//
// It skips when no broker is configured and FAILS when one is: everything
// below the gate is the same assertion the in-process arm makes, so a cluster
// that behaves differently from kfake is a failure here and not a skip.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_KAFKA_ADDR")
	if !ok {
		t.Skip("IQMQ_KAFKA_ADDR is unset: start a Kafka container and set it to the seed broker to run this example against one (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_KAFKA_ADDR="+addr)
	runExample(t, addr)
}

// runExample opens a client against brokers and holds the example to its
// output.
//
// It is one body for both arms on purpose: an arm that asserted less than the
// other would make "it passed against the broker" mean less than it looks.
func runExample(t *testing.T, brokers string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	client, backend, err := open(ctx, strings.Split(brokers, ","))
	if err != nil {
		t.Fatalf("open(%q): %v", brokers, err)
	}
	defer func() {
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	var out bytes.Buffer
	if err := run(ctx, &out, client, backend); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	if got := out.String(); got != wantOutput {
		t.Errorf("the example printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestReportRefusesALostKey is the first half of the ordering check, watched
// failing.
//
// The example's own passing run does not prove that `report` could ever
// object: a check that is never given a violation is a check nobody has seen
// work. This gives it one. A key whose events arrived on two partitions is
// what a producer that dropped Message.Key produces — Kafka then round-robins
// the records, the three events land in three logs, and no consumer can put
// them back in order.
func TestReportRefusesALostKey(t *testing.T) {
	obs := passingObservations()
	o := obs["ord-1001"]
	o.partitions = []int32{0, 2}
	obs["ord-1001"] = o

	var out bytes.Buffer
	err := report(&out, obs)
	if err == nil {
		t.Fatal("report accepted a key whose events arrived on two partitions; dropping Message.Key would then pass")
	}
	if !strings.Contains(err.Error(), "one key in one partition") {
		t.Errorf("report objected with %q, which does not name the guarantee that was broken", err)
	}
}

// TestReportRefusesReorderedEvents is the second half, watched failing.
//
// This is what a consumer that did NOT serialise a partition produces: the
// events are all in one log and one partition, and two workers handled them
// out of order anyway.
func TestReportRefusesReorderedEvents(t *testing.T) {
	obs := passingObservations()
	o := obs["ord-1002"]
	o.events = []string{"created", "shipped", "paid"}
	obs["ord-1002"] = o

	var out bytes.Buffer
	err := report(&out, obs)
	if err == nil {
		t.Fatal("report accepted shipped-before-paid; a consumer that stopped serialising a partition would then pass")
	}
	if !strings.Contains(err.Error(), "reordered") {
		t.Errorf("report objected with %q, which does not name the guarantee that was broken", err)
	}
}

// TestReportRefusesAShortAttemptCount holds the retry half of the example to
// its number.
//
// A dead-letter route that fired on the FIRST failure would still print a
// dead-lettered message and still leave the queue empty, so the attempt count
// is the only thing that says the retries happened at all.
func TestReportRefusesAShortAttemptCount(t *testing.T) {
	obs := passingObservations()
	o := obs[badKey]
	o.handled = 1
	obs[badKey] = o

	var out bytes.Buffer
	if err := report(&out, obs); err == nil {
		t.Fatal("report accepted a single attempt of the unprocessable order; a retry policy that never retried would then pass")
	}
}

// TestReportAcceptsWhatTheExampleObserves is the control: the fabricated
// observations the three tests above mutate must themselves pass, or those
// tests would be proving nothing about the mutation.
func TestReportAcceptsWhatTheExampleObserves(t *testing.T) {
	var out bytes.Buffer
	if err := report(&out, passingObservations()); err != nil {
		t.Fatalf("report rejected a correct observation: %v", err)
	}
	for _, key := range goodKeys {
		if !strings.Contains(out.String(), "key "+key+": created,paid,shipped in one partition, in order") {
			t.Errorf("report printed no ordering line for %s:\n%s", key, out.String())
		}
	}
}

// passingObservations is what a correct run observes: three good keys in one
// partition each with their events in order and acked by the handler, and the
// unprocessable one at its attempt bound.
func passingObservations() map[string]observation {
	obs := map[string]observation{
		badKey: {events: []string{"created"}, partitions: []int32{1}, handled: maxAttempts, brokerAttempt: 1},
	}
	for i, key := range goodKeys {
		obs[key] = observation{
			events:         slices.Clone(lifecycle),
			partitions:     []int32{int32(i)},
			handled:        len(lifecycle),
			brokerAttempt:  1,
			ackedByHandler: len(lifecycle),
		}
	}
	return obs
}

// TestTheBadKeyIsNotAGoodOne says the retry half of the example is about what
// it claims.
//
// If badKey were also in goodKeys the program would publish its lifecycle too
// and the attempt count would be measuring a different message.
func TestTheBadKeyIsNotAGoodOne(t *testing.T) {
	if slices.Contains(goodKeys, badKey) {
		t.Fatalf("goodKeys contains %q, so the unprocessable order would also be published as a working one", badKey)
	}
	if partitions < 2 {
		t.Fatalf("partitions is %d: with one partition every key is trivially in order and the ordering check proves nothing", partitions)
	}
}

// kfakeCluster starts an in-process Kafka cluster and returns its seed broker.
//
// The topics are seeded with the same partition count the program ensures, so
// that the run exercises EnsureTopic's idempotence — TOPIC_ALREADY_EXISTS is a
// success and not a failure — rather than depending on it for the count.
// GroupMinSessionTimeout is lowered because the broker's own minimum is six
// seconds and a group that takes six seconds to form makes this example look
// like a hang.
func kfakeCluster(tb testing.TB) string {
	tb.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.GroupMinSessionTimeout(time.Second),
		kfake.SeedTopics(partitions, topicOrders),
		kfake.SeedTopics(1, topicDLQ),
	)
	if err != nil {
		tb.Fatalf("kfake cluster: %v", err)
	}
	tb.Cleanup(c.Close)
	return strings.Join(c.ListenAddrs(), ",")
}

// TestTheCoreDeadLetterRouteWorksOnThisDriver is what the specification's
// pinned refusal became, and the inversion is the point: the test was written
// so that a paragraph could not go stale, and it is what noticed.
//
// the specification's own dead-letter route — mq.WithDeadLetter(topic) — publishes a COPY
// of the message stamped with five provenance headers, all of them under the
// reserved iqmq- prefix. This driver's Representable used to refuse any header
// under that prefix, the rule about a CALLER's headers applied to the
// core's own, so every dead-letter publication was refused before any I/O, the
// runner counted a DeadLetterFailure and requeued the message, and the message
// was redelivered for ever. driverkit.ReservedHeaderAllowed now admits exactly
// those five keys and nothing else, on all twelve drivers.
//
// The example above still takes mq.WithNativeDeadLetter(), which is a choice
// rather than a workaround now: the native route is one settlement and no
// publish-then-ack window, and the finding about it — no reason and no
// attempt count — is what the example prints. What this test holds is that the
// core route is AVAILABLE here, so the day the refusal comes back the example's
// account of the two routes fails rather than quietly becoming true again.
func TestTheCoreDeadLetterRouteWorksOnThisDriver(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	addr := kfakeCluster(t)
	backend, err := kq.OpenBackend(ctx, kq.Config{
		Brokers:           strings.Split(addr, ","),
		ClientID:          "iqmq-kafka-orders-dlq-probe",
		AutoCreateTopics:  true,
		Partitions:        partitions,
		ReplicationFactor: 1,
		CommitInterval:    50 * time.Millisecond,
		SessionTimeout:    6 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		FetchMaxWait:      50 * time.Millisecond,
		Linger:            -1,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	// The publish hook is how a refusal WOULD be read: the runner requeues
	// and counts rather than returning the error, so DeadLetterFailures says
	// that something went wrong and only the hook says what. It is kept now
	// that the route works, because a refusal that came back would otherwise
	// be invisible in this test — the copy simply would not arrive, and
	// "nothing arrived" is the least informative failure a dead-letter test
	// can give.
	refusals := make(chan error, 8)
	client := mq.NewClient(backend, mq.WithHooks(mq.Hooks{
		OnPublishDone: func(_ context.Context, e mq.PublishDoneEvent) {
			if e.Err == nil || e.Topic != topicDLQ {
				return
			}
			select {
			case refusals <- e.Err:
			default:
			}
		},
	}))
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: topicOrders, Subscription: "dlq-probe", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	failures := make(chan struct{}, 1)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	// `stopped` closes when Run returns, which is the only event in this
	// test that happens AFTER the runner's dead-letter accounting. See the
	// wait below for why that matters.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, _ *mq.Delivery) error {
			select {
			case failures <- struct{}{}:
			default:
			}
			return errUnprocessable
		}),
			mq.WithConcurrency(1),
			mq.WithMaxAttempts(1),
			mq.WithRetryBackoff(mq.ConstantBackoff(time.Millisecond)),
			mq.WithDeadLetter(topicDLQ),
			mq.WithFailurePolicy(mq.FailureDeadLetter))
	}()
	if err := client.Publish(ctx, topicOrders, order("ord-probe", "created", 1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-failures:
	case <-ctx.Done():
		t.Fatal("the handler was never called")
	}

	// The dead-letter publication is attempted once the handler's attempts
	// are exhausted, which is immediately: WithMaxAttempts(1). It is read
	// back through an ordinary consumer, because what an operator does with a
	// dead-letter topic is read it.
	audit, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: topicDLQ, Subscription: "dlq-probe-audit", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("Consumer on %s: %v", topicDLQ, err)
	}
	defer func() { _ = audit.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	dst := make([]mq.Delivery, 1)
	n, rerr := audit.Receive(readCtx, dst)
	if n != 1 {
		var why string
		select {
		case got := <-refusals:
			why = ", and a publication to it was refused with " + got.Error()
		default:
		}
		t.Fatalf("nothing arrived on %s (Receive = %d, %v)%s: the route is refused on this driver again, "+
			"so and B12b.1 need revisiting", topicDLQ, n, rerr, why)
	}
	for _, k := range []string{
		mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts,
		mq.HeaderFailure, mq.HeaderFirstDeliveredAt,
	} {
		if _, ok := dst[0].Headers.Get(k); !ok {
			t.Errorf("the dead-lettered copy carries no %s; an operator reading %s cannot see where it came from", k, topicDLQ)
		}
	}
	if v, _ := dst[0].Headers.Get(mq.HeaderFailure); !strings.Contains(string(v), errUnprocessable.Error()) {
		t.Errorf("%s = %q, want the handler's error %q", mq.HeaderFailure, v, errUnprocessable)
	}
	if v, _ := dst[0].Headers.Get(mq.HeaderOriginTopic); string(v) != topicOrders {
		t.Errorf("%s = %q, want %q", mq.HeaderOriginTopic, v, topicOrders)
	}
	if err := dst[0].Ack(); err != nil {
		t.Errorf("acking the dead-lettered copy: %v", err)
	}

	// The runner's accounting is complete when Run returns, not when the
	// publish hook fires: the hook is raised from INSIDE the driver's sink
	// while the dead-letter publication is still on the stack, and reading a
	// counter off that event lost the race 6 runs in 10 under -race
	// The specification forbids a sleep where an event will do, so
	// the wait is the LATER event.
	stop()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatalf("the runner did not return after its context was cancelled: %v", ctx.Err())
	}
	if n := consumer.Stats().DeadLetterFailures; n != 0 {
		var why string
		select {
		case got := <-refusals:
			why = ": " + got.Error()
		default:
		}
		t.Errorf("DeadLetterFailures = %d, want 0%s", n, why)
	}
	t.Logf("the specification's own dead-letter route works on this driver: the copy arrived on %s with its provenance", topicDLQ)
}
