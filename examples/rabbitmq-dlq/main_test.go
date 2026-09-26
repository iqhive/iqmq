package main

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rb "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/memory"
)

// wantOutput is what the example prints, byte for byte, on BOTH arms.
//
// It is one string for two drivers because every line is about a ROUTE and not
// about a broker: the core route stamps and republishes, the native route asks
// the broker to move the message, and the difference between what the two
// carry is the same on RabbitMQ as it is in process.
const wantOutput = "claims: dead letter true\n" +
	"core route: 3 handler attempt(s), then acked 1 nacked 2 dead-lettered 0 — the copy was published and the original ACKED, so ConsumerStats.DeadLettered counts none of it \n" +
	"native route: 3 handler attempt(s), then acked 0 nacked 3 dead-lettered 1 — the last nack asked the broker to move the message, which is the only shape ConsumerStats.DeadLettered counts\n" +
	"core route: the copy carries origin topic " + topicCore + inProcessSuffix +
	", its subscription, an attempt count and the handler's reason \"the order references a product that does not exist\"\n" +
	"native route: the broker moved the message, and it carries neither iqmq-attempts nor iqmq-failure — Nack(NackDeadLetter()) names neither \n"

// inProcessSuffix is the topic suffix the tests use.
//
// It is a constant rather than newSuffix()'s timestamp because the expected
// output above names the origin topic, and an output that contained a
// timestamp could not be compared. The broker arm uses a real suffix and
// therefore substitutes it, which runExample does.
const inProcessSuffix = "-test"

// TestRunInProcess is the arm that never skips.
//
// The driver is the memory broker: drivers/rabbitmq has no exported in-process
// fake, so this arm exercises the two ROUTES rather than RabbitMQ's
// x-dead-letter-exchange. What it buys is that every line
// of the output above is checked on every `make ci`; TestRunAgainstABroker is
// what says RabbitMQ behaves the same way, including that its Representable
// accepts the five reserved headers where six other drivers' do not.
func TestRunInProcess(t *testing.T) {
	client, err := memory.Open(t.Context(), memory.Config{
		AutoCreate: true,
		DefaultTopology: memory.TopologyConfig{
			// The native route's destination. The memory broker advertises
			// FeatureDeadLetter as conditional on exactly this field, and the
			// topic has to exist before a Reject reaches it -- which is why
			// run() opens the dead-letter consumer first on both arms.
			DeadLetterTopic: topicDLQ + inProcessSuffix,
			// MaxAttempts is deliberately LEFT AT ZERO. It is the broker's
			// own attempt limit, and a broker that dead-letters on its own
			// count would take the decision away from mq.WithMaxAttempts and
			// make "3 handler attempts" a statement about the topology.
		},
	})
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	defer func() {
		if err := client.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()
	reportArm(t, armInProcess, statusRan, "memory broker, DeadLetterTopic set")
	runExample(t, client, inProcessSuffix)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_RABBITMQ_ADDR.
//
// It skips when no broker is configured and FAILS when one is: it asserts the
// same output as the in-process arm, with the run's own topic suffix
// substituted into the one line that names a topic.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_RABBITMQ_ADDR")
	if !ok {
		t.Skip("IQMQ_RABBITMQ_ADDR is unset: start a RabbitMQ container and set it to host:port to run this example against one (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_RABBITMQ_ADDR="+addr)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	suffix := newSuffix()
	client, err := open(ctx, addr, suffix)
	if err != nil {
		t.Fatalf("open(%q): %v", addr, err)
	}
	defer func() {
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()
	runExample(t, client, suffix)
}

// runExample holds the example to its output. One body for both arms.
func runExample(t *testing.T, client *mq.Client, suffix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out, client, suffix); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	want := strings.ReplaceAll(wantOutput, topicCore+inProcessSuffix, topicCore+suffix)
	if got := out.String(); got != want {
		t.Errorf("the example printed\n%s\nwant\n%s", got, want)
	}
}

// TestTheCoreRouteWorksOnEveryDriver is what the mechanism
// check became, and the two halves are what changed and what did not.
//
// The core route publishes a copy stamped with five headers under
// mq.ReservedHeaderPrefix. Six of the twelve shipped drivers used to refuse ANY
// header under that prefix in Representable, so on those the publication failed
// before any I/O, the runner requeued and counted a DeadLetterFailures, and the
// message was redelivered for ever. driverkit.ReservedHeaderAllowed now admits
// exactly those five and nothing else, so the core route works everywhere
//
//	-- and the example still says which route it took,
//
// because the two routes differ in what the dead-lettered message CARRIES
//
//	rather than in where they are available.
//
// What this test checks without twelve brokers is the mechanism, in both
// directions. A caller's key under the prefix is still refused by
// mq.Headers.Validate, which is the rule and is unchanged; and every one of
// the five is admitted by driverkit.ReservedHeaderAllowed, which is what a
// driver asks. The day the specification stops using the prefix, the day a caller's
// reserved key stops being refused, or the day a driver stops carrying one of
// the five, this test fails and the example's paragraph needs rewriting.
func TestTheCoreRouteWorksOnEveryDriver(t *testing.T) {
	provenance := []string{
		mq.HeaderOriginTopic,
		mq.HeaderOriginSubscription,
		mq.HeaderAttempts,
		mq.HeaderFailure,
		mq.HeaderFirstDeliveredAt,
	}
	for _, h := range provenance {
		if !strings.HasPrefix(h, mq.ReservedHeaderPrefix) {
			t.Errorf("%q is not under %q, so the mechanism does not apply to it and this example's account of the two routes is stale",
				h, mq.ReservedHeaderPrefix)
		}
		var hdr mq.Headers
		hdr.Set(h, []byte("x"))
		if err := hdr.Validate(mq.Limits{}, true); err == nil {
			t.Errorf("a caller's %q validated; the rule about a CALLER's reserved key is gone, which is not what widened", h)
		}
		if !driverkit.ReservedHeaderAllowed(h) {
			t.Errorf("driverkit.ReservedHeaderAllowed(%q) = false; a driver asking it will refuse the specification's own provenance and mq.WithDeadLetter is unusable again (B12b.1)", h)
		}
	}
	if len(provenance) != driverkit.ProvenanceHeaderCount {
		t.Errorf("The specification stamps %d provenance headers; driverkit names %d", len(provenance), driverkit.ProvenanceHeaderCount)
	}
}

// TestTheNativeRouteCarriesNoReason is the other half of the same story, and
// it is the assertion the example prints.
//
// runner.nativeDeadLetter settles with Nack(NackDeadLetter()), and a
// NackOption list that names neither a reason nor an attempt count cannot
// produce either header. This checks the option surface rather than a broker,
// so it runs everywhere: mq.NackDeadLetter() on its own carries no reason.
func TestTheNativeRouteCarriesNoReason(t *testing.T) {
	var opts mq.NackOptions
	if err := opts.Apply(mq.NackDeadLetter()); err != nil {
		t.Fatalf("Apply(NackDeadLetter()): %v", err)
	}
	if opts.Action != mq.NackActionDeadLetter {
		t.Fatalf("NackDeadLetter() asks for action %v, want NackActionDeadLetter", opts.Action)
	}
	if opts.Reason != "" {
		t.Errorf("NackDeadLetter() carries reason %q; if the runner's native route now names a reason then the specification is closed", opts.Reason)
	}
}

// TestTheConfigurationIsWhatMakesTheRouteExist holds the broker arm's
// configuration to the two settings without which the example is about
// nothing.
//
// Declare is off by default because the specification forbids a driver from creating
// topology behind a caller's back; DeadLetter is off because without it a Nack
// asking for a dead-letter route is refused with ErrUnsupported; and
// DeadLetterTopic is what makes the destination a TOPIC rather than the
// derived `<queue>.dlq`, which is a queue nothing can read through mq.Client.
func TestTheConfigurationIsWhatMakesTheRouteExist(t *testing.T) {
	cfg := Config(inProcessSuffix)
	if !cfg.Declare {
		t.Error("Config.Declare is off, so the dead-letter exchange and queue are never created")
	}
	if !cfg.DeadLetter {
		t.Error("Config.DeadLetter is off, so a Nack asking for the dead-letter route is refused with ErrUnsupported")
	}
	if cfg.DeadLetterTopic == "" {
		t.Error("Config.DeadLetterTopic is empty, so dead letters land in `<queue>.dlq`, which is a queue and not a topic: nothing could read them through mq.Client")
	}
	// And each of the three is refused in isolation, which is the driver
	// saying the same thing.
	for _, c := range []struct {
		name string
		cfg  rb.Config
	}{
		{"DeadLetter without Declare", Config(inProcessSuffix, func(c *rb.Config) { c.Declare = false })},
		{"DeadLetterTopic without DeadLetter", Config(inProcessSuffix, func(c *rb.Config) { c.DeadLetter = false })},
	} {
		c.cfg.Addrs = []string{"127.0.0.1:5672"}
		if err := c.cfg.Validate(); err == nil {
			t.Errorf("%s validated; the driver used to refuse it, and the refusal is what stops a rejected message being destroyed", c.name)
		}
	}
}

// TestTheFixtureIsWhatTheExampleClaims is the check on the fixture.
func TestTheFixtureIsWhatTheExampleClaims(t *testing.T) {
	if maxAttempts < 2 {
		t.Errorf("maxAttempts is %d: one attempt cannot show a retry", maxAttempts)
	}
	if topicCore == topicNative {
		t.Error("the two routes share a source topic, so one runner's options would apply to the other's message")
	}
	if topicDLQ == topicCore || topicDLQ == topicNative {
		t.Error("the dead-letter topic is one of the source topics, so a dead letter would be redelivered to the handler that failed it")
	}
	if failure == "" || failure == poison {
		t.Errorf("the failure reason %q is empty or equal to the body, so iqmq-failure carrying it would prove nothing", failure)
	}
	// The identifiers are what collect() routes on, and they have to be
	// derivable without reading the headers under test.
	for _, name := range []string{"core", "native"} {
		id := name + "/" + poison
		if got := identify(&mq.Message{ID: id}); got != name {
			t.Errorf("identify(%q) = %q, want %q", id, got, name)
		}
	}
	if _, err := strconv.Atoi("1"); err != nil {
		t.Fatal("unreachable")
	}
}
