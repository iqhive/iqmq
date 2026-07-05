package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	ps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/memory"
)

// wantOutput is what the example prints, byte for byte, on BOTH arms.
//
// It is one string for two different drivers on purpose: the whole claim of
// this example is that ordering by key is a portable capability, so a line
// that differed between the memory broker and Pub/Sub would be a line about a
// driver rather than about the contract.
const wantOutput = "claims: ordering by key true at scope key\n" +
	"order: ord-1001 delivered created,paid,shipped\n" +
	"order: ord-1002 delivered created,paid,shipped\n" +
	"order: ord-1003 delivered created,paid,shipped\n" +
	"concurrency: 4 handlers over 3 key(s), and no key ever had two deliveries in flight at once\n" +
	"shutdown: received 9, acked 9, in flight 0, settle failures 0\n"

// TestRunInProcess is the arm that never skips.
//
// The driver is the memory broker rather than a Pub/Sub fake, and that is a
// decision with a cost: drivers/gcppubsub's in-process fake is
// `internal/psfake` and cannot be imported from outside that module, so this
// arm exercises the PORTABLE ordering contract and not Pub/Sub's own mapping
// of it. What it buys is that every line of the output
// above is checked on every `make ci`, and that the same program is shown
// running unchanged on two drivers -- which is the claim the project makes and
// the one an example is for. TestRunAgainstABroker is what says Pub/Sub keeps
// the same contract.
func TestRunInProcess(t *testing.T) {
	client, err := memory.Open(t.Context(), memory.Config{
		AutoCreate: true,
		// More partitions than keys, so nothing about the topology forces two
		// keys onto one lane. The hash(key) % lanes still can, which is
		// why the example asserts nothing about cross-key parallelism.
		DefaultTopology: memory.TopologyConfig{Partitions: 8},
	})
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	defer func() {
		if err := client.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()
	reportArm(t, armInProcess, statusRan, "memory broker, 8 partitions")
	runExample(t, client)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_PUBSUB_ADDR.
//
// It skips when no emulator is configured and FAILS when one is: it asserts
// the same wantOutput as the in-process arm, so a Pub/Sub that ordered
// differently is a failure here rather than a skip. That is what makes the
// memory-driver arm above a legitimate stand-in.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_PUBSUB_ADDR")
	if !ok {
		t.Skip("IQMQ_PUBSUB_ADDR is unset: start the Pub/Sub emulator and set it to host:port to run this example against one (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_PUBSUB_ADDR="+addr)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	client, err := open(ctx, addr)
	if err != nil {
		t.Fatalf("open(%q): %v", addr, err)
	}
	defer func() {
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()
	runExample(t, client)

	// The finding this example's documentation makes, checked where it can
	// be: Pub/Sub has no partitions and says so, and mq.WithOrdering does not
	// refuse the scope anyway.
	caps := client.Capabilities()
	if caps.Has(mq.FeatureOrderedByPartition) {
		t.Errorf("this client claims ordering by partition; Pub/Sub has no partitions and the capability table says so, so the specification needs rewriting")
	}
}

// runExample holds the example to its output. One body for both arms, so that
// an arm cannot assert less than the other.
func runExample(t *testing.T, client *mq.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out, client); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	if got := out.String(); got != wantOutput {
		t.Errorf("the example printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestOrderingByPartitionCollapsesEveryKeyOntoOneLane is the cost of asking
// for a scope the topology cannot spread, as a deterministic check.
//
// mq.WithOrdering routes; it is not a request the broker answers. On a
// single-partition topic every delivery carries partition 0, so
// mq.OrderingByPartition puts all nine on ONE lane and mq.WithConcurrency(4)
// buys nothing: the cross-key concurrency is exactly 1. The order is still
// kept -- one lane keeps every order there is -- which is why the collapse is
// a throughput cliff and not an error, and why nothing in the API reports it.
func TestOrderingByPartitionCollapsesEveryKeyOntoOneLane(t *testing.T) {
	client, err := memory.Open(t.Context(), memory.Config{
		AutoCreate:      true,
		DefaultTopology: memory.TopologyConfig{Partitions: 1},
	})
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	defer func() { _ = client.Shutdown(t.Context()) }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	l, err := runWith(ctx, &out, client, []mq.RunOption{mq.WithOrdering(mq.OrderingByPartition)})
	if err != nil {
		t.Fatalf("runWith(OrderingByPartition): %v\noutput so far:\n%s", err, out.String())
	}
	if got := l.LiveMax(); got != 1 {
		t.Fatalf("%d deliveries were handled at one moment on a single-partition topic under OrderingByPartition, want 1: %d lanes were configured and the topology can spread across only one",
			got, handlers)
	}
	// And the order survived, which is the half that makes this a cliff
	// rather than a bug.
	if got := out.String(); got != wantOutput {
		t.Errorf("the collapsed run printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestReportRefusesAReorderedKey, ...RefusesALostKey and
// ...RefusesOverlappingDeliveries break each of report's three checks against
// fabricated observations, so that each is a check somebody has watched fail.
func TestReportRefusesAReorderedKey(t *testing.T) {
	seen := passingObservations()
	o := seen[keys[0]]
	o.events = []string{"paid", "created", "shipped"}
	seen[keys[0]] = o
	if err := report(&bytes.Buffer{}, seen); err == nil {
		t.Fatal("report accepted a key whose events arrived out of order")
	} else if !strings.Contains(err.Error(), "publication order") {
		t.Errorf("the refusal is %q; it should name the order", err)
	}
}

func TestReportRefusesALostKey(t *testing.T) {
	seen := passingObservations()
	delete(seen, keys[1])
	if err := report(&bytes.Buffer{}, seen); err == nil {
		t.Fatal("report accepted a run in which one key was never delivered")
	}
}

func TestReportRefusesALostEvent(t *testing.T) {
	seen := passingObservations()
	o := seen[keys[2]]
	o.events = o.events[:len(o.events)-1]
	seen[keys[2]] = o
	if err := report(&bytes.Buffer{}, seen); err == nil {
		t.Fatal("report accepted a key that lost its last event")
	}
}

func TestReportRefusesOverlappingDeliveriesOfOneKey(t *testing.T) {
	seen := passingObservations()
	o := seen[keys[0]]
	o.maxInFlight = 2
	seen[keys[0]] = o
	err := report(&bytes.Buffer{}, seen)
	if err == nil {
		t.Fatal("report accepted two deliveries of one key in flight at once; that is the guarantee this example is about")
	}
	if !strings.Contains(err.Error(), "one lane is one handler") {
		t.Errorf("the refusal is %q; it should name the lane rule", err)
	}
}

func TestReportAcceptsWhatTheExampleObserves(t *testing.T) {
	var out bytes.Buffer
	if err := report(&out, passingObservations()); err != nil {
		t.Fatalf("report refused a correct run: %v", err)
	}
	// And it prints exactly the lines the expected output holds, so that the
	// four refusals above are refusals of the thing that is really printed.
	want := strings.Index(wantOutput, "order: ")
	if got := out.String(); got != wantOutput[want:strings.Index(wantOutput, "shutdown: ")] {
		t.Errorf("report printed\n%s\nwant\n%s", got, wantOutput[want:strings.Index(wantOutput, "shutdown: ")])
	}
}

// passingObservations is what a correct run observes.
func passingObservations() map[string]observation {
	seen := make(map[string]observation, len(keys))
	for _, k := range keys {
		seen[k] = observation{events: append([]string(nil), lifecycle...), maxInFlight: 1}
	}
	return seen
}

// TestTheKeysAndEventsAreDistinct is the check on the fixture.
//
// Two equal keys would make the ordering claim vacuous, and two equal events
// would make a reordering invisible to slices.Equal.
func TestTheKeysAndEventsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, k := range keys {
		if seen[k] {
			t.Errorf("key %q appears twice; the per-key order claim would be about one key", k)
		}
		seen[k] = true
	}
	ev := map[string]bool{}
	for _, e := range lifecycle {
		if ev[e] {
			t.Errorf("event %q appears twice; a reordering of two equal events cannot be detected", e)
		}
		ev[e] = true
	}
	if len(keys) < 2 {
		t.Error("one key cannot show that two keys are independent")
	}
	if handlers <= len(keys) {
		t.Errorf("%d handlers for %d keys: the example's point is that more lanes than keys still keep each key's order", handlers, len(keys))
	}
}

// TestOrderingIsWhatTheConfigurationAsksFor says the broker arm's
// configuration is the one the claims depend on.
//
// Config.Ordering is the whole difference: without it Pub/Sub orders nothing
// and a publisher REFUSES every publication that carries a key, so the first
// line of the expected output would be false and the second would never be
// reached.
func TestOrderingIsWhatTheConfigurationAsksFor(t *testing.T) {
	if !Config().Ordering {
		t.Fatal("the shared configuration does not set Config.Ordering, so the broker arm is not about ordering keys at all")
	}
	// Validate refuses InsecureTransport with no Endpoint -- which would
	// dial the managed service in the clear -- so the endpoint the arms
	// supply is part of what makes the configuration legal, not decoration.
	if err := Config().Validate(); err == nil {
		t.Error("the shared configuration validated with no Endpoint; InsecureTransport against the managed service would be a credential in the clear")
	}
	if err := Config(func(c *ps.Config) { c.Endpoint = "127.0.0.1:8085" }).Validate(); err != nil {
		t.Fatalf("the shared configuration is invalid with an endpoint: %v", err)
	}
	// And the emulator settings are the emulator's: a real project would not
	// carry InsecureTransport.
	if !Config().InsecureTransport {
		t.Error("the shared configuration expects TLS; the emulator speaks plain HTTP/2")
	}
	cfg := Config(func(c *ps.Config) { c.Ordering = false })
	if cfg.Ordering {
		t.Error("the mutator did not apply")
	}
	if os.Getenv("IQMQ_PUBSUB_ADDR") == "" {
		t.Log("IQMQ_PUBSUB_ADDR is unset, so this checks the configuration and not the emulator's answer to it")
	}
}
