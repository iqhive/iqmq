package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// wantOutput is what the example prints, byte for byte.
//
// Every line is a fact about the FIFO profile rather than about this program:
// which queue the topic became, which two claims the profile adds, that a
// group holds its later messages while one is in flight, that a blocked group
// looks like an empty poll, that the duplicate publication was discarded, and
// the three states of a visibility window.
const wantOutput = "driver sqs profile \"fifo\": topic orders is queue orders.fifo\n" +
	"claims: ordering by key true, deduplication true\n" +
	"in flight: acct-1/created held unsettled, so the next receive produced acct-2/created instead of acct-1's second message\n" +
	"blocked: with acct-1 in flight and acct-2 empty, a receive produced nothing though the queue still holds 2 message(s)\n" +
	"order: acct-1 delivered created,paid,shipped, in publication order\n" +
	"dedup: 5 publications produced 4 messages; the second copy of dedup id acct-1/created was discarded by the service\n" +
	"lease: granted for about 2s, this consumer's own AckDeadline\n" +
	"lease: extended by 6s, and Info().LeaseExpires still reports the original deadline — a DeliveryInfo is the snapshot the receive handed over \n" +
	"lease: a settlement after the window reports ErrLeaseLost, refused by the driver before the delete leaves the process\n" +
	"redelivery: acct-9/lease-demo came back marked as a redelivery, on a later attempt, and was settled\n"

// TestRunInProcess is the arm that never skips.
//
// sqsfake is this driver's own in-process server: queues, per-message
// visibility windows on an injected clock, a fresh receipt handle per receive
// with every earlier one remembered, ApproximateReceiveCount, and the FIFO
// rules this example is about — one in flight per group, ordered within a
// group, MessageDeduplicationId honoured. It is NOT an emulator and its
// numbers mean nothing; what it is, is enough of the service for every claim
// above to be checked with no Docker at all, which is why the container arm
// below is allowed to skip.
//
// The clock is the server's, shared with the driver. Sharing it is what makes
// the lease phase deterministic: the server decides when a window has passed
// and the driver's local lease check decides the same thing, so a test that
// advanced one and not the other would be asserting on a disagreement it
// created itself.
func TestRunInProcess(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{
		Region: "elasticmq",
		// The pessimistic reading of the service, and the one that makes the
		// driver's local lease check the thing under test: a delete carrying
		// a superseded handle succeeds at the service, so the refusal has to
		// happen before the call.
		StaleHandleDeletes: true,
	})
	backend, err := sq.BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed,
		Config(func(cfg *sq.Config) { cfg.Clock = serverClock{s: server} }))
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	client := mq.NewClient(backend, mq.WithClock(serverClock{s: server}))
	defer func() {
		if err := client.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	reportArm(t, armInProcess, statusRan, "sqsfake in-process server on its own clock")
	// Advance instead of sleeping: the window passes in microseconds rather
	// than in real seconds, and moving the clock is all of it.
	//
	// This used to call Server.ExpireVisibility first, and the workaround was
	// commented here at length: Advance made an invisible message visible
	// again without releasing its FIFO GROUP, so a FIFO message whose window
	// lapsed by the clock alone was visible and undeliverable for ever — the
	// queue reported depth 1, invisible 0 and every receive nothing. The
	// server now derives a group's hold from what is in flight rather than
	// counting it, so the clock releases the group exactly as a real queue
	// does and the two arms of this test move time the same way (closing).
	advance := func(_ context.Context, d time.Duration) error {
		server.Advance(d)
		return nil
	}
	runExample(t, client, advance)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_ELASTICMQ_ADDR.
//
// It skips when no broker is configured and FAILS when one is: everything
// below the gate is the same assertion the in-process arm makes, so an
// emulator that behaves differently from sqsfake is a failure here and not a
// skip. That is the whole reason sqsfake is allowed to stand in for the
// service in `make ci`: this arm is what says it did not lie.
//
// The window really passes here — sleepPast, eight seconds of it — because
// ElasticMQ's clock is the wall clock and there is no other way to be on the
// far side of a visibility timeout.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_ELASTICMQ_ADDR")
	if !ok {
		t.Skip("IQMQ_ELASTICMQ_ADDR is unset: start an ElasticMQ container and set it to host:port to run this example against one (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_ELASTICMQ_ADDR="+addr)

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
	runExample(t, client, sleepPast)
}

// runExample holds the example to its output. It is one body for both arms on
// purpose: an arm that asserted less than the other would make "it passed
// against the broker" mean less than it looks.
func runExample(t *testing.T, client *mq.Client, pastTheWindow func(context.Context, time.Duration) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out, client, pastTheWindow); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	if got := out.String(); got != wantOutput {
		t.Errorf("the example printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestTheProfileIsWhatMakesTheClaims is the check that says this example is
// about the FIFO profile and not about SQS in general.
//
// Four of the things above are FIFO's alone. On the standard profile the same
// driver, the same code and the same queue name logic produce a queue with no
// ordering, no deduplication and no groups — and every assertion in
// blockedGroup would then be measuring something else. The specification makes a capability
// table constant per CLIENT, so this is the one place the difference is
// visible without opening two clients.
func TestTheProfileIsWhatMakesTheClaims(t *testing.T) {
	standard := Config(func(cfg *sq.Config) { cfg.FIFO = false })
	if err := standard.Validate(); err != nil {
		t.Fatalf("the standard profile's configuration is invalid: %v", err)
	}
	plain, err := sq.QueueName(topicBase, "", false)
	if err != nil {
		t.Fatalf("QueueName on the standard profile: %v", err)
	}
	fifo, err := sq.QueueName(topicBase, "", true)
	if err != nil {
		t.Fatalf("QueueName on the FIFO profile: %v", err)
	}
	if plain == fifo {
		t.Errorf("the two profiles name the same queue (%q); a FIFO queue's name must end in .fifo", plain)
	}
	if !strings.HasSuffix(fifo, ".fifo") {
		t.Errorf("the FIFO queue is named %q, which does not end in .fifo", fifo)
	}
	if !Config().FIFO {
		t.Fatal("the shared configuration is not on the FIFO profile, so every claim this example prints is about a different queue")
	}
}

// TestDedupKeysAreDistinctPerMessage says the deduplication half of the
// example is about what it claims.
//
// The arithmetic in blockedGroup — five publications, four messages — would
// read the same way if two DIFFERENT messages happened to share a dedup
// identifier, and then the discarded one would be a message the example meant
// to keep. This is the assertion a well-meaning edit to the identifiers would
// break.
func TestDedupKeysAreDistinctPerMessage(t *testing.T) {
	seen := map[string]int{}
	for _, event := range lifecycle {
		seen[groupA+"/"+event]++
	}
	seen[groupB+"/"+lifecycle[0]]++
	seen[groupLease+"/lease-demo"]++
	// The duplicate the example publishes on purpose is the only repeat.
	seen[groupA+"/"+lifecycle[0]]++
	for id, n := range seen {
		want := 1
		if id == groupA+"/"+lifecycle[0] {
			want = 2
		}
		if n != want {
			t.Errorf("dedup id %q is used %d times, want %d", id, n, want)
		}
	}
	if groupLease == groupA || groupLease == groupB {
		t.Errorf("the lease demonstration shares group %q with the ordering half, so one could block the other", groupLease)
	}
}

// TestTheLeaseGuardIsShorterThanTheWindow holds the lease phase's arithmetic
// to something a reader can check.
//
// A guard at or beyond the visibility timeout would refuse every settlement,
// including the ones the ordering half of the example makes, and the
// ErrLeaseLost line would then be proving that the guard is misconfigured
// rather than that the window passed.
func TestTheLeaseGuardIsShorterThanTheWindow(t *testing.T) {
	cfg := Config()
	if cfg.LeaseGuard >= cfg.AckDeadline {
		t.Fatalf("LeaseGuard is %v and AckDeadline is %v: every settlement would be refused", cfg.LeaseGuard, cfg.AckDeadline)
	}
	if extension <= leaseWindow {
		t.Errorf("the extension (%v) is no longer than the window (%v), so the extended lease could not be told from the original", extension, leaseWindow)
	}
	// The reason there are two windows at all: see groupWindow.
	if groupWindow <= 3*nothingPoll {
		t.Errorf("groupWindow is %v and blockedGroup can spend %v polling: the held message's window would lapse mid-proof and the demonstration would become a redelivery test", groupWindow, 3*nothingPoll)
	}
}

// serverClock is the fake server's clock as an mq.Clock.
//
// NewTimer hands back a real timer: only the driver's time-based settlement
// policies ask for one, this example uses the default immediate policy, and a
// fake timer here would need the server's clock to drive a ticker nothing in
// this program arms.
type serverClock struct{ s *sqsfake.Server }

func (c serverClock) Now() time.Time { return c.s.Now() }

func (c serverClock) NewTimer(d time.Duration) mq.Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// TestExtendDoesNotShortenThisDriversLease is the evidence for the example's
// sharpest driver finding, and it is a test rather than a paragraph because a
// paragraph cannot notice being fixed.
//
// The specification and mq.Delivery.Extend both say an extension moves the deadline "at least
// by further into the future". This driver used to implement it as one
// ChangeMessageVisibility with VisibilityTimeout=by, and the service measures
// that FROM NOW — so Extend(by) set the window to `by` from now rather than
// `by` beyond the deadline it had, and a `by` shorter than what was left
// SHORTENED the lease. The driver then moved its own local view to match, so
// the settlement was refused locally as well: the caller asked for more time
// and got less, and the message became visible to another consumer early.
//
// The driver now sends the remaining window plus `by`, so a one-second
// extension of a ten-second window leaves eleven seconds and not one
// (closing). This test is the outside-the-driver
// statement of it: an example is a program written through the portable API,
// and this is the vantage point from which the defect was found.
//
// drivers/nsq guards the analogous case for `by <= 0`.
func TestExtendDoesNotShortenThisDriversLease(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{Region: "elasticmq", StaleHandleDeletes: true})
	clock := serverClock{s: server}
	// A ten-second window, so that a one-second "extension" is unambiguously
	// a shortening rather than a rounding.
	const window = 10 * time.Second
	const shorter = time.Second
	backend, err := sq.BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed,
		Config(func(cfg *sq.Config) {
			cfg.Clock = clock
			cfg.AckDeadline = window
		}))
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	client := mq.NewClient(backend, mq.WithClock(clock))
	defer func() { _ = client.Shutdown(t.Context()) }()

	probeTopic := newTopic()
	consumer, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: probeTopic, Subscription: "extend-probe"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	m := &mq.Message{Key: []byte(groupLease), Body: []byte("shorten"), ID: groupLease + "/shorten"}
	if err := client.Publish(t.Context(), probeTopic, m, mq.DedupKey(groupLease+"/shorten")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d, err := receive(t.Context(), consumer)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := d.Extend(shorter); err != nil {
		t.Fatalf("Extend(%v): %v", shorter, err)
	}
	// Past the extension asked for, and well short of the window the delivery
	// started with. An extension that replaced the window would have expired
	// here; one that moved the deadline has not.
	server.Advance(shorter + 500*time.Millisecond)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack %v after Extend(%v) on a %v window: %v; the specification makes an extension move the deadline at least %v BEYOND the one it had, and a driver that sends `by` as the visibility timeout replaces the remaining window with it instead ",
			shorter+500*time.Millisecond, shorter, window, err, shorter)
	}
	t.Logf("as expected: Extend(%v) on a %v window left the lease expiring %v beyond the deadline it had, not %v from the extension",
		shorter, window, shorter, shorter)
}
