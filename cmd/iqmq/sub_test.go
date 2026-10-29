package main

import (
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// TestSubSettlesWhatItPrinted is the round trip through one broker: publish
// four, consume two, and the report has to account for all four.
func TestSubSettlesWhatItPrinted(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "one", "two", "three", "four"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	h.err.Reset()

	if got := h.run("sub", sharedDSN, "orders", "billing",
		"--start", "earliest", "--max", "2", "--prefetch", "8"); got != exitOK {
		t.Fatalf("sub = %d\nstdout:\n%s\nstderr:\n%s", got, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "received 2") {
		t.Errorf("sub --max 2 did not receive two:\n%s", out)
	}
	if !strings.Contains(out, "acked 2") {
		t.Errorf("sub did not acknowledge the two it printed:\n%s", out)
	}
	if !strings.Contains(out, "settlement=acked") {
		t.Errorf("no delivery line carries its settlement:\n%s", out)
	}
	if !strings.Contains(out, "stopped: the --max count was reached") {
		t.Errorf("sub did not say why it stopped:\n%s", out)
	}
	for _, body := range []string{"one", "two"} {
		if !strings.Contains(out, body) {
			t.Errorf("the body %q was not printed:\n%s", body, out)
		}
	}
}

// TestSubGivesBackWhatItFetchedAndDidNotUse is the deterministic half of
// shutdown, and the defect this whole design exists to prevent: a delivery
// fetched past --max must be settled with --on-stop and COUNTED, not dropped
// on the floor to wait for its lease to expire with nothing in the output
// saying so.
func TestSubGivesBackWhatItFetchedAndDidNotUse(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders",
		"a", "b", "c", "d", "e", "f", "g", "h"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()

	// A prefetch of eight and a max of one: the batch crosses --max, so seven
	// deliveries have to be handed back.
	if got := h.run("sub", sharedDSN, "orders", "billing",
		"--start", "earliest", "--max", "1", "--prefetch", "8", "--on-stop", "nack"); got != exitOK {
		t.Fatalf("sub = %d\nstdout:\n%s\nstderr:\n%s", got, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "received 1") {
		t.Errorf("sub --max 1 received more than one:\n%s", out)
	}
	if !strings.Contains(out, "acked 1") {
		t.Errorf("the one delivery was not acknowledged:\n%s", out)
	}
	if !strings.Contains(out, "beyond-max:") {
		t.Errorf("sub fetched more than --max and did not account for the rest. That is a "+
			"delivery held until its lease expires with nothing saying so:\n%s", out)
	}
}

// TestSubGivesBackTheSurplusWhateverTheNackActionSays: --nack-action is how a
// HANDLED delivery is nacked. The surplus past --max was never printed, so
// handing it back with --on-stop nack has to requeue it. Settling it with the
// handled deliveries' drop or dead-letter destroyed up to a prefetch of
// messages the user never saw.
func TestSubGivesBackTheSurplusWhateverTheNackActionSays(t *testing.T) {
	h, source, _ := newBridgeHarness(t)
	ensure(t, source, "orders", "billing")
	publish(t, source, "orders",
		&mq.Message{Body: []byte("a")}, &mq.Message{Body: []byte("b")},
		&mq.Message{Body: []byte("c")}, &mq.Message{Body: []byte("d")})

	if got := h.run("sub", srcScheme+"://", "orders", "billing", "--max", "1", "--prefetch", "4",
		"--settle", "nack", "--nack-action", "drop", "--on-stop", "nack"); got != exitOK {
		t.Fatalf("sub = %d\nstdout:\n%s\nstderr:\n%s", got, h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), "beyond-max:nacked 3") {
		t.Fatalf("the run did not hand back the three it fetched past --max:\n%s", h.stdout())
	}
	if d := depth(t, source, "orders", "billing"); d != 3 {
		t.Errorf("the subscription holds %d message(s) after printing one of four, want 3: "+
			"the surplus was nacked with --nack-action drop", d)
	}
}

// TestSubSettlementActions covers each disposition and its own word in the
// report. "abandoned" is not a synonym for "acked".
func TestSubSettlementActions(t *testing.T) {
	for _, c := range []struct{ flag, want string }{
		{"ack", "acked"},
		{"nack", "nacked"},
		{"reject", "rejected"},
		{"none", "abandoned"},
	} {
		t.Run(c.flag, func(t *testing.T) {
			h, _ := newSharedHarness(t)
			if got := h.run("pub", sharedDSN, "orders", "x"); got != exitOK {
				t.Fatalf("pub = %d: %s", got, h.stderr())
			}
			h.out.Reset()
			code := h.run("sub", sharedDSN, "orders", "sub-"+c.flag,
				"--start", "earliest", "--max", "1", "--prefetch", "1", "--settle", c.flag)
			if code != exitOK {
				t.Fatalf("sub --settle %s = %d\nstdout:\n%s\nstderr:\n%s",
					c.flag, code, h.stdout(), h.stderr())
			}
			if !strings.Contains(h.stdout(), c.want+" 1") {
				t.Errorf("sub --settle %s did not count one %s:\n%s", c.flag, c.want, h.stdout())
			}
		})
	}
}

// TestSubSettleNoneStopsAtAFullPrefetchWindow: a delivery --settle none leaves
// unsettled holds a prefetch slot, and the usage promises a run that stops
// once the window is full and says why. The loop fetched again instead, so a
// broker that enforces the window left the run waiting until it was
// interrupted, and the memory broker, which does not, delivered past it.
//
// --idle bounds the failure only: a run that stops at the window never waits.
func TestSubSettleNoneStopsAtAFullPrefetchWindow(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "a", "b", "c", "d", "e", "f"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("sub", sharedDSN, "orders", "probe", "--start", "earliest",
		"--prefetch", "4", "--settle", "none", "--idle", "2s")
	if code != exitOK {
		t.Fatalf("sub --settle none = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{"received 4", "abandoned 4", "stopped: the prefetch window is full"} {
		if !strings.Contains(out, want) {
			t.Errorf("sub --settle none --prefetch 4 does not report %q:\n%s", want, out)
		}
	}
}

// TestSubSettleNoneHasNoWindowWithoutSettlement is the other side: a driver
// with no settlement at all (NATS Core) has nothing unsettled and no slot to
// hold, so stopping there would end a tail for no reason.
func TestSubSettleNoneHasNoWindowWithoutSettlement(t *testing.T) {
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		DefaultTopology: memory.TopologyConfig{
			Kind: memory.TopologyStream, Retention: memory.Retention{MaxMessages: 16},
		},
	})
	if err != nil {
		t.Fatalf("memory.NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	reg := dsn.NewRegistry()
	if err := reg.Register("nosettle", func(context.Context, string) (*mq.Client, error) {
		return mq.NewClient(&noSettlementBackend{Broker: broker}), nil
	}); err != nil {
		t.Fatalf("registering: %v", err)
	}
	h := harnessWith(t, reg, nil)
	if got := h.run("pub", "nosettle://", "orders", "a", "b", "c", "d", "e", "f"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("sub", "nosettle://", "orders", "probe", "--start", "earliest",
		"--prefetch", "4", "--settle", "none", "--max", "6")
	if code != exitOK {
		t.Fatalf("sub --settle none = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	if out := h.stdout(); !strings.Contains(out, "received 6") ||
		!strings.Contains(out, "stopped: the --max count was reached") {
		t.Errorf("a driver with no settlement stopped short of --max 6:\n%s", out)
	}
}

// noSettlementBackend is the memory broker advertising no settlement at all,
// which is NATS Core's shape, and the fan-out a tail needs.
type noSettlementBackend struct {
	*memory.Broker
}

// Capabilities advertises fan-out and no explicit acknowledgement.
func (b *noSettlementBackend) Capabilities() mq.Capabilities {
	var c mq.Capabilities
	for f, l := range map[mq.Feature]mq.Level{
		mq.FeatureExplicitAck: mq.Unsupported,
		mq.FeatureFanOut:      mq.Native,
		mq.FeatureAtLeastOnce: mq.Unsupported,
		mq.FeatureAtMostOnce:  mq.Native,
		mq.FeatureReplay:      mq.Native,
	} {
		if err := c.Set(f, mq.Support{Level: l}); err != nil {
			panic(err)
		}
	}
	return c
}

// Close leaves the broker open: each invocation's client borrows it, and the
// test closes it.
func (b *noSettlementBackend) Close() error { return nil }

// TestSubIdleStopsWithoutSleeping is the flake policy applied to a
// timeout: the idle timer is a FAKE clock's, already fired, so the test is
// deterministic and instant. A test that slept for the idle window would be
// the wrong shape and would be the first thing to flake on a loaded host.
func TestSubIdleStopsWithoutSleeping(t *testing.T) {
	h := newHarness(t).withClock(firedFor(time.Unix(1700000000, 0), 5*time.Second))
	code := h.run("sub", memDSN, "orders", "billing", "--idle", "5s", "--prefetch", "1")
	if code != exitOK {
		t.Fatalf("sub --idle = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "received 0") {
		t.Errorf("an empty topic delivered something:\n%s", out)
	}
	if !strings.Contains(out, "no delivery for 5s") {
		t.Errorf("sub did not report the IDLE expiry as the reason it stopped. "+
			"An idle expiry and an interrupt both arrive as context.Canceled from Receive, "+
			"so reporting the wrong one is reporting a falsehood:\n%s", out)
	}
}

// TestSubTimeoutThatHasAlreadyExpiredClaimsNoWork is --timeout's other end.
//
// The fake clock's timers have already fired, so the run's whole budget is
// spent before anything happens. TWO outcomes are legitimate and which one
// occurs is a race between the cancelling goroutine and the open: the open
// fails and the command exits non-zero, or the open wins and the loop stops
// on its first fetch. The first version of this test asserted the first
// outcome and flaked one run in about six under -shuffle, which is exactly
// the shape the flake policy forbids -- and it was the test that was
// wrong, not the program.
//
// What holds in BOTH cases is the invariant worth asserting: an expired run
// never claims work it did not do, and it never finishes silently.
func TestSubTimeoutThatHasAlreadyExpiredClaimsNoWork(t *testing.T) {
	for range 20 {
		h := newHarness(t).withClock(firedFor(time.Unix(1700000000, 0), 3*time.Second))
		code := h.run("sub", memDSN, "orders", "billing", "--timeout", "3s", "--prefetch", "1")
		out, errOut := h.stdout(), h.stderr()
		if strings.Contains(out, "received ") && !strings.Contains(out, "received 0") {
			t.Fatalf("an expired run reported deliveries it cannot have had:\n%s", out)
		}
		switch code {
		case exitOK:
			// The open won the race; the loop then stopped at once and has
			// to say why.
			if !strings.Contains(out, "stopped:") {
				t.Fatalf("an expired run succeeded without saying why it stopped:\n%s", out)
			}
		default:
			if errOut == "" {
				t.Fatalf("exit %d with nothing on stderr", code)
			}
		}
	}
}

// TestExpiringReportsWhichSideCancelled is the clock-injected timeout helper's
// own property, and the one the stop reasons rest on.
//
// "no delivery for five seconds" and "you pressed Ctrl-C" both reach Receive
// as context.Canceled, so nothing in the error can tell them apart -- a run
// that reported the wrong one would be stating a falsehood about why it
// stopped. expiring carries the answer beside the context.
func TestExpiringReportsWhichSideCancelled(t *testing.T) {
	t.Run("the timer cancelled it", func(t *testing.T) {
		e := &cliEnv{clock: firedFor(time.Unix(1700000000, 0), time.Second)}
		ctx, cancel, expired := e.expiring(context.Background(), time.Second)
		<-ctx.Done()
		cancel()
		if !expired() {
			t.Error("the timer fired and expired() reported false; an idle expiry would be " +
				"reported as an interrupt")
		}
	})
	t.Run("the caller cancelled it", func(t *testing.T) {
		e := &cliEnv{clock: &steppedClock{now: time.Unix(1700000000, 0), step: time.Nanosecond}}
		ctx, cancel, expired := e.expiring(context.Background(), time.Hour)
		cancel()
		<-ctx.Done()
		if expired() {
			t.Error("the caller cancelled and expired() reported true; an interrupt would be " +
				"reported as a timeout")
		}
	})
	t.Run("no bound of its own", func(t *testing.T) {
		e := &cliEnv{clock: firedFor(time.Unix(1700000000, 0), 0)}
		ctx, cancel, expired := e.expiring(context.Background(), 0)
		defer cancel()
		if expired() {
			t.Error("a zero duration is no bound at all and must never report an expiry")
		}
		if ctx.Err() != nil {
			t.Errorf("a zero duration cancelled the context immediately: %v", ctx.Err())
		}
	})
}

// TestSubConcurrency covers the worker path, whose bug would be a data race
// rather than a wrong count -- so it runs under -race in every make ci.
func TestSubConcurrency(t *testing.T) {
	h, _ := newSharedHarness(t)
	bodies := make([]string, 0, 35)
	bodies = append(bodies, "pub", sharedDSN, "orders")
	for i := range 32 {
		bodies = append(bodies, "message-"+itoa(i))
	}
	if got := h.run(bodies...); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("sub", sharedDSN, "orders", "billing", "--start", "earliest",
		"--max", "32", "--prefetch", "8", "--concurrency", "4", "--no-body")
	if code != exitOK {
		t.Fatalf("sub --concurrency 4 = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), "received 32") {
		t.Errorf("sub --concurrency lost or duplicated deliveries:\n%s", h.stdout())
	}
	if !strings.Contains(h.stdout(), "acked 32") {
		t.Errorf("not every delivery was settled:\n%s", h.stdout())
	}
}

// TestSubJSONOutput is the machine-readable form, one object per line.
func TestSubJSONOutput(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "-H", "trace=abc", "payload"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("sub", sharedDSN, "orders", "billing", "--start", "earliest",
		"--max", "1", "--prefetch", "1", "-o", "json", "--headers", "--meta")
	if code != exitOK {
		t.Fatalf("sub -o json = %d: %s", code, h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{`"n":1`, `"settlement":"acked"`, `"headers"`, `"trace":"abc"`} {
		if !strings.Contains(out, want) {
			t.Errorf("sub -o json does not contain %s:\n%s", want, out)
		}
	}
}

// TestSubHeadersAreOffByDefault is the half that applies to a CLI.
func TestSubHeadersAreOffByDefault(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "-H", "authorization=Bearer "+credToken,
		"payload"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("sub", sharedDSN, "orders", "billing", "--start", "earliest",
		"--max", "1", "--prefetch", "1")
	if code != exitOK {
		t.Fatalf("sub = %d: %s", code, h.stderr())
	}
	requireAbsent(t, "header value printed without --headers", credToken, h.stdout())
	if !strings.Contains(h.stdout(), "payload") {
		t.Errorf("the body was not printed, and showing the message is what sub is for:\n%s",
			h.stdout())
	}

	// With --headers it IS printed: an explicit request is not a leak.
	h2, _ := newSharedHarness(t)
	if got := h2.run("pub", sharedDSN, "orders", "-H", "x-trace=abc", "payload"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h2.stderr())
	}
	h2.out.Reset()
	if got := h2.run("sub", sharedDSN, "orders", "billing", "--start", "earliest",
		"--max", "1", "--prefetch", "1", "--headers"); got != exitOK {
		t.Fatalf("sub --headers = %d: %s", got, h2.stderr())
	}
	if !strings.Contains(h2.stdout(), "header x-trace=abc") {
		t.Errorf("--headers did not print the header:\n%s", h2.stdout())
	}
}

// TestSubEphemeralNote: a memory target cannot receive what another
// invocation published, and the CLI says so rather than appearing to lose a
// message.
func TestSubEphemeralNote(t *testing.T) {
	h := newHarness(t).withClock(firedFor(time.Unix(1700000000, 0), time.Second))
	if got := h.run("sub", memDSN, "orders", "billing", "--idle", "1s"); got != exitOK {
		t.Fatalf("sub = %d: %s", got, h.stderr())
	}
	if !strings.Contains(h.stdout(), "created by THIS invocation") {
		t.Errorf("sub against memory:// did not warn that the broker is per-invocation:\n%s",
			h.stdout())
	}

	// And a real target does not carry the note.
	h2, _ := newSharedHarness(t)
	h2.env.clock = firedFor(time.Unix(1700000000, 0), time.Second)
	h2.run("sub", sharedDSN, "orders", "billing", "--idle", "1s")
	if strings.Contains(h2.stdout(), "created by THIS invocation") {
		t.Errorf("the note appeared for a non-memory target:\n%s", h2.stdout())
	}
}

// TestParseSettleAndNack covers the flag vocabularies, including the refusals.
func TestParseSettleAndNack(t *testing.T) {
	for _, in := range []string{"ack", "nack", "reject", "none", ""} {
		if _, err := parseSettle("--settle", in); err != nil {
			t.Errorf("parseSettle(%q): %v", in, err)
		}
	}
	if _, err := parseSettle("--settle", "maybe"); err == nil {
		t.Error("parseSettle accepted \"maybe\"")
	}
	for _, in := range []string{"", "requeue", "drop", "dead-letter"} {
		if _, err := parseNackAction(in); err != nil {
			t.Errorf("parseNackAction(%q): %v", in, err)
		}
	}
	if _, err := parseNackAction("sideways"); err == nil {
		t.Error("parseNackAction accepted \"sideways\"")
	}
	for _, in := range []string{"", "latest", "earliest"} {
		if _, err := parseStart(in); err != nil {
			t.Errorf("parseStart(%q): %v", in, err)
		}
	}
	if _, err := parseStart("2026-01-01"); err == nil {
		t.Error("parseStart accepted a timestamp; --start offers only the two portable forms " +
			"because a time needs FeatureSeekByTime and a flag that silently failed on most " +
			"drivers would be worse than no flag")
	}
	// Every action has its own word, or two dispositions would be
	// indistinguishable in the report.
	words := map[string]bool{}
	for _, a := range []settleAction{settleAck, settleNack, settleReject, settleNone} {
		if words[a.String()] {
			t.Errorf("two settlement actions render as %q", a.String())
		}
		words[a.String()] = true
	}
}

// TestSubSettlementRefusedAsUnsupportedIsExitThree is the other half of
// --settle reject, and of the separation of the three failure kinds.
//
// Reject is Nack(NackActionDeadLetter), so it needs a dead-letter destination
// that EXISTS -- naming one in the topology is not the same as having one, and
// the memory driver refuses rather than creating a topic behind the operator's
// back. A refusal like that is exit 3 and not exit 1: nothing the caller
// retries will change it.
func TestSubSettlementRefusedAsUnsupportedIsExitThree(t *testing.T) {
	h := newHarness(t)
	// A plain memory target: no dead-letter topic at all.
	if got := h.run("pub", memDSN, "orders", "x"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	h.err.Reset()

	// One invocation that publishes and consumes, because memory:// is
	// per-invocation. bench does the round trip; here `sub` has to see a
	// delivery, so the shared broker is used with its dead-letter topic
	// REMOVED from the topology by naming a different one.
	h2, _ := newSharedHarnessNoDeadLetter(t)
	if got := h2.run("pub", sharedDSN, "orders", "x"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h2.stderr())
	}
	h2.out.Reset()
	code := h2.run("sub", sharedDSN, "orders", "billing",
		"--start", "earliest", "--max", "1", "--prefetch", "1", "--settle", "reject")
	if code != exitUnsupported {
		t.Fatalf("a settlement the driver refused as unsupported exited %d, want %d\n"+
			"stdout:\n%s\nstderr:\n%s", code, exitUnsupported, h2.stdout(), h2.stderr())
	}
	for _, want := range []string{
		"fact about the broker",
		"no retry changes it",
		"iqmq doctor",
	} {
		if !strings.Contains(h2.stderr(), want) {
			t.Errorf("the refusal does not say %q, so a caller cannot tell it from a broker "+
				"that was merely unreachable:\n%s", want, h2.stderr())
		}
	}
	if !strings.Contains(h2.stdout(), "reject-failed 1") {
		t.Errorf("the report does not count the refused settlement:\n%s", h2.stdout())
	}
}
