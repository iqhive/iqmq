package chaos

import (
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// memoryInjector is an [Injector] over the memory engine's own fault injector
// so that the whole chaos pipeline — the phases, the injection, the
// removal, the resume probe, the drain, the ledger and the verdict — runs in
// `make ci` with no container and no Toxiproxy.
//
// It does not claim to reproduce a network. A latency toxic has no in-process
// equivalent and this does not pretend otherwise: what is being tested here is
// the SCENARIO MACHINERY, and the machinery is where a chaos suite is most
// likely to be silently green. The toxics themselves are exercised by the
// tagged TestChaos_<suite> against a real broker.
type memoryInjector struct {
	b *memory.Broker
	// on records which methods were called, so a test can assert the
	// scenario reached for the fault it says it does.
	on []string
	// noop makes every method do nothing while still reporting success,
	// which is precisely what a mis-configured Toxiproxy proxy does.
	noop bool
}

func (m *memoryInjector) note(what string) { m.on = append(m.on, what) }

func (m *memoryInjector) apply(what string, arm, disarm func()) (func(context.Context) error, error) {
	m.note(what)
	if m.noop {
		return func(context.Context) error { return nil }, nil
	}
	arm()
	return func(context.Context) error {
		if disarm != nil {
			disarm()
		}
		return nil
	}, nil
}

func (m *memoryInjector) Latency(_ context.Context, _, _ int) (func(context.Context) error, error) {
	// The closest in-process analogue of "the acknowledgement took longer
	// than its deadline" is a settlement that finds its lease already
	// revoked, which is what DelaySettlement does deterministically.
	return m.apply("latency", func() { m.b.Faults().DelaySettlement(4) }, func() { m.b.Faults().DelaySettlement(0) })
}

func (m *memoryInjector) Reset(_ context.Context, _ int) (func(context.Context) error, error) {
	return m.apply("reset", func() { m.b.Faults().RejectPublish(4) }, func() { m.b.Faults().RejectPublish(0) })
}

func (m *memoryInjector) Bandwidth(_ context.Context, _ int) (func(context.Context) error, error) {
	return m.apply("bandwidth", func() {}, nil)
}

func (m *memoryInjector) Disable(_ context.Context) (func(context.Context) error, error) {
	return m.apply("disable", func() { m.b.Faults().CloseConsumers() }, nil)
}

func (m *memoryInjector) StopBroker(_ context.Context) (func(context.Context) error, error) {
	return m.apply("stop-broker", func() { m.b.Faults().Reconnect() }, nil)
}

func (m *memoryInjector) DeleteTopology(_ context.Context) (func(context.Context) error, error) {
	return m.apply("delete-topology", func() { m.b.Faults().RejectPublish(-1) }, func() { m.b.Faults().RejectPublish(0) })
}

// memEnv builds a memory client and the environment one scenario runs in.
func memEnv(t *testing.T, inj *memoryInjector) Env {
	t.Helper()
	b, err := memory.NewBroker(memory.Config{
		AutoCreate:      true,
		DefaultTopology: memory.TopologyConfig{Partitions: 1, AckDeadline: time.Minute},
	})
	if err != nil {
		t.Fatalf("building a broker: %v", err)
	}
	client := mq.NewClient(b)
	t.Cleanup(func() { _ = client.Close() })
	inj.b = b
	return Env{
		Client:         client,
		Topic:          "chaos-pipeline",
		Subscription:   "chaos",
		Injector:       inj,
		SettleTimeout:  time.Second,
		PublishTimeout: 2 * time.Second,
		ReceiveTimeout: 100 * time.Millisecond,
		// Short, so that the drain's "wait twice the lease before
		// converging" costs 200 ms rather than ten seconds: this file
		// runs in every `make ci`.
		AckDeadline: 100 * time.Millisecond,
		Log:         t.Logf,
	}
}

// find is one scenario by name.
func find(t *testing.T, name string) Scenario {
	t.Helper()
	for _, s := range Scenarios() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no scenario is called %q; they are %v", name, Names())
	return Scenario{}
}

// TestANoOpInjectorFailsTheRun is the anti-vacuity control, and it is the most
// important test in this package.
//
// A Toxiproxy toxic that does nothing — the wrong stream, a proxy name that
// does not match, a workload whose own timeouts are longer than the toxic —
// produces a chaos run in which nothing went wrong and every property holds
// trivially. That run must be RED, and this is the test that says so: the
// injector reports success and applies nothing, and the scenario's verdict has
// to reject it.
func TestANoOpInjectorFailsTheRun(t *testing.T) {
	inj := &memoryInjector{noop: true}
	env := memEnv(t, inj)
	s := find(t, "reset-mid-burst")
	s.Messages = 16
	s.ResumeBound = 2 * time.Second
	run, err := RunScenario(context.Background(), s, env)
	if err != nil {
		t.Fatalf("running the scenario: %v", err)
	}
	if len(inj.on) == 0 {
		t.Fatal("the scenario never reached for a fault at all")
	}
	err = s.Verdict(run)
	if err == nil {
		t.Fatalf("a run in which the fault did nothing was accepted: %s", run)
	}
	if !strings.Contains(err.Error(), "nothing was injected") {
		t.Fatalf("the rejection does not say the fault was a no-op: %v", err)
	}
	t.Logf("the verdict rejected a no-op fault: %v", err)
}

// TestARealFaultPassesTheSameScenario is the other direction: the same
// scenario with the fault actually applied must be accepted, or the check
// above is just a check that always fails.
func TestARealFaultPassesTheSameScenario(t *testing.T) {
	inj := &memoryInjector{}
	env := memEnv(t, inj)
	s := find(t, "reset-mid-burst")
	s.Messages = 16
	s.ResumeBound = 5 * time.Second
	run, err := RunScenario(context.Background(), s, env)
	if err != nil {
		t.Fatalf("running the scenario: %v", err)
	}
	if err := s.Verdict(run); err != nil {
		t.Fatalf("a run with the fault applied was rejected: %v\n  %s", err, run)
	}
	if run.Faulted == 0 || run.Report.Published[Rejected]+run.Report.Published[Ambiguous] == 0 {
		t.Fatalf("the fault was applied and the run recorded nothing: %s", run)
	}
	t.Logf("accepted: %s", run)
}

// TestTheLedgerCatchesLoss is the ledger half watched failing end to end: a
// fault that detaches every consumer leaves accepted messages undelivered, and the specification makes that data loss whatever the network did.
func TestTheLedgerCatchesLoss(t *testing.T) {
	inj := &memoryInjector{}
	env := memEnv(t, inj)
	s := find(t, "session-timeout-invalidates-the-generation")
	s.Messages = 16
	s.Checks = append(append([]Check(nil), s.Checks...), DrainConverged())
	// No resume probe: the point is the loss, and a resume probe would
	// publish more messages into a topic nobody is consuming.
	s.ResumeBound = 0
	// The phase stays DuringSettle. Moving it to BeforeAll seemed tidier
	// and was wrong: CloseConsumers detaches the consumers that EXIST, and
	// before the workload there are none — so the fault bit nothing, the run
	// recorded Faulted=0 and the verdict rejected it for being vacuous
	// rather than for the loss. The rejection read "the fault made 0
	// operation(s) fail ... nothing was injected, so nothing was tested",
	// which is FaultBit doing its job on a test that was asking the wrong
	// question.
	s.Phase = DuringSettle
	run, err := RunScenario(context.Background(), s, env)
	if err != nil {
		t.Fatalf("running the scenario: %v", err)
	}
	err = s.Verdict(run)
	if err == nil {
		t.Fatalf("a run whose consumers the fault detached was accepted: %s", run)
	}
	// The drain cannot converge on a client whose consumers are gone, so the
	// verdict is about the drain and NOT about loss: the ledger's eight
	// undelivered messages were never fetched, and claiming them as lost is the mistake. The ledger's own reading of the same
	// numbers, with the drain converged, is
	// TestNoLossWillNotClaimLossFromAnUnfinishedDrain.
	if !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("the rejection is not about the drain: %v\n  %s", err, run)
	}
	if got := len(run.Report.Missing); got == 0 {
		t.Fatalf("the fault delivered everything, so there is nothing for the drain guard to be about: %s", run)
	}
	t.Logf("the verdict rejected the run without claiming loss: %v", err)

	// And with the drain marked converged, the same ledger IS loss.
	converged := *run
	converged.Drained = true
	if err := NoLoss()(&converged); err == nil || !strings.Contains(err.Error(), "lost") {
		t.Fatalf("the same ledger with a converged drain reported %v, want a loss", err)
	}
}

// TestTopologyDeletionIsRefusedOrDegrades runs the BeforeAll scenario through
// the pipeline: a client whose topology has gone must refuse an operation or
// report itself degraded, and a run in which it did neither must be red.
func TestTopologyDeletionIsRefusedOrDegrades(t *testing.T) {
	inj := &memoryInjector{}
	env := memEnv(t, inj)
	s := find(t, "topology-deletion-is-refused-or-degrades")
	run, err := RunScenario(context.Background(), s, env)
	if err != nil {
		t.Fatalf("running the scenario: %v", err)
	}
	if err := s.Verdict(run); err != nil {
		t.Fatalf("a client whose publishes were all refused was failed: %v\n  %s", err, run)
	}
	t.Logf("accepted: %s (codes %v)", run, run.CodeNames())

	// And the negative half: a run in which nothing was refused and health
	// stayed ready must be rejected.
	clean := &Run{Scenario: s.Name, Report: NewLedger().Check()}
	if err := s.Verdict(clean); err == nil {
		t.Fatal("a run in which the deleted topology was never noticed was accepted")
	}
}

// TestScenarioRegistryIsWellFormed holds the registry to the shape the runner
// and the verdict assume of it. Every field here has been wrong in some
// version of this file.
func TestScenarioRegistryIsWellFormed(t *testing.T) {
	all := Scenarios()
	if len(all) < 6 {
		t.Fatalf("the registry holds %d scenario(s); the specification names six", len(all))
	}
	seen := map[string]bool{}
	for _, s := range all {
		t.Run(s.Name, func(t *testing.T) {
			switch {
			case s.Name == "":
				t.Error("no name")
			case seen[s.Name]:
				t.Errorf("two scenarios are called %q", s.Name)
			case s.Spec == "":
				t.Error("no spec reference, so a reader cannot tell what it is evidence for")
			case !s.Phase.Valid():
				t.Errorf("phase %d is not one of the three", s.Phase)
			case s.Inject == nil:
				t.Error("no fault")
			case len(s.Checks) == 0:
				t.Error("no checks, so the scenario cannot fail")
			case s.Messages <= 0:
				t.Error("no messages, so the ledger would be empty")
			}
			seen[s.Name] = true
			// Every check must be able to reject SOMETHING: a nil check
			// in the slice would be a silent pass.
			for i, c := range s.Checks {
				if c == nil {
					t.Errorf("check %d is nil", i)
				}
			}
			// A scenario with a resume bound must have the check that
			// reads it, or the bound is decoration.
			if s.ResumeBound > 0 && s.Name == "container-stop-start-reconnects" {
				if err := s.Verdict(&Run{Scenario: s.Name, Report: NewLedger().Check(), Faulted: 1}); err == nil {
					t.Error("a run that never resumed was accepted by a scenario with a resume bound")
				}
			}
		})
	}
	// Every scenario must reject the empty run. A scenario whose checks
	// all pass on a Run that recorded nothing is a scenario that cannot
	// fail, which is what this whole file exists to prevent.
	for _, s := range all {
		if err := s.Verdict(&Run{Scenario: s.Name, Report: NewLedger().Check()}); err == nil {
			t.Errorf("scenario %q accepted a run that recorded nothing at all", s.Name)
		}
	}
}

// TestSelectSeparatesNightlyFromExtended covers the specification's "nightly subset /
// weekly extended": the nightly set must be a strict subset and must not be
// empty, because a nightly that runs nothing and a nightly that runs
// everything are both wrong.
func TestSelectSeparatesNightlyFromExtended(t *testing.T) {
	nightly, extended := Select(false), Select(true)
	if len(nightly) == 0 {
		t.Fatal("the nightly subset is empty")
	}
	if len(nightly) >= len(extended) {
		t.Fatalf("the nightly subset holds %d of %d scenarios; nothing is weekly-only", len(nightly), len(extended))
	}
	in := map[string]bool{}
	for _, s := range nightly {
		in[s.Name] = true
		if s.Extended {
			t.Errorf("%q is marked extended and is in the nightly subset", s.Name)
		}
	}
	for _, s := range extended {
		if !s.Extended && !in[s.Name] {
			t.Errorf("%q is not extended and is missing from the nightly subset", s.Name)
		}
	}
	t.Logf("nightly %d, extended %d of %d", len(nightly), len(extended)-len(nightly), len(extended))
}

// TestRunScenarioRefusesAnUnstatedLease is the guard against the mismatch that
// produced twelve false loss reports.
//
// The drain waits twice the lease before it may converge, and nothing inside
// this package can know what lease the topology was created with. Defaulted,
// the two drifted — a five-second default against a thirty-second topology —
// and twelve Redis Streams entries that were still pending and unreclaimable
// were reported as lost with drained=true beside them. A caller that has to
// state it cannot write that by accident.
func TestRunScenarioRefusesAnUnstatedLease(t *testing.T) {
	inj := &memoryInjector{}
	env := memEnv(t, inj)
	env.AckDeadline = 0
	s := find(t, "reset-mid-burst")
	s.Messages = 2
	_, err := RunScenario(context.Background(), s, env)
	if err == nil {
		t.Fatal("a scenario with no lease stated ran anyway")
	}
	if !strings.Contains(err.Error(), "AckDeadline") {
		t.Fatalf("the refusal does not name the field: %v", err)
	}
}

// TestEveryResumeBoundIsJudged is the structural guard against the defect
// that got past two scenarios: a `ResumeBound` makes the runner spend up to
// that long probing for progress, and a scenario that declares one without a
// check that READS it reports `resumed=false` and passes.
func TestEveryResumeBoundIsJudged(t *testing.T) {
	checked := 0
	for _, s := range Scenarios() {
		if s.ResumeBound == 0 {
			continue
		}
		checked++
		t.Run(s.Name, func(t *testing.T) {
			// A run in which everything else is fine and the workload
			// never resumed must be rejected. If no check reads
			// Resumed, this passes and the bound is decoration.
			l := NewLedger()
			for i := range 20 {
				id := "ok" + string(rune('a'+i))
				l.Published(id, Accepted)
				l.Delivered(id)
			}
			stalled := &Run{
				Scenario: s.Name, Report: l.Check(),
				Faulted: 9, Unknown: 9, SettleFailed: 1,
				PeakPending: 1, PendingLimit: 16,
				Drained: true,
				Resumed: false, ResumeAfter: s.ResumeBound + time.Second,
			}
			if err := s.Verdict(stalled); err == nil {
				t.Fatalf("%q declares a %v resume bound and accepted a run that never resumed: %s",
					s.Name, s.ResumeBound, stalled)
			}
		})
	}
	if checked == 0 {
		t.Fatal("no scenario declares a resume bound, so this test asserts nothing")
	}
	t.Logf("%d scenario(s) with a resume bound, each judged", checked)
}
