package differential

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/memory"
)

// fuzzBudget is what ONE execution of FuzzDifferentialMemory may spend. It is
// the value TestFuzzBudgetFitsTheFuzzWorkersWatchdog holds to its two bounds:
// above the worst script MaxOps allows (29.2s of receive timeouts and drains
// over two arms) and below the 60s after which Go's fuzz worker panics and the
// coordinator files the input as a crasher that cannot be reproduced. The old
// value was 90s, which is above the watchdog and so could never fire.
const fuzzBudget = 45 * time.Second

// shortOpts keeps an in-process run quick: a memory arm answers immediately,
// so a two-second receive timeout is two seconds of nothing when a script
// receives from an empty topic.
func shortOpts() RunOptions {
	return RunOptions{
		ReceiveTimeout:     150 * time.Millisecond,
		AckDeadline:        300 * time.Millisecond,
		DrainBudget:        5 * time.Second,
		MaxDrainDeliveries: 8 * MaxOps,
	}
}

// TestPairRunsOneScriptOnBothArms is the pipeline end to end with no
// container: the same script against two memory brokers, which must agree.
//
// It is not a tautology. The script decode, the runner's outstanding FIFO, the
// drain's two bounds, the id round trip through the body and the relation all
// run here, and each of them can be wrong in a way that shows up as a
// divergence between two identical brokers.
func TestPairRunsOneScriptOnBothArms(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p := &Pair{Oracle: OracleArm(), Subject: MemoryArm("memory-subject", MemoryConfig{}), Opts: shortOpts()}
	// Written out rather than decoded from bytes: a byte literal with a
	// comment saying what it decodes to stops being true the moment the
	// alphabet changes, and this one did.
	script := Script{Keys: 2, Ops: []Op{
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionNackThenAck},
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionDrop},
		{Kind: OpReceive, N: 4},
		{Kind: OpReceive, N: 4},
	}}
	res, err := p.Run(ctx, script)
	if err != nil {
		t.Fatalf("running the pair: %v", err)
	}
	t.Log(res)
	if err := res.Result.Err(); err != nil {
		t.Fatalf("two identical memory brokers diverged: %v\n  oracle: %+v\n  subject: %+v", err, res.Oracle, res.Subject)
	}
	if res.Result.Summary.Compared == 0 {
		t.Fatal("the run compared nothing, so it proved nothing")
	}
}

// TestInjectedDefectsAreCaughtEndToEnd is the negative control over the whole
// pipeline rather than over the relation alone. Each row arms a real fault on
// the SUBJECT broker (the injector), asserts the fault actually fired — a
// fault that fired zero times is how a test that proves nothing looks from the
// outside — and requires the relation to reject the run under the kind named.
func TestInjectedDefectsAreCaughtEndToEnd(t *testing.T) {
	// A script with several messages per key and a settlement for each, so
	// that a duplicate or a reorder has room to be visible.
	script := Script{Keys: 2, Ops: []Op{
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpReceive, N: 8},
	}}

	t.Run("a settlement that arrives after its lease is a duplicate", func(t *testing.T) {
		var fired func() int64
		subject := MemoryArm("memory-delayed-settlement", MemoryConfig{
			Arm: func(b *memory.Broker) error {
				f := b.Faults()
				// Three, not every settlement: DelaySettlement(-1)
				// revokes the lease of every ack for ever, so the
				// drain never converges — the first run of this row
				// ended "the drain took 513 deliver(ies), over its
				// 512 ceiling", which is the harness bound doing
				// its job and not a comparison.
				f.DelaySettlement(3)
				fired = f.SettlementDelays
				return nil
			},
		})
		// Strictly no duplicates, which is what a driver claiming
		// exactly-once delivery advertises. The run's own permissions
		// come from the memory engine's table, which permits them, so
		// the stricter reading is set by hand here.
		subject.Perm = Permissions{ReorderAcrossKeys: true, AmbiguousMayVanish: true, AmbiguousMayArrive: true, LateRedelivery: true}
		res := runPair(t, subject, script)
		if fired == nil || fired() == 0 {
			t.Fatal("the DelaySettlement fault never fired, so the row asserts nothing")
		}
		requireRejected(t, res, KindDuplicate)
	})

	t.Run("a reordered relaxed lane breaks per-key order", func(t *testing.T) {
		var fired func() int64
		subject := MemoryArm("memory-reordered", MemoryConfig{
			Relaxed: true,
			Arm: func(b *memory.Broker) error {
				f := b.Faults()
				f.Reorder(-1)
				fired = f.Reorders
				return nil
			},
		})
		subject.Perm = Permissions{Duplicates: true, ReorderAcrossKeys: true, AmbiguousMayVanish: true, AmbiguousMayArrive: true, LateRedelivery: true}
		res := runPair(t, subject, script)
		if fired == nil || fired() == 0 {
			t.Skip("the Reorder fault did not fire against this engine binding, so there is nothing to assert")
		}
		requireRejected(t, res, KindOrderWithinKey)
	})
}

// TestInjectedObservationDefectsAreCaughtOnRealRuns applies the transform
// table to an observation a real run produced, rather than to the hand-written
// fixture.
//
// The fixture proves the relation's logic; this proves the SHAPE a run
// produces is a shape the relation binds. A relation that only ever saw the
// fixture could depend on something the fixture has and a run does not — three
// keys, an ambiguous publish, ids in a particular order — and would then accept
// every real run whatever it did.
func TestInjectedObservationDefectsAreCaughtOnRealRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p := &Pair{Oracle: OracleArm(), Subject: MemoryArm("memory-subject", MemoryConfig{}), Opts: shortOpts()}
	// The script is written out rather than decoded from bytes so that it
	// certainly contains one of each settlement: a base run with no dropped
	// and no nacked message skips five of the twelve defect rows, and five
	// skips look exactly like five passes in a summary line.
	base, err := p.Run(ctx, Script{Keys: 3, Ops: []Op{
		{Kind: OpPublish, Key: 0, Action: ActionNackThenAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionDrop},
		{Kind: OpPublish, Key: 2, Action: ActionAck},
		{Kind: OpPublish, Key: 0, Action: ActionAck},
		{Kind: OpPublish, Key: 1, Action: ActionAck},
		{Kind: OpReceive, N: 6},
		{Kind: OpReceive, N: 4},
	}})
	if err != nil {
		t.Fatalf("running the pair: %v", err)
	}
	if err := base.Result.Err(); err != nil {
		t.Fatalf("the base run diverged before anything was injected: %v", err)
	}
	t.Logf("base run: %s", base)

	if got := base.Result.Summary.Compared; got < 5 {
		t.Fatalf("the base run compared %d id(s); the defect rows below need several", got)
	}
	applied, skipped := 0, 0
	for _, tr := range Transforms() {
		if tr.Legal {
			continue
		}
		t.Run(tr.Name, func(t *testing.T) {
			got, changed := tr.Apply(base.Oracle, base.Subject.Clone())
			if !changed {
				skipped++
				t.Skipf("the transform found no target in this run's observation (%s)", base.Subject)
			}
			applied++
			res := Compare(base.Oracle, got, tr.Perm)
			if res.OK() {
				t.Fatalf("the defect was accepted on a real observation [%s]", res.Summary)
			}
			t.Logf("rejected as %v", res.Kinds())
		})
	}
	// Half the table is the floor, not zero: a base run that skipped most
	// of the rows would report a line of passes over a handful of
	// assertions, which is the shape of a check that stopped checking.
	defects := 0
	for _, tr := range Transforms() {
		if !tr.Legal {
			defects++
		}
	}
	if applied*2 < defects {
		t.Fatalf("only %d of %d defect(s) found a target in this run (%d skipped); the base script is too thin",
			applied, defects, skipped)
	}
	t.Logf("%d of %d injected defect(s) rejected on a real observation, %d found no target", applied, defects, skipped)
}

// TestDrainRefusesATruncatedObservation is the harness's own bound, watched
// working. A broker that redelivers every settled message for ever would
// otherwise let the drain spend its budget and then compare a TRUNCATED
// observation against a complete one — a divergence about the harness reported
// as a divergence about the broker.
//
// DelaySettlement(-1) revokes the lease of every settlement, so nothing ever
// settles and the redelivery never stops. The run must fail, and it must fail
// saying which bound it hit.
func TestDrainRefusesATruncatedObservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	subject := MemoryArm("memory-never-settles", MemoryConfig{
		Arm: func(b *memory.Broker) error { b.Faults().DelaySettlement(-1); return nil },
	})
	p := &Pair{Oracle: OracleArm(), Subject: subject, Opts: shortOpts()}
	_, err := p.Run(ctx, Script{Keys: 1, Ops: []Op{
		{Kind: OpPublish, Action: ActionAck},
		{Kind: OpPublish, Action: ActionAck},
		{Kind: OpReceive, N: 2},
	}})
	if err == nil {
		t.Fatal("a broker that never settles produced an observation the run accepted")
	}
	for _, want := range []string{"memory-never-settles", "drain"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	t.Logf("the drain refused the run: %v", err)
}

// TestOrderingSurvivesAKeylessTransport covers the grouping decision a driver whose transport carries no key at all still has
// its per-key order compared, because the grouping reads the key the SCRIPT
// published under. Grouping by the delivered key would put every delivery in
// one group on the subject and three on the oracle, and the relation would
// fail for a reason that is not about order.
func TestOrderingSurvivesAKeylessTransport(t *testing.T) {
	oracle := Fixture()
	subject := Subject()
	for i := range subject.Dels {
		subject.Dels[i].Key = "" // a transport with no keys
	}
	res := Compare(oracle, subject, Permissive())
	if err := res.Err(); err != nil {
		t.Fatalf("a keyless transport was refused: %v", err)
	}
	if res.Summary.OrderedKeys != 3 {
		t.Errorf("ordered-keys = %d, want 3: the ordering half did not bind", res.Summary.OrderedKeys)
	}
	if res.Summary.KeyMismatches != len(subject.Dels) {
		t.Errorf("key-mismatches = %d, want %d: the report does not say the keys differed",
			res.Summary.KeyMismatches, len(subject.Dels))
	}
	// And the ordering half still bites on such a transport.
	reordered, changed := swapFirst(subject, "m1", "m3")
	if !changed {
		t.Fatal("the swap was a no-op")
	}
	if !Compare(oracle, reordered, Permissive()).Has(KindOrderWithinKey) {
		t.Error("a reorder within a key went unnoticed once the keys were stripped")
	}
}

// TestScriptRestrictionKeepsBothArmsIdentical covers the reason Restrict
// exists: Kafka has no per-message nack, so a script with one would run
// differently on the two arms and the relation would report a broker defect.
func TestScriptRestrictionKeepsBothArmsIdentical(t *testing.T) {
	s := Script{Keys: 1, Ops: []Op{
		{Kind: OpPublish, Action: ActionNackThenAck},
		{Kind: OpPublish, Action: ActionDrop},
		{Kind: OpPublish, Action: ActionAck},
		{Kind: OpReceive, N: 1},
	}}
	out, n := s.Restrict(Features{Nack: false, Drop: false})
	if n != 2 {
		t.Fatalf("rewrote %d op(s), want 2", n)
	}
	for _, op := range out.Ops {
		if op.Kind == OpPublish && op.Action != ActionAck {
			t.Fatalf("a %s survived a restriction that forbids it: %s", op.Action, out)
		}
	}
	if kept, n := s.Restrict(Features{Nack: true, Drop: true}); n != 0 || kept.String() != s.String() {
		t.Errorf("a permissive restriction rewrote %d op(s) and produced %s", n, kept)
	}
	// Intersect is the thing that actually decides it.
	if got := Intersect(Features{Nack: true, Drop: true}, Features{Nack: true}); got.Drop {
		t.Error("Intersect granted a feature only one arm has")
	}
}

// TestDecodeScriptIsTotalAndRoundTrips holds the decoder to the two properties
// a fuzz target needs of it: every input decodes, and a script written back out
// decodes to itself — otherwise a committed seed is not the script it names.
func TestDecodeScriptIsTotalAndRoundTrips(t *testing.T) {
	for _, data := range [][]byte{nil, {}, {0}, {255}, {1, 2, 3}, make([]byte, 512)} {
		s := DecodeScript(data)
		if len(s.Ops) == 0 {
			t.Fatalf("DecodeScript(%v) produced no ops; Normalise should have added some", data)
		}
		if s.Keys < 1 || s.Keys > MaxKeys {
			t.Fatalf("DecodeScript(%v).Keys = %d", data, s.Keys)
		}
		if len(s.Ops) > MaxOps+MaxKeys+1 {
			t.Fatalf("DecodeScript(%v) produced %d ops, over the bound", data, len(s.Ops))
		}
		again := DecodeScript(WriteScript(s))
		if again.String() != s.String() {
			t.Fatalf("round trip changed the script:\n  before %s\n  after  %s", s, again)
		}
	}
	if !strings.Contains(DecodeScript(nil).String(), "publish") {
		t.Error("an empty input decoded to a script with no publish, which would compare nothing")
	}
}

// TestRunRefusesAnExhaustedBudget is the play phase's missing bound, watched
// working — the same reading [TestDrainRefusesATruncatedObservation] gives the
// drain phase, applied to the half that had no bound at all.
//
// A receive asks for ReceiveTimeout off the RUN's context. When the run's own
// context is what expired, every remaining receive returns instantly with
// nothing, which `receive` used to record as "nothing available" — a legal
// answer — so an over-budget run finished early and handed Compare a TRUNCATED
// observation. That is a divergence about the harness reported as one about the
// broker. It is the defect this case pins; it is NOT
// established as the cause of issue #58, whose input does not reproduce.
func TestRunRefusesAnExhaustedBudget(t *testing.T) {
	// 20 receives at shortOpts' 150ms is three seconds of script against a
	// budget of 600ms, which is long enough for the client to open and far
	// too short for the script to finish.
	script := Script{Keys: 1, Ops: []Op{{Kind: OpPublish, Action: ActionAck}}}
	for range 20 {
		script.Ops = append(script.Ops, Op{Kind: OpReceive, N: 1})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	obs, err := Run(ctx, OracleArm(), script, shortOpts())
	if err == nil {
		t.Fatalf("a run that spent its whole budget mid-script returned no error, so the observation it produced would have been compared: %+v", obs)
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Errorf("error %q does not say the run ran out of budget", err)
	}
	t.Logf("the run refused to go on: %v", err)
}

// TestFuzzBudgetFitsTheFuzzWorkersWatchdog is the arithmetic behind
// fuzzBudget, as a case that fails when one of its inputs moves.
//
// Go's fuzzing worker arms a timer per input and panics "deadlocked!" when one
// execution exceeds 60s (GOROOT/src/internal/fuzz/worker.go). The panic's
// message is never printed, the worker exits 2, and the coordinator reports
// "fuzzing process hung or terminated unexpectedly: exit status 2" and files
// the input as a crasher — which is exactly what issue #58 is, and why the
// input it names replays green. So the target's own budget has to sit between
// the worst script it can be handed and that watchdog; a budget above the
// watchdog can never fire.
func TestFuzzBudgetFitsTheFuzzWorkersWatchdog(t *testing.T) {
	const watchdog = 60 * time.Second
	o := shortOpts().resolve()
	// Per arm: every op a receive that finds nothing, then a drain that
	// spends its whole budget. Two arms, run one after the other.
	worst := 2 * (time.Duration(MaxOps)*o.ReceiveTimeout + o.DrainBudget)
	if fuzzBudget < worst {
		t.Errorf("fuzzBudget is %v and the worst script MaxOps(%d) allows costs %v: an input at the bound would be cut off by the budget rather than run",
			fuzzBudget, MaxOps, worst)
	}
	if fuzzBudget >= watchdog {
		t.Errorf("fuzzBudget is %v and the fuzz worker's watchdog is %v: the budget can never fire, so a slow input is filed as an unreproducible crasher instead of a named harness failure",
			fuzzBudget, watchdog)
	}
	t.Logf("worst script %v <= fuzzBudget %v < watchdog %v", worst, fuzzBudget, watchdog)
}

// FuzzDifferentialMemory is the container-free half of the differential: one
// script against two memory brokers, which must always agree.
//
// The broker arms are FuzzDifferential_<suite> in broker_test.go, behind the
// integration tag, and are therefore invisible to `make fuzz-seeds` and to the
// fuzz ratchet — `go test -list '^Fuzz'` builds with no tags. This target is
// what those two see, and it is the one that runs in every `make ci`.
func FuzzDifferentialMemory(f *testing.F) {
	f.Add([]byte(nil))
	// Two raw byte seeds, deliberately without a comment claiming what they
	// decode to: the mapping from bytes to ops is the fuzzer's business and
	// a comment about it goes stale the first time the alphabet moves.
	f.Add([]byte{2, 0, 0, 0, 1, 5, 4, 7, 7})
	f.Add([]byte{1, 0, 0, 0, 0, 5, 8, 9, 1, 5, 8, 7, 7, 7, 7})
	f.Add(WriteScript(Script{Keys: 2, Ops: []Op{
		{Kind: OpPublish, Key: 0, Action: ActionDrop},
		{Kind: OpPublish, Key: 1, Action: ActionNackThenAck},
		{Kind: OpReceive, N: 2},
		{Kind: OpReceive, N: 2},
	}}))
	f.Fuzz(func(t *testing.T, data []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), fuzzBudget)
		defer cancel()
		p := &Pair{Oracle: OracleArm(), Subject: MemoryArm("memory-subject", MemoryConfig{}), Opts: shortOpts()}
		res, err := p.Run(ctx, DecodeScript(data))
		if err != nil {
			t.Fatalf("running the pair: %v", err)
		}
		if err := res.Result.Err(); err != nil {
			t.Fatalf("two identical memory brokers diverged\n  %s\n  %v", res, err)
		}
	})
}

// runPair is the boilerplate of one injected-fault row.
func runPair(t *testing.T, subject Arm, script Script) PairResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p := &Pair{Oracle: OracleArm(), Subject: subject, Opts: shortOpts()}
	res, err := p.Run(ctx, script)
	if err != nil {
		t.Fatalf("running the pair: %v", err)
	}
	t.Log(res)
	return res
}

// requireRejected is the assertion a fault row ends with.
func requireRejected(t *testing.T, res PairResult, want Kind) {
	t.Helper()
	if res.Result.OK() {
		t.Fatalf("the injected fault was accepted [%s]\n  oracle:  %+v\n  subject: %+v",
			res.Result.Summary, res.Oracle, res.Subject)
	}
	if !res.Result.Has(want) {
		t.Fatalf("rejected as %v, want %v: %v", res.Result.Kinds(), want, res.Result.Err())
	}
	t.Logf("the relation rejected the injected fault as %v", res.Result.Kinds())
}
