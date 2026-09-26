package nack

import (
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestModel runs every kind through the reference model: three
// failure rates (the 0.1 %, 1 % and 10 %), a record that fails twice, and
// the shared invariants asserted after every step.
//
// The per-kind expectations below are the family's whole point. The four kinds
// do not agree on an answer, so the model cannot compare them against one
// oracle; what it can do is hold each of them to what its own documentation
// claims — order preserved or not, offset committed or not, consumer stopped or
// not — which is the property a caller chooses a kind by.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("failrate=0.1", func(t *testing.T) { modelRate(t, k, 1000) })
			t.Run("failrate=1", func(t *testing.T) { modelRate(t, k, 100) })
			t.Run("failrate=10", func(t *testing.T) { modelRate(t, k, 10) })
			t.Run("twice", func(t *testing.T) { modelTwice(t, k) })
			t.Run("actions", func(t *testing.T) { modelActions(t, k) })
			t.Run("exhaustion", func(t *testing.T) { modelExhaustion(t, k) })
		})
	}
}

// modelRate drives one kind at one failure rate and checks the kind's own
// claims about ordering, committing and stopping.
func modelRate(t *testing.T, k Kind, failEvery int) {
	const records = 200
	sm := newSim(t, mustStrategy(t, k, configFor(k)), simConfig{
		Records: records, FailEvery: failEvery, RestartOnStop: true,
	})
	sm.run()
	failures := (records + failEvery - 1) / failEvery
	if sm.delivered < records {
		t.Fatalf("%d records delivered of %d", sm.delivered, records)
	}
	if got := sm.pending(); got != 0 {
		t.Fatalf("%d record(s) never reached a terminal state", got)
	}
	switch k.Resolve() {
	case BlockPartition:
		if sm.violations != 0 {
			t.Fatalf("blockpartition reordered %d record(s); it holds the partition still precisely so that it reorders none", sm.violations)
		}
		if sm.stops != 0 {
			t.Fatalf("blockpartition stopped the consumer %d time(s)", sm.stops)
		}
		if sm.delivered != records+failures {
			t.Fatalf("blockpartition made %d deliveries; %d records plus %d in-place retries is %d", sm.delivered, records, failures, records+failures)
		}
	case RetryTopic:
		if failures > 0 && sm.violations != failures {
			t.Fatalf("retrytopic reordered %d record(s); every one of the %d failures is republished behind the partition", sm.violations, failures)
		}
		if sm.stops != 0 {
			t.Fatalf("retrytopic stopped the consumer %d time(s)", sm.stops)
		}
	case Stop:
		if sm.violations != 0 {
			t.Fatalf("stop reordered %d record(s); it commits nothing past the failure", sm.violations)
		}
		if failures > 0 && sm.stops != failures {
			t.Fatalf("stop halted %d time(s) for %d failure(s)", sm.stops, failures)
		}
	case Custom:
		if failures > 0 && sm.violations == 0 {
			t.Fatal("the custom arm republishes, so it must reorder what it republishes")
		}
	case Auto, kindCount:
		t.Fatalf("Kinds() produced %v, which is not a concrete kind", k)
	}
	if sm.maxLag > records {
		t.Fatalf("lag %d exceeds the partition", sm.maxLag)
	}
}

// modelTwice drives a workload whose failing records fail on their second
// attempt too, which is what makes a strategy decide about the same record
// twice.
func modelTwice(t *testing.T, k Kind) {
	sm := newSim(t, mustStrategy(t, k, configFor(k)), simConfig{
		Records: 60, FailEvery: 5, FailTwice: true, RestartOnStop: true,
	})
	sm.run()
	if got := sm.pending(); got != 0 {
		t.Fatalf("%d record(s) never reached a terminal state", got)
	}
	if k.Resolve() == BlockPartition && sm.violations != 0 {
		t.Fatalf("blockpartition reordered %d record(s) with two failures per record", sm.violations)
	}
}

// modelActions checks the two nack actions that are the caller's instruction
// rather than the strategy's choice. Every kind answers them identically,
// which is what makes the kinds comparable: what they differ about is a
// requeue.
func modelActions(t *testing.T, k Kind) {
	s := mustStrategy(t, k, configFor(k))
	d := Delivery{Topic: "orders", Offset: 7, Attempt: 1}

	p, err := s.Plan(d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if err != nil {
		t.Fatalf("dead-letter action: %v", err)
	}
	if k.Resolve() != Custom {
		if p.Op != OpDeadLetter || p.Topic != "orders.dlq" {
			t.Fatalf("dead-letter action produced %+v", p)
		}
	}
	if p, err = s.Plan(d, mq.NackOptions{Action: mq.NackActionDrop}); err != nil {
		t.Fatalf("drop action: %v", err)
	}
	if p.Op != OpDrop {
		t.Fatalf("drop action produced %+v", p)
	}
	// An action outside the three is refused, never guessed at.
	if _, err = s.Plan(d, mq.NackOptions{Action: mq.NackAction(9)}); err == nil {
		t.Fatal("an action outside the specification was accepted")
	}
	// A dead-letter action with no destination is refused rather than
	// turned into a drop: the specification only authorises a route off the topic when
	// an explicit destination exists.
	if k.Resolve() == Custom {
		return
	}
	bare := mustStrategy(t, k, Config{})
	if _, err = bare.Plan(d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err == nil {
		t.Fatal("a dead-letter action with no destination was accepted")
	}
}

// modelExhaustion checks ADR-011's failure policy: a record whose attempts are
// exhausted goes to the dead-letter destination when there is one, and halts
// the consumer when there is not. Nothing drops it.
func modelExhaustion(t *testing.T, k Kind) {
	if k.Resolve() == Custom {
		return // the custom arm's policy is the caller's, not ADR-011's.
	}
	d := Delivery{Topic: "orders", Offset: 3, Attempt: 4}

	withDLQ := mustStrategy(t, k, Config{MaxAttempts: 4, DeadLetterTopic: "orders.dlq"})
	p, err := withDLQ.Plan(d, mq.NackOptions{})
	if err != nil {
		t.Fatalf("exhausted with a dead-letter topic: %v", err)
	}
	if k.Resolve() == Stop {
		if p.Op != OpStop {
			t.Fatalf("stop produced %+v for an exhausted record", p)
		}
	} else if p.Op != OpDeadLetter || p.Topic != "orders.dlq" {
		t.Fatalf("exhausted record produced %+v; ADR-011 routes it to the dead-letter topic", p)
	}

	withoutDLQ := mustStrategy(t, k, Config{MaxAttempts: 4})
	if p, err = withoutDLQ.Plan(d, mq.NackOptions{}); err != nil {
		t.Fatalf("exhausted with no dead-letter topic: %v", err)
	}
	if p.Op != OpStop {
		t.Fatalf("exhausted record with no dead-letter topic produced %+v; ADR-011 halts rather than dropping", p)
	}
	if p.Completes() {
		t.Fatalf("%+v completes the offset of a record nothing has handled", p)
	}
}
