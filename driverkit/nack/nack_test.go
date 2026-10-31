package nack

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestKindProperties holds the enumeration to the shape every family's kind
// type has: four concrete kinds, distinct slugs, Auto resolving to one of them.
func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 4 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		got, ok := ParseKind(k.String())
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) = %v, %v", k, got, ok)
		}
	}
	if !Auto.Valid() || Auto.Resolve() != AutoKind || AutoKind == Auto {
		t.Fatalf("Auto resolves to %v", Auto.Resolve())
	}
	outside := kindCount
	if outside.Valid() || outside.String() != "kind(?)" || outside.Ident() != "" {
		t.Fatal("a kind outside the enumeration is reported as valid")
	}
	if _, ok := ParseKind("nonesuch"); ok {
		t.Fatal("ParseKind accepted a slug no kind has")
	}
}

// TestAutoIsTheOrderPreservingKind is own rule, as a check.
//
// "none silently chosen; default block-partition under SafeOnly" means the
// default may not be a kind that changes ordering or durability, whatever a
// measurement says. Binding Auto to retry topics because they are faster would
// silently reorder every caller's partitions, which is the one outcome the row
// forbids.
func TestAutoIsTheOrderPreservingKind(t *testing.T) {
	s, err := New(Auto, Config{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Plan(Delivery{Topic: "orders", Offset: 1, Attempt: 1}, mq.NackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !p.PreservesOrder() || p.Completes() {
		t.Fatalf("Auto (%v) plans %+v, which reorders or commits; the specification forbids selecting such a kind by default", AutoKind, p)
	}
}

// TestOpProperties holds the two derived predicates to their contract: an
// operation that completes an offset is one that has moved the record
// elsewhere, and one that preserves order is one that has not.
func TestOpProperties(t *testing.T) {
	for _, tc := range []struct {
		op                 Op
		completes, ordered bool
	}{
		{OpRetryInPlace, false, true},
		{OpRepublish, true, false},
		{OpDeadLetter, true, false},
		{OpDrop, true, false},
		{OpStop, false, true},
	} {
		if tc.op.Completes() != tc.completes || tc.op.PreservesOrder() != tc.ordered {
			t.Fatalf("%v: completes=%v ordered=%v", tc.op,
				tc.op.Completes(), tc.op.PreservesOrder())
		}
		if !tc.op.Valid() || tc.op.String() == "Op(?)" {
			t.Fatalf("%d: not a valid operation", tc.op)
		}
	}
	bad := opCount
	if bad.Valid() || bad.String() != "Op(?)" || bad.Completes() || bad.PreservesOrder() {
		t.Fatal("an operation outside the enumeration is reported as valid")
	}
}

// TestPlanValidateRefusesTheLosingCombinations is the guard a Custom strategy
// runs into. Both refusals lose a message rather than merely behaving oddly.
func TestPlanValidateRefusesTheLosingCombinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    Plan
		ok   bool
	}{
		{"retry in place", Plan{Op: OpRetryInPlace, Pause: true}, true},
		{"republish", Plan{Op: OpRepublish, Topic: "t.retry.1"}, true},
		{"dead letter", Plan{Op: OpDeadLetter, Topic: "t.dlq"}, true},
		{"drop", Plan{Op: OpDrop}, true},
		{"stop", Plan{Op: OpStop}, true},
		{"republish with no topic", Plan{Op: OpRepublish}, false},
		{"dead letter with no topic", Plan{Op: OpDeadLetter}, false},
		{"topic on a non-publishing op", Plan{Op: OpDrop, Topic: "t"}, false},
		{"paused and committed", Plan{Op: OpDrop, Pause: true}, false},
		{"negative delay", Plan{Op: OpStop, Delay: -time.Second}, false},
		{"op outside the enumeration", Plan{Op: opCount}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate(%+v) = %v", tc.p, err)
			}
			if err != nil && !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("Validate(%+v) = %v, want ErrInvalidPlan", tc.p, err)
			}
		})
	}
}

// TestCustomPlanIsValidated: a custom strategy's plan goes through Validate,
// so a function that returns a self-contradictory plan is refused rather than
// followed.
func TestCustomPlanIsValidated(t *testing.T) {
	s, err := New(Custom, Config{Decide: func(Delivery, mq.NackOptions) (Plan, error) {
		return Plan{Op: OpRepublish}, nil // no topic: publishes nowhere, then commits
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Plan(Delivery{Topic: "t"}, mq.NackOptions{}); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("Plan = %v, want ErrInvalidPlan", err)
	}
	// The function's own error is passed through rather than swallowed.
	boom := errors.New("boom")
	s, err = New(Custom, Config{Decide: func(Delivery, mq.NackOptions) (Plan, error) {
		return Plan{}, boom
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Plan(Delivery{Topic: "t"}, mq.NackOptions{}); !errors.Is(err, boom) {
		t.Fatalf("Plan = %v, want the decision function's own error", err)
	}
}

// TestConfigValidate covers the refusals, including the one the specification names: a
// custom strategy with no function is not quietly replaced by another kind.
func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		k    Kind
		cfg  Config
		want error
	}{
		{"zero is usable", BlockPartition, Config{}, nil},
		{"custom needs a function", Custom, Config{}, ErrNoDecideFunc},
		{"custom with a function", Custom, Config{Decide: func(Delivery, mq.NackOptions) (Plan, error) { return Plan{Op: OpStop}, nil }}, nil},
		{"kind outside the enumeration", kindCount, Config{}, ErrInvalidKind},
		{"negative delay", BlockPartition, Config{Delay: -1}, ErrInvalidConfig},
		{"negative retries", RetryTopic, Config{Retries: -1}, ErrInvalidConfig},
		{"unprintable dead-letter topic", BlockPartition, Config{DeadLetterTopic: "a\nb"}, ErrInvalidConfig},
		{"empty-ish dead-letter topic", BlockPartition, Config{DeadLetterTopic: " "}, ErrInvalidConfig},
		{"over-long dead-letter topic", BlockPartition, Config{DeadLetterTopic: strings.Repeat("x", 250)}, ErrInvalidConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate(tc.k)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				if _, nerr := New(tc.k, tc.cfg); nerr != nil {
					t.Fatalf("New = %v", nerr)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
			if _, nerr := New(tc.k, tc.cfg); !errors.Is(nerr, tc.want) {
				t.Fatalf("New = %v, want %v", nerr, tc.want)
			}
		})
	}
}

func TestRetryTopicNames(t *testing.T) {
	if got := RetryTopicName("orders", 1); got != "orders.retry.1" {
		t.Fatalf("RetryTopicName = %q", got)
	}
	if got := RetryTopicName("orders", 0); got != "orders" {
		t.Fatalf("RetryTopicName(·, 0) = %q; level zero is the base topic", got)
	}
	if got := RetryTopicName("orders", -2); got != "orders" {
		t.Fatalf("RetryTopicName(·, -2) = %q", got)
	}
	for _, tc := range []struct {
		topic string
		base  string
		n     int
		ok    bool
	}{
		{"orders.retry.1", "orders", 1, true},
		{"orders.retry.12", "orders", 12, true},
		{"a.b.c.retry.3", "a.b.c", 3, true},
		{"orders.retry.1.retry.2", "orders.retry.1", 2, true},
		{"orders", "orders", 0, false},
		{"orders.retry.", "orders.retry.", 0, false},
		{"orders.retry.0", "orders.retry.0", 0, false},
		{"orders.retry.01", "orders.retry.01", 0, false},
		{"orders.retry.-1", "orders.retry.-1", 0, false},
		{"orders.retry.x", "orders.retry.x", 0, false},
		{"orders.retry.1x", "orders.retry.1x", 0, false},
		{"orders.retry.9999999999", "orders.retry.9999999999", 0, false},
		{".retry.1", ".retry.1", 0, false},
	} {
		base, n, ok := ParseRetryTopic(tc.topic)
		if base != tc.base || n != tc.n || ok != tc.ok {
			t.Fatalf("ParseRetryTopic(%q) = %q, %d, %v; want %q, %d, %v",
				tc.topic, base, n, ok, tc.base, tc.n, tc.ok)
		}
	}
}

// TestRetryTopicWalksTheLevels: the level comes from the topic the record
// arrived on, not from its attempt count, which is the only source that
// survives a rebalance — a republished record's attempt is 1 again.
func TestRetryTopicWalksTheLevels(t *testing.T) {
	s := mustStrategy(t, RetryTopic, Config{Retries: 3, DeadLetterTopic: "orders.dlq"})
	want := []string{"orders.retry.1", "orders.retry.2", "orders.retry.3"}
	topic := "orders"
	for i, w := range want {
		p, err := s.Plan(Delivery{Topic: topic, Offset: int64(i), Attempt: 1}, mq.NackOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if p.Op != OpRepublish || p.Topic != w {
			t.Fatalf("level %d: %+v, want a republication to %q", i+1, p, w)
		}
		if !p.Completes() {
			t.Fatalf("level %d: %+v does not commit the original offset, so the partition would not move", i+1, p)
		}
		topic = p.Topic
	}
	p, err := s.Plan(Delivery{Topic: topic, Offset: 9, Attempt: 1}, mq.NackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Op != OpDeadLetter || p.Topic != "orders.dlq" {
		t.Fatalf("past the last level: %+v, want the dead-letter route", p)
	}
}

// TestRetryTopicRefusesAnEmptyTopic: FuzzNackPlan found that a delivery with
// no topic made the retry-topic strategy republish to ".retry.1" — a topic
// nobody created, which ParseRetryTopic then refuses to read back — and commit
// the original offset, so the record was lost to a dead end. The strategy
// refuses the settlement instead.
func TestRetryTopicRefusesAnEmptyTopic(t *testing.T) {
	s := mustStrategy(t, RetryTopic, Config{Retries: 3})
	if _, err := s.Plan(Delivery{Offset: 1, Attempt: 1}, mq.NackOptions{}); !errors.Is(err, ErrNoTopic) {
		t.Fatalf("Plan with no topic = %v, want ErrNoTopic", err)
	}
	// The order-preserving kinds derive no destination, so an empty topic
	// is none of their business and they still plan.
	for _, k := range []Kind{BlockPartition, Stop} {
		if _, err := mustStrategy(t, k, Config{}).Plan(Delivery{Offset: 1, Attempt: 1}, mq.NackOptions{}); err != nil {
			t.Fatalf("%v with no topic = %v", k, err)
		}
	}
}

// TestAttemptZeroIsTheFirstAttempt: the specification makes a zero attempt "the broker does
// not count attempts", so a strategy must not read it as an attempt before the
// first — which would give every record one extra try on every such broker.
func TestAttemptZeroIsTheFirstAttempt(t *testing.T) {
	s := mustStrategy(t, BlockPartition, Config{MaxAttempts: 1, DeadLetterTopic: "orders.dlq"})
	p, err := s.Plan(Delivery{Topic: "orders", Offset: 1, Attempt: 0}, mq.NackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Op != OpDeadLetter {
		t.Fatalf("attempt 0 with MaxAttempts 1 produced %+v; zero is the first attempt, so it is exhausted", p)
	}
}

// TestPlanAttemptIsTheNextDeliverysAttempt pins what Plan.Attempt says: the
// attempt the record carries when it is next delivered, which is one more than
// the delivery's own under OpRetryInPlace and OpRepublish, and zero under the
// rest: OpDeadLetter and OpDrop, after which this subscription does not deliver
// the record again, and OpStop, which leaves it to be delivered again once the
// subscription is reopened (TestOpProperties pins that it does not complete
// the offset). The doc used to say it was zero for every operation that
// publishes nothing, and OpRetryInPlace, which publishes nothing, carries the
// drivers' count of the in-process redelivery in it. It then said OpStop was
// one after which the record is not delivered again.
func TestPlanAttemptIsTheNextDeliverysAttempt(t *testing.T) {
	cfg := Config{MaxAttempts: 4, DeadLetterTopic: "orders.dlq", Retries: 2}
	for _, k := range Kinds() {
		if k.Resolve() == Custom {
			continue
		}
		s := mustStrategy(t, k, cfg)
		for _, topic := range []string{"orders", RetryTopicName("orders", 2)} {
			for attempt := range uint32(6) {
				for _, action := range []mq.NackAction{mq.NackActionRequeue, mq.NackActionDeadLetter, mq.NackActionDrop} {
					d := Delivery{Topic: topic, Offset: 1, Attempt: attempt}
					p, err := s.Plan(d, mq.NackOptions{Action: action})
					if err != nil {
						t.Fatalf("%v.Plan(%+v, %v): %v", k, d, action, err)
					}
					want := uint32(0)
					if p.Op == OpRetryInPlace || p.Op == OpRepublish {
						want = d.attempt() + 1
					}
					if p.Attempt != want {
						t.Errorf("%v.Plan(%+v, %v) = %v with Attempt %d, want %d", k, d, action, p.Op, p.Attempt, want)
					}
				}
			}
		}
	}
}

// TestDelayComesFromTheSettlementFirst: a caller that named a delay gets it,
// and the configured one is the fallback.
func TestDelayComesFromTheSettlementFirst(t *testing.T) {
	s := mustStrategy(t, BlockPartition, Config{Delay: time.Second})
	p, _ := s.Plan(Delivery{Topic: "orders"}, mq.NackOptions{Delay: time.Minute})
	if p.Delay != time.Minute {
		t.Fatalf("delay %v, want the settlement's own minute", p.Delay)
	}
	p, _ = s.Plan(Delivery{Topic: "orders"}, mq.NackOptions{})
	if p.Delay != time.Second {
		t.Fatalf("delay %v, want the configured second", p.Delay)
	}
}

// TestAllocs: planning is on the settlement path, and the order-preserving
// default plans without allocating. The republishing kinds build a topic name,
// which is one allocation and is the price of the strategy rather than a defect.
func TestAllocs(t *testing.T) {
	s := mustStrategy(t, BlockPartition, Config{MaxAttempts: 8})
	d := Delivery{Topic: "orders", Offset: 5, Attempt: 2}
	var sink Plan
	if a := testing.AllocsPerRun(200, func() {
		p, err := s.Plan(d, mq.NackOptions{})
		if err != nil {
			t.Fatal(err)
		}
		sink = p
	}); a != 0 {
		t.Fatalf("blockpartition Plan: %.1f allocs/op", a)
	}
	if sink.Op != OpRetryInPlace {
		t.Fatalf("sink = %+v", sink)
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the nack family
// (front-matter `auto: {nack: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "nack")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds nack yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds nack to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the strategies on the standard library: the root module
// declares no third-party dependency and driverkit is part of it.
func TestLayering(t *testing.T) {
	testguard.StdlibOnly(t, ".")
}
