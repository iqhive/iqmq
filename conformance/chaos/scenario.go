package chaos

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Fault is what a scenario injects. It returns a function that removes the
// fault, which every scenario calls before it decides whether the workload
// resumed.
//
// The signature takes an injector interface rather than the Toxiproxy client
// itself, so that the registry below builds and is tested in `make ci` — this
// package's own tests have no container — and so that a scenario whose fault
// is a container stop rather than a toxic fits the same shape.
type Fault func(ctx context.Context, inj Injector) (remove func(context.Context) error, err error)

// Injector is the part of the chaos environment a fault reaches for.
//
// Latency, Reset, Bandwidth and Disable are Toxiproxy; Stop and Start are the
// container. A driver arm supplies all six; this package's tests supply a
// recording stand-in, which is what makes the registry's shape checkable
// without a broker.
type Injector interface {
	// Latency adds a downstream delay of ms with jitter, and returns the
	// remover.
	Latency(ctx context.Context, ms, jitter int) (func(context.Context) error, error)
	// Reset closes connections with a TCP RST after ms milliseconds.
	Reset(ctx context.Context, ms int) (func(context.Context) error, error)
	// Bandwidth caps the pipe at kbps.
	Bandwidth(ctx context.Context, kbps int) (func(context.Context) error, error)
	// Disable severs the proxy entirely: new connections are refused and
	// existing ones dropped.
	Disable(ctx context.Context) (func(context.Context) error, error)
	// StopBroker stops the broker container and returns the restarter.
	StopBroker(ctx context.Context) (func(context.Context) error, error)
	// DeleteTopology removes the topic or stream the workload is using and
	// returns a function that recreates it.
	DeleteTopology(ctx context.Context) (func(context.Context) error, error)
}

// Phase is when in the workload a fault is injected.
type Phase uint8

// Phase values.
const (
	// DuringPublish injects while the publisher is in a burst.
	DuringPublish Phase = iota + 1
	// DuringSettle injects while the consumer is settling deliveries: the
	// only phase in which an acknowledgement's fate can be made unknown.
	DuringSettle
	// BeforeAll injects before the workload starts, which is what a
	// topology-deletion or an outage-at-open scenario needs.
	BeforeAll
)

// Valid reports whether p is one of the three phases.
func (p Phase) Valid() bool { return p >= DuringPublish && p <= BeforeAll }

// String implements fmt.Stringer.
func (p Phase) String() string {
	switch p {
	case DuringPublish:
		return "during-publish"
	case DuringSettle:
		return "during-settle"
	case BeforeAll:
		return "before-all"
	}
	return "invalid"
}

// Scenario is one layer-3 fault scenario.
type Scenario struct {
	// Name is the subtest name.
	Name string
	// Spec is the specification's own words, so that a
	// reader can tell what the scenario is evidence FOR.
	Spec string
	// Phase is when the fault goes in.
	Phase Phase
	// Extended marks a scenario that belongs to the weekly set rather than
	// the nightly subset (the specification's "nightly subset / weekly extended"). A
	// container stop-start costs half a minute of broker time and is not
	// worth a nightly.
	Extended bool
	// Inject applies the fault.
	Inject Fault
	// Checks is the verdict. Every scenario's first check is FaultBit:
	// a scenario in which nothing failed has tested nothing.
	Checks []Check
	// Messages is how many messages the workload publishes. It is a
	// scenario's own number because a reset mid-burst needs a burst and a
	// topology deletion needs almost nothing.
	Messages int
	// PendingLimit configures the publisher's bounded buffer, for the
	// scenario that checks.
	PendingLimit int
	// ResumeBound is how long the workload may take to make progress again
	// after the fault is removed. Zero means the scenario does not check it.
	ResumeBound time.Duration
}

// Verdict applies the scenario's checks to a run.
func (s Scenario) Verdict(r *Run) error {
	if err := All(r, s.Checks...); err != nil {
		return fmt.Errorf("%s (%s): %w", s.Name, s.Spec, err)
	}
	return nil
}

// Scenarios is the registry: the six layer-3 rows the plan names for this
// increment, in the order a run should take them — cheapest fault first, and
// the two that restart something last, because a scenario that leaves the
// broker in a different state should not be followed by one that assumes it
// is not.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name:  "reset-mid-burst",
			Spec:  "The specification layer-3 reset during publish: duplicate-not-loss",
			Phase: DuringPublish,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				// A reset 200 ms into each connection: long enough that
				// the first publishes of the burst get through and the
				// reset lands mid-burst rather than at open, which is
				// what makes the retry path the thing under test.
				return inj.Reset(ctx, 200)
			},
			Messages: 64,
			Checks: []Check{
				FaultBit(1),
				Accounted(8),
				// Duplicates are permitted and counted; loss is not.
				// The specification makes an accepted publish the broker, so the
				// reset excuses the duplicate and never the absence.
				NoLoss(),
				// A reset the driver never recovers from is the finding
				// this scenario exists to make, so the recovery is
				// JUDGED and not merely recorded: the bound was here as
				// a ResumeBound with no check reading it, and the run
				// then reported "resumed=false" as a pass.
				ResumedWithin(30 * time.Second),
				DrainConverged(),
			},
			ResumeBound: 30 * time.Second,
		},
		{
			Name:  "latency-over-timeout-during-ack",
			Spec:  "The specification layer-3 latency > timeout during ack: SettlementUnknown, then duplicate-not-loss",
			Phase: DuringSettle,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				// The settlement context is bounded well under this, so
				// the acknowledgement's fate is genuinely unknown rather
				// than merely slow.
				return inj.Latency(ctx, 4000, 0)
			},
			Messages: 24,
			Checks: []Check{
				FaultBit(1),
				Accounted(8),
				SettlementUnknownSeen(),
				NoLoss(),
				DrainConverged(),
				ResumedWithin(30 * time.Second),
			},
			ResumeBound: 30 * time.Second,
		},
		{
			Name:  "session-timeout-invalidates-the-generation",
			Spec:  "The specification layer-3 rebalance: a session timeout invalidates the assignment generation",
			Phase: DuringSettle,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				// A black hole rather than a reset: the connection stays
				// open and answers nothing, which is what makes the
				// broker time the session out rather than see a clean
				// disconnect. That is the difference between testing a
				// rebalance and testing a reconnect.
				return inj.Disable(ctx)
			},
			Messages: 24,
			Checks: []Check{
				FaultBit(1),
				Accounted(8),
				NoLoss(),
				// A rebalance the client never completes is not a
				// rebalance. This was the last scenario with a
				// ResumeBound no check read, which is the same defect
				// fixed for reset-mid-burst: the run reported
				// resumed=false and passed.
				ResumedWithin(45 * time.Second),
				DrainConverged(),
			},
			ResumeBound: 45 * time.Second,
		},
		{
			Name:  "bandwidth-bounds-backpressure",
			Spec:  "The specification layer-3 bandwidth: the publisher buffer stays bounded",
			Phase: DuringPublish,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				return inj.Bandwidth(ctx, 8)
			},
			Messages:     64,
			PendingLimit: 16,
			Checks: []Check{
				// No FaultBit: a bandwidth cap is the one fault whose
				// effect is slowness rather than failure, so the
				// evidence that it bit is that the publisher's buffer
				// filled — which PendingBounded reads — and not that
				// something returned an error.
				Accounted(8),
				PendingBounded(),
				NoLoss(),
				ResumedWithin(45 * time.Second),
				DrainConverged(),
			},
			ResumeBound: 45 * time.Second,
		},
		{
			Name:     "container-stop-start-reconnects",
			Spec:     "The specification layer-3 broker restart: bounded reconnect",
			Phase:    DuringSettle,
			Extended: true,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				return inj.StopBroker(ctx)
			},
			Messages: 24,
			Checks: []Check{
				FaultBit(1),
				Accounted(8),
				ResumedWithin(90 * time.Second),
			},
			ResumeBound: 90 * time.Second,
		},
		{
			Name:     "topology-deletion-is-refused-or-degrades",
			Spec:     "The specification layer-3 topology deletion: NotFound or Degraded",
			Phase:    BeforeAll,
			Extended: true,
			Inject: func(ctx context.Context, inj Injector) (func(context.Context) error, error) {
				return inj.DeleteTopology(ctx)
			},
			Messages: 8,
			Checks: []Check{
				DegradedOrCode(mq.CodeNotFound, mq.CodeUnavailable, mq.CodeInvalidArgument),
			},
		},
	}
}

// Select returns the scenarios a run should take: the nightly subset by
// default, and every scenario when extended is true.
func Select(extended bool) []Scenario {
	all := Scenarios()
	if extended {
		return all
	}
	out := make([]Scenario, 0, len(all))
	for _, s := range all {
		if !s.Extended {
			out = append(out, s)
		}
	}
	return out
}

// Names is every scenario's name, sorted.
func Names() []string {
	all := Scenarios()
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// envOr reads an environment variable with a default. It is here rather than
// in the tagged file so that the tagged and untagged halves cannot disagree
// about how EXTENDED is spelled.
func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
