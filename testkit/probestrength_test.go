package testkit_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// hookedHarness is brokenHarness with the backend built by the test, for the
// defects testkit.Faults has no switch for. open sees the scenario's Need,
// because several of the defects below depend on the topology it asks for.
//
// Its timeout is short because several of these defects show up as a
// delivery that never arrives, and the in-process broker needs no time to
// deliver one that does.
func hookedHarness(open func(ctx context.Context, n testkit.Need) (mq.Backend, error)) testkit.Harness {
	h := brokenHarness(testkit.Faults{})
	h.Timeout = 3 * time.Second
	h.Open = func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
		b, err := open(ctx, n)
		if err != nil {
			return nil, err
		}
		return mq.NewClient(b), nil
	}
	return h
}

// rewired is a backend whose sources a test rewires. Every hook left nil keeps
// the wrapped driver's own behaviour.
type rewired struct {
	mq.Backend
	// open rewrites a source's configuration, or refuses to open it.
	open func(cfg mq.SourceConfig) (mq.SourceConfig, error)
	// fetch rearranges a fetched batch in place.
	fetch func(batch []mq.DriverDelivery)
	// settler replaces a source's settler.
	settler func(mq.Settler) mq.Settler
}

// Admin delegates, so that a harness can provision through the wrapper.
func (b *rewired) Admin() (mq.Admin, error) {
	a, ok := b.Backend.(mq.AdminBackend)
	if !ok {
		return nil, mq.ErrUnsupported
	}
	return a.Admin()
}

// Health delegates.
func (b *rewired) Health() mq.HealthSnapshot {
	if h, ok := b.Backend.(mq.HealthReporter); ok {
		return h.Health()
	}
	return mq.HealthSnapshot{State: mq.HealthReady}
}

// OpenSource applies the hooks.
func (b *rewired) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if b.open != nil {
		var err error
		if cfg, err = b.open(cfg); err != nil {
			return nil, err
		}
	}
	s, err := b.Backend.OpenSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &rewiredSource{Source: s, fetch: b.fetch, settler: b.settler}, nil
}

// rewiredSource is a source with the rewired backend's hooks applied.
type rewiredSource struct {
	mq.Source
	fetch   func(batch []mq.DriverDelivery)
	settler func(mq.Settler) mq.Settler
}

// Fetch delegates and then rearranges the batch.
func (s *rewiredSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n, err := s.Source.Fetch(ctx, dst)
	if s.fetch != nil {
		s.fetch(dst[:n])
	}
	return n, err
}

// Settler returns the replacement settler.
func (s *rewiredSource) Settler() mq.Settler {
	inner := s.Source.Settler()
	if inner == nil || s.settler == nil {
		return inner
	}
	return s.settler(inner)
}

// ignoredExtend is a settler that accepts every lease extension and moves
// nothing: the defect a lease probe that only reads Extend's answer cannot see.
type ignoredExtend struct{ mq.Settler }

// Extend reports success and does nothing.
func (ignoredExtend) Extend(context.Context, *mq.DriverDelivery, time.Duration) error { return nil }

// destroyedReject is a settler whose Reject acknowledges the message instead of
// routing it, so a rejected message is gone.
type destroyedReject struct{ mq.Settler }

// Nack acknowledges a dead-letter settlement and delegates every other one.
func (s destroyedReject) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if o.Action == mq.NackActionDeadLetter {
		return s.Ack(ctx, d)
	}
	return s.Settler.Nack(ctx, d, o)
}

// ignoredStart accepts the start positions in kinds and opens at the latest
// position instead, which is what a driver that does not seek and does not
// refuse does.
func ignoredStart(kinds ...mq.StartKind) func(mq.SourceConfig) (mq.SourceConfig, error) {
	return func(cfg mq.SourceConfig) (mq.SourceConfig, error) {
		if slices.Contains(kinds, cfg.Start.Resolve().Kind()) {
			cfg.Start = mq.StartLatest()
		}
		return cfg, nil
	}
}

// orderedPerKeyOnly regroups a fetched batch by key, keeping each key's own
// order: a driver that orders per key and not across keys.
func orderedPerKeyOnly(batch []mq.DriverDelivery) {
	slices.SortStableFunc(batch, func(a, b mq.DriverDelivery) int {
		return bytes.Compare(a.Message.Key, b.Message.Key)
	})
}

// TestADeadLetterRouteNoProbeSawIsNotVerified is the end-to-end case the
// T1 finding of the second 2026-10-29 bug hunt reproduced: a memory wrapper
// whose Reject destroys the message was published DeadLetter VERIFIED.
//
// Two defects made it. The dead-letter probe skipped on ANY error from opening
// a consumer of the destination, so a destination that could not be read
// turned the one probe that would have seen the message vanish into a skip.
// And Verify took the positive direction from any positive scenario that
// passed, so dlq/failed-publish-never-acks, which checks that Reject is
// refused where there is NO destination, stood in for it.
func TestADeadLetterRouteNoProbeSawIsNotVerified(t *testing.T) {
	scenarios := []string{"capability/DeadLetter/works", "capability/DeadLetter/claim", "dlq/failed-publish-never-acks"}
	for _, tc := range []struct {
		name string
		// refusal is what opening a consumer of the destination answers.
		refusal error
		// probe is the outcome the dead-letter probe has to report.
		probe testkit.Outcome
	}{
		{
			// A destination the driver cannot open for an unexplained
			// reason is a broken driver, not a probe that could not run.
			name:    "a destination that fails to open",
			refusal: errors.New("the dead-letter topic cannot be opened"),
			probe:   testkit.OutcomeFail,
		},
		{
			// A destination the portable API cannot consume is an honest
			// skip, and the claim then has no positive evidence at all.
			name:    "a destination the portable API cannot consume",
			refusal: mq.ErrUnsupported,
			probe:   testkit.OutcomeSkip,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := hookedHarness(func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				if err != nil {
					return nil, err
				}
				w := &rewired{Backend: b}
				if n.DeadLetterTopic != "" {
					w.open = func(cfg mq.SourceConfig) (mq.SourceConfig, error) {
						if cfg.Topic == n.DeadLetterTopic {
							return cfg, tc.refusal
						}
						return cfg, nil
					}
					w.settler = func(s mq.Settler) mq.Settler { return destroyedReject{s} }
				}
				return w, nil
			})
			rep := &testkit.Report{
				Schema: testkit.Schema, Driver: h.Driver, Evidence: h.Evidence,
				Date: time.Now().UTC().Format(time.RFC3339),
			}
			for _, name := range scenarios {
				res, err := testkit.RunOne(t.Context(), h, name)
				if err != nil {
					t.Fatalf("RunOne(%s): %v", name, err)
				}
				t.Logf("%s -> %s: %s", name, res.Outcome, res.Note)
				rep.Results = append(rep.Results, res)
			}
			if got := rep.Results[0].Outcome; got != tc.probe {
				t.Errorf("the dead-letter probe reported %q, want %q: %s", got, tc.probe, rep.Results[0].Note)
			}
			for _, v := range rep.Verify() {
				if v.Feature == mq.FeatureDeadLetter.String() && v.Proven() {
					t.Errorf("DeadLetter is proven (positive %v, negative %v) for a driver whose Reject destroys the message and whose probe never saw a dead letter arrive",
						v.Positive, v.Negative)
				}
			}
		})
	}
}

// TestAProbeFailsTheDefectItIsNamedFor holds four capability probes to the
// standard TestTheKitRejectsADriverItShouldReject holds the rest to: break the
// one thing the probe is about, and it fails; remove the break, and it passes.
//
// Each of the four used to pass with its defect in place (the T2 finding of
// the second 2026-10-29 bug hunt), because it never made the observation its
// feature depends on:
//
//   - ExtendLease read only Extend's answer, so an Extend that moved nothing
//     passed;
//   - the two seek probes published AFTER opening the consumer, so a start
//     position that was accepted and ignored delivered the message anyway;
//   - OrderedGlobally published every message under one key, so a driver
//     that orders per key and not across keys passed;
//   - Replay read its history through a second subscription the scenario's
//     Need named, which a harness that provisions every named subscription
//     creates BEFORE the publication, so a queue that cannot replay at all
//     delivered it as fan-out.
func TestAProbeFailsTheDefectItIsNamedFor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scenario string
		open     func(ctx context.Context, n testkit.Need) (mq.Backend, error)
		wants    string
	}{
		{
			name:     "a lease extension that moves nothing",
			scenario: "capability/ExtendLease/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, settler: func(s mq.Settler) mq.Settler { return ignoredExtend{s} }}, err
			},
			wants: "original deadline",
		},
		{
			name:     "a time start position accepted and ignored",
			scenario: "capability/SeekByTime/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, open: ignoredStart(mq.StartKindTime)}, err
			},
			wants: "not honoured",
		},
		{
			name:     "a sequence start position accepted and ignored",
			scenario: "capability/SeekBySequence/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, open: ignoredStart(mq.StartKindSequence)}, err
			},
			wants: "not honoured",
		},
		{
			name:     "a global order kept only per key",
			scenario: "capability/OrderedGlobally/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, fetch: orderedPerKeyOnly}, err
			},
			wants: "this run observed the guarantee break",
		},
		{
			// The harness provisions every subscription the Need names
			// before the scenario runs, as the gcppubsub, rabbitmq and
			// pulsar harnesses do, and the broker is a queue: a
			// subscription holds what was published after it existed
			// and nothing else, and the head is the only start position.
			name:     "a replay that is fan-out to a subscription provisioned in advance",
			scenario: "capability/Replay/works",
			open: func(ctx context.Context, n testkit.Need) (mq.Backend, error) {
				queue := n
				queue.Retain = false
				b, err := selftestBroker(queue, 0)
				if err != nil {
					return nil, err
				}
				w := &rewired{Backend: b, open: ignoredStart(mq.StartKindEarliest)}
				admin, err := w.Admin()
				if err != nil {
					return nil, err
				}
				if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: n.Topic}); err != nil {
					return nil, err
				}
				for _, sub := range n.Subscriptions {
					if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: n.Topic, Name: sub}); err != nil {
						return nil, err
					}
				}
				return w, nil
			},
			wants: "did something else instead",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broken, err := testkit.RunOne(t.Context(), hookedHarness(tc.open), tc.scenario)
			if err != nil {
				t.Fatalf("RunOne: %v", err)
			}
			t.Logf("%s -> %s: %s", tc.scenario, broken.Outcome, broken.Note)
			if broken.Outcome != testkit.OutcomeFail {
				t.Fatalf("the kit reported %q for a driver with %s; a probe that passes the defect it is named for proves nothing",
					broken.Outcome, tc.name)
			}
			if !strings.Contains(broken.Note, tc.wants) {
				t.Errorf("the failure says %q, want it to mention %q", broken.Note, tc.wants)
			}
			healthy, err := testkit.RunOne(t.Context(), brokenHarness(testkit.Faults{}), tc.scenario)
			if err != nil {
				t.Fatalf("RunOne: %v", err)
			}
			if healthy.Outcome != testkit.OutcomePass {
				t.Fatalf("the unbroken driver reported %q for %s, want pass: %s", healthy.Outcome, tc.scenario, healthy.Note)
			}
		})
	}
}
