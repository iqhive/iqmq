package testkit_test

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
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
	// sink replaces a sink.
	sink func(mq.Sink) mq.Sink
	// admin replaces the backend's Admin.
	admin func(mq.Admin) mq.Admin
	// source wraps a source, for a defect a batch rearranged in place
	// cannot show.
	source func(mq.Source) mq.Source
}

// Admin delegates, so that a harness can provision through the wrapper, and
// applies the admin hook.
func (b *rewired) Admin() (mq.Admin, error) {
	a, ok := b.Backend.(mq.AdminBackend)
	if !ok {
		return nil, mq.ErrUnsupported
	}
	admin, err := a.Admin()
	if err != nil || b.admin == nil {
		return admin, err
	}
	return b.admin(admin), nil
}

// OpenSink applies the sink hook.
func (b *rewired) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	s, err := b.Backend.OpenSink(ctx, cfg)
	if err != nil || b.sink == nil {
		return s, err
	}
	return b.sink(s), nil
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
	if b.source != nil {
		s = b.source(s)
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

// replayedStart accepts the start positions in kinds and opens at the
// earliest retained position instead: a driver that does not seek, does not
// refuse, and replays from the start.
func replayedStart(kinds ...mq.StartKind) func(mq.SourceConfig) (mq.SourceConfig, error) {
	return func(cfg mq.SourceConfig) (mq.SourceConfig, error) {
		if slices.Contains(kinds, cfg.Start.Resolve().Kind()) {
			cfg.Start = mq.StartEarliest()
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

// unordered is a source that gathers everything ready and hands it out last
// first, which a broker with no order across messages, such as Pub/Sub, may do
// on any run. A batch reversed in place is not enough, because the memory
// driver hands a replayed history over one record at a time.
type unordered struct {
	mq.Source
	held []mq.DriverDelivery
}

// Fetch gathers until nothing more arrives for 200ms, then hands the gathered
// records out in reverse.
func (s *unordered) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(s.held) == 0 {
		n, err := s.Source.Fetch(ctx, dst)
		if n == 0 || err != nil {
			return n, err
		}
		s.held = append(s.held, dst[:n]...)
		for {
			more := make([]mq.DriverDelivery, 16)
			qctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			m, _ := s.Source.Fetch(qctx, more)
			cancel()
			if m == 0 {
				break
			}
			s.held = append(s.held, more[:m]...)
		}
		slices.Reverse(s.held)
	}
	n := copy(dst, s.held)
	s.held = s.held[n:]
	return n, nil
}

// reordered wraps a source in unordered.
func reordered(s mq.Source) mq.Source { return &unordered{Source: s} }

// lostWithTTL is a sink that accepts every publication carrying a TTL and
// delivers none of them, which is what a TTL written in the wrong unit, seconds
// where the broker reads milliseconds, does: each one expires at once.
type lostWithTTL struct{ mq.Sink }

// Publish drops a publication with a TTL and delegates every other one.
func (s lostWithTTL) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if o != nil && o.TTL > 0 {
		return nil
	}
	return s.Sink.Publish(ctx, m, o)
}

// oneDedupKey is a sink that sends every dedup key to the broker as the same
// one, a constant or the wrong field, so that every keyed publication after
// the first inside the window is discarded.
type oneDedupKey struct{ mq.Sink }

// Publish replaces a dedup key with the one key.
func (s oneDedupKey) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if o == nil || o.DedupKey == "" {
		return s.Sink.Publish(ctx, m, o)
	}
	c := *o
	c.DedupKey = "every-key-is-this-one"
	return s.Sink.Publish(ctx, m, &c)
}

// dedupByContent is a sink that de-duplicates by the body instead of the key
// the caller named, as an SQS FIFO queue with content-based de-duplication
// does.
type dedupByContent struct{ mq.Sink }

// Publish replaces a dedup key with the body.
func (s dedupByContent) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if o == nil || o.DedupKey == "" {
		return s.Sink.Publish(ctx, m, o)
	}
	c := *o
	c.DedupKey = string(m.Body)
	return s.Sink.Publish(ctx, m, &c)
}

// scaledDelay is a sink that holds a scheduled publication back for factor
// times the delay the caller asked for: a delay in the wrong unit, rounded to
// a broker's minimum or clamped to its maximum.
type scaledDelay struct {
	mq.Sink
	factor float64
}

// Publish scales a publication's delay.
func (s scaledDelay) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if o == nil || o.Delay <= 0 {
		return s.Sink.Publish(ctx, m, o)
	}
	c := *o
	c.Delay = time.Duration(float64(o.Delay) * s.factor)
	return s.Sink.Publish(ctx, m, &c)
}

// scaledNackDelay is scaledDelay for a delayed redelivery.
type scaledNackDelay struct {
	mq.Settler
	factor float64
}

// Nack scales a negative settlement's delay.
func (s scaledNackDelay) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	o.Delay = time.Duration(float64(o.Delay) * s.factor)
	return s.Settler.Nack(ctx, d, o)
}

// scaledDelays rewires b so that both kinds of delay are honoured at factor
// times what was asked.
func scaledDelays(b mq.Backend, factor float64) *rewired {
	return &rewired{
		Backend: b,
		sink:    func(s mq.Sink) mq.Sink { return scaledDelay{s, factor} },
		settler: func(s mq.Settler) mq.Settler { return scaledNackDelay{s, factor} },
	}
}

// unlisted is an Admin whose ListTopics lists nothing, the way one whose pages
// stop early does.
type unlisted struct{ mq.Admin }

// ListTopics lists nothing.
func (unlisted) ListTopics(context.Context) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

// purgeIgnored is an Admin whose Purge returns nil and does nothing, the way
// one that sends the request to the wrong vhost or namespace and takes the
// not-found for success does.
type purgeIgnored struct{ mq.Admin }

// Purge does nothing.
func (purgeIgnored) Purge(context.Context, string, string) error { return nil }

// deleteIgnored is purgeIgnored for DeleteTopic.
type deleteIgnored struct{ mq.Admin }

// DeleteTopic does nothing.
func (deleteIgnored) DeleteTopic(context.Context, string) error { return nil }

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
//
// The fifth bug hunt (2026-11-01) found five more that did not make the
// observation (tt-1, tt-2, tt-3, tt-13 and tt-14): the time seek read one
// delivery, the TTL probe never asked for a message to survive, the
// de-duplication probe never published a second key, the Admin probe read
// nothing back, and the delay probes had no bound on either side worth the
// name.
func TestAProbeFailsTheDefectItIsNamedFor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scenario string
		open     func(ctx context.Context, n testkit.Need) (mq.Backend, error)
		wants    string
		// healthy is the control's driver, when the plain memory driver
		// is not the one that shows the probe accepts the right answer.
		healthy func(ctx context.Context, n testkit.Need) (mq.Backend, error)
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
			// The T2 fix above made both seek probes publish first, which
			// a driver that starts at the END fails. A driver that starts
			// at the BEGINNING still passed both (tt-2 of the fourth bug
			// hunt, 2026-10-31): the time probe sought to before every
			// message, and the sequence probe accepted either of the first
			// two.
			name:     "a time start position replayed from the earliest",
			scenario: "capability/SeekByTime/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, open: replayedStart(mq.StartKindTime)}, err
			},
			wants: "which is a replay and not a seek",
		},
		{
			name:     "a sequence start position replayed from the earliest",
			scenario: "capability/SeekBySequence/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, open: replayedStart(mq.StartKindSequence)}, err
			},
			wants: "which is a replay and not a seek",
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
		{
			// The row above it on a broker with no order across
			// messages. The probe read only the first delivery and
			// accepted either later message first, so a replay passed
			// whenever the message before the position was not the
			// first one out. The control is the same broker seeking
			// correctly, which the probe has to keep accepting.
			name:     "a time start position replayed from the earliest by a broker with no order across messages",
			scenario: "capability/SeekByTime/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, open: replayedStart(mq.StartKindTime), source: reordered}, err
			},
			healthy: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, source: reordered}, err
			},
			wants: "which is a replay and not a seek",
		},
		{
			name:     "a TTL that expires every message at once",
			scenario: "capability/MessageTTL/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, sink: func(s mq.Sink) mq.Sink { return lostWithTTL{s} }}, err
			},
			wants: "the driver lost a message it had to keep until its TTL",
		},
		{
			name:     "dedup keys that all reach the broker as one",
			scenario: "capability/Deduplication/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, sink: func(s mq.Sink) mq.Sink { return oneDedupKey{s} }}, err
			},
			wants: "discarded a publication whose key it had never seen",
		},
		{
			name:     "a de-duplication by content rather than by key",
			scenario: "capability/Deduplication/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, sink: func(s mq.Sink) mq.Sink { return dedupByContent{s} }}, err
			},
			wants: "does not discard the duplicate",
		},
		{
			name:     "a ListTopics that lists nothing",
			scenario: "capability/Admin/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, admin: func(a mq.Admin) mq.Admin { return unlisted{a} }}, err
			},
			wants: "ListTopics never named the topic EnsureTopic had created",
		},
		{
			name:     "a Purge that does nothing",
			scenario: "capability/Admin/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, admin: func(a mq.Admin) mq.Admin { return purgeIgnored{a} }}, err
			},
			wants: "Purge returned nil and the message published before it was delivered after it",
		},
		{
			name:     "a DeleteTopic that does nothing",
			scenario: "capability/Admin/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return &rewired{Backend: b, admin: func(a mq.Admin) mq.Admin { return deleteIgnored{a} }}, err
			},
			wants: "DeleteTopic returned nil and ListTopics still named",
		},
		{
			// On a movable clock, which does not move while the probe
			// waits, the 300ms the probe looked for an early delivery
			// in saw nothing of any delay at all. A fifth of the delay
			// asked for is a unit slip's milder cousin.
			name:     "a scheduled publication held for a fifth of its delay",
			scenario: "capability/ScheduledPublish/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return scaledDelays(b, 0.2), err
			},
			wants: "before half its delay had passed",
		},
		{
			name:     "a delayed redelivery held for a fifth of its delay",
			scenario: "capability/DelayedRedelivery/works",
			open: func(_ context.Context, n testkit.Need) (mq.Backend, error) {
				b, err := selftestBroker(n, 0)
				return scaledDelays(b, 0.2), err
			},
			wants: "before half its delay had passed",
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
			control := brokenHarness(testkit.Faults{})
			if tc.healthy != nil {
				control = hookedHarness(tc.healthy)
			}
			healthy, err := testkit.RunOne(t.Context(), control, tc.scenario)
			if err != nil {
				t.Fatalf("RunOne: %v", err)
			}
			if healthy.Outcome != testkit.OutcomePass {
				t.Fatalf("the unbroken driver reported %q for %s, want pass: %s", healthy.Outcome, tc.scenario, healthy.Note)
			}
		})
	}
}

// TestADelayProbeHoldsADelayToBothBoundsOnAWallClock is the wall-clock half of
// tt-14 of the fifth bug hunt (2026-11-01), which is the clock every container
// suite has. The two delay probes looked for an early delivery for 300ms and
// then gave a late one the scenario's whole timeout, half a minute by default,
// so a two-second delay honoured as 400ms or as twenty seconds passed both.
// The thousandfold rows are the unit slips, milliseconds read as seconds and
// the other way round.
//
// The control is the same wall-clock driver honouring the delay as asked,
// which has to pass: a bound tight enough to fail the right answer is a probe
// that fails every network broker.
func TestADelayProbeHoldsADelayToBothBoundsOnAWallClock(t *testing.T) {
	for _, scenario := range []string{"capability/ScheduledPublish/works", "capability/DelayedRedelivery/works"} {
		for _, tc := range []struct {
			factor float64
			want   testkit.Outcome
			wants  string
		}{
			{factor: 1, want: testkit.OutcomePass},
			{factor: 0.2, want: testkit.OutcomeFail, wants: "before half its delay had passed"},
			{factor: 0.001, want: testkit.OutcomeFail, wants: "before half its delay had passed"},
			{factor: 10, want: testkit.OutcomeFail, wants: "more than 9.3s after its delay had passed, or not at all"},
			{factor: 1000, want: testkit.OutcomeFail, wants: "more than 9.3s after its delay had passed, or not at all"},
		} {
			t.Run(scenario+"/x"+strconv.FormatFloat(tc.factor, 'g', -1, 64), func(t *testing.T) {
				t.Parallel()
				h := hookedHarness(func(_ context.Context, n testkit.Need) (mq.Backend, error) {
					b, err := memory.NewBroker(memory.Config{
						AutoCreate:      true,
						DefaultTopology: memory.TopologyConfig{AckDeadline: n.AckDeadline},
					})
					return scaledDelays(b, tc.factor), err
				})
				h.Advance = nil
				h.Timeout = testkit.DefaultTimeout
				start := time.Now()
				res, err := testkit.RunOne(t.Context(), h, scenario)
				if err != nil {
					t.Fatalf("RunOne: %v", err)
				}
				t.Logf("%s -> %s after %v: %s", scenario, res.Outcome, time.Since(start).Round(100*time.Millisecond), res.Note)
				if res.Outcome != tc.want {
					t.Fatalf("the kit reported %q for a two-second delay honoured at %v times, want %q", res.Outcome, tc.factor, tc.want)
				}
				if !strings.Contains(res.Note, tc.wants) {
					t.Errorf("the result says %q, want it to mention %q", res.Note, tc.wants)
				}
			})
		}
	}
}
