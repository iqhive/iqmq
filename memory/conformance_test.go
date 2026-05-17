package memory

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// conformanceHarness builds the harness the conformance kit runs this driver
// through (the test suite).
//
// Two things about it are worth stating, because every driver's harness has to
// answer them.
//
// AutoCreate is off, which is what every network driver does and what the specification
// requires of Open: nothing creates production topology behind an operator's
// back. The topology each scenario needs is built here, before the client is
// handed over, which is what makes "no hidden topology creation" a scenario this
// driver can actually fail.
//
// The clock is a TestClock started at the wall clock's current instant. It has
// to be movable, or every lease-expiry and delayed-redelivery scenario would
// really wait; and it has to start at a plausible instant, or a seek to "an hour
// ago" would name a time after every message in the broker.
func conformanceHarness(t testing.TB) testkit.Harness {
	t.Helper()
	var seq atomic.Uint64
	return testkit.Harness{
		Driver:   DriverName,
		Evidence: testkit.EvidenceInProcess,
		Broker:   testkit.Broker{Image: "in-process", Version: Version},
		Client:   testkit.ClientInfo{Module: "bitbucket.org/iqhive/iqmq/internal/engine", Version: Version},
		Timeout:  20 * time.Second,
		// The specification asks for 100,000 interleaved messages. The engine does
		// that in well under a second, so the full figure stands here and
		// a driver whose broker is slower lowers it on its own page.
		Fidelity: 100_000,
		TopicName: func(purpose string) string {
			return "conf." + strconv.FormatUint(seq.Add(1), 36) + "." + sanitise(purpose)
		},
		Open:    openForConformance,
		Advance: advanceFor,
		Quiesce: quiesceFor,
		// The revision the run was made at, which is what lets a
		// reader tell a report produced by the kit committed beside it
		// from one produced four increments ago. `make
		// conformance-memory` sets it; a bare `go test` leaves it
		// empty, which the report records honestly as "the driver did
		// not say" (the rule for every empty field).
		Commit: os.Getenv("IQMQ_COMMIT"),
	}
}

// sanitise turns a scenario name into a portable topic name.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// openForConformance builds a broker with the topology one scenario needs and
// creates the topics before the client exists.
func openForConformance(_ context.Context, n testkit.Need) (*mq.Client, error) {
	topo := TopologyConfig{
		Partitions:       n.Partitions,
		AckDeadline:      n.AckDeadline,
		MaxAttempts:      n.MaxAttempts,
		DeadLetterTopic:  n.DeadLetterTopic,
		Priority:         n.Priority,
		DedupWindow:      n.DedupWindow,
		CapacityMessages: n.Capacity,
		MaxMessageSize:   n.MaxMessageSize,
	}
	if n.Retain {
		// Replay and the two seek positions are conditional on a stream
		// topology with a retention bound, which is what the capability
		// note says and what the scenario therefore has to ask for.
		topo.Kind = TopologyStream
		topo.Retention = Retention{MaxMessages: 4096}
	}
	// The broker's own default topology is the scenario's, minus the two
	// fields that cannot apply to every topic it creates: a dead-letter
	// destination the engine would then require to exist before itself, and
	// the stream profile, which the destination must not have. It matters
	// beyond topic creation, because Capabilities.Limits is built from the
	// broker's default topology — a scenario that asks for a maximum message
	// size has to see the driver advertise one, or the oversized-payload
	// scenario has no limit to exceed and skips.
	base := topo
	base.DeadLetterTopic, base.MaxAttempts = "", 0
	base.Kind, base.Retention = TopologyTopic, Retention{}
	cfg := Config{Clock: NewTestClock(time.Now()), DefaultTopology: base}
	b, err := NewBroker(cfg)
	if err != nil {
		return nil, err
	}
	// The dead-letter destination is created first and with a topology of
	// its own: the engine refuses a topic whose dead-letter destination does
	// not exist, and a destination that dead-letters to itself is a loop.
	if n.DeadLetterTopic != "" {
		if _, err := b.eng.CreateTopic(n.DeadLetterTopic, b.topo); err != nil {
			_ = b.Close()
			return nil, err
		}
	}
	resolved, err := topo.resolve()
	if err != nil {
		_ = b.Close()
		return nil, err
	}
	if _, err := b.eng.CreateTopic(n.Topic, resolved); err != nil {
		_ = b.Close()
		return nil, err
	}
	return mq.NewClient(b), nil
}

// advanceFor moves this client's broker forward deterministically.
//
// It does not wait for the broker to go idle afterwards, and that is not a
// shortcut: Advance runs the sweeps the move implies — time to live, retention
// age and lease expiry — before it returns, and WaitIdle waits for the broker to
// hold no record a consumer could take, which is the opposite of what a scenario
// wants here. A lease that has just expired puts its record back on the ready
// queue, so "advance past the lease and then wait for idle" waits for the very
// redelivery the scenario is about to fetch, for ever.
func advanceFor(_ context.Context, c *mq.Client, d time.Duration) error {
	b, ok := c.Native().(*Broker)
	if !ok {
		return mq.ErrUnsupported
	}
	return b.Advance(d)
}

// quiesceFor has nothing to wait for: this driver's publication is synchronous —
// Publish returns once the record is in every target lane — so there is
// never a publication in flight when a scenario asks.
func quiesceFor(context.Context, *mq.Client) error { return nil }

// TestConformance runs the whole suite against this driver.
func TestConformance(t *testing.T) {
	h := conformanceHarness(t)
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance: %d passed, %d failed, %d skipped, of %d scenarios", pass, fail, skip, len(rep.Results))
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
	}
	if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
		path, err := rep.Save(dir)
		if err != nil {
			t.Fatalf("saving the report: %v", err)
		}
		t.Logf("report written to %s", path)
	}
	if t.Failed() {
		// A failing run is worth keeping whatever the environment says.
		if path, err := rep.Save(filepath.Join(t.TempDir(), "reports")); err == nil {
			t.Logf("the failing run's report is at %s", path)
		}
	}
}
