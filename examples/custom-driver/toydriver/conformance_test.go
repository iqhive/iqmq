package toydriver_test

// This file is in the EXTERNAL test package on purpose.
//
// A driver's conformance harness has to build the topology each scenario asks
// for before the client exists, and the question the specification is about is whether a
// third party can write that harness against the published API. The memory
// driver's own harness cannot answer it: openForConformance there reaches
// b.eng.CreateTopic and topo.resolve(), which are unexported, so it proves
// only that the driver's own package can do it. `package toydriver_test` can
// reach nothing but the exported surface, so if this file compiles, a driver
// author outside this repository can write the same thing.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/examples/custom-driver/toydriver"
	"github.com/iqhive/iqmq/testkit"
)

// credential is the secret the broker is configured with, so that the specification's
// redaction scenario has something to look for. A harness that declares no
// secrets SKIPS that scenario rather than passing it, because a check with
// nothing to find proves nothing.
const credential = "toy-broker-p4ssw0rd-not-a-real-secret"

// harness builds what the conformance kit runs this driver through.
func harness() testkit.Harness {
	var seq atomic.Uint64
	return testkit.Harness{
		Driver:   toydriver.DriverName,
		Evidence: testkit.EvidenceInProcess,
		Broker:   testkit.Broker{Image: "in-process", Version: toydriver.Version},
		Client: testkit.ClientInfo{
			Module:  "github.com/iqhive/iqmq/examples/custom-driver/toydriver",
			Version: toydriver.Version,
		},
		Timeout: 20 * time.Second,
		// The specification asks for 100,000 interleaved messages and the harness
		// documentation invites a driver whose broker is slow enough for
		// that to dominate to lower it and say so. This broker holds every
		// message in one slice under one mutex and clones it per
		// subscription, so 100,000 is about a second of copying that says
		// nothing 5,000 does not.
		Fidelity: 5_000,
		Secrets:  []string{credential},
		TopicName: func(purpose string) string {
			return "conf." + strconv.FormatUint(seq.Add(1), 36) + "." + sanitise(purpose)
		},
		Open:    open,
		Advance: advance,
		// The revision the run was made at. A report with no
		// provenance cannot be read against the kit that produced it.
		Commit: os.Getenv("IQMQ_COMMIT"),
	}
}

// sanitise turns a scenario name into a name the portable topic grammar
// accepts.
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

// open builds a broker with the topology one scenario needs and creates the
// topics before the client exists.
//
// Need.Retain is deliberately NOT refused, and that is the one judgement call
// in this file. This broker retains nothing, so a strict reading of Need says
// Open should answer ErrUnsupported and let the scenario skip. It is accepted
// instead, because the topology a retaining broker would build is the topology
// this one already has — there is no retention to switch on — and the
// operations retention is actually needed FOR are refused where a caller can
// act on them, at OpenSource. Refusing at Open costs four scenarios their
// answer (start/positions plus the three seek and replay refusals); accepting
// and refusing the start positions verifies all four.
func open(_ context.Context, n testkit.Need) (*mq.Client, error) {
	cfg := toydriver.Config{
		MaxMessageBytes: n.MaxMessageSize,
		AckDeadline:     n.AckDeadline,
		Credential:      credential,
		// A fixed start instant, so that two runs of the same scenario see
		// the same timestamps. This broker refuses StartAt, so the
		// seek-by-time probe never reads its clock. A driver that copies
		// this harness and DOES seek by time has to start the clock at
		// time.Now() instead: the probe names its position from the wall
		// clock and needs the broker's clock within an hour of it
		// (testkit.Harness.Advance).
		Start: time.Date(2026, time.September, 13, 0, 0, 0, 0, time.UTC),
	}
	if n.DeadLetterTopic != "" {
		// Declared first: a topic that names a dead-letter destination is
		// refused when the destination does not exist yet, which is the
		// driver refusing to discover a misconfiguration at the moment a
		// poison message is in a handler's hands.
		cfg.Topics = append(cfg.Topics, toydriver.TopicSpec{
			Name:     n.DeadLetterTopic,
			Capacity: n.Capacity,
		})
	}
	cfg.Topics = append(cfg.Topics, toydriver.TopicSpec{
		Name: n.Topic,
		// Only the scenario's own subscriptions are pre-created. Every
		// other one is created by opening a consumer, which is what
		// start/positions and the replay refusal both depend on: a
		// subscription that did not exist when a message was published
		// must not receive it.
		Subscriptions:   n.Subscriptions,
		DeadLetterTopic: n.DeadLetterTopic,
		AckDeadline:     n.AckDeadline,
		Capacity:        n.Capacity,
	})
	return toydriver.Open(cfg)
}

// advance moves this client's broker forward deterministically.
//
// It reaches the broker through Client.Native, which is the escape hatch
// and the only route a harness in an external package has. A driver whose
// Native returned nil could not offer Advance at all, and its conformance run
// would really wait for every lease.
func advance(_ context.Context, c *mq.Client, d time.Duration) error {
	b, ok := c.Native().(*toydriver.Broker)
	if !ok {
		return mq.ErrUnsupported
	}
	return b.Advance(d)
}

// TestConformance runs the whole suite against the toy driver.
//
// Set IQMQ_CONFORMANCE_REPORTS to a directory to write the report, which is
// what conformance/cmd/confgen turns into a capability matrix.
func TestConformance(t *testing.T) {
	h := harness()
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
}
