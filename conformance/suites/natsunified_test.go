//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	nq "github.com/iqhive/iqmq/drivers/nats"
	uq "github.com/iqhive/iqmq/drivers/natsunified"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_natsunified runs the whole suite through the
// drivers/natsunified facade, ONCE PER PROFILE, against the pinned NATS
// container, and writes two reports: natsunified-core.json and
// natsunified-jetstream.json.
//
// Why a facade needs its own conformance evidence at all is the question
// exists to answer, and the answer is that a facade can lose a claim. The two
// reports are therefore read against the two the profile drivers produce
// themselves — nats-core.json and jetstream.json — scenario for scenario. A
// facade that dropped a documented skip would report a failure the broker cannot
// avoid; one that reset the fidelity figure would run a different scenario
// under the same name; one that merged the capability tables would change which
// scenarios the capability gates select at all. None of those is visible in the
// facade's own tests, because every one of them is still internally consistent.
//
// Both runs go through [uq.ConformanceHarness], whose Open is the facade's, so
// what these reports certify is the facade's entry point and not the drivers
// underneath it.
func TestConformance_natsunified(t *testing.T) {
	// goleak is on the DRIVER: a subscription goroutine, a parked Push, a
	// flusher or a consume loop that outlives Close fails this. The ignores
	// are the test harness's own — testcontainers keeps a connection to its
	// reaper for the life of the process, and net/http keeps its idle
	// connections — and none of them is anything either driver started.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	// One container for both profiles. It is started with JetStream enabled
	// because one image serves both, and the Core profile never touches it:
	// that is the whole of ADR-010, and a facade that quietly used a
	// stream on the Core profile would undo it — which is what the two
	// reports, read against nats-core.json, would show.
	b := harness.NATS(t)
	broker := testkit.Broker{Image: b.Image, Version: b.Version}

	for _, profile := range uq.Profiles() {
		t.Run(profile.String(), func(t *testing.T) {
			h := uq.ConformanceHarness(configFor(profile, b.Addr), testkit.EvidenceContainer, broker)
			h.Commit = os.Getenv("IQMQ_COMMIT")
			rep := testkit.RunConformance(t, h)
			pass, fail, skip := rep.Counts()
			t.Logf("conformance (%s): %d passed, %d failed, %d skipped, of %d scenarios",
				profile, pass, fail, skip, len(rep.Results))

			proven := 0
			for _, v := range rep.Verify() {
				if v.Failed {
					t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
				}
				if v.Proven() {
					proven++
					continue
				}
				t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s", v.Positive, v.Negative, v.Feature, v.Claimed)
			}
			t.Logf("capability claims proved in both directions (%s): %d of %d", profile, proven, len(rep.Verify()))

			dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS")
			if dir == "" {
				dir = os.Getenv("IQMQ_CONFORMANCE_REPORT_DIR")
			}
			if dir == "" {
				dir = t.TempDir()
			}
			path, err := rep.Save(dir)
			if err != nil {
				t.Fatalf("saving the report: %v", err)
			}
			t.Logf("report written to %s", path)
		})
	}
}

// configFor is the facade configuration one profile's run uses.
//
// It fills in the connection and nothing else. The kit's harness — which is the
// profile driver's own, with the facade's Open substituted — replaces the
// stream, the subject tree and the whole Need translation, so a stream named
// here would be overwritten and reading one in this file would suggest it
// mattered.
func configFor(p uq.Profile, addr string) uq.Config {
	switch p {
	case uq.ProfileCore:
		return uq.Config{Profile: p, Core: nq.Config{Addrs: []string{"nats://" + addr}}}
	case uq.ProfileJetStream:
		return uq.Config{Profile: p, JetStream: jq.Config{
			Addrs: []string{"nats://" + addr}, AckWait: 5 * time.Second,
		}}
	default:
		return uq.Config{}
	}
}
