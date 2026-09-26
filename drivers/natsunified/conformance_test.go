package natsunified_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/natsunified"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite through the facade, once per
// profile, against embedded servers.
//
// It is env-gated and it is NOT the shipped evidence: that is
// The conformance suite against the pinned container image,
// because the specification asks what a driver does against the broker it ships against and
// not against a library the test compiled in. This is the form that is fast
// enough to be useful while developing, and it is the one that catches a facade
// defect before a container run does.
//
// It is skipped in the ordinary test run: 155 scenarios per profile each open a
// client, and `make ci` runs the package several times.
func TestConformance(t *testing.T) {
	if os.Getenv("IQMQ_NATSUNIFIED_CONFORMANCE") == "" {
		t.Skip("set IQMQ_NATSUNIFIED_CONFORMANCE=1 to run the suite through the facade against embedded servers; the shipped evidence is the conformance suite")
	}
	for _, profile := range natsunified.Profiles() {
		t.Run(profile.String(), func(t *testing.T) {
			cfg := harnessConfig(t, profile)
			h := natsunified.ConformanceHarness(cfg, testkit.EvidenceInProcess,
				testkit.Broker{Image: "embedded nats-server", Version: "v2.14.6"})
			h.Commit = os.Getenv("IQMQ_COMMIT")
			rep := testkit.RunConformance(t, h)
			pass, fail, skip := rep.Counts()
			t.Logf("conformance (%s): %d passed, %d failed, %d skipped, of %d scenarios", profile, pass, fail, skip, len(rep.Results))
			proven, unproven := 0, 0
			for _, v := range rep.Verify() {
				if v.Failed {
					t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
				}
				if v.Proven() {
					proven++
					continue
				}
				unproven++
				t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s", v.Positive, v.Negative, v.Feature, v.Claimed)
			}
			t.Logf("claims (%s): %d proved in both directions, %d unverified", profile, proven, unproven)
			dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS")
			if dir == "" && t.Failed() {
				dir = filepath.Join(t.TempDir(), "reports")
			}
			if dir == "" {
				return
			}
			path, err := rep.Save(dir)
			if err != nil {
				t.Fatalf("saving the report: %v", err)
			}
			t.Logf("report written to %s", path)
		})
	}
}

// harnessConfig is the facade configuration one profile's conformance run uses.
//
// The JetStream half deliberately names NO stream: the kit's harness replaces
// the stream, the subjects and the whole Need translation, so a stream named
// here would be overwritten and reading one in this file would suggest it
// mattered.
func harnessConfig(tb testing.TB, p natsunified.Profile) natsunified.Config {
	tb.Helper()
	switch p {
	case natsunified.ProfileCore:
		addr := coreURL(tb)
		if env := os.Getenv("IQMQ_NATS_ADDR"); env != "" {
			addr = env
		}
		return natsunified.Config{Profile: p, Core: natsCoreConfig(addr)}
	case natsunified.ProfileJetStream:
		addr := jsURL(tb)
		if env := os.Getenv("IQMQ_NATS_ADDR"); env != "" {
			addr = env
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		return natsunified.Config{Profile: p, JetStream: jetstream.Config{
			Addrs: []string{addr}, AckWait: 5 * time.Second,
		}}
	default:
		tb.Fatalf("no conformance configuration for %v", p)
		return natsunified.Config{}
	}
}
