package pulsar_test

import (
	"os"
	"testing"
	"time"

	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the suite against a Pulsar named by
// IQMQ_PULSAR_ADDR, once per subscription type named by
// IQMQ_PULSAR_PROFILES.
//
// It is the dev-loop half. The shipped evidence — the reports the generated
// capability matrix is built from — comes from conformance/suites/pulsar_test.go,
// which starts the pinned image itself; both run the same
// [pd.ConformanceHarness], so the two cannot drift.
//
// Two environment variables, because they answer different questions:
// IQMQ_PULSAR_CONFORMANCE says whether to run at all (the suite takes minutes
// and creates a topic per scenario), and IQMQ_PULSAR_ADDR says where.
func TestConformance(t *testing.T) {
	if os.Getenv("IQMQ_PULSAR_CONFORMANCE") == "" {
		t.Skip("IQMQ_PULSAR_CONFORMANCE is not set; the suite creates a topic per scenario")
	}
	addr := os.Getenv("IQMQ_PULSAR_ADDR")
	if addr == "" {
		t.Skip("IQMQ_PULSAR_ADDR is not set; the specification needs a broker and an in-process model is not one")
	}
	for _, kind := range pd.ConformanceProfiles() {
		t.Run(kind.String(), func(t *testing.T) {
			cfg := pd.Config{
				ServiceURL:       "pulsar://" + addr,
				AdminURL:         os.Getenv("IQMQ_PULSAR_ADMIN_URL"),
				SubscriptionType: kind,
				Token:            dsn.Secret(os.Getenv("IQMQ_PULSAR_TOKEN")),
				OperationTimeout: 30 * time.Second,
			}
			h := pd.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{
				Image:   os.Getenv("IQMQ_PULSAR_IMAGE"),
				Version: os.Getenv("IQMQ_PULSAR_VERSION"),
			})
			h.Commit = os.Getenv("IQMQ_COMMIT")
			rep := testkit.RunConformance(t, h)
			reportSummary(t, rep)
			if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
				path, err := rep.Save(dir)
				if err != nil {
					t.Fatalf("saving the report: %v", err)
				}
				t.Logf("report: %s", path)
			}
		})
	}
}

// reportSummary logs what the run proved, in the shape every driver's suite
// logs it: the counts, the claims proved in both directions, the denials one run
// cannot refute, and the claims nobody could check.
func reportSummary(t *testing.T, rep *testkit.Report) {
	t.Helper()
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped, of %d scenarios",
		rep.Key(), pass, fail, skip, len(rep.Results))
	proven := 0
	for _, v := range rep.Verify() {
		switch {
		case v.Failed:
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		case v.Proven():
			proven++
		case v.Unrefuted():
			// A true denial of a GUARANTEE that nothing in this run
			// broke. It is not work outstanding: no number of runs
			// can observe a guarantee's absence.
			t.Logf("claim UNREFUTED: %-22s %s (a guarantee's absence is not observable in one run)",
				v.Feature, v.Claimed)
		default:
			t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
				v.Positive, v.Negative, v.Feature, v.Claimed)
		}
	}
	t.Logf("capability claims proved in both directions: %d of %d", proven, len(rep.Verify()))
}

// TestTheHarnessConfiguresWhatTheSuiteNeeds is what the conformance run depends
// on and what nobody would notice going wrong.
//
// The kit builds every publisher and consumer at the zero metadata mode, which
// on this broker means "refuse a binary key and refuse an empty key". The specification makes
// the envelope this broker's portable-metadata story for exactly those two, and the specification makes it an emulation a deployment asks for once — so the harness is
// where the ask lives, and if it stopped asking the suite would still be green
// with `publish/key-roundtrip` failing for a reason that reads as a driver
// defect.
func TestTheHarnessConfiguresWhatTheSuiteNeeds(t *testing.T) {
	for _, kind := range pd.ConformanceProfiles() {
		t.Run(kind.String(), func(t *testing.T) {
			cfg := pd.Config{ServiceURL: "pulsar://127.0.0.1:6650", AdminURL: "http://127.0.0.1:8080", SubscriptionType: kind}
			h := pd.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: "apachepulsar/pulsar:4.2.4"})
			if h.Driver != "pulsar" || h.Evidence != testkit.EvidenceContainer {
				t.Fatalf("the harness names driver %q with evidence %q", h.Driver, h.Evidence)
			}
			if h.Profile != kind.String() {
				t.Fatalf("the harness reports profile %q, want %q: the subscription type IS the "+
					"profile, because it decides every ordering claim", h.Profile, kind)
			}
			if h.Open == nil || h.Wildcard == nil || h.Quiesce == nil {
				t.Fatal("the harness leaves Open, Wildcard or Quiesce nil; a nil Wildcard skips both " +
					"wildcard scenarios and leaves the claim unverified in BOTH directions")
			}
			if _, skipped := h.Skip["metadata/native-strict-rejection"]; !skipped {
				t.Error("metadata/native-strict-rejection is not documented as inapplicable, but the " +
					"harness configures a metadata mode that puts no handle in strict mode")
			}
			if _, skipped := h.Skip["secrets/redaction"]; !skipped {
				t.Error("secrets/redaction is not documented as inapplicable and this harness declares " +
					"no credential, so the scenario would pass vacuously")
			}
			// Every name the harness produces has to be a legal Pulsar
			// topic, or the scenario fails at open with a message
			// about a topic name.
			for _, purpose := range []string{
				"publish/roundtrip-bodies", "capability/ExactlyOnceDelivery/refused",
				"lifecycle/graceful-shutdown-drains",
				"very-long-scenario-name/very-long-scenario-name/very-long-scenario-name/" +
					"very-long-scenario-name/very-long-scenario-name/very-long-scenario-name/" +
					"very-long-scenario-name/very-long-scenario-name/very-long-scenario-name",
			} {
				name := h.TopicName(purpose)
				if _, err := pd.TopicOf(cfg, name); err != nil {
					t.Errorf("the harness produced %q for %q, which Pulsar refuses: %v", name, purpose, err)
				}
			}
			if a, b := h.TopicName("x"), h.TopicName("x"); a == b {
				t.Errorf("two calls produced the same topic %q", a)
			}
			// And the harness's wildcard is a pattern for the topic it
			// is given and for nothing else in the run.
			topic := h.TopicName("wildcards-filters")
			other := h.TopicName("wildcards-unmatched")
			pat := h.Wildcard(topic)
			if !pd.HasPattern(pat) {
				t.Errorf("the harness's wildcard %q is not a pattern", pat)
			}
			if len(other) >= len(topic) && other[:len(topic)] == topic {
				t.Errorf("the unmatched topic %q begins with the matched one %q, so the pattern would "+
					"match both", other, topic)
			}
		})
	}
}

// TestConformanceProfilesAreTheShippedEvidence states which reports this driver
// publishes, so that a change to the list is a change to conformance/suites.json
// too.
func TestConformanceProfilesAreTheShippedEvidence(t *testing.T) {
	got := pd.ConformanceProfiles()
	want := []pd.SubscriptionKind{pd.SubscriptionShared, pd.SubscriptionKeyShared}
	if len(got) != len(want) {
		t.Fatalf("the driver ships %d conformance profiles, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("profile %d is %v, want %v", i, got[i], want[i])
		}
	}
}
