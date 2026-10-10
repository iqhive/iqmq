//go:build soak

package soak

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/testkit"
)

// The layer-4 runs. `make soak DRIVER=<suite> TIME=<duration>` selects one
// target and passes the duration through IQMQ_SOAK_DURATION.
//
// TIME is the run's own length and the verdict states it. Below
// Series.MinDuration the grade is "inconclusive" rather than "pass": a soak
// that reports no leak after four minutes has not tested a leak slower than
// four minutes, and reporting such a run as green is the one thing this tier
// must not do.
//
// ONE broker at a time on this host.

// soakDuration is TIME, with a default short enough to be useful on a laptop
// and long enough that the run says something: it is the MinDuration, so the
// default run is exactly at the threshold a conclusion needs.
func soakDuration(tb testing.TB) time.Duration {
	tb.Helper()
	d, err := ParseDuration(os.Getenv("IQMQ_SOAK_DURATION"), "10m")
	if err != nil {
		tb.Fatalf("%v", err)
	}
	return d
}

// reportDir is where the profile diff is written, which the soak workflow
// uploads. IQMQ_SOAK_REPORTS names it; an empty value writes nothing, so a
// local run leaves no file behind.
func reportDir() string { return os.Getenv("IQMQ_SOAK_REPORTS") }

// save writes the diff and the tally as JSON beside the run, so that the
// weekly job's artefact is a number a later run can be read against rather
// than a wall of log lines.
func save(tb testing.TB, name string, d Diff, st Stats) {
	tb.Helper()
	dir := reportDir()
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		tb.Errorf("creating %s: %v", dir, err)
		return
	}
	out := struct {
		Suite    string    `json:"suite"`
		At       time.Time `json:"at"`
		Commit   string    `json:"commit"`
		Grade    string    `json:"grade"`
		Duration string    `json:"duration"`
		Diff     Diff      `json:"diff"`
		Stats    string    `json:"stats"`
	}{
		Suite: name, At: time.Now().UTC(), Commit: os.Getenv("IQMQ_COMMIT"),
		Grade: d.Grade.String(), Duration: d.Duration.String(), Diff: d, Stats: st.String(),
	}
	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		tb.Errorf("encoding the soak report: %v", err)
		return
	}
	path := filepath.Join(dir, name+".json")
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		tb.Errorf("writing %s: %v", path, err)
		return
	}
	tb.Logf("soak report written to %s", path)
}

// runSoak is the body of every TestSoak_<suite>.
func runSoak(t *testing.T, name string, client *mq.Client, topic string) {
	d := soakDuration(t)
	// The context is the run's duration plus slack for the final drain and
	// the last sample; the run stops itself at the duration.
	ctx, cancel := context.WithTimeout(context.Background(), d+3*time.Minute)
	defer cancel()
	t.Logf("soak %s for %v (IQMQ_SOAK_DURATION)", name, d)
	diff, st, err := Run(ctx, Config{
		Client:       client,
		Topic:        topic,
		Subscription: "soak",
		Duration:     d,
		Batch:        32,
		// the reconnect storm and lease expiry, scaled to the run:
		// a ten-minute soak reconnects about twenty times and leaves
		// about forty leases to expire, which is enough for a leak in
		// either path to show against the thresholds.
		ReconnectEvery: d / 20,
		ExpireEvery:    d / 40,
		Log:            t.Logf,
	})
	if err != nil {
		t.Fatalf("running the soak: %v", err)
	}
	t.Logf("SOAK GRADE %s\n%s\n%s", diff.Grade, diff, st)
	save(t, name, diff, st)
	if err := diff.Err(); err != nil {
		t.Fatalf("%v", err)
	}
	if diff.Grade == GradeInconclusive {
		// Not a failure, and not silent either. The grade is in the log
		// above and in the artefact; this line is what a reader of a
		// green job sees.
		t.Logf("NOTE: this run is INCONCLUSIVE, not clean. %s", diff.Reasons[0])
	}
	if st.Errors > 0 {
		t.Logf("the workload saw %d error(s), last: %v", st.Errors, st.Last)
	}
}

// TestSoak_memory soaks the in-process engine. It is the arm that can run for
// hours on any host, and the one whose leaks are this repository's own rather
// than a client library's.
func TestSoak_memory(t *testing.T) {
	b, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		DefaultTopology: memory.TopologyConfig{
			Partitions: 4,
			// Short enough that the expiry limb really expires, long
			// enough that an ordinary cycle is not fighting it.
			AckDeadline: 2 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("building a broker: %v", err)
	}
	client := mq.NewClient(b)
	defer func() { _ = client.Close() }()
	runSoak(t, "memory", client, "soak.memory")
}

// TestSoak_jetstream soaks the JetStream driver against the pinned container.
func TestSoak_jetstream(t *testing.T) {
	b := harness.NATS(t)
	h := jq.ConformanceHarness(
		jq.Config{Addrs: []string{"nats://" + b.Addr}},
		testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version},
	)
	topic := h.TopicName("soak")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	client, err := h.Open(ctx, testkit.Need{
		Topic:         topic,
		Subscriptions: []string{"soak"},
		Partitions:    1,
		AckDeadline:   5 * time.Second,
	})
	cancel()
	if err != nil {
		t.Fatalf("opening a client: %v", err)
	}
	defer func() { _ = client.Close() }()
	runSoak(t, "jetstream", client, topic)
}
