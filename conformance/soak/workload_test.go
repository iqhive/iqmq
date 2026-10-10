package soak

import (
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// memClient is a memory client the workload can be soaked against, which is
// what makes the workload itself testable in `make ci`: a run loop nobody has
// exercised outside a weekly job is a run loop nobody has exercised.
func memClient(t *testing.T, ackDeadline time.Duration) *mq.Client {
	t.Helper()
	b, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		DefaultTopology: memory.TopologyConfig{
			Partitions:  1,
			AckDeadline: ackDeadline,
			MaxAttempts: 0,
		},
	})
	if err != nil {
		t.Fatalf("building a broker: %v", err)
	}
	client := mq.NewClient(b)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestWorkloadRunsEveryLimbOfTheTier exercises the run loop briefly and
// requires each of the three limbs to have actually happened.
//
// The assertions are on the COUNTERS and not on "it did not crash": a soak
// whose reconnect interval never elapsed, or whose lease never expired, has
// run a publish-and-ack loop under a different name, and the counters are the
// only way to tell from the outside.
func TestWorkloadRunsEveryLimbOfTheTier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	diff, st, err := Run(ctx, Config{
		Client:         memClient(t, 300*time.Millisecond),
		Topic:          "soak-limbs",
		Duration:       5 * time.Second,
		Batch:          8,
		ReconnectEvery: time.Second,
		ExpireEvery:    700 * time.Millisecond,
		Series:         Series{MinDuration: time.Second, MinSamples: 3},
		Log:            t.Logf,
	})
	if err != nil {
		t.Fatalf("running the soak: %v", err)
	}
	t.Logf("%s | %s", diff, st)
	switch {
	case st.Cycles < 2:
		t.Errorf("the workload ran %d cycle(s)", st.Cycles)
	case st.Published == 0:
		t.Error("nothing was published")
	case st.Settled == 0:
		t.Error("nothing was settled")
	case st.Expired == 0:
		t.Error("no delivery was left to expire, so the lease-expiry limb never ran")
	case st.Reconnects == 0:
		t.Error("the handles were never reopened, so the reconnect-storm limb never ran")
	}
	if diff.Progress <= 0 {
		t.Errorf("the profile's progress counter did not advance: %s", diff)
	}
	if len(diff.Reasons) == 0 {
		t.Error("the diff gave no reason for its grade")
	}
}

// TestAShortRunIsInconclusive is the honesty check at the workload level: the
// default minimum duration is ten minutes, so a three-second soak against a
// perfectly clean engine must not report a pass.
func TestAShortRunIsInconclusive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	diff, _, err := Run(ctx, Config{
		Client:   memClient(t, time.Minute),
		Topic:    "soak-short",
		Duration: 3 * time.Second,
		Batch:    8,
		Log:      t.Logf,
	})
	if err != nil {
		t.Fatalf("running the soak: %v", err)
	}
	if diff.Grade != GradeInconclusive {
		t.Fatalf("a three-second soak graded %s, want inconclusive: %s", diff.Grade, diff)
	}
	if !strings.Contains(strings.Join(diff.Reasons, " "), "a conclusion needs 10m0s") {
		t.Errorf("the reason does not name the minimum: %s", diff)
	}
	t.Logf("%s", diff)
}

// TestRunRefusesAClientItCannotUse covers the one case that is a run failure
// rather than a verdict: a topology the client cannot open at all.
func TestRunRefusesAClientItCannotUse(t *testing.T) {
	b, err := memory.NewBroker(memory.Config{}) // AutoCreate off, no topics
	if err != nil {
		t.Fatalf("building a broker: %v", err)
	}
	client := mq.NewClient(b)
	defer func() { _ = client.Close() }()
	if _, _, err := Run(context.Background(), Config{
		Client: client, Topic: "never-created", Duration: time.Second, Batch: 1,
	}); err == nil {
		t.Fatal("a soak against a topic that does not exist returned no error")
	}
}

// TestRunStopsWhenTheContextIsCancelled keeps a soak from outliving the job
// that started it: the weekly workflow bounds it with a timeout, and a run
// loop that ignored the context would be killed mid-cycle with no diff.
func TestRunStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	diff, st, err := Run(ctx, Config{
		Client:   memClient(t, time.Minute),
		Topic:    "soak-cancel",
		Duration: 10 * time.Minute,
		Batch:    8,
		Series:   Series{MinDuration: time.Second, MinSamples: 2},
	})
	if err != nil {
		t.Fatalf("running the soak: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("a cancelled ten-minute soak took %v to return", elapsed)
	}
	if st.Cycles == 0 {
		t.Error("the run returned before it did anything")
	}
	t.Logf("%s | %s", diff, st)
}
