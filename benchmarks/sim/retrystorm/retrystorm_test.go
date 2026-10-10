package retrystorm

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/backoff"
)

// smallParams is the configuration the agreement test uses: small enough
// for 500 goroutines in a bubble, large enough for the policies to differ.
func smallParams(k backoff.Kind) Params {
	return Params{
		Kind:           k,
		Config:         backoff.Config{Base: 100 * time.Millisecond, Max: 5 * time.Second, Growth: 2},
		Clients:        500,
		Outage:         3 * time.Second,
		FailLatency:    time.Millisecond,
		ServiceLatency: 5 * time.Millisecond,
		MaxAttempts:    200,
		Seed:           7,
	}
}

// TestBubbleMatchesEvents is the oracle for the fast simulation: the same
// experiment run with one goroutine per client under synctest must produce
// the same metrics as the discrete-event simulation the benchmarks use.
func TestBubbleMatchesEvents(t *testing.T) {
	for _, k := range backoff.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			p := smallParams(k)
			want := RunEvents(p)
			var got Result
			synctest.Test(t, func(t *testing.T) {
				got = RunBubble(t, p)
			})
			if got.Attempts != want.Attempts {
				t.Errorf("attempts: bubble %d, events %d", got.Attempts, want.Attempts)
			}
			if got.Failed != want.Failed {
				t.Errorf("failed: bubble %d, events %d", got.Failed, want.Failed)
			}
			if got.Recover != want.Recover {
				t.Errorf("recover: bubble %v, events %v", got.Recover, want.Recover)
			}
			if got.PeakConcurrency != want.PeakConcurrency {
				t.Errorf("peak concurrency: bubble %d, events %d", got.PeakConcurrency, want.PeakConcurrency)
			}
			if got.PeakAfterOutage != want.PeakAfterOutage {
				t.Errorf("peak after outage: bubble %d, events %d", got.PeakAfterOutage, want.PeakAfterOutage)
			}
			t.Logf("%v: peak=%d peak-after=%d recover=%v attempts=%d",
				k, got.PeakConcurrency, got.PeakAfterOutage, got.Recover, got.Attempts)
		})
	}
}

// stormParams is the experiment for one policy: 10 000 clients and
// a 30-second outage. A policy that never waits sends one request per
// millisecond per client, so it is given a smaller attempt budget — it
// exhausts any budget long before a 30-second outage ends, which is the
// finding, not an artefact.
func stormParams(k backoff.Kind) Params {
	p := Params{Kind: k, MaxAttempts: 2000}
	if k == backoff.None {
		p.MaxAttempts = 200
	}
	return p
}

// TestStorm runs the experiment — 10 000 clients through a
// 30-second outage — for every policy and records the metrics the ADR
// needs. It also holds the ordering the specification predicts: an
// un-jittered policy retries in lockstep, so its recovery wave is as tall
// as the client population, while jitter spreads it out.
func TestStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("the 10 000-client storm is not a short test")
	}
	results := map[backoff.Kind]Result{}
	for _, k := range backoff.Kinds() {
		p := stormParams(k)
		r := RunEvents(p)
		results[k] = r
		t.Logf("%-13s peak=%-6d peak-concurrency=%-6d recover-s=%-8.3f attempts=%-9d failed=%d (max-attempts=%d)",
			k, r.PeakConcurrency, r.PeakAfterOutage, r.Recover.Seconds(), r.Attempts, r.Failed, p.MaxAttempts)
		if r.PeakConcurrency != p.withDefaults().Clients {
			t.Errorf("%v: peak %d, want every client in flight at the start", k, r.PeakConcurrency)
		}
		if r.Recover < 0 {
			t.Errorf("%v: negative recovery time %v", k, r.Recover)
		}
		if k == backoff.None {
			// Without a delay every client burns its whole budget while
			// the broker is down and never reaches the recovery.
			if r.Failed != p.withDefaults().Clients {
				t.Errorf("no backoff: %d clients survived a 30s outage", p.withDefaults().Clients-r.Failed)
			}
			continue
		}
		if r.Failed != 0 {
			t.Errorf("%v: %d clients never recovered", k, r.Failed)
		}
	}
	// Jitter lowers the recovery wave: the whole point of the family.
	for _, k := range []backoff.Kind{backoff.Full, backoff.Equal, backoff.Decorrelated} {
		if results[k].PeakAfterOutage >= results[backoff.Constant].PeakAfterOutage {
			t.Errorf("%v peak %d is not below constant backoff's %d",
				k, results[k].PeakAfterOutage, results[backoff.Constant].PeakAfterOutage)
		}
	}
	// A policy without delay hammers a broker that is down: it sends more
	// requests with a tenth of the attempt budget than any other policy
	// sends in total.
	if results[backoff.None].Attempts <= results[backoff.Constant].Attempts {
		t.Errorf("no backoff sent %d attempts, constant %d",
			results[backoff.None].Attempts, results[backoff.Constant].Attempts)
	}
}

// TestBubbleStorm runs the full 10 000-client experiment for one policy in
// a synctest bubble, so the headline number is produced by real
// goroutines at least once and not only by the event simulation.
func TestBubbleStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("the 10 000-client storm is not a short test")
	}
	p := stormParams(backoff.Full)
	want := RunEvents(p)
	var got Result
	synctest.Test(t, func(t *testing.T) {
		got = RunBubble(t, p)
	})
	if got.PeakAfterOutage != want.PeakAfterOutage || got.Recover != want.Recover || got.Attempts != want.Attempts {
		t.Fatalf("bubble %+v, events %+v", got, want)
	}
	t.Logf("10000 clients, 30s outage, full jitter: peak-concurrency=%d recover-s=%.3f attempts=%d",
		got.PeakAfterOutage, got.Recover.Seconds(), got.Attempts)
}

// TestDeterministic: the same parameters always give the same metrics, so
// a bake-off result is reproducible.
func TestDeterministic(t *testing.T) {
	p := smallParams(backoff.Decorrelated)
	a, b := RunEvents(p), RunEvents(p)
	if a != b {
		t.Fatalf("%+v != %+v", a, b)
	}
	p.Seed++
	if c := RunEvents(p); c == a {
		t.Fatal("changing the seed changed nothing")
	}
}
