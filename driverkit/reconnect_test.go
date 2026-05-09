package driverkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/backoff"
)

// TestReconnectorDialMeansTheClientIsRestored is the first of the four gaps
// `hack/FOLLOWUPS.md` recorded in this helper: `Dial` has to mean "the client
// can work again", not "a socket is open".
//
// The helper cannot enforce that -- only the driver knows what restoring its
// client involves -- so what is pinned here is the half that makes the contract
// stateable: nothing the helper reports says "ready" until Dial has returned.
// A helper that offered a Dial and an AfterDial, or that reported readiness
// from inside the dial, would let a driver put the topology replay after the
// announcement, and the failure mode is a client reported ready that cannot
// publish.
func TestReconnectorDialMeansTheClientIsRestored(t *testing.T) {
	h := driverkit.NewHealth(nil)
	var insideState mq.HealthState
	var insideRestores uint64
	var r *driverkit.Reconnector
	var err error
	r, err = driverkit.NewReconnector(driverkit.ReconnectConfig{
		Health: h,
		Base:   time.Nanosecond,
		Max:    time.Nanosecond,
		// The dial is a two-step restore: the socket, and then the work a
		// driver with topology does before its client is usable. Both
		// observations are taken at the END of it, which is the last instant
		// at which a helper could still have announced readiness too early.
		Dial: func(context.Context) error {
			insideState = h.State()
			insideRestores = r.Restores()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewReconnector: %v", err)
	}
	if err := r.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if insideState == mq.HealthReady {
		t.Error("the reporter was Ready while Dial was still restoring the client: a driver that replays its topology inside Dial would have been announced as ready before the replay")
	}
	if insideRestores != 0 {
		t.Errorf("Restores was %d inside the first Dial, want 0: the counter has to rise after the restore, not before it", insideRestores)
	}
	if h.State() != mq.HealthReady {
		t.Errorf("the reporter is %v once Dial has returned, want Ready", h.State())
	}
	if got := r.Restores(); got != 1 {
		t.Errorf("Restores is %d after one successful dial, want 1", got)
	}
}

// TestReconnectorInvalidatesBetweenTheBumpAndTheDial is the second gap: there
// was no seam for "the connection is gone" that was not a dial, so every driver
// with handles wrote the same fifteen lines between `Lost` and `Connect` -- and
// the ORDER of those lines is load-bearing, because a settlement must be
// refused from the instant the connection died.
//
// [driverkit.ReconnectConfig.Invalidate] is that seam, and this pins the order
// the helper guarantees: the generation is already bumped when Invalidate runs,
// and Invalidate has run before the health report and before any dial.
func TestReconnectorInvalidatesBetweenTheBumpAndTheDial(t *testing.T) {
	boom := errors.New("peer went away")
	var steps []string
	var genAtInvalidate uint32
	var invalidated error
	h := driverkit.NewHealth(func(snap mq.HealthSnapshot) {
		steps = append(steps, "health:"+snap.State.String())
	})
	var r *driverkit.Reconnector
	var err error
	r, err = driverkit.NewReconnector(driverkit.ReconnectConfig{
		Health: h,
		Base:   time.Nanosecond,
		Max:    time.Nanosecond,
		Dial:   func(context.Context) error { steps = append(steps, "dial"); return nil },
		Invalidate: func(cause error) {
			steps = append(steps, "invalidate")
			genAtInvalidate, invalidated = r.Generation(), cause
		},
	})
	if err != nil {
		t.Fatalf("NewReconnector: %v", err)
	}
	if err := r.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	gen := r.Generation()
	steps = nil

	next := r.Lost(boom)
	if err := r.Connect(context.Background()); err != nil {
		t.Fatalf("Connect after the loss: %v", err)
	}
	if next == gen {
		t.Fatal("a lost connection did not bump the generation")
	}
	want := []string{"invalidate", "health:degraded", "health:connecting", "dial", "health:ready"}
	if len(steps) != len(want) {
		t.Fatalf("the loss went through %v, want %v", steps, want)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("the loss went through %v, want %v", steps, want)
		}
	}
	if genAtInvalidate != next {
		t.Errorf("Invalidate saw generation %d, want %d: a handle invalidated under the OLD generation would accept a settlement the connection has already lost", genAtInvalidate, next)
	}
	if !errors.Is(invalidated, boom) {
		t.Errorf("Invalidate was told %v, want the loss's own cause", invalidated)
	}
	t.Run("a reconnector with no Invalidate still reports the loss", func(t *testing.T) {
		plain, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
			Dial: func(context.Context) error { return nil }, Health: driverkit.NewHealth(nil),
		})
		if err != nil {
			t.Fatalf("NewReconnector: %v", err)
		}
		if got := plain.Lost(boom); got != 2 {
			t.Errorf("generation %d after one loss, want 2", got)
		}
	})
}

// TestReconnectorDefaultPolicyIsJittered is the third gap, and it was a trap
// that caught three drivers: [backoff.Kind]'s zero value is `Auto`, `Auto`
// resolves to the provisional winner, and that winner is `None` -- no
// delay at all. The specification's "reconnect with backoff (never busy-spin)" forbids it, and
// the helper only softened it by yielding for a nanosecond.
//
// The default is now [driverkit.DefaultReconnectPolicy]. A caller who wants the family's winner still gets it by naming [backoff.AutoKind], which is the half
// that keeps this a change of default rather than a refusal.
func TestReconnectorDefaultPolicyIsJittered(t *testing.T) {
	// Eight words spread across the whole range, so that a jittered policy
	// cannot pass by drawing the same delay twice.
	words := []uint64{0, 1 << 60, 1 << 61, 1 << 62, 1 << 63, 3 << 62, 7 << 61, ^uint64(0)}
	delays := func(policy backoff.Kind) []time.Duration {
		t.Helper()
		var i int
		r, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
			Dial:   func(context.Context) error { return nil },
			Policy: policy,
			Base:   time.Second,
			Max:    time.Second,
			Rand:   func() uint64 { w := words[i%len(words)]; i++; return w },
		})
		if err != nil {
			t.Fatalf("NewReconnector: %v", err)
		}
		out := make([]time.Duration, 0, len(words))
		for range words {
			out = append(out, r.Backoff(1, 0))
		}
		return out
	}

	def := delays(backoff.Kind(0))
	distinct := map[time.Duration]struct{}{}
	for _, d := range def {
		if d < 0 || d > time.Second {
			t.Errorf("the default policy chose %v, outside [0, Max]", d)
		}
		distinct[d] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Errorf("the default policy chose %v for eight different random words: an unset Policy is a dial loop that answers a broker restart from every client in the fleet at the same instant, which is what the specification forbids", def)
	}
	if !driverkit.DefaultReconnectPolicy.Jittered() {
		t.Errorf("DefaultReconnectPolicy is %v, which is not jittered", driverkit.DefaultReconnectPolicy)
	}

	// And the family's winner is still reachable by name, which is what makes
	// this a default rather than a policy the helper imposes.
	for _, d := range delays(backoff.AutoKind) {
		if d != 0 {
			t.Errorf("an explicit backoff.AutoKind chose %v, want the family winner's own delay", d)
		}
	}
}

// TestReconnectorMaxAttemptsBoundsOneOutage is the fourth gap, which was a
// question about what a number MEANS: a caller who sets `MaxReconnects: 100`
// means "give up after a hundred attempts at THIS outage", and a client that
// has survived twenty outages over a month must not have spent twenty of them.
//
// Both readings are now separate accessors, because one number answering to
// both names is what made the reading ambiguous in the first place.
func TestReconnectorMaxAttemptsBoundsOneOutage(t *testing.T) {
	boom := errors.New("nope")
	fail := true
	r, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
		Dial: func(context.Context) error {
			if fail {
				return boom
			}
			return nil
		},
		MaxAttempts: 2,
		Base:        time.Nanosecond,
		Max:         time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("NewReconnector: %v", err)
	}
	ctx := context.Background()
	for outage := 1; outage <= 3; outage++ {
		r.Lost(boom)
		if err := r.Connect(ctx); !errors.Is(err, boom) {
			t.Fatalf("outage %d: Connect: %v", outage, err)
		}
		if got := r.Attempts(); got != 2 {
			t.Errorf("outage %d spent %d attempts, want the whole budget of 2: a bound that carried across outages would give a month-old client no attempts at all", outage, got)
		}
		if want := 2 * outage; r.TotalAttempts() != want {
			t.Errorf("after outage %d TotalAttempts is %d, want %d", outage, r.TotalAttempts(), want)
		}
		if got := r.Restores(); got != 0 {
			t.Errorf("Restores is %d with every dial failing, want 0", got)
		}
	}
	fail = false
	r.Lost(boom)
	if err := r.Connect(ctx); err != nil {
		t.Fatalf("Connect once the broker is back: %v", err)
	}
	if got := r.Attempts(); got != 1 {
		t.Errorf("the successful outage spent %d attempts, want 1", got)
	}
	if got := r.TotalAttempts(); got != 7 {
		t.Errorf("TotalAttempts is %d over four outages, want 7", got)
	}
	if got := r.Restores(); got != 1 {
		t.Errorf("Restores is %d after one successful dial, want 1", got)
	}
}
