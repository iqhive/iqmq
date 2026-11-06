package kafkago

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// TestABackgroundSuccessClearsOnlyItsOwnPath: a background failure is
// cleared by the next success of the path it happened on, and by nothing else:
// not by another path's success, which would hide a path that keeps failing,
// whichever of the two failed first, and not by a new generation, which clears
// its own subscription's generation failure only. The health names the latest
// failure of a path still failing.
func TestABackgroundSuccessClearsOnlyItsOwnPath(t *testing.T) {
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.opened()
	errA, errB := errors.New("refused A"), errors.New("refused B")
	var sinkA, sinkB, gen healthPath
	state := func(want mq.HealthState, wantErr error, after string) {
		t.Helper()
		s := b.health.Snapshot()
		if s.State != want || !errors.Is(s.Err, wantErr) {
			t.Fatalf("Health after %s = %v (%v), want %v (%v)", after, s.State, s.Err, want, wantErr)
		}
	}
	b.failed(&sinkA, errA)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "another path's success")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errB, "the earlier failing path's success while the later one still fails")
	b.recovered(&sinkB)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	b.failed(&sinkB, errB)
	b.recovered(&sinkB)
	state(mq.HealthDegraded, errA, "the later failing path's success while the earlier one still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "both paths' successes")

	b.failed(&sinkA, errA)
	b.generationDown(&gen)
	b.recovered(&sinkA)
	state(mq.HealthDegraded, errDisconnected, "a background success while a generation is down")
	b.generationUp(t.Context(), &gen, false)
	state(mq.HealthReady, nil, "a new generation")

	b.failed(&sinkA, errA)
	b.generationDown(&gen)
	b.generationUp(t.Context(), &gen, false)
	state(mq.HealthDegraded, errA, "a new generation while another path still fails")
	b.recovered(&sinkA)
	state(mq.HealthReady, nil, "the failing path's success")
}
