package runner

import (
	"context"
	"testing"
	"time"
)

// TestDrainContextIsDetachedFromTheContextThatStoppedTheRun is the whole
// reason this package exists, so it is tested at the seam rather than through
// a driver: by the time the drain runs, the caller's context has ended, and a
// drain derived from it has no time at all.
func TestDrainContextIsDetachedFromTheContextThatStoppedTheRun(t *testing.T) {
	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "trace-42")
	ctx, cancel := context.WithCancel(parent)
	cancel()
	if ctx.Err() == nil {
		t.Fatal("the fixture's context is not cancelled")
	}

	dctx, release := drainContext(ctx, 250*time.Millisecond)
	defer release()

	if err := dctx.Err(); err != nil {
		t.Fatalf("the drain context is already done (%v); Shutdown would settle nothing", err)
	}
	deadline, ok := dctx.Deadline()
	if !ok {
		t.Fatal("the drain context has no deadline; a drain that cannot end is not a graceful shutdown")
	}
	if left := time.Until(deadline); left <= 0 || left > 250*time.Millisecond {
		t.Fatalf("the drain has %v left of its 250ms, want a fresh deadline", left)
	}
	if v, _ := dctx.Value(key{}).(string); v != "trace-42" {
		t.Fatalf("the drain context lost the caller's values: %q, want %q", v, "trace-42")
	}
}

func TestDrainDeadlineDefaults(t *testing.T) {
	if got := (Config{}).drain(); got != DefaultDrain {
		t.Fatalf("a zero Drain resolves to %v, want DefaultDrain (%v)", got, DefaultDrain)
	}
	if got := (Config{Drain: time.Second}).drain(); got != time.Second {
		t.Fatalf("Drain resolves to %v, want the configured second", got)
	}
}
