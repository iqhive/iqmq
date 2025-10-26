package leasectx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
	"bitbucket.org/iqhive/iqmq/internal/timer"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 2 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		s := New(k, Config{})
		if s.Kind() != k || s.Live() != 0 || s.Timers() != 0 {
			t.Fatalf("New(%v): kind %v live %d timers %d", k, s.Kind(), s.Live(), s.Timers())
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
	if !WithTimeout.PerDeliveryTimer() || Clock.PerDeliveryTimer() {
		t.Fatal("PerDeliveryTimer")
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted junk")
	}
}

func TestNewPanics(t *testing.T) {
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	expectPanic("invalid kind", func() { New(Kind(99), Config{}) })
	expectPanic("negative margin", func() { New(WithTimeout, Config{Margin: -time.Second}) })
	expectPanic("invalid timer kind", func() { New(Clock, Config{Timer: timer.Kind(99)}) })
	s := New(Auto, Config{})
	defer func() { _ = s.Close() }()
	if s.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", s.Kind())
	}
}

func TestConfig(t *testing.T) {
	n := Config{}.Normalize()
	if n.Clock == nil || n.Margin != DefaultMargin {
		t.Fatalf("zero config normalised to %+v", n)
	}
	fake := clock.NewFake(time.Unix(0, 0))
	if n := (Config{Clock: fake, Margin: time.Second}).Normalize(); n.Clock != fake || n.Margin != time.Second {
		t.Fatalf("config overwritten: %+v", n)
	}
	if err := (Config{Margin: -1}).Validate(); err == nil || errcode.CodeOf(err) != errcode.InvalidConfig {
		t.Fatalf("Validate(-1) = %v", err)
	}
	if err := (Config{Timer: timer.Kind(99)}).Validate(); err == nil {
		t.Fatal("Validate accepted an invalid timer kind")
	}
	if err := (Config{Margin: time.Second, Timer: timer.Heap}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// newSource builds a source of kind k for a test running inside a synctest
// bubble, where the real clock is the bubble's clock and both kinds are
// therefore deterministic.
func newSource(k Kind, margin time.Duration) Source {
	return New(k, Config{Margin: margin})
}

// mustNotBeDone fails when ctx is already done. It is sound whatever the
// scheduler does: nothing may cancel the context before its deadline.
func mustNotBeDone(t *testing.T, ctx context.Context, what string) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatalf("%s: context done early with %v", what, ctx.Err())
	default:
	}
}

// TestDeadlineFires checks the contract at the centre of the family: the
// derived context ends at the lease deadline, not before, with
// DeadlineExceeded.
func TestDeadlineFires(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 0)
				defer func() { _ = s.Close() }()
				start := time.Now()
				ctx, release := s.Derive(context.Background(), start.Add(time.Second))
				defer release()
				if d, ok := ctx.Deadline(); !ok || !d.Equal(start.Add(time.Second)) {
					t.Fatalf("Deadline = %v %v", d, ok)
				}
				if s.Live() != 1 {
					t.Fatalf("Live = %d", s.Live())
				}
				time.Sleep(999 * time.Millisecond)
				synctest.Wait()
				mustNotBeDone(t, ctx, "999ms into a 1s lease")
				time.Sleep(2 * time.Millisecond)
				synctest.Wait()
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("Err = %v", ctx.Err())
				}
				<-ctx.Done()
			})
		})
	}
}

// TestMarginBringsTheDeadlineForward checks that the handler is cancelled a
// margin before the broker may take the delivery back.
func TestMarginBringsTheDeadlineForward(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 200*time.Millisecond)
				defer func() { _ = s.Close() }()
				start := time.Now()
				ctx, release := s.Derive(context.Background(), start.Add(time.Second))
				defer release()
				if d, _ := ctx.Deadline(); !d.Equal(start.Add(800 * time.Millisecond)) {
					t.Fatalf("Deadline = %v, want the lease less the margin", d)
				}
				time.Sleep(799 * time.Millisecond)
				synctest.Wait()
				mustNotBeDone(t, ctx, "before the margin")
				time.Sleep(2 * time.Millisecond)
				synctest.Wait()
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("Err = %v", ctx.Err())
				}
			})
		})
	}
}

// TestReleaseCancels checks that releasing a lease ends its context at once,
// that releasing twice is harmless, and that the source stops counting it.
func TestReleaseCancels(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 0)
				defer func() { _ = s.Close() }()
				ctx, release := s.Derive(context.Background(), time.Now().Add(time.Hour))
				mustNotBeDone(t, ctx, "fresh lease")
				release()
				<-ctx.Done()
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("Err = %v", ctx.Err())
				}
				release()
				if s.Live() != 0 {
					t.Fatalf("Live = %d after release", s.Live())
				}
				// A released lease must not fire later.
				time.Sleep(2 * time.Hour)
				synctest.Wait()
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("Err changed to %v after the deadline passed", ctx.Err())
				}
			})
		})
	}
}

// TestParentCancels checks that the runner's context still governs: cancelling
// it cancels every handler context derived from it, with the parent's error.
func TestParentCancels(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 0)
				defer func() { _ = s.Close() }()
				parent, cancel := context.WithCancel(context.Background())
				ctx, release := s.Derive(parent, time.Now().Add(time.Hour))
				defer release()
				mustNotBeDone(t, ctx, "before the parent is cancelled")
				cancel()
				<-ctx.Done()
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("Err = %v", ctx.Err())
				}
				// A context derived from an already-cancelled parent is born
				// done.
				ctx2, release2 := s.Derive(parent, time.Now().Add(time.Hour))
				defer release2()
				<-ctx2.Done()
			})
		})
	}
}

// TestValuesComeFromTheParent checks that the derived context is transparent
// to values, which is how correlation ids and trace contexts reach a handler.
func TestValuesComeFromTheParent(t *testing.T) {
	type key struct{}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := newSource(k, 0)
			defer func() { _ = s.Close() }()
			parent := context.WithValue(context.Background(), key{}, "v")
			ctx, release := s.Derive(parent, time.Now().Add(time.Hour))
			defer release()
			if got := ctx.Value(key{}); got != "v" {
				t.Fatalf("Value = %v", got)
			}
			if got := ctx.Value(struct{ other int }{}); got != nil {
				t.Fatalf("unknown key returned %v", got)
			}
		})
	}
}

// TestPastDeadline checks that a lease that has already expired yields a
// context that is done immediately rather than one that never fires.
func TestPastDeadline(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 0)
				defer func() { _ = s.Close() }()
				ctx, release := s.Derive(context.Background(), time.Now().Add(-time.Second))
				defer release()
				synctest.Wait()
				<-ctx.Done()
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("Err = %v", ctx.Err())
				}
			})
		})
	}
}

// TestClosedSource checks that a closed source hands back contexts that are
// already done, so a handler started during shutdown cannot run unbounded.
func TestClosedSource(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := newSource(k, 0)
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			ctx, release := s.Derive(context.Background(), time.Now().Add(time.Hour))
			defer release()
			<-ctx.Done()
			if ctx.Err() == nil {
				t.Fatal("context from a closed source is not done")
			}
		})
	}
}

// TestCloseCancelsOutstandingLeases checks the Clock kind's declared extra:
// it holds its leases and cancels them when the source shuts down.
func TestCloseCancelsOutstandingLeases(t *testing.T) {
	s := New(Clock, Config{})
	ctx, release := s.Derive(context.Background(), time.Now().Add(time.Hour))
	defer release()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err = %v", ctx.Err())
	}
}

// TestTimersPerDerive is the measurement the family exists for, asserted as a
// contract: the reference arms one timer per delivery, and the candidate arms
// none — one for the whole stream, re-armed only when a deadline lands ahead
// of the one it is waiting for.
func TestTimersPerDerive(t *testing.T) {
	const n = 1000
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newSource(k, 0)
				defer func() { _ = s.Close() }()
				releases := make([]Release, 0, n)
				base := time.Now()
				for i := range n {
					// Deadlines arrive in order, as a stream of leases does.
					_, release := s.Derive(context.Background(), base.Add(time.Duration(i+1)*time.Second))
					releases = append(releases, release)
				}
				synctest.Wait()
				per := float64(s.Timers()) / n
				t.Logf("%v: %d timers for %d leases (%.4f per lease)", k, s.Timers(), n, per)
				if k.PerDeliveryTimer() {
					if s.Timers() != n {
						t.Fatalf("%v armed %d timers for %d leases", k, s.Timers(), n)
					}
				} else if per > 0.01 {
					t.Fatalf("%v armed %.4f timers per lease", k, per)
				}
				for _, release := range releases {
					release()
				}
				if s.Live() != 0 {
					t.Fatalf("Live = %d", s.Live())
				}
			})
		})
	}
}

// TestConcurrentDerive runs many goroutines through one source, which is how
// a worker pool uses it, under the race detector.
func TestConcurrentDerive(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := newSource(k, 0)
			defer func() { _ = s.Close() }()
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					for i := range 200 {
						d := time.Now().Add(time.Duration(w*i%7+1) * time.Millisecond)
						ctx, release := s.Derive(context.Background(), d)
						if i%3 == 0 {
							<-ctx.Done()
						}
						release()
						if ctx.Err() == nil {
							t.Errorf("released context is not done")
							return
						}
					}
				})
			}
			wg.Wait()
			if s.Live() != 0 {
				t.Fatalf("Live = %d", s.Live())
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the leasectx family
// (front-matter `auto: {leasectx: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "leasectx")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds leasectx yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds leasectx to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the package on the standard library and off the root.
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, ".")
	testguard.StdlibOnly(t, ".")
}
