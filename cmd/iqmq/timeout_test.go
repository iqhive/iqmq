package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// TestEveryCommandNamesTheBoundThatEndedItsOpen: every command opens its
// target under a bound of its own, and an open that bound cut off printed
// "opening <target>: context canceled", which names neither the bound nor
// the stop and reads as a Ctrl-C. Each command now says which bound ran out,
// and the driver saw it as the open's deadline.
func TestEveryCommandNamesTheBoundThatEndedItsOpen(t *testing.T) {
	for _, c := range []struct {
		name  string
		args  []string
		bound time.Duration
		want  string
	}{
		{
			"pub",
			[]string{"pub", "stall://", "orders", "body", "--timeout", "2s"},
			2 * time.Second,
			"iqmq pub: the --timeout of 2s ran out while opening stall://",
		},
		{
			"sub",
			[]string{"sub", "stall://", "orders", "billing", "--timeout", "2s"},
			2 * time.Second,
			"iqmq sub: the --timeout of 2s ran out while opening stall://",
		},
		{
			"tail",
			[]string{"tail", "stall://", "orders"},
			10 * time.Second,
			"iqmq tail: the 10s bound on reading the driver's capabilities ran out while opening stall://",
		},
		{
			"bench",
			[]string{"bench", "stall://", "orders", "--timeout", "2s"},
			2 * time.Second,
			"iqmq bench: the --timeout of 2s ran out while opening stall://",
		},
		{
			"bridge",
			[]string{"bridge", "stall://", "orders", "billing", "stall://", "mirror", "--timeout", "2s"},
			2 * time.Second, "iqmq bridge (source): the --timeout of 2s ran out while opening stall://",
		},
		{
			"topology",
			[]string{"topology", "plan", "stall://", "--topic", "orders", "--timeout", "2s"},
			2 * time.Second, "iqmq topology plan: the --timeout of 2s ran out while opening stall://",
		},
		{
			"doctor",
			[]string{"doctor", "stall://", "--timeout", "2s"},
			2 * time.Second,
			"FAILED: the --timeout of 2s ran out",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var deadline time.Time
				reg := dsn.NewRegistry()
				if err := reg.Register("stall", func(ctx context.Context, _ string) (*mq.Client, error) {
					deadline, _ = ctx.Deadline()
					<-ctx.Done()
					return nil, ctx.Err()
				}); err != nil {
					t.Fatal(err)
				}
				h := harnessWith(t, reg, nil)
				start := time.Now()
				code := h.run(c.args...)
				out := h.stdout() + h.stderr()
				if code != exitFailure {
					t.Errorf("%s = %d after its open ran out of time, want %d", c.name, code, exitFailure)
				}
				if !deadline.Equal(start.Add(c.bound)) {
					t.Errorf("the open saw a deadline %v away, want %v", deadline.Sub(start), c.bound)
				}
				if !strings.Contains(out, c.want) {
					t.Errorf("the output does not say %q", c.want)
				}
				if strings.Contains(out, "context canceled") {
					t.Errorf("an open nobody interrupted reads \"context canceled\"")
				}
				if t.Failed() {
					t.Logf("stdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
				}
			})
		})
	}
}

// TestExpiringIsADeadline: a bound was a timer that cancelled a context with
// no deadline, so a driver could not size its own wait from it, and its
// expiry was context.Canceled, which is a Ctrl-C's error. It is the context's
// deadline now, on the wall clock and on a test's clock alike, and a context a
// driver derives from the wall clock's bound reads DeadlineExceeded too. A
// bound nested in an earlier one has the earlier deadline, and its parent's
// expiry is not its own.
func TestExpiringIsADeadline(t *testing.T) {
	t.Run("the wall clock", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := &cliEnv{clock: realClock{}}
			start := time.Now()
			ctx, cancel, expired := e.expiring(context.Background(), time.Second, "the test's bound")
			defer cancel()
			derived, dcancel := context.WithCancel(ctx)
			defer dcancel()
			if at, ok := ctx.Deadline(); !ok || !at.Equal(start.Add(time.Second)) {
				t.Errorf("Deadline() = %v, %v; want %v, true", at, ok, start.Add(time.Second))
			}
			<-derived.Done()
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(derived.Err(), context.DeadlineExceeded) {
				t.Errorf("expired, the bound reads %v and a context derived from it %v; want %v",
					ctx.Err(), derived.Err(), context.DeadlineExceeded)
			}
			if !expired() {
				t.Error("the bound ran out and expired() reported false")
			}
		})
	})
	t.Run("a test's clock", func(t *testing.T) {
		now := time.Unix(1700000000, 0)
		e := &cliEnv{clock: firedFor(now, time.Second)}
		ctx, cancel, expired := e.expiring(context.Background(), time.Second, "the test's bound")
		defer cancel()
		<-ctx.Done()
		if at, ok := ctx.Deadline(); !ok || !at.Equal(now.Add(time.Second)) {
			t.Errorf("Deadline() = %v, %v; want %v, true: the deadline is on the environment's clock",
				at, ok, now.Add(time.Second))
		}
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Errorf("expired, the bound reads %v, want %v", ctx.Err(), context.DeadlineExceeded)
		}
		if !expired() {
			t.Error("the bound ran out and expired() reported false")
		}
	})
	t.Run("nested in an earlier bound", func(t *testing.T) {
		now := time.Unix(1700000000, 0)
		e := &cliEnv{clock: firedFor(now, time.Second)}
		outer, cancelOuter, outerExpired := e.expiring(context.Background(), time.Second, "the outer bound")
		defer cancelOuter()
		inner, cancelInner, innerExpired := e.expiring(outer, time.Hour, "the inner bound")
		defer cancelInner()
		<-inner.Done()
		if at, ok := inner.Deadline(); !ok || !at.Equal(now.Add(time.Second)) {
			t.Errorf("Deadline() = %v, %v; want the outer bound's %v", at, ok, now.Add(time.Second))
		}
		if !outerExpired() || innerExpired() {
			t.Errorf("the outer bound ran out: expired() is %v outside and %v inside, want true and false",
				outerExpired(), innerExpired())
		}
		if !errors.Is(inner.Err(), context.DeadlineExceeded) {
			t.Errorf("the outer bound ran out and the inner one reads %v, want %v",
				inner.Err(), context.DeadlineExceeded)
		}
		if got := ranOutOf(inner); got != "the outer bound ran out" {
			t.Errorf("ranOutOf(inner) = %q, want the outer bound named", got)
		}
	})
	t.Run("an interrupt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			parent, interrupt := context.WithCancel(context.Background())
			e := &cliEnv{clock: realClock{}}
			ctx, cancel, expired := e.expiring(parent, time.Hour, "the test's bound")
			defer cancel()
			interrupt()
			if expired() || ranOutOf(ctx) != "" || !errors.Is(ctx.Err(), context.Canceled) {
				t.Errorf("interrupted: expired() %v, ranOutOf %q, Err %v; want false, \"\" and %v",
					expired(), ranOutOf(ctx), ctx.Err(), context.Canceled)
			}
		})
	})
}

// TestPubNamesWhatStoppedAPublication: a publication the run's end cut off is
// "not published", with what ended the run: the --timeout that ran out, or
// the stop.
func TestPubNamesWhatStoppedAPublication(t *testing.T) {
	e := &cliEnv{clock: firedFor(time.Unix(1700000000, 0), time.Second)}
	ctx, cancel, _ := e.expiring(context.Background(), time.Second, timeoutBound(time.Second))
	defer cancel()
	<-ctx.Done()
	if got, want := pubOutcome(ctx, ctx.Err()), "not published (the --timeout of 1s ran out)"; got != want {
		t.Errorf("pubOutcome after the --timeout = %q, want %q", got, want)
	}
	stopped, stop := context.WithCancel(context.Background())
	stop()
	if got, want := pubOutcome(stopped, stopped.Err()), "not published (the run was stopped)"; got != want {
		t.Errorf("pubOutcome after a stop = %q, want %q", got, want)
	}
}

// TestPubNamesTheBoundThatEndedItsFlushAfterTheStop: after a stop pub flushes
// what it published on a context of its own, bounded at 10s, and a flush that
// bound ended read "flush: context canceled" as if Ctrl-C had ended it.
func TestPubNamesTheBoundThatEndedItsFlushAfterTheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := sinkHarness(t, &fakeSink{log: &seamLog{}, flushStalls: true})
		start := time.Now()
		code := h.run("pub", "seamdst://", "mirror", "body", "--timeout", "1s")
		if took := time.Since(start); took != time.Second+stoppedFlushBound {
			t.Errorf("pub took %v, want its --timeout and then the flush's %v", took, stoppedFlushBound)
		}
		if code != exitFailure {
			t.Errorf("pub = %d with its flush cut short, want %d", code, exitFailure)
		}
		if want := "iqmq pub: flush: the 10s flush after the stop ran out: "; !strings.Contains(h.stderr(), want) {
			t.Errorf("stderr does not say %q:\n%s", want, h.stderr())
		}
	})
}

// TestTailNamesTheBoundThatEndedItsRemoval: tail removes the subscription it
// generated on a context of its own, bounded at 10s, and a removal that bound
// ended read "could not remove ...: context canceled".
func TestTailNamesTheBoundThatEndedItsRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg, _ := tailRegistry(t, "stalledadmin", func(b *memory.Broker) mq.Backend {
			return stalledAdminBackend{Backend: b}
		})
		h := harnessWith(t, reg, nil)
		if code := h.run("tail", "stalledadmin://", "orders", "--idle", "1s"); code != exitOK {
			t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
		}
		name := subscriptionLine(t, h.stdout())
		want := "could not remove the subscription " + name +
			" this run created: the 10s bound on the removal ran out: "
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("stderr does not say %q:\n%s", want, h.stderr())
		}
	})
}

// stalledAdminBackend is the memory broker with an Admin surface whose
// DeleteSubscription answers only when its context ends.
type stalledAdminBackend struct {
	mq.Backend
}

// Close leaves the broker to the test.
func (stalledAdminBackend) Close() error { return nil }

// Admin wraps the broker's own surface.
func (b stalledAdminBackend) Admin() (mq.Admin, error) {
	inner, err := b.Backend.(mq.AdminBackend).Admin()
	if err != nil {
		return nil, err
	}
	return stalledAdmin{Admin: inner}, nil
}

// stalledAdmin is an Admin whose DeleteSubscription waits for its context.
type stalledAdmin struct {
	mq.Admin
}

// DeleteSubscription answers when its context ends.
func (stalledAdmin) DeleteSubscription(ctx context.Context, _, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

// closeTimesOutSource is a requeueSource whose Close fails with the driver's
// own timeout, as a final commit its own bound cut short does. Close takes no
// context, so the run's context cannot be its cause.
type closeTimesOutSource struct {
	*requeueSource
}

// Close closes and fails.
func (s closeTimesOutSource) Close() error {
	_ = s.requeueSource.Close()
	return &mq.OpError{
		Driver: "fake", Operation: mq.OpClose, Code: mq.CodeDeadlineExceeded,
		Err: errors.New("final commit: read tcp 10.0.0.1:9092: i/o timeout"),
	}
}

// TestADriversCloseFailureAfterAStopIsReported: sub and bridge leave out of
// their output only the Canceled a graceful close on the stopped run's
// context answers with. A driver's own close failure, CodeDeadlineExceeded
// from a final commit its own bound cut short, is reported whatever ended the
// run; hiding every deadline once the run had ended hid it after a Ctrl-C or
// a --timeout, and no predicate on the error can tell it from the run's own
// deadline.
func TestADriversCloseFailureAfterAStopIsReported(t *testing.T) {
	for name, args := range map[string][]string{
		"sub":    {"sub", "closefails://", "orders", "billing", "--settle", "nack"},
		"bridge": {"bridge", "--dry-run", "closefails://", "orders", "billing", "seamdst://", "mirror"},
	} {
		for _, how := range []string{"--timeout", "interrupted", "--max"} {
			t.Run(name+", "+how, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					src := newRequeueSource("")
					h, _ := sourceHarness(t, "closefails", closeTimesOutSource{requeueSource: src})
					a := append([]string(nil), args...)
					switch how {
					case "--timeout":
						src.stopAt, src.stop = 2, func() {} // the second fetch waits for the run's end
						a = append(a, "--timeout", "1s")
					case "interrupted":
						ctx, cancel := context.WithCancel(context.Background())
						t.Cleanup(cancel)
						h.env.ctx = ctx
						src.stopAt, src.stop = 2, cancel
					default:
						a = append(a, "--max", "1") // the run's context is live at the close
					}
					h.run(a...)
					if !strings.Contains(h.stderr(), "shutdown: ") || !strings.Contains(h.stderr(), "final commit") {
						t.Errorf("%s (%s) did not report the driver's own close failure\nstdout:\n%s\nstderr:\n%s",
							name, how, h.stdout(), h.stderr())
					}
				})
			})
		}
	}
}

// socketDeadlineSource is a requeueSource shaped like a client library that
// puts the caller's deadline on its socket, as rueidis does for a blocking
// XREADGROUP: at the deadline the read fails with an i/o timeout whether or
// not the context reads done yet, and the driver returns the context's error
// when it does and its own CodeDeadlineExceeded when it does not, as
// drivers/redis's source does.
type socketDeadlineSource struct {
	*requeueSource
}

// Fetch waits for the deadline and answers as the socket would.
func (s socketDeadlineSource) Fetch(ctx context.Context, _ []mq.DriverDelivery) (int, error) {
	at, ok := ctx.Deadline()
	if !ok {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.closed:
			return 0, mq.ErrClosed
		}
	}
	t := time.NewTimer(time.Until(at))
	defer t.Stop()
	select {
	case <-t.C:
	case <-s.closed:
		return 0, mq.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 0, &mq.OpError{
		Driver: "fake", Operation: mq.OpFetch, Code: mq.CodeDeadlineExceeded, Retryable: true,
		Err: errors.New("read tcp 10.0.0.1:6379: i/o timeout"),
	}
}

// TestADriversDeadlineAtTheBoundIsAStop: a driver that answers the bound's
// deadline with its own DeadlineExceeded, at the instant the bound runs out,
// can answer before the bound's timer has marked the context done. The run
// then read "Receive failed" and exited 1 for a clean --idle or --timeout
// stop, in about half the runs. The two timers are a race, so each case runs
// 50 times.
func TestADriversDeadlineAtTheBoundIsAStop(t *testing.T) {
	for _, cmd := range []struct {
		name string
		args []string
	}{
		{"sub", []string{"sub", "sockdl://", "orders", "billing"}},
		{"bridge --dry-run", []string{"bridge", "--dry-run", "sockdl://", "orders", "billing", "seamdst://", "mirror"}},
	} {
		for _, c := range []struct{ flag, want string }{
			{"--idle", "stopped: no delivery for 1s"},
			{"--timeout", "stopped: the --timeout of 1s ran out"},
		} {
			t.Run(cmd.name+" "+c.flag, func(t *testing.T) {
				failed := 0
				for range 50 {
					synctest.Test(t, func(t *testing.T) {
						h, _ := sourceHarness(t, "sockdl", socketDeadlineSource{requeueSource: newRequeueSource("")})
						code := h.run(append(append([]string(nil), cmd.args...), c.flag, "1s")...)
						if code != exitOK || !strings.Contains(h.stdout(), c.want) {
							failed++
							if failed == 1 {
								t.Logf("exit %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
							}
						}
					})
				}
				if failed > 0 {
					t.Errorf("%s %s 1s ended as a failure in %d of 50 runs, want %q and exit 0",
						cmd.name, c.flag, failed, c.want)
				}
			})
		}
	}
}

// TestAnIdleReceiveNamesTheTimeoutThatEndedIt: the commonest --timeout stop is
// a Receive waiting on an empty subscription that ends with the run's context.
// receiveStop names the bound there, and nothing else in the loop does.
func TestAnIdleReceiveNamesTheTimeoutThatEndedIt(t *testing.T) {
	for name, args := range map[string][]string{
		"sub":    {"sub", memDSN, "orders", "billing", "--timeout", "1s"},
		"tail":   {"tail", memDSN, "orders", "--timeout", "1s"},
		"bridge": {"bridge", "--dry-run", memDSN, "orders", "billing", memDSN, "mirror", "--timeout", "1s"},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t)
				if code := h.run(args...); code != exitOK {
					t.Errorf("%s = %d after its --timeout, want %d", name, code, exitOK)
				}
				if !strings.Contains(h.stdout(), "stopped: the --timeout of 1s ran out") {
					t.Errorf("%s does not say its --timeout ended it:\n%s", name, h.stdout())
				}
			})
		})
	}
}

// stallSink is a fakeSink whose Publish answers only when its context ends.
type stallSink struct {
	fakeSink
}

// Publish waits for the run's end.
func (s *stallSink) Publish(ctx context.Context, _ *mq.Message, _ *mq.PublishOptions) error {
	<-ctx.Done()
	return &mq.OpError{Driver: "fake", Operation: mq.OpPublish, Code: mq.CodeDeadlineExceeded, Err: ctx.Err()}
}

// stallSinkBackend is a seamBackend that hands out a stallSink.
type stallSinkBackend struct {
	*seamBackend
	sink *stallSink
}

// OpenSink hands out the stalling sink.
func (b stallSinkBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	return b.sink, nil
}

// TestBenchNamesTheTimeoutThatEndedItsPublishing: an arm whose --timeout ran
// out while it published counted what it did not publish as failures, and
// nothing named the --timeout.
func TestBenchNamesTheTimeoutThatEndedItsPublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := stallSinkBackend{
			seamBackend: &seamBackend{caps: seamCaps(t, mq.FeaturePublisherConfirm)},
			sink:        &stallSink{fakeSink: fakeSink{log: &seamLog{}}},
		}
		reg := dsn.NewRegistry()
		if err := reg.Register("stallsink", func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(b), nil
		}); err != nil {
			t.Fatal(err)
		}
		h := harnessWith(t, reg, nil)
		if code := h.run("bench", "stallsink://", "orders", "--messages", "5", "--timeout", "1s"); code != exitFailure {
			t.Errorf("bench = %d after its --timeout cut the publishing short, want %d", code, exitFailure)
		}
		if want := "iqmq bench: the --timeout of 1s ran out while publishing"; !strings.Contains(h.stderr(), want) {
			t.Errorf("stderr does not say %q\nstdout:\n%s\nstderr:\n%s", want, h.stdout(), h.stderr())
		}
	})
}
