package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// The Open family and the configuration it refuses.
//
// Every test in this package reaches for `BackendWithCommander`, so the
// entry points a caller actually uses -- `Open`, `OpenWithClient`,
// `OpenWithCommander` -- and the refusals that come before any dial were
// covered by nothing.

// TestEveryConfigurationRefusalIsReachedThroughOpen.
//
// The validation runs before anything is dialled, which is what makes a
// misconfiguration an InvalidConfig naming the field rather than an
// Unavailable naming a host the caller cannot do anything about.
func TestEveryConfigurationRefusalIsReachedThroughOpen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		shape func(*rd.Config)
	}{
		{"a negative database", func(c *rd.Config) { c.DB = -1 }},
		{"a negative partition count", func(c *rd.Config) { c.Partitions = -1 }},
		{"a negative stream bound", func(c *rd.Config) { c.MaxLen = -1 }},
		{"a negative duration", func(c *rd.Config) { c.AckDeadline = -time.Second }},
		{"a negative claim count", func(c *rd.Config) { c.ClaimCount = -1 }},
		{"a negative durable count", func(c *rd.Config) { c.DurableLocal = -1 }},
		{"a nack strategy that is not one", func(c *rd.Config) { c.Nack = streams.Kind(99) }},
		{"an acknowledgement guard that is not one", func(c *rd.Config) { c.AckGuard = streams.AckGuard(99) }},
		{"a key prefix carrying a hash tag", func(c *rd.Config) { c.KeyPrefix = "{tenant}" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := rd.Config{Addrs: []string{"127.0.0.1:1"}}
			tc.shape(&cfg)

			// The CODE is what makes this discriminating. The address is
			// one nothing listens on, so a driver that skipped the
			// validation and dialled would also return an error -- an
			// Unavailable rather than an InvalidConfig -- and an assertion
			// that only asked "did it fail" would pass either way.
			if _, err := rd.OpenBackend(t.Context(), cfg); mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Errorf("OpenBackend answered %v (code %v), want CodeInvalidConfig", err, mq.CodeOf(err))
			}
			if _, err := rd.Open(t.Context(), cfg); mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Errorf("Open answered %v (code %v), want CodeInvalidConfig", err, mq.CodeOf(err))
			}
		})
	}
}

// TestOpenWithNoClientOrCommanderIsRefused.
//
// Without the check it is a nil dereference on first use, a long way from
// the call that caused it.
func TestOpenWithNoClientOrCommanderIsRefused(t *testing.T) {
	t.Parallel()
	cfg := rd.Config{Addrs: []string{"127.0.0.1:1"}}

	if _, err := rd.BackendWithClient(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("BackendWithClient accepted a nil client")
	}
	if _, err := rd.OpenWithClient(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("OpenWithClient accepted a nil client")
	}
	if _, err := rd.BackendWithCommander(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("BackendWithCommander accepted a nil commander")
	}
	if _, err := rd.OpenWithCommander(t.Context(), nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("OpenWithCommander accepted a nil commander")
	}
}

// TestAnOwnershipThatIsNeitherIsRefused.
//
// The refusal is asserted rather than located: `driverkit` checks the value
// again further down, so deleting the check here changes nothing a test can
// see. Both layers refusing is the point, since the value reaches a backend
// from four entry points.
func TestAnOwnershipThatIsNeitherIsRefused(t *testing.T) {
	t.Parallel()
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	cfg := rd.Config{Addrs: []string{"127.0.0.1:1"}, Clock: fakeClock{srv}}

	if _, err := rd.BackendWithCommander(t.Context(), srv, driverkit.Ownership(42), cfg); err == nil {
		t.Error("BackendWithCommander accepted an ownership that is neither owned nor borrowed")
	}
}

// TestEveryEntryPointRespectsACancelledContext.
//
// The check is first, before the configuration is even read: a caller whose
// context is already dead gets that answer rather than a validation error
// about a field they may have set correctly.
func TestEveryEntryPointRespectsACancelledContext(t *testing.T) {
	t.Parallel()
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	cfg := rd.Config{Addrs: []string{"127.0.0.1:1"}, Clock: fakeClock{srv}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"OpenBackend", func() error { _, err := rd.OpenBackend(ctx, cfg); return err }},
		{"Open", func() error { _, err := rd.Open(ctx, cfg); return err }},
		{"BackendWithCommander", func() error {
			_, err := rd.BackendWithCommander(ctx, srv, driverkit.ClientBorrowed, cfg)
			return err
		}},
		{"OpenWithCommander", func() error {
			_, err := rd.OpenWithCommander(ctx, srv, driverkit.ClientBorrowed, cfg)
			return err
		}},
	} {
		if err := tc.call(); err == nil {
			t.Errorf("%s with a cancelled context returned a handle", tc.name)
		}
	}
}

// TestOpenWithCommanderBuildsAClient.
//
// `clientFor` is the wrapper every entry point shares, and nothing reached
// it: every test here stops at the backend. What it can be held to from
// outside is that the client carries THIS driver's backend, which is what
// `Native` promises.
//
// Its OPTIONS are not held. Whether the configured clock and hooks reached
// the client is not observable through mq.Client, so dropping them leaves
// this green -- that belongs to a test of the core's own wiring. The
// configuration here still sets both, so the branches that build them run.
func TestOpenWithCommanderBuildsAClient(t *testing.T) {
	t.Parallel()
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	cfg := rd.Config{
		Addrs: []string{"127.0.0.1:1"}, AutoCreate: true, Clock: fakeClock{srv},
		Hooks: mq.Hooks{OnPublishStart: func(context.Context, mq.PublishStartEvent) {}},
	}

	c, err := rd.OpenWithCommander(t.Context(), srv, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Native is the NATIVE client and not the backend, which is ADR-003's
	// escape hatch: a caller reaching past the portable API gets the thing
	// they would have built themselves, and here that is the commander this
	// test passed in.
	if got := c.Native(); got != any(srv) {
		t.Errorf("the client's Native is %T, want the commander it was opened with", got)
	}
	if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
		t.Errorf("closing the client the wrapper built: %v", err)
	}
}

// A DIAL failure is deliberately not tested here.
//
// It would need a real rueidis client, and rueidis leaves a delay goroutine
// sleeping for up to a second after a connection attempt that fails --
// harmless, and enough to fail this package's goleak check, which has NO
// ignores and says in TestMain that this is the point. Weakening that to
// reach three statements of error mapping would trade a check that finds a
// leaked reclaim sweep for coverage of a path the container arm exercises on
// every run.
