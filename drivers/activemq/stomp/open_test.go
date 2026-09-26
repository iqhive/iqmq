package stomp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/internal/activemq"
)

// The Open family, which every test in this package went round.
//
// They all reach for `OpenBackend` against a server that is up, so the
// wrappers that build a CLIENT and the refusals that come before any dial
// were covered by nothing: `Open`, `OpenWithClient`, `BackendWithClient` and
// the three checks they share.

// TestOpenBuildsAClientAndNotJustABackend.
//
// `Open` is the entry point in every example and on the driver page, and
// nothing reached it: every test here calls OpenBackend. What it adds over
// that is the client wrapper and its options; this holds it to returning a
// client carrying THIS driver's backend, which is what `Native` promises and
// what every typed escape hatch in the repository asserts on.
//
// It does not hold the OPTIONS: whether the configured clock reached the
// client is not observable from outside the core, and dropping
// `clientOptions` leaves this test green. That belongs to a test of the
// core's own wiring rather than to one of this driver.
func TestOpenBuildsAClientAndNotJustABackend(t *testing.T) {
	srv := artemisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	c, err := stomp.Open(ctx, artemisConfig(srv))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the client: %v", err)
		}
	})
	if _, ok := c.Native().(*stomp.Backend); !ok {
		t.Errorf("the client Open returned carries %T, not this driver's backend", c.Native())
	}
}

// TestOpenWithAConnectionTheCallerOwns is the borrowed-connection path (the specification's
// Ownership).
//
// A borrowed connection is NOT supervised: the caller owns its lifetime, and
// a driver that started a reconnect loop over somebody else's connection
// would redial a socket it does not own. The test asserts the open succeeds
// and the close leaves the caller's connection alone.
func TestOpenWithAConnectionTheCallerOwns(t *testing.T) {
	srv := artemisServer(t)
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	t.Cleanup(func() { _ = conn.Disconnect() })

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	cfg := artemisConfig(srv)
	c, err := stomp.OpenWithClient(ctx, conn, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
		t.Errorf("closing a client over a borrowed connection: %v", err)
	}
	// The caller's connection is still theirs: sending on it must still
	// work, which it would not if the driver had disconnected it.
	if err := conn.Send("/queue/still-here", "text/plain", []byte("x")); err != nil {
		t.Errorf("the caller's borrowed connection was closed by the driver: %v", err)
	}
}

// TestBackendWithClientRefusesWhatItCannotUse.
//
// Three checks, all before anything is dialled or sent. A nil connection is
// the one that matters most: without the check it is a nil dereference on
// first use, somewhere far away from the call that caused it.
func TestBackendWithClientRefusesWhatItCannotUse(t *testing.T) {
	srv := artemisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	if _, err := stomp.BackendWithClient(ctx, nil, driverkit.ClientOwned, artemisConfig(srv)); err == nil {
		t.Error("BackendWithClient accepted a nil connection; without the check it is a nil dereference on " +
			"first use, a long way from the call that caused it")
	}

	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	t.Cleanup(func() { _ = conn.Disconnect() })

	// An ownership that is neither owned nor borrowed. The refusal is
	// asserted rather than located: `newBackend` checks it again, so deleting
	// the check here changes nothing a test can see. Both layers refusing is
	// the point -- the value reaches a backend from two entry points.
	if _, err := stomp.BackendWithClient(ctx, conn, driverkit.Ownership(42), artemisConfig(srv)); err == nil {
		t.Error("BackendWithClient accepted an ownership that is neither owned nor borrowed")
	}

	// A configuration that does not validate. The broker is what every
	// destination name is derived from, so there is no default for it.
	bad := artemisConfig(srv)
	bad.Broker = activemq.Broker(99)
	if _, err := stomp.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, bad); err == nil {
		t.Error("BackendWithClient accepted a configuration naming no known broker; every destination name " +
			"is derived from it")
	}
}

// TestOpenBackendRefusesAConfigurationItCannotUse, before it dials.
//
// The refusal has to come first: a driver that dialled and then discovered
// the configuration was wrong would hold a connection nobody can reach to
// close. The test bounds the time to prove it -- a refusal that took the
// dial timeout would be a refusal that happened after the dial.
//
// An empty `Addrs` is NOT such a configuration and this test does not use
// one: it is DEFAULTED to localhost, which is a dial that fails slowly, not
// a refusal. The broker is the field with no default, because every
// destination name is derived from it.
func TestOpenBackendRefusesAConfigurationItCannotUse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// The address is a REACHABLE server, which is what makes the ordering
	// observable instead of merely fast: if the configuration check ran
	// after the dial, this server would have accepted a connection. It used
	// to point at a dead port and assert that both refusals took under a
	// second, which is a claim about the RUNNER as much as about the driver
	// -- a goroutine descheduled on a loaded host fails it while the code is
	// perfectly correct, and it is the same shape that took
	// `TestCriticalRefusedSubscribeReportsNotFound` red on another branch's
	// gate. Counting connections cannot be starved.
	srv := artemisServer(t)
	cfg := settings(activemq.Broker(99))
	cfg.Addrs = []string{srv.Addr()}

	if _, err := stomp.OpenBackend(ctx, cfg); err == nil {
		t.Error("OpenBackend accepted a configuration naming no known broker")
	}
	if _, err := stomp.Open(ctx, cfg); err == nil {
		t.Error("Open accepted a configuration naming no known broker")
	}
	if got := srv.Accepted(); got != 0 {
		t.Errorf("the two refusals dialled %d connection(s); a configuration check that runs after the dial is a "+
			"connection nobody can reach to close", got)
	}
}

// TestTheDefaultClockIsTheWallClock.
//
// Every test in this package configures a clock, so the default -- the one a
// real deployment gets -- ran nowhere. A timer that never fires is a driver
// whose receipts never time out.
func TestTheDefaultClockIsTheWallClock(t *testing.T) {
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	cfg.Clock = nil

	b := openBackend(t, cfg)
	clock := stomp.ClockForTest(b)
	if clock == nil {
		t.Fatal("a backend configured with no clock has none")
	}

	before := clock.Now()
	if before.IsZero() {
		t.Error("the default clock reports the zero time")
	}
	timer := clock.NewTimer(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(2 * time.Second):
		t.Fatal("a timer from the default clock never fired; the driver's receipt timeouts depend on it")
	}
	if timer.Stop() {
		t.Error("Stop reported true for a timer that had already fired")
	}
	timer.Reset(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(2 * time.Second):
		t.Fatal("a timer from the default clock did not fire after Reset")
	}
}
