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
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
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

// TestOpenReturnsAtOnceOnARefusalNoRetryChanges is the open's dial loop and
// the three answers a retry cannot change.
//
// The loop retried every failure until its context ended. A broker of the other
// product, one too old for the destination mapping and a refused credential
// are the same answer on every attempt, so with a context that never ends and
// no attempt limit -- what a long-lived service passes -- Open never returned,
// and the caller was told nothing at all. Here the context does end, three
// budgets out, which is what the defect cost: `DeadlineExceeded` instead of the
// refusal, at the deadline instead of at once.
//
// The refused credential is each broker's own sentence, which the server
// speaks for the product its header names. It was a fake Artemis sentence
// here, `AMQ229031`, which the real broker only logs, so this test passed
// while both pinned brokers' refusals mapped to CodeUnknown and OpenBackend
// against either one, measured with a wrong passcode, reported
// DeadlineExceeded after its full five-second context.
func TestOpenReturnsAtOnceOnARefusalNoRetryChanges(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker activemq.Broker
		opts   stompsrv.Options
		login  string
		code   mq.ErrorCode
	}{
		"the other product": {
			broker: activemq.Artemis,
			opts:   stompsrv.Options{ServerHeader: activemq.ServerClassic + "/6.2.0"}, code: mq.CodeUnsupported,
		},
		"a version without the mapping": {
			broker: activemq.Artemis,
			opts:   stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.6.0"}, code: mq.CodeUnsupported,
		},
		"a refused credential on Artemis": {
			broker: activemq.Artemis,
			opts:   stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0", Login: "app", Passcode: "right"},
			login:  "app", code: mq.CodeUnauthorized,
		},
		"a refused credential on Classic": {
			broker: activemq.Classic,
			opts:   stompsrv.Options{ServerHeader: activemq.ServerClassic + "/6.2.0", Login: "app", Passcode: "right"},
			login:  "app", code: mq.CodeUnauthorized,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := newServer(t, tc.opts)
			cfg := settings(tc.broker)
			cfg.Addrs = []string{srv.Addr()}
			cfg = reconnecting(cfg)
			if tc.login != "" {
				cfg.Login, cfg.Passcode, cfg.InsecurePlainLogin = tc.login, "wrong", true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*budget)
			defer cancel()
			start := time.Now()
			b, err := stomp.OpenBackend(ctx, cfg)
			took := time.Since(start)
			if err == nil {
				_ = b.Close()
				t.Fatal("OpenBackend accepted a broker that refuses this client")
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Errorf("OpenBackend reports %v (code %v) after %v, want %v: the dial loop retried a refusal no "+
					"retry changes until the context ended", err, got, took, tc.code)
			}
			if took >= budget {
				t.Errorf("OpenBackend took %v to report a refusal the first attempt had", took)
			}
		})
	}
}

// TestBackendWithClientAppliesTheProductGate is the gate a dial applies,
// applied to a connection the caller dialled. A client configured for one
// product and handed a connection to the other derived every destination the
// wrong way and published to names nothing consumed, because only the
// driver's own dial read the CONNECTED frame's `server` header.
func TestBackendWithClientAppliesTheProductGate(t *testing.T) {
	t.Parallel()
	srv := classicServer(t)
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	t.Cleanup(func() { _ = conn.Disconnect() })
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	cfg := settings(activemq.Artemis)
	cfg.Addrs = []string{srv.Addr()}
	b, err := stomp.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, cfg)
	if err == nil {
		_ = b.Close()
		t.Fatal("BackendWithClient accepted a connection to ActiveMQ Classic for a client configured for Artemis")
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("the refusal is %v (code %v), want %v", err, got, mq.CodeUnsupported)
	}
	if err := conn.Send("/queue/still-here", "text/plain", []byte("x")); err != nil {
		t.Errorf("refusing the caller's connection closed it: %v", err)
	}
}

// TestASourceOnAClientWithNoAddressIsRefused is a source on a client opened
// over the caller's connection with no Config.Addrs.
//
// A source dials a connection of its own, and it dialled Config.Addrs -- whose
// resolved default is localhost:61613, not the broker the caller's connection
// reached. The consumer then read some other broker, or nothing, behind a
// client that otherwise worked. With no address to dial there is nothing a
// source can do, and the open says so.
func TestASourceOnAClientWithNoAddressIsRefused(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	conn, err := gostomp.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dialling the server directly: %v", err)
	}
	t.Cleanup(func() { _ = conn.Disconnect() })
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	b, err := stomp.BackendWithClient(ctx, conn, driverkit.ClientBorrowed, settings(activemq.Artemis))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers"})
	if err == nil {
		_ = src.Close()
		t.Fatal("OpenSource on a client with no address of its own succeeded; its connection went to the " +
			"resolved default, localhost, rather than to the broker the caller's connection reaches")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidConfig {
		t.Errorf("the refusal is %v (code %v), want %v", err, got, mq.CodeInvalidConfig)
	}
	if got := srv.Accepted(); got != 1 {
		t.Errorf("the server took %d connections, want only the caller's", got)
	}
}
