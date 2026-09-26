package stomp

import (
	"context"
	"errors"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestAStaleDeathNoticeDoesNotTearDownTheReplacement is the defect behind the
// first GitHub run's red `TestCriticalSinkRecoversAfterTheConnectionDies`,
// stated so that it fails every time instead of on a slow runner.
//
// A connection's watcher reports its socket's death from go-stomp's reader
// goroutine, and nothing orders that report against the dial loop. When the
// report for the OLD connection arrived after the loop had already put a new
// one in place, the supervisor tore down whatever was current -- the new
// connection -- and the next publication failed with "the connection is not
// established". This test makes that late report by hand, with the generation
// of the connection it is about, and asserts the replacement survives it.
func TestAStaleDeathNoticeDoesNotTearDownTheReplacement(t *testing.T) {
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := OpenBackend(ctx, Config{
		Broker:          activemq.Artemis,
		Addrs:           []string{srv.Addr()},
		ReceiptTimeout:  time.Second,
		DialTimeout:     10 * time.Second,
		ReconnectBase:   time.Millisecond,
		ReconnectMax:    20 * time.Millisecond,
		ReconnectGrowth: 1.5,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})

	b.mu.Lock()
	first, firstGen := b.conn, b.connGen
	b.mu.Unlock()
	if first == nil || firstGen == 0 {
		t.Fatalf("the backend opened with no connection generation (conn=%v gen=%d)", first, firstGen)
	}

	// Replace the connection the way the dial loop does after a death.
	if err := b.dial(ctx); err != nil {
		t.Fatalf("re-dialling: %v", err)
	}
	b.mu.Lock()
	second, secondGen := b.conn, b.connGen
	b.mu.Unlock()
	if second == first || secondGen == firstGen {
		t.Fatalf("the re-dial did not replace the connection (gen %d -> %d)", firstGen, secondGen)
	}
	reconnects := b.Reconnects()

	// The old connection's watcher reports its death, late.
	b.reportLostFor(firstGen, errors.New("use of closed network connection"))

	// Wait for the supervisor to have DECIDED, whichever way it decides:
	// it either counts the notice stale, or it acts on it and replaces the
	// connection. Both are observable, so the wait needs no clock of its own.
	//
	// It used to wait two seconds and then assert. That bound was the last
	// wall-clock deadline in this area and it could only lie: on a host busy
	// enough for the supervisor not to have run, it expired and reported
	// "the stale notice was counted 0 times, want 1" -- the rule under test
	// blamed for a runner that was slow. Waiting for the decision instead
	// cannot expire, and it does not trade the failure message away for
	// that: a supervisor that acts on the stale notice leaves the loop on
	// its OTHER exit and fails below with the sentence that names the
	// defect, at once rather than two seconds later. Only a supervisor that
	// never runs at all hangs, and the package timeout's goroutine dump is a
	// better diagnosis of that than any assertion here could be.
	for b.staleNotices.Load() == 0 {
		b.mu.Lock()
		acted := b.conn != second
		b.mu.Unlock()
		if acted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	b.mu.Lock()
	now := b.conn
	b.mu.Unlock()
	if now != second {
		t.Fatalf("a death notice about connection generation %d replaced generation %d's working "+
			"connection (now %v): the supervisor acted on a report about a socket it had already replaced",
			firstGen, secondGen, now)
	}
	if got := b.staleNotices.Load(); got != 1 {
		t.Errorf("the stale notice was counted %d times, want 1", got)
	}
	if got := b.Reconnects(); got != reconnects {
		t.Errorf("a stale notice caused %d re-dial(s); it should cause none", got-reconnects)
	}

	// The operation path's own report is held to the same rule: an
	// operation that met the failure on the OLD connection must not tear
	// down the new one.
	b.connectionLostFor(first, errors.New("connection already closed"))
	b.mu.Lock()
	now = b.conn
	b.mu.Unlock()
	if now != second {
		t.Fatalf("an operation's report about the replaced connection tore down its replacement")
	}
}

// TestAnOperationsDeathNoticeNamesTheConnectionItToreDown is the half of the
// rule above that the identity check cannot reach, and it is the whole of
// `TestCriticalSinkRecoversAfterTheConnectionDies`'s remaining flake.
//
// An operation that meets a death on the CURRENT connection is right to tear
// it down, and it then leaves a notice for the supervisor. That notice used to
// say generation zero -- "the one current when this was sent" -- and
// [Backend.stale] read generation zero as never stale, so the notice was
// unorderable against the dial loop. [Backend.supervise] drains the channel
// after invalidating and before re-dialling, so a report that landed in the
// gap survived the drain, and the supervisor read it once the re-dial had
// already succeeded: it tore down the connection it had just made, and the
// next publication answered "the connection is not established".
//
// The test runs the two halves in the order the race produces them, with no
// supervisor to consume the notice first, and asserts both: the notice names
// the generation it tore down, and the supervisor's own predicate calls it
// stale once a newer connection is in place.
func TestAnOperationsDeathNoticeNamesTheConnectionItToreDown(t *testing.T) {
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	cfg := Config{
		Broker:          activemq.Artemis,
		Addrs:           []string{srv.Addr()},
		ReceiptTimeout:  time.Second,
		DialTimeout:     10 * time.Second,
		ReconnectBase:   time.Millisecond,
		ReconnectMax:    20 * time.Millisecond,
		ReconnectGrowth: 1.5,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Config.Validate: %v", err)
	}
	// No supervisor: this test IS the supervisor, so that the notice the
	// operation leaves can be read rather than raced for.
	b, err := newBackend(cfg.resolve(), nil, driverkit.ClientOwned)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.dial(ctx); err != nil {
		t.Fatalf("the first dial: %v", err)
	}
	b.mu.Lock()
	firstGen := b.connGen
	b.mu.Unlock()

	// An operation meets the death on the connection that is current, and
	// what it leaves behind is read the way [Backend.supervise] reads it:
	// the BELL first, then the notice. Both halves are the claim. The specification split one channel read into two, and this test kept only the
	// second for a round -- so a report that stored a notice and rang
	// nothing would have passed it while the supervisor slept through the
	// death for ever, which is the very failure the message below names.
	b.connectionLostFor(nil, errors.New("connection already closed"))
	select {
	case <-b.lost:
	default:
		t.Fatal("the operation's report rang no bell, so the supervisor would never wake and nothing would re-dial")
	}
	notice, ok := b.takeNotice()
	if !ok {
		t.Fatal("the operation's report woke the supervisor and left it no notice, so it would wake, find nothing " +
			"to act on, and sleep again with the connection still dead")
	}
	if notice.gen != firstGen {
		t.Errorf("the operation's death notice names generation %d, want %d: a notice that does not name a "+
			"connection cannot be told apart from one about a connection the dial loop has since replaced",
			notice.gen, firstGen)
	}

	// The dial loop then re-establishes the connection, which is the step
	// the notice above raced.
	if err := b.dial(ctx); err != nil {
		t.Fatalf("re-dialling: %v", err)
	}
	b.mu.Lock()
	second, secondGen := b.conn, b.connGen
	b.mu.Unlock()
	if !b.stale(notice) {
		t.Fatalf("the supervisor would act on a notice about generation %d with generation %d in place; acting "+
			"on it disconnects a working connection and the next publication reports that there is none",
			notice.gen, secondGen)
	}
	b.mu.Lock()
	now := b.conn
	b.mu.Unlock()
	if now != second {
		t.Fatalf("the staleness check itself replaced the connection")
	}
}

// TestANewerDeathNoticeDisplacesAnOlderOneRatherThanBeingDropped is, and it is the hazard the specification's own rejection text names.
//
// The notice used to be a `chan lostNotice` of capacity one written with a
// `default`, so a second report while one was waiting was DROPPED. Collapsing
// two handles' reports of one death into one re-dial is what that buffer is
// for -- but a full buffer does not mean "the same death". A stale notice
// about an already-replaced generation could occupy the slot while the
// CURRENT connection died, and the genuine notice was the one dropped: the
// supervisor read the stale one, discarded it as stale, and went back to
// sleep with nothing left to wake it. A client Degraded with no connection
// and nothing that would ever re-dial.
//
// Both orders are asserted, because the rule is about which notice SURVIVES
// and not about which arrived second.
func TestANewerDeathNoticeDisplacesAnOlderOneRatherThanBeingDropped(t *testing.T) {
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	cfg := Config{
		Broker: activemq.Artemis, Addrs: []string{srv.Addr()},
		ReceiptTimeout: time.Second, DialTimeout: 10 * time.Second,
		ReconnectBase: time.Millisecond, ReconnectMax: 20 * time.Millisecond, ReconnectGrowth: 1.5,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Config.Validate: %v", err)
	}
	// No supervisor: this test reads the notice the supervisor would.
	b, err := newBackend(cfg.resolve(), nil, driverkit.ClientOwned)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.dial(ctx); err != nil {
		t.Fatalf("the first dial: %v", err)
	}
	b.mu.Lock()
	oldGen := b.connGen
	b.mu.Unlock()
	if err := b.dial(ctx); err != nil {
		t.Fatalf("re-dialling: %v", err)
	}
	b.mu.Lock()
	liveGen := b.connGen
	b.mu.Unlock()

	// The old connection's watcher reports late, and then the connection
	// that is actually in place dies.
	b.reportLostFor(oldGen, errors.New("use of closed network connection"))
	b.reportLostFor(liveGen, errors.New("connection reset by peer"))
	got, ok := b.takeNotice()
	if !ok {
		t.Fatal("two death notices left the supervisor none")
	}
	if got.gen != liveGen {
		t.Fatalf("the waiting notice is about generation %d, want %d: the stale report held the slot and the "+
			"death of the connection the backend is USING was dropped, so nothing would re-dial", got.gen, liveGen)
	}
	if b.stale(got) {
		t.Errorf("the surviving notice is stale, so the supervisor would discard it and sleep")
	}

	// And the other order: the newer report must not be undone by an older
	// one arriving behind it.
	b.reportLostFor(liveGen, errors.New("connection reset by peer"))
	b.reportLostFor(oldGen, errors.New("use of closed network connection"))
	got, ok = b.takeNotice()
	if !ok {
		t.Fatal("two death notices left the supervisor none")
	}
	if got.gen != liveGen {
		t.Errorf("a later report about the OLD generation %d replaced the live one about %d", oldGen, liveGen)
	}
	if _, ok := b.takeNotice(); ok {
		t.Errorf("taking the notice left one behind")
	}
}

// TestAGenerationZeroNoticeIsStaleOnceAConnectionHasBeenDialled pins the
// clause REMOVED, which nothing else can reach.
//
// `stale` used to open with `n.gen != 0 &&`, so a generation-zero notice was
// never stale whatever connection was in place. Restoring that clause on its
// own leaves every other test in this module green, because after no
// caller inside the driver produces a generation-zero notice at all: the one
// that did now names the generation it tore down.
//
// What keeps zero an honest value rather than a hole is [BackendWithClient],
// whose borrowed connection no dial produced and whose generation is
// therefore zero. Both halves are asserted here, because the second is what
// makes the first safe: a zero notice matches the borrowed connection and is
// stale against any connection a dial has since put in place.
func TestAGenerationZeroNoticeIsStaleOnceAConnectionHasBeenDialled(t *testing.T) {
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A backend holding a BORROWED connection: generation zero, because no
	// dial handed it one.
	cfg := Config{
		Broker: activemq.Artemis, Addrs: []string{srv.Addr()},
		ReceiptTimeout: time.Second, DialTimeout: 10 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Config.Validate: %v", err)
	}
	lent, err := newBackend(cfg.resolve(), nil, driverkit.ClientOwned)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	conn, err := lent.dialOne(ctx, nil)
	if err != nil {
		t.Fatalf("dialling a connection to lend: %v", err)
	}
	t.Cleanup(func() { _ = conn.MustDisconnect() })
	b, err := BackendWithClient(ctx, conn, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})

	b.mu.Lock()
	borrowedGen := b.connGen
	b.mu.Unlock()
	if borrowedGen != 0 {
		t.Fatalf("a borrowed connection's generation is %d, want 0: generation zero is only honest while it is "+
			"the generation of a connection no dial produced", borrowedGen)
	}
	zero := lostNotice{gen: 0, cause: errors.New("connection already closed")}
	if b.stale(zero) {
		t.Errorf("a generation-zero notice about the borrowed connection is stale; it is the connection the "+
			"backend holds (generation %d), so nothing would ever repair it", borrowedGen)
	}

	// And once a dial has replaced it, the same notice is stale. This is the
	// assertion the removed clause makes fail.
	if err := b.dial(ctx); err != nil {
		t.Fatalf("dialling a replacement: %v", err)
	}
	b.mu.Lock()
	dialledGen := b.connGen
	b.mu.Unlock()
	if dialledGen == 0 {
		t.Fatalf("a dialled connection's generation is 0, so no notice could ever be told apart from a borrowed " +
			"connection's")
	}
	if !b.stale(zero) {
		t.Errorf("a generation-zero notice is not stale with dialled generation %d in place; the supervisor "+
			"would act on it and tear down a connection that works, which is the defect "+
			"removed the zero case for", dialledGen)
	}
}

// The specification: the rule `Source.fail` states, held to by the two call
// sites that broke it.
//
// A source owns its own connection  and repairs it itself.
// `Source.subscribe` and `settler.finish` nonetheless called
// the backend's own teardown for an error met on that socket -- `nil` to
// `Backend.connectionLostFor`, which means "tear down whatever the BACKEND
// has": a healthy connection disconnected, the
// generation every other handle compares against bumped, every other source's
// deliveries and every sink invalidated -- for a socket none of them was
// using. It is the same user-visible damage as the defect the specification fixed, one
// hop along.
//
// The two call sites get a test each, because each one's assertions have to be
// read while the PUMP is still parked -- see [parkedSource] -- and the repair
// under test is what wakes it.

// deathProbe is the fixture both tests use: a backend with a sink on its
// connection, a source with its own connection and its pump PARKED, and one
// delivery in the caller's hand.
type deathProbe struct {
	b    *Backend
	snk  mq.Sink
	src  *Source
	dead *gostomp.Conn
	d    mq.DriverDelivery

	conn  *gostomp.Conn
	gen   uint32
	redia uint64
	stale uint64
}

// parkedSource builds a [deathProbe] whose source socket is dead and whose
// pump cannot notice, which is what makes both tests deterministic.
//
// # Why the pump has to be parked and not merely raced
//
// The first version of this test closed the source's socket and settled, and
// failed 32 times in 500 runs at `GOMAXPROCS=2` with "acknowledging on a dead
// socket reported success". The pump was the winner: it reads the same socket,
// saw the death first, dropped the connection, and `settler.conn` then DIALLED
// A FRESH ONE -- so the acknowledgement succeeded and the call site under test
// never ran. A test that can silently stop exercising its own subject is worse
// than one that fails.
//
// So the pump is parked first, in the state the specification named: with the
// prefetch at one the token table holds two, and once the caller is holding
// one delivery and the buffer is holding the next, the third has nowhere to go
// and the pump WAITS for a slot. `Source.TokenWaits` counting one is the
// observable event that says it is there, and nothing frees a slot until the
// repair under test does -- so from that moment the pump is not reading the
// socket, cannot see it die, and the call site is the only thing that can act.
// Closing from the CLIENT side is the other half: `watchedConn.Close` disarms
// the death notice, so the source's own watcher cannot act either.
func parkedSource(t *testing.T) *deathProbe {
	t.Helper()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	// The reconnect loop RUNS, so that a backend torn down by mistake is
	// visible as a re-dial rather than as a client that simply stops.
	b, err := OpenBackend(ctx, Config{
		Broker:          activemq.Artemis,
		Addrs:           []string{srv.Addr()},
		ReceiptTimeout:  time.Second,
		DialTimeout:     10 * time.Second,
		ReconnectBase:   time.Millisecond,
		ReconnectMax:    20 * time.Millisecond,
		ReconnectGrowth: 1.5,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})
	// A sink, because the backend's connection is what a sink publishes on:
	// it is the handle a spurious teardown costs.
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	s, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	src, ok := s.(*Source)
	if !ok {
		t.Fatalf("OpenSource returned %T", s)
	}
	dest := src.Destination()
	if err := srv.WaitSubscribed(dest, 1, 10*time.Second); err != nil {
		t.Fatalf("the driver never subscribed to %s: %v", dest, err)
	}

	// Three deliveries against a prefetch of one: one for the caller, one for
	// the buffer, and one the pump cannot find a token for.
	if _, derr := srv.Deliver(dest,
		stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")},
		stompsrv.Message{Body: []byte("three")},
	); derr != nil {
		t.Fatalf("Deliver: %v", derr)
	}
	dst := make([]mq.DriverDelivery, 1)
	n, ferr := src.Fetch(ctx, dst)
	if ferr != nil || n != 1 {
		t.Fatalf("Fetch returned %d, %v; want one delivery", n, ferr)
	}
	deadline := time.Now().Add(10 * time.Second)
	for src.TokenWaits() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the pump never waited for a token (live=%d of %d): this fixture needs it parked, because "+
				"a pump that is still reading the socket repairs the connection before the call site under "+
				"test can", src.table.Live(), src.table.Cap())
		}
		time.Sleep(time.Millisecond)
	}

	b.mu.Lock()
	bconn := b.conn
	b.mu.Unlock()
	src.mu.Lock()
	sconn := src.conn
	src.mu.Unlock()
	if sconn == nil {
		t.Fatal("the source has no connection of its own, so this test cannot kill one")
	}
	if sconn == bconn {
		t.Fatal("the source is using the BACKEND's connection; the specification says it owns its own")
	}
	// From the CLIENT side, so that `watchedConn.Close` disarms the notice.
	_ = sconn.MustDisconnect()
	return &deathProbe{
		b: b, snk: snk, src: src, dead: sconn, d: dst[0],
		conn: bconn, gen: b.Generation(), redia: b.Reconnects(), stale: b.staleNotices.Load(),
	}
}

// check asserts the backend was not touched, and that the source repaired
// itself instead.
func (p *deathProbe) check(t *testing.T, what string) {
	t.Helper()
	p.b.mu.Lock()
	conn := p.b.conn
	p.b.mu.Unlock()
	if conn != p.conn {
		t.Errorf("%s replaced the BACKEND's connection; a source's socket is not the backend's, and every sink "+
			"on it has just been invalidated for a death it never met", what)
	}
	if got := p.b.Generation(); got != p.gen {
		t.Errorf("%s bumped the backend's generation from %d to %d, which invalidates every other handle's "+
			"deliveries", what, p.gen, got)
	}
	if got := p.b.Reconnects(); got != p.redia {
		t.Errorf("%s made the backend re-dial (%d -> %d)", what, p.redia, got)
	}
	if got := p.b.staleNotices.Load(); got != p.stale {
		t.Errorf("%s reported %d death notice(s) to the backend at all; a source's socket death is the source's "+
			"to repair", what, got-p.stale)
	}
	p.src.mu.Lock()
	sconn := p.src.conn
	p.src.mu.Unlock()
	if sconn == p.dead {
		t.Errorf("%s left the source holding the socket that died, so its next frame would go to the same "+
			"place; the repair a source's socket death needs is its OWN", what)
	}
	// And the backend's own connection still works, which is the whole point.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if perr := p.snk.Publish(ctx, &mq.Message{Body: []byte("after")}, nil); perr != nil {
		t.Errorf("publishing after %s: %v; the backend's connection was never the one that died", what, perr)
	}
}

// TestASettlementMeetingItsSourcesSocketDeathDoesNotTearDownTheBackend is
// `settler.finish`'s half.
func TestASettlementMeetingItsSourcesSocketDeathDoesNotTearDownTheBackend(t *testing.T) {
	p := parkedSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if aerr := p.src.Settler().Ack(ctx, &p.d); aerr == nil {
		t.Error("acknowledging on a dead socket reported success, so this run never reached the call site " +
			"under test")
	}
	p.check(t, "a settlement meeting its source's socket death")
}

// TestASubscribeMeetingItsSourcesSocketDeathDoesNotTearDownTheBackend is
// `Source.subscribe`'s half.
func TestASubscribeMeetingItsSourcesSocketDeathDoesNotTearDownTheBackend(t *testing.T) {
	p := parkedSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if serr := p.src.subscribe(ctx); serr == nil {
		t.Error("subscribing on a dead socket reported success, so this run never reached the call site under test")
	}
	p.check(t, "a SUBSCRIBE meeting its source's socket death")
}
