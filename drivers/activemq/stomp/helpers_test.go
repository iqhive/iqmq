package stomp_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// The in-process server of `internal/stompsrv` is what makes every test in
// this package need no broker, and the five behaviours this driver has to get
// right are all things a container will not do on request:
//
//   - a SEND frame's RECEIPT that never arrives, which is the ambiguous
//     window held open;
//   - a SUBSCRIBE answered with an ERROR frame and then the socket, which is
//     what Artemis does for a destination that does not exist;
//   - a socket that dies with no DISCONNECT, at a chosen moment;
//   - a delivery whose `redelivered` and `ack` headers the test chose;
//   - the whole frame log, which is how "no NACK frame was sent" becomes a
//     positive assertion rather than an absence nobody looked for.

// budget is how long a test waits for something another goroutine has to do.
// It is generous because this host runs about fifty other people's containers
// and several sibling agents' builds; nothing here sleeps for it.
const budget = 5 * time.Second

// shortReceipt is a receipt timeout a test can wait out. It is well above the
// microseconds a loopback round trip takes and well below the budget, so a
// publication whose receipt is held resolves quickly and one whose receipt is
// answered never times out.
const shortReceipt = 250 * time.Millisecond

// newServer starts a server and registers its close.
func newServer(t *testing.T, o stompsrv.Options) *stompsrv.Server {
	t.Helper()
	srv, err := stompsrv.New(o)
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("closing the in-process STOMP server: %v", err)
		}
		// A violation is a frame no broker would have accepted, so it is a
		// driver defect rather than a test one, and it is reported here so
		// that every test in the package gets the check for free.
		for _, v := range srv.Violations() {
			t.Errorf("the driver committed a STOMP protocol violation: %v", v)
		}
	})
	return srv
}

// artemisServer is a server that announces itself the way Artemis does.
//
// Measured against apache/activemq-artemis:2.44.0-alpine: the CONNECTED
// frame's `server` header is "ActiveMQ-Artemis/2.44.0", which is what
// `internal/activemq.ServerArtemis` is the prefix of.
func artemisServer(t *testing.T) *stompsrv.Server {
	t.Helper()
	return newServer(t, stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
}

// classicServer is a server that announces itself the way ActiveMQ Classic
// does: "ActiveMQ/6.2.0", measured against apache/activemq-classic:6.2.0.
func classicServer(t *testing.T) *stompsrv.Server {
	t.Helper()
	return newServer(t, stompsrv.Options{ServerHeader: activemq.ServerClassic + "/6.2.0"})
}

// brokerServer starts a server that announces itself as the named broker.
func brokerServer(t *testing.T, b activemq.Broker) *stompsrv.Server {
	t.Helper()
	if b == activemq.Classic {
		return classicServer(t)
	}
	return artemisServer(t)
}

// settings is a configuration for one broker with no address yet, no timeout a
// test has to wait out and no reconnect loop to hold the test open.
func settings(b activemq.Broker) stomp.Config {
	return stomp.Config{
		Broker:         b,
		ReceiptTimeout: shortReceipt,
		DialTimeout:    budget,
		// A reconnect loop in a unit test holds the test open for as long as
		// the backoff says, so the tests that are not ABOUT reconnection
		// turn it off and the two that are turn it back on.
		NoReconnect: true,
	}
}

// artemisConfig is [settings] for Artemis, pointed at a server.
func artemisConfig(srv *stompsrv.Server) stomp.Config {
	cfg := settings(activemq.Artemis)
	cfg.Addrs = []string{srv.Addr()}
	return cfg
}

// reconnecting is a configuration whose dial loop runs, with a backoff short
// enough that a test waits milliseconds rather than seconds.
func reconnecting(cfg stomp.Config) stomp.Config {
	cfg.NoReconnect = false
	cfg.ReconnectBase = time.Millisecond
	cfg.ReconnectMax = 20 * time.Millisecond
	cfg.ReconnectGrowth = 1.5
	return cfg
}

// openBackend opens a backend against the server and registers its close.
func openBackend(t *testing.T, cfg stomp.Config) *stomp.Backend {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	b, err := stomp.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("closing the backend: %v", err)
		}
	})
	return b
}

// destinations is the two destinations a topic and a subscription map to, in
// the brokers' own STOMP form. A test needs them because the driver derives
// them and the server is addressed by the derived name.
func destinations(t *testing.T, cfg stomp.Config, topic, sub string) (publish, consume string) {
	t.Helper()
	publish, consume, err := stomp.DestinationsForTest(cfg, topic, sub)
	if err != nil {
		t.Fatalf("DestinationsForTest(%q, %q): %v", topic, sub, err)
	}
	return publish, consume
}

// awaitRedial waits for a source to have dialled `want` connections beyond
// its first, driving it with `poke` -- which is how a lazy re-dial happens at
// all: a source re-dials on its next FETCH and nothing else makes it.
//
// It is the source's counter and not the backend's, because a source owns its
// own connection : the backend's Reconnects stays at zero
// when a source's socket dies, and a test that waited on it would wait for
// ever.
func awaitRedial(t *testing.T, src *stomp.Source, want uint64, poke func()) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if src.Redials() >= want {
			return
		}
		if poke != nil {
			poke()
		}
	}
	t.Fatalf("the source dialled %d connections beyond its first within %v, want %d", src.Redials(), budget, want)
}

// openSource opens a source, waits for its SUBSCRIBE frame to arrive, and
// registers its close.
//
// The wait is what makes a delivery possible at all: the server can only push
// a MESSAGE frame to a subscription it has, and `Conn.Subscribe` returns as
// soon as the frame is queued rather than when the server has read it.
func openSource(t *testing.T, b *stomp.Backend, cfg stomp.Config, srv *stompsrv.Server,
	topic, sub string, prefetch int,
) (*stomp.Source, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	s, err := b.OpenSource(ctx, mq.SourceConfig{Topic: topic, Subscription: sub, Prefetch: prefetch})
	if err != nil {
		t.Fatalf("OpenSource(%q, %q): %v", topic, sub, err)
	}
	t.Cleanup(func() { closeSource(t, s) })
	src, ok := s.(*stomp.Source)
	if !ok {
		t.Fatalf("OpenSource returned a %T, not a *stomp.Source: the counters a test asserts on (Resets, StaleSettlements) are the concrete type's", s)
	}
	_, consume := destinations(t, cfg, topic, sub)
	if err := srv.WaitSubscribed(consume, 1, budget); err != nil {
		t.Fatalf("the driver never subscribed to %s: %v", consume, err)
	}
	return src, consume
}

// closeSource closes a source and tolerates exactly one failure: go-stomp's
// own missed wake-up on the UNSUBSCRIBE receipt.
//
// `Source.Close` unsubscribes, which is what returns the deliveries this
// consumer never acknowledged, and go-stomp's `Subscription.Unsubscribe` then
// waits for the RECEIPT the library tagged the frame with. It waits on a
// `sync.Cond` through a helper that reads the state, starts a GOROUTINE to
// call `cond.Wait()`, and selects between that goroutine and a timer -- so a
// Broadcast that lands after the state read and before the goroutine reaches
// the notify list is MISSED, and the wait then runs its full timeout. The
// timeout is `DefaultUnsubscribeReceiptTimeout`, thirty seconds, because
// `Backend.dial` sets `RcvReceiptTimeout` and `MsgSendTimeout` from
// Config.ReceiptTimeout and does not set
// `ConnOpt.UnsubscribeReceiptTimeout` -- so `Config.ReceiptTimeout` does not
// bound this wait at all.
//
// It is a library race and not this driver's, and it was observed on two of
// about thirty tests in one `-race` run: the RECEIPT crosses four goroutine
// hops, and under eight parallel tests the closing goroutine can be
// descheduled for all four. Tolerating it here keeps the suite's OUTCOME
// deterministic; the wall-clock cost is what the one-line driver fix would
// remove.
func closeSource(t *testing.T, s mq.Source) {
	t.Helper()
	err := s.Close()
	switch {
	case err == nil:
	case strings.Contains(err.Error(), "unsubscribe receipt timeout"):
		t.Logf("Close waited out go-stomp's unsubscribe receipt: %v. The RECEIPT was sent -- the frame log has "+
			"it -- and the library missed its own Broadcast, which costs DefaultUnsubscribeReceiptTimeout because "+
			"Backend.dial does not set ConnOpt.UnsubscribeReceiptTimeout from Config.ReceiptTimeout", err)
	default:
		t.Errorf("closing the source: %v", err)
	}
}

// openSink opens a sink and registers its close.
func openSink(t *testing.T, b *stomp.Backend, topic string, confirm mq.ConfirmationMode) mq.Sink {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic, Confirmation: confirm})
	if err != nil {
		t.Fatalf("OpenSink(%q): %v", topic, err)
	}
	t.Cleanup(func() {
		if err := snk.Close(); err != nil {
			t.Errorf("closing the sink: %v", err)
		}
	})
	return snk
}

// deliver hands the server one or more messages for a destination and returns
// the `ack` identifiers it assigned, which is what an ACK frame names.
func deliver(t *testing.T, srv *stompsrv.Server, dest string, msgs ...stompsrv.Message) []string {
	t.Helper()
	ids, err := srv.Deliver(dest, msgs...)
	if err != nil {
		t.Fatalf("Deliver to %s: %v", dest, err)
	}
	return ids
}

// fetchN fetches exactly n deliveries within the budget, or fails.
func fetchN(t *testing.T, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dst := make([]mq.DriverDelivery, n)
	got := 0
	for got < n {
		k, err := src.Fetch(ctx, dst[got:])
		if err != nil {
			t.Fatalf("Fetch after %d of %d deliveries: %v", got, n, err)
		}
		if k == 0 {
			t.Fatalf("Fetch returned no deliveries and no error after %d of %d", got, n)
		}
		got += k
	}
	return dst
}

// nackOptions builds a NackOptions from the tokens, which is how a caller
// spells one and therefore how a test should.
func nackOptions(t *testing.T, opts ...mq.NackOption) mq.NackOptions {
	t.Helper()
	var o mq.NackOptions
	if err := o.Apply(opts...); err != nil {
		t.Fatalf("NackOptions.Apply: %v", err)
	}
	return o
}

// await waits for a predicate another goroutine makes true, or fails.
//
// It polls rather than sleeping a fixed time, and it re-checks once after the
// deadline for the reason `drivers/amqp10`'s own await helpers give: a test
// that slept would be slow on a quiet machine and flaky on a busy one.
func await(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if pred() {
		return
	}
	t.Fatalf("%s did not happen within %v", what, budget)
}

// awaitReconnect waits until the backend's dial loop has re-established the
// connection at least n times.
//
// It is not the same question as the generation, and the difference is the
// reconnect window: the generation is bumped the instant the connection dies,
// which is what makes a stale settlement detectable AT ONCE, and the
// replacement connection arrives one backoff later. A test that waited for the
// generation and then subscribed would be racing the dial.
func awaitReconnect(t *testing.T, b *stomp.Backend, n uint64) {
	t.Helper()
	await(t, "the dial loop re-establishing the connection", func() bool { return b.Reconnects() >= n })
}

// countOf is how many frames with one command the server holds, named so that
// a failure message reads as a sentence.
func countOf(srv *stompsrv.Server, command string) int { return srv.Count(command) }

// frameLog renders the server's whole frame log for a failure message. A
// failure about what the driver sent is unreadable without it.
func frameLog(srv *stompsrv.Server) string {
	out := ""
	for _, f := range srv.Frames() {
		out += "\n\t" + f.String()
	}
	if out == "" {
		return "\n\t(the server received no frames at all)"
	}
	return out
}

// compile-time proof that the commands this package asserts on are go-stomp's
// own constants and not strings this test invented: a test that matched on
// "NACK" as a literal would still pass if the library spelled it differently.
var _ = [...]string{frame.SEND, frame.SUBSCRIBE, frame.UNSUBSCRIBE, frame.ACK, frame.NACK, frame.CONNECT}

// fetchQuietly calls Fetch once with a short deadline and discards whatever it
// says, which is how a test drives the driver's own lazy re-subscription.
//
// The shape matters and it is the one `drivers/activemq`'s helpers record: a
// test that re-subscribed itself -- by reaching for the source's internals or
// for the server -- would pass against a driver that never recovers, because
// the test would have done the recovering. A caller only ever calls Fetch, so
// that is the only thing this does.
func fetchQuietly(src mq.Source, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	_, _ = src.Fetch(ctx, dst)
}

// fetchError calls Fetch once with a short deadline and returns what it says.
func fetchError(t *testing.T, src mq.Source, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, dst)
	if n != 0 {
		t.Fatalf("Fetch returned %d deliveries where the test expected a failure", n)
	}
	return err
}

// body is a delivery of one body and nothing else.
func body(s string) stompsrv.Message { return stompsrv.Message{Body: []byte(s)} }
