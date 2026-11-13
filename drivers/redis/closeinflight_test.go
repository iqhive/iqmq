package redis_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// stallCmder is the fake behind respsrv with a stall: once stall is set, a
// command named verb runs and its connection then waits for release before
// the reply is written. The server has applied the command and the client
// hears nothing, as from a server that hangs or a link that drops replies.
type stallCmder struct {
	*fakestreams.Server
	verb    string
	stall   atomic.Bool
	release chan struct{}
	ran     atomic.Int32
}

func (c *stallCmder) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	v, err := c.Server.Do(ctx, cmd)
	if c.stall.Load() && strings.EqualFold(cmd.Name(), c.verb) {
		c.ran.Add(1)
		select {
		case <-c.release:
		case <-ctx.Done():
		}
	}
	return v, err
}

// TestAPublicationInFlightWhenTheBackendClosesIsUnknown: publications whose
// XADDs the server ran, and whose replies had not come back, are still in
// flight when the backend closes. rueidis's Close waits a second for the
// connection to drain, closes it, and answers every command still waiting for
// its reply with ErrClosing, written or not.
//
// The driver mapped ErrClosing to a definite CodeClosed, because
// driverkit.ClassifyNet does not read it as a transport failure, so each such
// publication was reported as never made although its entry is in the stream,
// and a caller that published it again stored it twice. Each is now an
// unknown outcome, still CodeClosed.
func TestAPublicationInFlightWhenTheBackendClosesIsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n        int
		deadline bool
	}{
		// No deadline: rueidis hands the command to its background writer.
		{"one publication without a deadline", 1, false},
		// More than one call in flight: the background writer again.
		{"three publications with deadlines", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) { publishInFlightAtClose(t, tc.n, tc.deadline) })
	}
}

func publishInFlightAtClose(t *testing.T, n int, withDeadline bool) {
	fake := &stallCmder{
		Server:  fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}),
		verb:    "XADD",
		release: make(chan struct{}),
	}
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	var once sync.Once
	release := func() { once.Do(func() { close(fake.release) }) }
	t.Cleanup(release)
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{srv.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	b, err := rd.BackendWithClient(t.Context(), client, driverkit.ClientOwned, rd.Config{AutoCreate: true})
	if err != nil {
		client.Close()
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Topic: testTopic, Body: []byte("before")}, nil); err != nil {
		t.Fatalf("Publish before the stall: %v", err)
	}
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	fake.stall.Store(true)

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(context.Background())
			if withDeadline {
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			}
			defer cancel()
			errs[i] = sink.Publish(ctx, &mq.Message{Topic: testTopic, Body: []byte("in flight")}, nil)
		})
	}
	for deadline := time.Now().Add(5 * time.Second); fake.ran.Load() == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			release()
			wg.Wait()
			t.Fatal("setup: the server ran none of the publications")
		}
	}
	time.Sleep(100 * time.Millisecond) // the others reach the socket
	_ = b.Close()
	wg.Wait()
	release()
	// What the client wrote before its connection closed is still read and
	// run by the server.
	stored := 0
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if stored = len(fake.Entries(keys.Stream(0))) - 1; stored == n {
			break
		}
	}
	if stored == 0 {
		t.Fatal("setup: none of the publications in flight reached the stream, so the test proves nothing")
	}
	for i, err := range errs {
		if err == nil {
			continue
		}
		if !mq.IsAmbiguous(err) {
			t.Errorf("publication %d in flight when the backend closed = %v (ambiguous false), and %d of %d are in "+
				"the stream: a publication written before the close has an unknown outcome", i, err, stored, n)
		}
		if errors.Is(err, rueidis.ErrClosing) && !errors.Is(err, mq.ErrClosed) {
			t.Errorf("publication %d = %v: rueidis's closing answer no longer reads as ErrClosed", i, err)
		}
	}
}

// TestASettlementAnsweredClosingIsUnknown: rueidis answers a settlement still
// waiting for its reply at the close with ErrClosing, as it does a
// publication (TestAPublicationInFlightWhenTheBackendClosesIsUnknown), and
// the settlement may have landed, so its outcome is unknown. It was a
// definite CodeClosed, and the core reads a definite failure as a settlement
// that did not happen.
func TestASettlementAnsweredClosingIsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		verb   string
		settle func(context.Context, mq.Settler, *mq.DriverDelivery) error
	}{
		{"ack", "EVALSHA", func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error { return s.Ack(ctx, d) }},
		{"dead-letter nack", "EVALSHA", func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		}},
		{"extend", "EVALSHA", func(ctx context.Context, s mq.Settler, d *mq.DriverDelivery) error {
			return s.(mq.DeadlineExtender).Extend(ctx, d, time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, func(c *rd.Config) { c.DeadLetterSuffix = rd.DefaultDeadLetterSuffix })
			sink, src := r.openPair(t)
			if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("settled at the close")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)
			r.srv.Fail(tc.verb, rueidis.ErrClosing)
			err := tc.settle(r.ctx, src.Settler(), &d)
			if !mq.IsAmbiguous(err) || !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("%s answered ErrClosing = %v (ambiguous %v, ErrClosed %v), want an unknown outcome "+
					"that still reads as ErrClosed", tc.name, err, mq.IsAmbiguous(err), errors.Is(err, mq.ErrClosed))
			}
			// The flag itself, which mq.IsRetryable cannot show once the
			// error is ambiguous: a closed client is not a condition a
			// repeat gets past.
			if oe, ok := errors.AsType[*mq.OpError](err); !ok || oe.Retryable {
				t.Errorf("%s answered ErrClosing = %#v, want an *mq.OpError with Retryable false", tc.name, err)
			}
		})
	}
}

// silentCmder is the fake behind respsrv that, once silent is set, answers
// nothing: every command waits until gone is closed, as on a server that has
// stopped answering (a partition, a hung process).
type silentCmder struct {
	*fakestreams.Server
	silent atomic.Bool
	gone   chan struct{}
}

func (c *silentCmder) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if c.silent.Load() {
		select {
		case <-c.gone:
		case <-ctx.Done():
		}
		return streams.Nil(), ctx.Err()
	}
	return c.Server.Do(ctx, cmd)
}

// TestClosingAgainstASilentServerTakesOneBound: closing a backend with
// several sources open while the server answers nothing takes one source's
// bound, not one for each source.
//
// Backend.Close closed its sources one after another, and each source's close
// waited for its sweep round in flight, whose context came from Background
// and ran out only at the sweep interval, and then for its own tidy-up,
// closeTimeout (5s): a minute for four sources at the default 30s deadline.
// Client.Close closes its consumers one after another before the backend, so
// it did the same. A closing source now ends its round at once, and the
// tidy-ups after the backend began to close (Backend.BeginClose, which
// Client.Close calls first, or Close) share one closeTimeout.
func TestClosingAgainstASilentServerTakesOneBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s tidy-up")
	}
	const sources, interval = 3, time.Second
	// One source's own bound, its 5s tidy-up, and some slack. The client is
	// lent to the backend, so the close does not include rueidis's own, which
	// waits a second for each connection with a command in flight.
	const bound = 5*time.Second + 2*time.Second
	for _, tc := range []struct {
		name  string
		close func(b *rd.Backend, c *mq.Client) error
	}{
		{"Backend.Close", func(b *rd.Backend, _ *mq.Client) error { return b.Close() }},
		{"Client.Close", func(_ *rd.Backend, c *mq.Client) error { return c.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &silentCmder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}), gone: make(chan struct{})}
			srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
			if err != nil {
				t.Fatalf("respsrv.Serve: %v", err)
			}
			client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{srv.Addr()}, DisableCache: true})
			if err != nil {
				_ = srv.Close()
				t.Fatalf("rueidis.NewClient: %v", err)
			}
			t.Cleanup(client.Close)
			// Before the client's close, which would otherwise wait for the
			// silent server too.
			t.Cleanup(func() { close(fake.gone); _ = srv.Close() })
			b, err := rd.BackendWithClient(t.Context(), client, driverkit.ClientBorrowed, rd.Config{
				AutoCreate: true, AckDeadline: 3 * interval, ClaimInterval: interval, Block: 500 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			c := mq.NewClient(b)
			t.Cleanup(func() { _ = c.Close() })
			for i := range sources {
				if _, err := c.Consumer(t.Context(), mq.ConsumerConfig{
					Topic: testTopic, Subscription: "sub" + strconv.Itoa(i), Prefetch: 4, Start: mq.StartEarliest(),
				}); err != nil {
					t.Fatalf("Consumer %d: %v", i, err)
				}
			}
			fake.silent.Store(true)
			time.Sleep(interval + 200*time.Millisecond) // every source has a sweep round in flight
			start := time.Now()
			_ = tc.close(b, c)
			took := time.Since(start)
			t.Logf("%s took %v", tc.name, took.Round(time.Millisecond))
			if took > bound {
				t.Errorf("%s with %d sources against a server that answers nothing took %v, more than one "+
					"source's bound (%v): the sources' closes waited one after another", tc.name, sources, took, bound)
			}
		})
	}
}

// consumerRows is how many consumers the subscription's group has on
// testTopic's one shard.
func consumerRows(t *testing.T, srv *fakestreams.Server) int {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	group, err := streams.Group(testSub)
	if err != nil {
		t.Fatal(err)
	}
	v, err := srv.Do(context.Background(), streams.Cmd{
		Tokens: []string{"XINFO", "CONSUMERS", keys.Stream(0), group}, KeyStart: 2, KeyCount: 1,
	})
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	rows, _ := v.Arr()
	return len(rows)
}

// TestASourceClosedLongAfterBeginCloseStillTidiesUp: on a server that
// answers, a source that closes after the bound BeginClose started has
// passed still removes its consumer row. A handle's close can come that late
// with nothing wrong: Client.Close tells the backend first and then waits for
// a consumer's own Shutdown still draining, and under either a close another
// caller began may reach its source after BeginClose.
//
// Every close after BeginClose used the same deadline, passed or not, so
// such a source's tidy-up got a context already ended and left its consumer
// row, which no later source reuses, for good. The bound is now shared only
// while it lasts or once a tidy-up has run out of time
// (TestClosingAgainstASilentServerTakesOneBound is that side).
func TestASourceClosedLongAfterBeginCloseStillTidiesUp(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s bound")
	}
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("settled")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	r.b.BeginClose()
	time.Sleep(5*time.Second + 500*time.Millisecond)
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if n := consumerRows(t, r.srv); n != 0 {
		t.Errorf("a source closed 5.5s after BeginClose, holding nothing, left %d consumer rows behind on a "+
			"server that answers: its tidy-up was given a deadline already passed", n)
	}
}

// TestBeginCloseLeavesTheBackendWorking: after BeginClose, which may come
// while a drain is still running, the source still settles, extends and
// fetches, the sink still publishes, BeginClose may be called again and
// beside Close, and Close works.
func TestBeginCloseLeavesTheBackendWorking(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	for _, body := range []string{"a", "b", "c"} {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	d1 := fetchOne(t, r.ctx, src)
	d2 := fetchOne(t, r.ctx, src)
	r.b.BeginClose()
	r.b.BeginClose()
	set := src.Settler()
	if err := set.(mq.DeadlineExtender).Extend(r.ctx, &d1, time.Second); err != nil {
		t.Errorf("Extend after BeginClose: %v", err)
	}
	if err := set.Ack(r.ctx, &d1); err != nil {
		t.Errorf("Ack after BeginClose: %v", err)
	}
	if err := set.Nack(r.ctx, &d2, mq.NackOptions{Action: mq.NackActionRequeue}); err != nil {
		t.Errorf("Nack after BeginClose: %v", err)
	}
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("d")}, nil); err != nil {
		t.Errorf("Publish after BeginClose: %v", err)
	}
	d3 := fetchOne(t, r.ctx, src)
	if err := set.Ack(r.ctx, &d3); err != nil {
		t.Errorf("Ack of a fetch made after BeginClose: %v", err)
	}
	done := make(chan struct{})
	go func() { r.b.BeginClose(); close(done) }()
	if err := r.b.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	<-done
	r.b.BeginClose()
}

// roundHolder is the fake server holding the first round trip made once it is
// armed until that call's context ends, as a server that stopped answering
// holds a sweep round.
type roundHolder struct {
	*fakestreams.Server
	armed atomic.Bool
	held  chan struct{}
}

func (h *roundHolder) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if h.hold(ctx) {
		return streams.Nil(), ctx.Err()
	}
	return h.Server.Do(ctx, c)
}

func (h *roundHolder) DoMulti(ctx context.Context, cs []streams.Cmd) ([]streams.Result, error) {
	if h.hold(ctx) {
		return nil, ctx.Err()
	}
	return h.Server.DoMulti(ctx, cs)
}

func (h *roundHolder) hold(ctx context.Context) bool {
	if !h.armed.CompareAndSwap(true, false) {
		return false
	}
	close(h.held)
	<-ctx.Done()
	return true
}

// TestClosingASourceEndsItsSweepRound: a source's close ends the sweep round
// in flight rather than waiting it out. The round's context came from
// Background with the sweep interval as its timeout, so against a server that
// had stopped answering a close waited up to an interval, ten seconds at the
// defaults, before its own tidy-up. A round the close ended is not a failed
// one, and the health does not say so.
func TestClosingASourceEndsItsSweepRound(t *testing.T) {
	const interval = 2 * time.Second
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	holder := &roundHolder{Server: srv, held: make(chan struct{})}
	b, err := rd.BackendWithCommander(t.Context(), holder, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, AckDeadline: 3 * interval, ClaimInterval: interval, Block: time.Millisecond, Clock: fakeClock{srv},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: testTopic, Subscription: testSub, Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	holder.armed.Store(true)
	select {
	case <-holder.held:
	case <-time.After(3 * interval):
		t.Fatal("setup: no sweep round started")
	}
	start := time.Now()
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if took := time.Since(start); took > interval/2 {
		t.Errorf("Close with a sweep round in flight took %v: it waited the round out (the interval is %v)", took, interval)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Errorf("Health after the close = %v (%v), want Ready: the round the close ended did not fail", h.State, h.Err)
	}
}

// TestAClosingAnswerElsewhereStaysDefinite: ErrClosing answering a call that
// sends nothing a caller could repeat into a duplicate (opening a source, an
// administrative call) is still the definite CodeClosed it was.
func TestAClosingAnswerElsewhereStaysDefinite(t *testing.T) {
	r := newRig(t)
	r.srv.Fail("XGROUP CREATE", rueidis.ErrClosing)
	_, err := r.b.OpenSource(r.ctx, mq.SourceConfig{Topic: testTopic, Subscription: testSub, Prefetch: 1})
	if mq.IsAmbiguous(err) || !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("OpenSource answered ErrClosing = %v (ambiguous %v), want a definite ErrClosed", err, mq.IsAmbiguous(err))
	}
}
