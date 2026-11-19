package redis_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
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

// readKeeper is the in-memory server as a client library sees it. With keep
// set it is a library that sees a call's deadline and not its cancellation,
// as go-redis does with ContextTimeoutEnabled: an XREADGROUP runs to the end
// of its block, and a sweep round to its answer or its deadline, whatever its
// context's cancellation does. Without it a call ends with its context, as on
// rueidis. parked counts the reads at the server, once silent is set nothing
// is answered until gone is closed, and once far is set every other call is
// answered rtt after it is made.
type readKeeper struct {
	*fakestreams.Server
	keep   bool
	parked atomic.Int32
	silent atomic.Bool
	gone   chan struct{}
	far    atomic.Bool
	rtt    time.Duration
}

func newReadKeeper(keep bool) *readKeeper {
	return &readKeeper{
		Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}),
		keep:   keep,
		gone:   make(chan struct{}),
	}
}

func (k *readKeeper) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	read := strings.EqualFold(cmd.Name(), "XREADGROUP")
	if read {
		k.parked.Add(1)
		defer k.parked.Add(-1)
	}
	ctx, cancel := k.uncancelled(ctx)
	defer cancel()
	if err := k.hang(ctx, !read); err != nil {
		return streams.Nil(), err
	}
	return k.Server.Do(ctx, cmd)
}

func (k *readKeeper) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	ctx, cancel := k.uncancelled(ctx)
	defer cancel()
	if err := k.hang(ctx, true); err != nil {
		return nil, err
	}
	return k.Server.DoMulti(ctx, cmds)
}

// uncancelled is ctx as the library sees it: with keep, its deadline and not
// its cancellation, and the deadline a moment early, as the socket deadline a
// library makes of it fires before the context's own timer.
func (k *readKeeper) uncancelled(ctx context.Context) (context.Context, context.CancelFunc) {
	if !k.keep {
		return context.WithCancel(ctx)
	}
	free := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(free, deadline.Add(-socketEarly))
	}
	return context.WithCancel(free)
}

// socketEarly is how much sooner readKeeper's library gives up at a deadline
// than the context does.
const socketEarly = 2 * time.Millisecond

// hang holds a call as the server's state says: until gone while silent,
// and for rtt while far, if travels.
func (k *readKeeper) hang(ctx context.Context, travels bool) error {
	if travels && k.far.Load() {
		t := time.NewTimer(k.rtt)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !k.silent.Load() {
		return nil
	}
	select {
	case <-k.gone:
		return errServerGone
	case <-ctx.Done():
		return ctx.Err()
	}
}

// errServerGone answers what a silent readKeeper was holding once it goes.
var errServerGone = errors.New("the server has gone")

// awaitParked waits until n reads are parked at the server.
func (k *readKeeper) awaitParked(t *testing.T, n int32) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); k.parked.Load() != n; {
		if time.Now().After(deadline) {
			t.Fatalf("setup: %d reads parked at the server, want %d", k.parked.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// rowsOf is how many consumers subscription sub's group has on testTopic's
// one shard.
func rowsOf(t *testing.T, srv *fakestreams.Server, sub string) int {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	group, err := streams.Group(sub)
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

// openSettled opens a source of each subscription on b, publishes one message
// and has each source fetch and acknowledge it, so that every consumer row
// exists and holds nothing, and no source has anything left to read.
func openSettled(t *testing.T, b *rd.Backend, subs ...string) []mq.Source {
	t.Helper()
	srcs := make([]mq.Source, 0, len(subs))
	for _, sub := range subs {
		src, err := b.OpenSource(t.Context(), mq.SourceConfig{
			Topic: testTopic, Subscription: sub, Prefetch: 1, Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("OpenSource %s: %v", sub, err)
		}
		t.Cleanup(func() { _ = src.Close() })
		srcs = append(srcs, src)
	}
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	if err := sink.Publish(t.Context(), &mq.Message{Topic: testTopic, Body: []byte("settled")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for _, src := range srcs {
		d := fetchOne(t, t.Context(), src)
		if err := src.Settler().Ack(t.Context(), &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	return srcs
}

// subs is the subscription names sub0 to sub(n-1).
func subs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "sub" + strconv.Itoa(i)
	}
	return out
}

// parkFetch starts a Fetch with no deadline on src, as a Receive loop
// fetches, and returns what it returns.
func parkFetch(src mq.Source) <-chan error {
	fetched := make(chan error, 1)
	go func() {
		_, err := src.Fetch(context.Background(), make([]mq.DriverDelivery, 1))
		fetched <- err
	}()
	return fetched
}

// TestClosingASourceUnderAParkedFetchRemovesItsRow: a source holding nothing
// is closed while its Fetch is parked in XREADGROUP, which is where a
// consumer of a quiet topic spends its time. Its consumer row must go, as the
// driver page says it does for a source holding nothing.
//
// The close's tidy-up found the read in flight and left the row, because a
// read may yet make the consumer the owner of entries, and nothing tidied up
// once the read returned: every close of an idle consumer left a row for
// good, the name being this source instance's own. Close now ends the read,
// which rueidis does at once when the read's context ends, or, on a client
// that cannot (go-redis), waits for it, at most its block.
func TestClosingASourceUnderAParkedFetchRemovesItsRow(t *testing.T) {
	const block = time.Second
	for _, tc := range []struct {
		name   string
		keep   bool
		within time.Duration
	}{
		{"a read the close ends", false, block / 2},
		{"a read the close cannot end", true, block + time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newReadKeeper(tc.keep)
			b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
				AutoCreate: true, ClaimInterval: time.Hour, Block: block, Clock: fakeClock{k.Server},
			})
			if err != nil {
				t.Fatalf("BackendWithCommander: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			src := openSettled(t, b, testSub)[0]
			fetched := parkFetch(src)
			k.awaitParked(t, 1)
			start := time.Now()
			if err := src.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			took := time.Since(start)
			select {
			case err := <-fetched:
				if !errors.Is(err, mq.ErrClosed) {
					t.Errorf("the parked Fetch returned %v after Close, want ErrClosed", err)
				}
			case <-time.After(2 * block):
				t.Fatalf("the parked Fetch had not returned %v after Close", 2*block)
			}
			rows := rowsOf(t, k.Server, testSub)
			t.Logf("Close took %v; consumer rows after it %d", took.Round(time.Millisecond), rows)
			if rows != 0 {
				t.Errorf("a source holding nothing, closed with its Fetch parked in XREADGROUP, left %d consumer rows", rows)
			}
			if took > tc.within {
				t.Errorf("Close took %v, more than %v (the block is %v)", took, tc.within, block)
			}
		})
	}
}

// TestClosingParkedReadsAgainstASilentServerTakesOneBound: a backend whose
// sources each have a Fetch parked in a read the server never answers closes
// 4.8s after BeginClose, so the shared bound passes while the first source's
// close is under way, and every source after it has a second of its own.
//
// On a client that cannot end a read early (go-redis), a close waits for the
// read no longer than its own bound and no longer than the read's block and a
// second after it, by which time a server that answers has answered. Bounded
// by its own second alone, each close after the shared bound waited a second
// for a read that was never coming, one after another. On a client that ends
// the read (rueidis), the first tidy-up's round trips run out of their second,
// which says the server is not answering, and the closes after it do not wait.
func TestClosingParkedReadsAgainstASilentServerTakesOneBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out most of the 5s bound")
	}
	const sources, block = 5, time.Second
	for _, tc := range []struct {
		name string
		keep bool
	}{
		{"reads the close ends", false},
		{"reads the close cannot end", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newReadKeeper(tc.keep)
			b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
				AutoCreate: true, ClaimInterval: time.Hour, Block: block, Clock: fakeClock{k.Server},
			})
			if err != nil {
				t.Fatalf("BackendWithCommander: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			fetches := make([]<-chan error, 0, sources)
			for _, src := range openSettled(t, b, subs(sources)...) {
				fetches = append(fetches, parkFetch(src))
			}
			k.awaitParked(t, sources)
			b.BeginClose()
			began := time.Now()
			time.Sleep(3500 * time.Millisecond)
			// Each Fetch's next read, within a block from now, is one the
			// server never answers.
			k.silent.Store(true)
			time.Sleep(time.Until(began.Add(4800 * time.Millisecond)))
			start := time.Now()
			_ = b.Close()
			took := time.Since(start)
			close(k.gone)
			for i, fetched := range fetches {
				select {
				case err := <-fetched:
					if !errors.Is(err, mq.ErrClosed) {
						t.Errorf("Fetch %d of a closed source returned %v, want ErrClosed", i, err)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("Fetch %d had not returned once the server went", i)
				}
			}
			t.Logf("Backend.Close 4.8s after BeginClose took %v", took.Round(time.Millisecond))
			// Every read the server never answers began before the close, so
			// its block and the second after it end within a block and a
			// second of the close.
			if bound := block + time.Second; took > bound {
				t.Errorf("Backend.Close of %d sources whose reads the server never answers took %v, more than %v: "+
					"each source after the shared bound waited for the server again", sources, took, bound)
			}
		})
	}
}

// TestAReadAtTheEndOfTheSharedBoundIsWaitedFor: on a client that cannot end
// a read early (go-redis), a read that began 300ms before BeginClose with a
// 5.5s block is due back 200ms after the shared bound, and its source closes
// 300ms before the bound ends. The close waits for the read into the second
// after the bound, which the closing sources share, and removes its row; a
// wait that ended at the bound left the row.
func TestAReadAtTheEndOfTheSharedBoundIsWaitedFor(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out most of the 5s bound")
	}
	const block = 5500 * time.Millisecond
	k := newReadKeeper(true)
	b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, ClaimInterval: time.Hour, Block: block, Clock: fakeClock{k.Server},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := openSettled(t, b, testSub)[0]
	fetched := parkFetch(src)
	k.awaitParked(t, 1)
	read := time.Now()
	time.Sleep(300 * time.Millisecond)
	b.BeginClose()
	time.Sleep(time.Until(read.Add(5 * time.Second)))
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := <-fetched; !errors.Is(err, mq.ErrClosed) {
		t.Errorf("the parked Fetch returned %v after Close, want ErrClosed", err)
	}
	if rows := rowsOf(t, k.Server, testSub); rows != 0 {
		t.Errorf("a source closed 300ms before the shared bound ended, with its read due back 200ms after it, "+
			"left %d consumer rows on a server that answers", rows)
	}
}

// TestATidyUpAfterALongWaitForAReadStillGetsASecond: on a client that cannot
// end a read early (go-redis) and a server that answers every other call
// 150ms after it is made, a read that began 300ms before BeginClose with a
// 5.8s block comes back 500ms after the shared bound, and its source closes
// 300ms before the bound ends. The tidy-up's two round trips (300ms) start
// with the bound passed. They get a second of their own from when they start,
// so the row goes, and the source closed after it removes its row too:
// running out of what a long wait left says nothing about the server.
func TestATidyUpAfterALongWaitForAReadStillGetsASecond(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out most of the 5s bound")
	}
	const block = 5800 * time.Millisecond
	k := newReadKeeper(true)
	k.rtt = 150 * time.Millisecond
	b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, ClaimInterval: time.Hour, Block: block, Clock: fakeClock{k.Server},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	srcs := openSettled(t, b, subs(2)...)
	fetched := parkFetch(srcs[0])
	k.awaitParked(t, 1)
	read := time.Now()
	k.far.Store(true)
	time.Sleep(300 * time.Millisecond)
	b.BeginClose()
	time.Sleep(time.Until(read.Add(5 * time.Second)))
	for i, src := range srcs {
		if err := src.Close(); err != nil {
			t.Errorf("Close %d: %v", i, err)
		}
	}
	if err := <-fetched; !errors.Is(err, mq.ErrClosed) {
		t.Errorf("the parked Fetch returned %v after Close, want ErrClosed", err)
	}
	k.far.Store(false)
	for i, sub := range subs(2) {
		if rows := rowsOf(t, k.Server, sub); rows != 0 {
			t.Errorf("source %d, holding nothing, left %d consumer rows on a server that answered every call", i, rows)
		}
	}
}

// TestASilentServerTakesOneBoundWhenAReadOutlastsIt: against a server that
// answers nothing, on a client that cannot end a read early (go-redis), the
// first source's close waits for a read due back a block (5s) and a second
// after it began, just before BeginClose, and two sources with nothing in
// flight close after it. A read not back when it was due says the server is
// not answering, so they do not wait. Each source closed after the bound got
// five seconds of its own, and then a second, so the first of them added that
// to a close that had already taken the bound and the second after it.
func TestASilentServerTakesOneBoundWhenAReadOutlastsIt(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s bound")
	}
	const block = 5 * time.Second
	k := newReadKeeper(true)
	b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, ClaimInterval: time.Hour, Block: block, Clock: fakeClock{k.Server},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	srcs := openSettled(t, b, subs(3)...)
	k.silent.Store(true)
	fetched := parkFetch(srcs[0])
	t.Cleanup(func() {
		close(k.gone)
		if err := <-fetched; !errors.Is(err, mq.ErrClosed) {
			t.Errorf("the parked Fetch of a closed source returned %v once the server went, want ErrClosed", err)
		}
	})
	k.awaitParked(t, 1)
	b.BeginClose()
	start := time.Now()
	for i, src := range srcs {
		if err := src.Close(); err != nil {
			t.Errorf("Close %d: %v", i, err)
		}
	}
	took := time.Since(start)
	t.Logf("the closes took %v", took.Round(time.Millisecond))
	if bound := 5*time.Second + time.Second + 500*time.Millisecond; took > bound {
		t.Errorf("closing 3 sources against a server that answers nothing took %v, more than the shared bound, "+
			"a second and slack (%v)", took, bound)
	}
	if len(fetched) != 0 {
		t.Error("setup: the Fetch whose read the server never answers returned")
	}
}

// TestSweepRoundsTheCloseCannotEndKeepToTheSharedBound: against a server that
// answers nothing, on a client that cannot end a call early (go-redis), six
// sources sweeping every 2s close one after another after BeginClose, as
// Client.Close closes its consumers, while the ones not closed yet go on
// sweeping. A close waits for its source's round in flight, which go-redis
// ends only at the round's deadline, and each source still open had started
// a new round at its last tick: each close after the shared bound waited out
// one, and the closes added up a round each. A round now ends by the shared
// bound, and none starts after it.
func TestSweepRoundsTheCloseCannotEndKeepToTheSharedBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s bound")
	}
	const sources, interval = 6, 2 * time.Second
	k := newReadKeeper(true)
	b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, ClaimInterval: interval, Block: time.Millisecond, Clock: fakeClock{k.Server},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	srcs := openSettled(t, b, subs(sources)...)
	k.silent.Store(true)
	// Every source has a sweep round in flight that the server never answers.
	time.Sleep(interval + 500*time.Millisecond)
	b.BeginClose()
	start := time.Now()
	each := make([]time.Duration, 0, sources)
	for i, src := range srcs {
		began := time.Now()
		if err := src.Close(); err != nil {
			t.Errorf("Close %d: %v", i, err)
		}
		each = append(each, time.Since(began).Round(time.Millisecond))
	}
	took := time.Since(start)
	t.Logf("the %d closes took %v: %v", sources, took.Round(time.Millisecond), each)
	if bound := 5*time.Second + time.Second + 500*time.Millisecond; took > bound {
		t.Errorf("closing %d sources whose sweep rounds the server never answers took %v, more than the shared "+
			"bound, a second and slack (%v): each close waited out a round its source had just begun", sources, took, bound)
	}
}

// TestASourceClosedAfterTheSharedBoundWaitsASecondForASilentServer: a source
// holding nothing closes 5.5s after BeginClose, as one whose drain
// Client.Close joined can, while the server answers nothing, and no tidy-up
// has yet said so. Its tidy-up waits a second, not five: a source closing
// after the bound had five seconds of its own, so such a close took the whole
// close the bound and five seconds more.
func TestASourceClosedAfterTheSharedBoundWaitsASecondForASilentServer(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s bound")
	}
	k := newReadKeeper(false)
	b, err := rd.BackendWithCommander(t.Context(), k, driverkit.ClientOwned, rd.Config{
		AutoCreate: true, ClaimInterval: time.Hour, Block: time.Millisecond, Clock: fakeClock{k.Server},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := openSettled(t, b, testSub)[0]
	b.BeginClose()
	time.Sleep(5*time.Second + 500*time.Millisecond)
	k.silent.Store(true)
	start := time.Now()
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	took := time.Since(start)
	t.Logf("a close 5.5s after BeginClose against a silent server took %v", took.Round(time.Millisecond))
	if bound := time.Second + 500*time.Millisecond; took > bound {
		t.Errorf("a source closed after the shared bound against a server that answers nothing took %v, more "+
			"than a second and slack (%v)", took, bound)
	}
}

// TestAConsumerClosedWithItsReceiveParkedRemovesItsRow: a consumer of a quiet
// topic, holding nothing, is closed while its Receive or its runner's fetch is
// parked in XREADGROUP, by ConsumerHandle.Close, by its Shutdown or by
// Client.Close. Its consumer row must go. These are the ways a service's
// consumers stop, and each left a row for good, so a group's XINFO CONSUMERS
// grew with every restart and read as a list of crashed processes. With
// IQMQ_REDIS_ADDR set it also runs against that server.
func TestAConsumerClosedWithItsReceiveParkedRemovesItsRow(t *testing.T) {
	t.Run("the in-memory server", func(t *testing.T) {
		fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
		srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		closeParkedConsumers(t, srv.Addr())
	})
	t.Run("IQMQ_REDIS_ADDR", func(t *testing.T) {
		addr := os.Getenv("IQMQ_REDIS_ADDR")
		if addr == "" {
			t.Skip("set IQMQ_REDIS_ADDR to run this against a server")
		}
		closeParkedConsumers(t, addr)
	})
}

// closeParkedConsumers is TestAConsumerClosedWithItsReceiveParkedRemovesItsRow
// against the server at addr.
func closeParkedConsumers(t *testing.T, addr string) {
	const block = time.Second
	raw, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(raw.Close)
	for _, how := range []string{
		"ConsumerHandle.Close", "ConsumerHandle.Shutdown", "Client.Close",
		"a runner, ConsumerHandle.Close", "a runner, Client.Close",
	} {
		t.Run(how, func(t *testing.T) {
			ctx := t.Context()
			topic := "parked." + strconv.FormatInt(time.Now().UnixNano(), 36)
			keys, err := streams.NewKeys(topic, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = raw.Do(context.Background(), raw.B().Del().Key(keys.Stream(0)).Build()).Error() })
			cl, err := rd.Open(ctx, rd.Config{Addrs: []string{addr}, AutoCreate: true, Block: block})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = cl.Close() }()
			c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: testSub, Start: mq.StartEarliest()})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			// One message handled first, so that the row exists on every
			// version (6.2 makes it only when a read hands something over)
			// and holds nothing.
			if err := p.Publish(ctx, &mq.Message{Body: []byte("one")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			done := make(chan error, 1)
			if strings.HasPrefix(how, "a runner") {
				handled := make(chan struct{}, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
						handled <- struct{}{}
						return nil
					}))
				}()
				select {
				case <-handled:
				case <-time.After(5 * time.Second):
					t.Fatal("setup: the runner handled nothing")
				}
			} else {
				first := make([]mq.Delivery, 1)
				if n, err := c.Receive(ctx, first); n != 1 || err != nil {
					t.Fatalf("Receive = %d, %v", n, err)
				}
				if err := first[0].Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
				}
				go func() {
					_, err := c.Receive(context.Background(), make([]mq.Delivery, 4))
					done <- err
				}()
			}
			time.Sleep(300 * time.Millisecond) // the read is parked at the server, and a runner's acknowledgement has landed
			before := groupRows(t, raw, keys, testSub)
			start := time.Now()
			switch how {
			case "ConsumerHandle.Close", "a runner, ConsumerHandle.Close":
				err = c.Close()
			case "ConsumerHandle.Shutdown":
				sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = c.Shutdown(sctx)
				cancel()
			default:
				err = cl.Close()
			}
			took := time.Since(start)
			var ended error
			select {
			case ended = <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("the parked Receive or Run had not returned 5s after %s", how)
			}
			after := groupRows(t, raw, keys, testSub)
			t.Logf("%s = %v after %v (the parked call returned %v); consumer rows before %d, after %d",
				how, err, took.Round(time.Millisecond), ended, before, after)
			if after != 0 {
				t.Errorf("%s of a consumer holding nothing, with its read parked, left %d consumer rows", how, after)
			}
			if took > block+time.Second {
				t.Errorf("%s took %v, more than the block (%v) and a second", how, took, block)
			}
		})
	}
}

// groupRows is how many consumers subscription sub's group has on the first
// shard of keys, read through raw.
func groupRows(t *testing.T, raw rueidis.Client, keys streams.Keys, sub string) int {
	t.Helper()
	group, err := streams.Group(sub)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := raw.Do(context.Background(), raw.B().XinfoConsumers().Key(keys.Stream(0)).Group(group).Build()).ToArray()
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	return len(rows)
}

// TestClosingAClientOfManyParkedConsumersTakesOneBound: Client.Close of 8
// consumers holding nothing, each with a Receive loop whose reads are parked
// at its own point of the default 5s block. Client.Close closes them one after
// another while the ones it has not reached go on reading. The whole close
// must stay within the shared bound and the second after it, and remove every
// row. With IQMQ_REDIS_ADDR set it also runs against that server.
//
// A Receive under a context with a deadline is one rueidis cannot end (it runs
// such a read on its connection without watching the context), so each close
// waited for its read, and the Fetches not yet closed kept starting new ones:
// each close after the bound waited up to its second for a read that had just
// begun, gave up, and left its row. A Fetch now starts only a read that ends
// within the bound once the backend is closing, and the closes share one
// second after it.
func TestClosingAClientOfManyParkedConsumersTakesOneBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a 5s block")
	}
	t.Run("the in-memory server", func(t *testing.T) {
		fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
		srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		closeManyParked(t, srv.Addr())
	})
	t.Run("IQMQ_REDIS_ADDR", func(t *testing.T) {
		addr := os.Getenv("IQMQ_REDIS_ADDR")
		if addr == "" {
			t.Skip("set IQMQ_REDIS_ADDR to run this against a server")
		}
		closeManyParked(t, addr)
	})
}

// closeManyParked is TestClosingAClientOfManyParkedConsumersTakesOneBound
// against the server at addr.
func closeManyParked(t *testing.T, addr string) {
	const consumers = 8
	raw, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(raw.Close)
	for _, deadline := range []bool{false, true} {
		name := "Receive with no deadline"
		if deadline {
			name = "Receive with a one-minute deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			topic := "many." + strconv.FormatInt(time.Now().UnixNano(), 36)
			keys, err := streams.NewKeys(topic, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = raw.Do(context.Background(), raw.B().Del().Key(keys.Stream(0)).Build()).Error() })
			cl, err := rd.Open(ctx, rd.Config{Addrs: []string{addr}, AutoCreate: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = cl.Close() }()
			loops := parkMany(t, cl, topic, consumers, deadline)
			start := time.Now()
			err = cl.Close()
			took := time.Since(start)
			for i, loop := range loops {
				select {
				case <-loop:
				case <-time.After(10 * time.Second):
					t.Fatalf("consumer %d's Receive loop had not returned 10s after Client.Close", i)
				}
			}
			left := 0
			for i := range consumers {
				left += groupRows(t, raw, keys, "s"+strconv.Itoa(i))
			}
			t.Logf("Client.Close of %d consumers = %v after %v; rows left %d", consumers, err, took.Round(time.Millisecond), left)
			if bound := 5*time.Second + time.Second + 500*time.Millisecond; took > bound {
				t.Errorf("Client.Close of %d consumers with parked reads took %v, more than the shared bound, "+
					"the second after it and slack (%v)", consumers, took, bound)
			}
			if left != 0 {
				t.Errorf("Client.Close of %d consumers holding nothing left %d consumer rows", consumers, left)
			}
		})
	}
}

// parkMany opens n consumers of topic on cl, subscriptions s0 to s(n-1), has
// each receive and acknowledge one message, and starts a Receive loop for
// each, at its own point of the default block. It returns a channel per loop
// that is closed when the loop has returned.
func parkMany(t *testing.T, cl *mq.Client, topic string, n int, deadline bool) []chan struct{} {
	t.Helper()
	ctx := t.Context()
	cons := make([]*mq.ConsumerHandle, n)
	for i := range n {
		c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s" + strconv.Itoa(i), Start: mq.StartEarliest()})
		if err != nil {
			t.Fatalf("Consumer %d: %v", i, err)
		}
		cons[i] = c
	}
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, c := range cons {
		d := make([]mq.Delivery, 1)
		if got, err := c.Receive(ctx, d); got != 1 || err != nil {
			t.Fatalf("Receive %d = %d, %v", i, got, err)
		}
		if err := d[0].Ack(); err != nil {
			t.Fatalf("Ack %d: %v", i, err)
		}
	}
	loops := make([]chan struct{}, n)
	for i, c := range cons {
		loops[i] = make(chan struct{})
		go func() {
			defer close(loops[i])
			time.Sleep(time.Duration(n-1-i) * rd.DefaultBlock / time.Duration(n))
			buf := make([]mq.Delivery, 4)
			for {
				rctx, cancel := receiveContext(deadline)
				_, err := c.Receive(rctx, buf)
				cancel()
				if errors.Is(err, mq.ErrClosed) {
					return
				}
				if err != nil {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}
	// Every loop's read is parked, each at its own point of the block.
	time.Sleep(rd.DefaultBlock + time.Second)
	return loops
}

// receiveContext is a Receive's context: with a one-minute deadline, or with
// none.
func receiveContext(deadline bool) (context.Context, context.CancelFunc) {
	if deadline {
		return context.WithTimeout(context.Background(), time.Minute)
	}
	return context.WithCancel(context.Background())
}

// TestAnEntryHandedToAClosingReadIsNotDeleted: against a server
// (IQMQ_REDIS_ADDR that allows DEBUG), a rueidis consumer holding nothing is
// closed while its read is parked, as a Receive loop's ConsumerHandle.Close
// and a runner's Client.Shutdown close one, and the server is busy for a
// moment (DEBUG SLEEP 0.4, standing in for a slow command, a fork or load).
// rueidis ends the read by closing its connection, and the tidy-up and an
// XADD, both on the shared connection, reach the server before it has handled
// that close: the XADD serves the still-blocked read, so the entry is pending
// to the closing consumer.
//
// The tidy-up checked the pending list and then, a round trip later, removed
// the consumer, which deleted that entry: the message was gone from the
// subscription, on 6.2, 7.4 and 8.2 alike, reported only as Close's
// CorruptMessage. The check and the removal are one script now, and the read
// served after it makes the consumer again with the entry pending to it.
//
// The script is the root module's after v1.1.2, so the test skips against
// v1.1.2.
func TestAnEntryHandedToAClosingReadIsNotDeleted(t *testing.T) {
	if v, old := rd.RootModuleBeforeForTest(t, "v1.1.3"); old {
		t.Skipf("this build links root %s, whose tidy-up checks and removes the consumer in two round trips", v)
	}
	addr := os.Getenv("IQMQ_REDIS_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_REDIS_ADDR to a server that allows DEBUG to run this")
	}
	raw, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(raw.Close)
	if err := raw.Do(t.Context(), raw.B().Arbitrary("DEBUG", "SLEEP").Args("0").Build()).Error(); err != nil {
		t.Skipf("the server refuses DEBUG (%v); start it with --enable-debug-command yes", err)
	}
	for _, how := range []string{"a parked Receive, ConsumerHandle.Close", "a runner, Client.Shutdown"} {
		t.Run(how, func(t *testing.T) {
			entryHandedToAClosingRead(t, raw, addr, how)
		})
	}
}

// entryHandedToAClosingRead is one arm of
// TestAnEntryHandedToAClosingReadIsNotDeleted.
func entryHandedToAClosingRead(t *testing.T, raw rueidis.Client, addr, how string) {
	ctx := t.Context()
	topic := "handed." + strconv.FormatInt(time.Now().UnixNano(), 36)
	keys, err := streams.NewKeys(topic, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Do(context.Background(), raw.B().Del().Key(keys.Stream(0)).Build()).Error() })
	cfg := rd.Config{Addrs: []string{addr}, AutoCreate: true}
	opt := rd.ClientOptions(cfg)
	opt.PipelineMultiplex = -1 // one shared connection, which the tidy-up and the XADD share
	client, err := rueidis.NewClient(opt)
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	cl, err := rd.OpenWithClient(ctx, client, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	defer func() { _ = cl.Close() }()
	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: testSub, Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	xadd := func(body string) string {
		id, err := client.Do(context.Background(), client.B().Xadd().Key(keys.Stream(0)).Id("*").
			FieldValue().FieldValue("b", body).Build()).ToString()
		if err != nil {
			t.Errorf("XADD: %v", err)
		}
		return id
	}
	xadd("one")
	done := make(chan error, 1)
	runner := how == "a runner, Client.Shutdown"
	if runner {
		handled := make(chan struct{}, 4)
		go func() {
			done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
				handled <- struct{}{}
				return nil
			}))
		}()
		<-handled
	} else {
		d := make([]mq.Delivery, 1)
		if n, err := c.Receive(ctx, d); n != 1 || err != nil {
			t.Fatalf("Receive = %d, %v", n, err)
		}
		if err := d[0].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		go func() {
			_, err := c.Receive(context.Background(), make([]mq.Delivery, 4))
			done <- err
		}()
	}
	time.Sleep(500 * time.Millisecond) // the read is parked, and a runner's acknowledgement has landed
	slept := make(chan error, 1)
	go func() {
		slept <- raw.Do(context.Background(), raw.B().Arbitrary("DEBUG", "SLEEP").Args("0.4").Build()).Error()
	}()
	time.Sleep(50 * time.Millisecond)
	// A command on the shared connection while the server sleeps, so that the
	// server finds it ready before the read's connection closes.
	got := make(chan error, 1)
	go func() {
		got <- client.Do(context.Background(), client.B().Get().Key(keys.Stream(0)+"x").Build()).Error()
	}()
	time.Sleep(10 * time.Millisecond)
	closed := make(chan error, 1)
	go func() {
		if runner {
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			closed <- cl.Shutdown(sctx)
			return
		}
		closed <- c.Close()
	}()
	time.Sleep(60 * time.Millisecond)
	id := xadd("two") // behind the tidy-up on the shared connection
	cerr := <-closed
	ended := <-done
	if err := <-slept; err != nil {
		t.Errorf("DEBUG SLEEP: %v", err)
	}
	if err := <-got; err != nil && !rueidis.IsRedisNil(err) {
		t.Errorf("GET on the shared connection: %v", err)
	}
	group, err := streams.Group(testSub)
	if err != nil {
		t.Fatal(err)
	}
	pend, err := raw.Do(context.Background(), raw.B().Xpending().Key(keys.Stream(0)).Group(group).Build()).ToArray()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	var pending int64
	if len(pend) > 0 {
		pending, _ = pend[0].AsInt64()
	}
	groups, err := raw.Do(context.Background(), raw.B().XinfoGroups().Key(keys.Stream(0)).Build()).ToArray()
	if err != nil || len(groups) == 0 {
		t.Fatalf("XINFO GROUPS = %v, %v", groups, err)
	}
	m, err := groups[0].AsMap()
	if err != nil {
		t.Fatalf("XINFO GROUPS: %v", err)
	}
	lastMsg := m["last-delivered-id"]
	last, _ := lastMsg.ToString()
	rows := groupRows(t, raw, keys, testSub)
	t.Logf("%s = %v (the parked call returned %v); entry two %s, last delivered %s, pending %d, consumer rows %d",
		how, cerr, ended, id, last, pending, rows)
	if last == id && pending == 0 {
		t.Errorf("entry %s was handed to the closing consumer and deleted with it: the message is gone", id)
	}
}

// latencyCmder is the in-memory server behind a link with a fixed round-trip
// time: once slow is set, every call is answered rtt after it is made, and
// once silent is set nothing is answered. A slow one is a server some
// distance away that answers everything, not a silent one.
type latencyCmder struct {
	*fakestreams.Server
	rtt    time.Duration
	slow   atomic.Bool
	silent atomic.Bool
}

func (c *latencyCmder) wait(ctx context.Context) error {
	if c.silent.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	if !c.slow.Load() {
		return nil
	}
	t := time.NewTimer(c.rtt)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *latencyCmder) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if err := c.wait(ctx); err != nil {
		return streams.Nil(), err
	}
	return c.Server.Do(ctx, cmd)
}

func (c *latencyCmder) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return c.Server.DoMulti(ctx, cmds)
}

// TestATidyUpAtTheEndOfTheSharedBoundDoesNotSkipTheRest: sources holding
// nothing close one after another, starting 4.9s after BeginClose, so the
// first one's tidy-up starts with 100ms of the shared five seconds left.
//
// On a server that answers every call 150ms after it is made, that tidy-up
// (two round trips) ran out of time, which read as a server that was not
// answering: every source closed after it got the shared bound, already
// passed, and left its consumer row on a server that had answered everything.
// A tidy-up now has at least a second, and only one that ran out of that says
// the server is not answering. On a server that answers nothing the closes
// still take that second and not a tidy-up for each.
func TestATidyUpAtTheEndOfTheSharedBoundDoesNotSkipTheRest(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out most of the 5s bound")
	}
	const sources = 3
	for _, tc := range []struct {
		name   string
		silent bool
	}{
		{"a server 150ms away", false},
		{"a server that answers nothing", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
			link := &latencyCmder{Server: srv, rtt: 150 * time.Millisecond}
			b, err := rd.BackendWithCommander(t.Context(), link, driverkit.ClientOwned, rd.Config{
				AutoCreate: true, ClaimInterval: time.Hour, Block: time.Millisecond, Clock: fakeClock{srv},
			})
			if err != nil {
				t.Fatalf("BackendWithCommander: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			srcs := openSettled(t, b, subs(sources)...)
			link.slow.Store(!tc.silent)
			link.silent.Store(tc.silent)
			b.BeginClose()
			time.Sleep(4900 * time.Millisecond)
			start := time.Now()
			for i, src := range srcs {
				if err := src.Close(); err != nil {
					t.Errorf("Close %d: %v", i, err)
				}
			}
			took := time.Since(start)
			link.silent.Store(false)
			left := 0
			for i := range sources {
				left += rowsOf(t, srv, "sub"+strconv.Itoa(i))
			}
			t.Logf("the %d closes took %v and left %d consumer rows", sources, took.Round(time.Millisecond), left)
			if tc.silent {
				if bound := time.Second + 500*time.Millisecond; took > bound {
					t.Errorf("the %d closes against a server that answers nothing took %v, more than %v: "+
						"each tidy-up after the first waited for a server that had not answered", sources, took, bound)
				}
				return
			}
			if left != 0 {
				t.Errorf("%d of %d sources holding nothing left their consumer rows on a server that answered every "+
					"call in 150ms: a tidy-up cut short by the shared bound read as a server not answering", left, sources)
			}
		})
	}
}

// TestClosingManySourcesOnAFarServerRemovesEveryRow: Backend.Close of 20
// sources holding nothing, on a server that answers every call 150ms after
// it is made. Each tidy-up is two round trips, and the teardown runs them
// one after another under the shared bound, so the 17th started with less
// than its two round trips left, ran out of time, read as a server not
// answering, and the ones after it were given a context already ended. The
// control, the same sources closed one by one with no close of the backend
// under way, removes every row too.
func TestClosingManySourcesOnAFarServerRemovesEveryRow(t *testing.T) {
	if testing.Short() {
		t.Skip("takes 20 tidy-ups of 300ms")
	}
	const sources = 20
	for _, arm := range []string{"Backend.Close", "control: Source.Close each"} {
		t.Run(arm, func(t *testing.T) {
			srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
			link := &latencyCmder{Server: srv, rtt: 150 * time.Millisecond}
			b, err := rd.BackendWithCommander(t.Context(), link, driverkit.ClientBorrowed, rd.Config{
				AutoCreate: true, ClaimInterval: time.Hour, Block: time.Millisecond, Clock: fakeClock{srv},
			})
			if err != nil {
				t.Fatalf("BackendWithCommander: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			srcs := openSettled(t, b, subs(sources)...)
			link.slow.Store(true)
			start := time.Now()
			if arm == "Backend.Close" {
				_ = b.Close()
			} else {
				for _, src := range srcs {
					_ = src.Close()
				}
			}
			took := time.Since(start)
			left := 0
			for i := range sources {
				left += rowsOf(t, srv, "sub"+strconv.Itoa(i))
			}
			t.Logf("%s took %v and left %d of %d consumer rows", arm, took.Round(time.Millisecond), left, sources)
			if left != 0 {
				t.Errorf("%s left %d of %d consumer rows of sources holding nothing on a server that answered "+
					"every call in 150ms", arm, left, sources)
			}
		})
	}
}
