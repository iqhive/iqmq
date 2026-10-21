package redisclient_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"

	"github.com/iqhive/iqmq/benchmarks/redisclient"
)

const (
	benchTopic = "a12"
	benchGroup = "workers"
	benchName  = "c1"
)

// arm is one client library, its own in-memory server, and the server's own
// handle.
//
// Each arm gets a SERVER OF ITS OWN, which is what makes the comparison and
// the model test mean anything: almost every command here is destructive
// (XADD appends, XREADGROUP delivers once, XACK removes), so two arms sharing
// one server would measure the first arm's work and the second arm's empty
// replies.
type arm struct {
	kind   redisclient.Kind
	client redisclient.Client
	fake   *fakestreams.Server
	keys   streams.Keys
	group  string
	// trace is the rendered outcome of every command this side has run, for
	// the differential fuzz target.
	trace []string
}

// newArm starts a server and dials it with one client library.
func newArm(tb testing.TB, k redisclient.Kind) *arm {
	tb.Helper()
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		tb.Fatalf("respsrv.Serve: %v", err)
	}
	tb.Cleanup(func() {
		if cerr := srv.Close(); cerr != nil {
			tb.Errorf("respsrv.Close: %v", cerr)
		}
		for _, perr := range srv.Errors() {
			tb.Errorf("%s broke the protocol: %v", k, perr)
		}
	})
	client, err := redisclient.New(k, srv.Addr())
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	tb.Cleanup(func() {
		if cerr := client.Close(); cerr != nil {
			tb.Errorf("client.Close: %v", cerr)
		}
	})
	return attach(tb, k, client, fake)
}

// attach is the arm of a client already dialled to a server that serves fake:
// the topic's keys and its consumer group, created through the client.
func attach(tb testing.TB, k redisclient.Kind, client redisclient.Client, fake *fakestreams.Server) *arm {
	tb.Helper()
	keys, err := streams.NewKeys(benchTopic, 1)
	if err != nil {
		tb.Fatalf("NewKeys: %v", err)
	}
	group, err := streams.Group(benchGroup)
	if err != nil {
		tb.Fatalf("Group: %v", err)
	}
	a := &arm{kind: k, client: client, fake: fake, keys: keys, group: group}
	if _, err := client.Do(context.Background(), streams.XGroupCreate(a.stream(), group, streams.IDZero, true)); err != nil {
		tb.Fatalf("XGROUP CREATE: %v", err)
	}
	return a
}

// link is one client library dialled to one server for the life of a fuzz
// worker, serving whichever in-memory server the current input owns.
//
// The fuzz target used to start a server and dial a client for every input,
// and at a few hundred inputs a second the closed sockets' TIME_WAIT filled the
// loopback port space: dials and replies then timed out, which the weekly run
// reported as a crasher (39cd6147852ba2f1, "dial tcp ...: i/o timeout") in an
// input that replays clean. A server per input is what the comparison needs,
// and a socket per input is not.
type link struct {
	kind   redisclient.Kind
	client redisclient.Client
	to     *switchboard
}

// newLink starts the shared server and dials it.
func newLink(tb testing.TB, k redisclient.Kind) *link {
	tb.Helper()
	to := &switchboard{}
	to.cur.Store(fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}))
	srv, err := respsrv.Serve(to, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		tb.Fatalf("respsrv.Serve: %v", err)
	}
	tb.Cleanup(func() {
		if cerr := srv.Close(); cerr != nil {
			tb.Errorf("respsrv.Close: %v", cerr)
		}
		for _, perr := range srv.Errors() {
			tb.Errorf("%s broke the protocol: %v", k, perr)
		}
	})
	client, err := redisclient.New(k, srv.Addr())
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	tb.Cleanup(func() {
		if cerr := client.Close(); cerr != nil {
			tb.Errorf("client.Close: %v", cerr)
		}
	})
	return &link{kind: k, client: client, to: to}
}

// arm gives the link a fresh in-memory server and returns its arm.
func (l *link) arm(tb testing.TB) *arm {
	tb.Helper()
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	l.to.cur.Store(fake)
	return attach(tb, l.kind, l.client, fake)
}

// switchboard is the Commander a link's server serves: the current input's
// in-memory server.
type switchboard struct {
	cur atomic.Pointer[fakestreams.Server]
}

func (s *switchboard) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	return s.cur.Load().Do(ctx, c)
}

func (s *switchboard) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	return s.cur.Load().DoMulti(ctx, cmds)
}

func (s *switchboard) Close() error { return nil }

// newDirect is the model's side: the same in-memory server with NO client
// library and no network at all.
//
// It is the oracle both arms are compared against, and it is the right oracle
// because it is the same code the arms reach through a socket: a difference
// between an arm and this one is the client library's, which is the only thing
// the family varies.
func newDirect(tb testing.TB) *arm {
	tb.Helper()
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	keys, err := streams.NewKeys(benchTopic, 1)
	if err != nil {
		tb.Fatalf("NewKeys: %v", err)
	}
	group, err := streams.Group(benchGroup)
	if err != nil {
		tb.Fatalf("Group: %v", err)
	}
	a := &arm{kind: redisclient.Kind(255), client: fake, fake: fake, keys: keys, group: group}
	if _, err := fake.Do(context.Background(), streams.XGroupCreate(a.stream(), group, streams.IDZero, true)); err != nil {
		tb.Fatalf("XGROUP CREATE: %v", err)
	}
	return a
}

func (a *arm) stream() string { return a.keys.Stream(0) }

// xadd is one publication, built exactly as the driver builds it.
func (a *arm) xadd(body string) streams.Cmd {
	return streams.XAdd(streams.XAddArgs{
		Key:    a.stream(),
		Fields: []string{streams.FieldBody, body, streams.FieldKey, "k", streams.FieldTopic, benchTopic},
	})
}
