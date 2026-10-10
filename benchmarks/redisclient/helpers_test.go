package redisclient_test

import (
	"context"
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
