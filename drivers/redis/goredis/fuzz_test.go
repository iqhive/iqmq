package goredis_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	redis "github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// FuzzRedisClientReplyEquivalence is the differential this whole module is
// for: one reply, two client libraries, one [streams.Value].
//
// The reply is generated rather than taken from a command, because the reply
// shapes a real server produces are a tiny corner of the shapes the PROTOCOL
// produces — and the corner is exactly where a hand-written case list stops.
// A fuzzer builds an arbitrary Value, a stub Commander answers every command
// with it, `internal/streams/respsrv` writes it on the wire, and both shipped
// adapters read it back. Anything that comes out different is a divergence in
// the driver contract that depends on nothing but the library choice the specification
// calls an implementation detail.
//
// Nesting, empty arrays, nulls inside arrays, binary strings and the RESP3
// types are all reachable, which is where the two libraries could most easily
// differ: a null inside an array is an error value in go-redis and a message
// in rueidis, and a map is a Go map in both with no order to rely on.
func FuzzRedisClientReplyEquivalence(f *testing.F) {
	f.Add([]byte{0})                                  // a null
	f.Add([]byte{1, 5})                               // an integer
	f.Add([]byte{2, 3, 'a', 'b', 'c'})                // a string
	f.Add([]byte{4, 2, 1, 7, 0})                      // an array of an integer and a null
	f.Add([]byte{4, 3, 2, 2, '\r', '\n', 1, 1, 4, 0}) // nesting, binary string, empty array
	f.Add([]byte{3, 1})                               // a boolean
	f.Add([]byte{4, 0})                               // an empty array
	f.Fuzz(func(t *testing.T, data []byte) {
		want := buildValue(fuzzutil.NewReader(data), 0)
		rig, err := fuzzRig()
		if err != nil {
			t.Fatalf("the fuzz rig: %v", err)
		}
		rig.reply.Store(&want)
		ctx := context.Background()
		cmd := streams.Cmd{Tokens: []string{probeVerb, "k"}, KeyStart: 1, KeyCount: 1}
		gv, gerr := rig.goredis.Do(ctx, cmd)
		rv, rerr := rig.rueidis.Do(ctx, cmd)
		if (gerr == nil) != (rerr == nil) {
			t.Fatalf("one client failed and the other did not on %s: goredis %v, rueidis %v",
				render(want), gerr, rerr)
		}
		if gerr != nil {
			return
		}
		if got, wanted := render(gv), render(rv); got != wanted {
			t.Errorf("the two clients rendered one reply differently:\n sent:    %s\n goredis: %s\n rueidis: %s",
				render(want), got, wanted)
		}
	})
}

// fuzzRig is one pair of servers and one client each, built once per process
// and closed by TestMain.
//
// It is a singleton rather than a per-input rig because a per-input rig ran
// the host out of ephemeral ports: two listeners and two connection pools per
// execution, thousands of executions a minute, and the target then failed
// with "bind: address already in use" — a failing input file recording an
// exhausted port range rather than a divergence. Go's fuzzing calls the target
// sequentially within a worker process, so one rig per process is safe.
type rigPair struct {
	reply   atomic.Pointer[streams.Value]
	goredis streams.Commander
	rueidis streams.Commander
	closers []func()
}

var (
	fuzzRigOnce sync.Once
	fuzzRigVal  *rigPair
	fuzzRigErr  error
)

func fuzzRig() (*rigPair, error) {
	fuzzRigOnce.Do(func() {
		p := &rigPair{}
		stub := &holderCommander{pair: p, inner: fakestreams.New(fakestreams.Options{})}
		gsrv, err := respsrv.Serve(stub, respsrv.Options{})
		if err != nil {
			fuzzRigErr = err
			return
		}
		p.closers = append(p.closers, func() { _ = gsrv.Close() })
		rsrv, err := respsrv.Serve(stub, respsrv.Options{})
		if err != nil {
			p.close()
			fuzzRigErr = err
			return
		}
		p.closers = append(p.closers, func() { _ = rsrv.Close() })

		gclient := redis.NewUniversalClient(goredis.UniversalOptions(rd.Config{Addrs: []string{gsrv.Addr()}}))
		p.closers = append(p.closers, func() { _ = gclient.Close() })
		gcmder, err := goredis.NewCommander(gclient)
		if err != nil {
			p.close()
			fuzzRigErr = err
			return
		}
		rclient, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  []string{rsrv.Addr()},
			DisableCache: true,
		})
		if err != nil {
			p.close()
			fuzzRigErr = err
			return
		}
		rcmder, err := rd.NewCommander(rclient)
		if err != nil {
			rclient.Close()
			p.close()
			fuzzRigErr = err
			return
		}
		p.closers = append(p.closers, func() { _ = rcmder.Close() })
		p.goredis, p.rueidis = gcmder, rcmder
		fuzzRigVal = p
	})
	return fuzzRigVal, fuzzRigErr
}

func (p *rigPair) close() {
	for i := len(p.closers) - 1; i >= 0; i-- {
		p.closers[i]()
	}
	p.closers = nil
}

// closeFuzzRig releases the singleton, so that TestMain's goleak check sees a
// process with nothing of this package's running in it.
func closeFuzzRig() {
	if fuzzRigVal != nil {
		fuzzRigVal.close()
		fuzzRigVal = nil
	}
}

// buildValue reads an arbitrary reply out of the fuzzer's bytes.
//
// The depth bound is not decoration: an unbounded generator writes a reply the
// reader recurses into as deeply, and a fuzz target whose failure mode is a
// stack overflow in the CLIENT tells you nothing about the adapter.
func buildValue(r *fuzzutil.Reader, depth int) streams.Value {
	if r.Exhausted() || depth > 4 {
		return streams.Nil()
	}
	switch r.Choice(5) {
	case 0:
		return streams.Nil()
	case 1:
		return streams.Int(int64(r.Uint64()))
	case 2:
		return streams.Str(string(r.Bytes(64)))
	case 3:
		return streams.Bool(r.Bool())
	default:
		n := r.Int(0, 6)
		items := make([]streams.Value, 0, n)
		for range n {
			items = append(items, buildValue(r, depth+1))
		}
		return streams.ArrOf(items)
	}
}

// holderCommander answers XLEN with whatever reply the current input asked
// for and delegates everything else to a real in-memory server.
//
// The delegation is load-bearing rather than tidy. A client library asks a
// server several questions before it will run anything the test cares about --
// rueidis probes CLUSTER SLOTS and falls back to a single connection when the
// server refuses it -- and a stub that answered every command with the
// fuzzer's arbitrary value answered that probe too: an integer where a slot
// map belongs put rueidis into cluster mode against a cluster of no nodes, and
// every input then "diverged" for a reason that had nothing to do with the
// adapters.
type holderCommander struct {
	pair  *rigPair
	inner streams.Commander
}

const probeVerb = "XLEN"

func (c *holderCommander) current() streams.Value {
	if v := c.pair.reply.Load(); v != nil {
		return *v
	}
	return streams.Nil()
}

func (c *holderCommander) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if cmd.Name() == probeVerb {
		return c.current(), nil
	}
	return c.inner.Do(ctx, cmd)
}

func (c *holderCommander) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	out := make([]streams.Result, len(cmds))
	for i, cmd := range cmds {
		if cmd.Name() == probeVerb {
			out[i] = streams.Result{Value: c.current()}
			continue
		}
		v, err := c.inner.Do(ctx, cmd)
		out[i] = streams.Result{Value: v, Err: err}
	}
	return out, nil
}

func (c *holderCommander) Close() error { return nil }
