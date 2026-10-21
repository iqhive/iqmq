package redisclient_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/streams"

	"github.com/iqhive/iqmq/benchmarks/redisclient"
)

// FuzzRedisClientOps is the differential fuzz target: an op script
// driven against each client library and against the in-memory Commander the
// libraries reach through a socket, with every reply and the server's own
// state compared after every step.
//
// It is the same shape as every other family's fuzz target and the oracle is
// chosen for the same reason: the model must differ from the arm in exactly
// the dimension the family varies. Here that is the client library and the
// network under it, and the oracle is the same `fakestreams` server called
// directly — so a divergence is the library's reading of RESP, its command
// building, or its pipelining, and cannot be the command layer's, which both
// sides share.
//
// The op table is the driver's own commands, because the family's decision is
// about serving THIS driver: a fuzzer that sent arbitrary Redis commands would
// spend its budget on paths no driver takes.
func FuzzRedisClientOps(f *testing.F) {
	seeds := [][]byte{
		{0, 0, 0, 1},          // three publications and a read
		{1, 2, 3, 4, 5, 6, 7}, // one of each op
		{0, 1, 3, 2},          // publish, read, acknowledge
		{0, 1, 8, 4},          // publish, read, advance the clock, reclaim
		{0, 0, 9, 9, 1, 3},    // a pipelined publication and a settlement
		{9, 9, 9},             // pipelines with nothing to pipeline
		{},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	links := make([]*link, 0, len(redisclient.Kinds()))
	for _, k := range redisclient.Kinds() {
		links = append(links, newLink(f, k))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, l := range links {
			runOps(t, l, data)
		}
	})
}

// runOps drives one kind and the oracle through the same script.
func runOps(t *testing.T, l *link, data []byte) {
	t.Helper()
	ctx := context.Background()
	k := l.kind
	oracle := newDirect(t)
	sut := l.arm(t)
	r := fuzzutil.NewReader(data)
	for step := 0; !r.Exhausted() && step < 64; step++ {
		// Every argument the step needs is drawn ONCE, before the two sides
		// run, and this is the whole correctness of the harness. Drawing
		// inside the loop gave the two sides different counts and different
		// clock advances from the same bytes, so the very first seed
		// "diverged" because the script had: seed#4 recorded four replies on
		// one side and six on the other.
		op := r.Choice(10)
		count := r.Int(1, 8)
		advance := time.Duration(r.Int(1, 120)) * time.Second
		payload := strconv.Itoa(step)
		var (
			cmds []streams.Cmd
			ok   bool
		)
		for _, a := range []*arm{oracle, sut} {
			cmds, ok = opCommands(op, a, payload, count)
			if !ok {
				break
			}
			if op == 8 {
				// Advancing the clock is not a command: it is the one thing
				// a test can do to a server that a client cannot, and both
				// servers have to be moved together or every idle time
				// diverges.
				a.fake.Advance(advance)
				continue
			}
			if op == 9 {
				res, err := a.client.DoMulti(ctx, cmds)
				recordMulti(t, a, k, step, cmds, res, err)
				continue
			}
			for _, cmd := range cmds {
				v, err := a.client.Do(ctx, cmd)
				record(t, a, k, step, cmd, v, err)
			}
		}
		if !ok {
			continue
		}
		compare(t, k, step, oracle, sut)
	}
	compare(t, k, -1, oracle, sut)
}

// opCommands builds one step's commands for one server.
func opCommands(op int, a *arm, payload string, n int) ([]streams.Cmd, bool) {
	stream := a.stream()
	switch op {
	case 0:
		return []streams.Cmd{a.xadd("body-" + payload)}, true
	case 1:
		cmd, err := streams.XReadGroup(streams.XReadGroupArgs{
			Group: a.group, Consumer: benchName, Keys: []string{stream},
			IDs: []string{streams.IDNew}, Count: n,
		})
		return []streams.Cmd{cmd}, err == nil
	case 2:
		cmd, err := streams.XReadGroup(streams.XReadGroupArgs{
			Group: a.group, Consumer: benchName, Keys: []string{stream},
			IDs: []string{streams.IDZero}, Count: n,
		})
		return []streams.Cmd{cmd}, err == nil
	case 3:
		ids := a.pendingIDs(n)
		if len(ids) == 0 {
			return nil, false
		}
		return []streams.Cmd{streams.XAck(stream, a.group, ids)}, true
	case 4:
		cmd, err := streams.XAutoClaim(streams.XAutoClaimArgs{
			Key: stream, Group: a.group, Consumer: "sweeper",
			MinIdle: 30 * time.Second, Start: streams.IDZero, Count: n,
		})
		return []streams.Cmd{cmd}, err == nil
	case 5:
		cmd, err := streams.XPending(streams.XPendingArgs{
			Key: stream, Group: a.group, Start: streams.IDMin, End: streams.IDMax, Count: n,
		})
		return []streams.Cmd{cmd}, err == nil
	case 6:
		return []streams.Cmd{{Tokens: []string{"XLEN", stream}, KeyStart: 1, KeyCount: 1}}, true
	case 7:
		return []streams.Cmd{{Tokens: []string{"XINFO", "GROUPS", stream}, KeyStart: 2, KeyCount: 1}}, true
	case 8:
		return nil, true
	default:
		cmds := make([]streams.Cmd, 0, n+1)
		for i := range n {
			cmds = append(cmds, a.xadd("batch-"+payload+"-"+strconv.Itoa(i)))
		}
		return append(cmds, streams.Cmd{Tokens: []string{"XLEN", stream}, KeyStart: 1, KeyCount: 1}), true
	}
}

// pendingIDs is the identifiers this consumer holds, read from the server
// rather than from a reply.
//
// It is read from the SERVER because the two sides must acknowledge the same
// entries: reading them out of the arm's own reply would make the oracle
// acknowledge whatever the arm happened to have, which is the comparison
// arguing with itself.
func (a *arm) pendingIDs(n int) []streams.ID {
	rows := a.fake.Pending(a.stream(), benchGroup)
	if len(rows) > n {
		rows = rows[:n]
	}
	out := make([]streams.ID, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.ID)
	}
	return out
}

// record notes one reply for later comparison.
func record(t *testing.T, a *arm, k redisclient.Kind, step int, cmd streams.Cmd, v streams.Value, err error) {
	t.Helper()
	a.trace = append(a.trace, replyOf(cmd, v, err))
	_ = k
	_ = step
}

// recordMulti notes a pipeline's replies.
func recordMulti(t *testing.T, a *arm, k redisclient.Kind, step int, cmds []streams.Cmd, res []streams.Result, err error) {
	t.Helper()
	if err != nil {
		a.trace = append(a.trace, "multi-failed")
		return
	}
	if len(res) != len(cmds) {
		t.Fatalf("%s step %d: DoMulti returned %d results for %d commands", k, step, len(res), len(cmds))
	}
	for i := range cmds {
		a.trace = append(a.trace, replyOf(cmds[i], res[i].Value, res[i].Err))
	}
}

// replyOf renders one command's outcome. Only WHETHER a command failed is
// recorded, not the text: rueidis strips a leading "ERR " from every error
// reply and go-redis does not, which is and is the driver's
// error table's problem rather than this comparison's.
func replyOf(cmd streams.Cmd, v streams.Value, err error) string {
	if err != nil {
		return cmd.Name() + ":error"
	}
	return cmd.Name() + ":" + render(v)
}

// compare holds the two sides' traces and server states equal.
func compare(t *testing.T, k redisclient.Kind, step int, oracle, sut *arm) {
	t.Helper()
	if len(oracle.trace) != len(sut.trace) {
		t.Fatalf("%s step %d: the oracle recorded %d replies and the arm %d",
			k, step, len(oracle.trace), len(sut.trace))
	}
	for i := range oracle.trace {
		if oracle.trace[i] != sut.trace[i] {
			t.Fatalf("%s step %d, reply %d differs:\n oracle: %s\n arm:    %s",
				k, step, i, oracle.trace[i], sut.trace[i])
		}
	}
	if w, g := renderEntries(oracle), renderEntries(sut); w != g {
		t.Fatalf("%s step %d: the servers hold different entries:\n oracle: %s\n arm:    %s", k, step, w, g)
	}
	if w, g := renderPending(oracle), renderPending(sut); w != g {
		t.Fatalf("%s step %d: the servers hold different pending rows:\n oracle: %s\n arm:    %s", k, step, w, g)
	}
}
