package redis_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// recorder is a [streams.Commander] that keeps the commands it was handed and
// then runs them against a fake server.
//
// It records them UNMATERIALISED, which is the whole point: the question these
// tests ask is what the sink built, and a recorder that materialised would
// answer it identically whichever way the sink encoded.
type recorder struct {
	srv *fakestreams.Server

	mu   sync.Mutex
	sent []streams.Cmd
}

func (r *recorder) record(cs ...streams.Cmd) {
	r.mu.Lock()
	r.sent = append(r.sent, cs...)
	r.mu.Unlock()
}

func (r *recorder) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	r.record(c)
	return r.srv.Do(ctx, c)
}

func (r *recorder) DoMulti(ctx context.Context, cs []streams.Cmd) ([]streams.Result, error) {
	r.record(cs...)
	return r.srv.DoMulti(ctx, cs)
}

func (r *recorder) Close() error { return r.srv.Close() }

// ServerVersion is forwarded because the backend probes the server before it
// opens anything; a recorder that hid it would be refused at open.
func (r *recorder) ServerVersion(ctx context.Context) (string, string, error) {
	return r.srv.ServerVersion(ctx)
}

// xadds returns the XADDs among the recorded commands, which are the
// publications; everything else is topology and script loading.
func (r *recorder) xadds() []streams.Cmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]streams.Cmd, 0, len(r.sent))
	for _, c := range r.sent {
		if c.Name() == "XADD" {
			out = append(out, c)
		}
	}
	return out
}

// aliasingRecorder answers [streams.ArgAliaser] the way the go-redis adapter
// does, and copyingRecorder does not implement it at all, the way the rueidis
// adapter does not. They are two types rather than one with a flag because
// that is how the question is really asked: by whether the method is there.
type aliasingRecorder struct{ *recorder }

func (aliasingRecorder) AliasArgs() bool { return true }

type copyingRecorder struct{ *recorder }

// newRecordingRig is [newRig] with a recorder between the driver and the fake.
func newRecordingRig(t testing.TB, cmder streams.Commander, srv *fakestreams.Server) *rig {
	t.Helper()
	ctx := context.Background()
	b, err := rd.BackendWithCommander(ctx, cmder, driverkit.ClientOwned, rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: testLease,
		Block:         0,
		AutoCreate:    true,
		Clock:         fakeClock{srv},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &rig{srv: srv, b: b, ctx: ctx}
}

const blobBody = 8 << 10

// TestTheSinkAsksTheCommanderWhetherToCopyTheBody is the driver half.
//
// One driver, one command layer, one server, two commanders: the only thing
// that differs between the two halves is the answer to
// [streams.ArgAliaser.AliasArgs], and the command that goes out has to differ
// with it. The copying half is also the check on the client that needs it:
// mutate the caller's slice after Publish returns and the token the commander
// was given must be unchanged.
func TestTheSinkAsksTheCommanderWhetherToCopyTheBody(t *testing.T) {
	t.Run("copies-when-the-commander-does-not-alias", func(t *testing.T) {
		srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
		rec := &recorder{srv: srv}
		r := newRecordingRig(t, copyingRecorder{rec}, srv)
		sink, _ := r.openPair(t)
		body := bytes.Repeat([]byte{'a'}, blobBody)
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: body}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		cmds := r.check1(t, rec)
		if len(cmds[0].Blobs) != 0 {
			t.Fatalf("the sink built %d blobs for a commander that does not accept them: "+
				"that command sends an empty body", len(cmds[0].Blobs))
		}
		tok := bodyToken(t, cmds[0])
		for i := range body {
			body[i] = 'z'
		}
		if strings.Count(tok, "a") != blobBody {
			t.Errorf("the body token changed when the caller reused its buffer: %d of %d bytes are "+
				"still the published ones, and the specification says the caller may reuse it the instant "+
				"Publish returns", strings.Count(tok, "a"), blobBody)
		}
	})

	t.Run("aliases-when-the-commander-does", func(t *testing.T) {
		srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
		rec := &recorder{srv: srv}
		r := newRecordingRig(t, aliasingRecorder{rec}, srv)
		sink, _ := r.openPair(t)
		body := bytes.Repeat([]byte{'a'}, blobBody)
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: body}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		cmds := r.check1(t, rec)
		if len(cmds[0].Blobs) != 1 {
			t.Fatalf("the sink built %d blobs for a commander that accepts them, want 1: the body "+
				"is still being copied and the batched budget is still paying for it",
				len(cmds[0].Blobs))
		}
		if &cmds[0].Blobs[0].Bytes[0] != &body[0] {
			t.Errorf("the blob does not point at the caller's array, so the body was copied anyway")
		}
		// And the two encodings still describe one entry: materialising the
		// aliased command gives what the copying arm sent.
		if got := bodyOfMaterialised(cmds[0]); strings.Count(got, "a") != blobBody {
			t.Errorf("the aliased command materialises to %d published bytes out of %d",
				strings.Count(got, "a"), blobBody)
		}
	})
}

// check1 returns the recorded publications, insisting there is exactly one.
func (r *rig) check1(t testing.TB, rec *recorder) []streams.Cmd {
	t.Helper()
	cmds := rec.xadds()
	if len(cmds) != 1 {
		t.Fatalf("the sink sent %d XADDs, want 1", len(cmds))
	}
	if !cmds[0].Valid() {
		t.Fatalf("the sink built an invalid XADD: tokens %q blobs %v", cmds[0].Tokens, cmds[0].Blobs)
	}
	return cmds
}

// bodyToken returns the raw token that carries the entry's body.
func bodyToken(t testing.TB, c streams.Cmd) string {
	t.Helper()
	for i := range c.Tokens {
		if c.Tokens[i] == streams.FieldBody && i+1 < len(c.Tokens) {
			return c.Text(i + 1)
		}
	}
	t.Fatalf("no body field in %q", c.Tokens)
	return ""
}

// TestTheReloaderForwardsTheAliasAnswer guards the quiet failure.
//
// Every command goes through the script reloader, so the reloader is what the
// backend asks — and a wrapper that answered for itself would answer "copies"
// for every commander in the repository, leaving the go-redis driver paying
// the copy with every test still green. That is exactly how `Info()` came to
// report rueidis for a go-redis client.
func TestTheReloaderForwardsTheAliasAnswer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inner func(*recorder) streams.Commander
		want  bool
	}{
		{"aliasing", func(r *recorder) streams.Commander { return aliasingRecorder{r} }, true},
		{"copying", func(r *recorder) streams.Commander { return copyingRecorder{r} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
			rec := &recorder{srv: srv}
			r := newRecordingRig(t, tc.inner(rec), srv)
			if got := streams.AliasesArgs(r.b.Commander()); got != tc.want {
				t.Errorf("AliasesArgs through the backend's commander = %v, want %v: the wrapper "+
					"must forward the inner commander's answer, not give one of its own", got, tc.want)
			}
		})
	}
}

// lateWriter is the commander rueidis would be if it were allowed to alias:
// it answers "yes, hand me your bytes" and then reads them after the call
// that supplied them has returned.
//
// It exists as a NEGATIVE CONTROL. Nothing ships it, and the test below
// asserts that it corrupts the entry — because that is the failure the specification
// forbids, it is what `pipe.Do`'s abort path makes reachable on rueidis
// , and a change that made the sink copy unconditionally
// would make this control pass for the wrong reason and say so.
type lateWriter struct {
	*recorder
	queued []streams.Cmd
}

func (w *lateWriter) AliasArgs() bool { return true }

func (w *lateWriter) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if c.Name() != "XADD" {
		return w.recorder.Do(ctx, c)
	}
	w.record(c)
	w.queued = append(w.queued, c)
	// The identifier a queued XADD has not been given yet. The sink only
	// parses it, and what this control is about is the body.
	return streams.Str("1-1"), nil
}

// flush writes what was queued, which is what rueidis's background writer
// does after a cancelled Do has returned.
func (w *lateWriter) flush() []string {
	out := make([]string, 0, len(w.queued))
	for _, c := range w.queued {
		out = append(out, bodyOfMaterialised(c))
	}
	return out
}

func bodyOfMaterialised(c streams.Cmd) string {
	m := c.Materialize()
	for i := range m.Tokens {
		if m.Tokens[i] == streams.FieldBody && i+1 < len(m.Tokens) {
			return m.Tokens[i+1]
		}
	}
	return ""
}

// TestALateWritingCommanderCorruptsTheEntry is why `drivers/redis` copies.
func TestALateWritingCommanderCorruptsTheEntry(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	w := &lateWriter{recorder: &recorder{srv: srv}}
	r := newRecordingRig(t, w, srv)
	sink, _ := r.openPair(t)
	body := bytes.Repeat([]byte{'a'}, blobBody)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: body}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The specification: Publish has returned, so the buffer is the caller's again.
	for i := range body {
		body[i] = 'z'
	}
	written := w.flush()
	if len(written) != 1 {
		t.Fatalf("the late writer queued %d publications, want 1", len(written))
	}
	if strings.Count(written[0], "z") != blobBody {
		t.Errorf("the deferred write put %d mutated bytes on the wire out of %d: this control has "+
			"stopped demonstrating the hazard, so the checks beside it may have stopped "+
			"checking anything", strings.Count(written[0], "z"), blobBody)
	}
}
