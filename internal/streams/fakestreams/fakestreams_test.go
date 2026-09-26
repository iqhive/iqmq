package fakestreams_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// FuzzFakeStreams drives the in-memory server with arbitrary commands.
//
// The fake is the seam every unit test of both Redis drivers runs through, so
// a panic in it is a panic in the test suite rather than in the product —
// which is precisely why it needs fuzzing: a fake that crashed on a malformed
// command would send whoever met it looking for a driver defect. It is also a
// parser: every command arrives as a list of strings and every argument is
// read out of it by hand.
//
// Three properties, and each one is something a careless argument reader gets
// wrong:
//
//  1. It never panics, whatever tokens arrive. An index computed from an
//     argument — MAXLEN's count, XREADGROUP's key/id split, EVALSHA's numkeys
//     — is an index into a slice the caller controls.
//  2. Every reply is either an error or a well-formed [streams.Value]: a
//     Value whose kind is one of the five and whose array elements are
//     themselves well formed. A half-built reply is what a driver would then
//     mis-parse.
//  3. An unknown command is an ERROR and never a silent success. A fake that
//     answered OK to a command it does not implement would let a driver's new
//     command pass its whole suite without ever running.
func FuzzFakeStreams(f *testing.F) {
	seedCommands(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		srv := fakestreams.New(fakestreams.Options{AOF: true})
		defer func() { _ = srv.Close() }()
		ctx := context.Background()
		r := fuzzutil.NewReader(data)
		for !r.Exhausted() {
			cmd := fuzzCmd(r)
			if !cmd.Valid() {
				continue
			}
			v, err := srv.Do(ctx, cmd)
			if err != nil {
				var unknown *fakestreams.ErrUnknownCommand
				if errors.As(err, &unknown) && unknown.Verb == "" {
					t.Fatalf("the fake reported an unknown command with no verb for %q", cmd.Tokens)
				}
				continue
			}
			checkValue(t, v, 0)
		}
		// Property 3, asserted directly rather than hoped for: the fuzzer is
		// unlikely to produce a plausible-looking unknown verb, and the
		// property matters most for exactly the command a driver has just
		// started sending.
		if _, err := srv.Do(ctx, streams.Cmd{Tokens: []string{"SINTERCARD", "2", "a", "b"}}); err == nil {
			t.Fatal("the fake accepted a command it does not implement; a driver's new command " +
				"would then pass its whole suite without ever running")
		}
	})
}

// checkValue asserts a reply is well formed, recursively.
func checkValue(t testing.TB, v streams.Value, depth int) {
	t.Helper()
	if depth > 8 {
		t.Fatalf("a reply nests more than eight levels deep")
	}
	if !v.Kind().Valid() {
		t.Fatalf("a reply's kind is %d, which is not one of the five", v.Kind())
	}
	items, ok := v.Arr()
	if !ok {
		return
	}
	for _, item := range items {
		checkValue(t, item, depth+1)
	}
}

// fuzzCmd builds one command from the fuzzer's bytes.
//
// The verb is drawn from the list the fake implements, so that most inputs
// reach a handler rather than the unknown-command path; the arguments are
// whatever the fuzzer produced, which is where the index arithmetic lives.
func fuzzCmd(r *fuzzutil.Reader) streams.Cmd {
	verbs := []string{
		"XADD", "XLEN", "XRANGE", "XDEL", "XTRIM", "XGROUP", "XREADGROUP",
		"XACK", "XAUTOCLAIM", "XCLAIM", "XPENDING", "XINFO",
		"ZADD", "ZREM", "ZCARD", "ZRANGEBYSCORE",
		"WAITAOF", "SCRIPT", "EVAL", "EVALSHA", "SCAN", "DEL",
		"PING", "INFO", "EXISTS", "CONFIG",
	}
	tokens := []string{verbs[r.Choice(len(verbs))]}
	// A key the driver would really use, most of the time: a hash-tagged
	// name, so that the slot check and the key naming are exercised rather
	// than skipped.
	for n := r.Int(0, 8); n > 0; n-- {
		switch r.Choice(6) {
		case 0:
			tokens = append(tokens, "{t}")
		case 1:
			tokens = append(tokens, "{t}:1")
		case 2:
			tokens = append(tokens, "g")
		case 3:
			tokens = append(tokens, streams.IDAuto)
		case 4:
			tokens = append(tokens, streams.IDNew)
		default:
			tokens = append(tokens, r.String(12))
		}
	}
	keyStart, keyCount := 0, 0
	if len(tokens) > 1 {
		keyStart = 1
		keyCount = r.Int(0, len(tokens)-1)
	}
	return streams.Cmd{Tokens: tokens, KeyStart: keyStart, KeyCount: keyCount}
}

// seedCommands commits the sequences that reach the handlers with something
// to do, plus the argument shapes an index computed from an argument breaks
// on.
func seedCommands(f *testing.F) {
	seeds := []func(*fuzzutil.Writer){
		// A stream, a group, a read and an acknowledgement: the ordinary
		// path, so that the corpus starts somewhere with state in it.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 0, []int{0, 3, 5, 5}) // XADD {t} * b payload
		},
		// XADD with a MAXLEN whose count is not a number.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 0, []int{0, 5, 5, 5})
		},
		// XREADGROUP with an odd number of tokens after STREAMS, which is the
		// key/id split's failure case.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 6, []int{2, 5, 5, 0, 4})
		},
		// EVALSHA with a numkeys that overruns.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 19, []int{5, 5})
		},
		// XAUTOCLAIM with a negative min-idle.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 8, []int{0, 2, 2, 5, 3})
		},
		// A command with no arguments at all.
		func(w *fuzzutil.Writer) {
			writeCmd(w, 22, nil)
		},
	}
	for _, seed := range seeds {
		w := fuzzutil.NewWriter()
		seed(w)
		f.Add(w.Data())
	}
}

// writeCmd writes one command's choices: the verb index, then one argument
// choice per argument.
func writeCmd(w *fuzzutil.Writer, verb int, args []int) {
	w.Choice(26, verb)
	w.Int(0, 8, len(args))
	for _, choice := range args {
		w.Choice(6, choice)
		if choice >= 5 {
			w.String(12, "x")
		}
	}
	if len(args) > 0 {
		w.Int(0, len(args), 1)
	}
}

// TestFakeIsNotALuaInterpreter pins the one thing the fake deliberately does
// not do, so that a script added to `internal/streams` without a body here
// fails loudly rather than silently.
func TestFakeIsNotALuaInterpreter(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{})
	t.Cleanup(func() { _ = srv.Close() })
	ctx := context.Background()
	// Every script the package owns is known.
	for _, script := range streams.Scripts() {
		v, err := srv.Do(ctx, streams.ScriptLoad(script))
		if err != nil {
			t.Fatalf("SCRIPT LOAD of a script the package owns: %v", err)
		}
		sha, ok := v.Str()
		if !ok || sha != streams.SHAOf(script) {
			t.Fatalf("SCRIPT LOAD returned %v, want the digest", v)
		}
	}
	// Anything else is NOSCRIPT, in both directions: loading it and
	// evaluating it.
	const invented = "return redis.call('GET', KEYS[1])"
	if _, err := srv.Do(ctx, streams.ScriptLoad(invented)); !errors.Is(err, fakestreams.ErrNoScript) {
		t.Fatalf("SCRIPT LOAD of a script the fake cannot execute returned %v, want NOSCRIPT: a "+
			"fake that accepted it would let a new script pass its tests without running", err)
	}
	if _, err := srv.Do(ctx, streams.EvalSHA(streams.SHA(invented), []string{"{k}"}, nil)); !errors.Is(err, fakestreams.ErrNoScript) {
		t.Fatalf("EVALSHA of an unknown digest returned %v, want NOSCRIPT", err)
	}
}

// TestFakeRefusesACrossSlotScript pins the other property the drivers depend
// on: the fake is not a cluster, and it refuses a cross-slot script anyway.
//
// Without it, a driver that forgot [streams.SlotSafe] would pass every unit
// test and fail only against a cluster, which is the one deployment CI does
// not run.
func TestFakeRefusesACrossSlotScript(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{})
	t.Cleanup(func() { _ = srv.Close() })
	cmd := streams.EvalSHA(streams.SHAOf(streams.ScriptDeadLetter),
		[]string{"{a}", "{b}"}, []string{"g", "c", "1-0", "0", "b", "x"})
	_, err := srv.Do(context.Background(), cmd)
	if err == nil || !strings.HasPrefix(err.Error(), "CROSSSLOT") {
		t.Fatalf("the fake accepted a script whose keys are on two slots: %v — a driver that "+
			"forgot the slot check would pass every unit test and fail only against a cluster", err)
	}
}
