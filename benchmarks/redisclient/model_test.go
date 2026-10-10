package redisclient_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/testguard"

	"github.com/iqhive/iqmq/benchmarks/redisclient"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(redisclient.Kinds()))
	for _, k := range redisclient.Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestKindProperties holds the enumeration to the shape every family's kind
// type has: distinct slugs, distinct identifiers, a round trip through
// ParseKind, and nothing outside the enumeration being Valid.
func TestKindProperties(t *testing.T) {
	kinds := redisclient.Kinds()
	if len(kinds) != 2 {
		t.Fatalf("Kinds() = %v, want the two client libraries compares", kinds)
	}
	seen := map[string]bool{}
	for _, k := range kinds {
		if !k.Valid() {
			t.Errorf("%v is not Valid", k)
		}
		slug := k.String()
		if seen[slug] {
			t.Errorf("two kinds share the slug %q; the slug is the benchmark-name segment and the bake-off's identifier", slug)
		}
		seen[slug] = true
		if k.Ident() == "" || k.Ident() == "Kind(?)" {
			t.Errorf("%v has no Go identifier for the generated binding", k)
		}
		back, ok := redisclient.ParseKind(slug)
		if !ok || back != k {
			t.Errorf("ParseKind(%q) = %v, %v", slug, back, ok)
		}
	}
	if _, ok := redisclient.ParseKind("nosuchclient"); ok {
		t.Error("ParseKind accepted a name that is not a kind")
	}
	var beyond redisclient.Kind = 200
	if beyond.Valid() {
		t.Error("a value outside the enumeration reports Valid")
	}
	if _, err := redisclient.New(beyond, "127.0.0.1:1"); err == nil {
		t.Error("New accepted a kind outside the enumeration")
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds this family
// (front-matter `auto: {redisclient: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "redisclient")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds redisclient yet; AutoKind=%v (provisional=%v)",
			redisclient.AutoKind, redisclient.AutoProvisional)
		return
	}
	if slug != redisclient.AutoKind.String() {
		t.Fatalf("ADR binds redisclient to %q but AutoKind is %q", slug, redisclient.AutoKind)
	}
}

// script is the command sequence the model test drives, and it is the driver's
// own: a publication, a group read, an ownership-guarded acknowledgement, a
// reclaim sweep and the introspection the admin surface uses. A sequence of
// invented commands would compare the libraries on commands no driver sends.
func script(a *arm, n int) []streams.Cmd {
	stream := a.stream()
	cmds := make([]streams.Cmd, 0, 8+n)
	for i := range n {
		cmds = append(cmds, a.xadd("body-"+strconv.Itoa(i)))
	}
	read, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: a.group, Consumer: benchName, Keys: []string{stream},
		IDs: []string{streams.IDNew}, Count: n,
	})
	if err != nil {
		panic(err)
	}
	claim, err := streams.XAutoClaim(streams.XAutoClaimArgs{
		Key: stream, Group: a.group, Consumer: "c2", Start: streams.IDZero, Count: 16,
	})
	if err != nil {
		panic(err)
	}
	pending, err := streams.XPending(streams.XPendingArgs{
		Key: stream, Group: a.group, Start: streams.IDMin, End: streams.IDMax, Count: n,
	})
	if err != nil {
		panic(err)
	}
	return append(cmds,
		streams.Cmd{Tokens: []string{"XLEN", stream}, KeyStart: 1, KeyCount: 1},
		read,
		pending,
		claim,
		streams.Cmd{Tokens: []string{"XINFO", "GROUPS", stream}, KeyStart: 2, KeyCount: 1},
		streams.Cmd{Tokens: []string{"XRANGE", stream, streams.IDMin, streams.IDMax}, KeyStart: 1, KeyCount: 1},
	)
}

// TestModel drives each kind and the direct in-memory Commander through the
// same script and holds their answers equal.
//
// This is the family's correctness gate, and refuses to
// measure a kind whose TestModel subtest fails. What it proves is the only
// thing that can disqualify an arm here: a client library that reads the same
// server differently is a driver that sees different messages, whatever its
// throughput.
func TestModel(t *testing.T) {
	for _, k := range redisclient.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			ctx := context.Background()
			want := newDirect(t)
			got := newArm(t, k)
			wantCmds, gotCmds := script(want, 5), script(got, 5)
			for i := range wantCmds {
				wv, werr := want.client.Do(ctx, wantCmds[i])
				gv, gerr := got.client.Do(ctx, gotCmds[i])
				if (werr == nil) != (gerr == nil) {
					t.Fatalf("step %d (%s): the oracle says %v and %s says %v",
						i, wantCmds[i].Name(), werr, k, gerr)
				}
				if werr != nil {
					continue
				}
				if w, g := render(wv), render(gv); w != g {
					t.Errorf("step %d (%s) differs:\n oracle:  %s\n %-7s: %s",
						i, wantCmds[i].Name(), w, k.String(), g)
				}
			}
			// And the servers agree afterwards, which says the arm sent the
			// same bytes rather than merely read the same replies.
			if w, g := renderEntries(want), renderEntries(got); w != g {
				t.Errorf("the two servers hold different entries:\n oracle:  %s\n %-7s: %s", w, k.String(), g)
			}
			if w, g := renderPending(want), renderPending(got); w != g {
				t.Errorf("the two servers hold different pending rows:\n oracle:  %s\n %-7s: %s", w, k.String(), g)
			}
		})
	}
}

// TestPipeliningIsTheSameAnswerAsOneCommandAtATime holds DoMulti against Do.
//
// A pipeline is not a transaction: Redis runs each command independently and
// reports each outcome, and the two libraries build a pipeline in completely
// different ways — rueidis multiplexes onto its one connection, go-redis takes
// a connection and writes the batch. A pipeline whose results were misaligned,
// or that stopped at the first failure, would report one settlement's outcome
// for another.
func TestPipeliningIsTheSameAnswerAsOneCommandAtATime(t *testing.T) {
	for _, k := range redisclient.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			ctx := context.Background()
			serial, batched := newArm(t, k), newArm(t, k)
			cmds := script(batched, 4)
			// One refusal in the middle, so that alignment is observable
			// rather than assumed.
			cmds = append(cmds[:2], append([]streams.Cmd{{Tokens: []string{"XACK"}}}, cmds[2:]...)...)
			results, err := batched.client.DoMulti(ctx, cmds)
			if err != nil {
				t.Fatalf("DoMulti: %v", err)
			}
			if len(results) != len(cmds) {
				t.Fatalf("DoMulti returned %d results for %d commands", len(results), len(cmds))
			}
			serialCmds := script(serial, 4)
			serialCmds = append(serialCmds[:2], append([]streams.Cmd{{Tokens: []string{"XACK"}}}, serialCmds[2:]...)...)
			for i := range serialCmds {
				v, derr := serial.client.Do(ctx, serialCmds[i])
				if (derr == nil) != (results[i].Err == nil) {
					t.Fatalf("command %d (%s): serial says %v and pipelined says %v",
						i, serialCmds[i].Name(), derr, results[i].Err)
				}
				if derr != nil {
					continue
				}
				if s, p := render(v), render(results[i].Value); s != p {
					t.Errorf("command %d (%s) differs between serial and pipelined:\n serial:    %s\n pipelined: %s",
						i, serialCmds[i].Name(), s, p)
				}
			}
		})
	}
}

// renderEntries writes a stream's entries out in full.
func renderEntries(a *arm) string {
	var b strings.Builder
	for _, e := range a.fake.Entries(a.stream()) {
		b.WriteString("{")
		for i := 0; i+1 < len(e.Fields); i += 2 {
			b.WriteString(e.Fields[i] + "=" + e.Fields[i+1] + " ")
		}
		b.WriteString("} ")
	}
	return b.String()
}

// renderPending writes a group's pending rows out, consumer included: the
// consumer name is what the guarded acknowledgement compares.
func renderPending(a *arm) string {
	var b strings.Builder
	for _, p := range a.fake.Pending(a.stream(), benchGroup) {
		b.WriteString(p.Consumer + "x" + strconv.FormatUint(uint64(p.Delivered), 10) + " ")
	}
	return b.String()
}

// render writes a reply out in full, bodies included, so that a difference in
// one byte of one field fails the comparison.
//
// An entry's identifier is NOT rendered: it is the server's millisecond clock
// and a sequence within it, and the two arms run against two servers whose
// clocks were started independently. Everything else about a reply is
// comparable, and the identifiers are compared for SHAPE instead.
func render(v streams.Value) string {
	switch v.Kind() {
	case streams.KindNil:
		return "nil"
	case streams.KindInt:
		n, _ := v.Int64()
		return "int(" + strconv.FormatInt(n, 10) + ")"
	case streams.KindBool:
		b, _ := v.Bool()
		return "bool(" + strconv.FormatBool(b) + ")"
	case streams.KindString:
		s, _ := v.Str()
		if _, err := streams.ParseID(s); err == nil {
			return "id(...)"
		}
		return "str(" + strconv.Quote(s) + ")"
	case streams.KindArray:
		items, _ := v.Arr()
		var b strings.Builder
		b.WriteString("[")
		for i, item := range items {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(render(item))
		}
		b.WriteString("]")
		return b.String()
	}
	return "?"
}
