package id

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 4 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		if mustNew(t, k).Kind() != k {
			t.Fatalf("New(%v).Kind() = %v", k, mustNew(t, k).Kind())
		}
		if k.Size() == 0 || k.MaxTextLen() == 0 || k.Resolution() <= 0 {
			t.Fatalf("%v: Size/MaxTextLen/Resolution", k)
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if mustNew(t, Auto).Kind() != AutoKind || Auto.Size() != AutoKind.Size() {
		t.Fatal("Auto resolves through New/Size")
	}
	if UUIDv7.Size() != 16 || ULID.Size() != 16 || XID.Size() != 12 || Snowflake.Size() != 8 {
		t.Fatal("Size")
	}
	if UUIDv7.TextLen() != 36 || ULID.TextLen() != 26 || XID.TextLen() != 20 || Snowflake.TextLen() != 0 {
		t.Fatal("TextLen")
	}
	if !UUIDv7.Sortable() || !ULID.Sortable() || !XID.Sortable() || Snowflake.Sortable() {
		t.Fatal("Sortable")
	}
	if XID.Resolution() != time.Second || UUIDv7.Resolution() != time.Millisecond {
		t.Fatal("Resolution")
	}
	if Kind(9).Valid() || Kind(9).Ident() != "" || Kind(9).String() != "kind(?)" || Kind(9).Size() != 0 {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	for _, f := range []func() error{
		func() error { _, err := NewWith(Kind(9), Config{}); return err },
		func() error { _, err := MakeValue(UUIDv7, []byte{1, 2, 3}); return err },
	} {
		if f() == nil {
			t.Fatal("should fail")
		}
	}
	// The zero Value is inert.
	var zero Value
	if !zero.IsZero() || zero.String() != "" || zero.Kind() != Auto || len(zero.Raw()) != 0 ||
		!zero.Time().IsZero() || zero.Sequence() != 0 || zero.Node() != 0 {
		t.Fatal("zero Value")
	}
	if _, err := Parse(Kind(9), "x"); err == nil {
		t.Fatal("Parse with an invalid kind")
	}
	if errcode.CodeOf(ErrInvalidID) != errcode.InvalidArgument {
		t.Fatal("ErrInvalidID code")
	}
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "id")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds id yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds id to %q but AutoKind is %q", slug, AutoKind)
	}
}

var (
	allocSink  int
	valueSink  Value
	stringSink string
)

// TestAllocs pins the hot path at zero allocations: appending an
// identifier into a caller's buffer, producing its binary form and parsing
// one. Next returns a string and therefore allocates exactly once.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			g := mustNewWith(t, k, Config{Clock: clock.NewFake(modelStart), Seed: 5, Node: 3})
			buf := make([]byte, 0, 64)
			text := g.Next()
			if a := testing.AllocsPerRun(200, func() { allocSink += len(g.Append(buf)) }); a != 0 {
				t.Fatalf("Append: %.1f allocs/op", a)
			}
			if a := testing.AllocsPerRun(200, func() { valueSink = g.NextValue() }); a != 0 {
				t.Fatalf("NextValue: %.1f allocs/op", a)
			}
			if a := testing.AllocsPerRun(200, func() {
				v, err := Parse(k, text)
				if err != nil {
					t.Fatal(err)
				}
				valueSink = v
			}); a != 0 {
				t.Fatalf("Parse: %.1f allocs/op", a)
			}
			if a := testing.AllocsPerRun(200, func() { allocSink += len(valueSink.AppendText(buf)) }); a != 0 {
				t.Fatalf("AppendText: %.1f allocs/op", a)
			}
			if a := testing.AllocsPerRun(200, func() { stringSink = g.Next() }); a != 1 {
				t.Fatalf("Next: %.1f allocs/op, want exactly 1", a)
			}
		})
	}
}

// TestKnownAnswers checks each format against a hand-built value, so a
// change in the bit layout is caught rather than merely round-tripped.
func TestKnownAnswers(t *testing.T) {
	// 2026-09-06T10:00:00Z = 1788688800000 ms = 0x1A076291900. The text
	// forms below were produced by an independent implementation of each
	// format, not by this package.
	ms := modelStart.UnixMilli()
	if ms != 1788688800000 {
		t.Fatalf("test fixture drifted: %d", ms)
	}
	cases := []struct {
		kind Kind
		raw  []byte
		text string
	}{
		{
			UUIDv7,
			[]byte{0x01, 0xa0, 0x76, 0x29, 0x19, 0x00, 0x70, 0x01, 0x80, 0x02, 3, 4, 5, 6, 7, 8},
			"01a07629-1900-7001-8002-030405060708",
		},
		{
			ULID,
			[]byte{0x01, 0xa0, 0x76, 0x29, 0x19, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0},
			"01M1V2J680000G000000000000",
		},
		{XID, []byte{0x68, 0xbc, 0x1e, 0x18, 1, 2, 3, 4, 5, 0, 0, 9}, "d2u1s601081g8180004g"},
		{Snowflake, []byte{0, 0x00, 0x36, 0xc9, 0x2b, 0x1f, 0x00, 0x07}, "60237639778311"},
	}
	for _, c := range cases {
		t.Run(c.kind.String(), func(t *testing.T) {
			v := mustMakeValue(t, c.kind, c.raw)
			if got := v.String(); got != c.text {
				t.Fatalf("String() = %q, want %q", got, c.text)
			}
			back, err := Parse(c.kind, c.text)
			if err != nil || back != v {
				t.Fatalf("Parse(%q) = %v %v", c.text, back, err)
			}
		})
	}
	// The timestamp of the UUIDv7 and ULID fixtures is modelStart.
	for _, c := range cases[:2] {
		if got := mustMakeValue(t, c.kind, c.raw).Time(); !got.Equal(modelStart) {
			t.Fatalf("%v Time() = %v, want %v", c.kind, got, modelStart)
		}
	}
}

// TestFields checks the accessors that expose the packed fields.
func TestFields(t *testing.T) {
	f := clock.NewFake(modelStart)
	for _, k := range Kinds() {
		g := mustNewWith(t, k, Config{Clock: f, Seed: 11, Node: 513})
		first := g.NextValue()
		second := g.NextValue()
		if first.Sequence() != 0 || second.Sequence() != 1 {
			t.Fatalf("%v: sequences %d and %d", k, first.Sequence(), second.Sequence())
		}
		if k == Snowflake {
			if first.Node() != 513 {
				t.Fatalf("Node() = %d", first.Node())
			}
		} else if first.Node() != 0 {
			t.Fatalf("%v: Node() = %d", k, first.Node())
		}
	}
	// Two XID generators with different nodes produce different identity
	// bytes, so their identifiers never collide within one second.
	a := mustNewWith(t, XID, Config{Clock: f, Seed: 1, Node: 1}).NextValue()
	b := mustNewWith(t, XID, Config{Clock: f, Seed: 1, Node: 2}).NextValue()
	if bytes.Equal(a.Raw()[4:9], b.Raw()[4:9]) {
		t.Fatal("XID identity bytes ignore Node")
	}
	// The identity bytes are derived, never the process id or host name.
	c := mustNewWith(t, XID, Config{Clock: f, Seed: 2, Node: 1}).NextValue()
	if bytes.Equal(a.Raw()[4:9], c.Raw()[4:9]) {
		t.Fatal("XID identity bytes ignore Seed")
	}
}

// TestDefaultConfig: the zero Config produces a working generator on the
// wall clock with an unpredictable seed. Two independently seeded
// generators of a random-tailed kind never agree; two Snowflake
// generators agree unless they are given different node numbers, which is
// exactly what Config.Node is for.
func TestDefaultConfig(t *testing.T) {
	for _, k := range Kinds() {
		g1, g2 := mustNew(t, k), mustNew(t, k)
		if k == Snowflake {
			g1 = mustNewWith(t, k, Config{Node: 1})
			g2 = mustNewWith(t, k, Config{Node: 2})
		}
		v1, v2 := g1.NextValue(), g2.NextValue()
		if v1.Compare(v2) == 0 {
			t.Fatalf("%v: two default generators produced the same identifier", k)
		}
		if got := v1.Time(); got.Before(time.Now().Add(-time.Minute)) || got.After(time.Now().Add(time.Minute)) {
			t.Fatalf("%v: Time() = %v, not near now", k, got)
		}
	}
}

// TestStdlibOnly keeps the generators inside the layering rules.
func TestStdlibOnly(t *testing.T) {
	testguard.StdlibOnly(t, ".")
	testguard.NoRootImport(t, ".")
}

func mustNew(tb testing.TB, k Kind) Generator {
	tb.Helper()
	return mustNewWith(tb, k, Config{})
}

func mustNewWith(tb testing.TB, k Kind, cfg Config) Generator {
	tb.Helper()
	g, err := NewWith(k, cfg)
	if err != nil {
		tb.Fatalf("NewWith(%v): %v", k, err)
	}
	return g
}

func mustMakeValue(tb testing.TB, k Kind, b []byte) Value {
	tb.Helper()
	v, err := MakeValue(k, b)
	if err != nil {
		tb.Fatalf("MakeValue(%v): %v", k, err)
	}
	return v
}
