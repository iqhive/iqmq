package id

import (
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// idState is the op-script state: one generator of a fuzz-chosen kind over
// a fake clock, plus the last identifier and the set of identifiers seen,
// which together carry the monotonicity and uniqueness invariants.
type idState struct {
	kind Kind
	gen  Generator
	clk  *clock.Fake
	last Value
	n    int
	seen map[string]int
}

// fuzzScript drives the generator with clock movement (forwards, stalled
// and backwards), identifier generation through all three entry points,
// and parse/format round trips of both generated and fuzz-supplied text.
var fuzzScript = fuzzutil.Script[*idState]{
	Steps: []fuzzutil.Step[*idState]{
		{Name: "advance", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *idState) {
			s.clk.Advance(r.Duration(2 * time.Second))
		}},
		{Name: "rewind", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *idState) {
			s.clk.Set(s.clk.Now().Add(-r.Duration(3 * time.Second)))
		}},
		{Name: "leap", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *idState) {
			// Within a few ticks either side of the last time the kind's
			// time field holds, as a clock set far ahead reads.
			span := 4 * s.kind.Resolution()
			s.clk.Set(lastTime(s.kind).Add(r.Duration(2*span) - span))
		}},
		{Name: "next", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, s *idState) {
			var v Value
			var text string
			switch r.Choice(3) {
			case 0:
				v = s.gen.NextValue()
				text = v.String()
			case 1:
				text = s.gen.Next()
				parsed, err := Parse(s.kind, text)
				if err != nil {
					t.Fatalf("Next produced unparseable %q: %v", text, err)
				}
				v = parsed
			default:
				buf := make([]byte, r.Int(0, 4))
				out := s.gen.Append(buf)
				text = string(out[len(buf):])
				parsed, err := Parse(s.kind, text)
				if err != nil {
					t.Fatalf("Append produced unparseable %q: %v", text, err)
				}
				v = parsed
			}
			s.check(t, v, text)
		}},
		{Name: "parse", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *idState) {
			text := r.String(48)
			v, err := Parse(s.kind, text)
			if err != nil {
				if errcode.CodeOf(err) != errcode.InvalidArgument {
					t.Fatalf("Parse(%q): code %v", text, errcode.CodeOf(err))
				}
				return
			}
			// A parse that succeeds must be the canonical rendering, and
			// the value must survive both round trips.
			if got := v.String(); got != text {
				t.Fatalf("Parse(%q) re-formats to %q", text, got)
			}
			if again, err := Parse(s.kind, v.String()); err != nil || again != v {
				t.Fatalf("second parse of %q: %v %v", text, again, err)
			}
			if v.Kind() != s.kind || len(v.Raw()) != s.kind.Size() {
				t.Fatalf("Parse(%q) = %v with %d bytes", text, v.Kind(), len(v.Raw()))
			}
			if got := mustMakeValue(t, s.kind, v.Raw()); got != v {
				t.Fatalf("MakeValue round trip of %q", text)
			}
			// Time and Sequence never panic and stay in range.
			if v.Sequence() >= 1<<24 && s.kind != XID {
				t.Fatalf("Parse(%q): sequence %d", text, v.Sequence())
			}
			_ = v.Time()
		}},
		{Name: "compare", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *idState) {
			a := s.gen.NextValue()
			b := s.gen.NextValue()
			s.check(t, a, a.String())
			s.check(t, b, b.String())
			self := a
			if a.Compare(b) >= 0 || b.Compare(a) <= 0 || a.Compare(self) != 0 {
				t.Fatalf("Compare is not a strict order on %x and %x", a.Raw(), b.Raw())
			}
			// Cross-kind comparison is total and antisymmetric.
			other := Kinds()[r.Choice(len(Kinds()))]
			c := mustMakeValue(t, other, make([]byte, other.Size()))
			if a.Compare(c) != -c.Compare(a) {
				t.Fatal("Compare is not antisymmetric across kinds")
			}
		}},
	},
	MaxSteps: 200,
}

// check records one identifier and asserts the invariants that hold for
// every kind: strictly increasing, never seen before, text that parses
// back, and a timestamp within the window the clock has visited.
func (s *idState) check(t testing.TB, v Value, text string) {
	t.Helper()
	if v.Kind() != s.kind {
		t.Fatalf("kind %v, want %v", v.Kind(), s.kind)
	}
	if s.n > 0 && v.Compare(s.last) <= 0 {
		t.Fatalf("%x is not after %x", v.Raw(), s.last.Raw())
	}
	if prev, dup := s.seen[text]; dup {
		t.Fatalf("identifier %q repeated (first at step %d)", text, prev)
	}
	if s.kind.Sortable() && s.n > 0 && text <= s.last.String() {
		t.Fatalf("text %q is not after %q", text, s.last.String())
	}
	s.seen[text] = s.n
	s.last = v
	s.n++
}

// FuzzIDGen drives every generator with fuzz-chosen clock movement and
// operation mixes, and checks the family contract: identifiers are
// unique and strictly increasing (even when the clock stalls or moves
// backwards), the text form round trips through Parse, and parsing
// arbitrary text never panics and accepts only canonical renderings.
func FuzzIDGen(f *testing.F) {
	for ki, k := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Uint64(0x5eed)
		w.Int(0, 1023, 7)
		for range 4 {
			fuzzScript.WriteStep(f, w, "next")
			w.Choice(3, 0)
		}
		fuzzScript.WriteStep(f, w, "advance")
		w.Duration(2*time.Second, time.Millisecond)
		fuzzScript.WriteStep(f, w, "next")
		w.Choice(3, 2)
		w.Int(0, 4, 2)
		fuzzScript.WriteStep(f, w, "rewind")
		w.Duration(3*time.Second, time.Second)
		fuzzScript.WriteStep(f, w, "compare")
		w.Choice(len(Kinds()), ki)
		fuzzScript.WriteStep(f, w, "parse")
		w.String(48, mustNewWith(f, k, Config{Seed: 3, Node: 1}).Next())
		f.Add(w.Data())

		// A clock that crosses the end of the kind's time field.
		w = fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Uint64(0x5eed)
		w.Int(0, 1023, 7)
		fuzzScript.WriteStep(f, w, "leap")
		w.Duration(8*k.Resolution(), 2*k.Resolution())
		for range 4 {
			fuzzScript.WriteStep(f, w, "next")
			w.Choice(3, 0)
			fuzzScript.WriteStep(f, w, "advance")
			w.Duration(2*time.Second, time.Second)
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		seed := r.Uint64()
		node := uint16(r.Int(0, 1023))
		clk := clock.NewFake(modelStart)
		s := &idState{
			kind: k,
			gen:  mustNewWith(t, k, Config{Clock: clk, Seed: seed, Node: node}),
			clk:  clk,
			seen: make(map[string]int),
		}
		fuzzScript.Run(t, data[len(data)-r.Remaining():], s)
	})
}
