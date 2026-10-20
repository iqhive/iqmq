package id

import (
	"bytes"
	"encoding/binary"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

// TestModel checks the contract of every kind: canonical text, strict
// parsing, exact round trips, embedded time and sequence, strict
// monotonicity under sequential and concurrent generation, uniqueness over
// a million identifiers, and behaviour when the clock stalls or moves
// backwards.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			t.Run("format", func(t *testing.T) { modelFormat(t, k) })
			t.Run("parse-strict", func(t *testing.T) { modelParseStrict(t, k) })
			t.Run("time", func(t *testing.T) { modelTime(t, k) })
			t.Run("monotonic", func(t *testing.T) { modelMonotonic(t, k) })
			t.Run("clock-stall", func(t *testing.T) { modelClockStall(t, k) })
			t.Run("clock-backwards", func(t *testing.T) { modelClockBackwards(t, k) })
			t.Run("concurrent", func(t *testing.T) { modelConcurrent(t, k) })
			t.Run("collisions", func(t *testing.T) { modelCollisions(t, k) })
			t.Run("sortable", func(t *testing.T) { modelSortable(t, k) })
		})
	}
}

// testGen returns a deterministic generator of kind k over a fake clock.
func testGen(tb testing.TB, k Kind, start time.Time, node uint16) (Generator, *clock.Fake) {
	f := clock.NewFake(start)
	return mustNewWith(tb, k, Config{Clock: f, Seed: 0xA17A17A17A17A17, Node: node}), f
}

var modelStart = time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)

// modelFormat: the text form has the documented length and alphabet, and
// text and binary forms round trip in both directions.
func modelFormat(t *testing.T, k Kind) {
	g, f := testGen(t, k, modelStart, 7)
	for i := range 5000 {
		if i%97 == 0 {
			f.Advance(time.Millisecond)
		}
		v := g.NextValue()
		s := v.String()
		if n := k.TextLen(); n != 0 && len(s) != n {
			t.Fatalf("%q has %d characters, want %d", s, len(s), n)
		}
		if len(s) > k.MaxTextLen() {
			t.Fatalf("%q exceeds MaxTextLen %d", s, k.MaxTextLen())
		}
		back, err := Parse(k, s)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		if back != v {
			t.Fatalf("Parse(%q) = %x, want %x", s, back.Raw(), v.Raw())
		}
		if back.String() != s {
			t.Fatalf("re-format of %q = %q", s, back.String())
		}
		if bb, err := ParseBytes(k, []byte(s)); err != nil || bb != v {
			t.Fatalf("ParseBytes(%q) = %v %v", s, bb, err)
		}
		if got := mustMakeValue(t, k, v.Raw()); got != v {
			t.Fatal("MakeValue does not reproduce the value")
		}
		if len(v.Raw()) != k.Size() {
			t.Fatalf("Raw has %d bytes, want %d", len(v.Raw()), k.Size())
		}
	}
	// Append writes exactly what Next returns, into a caller's buffer.
	buf := make([]byte, 3, 64)
	out := g.Append(buf)
	if len(out) <= 3 || !bytes.Equal(out[:3], buf[:3]) {
		t.Fatal("Append did not append")
	}
	if _, err := Parse(k, string(out[3:])); err != nil {
		t.Fatalf("Append produced %q: %v", out[3:], err)
	}
	if s := g.Next(); len(s) == 0 || g.Kind() != k {
		t.Fatal("Next/Kind")
	}
}

// modelParseStrict: only the canonical rendering parses.
func modelParseStrict(t *testing.T, k Kind) {
	g, _ := testGen(t, k, modelStart, 1)
	good := g.Next()
	bad := []string{"", " ", good + "x", strings.ToUpper(good) + "!"}
	if k.TextLen() != 0 {
		// Fixed-length kinds reject anything of the wrong length; a
		// Snowflake with a digit removed is simply a smaller number.
		bad = append(bad, good[:len(good)-1], strings.Repeat("~", len(good)))
	}
	switch k {
	case UUIDv7:
		bad = append(bad,
			strings.ToUpper(good),                  // upper-case hex
			strings.Replace(good, "-", "0", 1),     // hyphen moved
			"00000000-0000-0000-0000-000000000000", // version 0
			good[:14]+"1"+good[15:],                // version nibble 1
			good[:19]+"0"+good[20:],                // wrong variant
		)
	case ULID:
		bad = append(bad,
			strings.ToLower(good), // lower case is not canonical
			"8"+good[1:],          // overflows 128 bits
			"I"+good[1:], "L"+good[1:], "O"+good[1:], "U"+good[1:],
		)
	case XID:
		bad = append(bad,
			strings.ToUpper(good),
			good[:19]+"1", // padding bits set
			good[:19]+"w", // outside the alphabet
		)
	case Snowflake:
		bad = append(bad, "0"+good, "-1", "+1", "9223372036854775808",
			"99999999999999999999", "1e3", " 1")
	default:
		t.Fatalf("no strict-parse cases for %v", k)
	}
	for _, s := range bad {
		if v, err := Parse(k, s); err == nil {
			t.Fatalf("Parse(%q) accepted as %v", s, v)
		}
	}
}

// modelTime: the embedded timestamp tracks the clock at the kind's
// resolution, for instants decades apart.
func modelTime(t *testing.T, k Kind) {
	for _, start := range []time.Time{
		time.Unix(0, 0).UTC(),
		modelStart,
		time.Date(2050, 3, 4, 5, 6, 7, 0, time.UTC),
	} {
		if k == Snowflake && start.Before(Epoch) {
			continue // the field counts from Epoch; earlier instants clamp
		}
		g, _ := testGen(t, k, start, 3)
		v := g.NextValue()
		want := start.Truncate(k.Resolution())
		if got := v.Time(); !got.Equal(want) {
			t.Fatalf("start %v: Time() = %v, want %v", start, got, want)
		}
	}
	// A Snowflake before the epoch clamps rather than wrapping.
	if k == Snowflake {
		g, _ := testGen(t, k, Epoch.Add(-time.Hour), 0)
		if got := g.NextValue().Time(); !got.Equal(Epoch) {
			t.Fatalf("pre-epoch Time() = %v, want %v", got, Epoch)
		}
	}
}

// modelMonotonic: identifiers strictly increase, in binary and (for
// sortable kinds) in text, whether or not the clock moves.
func modelMonotonic(t *testing.T, k Kind) {
	g, f := testGen(t, k, modelStart, 5)
	prev := g.NextValue()
	prevText := prev.String()
	for i := range 20000 {
		switch i % 400 {
		case 100:
			f.Advance(time.Millisecond)
		case 300:
			f.Advance(2 * time.Second)
		}
		v := g.NextValue()
		if v.Compare(prev) <= 0 {
			t.Fatalf("step %d: %x is not after %x", i, v.Raw(), prev.Raw())
		}
		if s := v.String(); k.Sortable() && s <= prevText {
			t.Fatalf("step %d: text %q is not after %q", i, s, prevText)
		} else {
			prevText = s
		}
		prev = v
	}
}

// modelClockStall: a generator whose clock never moves keeps producing
// distinct, increasing identifiers by borrowing from the future, and the
// borrowed time never exceeds what the sequence width implies.
func modelClockStall(t *testing.T, k Kind) {
	g, _ := testGen(t, k, modelStart, 2)
	const n = 200000
	prev := g.NextValue()
	first := prev
	for range n {
		v := g.NextValue()
		if v.Compare(prev) <= 0 {
			t.Fatalf("stalled clock: %x is not after %x", v.Raw(), prev.Raw())
		}
		prev = v
	}
	borrowed := prev.Time().Sub(first.Time())
	perTick := map[Kind]int64{UUIDv7: 1 << 12, ULID: 1 << 16, XID: 1 << 24, Snowflake: 1 << 12}[k]
	maxBorrow := time.Duration(n/perTick+1) * k.Resolution()
	if borrowed > maxBorrow {
		t.Fatalf("borrowed %v of future time for %d identifiers (limit %v)", borrowed, n, maxBorrow)
	}
}

// modelClockBackwards: a clock that jumps backwards (NTP step, a fake
// clock rewound) must not make identifiers repeat or go backwards.
func modelClockBackwards(t *testing.T, k Kind) {
	g, f := testGen(t, k, modelStart, 4)
	prev := g.NextValue()
	for i := range 5000 {
		if i%50 == 0 {
			f.Set(f.Now().Add(-time.Duration(i%7) * time.Second))
		}
		v := g.NextValue()
		if v.Compare(prev) <= 0 {
			t.Fatalf("clock rewind: %x is not after %x", v.Raw(), prev.Raw())
		}
		prev = v
	}
}

// modelConcurrent: identifiers produced by many goroutines from one
// generator are all distinct, and each goroutine's own sequence increases.
func modelConcurrent(t *testing.T, k Kind) {
	g, _ := testGen(t, k, modelStart, 6)
	const workers, per = 8, 4000
	got := make([][]Value, workers)
	var wg sync.WaitGroup
	for w := range workers {
		got[w] = make([]Value, per)
		wg.Go(func() {
			for i := range per {
				got[w][i] = g.NextValue()
			}
		})
	}
	wg.Wait()
	all := make([]Value, 0, workers*per)
	for w := range workers {
		for i := 1; i < per; i++ {
			if got[w][i].Compare(got[w][i-1]) <= 0 {
				t.Fatalf("worker %d: %x is not after %x", w, got[w][i].Raw(), got[w][i-1].Raw())
			}
		}
		all = append(all, got[w]...)
	}
	slices.SortFunc(all, Value.Compare)
	for i := 1; i < len(all); i++ {
		if all[i].Compare(all[i-1]) == 0 {
			t.Fatalf("duplicate identifier %x", all[i].Raw())
		}
	}
}

// collisionCount is the number of identifiers the collision test
// generates: 1e6 by default, overridable.
func collisionCount(t *testing.T) int {
	if v := os.Getenv("IQMQ_ID_COLLISION_KEYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1000 {
			t.Fatalf("IQMQ_ID_COLLISION_KEYS=%q", v)
		}
		return n
	}
	if testing.Short() {
		return 100000
	}
	return 1000000
}

// modelCollisions generates a million identifiers with the clock running
// and requires every one to be distinct, in binary and in text.
func modelCollisions(t *testing.T, k Kind) {
	n := collisionCount(t)
	g, f := testGen(t, k, modelStart, 1)
	seen := make(map[string]struct{}, n)
	var buf [40]byte
	for i := range n {
		if i%1000 == 0 {
			f.Advance(time.Millisecond)
		}
		s := string(g.Append(buf[:0]))
		if _, dup := seen[s]; dup {
			t.Fatalf("collision after %d identifiers: %q", i, s)
		}
		seen[s] = struct{}{}
	}
	t.Logf("%d identifiers, no collision", n)
}

// modelSortable: sorting the text forms of identifiers created in order
// reproduces that order, for the kinds that claim it.
func modelSortable(t *testing.T, k Kind) {
	g, f := testGen(t, k, modelStart, 1)
	texts := make([]string, 0, 2000)
	for i := range 2000 {
		if i%3 == 0 {
			f.Advance(time.Millisecond)
		}
		texts = append(texts, g.Next())
	}
	sorted := slices.Clone(texts)
	slices.Sort(sorted)
	if k.Sortable() {
		if !slices.Equal(texts, sorted) {
			t.Fatal("text forms do not sort in creation order")
		}
		return
	}
	// Snowflake sorts numerically, not lexicographically.
	values := make([]uint64, len(texts))
	for i, s := range texts {
		v, err := Parse(k, s)
		if err != nil {
			t.Fatal(err)
		}
		values[i] = binary.BigEndian.Uint64(v.Raw())
	}
	if !slices.IsSorted(values) {
		t.Fatal("numeric values are not in creation order")
	}
}

// TestSyncTest drives the generators under the bubble clock: the real
// clock inside a synctest bubble is deterministic, so the generators must
// work with no fake clock at all.
func TestSyncTest(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := mustNewWith(t, k, Config{Seed: 99, Node: 1})
				prev := g.NextValue()
				for range 1000 {
					time.Sleep(time.Millisecond)
					v := g.NextValue()
					if v.Compare(prev) <= 0 {
						t.Fatalf("%x is not after %x", v.Raw(), prev.Raw())
					}
					prev = v
				}
				if got, want := prev.Time().Sub(g.NextValue().Time()), time.Duration(0); got > want {
					t.Fatalf("time went backwards by %v", got)
				}
			})
		})
	}
}
