package yamlmini

import (
	"testing"
)

const sample = `
version: 1   # trailing comment
defaults: {threshold_pct: 3, alpha: 0.05, names: [a, "b c", 'd']}
hosts:
  authoritative: []
  pinning:
    taskset: "8-15"
    numa: 0
gate_sets:
  pr: ["BenchmarkA/*", "BenchmarkB/x"]
budgets:
  - {match: "X/*", ns_op_max: 15, allocs_op_max: 0}
  - match: Y
    ns_op_max: 20
    note: 'it''s fine'
  -
    match: Z
list:
- one
- two
empty:
flag: yes
neg: -3
`

func TestParseSample(t *testing.T) {
	v, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(*Map)
	if !ok {
		t.Fatalf("top level is %T", v)
	}
	if got, _ := Int(m.Values["version"]); got != 1 {
		t.Errorf("version = %v", m.Values["version"])
	}
	d := m.Values["defaults"].(*Map)
	if f, _ := Float(d.Values["alpha"]); f != 0.05 {
		t.Errorf("alpha = %v", d.Values["alpha"])
	}
	if names := Strings(d.Values["names"]); len(names) != 3 || names[1] != "b c" || names[2] != "d" {
		t.Errorf("names = %v", names)
	}
	h := m.Values["hosts"].(*Map)
	if a := h.Values["authoritative"].([]any); len(a) != 0 {
		t.Errorf("authoritative = %v", a)
	}
	if p := h.Values["pinning"].(*Map); String(p.Values["taskset"]) != "8-15" {
		t.Errorf("taskset = %v", p.Values["taskset"])
	}
	gs := m.Values["gate_sets"].(*Map)
	if pr := Strings(gs.Values["pr"]); len(pr) != 2 || pr[0] != "BenchmarkA/*" {
		t.Errorf("pr = %v", pr)
	}
	b := Maps(m.Values["budgets"])
	if len(b) != 3 {
		t.Fatalf("budgets = %d entries", len(b))
	}
	if String(b[0].Values["match"]) != "X/*" {
		t.Errorf("budget0 = %v", b[0].Values)
	}
	if f, _ := Float(b[1].Values["ns_op_max"]); f != 20 || String(b[1].Values["note"]) != "it's fine" {
		t.Errorf("budget1 = %v", b[1].Values)
	}
	if String(b[2].Values["match"]) != "Z" {
		t.Errorf("budget2 = %v", b[2].Values)
	}
	if l := Strings(m.Values["list"]); len(l) != 2 || l[1] != "two" {
		t.Errorf("list = %v", l)
	}
	if e, ok := m.Get("empty"); !ok || e != nil {
		t.Errorf("empty = %v,%v", e, ok)
	}
	if f, _ := Bool(m.Values["flag"]); !f {
		t.Error("flag not true")
	}
	if n, _ := Int(m.Values["neg"]); n != -3 {
		t.Errorf("neg = %v", m.Values["neg"])
	}
	if m.Keys[0] != "version" || m.Keys[len(m.Keys)-1] != "neg" {
		t.Errorf("key order lost: %v", m.Keys)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"a:\n\tb: 1",
		"a: 1\n  b: 2",
		"- a\nb: 1",
		"x: {a: 1",
		"x: [a, b",
		"just a scalar\nkey: v",
	}
	for _, s := range bad {
		if _, err := Parse([]byte(s)); err == nil {
			t.Errorf("no error for %q", s)
		}
	}
	if v, err := Parse(nil); err != nil || v != nil {
		t.Errorf("empty doc = %v, %v", v, err)
	}
	if v, err := Parse([]byte("# only a comment\n")); err != nil || v != nil {
		t.Errorf("comment-only doc = %v, %v", v, err)
	}
}

func TestScalarConversions(t *testing.T) {
	if String(nil) != "" || String(int64(3)) != "3" || String(2.5) != "2.5" || String(true) != "true" {
		t.Error("String conversions")
	}
	if _, ok := Float("x"); ok {
		t.Error("Float accepted non-number")
	}
	if _, ok := Int(2.5); ok {
		t.Error("Int accepted fraction")
	}
	if s := Strings("solo"); len(s) != 1 || s[0] != "solo" {
		t.Error("Strings on scalar")
	}
	if Maps("x") != nil {
		t.Error("Maps on scalar")
	}
	if m := Maps(&Map{}); len(m) != 1 {
		t.Error("Maps on single map")
	}
	var nilMap *Map
	if _, ok := nilMap.Get("k"); ok {
		t.Error("nil map Get")
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(sample))
	f.Add([]byte("a: [1, {b: c}]\n"))
	f.Add([]byte("- - x\n"))
	f.Add([]byte("k: \"unterminated\n"))
	f.Add([]byte("gates: [a, b], [c]\n"))
	f.Add([]byte("auto: {queue: x}}\n"))
	f.Add([]byte("threshold_pct: 3\nthreshold_pct: 50\n"))
	f.Add([]byte("areas:\n  - area: core\n    area: drivers\n"))
	f.Add([]byte("k: \"abc\" def\n"))
	f.Add([]byte("k: a: b\n"))
	f.Add([]byte("42\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := Parse(data)
		if err != nil {
			return
		}
		// Re-encoding is not supported; just walk the tree to make sure it is
		// well formed.
		var walk func(any, int)
		walk = func(x any, depth int) {
			if depth > 64 {
				t.Fatal("tree too deep")
			}
			switch y := x.(type) {
			case *Map:
				if len(y.Keys) != len(y.Values) {
					t.Fatalf("map keys/values mismatch: %d vs %d", len(y.Keys), len(y.Values))
				}
				for _, k := range y.Keys {
					walk(y.Values[k], depth+1)
				}
			case []any:
				for _, e := range y {
					walk(e, depth+1)
				}
			}
		}
		walk(v, 0)
	})
}
