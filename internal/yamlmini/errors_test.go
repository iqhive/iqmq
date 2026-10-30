package yamlmini

import (
	"math"
	"strings"
	"testing"
)

// yamlmini parses iqmq's OWN gate configuration -- hack/coverage-floors.yaml,
// hack/fuzz-ratchet.yaml, benchmarks/budgets.yaml -- so a document it reads
// WRONGLY is a gate that stops gating. This repository has two recorded
// instances of exactly that shape (the drivers coverage area that matched
// nothing and the `|| echo "::warning::"` that made every figure advisory), and
// both were silent.
//
// So the contract these tests hold is: a document this parser cannot represent
// is an ERROR THAT NAMES THE LINE, never a partial parse. A parser that skipped
// a line it did not understand would drop a floor, a budget or a ratchet entry
// and every downstream check would then pass over a config nobody wrote.

// parseErr parses src and returns the error, failing if there was none.
func parseErr(t *testing.T, src string) string {
	t.Helper()
	v, err := Parse([]byte(src))
	if err == nil {
		t.Fatalf("Parse(%q) succeeded and returned %#v; a document this parser cannot represent must be refused, not partly read", src, v)
	}
	if v != nil {
		t.Errorf("a refused document also returned %#v; a caller that ignored the error would act on a partial config", v)
	}
	return err.Error()
}

func TestParseRefusesADocumentItCannotRepresentAndNamesTheLine(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		src      string
		wantLine string
		wantWhat string
	}{
		{
			name:     "a sequence item where a mapping key belongs",
			src:      "areas:\n  - area: core\nbad: 1\n- stray\n",
			wantLine: "line 4",
			wantWhat: "sequence item inside mapping",
		},
		{
			name:     "a line indented past its siblings in a mapping",
			src:      "a: 1\n  b: 2\n",
			wantLine: "line 2",
			wantWhat: "unexpected indentation",
		},
		{
			name:     "a line that is not key: value",
			src:      "a: 1\nnotakey\n",
			wantLine: "line 2",
			wantWhat: "expected key: value",
		},
		{
			name:     "an unterminated flow mapping",
			src:      "auto: {queue: seqmpmc\n",
			wantLine: "line 1",
			wantWhat: "unterminated flow mapping",
		},
		{
			name:     "an unterminated flow sequence",
			src:      "variants: [A8, A9\n",
			wantLine: "line 1",
			wantWhat: "unterminated flow sequence",
		},
		{
			name:     "a flow mapping entry that is not key: value",
			src:      "auto: {queue}\n",
			wantLine: "line 1",
			wantWhat: "bad flow mapping entry",
		},
		{
			name:     "an unterminated flow nested inside a flow mapping",
			src:      "auto: {inner: [a, b}\n",
			wantLine: "line 1",
			wantWhat: "unterminated flow sequence",
		},
		{
			name:     "an unterminated flow nested inside a flow sequence",
			src:      "list: [{a: 1, [b]\n",
			wantLine: "line 1",
			wantWhat: "unterminated",
		},
		{
			name:     "an unterminated flow after a dash",
			src:      "areas:\n  - [a, b\n",
			wantLine: "line 2",
			wantWhat: "unterminated flow sequence",
		},
		{
			name:     "an unterminated flow as a dashed key's value",
			src:      "areas:\n  - area: {a: 1\n",
			wantLine: "line 2",
			wantWhat: "unterminated flow mapping",
		},
		{
			name:     "a bad line inside a nested mapping",
			src:      "outer:\n  inner:\n    a: 1\n     b: 2\n",
			wantLine: "line 4",
			wantWhat: "unexpected indentation",
		},
		{
			name:     "a bad line inside a nested sequence",
			src:      "outer:\n  - a\n   - b\n",
			wantLine: "line 3",
			wantWhat: "unexpected indentation",
		},
		// The line is the file's, comments and blank lines counted: the
		// parser used to number only the lines it kept.
		{
			name:     "a bad line after a comment and a blank line",
			src:      "# floors\n\na: 1\n  b: 2\n",
			wantLine: "line 4",
			wantWhat: "unexpected indentation",
		},
		{
			name:     "an unterminated flow after a blank line",
			src:      "a: 1\n\n# the budgets\nb: [x, y\n",
			wantLine: "line 4",
			wantWhat: "unterminated flow sequence",
		},
		{
			name:     "content after the document's block, below a comment",
			src:      "# rows\n  a: 1\nb: 2\n",
			wantLine: "line 3",
			wantWhat: "unexpected content",
		},
		{
			name:     "an unterminated flow as a dashed key's value, below a blank line",
			src:      "# areas\nareas:\n\n  - area: {a: 1\n",
			wantLine: "line 4",
			wantWhat: "unterminated flow mapping",
		},
		{
			name:     "a bad line in a sequence, below a comment",
			src:      "outer:\n  # the items\n  - a\n   - b\n",
			wantLine: "line 4",
			wantWhat: "unexpected indentation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := parseErr(t, tc.src)
			if !strings.Contains(msg, tc.wantWhat) {
				t.Errorf("error %q does not say %q", msg, tc.wantWhat)
			}
			// The line number is the whole diagnostic value: a config file is
			// edited by hand and an error without one makes the operator bisect
			// it.
			if !strings.Contains(msg, tc.wantLine) {
				t.Errorf("error %q does not name %s, so an operator has to bisect the file", msg, tc.wantLine)
			}
		})
	}
}

func TestParseReadsTheShapesIqmqsOwnConfigFilesUse(t *testing.T) {
	t.Parallel()
	// This is the shape of hack/coverage-floors.yaml, comments and all, plus
	// the flow forms ADR front matter uses. It is the control for the refusal
	// table above: if these stopped parsing, every case there would pass for
	// the wrong reason.
	const src = `# a leading comment
areas:
  - area: core        # a trailing comment
    target: 90.0
    floor: 90.0
    owner: nobody
  - area: memory
    target: 85.0
variants: [A8, A9, A22]
auto: {queue: seqmpmc, batch: sizeonly}
empty:
nothing: ~
quoted: "a: colon inside # and a hash"
single: 'it''s quoted'
flag: yes
off_flag: Off
hexish: 0x10
floaty: 1.5
`
	v, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := v.(*Map)
	if !ok {
		t.Fatalf("root is %T, want *Map", v)
	}
	areas := Maps(mustGet(t, root, "areas"))
	if len(areas) != 2 {
		t.Fatalf("got %d areas, want 2; a dropped area is an unmeasured coverage threshold", len(areas))
	}
	if got := String(mustGet(t, areas[0], "area")); got != "core" {
		t.Errorf("areas[0].area = %q, want core", got)
	}
	if got, ok := Float(mustGet(t, areas[0], "target")); !ok || got != 90.0 {
		t.Errorf("areas[0].target = %v (ok=%v), want 90", got, ok)
	}
	if got := String(mustGet(t, areas[0], "owner")); got != "nobody" {
		t.Errorf("the trailing comment ate a later key: owner = %q", got)
	}
	if got := String(mustGet(t, areas[1], "area")); got != "memory" {
		t.Errorf("areas[1].area = %q, want memory", got)
	}
	if _, has := areas[1].Get("owner"); has {
		t.Error("areas[1] gained a key from areas[0]; the inline mapping bled across dashes")
	}
	if got := Strings(mustGet(t, root, "variants")); len(got) != 3 || got[0] != "A8" || got[2] != "A22" {
		t.Errorf("variants = %q, want [A8 A9 A22]", got)
	}
	auto, ok := mustGet(t, root, "auto").(*Map)
	if !ok {
		t.Fatalf("auto is %T, want *Map", mustGet(t, root, "auto"))
	}
	if got := String(mustGet(t, auto, "queue")); got != "seqmpmc" {
		t.Errorf("auto.queue = %q, want seqmpmc", got)
	}
	if got := String(mustGet(t, auto, "batch")); got != "sizeonly" {
		t.Errorf("auto.batch = %q, want sizeonly", got)
	}
	// A key with no value, and an explicit null, are both nil rather than the
	// empty string: a floor that read as "" would compare as zero.
	for _, k := range []string{"empty", "nothing"} {
		if v := mustGet(t, root, k); v != nil {
			t.Errorf("%s = %#v, want nil", k, v)
		}
	}
	// A quoted scalar keeps the colon and the hash a comment stripper would
	// otherwise eat.
	if got := String(mustGet(t, root, "quoted")); got != "a: colon inside # and a hash" {
		t.Errorf("quoted = %q; the comment stripper or the key splitter reached inside the quotes", got)
	}
	if got := String(mustGet(t, root, "single")); got != "it's quoted" {
		t.Errorf("single = %q, want it's quoted", got)
	}
	if got, ok := Bool(mustGet(t, root, "flag")); !ok || !got {
		t.Errorf("flag = %v (ok=%v), want true", got, ok)
	}
	if got, ok := Bool(mustGet(t, root, "off_flag")); !ok || got {
		t.Errorf("off_flag = %v (ok=%v), want false", got, ok)
	}
	if got, ok := Int(mustGet(t, root, "hexish")); !ok || got != 16 {
		t.Errorf("hexish = %v (ok=%v), want 16", got, ok)
	}
	if got, ok := Float(mustGet(t, root, "floaty")); !ok || got != 1.5 {
		t.Errorf("floaty = %v (ok=%v), want 1.5", got, ok)
	}
}

func mustGet(t *testing.T, m *Map, key string) any {
	t.Helper()
	v, ok := m.Get(key)
	if !ok {
		t.Fatalf("no key %q in %v", key, m.Keys)
	}
	return v
}

func TestParseEmptyAndAbsentDocuments(t *testing.T) {
	t.Parallel()
	// An empty document is nil and not an error: a config file that exists but
	// is blank is a caller's problem to diagnose, and the parser saying
	// "nothing here" is the honest answer. What must NOT happen is an empty
	// document coming back as an empty mapping, because a caller that iterated
	// it would find no floors and report every area met.
	for _, src := range []string{"", "\n\n", "# only a comment\n", "   \n\t\n"} {
		v, err := Parse([]byte(src))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
		}
		if v != nil {
			t.Errorf("Parse(%q) = %#v, want nil", src, v)
		}
	}
	// A key whose value is an EMPTY sequence must be an empty slice and not
	// nil, so `areas: []` and a missing `areas:` are distinguishable: one says
	// "no areas, deliberately" and the other says "the key is absent".
	v, err := Parse([]byte("areas: []\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := v.(*Map)
	if !ok {
		t.Fatalf("root is %T", v)
	}
	seq, ok := mustGet(t, root, "areas").([]any)
	if !ok {
		t.Fatalf("areas is %T, want []any", mustGet(t, root, "areas"))
	}
	if seq == nil || len(seq) != 0 {
		t.Errorf("areas = %#v, want a non-nil empty slice", seq)
	}
	// And a dash-led document whose items are all absent still yields a
	// non-nil, empty-per-item sequence rather than nil.
	v, err = Parse([]byte("-\n-\n"))
	if err != nil {
		t.Fatalf("Parse(bare dashes): %v", err)
	}
	items, ok := v.([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("got %#v, want two nil items", v)
	}
	for i, it := range items {
		if it != nil {
			t.Errorf("item %d = %#v, want nil", i, it)
		}
	}
}

func TestSequenceOfMappingsUnderADashKeepsItsNestedBlocks(t *testing.T) {
	t.Parallel()
	// "- key:" followed by an indented block is how benchmarks/budgets.yaml
	// nests a row's own fields. A parser that attached the block to the wrong
	// map would move a budget from one row to another, and both rows would
	// still parse.
	const src = `rows:
  - name: first
    limits:
      p99: 10
      mean: 2
    tags: [a, b]
  - name: second
    limits:
      p99: 20
`
	v, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := v.(*Map)
	if !ok {
		t.Fatalf("root is %T", v)
	}
	rows := Maps(mustGet(t, root, "rows"))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for i, want := range []struct {
		name string
		p99  int
	}{{"first", 10}, {"second", 20}} {
		if got := String(mustGet(t, rows[i], "name")); got != want.name {
			t.Errorf("rows[%d].name = %q, want %q", i, got, want.name)
		}
		lim, ok := mustGet(t, rows[i], "limits").(*Map)
		if !ok {
			t.Fatalf("rows[%d].limits is %T, want *Map", i, mustGet(t, rows[i], "limits"))
		}
		if got, ok := Int(mustGet(t, lim, "p99")); !ok || got != want.p99 {
			t.Errorf("rows[%d].limits.p99 = %v (ok=%v), want %d", i, got, ok, want.p99)
		}
	}
	// The second row must not have inherited the first row's tags.
	if _, has := rows[1].Get("tags"); has {
		t.Error("rows[1] inherited rows[0].tags; a budget moved between rows")
	}
	if got := Strings(mustGet(t, rows[0], "tags")); len(got) != 2 {
		t.Errorf("rows[0].tags = %q, want two entries", got)
	}
	// "- key:" with nothing indented under it is a nil value, not a missing
	// key: the key was written and the document says it has no value.
	v, err = Parse([]byte("rows:\n  - name:\n  - name: x\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok = v.(*Map)
	if !ok {
		t.Fatalf("root is %T, want *Map", v)
	}
	rows = Maps(mustGet(t, root, "rows"))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if got, has := rows[0].Get("name"); !has || got != nil {
		t.Errorf("rows[0].name = (%#v, %v), want (nil, true)", got, has)
	}
}

// TestTheFirstKeyAfterADashTakesASequenceAtItsOwnIndent holds the first key of
// a dashed mapping to the rule every other key already follows: "A sequence may
// sit at the same indent as its key". The keys after the first are read by
// mapping, which allows it; the first was read by sequence, which did not, and
// refused a document whose second row was written the same way and parsed.
func TestTheFirstKeyAfterADashTakesASequenceAtItsOwnIndent(t *testing.T) {
	t.Parallel()
	const src = `rows:
  - removed:
    - a
    - b
    reason: first
  - reason: second
    removed:
    - c
`
	v, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := v.(*Map)
	if !ok {
		t.Fatalf("root is %T", v)
	}
	rows := Maps(mustGet(t, root, "rows"))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if got := Strings(mustGet(t, rows[0], "removed")); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("rows[0].removed = %q, want [a b]", got)
	}
	if got := String(mustGet(t, rows[0], "reason")); got != "first" {
		t.Errorf("rows[0].reason = %q, want first", got)
	}
	if got := Strings(mustGet(t, rows[1], "removed")); len(got) != 1 || got[0] != "c" {
		t.Errorf("rows[1].removed = %q, want [c]", got)
	}
}

// TestAQuoteInsideAPlainScalarDoesNotHideTheComment: a quote opens a quoted
// scalar only where a scalar starts. An apostrophe inside a plain one is a
// letter, and reading it as an opening quote swallowed the rest of the line,
// comment and all, into the value.
func TestAQuoteInsideAPlainScalarDoesNotHideTheComment(t *testing.T) {
	t.Parallel()
	for src, want := range map[string]string{
		"reason: it's flaky # see the issue\n":     "it's flaky",
		"reason: a 6\" pipe # inches\n":            "a 6\" pipe",
		"- don't # a sequence item\n":              "don't",
		"reason: 'quoted # not a comment' # one\n": "quoted # not a comment",
		"reason: \"x # y\" # one\n":                "x # y",
	} {
		v, err := Parse([]byte(src))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
			continue
		}
		var got any
		switch x := v.(type) {
		case *Map:
			got = x.Values["reason"]
		case []any:
			got = x[0]
		}
		if String(got) != want {
			t.Errorf("Parse(%q) read %q, want %q", src, String(got), want)
		}
	}
}

// TestAnEscapedQuoteDoesNotEndTheQuotation: a quoted scalar writes its own
// quote doubled inside single quotes and escaped with a backslash inside
// double quotes, and neither ends the quotation. Reading the first quote of
// the pair as the closing one put the rest of the scalar outside it, so a " #"
// later in the scalar was taken for a comment and the value was cut short.
func TestAnEscapedQuoteDoesNotEndTheQuotation(t *testing.T) {
	t.Parallel()
	for src, want := range map[string]string{
		"reason: 'it''s # not a comment'\n":           "it's # not a comment",
		"reason: 'it''s' # a comment\n":               "it's",
		`reason: "say \"hi\" # not a comment"` + "\n": `say "hi" # not a comment`,
		`reason: "a \\" # a comment` + "\n":           `a \`,
		"- 'it''s # not a comment'\n":                 "it's # not a comment",
	} {
		v, err := Parse([]byte(src))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
			continue
		}
		var got any
		switch x := v.(type) {
		case *Map:
			got = x.Values["reason"]
		case []any:
			got = x[0]
		}
		if String(got) != want {
			t.Errorf("Parse(%q) read %q, want %q", src, String(got), want)
		}
	}
}

// TestAnApostropheInAFlowCollectionIsALetter is the flow half of
// TestAQuoteInsideAPlainScalarDoesNotHideTheComment: a quote opens a quoted
// scalar only where a scalar starts. The flow splitter read the apostrophe of
// "it's" as an opening quote and took every comma after it for part of the
// scalar, so [it's, fine] was one entry.
func TestAnApostropheInAFlowCollectionIsALetter(t *testing.T) {
	t.Parallel()
	v, err := Parse([]byte("tags: [it's, fine, 'a, b', \"c, \\\"d\"]\nwho: {name: o'brien, n: 1}\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root := v.(*Map)
	if got, want := Strings(mustGet(t, root, "tags")), []string{"it's", "fine", "a, b", `c, "d`}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("tags = %q, want %q", got, want)
	}
	who := mustGet(t, root, "who").(*Map)
	if got := String(mustGet(t, who, "name")); got != "o'brien" {
		t.Errorf("who.name = %q, want o'brien", got)
	}
	if got, ok := Int(mustGet(t, who, "n")); !ok || got != 1 {
		t.Errorf("who.n = %v, want 1", mustGet(t, who, "n"))
	}
}

// TestAKeyAfterADashKeepsItsOwnColumn: the keys of a dashed mapping line up
// with its first key, wherever that key starts. The parser looked for them two
// columns past the dash whatever the spacing, so "-   name: a" followed by
// "    value: b" was refused as unexpected indentation.
func TestAKeyAfterADashKeepsItsOwnColumn(t *testing.T) {
	t.Parallel()
	v, err := Parse([]byte("rows:\n  -   name: a\n      value: b\n  - name: c\n    value: d\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rows := Maps(mustGet(t, v.(*Map), "rows"))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for i, want := range [][2]string{{"a", "b"}, {"c", "d"}} {
		if got := [2]string{String(rows[i].Values["name"]), String(rows[i].Values["value"])}; got != want {
			t.Errorf("rows[%d] = %q, want %q", i, got, want)
		}
	}
}

// TestFloatsAreReadAsYAMLWritesThem: the core schema spells infinity and
// not-a-number .inf and .nan, and has no hexadecimal floats. strconv's
// ParseFloat is Go's syntax instead, which read "inf", "nan" and "infinity" as
// numbers, made 0x1p-2 a quarter, and left ".inf" a string.
func TestFloatsAreReadAsYAMLWritesThem(t *testing.T) {
	t.Parallel()
	for src, want := range map[string]any{
		"1.5":      1.5,
		"-2e3":     -2000.0,
		".5":       0.5,
		".inf":     math.Inf(1),
		"-.Inf":    math.Inf(-1),
		"+.INF":    math.Inf(1),
		"inf":      "inf",
		"infinity": "infinity",
		"nan":      "nan",
		"NaN":      "NaN",
		"0x1p-2":   "0x1p-2",
	} {
		v, err := Parse([]byte("n: " + src + "\n"))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
			continue
		}
		if got := v.(*Map).Values["n"]; got != want {
			t.Errorf("n: %s read as %T(%v), want %T(%v)", src, got, got, want, want)
		}
	}
	for _, src := range []string{".nan", ".NaN", ".NAN"} {
		v, err := Parse([]byte("n: " + src + "\n"))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
			continue
		}
		if got, ok := v.(*Map).Values["n"].(float64); !ok || !math.IsNaN(got) {
			t.Errorf("n: %s read as %T(%v), want NaN", src, v.(*Map).Values["n"], v.(*Map).Values["n"])
		}
	}
}

// TestIntegersAreReadAsYAMLWritesThem: the core schema's integers are decimal,
// 0o octal and 0x hexadecimal. strconv's base 0 is Go's literal syntax
// instead, which read a leading zero as octal (010 was 8), made 08 a float, and
// took 0b101 for a number.
func TestIntegersAreReadAsYAMLWritesThem(t *testing.T) {
	t.Parallel()
	for src, want := range map[string]any{
		"010":   int64(10),
		"08":    int64(8),
		"-007":  int64(-7),
		"+12":   int64(12),
		"0x1f":  int64(31),
		"0o17":  int64(15),
		"0b101": "0b101",
		"0x-5":  "0x-5",
		"0o+5":  "0o+5",
	} {
		v, err := Parse([]byte("n: " + src + "\n"))
		if err != nil {
			t.Errorf("Parse(%q): %v", src, err)
			continue
		}
		if got := v.(*Map).Values["n"]; got != want {
			t.Errorf("n: %s read as %T(%v), want %T(%v)", src, got, got, want, want)
		}
	}
}

func TestTheAccessorsAnswerForEveryShapeAConfigCanHold(t *testing.T) {
	t.Parallel()
	// These are read by the gates, and each one's FALSE answer is what stops a
	// gate mis-reading a value: a floor written as "ninety" must not come back
	// as 0 with ok=true, because a floor of 0 is a threshold nothing can fail.
	for _, tc := range []struct {
		name string
		v    any
		f    float64
		fOK  bool
		i    int
		iOK  bool
		b    bool
		bOK  bool
		s    string
	}{
		{name: "an integer", v: int64(7), f: 7, fOK: true, i: 7, iOK: true, s: "7"},
		{name: "a whole float", v: 90.0, f: 90, fOK: true, i: 90, iOK: true, s: "90"},
		{name: "a fractional float", v: 90.6, f: 90.6, fOK: true, iOK: false, s: "90.6"},
		{name: "a numeric string", v: "42", f: 42, fOK: true, i: 42, iOK: true, s: "42"},
		{name: "a fractional numeric string", v: "1.5", f: 1.5, fOK: true, iOK: false, s: "1.5"},
		{name: "a non-numeric string", v: "ninety", fOK: false, iOK: false, s: "ninety"},
		{name: "a bool", v: true, fOK: false, iOK: false, b: true, bOK: true, s: "true"},
		{name: "nil", v: nil, fOK: false, iOK: false, s: ""},
		{name: "a mapping", v: &Map{}, fOK: false, iOK: false, s: "&{[] map[]}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, ok := Float(tc.v)
			if ok != tc.fOK || (tc.fOK && f != tc.f) {
				t.Errorf("Float = (%v, %v), want (%v, %v)", f, ok, tc.f, tc.fOK)
			}
			i, ok := Int(tc.v)
			if ok != tc.iOK || (tc.iOK && i != tc.i) {
				t.Errorf("Int = (%v, %v), want (%v, %v)", i, ok, tc.i, tc.iOK)
			}
			b, ok := Bool(tc.v)
			if ok != tc.bOK || (tc.bOK && b != tc.b) {
				t.Errorf("Bool = (%v, %v), want (%v, %v)", b, ok, tc.b, tc.bOK)
			}
			if got := String(tc.v); got != tc.s {
				t.Errorf("String = %q, want %q", got, tc.s)
			}
		})
	}
	// Strings of a lone scalar is a one-element slice, and of nil is nil: a
	// config that writes one variant without brackets means one variant, and a
	// config that omits the key means none.
	if got := Strings("A8"); len(got) != 1 || got[0] != "A8" {
		t.Errorf("Strings(scalar) = %q, want [A8]", got)
	}
	if got := Strings(nil); got != nil {
		t.Errorf("Strings(nil) = %#v, want nil", got)
	}
	if got := Strings([]any{int64(1), true, nil}); len(got) != 3 || got[0] != "1" || got[1] != "true" || got[2] != "" {
		t.Errorf("Strings = %q, want [1 true (empty)]", got)
	}
	// Maps of a lone mapping is a one-element slice; of a sequence it skips
	// non-mappings rather than returning nil for the lot, because one malformed
	// row must not delete the rows around it.
	if got := Maps(&Map{}); len(got) != 1 {
		t.Errorf("Maps(a lone mapping) = %v, want one", got)
	}
	if got := Maps([]any{&Map{}, "scalar", nil, &Map{}}); len(got) != 2 {
		t.Errorf("Maps skipped or kept the wrong items: got %d, want 2", len(got))
	}
	if got := Maps("scalar"); got != nil {
		t.Errorf("Maps(a scalar) = %v, want nil", got)
	}
}
