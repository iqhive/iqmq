package budgets

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadRepoBudgets(t *testing.T) {
	c, err := Load("../../budgets.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Version != 1 || c.Defaults.ThresholdPct != 3 || c.Defaults.Alpha != 0.05 || c.Defaults.MinCount != 10 {
		t.Errorf("defaults = %+v", c.Defaults)
	}
	if c.Hosts.Taskset != "" || c.Hosts.MaxLoadAvg != 0 || len(c.Hosts.Authoritative) != 0 {
		t.Errorf("hosts = %+v", c.Hosts)
	}
	if len(c.GateSets["pr"]) < 5 || len(c.GateSets["nightly"]) != 1 {
		t.Errorf("gate sets = %v", c.GateSets)
	}
	if len(c.Budgets) < 5 || len(c.Targets) < 3 {
		t.Errorf("budgets=%d targets=%d", len(c.Budgets), len(c.Targets))
	}

	if !c.InGateSet("pr", "BenchmarkHeaders/Get/keys=registry/n=8-16") {
		t.Error("Headers benchmark not in pr gate set")
	}
	if !c.InGateSet("nightly", "BenchmarkAnything/at/all") {
		t.Error("nightly gate set does not match everything")
	}
	if c.InGateSet("pr", "BenchmarkMemory/Wait/spin") {
		t.Error("Wait benchmark wrongly in pr gate set")
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"*", "BenchmarkX/y/z-8", true},
		{"BenchmarkHeaders/*/Get/n=8", "BenchmarkHeaders/binaryslice/Get/n=8-16", true},
		{"BenchmarkHeaders/*/Get/n=8", "BenchmarkHeaders/binaryslice/Set/n=8", false},
		{"BenchmarkHeaders/*/Get/n=8", "BenchmarkHeaders/binaryslice/Get/n=8/extra", false},
		{"BenchmarkWire/*/Encode/1KiB*", "BenchmarkWire/tlv/Encode/1KiB/headers=4", false},
		{"BenchmarkWire/*/Encode/1KiB*", "BenchmarkWire/tlv/Encode/1KiBx", true},
		{"BenchmarkMemory/PubSubAck/*/{owned,reuse}", "BenchmarkMemory/PubSubAck/1P1C/reuse-8", true},
		{"BenchmarkMemory/PubSubAck/*/{owned,reuse}", "BenchmarkMemory/PubSubAck/1P1C/safe", false},
		{"BenchmarkMemory/PubSubAck/1P1C/*", "BenchmarkMemory/PubSubAck/1P1C/safe/mode=x", true},
		{"BenchmarkMemory/PubSubAck/1P1C/*", "BenchmarkMemory/PubSubAck/1P1C", false},
		{"BenchmarkRun/Dispatch/*", "BenchmarkRun/Dispatch/8workers/ordered-64", true},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxc", false},
		{"Bench-1", "Bench-1", false}, // a trailing -N on the name is a procs suffix
		{"Bench", "Bench-1", true},
		{"Bench", "Bench-", false},
		{"x/{a,b", "x/a", false},
	}
	for _, c := range cases {
		if got := Match(c.pattern, c.name); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestWaivers(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	c, err := Parse([]byte(`
waivers:
  - {match: "BenchmarkA/*", issue: "#12", expires: "2026-12-31", reason: "known"}
  - {match: "BenchmarkB/*", issue: "#13", expires: "2020-01-01", reason: "expired"}
  - {match: "BenchmarkC/*", reason: "missing issue"}
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Waived("BenchmarkA/x", now); !ok {
		t.Error("active waiver not applied")
	}
	if _, ok := c.Waived("BenchmarkB/x", now); ok {
		t.Error("expired waiver applied")
	}
	if _, ok := c.Waived("BenchmarkC/x", now); ok {
		t.Error("malformed waiver applied")
	}
}

func TestParseErrors(t *testing.T) {
	for _, s := range []string{"- a\n", "budgets:\n  - {ns_op_max: 1}\n", "budgets:\n  - {match: \"x/{a\"}\n", "a: [\n"} {
		if _, err := Parse([]byte(s)); err == nil {
			t.Errorf("no error for %q", s)
		}
	}
	if _, err := Load(os.DevNull + "/nope"); err == nil {
		t.Error("Load of missing file succeeded")
	}
}

// TestParseRefusesAFieldThatIsNotAFiniteNumber: Parse dropped Float's and
// Int's answers, so a threshold yamlmini could not read as a number quietly
// became the default (`threshold_pct: nan` gated at 3%), a ceiling it could
// not read became no ceiling, and `.nan`, which YAML reads as a number,
// became a threshold no measurement can fail. Each is refused, naming the
// field.
func TestParseRefusesAFieldThatIsNotAFiniteNumber(t *testing.T) {
	for _, tc := range []struct {
		src, want string
	}{
		{"defaults:\n  threshold_pct: nan\n", "defaults: threshold_pct"},
		{"defaults:\n  threshold_pct: .nan\n", "defaults: threshold_pct"},
		{"defaults:\n  alpha: .inf\n", "defaults: alpha"},
		{"defaults:\n  min_count: ten\n", "defaults: min_count"},
		{"defaults:\n  min_count: 2.5\n", "defaults: min_count"},
		{"hosts:\n  provisional_pinning:\n    max_load_avg: high\n", "hosts.provisional_pinning: max_load_avg"},
		{"budgets:\n  - {match: \"BenchmarkX\", ns_op_max: ten}\n", "budget BenchmarkX: ns_op_max"},
		{"budgets:\n  - {match: \"BenchmarkX\", max_pct: -.inf}\n", "budget BenchmarkX: max_pct"},
		{"budgets:\n  - {numerator: \"BenchmarkA\", denominator: \"BenchmarkB\", unevaluated: some}\n", "budget BenchmarkA: unevaluated"},
		{"targets:\n  - {match: \"BenchmarkX\", ns_op_target: fast}\n", "target BenchmarkX: ns_op_target"},
	} {
		_, err := Parse([]byte(tc.src))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Parse(%q) = %v, want an error naming %q", tc.src, err, tc.want)
		}
	}
	// Absent and empty fields still take their defaults, and a quoted number
	// still counts.
	c, err := Parse([]byte("defaults:\n  alpha:\n  min_count: \"12\"\nbudgets:\n  - {match: \"BenchmarkX\"}\n"))
	if err != nil {
		t.Fatalf("Parse of absent, empty and quoted fields: %v", err)
	}
	if c.Defaults.ThresholdPct != 3 || c.Defaults.Alpha != 0.05 || c.Defaults.MinCount != 12 || c.Budgets[0].NsOpSet {
		t.Errorf("defaults %+v, budget %+v; want threshold 3, alpha 0.05, min_count 12 and no ns_op_max", c.Defaults, c.Budgets[0])
	}
}

func FuzzParse(f *testing.F) {
	data, _ := os.ReadFile("../../budgets.yaml")
	f.Add(data)
	f.Add([]byte("budgets:\n  - {match: \"a/{b,c}/*\", ns_op_max: 1}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := Parse(data)
		if err != nil {
			return
		}
		for i := range c.Budgets {
			Match(c.Budgets[i].Match, "BenchmarkX/y/z-8")
		}
		for _, w := range c.Waivers {
			w.Active(time.Now())
		}
	})
}
