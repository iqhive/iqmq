package budgets

import (
	"os"
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
