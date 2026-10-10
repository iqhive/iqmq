// Package budgets loads benchmarks/budgets.yaml and matches benchmark names
// against its glob patterns.
package budgets

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iqhive/iqmq/internal/yamlmini"
)

// Config is the parsed budgets file.
type Config struct {
	Version  int
	Defaults Defaults
	Hosts    Hosts
	GateSets map[string][]string
	// GatePackages names, PER GATE SET, the packages that hold a gated
	// benchmark without being a variant family, so `bakeoff gate-pattern
	// -packages` cannot derive them. The root
	// package is the case: it holds the benchmarks the budgets are written
	// about and has no variant kinds.
	//
	GatePackages map[string][]string
	Budgets      []Budget
	Targets      []Target
	Waivers      []Waiver
}

// Defaults are the gate thresholds.
type Defaults struct {
	ThresholdPct         float64
	Alpha                float64
	MinCount             int
	AllocsTolerance      float64
	BytesThresholdPct    float64
	CIHalfwidthRejectPct float64
}

// Hosts describes authoritative and provisional benchmark hosts.
type Hosts struct {
	Authoritative []string
	Taskset       string
	NUMANode      int
	MaxLoadAvg    float64
}

// Budget is an absolute ceiling on a benchmark's candidate medians. A zero
// Set flag means the ceiling is not defined.
//
// Kind says where the number comes from, and the distinction is what makes the
// gate mean anything: a SpecificationKind ceiling is a figure the API contract
// The specification states and may never be relaxed to fit a measurement -- a measurement
// above it is a finding about the implementation -- while a MeasuredKind
// ceiling is derived from the bake-off's own medians and carries its
// provenance, so a reader can tell a missed goal from a regression.
type Budget struct {
	Match       string
	NsOpMax     float64
	NsOpSet     bool
	AllocsOpMax float64
	AllocsSet   bool
	BOpMax      float64
	BOpSet      bool

	// Numerator, Denominator and MaxPct are a RatioKind budget: the API contract
	// the first two figures are a ratio between two DIFFERENT benchmarks
	// measured in the same run -- `BenchmarkDriver/<driver>/<op>/<size>` over
	// `BenchmarkNative/<client>/<op>/<size>` -- and neither of the other two
	// shapes can express one, so the project's second design principle was
	// measured, published and ungated.
	//
	// MaxPct is a THROUGHPUT LOSS in per cent, never a duration increase:
	// The specification budgets the rate, and an arm 10 % slower per operation moves
	// 9.09 % fewer messages a second, so reading the duration would report a
	// breach on a row that meets the budget.
	// ThroughputLossPct is the arithmetic, shared with benchmarks/cmd/overhead.
	Numerator   string
	Denominator string
	MaxPct      float64
	MaxPctSet   bool
	// Gates names the gate sets this budget is evaluated in; empty means every
	// set, which is what an absolute ceiling on one pattern's median wants. A
	// ratio budget MUST name its sets: both of its arms have to be measured in
	// one run, no gate set measures them by accident, and a row a gate set
	// selects and cannot measure is INCONCLUSIVE rather than passing
	//  -- so a ratio row left in scope of a set that never
	// runs a broker would make every run of that set red for a reason that is
	// not about the code.
	Gates []string

	// Kind is SpecificationKind, MeasuredKind or RatioKind.
	Kind string
	// Spec cites the specification row a SpecificationKind ceiling comes from.
	Spec string
	// MeasuredNsOp is the worst median a MeasuredKind ceiling was derived
	// from, MarginPct the margin added to it, and Run the campaign run id the
	// median was read out of.
	MeasuredNsOp float64
	MarginPct    float64
	Run          string
	// Note is prose for a reader; nothing checks it.
	Note string
	// Unevaluated is how many of the benchmark rows this budget's pattern
	// matches are NOT selected by the `pr` gate set -- the only gate set a
	// job actually runs. It is a declared number and `benchgate -lint`
	// compares it with the measured inventory, so a budget cannot quietly
	// come to cover rows nobody evaluates.
	//
	// It exists because the runner-dispatch ceiling is breached on three
	// of the nine rows of the arm it names, all three of them 64-worker
	// shapes that only `nightly: ["*"]` selects -- and the nightly job is
	// documented in the contributor notes and not installed. The budget's own
	// note said "met: worst median 240.95 ns over two full runs" for two
	// increments. Prose filled a gap in who
	// looks; a declared count cannot, because the lint recomputes it.
	Unevaluated    int
	UnevaluatedSet bool
}

// The kinds a budget may declare.
const (
	SpecificationKind = "specification"
	MeasuredKind      = "measured"
	// RatioKind is a ceiling on the throughput one benchmark gives up against
	// another measured in the same run. Its figure always comes from the
	// specification -- there is no measured ratio ceiling, because a ratio
	// derived from the very medians it is checking against would be vacuous --
	// so it carries `spec:` like a SpecificationKind ceiling does.
	RatioKind = "ratio"
)

// IsRatio reports whether the budget is a ratio between two benchmarks rather
// than an absolute ceiling on one.
func (b Budget) IsRatio() bool { return b.Kind == RatioKind }

// ID names the budget in a problem report or a verdict row: its `match` for an
// absolute ceiling, and both patterns for a ratio, which has no single pattern
// to be identified by.
func (b Budget) ID() string {
	if b.IsRatio() {
		return b.Numerator + " / " + b.Denominator
	}
	return b.Match
}

// InGate reports whether the budget is evaluated in the named gate set. A
// budget that names no set is evaluated in all of them.
func (b Budget) InGate(gate string) bool {
	if len(b.Gates) == 0 {
		return true
	}
	for _, g := range b.Gates {
		if g == gate {
			return true
		}
	}
	return false
}

// ThroughputLossPct is the throughput a numerator arm gives up against its
// denominator, in per cent: the RATE lost, not the duration added. The specification
// budgets the first and they are not the same figure -- an arm whose
// per-operation duration is a factor 1+x of its baseline's moves 1/(1+x) of
// the messages a second, so the loss is x/(1+x): 10 % slower is 9.09 % fewer
// messages a second, and the 5 % batched budget permits an operation 5.26 %
// longer.
//
// It lives here rather than in either command because both the gate and
// benchmarks/cmd/overhead have to produce the same number from the same two
// medians, and the first thing that drifts between a report and the gate that
// enforces it is the arithmetic (TestThroughputLossIsTheRateNotTheDuration and TestLossIsTheRateNotTheDuration, which pin the same boundaries on both sides).
func ThroughputLossPct(numeratorNs, denominatorNs float64) float64 {
	if numeratorNs == 0 {
		return 0
	}
	return (1 - denominatorNs/numeratorNs) * 100
}

// Target is a reported-only performance target.
type Target struct {
	Match            string
	NsOpTarget       float64
	NsEntryTarget    float64
	MsgsSTarget      float64
	P99NsTarget      float64
	BytesEntryTarget float64
	Note             string
}

// Metrics lists the metric each of the target's figures is read on, paired
// with the figure, skipping the ones this target does not set.
func (t Target) Metrics() []TargetMetric {
	var out []TargetMetric
	for _, m := range []TargetMetric{
		{"ns/op", t.NsOpTarget, true},
		{"ns/entry", t.NsEntryTarget, true},
		{"msgs/s", t.MsgsSTarget, false},
		{"p99-ns", t.P99NsTarget, true},
		{"B/entry", t.BytesEntryTarget, true},
	} {
		if m.Figure != 0 {
			out = append(out, m)
		}
	}
	return out
}

// TargetMetric is one figure of a target: the metric it is read on, the
// figure, and whether a lower measurement is the better one.
type TargetMetric struct {
	Unit      string
	Figure    float64
	LowerIsOK bool
}

// Waiver suppresses a budget or gate failure until Expires.
//
// Scope says how much it suppresses. BudgetScope, the default, clears budget
// breaches and leaves the base-against-candidate comparison alone, so a row
// whose absolute ceiling is waived is still held to not getting worse.
// TimeScope also clears the time half of the comparison -- ns/op, msgs/s and
// the too-wide-to-decide flag -- and keeps the allocation and byte halves,
// which are exact counts and need no confidence interval; it is for a row
// whose timing cannot be decided at the gate's sample budget but whose
// allocation count is what the row is about.
// AllScope clears everything and has to be asked for: a waiver that blinds
// the gate to regressions as a side effect of waiving a ceiling is the shape
// this repository has found vacuous six times.
type Waiver struct {
	Match   string
	Issue   string
	Expires string
	Reason  string
	Scope   string
}

// The scopes a waiver may declare.
const (
	BudgetScope = "budget"
	TimeScope   = "time"
	AllScope    = "all"
)

// WaivesRegressions reports whether the waiver suppresses the whole
// base-against-candidate comparison.
func (w Waiver) WaivesRegressions() bool { return w.Scope == AllScope }

// WaivesTime reports whether the waiver suppresses the time half of the
// comparison and the too-wide-to-decide flag.
func (w Waiver) WaivesTime() bool { return w.Scope == AllScope || w.Scope == TimeScope }

// Active reports whether the waiver is well formed and not expired at now.
func (w Waiver) Active(now time.Time) bool {
	if w.Match == "" || w.Issue == "" || w.Expires == "" {
		return false
	}
	exp, err := time.Parse("2006-01-02", w.Expires)
	if err != nil {
		return false
	}
	return now.Before(exp.Add(24 * time.Hour))
}

// Load reads and parses the file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// Parse parses the budgets document.
func Parse(data []byte) (*Config, error) {
	v, err := yamlmini.Parse(data)
	if err != nil {
		return nil, err
	}
	m, ok := v.(*yamlmini.Map)
	if !ok {
		return nil, errors.New("budgets: document is not a mapping")
	}
	c := &Config{GateSets: map[string][]string{}, GatePackages: map[string][]string{}}
	c.Version, _ = yamlmini.Int(m.Values["version"])
	if d, ok := m.Values["defaults"].(*yamlmini.Map); ok {
		c.Defaults.ThresholdPct, _ = yamlmini.Float(d.Values["threshold_pct"])
		c.Defaults.Alpha, _ = yamlmini.Float(d.Values["alpha"])
		c.Defaults.MinCount, _ = yamlmini.Int(d.Values["min_count"])
		c.Defaults.AllocsTolerance, _ = yamlmini.Float(d.Values["allocs_tolerance"])
		c.Defaults.BytesThresholdPct, _ = yamlmini.Float(d.Values["bytes_threshold_pct"])
		c.Defaults.CIHalfwidthRejectPct, _ = yamlmini.Float(d.Values["ci_halfwidth_reject_pct"])
	}
	if c.Defaults.ThresholdPct == 0 {
		c.Defaults.ThresholdPct = 3
	}
	if c.Defaults.Alpha == 0 {
		c.Defaults.Alpha = 0.05
	}
	if c.Defaults.MinCount == 0 {
		c.Defaults.MinCount = 10
	}
	if c.Defaults.BytesThresholdPct == 0 {
		c.Defaults.BytesThresholdPct = c.Defaults.ThresholdPct
	}
	if c.Defaults.CIHalfwidthRejectPct == 0 {
		c.Defaults.CIHalfwidthRejectPct = 5
	}
	if h, ok := m.Values["hosts"].(*yamlmini.Map); ok {
		c.Hosts.Authoritative = yamlmini.Strings(h.Values["authoritative"])
		if p, ok := h.Values["provisional_pinning"].(*yamlmini.Map); ok {
			c.Hosts.Taskset = yamlmini.String(p.Values["taskset"])
			c.Hosts.NUMANode, _ = yamlmini.Int(p.Values["numa_node"])
			c.Hosts.MaxLoadAvg, _ = yamlmini.Float(p.Values["max_load_avg"])
		}
	}
	if gp, ok := m.Values["gate_packages"].(*yamlmini.Map); ok {
		for _, k := range gp.Keys {
			c.GatePackages[k] = yamlmini.Strings(gp.Values[k])
		}
	}
	if g, ok := m.Values["gate_sets"].(*yamlmini.Map); ok {
		for _, k := range g.Keys {
			c.GateSets[k] = yamlmini.Strings(g.Values[k])
		}
	}
	for _, b := range yamlmini.Maps(m.Values["budgets"]) {
		var bd Budget
		bd.Match = yamlmini.String(b.Values["match"])
		bd.Numerator = yamlmini.String(b.Values["numerator"])
		bd.Denominator = yamlmini.String(b.Values["denominator"])
		bd.MaxPct, bd.MaxPctSet = yamlmini.Float(b.Values["max_pct"])
		bd.Gates = yamlmini.Strings(b.Values["gates"])
		// A ratio row has two patterns and no single one to be matched by, so
		// it is identified by its numerator instead of by a `match`.
		if bd.Match == "" && bd.Numerator == "" {
			return nil, errors.New("budgets: budget entry without match")
		}
		bd.NsOpMax, bd.NsOpSet = yamlmini.Float(b.Values["ns_op_max"])
		bd.AllocsOpMax, bd.AllocsSet = yamlmini.Float(b.Values["allocs_op_max"])
		bd.BOpMax, bd.BOpSet = yamlmini.Float(b.Values["b_op_max"])
		bd.Kind = yamlmini.String(b.Values["kind"])
		bd.Spec = yamlmini.String(b.Values["spec"])
		bd.MeasuredNsOp, _ = yamlmini.Float(b.Values["measured_ns_op"])
		bd.MarginPct, _ = yamlmini.Float(b.Values["margin_pct"])
		bd.Run = yamlmini.String(b.Values["run"])
		bd.Note = yamlmini.String(b.Values["note"])
		if n, ok := yamlmini.Int(b.Values["unevaluated"]); ok {
			bd.Unevaluated, bd.UnevaluatedSet = n, true
		}
		c.Budgets = append(c.Budgets, bd)
	}
	for _, t := range yamlmini.Maps(m.Values["targets"]) {
		var tg Target
		tg.Match = yamlmini.String(t.Values["match"])
		tg.NsOpTarget, _ = yamlmini.Float(t.Values["ns_op_target"])
		tg.NsEntryTarget, _ = yamlmini.Float(t.Values["ns_entry_target"])
		tg.Note = yamlmini.String(t.Values["note"])
		tg.MsgsSTarget, _ = yamlmini.Float(t.Values["msgs_s_target"])
		tg.P99NsTarget, _ = yamlmini.Float(t.Values["p99_ns_target"])
		tg.BytesEntryTarget, _ = yamlmini.Float(t.Values["bytes_entry_target"])
		c.Targets = append(c.Targets, tg)
	}
	for _, w := range yamlmini.Maps(m.Values["waivers"]) {
		scope := yamlmini.String(w.Values["scope"])
		if scope == "" {
			scope = BudgetScope
		}
		c.Waivers = append(c.Waivers, Waiver{
			Match:   yamlmini.String(w.Values["match"]),
			Issue:   yamlmini.String(w.Values["issue"]),
			Expires: yamlmini.String(w.Values["expires"]),
			Reason:  yamlmini.String(w.Values["reason"]),
			Scope:   scope,
		})
	}
	for i := range c.Budgets {
		for _, pat := range []string{c.Budgets[i].Match, c.Budgets[i].Numerator, c.Budgets[i].Denominator} {
			if _, err := expandBraces(pat); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

// Validate holds the document to the rules a reader of a verdict depends on:
// every budget says where its number comes from, a measured ceiling carries
// the median and margin it was derived from and agrees with the arithmetic, a
// waiver names an issue and an expiry and a scope, and no gate set is empty.
// It is deliberately not part of Parse: Parse is what the package's fuzz target
// drives, and what `benchgate -self-test` hands its own two-line document.
func (c *Config) Validate() []string {
	var problems []string
	for i := range c.Budgets {
		b := &c.Budgets[i]
		switch b.Kind {
		case RatioKind:
			problems = append(problems, c.validateRatio(b)...)
		case SpecificationKind:
			if b.Spec == "" {
				problems = append(problems, fmt.Sprintf("budget %s: a specification ceiling must cite its specification row in spec:", b.Match))
			}
			if b.MeasuredNsOp != 0 || b.MarginPct != 0 {
				problems = append(problems, fmt.Sprintf("budget %s: a specification ceiling is not derived from a measurement, so it carries no measured_ns_op or margin_pct", b.Match))
			}
		case MeasuredKind:
			switch {
			case b.MeasuredNsOp == 0:
				problems = append(problems, fmt.Sprintf("budget %s: a measured ceiling must record the median it came from in measured_ns_op", b.Match))
			case b.Run == "":
				problems = append(problems, fmt.Sprintf("budget %s: a measured ceiling must name the campaign run it was read from in run:", b.Match))
			case !b.NsOpSet:
				problems = append(problems, fmt.Sprintf("budget %s: a measured ceiling is a ns_op_max", b.Match))
			default:
				want := math.Ceil(b.MeasuredNsOp * (1 + b.MarginPct/100))
				if b.NsOpMax != want {
					problems = append(problems, fmt.Sprintf("budget %s: ns_op_max %g does not follow from measured_ns_op %g at margin_pct %g, which is %g",
						b.Match, b.NsOpMax, b.MeasuredNsOp, b.MarginPct, want))
				}
			}
		case "":
			problems = append(problems, fmt.Sprintf("budget %s: kind: must be %q or %q -- the difference between a figure the specification states and one a measurement produced is what a breach means", b.Match, SpecificationKind, MeasuredKind))
		default:
			problems = append(problems, fmt.Sprintf("budget %s: unknown kind %q", b.Match, b.Kind))
		}
		if !b.IsRatio() && !b.NsOpSet && !b.AllocsSet && !b.BOpSet {
			problems = append(problems, fmt.Sprintf("budget %s: sets no ceiling", b.Match))
		}
	}
	for _, w := range c.Waivers {
		switch {
		case w.Match == "":
			problems = append(problems, "waiver without a match")
		case w.Issue == "":
			problems = append(problems, fmt.Sprintf("waiver %s: the specification requires an issue reference", w.Match))
		case w.Expires == "":
			problems = append(problems, fmt.Sprintf("waiver %s: the specification requires an expiry", w.Match))
		case w.Reason == "":
			problems = append(problems, fmt.Sprintf("waiver %s: needs a reason", w.Match))
		}
		if _, err := time.Parse("2006-01-02", w.Expires); w.Expires != "" && err != nil {
			problems = append(problems, fmt.Sprintf("waiver %s: expires %q is not a YYYY-MM-DD date", w.Match, w.Expires))
		}
		switch w.Scope {
		case BudgetScope, TimeScope, AllScope:
		default:
			problems = append(problems, fmt.Sprintf("waiver %s: scope %q must be %q, %q or %q", w.Match, w.Scope, BudgetScope, TimeScope, AllScope))
		}
	}
	for name, set := range c.GateSets {
		if len(set) == 0 {
			problems = append(problems, fmt.Sprintf("gate set %s is empty, so it would pass without measuring anything", name))
		}
	}
	// A gate_packages key that names no gate set compiles a package for a set
	// that does not exist, which is the same shape as a budget that matches
	// nothing: it costs and reports nothing.
	for name := range c.GatePackages {
		if _, ok := c.GateSets[name]; !ok {
			problems = append(problems, fmt.Sprintf("gate_packages names %q, which is not a gate set, so its packages belong to nothing", name))
		}
	}
	sort.Strings(problems)
	return problems
}

// validateRatio holds a ratio budget to what a reader of its verdict depends
// on. Every rule here is a way the row could otherwise report nothing and be
// indistinguishable from a budget that is met.
func (c *Config) validateRatio(b *Budget) []string {
	var problems []string
	id := b.ID()
	if b.Match != "" {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio is identified by its two patterns, so it takes no match:", id))
	}
	if b.Numerator == "" || b.Denominator == "" {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio needs both a numerator: and a denominator:", id))
	}
	if b.Numerator != "" && b.Numerator == b.Denominator {
		problems = append(problems, fmt.Sprintf("budget %s: the numerator and the denominator are the same pattern, so the ratio is 1 by construction", id))
	}
	if !b.MaxPctSet {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio's ceiling is max_pct, a THROUGHPUT LOSS in per cent", id))
	}
	if b.NsOpSet || b.AllocsSet || b.BOpSet {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio is not an absolute ceiling, so it takes no ns_op_max, allocs_op_max or b_op_max", id))
	}
	if b.MeasuredNsOp != 0 || b.MarginPct != 0 {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio ceiling is a specification figure, so it carries no measured_ns_op or margin_pct", id))
	}
	if b.Spec == "" {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio ceiling must cite its specification row in spec:", id))
	}
	if len(b.Gates) == 0 {
		problems = append(problems, fmt.Sprintf("budget %s: a ratio budget must name the gate set(s) it is evaluated in; a ratio in scope of a set that cannot measure both arms is inconclusive for ever", id))
	}
	for _, g := range b.Gates {
		set, ok := c.GateSets[g]
		if !ok {
			problems = append(problems, fmt.Sprintf("budget %s: gates names %q, which is not a gate set", id, g))
			continue
		}
		// The set has to SELECT both arms, or the gate it names will not
		// measure them and the row is inconclusive every time it runs.
		for what, pat := range map[string]string{"numerator": b.Numerator, "denominator": b.Denominator} {
			if pat == "" {
				continue
			}
			selected := false
			for _, p := range set {
				if Match(p, pat) || Match(pat, p) {
					selected = true
					break
				}
			}
			if !selected {
				problems = append(problems, fmt.Sprintf("budget %s: gate set %s does not select the %s %s, so the gate would never measure it", id, g, what, pat))
			}
		}
	}
	return problems
}

// Waived reports whether name is covered by an active waiver.
func (c *Config) Waived(name string, now time.Time) (Waiver, bool) {
	for _, w := range c.Waivers {
		if w.Active(now) && Match(w.Match, name) {
			return w, true
		}
	}
	return Waiver{}, false
}

// InGateSet reports whether name matches any pattern of the named gate set.
func (c *Config) InGateSet(set, name string) bool {
	for _, p := range c.GateSets[set] {
		if Match(p, name) {
			return true
		}
	}
	return false
}

// Match reports whether a benchmark name (with or without -procs suffix)
// matches pattern. Patterns are "/"-separated; within a segment "*" matches
// any run of characters; "{a,b}" alternation is expanded; a final segment of
// exactly "*" matches one or more remaining segments; the pattern "*" matches
// everything.
func Match(pattern, name string) bool {
	name = stripProcs(name)
	alts, err := expandBraces(pattern)
	if err != nil {
		return false
	}
	for _, alt := range alts {
		if matchOne(alt, name) {
			return true
		}
	}
	return false
}

func stripProcs(name string) string {
	i := strings.LastIndexByte(name, '-')
	if i < 0 {
		return name
	}
	for _, r := range name[i+1:] {
		if r < '0' || r > '9' {
			return name
		}
	}
	if i+1 == len(name) {
		return name
	}
	return name[:i]
}

func matchOne(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	ps := strings.Split(pattern, "/")
	ns := strings.Split(name, "/")
	for i, p := range ps {
		if i >= len(ns) {
			return false
		}
		if p == "*" && i == len(ps)-1 {
			return true
		}
		if !matchSegment(p, ns[i]) {
			return false
		}
	}
	return len(ps) == len(ns)
}

func matchSegment(p, s string) bool {
	if p == "*" {
		return true
	}
	parts := strings.Split(p, "*")
	if len(parts) == 1 {
		return p == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for i := 1; i < len(parts)-1; i++ {
		idx := strings.Index(s, parts[i])
		if idx < 0 {
			return false
		}
		s = s[idx+len(parts[i]):]
	}
	return strings.HasSuffix(s, parts[len(parts)-1])
}

func expandBraces(p string) ([]string, error) {
	start := strings.IndexByte(p, '{')
	if start < 0 {
		return []string{p}, nil
	}
	end := strings.IndexByte(p[start:], '}')
	if end < 0 {
		return nil, fmt.Errorf("budgets: unterminated brace in pattern %q", p)
	}
	end += start
	var out []string
	for _, alt := range strings.Split(p[start+1:end], ",") {
		rest, err := expandBraces(p[:start] + alt + p[end+1:])
		if err != nil {
			return nil, err
		}
		out = append(out, rest...)
	}
	return out, nil
}
