package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestBenchIsTiny is the smoke run: a handful of messages, seconds not
// minutes. This command is a feature to implement and smoke-test, not a
// measurement campaign, and the host it runs on belongs to other people.
func TestBenchIsTiny(t *testing.T) {
	h := newHarness(t)
	code := h.run("bench", memDSN, "orders", "--messages", "20", "--size", "16")
	if code != exitOK {
		t.Fatalf("bench = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{"20 in ", "msg/s, operational", "publish latency", "host"} {
		if !strings.Contains(out, want) {
			t.Errorf("bench's report does not contain %q:\n%s", want, out)
		}
	}
}

// TestBenchSaysItsFiguresAreNotTheBudgets is the one assertion this command
// must never lose.
//
// the figures come from the benchmarks module under a load guard on a
// quiet host, and `make gate` is what enforces them. A CLI number from a
// loaded laptop must not be mistakable for one of those, in either output
// form.
func TestBenchSaysItsFiguresAreNotTheBudgets(t *testing.T) {
	h := newHarness(t)
	if got := h.run("bench", memDSN, "orders", "--messages", "5"); got != exitOK {
		t.Fatalf("bench = %d: %s", got, h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{
		"OPERATIONAL",
		"NOT the budgets",
		"benchguard.sh",
		"make gate",
		"not read two runs of this command as a regression gate",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("bench's text output does not say %q, so its figures could be quoted as "+
				"iqmq's performance:\n%s", want, out)
		}
	}
	// The notice comes BEFORE any figure, so a run somebody interrupts has
	// still said what its numbers are worth.
	noticeAt := strings.Index(out, "OPERATIONAL")
	figureAt := strings.Index(out, "msg/s")
	if noticeAt < 0 || figureAt < 0 || noticeAt > figureAt {
		t.Errorf("the notice does not precede the figures (notice at %d, figure at %d):\n%s",
			noticeAt, figureAt, out)
	}

	// And the machine-readable form, which is what a script or a dashboard
	// would read. Prose is not a contract; a field is.
	h2 := newHarness(t)
	if got := h2.run("bench", memDSN, "orders", "--messages", "5", "-o", "json"); got != exitOK {
		t.Fatalf("bench -o json = %d: %s", got, h2.stderr())
	}
	var rep benchReport
	if err := json.Unmarshal([]byte(h2.stdout()), &rep); err != nil {
		t.Fatalf("bench's JSON does not parse: %v\n%s", err, h2.stdout())
	}
	if rep.Source != benchSourceKey {
		t.Errorf("bench's JSON source = %q, want %q", rep.Source, benchSourceKey)
	}
	if rep.Authoritative {
		t.Error("bench's JSON claims to be authoritative")
	}
	if rep.Notice == "" {
		t.Error("bench's JSON carries no notice")
	}
	if rep.Host.GOMAXPROCS == 0 || rep.Host.NumCPU == 0 {
		t.Errorf("bench's JSON does not describe the host (%+v); a reader who cannot see the "+
			"load cannot judge the number", rep.Host)
	}
}

// TestBenchTraceWritesAFlightRecording covers --trace: the flight recorder is
// the tool for "it was fast and then it stalled", which a summary cannot show.
func TestBenchTraceWritesAFlightRecording(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iqmq.trace")
	h := newHarness(t)
	code := h.run("bench", memDSN, "orders", "--messages", "50", "--trace", path)
	if code != exitOK {
		t.Fatalf("bench --trace = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the flight recording was not written: %v", err)
	}
	if info.Size() == 0 {
		t.Error("the flight recording is empty")
	}
	if !strings.Contains(h.stdout(), "go tool trace") {
		t.Errorf("bench did not say how to read the recording:\n%s", h.stdout())
	}
}

// TestBenchVariantsSweepsTheMemoryEngine covers --variants against the one
// target a unit test can reach.
//
// The memory driver's DSN selects kinds in BULK -- engine=safe pins nine
// families at once -- so its sweep has two arms that each move nine families,
// and the label must say `engine` rather than a family id. Calling it the specification would
// be a lie about what was measured.
func TestBenchVariantsSweepsTheMemoryEngine(t *testing.T) {
	h := newHarness(t)
	code := h.run("bench", memDSN, "orders", "--messages", "10", "--variants")
	if code != exitOK {
		t.Fatalf("bench --variants = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if strings.Count(out, "arm engine") != 2 {
		t.Errorf("the memory sweep produced %d arms, want 2 (the parameter absent, and "+
			"engine=safe):\n%s", strings.Count(out, "arm engine"), out)
	}
	if !strings.Contains(out, "engine = safe") {
		t.Errorf("the sweep has no engine=safe arm:\n%s", out)
	}
	if !strings.Contains(out, "every Auto binding") {
		t.Errorf("the sweep does not say what the parameter-absent arm means:\n%s", out)
	}
	for _, forbidden := range []string{"arm A1", "arm A2 "} {
		if strings.Contains(out, forbidden) {
			t.Errorf("the memory sweep labelled a bulk selection as one family (%q):\n%s",
				forbidden, out)
		}
	}
}

// TestBenchVariantsRefusesATargetThatSelectsNone: running one arm and calling
// it a sweep would be the wrong answer.
func TestBenchVariantsRefusesATargetThatSelectsNone(t *testing.T) {
	h := newFullHarness(t)
	code := h.run("bench", "nats://127.0.0.1:4222", "orders", "--variants", "--messages", "1")
	if code != exitUsage {
		t.Errorf("bench --variants against a target with no swept family = %d, want %d\n"+
			"stderr:\n%s", code, exitUsage, h.stderr())
	}
	for _, want := range []string{"no variant family", "iqmq doctor"} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("the refusal does not say %q:\n%s", want, h.stderr())
		}
	}
}

// TestBenchVariantsNamesWhatTheTargetDoesSelect: a wrong family name gets the
// list rather than a bare refusal.
func TestBenchVariantsNamesWhatTheTargetDoesSelect(t *testing.T) {
	h := newFullHarness(t)
	code := h.run("bench", "kafka://127.0.0.1:9092", "orders", "--family", "A99", "--messages", "1")
	if code != exitUsage {
		t.Errorf("bench -- = %d, want %d", code, exitUsage)
	}
	for _, want := range []string{"A14", "A15", "tracker", "nack"} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("the refusal does not list what kafka does select (%q):\n%s", want, h.stderr())
		}
	}
}

// TestSpliceParam is the DSN rewriting a sweep depends on. It has to preserve
// the credential -- the arm's DSN must still OPEN the target -- while never
// being printed, which is why it splices the raw text rather than re-rendering
// a parsed Config.
func TestSpliceParam(t *testing.T) {
	for _, c := range []struct {
		raw, key, value, want string
	}{
		{"kafka://h:9092", "tracker", "bitset", "kafka://h:9092?tracker=bitset"},
		{"kafka://h:9092?a=1", "tracker", "bitset", "kafka://h:9092?a=1&tracker=bitset"},
		{"kafka://h:9092?tracker=heap", "tracker", "bitset", "kafka://h:9092?tracker=bitset"},
		{"kafka://h:9092?tracker=heap&a=1", "tracker", "bitset", "kafka://h:9092?a=1&tracker=bitset"},
		{"kafka://h:9092?tracker=heap", "tracker", "", "kafka://h:9092"},
		{"memory://?autocreate=true", "engine", "safe", "memory://?autocreate=true&engine=safe"},
		{"memory://?autocreate=true&engine=safe", "engine", "", "memory://?autocreate=true"},
	} {
		got, err := spliceParam(c.raw, c.key, c.value)
		if err != nil {
			t.Errorf("spliceParam(%q, %q, %q): %v", c.raw, c.key, c.value, err)
			continue
		}
		if got != c.want {
			t.Errorf("spliceParam(%q, %q, %q) = %q, want %q", c.raw, c.key, c.value, got, c.want)
		}
	}

	// The credential survives, or the arm cannot open the broker.
	spliced, err := spliceParam("sqs://key:"+credPassword+"@h:9324?region=x", "settle", "immediate")
	if err != nil {
		t.Fatalf("spliceParam: %v", err)
	}
	if !strings.Contains(spliced, credPassword) {
		t.Errorf("the credential was lost, so the arm's DSN cannot open the target: %q",
			redactTarget(spliced))
	}
	// And the arm's DSN is never what gets printed.
	requireAbsent(t, "spliced arm DSN rendered for display", credPassword, redactTarget(spliced))

	// A malformed DSN is refused here rather than at the driver.
	if _, err := spliceParam("not-a-dsn", "k", "v"); err == nil {
		t.Error("spliceParam accepted a DSN that does not parse")
	}
}

// TestBenchRejectsNonsenseSizes covers the validation, which happens before a
// connection: a contradictory invocation must not reach a broker.
func TestBenchRejectsNonsenseSizes(t *testing.T) {
	for _, args := range [][]string{
		{"--messages", "0"},
		{"--messages", "-1"},
		{"--size", "-1"},
		{"--concurrency", "0"},
		{"--batch", "-2"},
		{"--warmup", "-1"},
		{"-o", "yaml"},
	} {
		h := newHarness(t)
		full := append([]string{"bench", memDSN, "orders"}, args...)
		if got := h.run(full...); got != exitUsage {
			t.Errorf("bench %v = %d, want %d", args, got, exitUsage)
		}
		if h.stderr() == "" {
			t.Errorf("bench %v said nothing", args)
		}
	}
}

// TestBenchConcurrencyCoversEveryMessage: the work split must not lose a
// message to integer division.
func TestBenchConcurrencyCoversEveryMessage(t *testing.T) {
	for _, c := range []struct{ messages, concurrency int }{
		{10, 3}, {7, 7}, {1, 4}, {100, 7},
	} {
		h := newHarness(t)
		code := h.run("bench", memDSN, "orders", "-o", "json",
			"--messages", itoa(c.messages), "--concurrency", itoa(c.concurrency))
		if code != exitOK {
			t.Fatalf("bench %d/%d = %d: %s", c.messages, c.concurrency, code, h.stderr())
		}
		var rep benchReport
		if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
			t.Fatalf("parsing: %v\n%s", err, h.stdout())
		}
		if rep.Messages != c.messages {
			t.Errorf("bench --messages %d --concurrency %d published %d; the work split loses "+
				"messages to integer division", c.messages, c.concurrency, rep.Messages)
		}
	}
}

// TestPercentile is the arithmetic on its own.
func TestPercentile(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile of nothing = %v, want 0", got)
	}
	one := durations(1)
	if got := percentile(one, 0.99); got != one[0] {
		t.Errorf("percentile of one sample = %v, want %v", got, one[0])
	}
	hundred := durations(100)
	if got := percentile(hundred, 1.0); got != hundred[99] {
		t.Errorf("p100 = %v, want the maximum %v", got, hundred[99])
	}
	if got := percentile(hundred, 0.5); got != hundred[50] {
		t.Errorf("p50 = %v, want %v", got, hundred[50])
	}
}

// durations builds a sorted slice of n distinct durations.
func durations(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i+1) * time.Microsecond
	}
	return out
}
