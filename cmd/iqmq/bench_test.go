package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
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

// TestBenchRefusesAFlagThatSizesAnAllocation: run "never panics on any
// argument vector", and bench sized its body from --size, its batches from
// --batch and its publisher goroutines from --concurrency with only a check
// for a negative value. A large --size panicked in makeslice after the open,
// a large --batch panicked in a publisher goroutine, which kills the process
// from where no caller can recover, and a large --concurrency held the run in
// a loop over workers that had nothing to do. Each now has a ceiling, and a
// value past it is refused before anything is opened.
func TestBenchRefusesAFlagThatSizesAnAllocation(t *testing.T) {
	huge := itoa(1 << 50)
	for _, c := range []struct{ flag, value, want string }{
		{"--size", huge, "--size takes at most 1 GiB"},
		{"--size", itoa(maxBenchSize + 1), "--size takes at most 1 GiB"},
		{"--batch", huge, "--batch takes at most " + itoa(maxBenchBatch)},
		{"--concurrency", huge, "--concurrency takes at most " + itoa(maxBenchConcurrency)},
	} {
		t.Run(c.flag+" "+c.value, func(t *testing.T) {
			defer func() {
				if v := recover(); v != nil {
					t.Errorf("bench %s %s panicked: %v", c.flag, c.value, v)
				}
			}()
			h := newHarness(t)
			if got := h.run("bench", memDSN, "orders", "--messages", "1", c.flag, c.value); got != exitUsage {
				t.Errorf("bench %s %s = %d, want %d\nstderr:\n%s", c.flag, c.value, got, exitUsage, h.stderr())
			}
			if !strings.Contains(h.stderr(), c.want) {
				t.Errorf("the refusal does not say %q:\n%s", c.want, h.stderr())
			}
		})
	}
}

// TestBenchStopsAtItsTimeoutWithAHugeMessageCount: bench sized its latency
// samples from --messages, so a large count panicked in makeslice before
// anything was published (and a count under the runtime's limit asked the
// host for 8 bytes a message). The samples now grow as they are taken, and a
// publisher stops at the run's end instead of failing every publication it
// had left, one by one. The --timeout fires once the first publication is
// done.
func TestBenchStopsAtItsTimeoutWithAHugeMessageCount(t *testing.T) {
	clock := newOnDemandClock(time.Minute)
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		Hooks: mq.Hooks{OnPublishDone: func(context.Context, mq.PublishDoneEvent) {
			clock.fire()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	reg := dsn.NewRegistry()
	if err := reg.Register("benchmem", func(ctx context.Context, _ string) (*mq.Client, error) {
		return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if v := recover(); v != nil {
			t.Errorf("bench --messages %d panicked: %v", 1<<50, v)
		}
	}()
	h := harnessWith(t, reg, nil).withClock(clock)
	code := h.run("bench", "benchmem://x", "orders", "-o", "json", "--messages", itoa(1<<50),
		"--concurrency", "4", "--timeout", "1m")
	var rep benchReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("bench's JSON does not parse: %v\nstdout:\n%s\nstderr:\n%s", err, h.stdout(), h.stderr())
	}
	if code != exitFailure || rep.Messages == 0 || rep.Messages+rep.Failures != 1<<50 {
		t.Errorf("bench = %d with %d published and %d failed of %d, want exit %d with every one of "+
			"them accounted for\nstderr:\n%s", code, rep.Messages, rep.Failures, 1<<50, exitFailure, h.stderr())
	}
}

// benchSeamHarness registers "benchseam", a target whose sink takes every
// publication and whose source is src.
func benchSeamHarness(t *testing.T, src mq.Source) *harness {
	t.Helper()
	b := &sourceBackend{
		seamBackend: seamBackend{
			caps: seamCaps(t, mq.FeatureNack, mq.FeatureAtLeastOnce, mq.FeaturePublisherConfirm),
			sink: &fakeSink{log: &seamLog{}},
		},
		src: src,
	}
	reg := dsn.NewRegistry()
	if err := reg.Register("benchseam", func(context.Context, string) (*mq.Client, error) {
		return mq.NewClient(b), nil
	}); err != nil {
		t.Fatal(err)
	}
	return harnessWith(t, reg, nil)
}

// benchJSON runs bench against "benchseam" and decodes its report.
func benchJSON(t *testing.T, h *harness, args ...string) (int, benchReport) {
	t.Helper()
	code := h.run(append([]string{"bench", "benchseam://", "orders", "-o", "json"}, args...)...)
	var rep benchReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("bench's JSON does not parse: %v\nstdout:\n%s\nstderr:\n%s", err, h.stdout(), h.stderr())
	}
	return code, rep
}

// TestBenchFailsADrainItsTimeoutCutShort: `bench --subscription` measures the
// round trip, and exit 1 is "a deadline passed with the work unfinished".
// benchDrain took a Receive that ended on the arm's --timeout as the end of
// the drain and reported nothing, so a run whose subscription received none
// of the messages it published exited 0 with "consumed 0" and an empty
// stderr. The source here hands nothing out, which is a subscription the
// messages do not reach.
func TestBenchFailsADrainItsTimeoutCutShort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &seamLog{}
		src := &fakeSource{log: log, settler: &fakeSettler{log: log}, closed: make(chan struct{})}
		h := benchSeamHarness(t, src)
		code, rep := benchJSON(t, h, "--messages", "5", "--subscription", "s", "--timeout", "2s")
		if code != exitFailure || rep.Consumed != 0 || rep.Failures == 0 {
			t.Errorf("bench = %d with %d of %d consumed and failures %d, want exit %d and the drain "+
				"counted as a failure\nstderr:\n%s", code, rep.Consumed, rep.Messages, rep.Failures,
				exitFailure, h.stderr())
		}
		if !strings.Contains(h.stderr(), "the --timeout of 2s ran out with 0 of the 5 messages consumed") {
			t.Errorf("bench does not say that its --timeout ended the drain:\n%s", h.stderr())
		}
	})
}

// TestBenchCountsAFailedDrainInTheReport: a drain whose Receive fails makes
// bench exit 1, but the failure count was copied into the report before the
// drain ran, so the JSON "failures" and the text FAILURES row said nothing
// had failed.
func TestBenchCountsAFailedDrainInTheReport(t *testing.T) {
	h := benchSeamHarness(t, brokenSource{settler: &fakeSettler{log: &seamLog{}}})
	code, rep := benchJSON(t, h, "--messages", "5", "--subscription", "s")
	if code != exitFailure || rep.Failures != 1 {
		t.Errorf("bench = %d with failures %d after its drain failed, want %d and 1\nstderr:\n%s",
			code, rep.Failures, exitFailure, h.stderr())
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

// TestPercentile is the arithmetic on its own: the nearest rank, which is the
// smallest sample that at least a fraction p of the samples do not exceed. The
// index was floor(p*n), one rank high whenever p*n is whole, so the p99 of a
// hundred publications was the slowest of them and the p50 of two the larger.
func TestPercentile(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile of nothing = %v, want 0", got)
	}
	for _, c := range []struct {
		n    int
		p    float64
		rank int
	}{
		{1, 0.99, 1},
		{1, 0.50, 1},
		{2, 0.50, 1},
		{10, 0.90, 9},
		{100, 0.50, 50},
		{100, 0.90, 90},
		{100, 0.99, 99},
		{100, 1.00, 100},
		{101, 0.50, 51},
		{1000, 0.99, 990},
		{3, 0, 1},
	} {
		s := durations(c.n)
		if got := percentile(s, c.p); got != s[c.rank-1] {
			t.Errorf("p%v of %d samples = %v, want rank %d (%v)", c.p*100, c.n, got, c.rank, s[c.rank-1])
		}
	}
}

// TestBenchWarmupIsNotDrainedAsMeasured: bench opens the consumer before the
// warm-up, so the warm-up publications reach --subscription too, and the drain
// stopped after the measured count. The warm-up messages were counted as
// consumed measured ones and as many measured messages were left on the
// subscription unread and unacked, for the next reader; a Receive that crossed
// the count acked past it as well. The drain now takes the warm-up first,
// unmeasured, and acks no more than the run published: messages already on
// the subscription before the run are left for their reader.
func TestBenchWarmupIsNotDrainedAsMeasured(t *testing.T) {
	for _, c := range []struct{ messages, warmup, before int }{{200, 100, 0}, {10, 5, 0}, {10, 5, 10}} {
		t.Run(itoa(c.messages)+" after "+itoa(c.warmup)+" behind "+itoa(c.before), func(t *testing.T) {
			b, err := memory.NewBroker(memory.Config{AutoCreate: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			if c.before > 0 {
				ensure(t, b, "orders", "s")
				msgs := make([]*mq.Message, c.before)
				for i := range msgs {
					msgs[i] = &mq.Message{Body: []byte("left by an earlier run")}
				}
				publish(t, b, "orders", msgs...)
			}
			reg := dsn.NewRegistry()
			if err := reg.Register("benchmem", func(ctx context.Context, _ string) (*mq.Client, error) {
				return memory.OpenWithClient(ctx, b, memory.ClientBorrowed)
			}); err != nil {
				t.Fatal(err)
			}
			h := harnessWith(t, reg, nil)
			code := h.run("bench", "benchmem://x", "orders", "-o", "json", "--subscription", "s",
				"--messages", itoa(c.messages), "--warmup", itoa(c.warmup))
			if code != exitOK {
				t.Fatalf("bench = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
			}
			var rep benchReport
			if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
				t.Fatalf("bench's JSON does not parse: %v\n%s", err, h.stdout())
			}
			snap, err := b.Inspect("orders", "s")
			if err != nil {
				t.Fatal(err)
			}
			if rep.Consumed != c.messages {
				t.Errorf("consumed = %d, want the %d measured messages", rep.Consumed, c.messages)
			}
			if left := snap.Ready + snap.InFlight; left != c.before || snap.Acked != int64(c.messages+c.warmup) {
				t.Errorf("the subscription holds %d message(s) and acked %d after the run, want %d and %d",
					left, snap.Acked, c.before, c.messages+c.warmup)
			}
		})
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
