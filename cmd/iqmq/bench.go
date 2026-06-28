package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
)

const benchUsage = `usage: iqmq bench [flags] <dsn> <topic>

Generate load against a target and report what this run saw.

THE FIGURES ARE OPERATIONAL. They are measured by this CLI, on this host, at
whatever moment you ran it, against whatever else the machine is doing. They
are NOT iqmq's budgets: the figures come from the benchmarks
module, run under hack/benchguard.sh on a quiet host with contention recorded
and a run discarded when another benchmark touched the machine, and "make gate"
is what holds a change to them. Do not quote a number from here as iqmq's
performance, and do not compare two runs of this command as if it were a
regression gate.

What it IS for: checking that a broker is reachable and roughly how fast this
machine talks to it, and sweeping a variant family to see whether a kind makes
a difference on your workload.

flags:
      --messages N       publications per arm (default 1000)
      --size BYTES       body size (default 256)
      --concurrency N    publisher goroutines (default 1)
      --batch N          messages per PublishBatch (0 = one publication at a time)
      --warmup N         publications to discard before measuring (default 0)
      --subscription S   also consume from S and measure the round trip
      --variants         run one arm per kind of every variant family this
                         target selects through its DSN
      --family ID sweep this family alone (a the specification such as A14, or
                         its key such as offset); implies --variants
      --trace PATH       write a Go execution-trace flight recording to PATH
      --trace-min AGE    how much history the flight recorder keeps (default 5s)
      --timeout DURATION bound each arm (default 60s)
  -o, --output text|json output format (default text)

--trace turns on runtime/trace's flight recorder for the run and writes the
window it still holds when the run ends. The recording is a "go tool trace"
file; it is the tool for "the first two hundred publications were fast and then
something stalled", which a summary line cannot show you.
`

// benchOptions is one bench invocation.
type benchOptions struct {
	messages    int
	size        int
	concurrency int
	batch       int
	warmup      int
	sub         string
	family      string
	wantVariant bool
	tracePath   string
	traceMin    time.Duration
	timeout     time.Duration
	asJSON      bool
	// notes are the things the sweep could not do, collected while the arms
	// are built and printed before the first one runs.
	notes []string
}

// The disclaimer. It is a constant rather than a sentence built at the call
// site because it appears in both output formats and in the tests that hold it
// there: a figure this command printed must never be mistakable for the specification's.
const (
	benchSourceKey = "cli-operational"
	benchNotice    = "NOTE: these figures are OPERATIONAL, measured by the iqmq CLI on this host " +
		"under whatever else was running.\n" +
		"They are NOT the budgets, which come from the benchmarks module under " +
		"hack/benchguard.sh\n" +
		"on a quiet host with contention recorded, and which \"make gate\" is what enforces. " +
		"Do not quote them as iqmq's\n" +
		"performance and do not read two runs of this command as a regression gate.\n"
)

// benchReport is one arm's result.
type benchReport struct {
	// Source is the fixed marker that says where the number came from. A
	// machine reading this output branches on it rather than on prose.
	Source string `json:"source"`
	// Authoritative is always false here. It is present, and false, so that a
	// consumer of the JSON that forgot to check Source still has to explain
	// why it ignored this.
	Authoritative bool   `json:"authoritative"`
	Notice        string `json:"notice"`

	Target string `json:"target"`
	Topic  string `json:"topic"`
	Arm    string `json:"arm,omitempty"`
	Family string `json:"family,omitempty"`

	Host hostReport `json:"host"`

	Messages  int    `json:"messages"`
	Bytes     int64  `json:"bytes"`
	Failures  int    `json:"failures"`
	Duration  string `json:"duration"`
	PerSecond int64  `json:"messages_per_second"`

	P50 string `json:"publish_p50,omitempty"`
	P90 string `json:"publish_p90,omitempty"`
	P99 string `json:"publish_p99,omitempty"`
	Max string `json:"publish_max,omitempty"`

	Consumed        int    `json:"consumed,omitempty"`
	ConsumeDuration string `json:"consume_duration,omitempty"`

	Trace string `json:"trace,omitempty"`
}

// hostReport is what the machine looked like, which is the whole reason the
// figures above are not budgets: a reader who cannot see the load cannot judge
// the number.
type hostReport struct {
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	NumCPU     int    `json:"num_cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	// Load is the host's 1/5/15-minute load average where the platform
	// reports one, empty where it does not. It is recorded rather than acted
	// on: The host guard is what refuses to measure a busy host, and this
	// command deliberately does not pretend to be that guard.
	Load string `json:"load_average,omitempty"`
}

// hostOf describes the machine.
func hostOf() hostReport {
	h := hostReport{
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		f := strings.Fields(string(b))
		if len(f) >= 3 {
			h.Load = strings.Join(f[:3], " ")
		}
	}
	return h
}

// cmdBench is the bench subcommand.
func cmdBench(e *cliEnv, args []string) int {
	fs := newFlagSet("bench", e)
	var (
		o       benchOptions
		output  = fs.String("o", "text", "output format: text or json")
		outputL = fs.String("output", "", "output format: text or json")
	)
	fs.IntVar(&o.messages, "messages", 1000, "publications per arm")
	fs.IntVar(&o.size, "size", 256, "body size in bytes")
	fs.IntVar(&o.concurrency, "concurrency", 1, "publisher goroutines")
	fs.IntVar(&o.batch, "batch", 0, "messages per PublishBatch")
	fs.IntVar(&o.warmup, "warmup", 0, "publications to discard before measuring")
	fs.StringVar(&o.sub, "subscription", "", "also consume from this subscription")
	fs.BoolVar(&o.wantVariant, "variants", false, "sweep every variant family the DSN selects")
	fs.StringVar(&o.family, "family", "", "sweep this family alone; implies --variants")
	fs.StringVar(&o.tracePath, "trace", "", "write a flight recording to this path")
	fs.DurationVar(&o.traceMin, "trace-min", 5*time.Second, "how much history the flight recorder keeps")
	fs.DurationVar(&o.timeout, "timeout", 60*time.Second, "bound each arm")
	if code := parseFlags(fs, e, args, benchUsage); code >= 0 {
		return code
	}
	if o.family != "" {
		o.wantVariant = true
	}
	if fs.NArg() != 2 {
		_, _ = io.WriteString(e.stderr, "iqmq bench: a DSN and a topic are required\n"+benchUsage)
		return exitUsage
	}
	switch format := formatOf(*output, *outputL); format {
	case "text":
	case "json":
		o.asJSON = true
	default:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --output takes text or json, not "+quote(format)+"\n")
		return exitUsage
	}
	switch {
	case o.messages <= 0:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --messages takes at least 1\n")
		return exitUsage
	case o.size < 0:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --size takes a non-negative size\n")
		return exitUsage
	case o.concurrency < 1:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --concurrency takes at least 1\n")
		return exitUsage
	case o.batch < 0:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --batch takes a non-negative size\n")
		return exitUsage
	case o.warmup < 0:
		_, _ = io.WriteString(e.stderr, "iqmq bench: --warmup takes a non-negative count\n")
		return exitUsage
	}

	target, topic := fs.Arg(0), fs.Arg(1)
	scheme, err := dsn.Scheme(target)
	if err != nil {
		return fail(e, "bench", err)
	}

	e, flush := e.scrubbed(target)
	defer flush()
	out := e.stdout

	// The notice goes out before anything is measured, so that a run somebody
	// interrupts has still said what its numbers are worth.
	if !o.asJSON {
		_, _ = io.WriteString(out, benchNotice+"\n")
	}

	// The flight recorder spans every arm: a stall in arm three is what one is
	// for, and a recorder started per arm would lose the transition.
	var recorder *trace.FlightRecorder
	if o.tracePath != "" {
		recorder = trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: o.traceMin})
		if err := recorder.Start(); err != nil {
			return fail(e, "bench", err)
		}
		defer recorder.Stop()
	}

	arms, aerr := benchArms(scheme, target, &o)
	if aerr != nil {
		_, _ = io.WriteString(e.stderr, "iqmq bench: "+aerr.Error()+"\n")
		return exitUsage
	}

	for _, n := range o.notes {
		_, _ = io.WriteString(out, n+"\n")
	}

	code := exitOK
	for _, arm := range arms {
		rep, acode := e.benchArm(&o, arm, topic)
		if rep != nil {
			rep.Target = redactTarget(target)
			rep.Topic = topic
			if o.tracePath != "" {
				rep.Trace = o.tracePath
			}
			writeBenchReport(out, rep, &o)
		}
		if acode != exitOK {
			code = acode
		}
	}

	if recorder != nil {
		if werr := writeFlightRecording(recorder, o.tracePath); werr != nil {
			_, _ = io.WriteString(e.stderr, "iqmq bench: writing the flight recording: "+werr.Error()+"\n")
			code = exitFailure
		} else {
			_, _ = io.WriteString(out, "flight recording written to "+o.tracePath+
				"  (read it with: go tool trace "+o.tracePath+")\n")
		}
	}
	return code
}

// writeFlightRecording snapshots the recorder's window into a file.
func writeFlightRecording(r *trace.FlightRecorder, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := r.WriteTo(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// benchArm is one arm of a run: a DSN and the label that says which variant
// kind it selects.
type benchArm struct {
	// dsn is the arm's own target, which may carry a variant parameter this
	// command spliced in. It is never printed.
	dsn string
	// family is the variant family the arm varies, empty for a single run.
	family string
	// kind is the kind this arm selects, empty for a single run.
	kind string
}

// benchArms builds the arm list: one arm for an ordinary run, or one per kind
// for a --variants sweep.
func benchArms(scheme, target string, o *benchOptions) ([]benchArm, error) {
	if !o.wantVariant {
		return []benchArm{{dsn: target}}, nil
	}
	sweeps := sweepsForTarget(scheme)
	if len(sweeps) == 0 {
		return nil, errors.New("this target selects no variant family through its DSN, so there is " +
			"nothing to sweep; \"iqmq doctor " + redactTarget(target) + "\" lists the families it does resolve")
	}
	var chosen []benchSweep
	if o.family == "" {
		chosen = sweeps
	} else {
		for _, s := range sweeps {
			if s.family == o.family || s.param == o.family ||
				strings.HasPrefix(s.family, o.family+" ") || strings.HasSuffix(s.family, " "+o.family) {
				chosen = append(chosen, s)
			}
		}
		if len(chosen) == 0 {
			names := make([]string, 0, len(sweeps))
			for _, s := range sweeps {
				names = append(names, s.family+" ("+s.param+"=)")
			}
			return nil, errors.New("this target does not select " + quote(o.family) +
				"; it selects: " + strings.Join(names, ", "))
		}
	}
	var arms []benchArm
	for _, s := range chosen {
		for k, why := range s.excluded {
			o.notes = append(o.notes, "note: "+s.family+" has the kind "+quote(k)+
				" and this sweep does not measure it: "+why)
		}
		for _, k := range s.kinds {
			armDSN, err := spliceParam(target, s.param, k)
			if err != nil {
				return nil, err
			}
			arms = append(arms, benchArm{dsn: armDSN, family: s.family, kind: k})
		}
	}
	return arms, nil
}

// benchSweep is one variant family as a target can select it.
type benchSweep struct {
	family string
	param  string
	kinds  []string
	// excluded are the family's kinds this sweep cannot measure, with the
	// reason. They are PRINTED: a sweep that ran three arms for a four-kind
	// family without saying so would read as a complete comparison.
	excluded map[string]string
}

// sweepsForTarget lists the families a scheme selects through its DSN.
//
// The memory driver is the special case, and it is a real one: its DSN selects
// kinds only in BULK. "engine=safe" pins every family at once to its
// mutex-backed reference kind and there is no per-family parameter, so the
// sweep has two arms that each move nine families rather than one arm per kind
// of one family. Calling that "A1" would be a lie about what was measured.
func sweepsForTarget(scheme string) []benchSweep {
	var out []benchSweep
	if scheme == "memory" || scheme == "mem" {
		out = append(out, benchSweep{
			family: "engine",
			param:  "engine",
			// "" is the absence of the parameter, which is every family's
			// Auto; the memory DSN has no spelling for "auto" on purpose.
			kinds: []string{"", "safe"},
		})
	}
	fams := sweepsFor(scheme)
	for i := range fams {
		f := &fams[i]
		out = append(out, benchSweep{
			family: f.id + " " + f.key, param: f.param,
			kinds: f.dsnKinds(), excluded: f.dsnExcluded,
		})
	}
	return out
}

// spliceParam returns raw with key set to value, or with key removed when
// value is empty.
//
// It splices the raw text rather than re-rendering a parsed Config, because
// [dsn.Config.String] redacts the password on purpose and a re-rendered DSN
// would no longer open the target. The result is validated by parsing it, so a
// splice that produced a malformed DSN fails here rather than at the driver.
func spliceParam(raw, key, value string) (string, error) {
	if _, err := dsn.Parse(raw); err != nil {
		return "", err
	}
	head, query, hasQuery := strings.Cut(raw, "?")
	var kept []string
	if hasQuery {
		for _, pair := range strings.Split(query, "&") {
			if pair == "" {
				continue
			}
			k, _, _ := strings.Cut(pair, "=")
			if k == key {
				continue
			}
			kept = append(kept, pair)
		}
	}
	if value != "" {
		kept = append(kept, key+"="+value)
	}
	out := head
	if len(kept) > 0 {
		out += "?" + strings.Join(kept, "&")
	}
	if _, err := dsn.Parse(out); err != nil {
		return "", err
	}
	return out, nil
}

// benchArm runs one arm.
func (e *cliEnv) benchArm(o *benchOptions, arm benchArm, topic string) (*benchReport, int) {
	ctx, cancel := e.deadline(e.ctx, o.timeout)
	defer cancel()
	client, code := e.open(ctx, "bench", arm.dsn)
	if code >= 0 {
		return nil, code
	}
	defer func() { _ = client.Close() }()

	// The consumer is opened BEFORE anything is published, when one is
	// wanted. A round trip that published first and subscribed afterwards
	// would have to replay -- and a topic that fans out at publication time
	// has nothing to replay, so `--subscription` failed with a bare
	// "unsupported" against the reference driver until this order was fixed.
	// Measuring the delivery of the messages this run publishes is also the
	// thing the flag is for.
	var con *mq.ConsumerHandle
	if o.sub != "" {
		opened, cerr := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: o.sub})
		if cerr != nil {
			_, _ = io.WriteString(e.stderr, "iqmq bench: opening the subscription: "+cerr.Error()+"\n")
			return nil, codeFor(cerr)
		}
		con = opened
		defer func() { _ = con.Close() }()
	}

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq bench: "+err.Error()+"\n")
		return nil, codeFor(err)
	}
	defer func() { _ = pub.Close() }()

	body := make([]byte, o.size)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	// The warm-up publications are not measured and not counted. They are not
	// silently folded into the total either: a run with --warmup says how many
	// it discarded.
	for range o.warmup {
		m := &mq.Message{Body: body}
		if err := pub.Publish(ctx, m); err != nil {
			_, _ = io.WriteString(e.stderr, "iqmq bench: warm-up: "+err.Error()+"\n")
			return nil, codeFor(err)
		}
	}

	rep := &benchReport{
		Source: benchSourceKey, Authoritative: false, Notice: benchNotice,
		Arm: arm.kind, Family: arm.family, Host: hostOf(),
	}

	var (
		mu       sync.Mutex
		latency  = make([]time.Duration, 0, o.messages)
		failures int
	)
	perWorker := o.messages / o.concurrency
	extra := o.messages % o.concurrency
	start := e.clock.Now()
	var wg sync.WaitGroup
	for w := range o.concurrency {
		n := perWorker
		if w < extra {
			n++
		}
		if n == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, n)
			localFail := 0
			for range n {
				d, err := e.publishOnce(ctx, pub, body, o.batch)
				if err != nil {
					localFail++
					continue
				}
				local = append(local, d)
			}
			mu.Lock()
			latency = append(latency, local...)
			failures += localFail
			mu.Unlock()
		}()
	}
	wg.Wait()
	if err := pub.Flush(ctx); err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq bench: flush: "+err.Error()+"\n")
		failures++
	}
	elapsed := e.clock.Now().Sub(start)

	rep.Messages = len(latency) * max(o.batch, 1)
	rep.Bytes = int64(rep.Messages) * int64(o.size)
	rep.Failures = failures
	rep.Duration = elapsed.Round(time.Microsecond).String()
	if elapsed > 0 {
		rep.PerSecond = int64(float64(rep.Messages) / elapsed.Seconds())
	}
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	if len(latency) > 0 {
		rep.P50 = percentile(latency, 0.50).String()
		rep.P90 = percentile(latency, 0.90).String()
		rep.P99 = percentile(latency, 0.99).String()
		rep.Max = latency[len(latency)-1].String()
	}

	if con != nil {
		consumed, cd, cerr := e.benchDrain(ctx, con, rep.Messages)
		rep.Consumed = consumed
		rep.ConsumeDuration = cd.Round(time.Microsecond).String()
		if cerr != nil {
			_, _ = io.WriteString(e.stderr, "iqmq bench: consuming: "+cerr.Error()+"\n")
			failures++
		}
	}
	if failures > 0 {
		return rep, exitFailure
	}
	return rep, exitOK
}

// publishOnce publishes one message, or one batch of them, and returns how long
// it took on the environment's clock.
func (e *cliEnv) publishOnce(
	ctx context.Context, pub *mq.PublisherHandle, body []byte, batch int,
) (time.Duration, error) {
	if batch <= 1 {
		m := &mq.Message{Body: body}
		start := e.clock.Now()
		err := pub.Publish(ctx, m)
		return e.clock.Now().Sub(start), err
	}
	ms := make([]*mq.Message, batch)
	for i := range ms {
		ms[i] = &mq.Message{Body: body}
	}
	start := e.clock.Now()
	err := pub.PublishBatch(ctx, ms)
	return e.clock.Now().Sub(start), err
}

// benchDrain drains up to want messages from an already-open consumer,
// acknowledging each one, and reports how long that took.
func (e *cliEnv) benchDrain(
	ctx context.Context, con *mq.ConsumerHandle, want int,
) (int, time.Duration, error) {
	var err error
	dst := make([]mq.Delivery, 128)
	got := 0
	start := e.clock.Now()
	for got < want {
		n, rerr := con.Receive(ctx, dst)
		for i := range n {
			if aerr := dst[i].Ack(); aerr != nil && err == nil {
				err = aerr
			}
			got++
		}
		if rerr != nil {
			if errors.Is(rerr, context.Canceled) || errors.Is(rerr, context.DeadlineExceeded) {
				break
			}
			return got, e.clock.Now().Sub(start), rerr
		}
		if n == 0 {
			break
		}
	}
	return got, e.clock.Now().Sub(start), err
}

// percentile returns the p-quantile of a sorted slice, by nearest rank.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// formatOf resolves the short and long output flags.
func formatOf(short, long string) string {
	if long != "" {
		return long
	}
	return short
}

// writeBenchReport prints one arm.
func writeBenchReport(w io.Writer, r *benchReport, o *benchOptions) {
	if o.asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			_, _ = io.WriteString(w, "{\"error\":"+quote(err.Error())+"}\n")
		}
		return
	}
	p := func(s string) { _, _ = io.WriteString(w, s) }
	if r.Family != "" {
		kind := r.Arm
		if kind == "" {
			kind = "(the parameter left out, so every Auto binding)"
		}
		p("arm " + r.Family + " = " + kind + "\n")
	}
	p("  " + pad("target", 20) + r.Target + " topic " + r.Topic + "\n")
	p("  " + pad("host", 20) + r.Host.GOOS + "/" + r.Host.GOARCH +
		" cpus=" + itoa(r.Host.NumCPU) + " gomaxprocs=" + itoa(r.Host.GOMAXPROCS))
	if r.Host.Load != "" {
		p(" load=" + r.Host.Load)
	}
	p("\n")
	p("  " + pad("messages", 20) + itoa(r.Messages) + " in " + r.Duration +
		"  (" + i64toa(r.PerSecond) + " msg/s, operational)\n")
	p("  " + pad("bytes", 20) + i64toa(r.Bytes) + "\n")
	if r.P50 != "" {
		p("  " + pad("publish latency", 20) + "p50=" + r.P50 + " p90=" + r.P90 +
			" p99=" + r.P99 + " max=" + r.Max + "\n")
	}
	if r.ConsumeDuration != "" {
		p("  " + pad("consumed", 20) + itoa(r.Consumed) + " in " + r.ConsumeDuration + "\n")
	}
	if r.Failures > 0 {
		p("  " + pad("FAILURES", 20) + itoa(r.Failures) + "\n")
	}
}
