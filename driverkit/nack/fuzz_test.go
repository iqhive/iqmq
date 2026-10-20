package nack

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzNackPlan drives an op script against every kind and holds each plan to
// the contract: it validates, it agrees with its own operation about
// committing and ordering, and it never both pauses a partition and commits
// its offset.
//
// The script is the interesting part rather than the bytes: a fuzzer that only
// varied the delivery would never reach a strategy's second decision about the
// same record, and it is the second decision — the one taken on a retry topic,
// or at the attempt limit — that the three built-in kinds disagree about.
func FuzzNackPlan(f *testing.F) {
	seedPlan(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, k := range Kinds() {
			st := &planState{t: t, k: k, d: Delivery{Topic: "orders", Attempt: 1}}
			st.rebuild(Config{MaxAttempts: 4, DeadLetterTopic: "orders.dlq", Retries: 3})
			planScript().Run(t, data, st)
		}
	})
}

// planState is what the op script drives: one strategy, and the last delivery
// it planned for.
type planState struct {
	t   *testing.T
	k   Kind
	s   Strategy
	cfg Config
	d   Delivery
	// stops counts the plans that stopped the consumer, so the script can
	// assert that a stop is never accompanied by a commit.
	stops int
}

// rebuild replaces the strategy, which is what a configuration step does.
func (st *planState) rebuild(cfg Config) {
	if st.k.Resolve() == Custom {
		cfg.Decide = func(d Delivery, o mq.NackOptions) (Plan, error) {
			// A deliberately awkward custom policy: it alternates between
			// republishing and holding, and it answers the two explicit
			// actions the way the built-in kinds do, so the validation in
			// custom.Plan is what stands between it and a bad plan.
			switch {
			case o.Action == mq.NackActionDrop:
				return Plan{Op: OpDrop}, nil
			case o.Action == mq.NackActionDeadLetter && cfg.DeadLetterTopic != "":
				return Plan{Op: OpDeadLetter, Topic: cfg.DeadLetterTopic}, nil
			case o.Action == mq.NackActionDeadLetter:
				return Plan{}, mq.ErrUnsupported
			case d.Offset%2 == 0:
				return Plan{Op: OpRepublish, Topic: RetryTopicName("orders", 1), Attempt: d.Attempt + 1}, nil
			default:
				return Plan{Op: OpRetryInPlace, Attempt: d.Attempt + 1, Pause: true}, nil
			}
		}
	}
	s, err := New(st.k, cfg)
	if err != nil {
		// An invalid configuration is a refusal, not a crash: the script
		// keeps the strategy it had.
		return
	}
	st.s, st.cfg = s, cfg
}

// check asserts the invariants after every step.
func (st *planState) check(t testing.TB) {
	t.Helper()
	if st.s == nil {
		t.Fatal("the script lost its strategy")
	}
	if st.s.Kind() != st.k.Resolve() {
		t.Fatalf("strategy reports kind %v, want %v", st.s.Kind(), st.k.Resolve())
	}
}

// plan applies one Plan call and checks what came back.
func (st *planState) plan(t testing.TB, o mq.NackOptions) {
	t.Helper()
	p, err := st.s.Plan(st.d, o)
	if err != nil {
		// Every refusal is one of the enumerated ones; an unexpected error
		// would be a strategy inventing a failure mode.
		switch {
		case !o.Action.Valid():
		case o.Action == mq.NackActionDeadLetter && st.cfg.DeadLetterTopic == "":
		default:
			t.Fatalf("Plan(%+v, %+v) on %v: unexpected %v", st.d, o, st.k, err)
		}
		return
	}
	if verr := p.Validate(); verr != nil {
		t.Fatalf("%v planned %+v for %+v, which does not validate: %v", st.k, p, st.d, verr)
	}
	if p.Completes() != p.Op.Completes() || p.PreservesOrder() != p.Op.PreservesOrder() {
		t.Fatalf("%v: plan %+v disagrees with its own operation", st.k, p)
	}
	if p.Pause && p.Completes() {
		t.Fatalf("%v planned %+v: a paused partition whose offset is committed never gets its redelivery", st.k, p)
	}
	if (p.Op == OpRepublish || p.Op == OpDeadLetter) && p.Topic == "" {
		t.Fatalf("%v planned %+v: a publication with no destination", st.k, p)
	}
	if p.Op == OpDeadLetter && p.Topic != st.cfg.DeadLetterTopic && st.k.Resolve() != Custom {
		t.Fatalf("%v dead-lettered to %q, not to the configured %q", st.k, p.Topic, st.cfg.DeadLetterTopic)
	}
	if p.Op == OpStop {
		st.stops++
	}
	// A republication's destination is a topic this package can read back,
	// which is what lets the next delivery continue the chain.
	if p.Op == OpRepublish {
		if _, _, ok := ParseRetryTopic(p.Topic); !ok {
			t.Fatalf("%v republished to %q, which ParseRetryTopic does not recognise", st.k, p.Topic)
		}
		st.d.Topic = p.Topic
	}
	if p.Attempt != 0 && p.Attempt <= st.d.Attempt && st.k.Resolve() != Custom {
		t.Fatalf("%v planned attempt %d for a delivery already at attempt %d", st.k, p.Attempt, st.d.Attempt)
	}
}

// planScript is the op script: the deliveries and configurations a strategy is
// driven through.
func planScript() *fuzzutil.Script[*planState] {
	return &fuzzutil.Script[*planState]{
		MaxSteps: 96,
		Check:    func(t testing.TB, st *planState) { st.check(t) },
		Steps: []fuzzutil.Step[*planState]{
			{
				Name: "requeue", Weight: 4,
				Run: func(t testing.TB, r *fuzzutil.Reader, st *planState) {
					st.plan(t, mq.NackOptions{Delay: r.Duration(time.Minute)})
				},
			},
			{
				Name: "action",
				Run: func(t testing.TB, r *fuzzutil.Reader, st *planState) {
					st.plan(t, mq.NackOptions{Action: mq.NackAction(r.Int(0, 4))})
				},
			},
			{
				Name: "delivery", Weight: 3,
				Run: func(_ testing.TB, r *fuzzutil.Reader, st *planState) {
					st.d = Delivery{
						Topic:     st.d.Topic,
						Partition: int32(r.Int(0, 8)),
						Offset:    int64(r.Int(0, 1<<20)),
						Attempt:   uint32(r.Int(0, 6)),
					}
					if r.Bool() || st.d.Topic == "" {
						// Always a topic: a delivery with none is a driver
						// bug the strategy refuses , and
						// the script is here to exercise the strategies
						// rather than that refusal, which
						// TestRetryTopicRefusesAnEmptyTopic covers.
						st.d.Topic = "orders"
					}
				},
			},
			{
				Name: "configure",
				Run: func(_ testing.TB, r *fuzzutil.Reader, st *planState) {
					cfg := Config{
						MaxAttempts: uint32(r.Int(0, 6)),
						Retries:     r.Int(0, 5),
						Delay:       r.Duration(time.Second),
					}
					if r.Bool() {
						cfg.DeadLetterTopic = "orders.dlq"
					}
					st.rebuild(cfg)
				},
			},
		},
	}
}

// seedPlan commits the scripts that reach each strategy's interesting corners:
// the chain up the retry topics, the attempt limit, the two explicit actions
// and an action outside.
func seedPlan(f *testing.F) {
	f.Helper()
	sc := planScript()
	add := func(steps ...func(*fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		for _, step := range steps {
			step(w)
		}
		f.Add(w.Data())
	}
	step := func(name string, args ...func(*fuzzutil.Writer)) func(*fuzzutil.Writer) {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(f, w, name)
			for _, a := range args {
				a(w)
			}
		}
	}
	dur := func(d, maxD time.Duration) func(*fuzzutil.Writer) {
		return func(w *fuzzutil.Writer) { w.Duration(maxD, d) }
	}
	num := func(lo, hi, v int) func(*fuzzutil.Writer) {
		return func(w *fuzzutil.Writer) { w.Int(lo, hi, v) }
	}
	boolean := func(v bool) func(*fuzzutil.Writer) {
		return func(w *fuzzutil.Writer) { w.Bool(v) }
	}
	delivery := func(part, off, attempt int, reset bool) func(*fuzzutil.Writer) {
		return step("delivery", num(0, 8, part), num(0, 1<<20, off), num(0, 6, attempt), boolean(reset))
	}
	configure := func(maxAttempts, retries int, delay time.Duration, dlq bool) func(*fuzzutil.Writer) {
		return step("configure", num(0, 6, maxAttempts), num(0, 5, retries), dur(delay, time.Second), boolean(dlq))
	}

	// The retry-topic chain: four requeues walk orders -> retry.1 -> .2 ->
	// .3 -> the dead-letter route.
	chain := make([]func(*fuzzutil.Writer), 0, 6)
	chain = append(chain, configure(0, 3, 0, true), delivery(0, 10, 1, true))
	for range 4 {
		chain = append(chain, step("requeue", dur(0, time.Minute)))
	}
	add(chain...)
	// The attempt limit with a dead-letter destination, and without one.
	add(configure(2, 3, 0, true), delivery(1, 5, 4, true), step("requeue", dur(0, time.Minute)))
	add(configure(2, 3, 0, false), delivery(1, 5, 4, true), step("requeue", dur(0, time.Minute)))
	// The three actions and one outside it.
	add(configure(0, 1, 0, true),
		delivery(0, 1, 1, true),
		step("action", num(0, 4, int(mq.NackActionRequeue))),
		step("action", num(0, 4, int(mq.NackActionDrop))),
		step("action", num(0, 4, int(mq.NackActionDeadLetter))),
		step("action", num(0, 4, 4)),
	)
	// A dead-letter action with no destination is a refusal, not a drop.
	add(configure(0, 1, 0, false), delivery(0, 1, 1, true), step("action", num(0, 4, int(mq.NackActionDeadLetter))))
	// A delay the settlement named, on a strategy that has one configured.
	add(configure(0, 2, 250*time.Millisecond, true), delivery(2, 7, 2, true), step("requeue", dur(time.Second, time.Minute)))
}

// FuzzNackRetryTopic is the `<topic>.retry.<n>` round trip: every name
// RetryTopicName produces is read back exactly, and every name
// ParseRetryTopic accepts re-renders to itself.
//
// It is the parsing half of the package and it is fuzzed because a retry topic
// is a name the driver PUBLISHES to: a parser that accepted
// "orders.retry.007" and rendered it back as "orders.retry.7" would move a
// record to a topic nobody created.
func FuzzNackRetryTopic(f *testing.F) {
	for _, seed := range []string{
		"orders", "orders.retry.1", "orders.retry.12", "a.b.retry.3",
		"orders.retry.", "orders.retry.0", "orders.retry.01", "orders.retry.-1",
		"orders.retry.x", ".retry.1", "orders.retry.1.retry.2",
		"orders.retry.9999999999", "retry.1", "",
	} {
		f.Add(seed, 1)
	}
	f.Fuzz(func(t *testing.T, topic string, n int) {
		base, level, ok := ParseRetryTopic(topic)
		switch {
		case ok:
			if level <= 0 {
				t.Fatalf("ParseRetryTopic(%q) accepted level %d", topic, level)
			}
			if got := RetryTopicName(base, level); got != topic {
				t.Fatalf("ParseRetryTopic(%q) -> (%q, %d) re-renders as %q", topic, base, level, got)
			}
		default:
			if base != topic || level != 0 {
				t.Fatalf("ParseRetryTopic(%q) refused but returned (%q, %d); a refusal returns the name unchanged", topic, base, level)
			}
		}
		// Every rendered name is read back, for the levels a driver can
		// actually configure.
		if n > 0 && n <= 1_000_000 && !strings.Contains(topic, retryInfix) && topic != "" {
			name := RetryTopicName(topic, n)
			gotBase, gotN, gotOK := ParseRetryTopic(name)
			if !gotOK || gotBase != topic || gotN != n {
				t.Fatalf("RetryTopicName(%q, %d) = %q, which reads back as (%q, %d, %v)",
					topic, n, name, gotBase, gotN, gotOK)
			}
		}
	})
}
