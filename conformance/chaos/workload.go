package chaos

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Env is what one chaos scenario is run against: a client through the portable
// API and nothing else, the topology it publishes to, and the injector that
// applies the fault.
//
// The client is the PORTABLE API deliberately. A chaos scenario that reached
// into a driver would pass against a driver whose public behaviour was wrong,
// which is the same argument the specification makes about the conformance kit.
type Env struct {
	// Client is the client under test.
	Client *mq.Client
	// Topic and Subscription are the topology, which the arm created.
	Topic, Subscription string
	// Injector applies and removes the fault.
	Injector Injector
	// SettleTimeout bounds one settlement. It has to be SHORTER than the
	// latency scenario's toxic, or the acknowledgement is merely slow and
	// its fate is never unknown.
	SettleTimeout time.Duration
	// AckDeadline is the lease the topology was created with.
	//
	// The drain needs it. A delivery whose settlement was ambiguous stays
	// LEASED until the deadline passes, and on Redis Streams a pending
	// entry is only reclaimable after its idle time exceeds it — so a drain
	// that stopped at the first empty receive reported twelve accepted
	// messages as lost that were merely still leased, with drained=true
	// beside them.
	AckDeadline time.Duration
	// PublishTimeout bounds one publication.
	PublishTimeout time.Duration
	// ReceiveTimeout bounds one fetch.
	ReceiveTimeout time.Duration
	// Log, when set, records progress; a nil Log is silent.
	Log func(format string, args ...any)
}

func (e Env) resolve() Env {
	if e.SettleTimeout <= 0 {
		e.SettleTimeout = 1500 * time.Millisecond
	}
	if e.PublishTimeout <= 0 {
		e.PublishTimeout = 5 * time.Second
	}
	if e.ReceiveTimeout <= 0 {
		e.ReceiveTimeout = 3 * time.Second
	}
	if e.Log == nil {
		e.Log = func(string, ...any) {}
	}
	if e.Subscription == "" {
		e.Subscription = "chaos"
	}
	return e
}

// id is the ledger's identifier for the nth message. It goes in the body, so
// that the ledger is keyed by what the workload wrote rather than by anything
// the broker assigned: a broker that rewrites ids would otherwise be
// reconciled against its own bookkeeping.
func id(n int) string { return "c" + strconv.Itoa(n) }

const bodyPrefix = "iqmq-chaos:"

func body(n int) []byte { return []byte(bodyPrefix + id(n)) }

func idOf(b []byte) string {
	s := string(b)
	if len(s) <= len(bodyPrefix) || s[:len(bodyPrefix)] != bodyPrefix {
		return ""
	}
	return s[len(bodyPrefix):]
}

// RunScenario publishes the scenario's messages, injects its fault at its
// phase, removes it, waits for the workload to resume, drains and returns what
// it saw.
//
// An error is a failure of the RUN — the fault could not be injected, the
// topology could not be reached at all — and is never a verdict. The verdict is
// [Scenario.Verdict] over the returned Run, which is a pure function and is
// tested separately.
func RunScenario(ctx context.Context, s Scenario, env Env) (*Run, error) {
	if env.AckDeadline <= 0 {
		// Refused rather than defaulted, because the default has to match
		// the lease the TOPOLOGY was created with and nothing here can
		// know it. Defaulted to five seconds against a topology created
		// with thirty, the drain waited ten and twelve Redis Streams
		// entries were still pending and unreclaimable — reported as
		// twelve lost messages with drained=true beside them. A caller
		// that has to state the lease cannot write that mismatch by
		// accident.
		return nil, errors.New("chaos: Env.AckDeadline is unset; it must be the lease the topology was created with, because the drain waits twice it before it may converge")
	}
	env = env.resolve()
	run := &Run{Scenario: s.Name, PendingLimit: s.PendingLimit}
	ledger := NewLedger()

	var remove func(context.Context) error
	inject := func(when string) error {
		if remove != nil {
			return nil
		}
		rm, err := s.Inject(ctx, env.Injector)
		if err != nil {
			return fmt.Errorf("injecting %s's fault %s: %w", s.Name, when, err)
		}
		remove = rm
		env.Log("chaos %s: fault injected %s", s.Name, when)
		return nil
	}
	defer func() {
		if remove != nil {
			_ = remove(ctx)
		}
	}()

	if s.Phase == BeforeAll {
		if err := inject("before the workload"); err != nil {
			return run, err
		}
	}

	pcfg := mq.PublisherConfig{Topic: env.Topic, Metadata: mq.MetadataAdaptive}
	if s.PendingLimit > 0 {
		pcfg.MaxPendingMessages = s.PendingLimit
	}
	pub, err := env.Client.Publisher(ctx, pcfg)
	if err != nil {
		// A publisher that cannot be created under a fault injected
		// BeforeAll is a result rather than a run failure: it is the
		// refusal the topology-deletion scenario is looking for.
		if s.Phase == BeforeAll {
			run.Code(mq.CodeOf(err))
			run.Faulted++
			finish(run, ledger, env)
			return run, nil
		}
		return run, fmt.Errorf("publisher: %w", err)
	}
	defer func() { _ = pub.Close() }()

	con, err := env.Client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        env.Topic,
		Subscription: env.Subscription,
		Prefetch:     16,
		Metadata:     mq.MetadataAdaptive,
	})
	if err != nil {
		if s.Phase == BeforeAll {
			run.Code(mq.CodeOf(err))
			run.Faulted++
			finish(run, ledger, env)
			return run, nil
		}
		return run, fmt.Errorf("consumer: %w", err)
	}
	defer func() { _ = con.Close() }()

	// The workload runs in two halves with the fault injected BETWEEN them,
	// and that structure is the whole scenario rather than a detail.
	//
	// Written as "publish everything, then receive everything, and inject
	// somewhere in the middle", the fault's window contained no broker
	// round trip at all: by the time the settle loop started, every message
	// was already in the consumer's prefetch buffer, so every remaining
	// operation was local. Two of the four scenarios reported
	// "the fault made 0 operation(s) fail ... nothing was injected, so
	// nothing was tested" against a live NATS with a live Toxiproxy toxic
	// in place. The fault has to be surrounded by work, not preceded by it.
	dst := make([]mq.Delivery, 8)
	half := s.Messages / 2
	if half < 1 {
		half = 1
	}

	// ---- first half: warm the topology and the connection up ----
	for i := range half {
		publishOne(ctx, env, run, ledger, pub, i)
		run.trackPending(pub)
	}
	switch s.Phase {
	case DuringPublish:
		drainSome(ctx, env, run, ledger, con, dst, 1)
		if err := inject("mid-burst"); err != nil {
			return run, err
		}
	case DuringSettle:
		// One batch settled cleanly first, so that the fault lands on a
		// settlement path that is known to work.
		drainSome(ctx, env, run, ledger, con, dst, 1)
		if err := inject("mid-settlement"); err != nil {
			return run, err
		}
	case BeforeAll:
		// Already injected above, before the publisher existed.
	}

	// ---- second half: every operation here happens UNDER the fault ----
	for i := half; i < s.Messages; i++ {
		publishOne(ctx, env, run, ledger, pub, i)
		run.trackPending(pub)
	}
	drainSome(ctx, env, run, ledger, con, dst, 4)

	if remove == nil {
		// The phase was BeforeAll and the fault is already in, or the
		// scenario reached no injection point at all. The second is a
		// harness defect rather than a verdict, and injecting here is
		// what makes FaultBit report it honestly.
		if err := inject("after the second half"); err != nil {
			return run, err
		}
	}

	// ---- remove the fault and see whether the workload resumes ----
	if remove != nil {
		if err := remove(ctx); err != nil {
			return run, fmt.Errorf("removing %s's fault: %w", s.Name, err)
		}
		remove = nil
	}
	bound := s.ResumeBound
	if bound > 0 {
		run.Resumed, run.ResumeAfter = waitForProgress(ctx, env, run, ledger, pub, s.Messages, bound)
	}

	// ---- drain, so that the ledger accounts for everything ----
	//
	// `usable` is the fact that makes the drain's empty receives readable:
	// a receive that times out with nothing to show is "the topic is empty"
	// only on a client that works. The resume probe is the direct evidence
	// for that — it published and the publish was accepted — and it is used
	// in preference to Client.Health because health is the DRIVER's own
	// report of itself: against Redis Streams the run reported resumed=true
	// with a publish demonstrably accepted and a health reading that was
	// not Ready, which failed the drain for a reason that is about the
	// driver's health recovery rather than about the drain.
	usable := run.Resumed
	if bound == 0 {
		// No probe ran, so the driver's own health is all there is.
		usable = ready(env.Client)
	}
	drain(ctx, env, run, ledger, con, usable)
	finish(run, ledger, env)
	return run, nil
}

// drainSome receives and settles up to rounds batches, recording what it saw.
// It is bounded by rounds rather than by "until empty" because the workload's
// SECOND half has to happen under the fault, and a drain that ran to
// completion here would settle everything before the fault went in.
func drainSome(ctx context.Context, env Env, run *Run, l *Ledger, con *mq.ConsumerHandle, dst []mq.Delivery, rounds int) {
	for range rounds {
		rctx, cancel := context.WithTimeout(ctx, env.ReceiveTimeout)
		got, rerr := con.Receive(rctx, dst)
		cancel()
		if rerr != nil && !errors.Is(rerr, context.DeadlineExceeded) {
			// The code is recorded BEFORE the break: a consumer the
			// fault closed is the fault having bitten, and returning
			// without counting it made the run look like one in which
			// nothing was injected.
			run.Faulted++
			run.Code(mq.CodeOf(rerr))
			if errors.Is(rerr, mq.ErrClosed) {
				return
			}
		}
		if got == 0 {
			return
		}
		for i := range got {
			settleOne(env, run, l, &dst[i])
		}
	}
}

// publishOne publishes message n and records its outcome.
func publishOne(ctx context.Context, env Env, run *Run, l *Ledger, pub mq.Publisher, n int) {
	pctx, cancel := context.WithTimeout(ctx, env.PublishTimeout)
	defer cancel()
	m := &mq.Message{Topic: env.Topic, Key: []byte("k" + strconv.Itoa(n%4)), Body: body(n), ID: id(n)}
	err := pub.Publish(pctx, m)
	switch {
	case err == nil:
		l.Published(id(n), Accepted)
	case mq.IsAmbiguous(err),
		errors.Is(err, mq.ErrSettlementUnknown),
		errors.Is(err, mq.ErrPartial),
		errors.Is(err, context.DeadlineExceeded):
		// A publication whose context expired, or whose driver set
		// OpError.Ambiguous, is the textbook ambiguous outcome:
		// the write may be on the wire. Recording it as rejected would
		// let a broker lose it without the ledger noticing, which is the
		// one mistake that would make every chaos scenario pass.
		l.Published(id(n), Ambiguous)
		run.Faulted++
		run.Code(mq.CodeOf(err))
	case isDefinite(err):
		l.Published(id(n), Rejected)
		run.Faulted++
		run.Code(mq.CodeOf(err))
	default:
		// The driver said neither. The specification requires a driver to "classify
		// retryability and ambiguity accurately", so this is a finding
		// about the driver — and the LEDGER has to read it as ambiguous,
		// because a connection that died around an XADD or a publish
		// frame may well have written it. Read as definitely-rejected it
		// produced "4 rejected message(s) delivered [c32 c33 c37 c39]"
		// against Redis Streams under a reset storm: a legal redelivery
		// reported as a violation.
		l.Published(id(n), Ambiguous)
		run.Faulted++
		run.Unclassified++
		run.Code(mq.CodeOf(err))
	}
}

// isDefinite reports whether a publish error means the broker definitely did
// not take the message. Every one of these is decided before any I/O — an
// unsupported option, an oversized body, a name the driver refuses, a topic
// that is not there — so there is no window in which the write could have
// happened.
func isDefinite(err error) bool {
	return errors.Is(err, mq.ErrUnsupported) ||
		errors.Is(err, mq.ErrMessageTooLarge) ||
		errors.Is(err, mq.ErrInvalidTopic) ||
		errors.Is(err, mq.ErrTopicNotFound) ||
		errors.Is(err, mq.ErrClosed)
}

// settleOne records the delivery and acks it.
func settleOne(env Env, run *Run, l *Ledger, d *mq.Delivery) {
	mid := idOf(d.Body)
	if mid == "" {
		mid = "foreign:" + strconv.Itoa(len(d.Body))
	}
	l.Delivered(mid)
	// The settlement's own bound is the Env's and not the caller's context:
	// the latency scenario needs an acknowledgement whose fate is unknown,
	// which means a deadline SHORTER than the injected latency.
	done := make(chan error, 1)
	go func() { done <- d.Ack() }()
	timer := time.NewTimer(env.SettleTimeout)
	defer timer.Stop()
	var err error
	select {
	case err = <-done:
	case <-timer.C:
		// The settlement is still in flight. It is ambiguous by
		// construction — the broker may or may not have taken it — and
		// that is exactly the state the specification calls unknown.
		run.Unknown++
		run.Faulted++
		run.Code(mq.CodeSettlementUnknown)
		return
	}
	switch {
	case err == nil:
	case errors.Is(err, mq.ErrSettlementUnknown):
		run.Unknown++
		run.Faulted++
		run.Code(mq.CodeOf(err))
	case errors.Is(err, mq.ErrAlreadySettled):
		// Somebody else settled it; not a fault.
	default:
		run.SettleFailed++
		run.Faulted++
		run.Code(mq.CodeOf(err))
	}
}

// waitForProgress publishes probe messages until one is accepted, which is what
// "the workload resumed" means: a client that reconnected but cannot publish
// has not resumed.
func waitForProgress(ctx context.Context, env Env, run *Run, l *Ledger, pub mq.Publisher, base int, bound time.Duration) (bool, time.Duration) {
	start := time.Now()
	deadline := start.Add(bound)
	for n := base; time.Now().Before(deadline); n++ {
		before := l.Check().Published[Accepted]
		publishOne(ctx, env, run, l, pub, n)
		if l.Check().Published[Accepted] > before {
			return true, time.Since(start)
		}
		select {
		case <-ctx.Done():
			return false, time.Since(start)
		case <-time.After(500 * time.Millisecond):
		}
	}
	return false, time.Since(start)
}

// drain receives and acks until nothing comes back, so that the ledger's
// "missing" column means lost rather than "not fetched yet".
func drain(ctx context.Context, env Env, run *Run, l *Ledger, con *mq.ConsumerHandle, usable bool) {
	dst := make([]mq.Delivery, 8)
	deadline := time.Now().Add(2*env.AckDeadline + 45*time.Second)
	// healthy counts the CONSECUTIVE empty receives that returned no error.
	// Only those say "the topic is empty"; an empty receive that failed says
	// "the client could not ask", and conflating the two is what let a run
	// whose client never reconnected report eight messages as lost.
	healthy := 0
	// The drain may not converge before every lease it could be waiting on
	// has had a chance to expire: a message left leased by an ambiguous
	// settlement is neither delivered nor lost until then.
	earliest := time.Now().Add(2 * env.AckDeadline)
	for time.Now().Before(deadline) {
		if healthy >= 3 && !time.Now().Before(earliest) {
			break
		}
		rctx, cancel := context.WithTimeout(ctx, env.ReceiveTimeout)
		got, err := con.Receive(rctx, dst)
		cancel()
		failed := err != nil && !errors.Is(err, context.DeadlineExceeded)
		if failed && got == 0 {
			run.Code(mq.CodeOf(err))
			if errors.Is(err, mq.ErrClosed) {
				return
			}
			healthy = 0
			continue
		}
		if got == 0 {
			healthy++
			continue
		}
		healthy = 0
		for i := range got {
			settleOne(env, run, l, &dst[i])
		}
	}
	run.Drained = converged(healthy, !time.Now().Before(earliest), usable)
}

// converged is the drain's own rule, as a function of three facts, so that it
// can be held to a table rather than inferred from the loop above.
//
// The third fact is the one that was missing. A receive that returns
// context.DeadlineExceeded with nothing to show is counted as "the topic is
// empty" — which it is on a working client, and is NOT on a client that cannot
// reach the broker at all. Against a live JetStream under a reset storm the
// drain therefore reported drained=true beside resumed=false and eight
// messages "missing": three timeouts in a row on a dead connection read as
// three empty topics.
func converged(healthy int, pastLease, usable bool) bool {
	return healthy >= 3 && pastLease && usable
}

// ready reports whether the client is usable.
func ready(c *mq.Client) bool {
	return c != nil && c.Health().State == mq.HealthReady
}

// finish fills in the report and the health reading.
func finish(run *Run, l *Ledger, env Env) {
	run.Report = l.Check()
	if env.Client != nil {
		run.Degraded = env.Client.Health().State != mq.HealthReady
	}
	env.Log("chaos %s", run)
}
