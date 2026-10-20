package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
)

const subUsage = `usage: iqmq sub [flags] <dsn> <topic> <subscription>

Consume from a subscription, print each delivery and settle it explicitly.

Settlement is never implicit. --settle says what a delivery is settled with,
--on-stop says what an in-flight delivery is settled with when the run ends,
and both counts are reported: a run that abandoned four messages says so, and
"abandoned" is not spelled the same as "acknowledged".

flags:
      --max N            stop after N deliveries (0 = until interrupted)
      --timeout DURATION stop after DURATION (0 = no bound)
      --idle DURATION    stop after DURATION with no delivery (default 0)
      --concurrency N    settle and print on N goroutines (default 1)
      --prefetch N       how many messages the driver may hold (default 128)
      --start POSITION   latest|earliest, for a subscription with no position
      --ack-deadline D   ask for this lease duration
      --settle ACTION    ack|nack|reject|none (default ack)
      --nack-delay D     with --settle nack, delay the redelivery
      --nack-action A    requeue|drop|dead-letter, with --settle nack
      --on-stop ACTION   ack|nack|reject|none for a delivery still in flight
                         when the run ends (default nack)
  -o, --output text|json output format (default text)
      --headers print the message headers (off by default)
      --no-body          do not print the message body
      --base64           print the body as base64
      --meta             print the broker metadata (partition, offset, lease)

--settle none leaves every delivery unsettled, which is what makes sub a
read-only probe on a driver whose unsettled deliveries are redelivered. It is
not free: an unsettled delivery holds a prefetch slot, so a run with --settle
none stops when it has filled the prefetch window and says why.
`

// settleAction is what one delivery is settled with.
type settleAction uint8

// The settlement actions.
const (
	// settleAck is a positive settlement.
	settleAck settleAction = iota
	// settleNack is a negative settlement.
	settleNack
	// settleReject discards without redelivery.
	settleReject
	// settleNone leaves the delivery unsettled.
	settleNone
)

// String returns the action's own spelling, which is what the report and the
// --settle flag both use.
func (s settleAction) String() string {
	switch s {
	case settleAck:
		return "ack"
	case settleNack:
		return "nack"
	case settleReject:
		return "reject"
	case settleNone:
		return "none"
	default:
		return "settleAction(?)"
	}
}

// parseSettle maps a flag value to an action.
func parseSettle(flagName, s string) (settleAction, error) {
	switch s {
	case "ack":
		return settleAck, nil
	case "nack":
		return settleNack, nil
	case "reject":
		return settleReject, nil
	case "none", "":
		return settleNone, nil
	default:
		return 0, errors.New(flagName + " takes ack, nack, reject or none, not " + quote(s))
	}
}

// parseNackAction maps a flag value to the nack option.
func parseNackAction(s string) ([]mq.NackOption, error) {
	switch s {
	case "", "requeue":
		return []mq.NackOption{mq.NackRequeue()}, nil
	case "drop":
		return []mq.NackOption{mq.NackDrop()}, nil
	case "dead-letter":
		return []mq.NackOption{mq.NackDeadLetter()}, nil
	default:
		return nil, errors.New("--nack-action takes requeue, drop or dead-letter, not " + quote(s))
	}
}

// parseStart maps a flag value to the start position. Only the two
// portable forms are offered: a time or a sequence needs FeatureSeekByTime or
// FeatureSeekBySequence, and a CLI flag that silently failed on most drivers
// would be worse than not having it.
func parseStart(s string) (mq.StartPosition, error) {
	switch s {
	case "", "latest":
		return mq.StartLatest(), nil
	case "earliest":
		return mq.StartEarliest(), nil
	default:
		return mq.StartPosition{}, errors.New("--start takes latest or earliest, not " + quote(s))
	}
}

// consumeOptions are one sub or tail invocation's controls.
type consumeOptions struct {
	maxN        int
	timeout     time.Duration
	idle        time.Duration
	concurrency int
	prefetch    int
	start       string
	ackDeadline time.Duration
	settle      settleAction
	nackDelay   time.Duration
	nackOpts    []mq.NackOption
	onStop      settleAction
	render      renderOptions
}

// consumeResult is what a run did, which is what its last line reports.
type consumeResult struct {
	received int
	settled  map[string]int
	failures int
	// unsupported records that a settlement was refused because the driver
	// does not have the capability, which is exit 3 and not exit 1: nothing
	// the caller retries will change it. It is tracked separately from
	// failures because "the broker said no" and "this broker cannot do that
	// at all" are different answers.
	unsupported bool
	stopReason  string
}

// cmdSub is the sub subcommand.
func cmdSub(e *cliEnv, args []string) int {
	fs := newFlagSet("sub", e)
	var (
		o          consumeOptions
		settleStr  = fs.String("settle", "ack", "ack, nack, reject or none")
		onStopStr  = fs.String("on-stop", "nack", "ack, nack, reject or none for an in-flight delivery")
		nackAction = fs.String("nack-action", "requeue", "requeue, drop or dead-letter")
		output     = fs.String("o", "text", "output format: text or json")
		outputL    = fs.String("output", "", "output format: text or json")
		noBody     = fs.Bool("no-body", false, "do not print the message body")
	)
	fs.IntVar(&o.maxN, "max", 0, "stop after N deliveries")
	fs.DurationVar(&o.timeout, "timeout", 0, "stop after DURATION")
	fs.DurationVar(&o.idle, "idle", 0, "stop after DURATION with no delivery")
	fs.IntVar(&o.concurrency, "concurrency", 1, "settle and print on N goroutines")
	fs.IntVar(&o.prefetch, "prefetch", 0, "how many messages the driver may hold")
	fs.StringVar(&o.start, "start", "latest", "latest or earliest")
	fs.DurationVar(&o.ackDeadline, "ack-deadline", 0, "ask for this lease duration")
	fs.DurationVar(&o.nackDelay, "nack-delay", 0, "delay the redelivery")
	fs.BoolVar(&o.render.headers, "headers", false, "print the message headers")
	fs.BoolVar(&o.render.base64, "base64", false, "print the body as base64")
	fs.BoolVar(&o.render.meta, "meta", false, "print the broker metadata")
	if code := parseFlags(fs, e, args, subUsage); code >= 0 {
		return code
	}
	if fs.NArg() != 3 {
		_, _ = io.WriteString(e.stderr, "iqmq sub: a DSN, a topic and a subscription are required\n"+subUsage)
		return exitUsage
	}
	if code := o.finish(e, *settleStr, *onStopStr, *nackAction, *output, *outputL, *noBody); code >= 0 {
		return code
	}
	return e.consume(&o, "sub", fs.Arg(0), fs.Arg(1), fs.Arg(2))
}

// finish validates the string-valued flags and fills in the derived fields.
func (o *consumeOptions) finish(e *cliEnv, settleStr, onStopStr, nackAction, output, outputL string, noBody bool) int {
	bad := func(err error) int {
		_, _ = io.WriteString(e.stderr, "iqmq: "+err.Error()+"\n")
		return exitUsage
	}
	var err error
	if o.settle, err = parseSettle("--settle", settleStr); err != nil {
		return bad(err)
	}
	if o.onStop, err = parseSettle("--on-stop", onStopStr); err != nil {
		return bad(err)
	}
	if o.nackOpts, err = parseNackAction(nackAction); err != nil {
		return bad(err)
	}
	if o.nackDelay > 0 {
		o.nackOpts = append(o.nackOpts, mq.NackDelay(o.nackDelay))
	}
	if _, err = parseStart(o.start); err != nil {
		return bad(err)
	}
	format := output
	if outputL != "" {
		format = outputL
	}
	switch format {
	case "text":
	case "json":
		o.render.json = true
	default:
		return bad(errors.New("--output takes text or json, not " + quote(format)))
	}
	o.render.body = !noBody
	if o.concurrency < 1 {
		return bad(errors.New("--concurrency takes at least 1"))
	}
	if o.maxN < 0 {
		return bad(errors.New("--max takes a non-negative count"))
	}
	return -1
}

// requirements are the capabilities this invocation's flags need.
//
// They are checked before the source is opened, so an invocation that cannot
// work fails at construction with the feature NAMED, rather than at the first
// delivery with a bare "unsupported". Every one of these was found by running
// the command: `--settle reject` is Nack(DeadLetter) and needs a dead-letter
// destination, `--start earliest` needs a retained log, and neither said so
// until the settlement or the open refused.
func (o *consumeOptions) requirements() []mq.Requirement {
	var reqs []mq.Requirement
	switch o.settle {
	case settleNack:
		reqs = append(reqs, mq.RequireAtLeast(mq.FeatureNack, mq.Conditional))
	case settleReject:
		// Reject is Nack(NackActionDeadLetter): the specification makes "destroy this"
		// explicit, so Reject is the dead-letter route and not a drop.
		reqs = append(reqs, mq.RequireAtLeast(mq.FeatureDeadLetter, mq.Conditional))
	case settleAck, settleNone:
	}
	if o.nackDelay > 0 {
		reqs = append(reqs, mq.RequireAtLeast(mq.FeatureDelayedRedelivery, mq.Conditional))
	}
	if o.start == "earliest" {
		reqs = append(reqs, mq.RequireAtLeast(mq.FeatureReplay, mq.Conditional))
	}
	return reqs
}

// consume is the body of both `sub` and `tail`.
func (e *cliEnv) consume(o *consumeOptions, cmd, target, topic, sub string) int {
	e, flush := e.scrubbed(target)
	defer flush()
	out := e.stdout
	if isEphemeral(target) {
		_, _ = io.WriteString(out, ephemeralNote)
	}

	ctx, cancel, _ := e.expiring(e.ctx, o.timeout)
	defer cancel()
	client, code := e.open(ctx, cmd, target)
	if code >= 0 {
		return code
	}
	defer func() { _ = client.Close() }()

	start, err := parseStart(o.start)
	if err != nil {
		return fail(e, cmd, err)
	}
	cfg := mq.ConsumerConfig{
		Topic: topic, Subscription: sub, Start: start,
		Prefetch: o.prefetch, AckDeadline: o.ackDeadline,
		Requirements: o.requirements(),
	}
	con, err := client.Consumer(ctx, cfg)
	if err != nil {
		return fail(e, cmd, err)
	}
	defer func() { _ = con.Close() }()

	res := e.receiveLoop(ctx, out, con, o)

	// the graceful close: the consumer drains what it has admitted and
	// the client closes its children. It is bounded by the same context, so
	// an interrupted run does not hang on a broker that stopped answering.
	if err := con.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = io.WriteString(e.stderr, "iqmq "+cmd+": shutdown: "+err.Error()+"\n")
	}
	writeConsumeResult(out, &res)
	switch {
	case res.unsupported:
		return exitUnsupported
	case res.failures > 0:
		return exitFailure
	}
	return exitOK
}

// receiveLoop is the fetch-print-settle loop.
//
// The deterministic part of shutdown lives here: when the run stops -- because
// --max was reached part-way through a fetched batch, or because the context
// was cancelled -- every delivery that was fetched and not yet handled is
// settled with --on-stop and counted separately. A delivery fetched and then
// dropped on the floor is a message held until its lease expires and nothing
// in the output that says so, which is the shape of bug exists to make
// impossible to write by accident.
func (e *cliEnv) receiveLoop(
	ctx context.Context, out io.Writer, con *mq.ConsumerHandle, o *consumeOptions,
) consumeResult {
	res := consumeResult{settled: map[string]int{}}
	// The fetch is bounded by --prefetch and NOT by --max, which are two
	// different things: one Receive is one round trip, and asking for one
	// message at a time because --max is 1 would make N round trips for N
	// messages. --max bounds what is HANDLED. The surplus of a batch that
	// crosses it is settled with --on-stop and counted, which is the whole
	// reason that flag exists.
	//
	// Capping the slice at --max would also not reduce what is taken from the
	// broker: prefetch is the DRIVER's buffer, so the messages are already in
	// flight either way. Capping would only hide them from this loop and
	// leave them to Close, which reports nothing.
	batch := o.prefetch
	if batch <= 0 {
		batch = mq.DefaultPrefetch
	}
	dst := make([]mq.Delivery, batch)

	var mu sync.Mutex
	// settleAndPrint captures the delivery, settles it, then prints it with
	// what it was settled as. The capture is what makes that order legal: the specification
	// borrows the body until settlement, so the line cannot be built from a
	// delivery that has already been settled.
	//
	// bucket prefixes the counter name, which is how a delivery settled because
	// the run ended is counted apart from one settled because it was handled.
	settleAndPrint := func(n int, d *mq.Delivery, action settleAction, bucket string) {
		c := capture(d)
		what, err := settleDelivery(d, action, o.nackOpts)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			res.failures++
			note := ""
			if mq.CodeOf(err) == mq.CodeUnsupported {
				res.unsupported = true
				note = "\n  this driver does not have the capability that settlement needs; " +
					"it is a fact about the broker (the message contract) and no retry changes it.\n" +
					"  \"iqmq doctor <dsn>\" reports what it does advertise."
			}
			_, _ = io.WriteString(e.stderr, "iqmq: settling delivery "+itoa(n)+" with "+
				action.String()+": "+err.Error()+note+"\n")
			what = action.String() + "-failed"
		}
		res.settled[bucket+what]++
		if n > 0 {
			renderCaptured(out, n, &c, what, &o.render)
		}
	}

	limit := newInFlight(o.concurrency)

	for {
		if o.maxN > 0 && res.received >= o.maxN {
			res.stopReason = "the --max count was reached"
			break
		}
		rctx, rcancel, idleExpired := e.expiring(ctx, o.idle)
		n, err := con.Receive(rctx, dst)
		rcancel()
		if n > 0 {
			// Everything fetched is accounted for, including the tail of a
			// batch that crosses --max: those deliveries were taken from the
			// broker and have to be given back with the --on-stop disposition
			// rather than held until their leases expire.
			take := n
			if o.maxN > 0 && res.received+take > o.maxN {
				take = o.maxN - res.received
			}
			var wg sync.WaitGroup
			for i := range take {
				res.received++
				seq := res.received
				d := &dst[i]
				wg.Add(1)
				limit.enter()
				go func() {
					defer wg.Done()
					defer limit.leave()
					settleAndPrint(seq, d, o.settle, "")
				}()
			}
			// The batch is finished before the next fetch: Receive refills
			// dst, so a worker still holding &dst[i] would be settling the
			// delivery that replaced its own. Concurrency is therefore within
			// a batch, which is where the settlement round trips are.
			wg.Wait()
			for i := take; i < n; i++ {
				settleAndPrint(0, &dst[i], o.onStop, "beyond-max:")
			}
		}
		if err != nil {
			switch {
			case idleExpired():
				res.stopReason = "no delivery for " + o.idle.String()
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				res.stopReason = "the run was stopped"
			default:
				res.stopReason = "Receive failed: " + err.Error()
				res.failures++
			}
			break
		}
		if o.maxN > 0 && res.received >= o.maxN {
			res.stopReason = "the --max count was reached"
			break
		}
	}
	return res
}

// settleDelivery settles one delivery and returns the word the report counts
// it under.
//
// "abandoned" is the word for a delivery left unsettled on purpose. It is not
// a synonym for acknowledged and it is not a synonym for failed: the broker
// will redeliver it when the lease expires, and a reader of the output has to
// be able to see that this run did not consume it.
func settleDelivery(d *mq.Delivery, action settleAction, nackOpts []mq.NackOption) (string, error) {
	switch action {
	case settleAck:
		return "acked", d.Ack()
	case settleNack:
		return "nacked", d.Nack(nackOpts...)
	case settleReject:
		return "rejected", d.Reject()
	case settleNone:
		return "abandoned", nil
	default:
		return "abandoned", nil
	}
}

// writeConsumeResult prints the run's summary.
func writeConsumeResult(w io.Writer, res *consumeResult) {
	var b []byte
	b = append(b, "received "...)
	b = append(b, itoa(res.received)...)
	for _, k := range sortedStrings(keysOf(res.settled)) {
		if res.settled[k] == 0 {
			continue
		}
		b = append(b, ' ')
		b = append(b, k...)
		b = append(b, ' ')
		b = append(b, itoa(res.settled[k])...)
	}
	if res.stopReason != "" {
		b = append(b, "\nstopped: "...)
		b = append(b, res.stopReason...)
	}
	b = append(b, '\n')
	_, _ = w.Write(b)
}

// keysOf returns a map's keys.
func keysOf(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
