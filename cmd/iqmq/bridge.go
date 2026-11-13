package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
)

const bridgeUsage = `usage: iqmq bridge [flags] <src-dsn> <src-topic> <src-subscription> <dst-dsn> <dst-topic>

Move messages from a subscription on one broker to a topic on another.

WHAT THE SEAM GUARANTEES, AND WHAT IT CANNOT

A bridge settles on two brokers that know nothing about each other, and no
option below changes that. The order is fixed:

    receive from the source
      -> publish to the destination
        -> flush, so the publication is CONFIRMED and not merely admitted
          -> settle the source delivery

so the guarantee is AT-LEAST-ONCE with DUPLICATES POSSIBLE ON THE DESTINATION.
A crash, a cancelled context or a broker that stops answering between the
confirmation and the source settlement leaves the source delivery unsettled:
the source redelivers it and the destination receives it a second time. The run
reports how many deliveries were in that window.

The other order -- settle first, then publish -- loses messages instead, and
that is the trade being made: a duplicate is a fact the destination can be
built to tolerate, a lost message is not. There is no third order without a
transaction spanning both brokers, which the specification does not offer and no pair of
drivers in this repository could honour.

--dedup-from id asks the DESTINATION to suppress the duplicates, by carrying
each source message's id as the publication's de-duplication key. It
requires FeatureDeduplication and refuses without it. What it cannot do is
change the guarantee: the de-duplication WINDOW belongs to the destination
broker, so a redelivery that arrives after the window has closed is a duplicate
the destination accepts. The run prints the level the destination advertises
for that feature, and says so when the conformance kit has no probe for it.
A source whose delivery carries no message id gets no key and no suppression,
and the run counts those separately.

flags:
      --max N            stop after N messages (0 = until interrupted)
      --timeout DURATION bound the whole run (0 = no bound)
      --idle DURATION    stop after DURATION with no delivery (default 0)
      --prefetch N       how many messages the source driver may hold
      --start POSITION   latest|earliest, for a source subscription with no
                         position (default latest). A
                         subscription that already has a position is
                         unaffected: its backlog is what a bridge reads either
                         way. --start earliest needs FeatureReplay and refuses
                         without it.
      --confirm MODE     the destination's confirmation: default|accepted|durable
      --on-stop ACTION   nack|none for each delivery a --dry-run hands back
                         (default nack); a dry run refuses ack and reject,
                         because it must leave the source as it was. A run
                         that publishes hands back what it fetched and did
                         not publish (after a failed publication, or after
                         it was stopped) with a requeue whatever --on-stop
                         says, and accepts the flag without using it.
      --headers pass|drop  carry the source headers (default pass)
      --key pass|drop      carry the source key (default pass)
      --dedup-from id|none use the source message id as the destination's
                           de-duplication key (default none)
      --dry-run          receive and report, publish NOTHING, and return every
                         delivery to the source with --on-stop. With
                         --on-stop none each delivery holds a prefetch slot
                         until its lease expires, so the run stops when it
                         has filled the prefetch window, and says so. With
                         nack it stops when the source hands out a message it
                         has already returned, which it knows by the
                         broker's offset or sequence (with the message ids
                         agreeing where both carry one), or by the message
                         id of a redelivery. RabbitMQ's sequence is its
                         delivery tag, new on every delivery, so there it
                         knows a message by its id; without ids a reconnect,
                         whose new channel reuses delivery tags, can end the
                         dry run early. A message with neither is read
                         again until --max, --timeout or Ctrl-C ends the
                         run, and a queue that puts a requeued message back
                         ahead of the rest limits the run to about one
                         --prefetch window.
  -o, --output text|json output format (default text)
  -q, --quiet            print only the summary and the failures

Ordering: this bridge is single-flight by design. One
message is published and confirmed before the next is received, so the
destination sees the source's order wherever the source delivers in order. A
concurrent bridge would be faster and would reorder, and a flag that silently
reordered a stream somebody bridged for its order would be the wrong default
to offer at all. For the same reason a failed publication ends the run: its
delivery, and every one fetched after it, goes back to the source with a
requeue whatever --on-stop says, so no later message reaches the destination
ahead of it.

A publication the destination takes for some of its subscriptions and refuses
for the others (a partial fan-out) is not returned: the source delivery is
settled, because publishing it again would put a second copy on every
subscription that took the first. The run counts it as partial, says that the
other subscriptions do not have it, and exits 1.

exit codes: 0 every message moved; 1 a receive, a publication or a settlement
failed, or a publication reached only some of the destination's subscriptions;
2 the invocation was malformed; 3 a capability the flags need is absent.
`

// bridgeOptions are one bridge invocation's controls.
type bridgeOptions struct {
	maxN     int
	timeout  time.Duration
	idle     time.Duration
	prefetch int
	confirm  string
	onStop   settleAction
	headers  bool
	key      bool
	dedupID  bool
	start    string
	dryRun   bool
	json     bool
	quiet    bool
}

// bridgeResult is what a run did.
//
// The two fields that matter are the last two. Moved is the count a reader
// wants; duplicateRisk is the count that says what the guarantee cost on this
// run, and a bridge that reported only the first would be hiding the thing its
// own documentation is about.
type bridgeResult struct {
	Received int `json:"received"`
	// Published is how many publications the destination confirmed.
	Published int `json:"published"`
	// Moved is how many were confirmed AND settled on the source, which is
	// the only count for which nothing is outstanding.
	Moved int `json:"moved"`
	// PublishFailures is how many publications did not succeed. Each of those
	// deliveries was returned to the source, so nothing was lost.
	PublishFailures int `json:"publish_failures"`
	// Ambiguous is how many of those failures leave the destination's
	// acceptance UNKNOWN, so the destination may already hold a message the
	// source is about to redeliver.
	//
	// Every Flush failure is one, because Publish returning nil means the
	// message was admitted and a failed Flush says nothing about
	// whether the broker then took it -- against a SYNCHRONOUS sink it
	// already has. A Publish failure is one when the driver marked the
	// error ambiguous. Counting these as ordinary refusals understated the
	// duplicates by the whole of this bucket, which the interrupt race found.
	Ambiguous int `json:"ambiguous,omitempty"`
	// Partial is how many of the publications counted in Published the
	// destination took for some of its subscriptions and refused for the
	// others (mq.ErrPartial). Each was settled on the source, so the
	// subscriptions that refused it do not have it.
	Partial int `json:"partial,omitempty"`
	// DuplicateRisk is how many deliveries were confirmed on the destination
	// and could NOT be settled on the source. Each is a message the source
	// will redeliver and the destination already has.
	DuplicateRisk int `json:"duplicate_risk"`
	// Returned is how many deliveries went back to the source unpublished. A
	// --dry-run returns each one with --on-stop. A run that publishes returns
	// with a requeue those it fetched after it was stopped and those fetched
	// after a failed publication.
	Returned int `json:"returned"`
	// ReturnFailures is how many deliveries the run could not hand back,
	// because the source refused the settlement that returns them. Each
	// stays with this run's consumer until its lease runs out, and the run
	// exits 1 (3 when the refusal is a capability the source lacks).
	ReturnFailures int `json:"return_failures,omitempty"`
	// Unkeyed is how many publications went out with no de-duplication key
	// although --dedup-from id asked for one, because the source delivery
	// carried no message id.
	Unkeyed int `json:"unkeyed,omitempty"`
	// Unsupported records a capability refusal, which is exit 3.
	Unsupported bool   `json:"unsupported"`
	StopReason  string `json:"stop_reason,omitempty"`
	// receiveFailed records that the run ended because the source's Receive
	// failed, which is exit 1 as it is for sub. The stop reason carries the
	// error, so the JSON form needs no field of its own.
	receiveFailed bool
}

// bridgeReport is the JSON form of a whole run.
type bridgeReport struct {
	Source      bridgeEnd `json:"source"`
	Destination bridgeEnd `json:"destination"`
	DryRun      bool      `json:"dry_run"`
	Settlement  string    `json:"settlement"`
	// Dedup says what --dedup-from id is resting on, when it was asked for:
	// the level the DESTINATION advertises for FeatureDeduplication, and
	// whether any conformance run probes that feature at all. It is empty
	// when the flag was not given.
	Dedup  string       `json:"deduplication,omitempty"`
	Result bridgeResult `json:"result"`
}

// bridgeEnd is one side of the seam, redacted.
type bridgeEnd struct {
	DSN          string `json:"dsn"`
	Topic        string `json:"topic"`
	Subscription string `json:"subscription,omitempty"`
	Driver       string `json:"driver,omitempty"`
}

// settlementStatement is printed on every run and carried in the JSON.
//
// It is a sentence and not a flag because there is nothing to choose: the
// guarantee follows from there being two brokers. The specification forbids an unqualified
// exactly-once claim, and a bridge is exactly the place somebody would read
// one into silence.
const settlementStatement = "at-least-once across the seam, with duplicates possible on the " +
	"destination: publish, confirm, then settle the source"

// cmdBridge is the bridge subcommand.
func cmdBridge(e *cliEnv, args []string) int {
	fs := newFlagSet("bridge", e)
	var (
		o       bridgeOptions
		onStop  = fs.String("on-stop", "nack", "nack or none for each delivery a dry run hands back")
		headers = fs.String("headers", "pass", "pass or drop the source headers")
		keyMode = fs.String("key", "pass", "pass or drop the source key")
		dedup   = fs.String("dedup-from", "none", "id or none")
		output  = fs.String("o", "text", "output format: text or json")
		outputL = fs.String("output", "", "output format: text or json")
	)
	fs.IntVar(&o.maxN, "max", 0, "stop after N messages")
	fs.DurationVar(&o.timeout, "timeout", 0, "bound the whole run")
	fs.DurationVar(&o.idle, "idle", 0, "stop after DURATION with no delivery")
	fs.IntVar(&o.prefetch, "prefetch", 0, "how many messages the source driver may hold")
	fs.StringVar(&o.confirm, "confirm", "default", "default, accepted or durable")
	fs.StringVar(&o.start, "start", "latest", "latest or earliest")
	fs.BoolVar(&o.dryRun, "dry-run", false, "publish nothing")
	fs.BoolVar(&o.quiet, "q", false, "print only the summary and the failures")
	fs.BoolVar(&o.quiet, "quiet", false, "print only the summary and the failures")
	if code := parseFlags(fs, e, args, bridgeUsage); code >= 0 {
		return code
	}
	if fs.NArg() != 5 {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: a source DSN, topic and subscription and a "+
			"destination DSN and topic are required\n"+bridgeUsage)
		return exitUsage
	}
	bad := func(err error) int {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: "+err.Error()+"\n")
		return exitUsage
	}
	var err error
	if o.onStop, err = parseSettle("--on-stop", *onStop); err != nil {
		return bad(err)
	}
	if o.headers, err = parsePassDrop("--headers", *headers); err != nil {
		return bad(err)
	}
	if o.key, err = parsePassDrop("--key", *keyMode); err != nil {
		return bad(err)
	}
	switch *dedup {
	case "none", "":
	case "id":
		o.dedupID = true
	default:
		return bad(errors.New("--dedup-from takes id or none, not " + quote(*dedup)))
	}
	format := *output
	if *outputL != "" {
		format = *outputL
	}
	switch format {
	case "text":
	case "json":
		o.json = true
	default:
		return bad(errors.New("--output takes text or json, not " + quote(format)))
	}
	if o.maxN < 0 {
		return bad(errors.New("--max takes a non-negative count"))
	}
	confirm, err := confirmationOf(o.confirm)
	if err != nil {
		return bad(err)
	}
	if _, err := parseStart(o.start); err != nil {
		return bad(err)
	}
	if o.dryRun && o.dedupID {
		return bad(errors.New("--dry-run publishes nothing, so --dedup-from id has nothing to " +
			"de-duplicate; the capability check it asks for belongs to a run that publishes"))
	}
	if o.dryRun && settleDestroys(o.onStop) {
		return bad(errors.New("--dry-run must leave the source as it was, so --on-stop takes " +
			"nack or none with it, not " + o.onStop.String()))
	}
	return e.bridge(&o, confirm, fs.Args())
}

// settleDestroys reports whether the action removes the message from the
// source, which a run that publishes nothing must never do: the message would
// exist on neither broker.
func settleDestroys(a settleAction) bool { return a == settleAck || a == settleReject }

// parsePassDrop maps a pass/drop flag to whether the field is carried.
func parsePassDrop(name, s string) (bool, error) {
	switch s {
	case "pass", "":
		return true, nil
	case "drop":
		return false, nil
	default:
		return false, errors.New(name + " takes pass or drop, not " + quote(s))
	}
}

// bridge opens both ends and runs the transfer loop.
func (e *cliEnv) bridge(o *bridgeOptions, confirm mq.ConfirmationMode, args []string) int {
	srcTarget, srcTopic, srcSub := args[0], args[1], args[2]
	dstTarget, dstTopic := args[3], args[4]

	// BOTH targets' credentials are held by the scrub, because either end's
	// driver can put its own DSN into an error string and the report names
	// them both.
	e, flush := e.scrubbed(o.json, srcTarget, dstTarget)
	defer flush()
	out := e.stdout

	rep := bridgeReport{
		Source: bridgeEnd{
			DSN: redactTarget(srcTarget), Topic: srcTopic, Subscription: srcSub,
		},
		Destination: bridgeEnd{DSN: redactTarget(dstTarget), Topic: dstTopic},
		DryRun:      o.dryRun,
		Settlement:  settlementStatement,
	}
	if o.dryRun {
		rep.Settlement = "nothing is published and every delivery is returned with --on-stop, " +
			"so this run moves no message at all"
	}

	// A bridge whose two ends are the same ephemeral in-process broker moves
	// nothing anybody can observe, and the note for it is the same one `pub`
	// and `sub` print. It is said for each end that is ephemeral.
	if !o.quiet && !o.json && (isEphemeral(srcTarget) || isEphemeral(dstTarget)) {
		_, _ = io.WriteString(out, ephemeralNote)
	}

	ctx, cancel, _ := e.expiring(e.ctx, o.timeout)
	defer cancel()

	// Every early return writes the report first. A run that failed at the
	// dial still has to produce parseable output under -o json: a tool that
	// reads this command's JSON cannot be asked to fall back to parsing
	// stderr, and a run interrupted before its first receive was the case
	// that found this -- it printed nothing at all and the reason was on a
	// stream nothing was reading.
	abort := func(code int) int {
		e.writeBridgeResult(out, &rep, o)
		return code
	}

	src, code := e.open(ctx, "bridge (source)", srcTarget)
	if code >= 0 {
		return abort(code)
	}
	defer func() { _ = src.Close() }()
	rep.Source.Driver = src.Info().Name

	dst, code := e.open(ctx, "bridge (destination)", dstTarget)
	if code >= 0 {
		return abort(code)
	}
	defer func() { _ = dst.Close() }()
	rep.Destination.Driver = dst.Info().Name

	// The source must be able to take a delivery BACK: every failure path in
	// the loop below hands one to Nack, and a source that cannot nack turns a
	// refused publication into a LOST message rather than a returned one. It
	// is checked before the consumer exists so the refusal names the feature
	reqs := []mq.Requirement{mq.RequireAtLeast(mq.FeatureNack, mq.Conditional)}
	start, _ := parseStart(o.start)
	if o.start == "earliest" {
		reqs = append(reqs, mq.RequireAtLeast(mq.FeatureReplay, mq.Conditional))
	}
	con, err := src.Consumer(ctx, mq.ConsumerConfig{
		Topic: srcTopic, Subscription: srcSub, Start: start,
		Prefetch: o.prefetch, Requirements: reqs,
	})
	if err != nil {
		// The redacted target is named: a bridge has TWO ends, and a failure
		// that said only "source" leaves a caller reading their own command
		// line to work out which DSN it was about.
		return abort(fail(e, "bridge (source "+rep.Source.DSN+" "+srcTopic+"/"+srcSub+")", err))
	}
	defer func() { _ = con.Close() }()

	var pub *mq.PublisherHandle
	if !o.dryRun {
		cfg := mq.PublisherConfig{Topic: dstTopic, Confirmation: confirm}
		if o.dedupID {
			// FeatureDeduplication and not FeatureIdempotentPublish: the specification's
			// DedupKey is "broker-side de-duplication by a CALLER key",
			// which is this feature; idempotent publish is the producer-side
			// sequence a driver manages without being given a key, and
			// requiring it here would refuse brokers that can do what the
			// flag asks.
			cfg.Requirements = append(cfg.Requirements,
				mq.RequireAtLeast(mq.FeatureDeduplication, mq.Conditional))
		}
		pub, err = dst.Publisher(ctx, cfg)
		if err != nil {
			return abort(fail(e, "bridge (destination "+rep.Destination.DSN+" "+dstTopic+")", err))
		}
		defer func() { _ = pub.Close() }()
		if o.dedupID {
			rep.Dedup = dedupStatement(dst.Capabilities())
		}
	}

	if !o.quiet && !o.json {
		writeBridgeHeader(out, &rep, confirm)
	}
	// A dry run with --on-stop none leaves every delivery unsettled, and each
	// one keeps its prefetch slot until its lease expires, so the run can hold
	// one window of them, as `sub --settle none` can. A source with no
	// settlement holds nothing and has no window.
	window := 0
	if o.dryRun && o.onStop == settleNone &&
		src.Capabilities().Support(mq.FeatureExplicitAck).Level != mq.Unsupported {
		window = cmp.Or(o.prefetch, mq.DefaultPrefetch)
	}
	rep.Result = e.transfer(ctx, con, pub, o, window)

	// transfer has settled every delivery it fetched: moved, or handed back
	// with --on-stop or a requeue. The graceful shutdown closes the source,
	// which gives the messages its driver still buffers back to the broker,
	// bounded by the same context so that an interrupted run does not hang on
	// a broker that stopped answering. It knows nothing of --on-stop.
	if err := con.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: source shutdown: "+err.Error()+"\n")
	}
	e.writeBridgeResult(out, &rep, o)
	switch r := &rep.Result; {
	case r.Unsupported:
		return exitUnsupported
	case r.PublishFailures > 0 || r.Partial > 0 || r.DuplicateRisk > 0 || r.ReturnFailures > 0 ||
		r.receiveFailed:
		return exitFailure
	}
	return exitOK
}

// transfer is the receive-publish-confirm-settle loop.
//
// One message at a time, and the publication is CONFIRMED before the source
// delivery is settled. Both halves of that are load-bearing:
//
//   - Flush before the settlement, because Publish may only have admitted the
//     message to the driver's buffer. Settling the source against an admission
//     is the same lost message as settling before publishing, just harder to
//     see.
//   - Nack on every failure path, so a message that did not reach the
//     destination stays on the source. The delivery is never simply dropped:
//     a dropped delivery is held until its lease expires with nothing in the
//     output saying so, which is the engine's recurring defect shape.
//
// A failed publication ends the run. The rest of its batch was fetched
// already, and publishing it would put those messages on the destination
// ahead of the one the source is about to redeliver, so they go back too.
//
// window is how many deliveries the run may leave unsettled, or 0 for no
// bound. The run fetches no more than the window has left and stops once it is
// full, rather than waiting on a source that delivers nothing more until a
// lease expires.
//
// The run's context is asked before every fetch and every delivery, and not
// only through Receive's error: a Receive on a done context may still return
// what is ready, so against a source that always had a delivery ready the
// loop never heard --timeout or the first Ctrl-C. What a stopped run fetched
// and did not move goes back unpublished: with --on-stop in a dry run, and
// with a requeue in a run that publishes, whatever --on-stop says.
//
// A dry run also stops when the source hands out a message it has already
// returned. Its default --on-stop nack is a requeue, and a queue hands a
// requeued message straight back, so the dry run counted one message again and
// again and, with no --max, never ended. returnedSet says how it knows a
// message again. A delivery it cannot know is read again until --max,
// --timeout or Ctrl-C ends the run.
func (e *cliEnv) transfer(
	ctx context.Context, con *mq.ConsumerHandle, pub *mq.PublisherHandle, o *bridgeOptions, window int,
) bridgeResult {
	var res bridgeResult
	batch := o.prefetch
	if batch <= 0 {
		batch = mq.DefaultPrefetch
	}
	dst := make([]mq.Delivery, batch)
	// returned is what a dry run has handed back; a run that publishes keeps
	// none.
	var returned returnedSet
	if pub == nil {
		returned = returnedSet{at: make(map[deliveryKey]string), ids: make(map[string]struct{})}
	}

	for {
		if o.maxN > 0 && res.Received >= o.maxN {
			res.StopReason = "the --max count was reached"
			break
		}
		if ctx.Err() != nil {
			res.StopReason = stoppedReason
			break
		}
		fetch := receiveWindow(len(dst), o.maxN, res.Received)
		if window > 0 {
			fetch = min(fetch, window-res.Received)
		}
		rctx, rcancel, idleExpired := e.expiring(ctx, o.idle)
		n, rerr := con.Receive(rctx, dst[:fetch])
		rcancel()
		failed := 0 // the number of the delivery whose publication failed
		repeated, cut := false, false
		for i := range n {
			d := &dst[i]
			if ctx.Err() != nil {
				// Stopped part-way through the batch, so none of the rest
				// was published. A dry run hands it back with --on-stop, as
				// it does every delivery. A run that publishes requeues it
				// whatever --on-stop says, as after a failed publication:
				// --on-stop ack destroyed messages that never reached the
				// destination.
				back := settleNack
				if pub == nil {
					back = o.onStop
				}
				for j := i; j < n; j++ {
					res.Received++
					e.returnDelivery(&dst[j], back, &res)
				}
				cut = true
				break
			}
			if returned.ids != nil {
				if returned.repeat(d) {
					// Counted when it was first received, so it goes back
					// uncounted; only a failure to hand it back is counted.
					repeated = true
					if !e.giveBack(d, o.onStop, &res) {
						res.ReturnFailures++
					}
					continue
				}
				returned.add(d)
			}
			res.Received++
			if e.moveOne(ctx, d, pub, o, &res) {
				continue
			}
			failed = res.Received
			// With a requeue and not --on-stop, as publishFailed returns the
			// failed one: none of these was published, and --on-stop ack
			// would destroy them.
			for j := i + 1; j < n; j++ {
				res.Received++
				e.returnDelivery(&dst[j], settleNack, &res)
			}
			break
		}
		if failed > 0 {
			res.StopReason = "publishing message " + itoa(failed) +
				" failed, and the run stops rather than publish a later message ahead of it"
			if ctx.Err() != nil {
				res.StopReason = stoppedReason
			}
			break
		}
		// Asked before --max: the delivery the stop handed back may be the
		// one that reached it, and the run did not stop for having read
		// enough.
		if cut {
			res.StopReason = stoppedReason
			break
		}
		if rerr != nil {
			res.StopReason, res.receiveFailed = receiveStop(rerr, ctx.Err() != nil, idleExpired(), o.idle)
			break
		}
		if repeated {
			res.StopReason = "the source handed out a message this dry run had already returned, so it " +
				"has read every message it can reach without consuming one"
			break
		}
		// --max first, as sub does: when the same delivery reaches both, the
		// run stopped because the caller asked for that many.
		if o.maxN > 0 && res.Received >= o.maxN {
			res.StopReason = "the --max count was reached"
			break
		}
		if ctx.Err() != nil {
			res.StopReason = stoppedReason
			break
		}
		if window > 0 && res.Received >= window {
			res.StopReason = "the prefetch window is full: " + itoa(window) + " unsettled " +
				"deliveries hold every slot until their leases expire (a larger --prefetch takes more)"
			break
		}
	}
	return res
}

// receiveWindow is how many deliveries the next Receive may take.
//
// It never exceeds what --max has left. A delivery fetched past --max is one
// the run could only hand back unhandled, and handing it back with --on-stop
// ack destroyed a message that was never published.
func receiveWindow(batch, maxN, received int) int {
	if maxN > 0 {
		return min(batch, maxN-received)
	}
	return batch
}

// moveOne publishes one delivery and settles it. It reports false when the
// publication failed, which ends the run.
func (e *cliEnv) moveOne(
	ctx context.Context, d *mq.Delivery, pub *mq.PublisherHandle,
	o *bridgeOptions, res *bridgeResult,
) bool {
	if pub == nil { // --dry-run
		e.returnDelivery(d, o.onStop, res)
		return true
	}
	// The specification borrows the received body until the delivery is settled, and the
	// settlement below happens after the publication, so the message handed to
	// Publish may point at the source driver's storage for the whole
	// publication. CloneMessage is what makes that safe: an async driver that
	// retains the buffer past Publish would otherwise be holding memory the
	// source is free to reuse the moment Ack returns.
	msg := d.CloneMessage()
	msg.Topic = pub.Topic()
	if !o.headers {
		msg.Headers = mq.Headers{}
	}
	if !o.key {
		msg.Key = nil
	}
	var opts []mq.PublishOption
	if o.dedupID {
		switch d.ID {
		case "":
			// No id, no key, no suppression. It is counted rather than
			// silently published unkeyed: a run that asked for
			// de-duplication and got none for half its messages has to say
			// which half.
			res.Unkeyed++
		default:
			opts = append(opts, mq.DedupKey(d.ID))
		}
	}
	perr := pub.Publish(ctx, msg, opts...)
	if perr != nil && !partlyAccepted(perr) {
		e.publishFailed(d, perr, false, res)
		return false
	}
	// Confirmed, not merely admitted. A failure here is ALWAYS ambiguous:
	// Publish already returned, so the message reached the driver, and
	// whether the broker took it is exactly what the failed Flush could not
	// establish. A stop between the Publish and the Flush does not make the
	// Flush fail (flushPublisher): on the stopped context it did, and a
	// message the destination had taken went back to the source, a certain
	// duplicate.
	if err := e.flushPublisher(ctx, pub); err != nil {
		e.publishFailed(d, err, true, res)
		return false
	}
	res.Published++
	if perr != nil {
		e.partlyPublished(perr, res)
	}

	if err := d.Ack(); err != nil {
		// This is the window the whole command's documentation is about: the
		// destination HAS the message and the source still thinks it owes it.
		// It is reported as a duplicate risk and not as a failure to move,
		// because the message did move.
		res.DuplicateRisk++
		if mq.CodeOf(err) == mq.CodeUnsupported || mq.CodeOf(err) == mq.CodeNotSettleable {
			res.Unsupported = true
		}
		_, _ = io.WriteString(e.stderr, "iqmq bridge: the destination confirmed message "+
			itoa(res.Received)+" and the source would not settle it ("+err.Error()+
			"): the source will redeliver it and the destination already has it\n")
		return true
	}
	res.Moved++
	return true
}

// partlyPublished reports a publication the destination took for some of its
// subscriptions and refused for the others. It is a publication: the source
// delivery is settled like any other, because returning it would put a second
// copy on every subscription that took the first, which the destination's
// own documentation says a publisher must not do. The subscriptions that
// refused it do not have it, and the run says so and exits 1.
func (e *cliEnv) partlyPublished(err error, res *bridgeResult) {
	res.Partial++
	_, _ = io.WriteString(e.stderr, "iqmq bridge: message "+itoa(res.Received)+
		" reached only some of the destination's subscriptions: "+err.Error()+
		"\n  the source delivery is settled and the message is not published again, because the\n"+
		"  subscriptions that took it would receive it twice; the ones that refused it do not have it\n")
}

// publishFailed returns a delivery whose publication did not succeed.
//
// admitted says the message had already reached the driver, which makes the
// outcome ambiguous whatever the error says.
//
// It takes no *bridgeOptions on purpose: the delivery is returned with a
// requeue whatever --on-stop says, because --on-stop is about the end of the
// RUN and a failed publication has to put the message back however the run is
// configured to finish.
func (e *cliEnv) publishFailed(d *mq.Delivery, err error, admitted bool, res *bridgeResult) {
	res.PublishFailures++
	if mq.CodeOf(err) == mq.CodeUnsupported {
		res.Unsupported = true
	}
	note := "\n  the source delivery is being returned, so nothing is lost\n"
	if admitted || ambiguousPublish(err) {
		res.Ambiguous++
		note = "\n  the source delivery is being returned, so nothing is lost -- but the " +
			"destination MAY\n  already hold this message, so returning it can produce a " +
			"duplicate there\n"
	}
	_, _ = io.WriteString(e.stderr, "iqmq bridge: publishing message "+itoa(res.Received)+
		" to the destination: "+err.Error()+note)
	// Returned with a nack whatever --on-stop says: --on-stop is about the
	// end of the RUN, and a failed publication has to put the message back
	// however the run is configured to finish. A bridge that acked a message
	// it could not deliver would be a message shredder with a summary line.
	if nerr := d.Nack(mq.NackRequeue()); nerr != nil {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: returning that delivery to the source: "+
			nerr.Error()+"\n")
		if mq.CodeOf(nerr) == mq.CodeUnsupported || mq.CodeOf(nerr) == mq.CodeNotSettleable {
			res.Unsupported = true
		}
	}
}

// ambiguousPublish reports whether a driver marked a publication's outcome
// unknown (the third answer, which is neither success nor refusal). It is
// mq.IsAmbiguous, which reads the flag AND the codes CodeAmbiguous and
// CodeSettlementUnknown; reading the flag alone counted a failure ambiguous by
// its code as a clean refusal.
func ambiguousPublish(err error) bool { return mq.IsAmbiguous(err) }

// returnDelivery hands one delivery back unpublished: with --on-stop on a
// --dry-run or after the run was stopped, and with a nack after a failed
// publication. A nack requeues. A hand-back the source refuses is counted as a
// return failure and not as returned: the delivery is still this run's until
// its lease runs out.
func (e *cliEnv) returnDelivery(d *mq.Delivery, action settleAction, res *bridgeResult) {
	if e.giveBack(d, action, res) {
		res.Returned++
		return
	}
	res.ReturnFailures++
}

// giveBack settles d with action, a nack being a requeue, and reports whether
// the source took it. A refusal is reported on stderr, and one for a
// capability the source lacks is recorded as such.
func (e *cliEnv) giveBack(d *mq.Delivery, action settleAction, res *bridgeResult) bool {
	_, err := settleDelivery(d, action, []mq.NackOption{mq.NackRequeue()})
	if err == nil {
		return true
	}
	_, _ = io.WriteString(e.stderr, "iqmq bridge: returning a delivery with "+
		action.String()+": "+err.Error()+"\n")
	if mq.CodeOf(err) == mq.CodeUnsupported || mq.CodeOf(err) == mq.CodeNotSettleable {
		res.Unsupported = true
	}
	return false
}

// returnedSet is what a dry run has handed back, so that it knows a message
// the source hands out again. It grows with every distinct message the run
// reads, by about a hundred bytes each; a long dry run over a large log is
// bounded with --max.
//
// It knows a message two ways, and either is enough:
//   - by the broker's position, an offset or a sequence, with the message ids
//     agreeing where both deliveries carry one. A sequence need not be a
//     position: RabbitMQ's is the delivery tag, new on every delivery and
//     starting again at 1 on a new channel, so after a reconnect a message
//     the run never saw can carry a tag it returned, and the ids tell them
//     apart.
//   - by the message id, on a delivery the broker marks as a redelivery. An id
//     is the publisher's and need not be unique, so two messages with one id
//     are two messages until then.
type returnedSet struct {
	// at maps a position to the message id its delivery carried.
	at  map[deliveryKey]string
	ids map[string]struct{}
}

// repeat reports whether d is a message the run has already returned.
func (r returnedSet) repeat(d *mq.Delivery) bool {
	if key, ok := deliveryKeyOf(d); ok {
		if id, seen := r.at[key]; seen && (id == "" || d.ID == "" || id == d.ID) {
			return true
		}
	}
	if d.ID == "" || !d.Info().Redelivered {
		return false
	}
	_, seen := r.ids[d.ID]
	return seen
}

// add records d as returned.
func (r returnedSet) add(d *mq.Delivery) {
	if key, ok := deliveryKeyOf(d); ok {
		r.at[key] = d.ID
	}
	if d.ID != "" {
		r.ids[d.ID] = struct{}{}
	}
}

// deliveryKey is a delivery's broker position.
type deliveryKey struct {
	topic, source string
	partition     int32
	kind          uint8
	position      uint64
}

// The kinds of deliveryKey, in the order deliveryKeyOf prefers them.
const (
	keyOffset uint8 = iota + 1
	keySequence
)

// deliveryKeyOf returns d's position, its offset or else its sequence, and
// false when d carries neither.
func deliveryKeyOf(d *mq.Delivery) (deliveryKey, bool) {
	info := d.Info()
	k := deliveryKey{topic: info.Topic, source: info.Source}
	if info.HasPartition {
		k.partition = info.Partition
	}
	switch {
	case info.HasOffset:
		k.kind, k.position = keyOffset, uint64(info.Offset)
	case info.HasSequence:
		k.kind, k.position = keySequence, info.Sequence
	default:
		return deliveryKey{}, false
	}
	return k, true
}

// writeBridgeHeader prints what the run is about to do.
func writeBridgeHeader(w io.Writer, rep *bridgeReport, confirm mq.ConfirmationMode) {
	var b strings.Builder
	b.WriteString("iqmq bridge\n  from ")
	b.WriteString(rep.Source.DSN)
	b.WriteString(" ")
	b.WriteString(rep.Source.Topic)
	b.WriteString("/")
	b.WriteString(rep.Source.Subscription)
	b.WriteString("\n  to   ")
	b.WriteString(rep.Destination.DSN)
	b.WriteString(" ")
	b.WriteString(rep.Destination.Topic)
	b.WriteString("\n  destination confirmation ")
	b.WriteString(confirm.Resolve().String())
	b.WriteString("\n  settlement: ")
	b.WriteString(rep.Settlement)
	b.WriteString("\n")
	if rep.Dedup != "" {
		b.WriteString("  de-duplication: ")
		b.WriteString(rep.Dedup)
		b.WriteString("\n")
	}
	_, _ = io.WriteString(w, b.String())
}

// dedupStatement says what --dedup-from id rests on.
//
// The never-probed half is read from [neverProbedFeatures] rather than written
// out here, so that a feature the conformance kit LOSES a probe for starts
// carrying the caveat without anybody editing this file -- and one that gains
// one stops. The list itself is held to the generated matrix by
// TestNeverProbedMatchesTheGeneratedMatrix.
func dedupStatement(caps mq.Capabilities) string {
	f := mq.FeatureDeduplication
	s := caps.Support(f)
	out := f.String() + "=" + s.Level.String() + " AS ADVERTISED by the destination"
	if s.Note != "" {
		out += " (" + s.Note + ")"
	}
	switch {
	case isNeverProbed(f.String()):
		out += "; NO conformance run probes this feature, so that level is a claim and not evidence"
	default:
		out += "; the conformance kit probes it, but the de-duplication WINDOW is the broker's " +
			"and no run bounds it"
	}
	return out
}

// writeBridgeResult prints the summary.
func (e *cliEnv) writeBridgeResult(w io.Writer, rep *bridgeReport, o *bridgeOptions) {
	if o.json {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			_, _ = io.WriteString(e.stderr, "iqmq bridge: "+err.Error()+"\n")
		}
		return
	}
	r := &rep.Result
	var b strings.Builder
	b.WriteString("received ")
	b.WriteString(itoa(r.Received))
	b.WriteString(" published ")
	b.WriteString(itoa(r.Published))
	b.WriteString(" moved ")
	b.WriteString(itoa(r.Moved))
	if r.PublishFailures > 0 {
		b.WriteString(" publish-failed ")
		b.WriteString(itoa(r.PublishFailures))
	}
	if r.Returned > 0 {
		b.WriteString(" returned ")
		b.WriteString(itoa(r.Returned))
	}
	if r.ReturnFailures > 0 {
		b.WriteString(" return-failed ")
		b.WriteString(itoa(r.ReturnFailures))
	}
	if r.DuplicateRisk > 0 {
		b.WriteString(" duplicate-risk ")
		b.WriteString(itoa(r.DuplicateRisk))
	}
	if r.Ambiguous > 0 {
		b.WriteString(" ambiguous ")
		b.WriteString(itoa(r.Ambiguous))
	}
	if r.Partial > 0 {
		b.WriteString(" partial ")
		b.WriteString(itoa(r.Partial))
	}
	if r.Unkeyed > 0 {
		b.WriteString(" unkeyed ")
		b.WriteString(itoa(r.Unkeyed))
	}
	b.WriteString("\n")
	if r.StopReason != "" {
		b.WriteString("stopped: ")
		b.WriteString(r.StopReason)
		b.WriteString("\n")
	}
	if r.Ambiguous > 0 {
		b.WriteString("note: ")
		b.WriteString(itoa(r.Ambiguous))
		b.WriteString(" publication(s) failed with the destination's acceptance UNKNOWN.\n  ")
		b.WriteString("Each was admitted to the driver before the failure, so the destination ")
		b.WriteString("may hold it;\n  the source delivery was returned either way, so the ")
		b.WriteString("message is not lost.\n")
	}
	if r.Partial > 0 {
		b.WriteString("note: ")
		b.WriteString(itoa(r.Partial))
		b.WriteString(" publication(s) reached only some of the destination's subscriptions.\n  ")
		b.WriteString("Each was settled on the source, because publishing it again would put a second ")
		b.WriteString("copy on\n  every subscription that took the first; the ones that refused it do not ")
		b.WriteString("have it.\n")
	}
	if r.ReturnFailures > 0 {
		b.WriteString("note: ")
		b.WriteString(itoa(r.ReturnFailures))
		b.WriteString(" delivery(ies) could not be handed back to the source. Each stays with this ")
		b.WriteString("run's consumer\n  until its lease runs out, and the source then redelivers it.\n")
	}
	if r.DuplicateRisk > 0 {
		b.WriteString("note: ")
		b.WriteString(itoa(r.DuplicateRisk))
		b.WriteString(" message(s) were confirmed on the destination and NOT settled on the ")
		b.WriteString("source.\n  The source will redeliver each of them and the destination ")
		b.WriteString("already has it. That is the\n  seam's guarantee, not a bug in either ")
		b.WriteString("broker.\n")
	}
	_, _ = io.WriteString(w, b.String())
}
