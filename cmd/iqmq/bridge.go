package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
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
      --on-stop ACTION   ack|nack|reject|none for a source delivery still in
                         flight when the run ends (default nack)
      --headers pass|drop  carry the source headers (default pass)
      --key pass|drop      carry the source key (default pass)
      --dedup-from id|none use the source message id as the destination's
                           de-duplication key (default none)
      --dry-run          receive and report, publish NOTHING, and return every
                         delivery to the source with --on-stop
  -o, --output text|json output format (default text)
  -q, --quiet            print only the summary and the failures

Ordering: this bridge is single-flight by design. One
message is published and confirmed before the next is received, so the
destination sees the source's order wherever the source delivers in order. A
concurrent bridge would be faster and would reorder, and a flag that silently
reordered a stream somebody bridged for its order would be the wrong default
to offer at all.

exit codes: 0 every message moved; 1 a publication or a settlement failed;
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
	// DuplicateRisk is how many deliveries were confirmed on the destination
	// and could NOT be settled on the source. Each is a message the source
	// will redeliver and the destination already has.
	DuplicateRisk int `json:"duplicate_risk"`
	// Returned is how many deliveries were HANDLED and handed back with
	// --on-stop rather than published, which is every delivery of a
	// --dry-run.
	Returned int `json:"returned"`
	// BeyondMax is how many deliveries the source handed over past --max and
	// were given straight back.
	//
	// It is a separate bucket from Returned because the two are different
	// populations: a beyond-max delivery is not counted in Received at all
	// -- Received counts what the run HANDLED -- so adding them together was
	// an accounting identity that could not hold, and the interrupt race
	// found it at "1 deliveries received and 128 accounted for".
	BeyondMax int `json:"beyond_max,omitempty"`
	// Unkeyed is how many publications went out with no de-duplication key
	// although --dedup-from id asked for one, because the source delivery
	// carried no message id.
	Unkeyed int `json:"unkeyed,omitempty"`
	// Unsupported records a capability refusal, which is exit 3.
	Unsupported bool   `json:"unsupported"`
	StopReason  string `json:"stop_reason,omitempty"`
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
		onStop  = fs.String("on-stop", "nack", "ack, nack, reject or none for an in-flight delivery")
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
	return e.bridge(&o, confirm, fs.Args())
}

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
	e, flush := e.scrubbed(srcTarget, dstTarget)
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
	rep.Result = e.transfer(ctx, con, pub, o)

	// The consumer drains before the summary, so a delivery admitted and not
	// yet handled is settled with --on-stop rather than left to its lease.
	if err := con.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: source shutdown: "+err.Error()+"\n")
	}
	e.writeBridgeResult(out, &rep, o)
	switch {
	case rep.Result.Unsupported:
		return exitUnsupported
	case rep.Result.PublishFailures > 0 || rep.Result.DuplicateRisk > 0:
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
func (e *cliEnv) transfer(
	ctx context.Context, con *mq.ConsumerHandle, pub *mq.PublisherHandle, o *bridgeOptions,
) bridgeResult {
	var res bridgeResult
	batch := o.prefetch
	if batch <= 0 {
		batch = mq.DefaultPrefetch
	}
	dst := make([]mq.Delivery, batch)

	for {
		if o.maxN > 0 && res.Received >= o.maxN {
			res.StopReason = "the --max count was reached"
			break
		}
		rctx, rcancel, idleExpired := e.expiring(ctx, o.idle)
		n, rerr := con.Receive(rctx, dst)
		rcancel()
		for i := range n {
			d := &dst[i]
			if o.maxN > 0 && res.Received >= o.maxN {
				// Beyond --max: taken from the broker and given straight
				// back. Every delivery a bridge fetched has to be given back
				// explicitly -- one dropped on the floor is a message held
				// until its lease expires with nothing in the output saying
				// so, which is this repository's recurring defect shape.
				res.BeyondMax++
				e.returnDelivery(d, o, &res)
				continue
			}
			res.Received++
			e.moveOne(ctx, d, pub, o, &res)
		}
		if rerr != nil {
			switch {
			case idleExpired():
				res.StopReason = "no delivery for " + o.idle.String()
			case errors.Is(rerr, context.Canceled), errors.Is(rerr, context.DeadlineExceeded):
				res.StopReason = "the run was stopped"
			default:
				res.StopReason = "Receive failed: " + rerr.Error()
			}
			break
		}
	}
	return res
}

// moveOne publishes one delivery and settles it.
func (e *cliEnv) moveOne(
	ctx context.Context, d *mq.Delivery, pub *mq.PublisherHandle,
	o *bridgeOptions, res *bridgeResult,
) {
	if pub == nil { // --dry-run
		e.returnDelivery(d, o, res)
		return
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
		if d.ID == "" {
			// No id, no key, no suppression. It is counted rather than
			// silently published unkeyed: a run that asked for
			// de-duplication and got none for half its messages has to say
			// which half.
			res.Unkeyed++
		} else {
			opts = append(opts, mq.DedupKey(d.ID))
		}
	}
	if err := pub.Publish(ctx, msg, opts...); err != nil {
		e.publishFailed(d, err, false, res)
		return
	}
	// Confirmed, not merely admitted. A failure here is ALWAYS ambiguous:
	// Publish already returned nil, so the message reached the driver, and
	// whether the broker took it is exactly what the failed Flush could not
	// establish.
	if err := pub.Flush(ctx); err != nil {
		e.publishFailed(d, err, true, res)
		return
	}
	res.Published++

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
		return
	}
	res.Moved++
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
			"duplicate there \n"
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

// returnDelivery hands one delivery back with --on-stop.
func (e *cliEnv) returnDelivery(d *mq.Delivery, o *bridgeOptions, res *bridgeResult) {
	res.Returned++
	if _, err := settleDelivery(d, o.onStop, []mq.NackOption{mq.NackRequeue()}); err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq bridge: returning a delivery with --on-stop "+
			o.onStop.String()+": "+err.Error()+"\n")
		if mq.CodeOf(err) == mq.CodeUnsupported || mq.CodeOf(err) == mq.CodeNotSettleable {
			res.Unsupported = true
		}
	}
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
	if isNeverProbed(f.String()) {
		out += "; NO conformance run probes this feature, so that level is a claim and not evidence"
	} else {
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
	if r.BeyondMax > 0 {
		b.WriteString(" beyond-max ")
		b.WriteString(itoa(r.BeyondMax))
	}
	if r.DuplicateRisk > 0 {
		b.WriteString(" duplicate-risk ")
		b.WriteString(itoa(r.DuplicateRisk))
	}
	if r.Ambiguous > 0 {
		b.WriteString(" ambiguous ")
		b.WriteString(itoa(r.Ambiguous))
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
