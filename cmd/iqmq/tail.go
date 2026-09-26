package main

import (
	"io"
	"time"

	mq "github.com/iqhive/iqmq"
)

const tailUsage = `usage: iqmq tail [flags] <dsn> <topic> [subscription]

Watch a topic: start at the latest message and settle nothing where the driver
allows it.

Whether a topic can be tailed at all is a fact about the broker, not about this
tool, and tail reads it out of the driver's capabilities before it opens
anything (the message contract):

  FeatureFanOut     a subscription of its own gets a COPY, so watching costs a
                    competing consumer nothing. Without it, every message tail
                    receives is a message somebody else does not, and tail
                    refuses unless --consume says that is wanted.
  FeatureAtLeastOnce  an unsettled delivery is redelivered, so leaving one
                    unsettled consumes nothing. Without it (and with explicit
                    acknowledgement) a delivery tail does not settle is lost,
                    so tail has to settle to be honest about it and says so.

flags:
      --max N            stop after N deliveries (0 = until interrupted)
      --timeout DURATION stop after DURATION (0 = no bound)
      --idle DURATION    stop after DURATION with no delivery
      --prefetch N       how many messages the driver may hold
      --consume          proceed even though tailing this driver consumes
      --from-earliest    start at the oldest retained message instead of latest
      --subscription N   use this subscription name instead of a generated one
  -o, --output text|json output format (default text)
      --headers print the message headers (off by default)
      --no-body          do not print the message body
      --base64           print the body as base64
      --meta             print the broker metadata (partition, offset, lease)

tail never acknowledges on a driver whose unsettled deliveries come back. That
is not free: an unsettled delivery holds a prefetch slot, so a long tail fills
the window and stops, saying so, rather than appearing to hang.
`

// tailVerdict is what the capabilities say about tailing this driver.
type tailVerdict struct {
	// ownCopy reports FeatureFanOut: a subscription of tail's own receives a
	// copy rather than competing for the messages.
	ownCopy bool
	// leaveUnsettled reports that a delivery tail does not settle comes back:
	// either there is no settlement at all, or the driver is at-least-once
	// and an expired lease redelivers.
	leaveUnsettled bool
	// noSettlement reports that the driver has no settlement AT ALL, which
	// is a different reason from "an unsettled delivery comes back" and
	// needs a different sentence: a driver with no settlement has no leases
	// either, so saying a delivery comes back when its lease expires is
	// saying something false. NATS Core is the case, and it is the first
	// driver a tail was run against.
	noSettlement bool
	// why lists the capability facts the verdict was read from, verbatim, so
	// that a refusal names the feature and the level rather than asserting a
	// conclusion.
	why []string
}

// verdictFor reads the two facts tail needs out of the inventory.
func verdictFor(caps mq.Capabilities) tailVerdict {
	fanOut := caps.Support(mq.FeatureFanOut)
	explicit := caps.Support(mq.FeatureExplicitAck)
	atLeastOnce := caps.Support(mq.FeatureAtLeastOnce)
	v := tailVerdict{
		ownCopy: fanOut.Level != mq.Unsupported,
		// Nothing to settle, or an unsettled delivery is redelivered.
		leaveUnsettled: explicit.Level == mq.Unsupported || atLeastOnce.Level != mq.Unsupported,
		noSettlement:   explicit.Level == mq.Unsupported,
		why: []string{
			supportLine(caps, mq.FeatureFanOut),
			supportLine(caps, mq.FeatureExplicitAck),
			supportLine(caps, mq.FeatureAtLeastOnce),
		},
	}
	return v
}

// cmdTail is the tail subcommand.
func cmdTail(e *cliEnv, args []string) int {
	fs := newFlagSet("tail", e)
	var (
		o            consumeOptions
		consume      = fs.Bool("consume", false, "proceed even though tailing this driver consumes")
		fromEarliest = fs.Bool("from-earliest", false, "start at the oldest retained message")
		subName      = fs.String("subscription", "", "use this subscription name")
		output       = fs.String("o", "text", "output format: text or json")
		outputL      = fs.String("output", "", "output format: text or json")
		noBody       = fs.Bool("no-body", false, "do not print the message body")
	)
	fs.IntVar(&o.maxN, "max", 0, "stop after N deliveries")
	fs.DurationVar(&o.timeout, "timeout", 0, "stop after DURATION")
	fs.DurationVar(&o.idle, "idle", 0, "stop after DURATION with no delivery")
	fs.IntVar(&o.prefetch, "prefetch", 0, "how many messages the driver may hold")
	fs.BoolVar(&o.render.headers, "headers", false, "print the message headers")
	fs.BoolVar(&o.render.base64, "base64", false, "print the body as base64")
	fs.BoolVar(&o.render.meta, "meta", false, "print the broker metadata")
	if code := parseFlags(fs, e, args, tailUsage); code >= 0 {
		return code
	}
	if fs.NArg() < 2 || fs.NArg() > 3 {
		_, _ = io.WriteString(e.stderr, "iqmq tail: a DSN and a topic are required, "+
			"and a subscription is optional\n"+tailUsage)
		return exitUsage
	}
	o.concurrency = 1
	o.start = "latest"
	if *fromEarliest {
		o.start = "earliest"
	}
	// tail never settles on its own account. --on-stop is "none" too: a
	// delivery in flight when the run ends is left exactly as unsettled as
	// every other one, so stopping tail does not consume the message it was
	// showing you when you stopped it.
	if code := o.finish(e, "none", "none", "requeue", *output, *outputL, *noBody); code >= 0 {
		return code
	}

	target, topic := fs.Arg(0), fs.Arg(1)
	sub := *subName
	if sub == "" {
		sub = fs.Arg(2)
	}
	if sub == "" {
		sub = e.tailSubscription()
	}

	e, flush := e.scrubbed(target)
	defer flush()
	out := e.stdout

	// The capability read needs a client, so the connection is opened here
	// rather than inside consume: a refusal has to name the levels it read,
	// and the levels come from the driver.
	probeCtx, probeCancel := e.deadline(e.ctx, 10*time.Second)
	client, code := e.open(probeCtx, "tail", target)
	if code >= 0 {
		probeCancel()
		return code
	}
	v := verdictFor(client.Capabilities())
	_ = client.Close()
	probeCancel()

	_, _ = io.WriteString(out, "target "+redactTarget(target)+" topic "+topic+
		"\nsubscription "+sub+"\ncapabilities read:\n")
	for _, line := range v.why {
		_, _ = io.WriteString(out, "  "+line+"\n")
	}

	if !v.ownCopy && !*consume {
		_, _ = io.WriteString(e.stderr, "iqmq tail: this driver cannot be tailed without consuming.\n"+
			"  FeatureFanOut is unsupported, so a subscription of tail's own does not receive a copy:\n"+
			"  every message tail takes is one a competing consumer does not get.\n"+
			"  That is a property of the broker (the message contract), not of this tool.\n"+
			"  Pass --consume to tail anyway, or use \"iqmq sub\", which settles on purpose.\n")
		return exitUnsupported
	}
	if !v.leaveUnsettled {
		// The driver acknowledges by delivering: a delivery tail does not
		// settle is gone. Saying so and carrying on is the honest answer --
		// the caller asked to watch, the watch is destructive, and the
		// alternative is to refuse a command that would work.
		_, _ = io.WriteString(e.stderr, "iqmq tail: NOTE: this driver is not at-least-once and settles "+
			"explicitly, so a delivery tail does not settle is lost rather than redelivered.\n"+
			"  tail still settles nothing, which means the messages it prints are consumed "+
			"from this subscription's point of view.\n")
	}
	if !*consume && v.ownCopy {
		// Two different reasons, and saying the wrong one is saying something
		// false.
		if v.noSettlement {
			_, _ = io.WriteString(out, "settling nothing: this driver has no settlement at all, "+
				"so there is nothing to settle and nothing is consumed\n")
		} else {
			_, _ = io.WriteString(out, "settling nothing: an unsettled delivery holds a prefetch "+
				"slot and comes back when its lease expires\n")
		}
	}

	return e.consume(&o, "tail", target, topic, sub)
}

// tailSubscription generates the name of a subscription tail owns.
//
// It is generated rather than fixed so that two tails do not share one
// subscription and split the stream between them, which is the surprise a
// fixed name would produce the first time two people ran tail at once. The
// clock is the environment's, so a test gets a name it can predict.
func (e *cliEnv) tailSubscription() string {
	return "iqmq-tail-" + i64toa(e.clock.Now().UnixNano())
}
