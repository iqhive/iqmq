package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
)

const pubUsage = `usage: iqmq pub [flags] <dsn> <topic> [body ...]

Publish one message per positional body, or one per line of --file / stdin.

Confirmation is explicit and is reported. The exit status is non-zero when a
publication was not confirmed, which includes the ambiguous case: a publication
whose acceptance the broker never reported is NOT a success, and
the message contract gives it its own code so that a caller can tell it from a
refusal.

flags:
  -H, --header K=V       add a header; repeatable, and a repeated key adds a
                         second value where the driver supports it
  -k, --key STRING       the partition or ordering key
  -f, --file PATH        read bodies from a file, or "-" for stdin
      --lines            one message per line of the input (the default for
                         --file and stdin); with --no-lines the whole input is
                         one message
      --batch            publish the whole input as one batch (PublishBatch)
                         instead of one message at a time
      --confirm MODE     default|accepted|durable (the message contract)
      --delay DURATION   publish so that it becomes visible after DURATION
      --ttl DURATION     per-message expiry
      --dedup-key STRING broker-side de-duplication key
      --priority N       per-message priority (0-255)
      --partition N      publish to this partition
      --receipt          print what the broker said about each publication;
                         needs FeatureReceipts and refuses without it
      --timeout DURATION bound the whole run (default 30s)
  -q, --quiet            print only failures

A message body given as a positional argument is taken verbatim, including a
leading "-": the flags stop at the topic.
`

// headerFlag collects -H K=V into a Headers value.
//
// A repeated key ADDS rather than replaces, which is the duplicate-
// preserving order: a CLI that collapsed two Set-Cookie headers into one would
// hide the very thing a driver's FeatureDuplicateHeaders claim is about.
type headerFlag struct {
	h     mq.Headers
	given []string
}

// String renders what was given, for the flag package's own reporting. It
// renders the KEYS only: a header value can be a credential -- a bearer token
// in an "authorization" header is the obvious case -- and the specification keeps bodies and
// headers out of the output by default.
func (f *headerFlag) String() string {
	keys := make([]string, 0, len(f.given))
	for _, g := range f.given {
		k, _, _ := strings.Cut(g, "=")
		keys = append(keys, k)
	}
	return strings.Join(keys, ",")
}

// Set parses one K=V.
func (f *headerFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return errors.New("a header is written --header key=value")
	}
	f.h.Add(k, []byte(v))
	f.given = append(f.given, s)
	return nil
}

// confirmationOf maps the --confirm spelling to the mode.
func confirmationOf(s string) (mq.ConfirmationMode, error) {
	switch s {
	case "", "default":
		return mq.ConfirmDefault, nil
	case "accepted":
		return mq.ConfirmAccepted, nil
	case "durable":
		return mq.ConfirmDurable, nil
	default:
		return 0, errors.New("--confirm takes default, accepted or durable, not " + quote(s))
	}
}

// pubOptions are the publication controls one invocation carries.
type pubOptions struct {
	headers   headerFlag
	key       string
	file      string
	lines     bool
	noLines   bool
	batch     bool
	confirm   string
	delay     time.Duration
	ttl       time.Duration
	dedupKey  string
	priority  int
	partition int
	setPri    bool
	setPart   bool
	receipt   bool
	timeout   time.Duration
	quiet     bool
}

// cmdPub is the pub subcommand.
func cmdPub(e *cliEnv, args []string) int {
	fs := newFlagSet("pub", e)
	var o pubOptions
	fs.Var(&o.headers, "H", "add a header, key=value")
	fs.Var(&o.headers, "header", "add a header, key=value")
	fs.StringVar(&o.key, "k", "", "the partition or ordering key")
	fs.StringVar(&o.key, "key", "", "the partition or ordering key")
	fs.StringVar(&o.file, "f", "", "read bodies from a file, or - for stdin")
	fs.StringVar(&o.file, "file", "", "read bodies from a file, or - for stdin")
	fs.BoolVar(&o.lines, "lines", false, "one message per line of the input")
	fs.BoolVar(&o.noLines, "no-lines", false, "the whole input is one message")
	fs.BoolVar(&o.batch, "batch", false, "publish the input as one batch")
	fs.StringVar(&o.confirm, "confirm", "default", "default, accepted or durable")
	fs.DurationVar(&o.delay, "delay", 0, "publish with a delay")
	fs.DurationVar(&o.ttl, "ttl", 0, "per-message expiry")
	fs.StringVar(&o.dedupKey, "dedup-key", "", "broker-side de-duplication key")
	fs.IntVar(&o.priority, "priority", 0, "per-message priority")
	fs.IntVar(&o.partition, "partition", 0, "publish to this partition")
	fs.BoolVar(&o.receipt, "receipt", false, "print the broker's receipt per publication")
	fs.DurationVar(&o.timeout, "timeout", 30*time.Second, "bound the whole run")
	fs.BoolVar(&o.quiet, "q", false, "print only failures")
	fs.BoolVar(&o.quiet, "quiet", false, "print only failures")
	if code := parseFlags(fs, e, args, pubUsage); code >= 0 {
		return code
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "priority":
			o.setPri = true
		case "partition":
			o.setPart = true
		}
	})
	if fs.NArg() < 2 {
		_, _ = io.WriteString(e.stderr, "iqmq pub: a DSN and a topic are required\n"+pubUsage)
		return exitUsage
	}
	target, topic := fs.Arg(0), fs.Arg(1)
	bodies := fs.Args()[2:]

	// Both streams are scrubbed from here on, which is before anything can
	// produce a message that mentions the target.
	e, flush := e.scrubbed(target)
	defer flush()
	out := e.stdout

	confirm, err := confirmationOf(o.confirm)
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq pub: "+err.Error()+"\n")
		return exitUsage
	}
	if o.lines && o.noLines {
		_, _ = io.WriteString(e.stderr, "iqmq pub: --lines and --no-lines contradict each other\n")
		return exitUsage
	}

	// The bodies. Positional arguments win; otherwise the input is read from
	// --file or stdin. Nothing is read from stdin unless the invocation asked
	// for it: a CLI that blocks on an empty terminal because no body was given
	// looks hung rather than wrong.
	if len(bodies) == 0 {
		src, name, cerr := e.bodySource(o.file)
		if cerr != nil {
			return fail(e, "pub", cerr)
		}
		if src == nil {
			_, _ = io.WriteString(e.stderr, "iqmq pub: no body: give one as an argument, "+
				"or read one with --file PATH (or --file - for stdin)\n")
			return exitUsage
		}
		read, rerr := readBodies(src, !o.noLines)
		if closer, ok := src.(io.Closer); ok && name != "-" {
			_ = closer.Close()
		}
		if rerr != nil {
			return fail(e, "pub", rerr)
		}
		bodies = read
	}
	if len(bodies) == 0 {
		_, _ = io.WriteString(e.stderr, "iqmq pub: the input held no message\n")
		return exitFailure
	}

	// The publication controls are folded and validated BEFORE a connection
	// is opened, so that a contradictory invocation is refused without
	// touching a broker -- and so that the message names the option rather
	// than whatever the dial failed with.
	opts, err := publishOptions(&o)
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq pub: "+err.Error()+"\n")
		return exitUsage
	}

	ctx, cancel := e.deadline(e.ctx, o.timeout)
	defer cancel()
	client, code := e.open(ctx, "pub", target)
	if code >= 0 {
		return code
	}
	defer func() { _ = client.Close() }()

	cfg := mq.PublisherConfig{Topic: topic, Confirmation: confirm, Receipts: o.receipt}
	if o.receipt {
		// the fail-fast: a publisher that needs a capability the driver
		// does not advertise is never constructed, and the refusal names the
		// feature. It is exit 3 and not exit 1, because no retry changes it.
		cfg.Requirements = append(cfg.Requirements,
			mq.RequireAtLeast(mq.FeatureReceipts, mq.Conditional))
	}
	if o.delay > 0 {
		cfg.Requirements = append(cfg.Requirements,
			mq.RequireAtLeast(mq.FeatureScheduledPublish, mq.Conditional))
	}
	pub, err := client.Publisher(ctx, cfg)
	if err != nil {
		return fail(e, "pub", err)
	}
	defer func() { _ = pub.Close() }()

	if !o.quiet && isEphemeral(target) {
		_, _ = io.WriteString(out, ephemeralNote)
	}
	if !o.quiet {
		_, _ = io.WriteString(out, "target "+redactTarget(target)+" topic "+topic+
			"\nconfirmation "+confirm.Resolve().String()+
			"\nmessages "+itoa(len(bodies))+"\n")
	}

	confirmed, failures := e.publish(ctx, out, pub, bodies, opts, &o)

	// Whatever the driver buffered is completed before the count is reported:
	// a "confirmed" line printed before Flush would be counting admissions.
	if err := pub.Flush(ctx); err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq pub: flush: "+err.Error()+"\n")
		failures++
	}
	if !o.quiet {
		_, _ = io.WriteString(out, "confirmed "+itoa(confirmed)+" of "+itoa(len(bodies))+"\n")
	}
	if failures > 0 {
		_, _ = io.WriteString(e.stderr, "iqmq pub: "+itoa(failures)+
			" of "+itoa(len(bodies))+" publications were not confirmed\n")
		return exitFailure
	}
	return exitOK
}

// publish sends the bodies, one at a time or as one batch, and returns how
// many were confirmed and how many were not.
func (e *cliEnv) publish(
	ctx context.Context, out io.Writer, pub *mq.PublisherHandle,
	bodies []string, opts []mq.PublishOption, o *pubOptions,
) (confirmed, failures int) {
	if o.batch {
		ms := make([]*mq.Message, 0, len(bodies))
		receipts := make([]mq.PublishReceipt, len(bodies))
		for i, b := range bodies {
			m := e.message(b, o)
			if o.receipt {
				m.Receipt = &receipts[i]
			}
			ms = append(ms, m)
		}
		err := pub.PublishBatch(ctx, ms, opts...)
		if err == nil {
			if o.receipt {
				for i := range receipts {
					_, _ = io.WriteString(out, "receipt "+itoa(i)+" "+receiptLine(&receipts[i])+"\n")
				}
			}
			return len(bodies), 0
		}
		// the index alignment: a *BatchError reports each message's own
		// outcome, so a batch that partly failed is reported per message
		// rather than as one number nobody can act on.
		var be *mq.BatchError
		if errors.As(err, &be) && len(be.Errs) == len(ms) {
			for i, ierr := range be.Errs {
				if ierr == nil {
					confirmed++
					continue
				}
				failures++
				_, _ = io.WriteString(e.stderr, "iqmq pub: message "+itoa(i)+" "+outcomeWord(ierr)+": "+ierr.Error()+"\n")
			}
			return confirmed, failures
		}
		_, _ = io.WriteString(e.stderr, "iqmq pub: batch "+outcomeWord(err)+": "+err.Error()+"\n")
		return 0, len(bodies)
	}
	var receipt mq.PublishReceipt
	for i, b := range bodies {
		m := e.message(b, o)
		if o.receipt {
			receipt = mq.PublishReceipt{}
			m.Receipt = &receipt
		}
		if err := pub.Publish(ctx, m, opts...); err != nil {
			failures++
			_, _ = io.WriteString(e.stderr, "iqmq pub: message "+itoa(i)+" "+outcomeWord(err)+": "+err.Error()+"\n")
			continue
		}
		confirmed++
		if o.receipt {
			_, _ = io.WriteString(out, "receipt "+itoa(i)+" "+receiptLine(&receipt)+"\n")
		}
	}
	return confirmed, failures
}

// message builds one message from a body.
func (e *cliEnv) message(body string, o *pubOptions) *mq.Message {
	m := &mq.Message{Body: []byte(body), Headers: o.headers.h.Clone()}
	if o.key != "" {
		m.Key = []byte(o.key)
	}
	return m
}

// publishOptions maps the flags to the publication controls.
func publishOptions(o *pubOptions) ([]mq.PublishOption, error) {
	var opts []mq.PublishOption
	if o.delay > 0 {
		opts = append(opts, mq.Delay(o.delay))
	}
	if o.ttl > 0 {
		opts = append(opts, mq.TTL(o.ttl))
	}
	if o.dedupKey != "" {
		opts = append(opts, mq.DedupKey(o.dedupKey))
	}
	if o.setPri {
		if o.priority < 0 || o.priority > 255 {
			return nil, errors.New("--priority takes 0 to 255")
		}
		opts = append(opts, mq.Priority(o.priority))
	}
	if o.setPart {
		if o.partition < 0 {
			return nil, errors.New("--partition takes a non-negative partition")
		}
		opts = append(opts, mq.Partition(o.partition))
	}
	// The option set is validated here rather than at the first publish, so
	// that a contradictory invocation fails before a connection is opened.
	var resolved mq.PublishOptions
	if err := resolved.Apply(opts...); err != nil {
		return nil, err
	}
	return opts, nil
}

// receiptLine renders a publication receipt, naming only what the broker
// actually reported.
func receiptLine(r *mq.PublishReceipt) string {
	parts := make([]string, 0, 6)
	if r.MessageID != "" {
		parts = append(parts, "id="+r.MessageID)
	}
	parts = append(parts, "confirmation="+r.Confirmation.Resolve().String())
	if r.Partition != 0 {
		parts = append(parts, "partition="+itoa(int(r.Partition)))
	}
	if r.Offset != 0 {
		parts = append(parts, "offset="+i64toa(r.Offset))
	}
	if r.Sequence != 0 {
		parts = append(parts, "sequence="+i64toa(int64(r.Sequence)))
	}
	if !r.ConfirmedAt.IsZero() {
		parts = append(parts, "at="+r.ConfirmedAt.Format(time.RFC3339Nano))
	}
	return strings.Join(parts, " ")
}

// bodySource opens the input --file names, or the environment's stdin for "-".
// It returns a nil reader when the invocation asked for no input at all.
func (e *cliEnv) bodySource(file string) (io.Reader, string, error) {
	switch file {
	case "":
		return nil, "", nil
	case "-":
		return e.stdin, "-", nil
	default:
		f, err := os.Open(file)
		if err != nil {
			return nil, "", err
		}
		return f, file, nil
	}
}

// readBodies reads the input as one message per line, or as one whole message.
//
// A line is taken verbatim minus its newline, and a trailing newline at the
// end of the input does NOT produce an empty final message: a file written by
// a text editor would otherwise publish a spurious empty body every time.
func readBodies(r io.Reader, perLine bool) ([]string, error) {
	if !perLine {
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		return []string{string(b)}, nil
	}
	var out []string
	sc := bufio.NewScanner(r)
	// A message may be larger than bufio's default 64 KiB line, and a driver's
	// own MaxMessageBytes is what should refuse an oversized one -- not this
	// scanner, silently, at a boundary nothing documents.
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// notConfirmed is how `iqmq pub` names a publication whose acceptance is
// unknown.
const notConfirmed = "NOT CONFIRMED (acceptance unknown)"

// outcomeWord names a failed publication's outcome for the person reading the
// output. An ambiguous one is not a refusal: the broker may have it, and an
// operator told "refused" publishes it again. It asks mq.IsAmbiguous, which
// reads the flag and the two ambiguous codes; reading the code alone called a
// connection that died after the write -- CodeUnavailable with the flag --
// "refused".
func outcomeWord(err error) string {
	if mq.IsAmbiguous(err) {
		return notConfirmed
	}
	return "refused"
}
