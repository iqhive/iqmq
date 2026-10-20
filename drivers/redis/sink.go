package redis

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/wire"
)

// The sink implements the mandatory publication surface and the optional
// native batch.
//
// It is NOT an [github.com/iqhive/iqmq.AsyncSink], an
// [github.com/iqhive/iqmq.OwnedSink] or a
// [github.com/iqhive/iqmq.FlushSink], and each absence is a decision.
// rueidis pipelines automatically, so "asynchronous publication" on this
// client is what a concurrent Publish already gets and an AsyncSink would be
// a goroutine per message wrapped around a call that already pipelines, which
// The specification forbids. OwnedSink would have nothing to release earlier than Publish
// does: a publication is written before Publish returns, whether its body was
// copied into the command or sent as the caller's own bytes, so the earliest
// an owned body could be released is the moment Publish returns anyway.
// FlushSink has nothing to flush: there is no buffer this sink holds — a
// publication is in flight or it has returned.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
)

// envelopeLimits bounds what this driver accepts, and it is the entry
// layout's own set — which is the portable envelope's, field for field.
//
// See [streams.Limits]: the envelope's two halves are not symmetric, so a
// message admitted above the decoder's bound is published successfully and
// then breaks every Fetch on that subscription for ever. The bound applies in
// every metadata mode, because a message accepted in one mode has to be
// accepted in all of them.
var envelopeLimits = wire.DefaultLimits()

// sink is one open publisher, bound to one topic for its lifetime (ADR-021).
type sink struct {
	b       *Backend
	env     *driverkit.Envelope
	topic   string
	keys    streams.Keys
	meta    metadataMode
	durable bool

	// alias is the backend's [streams.AliasesArgs] answer, compiled in at
	// construction (rule 4): true when the client behind the commander
	// reads a command's tokens before the call returns and never after, so
	// the body can go out as the caller's own bytes.
	alias bool

	once   sync.Once
	closed chan struct{}

	// fields is the scratch the entry layout is built into, reused across
	// publications so that a publish allocates the command's tokens and
	// nothing else. blobs is its companion when alias is set: the body's
	// [streams.Blob], which [streams.XAdd] copies out with the fields.
	mu     sync.Mutex
	fields []string
	blobs  []streams.Blob
}

// Publish admits one message.
//
// The order is fixed and it is the specification: the per-call controls this broker
// cannot honour are refused before anything is written, then the limits are
// checked, then the entry is built, then XADD goes to the server, and only
// then — under ConfirmDurable — does WAITAOF wait for the append-only file.
//
// Nothing of m is retained, and the promise that a caller may reuse its
// buffers the instant Publish returns holds on both clients — by two different
// routes. Where the commander copies (rueidis), the entry layout copies every
// byte it needs into the command's tokens. Where it does not (go-redis), the
// body goes out as the caller's own slice and the client has finished writing
// it before this function returns; see [streams.ArgAliaser] for the claim that
// makes the difference, and `drivers/redis/client.go` for why rueidis cannot
// make it.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	shard, err := s.shardFor(m, o)
	if err != nil {
		return err
	}
	cmd, err := s.command(m, shard)
	if err != nil {
		return err
	}
	v, err := s.b.cmder.Do(ctx, cmd)
	if err != nil {
		// The command was written, so the outcome is unknown: the server may
		// have stored the entry before the link went away. Reporting it as a
		// definite failure is how a duplicate comes to look like a loss.
		return s.fail(err)
	}
	// CheckXAdd and not ParseXAdd: the specification says a publication's outcome is an error
	// or nothing, there is no receipt on this broker carrying the entry's
	// identifier, and this driver has no other use for it -- so the identifier
	// was built, discarded, and paid for per message. The
	// answers are the same three: ErrNoStream for NOMKSTREAM's refusal,
	// ErrReplyShape for a reply that is not a string, ErrBadID for a string
	// that is not an identifier.
	if err := streams.CheckXAdd(v); err != nil {
		return s.fail(err)
	}
	return s.confirmDurable(ctx)
}

// PublishBatch admits a batch.
//
// The saving is real and it is what rueidis's pipeline gives: every XADD is
// written in one flush and the replies come back together, so a batch of a
// hundred costs one round trip instead of a hundred. It is NOT atomic — Redis
// executes pipelined commands independently — which is why
// FeatureAtomicBatchPublish is unsupported and FeatureBatchPublish is native:
// the batching is the broker's, the atomicity is not on offer.
//
// The result is index-aligned with ms, which is the whole contract: a result
// aligned with anything else reports one message's outcome for another.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	cmds := make([]streams.Cmd, 0, len(ms))
	var errs []error
	failed := 0
	for i, m := range ms {
		shard, err := s.shardFor(m, o)
		if err == nil {
			var cmd streams.Cmd
			if cmd, err = s.command(m, shard); err == nil {
				cmds = append(cmds, cmd)
				continue
			}
		}
		// A message refused before any I/O keeps its place in the result by
		// having its error recorded now and no command sent for it.
		if errs == nil {
			errs = make([]error, len(ms))
		}
		errs[i] = err
		failed++
		cmds = append(cmds, streams.Cmd{})
	}
	live := make([]streams.Cmd, 0, len(cmds))
	at := make([]int, 0, len(cmds))
	for i := range cmds {
		if cmds[i].Valid() {
			live = append(live, cmds[i])
			at = append(at, i)
		}
	}
	if len(live) > 0 {
		results, err := s.b.cmder.DoMulti(ctx, live)
		if err != nil {
			if errs == nil {
				errs = make([]error, len(ms))
			}
			for _, i := range at {
				errs[i] = s.fail(err)
				failed++
			}
		} else {
			for n, i := range at {
				var res streams.Result
				if n < len(results) {
					res = results[n]
				} else {
					res = streams.Result{Err: errShortPipeline}
				}
				perr := res.Err
				if perr == nil {
					// The identifier is not read here and never was: the
					// index into errs is what aligns a reply with its
					// message (`at[n]`), not anything in the reply. So the
					// check is CheckXAdd, which answers the same error for
					// the same reply and does not build the number --
					// ParseXAdd cost two strconv.ParseUint calls per
					// message for a value this loop discarded.
					perr = streams.CheckXAdd(res.Value)
				}
				if perr == nil {
					continue
				}
				if errs == nil {
					errs = make([]error, len(ms))
				}
				errs[i] = s.fail(perr)
				failed++
			}
		}
	}
	if failed == 0 {
		return s.confirmDurable(ctx)
	}
	if failed == len(ms) {
		return batchAnswer(errs, nil)
	}
	// Part of the batch is in the stream, so the durable boundary still
	// applies to that part; a failure of it is added to the messages that
	// had none of their own.
	return batchAnswer(errs, s.confirmDurable(ctx))
}

// batchAnswer is the answer for a batch in which something failed. short, when
// it is not nil, is the durable boundary's own failure, and it is given to
// every message that had none of its own -- they are in the stream but short
// of the boundary, which is ambiguous. The answer is then driverkit's
// CollapseBatch: the single error only when every message failed with the same
// The specification outcome. The first error used to stand for a wholly failed batch, and a
// hand-built *BatchError for one the fill had made wholly failed, so a reply
// refusal next to a short WAITAOF read as a refusal.
func batchAnswer(errs []error, short error) error {
	if short != nil {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = short
			}
		}
	}
	return driverkit.CollapseBatch(errs)
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It does not touch the client: many sinks share one.
func (s *sink) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.b.forget(nil, s)
	})
	return nil
}

// isClosed reports whether Close has run.
func (s *sink) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// check is "enforced before I/O": the controls this broker has no way to
// honour are refused here, with ErrUnsupported, before a byte is written.
//
// A driver that ignored them instead would publish a message the caller asked
// to be delayed, to expire, or to be de-duplicated, and the caller would have
// no way to find out. Two of the five are worth naming, because a reader who
// knows Redis will wonder:
//
//   - A DELAY is refused even though the zset nack strategy can hold a
//     REDELIVERY back. They are not the same thing: a delayed publication is
//     a message that must not be deliverable yet, and a delayed redelivery is
//     a message that has already been delivered once. Reusing the retry
//     machinery for the first would mean acknowledging a message nobody has
//     consumed.
//   - A TTL is refused even though a stream has MAXLEN. MAXLEN is the whole
//     stream's retention and is measured in entries, not in time, and it
//     cannot expire one message out of the middle.
func (s *sink) check(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	if m == nil {
		return streams.ErrNilMessage
	}
	if o != nil {
		switch {
		case o.Delay > 0 || !o.DeliverAt.IsZero():
			return errNoSchedule
		case o.TTL > 0:
			return errNoTTL
		case o.PrioritySet:
			return errNoPriority
		case o.DedupKey != "":
			return errNoDedup
		}
	}
	// The layout's limits, in every metadata mode, before any I/O. This is
	// the check that keeps a publication the driver accepted decodable: see
	// [streams.Limits] and.
	if err := streams.DefaultLimits.Check(m); err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	// the strict mode refuses metadata the backend cannot represent
	// natively, BEFORE any I/O. driverkit.Envelope.Wrap deliberately does not
	// refuse — wrapping is not what strict mode does — so the refusal is the
	// driver's, and a driver that only called Wrap would silently envelope or
	// drop (the finding; see the maintenance notes).
	//
	// On this broker Needed is false for every message, because a stream
	// entry carries ordered, duplicate-preserving, binary header values
	// natively. The three lines are here anyway: they are the contract, and a
	// driver whose capability table later loses FeatureBinaryHeaders would
	// otherwise start dropping header values with nothing to notice.
	if s.meta == metaStrict && s.env.Needed(m) {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", mq.ErrUnsupported)
	}
	return nil
}

// shardFor decides which of the topic's shards a message goes to.
//
// The rule is the specification: an explicit partition wins, then the key's hash, then
// shard 0. The hash is [streams.ShardOf], which is stable across processes and
// across restarts — a key that hashed differently in two publishers would
// break per-key ordering without anything failing.
func (s *sink) shardFor(m *mq.Message, o *mq.PublishOptions) (int, error) {
	if o != nil && o.PartitionSet {
		if o.Partition < 0 || int(o.Partition) >= s.keys.Shards {
			return 0, errPartitionOutOfRange
		}
		return int(o.Partition), nil
	}
	if s.keys.Shards == 1 {
		return 0, nil
	}
	return ShardOf(m.Key, s.keys.Shards), nil
}

// command builds the XADD for one message.
//
// The body is the one token that can be megabytes, and whether it is copied
// into the command or carried as the caller's bytes is the commander's answer
// rather than this function's choice. Everything else is
// identical either way: [streams.XAdd] copies the field list out of the
// scratch, so the returned command owns its tokens, and a blob it carries
// points at m.Body — which stays valid because the caller still owns it until
// Publish or PublishBatch returns, and both send before they do.
//
// An enveloped message is aliased too, and safely: [driverkit.Envelope.Wrap]
// returns a message whose body it allocated for this call, so the alias
// outlives nothing.
func (s *sink) command(m *mq.Message, shard int) (streams.Cmd, error) {
	out := m
	if s.meta == metaEnvelope || s.meta == metaAdaptive {
		var err error
		if out, err = s.env.Wrap(m); err != nil {
			return streams.Cmd{}, s.b.errs.Map(mq.OpPublish, s.topic, "", err)
		}
	}
	s.mu.Lock()
	var blobs []streams.Blob
	if s.alias {
		s.fields, s.blobs = streams.EncodeAliased(s.fields[:0], s.blobs[:0], out)
		blobs = s.blobs
	} else {
		s.fields = streams.Encode(s.fields[:0], out)
	}
	fields := s.fields
	cmd := streams.XAdd(streams.XAddArgs{
		Key:        s.keys.Stream(shard),
		ID:         streams.IDAuto,
		Fields:     fields,
		FieldBlobs: blobs,
		MaxLen:     s.b.cfg.MaxLen,
		NoMkStream: !s.b.cfg.AutoCreate,
	})
	s.mu.Unlock()
	return cmd, nil
}

// confirmDurable applies the durable boundary when the publisher asked for
// one.
//
// XADD's reply is the memory-durable confirmation and WAITAOF is the
// conditional durable one. "Conditional" means the condition has to be
// STATED, and it is: the capability note names the version and the
// append-only file, and a server that cannot meet either refuses
// ConfirmDurable at open rather than here.
//
// What this does NOT do is treat a WAITAOF that came back short as a success.
// A write that reached fewer append-only files than the caller asked for has
// not reached the boundary the caller asked for, so it is reported — with
// CodeSettlementUnknown's publish-side equivalent, an ambiguous outcome,
// because the entry IS in the stream and may yet be fsynced.
func (s *sink) confirmDurable(ctx context.Context) error {
	if !s.durable {
		return nil
	}
	cmd, err := streams.WaitAOF(s.b.cfg.DurableLocal, s.b.cfg.DurableReplicas, s.b.cfg.DurableTimeout)
	if err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	v, err := s.b.cmder.Do(ctx, cmd)
	if err != nil {
		return s.fail(err)
	}
	ack, err := streams.ParseWaitAOF(v)
	if err != nil {
		return s.fail(err)
	}
	if !ack.Reached(s.b.cfg.DurableLocal, s.b.cfg.DurableReplicas) {
		return s.fail(errDurableShort)
	}
	return nil
}

// fail maps a native failure onto the model, with sentAlready deciding
// whether the outcome is ambiguous.
//
// Every failure that reaches here is sentAlready, because every one of them
// comes from a command that was written. What decides the ambiguity is
// therefore the rule table and driverkit.ClassifyNet's fallback, in that
// order:
//
//   - a server REPLY matches a rule, and every rule in this driver's table
//     leaves Ambiguous false — correctly, because a reply means the server
//     processed the command and refused it;
//   - a reply no rule knows falls through to ClassifyNet, which classifies an
//     unrecognised error as CodeUnknown and NOT ambiguous — also correct, for
//     the same reason;
//   - a transport failure falls through to ClassifyNet too, and there
//     sentAlready is what makes it ambiguous.
//
// So the honest mapping needs no special case for "is this a reply?", and an
// earlier version of this function had one that could not change any outcome.
// What it does need is for no rule to claim a transport failure, which is why
// the link rules are the two they are.
func (s *sink) fail(err error) error {
	// sentAlready is unconditionally true, and that is the point rather than
	// a simplification: every failure that reaches here comes from a command
	// that was WRITTEN, so its outcome is unknown until the rule table or
	// ClassifyNet says otherwise. A `fail(err, false)` call site would be one
	// that knew the command had not left the client, and this driver has
	// none — rueidis reports a build failure as a panic in Arbitrary and a
	// refusal to dial as an error from the constructor, neither of which
	// arrives here.
	return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, true)
}
