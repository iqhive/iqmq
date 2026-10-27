package streams

import (
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Kind identifies a negative-settlement strategy: what this driver does at
// the broker when a delivery is nacked (the API contract).
//
// Redis Streams has no nack. A consumer group knows two states for an entry —
// pending and acknowledged — and nothing in between, so "make this message
// deliverable again, optionally after a delay" has to be built out of the
// commands there are. There are four ways to build it and they differ in what
// they cost and in what they preserve, which is what the specification asks the
// bake-off to measure: redelivery latency accuracy, pending-entries-list
// growth, and duplicates under a crash.
//
// Auto resolves to [AutoKind], the recorded bake-off winner.
type Kind uint8

// The kinds. LeavePending is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// LeavePending does nothing at the broker: the entry stays in the
	// pending-entries list and the mandatory reclaim loop redelivers it once
	// it has been idle for the acknowledgement deadline.
	//
	// It is the reference and the cheapest: zero commands per nack (one
	// guarded idle-clock restart for a nack that asks for a delay), no new
	// entry, no auxiliary key, and the entry keeps its position and its
	// delivery counter, so the attempt accounting and the per-shard
	// order both survive a retry. What it cannot do is honour a delay
	// shorter or longer than the acknowledgement deadline: the redelivery
	// happens when the reclaim loop next sweeps, which is one deadline
	// later. A nack asking for a longer delay is refused rather than
	// silently redelivered early.
	LeavePending
	// ZSetMover acknowledges the entry and parks it in a sorted set scored
	// by the time it becomes deliverable, and a mover puts it back into the
	// stream when that time comes. Both halves are Lua scripts, so the park
	// and the acknowledgement are one step and so are the release and the
	// removal.
	//
	// It is the only kind that honours an arbitrary delay exactly, which is
	// what the specification's "zset for delays" means. It costs one script call per
	// nack, a second per release, and a sorted set per shard; the entry gets
	// a new identifier, so its position in the stream is lost and its
	// attempt count has to be carried in the entry itself
	// ([FieldAttempt]).
	ZSetMover
	// ReAdd acknowledges the entry and adds it back immediately under a new
	// identifier, in one Lua script.
	//
	// It is the lowest-latency redelivery there is — the message is
	// deliverable again before the nack returns — and it is the one kind
	// whose pending-entries list never grows, because the entry leaves it.
	// The costs are the mirror image: a new identifier means the message
	// moves to the end of its shard, so a retried message overtakes nothing
	// and is overtaken by everything published since, and a delay it cannot
	// honour at all.
	ReAdd
	// Stop does nothing at the broker AND stops the source reading new
	// entries from the nacked entry's shard until that entry is settled.
	//
	// It is the ordering-preserving arm: a message that failed is retried
	// before anything behind it is delivered, which is what a caller who
	// asked for strict per-key order expects a retry to mean. It pays for it
	// with head-of-line blocking — one poisonous message stops its whole
	// shard until MaxAttempts dead-letters it — which is why it is an
	// arm and not the default.
	Stop

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "leavepending", "zsetmover", "readd", "stop"}

var kindIdents = [kindCount]string{"Auto", "LeavePending", "ZSetMover", "ReAdd", "Stop"}

// String returns the kind's slug, as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := LeavePending; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// KindNames lists the concrete kinds' slugs, for a DSN parameter's
// enumeration.
func KindNames() []string { return kindSlugs[1:] }

// NackConfig parameterises a strategy.
type NackConfig struct {
	// Keys is the topic's key naming.
	Keys Keys
	// Group is the consumer group.
	Group string
	// Consumer is this source's consumer name, which every guarded script
	// checks the pending entry against.
	Consumer string
	// AckDeadline is the lease the source's deliveries are given, which is
	// also the reclaim loop's min-idle and therefore [LeavePending]'s
	// redelivery latency.
	AckDeadline time.Duration
	// MaxLen trims the stream a re-added or released entry goes into, when
	// positive. It is the publish path's trim applied to the retry path, so
	// a retry cannot grow a stream the operator bounded.
	MaxLen int64
	// DeadLetterStream is the key a Reject routes to, empty when the
	// subscription has no dead-letter destination.
	DeadLetterStream string
}

// NackRequest is one negative settlement.
type NackRequest struct {
	// Entry is the delivery's entry: its identifier, its shard and the
	// field-value list to re-add or park. Fields may be nil for a strategy
	// that does not move the entry.
	Entry Entry
	// Options are the action, delay and reason.
	Options mq.NackOptions
	// Now is the driver's clock reading, used to score a delayed park. A
	// strategy never reads a clock itself: the specification injects one and a script may
	// not read the server's.
	Now time.Time
	// Attempt is the delivery attempt being nacked, carried into a re-added
	// entry so that MaxAttempts accounting survives a strategy that
	// changes the identifier.
	Attempt uint32
}

// NackPlan is what a strategy decided: the commands to send, and what the
// source must do about the shard afterwards.
//
// It is a plan rather than an action because every strategy is then a pure
// function of its inputs — testable against a model, fuzzable against a
// reference, and benchmarkable without a server, which is what the bake-off
// needs of every arm.
type NackPlan struct {
	// Cmds are the commands to send, in order. An empty list is a strategy
	// that settles nothing at the broker, which is [LeavePending] and
	// [Stop] for a nack without a delay; with one, they send only the
	// idle-clock restart, which settles nothing either.
	Cmds []Cmd
	// Settled reports that the plan removes the entry from the
	// pending-entries list, so the reclaimer must forget it. A plan that
	// does not is one where the entry stays pending and the reclaim loop
	// will bring it back.
	Settled bool
	// HoldShard reports that the source must stop reading new entries from
	// the entry's shard until the entry is settled. Only [Stop] sets it.
	HoldShard bool
	// Apart reports that Cmds are the cross-slot dead-letter move, the XADD
	// to the dead-letter stream and then the guarded acknowledgement, which
	// must be sent one after the other: pipelined, an XADD that failed
	// beside an acknowledgement that succeeded loses the message.
	Apart bool
	// RedeliverAfter is when the message becomes deliverable again, relative
	// to NackRequest.Now, as the strategy arranged it. It is what row
	// the specification's "redelivery latency accuracy" is read against: a strategy that
	// cannot honour the delay asked for is refused rather than reporting a
	// latency it does not deliver.
	RedeliverAfter time.Duration
}

// Strategy turns a negative settlement into commands.
//
// Plan is pure: it reads no clock, sends nothing and allocates only the plan.
// Everything that talks to a server is the caller's, which is what lets one
// model test hold all four kinds to the same contract.
type Strategy interface {
	// Kind returns the concrete kind (never Auto).
	Kind() Kind
	// Plan returns the commands for one negative settlement, or an error
	// when this strategy cannot honour it — which is [mq.ErrUnsupported]
	// for a delay it has no way to arrange, and never a plan that quietly
	// does something else.
	Plan(req NackRequest) (NackPlan, error)
	// Config returns the resolved configuration.
	Config() NackConfig
	// Delays reports whether this strategy can honour an arbitrary
	// NackOptions.Delay. It is what the capability table's
	// DelayedRedelivery claim is built from, so a driver cannot advertise a
	// delay the bound strategy refuses.
	Delays() bool
	// Reorders reports whether a retried message loses its position in its
	// shard. It is the other half of the ordering claim: a strategy that
	// reorders cannot support the strict per-key order across a retry.
	Reorders() bool
}

// NewNack returns the strategy of kind k. It refuses an unknown kind and a
// configuration a strategy cannot work with, rather than panicking: the kind
// can come from a DSN parameter, which is a caller's string.
func NewNack(k Kind, cfg NackConfig) (Strategy, error) {
	if !k.Valid() {
		return nil, ErrUnknownKind
	}
	k = k.Resolve()
	if cfg.Group == "" || cfg.Consumer == "" {
		return nil, ErrBadArgs
	}
	if cfg.Keys.Shards < 1 {
		return nil, ErrShardCount
	}
	if cfg.AckDeadline <= 0 {
		return nil, ErrBadArgs
	}
	base := base{cfg: cfg}
	switch k {
	case LeavePending:
		return leavePending{base}, nil
	case ZSetMover:
		return zsetMover{base}, nil
	case ReAdd:
		return reAdd{base}, nil
	case Stop:
		return stop{base}, nil
	case Auto, kindCount:
		// Auto is unreachable: Resolve mapped it. Returning an error rather
		// than falling through keeps the switch exhaustive for the linter and
		// makes a future AutoKind of Auto a failure instead of a nil
		// strategy.
		return nil, ErrUnknownKind
	}
	return nil, ErrUnknownKind
}

// base is the shared configuration and the checks every strategy makes.
type base struct{ cfg NackConfig }

// Config returns the resolved configuration.
func (b base) Config() NackConfig { return b.cfg }

// checkAction refuses an action no strategy can take, which is the same
// refusal for all four: a drop destroys the message and a dead-letter routes
// it, and neither is a redelivery strategy's business.
//
// [NackActionDrop] is honoured by every strategy, because on this broker it
// is exactly an acknowledgement: the entry leaves the pending-entries list
// and is never delivered again. Saying so is what keeps the specification's "destructive
// and has to be written" honest — the driver does not pretend a drop is a
// requeue.
//
// [NackActionDeadLetter] needs a destination, and the plan is the Lua move.
// It is the same for every strategy because the strategies differ in how they
// make a message deliverable AGAIN, and a dead-letter makes it deliverable
// somewhere else.
func (b base) commonPlan(req NackRequest) (NackPlan, bool, error) {
	switch req.Options.Action {
	case mq.NackActionDrop:
		return NackPlan{Cmds: []Cmd{b.ackCmd(req.Entry)}, Settled: true}, true, nil
	case mq.NackActionDeadLetter:
		plan, err := b.deadLetterPlan(req)
		return plan, true, err
	case mq.NackActionRequeue:
		return NackPlan{}, false, nil
	}
	return NackPlan{}, true, ErrBadArgs
}

// ackCmd is the acknowledgement a drop sends. It is the guarded form: a drop
// destroys the message, so doing it to a delivery another consumer now owns
// is the worst version of the loss [Reclaimer] documents.
func (b base) ackCmd(e Entry) Cmd {
	return EvalSHA(SHAOf(ScriptAckGuard),
		[]string{b.cfg.Keys.Stream(e.Shard)},
		[]string{b.cfg.Group, b.cfg.Consumer, e.ID.String()})
}

// holdBack is what makes a delayed nack of a pending kind arrive no earlier
// than asked.
//
// [LeavePending] and [Stop] accept a delay no longer than the deadline on the
// grounds that the reclaim loop then redelivers later than asked. The loop
// claims an entry once it has been idle for the deadline, and the idle clock
// runs from the entry's last delivery, not from the nack: a handler that
// nacks four seconds into a five-second lease, asking for three, would see
// the entry back one second later. Restarting the clock at the nack puts the
// redelivery at least one deadline after it, which is at least the delay,
// and is what NackPlan.RedeliverAfter, counted from the nack, says.
//
// It is the guarded extension script, for the reasons [ScriptExtendGuard]
// gives: a bare XCLAIM would take the entry back from a consumer that
// reclaimed it meanwhile, and JUSTID leaves the delivery counter alone, so
// MaxAttempts counts the same deliveries it would have. A nack with no delay
// asked for nothing to be held back and sends nothing, which keeps the
// default nack free of commands.
func (b base) holdBack(req NackRequest) []Cmd {
	if req.Options.Delay <= 0 {
		return nil
	}
	return []Cmd{EvalSHA(SHAOf(ScriptExtendGuard),
		[]string{b.cfg.Keys.Stream(req.Entry.Shard)},
		[]string{b.cfg.Group, b.cfg.Consumer, req.Entry.ID.String()})}
}

// deadLetterPlan is the Lua dead-letter move.
//
// When the dead-letter stream does not share the topic's hash slot the script
// cannot run at all — a cluster refuses a script whose keys are on two slots
// — so the plan becomes two commands, sent in order by [Consumer.Nack], and
// the duplicate window that opens between them is documented on the driver
// page rather than hidden: a crash after the XADD and before the
// acknowledgement leaves the message in the dead-letter stream AND pending in
// the source, so it is dead-lettered twice.
func (b base) deadLetterPlan(req NackRequest) (NackPlan, error) {
	if b.cfg.DeadLetterStream == "" {
		return NackPlan{}, mq.ErrUnsupported
	}
	src := b.cfg.Keys.Stream(req.Entry.Shard)
	fields := deadLetterFields(req.Entry.Fields)
	if SlotSafe(src, b.cfg.DeadLetterStream) {
		args := make([]string, 0, 4+len(fields))
		args = append(args, b.cfg.Group, b.cfg.Consumer, req.Entry.ID.String(),
			strconv.FormatInt(b.cfg.MaxLen, 10))
		args = append(args, fields...)
		return NackPlan{
			Cmds:    []Cmd{EvalSHA(SHAOf(ScriptDeadLetter), []string{src, b.cfg.DeadLetterStream}, args)},
			Settled: true,
		}, nil
	}
	return NackPlan{
		Cmds: []Cmd{
			XAdd(XAddArgs{Key: b.cfg.DeadLetterStream, ID: IDAuto, Fields: fields, MaxLen: b.cfg.MaxLen}),
			b.ackCmd(req.Entry),
		},
		Settled: true,
		Apart:   true,
	}, nil
}

// deadLetterFields returns the entry's fields as the dead-letter copy carries
// them: with no attempt count and no group stamp.
//
// The count is the source subscription's bookkeeping. A re-added entry carries
// it ([FieldAttempt]) because its new identifier restarts the broker's
// delivery counter, and [Consumer.register] adds it to that counter. The
// dead-letter copy is a new message to the groups that read the dead-letter
// stream, so a copy that kept the source's count (or wrote the attempt being
// rejected into it) reached its first reader as attempt N+1, reported as a
// redelivery, and a dead-letter subscription with a MaxAttempts of its own
// had spent most of it before anyone saw the message. Without the count the
// first delivery is attempt 1, as the specification's DeliveryInfo.Attempt
// says it is.
//
// The [fieldGroup] stamp goes too: the dead-letter stream is read by groups
// of its own, and a copy still stamped for the source's group would be
// skipped by every one of them.
func deadLetterFields(fields []string) []string {
	if !hasField(fields, FieldAttempt) && !hasField(fields, fieldGroup) {
		return fields
	}
	out := make([]string, 0, len(fields))
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == FieldAttempt || fields[i] == fieldGroup {
			continue
		}
		out = append(out, fields[i], fields[i+1])
	}
	return out
}

// fieldGroup stamps a re-added or released entry with the consumer group that
// nacked it.
//
// A topic's stream is shared by every subscription: each one is a consumer
// group over the same key. So the XADD that makes a nacked message
// deliverable again under [ReAdd] or [ZSetMover] adds an entry that EVERY
// group will read, and a subscription that settled the message long ago would
// get it a second time. The stamp names the one group the retry is for, and
// [Consumer] acknowledges and skips an entry stamped for another group rather
// than delivering it.
//
// A stamp rather than a per-group retry stream, because the retry then keeps
// the shard's key, its hash slot, its MaxLen trim and the Lua scripts
// unchanged, and an entry written before the stamp existed carries none and is
// delivered to every group exactly as it was. It is unexported because it is
// not part of the entry layout a producer writes; it is the retry path's
// bookkeeping.
const fieldGroup = FieldPrefixInternal + "g"

// rewriteInternal copies fields, replacing the attempt count when attempt is
// positive and replacing the group stamp with group, or with none when group
// is empty. A re-add or a park passes its own group, which is the stamp that
// keeps the copy away from every other group.
func rewriteInternal(fields []string, attempt uint32, group string) []string {
	out := make([]string, 0, len(fields)+4)
	for i := 0; i+1 < len(fields); i += 2 {
		if replacedField(fields[i], attempt) {
			continue
		}
		out = append(out, fields[i], fields[i+1])
	}
	if attempt > 0 {
		out = append(out, FieldAttempt, strconv.FormatUint(uint64(attempt), 10))
	}
	if group != "" {
		out = append(out, fieldGroup, group)
	}
	return out
}

// replacedField reports whether rewriteInternal writes name itself.
func replacedField(name string, attempt uint32) bool {
	return name == fieldGroup || attempt > 0 && name == FieldAttempt
}

// hasField reports whether a field-value list carries name.
func hasField(fields []string, name string) bool {
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == name {
			return true
		}
	}
	return false
}

// The family's failures.
var (
	// ErrUnknownKind is a nack strategy this package does not have.
	ErrUnknownKind = errcode.New(errcode.InvalidConfig,
		"iqmq/streams: the negative-settlement strategy is not one of the kinds")
	// ErrNoFields is a strategy that has to re-add an entry being given no
	// fields to re-add.
	//
	// It is a refusal rather than an empty entry, because an entry with no
	// fields is a message with no body, no key and no headers: re-adding one
	// would replace the caller's message with an empty one and report
	// success.
	ErrNoFields = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: this strategy re-adds the entry and was given no fields to re-add")
	// ErrDelayUnsupported is a delay the bound strategy cannot arrange.
	//
	// It wraps ErrUnsupported so that the contract holds: a driver that
	// cannot honour a nack's action returns ErrUnsupported and settles
	// nothing.
	ErrDelayUnsupported = errcode.New(errcode.Unsupported,
		"iqmq/streams: this negative-settlement strategy cannot hold a redelivery back for the delay asked for; the zset mover is the kind that can")
)
