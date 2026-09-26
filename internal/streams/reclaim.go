package streams

import (
	"strconv"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// EntryKey names one pending entry within a subscription: which shard it is
// in and which identifier it has.
type EntryKey struct {
	// ID is the entry's identifier.
	ID ID
	// Shard is the topic shard the entry is in.
	Shard int
}

// Reclaimer is the pending-entries bookkeeping of one source: which entries
// this source has handed out, under which generation, and which of them a
// reclaim has superseded.
//
// # Why it exists
//
// the driver documentation's settlement contract and the generation rule together
// require that a settlement of a delivery that has been superseded reports
// ErrLeaseLost rather than being applied. On most brokers the broker enforces
// that itself: a Kafka generation, an SQS receipt handle, a JetStream ack
// token all stop being valid when the delivery is redelivered.
//
// Redis enforces nothing. `XACK key group id` removes the entry from the
// group's pending-entries list WHOEVER is holding it and WHATEVER its
// delivery count is — there is no token, no receipt, no ownership check. So
// the sequence
//
//	consumer A reads entry E          (E pending, owner A)
//	A's handler is slow
//	consumer B runs XAUTOCLAIM        (E pending, owner B)
//	B starts processing E
//	A finishes and sends XACK E       (E leaves the PEL)
//	B crashes before finishing
//
// loses the message: A acknowledged work B was doing, B never finished it,
// and nothing will ever deliver E again. The specification calls that a disqualifier, and
// [FuzzRedisReclaimer] exists for it.
//
// # What closes it
//
// Two halves, because the two cases have different evidence available.
//
// Within one source, the reclaimer is the evidence. Every entry handed out —
// by a fetch or by the reclaim loop — is registered here with a generation,
// and handing out an entry that is already registered SUPERSEDES the previous
// generation. A settlement carrying the superseded generation is refused
// before any command is sent. This is exact and costs no round trip.
//
// Across processes, the server's own pending-entries row is the evidence: it
// records which consumer holds the entry. The driver gives every source a
// consumer name that is unique to that source instance, so "the PEL row names
// a different consumer" is exactly "somebody else claimed it", and the
// guarded acknowledgement ([ScriptAckGuard]) checks that inside a Lua script,
// atomically with the XACK. That costs one script call instead of one XACK,
// which is why it is configurable — but it is the DEFAULT, because the
// alternative is the loss above.
//
// A Reclaimer is safe for concurrent use: a settlement and a fetch run at the
// same time by construction (the specification allows it), and so does the reclaim loop.
type Reclaimer struct {
	mu   sync.Mutex
	live map[EntryKey]entryLease
	gen  uint32
}

// entryLease is what this source knows about one entry it holds.
type entryLease struct {
	// gen is the generation the current delivery was handed out under.
	gen uint32
	// settling reports that a terminal settlement for this delivery is in
	// flight. A reclaim must not hand the entry out again while it is: the
	// settlement is about to remove it, and delivering it a third time
	// would be a duplicate this driver manufactured.
	settling bool
}

// NewReclaimer returns an empty reclaimer.
func NewReclaimer() *Reclaimer {
	return &Reclaimer{live: make(map[EntryKey]entryLease), gen: 1}
}

// Hand registers an entry this source is about to deliver and returns the
// generation to put in the delivery.
//
// superseded is the generation of a delivery this one replaces, and ok is
// false when the entry must NOT be delivered at all — which happens for
// exactly one reason: a terminal settlement for it is already in flight, so
// the entry is on its way out of the pending list and a delivery now would be
// a duplicate. A caller that reads ok=false drops the entry.
//
// The caller invalidates the superseded generation in whatever it keeps
// per-delivery state in (a driverkit.TokenTable, say); this type's own
// [Reclaimer.Check] already refuses it.
func (r *Reclaimer) Hand(k EntryKey) (gen, superseded uint32, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, live := r.live[k]
	if live && prev.settling {
		return 0, 0, false
	}
	r.gen++
	if r.gen == 0 {
		// Generation zero is reserved: a DriverDelivery whose Gen is zero
		// is one no reclaimer handed out, and a settlement of it must not
		// match a live lease by accident.
		r.gen = 1
	}
	r.live[k] = entryLease{gen: r.gen}
	if live {
		return r.gen, prev.gen, true
	}
	return r.gen, 0, true
}

// Check reports whether the delivery of k under gen is still the current one.
//
// It returns [mq.ErrLeaseLost] for a generation that has been superseded and
// for an entry this source does not hold at all. Those are the same answer on
// purpose: an entry the reclaimer has forgotten is one that was settled or
// that Close abandoned, and in both cases a settlement of it must not reach
// the server.
func (r *Reclaimer) Check(k EntryKey, gen uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.checkLocked(k, gen)
}

func (r *Reclaimer) checkLocked(k EntryKey, gen uint32) error {
	lease, live := r.live[k]
	if !live || lease.gen != gen {
		return mq.ErrLeaseLost
	}
	return nil
}

// Begin claims the right to settle the delivery of k under gen.
//
// It is [Reclaimer.Check] plus a mark, and the mark is what makes the check
// mean something: between the check and the command reaching the server, the
// reclaim loop could otherwise hand the same entry out again, and the
// settlement would then apply to a delivery that was already superseded. With
// the mark, [Reclaimer.Hand] refuses the entry while the settlement is in
// flight.
//
// Every path out of a settlement calls exactly one of [Reclaimer.Settled] and
// [Reclaimer.Abandon]. A path that called neither would leave the entry
// marked for ever, and the reclaim loop would stop redelivering it.
func (r *Reclaimer) Begin(k EntryKey, gen uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(k, gen); err != nil {
		return err
	}
	lease := r.live[k]
	if lease.settling {
		return mq.ErrSettlementInProgress
	}
	lease.settling = true
	r.live[k] = lease
	return nil
}

// Settled forgets the entry: its settlement succeeded, so nothing may settle
// it again and the reclaim loop must not redeliver it.
func (r *Reclaimer) Settled(k EntryKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.live, k)
}

// Abandon releases the settlement mark without forgetting the entry: the
// settlement failed definitely, so the specification leaves the delivery settleable and the
// caller may try again.
func (r *Reclaimer) Abandon(k EntryKey, gen uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, live := r.live[k]; live && lease.gen == gen {
		lease.settling = false
		r.live[k] = lease
	}
}

// Lost forgets the entry because the server said somebody else holds it.
//
// It is what a guarded acknowledgement's refusal produces: the entry is not
// ours any more, so this source must neither settle it nor extend it, and the
// reclaim loop must not count it as one of ours.
func (r *Reclaimer) Lost(k EntryKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.live, k)
}

// Live returns how many entries this source holds unsettled.
//
// It is the driver's in-flight count, and it is what Close reports on and
// what the health surface publishes: an in-flight count that never falls is
// the visible shape of a reclaim loop that has stopped.
func (r *Reclaimer) Live() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}

// Held appends every entry this source holds to dst, in no particular order.
//
// Close reads it to answer the specification's "requeues or abandons everything it has
// handed out that was never settled". On this broker the answer is abandon in
// place: an unsettled entry stays in the pending-entries list, and the reclaim
// loop of this or another consumer picks it up once it has been idle for the
// acknowledgement deadline. There is no "unclaim" command, and the one thing
// that would look like a prompt hand-back — XGROUP DELCONSUMER — deletes the
// entries along with the consumer, which is loss.
func (r *Reclaimer) Held(dst []EntryKey) []EntryKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.live {
		dst = append(dst, k)
	}
	return dst
}

// Forget drops every entry: the source is closing.
func (r *Reclaimer) Forget() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.live)
	r.live = make(map[EntryKey]entryLease)
	return n
}

// AckGuard is how strongly an acknowledgement checks that the delivery it
// settles is still the one this source was handed.
type AckGuard uint8

// The acknowledgement guards.
const (
	// GuardOwner checks the pending entry's owner inside a Lua script and
	// acknowledges only when it is still this consumer. It is the default:
	// it is the only setting under which a cross-process reclaim cannot make
	// this source acknowledge work another consumer is doing, which is the
	// loss [Reclaimer] documents.
	GuardOwner AckGuard = iota
	// GuardNone sends a plain XACK. One command instead of one script call,
	// and the loss window above is open: it is correct only when nothing
	// else reclaims from the same group, which in practice means a single
	// consumer process. The driver page states the trade rather than
	// implying the setting is free.
	GuardNone

	ackGuardCount
)

var ackGuardNames = [ackGuardCount]string{"owner", "none"}

// String returns the guard's lower-case name.
func (g AckGuard) String() string {
	if g < ackGuardCount {
		return ackGuardNames[g]
	}
	return "AckGuard(?)"
}

// Valid reports whether g names a guard.
func (g AckGuard) Valid() bool { return g < ackGuardCount }

// ParseAckGuard returns the guard with the given name.
func ParseAckGuard(name string) (AckGuard, bool) {
	for i, s := range ackGuardNames {
		if s == name {
			return AckGuard(i), true
		}
	}
	return 0, false
}

// AckGuardNames lists the guards' names in declaration order, for a DSN
// parameter's enumeration.
func AckGuardNames() []string { return ackGuardNames[:] }

// SweepPlan is one round of the mandatory pending-entries reclaim loop.
//
// The loop is mandatory on this broker and not an optimisation: a Redis
// consumer group hands an entry to a consumer and then does nothing about it
// for ever. Nothing expires a lease, nothing redelivers on its own, and a
// consumer that died holding a hundred entries holds them until something
// claims them away. XAUTOCLAIM with min-idle equal to the acknowledgement
// deadline is that something, and a driver without it is a driver whose
// at-least-once claim is false.
type SweepPlan struct {
	// Cmds are the commands of this round, one XAUTOCLAIM per shard.
	Cmds []Cmd
	// Shards are the shard numbers, index-aligned with Cmds, so a reply can
	// be attributed to the shard it came from.
	Shards []int
}

// Sweep builds one round of the reclaim loop: an XAUTOCLAIM per shard,
// resuming each shard from its own cursor.
//
// cursors is per-shard and is updated by the caller from each reply's cursor;
// a nil or short slice starts every shard at the beginning. count bounds how
// many entries one round claims, which is what keeps a sweep of a
// pending-entries list with a million entries in it from being one command
// that blocks the server.
func Sweep(k Keys, group, consumer string, minIdleMillis int64, cursors []string, count int) (SweepPlan, error) {
	if group == "" || consumer == "" || count <= 0 || minIdleMillis < 0 {
		return SweepPlan{}, ErrBadArgs
	}
	plan := SweepPlan{
		Cmds:   make([]Cmd, 0, k.Shards),
		Shards: make([]int, 0, k.Shards),
	}
	for i := range k.Shards {
		start := IDZero
		if i < len(cursors) && cursors[i] != "" {
			start = cursors[i]
		}
		cmd, err := xAutoClaimMillis(k.Stream(i), group, consumer, minIdleMillis, start, count)
		if err != nil {
			return SweepPlan{}, err
		}
		plan.Cmds = append(plan.Cmds, cmd)
		plan.Shards = append(plan.Shards, i)
	}
	return plan, nil
}

// xAutoClaimMillis builds XAUTOCLAIM from an already-rounded millisecond
// idle bound, which is what the sweep has: the deadline is rounded once, by
// the caller, so every shard of one round claims on the same bound.
func xAutoClaimMillis(key, group, consumer string, minIdleMillis int64, start string, count int) (Cmd, error) {
	if key == "" || group == "" || consumer == "" || count <= 0 || minIdleMillis < 0 {
		return Cmd{}, ErrBadArgs
	}
	return Cmd{
		Tokens: []string{
			"XAUTOCLAIM", key, group, consumer,
			strconv.FormatInt(minIdleMillis, 10), start,
			"COUNT", strconv.Itoa(count),
		},
		KeyStart: 1, KeyCount: 1,
	}, nil
}

// ErrGuardRefused is the guarded acknowledgement reporting that the pending
// entry is owned by another consumer.
//
// It wraps [mq.ErrLeaseLost] so that a caller's errors.Is finds the specification's
// answer, and says what happened so that an operator reading a log does not
// have to guess why an acknowledgement was refused.
var ErrGuardRefused = errcode.New(errcode.LeaseLost,
	"iqmq/streams: the pending entry is held by another consumer, so acknowledging it would settle that consumer's delivery (the memory engine)")
