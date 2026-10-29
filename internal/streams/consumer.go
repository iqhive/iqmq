package streams

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Consumer is one subscription's consume-and-settle logic: the read, the
// mandatory reclaim sweep, the settlement and the lease extension, with the
// bookkeeping that makes a superseded settlement detectable.
//
// It is here and not in either driver because it is the part that decides
// whether a message is lost. Both drivers wrap it — `drivers/redis` over
// rueidis, `drivers/redis/goredis` over go-redis — so both get the same
// reclaim accounting, the same guarded acknowledgement and the same nack
// strategy, and both are held to [FuzzRedisReclaimer].
//
// It holds no goroutine and does no timing of its own: the driver's source
// calls [Consumer.Read] and [Consumer.Sweep] on its own schedule, with its
// own injected clock.
type Consumer struct {
	cfg       ConsumerConfig
	rec       *Reclaimer
	nack      Strategy
	ackSHA    string
	extendSHA string
	shardKey  map[string]int

	mu      sync.Mutex
	cursors []string
	// held is the shard-hold of the Stop strategy: per shard, every entry
	// whose settlement the shard is waiting for. A shard with an entry here is
	// read from no further, which is what preserves the shard's order
	// across a retry. It is a list because two deliveries of one shard can
	// fail before either is settled, and the shard waits for both.
	held map[int][]ID
	// queued is what came back from behind a held entry: per shard, in
	// identifier order, deliveries a read or a sweep returned while the hold
	// was in force. They stay pending to this consumer, and Read hands them
	// out ahead of anything newer once no held entry is in front of them.
	queued map[int][]Delivery

	// claims counts the reads and sweeps in flight, and closing is set by
	// Close. Each command can make this consumer the owner of pending
	// entries, so Close removes the consumer only when it has seen none in
	// flight after closing, and neither starts once closing is set.
	claims  atomic.Int32
	closing atomic.Bool
}

// ConsumerConfig configures a [Consumer].
type ConsumerConfig struct {
	// Keys is the topic's key naming.
	Keys Keys
	// Group is the consumer group, which is the subscription name.
	Group string
	// Name is this source's consumer name.
	//
	// It MUST be unique to this source instance, and the driver makes it so
	// by appending a nonce. That uniqueness is load-bearing: the guarded
	// acknowledgement tells "somebody else claimed this entry" from "we
	// still hold it" by comparing the pending row's consumer name to this
	// one, and two sources sharing a name would each believe the other's
	// deliveries were their own. [NewConsumer] cannot check uniqueness —
	// nothing local can — so the obligation is stated here and discharged
	// at the one place that can honour it.
	Name string
	// Prefetch is XREADGROUP's COUNT.
	Prefetch int
	// Block is XREADGROUP's BLOCK. Zero polls.
	Block time.Duration
	// AckDeadline is the lease, and therefore the reclaim sweep's min-idle.
	AckDeadline time.Duration
	// ClaimCount bounds how many entries one sweep round claims per shard.
	// Zero means the prefetch.
	ClaimCount int
	// Guard is how strongly an acknowledgement checks ownership.
	Guard AckGuard
	// Nack is the negative-settlement strategy.
	Nack Kind
	// MaxLen trims a stream a re-added entry goes into, when positive.
	MaxLen int64
	// DeadLetterStream is the key a Reject routes to, empty for none.
	DeadLetterStream string
}

// Delivery is one entry handed to the caller, with the bookkeeping a
// settlement of it needs.
type Delivery struct {
	// Entry is the stream entry.
	Entry Entry
	// Key identifies the entry for settlement.
	Key EntryKey
	// Gen is the generation this delivery was handed out under. A settlement
	// carrying an older one is refused.
	Gen uint32
	// Attempt is the delivery attempt, 1 on the first delivery.
	Attempt uint32
	// Superseded is the generation this delivery replaced, or zero. A caller
	// keeping per-delivery state invalidates it.
	Superseded uint32
	// Redelivered reports that this delivery came from the reclaim sweep
	// rather than from a first read.
	Redelivered bool
}

// NewConsumer returns the consume logic for one subscription.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Group == "" || cfg.Name == "" {
		return nil, ErrBadArgs
	}
	if cfg.Keys.Shards < 1 {
		return nil, ErrShardCount
	}
	if cfg.Prefetch <= 0 || cfg.Block < 0 || cfg.AckDeadline <= 0 {
		return nil, ErrBadArgs
	}
	if !cfg.Guard.Valid() || !cfg.Nack.Valid() {
		return nil, ErrBadArgs
	}
	if cfg.ClaimCount <= 0 {
		cfg.ClaimCount = cfg.Prefetch
	}
	strategy, err := NewNack(cfg.Nack, NackConfig{
		Keys:             cfg.Keys,
		Group:            cfg.Group,
		Consumer:         cfg.Name,
		AckDeadline:      cfg.AckDeadline,
		MaxLen:           cfg.MaxLen,
		DeadLetterStream: cfg.DeadLetterStream,
	})
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		cfg:       cfg,
		rec:       NewReclaimer(),
		nack:      strategy,
		ackSHA:    SHAOf(ScriptAckGuard),
		extendSHA: SHAOf(ScriptExtendGuard),
		shardKey:  make(map[string]int, cfg.Keys.Shards),
		cursors:   make([]string, cfg.Keys.Shards),
		held:      map[int][]ID{},
		queued:    map[int][]Delivery{},
	}
	for i := range cfg.Keys.Shards {
		c.shardKey[cfg.Keys.Stream(i)] = i
	}
	return c, nil
}

// Reclaimer returns the bookkeeping, for a caller that needs to ask about a
// delivery outside a settlement.
func (c *Consumer) Reclaimer() *Reclaimer { return c.rec }

// Config returns the resolved configuration, which is what a driver reads to
// report a delivery's lease deadline and to decide whether a dead-letter
// destination exists.
func (c *Consumer) Config() ConsumerConfig { return c.cfg }

// Strategy returns the bound negative-settlement strategy.
func (c *Consumer) Strategy() Strategy { return c.nack }

// shardOf maps a stream key back to a shard.
func (c *Consumer) shardOf(key string) (int, bool) {
	i, ok := c.shardKey[key]
	return i, ok
}

// Read reads new entries and registers each one, appending the deliveries to
// dst.
//
// It reads only the shards that are not held. A held shard is one with a
// delivery nacked under the [Stop] strategy and not settled since, and
// reading past it would defeat the whole point of that strategy. The reply
// is checked against the holds again, because a read waits at the server for
// up to its block and a hold placed meanwhile does not reach it: the server
// answers with what was published during the block, which is behind the
// failed entry. Such an entry is queued rather than delivered, and a later
// Read hands it out, before anything newer from its shard, once no held entry
// is in front of it.
//
// When every shard is held and nothing queued is due, the read sends no
// command at all and returns nothing at once, which the caller treats as an
// empty fetch rather than as an error. It has not waited for the block, so a
// caller that loops on Read waits for a hold to lift before it reads again;
// going straight round spins a core until the sweep brings the held entry
// back.
func (c *Consumer) Read(ctx context.Context, cmder Commander, scratch []Entry, dst []Delivery) ([]Delivery, []Entry, error) {
	if !c.enterClaim() {
		return dst, scratch, mq.ErrClosed
	}
	defer c.claims.Add(-1)
	n := len(dst)
	dst, keys, ids := c.readPlan(dst)
	if len(dst) > n || len(keys) == 0 {
		return dst, scratch, nil
	}
	cmd, err := XReadGroup(XReadGroupArgs{
		Group:    c.cfg.Group,
		Consumer: c.cfg.Name,
		Keys:     keys,
		IDs:      ids,
		Count:    c.cfg.Prefetch,
		Block:    c.cfg.Block,
	})
	if err != nil {
		return dst, scratch, err
	}
	v, err := cmder.Do(ctx, cmd)
	if err != nil {
		return dst, scratch, err
	}
	entries, err := ParseXReadGroup(v, c.shardOf, scratch[:0])
	if err != nil {
		return dst, entries, err
	}
	var foreign map[int][]ID
	for _, e := range entries {
		if c.foreign(e) {
			foreign = addForeign(foreign, e)
			continue
		}
		d, ok := c.register(e, false)
		if !ok || c.queue(d) {
			continue
		}
		dst = append(dst, d)
	}
	c.skipForeign(ctx, cmder, foreign)
	return dst, entries, nil
}

// foreign reports whether e is another group's retry: a copy a [ReAdd] or
// [ZSetMover] nack of that group put back into the shared stream (see
// [fieldGroup]). An entry with no stamp is a publication, and every group
// gets it.
func (c *Consumer) foreign(e Entry) bool {
	g, ok := Internal(e.Fields, fieldGroup)
	return ok && g != c.cfg.Group
}

// addForeign records a foreign entry under its shard.
func addForeign(foreign map[int][]ID, e Entry) map[int][]ID {
	if foreign == nil {
		foreign = map[int][]ID{}
	}
	foreign[e.Shard] = append(foreign[e.Shard], e.ID)
	return foreign
}

// skipForeign acknowledges the foreign entries a read or a sweep was handed,
// so that they leave this group's pending list without being delivered.
//
// A failure is not reported. The entries stay pending to this consumer, the
// reclaim sweep claims them again once they have been idle for the deadline,
// and the sweep skips them the same way; reporting it would fail a read whose
// deliveries are all fine over entries that were never this group's.
func (c *Consumer) skipForeign(ctx context.Context, cmder Commander, foreign map[int][]ID) {
	if len(foreign) == 0 {
		return
	}
	cmds := make([]Cmd, 0, len(foreign))
	for shard, ids := range foreign {
		cmds = append(cmds, XAck(c.cfg.Keys.Stream(shard), c.cfg.Group, ids))
	}
	_, _ = cmder.DoMulti(ctx, cmds)
}

// enterClaim registers a read or sweep, or reports that the consumer is
// closing. The count goes up before closing is read and Close sets closing
// before it reads the count, so one of the two always sees the other.
func (c *Consumer) enterClaim() bool {
	c.claims.Add(1)
	if c.closing.Load() {
		c.claims.Add(-1)
		return false
	}
	return true
}

// readPlan is where a read starts: the queued deliveries no held entry is in
// front of any more, appended to dst, or when there are none, the shard keys
// and positions of a server read, every shard that is not held at ">".
//
// The two are decided under one lock because each depends on the other. A
// shard whose last hold lifted between them would be read from before what it
// queued was handed out, which is the reordering the queue exists to prevent.
func (c *Consumer) readPlan(dst []Delivery) (out []Delivery, keys, ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(dst)
	for shard := range c.cfg.Keys.Shards {
		ds := c.queued[shard]
		due := 0
		for due < len(ds) && !c.heldBeforeLocked(shard, ds[due].Key.ID) {
			due++
		}
		if due == 0 {
			continue
		}
		dst = append(dst, ds[:due]...)
		if due == len(ds) {
			delete(c.queued, shard)
			continue
		}
		c.queued[shard] = slices.Delete(ds, 0, due)
	}
	if len(dst) > n {
		return dst, nil, nil
	}
	for i := range c.cfg.Keys.Shards {
		if len(c.held[i]) > 0 {
			continue
		}
		keys = append(keys, c.cfg.Keys.Stream(i))
		ids = append(ids, IDNew)
	}
	return dst, keys, ids
}

// heldBeforeLocked reports whether the shard is held for an entry ahead of
// id, which is what puts id behind the hold. The held entry itself is not
// behind its own hold: its redelivery is what the hold waits for.
func (c *Consumer) heldBeforeLocked(shard int, id ID) bool {
	for _, h := range c.held[shard] {
		if h.Before(id) {
			return true
		}
	}
	return false
}

// queue puts d aside, and reports that it did, when d is behind an entry its
// shard is held for or behind a delivery already queued there. The second
// half keeps the queue's own order: once the hold lifts, an entry the sweep
// claimed again must not overtake the queued entries ahead of it.
//
// A queued delivery of the same entry is replaced, or dropped when d is
// delivered instead, because d is a newer generation of it.
func (c *Consumer) queue(d Delivery) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	shard := d.Key.Shard
	i, found := c.queuedAtLocked(shard, d.Key.ID)
	due := i == 0 && !c.heldBeforeLocked(shard, d.Key.ID)
	if due {
		if found {
			c.unqueueLocked(shard, i)
		}
		return false
	}
	if found {
		c.queued[shard][i] = d
		return true
	}
	c.queued[shard] = slices.Insert(c.queued[shard], i, d)
	return true
}

// queuedAtLocked is where id is in its shard's queue, or where it would go.
func (c *Consumer) queuedAtLocked(shard int, id ID) (int, bool) {
	return slices.BinarySearchFunc(c.queued[shard], id, func(q Delivery, id ID) int { return q.Key.ID.Compare(id) })
}

// unqueueLocked removes the i-th queued delivery of a shard.
func (c *Consumer) unqueueLocked(shard, i int) {
	ds := slices.Delete(c.queued[shard], i, i+1)
	if len(ds) == 0 {
		delete(c.queued, shard)
		return
	}
	c.queued[shard] = ds
}

// register turns an entry into a delivery, or reports that it must be
// dropped.
//
// The attempt count is the SUM of two halves, and it has to be, because the
// two redelivery routes count differently:
//
//   - The broker counts. An entry left pending and reclaimed has its delivery
//     counter incremented by XAUTOCLAIM, so the server's own number is the
//     attempt.
//   - A strategy that re-adds the entry starts a new pending row, whose
//     counter begins again at one. What is lost is everything before the
//     re-add, so the strategy writes it into the entry ([FieldAttempt]) and
//     this adds it back.
//
// Taking the maximum instead would under-report the mixed case — three
// attempts on a re-added entry that is then reclaimed — and the specification's
// MaxAttempts would take longer to terminate than the caller asked, or on a
// pathological interleaving not terminate at all.
func (c *Consumer) register(e Entry, redelivered bool) (Delivery, bool) {
	key := EntryKey{Shard: e.Shard, ID: e.ID}
	gen, superseded, ok := c.rec.Hand(key)
	if !ok {
		return Delivery{}, false
	}
	server := e.Attempt
	if server == 0 {
		// XREADGROUP does not report a delivery counter, and an entry read
		// with ">" has one by definition: the read is what created the
		// pending row.
		server = 1
	}
	attempt := Attempt(e.Fields) + server
	if redelivered && attempt < 2 {
		// A reclaimed entry has been delivered at least twice by
		// definition: XAUTOCLAIM increments the counter, so a reply that
		// reported nothing is a server that did not tell us, not a first
		// delivery.
		attempt = 2
	}
	return Delivery{
		Entry:       e,
		Key:         key,
		Gen:         gen,
		Attempt:     attempt,
		Superseded:  superseded,
		Redelivered: redelivered,
	}, true
}

// Sweep runs one round of the mandatory reclaim loop and appends whatever it
// reclaimed to dst.
//
// This is the only thing on this broker that redelivers. A Redis consumer
// group hands an entry to a consumer and then forgets about it: no lease
// expires, no timer fires, nothing redelivers. An entry whose holder died is
// pending for ever until something claims it away, so a driver without this
// loop has an at-least-once claim that is simply false.
//
// min-idle is the acknowledgement deadline, which is what makes the loop a
// lease expiry rather than a thief: an entry is claimable exactly when it has
// been idle longer than the lease its holder was given.
//
// A shard that is held by the [Stop] strategy is still swept. The hold stops
// NEW entries, not the redelivery of the entry the hold is waiting for —
// holding that back too would mean the shard never moves again. What the
// sweep claims from behind a held entry is queued, as a read's reply is, so
// that it is redelivered after the entry in front of it rather than beside
// it.
func (c *Consumer) Sweep(ctx context.Context, cmder Commander, scratch []Entry, dst []Delivery) ([]Delivery, []Entry, error) {
	if !c.enterClaim() {
		return dst, scratch, mq.ErrClosed
	}
	defer c.claims.Add(-1)
	c.refreshQueued(ctx, cmder)
	c.mu.Lock()
	cursors := append([]string(nil), c.cursors...)
	c.mu.Unlock()
	minIdle := millis(c.cfg.AckDeadline)
	plan, err := Sweep(c.cfg.Keys, c.cfg.Group, c.cfg.Name, minIdle, cursors, c.cfg.ClaimCount)
	if err != nil {
		return dst, scratch, err
	}
	// Each shard contributes two commands in one pipeline: the extended
	// XPENDING that reports the delivery counters, then the XAUTOCLAIM that
	// claims the entries.
	//
	// The XPENDING is not optional bookkeeping. XAUTOCLAIM returns the
	// entries and NOT their delivery counters, so a driver without it has no
	// idea how many times a reclaimed message has been delivered — and
	// the API's Attempt is what MaxAttempts counts, so a
	// reclaim loop that reported "2" for every redelivery would never
	// dead-letter a message that always fails. It costs no extra round trip:
	// the two commands are pipelined, and the counter XPENDING reports is
	// the pre-claim one, so the attempt is that plus the claim's own
	// increment.
	cmds := make([]Cmd, 0, 2*len(plan.Cmds))
	for i, claimCmd := range plan.Cmds {
		pend, perr := XPending(XPendingArgs{
			Key:   c.cfg.Keys.Stream(plan.Shards[i]),
			Group: c.cfg.Group,
			Start: cursorOf(cursors, plan.Shards[i]),
			End:   IDMax,
			Idle:  c.cfg.AckDeadline,
			Count: c.cfg.ClaimCount,
		})
		if perr != nil {
			return dst, scratch, perr
		}
		cmds = append(cmds, pend, claimCmd)
	}
	// After the claims, so that an entry this round claimed back reads as
	// ours: see heldStillOurs.
	held := c.heldKeys()
	for _, k := range held {
		check, perr := XPending(XPendingArgs{
			Key:   c.cfg.Keys.Stream(k.Shard),
			Group: c.cfg.Group,
			Start: k.ID.String(),
			End:   k.ID.String(),
			Count: 1,
		})
		if perr != nil {
			return dst, scratch, perr
		}
		cmds = append(cmds, check)
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil {
		return dst, scratch, err
	}
	if len(results) != len(cmds) {
		return dst, scratch, ErrReplyShape
	}
	entries := scratch[:0]
	var (
		first   error
		foreign map[int][]ID
	)
	for i := range plan.Cmds {
		shard := plan.Shards[i]
		pendRes, claimRes := results[2*i], results[2*i+1]
		if claimRes.Err != nil {
			if first == nil {
				first = claimRes.Err
			}
			continue
		}
		// A failed XPENDING costs the attempt counts and nothing else, so
		// the sweep goes on without them rather than abandoning the round:
		// a redelivery that reports a low attempt is a lesser fault than a
		// redelivery that does not happen.
		counts := map[ID]uint32{}
		if pendRes.Err == nil {
			if rows, rerr := ParseXPending(pendRes.Value); rerr == nil {
				for _, row := range rows {
					counts[row.ID] = row.Delivered
				}
			}
		}
		before := len(entries)
		claim, perr := ParseXAutoClaim(claimRes.Value, shard, false, entries)
		if perr != nil {
			if first == nil {
				first = perr
			}
			continue
		}
		entries = claim.Entries
		c.setCursor(shard, claim.Cursor)
		for j := before; j < len(entries); j++ {
			if n, ok := counts[entries[j].ID]; ok {
				// The claim incremented the counter XPENDING reported.
				entries[j].Attempt = n + 1
			}
			if c.foreign(entries[j]) {
				foreign = addForeign(foreign, entries[j])
				continue
			}
			d, ok := c.register(entries[j], true)
			if !ok || c.queue(d) {
				continue
			}
			dst = append(dst, d)
		}
		// An entry XAUTOCLAIM dropped because the stream entry is gone is
		// an entry nothing can deliver again. Forgetting it here keeps the
		// in-flight count honest, and releasing it keeps a shard the Stop
		// strategy held for it from waiting for ever; reporting it is the
		// caller's, through the health surface.
		for _, id := range claim.Deleted {
			k := EntryKey{Shard: shard, ID: id}
			c.rec.Lost(k)
			c.release(k)
		}
	}
	for n, k := range held {
		res := results[2*len(plan.Cmds)+n]
		if res.Err != nil || c.heldStillOurs(res.Value, k) {
			continue
		}
		c.release(k)
	}
	c.skipForeign(ctx, cmder, foreign)
	return dst, entries, first
}

// refreshQueued restarts the idle clocks of the queued deliveries before the
// sweep claims anything.
//
// A queued entry is pending to this consumer and idle while it waits, so the
// sweep would otherwise claim it once it had waited a deadline, and every
// claim increments its delivery counter: an entry held back behind a message
// that keeps failing would reach its handler counted as several attempts,
// and MaxAttempts would dead-letter it early. Another consumer's sweep would
// take it as well. The extension leaves the counter alone. An entry it
// reports lost is another consumer's now and leaves the queue; any other
// failure keeps it, and if the claim takes it the claim queues it again.
func (c *Consumer) refreshQueued(ctx context.Context, cmder Commander) {
	c.mu.Lock()
	var ds []Delivery
	for _, q := range c.queued {
		ds = append(ds, q...)
	}
	c.mu.Unlock()
	if len(ds) == 0 {
		return
	}
	for i, err := range c.Extend(ctx, cmder, ds) {
		if errors.Is(err, mq.ErrLeaseLost) {
			c.unqueue(ds[i].Key)
		}
	}
}

// unqueue removes an entry's queued delivery, if it has one.
func (c *Consumer) unqueue(k EntryKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i, found := c.queuedAtLocked(k.Shard, k.ID); found {
		c.unqueueLocked(k.Shard, i)
	}
}

// heldKeys is the entries the [Stop] strategy's holds are waiting for.
func (c *Consumer) heldKeys() []EntryKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.held) == 0 {
		return nil
	}
	var keys []EntryKey
	for shard, ids := range c.held {
		for _, id := range ids {
			keys = append(keys, EntryKey{Shard: shard, ID: id})
		}
	}
	return keys
}

// heldStillOurs reads a held entry's XPENDING row and reports whether the
// entry is still pending to this consumer, which is the only state in which
// the hold can end the way it was meant to: by this consumer settling the
// entry once the sweep has brought it back.
//
// Every other answer is a hold nothing would ever lift. Another consumer's
// sweep may have claimed the entry and settled it, or the nack may have come
// after the lease had already moved; either way this consumer never sees the
// entry again, and the shard would stop for good. An unreadable reply keeps
// the hold, because the next sweep asks again.
func (c *Consumer) heldStillOurs(v Value, k EntryKey) bool {
	rows, err := ParseXPending(v)
	if err != nil {
		return true
	}
	return len(rows) == 1 && rows[0].ID == k.ID && rows[0].Consumer == c.cfg.Name
}

// cursorOf is a shard's stored sweep cursor, or the beginning of the stream.
func cursorOf(cursors []string, shard int) string {
	if shard >= 0 && shard < len(cursors) && cursors[shard] != "" {
		return cursors[shard]
	}
	return IDMin
}

// setCursor records where a shard's next sweep resumes.
//
// A cursor of zero means the scan wrapped, which is the sweep having been
// round the whole pending list once.
func (c *Consumer) setCursor(shard int, cursor ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if shard < 0 || shard >= len(c.cursors) {
		return
	}
	if cursor.IsZero() {
		c.cursors[shard] = IDZero
		return
	}
	c.cursors[shard] = cursor.String()
}

// Ack settles a batch of deliveries positively.
//
// The result is index-aligned with ds, which is the SPI's batch
// contract: a result aligned with anything else reports one delivery's
// outcome for another. An entry whose generation has been superseded is
// refused without a command being sent, and one the server says another
// consumer holds is refused with the same answer — [mq.ErrLeaseLost] — which
// is the whole mechanism [Reclaimer] exists for.
//
// Deliveries are grouped by shard because XACK and the guard script both take
// one key: a batch spanning three shards is three commands in one pipeline,
// not three round trips.
func (c *Consumer) Ack(ctx context.Context, cmder Commander, ds []Delivery) []error {
	errs := make([]error, len(ds))
	type group struct {
		idx []int
		ids []ID
	}
	byShard := map[int]*group{}
	for i := range ds {
		if err := c.rec.Begin(ds[i].Key, ds[i].Gen); err != nil {
			errs[i] = err
			continue
		}
		g := byShard[ds[i].Key.Shard]
		if g == nil {
			g = &group{}
			byShard[ds[i].Key.Shard] = g
		}
		g.idx = append(g.idx, i)
		g.ids = append(g.ids, ds[i].Key.ID)
	}
	if len(byShard) == 0 {
		return errs
	}
	shards := make([]int, 0, len(byShard))
	cmds := make([]Cmd, 0, len(byShard))
	for shard, g := range byShard {
		shards = append(shards, shard)
		cmds = append(cmds, c.ackCmd(shard, g.ids))
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil {
		// Nothing is known about any of them. The specification makes an unknown outcome
		// sticky, so every delivery in the batch reports it and the
		// settlement mark is released so the caller may try again.
		for shard := range byShard {
			for _, i := range byShard[shard].idx {
				errs[i] = err
				c.rec.Abandon(ds[i].Key, ds[i].Gen)
			}
		}
		return errs
	}
	for n, shard := range shards {
		g := byShard[shard]
		var res Result
		if n < len(results) {
			res = results[n]
		} else {
			res = Result{Err: ErrReplyShape}
		}
		c.applyAck(ds, g.idx, g.ids, res, errs)
	}
	return errs
}

// ackCmd is the acknowledgement of one shard's identifiers, guarded or plain.
func (c *Consumer) ackCmd(shard int, ids []ID) Cmd {
	key := c.cfg.Keys.Stream(shard)
	if c.cfg.Guard == GuardNone {
		return XAck(key, c.cfg.Group, ids)
	}
	args := make([]string, 0, 2+len(ids))
	args = append(args, c.cfg.Group, c.cfg.Name)
	for _, id := range ids {
		args = append(args, id.String())
	}
	return EvalSHA(c.ackSHA, []string{key}, args)
}

// applyAck records one shard's acknowledgement outcome.
func (c *Consumer) applyAck(ds []Delivery, idx []int, ids []ID, res Result, errs []error) {
	if res.Err != nil {
		for _, i := range idx {
			errs[i] = res.Err
			c.rec.Abandon(ds[i].Key, ds[i].Gen)
		}
		return
	}
	lost := map[ID]struct{}{}
	if c.cfg.Guard != GuardNone {
		_, lostIDs, err := ParseAckGuardReply(res.Value)
		if err != nil {
			for _, i := range idx {
				errs[i] = err
				c.rec.Abandon(ds[i].Key, ds[i].Gen)
			}
			return
		}
		for _, id := range lostIDs {
			lost[id] = struct{}{}
		}
	}
	for n, i := range idx {
		if _, gone := lost[ids[n]]; gone {
			errs[i] = ErrGuardRefused
			c.rec.Lost(ds[i].Key)
			c.release(ds[i].Key)
			continue
		}
		c.rec.Settled(ds[i].Key)
		c.release(ds[i].Key)
	}
}

// Nack settles one delivery negatively through the bound strategy.
//
// The strategy decides the commands; this decides what happens to the
// bookkeeping afterwards, and the two are separate so that the strategy stays
// a pure function the bake-off can measure.
func (c *Consumer) Nack(ctx context.Context, cmder Commander, d Delivery, o mq.NackOptions, now time.Time) error {
	if err := c.rec.Begin(d.Key, d.Gen); err != nil {
		return err
	}
	plan, err := c.nack.Plan(NackRequest{
		Entry:   d.Entry,
		Options: o,
		Now:     now,
		Attempt: d.Attempt,
	})
	if err != nil {
		// A refusal settles nothing, so the delivery stays settleable.
		c.rec.Abandon(d.Key, d.Gen)
		return err
	}
	if plan.Apart {
		return c.deadLetterApart(ctx, cmder, d, plan)
	}
	if len(plan.Cmds) > 0 {
		results, derr := cmder.DoMulti(ctx, plan.Cmds)
		if derr != nil {
			c.rec.Abandon(d.Key, d.Gen)
			return derr
		}
		for _, res := range results {
			if res.Err != nil {
				c.rec.Abandon(d.Key, d.Gen)
				return res.Err
			}
		}
		if guardErr := c.checkPlanReplies(plan, results); guardErr != nil {
			c.rec.Lost(d.Key)
			c.release(d.Key)
			return guardErr
		}
	}
	if plan.HoldShard {
		c.hold(d.Key)
	}
	if plan.Settled {
		c.rec.Settled(d.Key)
		c.release(d.Key)
	} else {
		// The entry stays pending and the reclaim sweep will bring it back.
		// Forgetting it here is what makes the sweep's redelivery a NEW
		// generation rather than a second delivery under the old one.
		c.rec.Settled(d.Key)
	}
	return nil
}

// deadLetterApart sends the cross-slot dead-letter move one command at a time.
//
// The acknowledgement is sent only once the XADD has succeeded, because an
// acknowledgement beside a failed XADD destroys the only copy. A guard that
// then refuses the acknowledgement means another consumer owns the entry and
// will settle it itself, so the copy just added is deleted again; if that
// XDEL fails too the message is dead-lettered twice, which is the duplicate
// over the loss.
func (c *Consumer) deadLetterApart(ctx context.Context, cmder Commander, d Delivery, plan NackPlan) error {
	v, err := cmder.Do(ctx, plan.Cmds[0])
	if err != nil {
		c.rec.Abandon(d.Key, d.Gen)
		return err
	}
	moved, err := ParseXAdd(v)
	if err != nil {
		c.rec.Abandon(d.Key, d.Gen)
		return err
	}
	v, err = cmder.Do(ctx, plan.Cmds[1])
	if err != nil {
		c.rec.Abandon(d.Key, d.Gen)
		return err
	}
	if guardErr := c.checkPlanReplies(NackPlan{Cmds: plan.Cmds[1:]}, []Result{{Value: v}}); guardErr != nil {
		if errors.Is(guardErr, mq.ErrLeaseLost) {
			_, _ = cmder.Do(ctx, XDel(c.cfg.DeadLetterStream, []ID{moved}))
		}
		c.rec.Lost(d.Key)
		c.release(d.Key)
		return guardErr
	}
	c.rec.Settled(d.Key)
	c.release(d.Key)
	return nil
}

// checkPlanReplies reads a strategy's replies for the guard's refusal.
//
// Every write script answers "not yours" when the pending entry is held by
// another consumer, and that is a refusal rather than a success: a
// dead-letter move that moved nothing and reported done would drop the
// message.
//
// The reply shapes are dispatched on the script's own digest and not
// guessed from the command, because they are genuinely different: the
// acknowledgement guard answers {acked, {lost}} because it settles a batch,
// the extension guard a delayed pending nack sends answers {{extended},
// {lost}}, and the three write scripts answer an identifier or false because
// each settles one entry. Parsing one as the other reports a reply-shape failure
// for a settlement that worked, which is how a working drop came to look
// broken (the first thing TestModel found).
func (c *Consumer) checkPlanReplies(plan NackPlan, results []Result) error {
	if len(results) != len(plan.Cmds) {
		// A reply missing is a guard nobody read, which is not a
		// settlement that happened.
		return ErrReplyShape
	}
	for i, res := range results {
		cmd := plan.Cmds[i]
		if cmd.Name() != "EVALSHA" && cmd.Name() != "EVAL" || len(cmd.Tokens) < 2 {
			continue
		}
		sha := cmd.Tokens[1]
		if cmd.Name() == "EVAL" {
			sha = SHA(sha)
		}
		if sha == c.ackSHA {
			_, lost, err := ParseAckGuardReply(res.Value)
			if err != nil {
				return err
			}
			if len(lost) > 0 {
				return ErrGuardRefused
			}
			continue
		}
		if sha == c.extendSHA {
			// A delayed nack's idle-clock restart (base.holdBack). An entry
			// it did not extend is one another consumer holds, so the nack
			// settled nothing that was still ours.
			extended, _, err := ParseExtendGuardReply(res.Value)
			if err != nil {
				return err
			}
			if len(extended) == 0 {
				return ErrGuardRefused
			}
			continue
		}
		if _, err := ParseNewID(res.Value); err != nil {
			// Either the guard refused (ErrGuardRefused, which wraps
			// ErrLeaseLost) or the reply is not readable. Neither is a
			// settlement that happened.
			if errors.Is(err, mq.ErrLeaseLost) {
				return err
			}
			return err
		}
	}
	return nil
}

// hold stops a shard until the named entry is settled, on top of any other
// entry the shard is already waiting for.
func (c *Consumer) hold(k EntryKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.Contains(c.held[k.Shard], k.ID) {
		c.held[k.Shard] = append(c.held[k.Shard], k.ID)
	}
}

// release ends the named entry's part of its shard's hold. The shard stays
// held while another of its failed entries is unsettled.
func (c *Consumer) release(k EntryKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.held[k.Shard]
	i := slices.Index(ids, k.ID)
	if i < 0 {
		return
	}
	if len(ids) == 1 {
		delete(c.held, k.Shard)
		return
	}
	c.held[k.Shard] = slices.Delete(ids, i, i+1)
}

// Held reports how many shards are currently held by the Stop strategy.
func (c *Consumer) Held() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.held)
}

// Extend extends the leases of a batch of deliveries.
//
// It is XCLAIM ... JUSTID, which resets the entries' idle clocks and does NOT
// increment their delivery counters. The counter matters: a plain XCLAIM
// increments it, so extending a lease twenty times would make the delivery
// look like the twentieth attempt and trip MaxAttempts on a handler
// that was merely slow.
//
// An identifier the claim did not return is one that is no longer pending, so
// the lease really is gone and the caller is told so.
func (c *Consumer) Extend(ctx context.Context, cmder Commander, ds []Delivery) []error {
	errs := make([]error, len(ds))
	byShard := map[int][]int{}
	for i := range ds {
		if err := c.rec.Check(ds[i].Key, ds[i].Gen); err != nil {
			errs[i] = err
			continue
		}
		byShard[ds[i].Key.Shard] = append(byShard[ds[i].Key.Shard], i)
	}
	for shard, idx := range byShard {
		ids := make([]ID, 0, len(idx))
		for _, i := range idx {
			ids = append(ids, ds[i].Key.ID)
		}
		cmd, err := c.extendCmd(shard, ids)
		if err != nil {
			for _, i := range idx {
				errs[i] = err
			}
			continue
		}
		v, err := cmder.Do(ctx, cmd)
		if err != nil {
			for _, i := range idx {
				errs[i] = err
			}
			continue
		}
		got, perr := c.parseExtend(v)
		if perr != nil {
			for _, i := range idx {
				errs[i] = perr
			}
			continue
		}
		claimed := make(map[ID]struct{}, len(got))
		for _, id := range got {
			claimed[id] = struct{}{}
		}
		for _, i := range idx {
			if _, ok := claimed[ds[i].Key.ID]; !ok {
				errs[i] = ErrGuardRefused
				c.rec.Lost(ds[i].Key)
				c.release(ds[i].Key)
			}
		}
	}
	return errs
}

// extendCmd is the lease extension of one shard's identifiers.
//
// Under the owner guard it is a script, because XCLAIM is a claim: a bare
// `XCLAIM key group me 0 <id> JUSTID` takes the entry from whoever holds it,
// which turns an extension into a theft. Under GuardNone
// it is that bare XCLAIM, and the theft window is the documented cost of the
// setting.
func (c *Consumer) extendCmd(shard int, ids []ID) (Cmd, error) {
	key := c.cfg.Keys.Stream(shard)
	if c.cfg.Guard == GuardNone {
		return XClaimJustID(key, c.cfg.Group, c.cfg.Name, ids)
	}
	args := make([]string, 0, 2+len(ids))
	args = append(args, c.cfg.Group, c.cfg.Name)
	for _, id := range ids {
		args = append(args, id.String())
	}
	return EvalSHA(c.extendSHA, []string{key}, args), nil
}

// parseExtend reads whichever reply extendCmd asked for.
func (c *Consumer) parseExtend(v Value) ([]ID, error) {
	if c.cfg.Guard == GuardNone {
		return ParseJustIDs(v)
	}
	extended, _, err := ParseExtendGuardReply(v)
	return extended, err
}

// Close abandons every entry this consumer still holds and removes the
// consumer when it holds none.
//
// Abandoning is the only hand-back this broker offers. An unsettled entry
// stays in the pending-entries list and becomes claimable once it has been
// idle for the acknowledgement deadline, so the reclaim loop of this or
// another consumer picks it up — which is the specification's "requeues or abandons".
//
// The consumer itself is removed ONLY when its pending list is empty, and the
// reason is not tidiness: XGROUP DELCONSUMER deletes a consumer's pending
// entries along with the consumer, so calling it on a consumer that still
// holds entries deletes those messages from the group for ever. That is
// message loss performed by the shutdown path, and the emptiness check is
// what prevents it.
//
// Close also stops further reads and sweeps, and leaves the consumer in place
// when one is still in flight.
//
// The check is the server's pending list and not only the local bookkeeping.
// The bookkeeping forgets entries that are still pending on purpose (a nack
// that leaves the entry for the sweep, an entry whose reply could not be
// parsed, one that could not be decoded), so a consumer that held nothing by
// its own count could still own pending entries, and DELCONSUMER would have
// deleted them.
func (c *Consumer) Close(ctx context.Context, cmder Commander) error {
	c.closing.Store(true)
	held := c.rec.Forget()
	if held > 0 {
		// Entries are left pending deliberately; removing the consumer now
		// would delete them.
		return nil
	}
	if c.claims.Load() > 0 {
		// A read or sweep in flight may yet make this consumer the owner of
		// entries the pending list does not show, so the row is left.
		return nil
	}
	if !c.ownsNothing(ctx, cmder) {
		return nil
	}
	cmds := make([]Cmd, 0, c.cfg.Keys.Shards)
	for i := range c.cfg.Keys.Shards {
		cmds = append(cmds, XGroupDelConsumer(c.cfg.Keys.Stream(i), c.cfg.Group, c.cfg.Name))
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil {
		// A consumer left behind is a row in XINFO CONSUMERS and nothing
		// more; failing a Close over it would be worse than leaving it.
		return nil
	}
	for _, res := range results {
		if res.Err != nil {
			continue
		}
		if n, ok := res.Value.Int64(); ok && n > 0 {
			// DELCONSUMER removed pending entries, which means the
			// emptiness check above raced something. Say so rather than
			// swallow it: those messages are gone.
			return ErrConsumerHadPending
		}
	}
	return nil
}

// ownsNothing reports whether the server's pending list holds no entry for
// this consumer on any shard. Anything it cannot read counts as an entry,
// because the cost of a wrong yes is deleted messages and the cost of a wrong
// no is a row in XINFO CONSUMERS.
func (c *Consumer) ownsNothing(ctx context.Context, cmder Commander) bool {
	cmds := make([]Cmd, 0, c.cfg.Keys.Shards)
	for i := range c.cfg.Keys.Shards {
		cmd, err := XPending(XPendingArgs{
			Key:      c.cfg.Keys.Stream(i),
			Group:    c.cfg.Group,
			Consumer: c.cfg.Name,
			Count:    1,
		})
		if err != nil {
			return false
		}
		cmds = append(cmds, cmd)
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil || len(results) != len(cmds) {
		return false
	}
	for _, res := range results {
		if res.Err != nil {
			return false
		}
		rows, perr := ParseXPending(res.Value)
		if perr != nil || len(rows) > 0 {
			return false
		}
	}
	return true
}

// Provision creates the consumer group for every shard.
//
// mkstream decides whether a shard that does not exist is created, which is
// the API's rule: nothing creates topology behind an operator's
// back unless the configuration says it may.
func (c *Consumer) Provision(ctx context.Context, cmder Commander, start string, mkstream bool) error {
	if start == "" {
		start = IDLast
	}
	cmds := make([]Cmd, 0, c.cfg.Keys.Shards)
	for i := range c.cfg.Keys.Shards {
		cmds = append(cmds, XGroupCreate(c.cfg.Keys.Stream(i), c.cfg.Group, start, mkstream))
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil {
		return err
	}
	for _, res := range results {
		if res.Err != nil && !IsBusyGroup(res.Err) {
			return res.Err
		}
	}
	return nil
}

// IsBusyGroup reports whether an error is Redis's BUSYGROUP: the consumer
// group already exists.
//
// It is not a failure. Creating a subscription is idempotent, and
// two consumers starting at once both try, so exactly one of them sees this.
func IsBusyGroup(err error) bool { return err != nil && hasPrefix(err.Error(), "BUSYGROUP") }

// IsNoGroup reports whether an error is Redis's NOGROUP: the key or the
// consumer group does not exist.
//
// A source that meets it has had its group destroyed underneath it, which is
// an operator action rather than a transient failure, so the driver reports
// it as a definite refusal and not as something to retry.
func IsNoGroup(err error) bool { return err != nil && hasPrefix(err.Error(), "NOGROUP") }

// IsNoScript reports whether an error is Redis's NOSCRIPT: the script cache
// does not hold this script.
//
// It is the one error the driver recovers from by itself: a server that
// restarted, or a SCRIPT FLUSH, empties the cache, and the answer is to send
// the script body once and retry. A driver that did not would stop being able
// to acknowledge anything after a failover.
func IsNoScript(err error) bool { return err != nil && hasPrefix(err.Error(), "NOSCRIPT") }

// IsCrossSlot reports whether an error is Redis Cluster's CROSSSLOT.
func IsCrossSlot(err error) bool { return err != nil && hasPrefix(err.Error(), "CROSSSLOT") }

// hasPrefix is strings.HasPrefix, spelled out so that this file's only
// dependency on the error's text is in one place: Redis reports these
// conditions as error-reply prefixes and there is no typed alternative.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// ErrConsumerHadPending is Close having deleted pending entries.
//
// It is a driver defect if it is ever seen: Close checks that the pending
// list is empty before it removes the consumer, precisely because
// DELCONSUMER deletes what the consumer held.
var ErrConsumerHadPending = newErr(
	"iqmq/streams: XGROUP DELCONSUMER removed pending entries, so messages this consumer held were deleted from the group",
)
