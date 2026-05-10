package streams

import (
	"strconv"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// This file builds the commands the drivers send and reads the replies they
// get back. Nothing here does I/O: a builder returns a [Cmd] and a parser
// takes a [Value], so every command and every reply shape is testable without
// a server and every one of them is exercised by the package's own fuzz
// targets.
//
// The reply shapes are Redis's, and two of them have a RESP2 and a RESP3
// spelling. A [Commander] adapter normalises the difference: a map reply
// arrives here as a [KindArray] of alternating key and value, which is
// RESP2's shape and the one that preserves the order an entry's fields were
// written in. A layer that turned an entry's fields into a Go map would lose
// exactly the order [Decode]'s header indices depend on.

// Entry is one stream entry as a reply carries it.
//
// Fields is the flat name/value list, borrowed from the reply for as long as
// the caller holds the entry; [Decode] copies out of it. Shard is filled by
// the parser that knew which key the entry came from and is -1 when no key
// was in the reply.
type Entry struct {
	// Fields is the entry's field-value list: name, value, name, value.
	Fields []string
	// ID is the entry's identifier.
	ID ID
	// Shard is the topic shard the entry came from, or -1 when unknown.
	Shard int
	// Attempt is the delivery attempt the reply reported, and zero when the
	// reply did not report one. XREADGROUP never does — a "new entries"
	// read is attempt 1 by construction — and XPENDING always does.
	Attempt uint32
}

// XAddArgs are XADD's parameters.
type XAddArgs struct {
	// Key is the stream key.
	Key string
	// ID is the identifier to assign, [IDAuto] for the server's own.
	ID string
	// Fields is the flat field-value list, as [Encode] produces.
	Fields []string
	// FieldBlobs are Fields' blob tokens, as [EncodeAliased] produces, with
	// At an index into Fields. [XAdd] shifts each one onto the command's own
	// token positions.
	//
	// Fields and FieldBlobs have to come from the SAME encode call: a blob
	// whose At names a token the encoder filled in, or a field list whose
	// empty body slot has no blob, is an entry with an empty body and no
	// error anywhere. [Cmd.Valid] catches the first and [XAdd] cannot catch
	// the second, so the pairing is the caller's (see `drivers/redis`'s
	// sink, which is the only caller that passes either).
	FieldBlobs []Blob
	// MaxLen trims the stream to approximately this many entries when
	// positive (MAXLEN ~ n). Zero leaves the stream untrimmed.
	//
	// The '~' is not a shortcut: exact trimming walks the whole radix tree
	// and an approximate one stops at a node boundary, so an exact MAXLEN on
	// a hot stream is a latency spike on the publish path. Trimming is
	// explicit in this driver (the retention section says so), and when it
	// is asked for it is the approximate form.
	MaxLen int64
	// NoMkStream refuses to create the stream. XADD creates a stream that
	// does not exist, which is the API's hidden topology
	// creation; a driver whose AutoCreate is off passes true and gets a null
	// reply instead of a new key.
	NoMkStream bool
}

// XAdd builds XADD.
//
// The field list is COPIED into the command's own token slice, so a caller may
// reuse the scratch it encoded into; a blob's At is shifted by the length of
// the prefix this function put in front of the fields, so it names the same
// token in the command that it named in Fields.
func XAdd(a XAddArgs) Cmd {
	t := make([]string, 0, 8+len(a.Fields))
	t = append(t, "XADD", a.Key)
	keyStart := 1
	if a.NoMkStream {
		t = append(t, "NOMKSTREAM")
	}
	if a.MaxLen > 0 {
		t = append(t, "MAXLEN", "~", strconv.FormatInt(a.MaxLen, 10))
	}
	id := a.ID
	if id == "" {
		id = IDAuto
	}
	t = append(t, id)
	shift := len(t)
	t = append(t, a.Fields...)
	var blobs []Blob
	if len(a.FieldBlobs) != 0 {
		blobs = make([]Blob, len(a.FieldBlobs))
		for i, b := range a.FieldBlobs {
			blobs[i] = Blob{At: b.At + shift, Bytes: b.Bytes}
		}
	}
	return Cmd{Tokens: t, KeyStart: keyStart, KeyCount: 1, Blobs: blobs}
}

// ParseXAdd reads XADD's reply: the identifier the server assigned.
//
// A null reply is NOMKSTREAM refusing to create the stream, which is
// [ErrNoStream] rather than a parse failure: it is the answer a driver with
// auto-creation off asked for.
func ParseXAdd(v Value) (ID, error) {
	if v.IsNil() {
		return ID{}, ErrNoStream
	}
	s, ok := v.Str()
	if !ok {
		return ID{}, ErrReplyShape
	}
	return ParseID(s)
}

// CheckXAdd reads XADD's reply for a caller that does not want the identifier:
// it returns exactly the error [ParseXAdd] returns, and no ID.
//
// It is [ParseXAdd] with the number-building removed and NOT a weaker check.
// Each of the three answers is the same one for the same reply: a null is
// [ErrNoStream], which is how NOMKSTREAM's refusal is detected and the reason
// this cannot be an "ignore the reply" fast path; a reply that is not a string
// is [ErrReplyShape]; and a string that is not an identifier is [ErrBadID],
// because a server, a proxy or a script that answered XADD with something else
// has corrupted the publication, and reporting that as a success is worse than
// the nanoseconds saved. FuzzCheckXAddMatchesParseXAdd is the claim.
//
// Why it exists: the batch publish path checks one reply per message and
// discards every identifier, so a batch of a hundred paid two
// strconv.ParseUint calls a hundred times over for a number nobody reads,
// while the native client that batch is measured against calls its reply's
// Error() and stops.
func CheckXAdd(v Value) error {
	if v.IsNil() {
		return ErrNoStream
	}
	s, ok := v.Str()
	if !ok {
		return ErrReplyShape
	}
	if !ValidID(s) {
		return ErrBadID
	}
	return nil
}

// XGroupCreate builds XGROUP CREATE, with MKSTREAM when mkstream is true.
//
// start is where a group with no committed position begins: [IDLast] for the
// entries added from now on, [IDZero] to replay everything the stream still
// holds, or an identifier.
func XGroupCreate(key, group, start string, mkstream bool) Cmd {
	t := []string{"XGROUP", "CREATE", key, group, start}
	if mkstream {
		t = append(t, "MKSTREAM")
	}
	return Cmd{Tokens: t, KeyStart: 2, KeyCount: 1}
}

// XGroupSetID builds XGROUP SETID, which is this driver's replay: it moves a
// consumer group's last-delivered identifier, so the group's next read
// delivers from there.
//
// It is the whole of the message contract's seek on this broker, and its two
// limits are stated on the driver page rather than hidden: it moves the
// GROUP, so every consumer of that subscription is moved together and a
// single consumer cannot seek on its own; and it does not touch the pending
// entries list, so entries already handed out stay pending and are still
// redelivered by the reclaim loop.
func XGroupSetID(key, group, id string) Cmd {
	return Cmd{Tokens: []string{"XGROUP", "SETID", key, group, id}, KeyStart: 2, KeyCount: 1}
}

// XGroupDelConsumer builds XGROUP DELCONSUMER.
//
// It is DESTRUCTIVE and the driver calls it on exactly one condition: the
// consumer's pending-entries list is empty. DELCONSUMER removes the
// consumer's pending entries along with the consumer, so a call on a consumer
// that still holds entries deletes them from the PEL — which means those
// messages are never redelivered, by anybody. That is message loss performed
// by the tidy-up path, which is why the emptiness is checked and why this
// comment exists.
func XGroupDelConsumer(key, group, consumer string) Cmd {
	return Cmd{Tokens: []string{"XGROUP", "DELCONSUMER", key, group, consumer}, KeyStart: 2, KeyCount: 1}
}

// XGroupDestroy builds XGROUP DESTROY.
func XGroupDestroy(key, group string) Cmd {
	return Cmd{Tokens: []string{"XGROUP", "DESTROY", key, group}, KeyStart: 2, KeyCount: 1}
}

// XReadGroupArgs are XREADGROUP's parameters.
type XReadGroupArgs struct {
	// Group is the consumer group.
	Group string
	// Consumer is this source's consumer name.
	Consumer string
	// Keys are the stream keys to read, in shard order.
	Keys []string
	// IDs are the positions, one per key: [IDNew] for entries never
	// delivered to the group, or an identifier to re-read this consumer's
	// own pending entries from.
	IDs []string
	// Count bounds how many entries per key the server returns. It is the
	// prefetch, and it is always positive: the core resolves the default
	// before opening a source.
	Count int
	// Block is how long the server parks when nothing is available. Zero
	// means do not block at all, which is a poll; a negative value is
	// refused rather than sent, because BLOCK 0 in Redis means "for ever"
	// and a fetch that could never be woken is exactly what the specification forbids.
	Block time.Duration
	// NoAck asks the server not to add the entries to the pending-entries
	// list. It is at-most-once delivery and this driver never sets it: the
	// PEL is where a redelivery comes from, so a NOACK read is a read whose
	// messages cannot be recovered from a crash.
	NoAck bool
}

// XReadGroup builds XREADGROUP.
//
// The command is marked [Cmd.Blocking] when it blocks, which a client library
// has to know: rueidis routes a blocking command to a separate connection
// pool, and a BLOCK on a shared pipelined connection stalls every command
// queued behind it for the whole block.
func XReadGroup(a XReadGroupArgs) (Cmd, error) {
	if a.Group == "" || a.Consumer == "" {
		return Cmd{}, ErrBadArgs
	}
	if len(a.Keys) == 0 || len(a.Keys) != len(a.IDs) {
		return Cmd{}, ErrBadArgs
	}
	if a.Count <= 0 || a.Block < 0 {
		return Cmd{}, ErrBadArgs
	}
	t := make([]string, 0, 10+2*len(a.Keys))
	t = append(t, "XREADGROUP", "GROUP", a.Group, a.Consumer, "COUNT", strconv.Itoa(a.Count))
	if a.Block > 0 {
		t = append(t, "BLOCK", strconv.FormatInt(millis(a.Block), 10))
	}
	if a.NoAck {
		t = append(t, "NOACK")
	}
	t = append(t, "STREAMS")
	keyStart := len(t)
	t = append(t, a.Keys...)
	t = append(t, a.IDs...)
	return Cmd{Tokens: t, KeyStart: keyStart, KeyCount: len(a.Keys), Blocking: a.Block > 0}, nil
}

// millis rounds a duration up to whole milliseconds, with a floor of one.
//
// Rounding UP matters for a lease. BLOCK and IDLE are millisecond arguments,
// and a 500-microsecond deadline rounded down to zero milliseconds means
// "block for ever" to XREADGROUP and "claim everything" to XAUTOCLAIM. Both
// are the opposite of what the caller asked for, so a sub-millisecond
// positive duration becomes one millisecond.
func millis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		ms++
	}
	return int64(ms)
}

// ParseXReadGroup appends XREADGROUP's entries to dst and returns it.
//
// shardOf maps a stream key back to a shard number; it may be nil, in which
// case every entry's Shard is -1. A null reply is the block expiring with
// nothing available, which is not an error: dst comes back unchanged.
//
// Every entry the reply carried is an entry the server has added to this
// consumer's pending-entries list. A parser that dropped one — because its
// fields were malformed, say — would drop a message that is pending at the
// broker and that nothing would then settle, so a malformed entry is an
// error for the whole reply rather than a silent skip, and the driver's
// answer to it is the poison-message route rather than a shrug.
func ParseXReadGroup(v Value, shardOf func(key string) (int, bool), dst []Entry) ([]Entry, error) {
	if v.IsNil() {
		return dst, nil
	}
	top, ok := v.Arr()
	if !ok {
		return dst, ErrReplyShape
	}
	// RESP2 gives [[key, entries], ...]; a RESP3 map is normalised by the
	// adapter to [key, entries, key, entries, ...]. Both are handled,
	// because which one arrives depends on the client library's protocol
	// negotiation rather than on anything this package chose.
	if isFlatPairs(top) {
		for i := 0; i+1 < len(top); i += 2 {
			var err error
			if dst, err = parseKeyEntries(top[i], top[i+1], shardOf, dst); err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
	for _, streamVal := range top {
		pair, ok := streamVal.Arr()
		if !ok || len(pair) != 2 {
			return dst, ErrReplyShape
		}
		var err error
		if dst, err = parseKeyEntries(pair[0], pair[1], shardOf, dst); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// isFlatPairs reports whether a stream reply is the flattened map form.
//
// The two forms are told apart by the first element: the nested form's is an
// array (key and entries), the flattened form's is the key itself, a string.
func isFlatPairs(top []Value) bool {
	return len(top) >= 2 && len(top)%2 == 0 && top[0].Kind() == KindString
}

// parseKeyEntries appends one stream's entries.
func parseKeyEntries(keyVal, entriesVal Value, shardOf func(string) (int, bool), dst []Entry) ([]Entry, error) {
	key, ok := keyVal.Str()
	if !ok {
		return dst, ErrReplyShape
	}
	shard := -1
	if shardOf != nil {
		if s, known := shardOf(key); known {
			shard = s
		}
	}
	if entriesVal.IsNil() {
		return dst, nil
	}
	return parseEntries(entriesVal, shard, dst)
}

// parseEntries appends the [[id, [f, v, ...]], ...] shape.
func parseEntries(v Value, shard int, dst []Entry) ([]Entry, error) {
	items, ok := v.Arr()
	if !ok {
		return dst, ErrReplyShape
	}
	for _, item := range items {
		if item.IsNil() {
			// An XCLAIM of an entry that has been deleted from the stream
			// reports a null in the entry's place. It is not an entry and
			// it is not corruption.
			continue
		}
		pair, ok := item.Arr()
		if !ok || len(pair) != 2 {
			return dst, ErrReplyShape
		}
		ids, ok := pair[0].Str()
		if !ok {
			return dst, ErrReplyShape
		}
		id, err := ParseID(ids)
		if err != nil {
			return dst, err
		}
		fields, ok := stringArr(pair[1])
		if !ok {
			return dst, ErrReplyShape
		}
		dst = append(dst, Entry{ID: id, Shard: shard, Fields: fields})
	}
	return dst, nil
}

// stringArr reads an array of strings, refusing anything else. A null array
// reads as an empty field list, which is what an entry whose fields were all
// deleted looks like.
func stringArr(v Value) ([]string, bool) {
	if v.IsNil() {
		return nil, true
	}
	items, ok := v.Arr()
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.Str()
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// XAck builds XACK for one key's identifiers.
//
// XACK does NOT check who owns the pending entry: it removes the entry from
// the group's pending-entries list whoever is holding it. That is the whole
// reason [Reclaimer] and [AckGuard] exist — see reclaim.go — and the reason
// this builder is not the only route a settlement takes.
func XAck(key, group string, ids []ID) Cmd {
	t := make([]string, 0, 3+len(ids))
	t = append(t, "XACK", key, group)
	for _, id := range ids {
		t = append(t, id.String())
	}
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}
}

// ParseCount reads an integer reply, which is what XACK, XLEN, XDEL and XTRIM
// return.
func ParseCount(v Value) (int64, error) {
	n, ok := v.Int64()
	if !ok {
		return 0, ErrReplyShape
	}
	return n, nil
}

// XAutoClaimArgs are XAUTOCLAIM's parameters.
type XAutoClaimArgs struct {
	// Key is the stream key.
	Key string
	// Group is the consumer group.
	Group string
	// Consumer is who the entries are claimed FOR.
	Consumer string
	// MinIdle is how long an entry must have been idle to be claimed. It is
	// the acknowledgement deadline: an entry idle for longer than the lease
	// its holder was given is an entry whose holder has had its time.
	MinIdle time.Duration
	// Start is where the scan resumes: [IDZero] for the beginning, or the
	// cursor a previous reply returned.
	Start string
	// Count bounds how many entries are claimed in one call.
	Count int
	// JustID claims without returning the entries' fields, and — the part
	// that matters — WITHOUT incrementing their delivery counters. It is the
	// lease extension: re-claiming an entry for its current holder resets
	// its idle time, which is the only thing on this broker that makes a
	// long handler's delivery survive its deadline, and doing it with JUSTID
	// is what keeps the extension from looking like a redelivery to the specification's
	// attempt accounting.
	JustID bool
}

// XAutoClaim builds XAUTOCLAIM.
func XAutoClaim(a XAutoClaimArgs) (Cmd, error) {
	if a.Key == "" || a.Group == "" || a.Consumer == "" || a.Count <= 0 || a.MinIdle < 0 {
		return Cmd{}, ErrBadArgs
	}
	start := a.Start
	if start == "" {
		start = IDZero
	}
	t := []string{
		"XAUTOCLAIM", a.Key, a.Group, a.Consumer,
		strconv.FormatInt(millis(a.MinIdle), 10), start,
		"COUNT", strconv.Itoa(a.Count),
	}
	if a.JustID {
		t = append(t, "JUSTID")
	}
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}, nil
}

// ClaimResult is XAUTOCLAIM's reply.
type ClaimResult struct {
	// Cursor is where the next scan resumes. It is [IDZero] when the scan
	// wrapped, which is how the sweep knows it has been round once.
	Cursor ID
	// Entries are the entries claimed, empty under JUSTID.
	Entries []Entry
	// JustIDs are the identifiers claimed under JUSTID.
	JustIDs []ID
	// Deleted are the identifiers XAUTOCLAIM dropped from the pending list
	// because the entries no longer exist in the stream. Redis 7.0 added
	// this third element; on 6.2 it is absent and the slice is nil.
	//
	// It is not a curiosity. An entry trimmed out of the stream while it was
	// pending is a message that can never be delivered again, and before 7.0
	// it stayed in the PEL for ever, which is the PEL growth the specification asks the bake-off to measure.
	Deleted []ID
}

// ParseXAutoClaim reads XAUTOCLAIM's reply, appending entries to dst.
func ParseXAutoClaim(v Value, shard int, justID bool, dst []Entry) (ClaimResult, error) {
	top, ok := v.Arr()
	if !ok || len(top) < 2 {
		return ClaimResult{}, ErrReplyShape
	}
	cs, ok := top[0].Str()
	if !ok {
		return ClaimResult{}, ErrReplyShape
	}
	cursor, err := ParseID(cs)
	if err != nil {
		return ClaimResult{}, err
	}
	res := ClaimResult{Cursor: cursor}
	if justID {
		if res.JustIDs, err = parseIDs(top[1]); err != nil {
			return ClaimResult{}, err
		}
	} else if res.Entries, err = parseEntries(top[1], shard, dst); err != nil {
		return ClaimResult{}, err
	}
	if len(top) >= 3 && !top[2].IsNil() {
		if res.Deleted, err = parseIDs(top[2]); err != nil {
			return ClaimResult{}, err
		}
	}
	return res, nil
}

// parseIDs reads an array of identifiers.
func parseIDs(v Value) ([]ID, error) {
	if v.IsNil() {
		return nil, nil
	}
	items, ok := v.Arr()
	if !ok {
		return nil, ErrReplyShape
	}
	out := make([]ID, 0, len(items))
	for _, item := range items {
		s, ok := item.Str()
		if !ok {
			return nil, ErrReplyShape
		}
		id, err := ParseID(s)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// XClaimJustID builds XCLAIM ... JUSTID for a known set of identifiers.
//
// This is the lease extension of the SPI's DeadlineExtender. XCLAIM
// with JUSTID resets an entry's idle clock and does NOT increment its
// delivery counter, so extending a lease twenty times does not make the
// delivery look like the twentieth attempt — which it would with a plain
// XCLAIM, and which would then trip MaxAttempts and dead-letter a
// message whose handler was merely slow.
//
// minIdle is 0: the entries are ours already and the extension must not be
// conditional on how long we have held them.
func XClaimJustID(key, group, consumer string, ids []ID) (Cmd, error) {
	if key == "" || group == "" || consumer == "" || len(ids) == 0 {
		return Cmd{}, ErrBadArgs
	}
	t := make([]string, 0, 6+len(ids))
	t = append(t, "XCLAIM", key, group, consumer, "0")
	for _, id := range ids {
		t = append(t, id.String())
	}
	t = append(t, "JUSTID")
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}, nil
}

// ParseJustIDs reads XCLAIM JUSTID's reply: the identifiers that were still
// pending and are now claimed.
//
// An identifier that is NOT in the reply is an identifier the claim did not
// get — the entry was acknowledged or deleted while we held it — which for a
// lease extension means the lease is gone.
func ParseJustIDs(v Value) ([]ID, error) { return parseIDs(v) }

// XPendingArgs are the extended XPENDING's parameters.
type XPendingArgs struct {
	// Key is the stream key.
	Key string
	// Group is the consumer group.
	Group string
	// Start and End bound the identifier range, [IDMin] and [IDMax] for all
	// of it.
	Start, End string
	// Consumer filters to one consumer's entries, empty for every one.
	Consumer string
	// Idle filters to entries idle for at least this long.
	Idle time.Duration
	// Count bounds the reply.
	Count int
}

// XPending builds the extended form of XPENDING.
func XPending(a XPendingArgs) (Cmd, error) {
	if a.Key == "" || a.Group == "" || a.Count <= 0 {
		return Cmd{}, ErrBadArgs
	}
	start, end := a.Start, a.End
	if start == "" {
		start = IDMin
	}
	if end == "" {
		end = IDMax
	}
	t := make([]string, 0, 10)
	t = append(t, "XPENDING", a.Key, a.Group)
	if a.Idle > 0 {
		t = append(t, "IDLE", strconv.FormatInt(millis(a.Idle), 10))
	}
	t = append(t, start, end, strconv.Itoa(a.Count))
	if a.Consumer != "" {
		t = append(t, a.Consumer)
	}
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}, nil
}

// Pending is one pending-entries-list row.
type Pending struct {
	// Consumer is who holds the entry.
	Consumer string
	// ID is the entry's identifier.
	ID ID
	// Idle is how long since it was delivered or claimed.
	Idle time.Duration
	// Delivered is the entry's delivery counter.
	Delivered uint32
}

// ParseXPending reads the extended XPENDING's reply.
func ParseXPending(v Value) ([]Pending, error) {
	if v.IsNil() {
		return nil, nil
	}
	rows, ok := v.Arr()
	if !ok {
		return nil, ErrReplyShape
	}
	out := make([]Pending, 0, len(rows))
	for _, row := range rows {
		cols, ok := row.Arr()
		if !ok || len(cols) != 4 {
			return nil, ErrReplyShape
		}
		ids, ok := cols[0].Str()
		if !ok {
			return nil, ErrReplyShape
		}
		id, err := ParseID(ids)
		if err != nil {
			return nil, err
		}
		consumer, ok := cols[1].Str()
		if !ok {
			return nil, ErrReplyShape
		}
		idleMs, ok := cols[2].Int64()
		if !ok {
			return nil, ErrReplyShape
		}
		delivered, ok := cols[3].Int64()
		if !ok || delivered < 0 {
			return nil, ErrReplyShape
		}
		if idleMs < 0 {
			idleMs = 0
		}
		out = append(out, Pending{
			ID:        id,
			Consumer:  consumer,
			Idle:      time.Duration(idleMs) * time.Millisecond,
			Delivered: clampCount(delivered),
		})
	}
	return out, nil
}

// clampCount narrows a delivery counter to the uint32 the API's
// Attempt is, saturating rather than wrapping: an attempt count that wrapped
// to 1 after four billion redeliveries would reset MaxAttempts
// accounting, which is the one thing a saturating count cannot do.
func clampCount(n int64) uint32 {
	if n > int64(^uint32(0)) {
		return ^uint32(0)
	}
	if n < 0 {
		return 0
	}
	return uint32(n)
}

// XLen builds XLEN.
func XLen(key string) Cmd {
	return Cmd{Tokens: []string{"XLEN", key}, KeyStart: 1, KeyCount: 1}
}

// XTrimMaxLen builds XTRIM ... MAXLEN, exact when exact is true.
//
// Trimming is explicit in this driver: nothing trims a stream unless the
// configuration asked for it. A stream is a log, and a driver that trimmed by
// default would delete a subscription's unread messages to save memory
// nobody said was short.
func XTrimMaxLen(key string, maxLen int64, exact bool) Cmd {
	t := []string{"XTRIM", key, "MAXLEN"}
	if !exact {
		t = append(t, "~")
	}
	t = append(t, strconv.FormatInt(maxLen, 10))
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}
}

// XDel builds XDEL.
//
// It removes entries from the stream and NOT from any group's pending list,
// so it is never a settlement. The driver uses it for one thing: the
// dead-letter route, after the Lua script has copied the entry to its
// destination.
func XDel(key string, ids []ID) Cmd {
	t := make([]string, 0, 2+len(ids))
	t = append(t, "XDEL", key)
	for _, id := range ids {
		t = append(t, id.String())
	}
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}
}

// XRange builds XRANGE over a bounded range.
func XRange(key, start, end string, count int) Cmd {
	t := []string{"XRANGE", key, start, end}
	if count > 0 {
		t = append(t, "COUNT", strconv.Itoa(count))
	}
	return Cmd{Tokens: t, KeyStart: 1, KeyCount: 1}
}

// ParseXRange reads XRANGE's reply, appending entries to dst.
func ParseXRange(v Value, shard int, dst []Entry) ([]Entry, error) {
	return parseEntries(v, shard, dst)
}

// WaitAOF builds WAITAOF, which is this driver's conditional durable
// confirmation boundary (the message contract's ConfirmDurable).
//
// XADD's reply is the memory-durable confirmation: the entry is in the
// server's data structure and will be served to a reader, and nothing more
// than that is true. WAITAOF is the stronger boundary and it is CONDITIONAL,
// which means its condition has to be stated rather than implied — it returns
// the number of local and replica append-only files the write has reached, and
// it can only reach one if the server was started with appendonly yes. On a
// server with the AOF off it fails outright, which is the honest answer and
// the reason [ConfirmDurable] is refused at open on such a server rather than
// at the first publication.
//
// numLocal is 1 or 0, numReplicas is how many replicas must have fsynced, and
// timeout bounds the wait. Redis 7.0 added the command; a 6.2 server has no
// durable boundary to define at all.
func WaitAOF(numLocal, numReplicas int, timeout time.Duration) (Cmd, error) {
	if numLocal < 0 || numReplicas < 0 || timeout < 0 {
		return Cmd{}, ErrBadArgs
	}
	return Cmd{Tokens: []string{
		"WAITAOF",
		strconv.Itoa(numLocal),
		strconv.Itoa(numReplicas),
		strconv.FormatInt(millis(timeout), 10),
	}}, nil
}

// AOFAck is WAITAOF's reply: how many local and replica AOFs the write
// reached.
type AOFAck struct {
	// Local is 1 when the write is in the local append-only file.
	Local int64
	// Replicas is how many replicas have it in theirs.
	Replicas int64
}

// ParseWaitAOF reads WAITAOF's reply.
func ParseWaitAOF(v Value) (AOFAck, error) {
	pair, ok := v.Arr()
	if !ok || len(pair) != 2 {
		return AOFAck{}, ErrReplyShape
	}
	local, ok := pair[0].Int64()
	if !ok {
		return AOFAck{}, ErrReplyShape
	}
	replicas, ok := pair[1].Int64()
	if !ok {
		return AOFAck{}, ErrReplyShape
	}
	return AOFAck{Local: local, Replicas: replicas}, nil
}

// Reached reports whether the write reached the boundary the configuration
// asked for.
func (a AOFAck) Reached(numLocal, numReplicas int) bool {
	return a.Local >= int64(numLocal) && a.Replicas >= int64(numReplicas)
}

// Eval builds EVAL for a script.
//
// The keys are declared, which is what lets a cluster route the script and
// what [SlotSafe] checks before the call: a script whose keys are on two
// slots is refused by a cluster with CROSSSLOT, and refusing it here names
// the keys instead.
func Eval(script string, keys, args []string) Cmd {
	t := make([]string, 0, 3+len(keys)+len(args))
	t = append(t, "EVAL", script, strconv.Itoa(len(keys)))
	keyStart := len(t)
	t = append(t, keys...)
	t = append(t, args...)
	return Cmd{Tokens: t, KeyStart: keyStart, KeyCount: len(keys)}
}

// EvalSHA builds EVALSHA, the cached form of [Eval].
func EvalSHA(sha string, keys, args []string) Cmd {
	t := make([]string, 0, 3+len(keys)+len(args))
	t = append(t, "EVALSHA", sha, strconv.Itoa(len(keys)))
	keyStart := len(t)
	t = append(t, keys...)
	t = append(t, args...)
	return Cmd{Tokens: t, KeyStart: keyStart, KeyCount: len(keys)}
}

// ScriptLoad builds SCRIPT LOAD.
func ScriptLoad(script string) Cmd {
	return Cmd{Tokens: []string{"SCRIPT", "LOAD", script}}
}

// ZAdd builds ZADD for one member.
func ZAdd(key string, score int64, member string) Cmd {
	return Cmd{
		Tokens:   []string{"ZADD", key, strconv.FormatInt(score, 10), member},
		KeyStart: 1, KeyCount: 1,
	}
}

// ZCard builds ZCARD.
func ZCard(key string) Cmd {
	return Cmd{Tokens: []string{"ZCARD", key}, KeyStart: 1, KeyCount: 1}
}

// Ping builds PING.
func Ping() Cmd { return Cmd{Tokens: []string{"PING"}} }

// InfoServer builds INFO server, which is where the version comes from.
func InfoServer() Cmd { return Cmd{Tokens: []string{"INFO", "server"}} }

// Exists builds EXISTS for one key.
func Exists(key string) Cmd {
	return Cmd{Tokens: []string{"EXISTS", key}, KeyStart: 1, KeyCount: 1}
}

// The command layer's failures.
var (
	// ErrReplyShape is a reply that is not the shape the command's
	// documentation promises.
	ErrReplyShape = newErr("iqmq/streams: the server's reply is not the shape this command returns")
	// ErrBadArgs is a command this package refused to build.
	ErrBadArgs = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: the command's arguments are not usable")
	// ErrNoStream is NOMKSTREAM refusing to create a stream that does not
	// exist.
	ErrNoStream = errcode.New(errcode.NotFound,
		"iqmq/streams: the stream does not exist and auto-creation is off")
)

// newErr builds a corrupt-reply sentinel. The code is CorruptMessage because
// a reply this driver cannot read is the same class of failure as an entry it
// cannot decode: something on the other side is not what this driver was
// written against.
func newErr(msg string) error { return errcode.New(errcode.CorruptMessage, msg) }
