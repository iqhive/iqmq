package streams

import (
	"crypto/sha1" //nolint:gosec // SCRIPT LOAD's digest is SHA-1 by Redis's definition, not a security choice
	"encoding/hex"
	"strconv"
	"strings"
)

// The Lua scripts.
//
// Every one of them exists for atomicity and for nothing else. Redis runs a
// script to completion with nothing interleaved, so a script is the only way
// this driver can do two things to the pending-entries list as one thing —
// and each of these routes is a place where doing them as two would lose or
// duplicate a message:
//
//   - an acknowledgement has to check the entry's owner and remove it in one
//     step, or a reclaim between the two makes the check meaningless;
//   - a dead-letter move has to copy the entry and acknowledge it in one
//     step, or a crash between them either loses the message (acknowledged,
//     never copied) or duplicates it (copied, never acknowledged);
//   - a delayed redelivery has to park the entry and acknowledge it in one
//     step, for the same reason;
//   - a re-add has to add the new entry and acknowledge the old one in one
//     step, for the same reason;
//   - releasing a parked entry has to remove it from the sorted set and add
//     it to the stream in one step, or two movers release it twice.
//
// Two constraints shape how they are written. They take their keys as KEYS,
// which is what lets a cluster route them — and which is why every key a
// script touches has to share a hash slot, checked by [SlotSafe] before the
// call rather than discovered as a CROSSSLOT inside somebody else's script.
// And they never call a command whose result depends on the server's clock or
// on randomness, because Redis refuses to replicate such a script: every time
// a script needs "now", the caller passes it in.
const (
	// ScriptAckGuard acknowledges only the entries this consumer still holds.
	//
	// KEYS[1] is the stream; ARGV[1] the group, ARGV[2] this consumer's name,
	// and ARGV[3..] the identifiers. It returns {acked, {lost identifiers}}.
	//
	// The ownership test is the extended XPENDING for one identifier, whose
	// second column is the consumer holding the entry. An identifier with no
	// pending row is reported as lost too: it was acknowledged or the entry
	// was deleted, and either way this delivery is no longer settleable.
	ScriptAckGuard = `
local g = ARGV[1]
local me = ARGV[2]
local acked = 0
local lost = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local p = redis.call('XPENDING', KEYS[1], g, id, id, 1)
  if p[1] and p[1][2] == me then
    acked = acked + redis.call('XACK', KEYS[1], g, id)
  else
    lost[#lost + 1] = id
  end
end
return {acked, lost}
`

	// ScriptExtendGuard extends the leases of only the entries this consumer
	// still holds.
	//
	// KEYS[1] is the stream; ARGV[1] the group, ARGV[2] this consumer's name,
	// and ARGV[3..] the identifiers. It returns {{extended}, {lost}}.
	//
	// It exists because XCLAIM is a CLAIM. `XCLAIM key group me 0 <id>
	// JUSTID` takes the entry from whoever holds it — min-idle 0 means "no
	// matter how recently it was delivered" — so a lease extension written as
	// a bare XCLAIM steals an entry another consumer is processing and
	// reports the extension as successful. [FuzzRedisReclaimer] found that in
	// 1.3 seconds : consumer A read an entry, consumer B's
	// sweep claimed it away, and A's Extend took it back, after which two
	// consumers each believed the delivery was theirs.
	//
	// The ownership test is the same extended XPENDING the acknowledgement
	// guard uses, and it has to be in the same script as the claim: checking
	// it in the driver and claiming afterwards leaves exactly the window the
	// check was for.
	ScriptExtendGuard = `
local g = ARGV[1]
local me = ARGV[2]
local ok = {}
local lost = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local p = redis.call('XPENDING', KEYS[1], g, id, id, 1)
  if p[1] and p[1][2] == me then
    redis.call('XCLAIM', KEYS[1], g, me, 0, id, 'JUSTID')
    ok[#ok + 1] = id
  else
    lost[#lost + 1] = id
  end
end
return {ok, lost}
`

	// ScriptDeadLetter copies one entry to the dead-letter stream and
	// acknowledges it in the source, atomically.
	//
	// KEYS[1] is the source stream, KEYS[2] the dead-letter stream; ARGV[1]
	// the group, ARGV[2] this consumer, ARGV[3] the identifier, ARGV[4] the
	// dead-letter stream's MAXLEN (0 for none), and ARGV[5..] the entry's
	// field-value list. It returns the new identifier, or false when the
	// entry is not ours.
	//
	// The fields come from the caller rather than from an XRANGE inside the
	// script, which is deliberate: the caller already decoded the entry, and
	// re-reading it here would let the script write a different entry from
	// the one whose delivery is being dead-lettered — the stream could have
	// been trimmed, or the identifier reused after an XDEL.
	//
	// It does NOT delete the entry from the source stream. A dead-letter is
	// a decision about THIS subscription, and XDEL would remove the
	// entry for every other consumer group on the same topic.
	ScriptDeadLetter = `
local g = ARGV[1]
local me = ARGV[2]
local id = ARGV[3]
local maxlen = tonumber(ARGV[4])
local p = redis.call('XPENDING', KEYS[1], g, id, id, 1)
if not p[1] or p[1][2] ~= me then return false end
local newid
if maxlen > 0 then
  newid = redis.call('XADD', KEYS[2], 'MAXLEN', '~', maxlen, '*', unpack(ARGV, 5))
else
  newid = redis.call('XADD', KEYS[2], '*', unpack(ARGV, 5))
end
redis.call('XACK', KEYS[1], g, id)
return newid
`

	// ScriptDelayPark parks one entry in the delayed sorted set and
	// acknowledges it in the stream, atomically.
	//
	// KEYS[1] is the stream, KEYS[2] the delayed sorted set; ARGV[1] the
	// group, ARGV[2] this consumer, ARGV[3] the identifier, ARGV[4] the
	// score (the deliver-at time in milliseconds since the epoch), ARGV[5]
	// the packed member. It returns 1 when the entry was parked and false
	// when it is not ours.
	//
	// The member is packed by the caller ([PackDelayed]) so that the
	// encoding lives in Go where a fuzz target reaches it, and so that it
	// carries the source identifier: a sorted set is a SET, and two
	// identical messages parked for the same instant would otherwise be one
	// member and one of them would be lost.
	ScriptDelayPark = `
local g = ARGV[1]
local me = ARGV[2]
local id = ARGV[3]
local p = redis.call('XPENDING', KEYS[1], g, id, id, 1)
if not p[1] or p[1][2] ~= me then return false end
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
redis.call('XACK', KEYS[1], g, id)
return 1
`

	// ScriptDelayRelease moves one parked member back into the stream,
	// atomically.
	//
	// KEYS[1] is the delayed sorted set, KEYS[2] the stream; ARGV[1] the
	// member, ARGV[2] the stream's MAXLEN (0 for none), ARGV[3..] the
	// entry's field-value list. It returns the new identifier, or false when
	// the member was already gone — which is how two movers racing over the
	// same due member produce one redelivery: the ZREM decides, and the
	// loser adds nothing.
	ScriptDelayRelease = `
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return false end
local maxlen = tonumber(ARGV[2])
if maxlen > 0 then
  return redis.call('XADD', KEYS[2], 'MAXLEN', '~', maxlen, '*', unpack(ARGV, 3))
end
return redis.call('XADD', KEYS[2], '*', unpack(ARGV, 3))
`

	// ScriptReAdd adds the entry back to the stream under a new identifier
	// and acknowledges the old one, atomically.
	//
	// KEYS[1] is the stream; ARGV[1] the group, ARGV[2] this consumer,
	// ARGV[3] the identifier, ARGV[4] the MAXLEN (0 for none), ARGV[5..] the
	// entry's field-value list. It returns the new identifier, or false when
	// the entry is not ours.
	ScriptReAdd = `
local g = ARGV[1]
local me = ARGV[2]
local id = ARGV[3]
local maxlen = tonumber(ARGV[4])
local p = redis.call('XPENDING', KEYS[1], g, id, id, 1)
if not p[1] or p[1][2] ~= me then return false end
local newid
if maxlen > 0 then
  newid = redis.call('XADD', KEYS[1], 'MAXLEN', '~', maxlen, '*', unpack(ARGV, 5))
else
  newid = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 5))
end
redis.call('XACK', KEYS[1], g, id)
return newid
`
)

// The scripts' digests, computed once.
//
// SHA-1 over a two-kilobyte script is about a microsecond and two allocations,
// and a nack that computed it would pay that on every settlement: the
// benchmark showed 26 us and 16 allocations per plan before these existed,
// against 6.8 us and 16 after. A digest is a constant of the script, so it is
// computed once.
var (
	shaAckGuard     = SHA(ScriptAckGuard)
	shaExtendGuard  = SHA(ScriptExtendGuard)
	shaDeadLetter   = SHA(ScriptDeadLetter)
	shaDelayPark    = SHA(ScriptDelayPark)
	shaDelayRelease = SHA(ScriptDelayRelease)
	shaReAdd        = SHA(ScriptReAdd)
)

// SHAOf returns a script's digest from the precomputed table, and computes it
// for a script this package does not own.
func SHAOf(script string) string {
	switch script {
	case ScriptAckGuard:
		return shaAckGuard
	case ScriptExtendGuard:
		return shaExtendGuard
	case ScriptDeadLetter:
		return shaDeadLetter
	case ScriptDelayPark:
		return shaDelayPark
	case ScriptDelayRelease:
		return shaDelayRelease
	case ScriptReAdd:
		return shaReAdd
	}
	return SHA(script)
}

// SHA returns a script's SHA-1 digest, which is what EVALSHA names it by.
//
// The digest is Redis's own definition of a script's identity, so the hash
// function is not a choice this driver gets to make.
func SHA(script string) string {
	sum := sha1.Sum([]byte(script)) //nolint:gosec // see above
	return hex.EncodeToString(sum[:])
}

// Scripts returns every script this package uses, so that a driver can load
// them all at open and use EVALSHA thereafter.
func Scripts() []string {
	return []string{
		ScriptAckGuard, ScriptExtendGuard, ScriptDeadLetter,
		ScriptDelayPark, ScriptDelayRelease, ScriptReAdd,
	}
}

// ParseAckGuardReply reads [ScriptAckGuard]'s reply: how many entries were
// acknowledged, and which identifiers were refused because another consumer
// holds them.
func ParseAckGuardReply(v Value) (acked int64, lost []ID, err error) {
	pair, ok := v.Arr()
	if !ok || len(pair) != 2 {
		return 0, nil, ErrReplyShape
	}
	if acked, ok = pair[0].Int64(); !ok {
		return 0, nil, ErrReplyShape
	}
	if lost, err = parseIDs(pair[1]); err != nil {
		return 0, nil, err
	}
	return acked, lost, nil
}

// ParseExtendGuardReply reads [ScriptExtendGuard]'s reply: which identifiers
// were extended and which are no longer ours.
func ParseExtendGuardReply(v Value) (extended, lost []ID, err error) {
	pair, ok := v.Arr()
	if !ok || len(pair) != 2 {
		return nil, nil, ErrReplyShape
	}
	if extended, err = parseIDs(pair[0]); err != nil {
		return nil, nil, err
	}
	if lost, err = parseIDs(pair[1]); err != nil {
		return nil, nil, err
	}
	return extended, lost, nil
}

// ParseNewID reads a script's reply that is either a new stream identifier or
// false. Lua's false arrives as a null reply over RESP2 and as a boolean false
// over RESP3, and either means the entry was not ours.
//
// It is the one reply shape every write script shares, and it is why a
// refused settlement is [ErrGuardRefused] rather than a silent success: a
// dead-letter move that returned false moved nothing, and reporting it as
// done would drop the message.
func ParseNewID(v Value) (ID, error) {
	if luaFalse(v) {
		return ID{}, ErrGuardRefused
	}
	if n, ok := v.Int64(); ok {
		// ScriptDelayPark returns 1 rather than an identifier: it parked the
		// entry in a sorted set, which has none.
		if n == 1 {
			return ID{}, nil
		}
		return ID{}, ErrReplyShape
	}
	s, ok := v.Str()
	if !ok {
		return ID{}, ErrReplyShape
	}
	return ParseID(s)
}

// luaFalse reports whether v is how a script's false reply arrives.
func luaFalse(v Value) bool {
	if v.Kind() == KindBool {
		b, _ := v.Bool()
		return !b
	}
	return v.IsNil()
}

// PackDelayed encodes a parked entry as one sorted-set member.
//
// The framing is a header — the source shard, the source identifier and the
// attempt count, comma-separated — then the field-value list, each item
// written as its byte length, a colon and the bytes. A length prefix rather
// than a separator, for the reason [packHeader] gives: every byte is a byte a
// field value may contain.
//
// The source identifier is in the member because a sorted set is a set. Two
// publications of the same body parked for the same instant have the same
// fields and the same score, so without the identifier they would be one
// member — and the second nack would silently discard a message.
func PackDelayed(shard int, id ID, attempt uint32, fields []string) string {
	var b strings.Builder
	n := 24 + len(fields)*8
	for _, f := range fields {
		n += len(f)
	}
	b.Grow(n)
	b.WriteString(strconv.Itoa(shard))
	b.WriteByte(',')
	b.WriteString(id.String())
	b.WriteByte(',')
	b.WriteString(strconv.FormatUint(uint64(attempt), 10))
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(len(fields)))
	for _, f := range fields {
		b.WriteByte(',')
		b.WriteString(strconv.Itoa(len(f)))
		b.WriteByte(':')
		b.WriteString(f)
	}
	return b.String()
}

// Delayed is a parked entry as [UnpackDelayed] reads it back.
type Delayed struct {
	// Fields is the entry's field-value list.
	Fields []string
	// ID is the identifier the entry had in the source stream.
	ID ID
	// Shard is the shard it came from, and the shard it goes back to.
	Shard int
	// Attempt is the delivery attempt it had reached.
	Attempt uint32
}

// UnpackDelayed reverses [PackDelayed].
//
// It is strict: a member it cannot read is [ErrCorruptEntry] rather than a
// best effort, because a member half-read is an entry re-added with some of
// its headers missing, which is a message silently changed in transit. A
// mover that meets one reports it and leaves the member in the sorted set for
// an operator to look at.
func UnpackDelayed(member string) (Delayed, error) {
	rest, shard, err := cutInt(member)
	if err != nil {
		return Delayed{}, err
	}
	idStr, rest, ok := strings.Cut(rest, ",")
	if !ok {
		return Delayed{}, ErrCorruptEntry
	}
	id, err := ParseID(idStr)
	if err != nil {
		return Delayed{}, err
	}
	rest, attempt, err := cutInt(rest)
	if err != nil {
		return Delayed{}, err
	}
	if shard < 0 || attempt < 0 {
		return Delayed{}, ErrCorruptEntry
	}
	// strings.Cut already returns the whole remainder as `before` and "" as
	// `after` when the separator is absent, which is exactly the zero-field
	// member PackDelayed writes ("<shard>,<id>,<attempt>,0"). The branch that
	// used to stand here -- `if !hasFields { countStr, rest = rest, "" }` --
	// overwrote countStr with the already-empty rest, so Atoi("") failed and a
	// member this package had just written came back as ErrCorruptEntry. The
	// round trip PackDelayed and UnpackDelayed promise was broken for a
	// zero-field entry, and nothing noticed because the only production caller
	// (zsetMover.Plan) refuses an entry with no fields with ErrNoFields before
	// it ever packs one. See.
	countStr, rest, _ := strings.Cut(rest, ",")
	count, cerr := strconv.Atoi(countStr)
	if cerr != nil || count < 0 {
		return Delayed{}, ErrCorruptEntry
	}
	out := Delayed{ID: id, Shard: shard, Attempt: uint32(attempt)}
	if count > 0 {
		out.Fields = make([]string, 0, count)
	}
	for range count {
		colon := strings.IndexByte(rest, ':')
		if colon < 0 {
			return Delayed{}, ErrCorruptEntry
		}
		n, nerr := strconv.Atoi(rest[:colon])
		if nerr != nil || n < 0 || colon+1+n > len(rest) {
			return Delayed{}, ErrCorruptEntry
		}
		out.Fields = append(out.Fields, rest[colon+1:colon+1+n])
		rest = rest[colon+1+n:]
		if rest != "" {
			if rest[0] != ',' {
				return Delayed{}, ErrCorruptEntry
			}
			rest = rest[1:]
		}
	}
	if rest != "" || len(out.Fields) != count {
		return Delayed{}, ErrCorruptEntry
	}
	return out, nil
}

// cutInt reads a decimal integer up to the next comma.
func cutInt(s string) (rest string, n int, err error) {
	head, rest, ok := strings.Cut(s, ",")
	if !ok {
		return "", 0, ErrCorruptEntry
	}
	n, cerr := strconv.Atoi(head)
	if cerr != nil {
		return "", 0, ErrCorruptEntry
	}
	return rest, n, nil
}

// ZRangeDue builds the ZRANGEBYSCORE that finds the parked entries whose time
// has come.
//
// nowMillis is passed in rather than read here: a script may not read the
// clock, and the driver's clock is the injected one, so the mover's
// notion of now comes from the same place as every other deadline's.
func ZRangeDue(zset string, nowMillis int64, count int) Cmd {
	return Cmd{
		Tokens: []string{
			"ZRANGEBYSCORE", zset, "-inf", strconv.FormatInt(nowMillis, 10),
			"LIMIT", "0", strconv.Itoa(count),
		},
		KeyStart: 1, KeyCount: 1,
	}
}

// ParseMembers reads a sorted-set range's reply.
func ParseMembers(v Value) ([]string, error) {
	members, ok := stringArr(v)
	if !ok {
		return nil, ErrReplyShape
	}
	return members, nil
}
