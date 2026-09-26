package streams_test

import (
	"crypto/sha1" // Redis defines a script's identity as its SHA-1; see lua.go
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/streams"
)

// The script layer's replies and the parked-entry framing.
//
// Two things here are silent when they are wrong. A guard script's reply says
// whether a settlement HAPPENED, and reading a refusal as a success drops the
// message (lua.go says so above ParseNewID). And a parked entry is a sorted-set
// member -- a single string that has to carry a shard, an identifier, an
// attempt count and every field value, any of which may contain the bytes the
// framing uses. A member half-read is a message re-added with some of its
// headers missing, which is a message silently changed in transit.

func TestSHAOfReturnsTheSameDigestForEveryScriptItOwns(t *testing.T) {
	t.Parallel()
	// The table is a six-arm switch, and an arm that returned the wrong
	// variable would make the driver EVALSHA a DIFFERENT script -- a
	// dead-letter move running the delay-park script, say -- which the server
	// would happily execute. So every script's digest is checked against the
	// digest of that script, not merely against "some digest".
	scripts := streams.Scripts()
	if len(scripts) != 6 {
		t.Fatalf("Scripts() returned %d scripts, want 6; a script a driver does not load cannot be EVALSHA'd", len(scripts))
	}
	seen := map[string]string{}
	for _, s := range scripts {
		sum := sha1.Sum([]byte(s))
		want := hex.EncodeToString(sum[:])
		if got := streams.SHA(s); got != want {
			t.Errorf("SHA mismatch: %s vs %s", got, want)
		}
		if got := streams.SHAOf(s); got != want {
			t.Errorf("SHAOf returned %s for a script whose digest is %s: the switch arm names another script", got, want)
		}
		if prev, dup := seen[want]; dup {
			t.Errorf("two scripts share the digest %s; one of them is the other:\n%.60q\n%.60q", want, prev, s)
		}
		seen[want] = s
	}
	// A script this package does not own falls through to a computed digest
	// rather than to one of the six.
	const foreign = "return redis.call('PING')"
	sum := sha1.Sum([]byte(foreign))
	want := hex.EncodeToString(sum[:])
	if got := streams.SHAOf(foreign); got != want {
		t.Errorf("SHAOf(a foreign script) = %s, want its own digest %s", got, want)
	}
	if _, isOurs := seen[want]; isOurs {
		t.Error("a foreign script's digest collided with one of ours")
	}
}

func TestParseAckGuardReplyReadsTheCountAndTheLostIdentifiers(t *testing.T) {
	t.Parallel()
	acked, lost, err := streams.ParseAckGuardReply(
		streams.Arr(streams.Int(2), streams.Arr(streams.Str("5-1"))),
	)
	if err != nil {
		t.Fatalf("ParseAckGuardReply: %v", err)
	}
	if acked != 2 {
		t.Errorf("acked = %d, want 2", acked)
	}
	if !slices.Equal(lost, []streams.ID{{Ms: 5, Seq: 1}}) {
		t.Errorf("lost = %v, want [5-1]; a lost identifier is a lease another consumer holds", lost)
	}
	// An empty lost list is the ordinary case and must not be an error.
	acked, lost, err = streams.ParseAckGuardReply(streams.Arr(streams.Int(3), streams.Arr()))
	if err != nil || acked != 3 || len(lost) != 0 {
		t.Errorf("ParseAckGuardReply = (%d, %v, %v), want (3, empty, nil)", acked, lost, err)
	}
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"not an array", streams.Int(1), streams.ErrReplyShape},
		{"one element", streams.Arr(streams.Int(1)), streams.ErrReplyShape},
		{"three elements", streams.Arr(streams.Int(1), streams.Arr(), streams.Int(2)), streams.ErrReplyShape},
		{"the count is not an integer", streams.Arr(streams.Str("1"), streams.Arr()), streams.ErrReplyShape},
		{"the lost list is not an array", streams.Arr(streams.Int(1), streams.Int(2)), streams.ErrReplyShape},
		{"a lost identifier is unparseable", streams.Arr(streams.Int(1), streams.Arr(streams.Str("x"))), streams.ErrBadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			acked, lost, err := streams.ParseAckGuardReply(tc.reply)
			wantErr(t, err, tc.want, "ParseAckGuardReply")
			if acked != 0 || lost != nil {
				t.Errorf("a refused reply also returned (%d, %v); a caller that ignored the error would count settlements that did not happen", acked, lost)
			}
		})
	}
}

func TestParseExtendGuardReplySeparatesExtendedFromLost(t *testing.T) {
	t.Parallel()
	// These two lists must not be swapped: an identifier reported as extended
	// when the lease is gone means the runner keeps working on a delivery
	// another consumer now owns, which is the duplicate the guard exists to
	// prevent.
	extended, lost, err := streams.ParseExtendGuardReply(
		streams.Arr(streams.Arr(streams.Str("1-0")), streams.Arr(streams.Str("2-0"), streams.Str("3-0"))),
	)
	if err != nil {
		t.Fatalf("ParseExtendGuardReply: %v", err)
	}
	if !slices.Equal(extended, []streams.ID{{Ms: 1}}) {
		t.Errorf("extended = %v, want [1-0]", extended)
	}
	if !slices.Equal(lost, []streams.ID{{Ms: 2}, {Ms: 3}}) {
		t.Errorf("lost = %v, want [2-0 3-0]", lost)
	}
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"not an array", streams.Str("ok"), streams.ErrReplyShape},
		{"one element", streams.Arr(streams.Arr()), streams.ErrReplyShape},
		{"the extended list is not an array", streams.Arr(streams.Int(1), streams.Arr()), streams.ErrReplyShape},
		{"the lost list is not an array", streams.Arr(streams.Arr(), streams.Int(1)), streams.ErrReplyShape},
		{"an extended identifier is unparseable", streams.Arr(streams.Arr(streams.Str("-")), streams.Arr()), streams.ErrBadID},
		{"a lost identifier is unparseable", streams.Arr(streams.Arr(), streams.Arr(streams.Str("-"))), streams.ErrBadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			extended, lost, err := streams.ParseExtendGuardReply(tc.reply)
			wantErr(t, err, tc.want, "ParseExtendGuardReply")
			if extended != nil || lost != nil {
				t.Errorf("a refused reply also returned (%v, %v)", extended, lost)
			}
		})
	}
}

func TestParseNewIDTellsARefusedSettlementFromASucceededOne(t *testing.T) {
	t.Parallel()
	// This is the reply shape every write script shares. Lua's `false` arrives
	// as a null and means the entry was NOT ours: a dead-letter move that
	// returned false moved nothing, and reporting it as done would drop the
	// message. So the null must be ErrGuardRefused -- and ErrGuardRefused must
	// be LeaseLost, not CorruptMessage, because losing a lease is an ordinary
	// race the runner retries and corruption is not.
	if _, err := streams.ParseNewID(streams.Nil()); err != streams.ErrGuardRefused { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("a null reply gave %v, want ErrGuardRefused", err)
	}
	if code := errcode.CodeOf(streams.ErrGuardRefused); code != errcode.LeaseLost {
		t.Errorf("ErrGuardRefused is %v, want LeaseLost so a refused settlement is not read as corruption", code)
	}
	// ScriptDelayPark returns 1: it parked the entry in a sorted set, which has
	// no stream identifier. That is a SUCCESS with a zero identifier.
	id, err := streams.ParseNewID(streams.Int(1))
	if err != nil {
		t.Fatalf("the park reply (integer 1) must succeed, got %v", err)
	}
	if id != (streams.ID{}) {
		t.Errorf("id = %v, want the zero identifier for a park", id)
	}
	id, err = streams.ParseNewID(streams.Str("12-3"))
	if err != nil || id != (streams.ID{Ms: 12, Seq: 3}) {
		t.Fatalf("ParseNewID(\"12-3\") = (%v, %v), want 12-3", id, err)
	}
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"an integer that is not 1", streams.Int(2), streams.ErrReplyShape},
		{"zero", streams.Int(0), streams.ErrReplyShape},
		{"an array", streams.Arr(streams.Str("1-0")), streams.ErrReplyShape},
		{"an unparseable identifier", streams.Str("not-an-id"), streams.ErrBadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id, err := streams.ParseNewID(tc.reply)
			wantErr(t, err, tc.want, "ParseNewID")
			if id != (streams.ID{}) {
				t.Errorf("a refused reply also returned %v", id)
			}
		})
	}
}

func TestParseMembersRefusesAReplyThatIsNotAStringList(t *testing.T) {
	t.Parallel()
	got, err := streams.ParseMembers(streams.Arr(streams.Str("a"), streams.Str("b")))
	if err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("ParseMembers = (%v, %v), want [a b]", got, err)
	}
	// A null reply is an empty range, not a refusal: the mover asks for the
	// parked entries whose time has come and usually there are none.
	got, err = streams.ParseMembers(streams.Nil())
	if err != nil || got != nil {
		t.Errorf("ParseMembers(nil) = (%v, %v), want (nil, nil)", got, err)
	}
	for _, v := range []streams.Value{streams.Int(1), streams.Arr(streams.Int(1))} {
		got, err := streams.ParseMembers(v)
		wantErr(t, err, streams.ErrReplyShape, "ParseMembers")
		if got != nil {
			t.Errorf("a refused reply also returned %v", got)
		}
	}
}

func TestPackDelayedRoundTripsEveryByteAFieldValueMayContain(t *testing.T) {
	t.Parallel()
	// The framing is length-prefixed rather than separated BECAUSE a field
	// value may contain the framing bytes. A reimplementation that split on the
	// separators would pass a test whose fields were alphanumeric and corrupt
	// every real header, so the fields here contain the comma and the colon the
	// framing itself uses, an embedded newline, and an empty value.
	for _, tc := range []struct {
		name    string
		shard   int
		id      streams.ID
		attempt uint32
		fields  []string
	}{
		{"no fields", 0, streams.ID{Ms: 1}, 1, nil},
		{"plain fields", 3, streams.ID{Ms: 1700000000000, Seq: 7}, 4, []string{"h", "v"}},
		{"a field containing the separator", 1, streams.ID{Ms: 2}, 2, []string{"k", "a,b,c"}},
		{"a field containing the length delimiter", 1, streams.ID{Ms: 2}, 2, []string{"k", "3:xyz"}},
		{"a field that looks like a whole member", 1, streams.ID{Ms: 2}, 2, []string{"k", "0,1-0,1,1,1:x"}},
		{"an empty field value", 2, streams.ID{Ms: 9}, 0, []string{"body", ""}},
		{"an empty field name", 2, streams.ID{Ms: 9}, 0, []string{"", "v"}},
		{"a newline in a value", 2, streams.ID{Ms: 9}, 3, []string{"k", "a\nb"}},
		{"the maximum attempt count", 0, streams.ID{Ms: 1}, ^uint32(0), []string{"k", "v"}},
		{"many fields", 5, streams.ID{Ms: 4, Seq: 4}, 2, []string{"a", "1", "b", "2", "c", "3", "d", "4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			member := streams.PackDelayed(tc.shard, tc.id, tc.attempt, tc.fields)
			got, err := streams.UnpackDelayed(member)
			if err != nil {
				t.Fatalf("UnpackDelayed(%q): %v", member, err)
			}
			if got.Shard != tc.shard || got.ID != tc.id || got.Attempt != tc.attempt {
				t.Errorf("header = {shard %d id %v attempt %d}, want {%d %v %d}",
					got.Shard, got.ID, got.Attempt, tc.shard, tc.id, tc.attempt)
			}
			if len(tc.fields) == 0 {
				if len(got.Fields) != 0 {
					t.Errorf("fields = %q, want none", got.Fields)
				}
				return
			}
			if !slices.Equal(got.Fields, tc.fields) {
				t.Errorf("fields = %q, want %q", got.Fields, tc.fields)
			}
		})
	}
}

func TestPackDelayedGivesTwoEntriesWithTheSameBodyDifferentMembers(t *testing.T) {
	t.Parallel()
	// A sorted set is a set. lua.go's comment says the source identifier is in
	// the member precisely so that two publications of the same body parked for
	// the same instant do not collapse into one -- which would silently discard
	// the second nack's message.
	same := []string{"h", "v"}
	a := streams.PackDelayed(1, streams.ID{Ms: 100, Seq: 0}, 2, same)
	b := streams.PackDelayed(1, streams.ID{Ms: 100, Seq: 1}, 2, same)
	if a == b {
		t.Fatalf("two entries with the same body and score produced the same member %q; the second nack's message would be discarded", a)
	}
	// And the shard and the attempt are in the member too, so a round trip puts
	// the entry back where it came from with the attempt count it had reached.
	c := streams.PackDelayed(2, streams.ID{Ms: 100}, 2, same)
	d := streams.PackDelayed(1, streams.ID{Ms: 100}, 3, same)
	if c == streams.PackDelayed(1, streams.ID{Ms: 100}, 2, same) {
		t.Error("the shard is not in the member; the entry would be re-added to the wrong shard")
	}
	if d == streams.PackDelayed(1, streams.ID{Ms: 100}, 2, same) {
		t.Error("the attempt count is not in the member; a redelivery would restart the attempt accounting")
	}
}

func TestUnpackDelayedRefusesAHalfReadableMemberRatherThanGuessing(t *testing.T) {
	t.Parallel()
	// Every case is a member a corrupted or foreign writer could put in the
	// sorted set. The contract is that the mover reports it and LEAVES it for
	// an operator, so each of these must be an error -- a best-effort read
	// would re-add a message with some of its headers missing.
	for _, tc := range []struct {
		name   string
		member string
		want   error
	}{
		{"empty", "", streams.ErrCorruptEntry},
		{"no separator at all", "1", streams.ErrCorruptEntry},
		{"the shard is not a number", "x,1-0,1,0", streams.ErrCorruptEntry},
		{"the identifier is missing its separator", "1,1-0", streams.ErrCorruptEntry},
		{"the identifier is unparseable", "1,not-an-id,1,0", streams.ErrBadID},
		{"the attempt count is not a number", "1,1-0,x,0", streams.ErrCorruptEntry},
		{"the attempt count has no separator", "1,1-0,1", streams.ErrCorruptEntry},
		{"a negative shard", "-1,1-0,1,0", streams.ErrCorruptEntry},
		{"a negative attempt count", "1,1-0,-1,0", streams.ErrCorruptEntry},
		{"the field count is not a number", "1,1-0,1,x", streams.ErrCorruptEntry},
		// Two of UnpackDelayed's guards are REDUNDANT rather than load-bearing,
		// and these cases record the shape without claiming to pin them:
		// `count < 0` (below) and cutInt's `!ok` ("no separator at all" above).
		// Deleting either leaves every case here passing, because the final
		// `rest != "" || len(out.Fields) != count` check and the identifier read
		// that follows cutInt already refuse the same members with the same
		// sentinel. They are cheap and they say what they mean, so they are left
		// alone -- but a reader should not take their coverage for evidence.
		{"a negative field count", "1,1-0,1,-1", streams.ErrCorruptEntry},
		{"a field has no length delimiter", "1,1-0,1,1,abc", streams.ErrCorruptEntry},
		{"a field length is not a number", "1,1-0,1,1,x:abc", streams.ErrCorruptEntry},
		{"a negative field length", "1,1-0,1,1,-1:abc", streams.ErrCorruptEntry},
		{"a field length runs past the end", "1,1-0,1,1,9:ab", streams.ErrCorruptEntry},
		{"a field is not followed by a separator", "1,1-0,1,2,1:a1:b", streams.ErrCorruptEntry},
		// The case above is also refused by the length read that follows it, so
		// it does not on its own establish the separator check. This one does:
		// the byte where the separator should be is junk, and DROPPING it
		// leaves a perfectly readable second field -- so a parser that
		// consumed one byte without checking it would accept the member and
		// hand back a field list the writer never wrote.
		{"a stray byte where the separator should be", "1,1-0,1,2,1:ax1:b", streams.ErrCorruptEntry},
		{"fewer fields than the count claims", "1,1-0,1,3,1:a,1:b", streams.ErrCorruptEntry},
		{"trailing bytes after the last field", "1,1-0,1,1,1:a,junk", streams.ErrCorruptEntry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := streams.UnpackDelayed(tc.member)
			wantErr(t, err, tc.want, "UnpackDelayed("+tc.member+")")
			if got.Shard != 0 || got.ID != (streams.ID{}) || got.Attempt != 0 || got.Fields != nil {
				t.Errorf("a refused member also returned %+v; a caller that ignored the error would re-add it", got)
			}
		})
	}
	if code := errcode.CodeOf(streams.ErrCorruptEntry); code != errcode.CorruptMessage {
		t.Errorf("ErrCorruptEntry is %v, want CorruptMessage", code)
	}
	// The control: the shortest member that IS readable, so the cases above are
	// not all failing on some shared prefix mistake.
	//
	// It is also the regression. This exact member is what
	// PackDelayed writes for an entry with no fields, and UnpackDelayed used to
	// refuse it with ErrCorruptEntry: the branch below `strings.Cut(rest, ",")`
	// overwrote the field count with the already-empty remainder. Nothing
	// noticed because zsetMover.Plan refuses an entry with no fields before it
	// packs one, so the broken round trip was masked by one caller's guard.
	good, err := streams.UnpackDelayed("1,1-0,1,0")
	if err != nil {
		t.Fatalf("a zero-field member must be readable, got %v ", err)
	}
	if good.Shard != 1 || good.ID != (streams.ID{Ms: 1}) || good.Attempt != 1 || len(good.Fields) != 0 {
		t.Errorf("the control member read as %+v", good)
	}
}

func TestZRangeDueAsksForTheParkedEntriesWhoseTimeHasCome(t *testing.T) {
	t.Parallel()
	// The upper bound is the caller's clock, not the server's: a script may not
	// read the clock and the driver's clock is the injected one, so a builder
	// that substituted its own "now" would make every deadline in the driver
	// depend on the broker's wall time.
	c := streams.ZRangeDue("z", 1700000000123, 50)
	want := []string{"ZRANGEBYSCORE", "z", "-inf", "1700000000123", "LIMIT", "0", "50"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q", c.Tokens, want)
	}
	if !slices.Equal(c.Keys(), []string{"z"}) {
		t.Errorf("Keys() = %q, want [z]", c.Keys())
	}
	if strings.Contains(strings.Join(c.Tokens, " "), "+inf") {
		t.Error("the range is unbounded above; it would release every parked entry at once")
	}
}
