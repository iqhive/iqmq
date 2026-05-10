package streams_test

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// The command layer is the one place in this driver where a wrong answer is
// silent: a builder that omits an argument sends a command that SUCCEEDS and
// does something else, and a parser that drops an entry drops a message that
// is pending at the broker and that nothing will then settle (cmds.go says so
// above ParseXReadGroup). Neither failure raises anything on its own, so the
// assertions here are on the exact tokens a builder produces and on the
// IDENTITY of the error a malformed reply produces -- not on "an error
// happened".
//
// Identity and not errors.Is: internal/errcode matches by Code, and
// ErrReplyShape and ErrBadID are both CorruptMessage, so errors.Is cannot tell
// "this reply is the wrong shape" from "this identifier is unparseable". Those
// are different diagnoses for an operator reading a log, and the whole point
// of ErrBadID's existence is that it names the identifier rather than the
// reply. A test that accepted either would not notice them being swapped.

// wantErr fails unless err is exactly want.
func wantErr(t *testing.T, err, want error, what string) {
	t.Helper()
	if err != want { //nolint:errorlint // identity is the assertion; see above
		t.Fatalf("%s: error is %v, want exactly %v", what, err, want)
	}
}

func TestMillisRoundsUpSoASubMillisecondDeadlineIsNotForever(t *testing.T) {
	t.Parallel()
	// millis is unexported; XREADGROUP's BLOCK argument is how it is reached,
	// and BLOCK is where rounding DOWN is catastrophic: cmds.go's comment says
	// a 500-microsecond deadline rounded to zero means "block for ever" to
	// XREADGROUP. So the assertion is on the token, which is what the server
	// sees.
	for _, tc := range []struct {
		name  string
		block time.Duration
		want  string
	}{
		{"sub-millisecond rounds up to one, never to zero", 500 * time.Microsecond, "1"},
		{"a whole millisecond is itself", time.Millisecond, "1"},
		{"a remainder rounds up", 1500 * time.Microsecond, "2"},
		{"whole milliseconds do not gain one", 2 * time.Millisecond, "2"},
		{"a large duration is exact", 1234 * time.Millisecond, "1234"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.XReadGroup(streams.XReadGroupArgs{
				Group: "g", Consumer: "c",
				Keys: []string{"k"}, IDs: []string{streams.IDNew},
				Count: 1, Block: tc.block,
			})
			if err != nil {
				t.Fatalf("XReadGroup: %v", err)
			}
			i := slices.Index(c.Tokens, "BLOCK")
			if i < 0 || i+1 >= len(c.Tokens) {
				t.Fatalf("no BLOCK argument in %q", c.Tokens)
			}
			if got := c.Tokens[i+1]; got != tc.want {
				t.Errorf("BLOCK %v -> %q, want %q", tc.block, got, tc.want)
			}
			if !c.Blocking {
				t.Error("a command with BLOCK is not marked Blocking; rueidis would queue it on the shared pipelined connection")
			}
		})
	}
}

func TestXReadGroupWithoutBlockIsNotMarkedBlocking(t *testing.T) {
	t.Parallel()
	c, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: "g", Consumer: "c",
		Keys: []string{"k"}, IDs: []string{streams.IDNew}, Count: 4,
	})
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	if c.Blocking {
		t.Error("a command with no BLOCK is marked Blocking, which costs it a dedicated connection for nothing")
	}
	if slices.Contains(c.Tokens, "BLOCK") {
		t.Errorf("BLOCK present with Block unset: %q", c.Tokens)
	}
	if slices.Contains(c.Tokens, "NOACK") {
		t.Errorf("NOACK present with NoAck unset: %q", c.Tokens)
	}
	want := []string{"XREADGROUP", "GROUP", "g", "c", "COUNT", "4", "STREAMS", "k", ">"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q", c.Tokens, want)
	}
	if got := c.Keys(); !slices.Equal(got, []string{"k"}) {
		t.Errorf("Keys() = %q, want [k]; a wrong key span misroutes the command on a cluster", got)
	}
}

func TestXReadGroupNoAckIsSpelled(t *testing.T) {
	t.Parallel()
	// NOACK is the one flag here that changes the delivery guarantee: with it
	// the server never puts the entry in the pending list, so nothing can
	// redeliver it. A builder that dropped the token would silently upgrade
	// at-most-once to at-least-once.
	c, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: "g", Consumer: "c",
		Keys: []string{"k"}, IDs: []string{streams.IDNew}, Count: 1, NoAck: true,
	})
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	if !slices.Contains(c.Tokens, "NOACK") {
		t.Errorf("NoAck set but no NOACK token: %q", c.Tokens)
	}
}

func TestXReadGroupRefusesArgumentsItCannotBuild(t *testing.T) {
	t.Parallel()
	ok := streams.XReadGroupArgs{
		Group: "g", Consumer: "c",
		Keys: []string{"k"}, IDs: []string{streams.IDNew}, Count: 1,
	}
	mut := func(f func(*streams.XReadGroupArgs)) streams.XReadGroupArgs {
		a := ok
		a.Keys = slices.Clone(ok.Keys)
		a.IDs = slices.Clone(ok.IDs)
		f(&a)
		return a
	}
	for _, tc := range []struct {
		name string
		args streams.XReadGroupArgs
	}{
		{"no group", mut(func(a *streams.XReadGroupArgs) { a.Group = "" })},
		{"no consumer", mut(func(a *streams.XReadGroupArgs) { a.Consumer = "" })},
		{"no keys", mut(func(a *streams.XReadGroupArgs) { a.Keys = nil })},
		{"more keys than ids", mut(func(a *streams.XReadGroupArgs) { a.Keys = []string{"k", "k2"} })},
		{"more ids than keys", mut(func(a *streams.XReadGroupArgs) { a.IDs = []string{">", ">"} })},
		{"zero count", mut(func(a *streams.XReadGroupArgs) { a.Count = 0 })},
		{"negative count", mut(func(a *streams.XReadGroupArgs) { a.Count = -1 })},
		{"negative block", mut(func(a *streams.XReadGroupArgs) { a.Block = -time.Second })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.XReadGroup(tc.args)
			wantErr(t, err, streams.ErrBadArgs, "XReadGroup")
			if len(c.Tokens) != 0 {
				t.Errorf("a refused command came back with tokens %q; a caller that ignored the error would send it", c.Tokens)
			}
		})
	}
	// The control: the same args unmodified must build, or every case above
	// would pass for the wrong reason.
	if _, err := streams.XReadGroup(ok); err != nil {
		t.Fatalf("the unmodified args must build, got %v", err)
	}
}

func TestXAddBuildsTheOptionalArgumentsItWasAskedFor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args streams.XAddArgs
		want []string
	}{
		{
			"an empty ID means the server assigns one",
			streams.XAddArgs{Key: "k", Fields: []string{"f", "v"}},
			[]string{"XADD", "k", "*", "f", "v"},
		},
		{
			"NOMKSTREAM comes before MAXLEN",
			streams.XAddArgs{Key: "k", NoMkStream: true, MaxLen: 10, Fields: []string{"f", "v"}},
			[]string{"XADD", "k", "NOMKSTREAM", "MAXLEN", "~", "10", "*", "f", "v"},
		},
		{
			"MAXLEN is approximate, never exact",
			streams.XAddArgs{Key: "k", MaxLen: 99, ID: "5-5", Fields: nil},
			[]string{"XADD", "k", "MAXLEN", "~", "99", "5-5"},
		},
		{
			"a zero MaxLen trims nothing",
			streams.XAddArgs{Key: "k", MaxLen: 0, ID: "1-1"},
			[]string{"XADD", "k", "1-1"},
		},
		{
			"a negative MaxLen trims nothing either",
			streams.XAddArgs{Key: "k", MaxLen: -5, ID: "1-1"},
			[]string{"XADD", "k", "1-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := streams.XAdd(tc.args)
			if !slices.Equal(c.Tokens, tc.want) {
				t.Errorf("tokens = %q, want %q", c.Tokens, tc.want)
			}
			if !c.Valid() {
				t.Errorf("XAdd built an invalid command: %q", c.Tokens)
			}
		})
	}
}

func TestXAddShiftsBlobOffsetsOntoTheCommandsOwnTokens(t *testing.T) {
	t.Parallel()
	// A blob whose At still names the index it had in Fields names a DIFFERENT
	// token in the command -- one of XADD's own arguments. The entry then goes
	// to the server with the body in the wrong slot and no error anywhere, so
	// the shift is the assertion.
	fields := []string{"h", "hdr", "b", ""}
	body := []byte("payload")
	c := streams.XAdd(streams.XAddArgs{
		Key: "k", ID: "1-0", MaxLen: 7, Fields: fields,
		FieldBlobs: []streams.Blob{{At: 3, Bytes: body}},
	})
	// "XADD" "k" "MAXLEN" "~" "7" "1-0" then the fields at 6..9.
	const wantAt = 6 + 3
	if c.Tokens[wantAt] != "" {
		t.Fatalf("token %d is %q, want the empty body slot; the prefix length changed", wantAt, c.Tokens[wantAt])
	}
	if len(c.Blobs) != 1 {
		t.Fatalf("Blobs = %v, want one", c.Blobs)
	}
	if c.Blobs[0].At != wantAt {
		t.Errorf("blob At = %d, want %d (shifted by the %d-token prefix)", c.Blobs[0].At, wantAt, 6)
	}
	text, blob := c.Arg(wantAt)
	if !bytes.Equal(blob, body) || text != "" {
		t.Errorf("Arg(%d) = (%q, %q), want the blob %q", wantAt, text, blob, body)
	}
	if !c.Valid() {
		t.Error("a command whose blob names its own empty token is not Valid")
	}
	// Fields must be copied, not aliased: cmds.go promises the caller may reuse
	// the scratch it encoded into.
	fields[0] = "MUTATED"
	if c.Tokens[6] != "h" {
		t.Errorf("XAdd aliased the caller's Fields: token 6 is now %q", c.Tokens[6])
	}
}

func TestXAddWithNoBlobsCarriesNoBlobSlice(t *testing.T) {
	t.Parallel()
	c := streams.XAdd(streams.XAddArgs{Key: "k", Fields: []string{"f", "v"}})
	if c.Blobs != nil {
		t.Errorf("Blobs = %v, want nil when none were given", c.Blobs)
	}
}

func TestParseXAddSeparatesRefusalFromCorruption(t *testing.T) {
	t.Parallel()
	// These two must not be conflated. A null reply is NOMKSTREAM doing exactly
	// what a driver with auto-creation off asked for, and it is ErrNoStream
	// (NotFound) so the driver can report "the topic does not exist". A reply
	// of the wrong type is a server this driver was not written against, and it
	// is ErrReplyShape. Swapping them turns a configuration answer into a
	// corruption alarm.
	if _, err := streams.ParseXAdd(streams.Nil()); err != streams.ErrNoStream { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("a null XADD reply gave %v, want ErrNoStream", err)
	}
	if code := errcode.CodeOf(streams.ErrNoStream); code != errcode.NotFound {
		t.Errorf("ErrNoStream is %v, want NotFound so a caller can tell it from corruption", code)
	}
	for _, v := range []streams.Value{streams.Int(3), streams.Arr(streams.Str("1-1"))} {
		if _, err := streams.ParseXAdd(v); err != streams.ErrReplyShape { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
			t.Errorf("ParseXAdd(%v) gave %v, want ErrReplyShape", v.Kind(), err)
		}
	}
	if _, err := streams.ParseXAdd(streams.Str("nonsense")); err != streams.ErrBadID { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("an unparseable identifier gave %v, want ErrBadID so the log names the identifier", err)
	}
	id, err := streams.ParseXAdd(streams.Str("17-4"))
	if err != nil {
		t.Fatalf("ParseXAdd: %v", err)
	}
	if id != (streams.ID{Ms: 17, Seq: 4}) {
		t.Errorf("id = %v, want 17-4", id)
	}
}

// readGroupReply builds the RESP2 nested form: [[key, [[id, [f,v,...]], ...]]].
func readGroupReply(key string, entries ...streams.Value) streams.Value {
	return streams.Arr(streams.Arr(streams.Str(key), streams.Arr(entries...)))
}

func entryVal(id string, fields ...string) streams.Value {
	fs := make([]streams.Value, 0, len(fields))
	for _, f := range fields {
		fs = append(fs, streams.Str(f))
	}
	return streams.Arr(streams.Str(id), streams.Arr(fs...))
}

func TestParseXReadGroupReadsBothProtocolSpellings(t *testing.T) {
	t.Parallel()
	// RESP2 nests [key, entries]; a RESP3 map arrives flattened. Which one the
	// driver sees depends on the client library's negotiation, so both have to
	// produce the same entries -- and the flattened form is told apart by its
	// first element being a string.
	nested := readGroupReply("s:0", entryVal("1-0", "f", "v"))
	flat := streams.Arr(
		streams.Str("s:0"), streams.Arr(entryVal("1-0", "f", "v")),
		streams.Str("s:1"), streams.Arr(entryVal("2-0", "g", "w")),
	)
	shardOf := func(k string) (int, bool) {
		switch k {
		case "s:0":
			return 0, true
		case "s:1":
			return 1, true
		}
		return 0, false
	}
	got, err := streams.ParseXReadGroup(nested, shardOf, nil)
	if err != nil {
		t.Fatalf("nested: %v", err)
	}
	if len(got) != 1 || got[0].ID != (streams.ID{Ms: 1}) || got[0].Shard != 0 {
		t.Fatalf("nested gave %+v, want one entry 1-0 on shard 0", got)
	}
	if !slices.Equal(got[0].Fields, []string{"f", "v"}) {
		t.Errorf("fields = %q, want [f v]", got[0].Fields)
	}
	got, err = streams.ParseXReadGroup(flat, shardOf, nil)
	if err != nil {
		t.Fatalf("flat: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("flat gave %d entries, want 2 (the flattened RESP3 map was not recognised)", len(got))
	}
	if got[0].Shard != 0 || got[1].Shard != 1 {
		t.Errorf("shards = %d,%d, want 0,1", got[0].Shard, got[1].Shard)
	}
	// An unknown key is shard -1, not shard 0: shard 0 is a real shard and
	// settling against it would settle the wrong lane.
	got, err = streams.ParseXReadGroup(readGroupReply("who?", entryVal("3-0")), shardOf, nil)
	if err != nil {
		t.Fatalf("unknown key: %v", err)
	}
	if len(got) != 1 || got[0].Shard != -1 {
		t.Fatalf("an unknown key gave %+v, want Shard -1", got)
	}
	// A nil shardOf is allowed and means every shard is unknown.
	got, err = streams.ParseXReadGroup(readGroupReply("s:0", entryVal("3-0")), nil, nil)
	if err != nil {
		t.Fatalf("nil shardOf: %v", err)
	}
	if len(got) != 1 || got[0].Shard != -1 {
		t.Fatalf("nil shardOf gave %+v, want Shard -1", got)
	}
}

func TestParseXReadGroupNullReplyIsAnExpiredBlockNotAnError(t *testing.T) {
	t.Parallel()
	dst := []streams.Entry{{ID: streams.ID{Ms: 9}}}
	got, err := streams.ParseXReadGroup(streams.Nil(), nil, dst)
	if err != nil {
		t.Fatalf("a null reply is the block expiring, not an error: %v", err)
	}
	if len(got) != 1 || got[0].ID != (streams.ID{Ms: 9}) {
		t.Errorf("dst = %+v, want it returned unchanged", got)
	}
}

func TestParseXReadGroupRefusesAMalformedReplyRatherThanDroppingEntries(t *testing.T) {
	t.Parallel()
	// This is the contract cmds.go states above ParseXReadGroup: every entry in
	// the reply is already in this consumer's pending list, so a parser that
	// skipped a malformed one would strand a message that nothing settles. So
	// the assertion is BOTH that the error is ErrReplyShape and that the good
	// entries in front of the bad one were not handed back as if the reply had
	// been read -- a caller that appended them would have lost the rest.
	good := entryVal("1-0", "f", "v")
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"the top level is not an array", streams.Int(1), streams.ErrReplyShape},
		{"a stream is not a [key, entries] pair", streams.Arr(streams.Int(1)), streams.ErrReplyShape},
		{"a stream pair has three elements", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(), streams.Arr())), streams.ErrReplyShape},
		{"the key is not a string", streams.Arr(streams.Arr(streams.Int(7), streams.Arr(good))), streams.ErrReplyShape},
		{"the entry list is not an array", streams.Arr(streams.Arr(streams.Str("k"), streams.Int(1))), streams.ErrReplyShape},
		{"an entry is not a [id, fields] pair", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Int(1)))), streams.ErrReplyShape},
		{"an entry pair has one element", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Arr(streams.Str("1-0"))))), streams.ErrReplyShape},
		{"an identifier is not a string", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Arr(streams.Int(1), streams.Arr())))), streams.ErrReplyShape},
		{"an identifier is unparseable", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Arr(streams.Str("x-y"), streams.Arr())))), streams.ErrBadID},
		{"a field list is not an array", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Arr(streams.Str("1-0"), streams.Int(1))))), streams.ErrReplyShape},
		{"a field is not a string", streams.Arr(streams.Arr(streams.Str("k"), streams.Arr(streams.Arr(streams.Str("1-0"), streams.Arr(streams.Int(1)))))), streams.ErrReplyShape},
		{"a flattened reply's key is not a string", streams.Arr(streams.Str("k"), streams.Arr(good), streams.Str("k2"), streams.Int(3)), streams.ErrReplyShape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := streams.ParseXReadGroup(tc.reply, nil, nil)
			wantErr(t, err, tc.want, "ParseXReadGroup")
		})
	}
}

func TestParseXReadGroupNullEntryListIsAKeyWithNothingReady(t *testing.T) {
	t.Parallel()
	got, err := streams.ParseXReadGroup(
		streams.Arr(streams.Arr(streams.Str("k"), streams.Nil())), nil, nil,
	)
	if err != nil {
		t.Fatalf("a null entry list is not an error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want no entries", got)
	}
}

func TestParseEntriesSkipsANullEntryButNotAMalformedOne(t *testing.T) {
	t.Parallel()
	// A null in an entry's place is XCLAIM reporting an entry that was deleted
	// from the stream. It is not corruption and it is not an entry, so it is
	// skipped -- and the entries around it must still arrive. Turning the skip
	// into a refusal would make every claim of a trimmed stream fail; turning
	// the refusal of a malformed entry into a skip is the message-stranding
	// failure above.
	reply := streams.Arr(
		entryVal("1-0", "a", "1"),
		streams.Nil(),
		entryVal("2-0", "b", "2"),
	)
	got, err := streams.ParseXRange(reply, 3, nil)
	if err != nil {
		t.Fatalf("ParseXRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2 with the null skipped: %+v", len(got), got)
	}
	if got[0].ID != (streams.ID{Ms: 1}) || got[1].ID != (streams.ID{Ms: 2}) {
		t.Errorf("ids = %v,%v, want 1-0,2-0", got[0].ID, got[1].ID)
	}
	for i, e := range got {
		if e.Shard != 3 {
			t.Errorf("entry %d shard = %d, want the 3 the caller passed", i, e.Shard)
		}
	}
	// A null FIELD list is an entry whose fields were all deleted: an entry
	// with no fields, not a refusal.
	got, err = streams.ParseXRange(
		streams.Arr(streams.Arr(streams.Str("4-0"), streams.Nil())), 0, nil,
	)
	if err != nil {
		t.Fatalf("null field list: %v", err)
	}
	if len(got) != 1 || len(got[0].Fields) != 0 {
		t.Errorf("got %+v, want one entry with no fields", got)
	}
	if _, err := streams.ParseXRange(streams.Int(1), 0, nil); err != streams.ErrReplyShape { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell these sentinels apart
		t.Errorf("a non-array XRANGE reply gave %v, want ErrReplyShape", err)
	}
}

func TestParseXReadGroupAppendsToTheCallersSlice(t *testing.T) {
	t.Parallel()
	// The driver reuses one slice per read. A parser that replaced it instead
	// of appending would drop whatever the caller had already collected.
	dst := make([]streams.Entry, 0, 4)
	dst = append(dst, streams.Entry{ID: streams.ID{Ms: 100}, Shard: 7})
	got, err := streams.ParseXReadGroup(readGroupReply("k", entryVal("1-0")), nil, dst)
	if err != nil {
		t.Fatalf("ParseXReadGroup: %v", err)
	}
	if len(got) != 2 || got[0].ID != (streams.ID{Ms: 100}) {
		t.Fatalf("got %+v, want the caller's entry kept in front", got)
	}
}

func TestParseCountRefusesAnythingButAnInteger(t *testing.T) {
	t.Parallel()
	// XACK's reply is how many entries were removed from the pending list. A
	// parser that read a non-integer as zero would report "nothing was
	// settled" for a settlement that succeeded.
	n, err := streams.ParseCount(streams.Int(5))
	if err != nil || n != 5 {
		t.Fatalf("ParseCount(5) = (%d, %v), want (5, nil)", n, err)
	}
	for _, v := range []streams.Value{streams.Nil(), streams.Str("5"), streams.Arr(streams.Int(5))} {
		n, err := streams.ParseCount(v)
		wantErr(t, err, streams.ErrReplyShape, "ParseCount("+v.Kind().String()+")")
		if n != 0 {
			t.Errorf("a refused reply also returned %d", n)
		}
	}
}

func TestXAutoClaimBuildsAndRefuses(t *testing.T) {
	t.Parallel()
	ok := streams.XAutoClaimArgs{Key: "k", Group: "g", Consumer: "c", Count: 10, MinIdle: 30 * time.Second}
	c, err := streams.XAutoClaim(ok)
	if err != nil {
		t.Fatalf("XAutoClaim: %v", err)
	}
	want := []string{"XAUTOCLAIM", "k", "g", "c", "30000", "0-0", "COUNT", "10"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q (an empty Start must default to the beginning)", c.Tokens, want)
	}
	if slices.Contains(c.Tokens, "JUSTID") {
		t.Error("JUSTID present with JustID unset: it would stop the delivery counter incrementing")
	}
	withJustID := ok
	withJustID.JustID = true
	withJustID.Start = "5-5"
	c, err = streams.XAutoClaim(withJustID)
	if err != nil {
		t.Fatalf("XAutoClaim JUSTID: %v", err)
	}
	want = []string{"XAUTOCLAIM", "k", "g", "c", "30000", "5-5", "COUNT", "10", "JUSTID"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q", c.Tokens, want)
	}
	for _, tc := range []struct {
		name string
		mut  func(*streams.XAutoClaimArgs)
	}{
		{"no key", func(a *streams.XAutoClaimArgs) { a.Key = "" }},
		{"no group", func(a *streams.XAutoClaimArgs) { a.Group = "" }},
		{"no consumer", func(a *streams.XAutoClaimArgs) { a.Consumer = "" }},
		{"zero count", func(a *streams.XAutoClaimArgs) { a.Count = 0 }},
		{"negative min-idle", func(a *streams.XAutoClaimArgs) { a.MinIdle = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := ok
			tc.mut(&a)
			c, err := streams.XAutoClaim(a)
			wantErr(t, err, streams.ErrBadArgs, "XAutoClaim")
			if len(c.Tokens) != 0 {
				t.Errorf("a refused command came back with tokens %q", c.Tokens)
			}
		})
	}
}

func TestParseXAutoClaimReadsTheCursorTheEntriesAndTheDeletedList(t *testing.T) {
	t.Parallel()
	reply := streams.Arr(
		streams.Str("7-2"),
		streams.Arr(entryVal("1-0", "f", "v")),
		streams.Arr(streams.Str("9-9")),
	)
	res, err := streams.ParseXAutoClaim(reply, 4, false, nil)
	if err != nil {
		t.Fatalf("ParseXAutoClaim: %v", err)
	}
	if res.Cursor != (streams.ID{Ms: 7, Seq: 2}) {
		t.Errorf("cursor = %v, want 7-2; a wrong cursor makes the sweep loop or skip", res.Cursor)
	}
	if len(res.Entries) != 1 || res.Entries[0].Shard != 4 {
		t.Fatalf("entries = %+v, want one on shard 4", res.Entries)
	}
	if !slices.Equal(res.Deleted, []streams.ID{{Ms: 9, Seq: 9}}) {
		t.Errorf("Deleted = %v, want [9-9]; Redis 7.0's third element is the PEL growth signal", res.Deleted)
	}
	// A 6.2 server sends two elements and no deleted list.
	res, err = streams.ParseXAutoClaim(
		streams.Arr(streams.Str("0-0"), streams.Arr(entryVal("1-0"))), 0, false, nil,
	)
	if err != nil {
		t.Fatalf("two-element reply: %v", err)
	}
	if res.Deleted != nil {
		t.Errorf("Deleted = %v, want nil on a 6.2 reply", res.Deleted)
	}
	// A null third element is the same. This case records the shape rather than
	// pinning the guard that reads it: ParseXAutoClaim's `!top[2].IsNil()` is
	// redundant, because parseIDs already answers (nil, nil) for a null, and
	// deleting the guard leaves this assertion passing. It is kept because the
	// reply shape is real (a server may send a null where 6.2 sent nothing),
	// not because it establishes the guard.
	res, err = streams.ParseXAutoClaim(
		streams.Arr(streams.Str("0-0"), streams.Arr(), streams.Nil()), 0, false, nil,
	)
	if err != nil {
		t.Fatalf("null third element: %v", err)
	}
	if res.Deleted != nil {
		t.Errorf("Deleted = %v, want nil for a null third element", res.Deleted)
	}
	// JUSTID fills JustIDs and not Entries.
	res, err = streams.ParseXAutoClaim(
		streams.Arr(streams.Str("0-0"), streams.Arr(streams.Str("3-1"), streams.Str("4-0"))), 0, true, nil,
	)
	if err != nil {
		t.Fatalf("JUSTID: %v", err)
	}
	if !slices.Equal(res.JustIDs, []streams.ID{{Ms: 3, Seq: 1}, {Ms: 4}}) {
		t.Errorf("JustIDs = %v, want [3-1 4-0]", res.JustIDs)
	}
	if len(res.Entries) != 0 {
		t.Errorf("Entries = %+v, want none under JUSTID", res.Entries)
	}
}

func TestParseXAutoClaimRefusesMalformedReplies(t *testing.T) {
	t.Parallel()
	entries := streams.Arr(entryVal("1-0"))
	for _, tc := range []struct {
		name   string
		reply  streams.Value
		justID bool
		want   error
	}{
		{"not an array", streams.Int(1), false, streams.ErrReplyShape},
		{"one element", streams.Arr(streams.Str("0-0")), false, streams.ErrReplyShape},
		{"the cursor is not a string", streams.Arr(streams.Int(0), entries), false, streams.ErrReplyShape},
		{"the cursor is unparseable", streams.Arr(streams.Str("-"), entries), false, streams.ErrBadID},
		{"the entries are not an array", streams.Arr(streams.Str("0-0"), streams.Int(1)), false, streams.ErrReplyShape},
		{"a JUSTID list is not an array", streams.Arr(streams.Str("0-0"), streams.Int(1)), true, streams.ErrReplyShape},
		{"a JUSTID element is not a string", streams.Arr(streams.Str("0-0"), streams.Arr(streams.Int(1))), true, streams.ErrReplyShape},
		{"a JUSTID element is unparseable", streams.Arr(streams.Str("0-0"), streams.Arr(streams.Str("nope"))), true, streams.ErrBadID},
		{"the deleted list is not an array", streams.Arr(streams.Str("0-0"), entries, streams.Int(1)), false, streams.ErrReplyShape},
		{"a deleted element is unparseable", streams.Arr(streams.Str("0-0"), entries, streams.Arr(streams.Str("?"))), false, streams.ErrBadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := streams.ParseXAutoClaim(tc.reply, 0, tc.justID, nil)
			wantErr(t, err, tc.want, "ParseXAutoClaim")
			if res.Cursor != (streams.ID{}) || res.Entries != nil || res.JustIDs != nil {
				t.Errorf("a refused reply came back with %+v; a caller that ignored the error would advance the cursor", res)
			}
		})
	}
}

func TestXClaimJustIDIsTheLeaseExtensionAndSaysSo(t *testing.T) {
	t.Parallel()
	// Two things here are the whole reason this builder exists rather than a
	// plain XCLAIM: the trailing JUSTID (which keeps the delivery counter from
	// incrementing, so extending a lease twenty times does not trip
	// MaxAttempts) and the min-idle of "0" (the entries are ours already, so
	// the extension must not be conditional on how long we have held them).
	ids := []streams.ID{{Ms: 1, Seq: 2}, {Ms: 3}}
	c, err := streams.XClaimJustID("k", "g", "c", ids)
	if err != nil {
		t.Fatalf("XClaimJustID: %v", err)
	}
	want := []string{"XCLAIM", "k", "g", "c", "0", "1-2", "3-0", "JUSTID"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q", c.Tokens, want)
	}
	if c.Tokens[len(c.Tokens)-1] != "JUSTID" {
		t.Error("JUSTID is not last; without it the claim increments the delivery counter and a slow handler is dead-lettered")
	}
	if c.Tokens[4] != "0" {
		t.Errorf("min-idle is %q, want 0: a conditional extension can refuse to extend our own lease", c.Tokens[4])
	}
	if !slices.Equal(c.Keys(), []string{"k"}) {
		t.Errorf("Keys() = %q, want [k]", c.Keys())
	}
	for _, tc := range []struct {
		name                 string
		key, group, consumer string
		ids                  []streams.ID
	}{
		{"no key", "", "g", "c", ids},
		{"no group", "k", "", "c", ids},
		{"no consumer", "k", "g", "", ids},
		{"no identifiers", "k", "g", "c", nil},
		{"an empty identifier slice", "k", "g", "c", []streams.ID{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.XClaimJustID(tc.key, tc.group, tc.consumer, tc.ids)
			wantErr(t, err, streams.ErrBadArgs, "XClaimJustID")
			if len(c.Tokens) != 0 {
				t.Errorf("a refused command came back with tokens %q", c.Tokens)
			}
		})
	}
}

func TestParseJustIDsTellsAnEmptyClaimFromAMalformedOne(t *testing.T) {
	t.Parallel()
	// An identifier missing from the reply is a lease that is GONE -- the entry
	// was acknowledged or deleted while we held it. An empty reply therefore
	// has to be an empty result and not an error, or every expired extension
	// would look like corruption.
	got, err := streams.ParseJustIDs(streams.Nil())
	if err != nil || got != nil {
		t.Errorf("ParseJustIDs(nil) = (%v, %v), want (nil, nil)", got, err)
	}
	got, err = streams.ParseJustIDs(streams.Arr())
	if err != nil || len(got) != 0 {
		t.Errorf("ParseJustIDs([]) = (%v, %v), want an empty list and no error", got, err)
	}
	got, err = streams.ParseJustIDs(streams.Arr(streams.Str("2-1")))
	if err != nil || !slices.Equal(got, []streams.ID{{Ms: 2, Seq: 1}}) {
		t.Errorf("ParseJustIDs = (%v, %v), want [2-1]", got, err)
	}
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"not an array", streams.Int(1), streams.ErrReplyShape},
		{"an element is not a string", streams.Arr(streams.Int(1)), streams.ErrReplyShape},
		{"an element is unparseable", streams.Arr(streams.Str("1-")), streams.ErrBadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := streams.ParseJustIDs(tc.reply)
			wantErr(t, err, tc.want, "ParseJustIDs")
			if got != nil {
				t.Errorf("a refused reply also returned %v", got)
			}
		})
	}
}

func TestXPendingBuildsTheOptionalFiltersItWasGiven(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args streams.XPendingArgs
		want []string
	}{
		{
			"empty bounds default to the whole range",
			streams.XPendingArgs{Key: "k", Group: "g", Count: 10},
			[]string{"XPENDING", "k", "g", "-", "+", "10"},
		},
		{
			"IDLE comes before the bounds",
			streams.XPendingArgs{Key: "k", Group: "g", Count: 10, Idle: 1500 * time.Microsecond},
			[]string{"XPENDING", "k", "g", "IDLE", "2", "-", "+", "10"},
		},
		{
			"a consumer filter comes last",
			streams.XPendingArgs{Key: "k", Group: "g", Count: 3, Consumer: "c"},
			[]string{"XPENDING", "k", "g", "-", "+", "3", "c"},
		},
		{
			"explicit bounds are used as given",
			streams.XPendingArgs{Key: "k", Group: "g", Count: 3, Start: "1-0", End: "2-0"},
			[]string{"XPENDING", "k", "g", "1-0", "2-0", "3"},
		},
		{
			"a zero Idle adds no IDLE filter",
			streams.XPendingArgs{Key: "k", Group: "g", Count: 1, Idle: 0},
			[]string{"XPENDING", "k", "g", "-", "+", "1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.XPending(tc.args)
			if err != nil {
				t.Fatalf("XPending: %v", err)
			}
			if !slices.Equal(c.Tokens, tc.want) {
				t.Errorf("tokens = %q, want %q", c.Tokens, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name string
		args streams.XPendingArgs
	}{
		{"no key", streams.XPendingArgs{Group: "g", Count: 1}},
		{"no group", streams.XPendingArgs{Key: "k", Count: 1}},
		{"zero count", streams.XPendingArgs{Key: "k", Group: "g"}},
		{"negative count", streams.XPendingArgs{Key: "k", Group: "g", Count: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.XPending(tc.args)
			wantErr(t, err, streams.ErrBadArgs, "XPending")
			if len(c.Tokens) != 0 {
				t.Errorf("a refused command came back with tokens %q", c.Tokens)
			}
		})
	}
}

func pendingRow(id, consumer string, idleMs, delivered int64) streams.Value {
	return streams.Arr(streams.Str(id), streams.Str(consumer), streams.Int(idleMs), streams.Int(delivered))
}

func TestParseXPendingReadsARowAndSaturatesTheAttemptCount(t *testing.T) {
	t.Parallel()
	got, err := streams.ParseXPending(streams.Arr(pendingRow("1-2", "c1", 1500, 3)))
	if err != nil {
		t.Fatalf("ParseXPending: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	want := streams.Pending{ID: streams.ID{Ms: 1, Seq: 2}, Consumer: "c1", Idle: 1500 * time.Millisecond, Delivered: 3}
	if got[0] != want {
		t.Errorf("row = %+v, want %+v", got[0], want)
	}
	// The saturation is load-bearing: clampCount's comment says a delivery
	// counter that WRAPPED to a small number would reset MaxAttempts
	// accounting, so a message that had been redelivered four billion times
	// would start again from attempt one and never dead-letter.
	got, err = streams.ParseXPending(streams.Arr(pendingRow("1-0", "c", 0, int64(math.MaxUint32)+5)))
	if err != nil {
		t.Fatalf("saturating row: %v", err)
	}
	if got[0].Delivered != math.MaxUint32 {
		t.Errorf("Delivered = %d, want %d: a wrapped counter resets MaxAttempts", got[0].Delivered, uint32(math.MaxUint32))
	}
	// A negative idle is clamped to zero rather than becoming a negative
	// duration, which would read as "idle in the future".
	got, err = streams.ParseXPending(streams.Arr(pendingRow("1-0", "c", -5, 1)))
	if err != nil {
		t.Fatalf("negative idle: %v", err)
	}
	if got[0].Idle != 0 {
		t.Errorf("Idle = %v, want 0 for a negative reply", got[0].Idle)
	}
	// A null reply is an empty pending list, not an error.
	got, err = streams.ParseXPending(streams.Nil())
	if err != nil || got != nil {
		t.Errorf("ParseXPending(nil) = (%v, %v), want (nil, nil)", got, err)
	}
}

func TestParseXPendingRefusesMalformedRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		reply streams.Value
		want  error
	}{
		{"not an array", streams.Int(1), streams.ErrReplyShape},
		{"a row is not an array", streams.Arr(streams.Int(1)), streams.ErrReplyShape},
		{"a row has three columns", streams.Arr(streams.Arr(streams.Str("1-0"), streams.Str("c"), streams.Int(0))), streams.ErrReplyShape},
		{"a row has five columns", streams.Arr(streams.Arr(streams.Str("1-0"), streams.Str("c"), streams.Int(0), streams.Int(1), streams.Int(2))), streams.ErrReplyShape},
		{"the identifier is not a string", streams.Arr(streams.Arr(streams.Int(1), streams.Str("c"), streams.Int(0), streams.Int(1))), streams.ErrReplyShape},
		{"the identifier is unparseable", streams.Arr(streams.Arr(streams.Str("a-b"), streams.Str("c"), streams.Int(0), streams.Int(1))), streams.ErrBadID},
		{"the consumer is not a string", streams.Arr(streams.Arr(streams.Str("1-0"), streams.Int(2), streams.Int(0), streams.Int(1))), streams.ErrReplyShape},
		{"the idle time is not an integer", streams.Arr(streams.Arr(streams.Str("1-0"), streams.Str("c"), streams.Str("0"), streams.Int(1))), streams.ErrReplyShape},
		{"the delivery count is not an integer", streams.Arr(streams.Arr(streams.Str("1-0"), streams.Str("c"), streams.Int(0), streams.Str("1"))), streams.ErrReplyShape},
		{"the delivery count is negative", streams.Arr(pendingRow("1-0", "c", 0, -1)), streams.ErrReplyShape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := streams.ParseXPending(tc.reply)
			wantErr(t, err, tc.want, "ParseXPending")
			if got != nil {
				t.Errorf("a refused reply also returned %+v", got)
			}
		})
	}
}

func TestWaitAOFIsTheDurableBoundaryAndItsReply(t *testing.T) {
	t.Parallel()
	c, err := streams.WaitAOF(1, 2, 1500*time.Microsecond)
	if err != nil {
		t.Fatalf("WaitAOF: %v", err)
	}
	want := []string{"WAITAOF", "1", "2", "2"}
	if !slices.Equal(c.Tokens, want) {
		t.Errorf("tokens = %q, want %q", c.Tokens, want)
	}
	for _, tc := range []struct {
		name            string
		local, replicas int
		timeout         time.Duration
	}{
		{"negative local", -1, 0, 0},
		{"negative replicas", 0, -1, 0},
		{"negative timeout", 0, 0, -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := streams.WaitAOF(tc.local, tc.replicas, tc.timeout)
			wantErr(t, err, streams.ErrBadArgs, "WaitAOF")
			if len(c.Tokens) != 0 {
				t.Errorf("a refused command came back with tokens %q", c.Tokens)
			}
		})
	}
	ack, err := streams.ParseWaitAOF(streams.Arr(streams.Int(1), streams.Int(3)))
	if err != nil {
		t.Fatalf("ParseWaitAOF: %v", err)
	}
	if ack != (streams.AOFAck{Local: 1, Replicas: 3}) {
		t.Errorf("ack = %+v, want {1 3}", ack)
	}
	// Reached is the confirmation boundary's verdict: reporting "reached" for a
	// write that did not reach the AOF is a durability claim the data does not
	// support.
	for _, tc := range []struct {
		ack             streams.AOFAck
		local, replicas int
		want            bool
	}{
		{streams.AOFAck{Local: 1, Replicas: 3}, 1, 2, true},
		{streams.AOFAck{Local: 1, Replicas: 3}, 1, 3, true},
		{streams.AOFAck{Local: 1, Replicas: 3}, 1, 4, false},
		{streams.AOFAck{Local: 0, Replicas: 9}, 1, 0, false},
		{streams.AOFAck{}, 0, 0, true},
	} {
		if got := tc.ack.Reached(tc.local, tc.replicas); got != tc.want {
			t.Errorf("%+v.Reached(%d, %d) = %v, want %v", tc.ack, tc.local, tc.replicas, got, tc.want)
		}
	}
	for _, tc := range []struct {
		name  string
		reply streams.Value
	}{
		{"not an array", streams.Int(1)},
		{"one element", streams.Arr(streams.Int(1))},
		{"three elements", streams.Arr(streams.Int(1), streams.Int(2), streams.Int(3))},
		{"the local count is not an integer", streams.Arr(streams.Str("1"), streams.Int(2))},
		{"the replica count is not an integer", streams.Arr(streams.Int(1), streams.Str("2"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ack, err := streams.ParseWaitAOF(tc.reply)
			wantErr(t, err, streams.ErrReplyShape, "ParseWaitAOF")
			if ack != (streams.AOFAck{}) {
				t.Errorf("a refused reply also returned %+v; a caller that ignored the error would claim durability", ack)
			}
		})
	}
}

func TestTheSmallBuildersSpellWhatTheyPromise(t *testing.T) {
	t.Parallel()
	ids := []streams.ID{{Ms: 1}, {Ms: 2, Seq: 3}}
	for _, tc := range []struct {
		name string
		got  streams.Cmd
		want []string
		keys []string
	}{
		{
			"XGROUP CREATE without MKSTREAM", streams.XGroupCreate("k", "g", streams.IDLast, false),
			[]string{"XGROUP", "CREATE", "k", "g", "$"},
			[]string{"k"},
		},
		{
			"XGROUP CREATE with MKSTREAM", streams.XGroupCreate("k", "g", streams.IDZero, true),
			[]string{"XGROUP", "CREATE", "k", "g", "0-0", "MKSTREAM"},
			[]string{"k"},
		},
		{
			"XGROUP SETID", streams.XGroupSetID("k", "g", "5-0"),
			[]string{"XGROUP", "SETID", "k", "g", "5-0"},
			[]string{"k"},
		},
		{
			"XGROUP DELCONSUMER", streams.XGroupDelConsumer("k", "g", "c"),
			[]string{"XGROUP", "DELCONSUMER", "k", "g", "c"},
			[]string{"k"},
		},
		{
			"XGROUP DESTROY", streams.XGroupDestroy("k", "g"),
			[]string{"XGROUP", "DESTROY", "k", "g"},
			[]string{"k"},
		},
		{
			"XACK", streams.XAck("k", "g", ids),
			[]string{"XACK", "k", "g", "1-0", "2-3"},
			[]string{"k"},
		},
		{"XLEN", streams.XLen("k"), []string{"XLEN", "k"}, []string{"k"}},
		{
			"XTRIM approximate", streams.XTrimMaxLen("k", 100, false),
			[]string{"XTRIM", "k", "MAXLEN", "~", "100"},
			[]string{"k"},
		},
		{
			"XTRIM exact", streams.XTrimMaxLen("k", 100, true),
			[]string{"XTRIM", "k", "MAXLEN", "100"},
			[]string{"k"},
		},
		{"XDEL", streams.XDel("k", ids), []string{"XDEL", "k", "1-0", "2-3"}, []string{"k"}},
		{
			"XRANGE without COUNT", streams.XRange("k", "-", "+", 0),
			[]string{"XRANGE", "k", "-", "+"},
			[]string{"k"},
		},
		{
			"XRANGE with COUNT", streams.XRange("k", "1-0", "2-0", 7),
			[]string{"XRANGE", "k", "1-0", "2-0", "COUNT", "7"},
			[]string{"k"},
		},
		{
			"EVAL declares its keys", streams.Eval("return 1", []string{"a", "b"}, []string{"x"}),
			[]string{"EVAL", "return 1", "2", "a", "b", "x"},
			[]string{"a", "b"},
		},
		{
			"EVALSHA declares its keys", streams.EvalSHA("deadbeef", []string{"a"}, []string{"x", "y"}),
			[]string{"EVALSHA", "deadbeef", "1", "a", "x", "y"},
			[]string{"a"},
		},
		{
			"EVAL with no keys", streams.Eval("return 1", nil, nil),
			[]string{"EVAL", "return 1", "0"},
			nil,
		},
		{
			"SCRIPT LOAD", streams.ScriptLoad("return 1"),
			[]string{"SCRIPT", "LOAD", "return 1"},
			nil,
		},
		{"ZADD", streams.ZAdd("k", -7, "m"), []string{"ZADD", "k", "-7", "m"}, []string{"k"}},
		{"ZCARD", streams.ZCard("k"), []string{"ZCARD", "k"}, []string{"k"}},
		{"PING", streams.Ping(), []string{"PING"}, nil},
		{"INFO server", streams.InfoServer(), []string{"INFO", "server"}, nil},
		{"EXISTS", streams.Exists("k"), []string{"EXISTS", "k"}, []string{"k"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !slices.Equal(tc.got.Tokens, tc.want) {
				t.Errorf("tokens = %q, want %q", tc.got.Tokens, tc.want)
			}
			if !tc.got.Valid() {
				t.Errorf("%q is not Valid", tc.got.Tokens)
			}
			if got := tc.got.Keys(); !slices.Equal(got, tc.keys) {
				t.Errorf("Keys() = %q, want %q; a wrong key span is a CROSSSLOT or a misroute on a cluster", got, tc.keys)
			}
			if tc.got.Name() != tc.want[0] {
				t.Errorf("Name() = %q, want %q", tc.got.Name(), tc.want[0])
			}
		})
	}
}

func TestCmdNameAndKeysOnACommandThatIsNotOne(t *testing.T) {
	t.Parallel()
	// Cmd is a value type, so a zero one reaches these accessors whenever a
	// caller ignores a builder's error. They must answer rather than panic --
	// and Keys must answer nil rather than a slice of the command's own verb,
	// which a cluster router would then hash.
	var zero streams.Cmd
	if zero.Name() != "" {
		t.Errorf("zero Cmd Name() = %q, want empty", zero.Name())
	}
	if zero.Keys() != nil {
		t.Errorf("zero Cmd Keys() = %q, want nil", zero.Keys())
	}
	if zero.Valid() {
		t.Error("a zero Cmd is Valid")
	}
	if (streams.Cmd{Tokens: []string{""}}).Valid() {
		t.Error("a command whose verb is the empty string is Valid")
	}
	for _, c := range []streams.Cmd{
		{Tokens: []string{"GET", "k"}, KeyStart: 1, KeyCount: 5},
		{Tokens: []string{"GET", "k"}, KeyStart: -1, KeyCount: 1},
		{Tokens: []string{"GET", "k"}, KeyStart: 2, KeyCount: 1},
	} {
		if got := c.Keys(); got != nil {
			t.Errorf("Keys() on an out-of-range span (%+v) = %q, want nil", c, got)
		}
		if c.Valid() {
			t.Errorf("a command with an out-of-range key span is Valid: %+v", c)
		}
	}
}

// TestSentinelErrorCodesAreDistinguishable pins the one thing the sentinels
// are for. The messages are not the assertion; the CODES are, because a driver
// maps a code onto a portable iqmq error and an operator sees that.
func TestSentinelErrorCodesAreDistinguishable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err  error
		want errcode.Code
	}{
		{streams.ErrReplyShape, errcode.CorruptMessage},
		{streams.ErrBadID, errcode.CorruptMessage},
		{streams.ErrBadArgs, errcode.InvalidArgument},
		{streams.ErrNoStream, errcode.NotFound},
	} {
		if got := errcode.CodeOf(tc.err); got != tc.want {
			t.Errorf("%v is %v, want %v", tc.err, got, tc.want)
		}
	}
	// ErrBadArgs is a caller's mistake and ErrReplyShape is the server's; a
	// driver that could not tell them apart would retry one of them for ever.
	if errors.Is(streams.ErrBadArgs, streams.ErrReplyShape) {
		t.Error("ErrBadArgs matches ErrReplyShape; a bad argument would be retried as a corrupt reply")
	}
	if errors.Is(streams.ErrNoStream, streams.ErrReplyShape) {
		t.Error("ErrNoStream matches ErrReplyShape; a missing topic would be reported as corruption")
	}
}

// TestClampCountBoundaryViaXPending walks the exact boundary of the saturating
// narrow, which is where an off-by-one would either wrap or clamp a legitimate
// count.
func TestClampCountBoundaryViaXPending(t *testing.T) {
	t.Parallel()
	for _, n := range []int64{0, 1, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1, math.MaxInt64} {
		got, err := streams.ParseXPending(streams.Arr(pendingRow("1-0", "c", 0, n)))
		if err != nil {
			t.Fatalf("delivered=%d: %v", n, err)
		}
		want := uint32(math.MaxUint32)
		if n < math.MaxUint32 {
			want = uint32(n)
		}
		if got[0].Delivered != want {
			t.Errorf("delivered=%d -> %d, want %d", n, got[0].Delivered, want)
		}
	}
	// And the spelling a reply actually carries, so the test does not depend on
	// Go's own integer literal handling alone.
	if _, err := strconv.ParseInt("4294967296", 10, 64); err != nil {
		t.Fatalf("the boundary spelling must parse: %v", err)
	}
}
