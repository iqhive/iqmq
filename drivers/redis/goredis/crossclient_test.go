package goredis_test

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestInfoNamesTheClientLibraryBehindTheDriver is the honesty rule applied
// to the one field a shared driver cannot know statically.
//
// `drivers/redis` is written against [streams.Commander] and is opened over
// rueidis, over go-redis and over an in-memory server. A hard-coded
// ClientName is therefore a claim that is wrong two ways out of three, and the specification makes an EMPTY field "the driver did not say" while a populated one is
// a claim — so the claim has to come from the commander that knows.
func TestInfoNamesTheClientLibraryBehindTheDriver(t *testing.T) {
	r := newRig(t)
	info := r.b.Info()
	if info.ClientName != goredis.ClientModule {
		t.Errorf("Info().ClientName = %q, want %q", info.ClientName, goredis.ClientModule)
	}
	if info.ClientVersion != goredis.ClientVersion {
		t.Errorf("Info().ClientVersion = %q, want %q", info.ClientVersion, goredis.ClientVersion)
	}
	if info.Name != "redis" {
		t.Errorf("Info().Name = %q, want %q: this is the same driver, behind another client", info.Name, "redis")
	}
	if info.BrokerVersion != "7.4.2" {
		t.Errorf("Info().BrokerVersion = %q, want the version the server reported", info.BrokerVersion)
	}
}

// errorReplyCases are server replies whose iqmq code the driver table
// decides, chosen so that every one of them is reachable by sending a command.
var errorReplyCases = []struct {
	name   string
	tokens []string
	want   mq.ErrorCode
}{
	// The condition word carries no "ERR ": both clients present it
	// identically, so these are the controls.
	{"nogroup", []string{"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "nosuch", ">"}, mq.CodeNotFound},
	// A generic "ERR ..." reply. rueidis strips the prefix and go-redis keeps
	// it, so a table matching on "ERR wrong number of arguments" fires for
	// one client and not the other.
	{"wrong-arity", []string{"XACK"}, mq.CodeInvalidArgument},
	{"unknown-command", []string{"NOSUCHCOMMAND"}, mq.CodeUnsupported},
	{"bad-stream-id", []string{"XADD", "s", "not-an-id", "b", "x"}, mq.CodeInvalidArgument},
}

// TestBothClientsMapOneServerReplyToOneCode is the cross-client equivalence
// this whole module exists to make checkable.
//
// The driver, the command layer, the error table and the SERVER are identical
// for the two arms; the only difference is the client library. So a reply that
// comes out as two different iqmq codes is a divergence in the contract the specification
// defines, and the caller's error handling — which branches on the code —
// would behave differently depending on a library choice the specification calls an
// implementation detail.
func TestBothClientsMapOneServerReplyToOneCode(t *testing.T) {
	r := newRig(t)
	clients := map[string]streams.Commander{
		"goredis": goredisCommander(t, r.srv.Addr()),
		"rueidis": rueidisCommander(t, r.srv.Addr()),
	}
	for _, tc := range errorReplyCases {
		t.Run(tc.name, func(t *testing.T) {
			codes := map[string]mq.ErrorCode{}
			for name, cmder := range clients {
				_, err := cmder.Do(r.ctx, streams.Cmd{Tokens: tc.tokens})
				if err == nil {
					t.Fatalf("%s: %v produced no error", name, tc.tokens)
				}
				mapped := rd.MapForTest(r.b, mq.OpFetch, err)
				codes[name] = mq.CodeOf(mapped)
				t.Logf("%-7s reply %q -> %v", name, err.Error(), codes[name])
			}
			if codes["goredis"] != codes["rueidis"] {
				t.Errorf("the two clients disagree: goredis -> %v, rueidis -> %v",
					codes["goredis"], codes["rueidis"])
			}
			for name, got := range codes {
				if got != tc.want {
					t.Errorf("%s: code = %v, want %v", name, got, tc.want)
				}
			}
		})
	}
}

// arm is one client library behind one in-memory server of its own.
//
// Each arm gets a SEPARATE server, which matters more than it looks: the
// interesting commands are destructive. `XREADGROUP ... >` delivers each entry
// once, so two clients reading one server would compare the first client's
// entries against the second's empty reply and call that a divergence. Two
// servers seeded by the same script compare like for like, and the servers'
// own state afterwards is then a second, stronger assertion: it says the two
// clients sent the same BYTES, not merely that they read the same replies.
type arm struct {
	name  string
	cmder streams.Commander
	fake  *fakestreams.Server
}

// newArms returns one arm per shipped client library, in a fixed order.
func newArms(t testing.TB) []arm {
	t.Helper()
	gfake, gsrv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	rfake, rsrv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	return []arm{
		{name: "goredis", cmder: goredisCommander(t, gsrv.Addr()), fake: gfake},
		{name: "rueidis", cmder: rueidisCommander(t, rsrv.Addr()), fake: rfake},
	}
}

// TestBothClientsRenderOneReplyAsOneValue holds the two adapters to the same
// [streams.Value] for every reply shape a stream command produces.
//
// The reply-parsing half of an adapter is the part that has to be identical
// and the part a second copy gets wrong: a null that became an empty array, a
// bulk string that lost its bytes, an integer read as a string. The script
// below is one command per reply shape the five [streams.ValueKind]s cover,
// plus the two nestings that appear in a real fetch.
func TestBothClientsRenderOneReplyAsOneValue(t *testing.T) {
	r := newRig(t)
	keys, err := streams.NewKeys("shapes", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	stream := keys.Stream(0)
	script := [][]string{
		{"PING"},
		{"XLEN", "nosuchstream"},
		{"EXISTS", "nosuchstream"},
		{"XRANGE", "nosuchstream", "-", "+"},
		{"XGROUP", "CREATE", stream, "g", "0", "MKSTREAM"},
		{"XADD", stream, "*", "b", "one\r\n\x00two", "k", "key"},
		{"XADD", stream, "*", "b", ""},
		{"XLEN", stream},
		{"XRANGE", stream, "-", "+"},
		{"XINFO", "GROUPS", stream},
		{"XPENDING", stream, "g"},
		{"XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", stream, ">"},
		{"XPENDING", stream, "g", "-", "+", "10"},
		{"XINFO", "CONSUMERS", stream, "g"},
		{"XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "BLOCK", "1", "STREAMS", stream, ">"},
		{"XACK", stream, "g", "1767225600000-0"},
		{"CONFIG", "GET", "appendonly"},
		{"WAITAOF", "0", "0", "1"},
		{"ZCARD", "nosuchzset"},
		{"XACK"},
		{"NOSUCHCOMMAND"},
	}
	arms := newArms(t)
	for step, tokens := range script {
		rendered := make([]string, len(arms))
		failed := make([]string, len(arms))
		for i, a := range arms {
			v, derr := a.cmder.Do(r.ctx, streams.Cmd{Tokens: tokens})
			rendered[i] = render(v)
			if derr != nil {
				// Only whether a reply WAS an error is compared here; the
				// text differs by design and its mapping is the subject of
				// TestBothClientsMapOneServerReplyToOneCode.
				failed[i] = "error"
			}
		}
		if failed[0] != failed[1] {
			t.Errorf("step %d %v: %s says %q and %s says %q",
				step, tokens, arms[0].name, failed[0], arms[1].name, failed[1])
			continue
		}
		if failed[0] != "" {
			continue
		}
		if rendered[0] != rendered[1] {
			t.Errorf("step %d %v rendered differently:\n %s: %s\n %s: %s",
				step, tokens, arms[0].name, rendered[0], arms[1].name, rendered[1])
		}
	}
	// The servers' own state is the other half: two clients that read the
	// same replies could still have sent different commands.
	if got, want := renderEntries(arms[0].fake, stream), renderEntries(arms[1].fake, stream); got != want {
		t.Errorf("the two servers hold different entries:\n %s: %s\n %s: %s",
			arms[0].name, got, arms[1].name, want)
	}
	if got, want := renderPending(arms[0].fake, stream), renderPending(arms[1].fake, stream); got != want {
		t.Errorf("the two servers hold different pending rows:\n %s: %s\n %s: %s",
			arms[0].name, got, arms[1].name, want)
	}
}

// renderEntries writes a stream's entries out in full.
func renderEntries(fake *fakestreams.Server, key string) string {
	out := ""
	for _, e := range fake.Entries(key) {
		out += e.ID.String() + "{"
		for i := 0; i+1 < len(e.Fields); i += 2 {
			out += quote(e.Fields[i]) + "=" + quote(e.Fields[i+1]) + " "
		}
		out += "} "
	}
	return out
}

// renderPending writes a group's pending rows out, consumer included: the
// consumer name is what the guarded acknowledgement compares.
func renderPending(fake *fakestreams.Server, key string) string {
	out := ""
	for _, p := range fake.Pending(key, "g") {
		out += p.ID.String() + "@" + p.Consumer + "x" + itoa(int64(p.Delivered)) + " "
	}
	return out
}

// TestAMapReplyIsFlattenedInPairsByBothClients is the one shape whose ORDER
// the two clients cannot be held to, stated as what CAN be held.
//
// A RESP3 map reaches a Go map in both libraries, so the pair order is the map
// iteration's and is not stable -- which is acceptable for the one place a map
// reply reaches this driver (XINFO, read by name) and is why neither adapter
// may use a map reply for anything ordered. What the test holds is that both
// flatten to field/value PAIRS and that a field's own value is beside it.
func TestAMapReplyIsFlattenedInPairsByBothClients(t *testing.T) {
	r := newRig(t)
	keys, err := streams.NewKeys("mapshape", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	stream := keys.Stream(0)
	for _, a := range newArms(t) {
		if _, serr := a.cmder.Do(r.ctx, streams.Cmd{Tokens: []string{"XGROUP", "CREATE", stream, "g", "0", "MKSTREAM"}}); serr != nil {
			t.Fatalf("%s: setup: %v", a.name, serr)
		}
		v, derr := a.cmder.Do(r.ctx, streams.Cmd{Tokens: []string{"XINFO", "GROUPS", stream}})
		if derr != nil {
			t.Fatalf("%s: XINFO GROUPS: %v", a.name, derr)
		}
		groups, ok := v.Arr()
		if !ok || len(groups) != 1 {
			t.Fatalf("%s: XINFO GROUPS rendered %v (%d elements), want one group", a.name, v.Kind(), len(groups))
		}
		items, ok := groups[0].Arr()
		if !ok {
			t.Fatalf("%s: the group rendered %v, want an array", a.name, groups[0].Kind())
		}
		if len(items)%2 != 0 {
			t.Fatalf("%s: the group rendered %d elements, which cannot be field/value pairs", a.name, len(items))
		}
		found := false
		for i := 0; i+1 < len(items); i += 2 {
			if s, _ := items[i].Str(); s == "pending" {
				found = true
				if _, isInt := items[i+1].Int64(); !isInt {
					t.Errorf("%s: \"pending\" is followed by %v, want the integer that belongs to it", a.name, items[i+1].Kind())
				}
			}
		}
		if !found {
			t.Errorf("%s: XINFO GROUPS rendered no \"pending\" field", a.name)
		}
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	var buf [24]byte
	i := len(buf)
	for n != 0 {
		i--
		d := n % 10
		if d < 0 {
			d = -d
		}
		buf[i] = byte('0' + d)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func quote(s string) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := range len(s) {
		c := s[i]
		if c < 0x20 || c > 0x7e || c == '"' || c == '\\' {
			out = append(out, '\\', 'x', hex[c>>4], hex[c&0xf])
			continue
		}
		out = append(out, c)
	}
	return string(append(out, '"'))
}

var _ = errors.Is

// render writes a Value out in full, bodies included, so that a difference in
// a single byte of a single field fails the comparison.
//
// The specification forbids a payload in anything the LIBRARY reports; a test's own
// failure message is not that, and a comparison that elided the bodies would
// not be able to see the difference it exists to find.
func render(v streams.Value) string {
	switch v.Kind() {
	case streams.KindNil:
		return "nil"
	case streams.KindInt:
		n, _ := v.Int64()
		return "int(" + itoa(n) + ")"
	case streams.KindBool:
		b, _ := v.Bool()
		if b {
			return "bool(true)"
		}
		return "bool(false)"
	case streams.KindString:
		s, _ := v.Str()
		return "str(" + quote(s) + ")"
	case streams.KindArray:
		items, _ := v.Arr()
		out := "["
		for i, item := range items {
			if i > 0 {
				out += " "
			}
			out += render(item)
		}
		return out + "]"
	}
	return "?"
}
