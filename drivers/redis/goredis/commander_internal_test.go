package goredis

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"

	redis "github.com/redis/go-redis/v9"

	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
	"bitbucket.org/iqhive/iqmq/internal/streams/respsrv"
)

// TestConvertRendersEveryReplyGoRedisCanProduce is the reply-parsing half of
// the adapter, held to every Go type go-redis's own reader can hand over.
//
// The list is not invented: it is exactly the return types of
// `proto.Reader.ReadReply` — string (simple, bulk and verbatim), int64,
// float64, bool, *big.Int, []any (array, set and push), map[any]any, and nil
// with [redis.Nil] for the null. A type missing from the switch would reach
// the driver as a corrupt-message error at run time rather than as a compile
// failure, so the switch is held to the list here.
func TestConvertRendersEveryReplyGoRedisCanProduce(t *testing.T) {
	cases := []struct {
		name string
		in   any
		err  error
		want string
	}{
		{name: "null-is-an-error-in-go-redis", err: redis.Nil, want: "nil"},
		{name: "untyped-nil", in: nil, want: "nil"},
		{name: "integer", in: int64(-7), want: "int(-7)"},
		{name: "boolean-true", in: true, want: "bool(true)"},
		{name: "boolean-false", in: false, want: "bool(false)"},
		{name: "string", in: "1767225600000-0", want: `str("1767225600000-0")`},
		{name: "binary-string", in: "a\r\n\x00b", want: `str("a\x0d\x0a\x00b")`},
		{name: "bytes", in: []byte{0, 0xff}, want: `str("\x00\xff")`},
		{name: "float", in: 1.5, want: `str("1.5")`},
		{name: "float-whole", in: float64(3), want: `str("3")`},
		{name: "big-number", in: big.NewInt(math.MaxInt64), want: `str("9223372036854775807")`},
		{name: "nil-big-number", in: (*big.Int)(nil), want: "nil"},
		{name: "empty-array", in: []any{}, want: "[]"},
		{name: "nested-array", in: []any{"k", []any{int64(1), nil}}, want: `[str("k") [int(1) nil]]`},
		{name: "one-pair-map", in: map[any]any{"length": int64(2)}, want: `[str("length") int(2)]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convert(tc.in, tc.err)
			if err != nil {
				t.Fatalf("convert(%#v, %v) failed: %v", tc.in, tc.err, err)
			}
			if r := renderValue(got); r != tc.want {
				t.Errorf("convert(%#v) = %s, want %s", tc.in, r, tc.want)
			}
		})
	}
}

// TestConvertRefusesWhatItCannotRender is the other half: a reply this adapter
// does not understand has to become an ERROR and not an empty value.
//
// A zero Value would be read by the command layer as a legitimate reply — an
// XADD that returned no identifier, an XAUTOCLAIM that claimed nothing — so a
// silent fall-through here is a driver that mis-reads the server rather than
// one that reports it cannot.
func TestConvertRefusesWhatItCannotRender(t *testing.T) {
	for _, in := range []any{
		int(3),                     // go-redis never returns a plain int; a switch on it would be a lie
		uint64(3),                  //
		map[string]string{"a": ""}, // nor a typed map
		struct{}{},
	} {
		if _, err := convert(in, nil); err == nil {
			t.Errorf("convert(%#v) accepted a reply type this adapter does not render", in)
		}
	}
}

// TestANestedErrorFailsTheWholeReply pins the one place the two client
// libraries genuinely differ in SHAPE rather than in text.
//
// go-redis puts a nested error reply into the slice as a VALUE where rueidis
// fails the whole reply, and the whole reply is the answer that matches: a
// reply with an error inside it is not data the command layer can read, and
// two clients that disagreed about whether it is would make one of them
// deliver a message built out of an error.
func TestANestedErrorFailsTheWholeReply(t *testing.T) {
	nested := []any{"1767225600000-0", redis.Nil, []any{"b", errors.New("ERR nope")}}
	if _, err := convert(nested, nil); err == nil {
		t.Fatal("a reply with an error nested inside it was rendered as data")
	}
}

// TestBuildRefusesACommandItCannotRoute is the cluster-safety bound.
//
// go-redis carries the first key's position in an int8, so a command whose
// first key sits beyond 127 tokens cannot be routed and would go to a RANDOM
// node — correct by accident on a standalone server and wrong on every
// cluster. No command this driver builds comes close, and a refusal says so
// rather than shipping a silent clamp.
func TestBuildRefusesACommandItCannotRoute(t *testing.T) {
	tokens := make([]string, 200)
	for i := range tokens {
		tokens[i] = "t"
	}
	tokens[0] = "EVALSHA"
	if _, err := build(context.Background(), streams.Cmd{Tokens: tokens, KeyStart: 150, KeyCount: 1}); err == nil {
		t.Fatal("build accepted a command whose first key is beyond the position go-redis can be told")
	}
	// The same command with its key inside the bound is built.
	if _, err := build(context.Background(), streams.Cmd{Tokens: tokens, KeyStart: 3, KeyCount: 1}); err != nil {
		t.Fatalf("build refused a command whose key is at token 3: %v", err)
	}
	// And a command with no keys at all needs no position.
	if _, err := build(context.Background(), streams.Cmd{Tokens: []string{"PING"}}); err != nil {
		t.Fatalf("build refused a keyless command: %v", err)
	}
}

// TestBuildCarriesEveryTokenInOrder holds the argument list itself. A token
// dropped or reordered is a different command.
func TestBuildCarriesEveryTokenInOrder(t *testing.T) {
	tokens := []string{"XADD", "{t}:0", "*", "b", "a\r\n\x00b", "h:trace", ""}
	built, err := build(context.Background(), streams.Cmd{Tokens: tokens, KeyStart: 1, KeyCount: 1})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	args := built.Args()
	if len(args) != len(tokens) {
		t.Fatalf("build produced %d arguments for %d tokens", len(args), len(tokens))
	}
	for i, tok := range tokens {
		s, ok := args[i].(string)
		if !ok {
			t.Errorf("argument %d is %T, want a string: a non-string argument is formatted by "+
				"go-redis and a formatted body is not the body", i, args[i])
			continue
		}
		if s != tok {
			t.Errorf("argument %d = %q, want %q", i, s, tok)
		}
	}
}

// TestNewCommanderRefusesANilClient keeps a nil client from becoming a panic
// on the first command instead of a refusal at construction.
func TestNewCommanderRefusesANilClient(t *testing.T) {
	if _, err := NewCommander(nil); err == nil {
		t.Fatal("NewCommander(nil) returned a commander")
	}
}

// TestABorrowedClientIsNotClosed is the specification's ownership rule at the seam that
// holds the client. The commander is what the backend closes, so a commander
// that closed a borrowed client would tear down a client the application is
// still using.
func TestABorrowedClientIsNotClosed(t *testing.T) {
	srv, err := respsrv.Serve(fakestreams.New(fakestreams.Options{}), respsrv.Options{})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	defer func() { _ = srv.Close() }()
	ctx := context.Background()

	// A borrowed commander is what [BackendWithClient] builds for
	// driverkit.ClientBorrowed; [NewCommander] is the owning form, and both
	// halves are here because the specification makes the distinction the caller's and a
	// commander that got it backwards would either tear down a client the
	// application is still using or leak the one it dialled itself.
	borrowed := redis.NewClient(&redis.Options{Addr: srv.Addr(), MaxRetries: -1})
	defer func() { _ = borrowed.Close() }()
	c := &commander{client: borrowed}
	if cerr := c.Close(); cerr != nil {
		t.Fatalf("Close of a borrowed client: %v", cerr)
	}
	if perr := borrowed.Ping(ctx).Err(); perr != nil {
		t.Fatalf("Close closed a client this commander does not own: %v", perr)
	}

	owned := &commander{client: redis.NewClient(&redis.Options{Addr: srv.Addr(), MaxRetries: -1}), own: true}
	if perr := owned.client.Ping(ctx).Err(); perr != nil {
		t.Fatalf("the owned client cannot reach the server: %v", perr)
	}
	if cerr := owned.Close(); cerr != nil {
		t.Fatalf("Close of an owned client: %v", cerr)
	}
	if perr := owned.client.Ping(ctx).Err(); !errors.Is(perr, redis.ErrClosed) {
		t.Fatalf("Close left an owned client open: %v", perr)
	}
}

// renderValue writes a Value out in full, for a failure message that shows
// which byte differed.
func renderValue(v streams.Value) string {
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
		return "str(" + quoteBytes(s) + ")"
	case streams.KindArray:
		items, _ := v.Arr()
		out := "["
		for i, item := range items {
			if i > 0 {
				out += " "
			}
			out += renderValue(item)
		}
		return out + "]"
	}
	return "?"
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

func quoteBytes(s string) string {
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
