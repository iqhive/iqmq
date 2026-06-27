package goredis

import (
	"context"
	"testing"

	redis "github.com/redis/go-redis/v9"

	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// redisNil is go-redis's null, which it reports as an error rather than as a
// value.
//
// It is declared with an explicit `error` type, and that is the difference
// between this test measuring the adapter and measuring itself: `redis.Nil` is
// a CONSTANT of a string type, so `var redisNil = redis.Nil` gives a
// string-typed variable that is boxed into an interface on every call — one
// allocation per iteration, charged to `convert`, which allocates none.
var redisNil error = redis.Nil

// builtCmd is where the measured build's result goes.
//
// It is a package-level variable and not a discard, and that is the whole
// difference between measuring this adapter and measuring a command no caller
// ever gets. `build` returns a `*redis.Cmd`; a loop that DISCARDS it lets the
// compiler inline `build`, prove the pointer never escapes and put the command
// on the stack, so the measurement came out one allocation short of what `Do`
// and `DoMulti` actually pay -- a 128-byte `redis.Cmd` per command, invisible.
// This variable forces the escape that production has, and costs nothing
// itself: a store to a package-level pointer allocates nothing.
var builtCmd *redis.Cmd

// TestAllocsAdapting pins what the adapter costs per command and per reply.
//
// It is a ceiling and not a zero, and the difference is the subject of variant
// the specification's "allocs" criterion. Go-redis takes a command as `...any`, so
// every token is BOXED — one allocation per token plus the slice plus the
// command object — where rueidis's builder appends strings to a pooled
// command. That is a property of the library's API rather than of this
// adapter, and it cannot be optimised away from here; what CAN regress is the
// adapter growing an allocation of its own, which is what these numbers are
// for.
//
// The ceilings are the measured counts plus nothing. A ceiling with slack in
// it is a check that cannot fail on a small regression, and a small regression
// per command is what a hot path notices.
func TestAllocsAdapting(t *testing.T) {
	ctx := context.Background()
	xadd := streams.XAdd(streams.XAddArgs{
		Key:    "{acme.orders}:0",
		Fields: []string{"b", "payload", "k", "order-4711", "t", "acme.orders"},
	})
	// The error is checked once, OUTSIDE the measured loop. A branch on the
	// returned error inside the loop would add an allocation of its own and
	// this test exists to notice one.
	if _, err := build(ctx, xadd); err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := int(testing.AllocsPerRun(200, func() {
		builtCmd, _ = build(ctx, xadd)
	})); got > 11 {
		t.Errorf("build of a nine-token XADD allocates %d times, want at most 11 "+
			"(one per boxed token, plus the argument slice, plus the command): %d", got, got)
	}
	// The same command with its body carried as a blob, which is what a
	// publication really looks like on this client. It
	// must cost no MORE than the copying form: the placeholder token is the
	// empty string, which boxes without allocating, and the second pass over
	// the arguments replaces it with the caller's slice.
	blobbed := streams.XAdd(streams.XAddArgs{
		Key:        "{acme.orders}:0",
		Fields:     []string{"b", "", "k", "order-4711", "t", "acme.orders"},
		FieldBlobs: []streams.Blob{{At: 1, Bytes: []byte("payload")}},
	})
	if !blobbed.Valid() {
		t.Fatalf("the blob-carrying XADD is not Valid: %q %v", blobbed.Tokens, blobbed.Blobs)
	}
	if got := int(testing.AllocsPerRun(200, func() {
		builtCmd, _ = build(ctx, blobbed)
	})); got > 11 {
		t.Errorf("build of a nine-token XADD carrying its body as a blob allocates %d times, "+
			"want at most 11 — the same as the copying form", got)
	}

	// A fetch's reply: one stream, two entries, six fields each. Converting
	// it allocates one []Value per nesting level and one Value per element,
	// and nothing per byte: the strings are the client's own.
	reply := []any{
		[]any{"{acme.orders}:0", []any{
			[]any{"1767225600000-0", []any{"b", "payload", "k", "order-4711", "t", "acme.orders"}},
			[]any{"1767225600000-1", []any{"b", "payload", "k", "order-4712", "t", "acme.orders"}},
		}},
	}
	if _, err := convert(reply, nil); err != nil {
		t.Fatalf("convert: %v", err)
	}
	if got := int(testing.AllocsPerRun(200, func() {
		_, _ = convert(reply, nil)
	})); got > 7 {
		t.Errorf("convert of a two-entry fetch reply allocates %d times, want at most 7 "+
			"(one []Value per nesting level, and nothing per byte)", got)
	}

	// The null path must not allocate at all: an idle consumer's XREADGROUP
	// returns one of these every BLOCK, for ever.
	if _, err := convert(nil, redisNil); err != nil {
		t.Fatalf("convert of a null reply: %v", err)
	}
	if got := int(testing.AllocsPerRun(500, func() {
		_, _ = convert(nil, redisNil)
	})); got != 0 {
		t.Errorf("convert of a null reply allocates %d times, want 0: an idle consumer takes this "+
			"path once per BLOCK for ever", got)
	}
}
