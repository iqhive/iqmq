// Package streams is the Redis Streams command layer: the key naming, the
// entry field layout, the commands the drivers send and the replies they
// read, the pending-entries reclaimer, and the negative-settlement strategies
// (the driver documentation).
//
// The seam is [Commander]: one method that sends a command and returns a
// reply. It is deliberately the RAW seam rather than a typed one. rueidis
// spells a command as `client.B().Arbitrary(tokens...).Keys(...).Build()` and
// go-redis as `client.Do(ctx, args...)`, so an adapter over either is a few
// lines; a typed interface with thirty methods would put the reply parsing
// inside each adapter, which is exactly the part that has to be identical and
// the part [FuzzRedisFields] exists to hold.
//
// Nothing here does I/O, reads a clock or starts a goroutine. The package is
// standard-library-only, which is what lets it live in the root module beside
// the SPI.
package streams

import (
	"context"
	"strconv"
	"strings"
)

// Cmd is one Redis command: its tokens in wire order, plus which of them are
// keys.
//
// The key span is not decoration. A cluster client has to hash the key tokens
// to pick a node, and it cannot find them by position: XADD's key is token 1,
// XREADGROUP's keys are everything between STREAMS and the first identifier,
// and EVAL's are the numkeys tokens after the script. Every command this
// package builds has its keys in one contiguous run, so a start and a count
// describe all of them — and a command built with the wrong span is a command
// a cluster sends to the wrong node, which is why [Cmd.Keys] is what the
// adapters read rather than a guess from the verb.
type Cmd struct {
	// Tokens are the command's words in wire order, the verb first. Values
	// are arbitrary bytes carried as a string: RESP bulk strings are
	// binary-safe, so a header value with a NUL in it survives.
	Tokens []string
	// KeyStart is the index in Tokens of the first key token.
	KeyStart int
	// KeyCount is how many key tokens there are.
	KeyCount int
	// Blocking reports that the command may park at the server (XREADGROUP
	// with BLOCK). rueidis routes a blocking command to a separate
	// connection pool, and a client that did not would stall every
	// pipelined command behind it.
	Blocking bool
	// Blobs names the tokens whose value is carried as a []byte the CALLER
	// still owns, rather than as a string in Tokens. Each entry's Tokens
	// slot is the empty string and carries no value at all.
	//
	// It exists because the one token that can be megabytes -- a message
	// body -- is a []byte the caller owns, and `string(body)` copies every
	// byte of it. A client that can write a token from a
	// []byte without retaining it takes the blob and copies nothing; every
	// other reader calls [Cmd.Text] or [Cmd.Materialize] and gets exactly
	// the string it would have got before, which is what makes the fast path
	// an optimisation rather than a second encoding.
	//
	// A Cmd carrying blobs is built ONLY for a commander that answered true
	// to [ArgAliaser.AliasArgs], because a commander that ignored the field
	// would send an empty body and store a corrupt entry rather than fail.
	// Entries are ordered by At, At is never a key token, and every At is
	// inside Tokens: [Cmd.Valid] refuses a Cmd that breaks any of that.
	Blobs []Blob
}

// Blob is one command token carried as bytes.
//
// Bytes ALIASES the caller's memory: it is valid for exactly as long as the
// Do or DoMulti call it was passed to, and a commander that reads it after
// that call returns reads whatever the caller wrote next. See [ArgAliaser]
// for the contract that makes a blob safe to hand over at all.
type Blob struct {
	// At is the index in [Cmd.Tokens] of the token this blob carries.
	At int
	// Bytes is the token's value. A nil Bytes is an empty token, which is
	// what a zero-length body encodes to.
	Bytes []byte
}

// Name returns the command's verb, or "" for an empty command.
func (c Cmd) Name() string {
	if len(c.Tokens) == 0 {
		return ""
	}
	return c.Tokens[0]
}

// Keys returns the key tokens.
func (c Cmd) Keys() []string {
	if c.KeyCount <= 0 || c.KeyStart < 0 || c.KeyStart+c.KeyCount > len(c.Tokens) {
		return nil
	}
	return c.Tokens[c.KeyStart : c.KeyStart+c.KeyCount]
}

// Valid reports whether c names a command, its key span is inside its tokens,
// and its blob set is well formed.
func (c Cmd) Valid() bool {
	if len(c.Tokens) == 0 || c.Tokens[0] == "" {
		return false
	}
	if !c.blobsValid() {
		return false
	}
	if c.KeyCount == 0 {
		return c.KeyStart >= 0
	}
	return c.KeyCount > 0 && c.KeyStart >= 0 && c.KeyStart+c.KeyCount <= len(c.Tokens)
}

// blobsValid holds the blob set to the five things every reader of it assumes.
//
// The verb is never a blob (a command whose name arrives as bytes has no name
// until someone materialises it, and [Cmd.Name] is read before anything else);
// a key is never a blob (a cluster client hashes the key tokens, and both
// client libraries want them as strings); the indices rise strictly, so one
// pass over Tokens can walk the blobs beside it; and the Tokens slot of a blob
// is empty, so a reader that ignores the blob set sends a token that is
// visibly absent rather than a plausible wrong value.
func (c Cmd) blobsValid() bool {
	prev := 0
	for i, b := range c.Blobs {
		if b.At <= 0 || b.At >= len(c.Tokens) {
			return false
		}
		if i > 0 && b.At <= prev {
			return false
		}
		prev = b.At
		if c.Tokens[b.At] != "" {
			return false
		}
		if c.KeyCount > 0 && b.At >= c.KeyStart && b.At < c.KeyStart+c.KeyCount {
			return false
		}
	}
	return true
}

// Arg returns token i as a string and as bytes, exactly one of which carries
// the value: blob is non-nil for a blob token and nil for every other, and
// text is the token for every other and empty for a blob.
//
// It is what a commander whose client can write a []byte without retaining it
// reads; a commander that cannot calls [Cmd.Text].
//
// A blob with NO bytes is the one case where the two returns are the same
// answer, and it needs no sentinel: a blob token's Tokens slot is the empty
// string, so a caller that sees a nil blob and writes text writes exactly the
// empty bulk string the bytes would have produced. An earlier version returned
// an empty non-nil slice to keep the cases apart and cost one heap allocation
// per token on the whole publish path, because the literal escaped into the
// inlined caller whether that branch was taken or not.
func (c Cmd) Arg(i int) (text string, blob []byte) {
	if len(c.Blobs) != 0 {
		if b, ok := c.blobAt(i); ok {
			return "", b
		}
	}
	return c.Tokens[i], nil
}

// Text returns token i as a string, COPYING a blob token's bytes.
//
// It is the safe fallback of the whole blob mechanism :
// every reader that wants a []string -- the in-memory server, the rueidis
// adapter, a test comparing two encodings -- gets byte-for-byte what it got
// before blobs existed, at the cost of the copy blobs exist to avoid. A
// reader that wants the fast path asks [Cmd.Arg] instead.
func (c Cmd) Text(i int) string {
	if len(c.Blobs) != 0 {
		if b, ok := c.blobAt(i); ok {
			return string(b)
		}
	}
	return c.Tokens[i]
}

// blobAt reports whether token i is a blob, and its bytes if so.
//
// It is a linear scan because a command carries at most one blob today and the
// slice is ordered: a map would allocate on every command to answer a question
// with one candidate answer.
func (c Cmd) blobAt(i int) ([]byte, bool) {
	for _, b := range c.Blobs {
		if b.At == i {
			return b.Bytes, true
		}
		if b.At > i {
			break
		}
	}
	return nil, false
}

// Materialize returns c with every blob copied into Tokens and Blobs empty.
//
// A Cmd with no blobs is returned unchanged and allocates nothing, which is
// what lets a reader call it unconditionally. A Cmd WITH blobs is copied --
// tokens slice included -- because c.Tokens is the caller's and a reader that
// wrote the materialised value into it would change the command the caller
// still holds.
func (c Cmd) Materialize() Cmd {
	if len(c.Blobs) == 0 {
		return c
	}
	toks := make([]string, len(c.Tokens))
	copy(toks, c.Tokens)
	for _, b := range c.Blobs {
		toks[b.At] = string(b.Bytes)
	}
	return Cmd{Tokens: toks, KeyStart: c.KeyStart, KeyCount: c.KeyCount, Blocking: c.Blocking}
}

// ValueKind is the shape of a reply.
type ValueKind uint8

// The reply shapes the stream commands produce. There is no separate map
// kind: Redis returns an entry's fields as a flat array even under RESP3, and
// a layer that turned them into a Go map would lose the order the field
// layout depends on.
const (
	// KindNil is a null reply: an XREADGROUP that timed out, an XADD that
	// NOMKSTREAM refused.
	KindNil ValueKind = iota
	// KindInt is an integer reply.
	KindInt
	// KindString is a bulk or simple string reply.
	KindString
	// KindArray is an array reply.
	KindArray
	// KindBool is RESP3's boolean, which WAITAOF's siblings can return.
	KindBool

	valueKindCount
)

var valueKindNames = [valueKindCount]string{"nil", "int", "string", "array", "bool"}

// String returns the kind's lower-case name.
func (k ValueKind) String() string {
	if k < valueKindCount {
		return valueKindNames[k]
	}
	return "ValueKind(?)"
}

// Valid reports whether k names a shape.
func (k ValueKind) Valid() bool { return k < valueKindCount }

// Value is one decoded reply.
//
// It is a value type with no interface boxing, so a fetch of a hundred
// entries costs one slice of Values per nesting level rather than a hundred
// interface conversions. An adapter builds it from its client's own reply
// type; [Nil], [Int], [Str] and [Arr] are the constructors.
type Value struct {
	kind ValueKind
	num  int64
	str  string
	arr  []Value
}

// Nil returns the null reply.
func Nil() Value { return Value{kind: KindNil} }

// Int returns an integer reply.
func Int(n int64) Value { return Value{kind: KindInt, num: n} }

// Bool returns a boolean reply.
func Bool(b bool) Value {
	v := Value{kind: KindBool}
	if b {
		v.num = 1
	}
	return v
}

// Str returns a string reply.
func Str(s string) Value { return Value{kind: KindString, str: s} }

// Arr returns an array reply. The slice is retained, not copied: a reply is
// read once and discarded, and copying every level of an XREADGROUP reply
// would allocate the whole fetch again.
func Arr(vs ...Value) Value { return Value{kind: KindArray, arr: vs} }

// ArrOf returns an array reply over an existing slice.
func ArrOf(vs []Value) Value { return Value{kind: KindArray, arr: vs} }

// Kind returns the reply's shape.
func (v Value) Kind() ValueKind { return v.kind }

// IsNil reports whether the reply is null.
func (v Value) IsNil() bool { return v.kind == KindNil }

// Int64 returns the integer, and false for any other shape. A string that
// holds a decimal integer is NOT accepted: Redis distinguishes the two and a
// layer that did not would read a stream identifier as a number.
func (v Value) Int64() (int64, bool) {
	if v.kind == KindInt {
		return v.num, true
	}
	return 0, false
}

// Bool returns the boolean, accepting RESP2's integer encoding of it.
func (v Value) Bool() (bool, bool) {
	switch v.kind {
	case KindBool, KindInt:
		return v.num != 0, true
	case KindNil, KindString, KindArray, valueKindCount:
		return false, false
	}
	return false, false
}

// Str returns the string, and false for any other shape.
func (v Value) Str() (string, bool) {
	if v.kind == KindString {
		return v.str, true
	}
	return "", false
}

// Arr returns the array elements, and false for any other shape. A null
// reply is not an empty array: an XREADGROUP that timed out and one that
// returned no entries are different answers.
func (v Value) Arr() ([]Value, bool) {
	if v.kind == KindArray {
		return v.arr, true
	}
	return nil, false
}

// Len returns the number of elements of an array reply, the length of a
// string, and zero otherwise.
func (v Value) Len() int {
	switch v.kind {
	case KindArray:
		return len(v.arr)
	case KindString:
		return len(v.str)
	case KindNil, KindInt, KindBool, valueKindCount:
		return 0
	}
	return 0
}

// String renders the reply for a failure message. It never renders an
// entry's body: the specification forbids a payload in anything iqmq reports, and a
// stream entry's fields are the payload.
func (v Value) String() string {
	switch v.kind {
	case KindNil:
		return "nil"
	case KindInt:
		return "int(" + strconv.FormatInt(v.num, 10) + ")"
	case KindBool:
		if v.num != 0 {
			return "bool(true)"
		}
		return "bool(false)"
	case KindString:
		return "string(" + strconv.Itoa(len(v.str)) + " bytes)"
	case KindArray:
		return "array(" + strconv.Itoa(len(v.arr)) + " elements)"
	case valueKindCount:
	}
	return "ValueKind(?)"
}

// Result is one command's outcome inside a pipeline: a reply or the error
// that command alone failed with.
//
// A pipeline is not a transaction. Redis executes the commands of a pipeline
// independently and reports each one's outcome, so a batch of ten XACKs may
// have one failure and nine successes — which is exactly the per-item result
// the SPI's batch contract needs, and the reason [Commander.DoMulti]
// returns a slice index-aligned with what it was given rather than a single
// error.
type Result struct {
	// Value is the reply, valid when Err is nil.
	Value Value
	// Err is why this command failed, and nil when it did not.
	Err error
}

// Commander sends commands to a Redis-compatible server.
//
// It is the whole of what this package needs from a client library, and the
// reason it is this small: `drivers/redis` implements it over rueidis and
// `drivers/redis/goredis` will implement it over go-redis's UniversalClient,
// and every decision about correctness — which command, which fields, which
// guard before an ack — is above the interface rather than inside each
// adapter.
//
// An implementation must be safe for concurrent use, must not retain a Cmd
// past the call, and must return a [Value] whose strings are independent of
// the client's own buffers: the caller keeps an entry's body until the
// delivery is settled.
type Commander interface {
	// Do sends one command and returns its reply. A Redis error reply is an
	// error, not a Value: an adapter maps it to something errors.Is can
	// match, and [IsNoGroup] and its siblings recognise the conditions this
	// package has to branch on.
	Do(ctx context.Context, c Cmd) (Value, error)
	// DoMulti pipelines cmds and returns one Result per command, in order.
	// The slice is always len(cmds) long when err is nil; a non-nil err
	// means no command's outcome is known.
	DoMulti(ctx context.Context, cmds []Cmd) ([]Result, error)
	// Close releases the client. It is idempotent.
	Close() error
}

// Pinger is the optional liveness surface: a Commander that can answer
// the API's Ping more cheaply than by building a command.
type Pinger interface {
	// Ping round-trips with the server.
	Ping(ctx context.Context) error
}

// ServerInfo is the optional version surface. The capability table branches
// on the server's version — WAITAOF needs 7.0, XAUTOCLAIM 6.2 — so a driver
// that could not ask would either claim what it may not have or claim
// nothing.
type ServerInfo interface {
	// ServerVersion returns the server's version string ("7.4.2") and the
	// software it reports ("redis" or "valkey"), both empty when unknown.
	ServerVersion(ctx context.Context) (version, software string, err error)
}

// ClientLibrary is the optional client-identity surface.
type ClientLibrary interface {
	// ClientLibrary returns the client library's module path and version,
	// both empty when the commander is not built on one.
	ClientLibrary() (module, version string)
}

// ArgAliaser is the optional surface that says a commander may be handed a
// [Cmd] carrying [Blob] tokens -- the tokens whose bytes are the caller's and
// are never copied.
//
// It is a statement about the CLIENT LIBRARY and not about the adapter, and it
// is a strong one: every byte of every token must be read before Do or DoMulti
// returns, and never afterwards. That has to hold on the ordinary path, on a
// retry, after a reconnection, and -- the case that decides it -- when the
// context is cancelled while the command is in flight. the message contract
// lets the caller reuse its buffer the instant the publish call returns, so a
// library that writes the command later writes whatever the caller wrote next:
// a corrupt entry in the stream rather than a failed publication.
//
// The two libraries iqmq ships answer differently, and neither answer is a
// preference :
//
//   - go-redis writes and reads in the CALLING goroutine (`baseClient._process`
//     wraps `cn.WithWriter`/`cn.WithReader`, and a pipeline's `Exec` does the
//     same for the whole batch), so nothing reads a token after the call.
//     `drivers/redis/goredis` answers true.
//   - rueidis queues a command for a background writer and, when the context
//     is cancelled, returns while the command is still in that queue --
//     `pipe.Do`'s abort path hands the channel to a goroutine and returns
//     `ctx.Err()`, and `_backgroundWrite` then writes the queued command's
//     tokens from that other goroutine. `drivers/redis` answers false (it does
//     not implement this interface at all) and pays the copy.
//
// A commander that does not implement ArgAliaser does not accept blobs, which
// is the safe default: [AliasesArgs] is what asks.
type ArgAliaser interface {
	// AliasArgs reports whether this commander accepts a [Cmd] carrying
	// [Blob] tokens. It is asked once, at construction, and must not change
	// its answer afterwards.
	AliasArgs() bool
}

// AliasesArgs reports whether c accepts a [Cmd] carrying [Blob] tokens.
//
// A commander that does not implement [ArgAliaser] does not, which is the
// answer that keeps a wrapper, a fake or a future second client correct
// without having to know the mechanism exists.
func AliasesArgs(c Commander) bool {
	a, ok := c.(ArgAliaser)
	return ok && a.AliasArgs()
}

// ParseServerInfo picks the version and the software out of INFO's reply.
//
// It is here rather than in either driver because both need it and there is
// exactly one right answer. Valkey reports its own version in `valkey_version`
// AND a Redis version in `redis_version` for compatibility, so a parser that
// read `redis_version` alone would report a Valkey 8 as a Redis 7.2.4 -- and
// the capability table branches on the version, so the broker-version matrix
// would attribute Valkey's answers to Redis.
func ParseServerInfo(text string) (version, software string) {
	software = "redis"
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimRight(line, "\r")
		name, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch name {
		case "redis_version":
			version = val
		case "valkey_version":
			// Valkey 8 reports both; its own key wins, because a Valkey
			// server reporting redis_version 7.2.4 for compatibility is not
			// a Redis 7.2.4, and the version matrix has to tell them apart.
			version = val
			software = "valkey"
		case "server_name":
			if val != "" {
				software = val
			}
		}
	}
	return version, software
}
