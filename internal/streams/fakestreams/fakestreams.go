// Package fakestreams is an in-memory [streams.Commander]: enough of Redis
// Streams to run both Redis drivers' whole unit-test suite, their model tests
// and their fuzz targets with no container and no network.
//
// It exists because of what it can do that a container cannot. A container
// gives you a real server and a real clock; this gives you a server whose
// clock you set, whose pending-entries list you can inspect between two
// commands, and whose failures you schedule. The reclaim race that loses a
// message — consumer A holds an entry, consumer B claims it away, A
// acknowledges it — is a three-line test here and a flaky twenty-second test
// against a container, and [streams.FuzzRedisReclaimer] drives it thousands of
// times a second.
//
// What it is NOT:
//
//   - a Lua interpreter. EVAL and EVALSHA recognise the scripts of
//     [bitbucket.org/iqhive/iqmq/internal/streams] by their digest and execute
//     equivalent Go, under the same mutex, which is the property that matters
//     — a script runs to completion with nothing interleaved. A script this
//     package does not know is refused with NOSCRIPT rather than ignored, so
//     a new script cannot silently pass its tests here. The conformance suite
//     against a real container is what proves the Lua itself.
//   - a performance model. Everything is a map and a slice under one mutex.
//   - complete. Only the commands the drivers send are implemented; anything
//     else is an error naming the command, so a driver that started sending a
//     new one fails loudly.
package fakestreams

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// Server is an in-memory Redis-compatible server.
type Server struct {
	mu      sync.Mutex
	streams map[string]*stream
	zsets   map[string]*zset
	scripts map[string]struct{}
	clock   func() time.Time
	offset  time.Duration
	seq     uint64

	// notify is closed and replaced whenever an entry is added, which is
	// what wakes a blocked XREADGROUP.
	notify chan struct{}

	// version and software are what INFO server reports.
	version, software string
	// aof reports whether the server was started with appendonly yes, which
	// is WAITAOF's condition.
	aof bool
	// unknownLag makes XINFO GROUPS report a nil `lag`, as Redis does once a
	// trim or a delete has made the count unreliable.
	unknownLag bool
	// faults is the scheduled-failure table.
	faults map[string]error
	closed bool
}

// Options configure a [Server].
type Options struct {
	// Clock is the time source. Nil means a frozen clock starting at a
	// fixed instant, which is what makes an idle time in a test a number
	// the test chose rather than a number the scheduler chose.
	Clock func() time.Time
	// Version is what INFO server reports ("7.4.2").
	Version string
	// Software is "redis" or "valkey".
	Software string
	// AOF reports whether the append-only file is on, which decides whether
	// WAITAOF can succeed.
	AOF bool
}

// New returns a server.
func New(opts Options) *Server {
	s := &Server{
		streams:  map[string]*stream{},
		zsets:    map[string]*zset{},
		scripts:  map[string]struct{}{},
		faults:   map[string]error{},
		notify:   make(chan struct{}),
		clock:    opts.Clock,
		version:  opts.Version,
		software: opts.Software,
		aof:      opts.AOF,
	}
	if s.clock == nil {
		// A fixed start rather than time.Now: an idle time computed from a
		// frozen clock is exactly what the test asked for.
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		s.clock = func() time.Time { return base }
	}
	if s.version == "" {
		s.version = "7.4.2"
	}
	if s.software == "" {
		s.software = "redis"
	}
	for _, script := range streams.Scripts() {
		s.scripts[streams.SHA(script)] = struct{}{}
	}
	return s
}

// stream is one stream key.
type stream struct {
	groups  map[string]*group
	entries []entry
	last    streams.ID
	maxDel  streams.ID
}

// entry is one stream entry.
type entry struct {
	fields []string
	id     streams.ID
}

// group is one consumer group.
type group struct {
	pending   map[streams.ID]*pel
	consumers map[string]time.Time
	last      streams.ID
}

// pel is one pending-entries-list row.
type pel struct {
	consumer  string
	delivered time.Time
	count     uint32
}

// zset is one sorted set, kept as a slice because a fake does not need a skip
// list and a slice makes the order visible in a failure message.
type zset struct {
	members []zmember
}

type zmember struct {
	member string
	score  int64
}

// Now returns the server's current time, offset included.
func (s *Server) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowLocked()
}

func (s *Server) nowLocked() time.Time { return s.clock().Add(s.offset) }

// Advance moves the server's clock forward, which is how a test makes an
// entry idle enough to be reclaimed without waiting.
func (s *Server) Advance(d time.Duration) {
	s.mu.Lock()
	s.offset += d
	s.mu.Unlock()
	s.wake()
}

// Fail schedules an error for the next command whose verb is name
// (upper-case, the sub-command appended for XGROUP: "XGROUP CREATE").
//
// It is how a test reaches the paths a real server only takes when something
// has gone wrong — a settlement whose round trip fails, a reclaim that cannot
// run — which are the paths where a driver either leaves a delivery
// settleable or loses it.
func (s *Server) Fail(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[strings.ToUpper(name)] = err
}

// ReportUnknownLag makes XINFO GROUPS answer a nil `lag`.
//
// Redis does this after an XDEL or an XTRIM, and it is the one figure in a
// group's statistics that can be missing. This server implements neither
// command, so the switch is how a driver's "the server did not say" branch is
// reached at all.
func (s *Server) ReportUnknownLag() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unknownLag = true
}

// Close makes further commands report [ErrClosed]. It is idempotent.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.wake()
	return nil
}

// Ping is the liveness check.
func (s *Server) Ping(ctx context.Context) error {
	_, err := s.Do(ctx, streams.Ping())
	return err
}

// ServerVersion reports the version and software INFO would.
func (s *Server) ServerVersion(_ context.Context) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, s.software, nil
}

// DoMulti runs each command in order and returns one result per command.
//
// It is a pipeline and not a transaction, which is Redis's own behaviour: a
// command that fails does not stop the ones behind it, and each one's outcome
// is its own. A fake that stopped at the first failure would make a driver's
// batch-settlement code look correct when it is not.
func (s *Server) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	out := make([]streams.Result, len(cmds))
	for i, c := range cmds {
		v, err := s.Do(ctx, c)
		out[i] = streams.Result{Value: v, Err: err}
	}
	return out, nil
}

// AliasArgs accepts blob tokens, and the acceptance is what puts the aliased
// encoding under every model test and every fuzz target in this repository.
//
// This server could not care less how a token arrived -- it materialises
// immediately, which is the copy a real client is trusted to avoid -- so
// answering false would have been free and would have left the fast path
// exercised only by the one driver that uses it. Answering true means the
// command `drivers/redis` builds for go-redis is the command the fake is
// driven with, and [streams.Cmd.Materialize] is proved on every entry the
// model tests publish rather than in one unit test.
//
// It is also honest under the contract [streams.ArgAliaser] states: Do
// materialises before it returns and retains nothing.
func (s *Server) AliasArgs() bool { return true }

// Do runs one command.
//
// A blob-carrying command is materialised first. The fake stores the entry it
// is given and a test then mutates the caller's buffer to prove the driver
// copied when it had to, so a fake that aliased into its own storage would
// make that test measure the fake.
func (s *Server) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if err := ctx.Err(); err != nil {
		return streams.Nil(), err
	}
	if !c.Valid() {
		return streams.Nil(), errBadCommand
	}
	c = c.Materialize()
	verb := strings.ToUpper(c.Tokens[0])
	args := c.Tokens[1:]
	// XREADGROUP with BLOCK is the one command that waits, so it is handled
	// outside the mutex: holding it while parked would stop every other
	// command, which is the opposite of what a blocking read does on a real
	// server.
	if verb == "XREADGROUP" {
		return s.xreadgroup(ctx, args)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doLocked(verb, args)
}

// doLocked runs one non-blocking command under the mutex.
func (s *Server) doLocked(verb string, args []string) (streams.Value, error) {
	if s.closed {
		return streams.Nil(), ErrClosed
	}
	name := verb
	if (verb == "XGROUP" || verb == "SCRIPT") && len(args) > 0 {
		name = verb + " " + strings.ToUpper(args[0])
	}
	if err, ok := s.faults[name]; ok {
		delete(s.faults, name)
		return streams.Nil(), err
	}
	switch verb {
	case "PING":
		return streams.Str("PONG"), nil
	case "INFO":
		return streams.Str("# Server\r\nredis_version:" + s.version +
			"\r\nredis_mode:standalone\r\nserver_name:" + s.software + "\r\n"), nil
	case "CONFIG":
		// Only `CONFIG GET appendonly` is implemented, because it is the one
		// configuration this driver reads: WAITAOF's condition. Anything else
		// is an unknown command rather than a silent default, so a driver
		// that started reading another setting fails loudly.
		if len(args) == 2 && strings.EqualFold(args[0], "GET") && args[1] == "appendonly" {
			val := "no"
			if s.aof {
				val = "yes"
			}
			return streams.Arr(streams.Str("appendonly"), streams.Str(val)), nil
		}
		return streams.Nil(), &ErrUnknownCommand{Verb: "CONFIG " + upperArg(args, 0)}
	case "EXISTS":
		if len(args) != 1 {
			return streams.Nil(), errBadCommand
		}
		if _, ok := s.streams[args[0]]; ok {
			return streams.Int(1), nil
		}
		if _, ok := s.zsets[args[0]]; ok {
			return streams.Int(1), nil
		}
		return streams.Int(0), nil
	case "XADD":
		return s.xadd(args)
	case "SCAN":
		return s.scan(args)
	case "DEL":
		return s.del(args)
	case "XLEN":
		if len(args) != 1 {
			return streams.Nil(), errBadCommand
		}
		st := s.streams[args[0]]
		if st == nil {
			return streams.Int(0), nil
		}
		return streams.Int(int64(len(st.entries))), nil
	case "XRANGE":
		return s.xrange(args)
	case "XDEL":
		return s.xdel(args)
	case "XTRIM":
		return s.xtrim(args)
	case "XGROUP":
		return s.xgroup(args)
	case "XACK":
		return s.xack(args)
	case "XAUTOCLAIM":
		return s.xautoclaim(args)
	case "XCLAIM":
		return s.xclaim(args)
	case "XPENDING":
		return s.xpending(args)
	case "XINFO":
		return s.xinfo(args)
	case "ZADD":
		return s.zadd(args)
	case "ZREM":
		return s.zrem(args)
	case "ZCARD":
		if len(args) != 1 {
			return streams.Nil(), errBadCommand
		}
		z := s.zsets[args[0]]
		if z == nil {
			return streams.Int(0), nil
		}
		return streams.Int(int64(len(z.members))), nil
	case "ZRANGEBYSCORE":
		return s.zrangebyscore(args)
	case "WAITAOF":
		return s.waitaof(args)
	case "SCRIPT":
		return s.script(args)
	case "EVAL":
		if len(args) < 2 {
			return streams.Nil(), errBadCommand
		}
		return s.eval(streams.SHA(args[0]), args[1:])
	case "EVALSHA":
		if len(args) < 2 {
			return streams.Nil(), errBadCommand
		}
		return s.eval(args[0], args[1:])
	}
	return streams.Nil(), &ErrUnknownCommand{Verb: verb}
}

// upperArg is args[i] upper-cased, or "" when there is no such argument.
//
// It exists because an error message built from an argument the caller may
// not have sent is a panic on the error path — which FuzzFakeStreams found in
// a second, from `CONFIG` with no arguments at all. An index computed from an
// argument is an index into a slice the caller controls, and the error path is
// where that is easiest to forget.
func upperArg(args []string, i int) string {
	if i < 0 || i >= len(args) {
		return ""
	}
	return strings.ToUpper(args[i])
}

// wake releases every blocked read.
func (s *Server) wake() {
	s.mu.Lock()
	ch := s.notify
	s.notify = make(chan struct{})
	s.mu.Unlock()
	close(ch)
}

// The fake's own errors. They are deliberately shaped like a real server's
// error replies, prefix included, because a driver's error mapper matches on
// those prefixes and a fake whose errors did not carry them would let a
// mapping rule go untested.
var (
	// ErrClosed is a command on a closed server.
	ErrClosed = errors.New("ERR the server is closed")
	// ErrNoGroup is the BUSYGROUP/NOGROUP family: the consumer group or the
	// key does not exist.
	ErrNoGroup = errors.New("NOGROUP No such key or consumer group")
	// ErrBusyGroup is XGROUP CREATE on a group that already exists.
	ErrBusyGroup = errors.New("BUSYGROUP Consumer Group name already exists")
	// ErrNoScript is EVALSHA for a script this fake does not know.
	ErrNoScript = errors.New("NOSCRIPT No matching script")
	// ErrCrossSlot is a multi-key command whose keys are on two slots.
	ErrCrossSlot = errors.New("CROSSSLOT Keys in request don't hash to the same slot")
	// ErrNoAOF is WAITAOF on a server with appendonly no.
	ErrNoAOF = errors.New("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled")
	// ErrWrongType is a stream command on a key holding something else.
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

	errBadCommand = errors.New("ERR wrong number of arguments")
	errBadID      = errors.New("ERR Invalid stream ID specified as stream command argument")
)

// ErrUnknownCommand is a command this fake does not implement.
//
// It names the verb, because the alternative — ignoring it, or returning OK —
// is how a fake comes to pass a test for a command it never ran.
type ErrUnknownCommand struct{ Verb string }

func (e *ErrUnknownCommand) Error() string {
	return "ERR unknown command '" + e.Verb + "' (fakestreams implements only what the drivers send)"
}

// nextID assigns an identifier for an XADD with "*".
//
// It is the server's millisecond clock and a sequence within it, which is
// Redis's own rule, and it is monotone: an identifier must be greater than
// the stream's last, so a clock that went backwards still produces an
// increasing identifier.
func (s *Server) nextID(st *stream) streams.ID {
	ms := uint64(s.nowLocked().UnixMilli())
	id := streams.ID{Ms: ms}
	if !st.last.Before(id) {
		id = st.last.Next()
	}
	s.seq++
	return id
}

// streamFor returns the stream, creating it when create is true.
func (s *Server) streamFor(key string, create bool) *stream {
	st := s.streams[key]
	if st == nil && create {
		st = &stream{groups: map[string]*group{}}
		s.streams[key] = st
	}
	return st
}

// entryAt returns the index of an identifier in a stream, or -1.
func (st *stream) entryAt(id streams.ID) int {
	for i := range st.entries {
		if st.entries[i].id == id {
			return i
		}
	}
	return -1
}

// parseMillis reads a millisecond argument.
func parseMillis(s string) (time.Duration, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, errBadCommand
	}
	return time.Duration(n) * time.Millisecond, nil
}
