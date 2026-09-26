package redisstore

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/x/idempotency"
)

// DefaultPrefix is the key prefix an [Options] with none takes.
//
// A prefix is not cosmetic: two services sharing a Redis and a key space make
// each other's messages duplicates, which is the one failure in this package
// that discards work. It is namespaced under "iqmq:" so that a
// `SCAN MATCH iqmq:idem:*` finds exactly this store's keys and nothing else.
const DefaultPrefix = "iqmq:idem:"

// The Lua scripts. Each is a compare-and-set, which is why each is a script.
//
// They are package-level strings rather than built per store so that the SHA
// rueidis caches is shared, and they are spelled out here rather than
// assembled from fragments so that `SCRIPT LOAD`-ing them by hand against a
// server reproduces exactly what this package sends.
const (
	// claimScript takes the key and returns either {"first"} or
	// {"exists", <current value>}.
	//
	// The GET after a failed SET NX is what distinguishes an open claim from a
	// committed one, and it is inside the script because a GET sent separately
	// could read a value another worker wrote between the two commands.
	//
	// The `gone` branch is a key that vanished between the SET and the GET.
	// Redis evaluates a script atomically and treats a key as expired at the
	// script's start, so it is not reachable on a real server; it is returned
	// and mapped rather than falling off the end of the function, because a
	// nil that means "I do not know" must not be read as "first".
	claimScript = `
local set = redis.call('SET', KEYS[1], ARGV[3] .. ARGV[1], 'NX', 'PX', ARGV[2])
if set then return {'first'} end
local cur = redis.call('GET', KEYS[1])
if cur then return {'exists', cur} end
return {'gone'}
`
	// commitScript re-stamps this claim's key as committed and re-bases the
	// window. It accepts the already-committed value as well as the open one,
	// so that a second Commit of a live claim is idempotent -- which is what
	// the in-process store does, and the contract suite's
	// CommitIsIdempotent pins them to each other.
	commitScript = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[3] .. ARGV[1] or cur == ARGV[4] .. ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[4] .. ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`
	// abortScript deletes the key only if it still holds this claim.
	abortScript = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[2] .. ARGV[1] or cur == ARGV[3] .. ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`
)

// The verdicts the claim script returns. They are constants because the
// script's text and this switch have to agree, and a typo in one of them would
// fall through to the unknown-verdict error -- which is loud, but only at run
// time against a server.
const (
	verdictFirst  = "first"
	verdictExists = "exists"
	verdictGone   = "gone"
)

// The value prefixes. One byte and a colon, so that a value is readable in
// `redis-cli GET` and a claim's phase can be told from its token at a glance.
const (
	phaseOpen      = "p:"
	phaseCommitted = "d:"
)

// The evaluators. rueidis caches each script's SHA and falls back to EVAL on
// NOSCRIPT, so a server that was restarted or had its script cache flushed
// costs one extra round trip rather than an error.
var (
	claimLua  = rueidis.NewLuaScript(claimScript)
	commitLua = rueidis.NewLuaScript(commitScript)
	abortLua  = rueidis.NewLuaScript(abortScript)
)

// Options configures a store.
type Options struct {
	// Prefix is prepended to every key. Zero takes [DefaultPrefix].
	Prefix string

	// The fields below are read by [Open] only; [New] takes a client the
	// caller built and ignores them apart from Prefix and
	// RequireSafeEviction.

	// Addrs are the Redis addresses, host:port. Required by [Open].
	Addrs []string
	// Username and Password authenticate.
	//
	// Password is a [dsn.Secret], the type every shipped driver holds its
	// credential in, so that printing an Options -- `%+v`, `%#v`, slog's text
	// or JSON handler, a start-up log of the service's configuration --
	// renders "redacted" and not the password. Options has no String method
	// of its own, so each field renders through its own type and this one is
	// what decides. A literal converts without ceremony; a password read at
	// run time takes a conversion, `dsn.Secret(os.Getenv("REDIS_PASSWORD"))`.
	Username string
	Password dsn.Secret
	// DB selects a logical database, which is the cheapest way to keep claim
	// keys away from a cache that is configured to evict (see the package
	// documentation). It is ignored by Redis Cluster, which has one.
	DB int
	// DialTimeout bounds the connection. Zero takes five seconds.
	DialTimeout time.Duration
	// TLS, when non-nil, is used for the connection.
	TLS *tls.Config

	// RequireSafeEviction makes [Open] read the server's
	// `maxmemory-policy` and refuse to build a store on one that evicts keys
	// that have not expired. It is off by default because a caller pointing
	// this at a shared cache may knowingly accept the risk, and because the
	// `CONFIG GET` it needs is disabled on several managed Redis offerings —
	// see [Store.EvictionPolicy].
	RequireSafeEviction bool
}

// Store is [idempotency.Store] on Redis.
type Store struct {
	client rueidis.Client
	prefix string
	// owned says whether Close closes the client. The specification's ownership rule: a
	// handle closes what it opened and nothing else.
	owned bool

	mu     sync.Mutex
	closed bool
}

// The errors this package reports of its own.
var (
	// ErrNoAddrs reports an [Open] with no address.
	ErrNoAddrs = errors.New("redisstore: Options.Addrs is required")
	// ErrNoClient reports a [New] with a nil client.
	ErrNoClient = errors.New("redisstore: the rueidis client must not be nil")
	// ErrUnsafeEviction reports a server whose maxmemory-policy evicts keys
	// that have not expired, when Options.RequireSafeEviction asked for one
	// that does not.
	ErrUnsafeEviction = errors.New("redisstore: the server's maxmemory-policy evicts unexpired keys, so a claim can disappear inside its window")
)

// New builds a store over a client the caller owns.
//
// The client is BORROWED: [Store.Close] does not close it, because a caller
// who built a client for their own use as well would find it closed under
// them. This is the constructor to use when the process already has a Redis
// client — including the one behind
// [github.com/iqhive/iqmq/drivers/redis], whose [drivers/redis.Backend]
// exposes it — so that the claims and the stream share one connection pool.
func New(client rueidis.Client, opts Options) (*Store, error) {
	if client == nil {
		return nil, ErrNoClient
	}
	return &Store{client: client, prefix: prefixOf(opts), owned: false}, nil
}

// Open dials Redis and builds a store that owns the connection.
func Open(ctx context.Context, opts Options) (*Store, error) {
	if len(opts.Addrs) == 0 {
		return nil, ErrNoAddrs
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	copt := rueidis.ClientOption{
		InitAddress: opts.Addrs,
		Username:    opts.Username,
		Password:    opts.Password.Reveal(),
		SelectDB:    opts.DB,
		Dialer:      net.Dialer{Timeout: timeout},
		// Client-side caching caches key VALUES, and every value this store
		// reads is one it is about to change. Leaving it on would allocate a
		// cache per connection for nothing, which is the same reasoning
		// drivers/redis records.
		DisableCache: true,
	}
	if opts.TLS != nil {
		copt.TLSConfig = opts.TLS
	}
	client, err := rueidis.NewClient(copt)
	if err != nil {
		return nil, fmt.Errorf("redisstore: dialing %s: %w", strings.Join(opts.Addrs, ","), err)
	}
	s := &Store{client: client, prefix: prefixOf(opts), owned: true}
	if opts.RequireSafeEviction {
		policy, err := s.EvictionPolicy(ctx)
		if err != nil {
			client.Close()
			return nil, err
		}
		if !SafeEvictionPolicy(policy) {
			client.Close()
			return nil, fmt.Errorf("%w: maxmemory-policy is %q", ErrUnsafeEviction, policy)
		}
	}
	return s, nil
}

// prefixOf resolves the key prefix.
func prefixOf(opts Options) string {
	if opts.Prefix != "" {
		return opts.Prefix
	}
	return DefaultPrefix
}

// SafeEvictionPolicy reports whether a Redis `maxmemory-policy` leaves a key
// that has not expired alone.
//
// `volatile-*` policies only ever evict keys that HAVE a TTL — which every key
// this store writes does — so `volatile-lru`, `volatile-lfu`,
// `volatile-random` and `volatile-ttl` are all unsafe for this purpose, and
// only `noeviction` is safe. That is worth spelling out because "volatile"
// sounds like the careful choice: it protects keys without a TTL, which is the
// opposite of what a claim store needs.
func SafeEvictionPolicy(policy string) bool {
	return strings.EqualFold(strings.TrimSpace(policy), "noeviction")
}

// EvictionPolicy reads the server's `maxmemory-policy`.
//
// A server that refuses `CONFIG GET` — which several managed offerings do —
// reports an error, and a caller who cannot ask has to know the answer another
// way. Reporting "noeviction" in that case would be inventing evidence.
func (s *Store) EvictionPolicy(ctx context.Context) (string, error) {
	res := s.client.Do(ctx, s.client.B().ConfigGet().Parameter("maxmemory-policy").Build())
	m, err := res.AsStrMap()
	if err != nil {
		return "", fmt.Errorf("redisstore: reading maxmemory-policy: %w", err)
	}
	v, ok := m["maxmemory-policy"]
	if !ok {
		return "", errors.New("redisstore: the server answered CONFIG GET maxmemory-policy with no such parameter")
	}
	return v, nil
}

// Claim implements [idempotency.Store].
func (s *Store) Claim(ctx context.Context, key string, window time.Duration) (idempotency.Claim, error) {
	if err := validate(key, window); err != nil {
		return idempotency.Claim{}, err
	}
	if err := s.alive(); err != nil {
		return idempotency.Claim{}, err
	}
	token, err := newToken()
	if err != nil {
		return idempotency.Claim{}, fmt.Errorf("redisstore: generating a claim token: %w", err)
	}
	ms := windowMillis(window)
	res := claimLua.Exec(ctx, s.client, []string{s.prefix + key},
		[]string{token, strconv.FormatInt(ms, 10), phaseOpen})
	reply, err := res.AsStrSlice()
	if err != nil {
		return idempotency.Claim{}, fmt.Errorf("redisstore: the claim script failed: %w", err)
	}
	return verdictOf(key, token, window, time.Now(), reply)
}

// Commit implements [idempotency.Store].
func (s *Store) Commit(ctx context.Context, c idempotency.Claim) error {
	if err := heldClaim(c); err != nil {
		return err
	}
	if err := s.alive(); err != nil {
		return err
	}
	// The committed window is the claim's OWN window and not what is left of
	// it. A handler that took most of its window would otherwise commit a
	// claim that suppresses nothing, and the duplicate the caller was
	// promised would arrive a moment later; storetest's
	// CommitStartsTheWindowAfresh is that sentence for both stores.
	ms := windowMillis(c.Window)
	res := commitLua.Exec(ctx, s.client, []string{s.prefix + c.Key},
		[]string{c.Token, strconv.FormatInt(ms, 10), phaseOpen, phaseCommitted})
	return casResult(res.AsInt64())
}

// Abort implements [idempotency.Store].
func (s *Store) Abort(ctx context.Context, c idempotency.Claim) error {
	if err := heldClaim(c); err != nil {
		return err
	}
	if err := s.alive(); err != nil {
		return err
	}
	res := abortLua.Exec(ctx, s.client, []string{s.prefix + c.Key},
		[]string{c.Token, phaseOpen, phaseCommitted})
	return casResult(res.AsInt64())
}

// Close implements [idempotency.Store]. It is idempotent, and it closes the
// client only if this store opened it.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.owned {
		s.client.Close()
	}
	return nil
}

// alive reports [idempotency.ErrClosed] for a closed store.
func (s *Store) alive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return idempotency.ErrClosed
	}
	return nil
}

// verdictOf turns the claim script's reply into a verdict.
//
// It takes the reply as a []string and the time as an argument rather than
// reaching for a client and a clock, which is what makes every reply shape --
// including the two a correct server cannot produce -- table-testable with no
// Redis at all. TestVerdictOf is that table, and it is the half of this
// package that runs on every `make ci`.
func verdictOf(key, token string, window time.Duration, now time.Time, reply []string) (idempotency.Claim, error) {
	if len(reply) == 0 {
		return idempotency.Claim{}, errors.New("redisstore: the claim script returned an empty reply")
	}
	switch reply[0] {
	case verdictFirst:
		return idempotency.Claim{
			Key: key, Token: token, Status: idempotency.StatusFirst, Window: window,
			// This deadline is the CLIENT's clock plus the window, while the
			// key lapses when the SERVER says so. A skew between the two
			// therefore shows up here and nowhere else, which is why nothing
			// in this package decides anything from it: Commit sends the
			// window, and the script compares the value.
			Expires: now.Add(window),
		}, nil
	case verdictExists:
		if len(reply) < 2 {
			return idempotency.Claim{}, errors.New("redisstore: the claim script said 'exists' and returned no value")
		}
		cur := reply[1]
		switch {
		case strings.HasPrefix(cur, phaseCommitted):
			return idempotency.Claim{Key: key, Status: idempotency.StatusDuplicate}, nil
		case strings.HasPrefix(cur, phaseOpen):
			return idempotency.Claim{Key: key, Status: idempotency.StatusInFlight}, nil
		}
		// A value under this store's prefix that this store did not write.
		// It is reported rather than guessed at: reading it as "first" would
		// hand out a claim on somebody else's key, and reading it as a
		// duplicate would discard work.
		return idempotency.Claim{Key: key}, fmt.Errorf("redisstore: key %q holds a value this store did not write (%q); a prefix collision would make every message a duplicate", key, elide(cur))
	case verdictGone:
		return idempotency.Claim{Key: key}, errors.New("redisstore: the key vanished between the SET and the GET of one script, which Redis's script atomicity should make impossible")
	}
	return idempotency.Claim{Key: key}, fmt.Errorf("redisstore: the claim script returned an unknown verdict %q", elide(reply[0]))
}

// casResult turns a compare-and-set script's 1-or-0 into an error.
//
// It takes the already-extracted integer so that the mapping of 0 to
// ErrClaimLost is testable without a server, which is the mapping the whole
// two-phase claim rests on.
func casResult(n int64, err error) error {
	if err != nil {
		return fmt.Errorf("redisstore: a claim script failed: %w", err)
	}
	if n == 0 {
		return idempotency.ErrClaimLost
	}
	return nil
}

// windowMillis rounds a window UP to whole milliseconds, which is the only
// unit `PX` takes.
//
// Rounding down would be a window shorter than the caller asked for, which is
// a duplicate the caller was told would be caught; rounding up is at most a
// millisecond of extra suppression. A non-positive duration becomes one
// millisecond rather than zero, because `PX 0` is an error and `PX` with a
// negative value deletes the key.
func windowMillis(d time.Duration) int64 {
	if d <= 0 {
		return 1
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// validate mirrors x/idempotency's argument check. It is duplicated rather
// than exported from there because the check is two comparisons and an
// exported helper for it would be API surface that exists for one caller.
func validate(key string, window time.Duration) error {
	if key == "" {
		return idempotency.ErrEmptyKey
	}
	if window <= 0 {
		return idempotency.ErrBadWindow
	}
	return nil
}

// heldClaim mirrors x/idempotency's held-claim check.
func heldClaim(c idempotency.Claim) error {
	if c.Status != idempotency.StatusFirst || c.Key == "" || c.Token == "" {
		return idempotency.ErrNoClaim
	}
	return nil
}

// newToken returns an unguessable claim token.
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// elide bounds a value that goes into an error message. A claim key's value is
// this store's own, but a prefix collision means it is somebody else's data,
// and an error is a place data should not be quoted at length.
func elide(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
