// Package corr holds the correlation stores of the request/reply pattern
// (the API, RR-01): the table that maps the
// correlation id of an outstanding request to the waiter that must be woken
// when its reply arrives, and that releases waiters whose deadline passed.
//
// One reply consumer per process serves every outstanding request, so the
// store is on the hot path of every reply: a Put per request, a Take per
// reply, and a periodic Expire sweep. All three must be cheap under
// contention, which is what the bake-off measures across the kinds behind the
// one interface here: Sharded (the reference), SyncMap and LockFreeOA.
//
// Contract. Ids are unique and non-zero (the caller allocates them from a
// counter). A reply is matched at most once: Take removes the entry and only
// one caller can observe it, whether it races another Take or an Expire
// sweep. Expire removes and returns every entry whose deadline has passed, so
// a timed-out request always frees its slot; passing a deadline far in the
// future drains the store, which is what Close does. Capacity bounds the
// store: Put refuses a new entry once Len reaches it, so a broker that never
// replies cannot grow memory without bound.
//
// Stores are safe for concurrent use. Put reserves its slot on the length
// counter before the entry becomes visible and releases it when the entry is
// refused, so Len stays within [0, Capacity] at every instant however the
// operations interleave. The duplicate refusal is exact for
// Sharded and SyncMap; LockFreeOA may accept a duplicate id when two Puts of
// the same id race, which the ids reqreply allocates never do.
package corr

import (
	"fmt"
	"math/bits"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/errcode"
)

// ID is a correlation id. Zero is reserved and never stored.
type ID uint64

// Kind identifies a correlation store variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Sharded is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Sharded is a fixed set of mutex-guarded maps selected by a hash of the
	// id: the reference implementation.
	Sharded
	// SyncMap is one sync.Map with an atomic length.
	SyncMap
	// LockFreeOA is a fixed-capacity open-addressing table whose slots carry
	// their own state word, so Put, Take and Expire take no lock.
	LockFreeOA

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "sharded", "syncmap", "lockfreeoa"}

var kindIdents = [kindCount]string{"Auto", "Sharded", "SyncMap", "LockFreeOA"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := Sharded; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// ErrInvalidConfig reports a Config that Validate rejects.
var ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq: invalid correlation store configuration")

// Defaults for the zero Config.
const (
	// DefaultCapacity is the number of outstanding requests a store admits.
	DefaultCapacity = 1 << 14
	// DefaultShards is the shard count of the Sharded kind. It is a power of
	// two so the shard index is a mask of the mixed id.
	DefaultShards = 16
)

// Config bounds a correlation store. The zero value is usable.
type Config struct {
	// Capacity is the number of entries the store admits; Put refuses once
	// Len reaches it. Zero selects DefaultCapacity.
	Capacity int
	// Shards is the number of independent maps of the Sharded kind, rounded
	// up to a power of two. Zero selects DefaultShards. Other kinds ignore
	// it.
	Shards int
}

// Normalize fills in the zero values and returns the effective configuration.
func (c Config) Normalize() Config {
	if c.Capacity <= 0 {
		c.Capacity = DefaultCapacity
	}
	if c.Shards <= 0 {
		c.Shards = DefaultShards
	}
	c.Shards = pow2ceil(c.Shards)
	return c
}

// Validate reports whether the configuration is usable. It is pure.
func (c Config) Validate() error {
	if c.Capacity < 0 || c.Shards < 0 {
		return ErrInvalidConfig
	}
	return nil
}

// Pending is one entry of the store, as returned by Expire.
type Pending[T any] struct {
	// ID is the correlation id the entry was stored under.
	ID ID
	// Deadline is the timestamp at which the entry expires, in the units of
	// the caller's clock.
	Deadline int64
	// Value is the waiter the caller stored.
	Value T
}

// Store maps correlation ids to waiters.
//
// Implementations are safe for concurrent use. now and Deadline are timestamps
// from the caller's injected clock (internal/clock), in nanoseconds: the store
// never reads a clock itself.
type Store[T any] interface {
	// Put registers v under id, to expire at deadline. It reports false when
	// id is zero, when id is already present, or when the store is full.
	Put(id ID, deadline int64, v T) bool
	// Take removes the entry stored under id and returns its value. At most
	// one caller ever observes a given entry, whether it races another Take
	// or an Expire sweep.
	Take(id ID) (T, bool)
	// Expire removes every entry whose deadline is at or below now, appends
	// them to dst in unspecified order and returns the extended slice.
	// Passing the largest timestamp drains the store, which is what a close
	// does. dst is the caller's buffer: reusing it keeps the sweep
	// allocation-free.
	Expire(now int64, dst []Pending[T]) []Pending[T]
	// Len returns the number of entries currently held.
	Len() int
	// Kind returns the variant this store implements.
	Kind() Kind
}

// New returns a store of kind k configured by cfg, or fails on an invalid
// kind or configuration.
func New[T any](k Kind, cfg Config) (Store[T], error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("corr: %w", err)
	}
	cfg = cfg.Normalize()
	switch k.Resolve() {
	case Sharded:
		return newSharded[T](cfg), nil
	case SyncMap:
		return newSyncMap[T](cfg), nil
	case LockFreeOA:
		return newLockFreeOA[T](cfg), nil
	default:
		return nil, fmt.Errorf("corr: invalid kind %d", int(k))
	}
}

// counter is the length of a store and its capacity bound in one word. A Put
// reserves a slot before its entry becomes visible and releases it again when
// the entry is refused, so Len stays within [0, Capacity] at every instant,
// however Puts, Takes and sweeps interleave. The reservation is a
// compare-and-swap rather than an add so the counter never even transiently
// exceeds the bound.
type counter struct {
	n   atomic.Int64
	cap int64
}

// reserve claims one slot, or reports false when the store is full.
func (c *counter) reserve() bool {
	for {
		n := c.n.Load()
		if n >= c.cap {
			return false
		}
		if c.n.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// release returns n slots.
func (c *counter) release(n int64) { c.n.Add(-n) }

// len returns the number of entries held.
func (c *counter) len() int { return int(c.n.Load()) }

// mix spreads a correlation id over the whole word: ids come from a counter,
// whose low bits alone would send consecutive requests to one shard or one
// probe chain. It is splitmix64's finaliser.
func mix(id ID) uint64 {
	x := uint64(id)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// pow2ceil returns the smallest power of two ≥ n (n ≥ 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// cacheLine is the assumed cache-line size used to pad contended shards.
const cacheLine = 64
