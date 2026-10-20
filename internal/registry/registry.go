// Package registry holds every topic-registry variant behind one generic interface, so that the
// bake-off can measure them against each other on read-heavy, churn and
// mixed workloads and the engine can bind the winner to Auto.
//
// Keys are topic names interned with the unique package (the API contract
// The specification): a Key compares and hashes as a single word, so every kind pays for
// the string exactly once, when the name is interned, and never on lookup.
//
// All kinds are safe for concurrent use. LoadOrStore is atomic per key:
// when several goroutines race to create the same key exactly one stores
// and every caller receives the stored value. Range visits a snapshot of
// the entries that is consistent per kind (see Kind.RangeSnapshot); the
// callback may call any method of the registry.
package registry

import (
	"errors"
	"hash/maphash"
	"unique"
)

// Key is an interned topic name.
type Key = unique.Handle[string]

// Intern returns the Key for name. Equal names yield equal Keys.
func Intern(name string) Key { return unique.Make(name) }

// Kind identifies a registry variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. RWMutexMap is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// RWMutexMap is a map guarded by one sync.RWMutex: the reference.
	RWMutexMap
	// COW publishes an immutable map through an atomic pointer; writers
	// serialise on a mutex and copy the whole map, readers never lock.
	COW
	// SyncMap wraps sync.Map; Len walks the map (see ExactLen).
	SyncMap
	// Sharded spreads keys over cache-line separated RWMutex-guarded maps
	// selected by a maphash of the interned Key.
	Sharded

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "rwmutexmap", "cow", "syncmap", "sharded"}

var kindIdents = [kindCount]string{"Auto", "RWMutexMap", "COW", "SyncMap", "Sharded"}

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
	for k := RWMutexMap; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// RangeSnapshot reports whether Range visits one instantaneous snapshot of
// the whole registry. Kinds that report false visit a snapshot per shard
// (Sharded) or tolerate concurrent mutation entry by entry (SyncMap); in
// every case an entry that exists for the whole Range is visited exactly
// once and an entry that never existed during it is not visited.
func (k Kind) RangeSnapshot() bool {
	switch k.Resolve() {
	case RWMutexMap, COW:
		return true
	default:
		return false
	}
}

// ExactLen reports whether Len is the number of entries at one instant
// during the call even while other goroutines mutate the registry. Kinds
// that report false (SyncMap, Sharded) count each key at most once and
// count no key that was absent throughout, but read the parts of the
// registry at different instants, so their result need not equal the count
// at any one of them. See Registry.Len.
func (k Kind) ExactLen() bool {
	switch k.Resolve() {
	case RWMutexMap, COW:
		return true
	default:
		return false
	}
}

// LockFreeLoad reports whether Load takes no lock (COW, SyncMap).
func (k Kind) LockFreeLoad() bool {
	switch k.Resolve() {
	case COW, SyncMap:
		return true
	default:
		return false
	}
}

// Registry maps interned topic names to values of type V.
type Registry[V any] interface {
	// Load returns the value stored for k.
	Load(k Key) (V, bool)
	// LoadOrStore returns the value stored for k if present (loaded is
	// true); otherwise it stores v and returns it. Exactly one of several
	// concurrent callers for a missing k stores.
	LoadOrStore(k Key, v V) (actual V, loaded bool)
	// Delete removes k and returns the removed value, if any.
	Delete(k Key) (old V, deleted bool)
	// Range calls fn for every entry until fn returns false.
	Range(fn func(k Key, v V) bool)
	// Len returns the number of entries. It is exact when the registry is
	// quiescent. Under concurrent mutation only the kinds declaring
	// Kind.ExactLen return the count at one instant during the call; the
	// others promise the weaker contract that every key present for the
	// whole call is counted exactly once and no key absent for the whole
	// call is counted, so the result lies between the number of entries
	// that were there throughout and the number of keys that existed at
	// some point during it. No kind may report an entry twice.
	Len() int
}

// DefaultShards is the shard count Sharded uses under New.
const DefaultShards = 32

// New builds an empty registry of kind k, or fails on an invalid kind.
func New[V any](k Kind) (Registry[V], error) {
	switch k.Resolve() {
	case RWMutexMap:
		return newRWMutexMap[V](), nil
	case COW:
		return newCOW[V](), nil
	case SyncMap:
		return newSyncMap[V](), nil
	case Sharded:
		return newSharded[V](DefaultShards), nil
	default:
		return nil, errors.New("registry: invalid kind")
	}
}

// cacheLine is the assumed cache-line size used to separate hot shards.
const cacheLine = 64

// shardSeed hashes interned keys onto shards. One seed per process is
// enough: the hash only has to spread distinct handles, not resist
// adversarial input, and interned handles are never attacker-chosen
// addresses.
var shardSeed = maphash.MakeSeed()

// hashKey returns a well-mixed hash of the interned key. Interned handles
// are pointer-sized, so this is a constant-time word hash regardless of
// the topic name's length.
func hashKey(k Key) uint64 { return maphash.Comparable(shardSeed, k) }

// pow2ceil returns the smallest power of two ≥ n (n ≥ 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
