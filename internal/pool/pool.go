// Package pool holds the object and buffer pooling variants behind two small interfaces, so that the
// bake-off can measure them against each other (throughput, GC cycles,
// resident size) and the engine can bind the winner to Auto.
//
// ObjectPool recycles fixed-size records such as envelopes; BufferPool
// recycles byte slices such as safe-mode body clones. Every kind is safe for
// concurrent use. The kinds differ in where idle items live and how they are
// allocated:
//
//   - None allocates on every Get and lets the garbage collector reclaim
//     everything: the reference and the GC-only baseline.
//   - SyncPool wraps sync.Pool: per-P caches that the garbage collector
//     clears, so idle items are never retained across two GC cycles.
//   - FreeList is one mutex-guarded, bounded LIFO. It is meant to be
//     instantiated per lane, so contention stays local.
//   - SizeClass is a shared pool: one bounded LIFO per power-of-two size
//     class, each striped over several mutexes that callers probe with
//     TryLock starting at the stripe of the last Put, so an uncontended
//     caller keeps hitting one stripe while contended callers spread out.
//     A fixed-size T has exactly one class.
//   - Slab allocates items in chunks (objects: a slice of T; buffers: a byte
//     slab carved into equal slots of a class) and keeps the free slots on a
//     bounded LIFO. Slab memory returns to the GC only when every slot of a
//     chunk is unreachable.
//
// Reset invariant: every object returned by ObjectPool.Get is in its reset
// state (the zero value unless Config.Reset says otherwise), whatever the
// previous holder left in it. Buffer contents are unspecified after Get; in
// iqmq_debug builds recycled buffers are poisoned on Put so a stale read is
// obvious, and a second Put of an object or buffer that is already idle
// (a double Put) panics with DoublePutPanic.
package pool

import (
	"fmt"
	"math/bits"
	"runtime"
)

// Kind identifies a pooling variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. None is the reference (GC-only) implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// None allocates on every Get; Put discards. The reference.
	None
	// SyncPool wraps sync.Pool: per-P caches cleared by the garbage
	// collector.
	SyncPool
	// FreeList is a mutex-guarded bounded LIFO, intended one per lane.
	FreeList
	// SizeClass keeps one striped, bounded LIFO per power-of-two size class
	// and is intended to be shared.
	SizeClass
	// Slab allocates in chunks and keeps free slots on a bounded LIFO.
	Slab

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "none", "syncpool", "freelist", "sizeclass", "slab"}

var kindIdents = [kindCount]string{"Auto", "None", "SyncPool", "FreeList", "SizeClass", "Slab"}

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
	for k := None; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Retains reports whether Put can keep an item for a later Get. None never
// retains anything.
func (k Kind) Retains() bool { return k.Resolve() != None }

// GCCleared reports whether the garbage collector may drop idle items, so a
// Get after a GC cycle can allocate even though items were Put. Retaining
// kinds other than SyncPool keep idle items until the pool is unreachable.
func (k Kind) GCCleared() bool { return k.Resolve() == SyncPool }

// Classed reports whether BufferPool capacities are rounded up to a power of
// two (at least MinClass) so that buffers are pooled by size class.
func (k Kind) Classed() bool {
	switch k.Resolve() {
	case SizeClass, Slab:
		return true
	default:
		return false
	}
}

// Chunked reports whether items are allocated several at a time.
func (k Kind) Chunked() bool { return k.Resolve() == Slab }

// Bounded reports whether Config.MaxIdle / BufferConfig.MaxIdle limit the
// idle items retained. SyncPool leaves the bound to the garbage collector.
func (k Kind) Bounded() bool {
	switch k.Resolve() {
	case FreeList, SizeClass, Slab:
		return true
	default:
		return false
	}
}

// ObjectPool recycles pointers to T.
type ObjectPool[T any] interface {
	// Get returns an object in its reset state: the zero value, or the
	// state Config.Reset establishes. The object is exclusively owned by the
	// caller until it is passed to Put.
	Get() *T
	// Put returns v to the pool; v must not be used afterwards. Put(nil) is
	// a no-op. Putting the same object twice without a Get in between is a
	// caller bug that iqmq_debug builds detect.
	Put(v *T)
}

// BufferPool recycles byte slices.
type BufferPool interface {
	// Get returns a slice with len(b) == n and cap(b) >= n. Contents are
	// unspecified. Requests larger than BufferConfig.MaxSize are served from
	// the heap and never pooled.
	Get(n int) []byte
	// Put returns b to the pool; b must not be used afterwards. Slices with
	// zero capacity, or larger than MaxSize, are dropped. Classed kinds file
	// b under the largest class that fits cap(b).
	Put(b []byte)
}

// Defaults for the zero fields of Config and BufferConfig.
const (
	// DefaultMaxIdle is the number of idle items a bounded pool retains.
	DefaultMaxIdle = 1024
	// DefaultChunk is the number of objects a Slab object pool allocates at
	// a time.
	DefaultChunk = 64
	// DefaultMaxSize is the largest buffer capacity a BufferPool pools.
	DefaultMaxSize = 64 << 10
	// DefaultChunkBytes is the size of a Slab buffer chunk; a chunk always
	// holds at least one slot, so a class larger than the chunk allocates
	// one slot per chunk.
	DefaultChunkBytes = 64 << 10
	// MinClass is the smallest buffer class of the classed kinds; smaller
	// requests are rounded up to it.
	MinClass = 16
	// MaxStripes bounds the number of mutex stripes a SizeClass pool uses;
	// the actual count is GOMAXPROCS rounded up to a power of two.
	MaxStripes = 16
)

// Config tunes an ObjectPool. The zero value is valid.
type Config[T any] struct {
	// MaxIdle bounds the idle objects retained by bounded kinds; Puts beyond
	// it are dropped. Zero means DefaultMaxIdle.
	MaxIdle int
	// Chunk is the number of objects Slab allocates per chunk (at most
	// MaxIdle). Zero means DefaultChunk.
	Chunk int
	// Reset restores an object to its initial state before it is reused or
	// after it is allocated. Nil means the zero value.
	Reset func(*T)
}

func (c Config[T]) maxIdle() int {
	if c.MaxIdle <= 0 {
		return DefaultMaxIdle
	}
	return c.MaxIdle
}

func (c Config[T]) chunk() int {
	n := c.Chunk
	if n <= 0 {
		n = DefaultChunk
	}
	return max(1, min(n, c.maxIdle()))
}

// BufferConfig tunes a BufferPool. The zero value is valid.
type BufferConfig struct {
	// MaxIdle bounds the idle buffers retained: in total for FreeList, per
	// class for SizeClass and Slab. Zero means DefaultMaxIdle.
	MaxIdle int
	// MaxSize is the largest capacity pooled, rounded up to a power of two
	// (at least MinClass). Zero means DefaultMaxSize.
	MaxSize int
	// Chunk is the number of bytes Slab allocates per chunk; a chunk holds
	// between one and MaxIdle slots of its class. Zero means
	// DefaultChunkBytes.
	Chunk int
}

func (c BufferConfig) maxIdle() int {
	if c.MaxIdle <= 0 {
		return DefaultMaxIdle
	}
	return c.MaxIdle
}

func (c BufferConfig) maxSize() int {
	if c.MaxSize <= 0 {
		return DefaultMaxSize
	}
	return pow2ceil(max(c.MaxSize, MinClass))
}

func (c BufferConfig) chunk() int {
	if c.Chunk <= 0 {
		return DefaultChunkBytes
	}
	return c.Chunk
}

// New builds an ObjectPool of kind k, or fails on an invalid kind.
func New[T any](k Kind, cfg Config[T]) (ObjectPool[T], error) {
	switch k.Resolve() {
	case None:
		return &noneObjects[T]{reset: cfg.Reset}, nil
	case SyncPool:
		return newSyncObjects(cfg), nil
	case FreeList:
		return newFreeList(cfg), nil
	case SizeClass:
		return newStripedObjects(cfg), nil
	case Slab:
		return newSlabObjects(cfg), nil
	default:
		return nil, fmt.Errorf("pool: invalid kind %d", int(k))
	}
}

// NewBuffers builds a BufferPool of kind k, or fails on an invalid kind.
func NewBuffers(k Kind, cfg BufferConfig) (BufferPool, error) {
	switch k.Resolve() {
	case None:
		return &noneBuffers{}, nil
	case SyncPool:
		return newSyncBuffers(cfg), nil
	case FreeList:
		return newFreeBuffers(cfg), nil
	case SizeClass:
		return newClassedBuffers(cfg), nil
	case Slab:
		return newSlabBuffers(cfg), nil
	default:
		return nil, fmt.Errorf("pool: invalid kind %d", int(k))
	}
}

// fresh allocates a new object in its reset state.
func fresh[T any](reset func(*T)) *T {
	v := new(T)
	if reset != nil {
		reset(v)
	}
	return v
}

// resetObject restores v to its reset state.
func resetObject[T any](v *T, reset func(*T)) {
	if reset == nil {
		var zero T
		*v = zero
		return
	}
	reset(v)
}

// pow2ceil returns the smallest power of two >= n (n >= 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// classOf returns the capacity class that serves a request for n bytes.
func classOf(n int) int { return max(MinClass, pow2ceil(n)) }

// classIndex returns the index of the class with capacity c (a power of two
// >= MinClass).
func classIndex(c int) int { return bits.Len(uint(c)) - 1 - (bits.Len(MinClass) - 1) }

// floorClass returns the largest class capacity <= c, or 0 when c < MinClass.
func floorClass(c int) int {
	if c < MinClass {
		return 0
	}
	return 1 << (bits.Len(uint(c)) - 1)
}

// stripeCount returns the number of mutex stripes for shared pools.
func stripeCount() int { return max(1, min(pow2ceil(runtime.GOMAXPROCS(0)), MaxStripes)) }

// bufKey identifies a buffer's storage for the debug canary: the address of
// the first byte of its full capacity. cap(b) must be > 0.
func bufKey(b []byte) *byte { return &b[:cap(b)][0] }
