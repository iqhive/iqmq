// Package queue holds every bounded (and one unbounded) lane-queue variant
// behind one generic interface, so
// that the bake-off can measure them against each other and the engine can
// bind the winner to Auto.
//
// All kinds are safe for concurrent use in the modes reported by Kind.Modes.
// Operations never block on a full or empty queue: TryPush reports false when
// full and TryPop reports false when empty. Kinds built on reservation
// (Disruptor) may spin briefly while another producer or consumer completes
// a reservation it already holds, but never wait for capacity.
package queue

import (
	"math/bits"
	"strings"
	"sync/atomic"
)

// Kind identifies a queue variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. MutexRing is the Safe reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// MutexRing is a mutex-guarded ring buffer: the Safe reference.
	MutexRing
	// Chan wraps a buffered channel.
	Chan
	// SeqMPMC is a bounded Vyukov ring: power-of-two slots, a per-slot
	// sequence number, cache-line padded cursors, complete-after-reservation.
	SeqMPMC
	// Disruptor is a single-sequence ring with CAS producer claims on a
	// shared claim cursor, per-slot published/released rounds and a shared
	// consumer cursor.
	Disruptor
	// ShardedMPMC spreads pushes round-robin over several SeqMPMC shards;
	// it does not preserve FIFO order.
	ShardedMPMC
	// AppendLog serialises writers with a mutex and lets readers advance a
	// lock-free cursor over the committed prefix.
	AppendLog
	// SPSC is a Lamport ring for exactly one producer and one consumer.
	SPSC
	// MPSC is a Vyukov ring whose single consumer needs no CAS.
	MPSC
	// Chunked is an unbounded mutex-guarded list of fixed chunks.
	Chunked

	kindCount
)

var kindSlugs = [kindCount]string{
	"auto", "mutexring", "chan", "seqmpmc", "disruptor", "sharded", "appendlog", "spsc", "mpsc", "chunked",
}

var kindIdents = [kindCount]string{
	"Auto", "MutexRing", "Chan", "SeqMPMC", "Disruptor", "ShardedMPMC", "AppendLog", "SPSC", "MPSC", "Chunked",
}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(" + itoa(int(k)) + ")"
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
	for k := MutexRing; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Mode is a set of producer/consumer concurrency modes.
type Mode uint8

// Modes: S = single, M = multiple; P = producer, C = consumer.
const (
	ModeSPSC Mode = 1 << iota
	ModeMPSC
	ModeSPMC
	ModeMPMC

	// AllModes is every mode.
	AllModes = ModeSPSC | ModeMPSC | ModeSPMC | ModeMPMC
)

var modeNames = []struct {
	m    Mode
	name string
}{{ModeSPSC, "SPSC"}, {ModeMPSC, "MPSC"}, {ModeSPMC, "SPMC"}, {ModeMPMC, "MPMC"}}

// String renders the set as "SPSC|MPMC".
func (m Mode) String() string {
	var sb strings.Builder
	for _, e := range modeNames {
		if m&e.m != 0 {
			if sb.Len() > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(e.name)
		}
	}
	if sb.Len() == 0 {
		return "none"
	}
	return sb.String()
}

// Has reports whether every mode in want is in m.
func (m Mode) Has(want Mode) bool { return m&want == want }

// MultiProducer reports whether the mode allows more than one producer.
func (m Mode) MultiProducer() bool { return m&(ModeMPSC|ModeMPMC) != 0 }

// MultiConsumer reports whether the mode allows more than one consumer.
func (m Mode) MultiConsumer() bool { return m&(ModeSPMC|ModeMPMC) != 0 }

// Modes reports the concurrency modes k supports.
func (k Kind) Modes() Mode {
	switch k.Resolve() {
	case SPSC:
		return ModeSPSC
	case MPSC:
		return ModeSPSC | ModeMPSC
	default:
		return AllModes
	}
}

// ExactCapacity reports whether New(k, n).Cap() == n for every n ≥ 1.
// Power-of-two rings round up; Chunked is unbounded.
func (k Kind) ExactCapacity() bool {
	switch k.Resolve() {
	case MutexRing, Chan, AppendLog:
		return true
	default:
		return false
	}
}

// Bounded reports whether TryPush can ever return false.
func (k Kind) Bounded() bool { return k.Resolve() != Chunked }

// Ordered reports whether pops observe pushes in FIFO order (per producer,
// and globally in reservation order).
func (k Kind) Ordered() bool { return k.Resolve() != ShardedMPMC }

// Queue is a lane queue of T. Implementations never block on capacity.
type Queue[T any] interface {
	// TryPush appends v; it reports false when the queue is full.
	TryPush(v T) bool
	// TryPop removes the oldest element; it reports false when empty.
	TryPop() (T, bool)
	// PushBatch appends as many of vs as fit, in order, and returns the count.
	PushBatch(vs []T) int
	// PopBatch fills dst with up to len(dst) oldest elements and returns the count.
	PopBatch(dst []T) int
	// Len returns the number of elements; for lock-free kinds it is an
	// instantaneous approximation that is exact when the queue is quiescent.
	Len() int
	// Cap returns the capacity, or 0 for unbounded kinds.
	Cap() int
}

// New builds a queue of kind k holding at least capacity elements. Capacity
// must be ≥ 1 for bounded kinds; Chunked ignores it. New panics on an
// invalid kind or capacity.
func New[T any](k Kind, capacity int) Queue[T] {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		panic("queue: invalid kind")
	}
	if capacity < 1 && k.Bounded() {
		panic("queue: capacity must be >= 1")
	}
	switch k {
	case MutexRing:
		return newMutexRing[T](capacity)
	case Chan:
		return newChanQueue[T](capacity)
	case SeqMPMC:
		return newSeqMPMC[T](capacity)
	case Disruptor:
		return newDisruptor[T](capacity)
	case ShardedMPMC:
		return newSharded[T](capacity)
	case AppendLog:
		return newAppendLog[T](capacity)
	case SPSC:
		return newSPSC[T](capacity)
	case MPSC:
		return newMPSC[T](capacity)
	case Chunked:
		return newChunked[T]()
	default:
		panic("queue: unreachable")
	}
}

// cacheLine is the assumed cache-line size used for padding hot cursors.
const cacheLine = 64

// pad separates hot atomics so producers and consumers do not false-share.
type pad [cacheLine]byte

// pow2ceil returns the smallest power of two ≥ n (n ≥ 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// clampLen turns a possibly-racy enq-deq difference into a plausible length.
func clampLen(enq, deq uint64, capacity int) int {
	if enq <= deq {
		return 0
	}
	n := enq - deq
	if n > uint64(capacity) {
		return capacity
	}
	return int(n)
}

// itoa avoids importing strconv (and fmt) into a hot-path package.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// loadedCursor is a padded atomic counter.
type loadedCursor struct {
	_ pad
	v atomic.Uint64
	_ pad
}
