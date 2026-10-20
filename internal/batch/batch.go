// Package batch holds the publisher batching triggers how a publisher decides that
// the batch it is filling should go out now.
//
// Every kind fills the same batch and honours the same size thresholds
// (Config.MaxMessages, Config.MaxBytes); they differ only in how the
// linger deadline — the promise that a partly filled batch still leaves
// within Config.Linger — is armed and detected:
//
//   - PerBatchTimer arms one timer per batch, so latency is exact and the
//     cost is one timer per batch (the timers/op benchmarks report it).
//   - SharedTicker registers with one ticker shared by every batcher, so
//     the cost is one timer per process at the price of rounding each
//     deadline up to the next tick.
//   - SizeOnly arms nothing: a batch leaves when it is full and otherwise
//     waits for the caller to flush it, which is the cheapest trigger and
//     the one with no latency bound at a low publish rate.
//
// Readiness is a pure function of the batch's contents and the current
// time (Ready, TakeReady), so a caller can poll, and a test can drive a
// whole publisher deterministically off a fake clock. The timer and the
// ticker only wake a caller that is blocked in a select: Wait returns the
// channel to select on. Batchers are safe for concurrent use.
package batch

import (
	"fmt"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Kind identifies a batching trigger. Auto resolves to AutoKind.
type Kind uint8

// Kinds. PerBatchTimer is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// PerBatchTimer arms a fresh timer when the first item of a batch
	// arrives and stops it when the batch leaves. It is the reference.
	PerBatchTimer
	// SharedTicker rounds the deadline up to the next tick of a ticker
	// shared by every batcher registered with it.
	SharedTicker
	// SizeOnly ignores Config.Linger: a batch is ready only when it
	// reaches MaxMessages or MaxBytes, or when the caller flushes it.
	SizeOnly

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "perbatchtimer", "sharedticker", "sizeonly"}

var kindIdents = [kindCount]string{"Auto", "PerBatchTimer", "SharedTicker", "SizeOnly"}

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
	for k := PerBatchTimer; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Timed reports whether the kind bounds the age of a partly filled batch.
func (k Kind) Timed() bool { return k.Resolve() != SizeOnly }

// Exact reports whether the linger deadline is honoured exactly, rather
// than rounded up to a shared tick.
func (k Kind) Exact() bool { return k.Resolve() == PerBatchTimer }

// TimersPerBatch reports how many timers the kind creates per batch: one
// for PerBatchTimer, none for the others (SharedTicker amortises a single
// timer over every batcher and every batch).
func (k Kind) TimersPerBatch() int {
	if k.Resolve() == PerBatchTimer {
		return 1
	}
	return 0
}

// Config parameterises a batcher. A zero threshold means "no limit from
// this dimension"; a batcher with no threshold at all only ever flushes
// when the caller asks.
type Config struct {
	// MaxMessages is the item count that makes a batch ready. It is a
	// trigger, not a cap: Add never refuses an item, so a caller that
	// ignores a ready batch keeps filling it.
	MaxMessages int
	// MaxBytes is the accumulated size that makes a batch ready. An item
	// larger than MaxBytes makes its batch ready on its own.
	MaxBytes int
	// Linger is how long a partly filled batch may wait. Zero means the
	// batch is ready as soon as it holds an item (for the timed kinds).
	Linger time.Duration
	// Clock is the time source; nil means clock.Real.
	Clock clock.Clock
	// Ticker is the shared ticker for the SharedTicker kind. Nil means the
	// batcher creates one of its own, which is the degenerate case: the
	// kind exists to share one ticker between many batchers.
	Ticker *Ticker
}

// Batcher accumulates items of type T until the batch is ready.
//
// A caller either polls (TakeReady) or blocks (select on Wait). The
// channel Wait returns belongs to the current batch: fetch it again after
// every flush.
type Batcher[T any] interface {
	// Kind returns the concrete kind (never Auto).
	Kind() Kind
	// Add appends v, whose size counts size bytes towards MaxBytes, and
	// reports whether the batch is ready to leave now. It returns
	// ErrClosed after Close, having added nothing.
	Add(v T, size int) (ready bool, err error)
	// Ready reports whether the batch should leave at now.
	Ready(now time.Time) bool
	// TakeReady appends the batch to dst and clears it when it is ready at
	// now, reporting whether it did.
	TakeReady(now time.Time, dst []T) ([]T, bool)
	// Flush appends every pending item to dst and clears the batch,
	// whether or not it was ready. It works after Close, so a closed
	// batcher never strands an item.
	Flush(dst []T) []T
	// Wait returns the channel that fires when the batch's linger deadline
	// passes, or nil when the kind or the configuration arms nothing. The
	// channel is only a wake-up: readiness is decided by Ready.
	Wait() <-chan time.Time
	// Deadline returns the instant at which the batch becomes ready
	// without further items, and false when nothing is armed.
	Deadline() (time.Time, bool)
	// Len returns the number of pending items.
	Len() int
	// Bytes returns the accumulated size of the pending items.
	Bytes() int
	// Close releases the batcher's timer or ticker registration. It is
	// idempotent and never discards pending items: Flush still returns
	// them. The linger deadline is disarmed, so after Close a batch is
	// ready only on its size thresholds and the caller must Flush.
	Close() error
}

// New returns a batcher of kind k, or fails on an invalid kind.
func New[T any](k Kind, cfg Config) (Batcher[T], error) {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		return nil, fmt.Errorf("batch: invalid kind %d", int(k))
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Linger < 0 {
		cfg.Linger = 0
	}
	b := &batcher[T]{kind: k, cfg: cfg, clk: cfg.Clock}
	if k == SharedTicker {
		b.ticker = cfg.Ticker
		if b.ticker == nil {
			b.ticker = NewTicker(cfg.Clock, tickPeriod(cfg.Linger))
			b.ownTicker = true
		}
		b.wake = make(chan time.Time, 1)
	}
	return b, nil
}

// tickPeriod is the period a batcher-owned ticker runs at: the linger
// itself, so a deadline is never rounded up by more than one linger.
func tickPeriod(linger time.Duration) time.Duration {
	if linger <= 0 {
		return time.Millisecond
	}
	return linger
}

// batcher implements every kind; the kind selects how the deadline is
// computed and armed.
type batcher[T any] struct {
	kind Kind
	cfg  Config
	clk  clock.Clock

	mu       sync.Mutex
	items    []T
	bytes    int
	deadline time.Time
	timed    bool // a deadline is set for the current batch
	closed   bool

	timer clock.Timer    // PerBatchTimer: the current batch's timer
	wake  chan time.Time // SharedTicker: the ticker's wake-up channel

	ticker    *Ticker
	ownTicker bool
	regd      bool
}

func (b *batcher[T]) Kind() Kind { return b.kind }

func (b *batcher[T]) Add(v T, size int) (bool, error) {
	if size < 0 {
		size = 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false, errcode.ErrClosed
	}
	first := len(b.items) == 0
	b.items = append(b.items, v)
	b.bytes += size
	if b.bytes < 0 { // saturate rather than wrap on absurd sizes
		b.bytes = int(^uint(0) >> 1)
	}
	if first {
		b.arm(b.clk.Now())
	}
	return b.sizeReady(), nil
}

// arm sets the deadline of a batch whose first item arrived at now and
// wakes the mechanism that will notify a blocked caller.
func (b *batcher[T]) arm(now time.Time) {
	switch b.kind {
	case PerBatchTimer:
		b.deadline = now.Add(b.cfg.Linger)
		b.timed = true
		b.timer = b.clk.NewTimer(b.cfg.Linger)
	case SharedTicker:
		b.deadline = b.ticker.deadlineFor(now.Add(b.cfg.Linger))
		b.timed = true
		if !b.regd {
			b.ticker.register(b.wake)
			b.regd = true
		}
	case SizeOnly:
		b.timed = false
	default:
		b.timed = false
	}
}

// disarm releases whatever arm installed.
func (b *batcher[T]) disarm() {
	b.timed = false
	b.deadline = time.Time{}
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if b.regd {
		b.ticker.unregister(b.wake)
		b.regd = false
		select { // drop a tick delivered while the batch was leaving
		case <-b.wake:
		default:
		}
	}
}

// sizeReady reports whether the size thresholds are met. b.mu held.
func (b *batcher[T]) sizeReady() bool {
	if len(b.items) == 0 {
		return false
	}
	if b.cfg.MaxMessages > 0 && len(b.items) >= b.cfg.MaxMessages {
		return true
	}
	return b.cfg.MaxBytes > 0 && b.bytes >= b.cfg.MaxBytes
}

// ready reports readiness at now. b.mu held.
func (b *batcher[T]) ready(now time.Time) bool {
	if len(b.items) == 0 {
		return false
	}
	if b.sizeReady() {
		return true
	}
	return b.timed && !now.Before(b.deadline)
}

func (b *batcher[T]) Ready(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready(now)
}

func (b *batcher[T]) TakeReady(now time.Time, dst []T) ([]T, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.ready(now) {
		return dst, false
	}
	return b.take(dst), true
}

func (b *batcher[T]) Flush(dst []T) []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		return dst
	}
	return b.take(dst)
}

// take moves the batch into dst and resets the batcher. b.mu held.
func (b *batcher[T]) take(dst []T) []T {
	dst = append(dst, b.items...)
	var zero T
	for i := range b.items {
		b.items[i] = zero // let the items be collected
	}
	b.items = b.items[:0]
	b.bytes = 0
	b.disarm()
	return dst
}

func (b *batcher[T]) Wait() <-chan time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.timer != nil:
		return b.timer.C()
	case b.kind == SharedTicker && b.regd:
		return b.wake
	default:
		return nil
	}
}

func (b *batcher[T]) Deadline() (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deadline, b.timed
}

func (b *batcher[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

func (b *batcher[T]) Bytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes
}

func (b *batcher[T]) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.disarm()
	if b.ownTicker {
		_ = b.ticker.Close()
	}
	return nil
}
