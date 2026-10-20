// Package mwchain composes middleware into the one handler the runner calls
// per delivery (the API, OBS-01).
//
// A chain is composed once, when Run starts, and then called for every
// message: the composition may allocate, the call must not. That is the whole
// point of the family. The kinds here differ only in how the composed chain
// is laid out — nested closures, a slice walked by index, or a generated type
// per arity — and the bake-off decides which becomes Auto against the budget
// of 30 ns and zero allocations for five no-op middlewares.
//
// A middleware receives the delivery and the rest of the chain, and decides
// what to do with it:
//
//	mw := mwchain.Middleware[*Delivery]{Name: "timeout", Fn: func(
//		ctx context.Context, d *Delivery, next mwchain.Handler[*Delivery],
//	) error {
//		return next(ctx, d)
//	}}
//
// Contract. Every enabled middleware runs exactly once per call, in the order
// given, outermost first; a middleware that does not call next short-circuits
// the rest of the chain and the handler; the value it returns is what the
// caller sees. A disabled middleware is left out when the chain is composed,
// so the cost of a middleware nobody enabled is nothing at all — not a branch
// per message. A middleware that calls next more than once gets the rest of
// the chain more than once: the chain holds no per-call state, which is what
// makes re-entrancy well defined and Handle safe to call from several
// goroutines at once.
//
// Panics are not caught here. A Recover middleware installed at the head of
// the chain sees the panic of anything after it, which is how the runner
// turns a handler panic into a *PanicError.
package mwchain

import (
	"context"
	"errors"
)

//go:generate go run ./internal/fusegen/cmd -o fused_gen.go

// Kind identifies a chain composition. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Closure is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Closure nests one closure per middleware, each capturing the rest of
	// the chain: the composition every Go middleware library uses, and the
	// reference implementation.
	Closure
	// Slice keeps the middlewares in a slice and walks it by index, with the
	// continuations bound once at composition time.
	Slice
	// Fused uses a generated struct per chain length, so the middleware
	// functions and their continuations sit together in one object instead
	// of in a chain of separately allocated closures. Chains longer than
	// MaxFused fall back to the closure composition.
	Fused

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "closure", "slice", "fused"}

var kindIdents = [kindCount]string{"Auto", "Closure", "Slice", "Fused"}

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
	for k := Closure; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Generated reports whether the kind's composition comes from generated code
// and therefore only covers chains up to MaxFused.
func (k Kind) Generated() bool { return k.Resolve() == Fused }

// Handler is the terminal of a chain and the continuation a middleware is
// given. It returns the outcome of the delivery; the runner turns that into
// an ack, a nack or a dead-letter.
type Handler[D any] func(ctx context.Context, d D) error

// Func is one middleware's interception function. It receives the delivery
// and the rest of the chain, and must call next at most once per outcome it
// wants to produce; calling it zero times short-circuits, and calling it more
// than once repeats the rest of the chain.
type Func[D any] func(ctx context.Context, d D, next Handler[D]) error

// Middleware is one named link of a chain.
type Middleware[D any] struct {
	// Name identifies the middleware in traces and benchmarks.
	Name string
	// Fn is the interception function. A nil Fn is a middleware that is not
	// there: New leaves it out.
	Fn Func[D]
	// Disabled leaves the middleware out of the composed chain, so that a
	// feature nobody turned on costs nothing per message. The zero value is
	// therefore an enabled middleware.
	Disabled bool
}

// Chain is a composed middleware chain. It is immutable and safe for
// concurrent use: Handle keeps no per-call state anywhere.
type Chain[D any] interface {
	// Handle runs the chain and then the handler.
	Handle(ctx context.Context, d D) error
	// Len returns the number of middlewares in the composed chain, which is
	// the number that were enabled.
	Len() int
	// Kind returns the composition this chain uses. A Fused chain longer
	// than MaxFused reports Closure, the composition it fell back to.
	Kind() Kind
}

// New composes mws around h using composition k. Disabled middlewares and
// those with a nil Fn are left out. It fails on an invalid kind or a nil
// handler.
func New[D any](k Kind, h Handler[D], mws ...Middleware[D]) (Chain[D], error) {
	if h == nil {
		return nil, errors.New("mwchain: nil handler")
	}
	fns := enabled(mws)
	switch k.Resolve() {
	case Closure:
		return newClosureChain(h, fns), nil
	case Slice:
		return newSliceChain(h, fns), nil
	case Fused:
		return newFusedChain(h, fns), nil
	default:
		return nil, errors.New("mwchain: invalid kind")
	}
}

// enabled returns the interception functions of the middlewares that are in
// force, in order.
func enabled[D any](mws []Middleware[D]) []Func[D] {
	n := 0
	for i := range mws {
		if mws[i].Fn != nil && !mws[i].Disabled {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	out := make([]Func[D], 0, n)
	for i := range mws {
		if mws[i].Fn != nil && !mws[i].Disabled {
			out = append(out, mws[i].Fn)
		}
	}
	return out
}

// closureChain nests one closure per middleware. Each closure captures the
// middleware and the continuation it wraps, so a call walks a chain of
// separately allocated closure objects.
type closureChain[D any] struct {
	run Handler[D]
	n   int
}

func newClosureChain[D any](h Handler[D], fns []Func[D]) *closureChain[D] {
	run := h
	for i := len(fns) - 1; i >= 0; i-- {
		fn, next := fns[i], run
		run = func(ctx context.Context, d D) error { return fn(ctx, d, next) }
	}
	return &closureChain[D]{run: run, n: len(fns)}
}

// Handle implements Chain.
func (c *closureChain[D]) Handle(ctx context.Context, d D) error { return c.run(ctx, d) }

// Len implements Chain.
func (c *closureChain[D]) Len() int { return c.n }

// Kind implements Chain.
func (c *closureChain[D]) Kind() Kind { return Closure }

// sliceChain keeps the middlewares in one slice and walks it by index. The
// continuations are bound once at composition time, so a call allocates
// nothing; what it pays instead of a closure dereference is a bounds-checked
// slice load per link.
type sliceChain[D any] struct {
	fns  []Func[D]
	next []Handler[D]
	h    Handler[D]
}

func newSliceChain[D any](h Handler[D], fns []Func[D]) *sliceChain[D] {
	c := &sliceChain[D]{fns: fns, h: h}
	if len(fns) == 0 {
		return c
	}
	c.next = make([]Handler[D], len(fns))
	for i := range fns {
		c.next[i] = func(ctx context.Context, d D) error { return c.step(ctx, i+1, d) }
	}
	return c
}

// step runs link i of the chain, or the handler when the chain is exhausted.
func (c *sliceChain[D]) step(ctx context.Context, i int, d D) error {
	if i >= len(c.fns) {
		return c.h(ctx, d)
	}
	return c.fns[i](ctx, d, c.next[i])
}

// Handle implements Chain.
func (c *sliceChain[D]) Handle(ctx context.Context, d D) error { return c.step(ctx, 0, d) }

// Len implements Chain.
func (c *sliceChain[D]) Len() int { return len(c.fns) }

// Kind implements Chain.
func (c *sliceChain[D]) Kind() Kind { return Slice }
