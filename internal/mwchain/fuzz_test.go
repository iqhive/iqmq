package mwchain

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// behaviour is what one fuzz-built middleware does with the rest of the
// chain. Between them they cover the contract: pass through, short-circuit
// with an error, short-circuit successfully, mutate the delivery, swallow the
// outcome, run the rest twice, recover a panic, and panic.
type behaviour uint8

const (
	bPass behaviour = iota
	bShortErr
	bShortOK
	bMutate
	bSwallow
	bTwice
	bRecover
	bBoom

	behaviourCount
)

var behaviourNames = [behaviourCount]string{
	"pass", "shorterr", "shortok", "mutate", "swallow", "twice", "recover", "boom",
}

func (b behaviour) String() string {
	if b < behaviourCount {
		return behaviourNames[b]
	}
	return "behaviour(?)"
}

// maxTwice bounds how many middlewares may run the rest of the chain twice:
// every one of them doubles the work, and the contract they exercise is the
// same at three as at ten.
const maxTwice = 3

// spec is one fuzz-chosen middleware.
type spec struct {
	name     string
	b        behaviour
	disabled bool
}

// middleware builds the interception function of s. The same functions are
// given to the compositions under test and to the model, so the only thing
// that varies between them is how they are composed.
func (s spec) middleware() Middleware[*delivery] {
	name := s.name
	mw := Middleware[*delivery]{Name: name, Disabled: s.disabled}
	switch s.b {
	case bPass:
		mw.Fn = func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
			d.mark(name + ">")
			err := next(ctx, d)
			d.mark(name + "<")
			return err
		}
	case bShortErr:
		mw.Fn = func(_ context.Context, d *delivery, _ Handler[*delivery]) error {
			d.mark(name + "!")
			return fmt.Errorf("%s: %w", name, errShort)
		}
	case bShortOK:
		mw.Fn = func(_ context.Context, d *delivery, _ Handler[*delivery]) error {
			d.mark(name + "~")
			return nil
		}
	case bMutate:
		mw.Fn = func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
			d.n++
			d.mark(name + "+")
			return next(ctx, d)
		}
	case bSwallow:
		mw.Fn = func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
			_ = next(ctx, d)
			d.mark(name + "=")
			return nil
		}
	case bTwice:
		mw.Fn = func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
			d.mark(name + "2")
			_ = next(ctx, d)
			return next(ctx, d)
		}
	case bRecover:
		mw.Fn = func(ctx context.Context, d *delivery, next Handler[*delivery]) (err error) {
			defer func() {
				if r := recover(); r != nil {
					d.mark(name + "?")
					err = errBoom
				}
			}()
			return next(ctx, d)
		}
	case bBoom:
		mw.Fn = func(_ context.Context, d *delivery, _ Handler[*delivery]) error {
			d.mark(name + "*")
			panic(name)
		}
	default:
		panic("unknown behaviour")
	}
	return mw
}

// specsFrom decodes a chain of middlewares from fuzz input.
func specsFrom(r *fuzzutil.Reader) []spec {
	n := r.Int(0, 10)
	out := make([]spec, 0, n)
	twice := 0
	for i := range n {
		b := behaviour(r.Choice(int(behaviourCount)))
		if b == bTwice {
			if twice >= maxTwice {
				b = bPass
			} else {
				twice++
			}
		}
		out = append(out, spec{name: string(rune('a' + i)), b: b, disabled: r.Int(0, 7) == 0})
	}
	return out
}

// modelChain is the oracle: the composition written the obvious way, walking
// the middleware list at call time and building the continuation as it goes.
// It allocates per call, which is exactly why it is not one of the kinds, and
// it is short enough to be read as the definition of the contract.
type modelChain struct {
	mws []Middleware[*delivery]
	h   Handler[*delivery]
}

func (m modelChain) Handle(ctx context.Context, d *delivery) error {
	var step func(i int, ctx context.Context, d *delivery) error
	step = func(i int, ctx context.Context, d *delivery) error {
		for i < len(m.mws) && (m.mws[i].Disabled || m.mws[i].Fn == nil) {
			i++
		}
		if i >= len(m.mws) {
			return m.h(ctx, d)
		}
		return m.mws[i].Fn(ctx, d, func(ctx context.Context, d *delivery) error { return step(i+1, ctx, d) })
	}
	return step(0, ctx, d)
}

// outcome is everything one run of a chain produced.
type outcome struct {
	trace   string
	n       int
	err     string
	recover string
}

// runChain runs c over a fresh delivery and records what happened, including
// a panic that reached the caller.
func runChain(handle func(ctx context.Context, d *delivery) error) (o outcome) {
	d := &delivery{}
	defer func() {
		if r := recover(); r != nil {
			o.recover = fmt.Sprint(r)
		}
		o.trace = strings.Join(d.trace, " ")
		o.n = d.n
	}()
	err := handle(context.Background(), d)
	if err != nil {
		o.err = err.Error()
	}
	return o
}

// handlerFor returns the terminal handler used by every composition of one
// fuzz case: it marks the delivery and alternates its outcome.
func handlerFor() Handler[*delivery] {
	calls := 0
	return func(_ context.Context, d *delivery) error {
		calls++
		d.mark("h")
		if calls%2 == 1 {
			return errHandler
		}
		return nil
	}
}

// compareChains runs the model and every kind over the same middlewares and
// requires them to agree on everything an observer can see.
func compareChains(t testing.TB, specs []spec) {
	t.Helper()
	mws := middlewaresOf(specs)
	for _, k := range Kinds() {
		compareKind(t, k, mws, specs)
	}
}

// compareKind runs one kind and the model over the same middlewares and
// requires them to agree on everything an observer can see.
func compareKind(t testing.TB, k Kind, mws []Middleware[*delivery], specs []spec) {
	t.Helper()
	want := runChain(modelChain{mws: mws, h: handlerFor()}.Handle)
	enabledCount := 0
	for i := range mws {
		if !mws[i].Disabled && mws[i].Fn != nil {
			enabledCount++
		}
	}
	c := New(k, handlerFor(), mws...)
	if c.Len() != enabledCount {
		t.Fatalf("%v: Len = %d, want %d", k, c.Len(), enabledCount)
	}
	if got := runChain(c.Handle); got != want {
		t.Fatalf("%v: chain of %s\n got %+v\nwant %+v", k, describe(specs), got, want)
	}
}

// middlewaresOf builds the interception functions of a fuzz-chosen chain.
func middlewaresOf(specs []spec) []Middleware[*delivery] {
	mws := make([]Middleware[*delivery], len(specs))
	for i, s := range specs {
		mws[i] = s.middleware()
	}
	return mws
}

// describe renders a chain for a failure message.
func describe(specs []spec) string {
	parts := make([]string, len(specs))
	for i, s := range specs {
		parts[i] = s.b.String()
		if s.disabled {
			parts[i] += "(off)"
		}
	}
	return strings.Join(parts, ",")
}

// mwSeeds authors readable seed chains.
func mwSeeds() [][]byte {
	chains := [][]behaviour{
		{},
		{bPass},
		{bPass, bPass, bPass},
		{bPass, bShortErr, bPass},
		{bPass, bShortOK},
		{bMutate, bMutate, bPass},
		{bRecover, bPass, bBoom},
		{bPass, bTwice, bPass},
		{bSwallow, bShortErr},
		{bPass, bPass, bPass, bPass, bPass},
		{bRecover, bTwice, bMutate, bSwallow, bPass, bShortErr},
	}
	seeds := make([][]byte, 0, len(chains))
	for _, chain := range chains {
		w := fuzzutil.NewWriter()
		w.Int(0, 10, len(chain))
		for _, b := range chain {
			w.Choice(int(behaviourCount), int(b))
			w.Int(0, 7, 1) // enabled
		}
		seeds = append(seeds, w.Data())
	}
	// One chain with a disabled link in the middle.
	w := fuzzutil.NewWriter()
	w.Int(0, 10, 3)
	for i, b := range []behaviour{bPass, bMutate, bPass} {
		w.Choice(int(behaviourCount), int(b))
		if i == 1 {
			w.Int(0, 7, 0) // disabled
		} else {
			w.Int(0, 7, 1)
		}
	}
	seeds = append(seeds, w.Data())
	return seeds
}

// FuzzMiddlewareChain is the differential fuzz target (the test suite): a fuzz-chosen chain of middlewares is
// composed by every kind and by the obvious model composition, and all four
// must produce the same trace, the same delivery, the same error and the same
// escaping panic. That covers each middleware running exactly once and in
// order, short-circuits, re-entrancy, recovery where it is installed, and
// disabled links being absent rather than skipped.
func FuzzMiddlewareChain(f *testing.F) {
	for _, s := range mwSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		compareChains(t, specsFrom(r))
	})
}
