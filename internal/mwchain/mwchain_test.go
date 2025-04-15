package mwchain

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/mwchain/internal/fusegen"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// delivery is what the tests push through a chain: a mutable counter, so a
// middleware can be seen to have run, and a trace of who ran in which order.
type delivery struct {
	n     int
	trace []string
}

func (d *delivery) mark(s string) { d.trace = append(d.trace, s) }

// errShort and errBoom are the outcomes the test middlewares produce.
var (
	errShort   = errors.New("short-circuited")
	errHandler = errors.New("handler")
	errBoom    = errors.New("recovered panic")
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 3 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		c := New[*delivery](k, func(context.Context, *delivery) error { return nil })
		if c.Len() != 0 || c.Kind() != k {
			t.Fatalf("New(%v) empty chain: len %d kind %v", k, c.Len(), c.Kind())
		}
	}
	if !Fused.Generated() || Closure.Generated() {
		t.Fatal("Generated")
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted junk")
	}
}

func TestNewPanics(t *testing.T) {
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	expectPanic("invalid kind", func() {
		New[*delivery](Kind(99), func(context.Context, *delivery) error { return nil })
	})
	expectPanic("nil handler", func() { New[*delivery](Closure, nil) })
	c := New[*delivery](Auto, func(context.Context, *delivery) error { return nil })
	if c.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", c.Kind())
	}
}

// passthrough returns a middleware that records its name on the way in and on
// the way out and calls the rest of the chain in between.
func passthrough(name string) Middleware[*delivery] {
	return Middleware[*delivery]{Name: name, Fn: func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
		d.mark(name + ">")
		err := next(ctx, d)
		d.mark(name + "<")
		return err
	}}
}

// TestOrder checks the contract every kind shares: each enabled middleware
// runs once, outermost first, and the handler runs last.
func TestOrder(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			d := &delivery{}
			c := New(k,
				func(_ context.Context, d *delivery) error { d.mark("h"); return errHandler },
				passthrough("a"), passthrough("b"), passthrough("c"))
			if c.Len() != 3 {
				t.Fatalf("Len = %d", c.Len())
			}
			if err := c.Handle(context.Background(), d); !errors.Is(err, errHandler) {
				t.Fatalf("Handle = %v", err)
			}
			if got := strings.Join(d.trace, " "); got != "a> b> c> h c< b< a<" {
				t.Fatalf("trace = %q", got)
			}
		})
	}
}

// TestDisabledCostsNothing checks that a disabled middleware and one with no
// function are left out of the composed chain entirely, which is what makes
// the disabled path free.
func TestDisabledCostsNothing(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			off := passthrough("off")
			off.Disabled = true
			d := &delivery{}
			c := New(k,
				func(_ context.Context, d *delivery) error { d.mark("h"); return nil },
				off, Middleware[*delivery]{Name: "nil"}, passthrough("on"), off)
			if c.Len() != 1 {
				t.Fatalf("Len = %d, want 1", c.Len())
			}
			if err := c.Handle(context.Background(), d); err != nil {
				t.Fatalf("Handle = %v", err)
			}
			if got := strings.Join(d.trace, " "); got != "on> h on<" {
				t.Fatalf("trace = %q", got)
			}
		})
	}
}

// TestShortCircuit checks that a middleware that does not call next stops the
// rest of the chain and the handler, and that its error is what the caller
// sees.
func TestShortCircuit(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			stop := Middleware[*delivery]{Name: "stop", Fn: func(_ context.Context, d *delivery, _ Handler[*delivery]) error {
				d.mark("stop")
				return errShort
			}}
			d := &delivery{}
			c := New(k,
				func(_ context.Context, d *delivery) error { d.mark("h"); return nil },
				passthrough("a"), stop, passthrough("b"))
			if err := c.Handle(context.Background(), d); !errors.Is(err, errShort) {
				t.Fatalf("Handle = %v", err)
			}
			if got := strings.Join(d.trace, " "); got != "a> stop a<" {
				t.Fatalf("trace = %q", got)
			}
		})
	}
}

// TestReentrant checks that a middleware may run the rest of the chain more
// than once: the chain holds no per-call state, so the second pass sees the
// same continuation as the first.
func TestReentrant(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			retry := Middleware[*delivery]{Name: "retry", Fn: func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
				if err := next(ctx, d); err != nil {
					return next(ctx, d)
				}
				return nil
			}}
			attempts := 0
			d := &delivery{}
			c := New(k, func(_ context.Context, d *delivery) error {
				attempts++
				d.mark("h")
				if attempts == 1 {
					return errHandler
				}
				return nil
			}, passthrough("a"), retry, passthrough("b"))
			if err := c.Handle(context.Background(), d); err != nil {
				t.Fatalf("Handle = %v", err)
			}
			if attempts != 2 {
				t.Fatalf("handler ran %d times", attempts)
			}
			if got := strings.Join(d.trace, " "); got != "a> b> h b< b> h b< a<" {
				t.Fatalf("trace = %q", got)
			}
		})
	}
}

// TestPanicPropagates checks that the chain does not swallow a panic and that
// a recovering middleware sees everything after it.
func TestPanicPropagates(t *testing.T) {
	boom := Middleware[*delivery]{Name: "boom", Fn: func(_ context.Context, d *delivery, _ Handler[*delivery]) error {
		d.mark("boom")
		panic("boom")
	}}
	recoverMW := Middleware[*delivery]{Name: "recover", Fn: func(ctx context.Context, d *delivery, next Handler[*delivery]) (err error) {
		defer func() {
			if r := recover(); r != nil {
				d.mark("recovered")
				err = errBoom
			}
		}()
		return next(ctx, d)
	}}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			d := &delivery{}
			c := New(k, func(context.Context, *delivery) error { return nil }, recoverMW, passthrough("a"), boom)
			if err := c.Handle(context.Background(), d); !errors.Is(err, errBoom) {
				t.Fatalf("Handle = %v", err)
			}
			if got := strings.Join(d.trace, " "); got != "a> boom recovered" {
				t.Fatalf("trace = %q", got)
			}
			// Without the recovering middleware the panic reaches the caller.
			d = &delivery{}
			c = New(k, func(context.Context, *delivery) error { return nil }, passthrough("a"), boom)
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Fatal("chain swallowed the panic")
					}
				}()
				_ = c.Handle(context.Background(), d)
			}()
		})
	}
}

// TestFusedFallsBackBeyondMax checks the declared narrowing of the generated
// kind: a chain longer than MaxFused is composed as a closure chain, and says
// so.
func TestFusedFallsBackBeyondMax(t *testing.T) {
	mws := make([]Middleware[*delivery], 0, MaxFused+1)
	for i := range MaxFused + 1 {
		mws = append(mws, passthrough(string(rune('a'+i))))
	}
	c := New(Fused, func(_ context.Context, d *delivery) error { d.mark("h"); return nil }, mws...)
	if c.Len() != MaxFused+1 {
		t.Fatalf("Len = %d", c.Len())
	}
	if c.Kind() != Closure {
		t.Fatalf("Kind = %v, want the closure fallback", c.Kind())
	}
	d := &delivery{}
	if err := c.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v", err)
	}
	if len(d.trace) != 2*(MaxFused+1)+1 {
		t.Fatalf("trace = %v", d.trace)
	}
	// Every generated arity is reachable and reports itself as fused.
	for n := range MaxFused + 1 {
		c := New(Fused, func(context.Context, *delivery) error { return nil }, mws[:n]...)
		if c.Len() != n || c.Kind() != Fused {
			t.Fatalf("n=%d: len %d kind %v", n, c.Len(), c.Kind())
		}
	}
}

// TestGeneratedFileIsUpToDate fails when fused_gen.go and the generator that
// writes it have drifted apart (make generate / go generate ./internal/mwchain).
func TestGeneratedFileIsUpToDate(t *testing.T) {
	on, err := os.ReadFile("fused_gen.go")
	if err != nil {
		t.Fatal(err)
	}
	if want := fusegen.Source(MaxFused); !bytes.Equal(on, want) {
		t.Fatalf("fused_gen.go is stale: run go generate ./internal/mwchain (%d bytes on disk, %d generated)", len(on), len(want))
	}
	if fusegen.Max != MaxFused {
		t.Fatalf("fusegen.Max = %d but MaxFused = %d", fusegen.Max, MaxFused)
	}
}

// TestAllocs pins the budget five no-op middlewares must add no
// allocation at all to a delivery.
func TestAllocs(t *testing.T) {
	count := Middleware[*delivery]{Name: "count", Fn: func(ctx context.Context, d *delivery, next Handler[*delivery]) error {
		d.n++
		return next(ctx, d)
	}}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			for _, n := range []int{0, 1, 3, 5} {
				mws := make([]Middleware[*delivery], n)
				for i := range mws {
					mws[i] = count
				}
				c := New(k, func(_ context.Context, d *delivery) error { d.n++; return nil }, mws...)
				d := &delivery{}
				ctx := context.Background()
				call := func() {
					if err := c.Handle(ctx, d); err != nil {
						t.Fatalf("Handle: %v", err)
					}
				}
				for range 100 {
					call()
				}
				if allocs := testing.AllocsPerRun(1000, call); allocs != 0 {
					t.Fatalf("%v n=%d: %.2f allocs per call", k, n, allocs)
				}
				if d.n == 0 {
					t.Fatal("chain did not run")
				}
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the mwchain family
// (front-matter `auto: {mwchain: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "mwchain")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds mwchain yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds mwchain to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the package on the standard library and off the root.
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, ".")
	testguard.StdlibOnly(t, ".")
}
