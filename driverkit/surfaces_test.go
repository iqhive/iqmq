package driverkit_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// Three of the four defects, and the specification recorded in hack/FOLLOWUPS.md are
// instances of one class: a wrapper in this package that narrows a driver's
// surface without saying so. The specification found it for DeadlineExtender, found
// it one surface along for BatchSettler, and the next wrapper somebody adds
// will find it again unless something enumerates the pairs.
//
// This file is that enumeration: every optional interface the specification defines, against
// every wrapper this package has, with the answer written down and checked in
// both directions — a surface the wrapped value has is preserved, and a
// surface it does not have is not invented.

// optionalSurface is one of the optional interfaces.
type optionalSurface struct {
	name string
	has  func(any) bool
}

// optionalSurfaces is every optional interface the SPI defines, in
// spi.go's declaration order. A wrapper is measured against all of them and
// not only against the two that have already gone wrong.
var optionalSurfaces = []optionalSurface{
	{"BatchSink", func(v any) bool { _, ok := v.(mq.BatchSink); return ok }},
	{"AsyncSink", func(v any) bool { _, ok := v.(mq.AsyncSink); return ok }},
	{"OwnedSink", func(v any) bool { _, ok := v.(mq.OwnedSink); return ok }},
	{"FlushSink", func(v any) bool { _, ok := v.(mq.FlushSink); return ok }},
	{"BatchSettler", func(v any) bool { _, ok := v.(mq.BatchSettler); return ok }},
	{"DeadlineExtender", func(v any) bool { _, ok := v.(mq.DeadlineExtender); return ok }},
	{"AdminBackend", func(v any) bool { _, ok := v.(mq.AdminBackend); return ok }},
	{"HealthReporter", func(v any) bool { _, ok := v.(mq.HealthReporter); return ok }},
}

// wrapperCase is one wrapper of this package, with the surfaces it must
// forward and the reason the rest are not its business.
type wrapperCase struct {
	// name is the wrapper's qualified Go name, which
	// TestEveryWrapperIsInTheSurfaceTable matches against the source.
	name string
	// forwards names the surfaces the wrapper must preserve. Every other
	// surface in optionalSurfaces must be absent from the wrapper's result,
	// whatever the wrapped value has.
	forwards []string
	// why explains, for the record, what the wrapper's subject is and why
	// the surfaces it does not forward do not apply to it.
	why string
	// rich wraps a value that implements every surface in forwards, and
	// returns the thing the core type-asserts.
	rich func(*testing.T) any
	// bare wraps a value that implements none of them.
	bare func(*testing.T) any
}

// fullSettler is a driver's settler with both of the settlement surfaces the specification
// makes optional: Redis, whose settlement is a round trip and whose deliveries
// hold a lease.
type fullSettler struct{ batchingDriverSettler }

// bareSettler is a driver's settler with neither: NATS Core, whose settlement
// is fire and forget and which has no lease to extend.
type bareSettler struct{}

func (bareSettler) Ack(context.Context, *mq.DriverDelivery) error { return nil }

func (bareSettler) Nack(context.Context, *mq.DriverDelivery, mq.NackOptions) error { return nil }

// stubSource is a Source whose settler is whatever the case under test needs.
type stubSource struct{ settler mq.Settler }

func (*stubSource) Fetch(context.Context, []mq.DriverDelivery) (int, error) { return 0, nil }

func (s *stubSource) Settler() mq.Settler { return s.settler }

func (*stubSource) Close() error { return nil }

// batchSettlerOver is the value a driver's Source.Settler returns when the
// driver settles through the kit's coalescing settler.
func batchSettlerOver(t *testing.T, inner mq.Settler) any {
	t.Helper()
	b, err := driverkit.NewBatchSettler(inner, driverkit.BatchConfig{})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b.Settler()
}

// wrappedSource is the source the envelope's adapter hands the core.
func wrappedSource(t *testing.T, inner mq.Settler) mq.Source {
	t.Helper()
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return env.WrapSource(&stubSource{settler: inner})
}

// settlementSurfaces is the pair the specification makes optional on a Settler.
var settlementSurfaces = []string{"BatchSettler", "DeadlineExtender"}

// wrapperCases is the table: this package's wrappers, and what each of the specification's
// optional interfaces means for them.
var wrapperCases = []wrapperCase{
	{
		name:     "WrapSettler",
		forwards: settlementSurfaces,
		why: "its subject is a Settler, so the four sink surfaces and the two " +
			"backend surfaces cannot apply: a Settler is never opened as a sink " +
			"and never answers for a backend. FlushSink in particular must not " +
			"appear — the kit's own coalescing settler has a Flush(ctx) error " +
			"method of its own, which would satisfy the SINK interface by " +
			"accident and tell the core a settler buffers publications.",
		rich: func(*testing.T) any {
			return driverkit.WrapSettler(bareSettler{}, &fullSettler{})
		},
		bare: func(*testing.T) any {
			return driverkit.WrapSettler(bareSettler{}, bareSettler{})
		},
	},
	{
		name:     "(*BatchSettler).Settler",
		forwards: settlementSurfaces,
		why: "it is the value a driver's Source.Settler hands the core, and it " +
			"is WrapSettler's result: same subject, same answer.",
		rich: func(t *testing.T) any { return batchSettlerOver(t, &fullSettler{}) },
		bare: func(t *testing.T) any { return batchSettlerOver(t, bareSettler{}) },
	},
	{
		name:     "(*Envelope).WrapSource",
		forwards: nil,
		why: "its subject is a Source, which the specification gives no optional interface of " +
			"its own, so the wrapped source must answer none of them. What a " +
			"source adapter can still narrow is the settler it hands on, which " +
			"is (*envelopeSource).Settler's row.",
		rich: func(t *testing.T) any { return wrappedSource(t, &fullSettler{}) },
		bare: func(t *testing.T) any { return wrappedSource(t, bareSettler{}) },
	},
	{
		name:     "(*envelopeSource).Settler",
		forwards: settlementSurfaces,
		why: "the envelope is a body encoding and not a handle, so this adapter " +
			"passes the driver's own settler through untouched rather than " +
			"wrapping it; the pass-through is what keeps both settlement " +
			"surfaces, and it is checked here because a future adapter that " +
			"wrapped would silently drop them.",
		rich: func(t *testing.T) any { return wrappedSource(t, &fullSettler{}).Settler() },
		bare: func(t *testing.T) any { return wrappedSource(t, bareSettler{}).Settler() },
	},
}

// TestNoWrapperDropsAnOptionalSurface is the check that stops the next
// instance of the defect class.
//
// For every wrapper in this package and every optional interface the specification defines it
// asserts both directions. A surface the wrapped value has must survive the
// wrapper, because the core type-asserts once when the source is opened and a
// narrowed wrapper takes the ability away for ever with nothing reporting a
// failure. A surface the wrapped value does NOT have must not appear, because a
// wrapper that always implements it makes a broker look as though it had a
// feature it has not got.
func TestNoWrapperDropsAnOptionalSurface(t *testing.T) {
	for _, w := range wrapperCases {
		t.Run(w.name, func(t *testing.T) {
			forwards := make(map[string]bool, len(w.forwards))
			for _, name := range w.forwards {
				if !surfaceExists(name) {
					t.Fatalf("%s forwards %q, which is not one of the optional interfaces", w.name, name)
				}
				forwards[name] = true
			}
			rich, bare := w.rich(t), w.bare(t)
			for _, s := range optionalSurfaces {
				switch {
				case forwards[s.name]:
					if !s.has(rich) {
						t.Errorf("%s dropped %s: a driver that has it loses it through the wrapper, "+
							"and the core decides once at open. %s", w.name, s.name, w.why)
					}
					if s.has(bare) {
						t.Errorf("%s invented %s over a driver that has none: the core would offer a "+
							"feature the broker has not got", w.name, s.name)
					}
				default:
					if s.has(rich) || s.has(bare) {
						t.Errorf("%s answers %s, which the table says does not apply to it: %s",
							w.name, s.name, w.why)
					}
				}
			}
		})
	}
}

// surfaceExists reports whether name is one of the optional interfaces.
func surfaceExists(name string) bool {
	for _, s := range optionalSurfaces {
		if s.name == name {
			return true
		}
	}
	return false
}

// notWrappers are the declarations the source scan below finds and that are
// not wrappers, each with the reason it is exempt. An exemption is a row
// somebody has to write, which is the point: the list is short and visible.
var notWrappers = map[string]string{
	"Unwrap": "the convention for reaching PAST a wrapper rather than a wrapper itself; " +
		"it returns the value it was built over and adds no surface",
}

// TestEveryWrapperIsInTheSurfaceTable is what keeps the table above honest as
// the package grows.
//
// A table of wrappers that a new wrapper does not have to join is a table that
// goes stale on the next increment, which is how the same omission reached
// three surfaces. So the source is read: every function and method in
// driverkit that RETURNS one of the SPI's driver interfaces is a candidate
// wrapper, and every one of them must be named in wrapperCases or exempted by
// name in notWrappers.
//
// The scan walks driverkit and every package under it — driverkit/offset and
// driverkit/nack have no SPI surface today, and a wrapper added to either has
// to join the table rather than arrive unmeasured. The root-package alias is
// read from each file's own imports rather than assumed to be "mq", because a
// check that depends on an import alias is a check that stops working when
// somebody renames one.
func TestEveryWrapperIsInTheSurfaceTable(t *testing.T) {
	named := make(map[string]bool, len(wrapperCases))
	for _, w := range wrapperCases {
		named[w.name] = true
	}
	fset := token.NewFileSet()
	found := 0
	err := filepath.WalkDir(".", func(path string, e fs.DirEntry, werr error) error {
		switch {
		case werr != nil:
			return werr
		case e.IsDir():
			if e.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		case !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go"):
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		alias := rootAlias(f)
		if alias == "" {
			return nil // a package that cannot name an SPI interface
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Type.Results == nil || !returnsDriverInterface(fn, alias) {
				continue
			}
			found++
			if _, exempt := notWrappers[fn.Name.Name]; exempt {
				continue
			}
			if qual := qualifiedName(fn); !named[qual] {
				t.Errorf("%s:%d: %s returns one of the SPI's driver interfaces and is not in "+
					"wrapperCases: a wrapper that nothing measures is how DeadlineExtender and "+
					"BatchSettler were each dropped in turn", path,
					fset.Position(fn.Pos()).Line, qual)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("the source scan found no wrapper at all, so this check cannot fail: " +
			"driverkit's mq.Settler/mq.Source returns were not recognised")
	}
}

// rootAlias returns the name f refers to the root package by, or "" when f
// does not import it. Reading the alias is what keeps the scan working when
// somebody imports the root package as something other than mq.
func rootAlias(f *ast.File) string {
	for _, imp := range f.Imports {
		if imp.Path.Value != `"bitbucket.org/iqhive/iqmq"` {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return "iqmq"
	}
	return ""
}

// wrappedInterfaces are the SPI interfaces a wrapper hands back. A function
// returning one of these is the shape that can narrow a driver's surface.
var wrappedInterfaces = map[string]bool{
	"Settler": true, "Source": true, "Sink": true, "Backend": true,
}

// returnsDriverInterface reports whether fn returns one of the SPI's driver
// interfaces, named through the file's own alias for the root package.
func returnsDriverInterface(fn *ast.FuncDecl, alias string) bool {
	for _, res := range fn.Type.Results.List {
		sel, ok := res.Type.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && pkg.Name == alias && wrappedInterfaces[sel.Sel.Name] {
			return true
		}
	}
	return false
}

// qualifiedName renders a declaration as the table spells it: "Name" for a
// function and "(*Recv).Name" for a method.
func qualifiedName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var b strings.Builder
	b.WriteByte('(')
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		b.WriteByte('*')
		if id, ok := t.X.(*ast.Ident); ok {
			b.WriteString(id.Name)
		}
	case *ast.Ident:
		b.WriteString(t.Name)
	}
	b.WriteString(").")
	b.WriteString(fn.Name.Name)
	return b.String()
}

// TestBatchSettlerSettlerKeepsTheBatchSurface is the finding, on its own,
// with the failure the driver saw.
//
// A driver whose settler has a native batch settles through
// driverkit.BatchSettler so that a fetch's worth of deliveries becomes one
// round trip. The value its Source.Settler hands the core is
// BatchSettler.Settler(), and if that value is not an mq.BatchSettler the
// core's batched settlement path cannot reach the driver's batch at all: every
// settlement goes one at a time, which is the round trip per message the
// helper exists to avoid, and nothing reports a failure. drivers/redis worked
// around it with a facade of its own.
func TestBatchSettlerSettlerKeepsTheBatchSurface(t *testing.T) {
	ctx := context.Background()
	driver := &batchingDriverSettler{}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	settler := bs.Settler()
	batch, ok := settler.(mq.BatchSettler)
	if !ok {
		t.Fatal("BatchSettler.Settler() is not an mq.BatchSettler: a driver with a native " +
			"batch cannot expose it through the kit, so the core settles one at a time")
	}
	ds := []*mq.DriverDelivery{
		{Message: &mq.Message{}, Token: 1},
		{Message: &mq.Message{}, Token: 2},
		{Message: &mq.Message{}, Token: 3},
	}
	if err := batch.AckBatch(ctx, ds); err != nil {
		t.Fatalf("AckBatch through the adapter: %v", err)
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if len(driver.batches) != 1 || len(driver.batches[0]) != 3 {
		t.Fatalf("the driver saw %v, want one batch of three: the adapter unrolled the batch",
			driver.batches)
	}
	if got := driver.ackCalls; got != 1 {
		t.Fatalf("the driver's AckBatch was called %d times for one batch of three", got)
	}
}
