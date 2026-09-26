package toydriver

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// wantIqmqImports is every iqmq package this module is allowed to use.
//
// It is an exact set and not a prefix rule, so that adding a dependency on
// another iqmq package is a deliberate act with a diff. What the shortness of
// the list says is the whole point of the module: a driver, its conformance
// harness and an application over it need the root package for the SPI,
// driverkit for the toolbox and testkit for the suite.
//
// dsn is here for one type, dsn.Secret, which is what a driver outside this
// repository holds its credential in -- the same type all sixteen shipped
// drivers use, and one a driver author can reach, because dsn is a public
// package of the root module.
var wantIqmqImports = []string{
	"github.com/iqhive/iqmq",
	"github.com/iqhive/iqmq/driverkit",
	"github.com/iqhive/iqmq/dsn",
	"github.com/iqhive/iqmq/examples/custom-driver/toydriver",
	"github.com/iqhive/iqmq/testkit",
	"github.com/iqhive/iqmq/wire",
}

// TestNoInternalImports is what turns "written from outside this repository"
// from a slogan into a check.
//
// Go's internal rule is path-based and not module-based, so a module whose
// path begins with github.com/iqhive/iqmq CAN import
// github.com/iqhive/iqmq/internal/... even though it has a go.mod of its own —
// verified by trying it, which compiles. This module therefore sits inside the
// blast radius of the very thing it claims to be outside of, and only an
// assertion closes the gap.
//
// It matters because the repository's own drivers do use internal packages:
// drivers/nats reaches github.com/iqhive/iqmq/internal/errcode to construct
// errors carrying the codes, which a driver at github.com/acme/mq-foo
// cannot do. A toy driver that quietly did the same would prove nothing at all
// about the published SPI, and would prove it while passing 77 conformance
// scenarios. So: no import here may contain "/internal/", and the set of iqmq
// packages used is pinned exactly.
func TestNoInternalImports(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	files, internals := 0, 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		files++
		rel, _ := filepath.Rel(root, path)
		for _, spec := range f.Imports {
			p, uerr := strconv.Unquote(spec.Path.Value)
			if uerr != nil {
				t.Errorf("%s: unquoting %s: %v", rel, spec.Path.Value, uerr)
				continue
			}
			if strings.Contains(p, "/internal/") || strings.HasSuffix(p, "/internal") {
				internals++
				t.Errorf("%s imports %s: a driver outside this repository cannot reach an internal package, so anything this module needs from one is a gap in the published API", rel, p)
			}
			if p == "github.com/iqhive/iqmq" || strings.HasPrefix(p, "github.com/iqhive/iqmq/") {
				seen[p] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if files == 0 {
		t.Fatalf("no Go files found under %s: the check would pass over nothing", root)
	}
	got := make([]string, 0, len(seen))
	for p := range seen {
		got = append(got, p)
	}
	slices.Sort(got)
	if !slices.Equal(got, wantIqmqImports) {
		t.Errorf("this module imports the iqmq packages\n  %v\nand the pinned set is\n  %v\nif the change is intended, update wantIqmqImports and say in the commit what the new dependency buys",
			got, wantIqmqImports)
	}
	t.Logf("%d Go file(s), %d iqmq package(s), %d internal import(s)", files, len(got), internals)
}
