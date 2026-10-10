package iqmq

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestDocsNameIdentifiersThatExist fails when a page under docs/ names an
// exported identifier of this package, of iqmq/middleware or of iqmq/codec
// that does not exist.
//
// It exists because the three migration guides shipped with SEVEN invented
// identifiers in their first draft -- mq.HeadersFromMap, mq.OrderingHint,
// mq.FeatureDelay, mq.FeatureOrdering, mq.RequireExplicitAck,
// mq.RequireOrdering and mq.ErrCorruptMessage -- every one of them a plausible
// name for a thing that is really called something else, and every one of them
// in a code block a reader would copy. A migration guide is the page most
// likely to be read by somebody who cannot yet tell; prose that does not
// compile is worse there than anywhere.
//
// It is a test rather than a make target so that `make test-race` runs it and
// `make ci-parity` needs no waiver. It parses the packages with go/ast so that
// the root module stays standard-library-only.
//
// What it deliberately does NOT check: an identifier of a driver, an
// integration or an `x/` module (those are other modules and this test cannot
// import them), a method on a type (`Headers.Set` is spelled without a
// package qualifier), and a name inside a fenced block in some other
// language. Widening it is welcome; what matters is that the common case --
// `mq.SomethingPlausible` in a Go snippet -- can no longer be invented.
func TestDocsNameIdentifiersThatExist(t *testing.T) {
	known := map[string]map[string]bool{
		"mq":         exportedOf(t, "."),
		"iqmq":       exportedOf(t, "."),
		"middleware": exportedOf(t, "middleware"),
		"codec":      exportedOf(t, "codec"),
	}

	// A qualified reference in prose or in a Go block: one of the package
	// aliases above, a dot, an exported name.
	ref := regexp.MustCompile(`\b(mq|iqmq|middleware|codec)\.([A-Z][A-Za-z0-9_]*)`)

	// notAnIdentifier holds the references that look qualified and are not.
	// One entry, one reason; a check with an unexplained allowlist is a check
	// with a hole in it.
	notAnIdentifier := map[string]string{
		// A broker DESTINATION name on the ActiveMQ pages -- the `iqmq.`
		// there is a topic prefix, not a Go package qualifier
		// (`iqmq.VirtualTopic.orders` is a queue).
		"iqmq.VirtualTopic": "a broker destination name, not a Go identifier",
	}

	// deliberatelyAbsent holds identifiers a page names in order to record
	// that the API does NOT have them. It is a different claim from
	// notAnIdentifier above -- these ARE Go identifier forms, and the page
	// mentioning one is saying so -- and it is held to the same three rules:
	// the name must not exist, some page must still mention it, and the two
	// lists may not overlap. Without it, a document that records a declined
	// API cannot be written at all: naming the thing you did not add is the
	// whole content of such a section.
	deliberatelyAbsent := map[string]string{}

	type miss struct{ file, name string }
	var misses []miss
	files := 0

	err := filepath.WalkDir("docs", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		files++
		for _, m := range ref.FindAllStringSubmatch(string(b), -1) {
			pkg, name := m[1], m[2]
			if _, ok := notAnIdentifier[pkg+"."+name]; ok {
				continue
			}
			if _, ok := deliberatelyAbsent[pkg+"."+name]; ok {
				continue
			}
			if !known[pkg][name] {
				misses = append(misses, miss{file: path, name: pkg + "." + name})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking docs/: %v", err)
	}
	if files == 0 {
		t.Fatal("no markdown file under docs/ was read, so this check saw nothing")
	}
	// Every allowlist entry must still appear somewhere, or it is a hole kept
	// open for nothing.
	for ref := range notAnIdentifier {
		if !allowlistUsed(t, ref) {
			t.Fatalf("the allowlist keeps %q open and no page mentions it any more; delete the entry", ref)
		}
	}
	for ref, why := range deliberatelyAbsent {
		if why == "" {
			t.Fatalf("the deliberately-absent entry %q carries no reason", ref)
		}
		if !allowlistUsed(t, ref) {
			t.Fatalf("no page records the absence of %q any more; delete the entry", ref)
		}
		if _, both := notAnIdentifier[ref]; both {
			t.Fatalf("%q is on both allowlists; an identifier is either not one or absent, not both", ref)
		}
	}
	// The check has to be able to fail, and the cheapest proof is that it
	// finds the identifiers it is meant to find when they are there.
	for _, invented := range []string{"HeadersFromMap", "OrderingHint", "FeatureDelay"} {
		if known["mq"][invented] {
			t.Fatalf("mq.%s exists now, so it is no longer an example of an invented name; "+
				"pick another for this self-check", invented)
		}
		if _, allowed := notAnIdentifier["mq."+invented]; allowed {
			t.Fatalf("mq.%s is on the allowlist, so the self-check above could not see it; "+
				"pick a name the allowlist does not cover", invented)
		}
		if _, allowed := deliberatelyAbsent["mq."+invented]; allowed {
			t.Fatalf("mq.%s is recorded as deliberately absent, so the self-check above could "+
				"not see it; pick a name neither list covers", invented)
		}
	}
	// And the allowlist may not cover a name that DOES exist: an entry for a
	// real identifier is an exemption that hides nothing and would silently
	// stop covering it if the identifier were later removed.
	for ref := range notAnIdentifier {
		pkg, name, ok := strings.Cut(ref, ".")
		if !ok {
			t.Fatalf("allowlist entry %q is not package-qualified", ref)
		}
		if known[pkg][name] {
			t.Fatalf("the allowlist exempts %q, which exists; delete the entry", ref)
		}
	}
	// And the same in the other direction: a name recorded as deliberately
	// absent that has since been ADDED is a page saying the API does not have
	// something it has.
	for ref := range deliberatelyAbsent {
		pkg, name, ok := strings.Cut(ref, ".")
		if !ok {
			t.Fatalf("deliberately-absent entry %q is not package-qualified", ref)
		}
		if known[pkg][name] {
			t.Fatalf("%q is recorded as deliberately absent and it exists; the page that says "+
				"the surface does not have it is now wrong", ref)
		}
	}
	if len(misses) > 0 {
		seen := map[string]bool{}
		var lines []string
		for _, m := range misses {
			line := m.file + ": " + m.name
			if seen[line] {
				continue
			}
			seen[line] = true
			lines = append(lines, line)
		}
		sort.Strings(lines)
		t.Fatalf("%d documentation page(s) name an identifier that does not exist:\n  %s",
			len(lines), strings.Join(lines, "\n  "))
	}
	t.Logf("%d markdown file(s) under docs/ checked against %d root, %d middleware and %d codec identifier(s)",
		files, len(known["mq"]), len(known["middleware"]), len(known["codec"]))
}

// allowlistUsed reports whether any page still mentions a reference the
// allowlist excuses.
func allowlistUsed(t *testing.T, ref string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir("docs", func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".md") || found {
			return err
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(b), ref) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking docs/: %v", err)
	}
	return found
}

// exportedOf returns every exported identifier a package declares at file
// scope, including struct field names, so that `mq.Message.Headers` and
// `mq.PublisherConfig.Confirmation` resolve on their first segment.
func exportedOf(t *testing.T, dir string) map[string]bool {
	t.Helper()
	// The files are parsed one at a time rather than through
	// parser.ParseDir, which is deprecated because it ignores build tags. One
	// build-tagged file matters here: codec's encoding/json/v2 kind is behind
	// //go:build go1.27, and its identifiers are the same under either tag, so
	// reading every file regardless of tag is what this check wants -- it asks
	// "is this name declared anywhere", not "is it in this build".
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", filepath.Join(dir, name), perr)
		}
		{
			for _, decl := range f.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Recv == nil && d.Name.IsExported() {
						out[d.Name.Name] = true
					}
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								out[s.Name.Name] = true
							}
						case *ast.ValueSpec:
							for _, n := range s.Names {
								if n.IsExported() {
									out[n.Name] = true
								}
							}
						}
					}
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s declares no exported identifier, so the check would accept anything", dir)
	}
	return out
}
