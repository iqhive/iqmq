package iqmq_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The module graph's own compatibility rules (the release checks), checked
// from the tree rather than assumed.
//
// The specification promises a release train that "tags all official modules together",
// a v1.0 stability guarantee for the root package and the SPI, and Go version
// bumps "only on minors, announced". Forty-eight modules is too many to hold
// to that by reading, and every one of these rules has gone wrong in some
// repository: a module path that no longer matches its directory, one module
// left on an older Go directive, a `toolchain` line that forces a consumer's
// toolchain download, and a sibling requirement with no `replace`, which
// resolves from the proxy and silently builds against a PUBLISHED version of
// the code in the next directory.
//
// The version audit is the one that found something : every
// one of the forty-seven non-root modules requires the root at the zero
// pseudo-version `v0.0.0-00010101000000-000000000000`, which is what `go mod
// tidy` writes when a `replace` makes the version irrelevant. A `replace` in a
// DEPENDENCY's go.mod is ignored by whoever depends on it, so a third party
// who runs `go get github.com/iqhive/iqmq/drivers/kafka` gets a module whose
// only requirement on the core is a pseudo-version that does not exist. The
// release train has to rewrite all forty-seven before it tags, and this test
// is the reminder: it asserts the CURRENT shape so that the day the shape
// changes is a day somebody looks at it.

// gomod is one module's parsed go.mod, to the depth these checks need.
type gomod struct {
	dir      string
	path     string
	goVer    string
	tool     string
	requires map[string]string // iqhive/iqmq* module path -> version
	replaces map[string]string // iqhive/iqmq* module path -> relative directory
}

func readModules(t *testing.T) []gomod {
	t.Helper()
	var out []gomod
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			// .claude holds agents' worktrees, each a whole second checkout
			// whose go.mod files are not modules of this tree.
			case ".git", ".claude", "testdata", "node_modules", "states":
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		out = append(out, parseGomod(t, path, filepath.ToSlash(filepath.Dir(path))))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 40 {
		t.Fatalf("found %d go.mod files, which is too few to be this repository: the walk is wrong", len(out))
	}
	return out
}

// parseGomod reads one go.mod to the depth these checks need.
func parseGomod(t *testing.T, path, dir string) gomod {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := gomod{dir: dir, requires: map[string]string{}, replaces: map[string]string{}}
	{
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(strings.TrimSpace(strings.SplitN(line, "//", 2)[0]))
			switch {
			case len(f) == 2 && f[0] == "module":
				m.path = f[1]
			case len(f) == 2 && f[0] == "go":
				m.goVer = f[1]
			case len(f) == 2 && f[0] == "toolchain":
				m.tool = f[1]
			// Both spellings: "replace X => Y" and, inside a replace block,
			// "X => Y". The module path is two fields before the target in
			// each, which the first version of this parser got wrong by one
			// -- it read the word "replace" as the module path and reported
			// every replace in the repository as missing.
			case len(f) >= 3 && f[len(f)-2] == "=>" && strings.HasPrefix(f[len(f)-3], "github.com/iqhive/iqmq"):
				m.replaces[f[len(f)-3]] = f[len(f)-1]
			case len(f) == 2 && strings.HasPrefix(f[0], "github.com/iqhive/iqmq"):
				m.requires[f[0]] = f[1]
			case len(f) == 3 && f[0] == "require" && strings.HasPrefix(f[1], "github.com/iqhive/iqmq"):
				m.requires[f[1]] = f[2]
			}
		}
	}
	return m
}

// TestEveryModulePathMatchesItsDirectory holds the release train to the
// only invariant that makes "tag all official modules together" mean anything:
// a module's import path is its directory under the root's.
func TestEveryModulePathMatchesItsDirectory(t *testing.T) {
	const root = "github.com/iqhive/iqmq"
	for _, m := range readModules(t) {
		want := root
		if m.dir != "." {
			want = root + "/" + m.dir
		}
		if m.path != want {
			t.Errorf("%s/go.mod declares module %q, want %q", m.dir, m.path, want)
		}
	}
}

// TestEveryModuleDeclaresOneGoFloor is the specification's "Go version bumps only on
// minors, announced": one announced floor for the whole family, not
// forty-eight independent ones. A module left behind would compile here — the
// go.work covers it — and refuse to build for a consumer on the older Go the
// rest of the family still claims to support.
func TestEveryModuleDeclaresOneGoFloor(t *testing.T) {
	mods := readModules(t)
	var rootVer string
	for _, m := range mods {
		if m.dir == "." {
			rootVer = m.goVer
		}
	}
	if rootVer == "" {
		t.Fatal("the root go.mod has no go directive")
	}
	for _, m := range mods {
		if m.goVer != rootVer {
			t.Errorf("%s/go.mod declares go %s; the root declares go %s", m.dir, m.goVer, rootVer)
		}
		// A toolchain line in a PUBLISHED module makes the consumer's go
		// command download that toolchain, which is not a decision this
		// repository gets to make for somebody else's build.
		if m.tool != "" {
			t.Errorf("%s/go.mod declares toolchain %s; a published module must not pin a consumer's toolchain", m.dir, m.tool)
		}
	}
}

// releaseContract reads the same module train used by release preparation.
func releaseContract(t *testing.T) (string, map[string]bool) {
	t.Helper()
	raw, err := os.ReadFile("release/manifest.yaml")
	if err != nil {
		t.Fatal(err)
	}
	train := ""
	modules := map[string]bool{}
	path := ""
	inModules := false
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "train:") {
			train = strings.TrimSpace(strings.TrimPrefix(line, "train:"))
		}
		if line == "modules:" {
			inModules = true
			continue
		}
		if inModules && len(line) > 0 && line[0] != ' ' && line[0] != '#' {
			inModules = false
		}
		if !inModules {
			continue
		}
		s := strings.TrimSpace(line)
		if strings.HasPrefix(s, "- path:") {
			path = strings.TrimSpace(strings.TrimPrefix(s, "- path:"))
		}
		if s == "release: train" {
			modules[path] = true
		}
	}
	if train == "" || len(modules) < 20 {
		t.Fatal("incomplete release contract")
	}
	return train, modules
}

// Published modules resolve real train versions. Development-only modules keep local replacements.
func TestSiblingResolutionMatchesReleaseContract(t *testing.T) {
	train, published := releaseContract(t)
	for _, m := range readModules(t) {
		if train != "v0.0.0" && published[m.dir] {
			if len(m.replaces) != 0 {
				t.Errorf("%s retains development replacements in a release module", m.dir)
			}
			continue
		}
		for dep := range m.requires {
			rel, ok := m.replaces[dep]
			if !ok {
				t.Errorf("%s has no development replacement for %s", m.dir, dep)
				continue
			}
			if _, err := os.Stat(filepath.Join(m.dir, rel, "go.mod")); err != nil {
				t.Error(err)
			}
		}
	}
}

// Every published sibling requirement names this train; internal modules have no public tag.
func TestSiblingVersionsMatchReleaseContract(t *testing.T) {
	train, published := releaseContract(t)
	const zero = "v0.0.0-00010101000000-000000000000"
	n := 0
	for _, m := range readModules(t) {
		for dep, version := range m.requires {
			dir := strings.TrimPrefix(dep, "github.com/iqhive/iqmq/")
			if dep == "github.com/iqhive/iqmq" {
				dir = "."
			}
			want := zero
			if train != "v0.0.0" && published[dir] {
				want = train
			}
			if published[m.dir] && !published[dir] {
				t.Errorf("published %s depends on internal module %s", m.dir, dep)
			}
			if version != want {
				t.Errorf("%s requires %s at %s, want %s", m.dir, dep, version, want)
			}
			n++
		}
	}
	if n < 40 {
		t.Fatalf("checked only %d sibling requirements", n)
	}
}

// TestReadModulesParsesBothReplaceForms is the parser's own control. The first
// version of readModules read "replace" as the module path in the
// single-line form and reported all 100-odd intra-repository replaces as
// missing, which is a check reporting a catastrophe that was not there --
// worth as much suspicion as one reporting success.
func TestReadModulesParsesBothReplaceForms(t *testing.T) {
	dir := t.TempDir()
	const body = `module github.com/iqhive/iqmq/probe

go 1.26.0

replace github.com/iqhive/iqmq => ../..

replace (
	github.com/iqhive/iqmq/drivers/nats => ../drivers/nats
)

require (
	github.com/iqhive/iqmq v0.0.0-00010101000000-000000000000
	github.com/iqhive/iqmq/drivers/nats v0.0.0-00010101000000-000000000000
	example.com/other v1.2.3
)

require github.com/iqhive/iqmq/codec/avro v0.0.0-00010101000000-000000000000
`
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	m := parseGomod(t, filepath.Join(dir, "go.mod"), "probe")
	for _, want := range []string{"github.com/iqhive/iqmq", "github.com/iqhive/iqmq/drivers/nats"} {
		if got := m.replaces[want]; got == "" {
			t.Errorf("replace for %s not parsed; have %v", want, m.replaces)
		}
	}
	for _, want := range []string{
		"github.com/iqhive/iqmq",
		"github.com/iqhive/iqmq/drivers/nats",
		"github.com/iqhive/iqmq/codec/avro",
	} {
		if got := m.requires[want]; got == "" {
			t.Errorf("require for %s not parsed; have %v", want, m.requires)
		}
	}
	if _, ok := m.requires["example.com/other"]; ok {
		t.Error("a foreign requirement was collected; only intra-repository ones are this audit's business")
	}
	if m.goVer != "1.26.0" || m.tool != "" {
		t.Errorf("go=%q toolchain=%q", m.goVer, m.tool)
	}
}
