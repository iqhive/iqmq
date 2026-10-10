package nativebench_test

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

const (
	moduleDir = ".."
	repoRoot  = "../../.."
)

// TestClientsList logs the registry the way the variant families log their
// kinds, so a bench-native run records exactly which client versions
// produced its numbers.
func TestClientsList(t *testing.T) {
	list := nativebench.List()
	if list == "" {
		t.Fatal("empty client list")
	}
	t.Logf("clients: %s", list)
	seen := map[string]bool{}
	for _, c := range nativebench.Clients() {
		switch {
		case c.Slug == "" || c.Module == "" || c.Version == "":
			t.Errorf("incomplete registry entry %+v", c)
		case seen[c.Slug]:
			t.Errorf("duplicate slug %q", c.Slug)
		case c.Confirmation == "":
			t.Errorf("%s: no confirmation boundary recorded", c.Slug)
		}
		seen[c.Slug] = true
		if got, ok := nativebench.ClientBySlug(c.Slug); !ok || got != c {
			t.Errorf("ClientBySlug(%q) = %+v, %v", c.Slug, got, ok)
		}
	}
	if _, ok := nativebench.ClientBySlug("nope"); ok {
		t.Error("ClientBySlug found an unregistered slug")
	}
}

// TestPinnedVersions keeps the registry and go.mod in step: a baseline number
// is only meaningful next to the client version that produced it, and the
// driver pages quote the registry.
func TestPinnedVersions(t *testing.T) {
	mod, err := os.ReadFile(filepath.Join(moduleDir, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	require := map[string]string{}
	for line := range strings.SplitSeq(string(mod), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) >= 2 && strings.HasPrefix(fields[1], "v") && strings.Contains(fields[0], "/") {
			require[fields[0]] = fields[1]
		}
	}
	for _, c := range nativebench.Clients() {
		got, ok := require[c.Module]
		if !ok {
			t.Errorf("%s: %s is not required by go.mod", c.Slug, c.Module)
			continue
		}
		if got != c.Version {
			t.Errorf("%s: registry pins %s, go.mod has %s", c.Slug, c.Version, got)
		}
	}
}

// TestBrokerImagesPinned checks every baseline names a broker the harness can
// start from a pinned image.
func TestBrokerImagesPinned(t *testing.T) {
	for _, c := range nativebench.Clients() {
		if harness.Images[c.Broker] == "" {
			t.Errorf("%s: harness has no image for broker %q", c.Slug, c.Broker)
		}
	}
}

// TestClientPackages checks the registry against the directories: every
// baseline has a package, every package is registered, and each one calls
// nativebench.Run with its own slug so the benchmark names cannot drift.
func TestClientPackages(t *testing.T) {
	entries, err := os.ReadDir(moduleDir)
	if err != nil {
		t.Fatalf("read module dir: %v", err)
	}
	dirs := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() && e.Name() != "nativebench" && !strings.HasPrefix(e.Name(), ".") {
			dirs[e.Name()] = true
		}
	}
	for _, c := range nativebench.Clients() {
		if !dirs[c.Slug] {
			t.Errorf("%s: no package directory", c.Slug)
			continue
		}
		delete(dirs, c.Slug)
		src, err := os.ReadFile(filepath.Join(moduleDir, c.Slug, c.Slug+"_test.go"))
		if err != nil {
			t.Errorf("%s: %v", c.Slug, err)
			continue
		}
		if !strings.Contains(string(src), "nativebench.Run(b, \""+c.Slug+"\"") {
			t.Errorf("%s: does not call nativebench.Run with its own slug", c.Slug)
		}
		if !strings.Contains(string(src), "func BenchmarkNative(b *testing.B)") {
			t.Errorf("%s: no BenchmarkNative entry point", c.Slug)
		}
	}
	for d := range dirs {
		t.Errorf("directory %q is not a registered baseline", d)
	}
}

// TestBenchmarkNames pins the names the module produces.
func TestBenchmarkNames(t *testing.T) {
	if got, want := strings.Join(nativebench.Ops(), ","), "Publish,PublishBatch,Receive,EndToEnd"; got != want {
		t.Errorf("ops = %q, want %q", got, want)
	}
	sizes := make([]string, 0, len(nativebench.Sizes()))
	for _, s := range nativebench.Sizes() {
		sizes = append(sizes, s.Name)
	}
	if got, want := strings.Join(sizes, ","), "64B,1KiB,64KiB"; got != want {
		t.Errorf("sizes = %q, want %q", got, want)
	}
	if got, want := nativebench.Sizes(), []int{64, 1024, 65536}; len(got) != len(want) {
		t.Fatalf("sizes: %d", len(got))
	} else {
		for i, s := range got {
			if s.Bytes != want[i] {
				t.Errorf("size %s = %d bytes, want %d", s.Name, s.Bytes, want[i])
			}
		}
	}
	names := nativebench.Names("franzgo")
	if len(names) != 12 {
		t.Fatalf("names: %d, want 12", len(names))
	}
	if names[0] != "BenchmarkNative/franzgo/Publish/64B" {
		t.Errorf("first name = %q", names[0])
	}
	if names[11] != "BenchmarkNative/franzgo/EndToEnd/64KiB" {
		t.Errorf("last name = %q", names[11])
	}
}

// TestParamsBatched documents which leaves publish in client-level batches.
func TestParamsBatched(t *testing.T) {
	for op, want := range map[string]bool{
		"Publish": false, "PublishBatch": true, "Receive": true, "EndToEnd": false,
	} {
		if got := (nativebench.Params{Op: op}).Batched(); got != want {
			t.Errorf("Params{%q}.Batched() = %v, want %v", op, got, want)
		}
	}
}

// TestPayloads checks the payload generator: deterministic, exact length,
// distinct per batch entry.
func TestPayloads(t *testing.T) {
	for _, s := range nativebench.Sizes() {
		p := nativebench.Payload(s.Bytes)
		if len(p) != s.Bytes {
			t.Errorf("%s: len = %d", s.Name, len(p))
		}
		if !bytes.Equal(p, nativebench.Payload(s.Bytes)) {
			t.Errorf("%s: payload is not deterministic", s.Name)
		}
	}
	batch := nativebench.Payloads(nativebench.BatchSize, 64)
	if len(batch) != nativebench.BatchSize {
		t.Fatalf("batch len = %d", len(batch))
	}
	seen := map[string]bool{}
	for _, p := range batch {
		if len(p) != 64 {
			t.Fatalf("batch entry len = %d", len(p))
		}
		if seen[string(p)] {
			t.Error("batch entries are not distinct")
		}
		seen[string(p)] = true
	}
}

// TestNames checks that generated resource names are unique and broker-safe.
func TestNames(t *testing.T) {
	ok := regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	seen := map[string]bool{}
	for range 100 {
		n := nativebench.Name("iqmqbench")
		if !ok.MatchString(n) {
			t.Fatalf("name %q is not broker-safe", n)
		}
		if seen[n] {
			t.Fatalf("duplicate name %q", n)
		}
		seen[n] = true
	}
}

// TestNoRootImport is the module's reason to exist: a baseline that imported
// iqmq would measure iqmq. Two packages from the iqmq tree are allowed, and
// each one is itself walked to prove it imports nothing from the root module:
// conformance/harness, which starts the containers, and benchmarks/opbench,
// which is the timing loop this module shares with benchmarks/overhead so that
// the two arms of the overhead budget are one experiment rather than two.
func TestNoRootImport(t *testing.T) {
	allowed := map[string]bool{
		"github.com/iqhive/iqmq/conformance/harness": true,
		"github.com/iqhive/iqmq/benchmarks/opbench":  true,
	}
	check := func(dir string, allowHelpers bool) {
		t.Helper()
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			f, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if perr != nil {
				t.Errorf("parse %s: %v", path, perr)
				return nil
			}
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, `"`)
				if !strings.HasPrefix(p, "github.com/iqhive/iqmq") {
					continue
				}
				switch {
				case strings.HasPrefix(p, "github.com/iqhive/iqmq/benchmarks/native-baselines"):
				case allowHelpers && allowed[p]:
				default:
					t.Errorf("%s imports %s", path, p)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	check(moduleDir, true)
	check(filepath.Join(repoRoot, "conformance", "harness"), false)
	check(filepath.Join(repoRoot, "benchmarks", "opbench"), false)
}

// TestFactsRecorded checks that every fact verified here is
// recorded verbatim on its driver page, and that each page names the client
// version the baseline pins. Without this the pages drift from what was
// actually checked.
func TestFactsRecorded(t *testing.T) {
	pages := map[string]string{}
	read := func(driver string) string {
		if s, ok := pages[driver]; ok {
			return s
		}
		b, err := os.ReadFile(filepath.Join(repoRoot, "docs", "drivers", driver+".md"))
		if err != nil {
			t.Fatalf("read driver page: %v", err)
		}
		pages[driver] = string(b)
		return pages[driver]
	}
	for _, f := range nativebench.Facts() {
		if f.Subject == "" || f.Claim == "" || f.Source == "" {
			t.Errorf("incomplete fact %+v", f)
			continue
		}
		if !strings.Contains(read(f.Driver), f.Claim) {
			t.Errorf("docs/drivers/%s.md does not record the %q fact verbatim", f.Driver, f.Subject)
		}
		if !strings.Contains(read(f.Driver), f.Source) {
			t.Errorf("docs/drivers/%s.md does not cite the source for %q", f.Driver, f.Subject)
		}
	}
	for _, c := range nativebench.Clients() {
		page := read(c.Driver)
		if !strings.Contains(page, c.Pin()) {
			t.Errorf("docs/drivers/%s.md does not pin %s", c.Driver, c.Pin())
		}
		if !strings.Contains(page, c.Confirmation) {
			t.Errorf("docs/drivers/%s.md does not record %s's confirmation boundary", c.Driver, c.Slug)
		}
	}
	if len(nativebench.FactsFor("kafka")) == 0 {
		t.Error("no facts recorded for kafka")
	}
	if len(nativebench.FactsFor("nothing")) != 0 {
		t.Error("facts for an unknown driver")
	}
}
