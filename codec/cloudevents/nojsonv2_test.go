package cloudevents_test

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestThePackageBuildsWithoutTheJSONv2Experiment is the reported defect: Go
// 1.27 builds encoding/json/jsontext only under the jsonv2 experiment, which
// is on by default, and this package imported it, so a build with
// GOEXPERIMENT=nojsonv2 failed here even once the codec package compiled
// there.
func TestThePackageBuildsWithoutTheJSONv2Experiment(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.27") {
		t.Skip("the experiment is Go 1.27's")
	}
	if testing.Short() {
		t.Skip("builds the standard library under another experiment")
	}
	// The codec package builds without the experiment from v1.1.3. Under
	// GOWORK=off this module is built against the root release its go.mod
	// requires, so the test skips there until the release train moves that
	// requirement on.
	if have, before := rootModuleBefore(t, "v1.1.3"); before {
		t.Skipf("built against root module %s, whose codec package does not build without the experiment", have)
	}
	cmd := exec.Command("go", "vet", ".")
	cmd.Env = append(os.Environ(), "GOEXPERIMENT=nojsonv2")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("GOEXPERIMENT=nojsonv2 go vet: %v\n%s", err, out)
	}
}

// rootModuleBefore reports the version of the root module this test binary was
// built with, and whether it is a release older than want. A workspace build
// uses the root module in the tree, which the build information reports as
// (devel), and so does a replacement by a directory; neither is older than
// anything. drivers/kafka has the same helper.
func rootModuleBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list the root module")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.2 or v1.1.3-0.20261110010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}
