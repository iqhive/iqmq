package amqp10_test

import (
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// rootModuleBefore reports the version of the root module this test binary was
// built with, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the root release its
// go.mod requires, so a test of behaviour the root module gained after that
// release skips there until the release train moves the requirement on. A
// workspace build uses the root module in the tree, which the build
// information reports as (devel), and so does a replacement by a directory;
// neither is older than anything. drivers/kafka and drivers/redis have the
// same helper.
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
// such as v1.1.1 or v1.1.2-0.20261030010203-0123456789ab, and false for
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
