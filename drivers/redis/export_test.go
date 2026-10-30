package redis

import (
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
//
// Nack is resolved here and not in `resolve`, because this driver resolves it
// at each point of use -- the capability table, the source and the rendering
// all call `Nack.Resolve()` -- so a Config holding Auto and one holding the
// kind Auto resolves to are the same configuration. Comparing them without
// this made every row of the render test fail for a reason that was the
// instrument's rather than the renderer's.
func ResolvedForTest(c Config) Config {
	c = c.resolve()
	c.Nack = c.Nack.Resolve()
	return c
}

// RootModuleBeforeForTest reports the version of the root module this test
// binary was built with, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the root release its
// go.mod requires, so a test of behaviour the root module gained after that
// release skips there until the release train moves the requirement on. It
// reads the version rather than probing the behaviour, because a probe skips
// in exactly the build that lacks the behaviour, which is the build the test
// exists to fail. A workspace build uses the root module in the tree, which
// the build information reports as (devel), and so does a replacement by a
// directory; neither is older than anything, so the test runs there.
func RootModuleBeforeForTest(t testing.TB, want string) (string, bool) {
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
