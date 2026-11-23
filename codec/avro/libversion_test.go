package avro_test

import (
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// avroLibraryBefore reports the version of github.com/iqhive/avro/v2 this test
// binary was built with, and whether it is a release older than want.
//
// A test of behaviour the library gained after the release go.mod requires
// skips until the requirement moves on. A replacement by a directory, as when
// the library is tested from a clone, reports no version and is older than
// nothing. drivers/nsq's rootModuleBefore is the same helper for the root
// module.
func avroLibraryBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/avro/v2" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list github.com/iqhive/avro/v2")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v2.36.0 or v2.36.1-0.20261123010203-0123456789ab, and false for
// anything else, the empty version of a directory among them.
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
