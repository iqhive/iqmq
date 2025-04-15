// Package testguard enforces import-graph rules from tests.
//
// The Phase-0 bake-off requires that internal/engine and codec never depend
// on the root package or on the memory driver; the root module requires that
// it depends on the standard library only. Both rules are
// checked by running go list from a test, so a violation fails the ordinary
// test run rather than a separate lint step.
package testguard

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Module is the import path of the root module.
const Module = "bitbucket.org/iqhive/iqmq"

// Deps returns the transitive, non-test dependencies of the packages matching
// pattern as reported by go list.
func Deps(t testing.TB, pattern string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", pattern)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go list -deps %s: %v\n%s", pattern, err, stderr)
	}
	var deps []string
	for _, line := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
		if len(line) > 0 {
			deps = append(deps, string(line))
		}
	}
	return deps
}

// AssertNoDeps fails the test if any dependency of pattern equals one of the
// forbidden import paths or has one of them as a path prefix.
func AssertNoDeps(t testing.TB, pattern string, forbidden ...string) {
	t.Helper()
	for _, dep := range Deps(t, pattern) {
		for _, f := range forbidden {
			if dep == f || strings.HasPrefix(dep, f+"/") {
				t.Errorf("%s depends on forbidden package %s", pattern, dep)
			}
		}
	}
}

// NoRootImport fails the test if pattern depends on the root package or on
// the memory driver. It is the guard for the Phase-0 layering rule.
func NoRootImport(t testing.TB, pattern string) {
	t.Helper()
	for _, dep := range Deps(t, pattern) {
		if dep == Module || strings.HasPrefix(dep, Module+"/memory") {
			t.Errorf("%s depends on %s, which the Phase-0 layering forbids", pattern, dep)
		}
	}
}

// StdlibOnly fails the test if pattern depends on any package outside the
// standard library and the root module.
func StdlibOnly(t testing.TB, pattern string) {
	t.Helper()
	for _, dep := range Deps(t, pattern) {
		if dep == Module || strings.HasPrefix(dep, Module+"/") {
			continue
		}
		first, _, _ := strings.Cut(dep, "/")
		if strings.Contains(first, ".") {
			t.Errorf("%s depends on non-stdlib package %s", pattern, dep)
		}
	}
}
