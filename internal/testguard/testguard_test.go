package testguard

import "testing"

func TestGuardsOnSelf(t *testing.T) {
	NoRootImport(t, "./...")
	StdlibOnly(t, "./...")
	AssertNoDeps(t, ".", "example.com/never")
}

func TestStdlibOnlyRootModule(t *testing.T) {
	StdlibOnly(t, Module+"/...")
}
