package linear_test

import (
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestLayering pins the two rules the linearizability checker lives by: it is
// usable from every layer because it depends on nothing but the standard
// library, and in particular not on the root package (the specification C).
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, "./...")
	testguard.StdlibOnly(t, "./...")
}
