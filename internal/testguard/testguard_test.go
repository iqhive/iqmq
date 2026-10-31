package testguard

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"
)

func TestGuardsOnSelf(t *testing.T) {
	NoRootImport(t, "./...")
	StdlibOnly(t, "./...")
	AssertNoDeps(t, ".", "example.com/never")
}

func TestStdlibOnlyRootModule(t *testing.T) {
	StdlibOnly(t, Module+"/...")
}

// TestRootModuleRequiresNoModule reads the root go.mod's requirements, which
// are the module graph every consumer of the root module inherits. The
// package graph StdlibOnly reads is the default build's only, so a
// third-party import behind one of the repository's own build tags
// (iqmq_debug, which `make test-tags` builds) passed it, while `go mod tidy`
// had put the requirement in go.mod.
func TestRootModuleRequiresNoModule(t *testing.T) {
	cmd := exec.Command("go", "mod", "edit", "-json", "../../go.mod")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go mod edit -json: %v", err)
	}
	var mod struct {
		Module  struct{ Path string }
		Require []struct{ Path, Version string }
	}
	if err := json.Unmarshal(out, &mod); err != nil {
		t.Fatal(err)
	}
	if mod.Module.Path != Module {
		t.Fatalf("../../go.mod is %s, not the root module %s", mod.Module.Path, Module)
	}
	for _, r := range mod.Require {
		t.Errorf("the root module requires %s %s; it depends on the standard library only", r.Path, r.Version)
	}
}
