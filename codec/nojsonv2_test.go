package codec

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestTheCodecBuildsWithoutTheJSONv2Experiment is the reported defect:
// Go 1.27 builds encoding/json/v2 only under the jsonv2 experiment, which is
// on by default, and jsonv2.go was selected by its go1.27 constraint alone,
// so with GOEXPERIMENT=nojsonv2 this package, typed and everything importing
// them failed to compile, and jsonv2_unavailable.go's promise (JSONv2 reported
// unavailable) was never reached.
func TestTheCodecBuildsWithoutTheJSONv2Experiment(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.27") {
		t.Skip("the experiment is Go 1.27's")
	}
	if testing.Short() {
		t.Skip("builds the standard library under another experiment")
	}
	env := append(os.Environ(), "GOEXPERIMENT=nojsonv2")
	cmd := exec.Command("go", "vet", ".", "../typed")
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("GOEXPERIMENT=nojsonv2 go vet: %v\n%s", err, out)
	}
	// And the kind reports what that build holds.
	cmd = exec.Command("go", "test", "-count=1", "-v", "-run", "^TestKindProperties$", ".")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("GOEXPERIMENT=nojsonv2 go test: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "jsonv2 available: false") {
		t.Fatalf("GOEXPERIMENT=nojsonv2: JSONv2 is not reported unavailable:\n%s", out)
	}
}
