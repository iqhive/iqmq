package testkit

import (
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
)

// recorder stands in for *testing.T so the branches of checkClientPin can each
// be driven. testing.TB cannot be implemented outside the testing package,
// which is why checkClientPin takes the narrower pinReporter.
type recorder struct {
	errs  []string
	fatal []string
	skips []string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(f string, a ...any) {
	r.errs = append(r.errs, sprintf(f, a...))
}

func (r *recorder) Fatalf(f string, a ...any) {
	r.fatal = append(r.fatal, sprintf(f, a...))
}

func (r *recorder) Skipf(f string, a ...any) {
	r.skips = append(r.skips, sprintf(f, a...))
}

func sprintf(f string, a ...any) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, f, a...)
	return b.String()
}

func info(deps ...*debug.Module) func() (*debug.BuildInfo, bool) {
	return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Deps: deps}, true }
}

// TestCheckClientPinAgreementPasses is the control: without it every case
// below is satisfied by a function that always complains.
func TestCheckClientPinAgreementPasses(t *testing.T) {
	var r recorder
	checkClientPin(&r, ClientInfo{Module: "example.com/c", Version: "v1.2.3"},
		info(&debug.Module{Path: "example.com/c", Version: "v1.2.3"}))
	if len(r.errs)+len(r.fatal)+len(r.skips) != 0 {
		t.Errorf("an agreeing pin was reported: errs=%v fatal=%v skips=%v", r.errs, r.fatal, r.skips)
	}
}

// TestCheckClientPinCatchesDrift is the whole point, and it is the shape that
// was live on main: drivers/nats and drivers/jetstream advertised nats.go
// v1.53.1 while their binaries linked v1.54.0.
func TestCheckClientPinCatchesDrift(t *testing.T) {
	var r recorder
	checkClientPin(&r, ClientInfo{Module: "example.com/c", Version: "v1.53.1"},
		info(&debug.Module{Path: "example.com/c", Version: "v1.54.0"}))
	if len(r.errs) != 1 {
		t.Fatalf("drift was not reported once: errs=%v", r.errs)
	}
	for _, want := range []string{"v1.53.1", "v1.54.0", "example.com/c"} {
		if !strings.Contains(r.errs[0], want) {
			t.Errorf("the report does not name %q, so a reader cannot act on it: %s", want, r.errs[0])
		}
	}
}

// TestCheckClientPinReportsAnAbsentModule: a driver wraps its client, so the
// client is linked. An absence means the pair names something this binary does
// not use, which is its own defect and must not read as a pass.
func TestCheckClientPinReportsAnAbsentModule(t *testing.T) {
	var r recorder
	checkClientPin(&r, ClientInfo{Module: "example.com/missing", Version: "v1.0.0"},
		info(&debug.Module{Path: "example.com/other", Version: "v1.0.0"}))
	if len(r.errs) != 1 {
		t.Fatalf("an absent module was not reported: errs=%v", r.errs)
	}
	if !strings.Contains(r.errs[0], "not among") {
		t.Errorf("the report does not say the module was not linked: %s", r.errs[0])
	}
}

// TestCheckClientPinSkipsAReplacedModule: under a `replace` the selected
// module is something else, and its version is a statement about the
// replacement rather than about this pin. Saying so beats failing a pin that
// may well be right -- and beats passing one that was never checked.
func TestCheckClientPinSkipsAReplacedModule(t *testing.T) {
	var r recorder
	checkClientPin(&r, ClientInfo{Module: "example.com/c", Version: "v1.2.3"},
		info(&debug.Module{
			Path:    "example.com/c",
			Version: "v0.0.0-00010101000000-000000000000",
			Replace: &debug.Module{Path: "../local", Version: "(devel)"},
		}))
	if len(r.skips) != 1 {
		t.Fatalf("a replaced module was not skipped: skips=%v errs=%v", r.skips, r.errs)
	}
	if len(r.errs) != 0 {
		t.Errorf("a replaced module was also reported as drift: %v", r.errs)
	}
}

// TestCheckClientPinRefusesAnIncompletePair stops the check passing vacuously
// on a driver that declares half a pin.
func TestCheckClientPinRefusesAnIncompletePair(t *testing.T) {
	for _, c := range []ClientInfo{
		{Module: "", Version: "v1"},
		{Module: "example.com/c", Version: ""},
		{},
	} {
		var r recorder
		checkClientPin(&r, c, info(&debug.Module{Path: "example.com/c", Version: "v1"}))
		if len(r.fatal) != 1 {
			t.Errorf("an incomplete pin %+v was not refused: fatal=%v", c, r.fatal)
		}
	}
}

// TestCheckClientPinRefusesWithoutBuildInfo: a binary with no build info
// cannot answer the question, and must say so rather than pass.
func TestCheckClientPinRefusesWithoutBuildInfo(t *testing.T) {
	var r recorder
	checkClientPin(&r, ClientInfo{Module: "example.com/c", Version: "v1"},
		func() (*debug.BuildInfo, bool) { return nil, false })
	if len(r.fatal) != 1 {
		t.Fatalf("a binary with no build info was not refused: fatal=%v", r.fatal)
	}
	var r2 recorder
	checkClientPin(&r2, ClientInfo{Module: "example.com/c", Version: "v1"},
		func() (*debug.BuildInfo, bool) { return nil, true })
	if len(r2.fatal) != 1 {
		t.Errorf("a nil BuildInfo reported ok was not refused: fatal=%v", r2.fatal)
	}
}
