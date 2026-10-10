// This file carries NO build tag, deliberately: it holds the rule that decides
// which file each arm of the SQS suite writes its report to, and the two tests
// that watch it. Everything container-shaped stays in sqs_test.go behind
// `integration`, so `make test` runs both assertions in milliseconds with no
// Docker and no LocalStack.

package suites

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/conformance/harness"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/testkit"
)

// sqsShippedEmulator is the harness name of the emulator whose reports ARE this
// driver's shipped capability evidence: conformance/reports/latest/sqs-standard.json
// and sqs-fifo.json, which the conformance suites manifest names and the generated
// matrix renders. Its arm therefore keeps the driver's own profile verbatim.
const sqsShippedEmulator = "elasticmq"

// sqsSuiteFile is the file the two tests below read their arms out of. It is
// behind the `integration` build tag, so they read it as SOURCE.
const sqsSuiteFile = "sqs_test.go"

// sqsTestPrefix is the name every arm of this suite starts with.
const sqsTestPrefix = "TestConformance_sqs"

// sqsHarness is the harness one arm of the SQS suite hands the kit.
//
// It exists for one line — the profile — and it is a function rather than that
// line inlined in sqs_test.go because sqs_test.go is behind the `integration`
// build tag and nothing without a container can run what is written there. The
// report key is worth an instrument: testkit.Report.Save writes
// driver-profile.json with os.Create, so two arms that computed the same key
// overwrite each other in the single directory hack/conformance.sh writes into
// and .github/workflows/integration.yml merges every suite's artifact into, and
// the survivor is whichever run finished last.
func sqsHarness(b *harness.Broker, cfg sq.Config) testkit.Harness {
	h := sq.ConformanceHarness(cfg, testkit.EvidenceEmulator, testkit.Broker{
		Image: b.Image, Version: b.Version,
	})
	h.Profile = sqsReportProfile(b.Name, h.Profile)
	return h
}

// sqsReportProfile is the report profile for one emulator: the driver's own
// ("standard", "fifo") for the emulator the shipped evidence comes from, and the
// driver's own with the emulator's name appended for any other.
//
// The emulator is added HERE and not in drivers/sqs.Config.Profile(), which also
// feeds the profile a live SQS client reports at runtime and knows nothing about
// emulators: making that method emulator-aware would rename the shipped
// sqs-standard column for every consumer of it.
// conformance/suites/rabbitmq_test.go does the same thing at the same altitude
// for that driver's env-gated cross-check ("amqp091-delayed").
//
// The shipped arm's key is deliberately UNCHANGED rather than uniformly
// suffixed: sqs-standard.json and sqs-fifo.json are committed, named in
// The conformance suites manifest, and read by confgen, so a rename would orphan both.
func sqsReportProfile(emulator, driverProfile string) string {
	if emulator == sqsShippedEmulator {
		return driverProfile
	}
	return driverProfile + "-" + emulator
}

// sqsArm is one TestConformance_sqs* function's call to runSQSConformance: the
// emulator it points the suite at and the profile it asks for.
type sqsArm struct {
	test     string
	emulator string
	fifo     bool
}

// sqsBrokerNames maps a conformance/harness constructor to the Name the Broker
// it returns carries, which is what sqsReportProfile reads. The two are written
// down rather than derived by lowercasing, so that an arm against an emulator
// this test has never seen FAILS instead of guessing a name that would silently
// be the wrong key.
var sqsBrokerNames = map[string]string{
	"ElasticMQ":  "elasticmq",
	"LocalStack": "localstack",
}

// TestSQSReportKeysDistinguishTheEmulators holds every arm of the SQS suite to
// writing a different report file, and the two shipped arms to the names that
// are already committed.
//
// Both halves matter. Distinctness is the defect: TestConformance_sqs and
// TestConformance_sqs_localstack were both driver "sqs" profile "standard", so
// both wrote sqs-standard.json, and a run of the two into the one directory CI
// merges every suite's artifact into kept whichever finished last. The exact
// names are the other half — a fix that made the keys distinct by renaming the
// ElasticMQ arm would orphan the committed reports and the matrix columns built
// from them, so "distinct" on its own would be an instrument broader than the
// property.
//
// The arms are read out of sqs_test.go rather than listed here, because the
// collision is a property of the SET of arms: two arms naming the same emulator
// and the same profile would collide again however the profile is derived, and a
// list maintained by hand beside the suite would not notice.
//
// The report's broker image is not part of the key and is not set here;
// confgen's own suite-image attestation (conformance/cmd/confgen/main.go) is
// what refuses a report whose broker is not the image its suite runs against,
// and it is deliberately left alone.
func TestSQSReportKeysDistinguishTheEmulators(t *testing.T) {
	t.Parallel()
	// The committed names, one per arm. An arm absent from this map fails
	// rather than passing quietly: a new emulator is a new report key, and
	// whether that key is evidence or a cross-check is a decision for
	// The conformance suites manifest, not for whatever the code happens to emit.
	want := map[string]string{
		"TestConformance_sqs":            "sqs-standard.json",
		"TestConformance_sqs_fifo":       "sqs-fifo.json",
		"TestConformance_sqs_localstack": "sqs-standard-localstack.json",
	}
	arms := sqsSuiteArms(t)
	if len(arms) == 0 {
		t.Fatalf("%s has no arm calling runSQSConformance, so this test asserts nothing", sqsSuiteFile)
	}
	writer := map[string]string{}
	seen := map[string]bool{}
	for _, a := range arms {
		h := sqsHarness(&harness.Broker{Name: a.emulator}, sq.Config{FIFO: a.fifo})
		rep := &testkit.Report{Driver: h.Driver, Profile: h.Profile}
		got := rep.FileName()
		seen[a.test] = true
		if w, ok := want[a.test]; !ok {
			t.Errorf("%s is a new arm (emulator %q, fifo %v) and writes %s; name the file here and say in the conformance suites manifest whether it is evidence or a cross-check",
				a.test, a.emulator, a.fifo, got)
		} else if got != w {
			t.Errorf("%s (emulator %q, fifo %v) writes %s, want %s", a.test, a.emulator, a.fifo, got, w)
		}
		if other, dup := writer[got]; dup {
			t.Errorf("%s and %s both write %s: one directory holds one of them, and it is whichever run finished last",
				other, a.test, got)
		}
		writer[got] = a.test
	}
	for test := range want {
		if !seen[test] {
			t.Errorf("%s names no arm %s, so this test is asserting the key of a run that no longer exists", sqsSuiteFile, test)
		}
	}
}

// TestSQSSuiteBuildsItsHarnessThroughTheKeyRule reads runSQSConformance and
// requires it to get its harness from sqsHarness.
//
// It is here because the rule above sits in a file the suite's own body cannot
// be run from: sqs_test.go is behind `integration`, so a body that went back to
// calling sq.ConformanceHarness directly — which is what it did before this fix,
// and what a merge resolved in favour of the old hunk would restore — would
// reinstate the collision with every other test in this file still passing. That
// was MEASURED rather than assumed: with the call site bypassed,
// `go test ./suites/` was still ok and `go vet -tags integration ./suites/` still
// exited 0.
func TestSQSSuiteBuildsItsHarnessThroughTheKeyRule(t *testing.T) {
	t.Parallel()
	body := sqsRunBody(t)
	var direct, viaRule int
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if fun.Name == "sqsHarness" {
				viaRule++
			}
		case *ast.SelectorExpr:
			if fun.Sel.Name == "ConformanceHarness" {
				direct++
			}
		}
		return true
	})
	if viaRule == 0 {
		t.Errorf("runSQSConformance in %s does not call sqsHarness, so the emulator is not in the report key and two arms of this suite write one file",
			sqsSuiteFile)
	}
	if direct != 0 {
		t.Errorf("runSQSConformance in %s calls ConformanceHarness directly %d time(s); the driver's own profile knows nothing about the emulator, so that harness carries the colliding key",
			sqsSuiteFile, direct)
	}
}

// sqsSuiteArms parses sqsSuiteFile and returns one arm per TestConformance_sqs*
// function, read from its call to runSQSConformance.
func sqsSuiteArms(t *testing.T) []sqsArm {
	t.Helper()
	var out []sqsArm
	for _, decl := range sqsParse(t).Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Name == nil || fd.Body == nil {
			continue
		}
		if !strings.HasPrefix(fd.Name.Name, sqsTestPrefix) {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != "runSQSConformance" {
				return true
			}
			if len(call.Args) != 3 {
				t.Errorf("%s: %s calls runSQSConformance with %d arguments; this test reads (t, broker, fifo)",
					sqsSuiteFile, fd.Name.Name, len(call.Args))
				return false
			}
			out = append(out, sqsArm{
				test:     fd.Name.Name,
				emulator: sqsEmulator(t, fd.Name.Name, call.Args[1]),
				fifo:     sqsBool(t, fd.Name.Name, call.Args[2]),
			})
			return false
		})
	}
	return out
}

// sqsRunBody is runSQSConformance's body.
func sqsRunBody(t *testing.T) *ast.BlockStmt {
	t.Helper()
	for _, decl := range sqsParse(t).Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if ok && fd.Recv == nil && fd.Name != nil && fd.Name.Name == "runSQSConformance" && fd.Body != nil {
			return fd.Body
		}
	}
	t.Fatalf("%s declares no runSQSConformance; the suite was restructured and this test is reading the wrong thing", sqsSuiteFile)
	return nil
}

// sqsParse parses the suite file. The test binary runs in the package
// directory, so the name is enough, and the `integration` tag does not stop a
// parser from reading it.
func sqsParse(t *testing.T) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), sqsSuiteFile, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", sqsSuiteFile, err)
	}
	return file
}

// sqsEmulator resolves the broker argument (`harness.ElasticMQ(t)`) to the Name
// the Broker it returns carries.
func sqsEmulator(t *testing.T, test string, arg ast.Expr) string {
	t.Helper()
	call, ok := arg.(*ast.CallExpr)
	if !ok {
		t.Errorf("%s: %s does not pass a harness constructor as its broker", sqsSuiteFile, test)
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		t.Errorf("%s: %s passes a broker this test cannot name", sqsSuiteFile, test)
		return ""
	}
	name, ok := sqsBrokerNames[sel.Sel.Name]
	if !ok {
		t.Errorf("%s: %s runs against harness.%s and sqsBrokerNames does not say which harness.Broker.Name that is; the report key depends on it",
			sqsSuiteFile, test, sel.Sel.Name)
		return ""
	}
	return name
}

// sqsBool reads the fifo argument, which is a literal in every arm.
func sqsBool(t *testing.T, test string, arg ast.Expr) bool {
	t.Helper()
	id, ok := arg.(*ast.Ident)
	if !ok || (id.Name != "true" && id.Name != "false") {
		t.Errorf("%s: %s passes a fifo argument that is not a literal, so this test cannot say which profile it runs", sqsSuiteFile, test)
		return false
	}
	return id.Name == "true"
}
