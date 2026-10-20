// Command suites reads conformance/suites.json — the list of conformance
// suites CI drives — and answers two questions about it.
//
//	suites -check                      # is every entry true of this repository?
//	suites -list -mode integration     # which suites have an integration run?
//	suites -resolve nats -mode integration   # where does that run live?
//
// # What an entry means
//
// One entry is one NAMED RUN of the §11.2 conformance kit against one broker
// configuration. It carries the name CI passes as DRIVER, the driver package
// behind it, the test that drives it, the report files that run writes, §2.7's
// maintenance tier, and the RUN MODES that actually exist for it. Nothing here
// is a plan: -check holds every field to the repository, so an entry naming a
// test that does not exist, a run mode whose package is missing or a report
// nobody committed is a failure rather than a silently skipped job.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iqhive/iqmq/testkit"
)

// Schema is the manifest format version.
const Schema = 1

// ManifestPath is the manifest's path from the repository root.
const ManifestPath = "conformance/suites.json"

// ReportDir is where the committed reports a suite names live.
const ReportDir = "conformance/reports/latest"

// ImageLockPath is where hack/images-lock.sh records the digest every broker
// image resolved to.
const ImageLockPath = "conformance/harness/images.lock"

// imageLock reads ImageLockPath and returns its `name repository@digest` map,
// keyed by name.
//
// It is parsed rather than imported because conformance/harness lives in a
// module this one cannot reach, and because the lock is data: one line per
// image, written by hack/images-lock.sh.
func imageLock(root string) (map[string]string, error) {
	path := filepath.Join(root, ImageLockPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("every suite names its broker image by the name %s gives it, and the lock cannot be read: %w", ImageLockPath, err)
	}
	out := map[string]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s:%d: %q is not `name repository@digest`", path, i+1, line)
		}
		out[fields[0]] = fields[1]
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s pins no image", path)
	}
	return out, nil
}

// Manifest is conformance/suites.json.
type Manifest struct {
	// Schema is the format version.
	Schema int `json:"schema"`
	// Suites are the conformance suites, in the order CI enumerates them.
	Suites []Suite `json:"suites"`
}

// Suite is one named conformance run.
type Suite struct {
	// Name is what CI passes as DRIVER. It is also the test's suffix: the
	// integration test of suite "kafka-kafkago" is
	// TestConformance_kafka_kafkago, and -check holds the two together so
	// that a name cannot drift from the test it selects.
	Name string `json:"name"`
	// Driver is the registered driver name the run reports under, which is
	// the "driver" field of every report the suite writes.
	Driver string `json:"driver"`
	// Package is the driver's directory from the repository root. It is
	// where the driver's own tests live, which is what the critical run
	// mode runs.
	Package string `json:"package"`
	// Test is the test function that drives the §11.2 suite.
	Test string `json:"test"`
	// Reports are the report file names in conformance/reports/latest that
	// this suite's run writes. A suite that runs the kit once per profile
	// writes one per profile.
	Reports []string `json:"reports"`
	// CrossCheck marks a run that publishes no capability evidence of its
	// own and exists to be compared with another suite's. It is the only
	// way an entry may name no report, and it is not a way to avoid
	// committing one: every report in conformance/reports/latest still has
	// to be claimed by a suite that is not a cross-check, so the evidence
	// behind a matrix row always names the run that produced it.
	CrossCheck bool `json:"cross_check,omitempty"`
	// Image is the name conformance/harness/images.lock gives the broker
	// image this run is made against — `nats`, `elasticmq`, `artemis` —
	// and not the reference itself, which lives in
	// conformance/harness/images.go and is resolved to a digest in the
	// lock. It is what binds a capability claim to a broker version:
	// confgen requires the report's own `broker.image` to be that image's
	// repository, so a report filed under the wrong suite, or a suite
	// pointed at a broker it does not test, is refused rather than
	// published.
	//
	// A suite with inprocess evidence names none, because there is no
	// container. A cross-check may name one and need not: it publishes no
	// capability evidence, and rabbitmq-delayed's image is BUILT rather
	// than pulled, so the lock has no entry for it.
	Image string `json:"image,omitempty"`
	// Evidence is how the run's results are obtained (§11.2): container,
	// emulator, managed or inprocess. It has to agree with the committed
	// reports, because it is what keeps an emulator's answer from being
	// read as a broker's.
	Evidence string `json:"evidence"`
	// Tier is §2.7's maintenance tier, 1, 2 or 3. It is what the weekly
	// soak job selects on, and it is not a run mode.
	Tier int `json:"tier"`
	// Runs are the run modes that EXIST for this suite. A mode listed here
	// has a package and a matching test in this repository; -check proves
	// it. A mode a suite does not list is a job CI does not run for it,
	// rather than a job that fails.
	Runs []string `json:"runs"`
	// Env is an environment variable the suite's test requires, for a run
	// that is opt-in because it is slow. The test skips itself without it;
	// naming it here is what tells a reader why the job logs a skip.
	Env string `json:"env,omitempty"`
	// Why is the entry's one-line reason for existing, for a reader of the
	// JSON. The format carries no comments and an entry whose purpose is
	// not obvious from its name is worth a sentence.
	Why string `json:"why,omitempty"`
	//
	// It is OPTIONAL and its absence means something definite: nobody has
	// run `iqmq doctor` against this broker and recorded the DSN that
	// worked. hack/doctor-harness.sh reports such a suite as unreachable
	// rather than guessing a DSN, so the gap is visible instead of silent,
	// and a template is added here only once it has been watched working.
	//
	// It lives here rather than in a table inside cmd/iqmq because this
	// file is what knows which harnesses exist: a second list would drift,
	// which is the defect B17 spent its increment removing from the
	// generated artefacts.
	Doctor *DoctorTarget `json:"doctor,omitempty"`
}

// DoctorTarget is a recipe for reaching one suite's broker with a DSN.
//
// It is deliberately NOT a general container recipe. conformance/harness holds
// the wait strategies, the multi-container pairs and the emulator setup, in Go,
// and duplicating that here in JSON would be a second implementation of it.
// What this carries is the minimum a shell script needs to run ONE container
// from the pinned digest and hand a DSN to the CLI: the port to publish, the
// arguments and environment the image needs to come up, and the DSN template.
// A broker the harness has to work harder for has no entry, which is the
// honest answer rather than a half-recipe.
type DoctorTarget struct {
	// DSN is the connection string, with {addr} standing for the host and
	// the ephemeral port the container was published on. The port is
	// ephemeral on purpose: this host runs other people's containers, and a
	// fixed port collides with them.
	DSN string `json:"dsn"`
	// Ports are the container ports to publish, the FIRST of which {addr}
	// and {port} name; the second is {addr2} and {port2}, and so on.
	//
	// More than one is not hypothetical: `doctor` against Pulsar cannot ping
	// at all without either `admin=` or `pingtopic=`, because the driver's
	// probe has no target otherwise -- so its recipe publishes the binary
	// protocol AND the admin API. A single-port field could not express it.
	Ports []int `json:"ports"`
	// Command replaces the image's own command, when the broker needs one.
	Command []string `json:"command,omitempty"`
	// Env is the environment the image needs, as NAME=VALUE.
	Env []string `json:"env,omitempty"`
	// Verified is the date somebody watched `iqmq doctor` succeed against
	// this recipe, in the form conformance reports use. An entry with no
	// date is refused: the whole point of the field is that it is evidence
	// rather than a guess.
	Verified string `json:"verified"`
}

// Mode is one run mode: a tier of testing the Makefile, hack/conformance.sh
// and the integration workflow all name.
//
// Dir, Prefix and Tags are how a mode is RESOLVED, and Implemented is whether
// this repository has it at all. A mode that is not implemented may not be
// listed by any suite, which is the check three increments needed and did not
// have: `make critical` resolved ./critical/... in the conformance module,
// conformance/critical has never existed, and the first suite entry added
// would therefore have turned the integration job red at a step that cannot
// pass.
type Mode struct {
	// Name is the mode's name, which is the Makefile target's name too.
	Name string
	// Dir is where the mode's tests live. An empty Dir means the suite's
	// own Package, in the root of the repository rather than under
	// conformance/.
	Dir string
	// Prefix is what the mode's test names begin with.
	Prefix string
	// Tags is the build tag the tests carry, empty for none.
	Tags string
	// Timeout bounds one run.
	Timeout string
	// PerSuite is whether the mode's test is named after the suite
	// (Prefix+ident(name)) or is merely a set of tests under Prefix.
	PerSuite bool
	// Implemented is whether the mode's tests exist in this repository.
	// An unimplemented mode is kept here, rather than deleted, because the
	// Makefile, the workflows and the plan all name it: a reader who runs
	// `make chaos` gets the reason it does nothing instead of a Go build
	// error about a package that is not there.
	Implemented bool
	// Note says what the mode is, and for an unimplemented one what is
	// missing.
	Note string
}

// Modes is the run-mode registry, in the order the Makefile lists them.
//
// Implemented is a claim about this repository and -check holds it in both
// directions: an implemented mode's directory has to exist and hold tests
// under its prefix, and an unimplemented mode's must not.
func Modes() []Mode {
	return []Mode{
		{
			Name: "integration", Dir: "conformance/suites", Prefix: "TestConformance_",
			Tags: "integration", Timeout: "40m", PerSuite: true, Implemented: true,
			Note: "the whole §11.2 kit against the suite's containerised broker; writes the reports the capability matrix is generated from",
		},
		{
			Name: "critical", Prefix: "TestCritical", Timeout: "20m", Implemented: true,
			Note: "§11.2's driver-specific critical tests, in the driver's own package against an in-process fake. It needs no container, so `make ci` already runs it; `make critical DRIVER=x` is the way to run one driver's set by name",
		},
		{
			Name: "matrix", Dir: "conformance/matrix", Prefix: "TestMatrix_",
			Tags: "matrix", Timeout: "90m", PerSuite: true, Implemented: true,
			Note: "the broker-version matrix from IQMQ_<BROKER>_IMAGES; nightly",
		},
		{
			Name: "chaos", Dir: "conformance/chaos", Prefix: "TestChaos_",
			Tags: "chaos", Timeout: "60m", PerSuite: true, Implemented: true,
			Note: "§11.2's L3 toxiproxy fault scenarios: reset mid-burst, latency over the ack timeout, session timeout, bandwidth, container stop-start and topology deletion. `make chaos DRIVER=x` runs one suite's set, and EXTENDED=1 adds the two weekly scenarios; nightly subset, weekly extended",
		},
		{
			Name: "soak", Dir: "conformance/soak", Prefix: "TestSoak_",
			Tags: "soak", Timeout: "26h", PerSuite: true, Implemented: true,
			Note: "§11.4's L4 long run with heap and goroutine profile diffs, reconnect storms and lease expiry. `make soak DRIVER=x TIME=4h`; a run shorter than ten minutes grades INCONCLUSIVE rather than clean; weekly",
		},
		{
			Name: "differential", Dir: "conformance/differential", Prefix: "FuzzDifferential_",
			Tags: "integration", Timeout: "40m", PerSuite: true, Implemented: true,
			Note: "§11.3 row 32: one op script against the memory engine and a real broker, compared by settled multiset and per-key first-delivery order. `make differential DRIVER=x` runs one broker's seeds; nightly",
		},
		{
			Name: "inprocess", Prefix: "TestConformance", Timeout: "20m", Implemented: true,
			Note: "the §11.2 kit against a driver that needs no broker, in the driver's own package. `make conformance-memory` is this mode for the memory driver, and `make ci` runs it",
		},
	}
}

// registry is a set of run modes.
//
// It is unexported because this is `package main` and nothing outside it can
// use it — and because exporting it changed `hack/api/root.export`, which
// three increments landing in the same round would each have had to
// regenerate. It exists so that validateWith can be handed a registry other
// than [Modes], which is what keeps the "a suite may not declare a mode this
// repository does not implement" guard under test now that every mode in
// Modes() IS implemented: with no unimplemented mode left to name, the only
// way to exercise that branch is to inject one.
type registry []Mode

// Lookup returns the mode of that name.
func (r registry) Lookup(name string) (Mode, bool) {
	for _, m := range r {
		if m.Name == name {
			return m, true
		}
	}
	return Mode{}, false
}

// LookupMode returns the mode of that name from [Modes].
func LookupMode(name string) (Mode, bool) { return registry(Modes()).Lookup(name) }

// Ident is the Go identifier fragment a suite name contributes to a test
// name: the name with every character a Go identifier cannot hold replaced by
// an underscore. It is the same substitution hack/conformance.sh applies, and
// -check compares the result with the entry's own Test.
func Ident(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, name)
}

// Load reads and unmarshals the manifest under root.
func Load(root string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(root, ManifestPath))
	if err != nil {
		return nil, err
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: %w", ManifestPath, err)
	}
	return &m, nil
}

// Select returns the suites a mode and a tier select, in manifest order. An
// empty mode selects every suite and a zero tier every tier.
func (m *Manifest) Select(mode string, tier int) []Suite {
	var out []Suite
	for i := range m.Suites {
		s := &m.Suites[i]
		if tier != 0 && s.Tier != tier {
			continue
		}
		if mode != "" && !s.Has(mode) {
			continue
		}
		out = append(out, *s)
	}
	return out
}

// Find returns the suite of that name.
func (m *Manifest) Find(name string) (Suite, bool) {
	for i := range m.Suites {
		if m.Suites[i].Name == name {
			return m.Suites[i], true
		}
	}
	return Suite{}, false
}

// Has reports whether the suite declares a run mode.
func (s Suite) Has(mode string) bool {
	for _, r := range s.Runs {
		if r == mode {
			return true
		}
	}
	return false
}

// Resolution is where and how one suite's run of one mode happens. It is what
// hack/conformance.sh needs and the one place that decides it.
type Resolution struct {
	// Dir is the directory to run `go test` in, from the repository root.
	Dir string
	// Pattern is the -run pattern, ANCHORED AT BOTH ENDS where the mode's
	// test is named after the suite. The anchor matters: `-run
	// '^TestConformance_nats'` also selects TestConformance_natsunified,
	// so an unanchored pattern for DRIVER=nats runs two suites and files
	// the second one's report under the first one's job.
	Pattern string
	// Tags is the build tag to pass, empty for none.
	Tags string
	// Timeout bounds the run.
	Timeout string
}

// Resolve returns where a suite's run of a mode lives, or an error naming what
// is wrong — an unknown mode, a mode this repository does not implement, or a
// mode this suite does not declare.
func (m *Manifest) Resolve(name, mode string) (Resolution, error) {
	s, ok := m.Find(name)
	if !ok {
		return Resolution{}, fmt.Errorf("no conformance suite is called %q; %s lists %s", name, ManifestPath, strings.Join(m.names(), ", "))
	}
	md, ok := LookupMode(mode)
	if !ok {
		return Resolution{}, fmt.Errorf("%q is not a run mode; the modes are %s", mode, strings.Join(modeNames(), ", "))
	}
	if !md.Implemented {
		return Resolution{}, fmt.Errorf("run mode %q is not implemented in this repository: %s", mode, md.Note)
	}
	if !s.Has(mode) {
		return Resolution{}, fmt.Errorf("suite %q does not declare the %q run mode; it declares %s", name, mode, strings.Join(s.Runs, ", "))
	}
	dir := md.Dir
	if dir == "" {
		dir = s.Package
	}
	// A per-suite mode's test is Prefix+Ident(name) and NOT the entry's
	// Test, which names the integration run alone. Using Test here made
	// `make matrix DRIVER=jetstream` run
	// `-run '^TestConformance_jetstream$'` inside conformance/matrix, where
	// the test is TestMatrix_jetstream -- and `go test` exits 0 and prints
	// PASS when its pattern selects nothing, so the nightly broker-version
	// matrix would have been green and measured nothing. It is family
	// A29's failure exactly, found here because
	// TestResolveSelectsExactlyOneTestPerMode covers every per-suite mode
	// and not just the one the author was thinking about.
	pattern := "^" + md.Prefix
	if md.PerSuite {
		pattern = "^" + md.Prefix + Ident(s.Name) + "$"
	}
	return Resolution{Dir: dir, Pattern: pattern, Tags: md.Tags, Timeout: md.Timeout}, nil
}

// names is every suite name, for an error message.
func (m *Manifest) names() []string {
	out := make([]string, 0, len(m.Suites))
	for i := range m.Suites {
		out = append(out, m.Suites[i].Name)
	}
	return out
}

// modeNames is every mode's name, for an error message.
func modeNames() []string {
	ms := Modes()
	out := make([]string, 0, len(ms))
	for i := range ms {
		out = append(out, ms[i].Name)
	}
	return out
}

// evidenceKinds are the report's four evidence values (§11.2).
var evidenceKinds = []string{
	string(testkit.EvidenceContainer), string(testkit.EvidenceEmulator),
	string(testkit.EvidenceManaged), string(testkit.EvidenceInProcess),
}

// Validate holds every entry of the manifest to the repository under root and
// returns one error per problem.
//
// It is deliberately offline: no container, no network, no build. The whole
// value of the check is that it runs in `make ci` on the branch that edits the
// list, rather than in a nightly job nobody reads.
func (m *Manifest) Validate(root string) []error { return m.validateWith(root, Modes()) }

// validateWith is [Manifest.Validate] against a given run-mode registry.
func (m *Manifest) validateWith(root string, reg registry) []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if m.Schema != Schema {
		fail("%s: schema is %d, want %d", ManifestPath, m.Schema, Schema)
	}
	if len(m.Suites) == 0 {
		fail("%s lists no suites, so CI drives no conformance suite at all; an empty list is indistinguishable from a green pipeline", ManifestPath)
	}

	// The mode registry's own claim, in both directions. An implemented
	// mode has to have its tests, and an unimplemented one has to have
	// none: the second half is what stops a suite from declaring a mode
	// the day its package appears without the list being revisited.
	tests := map[string]map[string]bool{}
	for _, md := range reg {
		if md.Dir == "" {
			continue
		}
		found, err := testNames(filepath.Join(root, md.Dir))
		switch {
		case errors.Is(err, os.ErrNotExist):
			if md.Implemented {
				fail("run mode %q claims to be implemented and %s does not exist", md.Name, md.Dir)
			}
			continue
		case err != nil:
			fail("run mode %q: reading %s: %v", md.Name, md.Dir, err)
			continue
		}
		tests[md.Dir] = found
		switch got := countPrefix(found, md.Prefix); {
		case md.Implemented && got == 0:
			fail("run mode %q claims to be implemented and %s holds no test called %s*", md.Name, md.Dir, md.Prefix)
		case !md.Implemented && got != 0:
			fail("run mode %q is marked unimplemented and %s holds %d test(s) called %s*; the registry is out of date", md.Name, md.Dir, got, md.Prefix)
		}
	}

	lock, err := imageLock(root)
	if err != nil {
		fail("%v", err)
	}

	seen := map[string]string{}
	claimedReports := map[string]bool{}
	claimedTests := map[string]bool{}
	for i := range m.Suites {
		s := m.Suites[i]
		where := fmt.Sprintf("%s: suite %q", ManifestPath, s.Name)
		switch {
		case s.Name == "":
			fail("%s: entry %d has no name", ManifestPath, i)
			continue
		case seen[s.Name] != "":
			fail("%s: two entries are called %q; the name is the job's identity in the CI matrix", ManifestPath, s.Name)
			continue
		}
		seen[s.Name] = s.Name
		if s.Driver == "" {
			fail("%s names no driver", where)
		}
		if s.Tier < 1 || s.Tier > 3 {
			fail("%s has tier %d; §2.7's tiers are 1, 2 and 3", where, s.Tier)
		}
		if !slicesContains(evidenceKinds, s.Evidence) {
			fail("%s has evidence %q, want one of %s", where, s.Evidence, strings.Join(evidenceKinds, ", "))
		}
		// The broker image, by the name the lock gives it. A suite that
		// names none publishes capability evidence nothing can tie to a
		// broker version, which is what confgen refuses on the other
		// side.
		// A run with no container of its own names no image: the
		// in-process kit has none, and a MANAGED run is a real hosted
		// service, which is the reason §11.2 makes managed its own
		// evidence kind. Requiring a lock entry for either would send
		// the next author looking for an image that does not exist.
		containerless := s.Evidence == string(testkit.EvidenceInProcess) || s.Evidence == string(testkit.EvidenceManaged)
		switch {
		case s.Image == "":
			if !s.CrossCheck && !containerless {
				fail("%s names no image, so its capability evidence names no broker version; the name is %s's, such as `nats` or `artemis`", where, ImageLockPath)
			}
		case containerless:
			fail("%s has %s evidence and names the image %q; there is no container to pin", where, s.Evidence, s.Image)
		case len(lock) == 0:
			// Reported once, below.
		case lock[s.Image] == "":
			names := make([]string, 0, len(lock))
			for n := range lock {
				names = append(names, n)
			}
			sort.Strings(names)
			fail("%s names the image %q, which %s does not pin (it pins %s)", where, s.Image, ImageLockPath, strings.Join(names, ", "))
		}
		switch {
		case s.Package == "":
			fail("%s names no package", where)
		default:
			if fi, err := os.Stat(filepath.Join(root, s.Package)); err != nil || !fi.IsDir() {
				fail("%s names the package %s, which is not a directory in this repository", where, s.Package)
			}
		}
		if len(s.Runs) == 0 {
			fail("%s declares no run mode, so nothing would ever run it", where)
		}
		for _, r := range s.Runs {
			md, ok := reg.Lookup(r)
			switch {
			case !ok:
				fail("%s declares the run mode %q, which is not one of %s", where, r, strings.Join(modeNames(), ", "))
				continue
			case !md.Implemented:
				fail("%s declares the run mode %q, which this repository does not implement: %s", where, r, md.Note)
				continue
			}
			dir := md.Dir
			if dir == "" {
				dir = s.Package
			}
			found, ok := tests[dir]
			if !ok {
				var err error
				if found, err = testNames(filepath.Join(root, dir)); err != nil {
					fail("%s declares %q and %s cannot be read: %v", where, r, dir, err)
					continue
				}
				tests[dir] = found
			}
			switch {
			case md.PerSuite:
				want := md.Prefix + Ident(s.Name)
				if !found[want] {
					fail("%s declares the %q run mode and %s has no %s; the run would select nothing and `go test -bench`-style exit 0 would report it as a pass", where, r, dir, want)
				}
				if r == "integration" && s.Test != want {
					fail("%s names the test %q; the %q run mode resolves the name %q to %q, so the entry and the run would drive different tests", where, s.Test, r, s.Name, want)
				}
			case countPrefix(found, md.Prefix) == 0:
				fail("%s declares the %q run mode and %s holds no test called %s*", where, r, dir, md.Prefix)
			}
		}
		if s.Test == "" {
			fail("%s names no test", where)
		}
		claimedTests[s.Test] = true
		if len(s.Reports) == 0 && !s.CrossCheck {
			fail("%s names no report, so its run publishes no capability evidence; a run that is not meant to, such as a second emulator read against the first, sets cross_check", where)
		}
		for _, name := range s.Reports {
			if !s.CrossCheck {
				claimedReports[name] = true
			}
			rep, err := readReport(filepath.Join(root, ReportDir, name))
			if errors.Is(err, os.ErrNotExist) {
				if names, listErr := reportNames(filepath.Join(root, ReportDir)); listErr == nil && len(names) == 0 {
					continue
				}
			}
			if err != nil {
				fail("%s names the report %s: %v", where, name, err)
				continue
			}
			if rep.Driver != s.Driver {
				fail("%s names the report %s, whose driver is %q", where, name, rep.Driver)
			}
			if rep.FileName() != name {
				fail("%s names the report %s, which a run of driver %q profile %q would write as %s", where, name, rep.Driver, rep.Profile, rep.FileName())
			}
			if string(rep.Evidence) != s.Evidence {
				fail("%s declares evidence %q and its report %s was made with %q", where, s.Evidence, name, rep.Evidence)
			}
			// B05's hand-off: a committed report carrying a failure is
			// a driver that does not pass the suite, and the list is
			// where that stops being invisible. The scenario that fails
			// for a driver advertising no limits at all against a real
			// broker is the one this was written for.
			for _, res := range rep.Failures() {
				fail("%s: the committed report %s records %s as FAILED: %s", where, name, res.Scenario, res.Note)
			}
		}
	}

	// A test in conformance/suites that no entry names is a suite CI does
	// not run, which is the state the whole list was in.
	if found := tests["conformance/suites"]; found != nil {
		for name := range found {
			if strings.HasPrefix(name, "TestConformance_") && !claimedTests[name] {
				fail("%s is in conformance/suites and no entry of %s names it, so CI never runs it", name, ManifestPath)
			}
		}
	}
	// And a committed report no entry names is capability evidence whose
	// provenance nothing in the repository states.
	names, err := reportNames(filepath.Join(root, ReportDir))
	if err != nil {
		fail("reading %s: %v", ReportDir, err)
	}
	for _, name := range names {
		if !claimedReports[name] {
			fail("%s/%s is committed capability evidence and no entry of %s says which suite produces it", ReportDir, name, ManifestPath)
		}
	}
	// Every doctor recipe is held to being usable: a template with no
	// {addr} would be a DSN that never reached the container the script
	// started, and a recipe with no verification date would be a guess
	// wearing evidence's clothes.
	for i := range m.Suites {
		s := &m.Suites[i]
		if s.Doctor == nil {
			continue
		}
		where := fmt.Sprintf("%s: suite %q: doctor", ManifestPath, s.Name)
		d := s.Doctor
		switch {
		case d.DSN == "":
			fail("%s names no DSN", where)
		case !strings.Contains(d.DSN, "{addr}") && !strings.HasPrefix(d.DSN, "memory://") &&
			!strings.HasPrefix(d.DSN, "mem://"):
			fail("%s has no {addr} placeholder, so the DSN cannot reach the container the script starts: %q",
				where, d.DSN)
		}
		if len(d.Ports) == 0 {
			fail("%s publishes no port, so nothing can reach the container", where)
		}
		for _, port := range d.Ports {
			if port <= 0 || port > 65535 {
				fail("%s publishes port %d, which is not a port", where, port)
			}
		}
		if d.Verified == "" {
			fail("%s records no verification date; a recipe nobody has watched work is a guess, and "+
				"hack/doctor-harness.sh reports a suite with no recipe as unreachable rather than "+
				"pretending", where)
		}
		for _, kv := range d.Env {
			if !strings.Contains(kv, "=") {
				fail("%s: env %q is not NAME=VALUE", where, kv)
			}
		}
		if s.Image == "" {
			fail("%s names a container recipe and the suite names no image, so there is nothing "+
				"to pull it from", where)
		}
	}

	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errs
}

// slicesContains is the one-line membership test, spelled out because the root
// module is held to the standard library and this file is read by people
// checking a CI list.
func slicesContains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// countPrefix is how many of the names begin with prefix.
func countPrefix(names map[string]bool, prefix string) int {
	n := 0
	for name := range names {
		if strings.HasPrefix(name, prefix) {
			n++
		}
	}
	return n
}

// testNames parses every _test.go file in dir — build tags and all, because a
// tagged file's declarations are what a tagged run selects — and returns the
// names of its top-level Test and Fuzz functions.
func testNames(dir string) (map[string]bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Name == nil {
				continue
			}
			if strings.HasPrefix(fd.Name.Name, "Test") || strings.HasPrefix(fd.Name.Name, "Fuzz") {
				out[fd.Name.Name] = true
			}
		}
	}
	return out, nil
}

// reportNames is every committed report file name in dir.
func reportNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// readReport decodes one committed conformance report.
//
// It reads it as a testkit.Report — the type the kit WRITES — rather than
// through conformance/report, which lives in a module this one cannot import.
// The two declarations are held together by TestReportMatchesConformanceSchema
// in the conformance module.
func readReport(path string) (*testkit.Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rep testkit.Report
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rep); err != nil {
		return nil, err
	}
	if err := rep.Validate(); err != nil {
		return nil, err
	}
	return &rep, nil
}
