// Package testkit is iqmq's conformance kit: the suite every driver has to pass
// and the machine-readable report it produces (the test suite).
//
// # What it is for
//
// A driver's capability table is a set of claims. The specification says they have to be
// true, and this is what makes them checkable: every scenario is gated by a
// capability, and every claim is proved in both directions — the feature works
// when the driver says it does, and the unsupported path is refused when it says
// it does not. A claim proved in only one direction is not proved. That is not a
// nicety: a driver that advertises delayed redelivery and silently redelivers
// immediately, and a driver that says it cannot dead-letter while its Reject
// destroys the message, are both invisible to a test suite that only checks the
// happy path of what is claimed.
//
// # It uses only the portable API
//
// Every scenario goes through [github.com/iqhive/iqmq.Client] and the handles it
// opens. Nothing here reaches for the SPI, for a driver's own types or for
// Native. A conformance kit that used a driver's internals would pass a driver
// whose public behaviour was wrong, which is the one thing it exists to prevent.
//
// # Running it
//
//	func TestConformance(t *testing.T) {
//		testkit.RunConformance(t, testkit.Harness{
//			Driver: "memory",
//			Open:   open,
//			TopicName: func(p string) string { return "conformance." + p },
//			Timeout:   30 * time.Second,
//			Evidence:  testkit.EvidenceInProcess,
//		})
//	}
//
// [RunConformance] is the testing entry point. [Run] is the same suite without a
// *testing.T, which is what lets the kit's own tests check that it rejects a
// driver it should reject: a conformance kit nobody has watched fail is assumed
// not to work.
package testkit

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Schema is the report format version. It is the same number and the same
// layout the conformance report package reads, because that package
// lives in a module that may not be imported from here: the root module depends
// on the standard library alone, and the conformance module depends on
// testcontainers. The two declarations are held together by
// TestReportMatchesConformanceSchema in the conformance module.
const Schema = 1

// Evidence classifies how a run's results were obtained. It is what keeps an
// emulator's answer from being presented as a broker's.
type Evidence string

// The evidence kinds.
const (
	// EvidenceContainer is a real broker in a container.
	EvidenceContainer Evidence = "container"
	// EvidenceEmulator is a vendor emulator: Pub/Sub's, LocalStack, ElasticMQ.
	EvidenceEmulator Evidence = "emulator"
	// EvidenceManaged is a real managed service.
	EvidenceManaged Evidence = "managed"
	// EvidenceInProcess is the memory driver.
	EvidenceInProcess Evidence = "inprocess"
)

// Valid reports whether e names an evidence kind.
func (e Evidence) Valid() bool {
	switch e {
	case EvidenceContainer, EvidenceEmulator, EvidenceManaged, EvidenceInProcess:
		return true
	}
	return false
}

// Outcome is the result of one scenario.
type Outcome string

// The outcomes. They carry the Outcome prefix because [Skip] is the function a
// scenario returns to become one, and a constant and a function cannot share a
// name.
const (
	// OutcomePass is a scenario that held.
	OutcomePass Outcome = "pass"
	// OutcomeFail is a scenario that did not.
	OutcomeFail Outcome = "fail"
	// OutcomeSkip is a scenario that could not run, with the reason in the
	// note. A skip is never a pass: an unrun scenario proves nothing, and
	// the claim it would have proved stays unverified.
	OutcomeSkip Outcome = "skip"
)

// Kind says what sort of claim a scenario's outcome is evidence about, which is
// what decides whether ONE RUN can settle it.
//
// The distinction is the increment that added it: the kit could refute a
// negative claim about a FEATURE and not one about a GUARANTEE. "The driver
// says it cannot do this; prove it refuses" is right for a delay, a lease
// extension or a seek — each of those is a call, and a call can be refused —
// and wrong for an ordering or a delivery multiplicity, which are promises
// about every run. A standard SQS queue does not promise order and this
// repository's probe published thirty-two keyed messages and FAILED the driver
// when they arrived in order, which they almost always do with one consumer at
// that volume. Three true denials were lost to it.
type Kind string

// The claim kinds.
const (
	// KindBehaviour is a claim about a call the driver can refuse. One run
	// settles it in both directions, which is what every capability
	// scenario assumed before Kind existed. It is the zero value.
	KindBehaviour Kind = ""
	// KindGuarantee is a promise about every run — an ordering, a delivery
	// multiplicity, a durability. A single run can observe it BROKEN and no
	// number of runs can observe it absent, so the kit treats the two
	// directions asymmetrically: a claimed guarantee is refuted by one
	// counter-example and never positively proved, and a DENIED guarantee
	// is confirmed by one counter-example and otherwise left unrefuted
	// rather than failed.
	KindGuarantee Kind = "guarantee"
)

// Valid reports whether k names a kind.
func (k Kind) Valid() bool {
	switch k {
	case KindBehaviour, KindGuarantee:
		return true
	}
	return false
}

// Level mirrors the capability support levels as strings, so the report does not
// depend on the API's numbering.
type Level string

// The levels.
const (
	LevelUnsupported      Level = "unsupported"
	LevelConditional      Level = "conditional"
	LevelEmulatedVolatile Level = "emulated-volatile"
	LevelEmulatedDurable  Level = "emulated-durable"
	LevelNative           Level = "native"
)

// LevelUnread is NOT one of the levels. It is the explicit record that a
// scenario about a feature returned before it had a client, so the level the
// driver advertises for that feature was never read: a documented
// [Harness.Skip], a topology the driver refused with ErrUnsupported, and an
// Open that failed. The advertised level comes from a client's capability
// table, and those three paths never obtain one.
//
// It exists because the absence of a level used to say it. An empty `claimed`
// on a result with a feature meant "no client", and also meant "the kit dropped
// the level", and a reader could not tell the two apart -- so testkit accepted
// the empty level on a skip , conformance/report kept
// refusing it, and every nightly delivery since was rejected by the generator
// after all of its suites had passed. Neither a level from
// another scenario's client nor `unsupported` is a substitute: a feature's
// level depends on the topology the client was built for -- Pulsar advertises
// DeadLetter natively only when the scenario names a dead-letter destination --
// so either would be a claim this run did not read.
const LevelUnread Level = "unread"

// LevelOf maps a capability level to its report spelling.
func LevelOf(l mq.Level) Level {
	switch l {
	case mq.Native:
		return LevelNative
	case mq.EmulatedDurable:
		return LevelEmulatedDurable
	case mq.EmulatedVolatile:
		return LevelEmulatedVolatile
	case mq.Conditional:
		return LevelConditional
	case mq.Unsupported:
		return LevelUnsupported
	}
	return LevelUnsupported
}

// Result is one scenario's result.
type Result struct {
	// Scenario is the scenario's name.
	Scenario string `json:"scenario"`
	// Feature is the capability the scenario exercises, empty for a general
	// scenario.
	Feature string `json:"feature,omitempty"`
	// Claimed is the level the driver advertises for Feature, read from the
	// capability table of the client the scenario opened. It is
	// [LevelUnread] when the scenario returned before it had a client, and
	// it is never empty on a result that names a feature.
	Claimed Level `json:"claimed,omitempty"`
	// Positive reports whether the scenario verified that the feature works
	// (true) or that the unsupported path is refused (false). A claim
	// renders in the matrix only when both have passed.
	Positive bool `json:"positive"`
	// Kind says what sort of claim the outcome is evidence about, empty for
	// a behaviour. It is what tells a reader of the report whether the
	// evidence is two-sided or one-sided: a pass on a guarantee's positive
	// direction means "this run saw no counter-example", which is the
	// strongest thing any single run can say about a promise.
	Kind Kind `json:"kind,omitempty"`
	// Outcome is pass, fail or skip.
	Outcome Outcome `json:"outcome"`
	// Note is the reason for a skip or a failure.
	Note string `json:"note,omitempty"`
	// Duration is how long the scenario took.
	Duration time.Duration `json:"duration_ns,omitempty"`
}

// Broker identifies the broker a run was made against.
type Broker struct {
	// Image is what the run was made against, as the driver's harness
	// declared it: the container image reference the conformance harness
	// started -- repository and tag, or the digest
	// conformance/harness/images.lock pins -- or a placeholder for a driver
	// that starts no container, which is the "in-process" the memory
	// driver and the toy driver record here.
	Image string `json:"image"`
	// Version is the broker version the run recorded. The conformance
	// harness derives it from the image reference's TAG and never asks the
	// running broker, so it says which image was pulled and not which build
	// answered; a driver whose Image is a placeholder has no tag to read
	// and leaves this empty.
	Version string `json:"version,omitempty"`
}

// ClientInfo identifies the native client library.
type ClientInfo struct {
	// Module is the client library's module path, as the driver's go.mod
	// requires it.
	Module string `json:"module"`
	// Version is the module version that was linked in, so a report can be
	// read against the client release it was taken with.
	Version string `json:"version"`
}

// Report is one conformance run.
type Report struct {
	// Schema is the report format's own version. A reader that does not know
	// the value refuses the file rather than guessing at its fields.
	Schema int `json:"schema"`
	// Driver is the driver's suites.json name, and Profile the profile
	// within it for a driver that ships more than one (empty otherwise).
	// The pair is what Key returns and what identifies a report.
	Driver  string `json:"driver"`
	Profile string `json:"profile,omitempty"`
	// Broker and Client record what the run was made against: the container
	// image and the native client library.
	Broker Broker     `json:"broker"`
	Client ClientInfo `json:"client"`
	// Go is the toolchain version that produced the run, Date the RFC 3339
	// timestamp stamped when the run STARTED, and Commit the revision it
	// was measured at, read out of IQMQ_COMMIT -- which the conformance runner
	// and `make conformance-memory` set from `git rev-parse HEAD`, falling
	// back to the literal "unknown", so an empty Commit means a harness run
	// outside those. They are provenance, not inputs.
	Go     string `json:"go"`
	Date   string `json:"date"`
	Commit string `json:"commit,omitempty"`
	// CommitStatus is "unavailable" when the recorded source revision is absent.
	CommitStatus string `json:"commit_status,omitempty"`
	// Evidence says HOW the results were obtained: [EvidenceContainer],
	// [EvidenceEmulator], [EvidenceManaged] or [EvidenceInProcess]. It is
	// the field that keeps emulator evidence from being presented as
	// broker evidence, and a report carrying any other value -- including
	// the empty one -- is refused.
	Evidence Evidence `json:"evidence"`
	// Limits are the limits the driver advertised, keyed by the names in
	// Limits, with byte and count limits in their own units and durations
	// in nanoseconds.
	//
	// A limit the driver does NOT advertise is absent rather than zero,
	// because the specification makes the two different answers and forbids reading an
	// unadvertised limit as a bound. The map is in the report because a
	// limit a driver reads from a broker is a claim no in-process fake can
	// falsify: the specification's `message.max.bytes` lookup returned nothing against
	// Apache Kafka for four months and nothing noticed, because
	// publish/oversized-payload SKIPS when the limit is unknown and a skip
	// is indistinguishable from a pass.
	Limits map[string]int64 `json:"limits,omitempty"`
	// Results is one entry scenario the run reached, in the order
	// the suite ran them. A scenario that was never reached is absent, which
	// is a different answer from a skip.
	Results []Result `json:"results"`
}

// Key identifies the driver/profile pair.
func (r *Report) Key() string {
	if r.Profile == "" {
		return r.Driver
	}
	return r.Driver + "/" + r.Profile
}

// FileName is the conventional file name for a report.
func (r *Report) FileName() string { return strings.ReplaceAll(r.Key(), "/", "-") + ".json" }

// Counts returns how many scenarios passed, failed and were skipped.
func (r *Report) Counts() (pass, fail, skip int) {
	for _, res := range r.Results {
		switch res.Outcome {
		case OutcomePass:
			pass++
		case OutcomeFail:
			fail++
		case OutcomeSkip:
			skip++
		}
	}
	return pass, fail, skip
}

// Failures returns the results that failed.
func (r *Report) Failures() []Result {
	var out []Result
	for _, res := range r.Results {
		if res.Outcome == OutcomeFail {
			out = append(out, res)
		}
	}
	return out
}

// Find returns the result of one scenario.
func (r *Report) Find(scenario string) (Result, bool) {
	for _, res := range r.Results {
		if res.Scenario == scenario {
			return res, true
		}
	}
	return Result{}, false
}

// Verified is a feature's verification state.
type Verified struct {
	// Feature is the feature's name.
	Feature string
	// Claimed is the level the driver advertises, or [LevelUnread] when no
	// scenario about the feature reached a client to read it.
	Claimed Level
	// Positive reports whether a positive scenario passed.
	Positive bool
	// Negative reports whether a negative scenario passed.
	Negative bool
	// Failed reports whether any scenario for the feature failed.
	Failed bool
	// Guarantee reports whether the feature is a promise about every run
	// rather than a call that can be refused. It is what distinguishes
	// "nobody checked this" from "one run cannot check this": a denied
	// guarantee that no run has broken is Proven false for ever, and the
	// report says which of the two it is.
	Guarantee bool
}

// Proven reports whether the claim may render as supported: never when a
// scenario failed; an unsupported claim needs its negative path proven; a
// supported claim needs both.
//
// It is the same rule the conformance report package applies, stated
// here so that a driver's own test can assert it without the conformance module.
//
// The rule is unchanged for a guarantee, and [Verified.Unrefuted] is what says
// why one is not proven: a DENIED guarantee's negative direction is only
// established by a run that observed the guarantee break, so a driver that
// truthfully denies an ordering nothing reordered stays unproven for ever.
// That is the honest answer — the evidence rule is that an unproven claim
// does not RENDER, and an unsupported claim renders as "—" either way — and it
// replaces a scenario that FAILED such a driver.
func (v Verified) Proven() bool {
	if v.Failed {
		return false
	}
	if v.Claimed == LevelUnsupported {
		return v.Negative
	}
	return v.Positive && v.Negative
}

// Unrefuted reports whether the claim is one no single run can establish: a
// guarantee the driver does not promise and that nothing in this run broke.
//
// It is the difference between "nobody checked" and "one run cannot check",
// and a driver's own test logs the two differently: the first is work to do
// and the second is the state of the world.
func (v Verified) Unrefuted() bool {
	return v.Guarantee && !v.Failed && !v.Negative && v.Claimed == LevelUnsupported
}

// Verify reports the verification state of every feature the run touched.
func (r *Report) Verify() []Verified {
	byFeature := map[string]*Verified{}
	for _, res := range r.Results {
		if res.Feature == "" {
			continue
		}
		v := byFeature[res.Feature]
		if v == nil {
			v = &Verified{Feature: res.Feature}
			byFeature[res.Feature] = v
		}
		// The level comes from a result that READ one: a scenario that
		// never got a client carries LevelUnread, and taking the level
		// from whichever result came first (or last) would erase a
		// proven claim.
		if res.Claimed != "" && res.Claimed != LevelUnread {
			v.Claimed = res.Claimed
		}
		if res.Kind == KindGuarantee {
			v.Guarantee = true
		}
		switch res.Outcome {
		case OutcomePass:
			if res.Positive {
				v.Positive = true
			} else {
				v.Negative = true
			}
		case OutcomeFail:
			v.Failed = true
		case OutcomeSkip:
		}
	}
	out := make([]Verified, 0, len(byFeature))
	for _, v := range byFeature {
		if v.Claimed == "" {
			// No scenario about the feature reached a client, so
			// the run never read what the driver advertises. Said
			// explicitly, because an empty level reads as a level.
			v.Claimed = LevelUnread
		}
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Feature < out[j].Feature })
	return out
}

// Validate checks the structural invariants the consumer of a report relies on.
func (r *Report) Validate() error {
	if r.Schema != Schema {
		return errReportSchema
	}
	if r.Driver == "" {
		return errReportDriver
	}
	if !r.Evidence.Valid() {
		return errReportEvidence
	}
	if _, err := time.Parse(time.RFC3339, r.Date); err != nil {
		return errReportDate
	}
	for _, res := range r.Results {
		if res.Scenario == "" {
			return errReportScenario
		}
		switch res.Outcome {
		case OutcomePass, OutcomeFail, OutcomeSkip:
		default:
			return errReportOutcome
		}
		if !res.Kind.Valid() {
			return errReportKind
		}
		if res.Feature != "" {
			switch res.Claimed {
			case LevelUnsupported, LevelConditional, LevelEmulatedVolatile, LevelEmulatedDurable, LevelNative:
			case LevelUnread:
				// No level, because this scenario never reached a
				// client. `runScenario` records the FEATURE before
				// anything can go wrong -- a scenario's feature is a
				// property of the scenario, and recording it only
				// once Open had succeeded filed 70 results of the
				// committed corpus against no capability at all.
				// The level cannot move with it:
				// it is read from the client's capability table, and
				// a documented `Harness.Skip`, a topology the driver
				// refuses, and a failed Open all return before there
				// is one.
				//
				// A PASS is the case that stays a refusal: a
				// scenario cannot pass without a client, so its
				// capability table WAS read and "unread" is false.
				if res.Outcome == OutcomePass {
					return errReportLevel
				}
			default:
				// Including the EMPTY level, on every outcome. It
				// used to be accepted on a skip or a failure as
				// meaning "no client", which is also what a kit that
				// dropped a level writes; and conformance/report,
				// which is what confgen reads with, never accepted it
				// at all, so every report the nightly delivered was
				// rejected after its suite had passed.
				return errReportLevel
			}
		}
	}
	return nil
}

// Write encodes the report as indented JSON, in the layout confgen reads.
func (r *Report) Write(w io.Writer) error {
	if err := r.Validate(); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// Save writes the report into dir under its conventional name and returns the
// path. It is what a driver's conformance test calls so that
// conformance/cmd/confgen can turn the run into capabilities.json and the driver
// matrix.
func (r *Report) Save(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(dir, r.FileName())
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	if err := r.Write(f); err != nil {
		_ = f.Close()
		return "", err
	}
	return path, f.Close()
}

// newReport builds an empty report for a harness.
func newReport(h Harness, now time.Time) *Report {
	return &Report{
		Schema:   Schema,
		Driver:   h.Driver,
		Profile:  h.Profile,
		Broker:   h.Broker,
		Client:   h.Client,
		Go:       runtime.Version(),
		Date:     now.UTC().Format(time.RFC3339),
		Commit:   h.Commit,
		Evidence: h.Evidence,
	}
}

// The report's structural failures.
var (
	errReportSchema   = errors.New("testkit: the report's schema is not the one this kit writes")
	errReportDriver   = errors.New("testkit: a report names no driver")
	errReportEvidence = errors.New("testkit: the report's evidence is not container, emulator, managed or inprocess")
	errReportDate     = errors.New("testkit: the report's date is not RFC 3339")
	errReportScenario = errors.New("testkit: a result names no scenario")
	errReportOutcome  = errors.New("testkit: a result's outcome is not pass, fail or skip")
	errReportLevel    = errors.New("testkit: a result's claimed level is not one of the five, nor \"unread\" on a scenario that never reached a client")
	errReportKind     = errors.New("testkit: a result's kind is neither a behaviour nor a guarantee")
)

// CheckClientPin fails t when a driver advertises a client version that
// differs from the module linked into its test binary.
//
// The comparison uses [debug.ReadBuildInfo] so it accounts for module-graph
// version selection rather than only the requirements in go.mod.
// A missing module is reported as a failure rather than skipped.
// Explicit module replacements are skipped because their selected version
// does not identify the advertised dependency.
func CheckClientPin(t *testing.T, c ClientInfo) {
	t.Helper()
	checkClientPin(t, c, debug.ReadBuildInfo)
}

// pinReporter is the part of *testing.T that checkClientPin uses. It exists so
// the branches below have tests of their own: testing.TB cannot be implemented
// outside the testing package, and this function decides whether sixteen
// drivers pass.
type pinReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
}

// checkClientPin is [CheckClientPin] with the build-info source injected.
//
// Every Fatalf is followed by a return: a real *testing.T stops there, and the
// recorder the tests use does not, so without the return a case would run on
// past its own failure and report something else.
func checkClientPin(t pinReporter, c ClientInfo, readBuildInfo func() (*debug.BuildInfo, bool)) {
	t.Helper()
	if c.Module == "" || c.Version == "" {
		t.Fatalf("the driver declares an incomplete client pin: module=%q version=%q", c.Module, c.Version)
		return
	}
	bi, ok := readBuildInfo()
	if !ok || bi == nil {
		t.Fatalf("no build information in this test binary, so the linked client version cannot be read")
		return
	}
	for _, d := range bi.Deps {
		if d.Path != c.Module {
			continue
		}
		linked := d.Version
		// A `replace` makes the selected module something else, and then the
		// version the driver advertises cannot be checked against it -- the
		// replacement's version is a statement about the replacement. Say so
		// rather than fail a pin that may well be right.
		if d.Replace != nil {
			t.Skipf("%s is replaced by %s@%s, so the advertised %s cannot be checked against a selected version",
				c.Module, d.Replace.Path, d.Replace.Version, c.Version)
			return
		}
		if linked != c.Version {
			t.Errorf("this driver advertises %s %s through DriverInfo.ClientVersion, but the binary linked %s.\n"+
				"One of the two is wrong, and the advertised one is what users and the driver pages read.\n"+
				"If the bump is intended, move the constant with it.",
				c.Module, c.Version, linked)
		}
		return
	}
	t.Errorf("the driver advertises %s %s, but %s is not among the %d module(s) this binary linked, so the pair names a client this build does not use",
		c.Module, c.Version, c.Module, len(bi.Deps))
}
