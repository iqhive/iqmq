// Package report defines the machine-readable conformance report produced by
// every driver's conformance run and consumed by confgen to generate the
// capability matrix and capabilities.json.
//
// A claim renders as supported only when the report shows it verified both
// positively (the feature worked) and negatively (the unsupported path
// returned ErrUnsupported or the documented rejection). Evidence gathered
// against an emulator is marked as such and never presented as broker
// evidence.
package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Schema is the report format version.
const Schema = 1

// Evidence classifies how the results were obtained.
type Evidence string

// Evidence values.
const (
	EvidenceContainer Evidence = "container" // real broker in a container
	EvidenceEmulator  Evidence = "emulator"  // vendor emulator (Pub/Sub, LocalStack, ElasticMQ)
	EvidenceManaged   Evidence = "managed"   // real managed service
	EvidenceInProcess Evidence = "inprocess" // the memory driver
)

// Outcome is the result of one scenario.
type Outcome string

// Outcome values.
const (
	Pass Outcome = "pass"
	Fail Outcome = "fail"
	Skip Outcome = "skip"
)

// Kind says what sort of claim a scenario's outcome is evidence about: a
// behaviour the driver can refuse, or a guarantee about every run that one run
// can only ever break. See testkit.Kind, whose spelling this mirrors.
type Kind string

// Kind values.
const (
	KindBehaviour Kind = ""          // a call that can be refused; two-sided evidence
	KindGuarantee Kind = "guarantee" // a promise about every run; one-sided evidence
)

// State is what a report's evidence says about one capability claim, and it
// exists because the generated matrix used to render four different answers
// with one glyph: a refusal the run observed, a claim the driver advertises
// that nothing probed, a denial nothing could probe, and a scenario that
// failed. Eighteen drivers advertise DurableStorage natively and the matrix
// said "—" for all of them, which reads as "no driver here is durable".
type State string

// State values, in the order a reader of the matrix cares about them.
const (
	// StateFailed is a claim one of whose scenarios FAILED. It is never
	// rendered as support, whatever the driver advertises.
	StateFailed State = "failed"
	// StateVerified is an advertised claim proven in both directions.
	StateVerified State = "verified"
	// StateRefuted is a denial the run observed: the driver says it does
	// not do this and the probe saw it refuse. It is the strongest thing
	// a negative claim can be.
	StateRefuted State = "refuted"
	// StateUnrefuted is a denied GUARANTEE that no run contradicted.
	// the one-sided evidence: a driver that truthfully says it does
	// not order messages can never prove it.
	StateUnrefuted State = "unrefuted"
	// StateAdvertised is a claim the driver advertises and no probe
	// verified. It is a claim without evidence, which the specification does not let
	// the matrix present as support.
	StateAdvertised State = "advertised"
	// StateUnverified is a denial nothing established either: no probe
	// exists, or the probe could not run.
	StateUnverified State = "unverified"
	// StateUnread is a claim whose LEVEL the run never read: every scenario
	// about the feature returned before it had a client, because the driver
	// documented it as inapplicable or refused the topology it needs. It is
	// not a denial and not an advertisement -- the run cannot say which --
	// and rendering it as either is what an empty level used to do.
	StateUnread State = "unread"
)

// The three scenarios the kit runs per capability, by the suffix of the
// scenario name the report carries.
//
// ScenarioClaim is the one that is NOT behavioural evidence: it holds the specification's
// rules about the CLAIM -- a level that exists, a condition where one is
// required -- and it passes for a feature the kit cannot probe at all. Nine
// features in this repository have no behavioural probe and their `claim`
// scenario passes for every driver that advertises them, so counting that
// pass as evidence is what would let an unprobed claim render as support.
const (
	ScenarioWorks   = "/works"
	ScenarioClaim   = "/claim"
	ScenarioRefused = "/refused"
)

// CapabilityPrefix is the scenario-name prefix of the per-capability
// scenarios: `capability/<Feature>/<direction>`.
const CapabilityPrefix = "capability/"

// FeatureOfScenario returns the feature a per-capability scenario's NAME
// names, and whether the name has that shape.
//
// It exists because a scenario that never got a client carries no Feature
// field at all -- the kit sets it after Open succeeds -- so 70 of the
// committed corpus's capability results are filed against no feature and
// their skip reason is lost. The name still says which claim they are about.
func FeatureOfScenario(scenario string) (string, bool) {
	if !strings.HasPrefix(scenario, CapabilityPrefix) {
		return "", false
	}
	rest := scenario[len(CapabilityPrefix):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return "", false
	}
	switch rest[i:] {
	case ScenarioWorks, ScenarioClaim, ScenarioRefused:
		return rest[:i], true
	}
	return "", false
}

// Level mirrors the capability support levels of the root package as strings
// so the report does not depend on the API.
type Level string

// Level values.
const (
	LevelUnsupported      Level = "unsupported"
	LevelConditional      Level = "conditional"
	LevelEmulatedVolatile Level = "emulated-volatile"
	LevelEmulatedDurable  Level = "emulated-durable"
	LevelNative           Level = "native"
)

// LevelUnread is NOT one of the levels: it is testkit's explicit record that
// a scenario about a feature returned before it had a client -- a documented
// skip, a topology the driver refused, an Open that failed -- so the level the
// driver advertises was never read. testkit.LevelUnread is the writer's
// spelling of it and says why neither an empty level nor a substituted one
// will do.
const LevelUnread Level = "unread"

// Result is one scenario result.
type Result struct {
	// Scenario is the conformance scenario name.
	Scenario string `json:"scenario"`
	// Feature is the capability the scenario exercises, empty for general
	// scenarios.
	Feature string `json:"feature,omitempty"`
	// Claimed is the level the driver advertises for Feature, or
	// LevelUnread when the scenario never reached a client to read it. It is
	// never empty on a result that names a feature.
	Claimed Level `json:"claimed,omitempty"`
	// Positive reports whether the scenario verified the feature works
	// (true) or verified the unsupported/rejection path (false).
	Positive bool `json:"positive"`
	// Kind says what sort of claim the outcome is evidence about, empty for
	// a behaviour.
	Kind Kind `json:"kind,omitempty"`
	// Outcome is pass, fail or skip.
	Outcome Outcome `json:"outcome"`
	// Note carries a short reason for skips and failures.
	Note string `json:"note,omitempty"`
	// Duration of the scenario.
	Duration time.Duration `json:"duration_ns,omitempty"`
}

// Broker identifies the broker under test.
type Broker struct {
	// Image is what the run was made against, as the driver's testkit
	// harness declared it: the container image reference the conformance
	// harness started -- repository and tag, or the digest
	// conformance/harness/images.lock pins -- or a placeholder for a
	// driver that starts no container, which is why the memory driver's
	// committed report carries "in-process" here.
	Image string `json:"image"`
	// Version is the broker version the run recorded. For a containerised
	// broker the harness derives it from the image reference's TAG (see
	// harness.Version) rather than asking the running broker, so it says
	// which image was pulled and not which build answered. It is empty
	// when there is no tag to read one from, and an absent Version means
	// "not recorded" rather than any particular version.
	Version string `json:"version,omitempty"`
}

// Client identifies the native client library.
type Client struct {
	// Module is the client library's module path -- github.com/twmb/franz-go,
	// github.com/nats-io/nats.go -- as the driver's testkit harness names
	// it. A driver with no third-party client names the package it does use
	// instead, which is how the memory driver's report names
	// internal/engine.
	Module string `json:"module"`
	// Version is the module version that library was at. Every driver
	// supplies it as a constant in its own testing.go rather than reading
	// it out of the build, so it is what the driver author last recorded --
	// and it is empty for a driver whose client is part of this repository
	// and has no module version of its own.
	Version string `json:"version"`
}

// Report is one conformance run.
type Report struct {
	// Schema is the report format's own version. Validate refuses any
	// value but the Schema constant, so a reader that does not know the
	// format fails on the file instead of reading its fields as though it
	// did.
	Schema int `json:"schema"`
	// Driver is the driver's name, and Profile distinguishes runs of that
	// driver against different topologies or client libraries
	// ("artemis-stomp", "fifo", "keyshared"); Profile is empty for a driver
	// that ships one. The pair is what Key and FileName are built from, and
	// confgen refuses a committed report whose file name is not the one the
	// pair produces -- so the pair is the report's identity rather than a
	// label on it.
	Driver  string `json:"driver"`
	Profile string `json:"profile,omitempty"`
	// Broker is what the run was made against, and Client the native
	// client library the driver was linked to. Both are recorded by the
	// driver's own harness, and both are provenance: a reader compares two
	// reports by them, and nothing here reads them as an input.
	Broker Broker `json:"broker"`
	Client Client `json:"client"`
	// Go is the toolchain the run was built with, as runtime.Version
	// reported it inside the run.
	Go string `json:"go"`
	// Date is when the run STARTED -- the kit stamps it before the first
	// scenario -- as an RFC 3339 timestamp in UTC. Validate refuses a
	// report whose Date does not parse.
	Date string `json:"date"`
	// Commit is the revision the run was measured at, read out of
	// IQMQ_COMMIT. hack/conformance.sh and `make conformance-memory` set
	// that from `git rev-parse HEAD` and fall back to the literal
	// "unknown" when git cannot answer, so an EMPTY Commit means a harness
	// run outside those with the variable unset -- not a tree with no
	// revision. It ties committed evidence back to the source that
	// produced it; nothing in the tree yet compares it with anything.
	Commit string `json:"commit,omitempty"`
	// CommitStatus is "unavailable" when the recorded source revision is absent.
	CommitStatus string `json:"commit_status,omitempty"`
	// Evidence says HOW the results were obtained: a real broker in a
	// container, a vendor emulator, a real managed service, or in process.
	// It is the field that stops emulator evidence being presented as
	// broker evidence, so Validate refuses a report carrying any other
	// value -- including the empty one.
	Evidence Evidence `json:"evidence"`
	// Limits are the limits the driver advertised, keyed by the field names
	// of the root package's Limits. An unadvertised limit is ABSENT rather
	// than zero, which the specification makes a different answer.
	Limits map[string]int64 `json:"limits,omitempty"`
	// Results is one entry per scenario the run reached, in the order the
	// suite ran them. A scenario that was never reached is ABSENT here,
	// which is a different answer from one that ran and skipped: Verify
	// can say why a skip left a claim unverified and has nothing at all to
	// say about a scenario that is missing.
	Results []Result `json:"results"`
}

// Key identifies the driver/profile pair.
func (r *Report) Key() string {
	if r.Profile == "" {
		return r.Driver
	}
	return r.Driver + "/" + r.Profile
}

// Validate checks structural invariants.
func (r *Report) Validate() error {
	if r.Schema != Schema {
		return fmt.Errorf("report: schema %d, want %d", r.Schema, Schema)
	}
	if r.Driver == "" {
		return errors.New("report: driver is required")
	}
	switch r.Evidence {
	case EvidenceContainer, EvidenceEmulator, EvidenceManaged, EvidenceInProcess:
	default:
		return fmt.Errorf("report: invalid evidence %q", r.Evidence)
	}
	if _, err := time.Parse(time.RFC3339, r.Date); err != nil {
		return fmt.Errorf("report: invalid date %q", r.Date)
	}
	for i, res := range r.Results {
		if res.Scenario == "" {
			return fmt.Errorf("report: result %d has no scenario", i)
		}
		switch res.Outcome {
		case Pass, Fail, Skip:
		default:
			return fmt.Errorf("report: result %q has invalid outcome %q", res.Scenario, res.Outcome)
		}
		switch res.Kind {
		case KindBehaviour, KindGuarantee:
		default:
			return fmt.Errorf("report: result %q has invalid kind %q", res.Scenario, res.Kind)
		}
		if res.Feature != "" {
			switch res.Claimed {
			case LevelUnsupported, LevelConditional, LevelEmulatedVolatile, LevelEmulatedDurable, LevelNative:
			case LevelUnread:
				// A scenario that passed had a client, so its level
				// WAS read; "unread" on a pass is a kit that lost it.
				if res.Outcome == Pass {
					return fmt.Errorf("report: result %q passed and says its claimed level was %q; a scenario cannot pass without the client the level is read from", res.Scenario, res.Claimed)
				}
			default:
				// Including the empty level: a result about a
				// feature says what the driver claimed, or says
				// explicitly that it never read it.
				return fmt.Errorf("report: result %q has invalid claimed level %q", res.Scenario, res.Claimed)
			}
		}
	}
	return nil
}

// Verified reports, per feature, whether the claim was proven both
// positively and negatively (or, for unsupported claims, negatively).
type Verified struct {
	// Feature is the capability the claim is about, spelled as the driver
	// advertises it in its Capabilities and as the results that mention it
	// spell it. Verify returns one Verified per distinct Feature the run's
	// results named, sorted by this field.
	Feature string
	// Claimed is the level the driver advertises for Feature. Verify takes
	// it from a result that actually READ a level, not from whichever
	// result came first, and leaves LevelUnread here when no scenario
	// about the feature ever reached a client to read one.
	Claimed Level
	// Positive reports whether the feature's own probe,
	// `capability/<F>/works`, passed, and Negative whether its own
	// negative scenario passed: `capability/<F>/refused`, which exercises
	// the refusal (ErrUnsupported, or the documented rejection), or
	// `capability/<F>/claim` for an advertised feature. Another scenario
	// tagged with the feature checks a neighbouring property, so its pass
	// counts for neither, and its failure still sets Failed. The specification wants
	// both directions before a claim may render as support, which is what
	// Proven asks for; the two are separate fields because a claim with
	// only one of them is the common case and not an error.
	Positive bool
	Negative bool
	// Failed reports whether any scenario for the feature FAILED. It
	// outranks everything else the run saw: Proven is false and State is
	// StateFailed whatever the driver advertises and whatever else passed.
	Failed bool
	// Guarantee reports whether the feature is a promise about every run
	// rather than a call that can be refused, in which case a denial no
	// run has broken stays unproven for ever rather than being work to do.
	Guarantee bool
	// Probed reports whether the run produced BEHAVIOURAL evidence about
	// the claim: a pass of its own `works` or `refused` scenario, or a
	// failure of any scenario that exercised it. A pass of another
	// scenario tagged with the feature is not evidence about the claim,
	// as Positive says. The `capability/<F>/claim` scenario is excluded
	// deliberately -- it checks the claim's own well-formedness and passes
	// for a feature nothing can probe -- so Probed is what separates "the
	// driver advertises this and nobody checked" from "the driver
	// advertises this and it works".
	Probed bool
	// Note is the reason the run gives for an unverified claim, taken from
	// the scenarios that skipped. It is what lets the driver page say WHY
	// a claim is unverified instead of printing "false".
	Note string
}

// Verify reports the verification state of every claimed feature.
func (r *Report) Verify() []Verified {
	byFeature := map[string]*Verified{}
	skips := map[string][]Result{}
	for _, res := range r.Results {
		feature := res.Feature
		named, ok := FeatureOfScenario(res.Scenario)
		if feature == "" {
			// A result the kit wrote before it had a client carries
			// no Feature. Its scenario NAME still says which claim
			// it is about, and its skip reason is the only record
			// of why that claim is unverified, so it is attributed
			// by name -- for the reason alone, never as evidence:
			// the outcome of a scenario that never ran cannot
			// prove or refute anything.
			if ok && res.Outcome == Skip && res.Note != "" {
				skips[named] = append(skips[named], res)
			}
			continue
		}
		v := byFeature[feature]
		if v == nil {
			v = &Verified{Feature: feature}
			byFeature[feature] = v
		}
		// The level comes from a result that READ one. Taking it from
		// whichever result came first erased a proven native claim
		// when the run's first scenario for it was a topology refusal.
		if res.Claimed != "" && res.Claimed != LevelUnread {
			v.Claimed = res.Claimed
		}
		if res.Kind == KindGuarantee {
			v.Guarantee = true
		}
		behavioural := !strings.HasSuffix(res.Scenario, ScenarioClaim)
		switch res.Outcome {
		case Pass:
			// Only the feature's own capability scenarios prove
			// it. Every other scenario tagged with the feature
			// checks a neighbouring property, and its pass is no
			// evidence that the feature works:
			// dlq/failed-publish-never-acks, about a Reject with NO
			// destination, used to render a dead-letter route
			// "verified by a probe" whose own probe had skipped.
			if !ok || named != feature {
				break
			}
			switch {
			case res.Positive:
				v.Positive = true
			default:
				v.Negative = true
			}
			v.Probed = v.Probed || behavioural
		case Fail:
			// Any scenario about the feature can refute it.
			v.Failed = true
			v.Probed = v.Probed || behavioural
		case Skip:
			if res.Note != "" {
				skips[feature] = append(skips[feature], res)
			}
		}
	}
	// The reason a claim is unverified comes from the scenarios whose
	// DIRECTION applies to it, and only those. `capability/<F>/refused` is
	// gated on the driver NOT advertising the feature, so for a driver
	// that advertises it the scenario skips with "the capability gate does
	// not select this driver" -- which says nothing about why the claim is
	// unverified and, appended to the real reason, reads as though it did.
	for f, v := range byFeature {
		if v.Claimed == "" {
			// No scenario about the feature reached a client, so
			// the run never read the driver's level for it.
			v.Claimed = LevelUnread
		}
		var reasons []string
		seen := map[string]bool{}
		for _, res := range skips[f] {
			if !applies(res.Scenario, v.Claimed) || seen[res.Note] {
				continue
			}
			// Once each: an unread claim's three scenarios usually
			// share one reason -- the topology they all needed.
			seen[res.Note] = true
			reasons = append(reasons, res.Note)
		}
		v.Note = strings.Join(reasons, "; ")
	}
	out := make([]Verified, 0, len(byFeature))
	for _, v := range byFeature {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Feature < out[j].Feature })
	return out
}

// Unrefuted reports whether the claim is one no single run can establish: a
// guarantee the driver does not promise and that nothing in the run broke.
func (v Verified) Unrefuted() bool {
	return v.Guarantee && !v.Failed && !v.Negative && v.Claimed == LevelUnsupported
}

// applies reports whether a scenario's direction is one the claimed level can
// be asked about: a driver that advertises a feature is never asked to refuse
// it, and a driver that denies one is never asked to demonstrate it. A claim
// whose level was never read has no direction to rule out, so every reason
// applies to it.
func applies(scenario string, claimed Level) bool {
	if claimed == LevelUnread {
		return true
	}
	denied := claimed == LevelUnsupported
	switch {
	case strings.HasSuffix(scenario, ScenarioRefused):
		return denied
	case strings.HasSuffix(scenario, ScenarioWorks), strings.HasSuffix(scenario, ScenarioClaim):
		return !denied
	default:
		return true
	}
}

// State classifies the claim, which is what the generated artefacts render.
// The seven states partition the space: every claim is in exactly one.
func (v Verified) State() State {
	switch {
	case v.Failed:
		return StateFailed
	case v.Claimed == LevelUnread:
		// Before Proven, although an unread claim can never be proven
		// (nothing passed without a client): an empty level fell through
		// to StateAdvertised here and rendered a claim the driver never
		// made.
		return StateUnread
	case v.Proven():
		if v.Claimed == LevelUnsupported {
			return StateRefuted
		}
		return StateVerified
	case v.Unrefuted():
		return StateUnrefuted
	case v.Claimed == LevelUnsupported:
		return StateUnverified
	default:
		return StateAdvertised
	}
}

// Proven reports whether a claim is rendered as supported: never when a
// scenario failed; unsupported claims need the negative path proven;
// supported claims need both the positive and the negative path proven.
//
// The rule is unchanged for a guarantee; Unrefuted is what says why one is not
// proven.
func (v Verified) Proven() bool {
	if v.Failed {
		return false
	}
	if v.Claimed == LevelUnsupported {
		return v.Negative
	}
	return v.Positive && v.Negative
}

// Write encodes the report as indented JSON.
func Write(w io.Writer, r *Report) error {
	if err := r.Validate(); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// Read decodes and validates a report.
func Read(rd io.Reader) (*Report, error) {
	var r Report
	dec := json.NewDecoder(io.LimitReader(rd, 64<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

// Parse decodes a report from bytes.
func Parse(data []byte) (*Report, error) { return Read(strings.NewReader(string(data))) }

// FileName returns the conventional file name for a report.
func (r *Report) FileName() string {
	return strings.ReplaceAll(r.Key(), "/", "-") + ".json"
}

// Save writes the report into dir under its conventional name.
func (r *Report) Save(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(dir, r.FileName())
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	if err := Write(f, r); err != nil {
		_ = f.Close()
		return "", err
	}
	return path, f.Close()
}

// LoadDir reads every *.json report in dir, keeping the newest per key.
func LoadDir(dir string) ([]*Report, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	newest := map[string]*Report{}
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			return nil, err
		}
		r, err := Parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m, err)
		}
		if cur, ok := newest[r.Key()]; !ok || r.Date > cur.Date {
			newest[r.Key()] = r
		}
	}
	out := make([]*Report, 0, len(newest))
	for _, r := range newest {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, nil
}
