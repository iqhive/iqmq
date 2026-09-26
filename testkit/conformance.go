package testkit

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Scenario is one conformance check.
//
// A scenario is a function and an error, not a *testing.T: that is what lets the
// same suite run under [RunConformance] and under [Run], and it is what lets the
// kit's own tests assert that a scenario fails for a driver that deserves it.
type Scenario struct {
	// Name is the scenario's name, which is also its key in the report and
	// in Harness.Skip. Names are stable: a driver's matrix entry is keyed by
	// them.
	Name string
	// Feature is the capability the scenario exercises. A scenario with no
	// feature has Has false and contributes to no claim.
	Feature mq.Feature
	// Has reports whether Feature is meaningful.
	Has bool
	// Positive reports whether the scenario verifies that the feature works
	// (true) or that the unsupported path is refused (false).
	Positive bool
	// Kind says what sort of claim the scenario is evidence about. The zero
	// value, [KindBehaviour], is a call the driver can refuse, which one
	// run settles in both directions. [KindGuarantee] is a promise about
	// every run, which one run can only ever break — and which the kit
	// therefore handles asymmetrically.
	Kind Kind
	// Gate decides whether the scenario applies to these capabilities. Nil
	// means it always does.
	Gate func(caps mq.Capabilities) bool
	// Shape adjusts the topology the scenario needs before the client is
	// opened.
	Shape func(n *Need)
	// Run is the check. It returns nil for a pass, a skip from [Skip] for a
	// scenario that could not run, and any other error for a failure.
	Run func(ctx context.Context, e *Env) error
}

// RunConformance runs the whole suite against a driver and fails t for every
// scenario that did not hold.
//
// Each scenario is a subtest, so `go test -run
// TestConformance/publish/roundtrip` runs one. The report is returned so that a
// driver's test can save it for conformance/cmd/confgen, which turns it into
// capabilities.json and the driver matrix.
func RunConformance(t *testing.T, h Harness) *Report {
	t.Helper()
	if err := h.validate(); err != nil {
		t.Fatalf("testkit: %v", err)
	}
	rep := newReport(h, now(h))
	for _, s := range Scenarios() {
		res := Result{Scenario: s.Name, Positive: s.Positive, Kind: s.Kind}
		t.Run(s.Name, func(t *testing.T) {
			var caps mq.Capabilities
			var opened bool
			res, caps, opened = runScenario(t.Context(), h, s)
			if opened && rep.Limits == nil {
				rep.Limits = limitMap(caps)
			}
			switch res.Outcome {
			case OutcomeFail:
				t.Errorf("%s", res.Note)
			case OutcomeSkip:
				t.Skip(res.Note)
			case OutcomePass:
			}
		})
		rep.Results = append(rep.Results, res)
	}
	return rep
}

// Run runs the whole suite and returns the report, without a *testing.T.
//
// It is the entry point the kit's own tests use. A conformance kit that has
// never rejected a driver is assumed not to work, and the only way to assert
// that it rejects one is to run it against a driver that deserves rejection and
// read the report — which a *testing.T-shaped API cannot do, because the failure
// would fail the test doing the asserting.
func Run(ctx context.Context, h Harness) (*Report, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	rep := newReport(h, now(h))
	for _, s := range Scenarios() {
		res, caps, opened := runScenario(ctx, h, s)
		if opened && rep.Limits == nil {
			rep.Limits = limitMap(caps)
		}
		rep.Results = append(rep.Results, res)
	}
	return rep, nil
}

// RunOne runs a single scenario by name and returns its result.
//
// It exists for the kit's own tests and for a driver author bisecting one
// failure: `go test -run Conformance/settle/ack-idempotency` is the same thing
// through the testing package, and this is the form that can be asserted on.
func RunOne(ctx context.Context, h Harness, name string) (Result, error) {
	if err := h.validate(); err != nil {
		return Result{}, err
	}
	for _, s := range Scenarios() {
		if s.Name == name {
			res, _, _ := runScenario(ctx, h, s)
			return res, nil
		}
	}
	return Result{}, errNoSuchScenario
}

// errNoSuchScenario is RunOne being given a name the suite does not have, which
// is a typo in a test rather than a driver defect.
var errNoSuchScenario = errors.New("testkit: no scenario of that name")

// now reads the harness's clock.
func now(h Harness) time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// runScenario opens a client for one scenario, runs it and records the result.
//
// The result is a NAMED return so that the deferred duration lands in it. With
// an unnamed one the defer writes to a local the return value was already
// copied out of, and every scenario reports a duration of zero — which, with
// `omitempty` on the field, is a report that silently carries no timings at
// all.
func runScenario(ctx context.Context, h Harness, s Scenario) (res Result, caps mq.Capabilities, opened bool) {
	res = Result{Scenario: s.Name, Positive: s.Positive, Kind: s.Kind}
	if s.Has {
		// The feature is a property of the SCENARIO, so it is recorded
		// before anything can go wrong. It used to be recorded only
		// after Open succeeded, which filed the two early returns
		// below -- a documented skip and a topology the driver refuses
		// -- against no capability at all: 70 results of the committed
		// conformance corpus, each one the only record of why a claim
		// is unverified, dropped on the floor. The advertised level
		// cannot move with it, because it comes from a client these
		// paths never got; Report.Verify therefore takes the level from
		// a result that has one.
		//
		// So the level starts as LevelUnread, SAID rather than left
		// empty, and is overwritten the moment a client exists. An
		// empty level on these paths was indistinguishable from a kit
		// that lost one, and confgen refused it.
		res.Feature = s.Feature.String()
		res.Claimed = LevelUnread
	}
	if reason, skipped := h.Skip[s.Name]; skipped {
		res.Outcome, res.Note = OutcomeSkip, "the driver documents this scenario as inapplicable: "+reason
		return res, caps, false
	}
	need := Need{Topic: h.topicName(s.Name), Subscriptions: []string{"conformance"}}
	if s.Shape != nil {
		s.Shape(&need)
	}
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()

	sctx, cancel := context.WithTimeout(ctx, h.timeout())
	defer cancel()

	client, err := h.Open(sctx, need)
	if err != nil {
		if errors.Is(err, mq.ErrUnsupported) {
			res.Outcome, res.Note = OutcomeSkip, "the driver cannot build the topology this scenario needs: "+err.Error()
			return res, caps, false
		}
		res.Outcome, res.Note = OutcomeFail, "opening a client: "+err.Error()
		return res, caps, false
	}
	defer func() { _ = client.Close() }()

	caps, opened = client.Capabilities(), true
	if s.Has {
		res.Claimed = LevelOf(caps.Support(s.Feature).Level)
	}
	if s.Gate != nil && !s.Gate(caps) {
		res.Outcome, res.Note = OutcomeSkip, "the capability gate does not select this driver"
		return res, caps, opened
	}
	env := &Env{Harness: h, Client: client, Caps: caps, Need: need, Topic: need.Topic, Sub: need.Sub(0)}
	err = s.Run(sctx, env)
	switch reason, skipped := IsSkip(err); {
	case err == nil:
		res.Outcome = OutcomePass
		if s.Kind == KindGuarantee && s.Positive {
			// A guarantee's positive direction is one-sided
			// evidence and the report says so rather than letting
			// a bare "pass" read as a proof.
			env.Note("this run observed no counter-example, which is the strongest evidence one run can be for a guarantee")
		}
	case skipped:
		res.Outcome, res.Note = OutcomeSkip, reason
	default:
		res.Outcome, res.Note = OutcomeFail, err.Error()
		// Include health after a failed scenario. A timeout says what was
		// missing; health can explain a background fetch or subscription
		// failure that was not returned through Receive. Reading it after
		// the scenario captures the state left by the failure.
		if note := healthNote(client.Health()); note != "" {
			env.Note(note)
		}
	}
	res.Note = withNotes(res.Note, env.notes)
	if env.oneSided {
		// The run found the claim to be one a single run cannot settle,
		// which is a property of the DRIVER rather than of the feature:
		// fan-out has a call on a broker with two subscriptions and none
		// on a broker whose two names are one queue. Kind is per result
		// for exactly this reason, and conformance/report derives
		// "unrefuted" from it -- so a divergence that did not promote
		// the kind would render as "nobody checked this" about a run
		// that checked it and found nothing to refuse. It is why no
		// fourth Outcome was added: the kind already carries the
		// distinction, per result, on both sides of the schema.
		res.Kind = KindGuarantee
	}
	return res, caps, opened
}

// healthNote describes a driver's health for a failing scenario's note, and is
// empty when the driver is Ready.
//
// Empty on Ready is the part worth stating: a scenario can fail for a reason
// that has nothing to do with the driver's health -- a wrong body, a missing
// header, a claim proved in one direction only -- and appending "driver health
// is Ready" to those would be noise on every one of them. The note exists to
// carry a cause the report was otherwise dropping, so it says nothing when
// there is nothing to carry.
func healthNote(hs mq.HealthSnapshot) string {
	if hs.State == mq.HealthReady {
		return ""
	}
	note := "driver health is " + hs.State.String() + " after this failure"
	if hs.Err != nil {
		note += ": " + hs.Err.Error()
	}
	return note
}

// withNotes folds a scenario's own notes into the report's note field.
//
// The outcome's reason comes first, because that is what a failing CI log has
// to lead with; the notes are what the scenario recorded on the way past, and
// on a pass they are all there is.
func withNotes(reason string, notes []string) string {
	if len(notes) == 0 {
		return reason
	}
	joined := strings.Join(notes, "; ")
	if reason == "" {
		return joined
	}
	return reason + " (" + joined + ")"
}

// limitMap is the driver's advertised limits, in the shape the report carries:
// one entry per limit the driver ADVERTISES, and nothing at all for one it
// does not.
//
// The specification makes "unknown" and "advertised as zero" different answers, and the map
// keeps them different: a reader of the report cannot mistake an absent key for
// a bound of zero, which is the mistake that would turn a broken limit lookup
// into a driver that appears to forbid everything.
func limitMap(caps mq.Capabilities) map[string]int64 {
	out := map[string]int64{}
	for k := range mq.LimitKeyCount {
		key := mq.LimitKey(k)
		if lim := caps.Limit(key); lim.Known {
			out[key.String()] = lim.Value
		}
	}
	return out
}

// Scenarios returns the whole suite, in a stable order.
//
// The order is stable because a driver's matrix entry and a CI log are read
// against it; nothing in the suite depends on it, and every scenario opens its
// own client, so nothing carries over from one to the next.
func Scenarios() []Scenario {
	general := generalScenarios()
	caps := capabilityScenarios()
	out := make([]Scenario, 0, len(general)+len(caps))
	out = append(out, general...)
	out = append(out, caps...)
	return out
}

// claims reports whether a feature is advertised at any level.
func claims(f mq.Feature) func(mq.Capabilities) bool {
	return func(c mq.Capabilities) bool { return c.Has(f) }
}

// lacks reports whether a feature is not advertised.
func lacks(f mq.Feature) func(mq.Capabilities) bool {
	return func(c mq.Capabilities) bool { return !c.Has(f) }
}

// violationError is a probe reporting that it OBSERVED a guarantee break.
//
// It is distinct from an ordinary failure because the two directions read it
// oppositely: a driver that CLAIMS the guarantee is refuted by it, and a
// driver that DENIES the guarantee is confirmed by it. An ordinary error means
// neither — it means the probe could not run.
type violationError struct{ what string }

// Error implements error.
func (v *violationError) Error() string { return v.what }

// violation is what a guarantee's probe returns when it sees a counter-example,
// which is the only observation a single run can make about a promise.
func violation(format string, args ...any) error {
	return &violationError{what: failf(format, args...).Error()}
}

// isViolation reports whether err is an observed counter-example, and returns
// what was observed.
func isViolation(err error) (string, bool) {
	var v *violationError
	if errors.As(err, &v) {
		return v.what, true
	}
	return "", false
}

// divergenceError is a probe reporting that the driver ACCEPTED the feature's
// own call and then did something other than the feature.
//
// It is the third thing a negative probe can see, and until this increment the
// kit had no word for it. "The driver says it cannot do this; prove it
// refuses" has an answer only where there is a call to refuse. A queue broker
// accepts two subscription NAMES and makes them one subscription; a broker
// that deletes a settled message accepts a second reader and has nothing to
// give it; a broker with no batching accepts a Receive with room for eight and
// returns one. Nothing is refused, the call succeeds, and the difference shows
// up only in what arrives -- so the probe can neither report a refusal nor
// report the feature working.
type divergenceError struct{ what string }

// Error implements error.
func (d *divergenceError) Error() string { return d.what }

// Diverged is what a probe returns when the driver neither provided the
// feature nor refused it: the call was accepted and something else happened,
// and this says what.
//
// The two directions read it oppositely, which is why it is named rather than
// returned as a bare error. A driver that CLAIMS the feature is failed by it,
// because it advertised the feature and did something else. A driver that
// DENIES the feature is neither refuted nor confirmed by it: the denial is
// true as far as this run can tell, and there was no call for the driver to
// refuse, so the claim is left UNREFUTED the way a guarantee's is -- which is
// what a single run can honestly say, and what replaces a scenario that failed
// three drivers for denials that were true.
func Diverged(format string, args ...any) error {
	return &divergenceError{what: failf(format, args...).Error()}
}

// isDivergence reports whether err is an accepted call that did something
// else, and returns what was observed.
func isDivergence(err error) (string, bool) {
	var d *divergenceError
	if errors.As(err, &d) {
		return d.what, true
	}
	return "", false
}

// refusalError marks an answer as coming from the call a driver that lacks the
// feature has to refuse.
//
// Attribution is the whole of it. A probe is five or six portable calls -- open
// a consumer, publish, receive, settle, close -- and exactly one of them is the
// feature's. The negative direction used to read the WHOLE probe's error and
// pass on an ErrUnsupported from any of them, so a driver that could not open a
// consumer at all had every one of its negative claims recorded as "refuted by
// observation" with the feature's own call never made.
type refusalError struct {
	// at names the call, in the portable API's own spelling, because the
	// name is published in the report and read by somebody deciding
	// whether the evidence is about the feature.
	at string
	// err is the driver's answer, which still has to BE a refusal: the specification
	// allows ErrUnsupported and the specification allows ErrNotSettleable, and anything
	// else is a driver that did something other than refuse.
	err error
}

// Error implements error, naming the call so that a failing CI log says which
// answer came from where.
func (r *refusalError) Error() string { return r.at + ": " + r.err.Error() }

// Unwrap keeps the sentinel and the portable code reachable.
func (r *refusalError) Unwrap() error { return r.err }

// refusedAt marks err as the answer of the one call the negative direction is
// about. A nil answer, a skip, a violation and a divergence pass through
// unchanged: each of those is already a classified observation, and wrapping
// one would claim it came from a refusal.
func refusedAt(what string, err error) error {
	if err == nil {
		return nil
	}
	if _, skipped := IsSkip(err); skipped {
		return err
	}
	if _, broke := isViolation(err); broke {
		return err
	}
	if _, diverged := isDivergence(err); diverged {
		return err
	}
	return &refusalError{at: what, err: err}
}

// isRefusal reports whether err came from a marked call, and returns it.
func isRefusal(err error) (*refusalError, bool) {
	var r *refusalError
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// failf is the scenario failure constructor. Scenarios are not a hot path, so
// formatting one is free.
func failf(format string, args ...any) error {
	var b strings.Builder
	rest := args
	for len(format) > 0 {
		i := strings.IndexByte(format, '%')
		if i < 0 || i == len(format)-1 {
			b.WriteString(format)
			break
		}
		b.WriteString(format[:i])
		verb := format[i+1]
		format = format[i+2:]
		if verb == '%' {
			b.WriteByte('%')
			continue
		}
		if len(rest) == 0 {
			b.WriteString("%!(MISSING)")
			continue
		}
		b.WriteString(render(rest[0]))
		rest = rest[1:]
	}
	return errors.New(b.String())
}

// render turns one argument into text without reaching for fmt, which the
// repository's lint rules keep off every path a driver might inherit.
func render(v any) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return x
	case error:
		if x == nil {
			return "<nil>"
		}
		return x.Error()
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case []byte:
		return strconv.Quote(string(x))
	case time.Duration:
		return x.String()
	case mq.ErrorCode:
		return x.String()
	case mq.Feature:
		return x.String()
	case mq.Level:
		return x.String()
	case Level:
		return string(x)
	case Outcome:
		return string(x)
	case mq.Support:
		return "{" + x.Level.String() + " " + x.Scope.String() + " " + strconv.Quote(x.Note) + "}"
	default:
		return "<value>"
	}
}
