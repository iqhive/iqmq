package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// topoScheme is the scheme the admin harness registers. It is not a driver's
// scheme: two invocations have to reach ONE broker for a "plan changed
// nothing" assertion to mean anything, and memory:// deliberately cannot do
// that.
const topoScheme = "topo"

// newTopoHarness registers a scheme over one memory broker with AutoCreate
// OFF, and returns the broker so a test can look at what the CLI did to it.
//
// AutoCreate is off on purpose: with it on, a topic appears the moment
// anything mentions one, and "plan created nothing" would be unfalsifiable.
// With it off, EnsureTopic is the ONLY way a topic can come into existence,
// which is what makes the plan-versus-apply distinction observable.
func newTopoHarness(t *testing.T) (*harness, *memory.Broker) {
	t.Helper()
	broker, err := memory.NewBroker(memory.Config{AutoCreate: false})
	if err != nil {
		t.Fatalf("memory.NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	reg := dsn.NewRegistry()
	if err := reg.Register(topoScheme, func(ctx context.Context, _ string) (*mq.Client, error) {
		return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
	}); err != nil {
		t.Fatalf("registering %q: %v", topoScheme, err)
	}
	return harnessWith(t, reg, nil), broker
}

// brokerTopics is what the broker holds, read through its own Admin surface
// rather than through the CLI: a test that checked the CLI's output against
// the CLI's own reading of the broker would agree with itself.
func brokerTopics(t *testing.T, b *memory.Broker) []string {
	t.Helper()
	client, err := memory.OpenWithClient(context.Background(), b, memory.ClientBorrowed)
	if err != nil {
		t.Fatalf("opening a client over the broker: %v", err)
	}
	defer func() { _ = client.Close() }()
	admin, err := client.Admin()
	if err != nil {
		t.Fatalf("the memory broker has no admin surface: %v", err)
	}
	var out []string
	for name, err := range admin.ListTopics(context.Background()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		out = append(out, name)
	}
	return sortedStrings(out)
}

// TestTopologyPlanChangesNothing is the property that makes `plan` worth
// having: it prints what apply would do and does none of it.
func TestTopologyPlanChangesNothing(t *testing.T) {
	h, broker := newTopoHarness(t)
	code := h.run("topology", "plan", topoScheme+"://",
		"--topic", "orders", "--subscription", "orders=billing")
	if code != exitOK {
		t.Fatalf("topology plan = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitOK, h.stdout(), h.stderr())
	}
	for _, want := range []string{"topic orders", "subscription orders/billing"} {
		if !strings.Contains(h.stdout(), want) {
			t.Errorf("the plan does not name %q:\n%s", want, h.stdout())
		}
	}
	if got := brokerTopics(t, broker); len(got) != 0 {
		t.Errorf("topology plan created %v on the broker; plan must change nothing", got)
	}

	// The rows themselves out of the machine-readable form: see planJSON for
	// why a substring of the text form cannot carry this claim.
	h2 := harnessWith(t, h.env.registry, nil)
	rep := planJSON(t, h2, "plan", topoScheme+"://",
		"--topic", "orders", "--subscription", "orders=billing")
	for _, o := range rep.Objects {
		if o.Before != existAbsent.String() || o.Action != actionCreate {
			t.Errorf("%s %q/%q: before=%q action=%q, want absent/%s",
				o.Kind, o.Topic, o.Name, o.Before, o.Action, actionCreate)
		}
	}
	if got := brokerTopics(t, broker); len(got) != 0 {
		t.Errorf("the -o json plan created %v on the broker", got)
	}
}

// TestTopologyApplyEnsuresAndVerifies covers the apply path and the re-read.
func TestTopologyApplyEnsuresAndVerifies(t *testing.T) {
	h, broker := newTopoHarness(t)
	code := h.run("topology", "apply", topoScheme+"://",
		"--topic", "orders", "--subscription", "orders=billing")
	if code != exitOK {
		t.Fatalf("topology apply = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitOK, h.stdout(), h.stderr())
	}
	if n := countRowResults(h.stdout(), resultEnsured); n != 2 {
		t.Errorf("%d object row(s) reported %q, want 2:\n%s", n, resultEnsured, h.stdout())
	}
	if got := brokerTopics(t, broker); len(got) != 1 || got[0] != "orders" {
		t.Errorf("the broker holds %v, want [orders]", got)
	}
}

// TestTopologyApplyIsIdempotent: Ensure means Ensure, so a second apply is a
// no-op that reports "no-change" rather than a failure.
func TestTopologyApplyIsIdempotent(t *testing.T) {
	h, broker := newTopoHarness(t)
	if code := h.run("topology", "apply", topoScheme+"://", "--topic", "orders"); code != exitOK {
		t.Fatalf("the first apply = %d\nstderr:\n%s", code, h.stderr())
	}
	h2 := harnessWith(t, h.env.registry, nil)
	if code := h2.run("topology", "apply", topoScheme+"://", "--topic", "orders"); code != exitOK {
		t.Fatalf("the second apply = %d\nstderr:\n%s", code, h2.stderr())
	}
	if !strings.Contains(h2.stdout(), actionNoChange) {
		t.Errorf("the second apply did not report %q:\n%s", actionNoChange, h2.stdout())
	}
	if got := brokerTopics(t, broker); len(got) != 1 {
		t.Errorf("two applies left %v; Ensure is meant to be idempotent", got)
	}
}

// planJSON runs one invocation with -o json and returns the parsed report.
//
// Every assertion about a ROW goes through this rather than through
// strings.Contains on the text form, and the reason is a check of this file's
// own that could not fail: the note under the table contains the sentence
// "makes the answer unknown rather than absent", so
// strings.Contains(out, "unknown") and strings.Contains(out, "absent") are
// BOTH true of every plan this command prints, whatever the rows say. A
// mutation that made an unsupported Stats read as "absent" left the test
// passing. The text renderer is still covered -- countRowResults anchors on
// the end of a row line -- but a claim about one object's state is read out of
// the machine-readable form, where a field is a field.
func planJSON(t *testing.T, h *harness, args ...string) topologyReport {
	t.Helper()
	full := append([]string{"topology"}, args...)
	full = append(full, "-o", "json")
	if code := h.run(full...); code != exitOK {
		t.Fatalf("%v = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			full, code, exitOK, h.stdout(), h.stderr())
	}
	var rep topologyReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	return rep
}

// objectByName finds one row of a report.
func objectByName(t *testing.T, rep *topologyReport, kind, topic, name string) topoRow {
	t.Helper()
	for i := range rep.Objects {
		if o := &rep.Objects[i]; o.Kind == kind && o.Topic == topic && o.Name == name {
			return *o
		}
	}
	t.Fatalf("no %s row for %q/%q in %+v", kind, topic, name, rep.Objects)
	return topoRow{}
}

// countRowResults counts the OBJECT lines whose result is exactly want.
//
// It matches on the end of a line rather than anywhere in the output, because
// the prose above the table says "each object was ensured and then re-read"
// and a plain strings.Count of "ensured" therefore returns one more than there
// are objects. That was this test's first reading and it was the test that was
// wrong, not the command.
func countRowResults(out, want string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasSuffix(strings.TrimRight(line, " "), want) && strings.HasPrefix(line, "  ") {
			n++
		}
	}
	return n
}

// fakeAdmin is an Admin whose every answer a test chooses.
//
// It exists for the three answers no shipped driver gives on demand: an
// EnsureTopic that succeeds and creates nothing, a Stats that is unsupported,
// and an Admin surface that a capability table contradicts. Each of those is a
// case the CLI has to report as a defect in the driver, and a check for it
// that could only be exercised by a broken driver would never run at all.
type fakeAdmin struct {
	topics       []string
	ensureErr    error
	ensureHides  bool
	statsErr     error
	subscription bool
	// noFigures makes Stats answer every figure unknown and no error, which
	// is what the Kafka drivers and Pulsar answer for a subscription that
	// does not exist as well as for some that do.
	noFigures bool
	// flatten records a topic the way drivers/sqs names its queue, with
	// each "/" written as "-", which is what its ListTopics then reports.
	flatten bool
}

// EnsureTopic records the topic, unless this fake is pretending to.
func (f *fakeAdmin) EnsureTopic(_ context.Context, spec mq.TopicSpec) error {
	if f.ensureErr != nil {
		return f.ensureErr
	}
	switch {
	case f.ensureHides:
	case f.flatten:
		f.topics = append(f.topics, strings.ReplaceAll(spec.Name, "/", "-"))
	default:
		f.topics = append(f.topics, spec.Name)
	}
	return nil
}

// DeleteTopic is never called by this command and says so.
func (f *fakeAdmin) DeleteTopic(context.Context, string) error { return mq.ErrUnsupported }

// ListTopics yields what the fake holds.
func (f *fakeAdmin) ListTopics(context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for _, t := range f.topics {
			if !yield(t, nil) {
				return
			}
		}
	}
}

// EnsureSubscription records the subscription.
func (f *fakeAdmin) EnsureSubscription(_ context.Context, _ mq.SubscriptionSpec) error {
	if f.ensureErr != nil {
		return f.ensureErr
	}
	if !f.ensureHides {
		f.subscription = true
	}
	return nil
}

// DeleteSubscription is never called by this command.
func (f *fakeAdmin) DeleteSubscription(context.Context, string, string) error {
	return mq.ErrUnsupported
}

// Purge is never called by this command.
func (f *fakeAdmin) Purge(context.Context, string, string) error { return mq.ErrUnsupported }

// Stats is the subscription probe, and the answer a test chooses.
func (f *fakeAdmin) Stats(context.Context, string, string) (mq.QueueStats, error) {
	if f.statsErr != nil {
		return mq.QueueStats{}, f.statsErr
	}
	if f.noFigures {
		return mq.QueueStats{}, nil
	}
	if !f.subscription {
		// An *mq.OpError with CodeNotFound, because the root exports
		// ErrTopicNotFound and NO subscription-not-found sentinel: what the
		// CLI keys on is the portable CODE, which is the only thing a driver
		// can be relied on to set (see the maintenance notes, the findings).
		return mq.QueueStats{}, &mq.OpError{
			Driver: "fake", Operation: mq.OpAdmin, Code: mq.CodeNotFound,
		}
	}
	return mq.QueueStats{Depth: mq.Limit[int64]{Known: true}}, nil
}

// fakeBackend is the smallest Backend that can carry an Admin and a capability
// table. It opens no sink and no source: `topology` never publishes and never
// consumes, and a fake that could would be a fake of something else.
type fakeBackend struct {
	admin    mq.Admin
	adminErr error
	caps     mq.Capabilities
}

// Info names the fake.
func (b *fakeBackend) Info() mq.DriverInfo { return mq.DriverInfo{Name: "fake"} }

// Capabilities is the table the test built.
func (b *fakeBackend) Capabilities() mq.Capabilities { return b.caps }

// OpenSink refuses: this backend exists for the topology surface alone.
func (b *fakeBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	return nil, mq.ErrUnsupported
}

// OpenSource refuses, for the same reason.
func (b *fakeBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return nil, mq.ErrUnsupported
}

// Ping succeeds.
func (b *fakeBackend) Ping(context.Context) error { return nil }

// Native offers no escape hatch.
func (b *fakeBackend) Native() any { return nil }

// Close releases nothing.
func (b *fakeBackend) Close() error { return nil }

// Admin is the AdminBackend half.
func (b *fakeBackend) Admin() (mq.Admin, error) {
	if b.adminErr != nil {
		return nil, b.adminErr
	}
	return b.admin, nil
}

// capsWith builds a table in which one feature has one level and everything
// else is unsupported, which is the builder's own default for a feature a
// driver never mentions.
func capsWith(tb testing.TB, f mq.Feature, level mq.Level) mq.Capabilities {
	b := driverkit.NewCapabilities()
	switch level {
	case mq.Native:
		b.Native(f, mq.ScopeTopic)
	case mq.Conditional:
		b.Conditional(f, mq.ScopeTopic, "the test asked for conditional")
	case mq.EmulatedVolatile:
		b.EmulatedVolatile(f, mq.ScopeTopic, "the test asked for emulated")
	case mq.EmulatedDurable:
		b.EmulatedDurable(f, mq.ScopeTopic, "the test asked for emulated-durable")
	case mq.Unsupported:
		b.Unsupported(f)
	}
	return mustBuild(tb, b)
}

// newFakeHarness registers a scheme over one fake backend.
func newFakeHarness(t *testing.T, scheme string, b *fakeBackend) *harness {
	t.Helper()
	reg := dsn.NewRegistry()
	if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
		return mq.NewClient(b), nil
	}); err != nil {
		t.Fatalf("registering %q: %v", scheme, err)
	}
	return harnessWith(t, reg, nil)
}

// TestApplyFailsWhenAnEnsureLeavesNothingVisible is the reason apply re-reads.
//
// EnsureTopic returns nil whether it created the topic or found one, so the
// call's own success is not evidence that anything is there. This fake returns
// nil and creates nothing — the shape of a driver that provisions
// asynchronously, or against the wrong namespace — and apply has to call that
// out rather than print "ensured".
func TestApplyFailsWhenAnEnsureLeavesNothingVisible(t *testing.T) {
	b := &fakeBackend{
		admin: &fakeAdmin{ensureHides: true},
		caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
	}
	h := newFakeHarness(t, "fakeadmin", b)
	code := h.run("topology", "apply", "fakeadmin://host", "--topic", "orders")
	if code != exitFailure {
		t.Fatalf("topology apply = %d, want %d (the ensure was invisible)\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), resultInvisible) {
		t.Errorf("the report does not carry %q:\n%s", resultInvisible, h.stdout())
	}
	if !strings.Contains(h.stderr(), "defect in the driver") {
		t.Errorf("the failure does not say whose defect it is:\n%s", h.stderr())
	}
}

// TestAHierarchicalTopicTheListingFlattensIsNotAbsent: drivers/sqs maps
// "orders/eu" to the queue "orders-eu", and since "orders-eu" maps there too
// its ListTopics can only report the queue as "orders-eu". The topic probe
// looked the portable name up in the listing, so `plan` read an existing
// "orders/eu" as absent and `apply` ensured it, re-read, printed ENSURED BUT
// NOT VISIBLE, blamed the driver and exited 1 on every run. A hierarchical
// name the listing reports with each "/" written as one other byte is
// unknown now, not absent: the portable grammar's "/" is the byte drivers
// write differently. A hierarchical name the listing does report is still
// present, and one nothing like it is listed for is still absent, on a plan
// and on apply's re-read, which a driver whose listing is portable keeps.
func TestAHierarchicalTopicTheListingFlattensIsNotAbsent(t *testing.T) {
	adm := &fakeAdmin{flatten: true}
	b := &fakeBackend{admin: adm, caps: capsWith(t, mq.FeatureAdmin, mq.Native)}
	h := newFakeHarness(t, "flattens", b)
	if code := h.run("topology", "apply", "flattens://host", "--topic", "orders/eu"); code != exitOK {
		t.Fatalf("topology apply of a topic the driver lists as %q = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			adm.topics, code, exitOK, h.stdout(), h.stderr())
	}
	if strings.Contains(h.stdout(), resultInvisible) || strings.Contains(h.stderr(), "defect in the driver") {
		t.Errorf("apply blamed the driver for a listing that names the topic its own way:\n%s\n%s",
			h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), resultUnverified) {
		t.Errorf("apply does not say the topic could not be verified:\n%s", h.stdout())
	}

	p := newFakeHarness(t, "flattens", b)
	rep := planJSON(t, p, "plan", "flattens://host", "--topic", "orders/eu", "--topic", "invoices")
	row := objectByName(t, &rep, "topic", "orders/eu", "")
	if row.Before != existUnknown.String() || row.Action != actionUnknown {
		t.Errorf("a plan reads the topic apply ensured as %q, %q; want %q, %q", row.Before, row.Action,
			existUnknown, actionUnknown)
	}
	if !strings.Contains(row.Detail, "ListTopics") {
		t.Errorf("the row does not say why the answer is unknown: %q", row.Detail)
	}
	if row := objectByName(t, &rep, "topic", "invoices", ""); row.Before != existAbsent.String() {
		t.Errorf("a flat topic the listing does not name is %q, want %q", row.Before, existAbsent)
	}

	// "a/b/c" against "a-b_c" is two bytes for "/", and "refunds/eu" has
	// nothing like it listed: both are absent.
	adm.topics = append(adm.topics, "a-b_c")
	rep = planJSON(t, newFakeHarness(t, "flattens", b), "plan", "flattens://host",
		"--topic", "a/b/c", "--topic", "refunds/eu")
	for _, name := range []string{"a/b/c", "refunds/eu"} {
		if row := objectByName(t, &rep, "topic", name, ""); row.Before != existAbsent.String() {
			t.Errorf("a hierarchical topic nothing like it is listed for, %q, is %q (%s), want %q",
				name, row.Before, row.Detail, existAbsent)
		}
	}

	listed := &fakeBackend{admin: &fakeAdmin{topics: []string{"orders/eu"}}, caps: capsWith(t, mq.FeatureAdmin, mq.Native)}
	rep = planJSON(t, newFakeHarness(t, "lists", listed), "plan", "lists://host", "--topic", "orders/eu")
	if row := objectByName(t, &rep, "topic", "orders/eu", ""); row.Before != existPresent.String() {
		t.Errorf("a hierarchical topic the listing names is %q, want %q", row.Before, existPresent)
	}

	// A driver whose listing is portable and whose Ensure left nothing
	// visible is still a defect, hierarchical name or not.
	hides := &fakeBackend{admin: &fakeAdmin{ensureHides: true}, caps: capsWith(t, mq.FeatureAdmin, mq.Native)}
	hh := newFakeHarness(t, "hides", hides)
	if code := hh.run("topology", "apply", "hides://host", "--topic", "orders/eu"); code != exitFailure ||
		!strings.Contains(hh.stdout(), resultInvisible) {
		t.Errorf("apply of a hierarchical topic the driver left invisible = %d, want %d and %q:\n%s",
			code, exitFailure, resultInvisible, hh.stdout())
	}
}

// TestSubscriptionStateIsUnknownRatherThanAbsent: the specification has no
// ListSubscriptions, so Stats is the only read-only question there is. A
// driver without Stats leaves the answer UNKNOWN, and a plan that rendered
// that as "absent" would be claiming evidence it has not got.
func TestSubscriptionStateIsUnknownRatherThanAbsent(t *testing.T) {
	b := &fakeBackend{
		admin: &fakeAdmin{statsErr: mq.ErrUnsupported},
		caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
	}
	h := newFakeHarness(t, "nostats", b)
	rep := planJSON(t, h, "plan", "nostats://host", "--subscription", "orders=billing")
	row := objectByName(t, &rep, "subscription", "orders", "billing")
	if row.Before != existUnknown.String() {
		t.Errorf("the subscription's state is %q, want %q: an unsupported Stats is "+
			"evidence of nothing and must not read as absence", row.Before, existUnknown)
	}
	if row.Action != actionUnknown {
		t.Errorf("the action is %q, want %q", row.Action, actionUnknown)
	}
	if !strings.Contains(row.Detail, "ListSubscriptions") {
		t.Errorf("the row does not say why the answer is unknown: %q", row.Detail)
	}
	// And the present case, so "unknown" is not simply what this command
	// always answers about a subscription.
	b2 := &fakeBackend{
		admin: &fakeAdmin{subscription: true},
		caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
	}
	h2 := newFakeHarness(t, "withstats", b2)
	rep2 := planJSON(t, h2, "plan", "withstats://host", "--subscription", "orders=billing")
	if row := objectByName(t, &rep2, "subscription", "orders", "billing"); row.Before != existPresent.String() {
		t.Errorf("a subscription Stats DID answer for is %q, want %q", row.Before, existPresent)
	}
}

// TestTopologyRefusedWhereTheDriverHasNoAdmin holds the exit-3 contract: a
// driver with no AdminBackend is a fact about the driver and no retry changes
// it.
func TestTopologyRefusedWhereTheDriverHasNoAdmin(t *testing.T) {
	b := &fakeBackend{adminErr: mq.ErrUnsupported, caps: capsWith(t, mq.FeatureAdmin, mq.Unsupported)}
	h := newFakeHarness(t, "noadmin", b)
	for _, mode := range []string{"plan", "apply"} {
		h := harnessWith(t, h.env.registry, nil)
		if code := h.run("topology", mode, "noadmin://host", "--topic", "orders"); code != exitUnsupported {
			t.Errorf("topology %s = %d, want %d\nstderr:\n%s", mode, code, exitUnsupported, h.stderr())
		}
		if !strings.Contains(h.stderr(), "no Admin surface") {
			t.Errorf("topology %s does not name the missing surface:\n%s", mode, h.stderr())
		}
		if strings.Contains(h.stderr(), "one of the two is wrong") {
			t.Errorf("topology %s reported a disagreement where the table and the surface "+
				"agree:\n%s", mode, h.stderr())
		}
	}
}

// TestAdminAdvertisementDisagreementIsReported covers both directions of the
// contradiction between a capability table and the surface behind it.
//
// Neither is reachable with a shipped driver, which is the point: doctor can
// only ever report the advertisement, and this command is the first thing in
// the repository that produces the other half and can therefore compare them.
func TestAdminAdvertisementDisagreementIsReported(t *testing.T) {
	t.Run("advertises Admin and refuses one", func(t *testing.T) {
		b := &fakeBackend{adminErr: mq.ErrUnsupported, caps: capsWith(t, mq.FeatureAdmin, mq.Native)}
		h := newFakeHarness(t, "liar", b)
		if code := h.run("topology", "plan", "liar://host", "--topic", "t"); code != exitUnsupported {
			t.Fatalf("topology plan = %d, want %d", code, exitUnsupported)
		}
		if !strings.Contains(h.stderr(), "one of the two is wrong") {
			t.Errorf("the disagreement is not reported:\n%s", h.stderr())
		}
	})
	t.Run("advertises nothing and answers one", func(t *testing.T) {
		b := &fakeBackend{
			admin: &fakeAdmin{},
			caps:  capsWith(t, mq.FeatureAdmin, mq.Unsupported),
		}
		h := newFakeHarness(t, "modest", b)
		if code := h.run("topology", "plan", "modest://host", "--topic", "t"); code != exitOK {
			t.Fatalf("topology plan = %d, want %d\nstderr:\n%s", code, exitOK, h.stderr())
		}
		if !strings.Contains(h.stderr(), "understates the driver") {
			t.Errorf("the understatement is not reported:\n%s", h.stderr())
		}
		if !strings.Contains(h.stdout(), "the advertisement and the surface disagree") {
			t.Errorf("the report does not carry the disagreement:\n%s", h.stdout())
		}
	})
}

// TestTopologyNamesAnUnregistrableScheme: "no driver claims that scheme" and
// "a shipped module claims it and this binary cannot register it" read the
// same to a user and mean entirely different things.
func TestTopologyNamesAnUnregistrableScheme(t *testing.T) {
	h := newFullHarness(t)
	if code := h.run("topology", "plan", "amqp10://host", "--topic", "orders"); code != exitUsage {
		t.Fatalf("topology plan = %d, want %d\nstderr:\n%s", code, exitUsage, h.stderr())
	}
	for _, want := range []string{"drivers/amqp10", "cannot register", "Profile"} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, h.stderr())
		}
	}
	if strings.Contains(h.stderr(), "no driver in this binary claims") {
		t.Errorf("the refusal reports a claimed scheme as unclaimed:\n%s", h.stderr())
	}
}

// TestTopologyNamesTheSchemeCollision: a scheme two shipped modules claim
// reports which driver answered, because `natsjs://` reaching
// drivers/jetstream rather than drivers/natsunified changes what a caller
// gets.
func TestTopologyNamesTheSchemeCollision(t *testing.T) {
	h := newFullHarness(t)
	// --no-connect, so no NATS server is needed: the collision is a build
	// fact and nothing about it requires a broker.
	if code := h.run("topology", "plan", "--no-connect", "natsjs://host?stream=ORDERS",
		"--topic", "orders"); code != exitOK {
		t.Fatalf("topology plan = %d, want %d\nstderr:\n%s", code, exitOK, h.stderr())
	}
	for _, want := range []string{"natsunified", "The driver that answered here is jetstream"} {
		if !strings.Contains(h.stdout(), want) {
			t.Errorf("the report does not mention %q:\n%s", want, h.stdout())
		}
	}
}

// TestTopologyNoConnectReportsEverythingUnknown: a plan that opened nothing
// has no evidence about the broker and says so on every row.
func TestTopologyNoConnectReportsEverythingUnknown(t *testing.T) {
	h, broker := newTopoHarness(t)
	if code := h.run("topology", "plan", "--no-connect", topoScheme+"://",
		"--topic", "orders", "--subscription", "orders=billing"); code != exitOK {
		t.Fatalf("topology plan --no-connect = %d\nstderr:\n%s", code, h.stderr())
	}
	if n := strings.Count(h.stdout(), existUnknown.String()); n < 2 {
		t.Errorf("only %d row(s) are unknown; --no-connect can know nothing:\n%s", n, h.stdout())
	}
	if strings.Contains(h.stdout(), "connection          opened") {
		t.Errorf("--no-connect opened a connection:\n%s", h.stdout())
	}
	if got := brokerTopics(t, broker); len(got) != 0 {
		t.Errorf("--no-connect touched the broker: %v", got)
	}
}

// TestTopologyApplyRefusesNoConnect: an apply that changes nothing is a plan,
// and offering it under the other name would be a command that silently did
// not do its job.
func TestTopologyApplyRefusesNoConnect(t *testing.T) {
	h, _ := newTopoHarness(t)
	if code := h.run("topology", "apply", "--no-connect", topoScheme+"://",
		"--topic", "orders"); code != exitUsage {
		t.Fatalf("topology apply --no-connect = %d, want %d", code, exitUsage)
	}
}

// TestTopologySpecIsStrict holds the spec format to its refusals.
func TestTopologySpecIsStrict(t *testing.T) {
	cases := []struct {
		name string
		spec string
		want string
	}{
		{
			"no version",
			`{"topics":[{"name":"orders"}]}`,
			`no "version" field`,
		},
		{
			"a version this tool does not read",
			`{"version":2,"topics":[{"name":"orders"}]}`,
			"declares version 2",
		},
		{
			"an unknown field",
			`{"version":1,"topics":[{"name":"orders"}],"retention":"7d"}`,
			`unknown field "retention"`,
		},
		{
			"a topic name the portable grammar forbids",
			`{"version":1,"topics":[{"name":"orders queue"}]}`,
			"topic \"orders queue\"",
		},
		{
			"a subscription with no name",
			`{"version":1,"subscriptions":[{"topic":"orders"}]}`,
			"has no name",
		},
		{
			"two documents",
			`{"version":1,"topics":[{"name":"a"}]} {"version":1}`,
			"more than one JSON document",
		},
		// The decoder's More() answers false for a closing bracket, so a
		// spec with a stray one after the document, which is not JSON, was
		// read as if it were.
		{
			"a stray closing brace after the document",
			`{"version":1,"topics":[{"name":"a"}]}}`,
			"text after its JSON document",
		},
		{
			"a stray closing bracket after the document",
			`{"version":1,"topics":[{"name":"a"}]}` + "\n]",
			"text after its JSON document",
		},
		{
			"several stray closing brackets",
			`{"version":1,"topics":[{"name":"a"}]}}}]`,
			"text after its JSON document",
		},
		{
			"nothing at all",
			`{"version":1}`,
			"names no topic and no subscription",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "spec.json")
			if err := os.WriteFile(path, []byte(c.spec), 0o600); err != nil {
				t.Fatalf("writing the spec: %v", err)
			}
			h, _ := newTopoHarness(t)
			if code := h.run("topology", "plan", topoScheme+"://", path); code != exitUsage {
				t.Fatalf("topology plan = %d, want %d\nstdout:\n%s\nstderr:\n%s",
					code, exitUsage, h.stdout(), h.stderr())
			}
			if !strings.Contains(h.stderr(), c.want) {
				t.Errorf("the refusal does not mention %q:\n%s", c.want, h.stderr())
			}
		})
	}
}

// TestTopologyReadsASpecFile covers the accepting half, so the strictness
// above is not the whole of what the parser can do.
func TestTopologyReadsASpecFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec.json")
	spec := `{"version":1,
	  "topics":[{"name":"orders"},{"name":"orders/dead"}],
	  "subscriptions":[{"topic":"orders","name":"billing"}]}` + "\n\t \r\n" // white space after it is fine
	if err := os.WriteFile(path, []byte(spec), 0o600); err != nil {
		t.Fatalf("writing the spec: %v", err)
	}
	h, broker := newTopoHarness(t)
	if code := h.run("topology", "apply", topoScheme+"://", path); code != exitOK {
		t.Fatalf("topology apply = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	got := brokerTopics(t, broker)
	if len(got) != 2 || got[0] != "orders" || got[1] != "orders/dead" {
		t.Errorf("the broker holds %v, want [orders orders/dead]", got)
	}
}

// TestTopologyRefusesTwoSpecs: the file cannot be given twice.
func TestTopologyRefusesTwoSpecs(t *testing.T) {
	h, _ := newTopoHarness(t)
	if code := h.run("topology", "plan", topoScheme+"://", "a.json",
		"--file", "b.json"); code != exitUsage {
		t.Fatalf("topology plan = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(h.stderr(), "both as an argument and with --file") {
		t.Errorf("the refusal does not say what the conflict is:\n%s", h.stderr())
	}
}

// TestTopologyTimeoutBoundsReadingTheSpec: the usage says --timeout bounds
// the whole run, but topology read `--file -` with no deadline and started
// one only once the spec was read. A producer that wrote the spec and was
// slow to close its end held the run for as long as it liked, and the plan
// then succeeded under a fresh --timeout of its own. The deadline now covers
// the read, as pub's does: a read that returns after it ends the run, which
// plans and applies nothing. The interrupt is the same window for Ctrl-C. A
// producer that writes again after the stop, rather than closing its end,
// ends the run at that write, as the next line ends pub's.
func TestTopologyTimeoutBoundsReadingTheSpec(t *testing.T) {
	const spec = `{"version": 1, "topics": [{"name": "orders"}]}` + "\n"
	const timedOut = "the --timeout of 1s ran out while reading the spec, so nothing was "
	for _, c := range []struct {
		name, mode, want      string
		interrupt, writeAgain bool
	}{
		{"plan --timeout", "plan", timedOut + "planned", false, false},
		{"apply --timeout", "apply", timedOut + "applied", false, false},
		{"apply --timeout, written to again", "apply", timedOut + "applied", false, true},
		{"apply interrupted", "apply", "interrupted while reading the spec, so nothing was applied", true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, broker := newTopoHarness(t)
				stdin, typed := io.Pipe()
				h.env.stdin = stdin
				ctx, interrupt := context.WithCancel(context.Background())
				defer interrupt()
				h.env.ctx = ctx
				done := make(chan int, 1)
				go func() {
					done <- h.run("topology", c.mode, topoScheme+"://", "--file", "-", "--timeout", "1s")
				}()
				if _, err := typed.Write([]byte(spec)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if c.interrupt {
					interrupt()
				} else {
					time.Sleep(2 * time.Second) // the producer is slow to close its end
				}
				code, ended := 0, false
				if c.writeAgain {
					go func() { _, _ = typed.Write([]byte(" \n")) }()
					synctest.Wait()
					select {
					case code = <-done:
						ended = true
					default:
						t.Errorf("topology %s went on reading its spec after its --timeout and more input", c.mode)
					}
				}
				synctest.Wait()
				_ = typed.Close()
				if !ended {
					code = <-done
				}
				if code != exitFailure {
					t.Errorf("topology %s = %d after its read outlived the run, want %d\nstdout:\n%s\nstderr:\n%s",
						c.mode, code, exitFailure, h.stdout(), h.stderr())
				}
				if !strings.Contains(h.stderr(), c.want) {
					t.Errorf("topology %s does not say why it stopped:\n%s", c.mode, h.stderr())
				}
				if got := brokerTopics(t, broker); len(got) != 0 {
					t.Errorf("topology %s created %v after its run had ended", c.mode, got)
				}
			})
		})
	}
}

// topicWaitBound is the bound waitingAdmin applies when its caller gave none,
// the Kafka drivers' RequestTimeout default.
const topicWaitBound = 30 * time.Second

// waitingAdmin is fakeAdmin with an EnsureSubscription shaped like the Kafka
// drivers': it waits for the subscription's topic until the caller's deadline,
// or for topicWaitBound when the context has none. A topic that never appears
// is NotFound, and a caller whose own context ended gets that context's error.
// It records the deadline it saw.
type waitingAdmin struct {
	fakeAdmin
	deadline    time.Time
	sawDeadline bool
}

// EnsureSubscription waits for a topic nobody creates.
func (w *waitingAdmin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	w.deadline, w.sawDeadline = ctx.Deadline()
	bound := time.Duration(math.MaxInt64)
	if !w.sawDeadline {
		bound = topicWaitBound
	}
	wait, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	<-wait.Done()
	if err := ctx.Err(); err != nil {
		code := mq.CodeCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			code = mq.CodeDeadlineExceeded
		}
		return &mq.OpError{Driver: "fake", Operation: mq.OpAdmin, Topic: spec.Topic, Code: code, Err: err}
	}
	return &mq.OpError{
		Driver: "fake", Operation: mq.OpAdmin, Topic: spec.Topic, Code: mq.CodeNotFound,
		Err: errors.New("UNKNOWN_TOPIC_OR_PARTITION"),
	}
}

// TestTopologyTimeoutIsADeadlineTheDriverSees: --timeout was a timer that
// cancelled a context with no deadline, so a driver that sizes its own wait
// from ctx.Deadline() could not see it, and its expiry read "context
// canceled", as a Ctrl-C does. The Kafka drivers wait for a subscription's
// topic until the caller's deadline, or for their own 30s when there is none:
// `apply --subscription <missing>=s1` printed "Canceled: context canceled"
// with the default --timeout, and with --timeout 90s still ended at 30s. The
// bound is now the context's deadline, the default is 60s, and a run it ends
// says so. --timeout 0 is no bound, and the driver's own answer arrives.
func TestTopologyTimeoutIsADeadlineTheDriverSees(t *testing.T) {
	for _, c := range []struct {
		name  string
		flags []string
		bound time.Duration // the deadline the driver sees, 0 for none
		want  string
	}{
		{
			"the default", nil, time.Minute,
			"the --timeout of 1m0s ran out while ensuring subscription missing/s1",
		},
		{
			"--timeout 90s",
			[]string{"--timeout", "90s"},
			90 * time.Second,
			"the --timeout of 1m30s ran out while ensuring subscription missing/s1",
		},
		{"--timeout 0", []string{"--timeout", "0"}, 0, "NotFound: UNKNOWN_TOPIC_OR_PARTITION"},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				adm := &waitingAdmin{}
				h := newFakeHarness(t, "waits", &fakeBackend{admin: adm, caps: capsWith(t, mq.FeatureAdmin, mq.Native)})
				start := time.Now()
				code := h.run(append([]string{"topology", "apply", "waits://", "--subscription", "missing=s1"},
					c.flags...)...)
				took := time.Since(start)
				out := h.stdout() + h.stderr()
				if code != exitFailure {
					t.Errorf("topology apply = %d, want %d", code, exitFailure)
				}
				switch {
				case c.bound == 0 && adm.sawDeadline:
					t.Errorf("--timeout 0 is no bound, and the driver saw a deadline %v away", adm.deadline.Sub(start))
				case c.bound > 0 && !adm.sawDeadline:
					t.Errorf("the driver's EnsureSubscription saw no deadline, although --timeout bounds the run")
				case c.bound > 0 && !adm.deadline.Equal(start.Add(c.bound)):
					t.Errorf("the driver saw a deadline %v away, want %v", adm.deadline.Sub(start), c.bound)
				}
				if want := cmp.Or(c.bound, topicWaitBound); took != want {
					t.Errorf("the run took %v, want %v", took, want)
				}
				if !strings.Contains(out, c.want) {
					t.Errorf("the output does not say %q", c.want)
				}
				if strings.Contains(out, "context canceled") {
					t.Errorf("a run nobody interrupted reads \"context canceled\"")
				}
				if t.Failed() {
					t.Logf("exit %d after %v\nstdout:\n%s\nstderr:\n%s", code, took, h.stdout(), h.stderr())
				}
			})
		})
	}
}

// stallingAdmin is fakeAdmin with reads that answer only when their context
// ends, as a broker that took the connection and then stopped answering does.
// The first answeredLists calls to ListTopics answer at once, which is how a
// test stalls apply's re-read and not its plan.
type stallingAdmin struct {
	fakeAdmin
	answeredLists int
	stallStats    bool
	lists         int
}

// ListTopics stalls once answeredLists calls have answered.
func (s *stallingAdmin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	s.lists++
	if s.lists <= s.answeredLists {
		return s.fakeAdmin.ListTopics(ctx)
	}
	return func(yield func(string, error) bool) {
		<-ctx.Done()
		yield("", ctx.Err())
	}
}

// Stats stalls when the test asked it to.
func (s *stallingAdmin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	if s.stallStats {
		<-ctx.Done()
		return mq.QueueStats{}, ctx.Err()
	}
	return s.fakeAdmin.Stats(ctx, topic, sub)
}

// TestTopologyFailsWhenItsRunEndsWhileItReadsTheBroker: a plan whose
// --timeout ran out during the read-only probe exited 0 with every object
// unknown, and an apply whose re-read it cut short reported each object
// "ensured, not verifiable" and exited 0 with no note, although exit 1 is "a
// deadline passed with the work unfinished". Both now exit 1 and say what
// ended the read; an apply whose probe was cut short ensures nothing. A probe
// the driver cannot answer is still unknown with exit 0.
func TestTopologyFailsWhenItsRunEndsWhileItReadsTheBroker(t *testing.T) {
	const ranOutReading = "the --timeout of 2s ran out while reading the broker"
	for _, c := range []struct {
		name, mode string
		adm        *stallingAdmin
		args       []string
		interrupt  bool
		code       int
		want       string
	}{
		{
			name: "plan, ListTopics", mode: "plan", adm: &stallingAdmin{},
			args: []string{"--topic", "orders"}, code: exitFailure, want: ranOutReading,
		},
		{
			name: "plan, Stats", mode: "plan", adm: &stallingAdmin{answeredLists: 1, stallStats: true},
			args: []string{"--subscription", "orders=billing"}, code: exitFailure, want: ranOutReading,
		},
		{
			name: "plan interrupted", mode: "plan", adm: &stallingAdmin{}, interrupt: true,
			args: []string{"--topic", "orders"}, code: exitFailure, want: "interrupted while reading the broker",
		},
		{
			name: "plan, Stats unsupported", mode: "plan",
			adm:  &stallingAdmin{answeredLists: 1, fakeAdmin: fakeAdmin{statsErr: mq.ErrUnsupported}},
			args: []string{"--subscription", "orders=billing"}, code: exitOK,
		},
		{
			name: "apply, the probe", mode: "apply", adm: &stallingAdmin{},
			args: []string{"--topic", "orders"}, code: exitFailure, want: ranOutReading + ", so nothing was applied",
		},
		{
			name: "apply, the re-read", mode: "apply", adm: &stallingAdmin{answeredLists: 1},
			args: []string{"--topic", "orders"}, code: exitFailure,
			want: "the --timeout of 2s ran out while re-reading the broker",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newFakeHarness(t, "stalls", &fakeBackend{admin: c.adm, caps: capsWith(t, mq.FeatureAdmin, mq.Native)})
				ctx, interrupt := context.WithCancel(context.Background())
				defer interrupt()
				h.env.ctx = ctx
				done := make(chan int, 1)
				go func() {
					done <- h.run(append([]string{"topology", c.mode, "stalls://", "--timeout", "2s"}, c.args...)...)
				}()
				synctest.Wait()
				if c.interrupt {
					interrupt()
				}
				code := <-done
				if code != c.code {
					t.Errorf("topology %s = %d, want %d", c.mode, code, c.code)
				}
				if c.want != "" && !strings.Contains(h.stderr(), c.want) {
					t.Errorf("stderr does not say %q", c.want)
				}
				switch c.name {
				case "apply, the probe":
					if len(c.adm.topics) != 0 {
						t.Errorf("apply ensured %v although its run ended before the broker answered", c.adm.topics)
					}
				case "apply, the re-read":
					if !strings.Contains(h.stdout(), "the re-read's ListTopics could not be read") {
						t.Errorf("the notes do not say that the re-read's ListTopics failed")
					}
				}
				if t.Failed() {
					t.Logf("stdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
				}
			})
		})
	}
}

// TestTopologyNeedsAFigureToCallASubscriptionPresent: the probe took any Stats
// that did not fail as evidence that the subscription exists. The Kafka
// drivers and Pulsar answer every figure unknown, and no error, for a
// subscription that does not exist, so on them every subscription read
// "present, no-change" in a plan, even on a topic the same plan read absent,
// and "ensured" in apply's re-read, where ENSURED BUT NOT VISIBLE could then
// never fire. A Stats with no figure is unknown now. ListTopics does not
// decide it: RabbitMQ lists only the exchanges this process declared, so a
// subscription Stats gives a figure for is present whatever ListTopics says.
func TestTopologyNeedsAFigureToCallASubscriptionPresent(t *testing.T) {
	t.Run("plan, no figure", func(t *testing.T) {
		h := newFakeHarness(t, "nofigures", &fakeBackend{
			admin: &fakeAdmin{topics: []string{"orders"}, noFigures: true},
			caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
		})
		rep := planJSON(t, h, "plan", "nofigures://", "--subscription", "orders=never-created")
		row := objectByName(t, &rep, "subscription", "orders", "never-created")
		if row.Before != existUnknown.String() || row.Action != actionUnknown {
			t.Errorf("a Stats that knows no figure reads %q / %q, want %q / %q",
				row.Before, row.Action, existUnknown, actionUnknown)
		}
		if !strings.Contains(row.Detail, "no figure") {
			t.Errorf("the row does not say why its state is unknown: %q", row.Detail)
		}
	})
	t.Run("plan, a topic ListTopics does not list", func(t *testing.T) {
		// A fresh process's RabbitMQ ListTopics, for an exchange and a
		// queue that exist.
		h := newFakeHarness(t, "partlist", &fakeBackend{
			admin: &fakeAdmin{subscription: true},
			caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
		})
		rep := planJSON(t, h, "plan", "partlist://", "--topic", "orders", "--subscription", "orders=billing")
		if row := objectByName(t, &rep, "subscription", "orders", "billing"); row.Before != existPresent.String() {
			t.Errorf("a subscription Stats gives a figure for reads %q, want %q: ListTopics lists only part of "+
				"what exists on some drivers (%+v)", row.Before, existPresent, row)
		}
	})
	t.Run("apply, no figure", func(t *testing.T) {
		h := newFakeHarness(t, "nofigures", &fakeBackend{
			admin: &fakeAdmin{topics: []string{"orders"}, noFigures: true},
			caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
		})
		if code := h.run("topology", "apply", "nofigures://", "--subscription", "orders=billing"); code != exitOK {
			t.Fatalf("topology apply = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
		}
		if countRowResults(h.stdout(), resultUnverified) != 1 {
			t.Errorf("a re-read whose Stats knows no figure did not leave the subscription %q:\n%s",
				resultUnverified, h.stdout())
		}
	})
}

// TestTopologyPrintsNoCredential is the specification held over this command's two streams.
//
// The DSN carries a password AND a credential-named parameter, and neither may
// appear in either stream. The scheme is one no driver claims, so the run fails
// -- which is the harder case, because the failure path is the one that
// carries a driver's own message.
func TestTopologyPrintsNoCredential(t *testing.T) {
	const (
		password = "s3cr3t-pa55w0rd"
		token    = "tok-ABCDEF123456"
	)
	h := newFullHarness(t)
	target := "nosuchbroker://user:" + password + "@host:1234?sessiontoken=" + token
	if code := h.run("topology", "plan", target, "--topic", "orders"); code == exitOK {
		t.Fatalf("topology plan succeeded against an unclaimed scheme")
	}
	both := h.stdout() + h.stderr()
	for _, secret := range []string{password, token} {
		if strings.Contains(both, secret) {
			t.Errorf("a credential reached the terminal:\n%s", both)
		}
	}
	if !strings.Contains(both, dsn.Redacted) {
		t.Errorf("the target was not rendered redacted at all:\n%s", both)
	}

	// And the succeeding path, which prints the whole report rather than one
	// refusal line: it is the longer output and the one a user pastes into a
	// ticket.
	h2, _ := newTopoHarness(t)
	ok := topoScheme + "://user:" + password + "@host:1234?sessiontoken=" + token
	if code := h2.run("topology", "plan", ok, "--topic", "orders"); code != exitOK {
		t.Fatalf("topology plan = %d\nstderr:\n%s", code, h2.stderr())
	}
	both = h2.stdout() + h2.stderr()
	for _, secret := range []string{password, token} {
		if strings.Contains(both, secret) {
			t.Errorf("a credential reached the terminal on the succeeding path:\n%s", both)
		}
	}
	for _, want := range []string{dsn.Redacted, "parameter, not shown"} {
		if !strings.Contains(both, want) {
			t.Errorf("the report does not contain %q:\n%s", want, both)
		}
	}
}

// TestTopologyJSONIsMachineReadable: `-o json` is the form a deployment tool
// reads, so it has to parse and carry the same answers as the text.
func TestTopologyJSONIsMachineReadable(t *testing.T) {
	h, _ := newTopoHarness(t)
	if code := h.run("topology", "plan", "-o", "json", topoScheme+"://",
		"--topic", "orders", "--subscription", "orders=billing"); code != exitOK {
		t.Fatalf("topology plan -o json = %d\nstderr:\n%s", code, h.stderr())
	}
	var rep topologyReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	if rep.Mode != "plan" {
		t.Errorf("mode = %q, want plan", rep.Mode)
	}
	if len(rep.Objects) != 2 {
		t.Fatalf("%d object(s) in the JSON form, want 2", len(rep.Objects))
	}
	if !rep.Admin.Available {
		t.Error("the memory driver's Admin surface is reported as unavailable")
	}
	for _, o := range rep.Objects {
		if o.Before != existAbsent.String() || o.Action != actionCreate {
			t.Errorf("object %+v: want before=absent action=%q", o, actionCreate)
		}
	}
}

// TestTopologyEnsureFailureIsAFailure covers the ordinary broker refusal,
// which is exit 1 and not exit 3: a retry may well change it.
func TestTopologyEnsureFailureIsAFailure(t *testing.T) {
	b := &fakeBackend{
		admin: &fakeAdmin{ensureErr: errors.New("the broker is out of quota")},
		caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
	}
	h := newFakeHarness(t, "broken", b)
	if code := h.run("topology", "apply", "broken://host", "--topic", "orders"); code != exitFailure {
		t.Fatalf("topology apply = %d, want %d\nstderr:\n%s", code, exitFailure, h.stderr())
	}
	if !strings.Contains(h.stdout(), "out of quota") {
		t.Errorf("the report does not carry the broker's own reason:\n%s", h.stdout())
	}
}

// TestTopologyEnsureUnsupportedIsExitThree: an Ensure the driver refuses as a
// capability it does not have is exit 3, because no retry changes it.
func TestTopologyEnsureUnsupportedIsExitThree(t *testing.T) {
	b := &fakeBackend{
		admin: &fakeAdmin{ensureErr: mq.ErrUnsupported},
		caps:  capsWith(t, mq.FeatureAdmin, mq.Native),
	}
	h := newFakeHarness(t, "partial", b)
	if code := h.run("topology", "apply", "partial://host", "--topic", "orders"); code != exitUnsupported {
		t.Fatalf("topology apply = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitUnsupported, h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), resultUnsupported) {
		t.Errorf("the report does not carry %q:\n%s", resultUnsupported, h.stdout())
	}
}

// TestTopologyUsage covers the routing: no subcommand, a wrong one, and -h.
func TestTopologyUsage(t *testing.T) {
	cases := []struct {
		args []string
		code int
		want string
	}{
		{[]string{"topology"}, exitUsage, "plan or apply is required"},
		{[]string{"topology", "destroy"}, exitUsage, "no subcommand"},
		{[]string{"topology", "plan"}, exitUsage, "a DSN is required"},
		{[]string{"topology", "plan", "a", "b", "c"}, exitUsage, "at most one spec file"},
		{[]string{"topology", "plan", "-o", "yaml", "x://y"}, exitUsage, "text or json"},
		{[]string{"topology", "plan", "x://y", "--subscription", "nope"}, exitUsage, "TOPIC=SUBSCRIPTION"},
	}
	for _, c := range cases {
		h := newFullHarness(t)
		if got := h.run(c.args...); got != c.code {
			t.Errorf("run(%v) = %d, want %d\nstderr:\n%s", c.args, got, c.code, h.stderr())
		}
		if !strings.Contains(h.stderr(), c.want) {
			t.Errorf("run(%v) stderr does not contain %q:\n%s", c.args, c.want, h.stderr())
		}
	}
	h := newFullHarness(t)
	if got := h.run("topology", "-h"); got != exitOK {
		t.Errorf("topology -h = %d, want %d", got, exitOK)
	}
	if !strings.Contains(h.stdout(), "usage: iqmq topology") {
		t.Errorf("topology -h printed no usage:\n%s", h.stdout())
	}
}

// TestApplyNeverReachesADestructiveOperation is the property the fake can
// prove and a real broker cannot: nothing this command does calls DeleteTopic,
// DeleteSubscription or Purge, whatever the spec says.
//
// The fake's three destructive methods answer ErrUnsupported, so a call would
// turn into a visible failure rather than into silence.
func TestApplyNeverReachesADestructiveOperation(t *testing.T) {
	adm := &countingAdmin{fakeAdmin: fakeAdmin{}}
	b := &fakeBackend{admin: adm, caps: capsWith(t, mq.FeatureAdmin, mq.Native)}
	h := newFakeHarness(t, "counted", b)
	if code := h.run("topology", "apply", "counted://host",
		"--topic", "orders", "--subscription", "orders=billing"); code != exitOK {
		t.Fatalf("topology apply = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	if adm.destructive != 0 {
		t.Errorf("apply reached %d destructive operation(s); Ensure is the whole of what it does",
			adm.destructive)
	}
	if adm.ensures == 0 {
		t.Error("apply reached no Ensure at all, so the count above proves nothing")
	}
}

// countingAdmin counts what the command reached.
type countingAdmin struct {
	fakeAdmin
	ensures     int
	destructive int
}

// EnsureTopic counts and delegates.
func (c *countingAdmin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	c.ensures++
	return c.fakeAdmin.EnsureTopic(ctx, spec)
}

// EnsureSubscription counts and delegates.
func (c *countingAdmin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	c.ensures++
	return c.fakeAdmin.EnsureSubscription(ctx, spec)
}

// DeleteTopic counts a destructive call.
func (c *countingAdmin) DeleteTopic(context.Context, string) error {
	c.destructive++
	return mq.ErrUnsupported
}

// DeleteSubscription counts a destructive call.
func (c *countingAdmin) DeleteSubscription(context.Context, string, string) error {
	c.destructive++
	return mq.ErrUnsupported
}

// Purge counts a destructive call.
func (c *countingAdmin) Purge(context.Context, string, string) error {
	c.destructive++
	return mq.ErrUnsupported
}

func mustBuild(tb testing.TB, b *driverkit.CapabilityBuilder) mq.Capabilities {
	tb.Helper()
	caps, err := b.Build()
	if err != nil {
		tb.Fatalf("Build: %v", err)
	}
	return caps
}
