package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

const topologyUsage = `usage: iqmq topology plan  [flags] <dsn> [spec.json]
       iqmq topology apply [flags] <dsn> [spec.json]

Declare the topics and subscriptions a deployment needs, see what would change,
and then make it so.

the API forbids Open from creating production topology, which is
why every other command in this tool fails against a topic that does not
exist. "apply" is the explicit route the specification keeps for that, over the portable
Admin surface: Ensure operations only, never a delete and never a purge, so an
apply cannot destroy anything whatever the spec says.

"plan" performs NO mutation. It reads what exists and prints what apply would
do. "apply" prints the same plan, ensures each object, and then RE-READS the
broker: an Ensure that reported success and left nothing visible is a defect in
the driver, and it is reported as one rather than as a success.

The desired topology comes from the flags, from a JSON file, or from both:

    iqmq topology plan memory://?autocreate=true --topic orders \
        --subscription orders=billing

    {"version": 1,
     "topics": [{"name": "orders"}],
     "subscriptions": [{"topic": "orders", "name": "billing"}]}

flags:
      --topic NAME        a topic the deployment needs; repeatable
      --subscription T=S  subscription S on topic T; repeatable
  -f, --file PATH         read a JSON spec, or "-" for stdin
  -o, --output text|json  output format (default text)
      --no-connect        plan without opening a connection: every object's
                          current state is then reported as unknown
      --timeout DURATION  bound the whole run (default 30s)

A subscription is written T=S because "=" is the one byte a portable topic
name cannot contain (the grammar is segments of ALPHA/DIGIT/"."/"_"/"-"
joined by "/"), so the split is unambiguous for every name this tool accepts.

It prints no credential: the spec carries none, the target is rendered
redacted, and the whole report passes through the same scrub as doctor's.

exit codes: 0 the plan or the apply succeeded; 1 the broker refused or an
object was ensured and did not appear; 2 the invocation or the spec was
malformed; 3 the driver has no Admin surface at all, which no retry changes.
`

// specVersion is the only version of the spec file this tool reads.
//
// It is REQUIRED, and a file without it is refused naming the field. Treating
// a missing version as 1 was the other option and it is a trap: a format that
// accepts an unversioned file can never introduce a version later without
// having to guess which of two grammars an old file was written in.
const specVersion = 1

// topologySpec is the desired topology one invocation names.
//
// It is this package's own type with its own JSON tags rather than a slice of
// [mq.TopicSpec]: a file format is a compatibility promise, and one whose
// field names came from a frozen API's identifiers would change shape whenever
// that API did.
type topologySpec struct {
	Version       int                 `json:"version"`
	Topics        []topicEntry        `json:"topics,omitempty"`
	Subscriptions []subscriptionEntry `json:"subscriptions,omitempty"`
}

// topicEntry is one desired topic.
//
// Only the name, because [mq.TopicSpec] carries only the name: everything else
// a broker wants (partitions, replication, retention) is named by the driver's
// own typed administration, which the specification keeps as the advanced API. A spec
// field this tool could not act on would be a promise the Admin surface cannot
// keep.
type topicEntry struct {
	Name string `json:"name"`
}

// subscriptionEntry is one desired subscription.
type subscriptionEntry struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
}

// empty reports whether the spec asks for nothing.
func (s *topologySpec) empty() bool { return len(s.Topics) == 0 && len(s.Subscriptions) == 0 }

// validate holds the spec to what the portable surface can act on.
//
// It runs before anything is opened, so a typo in a topic name is a refusal
// that names the byte rather than a dial that fails for an unrelated reason.
func (s *topologySpec) validate() error {
	if s.Version != specVersion {
		if s.Version == 0 {
			return errors.New(`the spec has no "version" field; this tool reads {"version": 1, ...}`)
		}
		return errors.New(`the spec declares version ` + itoa(s.Version) +
			`; this tool reads version ` + itoa(specVersion))
	}
	for _, t := range s.Topics {
		if err := mq.ValidateTopic(t.Name); err != nil {
			return errors.New("topic " + quote(t.Name) + ": " + err.Error())
		}
	}
	for _, sub := range s.Subscriptions {
		if err := mq.ValidateTopic(sub.Topic); err != nil {
			return errors.New("subscription on topic " + quote(sub.Topic) + ": " + err.Error())
		}
		if sub.Name == "" {
			return errors.New("the subscription on topic " + quote(sub.Topic) + " has no name")
		}
	}
	return nil
}

// parseTopologySpec reads a spec file.
//
// Unknown fields are REFUSED, which is ADR-010's strictness applied to a file
// instead of to a DSN: a misspelled key that is silently ignored is a
// deployment that quietly does not provision what its author wrote down.
func parseTopologySpec(r io.Reader) (*topologySpec, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var s topologySpec
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("the spec holds more than one JSON document")
	}
	return &s, nil
}

// existence is what a read-only probe could determine about one object.
//
// The third state is the point. Admin has no ListSubscriptions and a
// driver may not implement Stats, so "this subscription does not exist" and
// "nothing here can tell me whether it exists" are different answers, and a
// plan that rendered the second as the first would promise a creation it had
// no evidence was needed.
type existence uint8

// The three answers.
const (
	// existUnknown means nothing available could tell.
	existUnknown existence = iota
	// existPresent means the object was observed.
	existPresent
	// existAbsent means the broker was asked and said no.
	existAbsent
)

// String returns the word the report prints.
func (x existence) String() string {
	switch x {
	case existPresent:
		return "present"
	case existAbsent:
		return "absent"
	case existUnknown:
		return "unknown"
	default:
		return "existence(?)"
	}
}

// The actions a plan can name. "unknown" is not a refusal: apply's Ensure is
// idempotent, so an object whose current state could not be read is still safe
// to ensure -- what is lost is only the ability to say in advance whether
// anything will change.
const (
	actionCreate   = "create"
	actionNoChange = "no-change"
	actionUnknown  = "ensure (current state unknown)"
)

// The results one object can have after apply.
const (
	// resultEnsured is an Ensure that succeeded AND a re-read that found the
	// object.
	resultEnsured = "ensured"
	// resultUnverified is an Ensure that succeeded and a re-read that could
	// not confirm it, because the surface has no way to ask.
	resultUnverified = "ensured, not verifiable"
	// resultInvisible is an Ensure that succeeded and a re-read that did NOT
	// find the object. It is a defect in the driver and exits 1.
	resultInvisible = "ENSURED BUT NOT VISIBLE"
	// resultFailed is an Ensure that returned an error.
	resultFailed = "failed"
	// resultUnsupported is an Ensure the driver refuses as a capability it
	// does not have.
	resultUnsupported = "unsupported"
)

// topoRow is one object's line of the report.
type topoRow struct {
	// Kind is "topic" or "subscription".
	Kind string `json:"kind"`
	// Topic is the topic, for both kinds.
	Topic string `json:"topic"`
	// Name is the subscription's name, empty for a topic.
	Name string `json:"name,omitempty"`
	// Before is what the read-only probe found.
	Before string `json:"before"`
	// Action is what apply would do.
	Action string `json:"action"`
	// Detail says why Before is unknown, when it is.
	Detail string `json:"detail,omitempty"`
	// Result is what apply did; empty for a plan.
	Result string `json:"result,omitempty"`
	// After is the re-read, for apply.
	After string `json:"after,omitempty"`
	// Error is the driver's own message, when one failed.
	Error string `json:"error,omitempty"`
}

// adminReport is the two facts about a target's Admin surface, kept apart.
//
// Advertised is the level the driver claims for [mq.FeatureAdmin]; Available is
// whether Client.Admin() actually answered one. doctor can only report the
// first -- it is an advertisement and never evidence -- and this command
// produces the second, which is why they are two fields and why a disagreement
// between them is reported rather than resolved.
type adminReport struct {
	Advertised string `json:"advertised_level"`
	Available  bool   `json:"available"`
	Error      string `json:"error,omitempty"`
	// Disagrees reports that the advertisement and the surface contradict each
	// other, which is a defect in the driver either way round.
	Disagrees bool `json:"advertisement_disagrees"`
}

// topologyReport is the whole report, and its JSON form. Nothing in it can
// carry a credential: the target is redacted before it is stored and every
// other string is a name from the spec or a driver's own message.
type topologyReport struct {
	Mode      string      `json:"mode"`
	DSN       string      `json:"dsn"`
	Scheme    string      `json:"scheme"`
	Driver    string      `json:"driver,omitempty"`
	Module    string      `json:"module,omitempty"`
	Connected bool        `json:"connected"`
	Admin     adminReport `json:"admin"`
	Objects   []topoRow   `json:"objects"`
	Notes     []string    `json:"notes,omitempty"`
}

// topoOptions are one invocation's controls.
type topoOptions struct {
	topics  topicFlag
	subs    topicFlag
	file    string
	output  string
	noConn  bool
	timeout time.Duration
}

// topicFlag collects a repeatable string flag.
type topicFlag struct{ names []string }

// String renders what was given, for the flag package.
func (f *topicFlag) String() string { return strings.Join(f.names, ",") }

// Set appends one value.
func (f *topicFlag) Set(s string) error {
	if s == "" {
		return errors.New("an empty name names nothing")
	}
	f.names = append(f.names, s)
	return nil
}

// cmdTopology routes `iqmq topology <plan|apply>`.
func cmdTopology(e *cliEnv, args []string) int {
	if len(args) == 0 {
		_, _ = io.WriteString(e.stderr, "iqmq topology: plan or apply is required\n"+topologyUsage)
		return exitUsage
	}
	switch args[0] {
	case "plan":
		return e.topology("plan", args[1:])
	case "apply":
		return e.topology("apply", args[1:])
	case "-h", "--help", "-help", "help":
		_, _ = io.WriteString(e.stdout, topologyUsage)
		return exitOK
	default:
		_, _ = io.WriteString(e.stderr, "iqmq topology: no subcommand "+quote(args[0])+
			"; it takes plan or apply\n"+topologyUsage)
		return exitUsage
	}
}

// topology is the body of both subcommands. mode is "plan" or "apply".
func (e *cliEnv) topology(mode string, args []string) int {
	fs := newFlagSet("topology "+mode, e)
	var o topoOptions
	fs.Var(&o.topics, "topic", "a topic the deployment needs")
	fs.Var(&o.subs, "subscription", "subscription S on topic T, written T=S")
	fs.StringVar(&o.file, "f", "", "read a JSON spec, or - for stdin")
	fs.StringVar(&o.file, "file", "", "read a JSON spec, or - for stdin")
	fs.StringVar(&o.output, "o", "text", "output format: text or json")
	outputL := fs.String("output", "", "output format: text or json")
	fs.BoolVar(&o.noConn, "no-connect", false, "plan without opening a connection")
	fs.DurationVar(&o.timeout, "timeout", 30*time.Second, "bound the whole run")
	if code := parseFlags(fs, e, args, topologyUsage); code >= 0 {
		return code
	}
	if *outputL != "" {
		o.output = *outputL
	}
	if o.output != "text" && o.output != "json" {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+
			": --output takes text or json, not "+quote(o.output)+"\n")
		return exitUsage
	}
	if fs.NArg() == 0 {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": a DSN is required\n"+topologyUsage)
		return exitUsage
	}
	if fs.NArg() > 2 {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+
			": takes a DSN and at most one spec file\n"+topologyUsage)
		return exitUsage
	}
	if mode == "apply" && o.noConn {
		_, _ = io.WriteString(e.stderr, "iqmq topology apply: --no-connect asks for an apply that "+
			"changes nothing, which is what \"iqmq topology plan --no-connect\" is\n")
		return exitUsage
	}
	target := fs.Arg(0)
	if fs.NArg() == 2 {
		if o.file != "" {
			_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+
				": the spec was given both as an argument and with --file\n")
			return exitUsage
		}
		o.file = fs.Arg(1)
	}

	// Both streams are scrubbed before anything can name the target.
	e, flush := e.scrubbed(o.output == "json", target)
	defer flush()

	spec, code := e.readSpec(mode, &o)
	if code >= 0 {
		return code
	}
	return e.runTopology(mode, target, spec, &o)
}

// readSpec merges the flags and the file into one spec, or reports the exit
// code to return.
func (e *cliEnv) readSpec(mode string, o *topoOptions) (*topologySpec, int) {
	bad := func(msg string) (*topologySpec, int) {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": "+msg+"\n")
		return nil, exitUsage
	}
	spec := &topologySpec{Version: specVersion}
	if o.file != "" {
		r, name, err := e.bodySource(o.file)
		if err != nil {
			return bad(err.Error())
		}
		if r == nil {
			return bad("--file names no readable spec")
		}
		parsed, perr := parseTopologySpec(r)
		if closer, ok := r.(io.Closer); ok && name != "-" {
			_ = closer.Close()
		}
		if perr != nil {
			return bad("reading the spec: " + perr.Error())
		}
		spec = parsed
	}
	for _, t := range o.topics.names {
		spec.Topics = append(spec.Topics, topicEntry{Name: t})
	}
	for _, s := range o.subs.names {
		topic, name, ok := strings.Cut(s, "=")
		if !ok || topic == "" || name == "" {
			return bad("--subscription takes TOPIC=SUBSCRIPTION, not " + quote(s))
		}
		spec.Subscriptions = append(spec.Subscriptions, subscriptionEntry{Topic: topic, Name: name})
	}
	if err := spec.validate(); err != nil {
		return bad(err.Error())
	}
	if spec.empty() {
		return bad("the spec names no topic and no subscription; " +
			"give --topic NAME, --subscription TOPIC=SUB, or a spec file")
	}
	return spec, -1
}

// runTopology resolves the target and produces the report.
func (e *cliEnv) runTopology(mode, target string, spec *topologySpec, o *topoOptions) int {
	rep := topologyReport{Mode: mode, DSN: redactTarget(target)}
	scheme, err := dsn.Scheme(target)
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": "+err.Error()+"\n")
		return exitUsage
	}
	rep.Scheme = scheme

	// The REGISTRY decides whether the scheme can be opened, not the driver
	// table: the table is this binary's build-time list and the registry is
	// what a caller -- or a test -- actually handed the command. Asking the
	// table was this command's first defect, and it made every test that
	// registered a scheme of its own fail with "no driver in this binary
	// claims the scheme" while the registry held exactly that scheme.
	if _, claimed := e.registry.Lookup(scheme); !claimed {
		// "no driver claims this scheme" is only half an answer when a
		// shipped module claims it and this binary could not register it.
		// Reporting the absence without the reason is what makes a user read
		// "the CLI cannot open it" as "iqmq has no such driver".
		msg := "no driver in this binary claims the scheme " + quote(scheme) +
			"\n  this binary opens: " + strings.Join(e.registry.Schemes(), " ") + "\n"
		if u, blocked := unregistrableScheme(scheme); blocked {
			msg = "the scheme " + quote(scheme) + " is claimed by " + u.module +
				", which this binary cannot register:\n    " + u.reason + "\n"
		}
		// The redacted target is named as well as the scheme: a refusal that
		// said only "nosuchbroker" leaves a caller with several DSNs guessing
		// which one it was about.
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": "+rep.DSN+": "+msg)
		return exitUsage
	}
	// The driver table names what answered, when it knows: it is where the
	// module path lives, and a registry entry carries only the opener.
	answered := "the driver registered for this scheme"
	if entry, known := entryForScheme(scheme); known {
		rep.Driver, rep.Module = entry.name, entry.module
		answered = entry.name
	}
	for _, u := range unreachableDrivers() {
		for _, s := range u.schemes {
			if s != scheme {
				continue
			}
			rep.Notes = append(rep.Notes, "the scheme "+quote(scheme)+" is also claimed by "+
				u.module+", which this binary does not register: "+u.reason+
				". The driver that answered here is "+answered+".")
		}
	}
	if isEphemeral(target) {
		rep.Notes = append(rep.Notes, strings.TrimSuffix(
			strings.ReplaceAll(ephemeralNote, "\n  ", " "), "\n",
		))
	}

	rep.Objects = planRows(spec, nil)
	if o.noConn {
		rep.Notes = append(rep.Notes, "--no-connect: nothing was opened, so every object's "+
			"current state is unknown and no plan below is evidence about this broker.")
		rep.Admin.Advertised = "not read (--no-connect)"
		e.writeTopology(&rep, o)
		return exitOK
	}

	ctx, cancel := e.deadline(e.ctx, o.timeout)
	defer cancel()
	client, code := e.open(ctx, "topology "+mode, target)
	if code >= 0 {
		return code
	}
	defer func() { _ = client.Close() }()
	rep.Connected = true

	admin, code := e.adminOf(client, mode, &rep)
	if code >= 0 {
		e.writeTopology(&rep, o)
		return code
	}

	p := newTopoProbe(ctx, admin)
	rep.Objects = planRows(spec, p)
	if p.topicsErr != "" {
		rep.Notes = append(rep.Notes, "ListTopics could not be read ("+p.topicsErr+
			"), so a topic's current state is unknown here.")
	}
	rep.Notes = append(rep.Notes, "a subscription's current state is probed with Admin.Stats, "+
		"because Admin has no ListSubscriptions; a driver without Stats makes the "+
		"answer unknown rather than absent.")
	if mode == "plan" {
		e.writeTopology(&rep, o)
		return exitOK
	}
	return e.applyTopology(ctx, admin, &rep, o)
}

// adminOf reads the two Admin facts and reports them.
//
// The advertised level is read first and unconditionally, so that a driver
// which claims FeatureAdmin and then refuses to hand one over is caught. Both
// directions of the disagreement are a defect: an advertisement nothing backs,
// or a surface nothing announces.
func (e *cliEnv) adminOf(client *mq.Client, mode string, rep *topologyReport) (mq.Admin, int) {
	level := client.Capabilities().Support(mq.FeatureAdmin).Level
	rep.Admin.Advertised = level.String()
	admin, err := client.Admin()
	if err != nil {
		rep.Admin.Error = err.Error()
		rep.Admin.Disagrees = level != mq.Unsupported
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": this driver has no Admin surface: "+
			err.Error()+"\n  the SPI makes AdminBackend optional, so a topology cannot be "+
			"planned or applied\n  through the portable surface here. "+
			"\"iqmq doctor <dsn>\" reports what it does advertise;\n  the driver's own typed "+
			"administration remains the advanced API.\n")
		if rep.Admin.Disagrees {
			_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": and it ADVERTISES Admin="+
				level.String()+" while Client.Admin() refuses, which is a defect in the driver: "+
				"one of the two is wrong.\n")
		}
		return nil, exitUnsupported
	}
	rep.Admin.Available = true
	rep.Admin.Disagrees = level == mq.Unsupported
	if rep.Admin.Disagrees {
		_, _ = io.WriteString(e.stderr, "iqmq topology "+mode+": this driver ADVERTISES Admin="+
			level.String()+" and Client.Admin() answered one anyway, which is a defect in the "+
			"driver: a capability table that understates the driver makes every caller's "+
			"RequireAtLeast refuse work the broker can do.\n")
	}
	return admin, -1
}

// topoProbe is the read-only state of a broker: the topics it lists, and the
// Admin surface a per-subscription probe needs.
type topoProbe struct {
	ctx   context.Context
	admin mq.Admin
	// topics is the set ListTopics yielded, nil when it could not be read.
	topics map[string]bool
	// topicsErr is why, when it could not.
	topicsErr string
}

// newTopoProbe reads what can be read without changing anything.
func newTopoProbe(ctx context.Context, admin mq.Admin) *topoProbe {
	p := &topoProbe{ctx: ctx, admin: admin}
	set := map[string]bool{}
	for name, err := range admin.ListTopics(ctx) {
		if err != nil {
			p.topicsErr = err.Error()
			return p
		}
		set[name] = true
	}
	p.topics = set
	return p
}

// topic reports what is known about one topic.
func (p *topoProbe) topic(name string) (existence, string) {
	if p == nil {
		return existUnknown, "nothing was opened"
	}
	if p.topics == nil {
		return existUnknown, "ListTopics: " + p.topicsErr
	}
	if p.topics[name] {
		return existPresent, ""
	}
	return existAbsent, ""
}

// subscription reports what is known about one subscription.
//
// Stats is the probe because offers nothing else: there is no
// ListSubscriptions, so the only read-only question a caller can ask about a
// subscription is what its depth is. A CodeNotFound answer is evidence of
// absence; ErrUnsupported is evidence of nothing.
func (p *topoProbe) subscription(topic, name string) (existence, string) {
	if p == nil {
		return existUnknown, "nothing was opened"
	}
	if _, err := p.admin.Stats(p.ctx, topic, name); err != nil {
		switch mq.CodeOf(err) {
		case mq.CodeNotFound:
			return existAbsent, ""
		case mq.CodeUnsupported:
			return existUnknown, "Admin.Stats is unsupported, and the specification has no ListSubscriptions"
		default:
			return existUnknown, "Admin.Stats: " + err.Error()
		}
	}
	return existPresent, ""
}

// planRows builds one row per object. A nil probe is the --no-connect case and
// makes every row unknown.
func planRows(spec *topologySpec, p *topoProbe) []topoRow {
	rows := make([]topoRow, 0, len(spec.Topics)+len(spec.Subscriptions))
	for _, t := range spec.Topics {
		before, detail := p.topic(t.Name)
		rows = append(rows, topoRow{
			Kind: "topic", Topic: t.Name,
			Before: before.String(), Action: actionFor(before), Detail: detail,
		})
	}
	for _, s := range spec.Subscriptions {
		before, detail := p.subscription(s.Topic, s.Name)
		rows = append(rows, topoRow{
			Kind: "subscription", Topic: s.Topic, Name: s.Name,
			Before: before.String(), Action: actionFor(before), Detail: detail,
		})
	}
	return rows
}

// actionFor maps a current state to what apply would do.
func actionFor(x existence) string {
	switch x {
	case existPresent:
		return actionNoChange
	case existAbsent:
		return actionCreate
	case existUnknown:
		return actionUnknown
	default:
		return actionUnknown
	}
}

// applyTopology ensures each object and then re-reads the broker.
//
// The re-read is the whole reason apply is more than a loop over Ensure.
// EnsureTopic returns nil whether it created the topic or found it, so the
// call's own success says nothing about what is now there; asking again does.
// An object that was ensured and is still absent is a driver defect and the
// run fails on it.
func (e *cliEnv) applyTopology(
	ctx context.Context, admin mq.Admin, rep *topologyReport, o *topoOptions,
) int {
	failures, unsupported, invisible := 0, false, 0
	for i := range rep.Objects {
		row := &rep.Objects[i]
		var err error
		switch row.Kind {
		case "topic":
			err = admin.EnsureTopic(ctx, mq.TopicSpec{Name: row.Topic})
		case "subscription":
			err = admin.EnsureSubscription(ctx,
				mq.SubscriptionSpec{Topic: row.Topic, Name: row.Name})
		}
		if err != nil {
			row.Error = err.Error()
			if mq.CodeOf(err) == mq.CodeUnsupported {
				row.Result = resultUnsupported
				unsupported = true
				continue
			}
			row.Result = resultFailed
			failures++
			continue
		}
	}

	// ONE re-read for the whole run: the topic set is read once here rather
	// than once per object, so N topics cost one ListTopics and not N. A
	// subscription still costs a Stats each, because that is the only
	// question lets a caller ask about one.
	after := newTopoProbe(ctx, admin)
	for i := range rep.Objects {
		row := &rep.Objects[i]
		if row.Result != "" {
			continue // it failed or was refused; there is nothing to verify
		}
		row.Result, row.After = verifyObject(after, row)
		if row.Result == resultInvisible {
			invisible++
		}
	}
	e.writeTopology(rep, o)
	switch {
	case unsupported && failures == 0 && invisible == 0:
		_, _ = io.WriteString(e.stderr, "iqmq topology apply: an Ensure was refused as a "+
			"capability this driver does not have; no retry changes that \n")
		return exitUnsupported
	case invisible > 0:
		_, _ = io.WriteString(e.stderr, "iqmq topology apply: "+itoa(invisible)+
			" object(s) were ensured successfully and are STILL not visible to a re-read. "+
			"That is a defect in the driver, not in the spec.\n")
		return exitFailure
	case failures > 0 || unsupported:
		_, _ = io.WriteString(e.stderr, "iqmq topology apply: "+itoa(failures)+
			" object(s) could not be ensured\n")
		return exitFailure
	}
	return exitOK
}

// verifyObject reads one object out of the post-apply probe.
func verifyObject(p *topoProbe, row *topoRow) (result, after string) {
	var x existence
	switch row.Kind {
	case "topic":
		x, _ = p.topic(row.Topic)
	default:
		x, _ = p.subscription(row.Topic, row.Name)
	}
	switch x {
	case existPresent:
		return resultEnsured, x.String()
	case existAbsent:
		return resultInvisible, x.String()
	case existUnknown:
		return resultUnverified, x.String()
	default:
		return resultUnverified, x.String()
	}
}

// writeTopology renders the report.
func (e *cliEnv) writeTopology(rep *topologyReport, o *topoOptions) {
	if o.output == "json" {
		enc := json.NewEncoder(e.stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			_, _ = io.WriteString(e.stderr, "iqmq topology "+rep.Mode+": "+err.Error()+"\n")
		}
		return
	}
	writeTopologyText(e.stdout, rep)
}

// writeTopologyText renders the report for a terminal.
func writeTopologyText(w io.Writer, rep *topologyReport) {
	p := func(s string) { _, _ = io.WriteString(w, s) }
	row := func(k, v string) { p("  " + pad(k, 22) + v + "\n") }

	p("iqmq topology " + rep.Mode + "\n\ntarget\n")
	row("dsn", rep.DSN)
	row("scheme", rep.Scheme)
	if rep.Driver != "" {
		row("driver", rep.Driver+" ("+rep.Module+")")
	}
	state := "not opened"
	if rep.Connected {
		state = "opened"
	}
	row("connection", state)
	row("admin advertised", rep.Admin.Advertised)
	avail := "no"
	if rep.Admin.Available {
		avail = "yes"
	}
	row("admin available", avail+what(rep.Admin.Error))
	if rep.Admin.Disagrees {
		p("  !! the advertisement and the surface disagree; one of the two is wrong\n")
	}

	switch rep.Mode {
	case "plan":
		p("\nplan (NOTHING below was changed; \"iqmq topology apply\" is what changes it)\n")
	default:
		p("\napply (each object was ensured and then re-read)\n")
	}
	for i := range rep.Objects {
		o := &rep.Objects[i]
		name := o.Kind + " " + o.Topic
		if o.Name != "" {
			name += "/" + o.Name
		}
		line := pad(o.Before, 10) + o.Action
		if o.Result != "" {
			line = pad(o.Before, 10) + pad(o.Action, 30) + o.Result
		}
		row(name, line)
		if o.Detail != "" {
			p("      " + o.Detail + "\n")
		}
		if o.Error != "" {
			p("      error: " + o.Error + "\n")
		}
	}
	if len(rep.Notes) > 0 {
		p("\nnotes\n")
		for _, n := range rep.Notes {
			p("  - " + n + "\n")
		}
	}
	p("\nApply ensures and never deletes: DeleteTopic, DeleteSubscription and Purge are\n" +
		"the destructive operations and no spec this tool reads can reach them.\n")
}

// what renders an optional parenthetical.
func what(s string) string {
	if s == "" {
		return ""
	}
	return " (" + s + ")"
}

// unregistrableScheme reports the shipped module that claims a scheme this
// binary could not register.
func unregistrableScheme(scheme string) (unreachable, bool) {
	for _, u := range unreachableDrivers() {
		for _, s := range u.schemes {
			if s == scheme {
				return u, true
			}
		}
	}
	return unreachable{}, false
}
