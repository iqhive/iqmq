package main

import (
	"encoding/json"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

const doctorUsage = `usage: iqmq doctor [flags] [dsn]

Resolve a DSN and report what the driver behind it SAYS it can and cannot do:
the capability levels it advertises (the message contract), the limits it
advertises, the variant kinds in force and whether their bindings are still
provisional.

What doctor reports is the driver's own advertisement, read from
Client.Capabilities(). It is NOT probe evidence. A driver advertising a feature
and a conformance run having verified it are different facts, and
docs/drivers/matrix.md renders six distinct states to keep them apart; nine of
the features below have no behavioural probe in ANY conformance run, so a
driver that advertises one cannot be held to it. doctor names those nine in its
own output rather than leaving them to be looked up.

With no DSN it reports the build instead: the schemes this binary can open, the
driver modules it links, the ones it cannot register and why, and every variant
family's auto binding.

It prints no credential. A DSN's password and its credential-bearing
parameters are never rendered, the resolved configuration is rendered by the
driver's own safe String, and the whole report passes through a scrub that
holds every credential the DSN carried. An environment variable is
reported by name and by set/unset, never by value.

flags:
  -o, --output text|json   output format (default text)
  --no-connect             parse and report without opening a connection
  --timeout DURATION       bound the connection and the ping (default 10s)
  --drivers                report the driver table and exit

The DSN may also come from $IQMQ_DSN when no argument is given and --drivers
is not set.
`

// doctorReport is the whole report, and the JSON form of it.
//
// Every string in it has already been through a redaction: there is no field
// here that a credential can reach, which is a property of what fills the
// struct rather than of how it is printed, so the text and the JSON form
// cannot disagree about what is safe.
type doctorReport struct {
	Version  string          `json:"version"`
	Go       string          `json:"go"`
	Platform string          `json:"platform"`
	Build    buildReport     `json:"build"`
	Target   *targetReport   `json:"target,omitempty"`
	Variants []variantReport `json:"variants"`
	Env      []envReport     `json:"environment,omitempty"`
}

// buildReport is what this binary is, independent of any target.
type buildReport struct {
	Drivers     []driverReport `json:"drivers"`
	Schemes     []string       `json:"schemes"`
	Unreachable []moduleReport `json:"unreachable,omitempty"`
	Provisional int            `json:"provisional_families"`
	Families    int            `json:"families"`
}

// driverReport is one registered driver.
type driverReport struct {
	Name    string   `json:"name"`
	Module  string   `json:"module"`
	Schemes []string `json:"schemes"`
}

// moduleReport is a shipped driver module this binary cannot register.
type moduleReport struct {
	Module string `json:"module"`
	Reason string `json:"reason"`
	// Schemes are the DSN schemes the module claims, when it names any. A
	// reader with a DSN in their hand matches on the scheme and not on the
	// module path, and `iqmq topology` refuses such a target by naming this
	// entry.
	Schemes []string `json:"schemes,omitempty"`
}

// targetReport is what one DSN resolved to.
type targetReport struct {
	// DSN is the redacted target: scheme, user, endpoints, path. It never
	// carries a parameter value.
	DSN string `json:"dsn"`
	// Parameters are the parameter NAMES the DSN carried, with a
	// credential-bearing one marked rather than valued.
	Parameters []string `json:"parameters,omitempty"`
	Scheme     string   `json:"scheme"`
	Driver     string   `json:"driver"`
	Module     string   `json:"module"`
	// Config is the driver's own rendering of the configuration the DSN
	// parsed into, which the specification makes the driver's obligation to redact.
	Config string `json:"config,omitempty"`
	// ConfigError is why the driver could not parse the DSN, when it could
	// not. It is the driver's own error string and carries no DSN.
	ConfigError string `json:"config_error,omitempty"`
	// Connected reports whether a connection was attempted and opened.
	Connected bool `json:"connected"`
	// OpenError is why it was not.
	OpenError string `json:"open_error,omitempty"`
	// Info is the driver and client versions, after a connection.
	Info *infoReport `json:"info,omitempty"`
	// Ping is the result of one liveness check.
	Ping string `json:"ping,omitempty"`
	// Health is the client's own state.
	Health string `json:"health,omitempty"`
	// Features is every feature of the inventory with the level and
	// scope this driver ADVERTISES. It is not evidence: see
	// neverProbedFeatures and the note doctor prints above the table.
	Features []featureReport `json:"advertised_features,omitempty"`
	// Limits are the limits it advertises, "not advertised" left as absent
	// rather than as zero.
	Limits []limitReport `json:"limits,omitempty"`
	// Kinds are the variant kinds in force for THIS target, from the
	// driver's own typed configuration.
	Kinds []kindReport `json:"kinds,omitempty"`
}

// infoReport is mq.DriverInfo, with the empty fields left out because the specification
// makes an empty string "the driver did not say" and a rendered empty field
// reads as a claim of emptiness.
type infoReport struct {
	Name          string `json:"name,omitempty"`
	Profile       string `json:"profile,omitempty"`
	DriverVersion string `json:"driver_version,omitempty"`
	ClientName    string `json:"client_name,omitempty"`
	ClientVersion string `json:"client_version,omitempty"`
	BrokerVersion string `json:"broker_version,omitempty"`
}

// featureReport is one row of the capability table.
//
// Every field here describes what the driver ADVERTISES. The JSON field is
// named "advertised_level" and not "level" for that reason: a consumer reading
// `level: native` would have no way to know it was reading a claim rather than
// a verified result, and this repository has already shipped two artefacts
// that presented one as the other.
type featureReport struct {
	Feature string `json:"feature"`
	// AdvertisedLevel is the level the driver claims.
	AdvertisedLevel string `json:"advertised_level"`
	Scope           string `json:"scope,omitempty"`
	Note            string `json:"note,omitempty"`
	Durable         bool   `json:"durable"`
	// NeverProbed marks a feature the conformance kit has no scenario for at
	// all, so that no run can verify or refute what the driver claims about
	// it. It is carried per row rather than left to the prose, because a
	// machine reading this output should be able to tell the difference
	// without parsing a paragraph.
	NeverProbed bool `json:"never_probed"`
}

// neverProbedFeatures lists features for which this CLI has no behavioural
// probe to cite. The generated-matrix check compares this list when
// conformance reports and their matrix have been generated.
var neverProbedFeatures = []string{
	"DurableStorage",
	"AtMostOnce",
	"AtomicBatchPublish",
	"BatchSettle",
	"IdempotentPublish",
	"Transactions",
	"ServerFilter",
	"RequestReply",
	"NativeTracing",
}

// isNeverProbed reports whether the kit has no scenario for a feature.
func isNeverProbed(name string) bool {
	for _, f := range neverProbedFeatures {
		if f == name {
			return true
		}
	}
	return false
}

// limitReport is one advertised limit. A limit the driver does not advertise
// is not in the list at all: the specification forbids reading an unknown as zero, and a
// JSON field of 0 is exactly that mistake.
type limitReport struct {
	Limit string `json:"limit"`
	Value int64  `json:"value"`
}

// variantReport is one family's auto binding in this build.
type variantReport struct {
	Family      string `json:"family"`
	Key         string `json:"key"`
	Package     string `json:"package"`
	Auto        string `json:"auto"`
	Provisional bool   `json:"provisional"`
	// Parameter is the DSN parameter that selects a kind, when one does.
	Parameter string `json:"parameter,omitempty"`
}

// kindReport is one variant kind in force for a target.
type kindReport struct {
	Family string `json:"family"`
	Key    string `json:"key"`
	Kind   string `json:"kind"`
	// Source is "dsn" when the DSN named the kind and "auto" when it left
	// the field at Auto and the binding chose.
	Source string `json:"source"`
	// Provisional carries the family's flag, so that a reader of one row
	// does not have to cross-reference the build table to learn that the
	// kind in force was chosen by a non-authoritative campaign.
	Provisional bool `json:"provisional"`
}

// envReport is one environment variable, by name and by set/unset. There is
// deliberately no Value field: the third place a credential can appear is an
// environment variable, and a struct with nowhere to put the value cannot
// print it.
type envReport struct {
	Name string `json:"name"`
	Set  bool   `json:"set"`
	// Note says what the variable does, so that a reader who has not set one
	// knows whether they should have.
	Note string `json:"note,omitempty"`
}

// interestingEnv are the environment variables that change what a driver
// does, reported by name so that "the credential is in the environment" is
// visible without the credential being.
func interestingEnv() []envReport {
	return []envReport{
		{Name: "IQMQ_DSN", Note: "the default target when no DSN argument is given"},
		{Name: "AWS_ACCESS_KEY_ID", Note: "drivers/sqs, drivers/snssqs: the SDK default chain"},
		{Name: "AWS_SECRET_ACCESS_KEY", Note: "drivers/sqs, drivers/snssqs: the SDK default chain"},
		{Name: "AWS_SESSION_TOKEN", Note: "drivers/sqs, drivers/snssqs: the SDK default chain"},
		{Name: "AWS_REGION", Note: "drivers/sqs, drivers/snssqs: the region when the DSN names none"},
		{Name: "GOOGLE_APPLICATION_CREDENTIALS", Note: "drivers/gcppubsub: the service-account key file"},
		{Name: "PUBSUB_EMULATOR_HOST", Note: "drivers/gcppubsub: the emulator endpoint"},
	}
}

// cmdDoctor is the doctor subcommand.
func cmdDoctor(e *cliEnv, args []string) int {
	fs := newFlagSet("doctor", e)
	var (
		output    = fs.String("o", "text", "output format: text or json")
		outputL   = fs.String("output", "", "output format: text or json")
		noConnect = fs.Bool("no-connect", false, "do not open a connection")
		timeout   = fs.Duration("timeout", 10*time.Second, "bound the connection and the ping")
		onlyDrv   = fs.Bool("drivers", false, "report the driver table and exit")
	)
	if code := parseFlags(fs, e, args, doctorUsage); code >= 0 {
		return code
	}
	format := *output
	if *outputL != "" {
		format = *outputL
	}
	if format != "text" && format != "json" {
		_, _ = io.WriteString(e.stderr, "iqmq doctor: --output takes text or json, not "+quote(format)+"\n")
		return exitUsage
	}
	if fs.NArg() > 1 {
		_, _ = io.WriteString(e.stderr, "iqmq doctor: takes at most one DSN\n"+doctorUsage)
		return exitUsage
	}

	target := fs.Arg(0)
	if target == "" && !*onlyDrv {
		if v, ok := e.getenv("IQMQ_DSN"); ok {
			target = v
		}
	}

	// Every byte of the report AND of any failure goes through the scrub,
	// holding the credentials this DSN and the environment carry. It is the
	// second layer: the first is that nothing below renders a parameter or a
	// configuration field itself.
	e, flush := e.scrubbed(format == "json", target)
	defer flush()
	out := e.stdout

	rep := doctorReport{
		Version:  version(),
		Go:       runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Build:    buildOf(),
		Variants: variantsOf(),
		Env:      envOf(e),
	}
	code := exitOK
	if target != "" && !*onlyDrv {
		t, tcode := e.inspect(target, *noConnect, *timeout)
		rep.Target = t
		code = tcode
		if code != exitOK {
			// The report itself goes to stdout, and a non-zero exit has to
			// say something on stderr as well: a script reads the status and
			// the operator reads the line, and a command that failed
			// silently on stderr is one whose failure a pipeline swallows.
			// It is also FuzzCLIArgs's own invariant.
			reason := firstNonEmpty(t.ConfigError, t.OpenError, t.Ping, "the target could not be resolved")
			_, _ = io.WriteString(e.stderr, "iqmq doctor: "+t.DSN+": "+reason+"\n")
		}
	}
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return fail(e, "doctor", err)
		}
		return code
	}
	writeDoctorText(out, &rep)
	return code
}

// buildOf describes this binary's driver table.
func buildOf() buildReport {
	var b buildReport
	for _, d := range drivers() {
		b.Drivers = append(b.Drivers, driverReport{Name: d.name, Module: d.module, Schemes: d.schemes})
	}
	for _, u := range unreachableDrivers() {
		b.Unreachable = append(b.Unreachable,
			moduleReport{Module: u.module, Reason: u.reason, Schemes: u.schemes})
	}
	b.Schemes = schemes()
	b.Provisional, b.Families = provisionalCount()
	return b
}

// variantsOf renders the build's variant bindings.
func variantsOf() []variantReport {
	fams := variantFamilies()
	out := make([]variantReport, 0, len(fams))
	for i := range fams {
		f := &fams[i]
		out = append(out, variantReport{
			Family: f.id, Key: f.key, Package: f.pkg,
			Auto: f.auto, Provisional: f.provisional, Parameter: f.param,
		})
	}
	return out
}

// envOf reports the interesting environment by name and by set/unset.
func envOf(e *cliEnv) []envReport {
	out := interestingEnv()
	for i := range out {
		_, ok := e.getenv(out[i].Name)
		out[i].Set = ok
	}
	return out
}

// inspect resolves one target. It returns the report and the exit code: a DSN
// that does not parse is a usage failure, a broker that cannot be reached is
// an operational one, and a driver the binary does not have is a usage failure
// naming what it does have.
func (e *cliEnv) inspect(target string, noConnect bool, timeout time.Duration) (*targetReport, int) {
	t := &targetReport{DSN: redactTarget(target), Parameters: paramNames(target)}
	scheme, err := dsn.Scheme(target)
	if err != nil {
		t.ConfigError = err.Error()
		return t, exitUsage
	}
	t.Scheme = scheme
	// The build-time table is asked first, because it is what carries the
	// typed `describe` and `resolved` steps below -- but it is not the
	// authority on what this process can OPEN. That is `e.registry`, which a
	// caller or a test may have given its own drivers, and the open at the
	// bottom of this function has always gone through it. Refusing here on
	// the table alone reported "no driver in this binary claims this scheme"
	// about a scheme the very next statement would have opened.
	entry, ok := entryForScheme(scheme)
	if !ok {
		name, known := e.registry.DriverName(scheme)
		if !known {
			t.ConfigError = "no driver in this binary claims this scheme; it opens: " +
				strings.Join(e.registry.Schemes(), " ")
			return t, exitUsage
		}
		// Registered but not in the table: there is no typed configuration to
		// render, and the open below still decides whether it works.
		t.Driver = name
		return e.connect(t, target, noConnect, timeout)
	}
	t.Driver, t.Module = entry.name, entry.module
	if entry.describe != nil {
		cfg, derr := entry.describe(target)
		if derr != nil {
			t.ConfigError = derr.Error()
			return t, exitUsage
		}
		t.Config = cfg
	}
	if entry.resolved != nil {
		kinds, kerr := entry.resolved(target)
		if kerr != nil {
			t.ConfigError = kerr.Error()
			return t, exitUsage
		}
		t.Kinds = kinds
	}
	return e.connect(t, target, noConnect, timeout)
}

// connect is inspect's second half: the open, and what the client reports
// about itself. It is its own function because a scheme the driver TABLE does
// not carry still reaches it through the registry.
func (e *cliEnv) connect(t *targetReport, target string, noConnect bool, timeout time.Duration) (*targetReport, int) {
	if noConnect {
		return t, exitOK
	}

	ctx, cancel := e.deadline(e.ctx, timeout)
	defer cancel()
	client, err := e.registry.Open(ctx, target)
	if err != nil {
		t.OpenError = err.Error()
		return t, codeFor(err)
	}
	defer func() { _ = client.Close() }()
	t.Connected = true
	t.Info = infoOf(client.Info())
	t.Features, t.Limits = capabilitiesOf(client.Capabilities())

	start := e.clock.Now()
	if err := client.Ping(ctx); err != nil {
		t.Ping = "failed: " + err.Error()
		t.Health = client.Health().State.String()
		return t, codeFor(err)
	}
	t.Ping = "ok in " + e.clock.Now().Sub(start).Round(time.Microsecond).String()
	t.Health = client.Health().State.String()
	return t, exitOK
}

// infoOf renders a DriverInfo, or nil when the driver said nothing at all.
func infoOf(i mq.DriverInfo) *infoReport {
	r := &infoReport{
		Name: i.Name, Profile: i.Profile, DriverVersion: i.DriverVersion,
		ClientName: i.ClientName, ClientVersion: i.ClientVersion, BrokerVersion: i.BrokerVersion,
	}
	if *r == (infoReport{}) {
		return nil
	}
	return r
}

// capabilitiesOf renders the feature inventory and the advertised limits.
//
// Every feature is reported, including the unsupported ones: "this driver does
// not do dead-lettering" is the answer somebody ran doctor for, and a table
// that listed only what works would make the absence invisible.
func capabilitiesOf(caps mq.Capabilities) ([]featureReport, []limitReport) {
	features := make([]featureReport, 0, mq.FeatureCount)
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		row := featureReport{
			Feature: f.String(), AdvertisedLevel: s.Level.String(),
			Note: s.Note, Durable: s.Level.Durable(),
			NeverProbed: isNeverProbed(f.String()),
		}
		if s.Scope != mq.ScopeNone {
			row.Scope = s.Scope.String()
		}
		features = append(features, row)
	}
	limits := make([]limitReport, 0, mq.LimitKeyCount)
	for i := range mq.LimitKeyCount {
		k := mq.LimitKey(i)
		if l := caps.Limit(k); l.Known {
			limits = append(limits, limitReport{Limit: k.String(), Value: l.Value})
		}
	}
	return features, limits
}

// writeDoctorText renders the report for a terminal.
func writeDoctorText(w io.Writer, r *doctorReport) {
	p := func(s string) { _, _ = io.WriteString(w, s) }
	row := func(k, v string) { p("  " + pad(k, 22) + v + "\n") }

	p("iqmq doctor\n\nbuild\n")
	row("version", r.Version)
	row("go", r.Go)
	row("platform", r.Platform)
	row("drivers", itoa(len(r.Build.Drivers))+" registered, "+itoa(len(r.Build.Schemes))+" schemes")
	row("variant families", itoa(r.Build.Families)+", "+itoa(r.Build.Provisional)+" provisionally bound")

	if r.Target != nil {
		t := r.Target
		p("\ntarget\n")
		row("dsn", t.DSN)
		if len(t.Parameters) > 0 {
			row("parameters", strings.Join(t.Parameters, " "))
		}
		row("scheme", t.Scheme)
		if t.Driver != "" {
			row("driver", t.Driver+" ("+t.Module+")")
		}
		if t.Config != "" {
			row("config", t.Config)
		}
		if t.ConfigError != "" {
			row("config", "REFUSED: "+t.ConfigError)
		}
		if t.OpenError != "" {
			row("connection", "FAILED: "+t.OpenError)
		}
		if t.Info != nil {
			row("driver info", infoLine(t.Info))
		}
		if t.Ping != "" {
			row("ping", t.Ping)
		}
		if t.Health != "" {
			row("health", t.Health)
		}
		if len(t.Kinds) > 0 {
			p("\nvariant kinds in force for this target\n")
			for _, k := range t.Kinds {
				note := "from the DSN"
				if k.Source == "auto" {
					note = "from the Auto binding"
				}
				if k.Provisional {
					note += ", provisional"
				}
				row(k.Family+" "+k.Key, pad(k.Kind, 18)+"("+note+")")
			}
		}
		if len(t.Features) > 0 {
			p("\ncapabilities AS ADVERTISED BY THIS DRIVER (the message contract)\n" +
				"  Read from Client.Capabilities(). This is what the driver claims about itself.\n" +
				"  It is NOT probe evidence: a conformance run verifying a claim is a separate fact,\n" +
				"  Generate fresh reports and docs/drivers/matrix.md to compare claims with probe results.\n" +
				"  A row marked [never probed] is one the conformance kit has NO scenario for in any\n" +
				"  run, so nothing can hold this driver to what it claims about it. Those nine are:\n" +
				"    " + strings.Join(neverProbedFeatures, " ") + "\n")
			for _, f := range t.Features {
				v := f.AdvertisedLevel
				if f.Scope != "" {
					v += " scope=" + f.Scope
				}
				if f.NeverProbed {
					v += "  [never probed]"
				}
				if f.Note != "" {
					v += "  " + f.Note
				}
				row(f.Feature, v)
			}
		}
		if t.Connected {
			p("\nlimits the driver advertises (an absent limit is NOT advertised, and is never zero)\n")
			if len(t.Limits) == 0 {
				row("(none)", "this driver advertises no limit at all")
			}
			for _, l := range t.Limits {
				row(l.Limit, i64toa(l.Value))
			}
		}
	}

	p("\nvariant bindings in this build \n")
	for _, v := range r.Variants {
		s := pad(v.Auto, 18)
		switch {
		case v.Provisional:
			s += "provisional"
		default:
			s += "ratified"
		}
		if v.Parameter != "" {
			s += "   dsn: " + v.Parameter + "="
		}
		row(v.Family+" "+v.Key, s)
	}

	if len(r.Build.Unreachable) > 0 {
		p("\ndriver modules this binary does NOT register\n")
		for _, u := range r.Build.Unreachable {
			p("  " + u.Module + "\n")
			if len(u.Schemes) > 0 {
				p("    claims: " + strings.Join(u.Schemes, " ") + "\n")
			}
			p("    " + u.Reason + "\n")
		}
	}
	if len(r.Env) > 0 {
		p("\nenvironment (names and set/unset only; no value is ever printed)\n")
		for _, ev := range r.Env {
			state := "not set"
			if ev.Set {
				state = "set"
			}
			row(ev.Name, pad(state, 10)+ev.Note)
		}
	}
	p("\nschemes this binary opens\n  " + strings.Join(r.Build.Schemes, " ") + "\n")
}

// infoLine renders a DriverInfo on one line, naming only the fields the driver
// filled in.
func infoLine(i *infoReport) string {
	parts := make([]string, 0, 6)
	for _, kv := range [][2]string{
		{"name", i.Name},
		{"profile", i.Profile},
		{"driver", i.DriverVersion},
		{"client", i.ClientName},
		{"client-version", i.ClientVersion},
		{"broker", i.BrokerVersion},
	} {
		if kv[1] != "" {
			parts = append(parts, kv[0]+"="+kv[1])
		}
	}
	return strings.Join(parts, " ")
}

// pad right-pads to n columns, with one trailing space for a value that is
// already longer, so two columns never run together.
func pad(s string, n int) string {
	if len(s) >= n {
		return s + " "
	}
	return s + strings.Repeat(" ", n-len(s))
}

// firstNonEmpty returns the first non-empty string, or the empty string.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// sortedStrings returns a sorted copy.
func sortedStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}
