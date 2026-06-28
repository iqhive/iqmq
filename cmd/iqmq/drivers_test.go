package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/dsn"
)

// TestRegistryHoldsEveryDriver is the build-time property the driver table's
// fixed order exists for: a scheme conflict must be a failure here rather than
// a half-registered registry at run time.
//
// It is not hypothetical. The sixteen shipped driver profiles CANNOT all be
// registered in one registry -- drivers/natsunified claims nats:// and
// natsjs://, which the two profile drivers already claim -- so this table is
// the place that decision is recorded.
func TestRegistryHoldsEveryDriver(t *testing.T) {
	reg, err := newRegistry()
	if err != nil {
		t.Fatalf("newRegistry: %v\nthe driver table has a scheme conflict", err)
	}
	got := reg.Schemes()
	want := schemes()
	sort.Strings(want)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("the registry holds %v and the table claims %v", got, want)
	}
	for _, s := range got {
		if _, ok := reg.Lookup(s); !ok {
			t.Errorf("the registry reports the scheme %q and cannot open it", s)
		}
		if _, ok := entryForScheme(s); !ok {
			t.Errorf("the registry holds %q and no table entry claims it", s)
		}
	}
	// Every scheme is claimed once. A duplicate would have failed above, so
	// this is the other direction: a scheme in two entries' lists.
	seen := map[string]string{}
	for _, d := range drivers() {
		for _, s := range d.schemes {
			if prev, ok := seen[s]; ok {
				t.Errorf("the scheme %q is claimed by both %s and %s", s, prev, d.name)
			}
			seen[s] = d.name
		}
	}
}

// TestUnregisteredModulesAreNotRegistered is the other half of the
// modules doctor reports as unreachable must actually be unreachable, or the
// report is wrong in the direction that matters least but is still wrong.
func TestUnregisteredModulesAreNotRegistered(t *testing.T) {
	registered := map[string]bool{}
	for _, d := range drivers() {
		registered[d.module] = true
	}
	for _, u := range unreachableDrivers() {
		if registered[u.module] {
			t.Errorf("%s is reported as unregistered and the table registers it", u.module)
		}
		if u.reason == "" {
			t.Errorf("%s is reported as unregistered with no reason, which is the half of the "+
				"report that stops it reading as 'iqmq has no such driver'", u.module)
		}
	}
	if len(unreachableDrivers()) == 0 {
		t.Error("no module is reported as unregistered; four shipped driver modules cannot be " +
			"reached by a DSN and the report has stopped saying so")
	}
}

// TestEveryDriverModuleIsAccountedFor walks the drivers/ tree and fails on a
// module that is neither registered nor explained.
//
// It is the check that makes the driver table unable to go quietly stale: a
// seventeenth driver module lands and this test names it.
func TestEveryDriverModuleIsAccountedFor(t *testing.T) {
	root := repoRoot(t)
	var found []string
	err := filepath.WalkDir(filepath.Join(root, "drivers"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "go.mod" {
			return nil
		}
		rel, rerr := filepath.Rel(root, filepath.Dir(path))
		if rerr != nil {
			return rerr
		}
		found = append(found, "bitbucket.org/iqhive/iqmq/"+filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walking drivers/: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("no driver module was found at all, so this check proves nothing")
	}
	accounted := map[string]bool{}
	for _, d := range drivers() {
		accounted[d.module] = true
	}
	for _, u := range unreachableDrivers() {
		accounted[u.module] = true
	}
	for _, m := range found {
		if !accounted[m] {
			t.Errorf("the driver module %s is neither registered by cmd/iqmq nor listed in "+
				"unreachableDrivers with a reason, so the CLI silently cannot open it", m)
		}
	}
	t.Logf("%d driver modules, %d registered, %d explained",
		len(found), len(drivers())-1, len(unreachableDrivers()))
}

// TestVariantTableCoversEveryAutoBinding walks the repository's own generated
// auto.go files and fails on a binding with no row in the variant table.
//
// Nothing generates that table, so this is the only thing that stops it going
// stale when a family is added.
func TestVariantTableCoversEveryAutoBinding(t *testing.T) {
	root := repoRoot(t)
	// The packages a CLI can reach: the root module's internals, driverkit
	// and codec. X/apibench's fourteen families are Class B rows --
	// API shapes already frozen into the surface, with no kind to resolve at
	// run time -- and the benchmarks module is not linked at all.
	decl := regexp.MustCompile(`const ([A-Za-z]*)AutoKind = `)
	have := map[string]bool{}
	for _, f := range variantFamilies() {
		have[f.pkg+"/"+f.key] = true
	}
	byPkg := map[string]int{}
	for _, f := range variantFamilies() {
		byPkg[f.pkg]++
	}

	var missing []string
	roots := []string{"internal", "driverkit", "codec"}
	for _, dir := range roots {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasPrefix(d.Name(), "auto") || !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			rel, rerr := filepath.Rel(root, filepath.Dir(path))
			if rerr != nil {
				return rerr
			}
			pkg := filepath.ToSlash(rel)
			n := len(decl.FindAllStringSubmatch(string(data), -1))
			if n == 0 {
				return nil
			}
			if byPkg[pkg] < n {
				missing = append(missing, pkg+" declares "+itoa(n)+
					" Auto binding(s) and the variant table has "+itoa(byPkg[pkg]))
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
	if len(missing) > 0 {
		t.Errorf("cmd/iqmq's variant table does not cover every Auto binding in the "+
			"repository, so doctor under-reports what this binary was built with:\n  %s",
			strings.Join(missing, "\n  "))
	}
	if len(variantFamilies()) < 20 {
		t.Errorf("the variant table holds %d families; the specification has far more runtime-selectable "+
			"rows than that and the table has probably lost some", len(variantFamilies()))
	}
}

// TestVariantSweepParametersAreRealDSNParameters is the check that a sweep
// table cannot be wrong about a driver.
//
// A family's sweep says "scheme X selects this family with parameter P, whose
// values are these kind slugs". Every part of that can be wrong, and a
// --variants run would then fail at the driver with a parameter it does not
// have -- or, worse, silently sweep one arm. This builds a DSN per (scheme,
// parameter, kind) and holds the driver to accepting it.
func TestVariantSweepParametersAreRealDSNParameters(t *testing.T) {
	// A minimal DSN per scheme that the driver accepts on its own, so that a
	// failure is about the swept parameter and not about the rest.
	base := map[string]string{
		"kafka":    "kafka://127.0.0.1:9092",
		"kafkago":  "kafkago://127.0.0.1:9092",
		"redis":    "redis://127.0.0.1:6379",
		"sqs":      "sqs://key:secret@127.0.0.1:9324?region=elasticmq",
		"snssqs":   "snssqs://key:secret@127.0.0.1:4566?region=us-east-1",
		"rabbitmq": "rabbitmq://guest:guest@127.0.0.1:5672",
	}
	checked := 0
	for _, f := range variantFamilies() {
		if f.param == "" {
			continue
		}
		if len(f.schemes) == 0 {
			t.Errorf("family %s %s names the parameter %q and no scheme, so nothing can sweep it",
				f.id, f.key, f.param)
			continue
		}
		for _, scheme := range f.schemes {
			b, ok := base[scheme]
			if !ok {
				t.Errorf("family %s %s sweeps scheme %q and this test has no base DSN for it, "+
					"so the claim is unchecked", f.id, f.key, scheme)
				continue
			}
			entry, ok := entryForScheme(scheme)
			if !ok {
				t.Errorf("family %s %s sweeps the scheme %q, which no driver claims", f.id, f.key, scheme)
				continue
			}
			if entry.describe == nil {
				t.Errorf("family %s %s sweeps %q and that driver cannot parse a DSN", f.id, f.key, scheme)
				continue
			}
			// dsnKinds is the family's kinds minus Auto (the zero kind is what
			// "the DSN did not say" means, and every driver refuses the
			// spelling) and minus the ones a connection string cannot carry.
			kinds := f.dsnKinds()
			if len(kinds) == 0 {
				t.Errorf("family %s %s names the parameter %q and no kind a DSN can select",
					f.id, f.key, f.param)
			}
			for _, kind := range kinds {
				target, err := spliceParam(b, f.param, kind)
				if err != nil {
					t.Errorf("family %s %s: splicing %s=%s into %s: %v", f.id, f.key, f.param, kind, scheme, err)
					continue
				}
				if _, err := entry.describe(target); err != nil {
					t.Errorf("family %s %s claims %s selects it with %s=%s, and the driver "+
						"refuses that DSN: %v", f.id, f.key, scheme, f.param, kind, err)
					continue
				}
				checked++
			}
		}
	}
	if checked == 0 {
		t.Fatal("no sweep parameter was exercised, so this check proves nothing")
	}
	t.Logf("checked %d (scheme, parameter, kind) combinations", checked)
}

// TestResolvedKindsCoverEverySweptFamily ties the two tables together: a family a target can SELECT through its DSN must be a family doctor can
// REPORT the resolved kind of, or `bench --variants` sweeps something doctor
// cannot explain.
func TestResolvedKindsCoverEverySweptFamily(t *testing.T) {
	base := map[string]string{
		"kafka":    "kafka://127.0.0.1:9092",
		"kafkago":  "kafkago://127.0.0.1:9092",
		"redis":    "redis://127.0.0.1:6379",
		"sqs":      "sqs://key:secret@127.0.0.1:9324?region=elasticmq",
		"snssqs":   "snssqs://key:secret@127.0.0.1:4566?region=us-east-1",
		"rabbitmq": "rabbitmq://guest:guest@127.0.0.1:5672",
	}
	for _, f := range variantFamilies() {
		if f.param == "" {
			continue
		}
		for _, scheme := range f.schemes {
			entry, ok := entryForScheme(scheme)
			if !ok {
				continue
			}
			if entry.resolved == nil {
				t.Errorf("scheme %q selects family %s %s and the driver entry reports no "+
					"resolved kinds at all", scheme, f.id, f.key)
				continue
			}
			rows, err := entry.resolved(base[scheme])
			if err != nil {
				t.Errorf("resolving %q: %v", scheme, err)
				continue
			}
			found := false
			for _, r := range rows {
				if r.Family == f.id {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("scheme %q selects family %s (%s) through %s= and doctor does not "+
					"report a resolved kind for it", scheme, f.id, f.key, f.param)
			}
		}
	}
}

// TestDescribeRendersTheDriversOwnConfig: a driver with a describe must
// actually produce something, or doctor's configuration line is empty for it.
func TestDescribeRendersTheDriversOwnConfig(t *testing.T) {
	targets := map[string]string{
		"kafka":          "kafka://127.0.0.1:9092/acme.",
		"kafkago":        "kafkago://127.0.0.1:9092",
		"nats":           "nats://127.0.0.1:4222",
		"natsjs":         "natsjs://127.0.0.1:4222?stream=ORDERS",
		"nsq":            "nsq://127.0.0.1:4150",
		"redis":          "redis://127.0.0.1:6379",
		"sqs":            "sqs://key:secret@127.0.0.1:9324?region=elasticmq",
		"snssqs":         "snssqs://key:secret@127.0.0.1:4566?region=us-east-1",
		"rabbitmq":       "rabbitmq://guest:guest@127.0.0.1:5672",
		"pulsar":         "pulsar://127.0.0.1:6650/public/default",
		"gcppubsub":      "gcppubsub://127.0.0.1:8085/iqmq-test", // the emulator, and the project in the path
		"activemq":       "activemq://127.0.0.1:5672?broker=artemis",
		"activemq+stomp": "activemq+stomp://127.0.0.1:61613?broker=artemis",
		// The AMQP 1.0 core refuses SASL PLAIN in the clear unless the caller
		// says so deliberately, which is the specification's "insecure transport requires
		// an option whose name contains Insecure" working as intended.
		"rabbitmq+amqp10": "rabbitmq+amqp10://guest:guest@127.0.0.1:5672?insecureplainsasl=true",
	}
	checked := 0
	for scheme, target := range targets {
		entry, ok := entryForScheme(scheme)
		if !ok {
			t.Errorf("no driver claims the scheme %q", scheme)
			continue
		}
		if entry.describe == nil {
			continue
		}
		got, err := entry.describe(target)
		if err != nil {
			t.Errorf("%s: describing %s: %v\nthe test's DSN is wrong, or the driver's "+
				"parameter table changed", scheme, redactTarget(target), err)
			continue
		}
		if strings.TrimSpace(got) == "" {
			t.Errorf("%s: the driver's Config.String rendered nothing, so doctor's "+
				"configuration line is blank", scheme)
			continue
		}
		checked++
	}
	if checked < 10 {
		t.Errorf("only %d driver configurations were rendered; the table has fifteen entries "+
			"and this check has lost its coverage", checked)
	}
}

// TestSchemeListIsSortedAndComplete: the list a refusal prints has to be
// readable and has to be the whole set.
func TestSchemeListIsSortedAndComplete(t *testing.T) {
	got := schemes()
	if !sort.StringsAreSorted(got) {
		t.Errorf("schemes() is not sorted: %v", got)
	}
	for _, want := range []string{
		"mem", "memory", "kafka", "kafkago", "nats", "natsjs", "nsq", "gcppubsub",
		"sqs", "snssqs", "rabbitmq", "rabbitmq+amqp10", "activemq", "activemq+stomp",
		"redis", "pulsar", "pulsar+ssl",
	} {
		found := false
		for _, s := range got {
			if s == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("the scheme %q is not claimed by this binary: %v", want, got)
		}
	}
	// And the CLI does not claim a scheme it cannot honour.
	for _, s := range got {
		if _, err := dsn.Scheme(s + "://x"); err != nil {
			t.Errorf("the table claims %q, which is not a usable DSN scheme: %v", s, err)
		}
	}
}

// TestEveryAutoBindingIsProvisionalWhileNoHostIsAuthoritative is the
// machine-checked half of the plan's row: "ADRs stay 'Accepted
// (provisional, single host)' with the v1 caveat unless a pinned host exists".
//
// `doctor` reports "N families, M provisionally bound" and both numbers are
// derived -- `provisionalCount` counts the table and reads each family's own
// generated `AutoProvisional` constant -- so the LINE cannot go stale. What
// could go stale in silence is the answer: flipping one constant to false
// would make doctor say "28, 27" and nothing in `make ci` would notice, and
// the claim doctor is making is about a property of the HOST the campaign ran
// on rather than about the family.
//
// So the assertion is tied to the evidence instead of being a tautology.
// `the benchmark budgets` declares `hosts: {authoritative: []}` -- no pinned
// benchmark host exists, which is also why every figure in the
// repository carries a provisional caveat -- and while that list is empty
// every binding in this binary must be provisional. The day somebody adds a
// host to it, this test fails and says what re-ratification means, which is
// the trigger the row's escape clause needs and did not have.
func TestEveryAutoBindingIsProvisionalWhileNoHostIsAuthoritative(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "benchmarks", "budgets.yaml"))
	if err != nil {
		t.Fatalf("reading budgets.yaml: %v", err)
	}
	// One line, matched rather than parsed: this module is stdlib-only and the
	// budgets parser lives in the benchmarks module. An `authoritative:` whose
	// value is not the empty list -- on the line or as a block below it -- does
	// not match, which is the safe direction: the test then fails and asks for
	// a decision.
	empty := regexp.MustCompile(`(?m)^\s*authoritative:\s*\[\s*\]`)
	authoritative := !empty.Match(data)

	fams := variantFamilies()
	if len(fams) == 0 {
		t.Fatal("the variant table is empty")
	}
	var settled []string
	for _, f := range fams {
		if !f.provisional {
			settled = append(settled, f.id+" "+f.key+" ("+f.pkg+")")
		}
	}
	if authoritative {
		if len(settled) == 0 {
			t.Errorf("the benchmark budgets now declares an authoritative host, and all %d "+
				"variant bindings are still marked provisional.\nRe-ratify the bake-off on that "+
				"host: re-run the campaign, re-render the ADRs, drop the \"(provisional, single "+
				"host)\" qualifier from the ones it decides, and set AutoProvisional = false in "+
				"the auto.go files it re-decides (the contribution policy).", len(fams))
		}
		return
	}
	if len(settled) > 0 {
		t.Errorf("the benchmark budgets declares `hosts: {authoritative: []}`, so no benchmark "+
			"on this host can settle a default -- and %d of %d variant bindings are marked "+
			"NOT provisional:\n  %s\n`iqmq doctor` reports that number, so this is a claim the "+
			"binary makes to its user. Either add the pinned host to budgets.yaml and re-ratify, "+
			"or set AutoProvisional = true (the contribution policy and (5)).",
			len(settled), len(fams), strings.Join(settled, "\n  "))
	}
	t.Logf("no authoritative host; %d of %d bindings provisional", len(fams)-len(settled), len(fams))
}
