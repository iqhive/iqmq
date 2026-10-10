package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// tmpl is a §7.2-shaped template: fifteen level-2 sections, which is the
// count checkSections holds the real template to.
var tmpl = "# T\n\n" + sections(1)

// sections renders the template's sections from the nth onwards.
func sections(from int) string {
	var b strings.Builder
	for i := from; i <= 15; i++ {
		b.WriteString("## S" + strconv.Itoa(i) + "\n")
	}
	return b.String()
}

// TestSlugMatchesPythonMarkdown pins the one piece of arithmetic this tool
// cannot get wrong: the heading id mkdocs actually mints.
//
// Every expectation below was read out of the built site's HTML rather than
// derived from the algorithm, which is why the two-dash cases are here: nine
// links in this repository used GitHub's slug, mkdocs reported them as INFO,
// and `--strict` was green.
func TestSlugMatchesPythonMarkdown(t *testing.T) {
	for _, tc := range []struct{ heading, want string }{
		{"Ack / nack / extend mapping", "ack-nack-extend-mapping"},
		{"Config & DSN", "config-dsn"},
		{"Client & version policy", "client-version-policy"},
		{"Publish path & confirmation boundary", "publish-path-confirmation-boundary"},
		{"At-least-once window", "at-least-once-window"},
		{"The rebalance sequence (§8.5)", "the-rebalance-sequence-85"},
		{"What §7.2 marks (verify), and what was actually verified", "what-72-marks-verify-and-what-was-actually-verified"},
		{"Conformance: the diff against `redis-streams.json`", "conformance-the-diff-against-redis-streamsjson"},
		{"Coverage: which profiles are measured, and which are not", "coverage-which-profiles-are-measured-and-which-are-not"},
		{"**Bold** heading", "bold-heading"},
	} {
		if got := slug(tc.heading); got != tc.want {
			t.Errorf("slug(%q) = %q, want %q", tc.heading, got, tc.want)
		}
	}
}

// TestHeadingLinesIgnoresFencedCode: a `# comment` in a driver page's shell
// block is not a heading, and a markdown link in a code sample is not a link.
func TestHeadingLinesIgnoresFencedCode(t *testing.T) {
	body := "# Real\n\n```sh\n# not a heading\n[nope](missing.md)\n```\n\n## Also real\n"
	p := parse("x.md", body)
	if len(p.headings) != 2 {
		t.Fatalf("headings = %q, want the two outside the fence", p.headings)
	}
	if got := p.links(); len(got) != 0 {
		t.Errorf("links = %q, want none: the only link is inside a fence", got)
	}
}

// TestLinksIgnoreInlineCode: a link pattern inside backticks is not a link.
//
// `mq.NativeAs[T](d)` in docs/migration/gocloud.md was reported as a broken
// link to "d" for exactly as long as this tool has existed, because the link
// regexp was applied to the raw line. Both directions are checked here: the
// code span yields nothing, and a real link on the same line still yields its
// target -- a mask that swallowed the whole line would have "fixed" the false
// positive by removing the check.
func TestLinksIgnoreInlineCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{
			"a generic call in a code span",
			"# x\n\n| `topic.As(&native)` | `mq.NativeAs[T](d)` | same |\n",
			nil,
		},
		{
			"a real link beside a code span",
			"# x\n\nsee `mq.NativeAs[T](d)` and [the guide](guide.md)\n",
			[]string{"guide.md"},
		},
		{
			"a link whose TEXT is code",
			"# x\n\n[`redis.md`](redis.md#ack-nack-extend-mapping)\n",
			[]string{"redis.md#ack-nack-extend-mapping"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parse("x.md", tc.body).links()
			if len(got) != len(tc.want) {
				t.Fatalf("links = %q, want %q", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("links = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

// TestDuplicateHeadingsMintBothAnchors: python-markdown appends _1 to the
// second identical heading and a link may name either form.
func TestDuplicateHeadingsMintBothAnchors(t *testing.T) {
	p := parse("x.md", "## Ordering\n\n## Ordering\n")
	for _, a := range []string{"ordering", "ordering_1"} {
		if !p.anchors[a] {
			t.Errorf("anchor %q missing from %v", a, p.anchors)
		}
	}
}

// TestChecksFailOnABrokenTree runs the real checks over a fixture tree that
// carries one of each defect, so every branch that can fail is watched
// failing here as well as on the repository.
func TestChecksFailOnABrokenTree(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("docs/drivers/_template.md", tmpl)
	// A driver page missing S2, with S3 before S1, and a link into
	// a sibling's anchor in GitHub's spelling.
	write("docs/drivers/broken.md", "# Broken\n\n## S3\n## S1\n"+sections(4)+"\n[x](other.md#a--b)\n[y](nowhere.md)\n")
	write("docs/drivers/other.md", "# Other\n\n"+sections(1)+"\n## A b\n")
	write("docs/orphan.md", "# Orphan\n")
	write("mkdocs.yml", "docs_dir: docs\nvalidation:\n  anchors: info\nnav:\n  - D: drivers/broken.md\n  - O: drivers/other.md\n  - M: missing.md\n")
	write("README.md", "# r\n\n## License\n\nExactly-once everywhere.\n")

	c := &checker{root: root}
	c.load()
	c.checkSections()
	c.checkLinks()
	c.checkNav()
	c.checkREADME()

	want := []string{
		`docs/drivers/broken.md: §7.2 section(s) missing: S2`,
		`"S3" comes before "S1"`,
		`has no anchor "a--b"`,
		`nowhere.md does not exist`,
		`validation.anchors is "info"`,
		`nav names docs/missing.md, which does not exist`,
		`orphan.md`,
		`§13 section "Thirty seconds" is missing`,
		`unqualified exactly-once claim`,
	}
	all := strings.Join(c.problems, "\n")
	for _, w := range want {
		if !strings.Contains(all, w) {
			t.Errorf("no problem contains %q; got:\n%s", w, all)
		}
	}
	if len(c.report) != 0 {
		t.Errorf("a tree this broken reported %d successes: %q", len(c.report), c.report)
	}
}

// TestChecksPassOnAGoodTree is the other half: the same shapes, correct. A
// check that cannot pass is as useless as one that cannot fail, and six in
// this repository turned out to be one or the other.
func TestChecksPassOnAGoodTree(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("docs/drivers/_template.md", tmpl)
	// The template is declared in not_in_nav AND linked, because not_in_nav
	// excuses a page from a nav ENTRY and not from being reachable.
	write("docs/drivers/good.md", "# Good\n\n"+sections(1)+"\n[x](other.md#a-b)\n[gen](good.capabilities.md)\n[m](../performance/methodology.md)\n[o](../performance/overhead.md)\n[t](_template.md)\n`mq.NativeAs[T](d)`\n")
	write("docs/drivers/other.md", "# Other\n\n"+sections(1)+"\n## A b\n")
	write("docs/drivers/good.capabilities.md", "| a | b |\n")
	write("mkdocs.yml", "docs_dir: docs\nvalidation:\n  anchors: warn\nnot_in_nav: |\n  drivers/_template.md\nnav:\n  - D: drivers/good.md\n  - O: drivers/other.md\n")
	readme := "# r\n\n"
	for _, s := range readmeOrder {
		readme += "## " + s + "\n\n"
		if s == "Performance" {
			readme += "[Methodology](docs/performance/methodology.md).\n\n"
		}
	}
	write("README.md", readme)
	write("docs/performance/methodology.md", "# m\n")
	write("docs/performance/overhead.md", "# o\n")

	c := &checker{root: root}
	c.load()
	c.checkSections()
	c.checkLinks()
	c.checkNav()
	c.checkREADME()
	if len(c.problems) != 0 {
		t.Errorf("a correct tree reported problems:\n%s", strings.Join(c.problems, "\n"))
	}
	if len(c.report) != 4 {
		t.Errorf("report = %q, want one line per check", c.report)
	}
}

// TestGluedNavEntriesAreCaught is the shape of damage a bad merge does to a
// YAML file and no check here could see: two nav entries joined onto one line.
// mkdocs refuses such a file outright -- "mapping values are not allowed here"
// -- while this tool read its 96 nav entries and passed, because it scans
// lines rather than parsing YAML (ADR-002 keeps a YAML library out of the root
// module). The `docs` workflow went red on it and `make docs-check` stayed
// green, which is the disagreement the check exists to end.
//
// The second case is the regression that found itself: \s matches a NEWLINE in
// Go's regexp, so the first version of the pattern spanned two perfectly good
// consecutive lines and reported 54 problems in a file that had one.
func TestGluedNavEntriesAreCaught(t *testing.T) {
	for name, tc := range map[string]struct {
		nav  string
		want bool
	}{
		"two entries on one line": {
			nav:  "  - A: a.md  - B: b.md\n",
			want: true,
		},
		"an entry and a section header on one line": {
			nav:  "  - A: a.md  - Section:\n",
			want: true,
		},
		"two entries on their own lines": {
			nav:  "  - A: a.md\n  - B: b.md\n",
			want: false,
		},
		"a section header then its entry": {
			nav:  "  - Section:\n      - A: a.md\n",
			want: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := gluedNavRe.MatchString("nav:\n" + tc.nav)
			if got != tc.want {
				t.Errorf("gluedNavRe on %q = %v, want %v", tc.nav, got, tc.want)
			}
		})
	}
}

// ------------------------------------------------- §2.7 maturity, §16 clause 10

// clientsGo is the native-baseline registry in the shape baselines() parses.
const clientsGo = `package nativebench

type Client struct{ Slug, Driver string }

func Clients() []Client {
	return []Client{
		{Slug: "raw", Driver: "good"},
		{Slug: "lonely", Driver: "unmeasured"},
		{Slug: "emu", Driver: "emulated"},
	}
}
`

// emulatedRow is the "Not measured" row that carries §2.7's emulator
// exemption, kept separate so a case below can replace or remove exactly it.
const emulatedRow = "| `emulated` | **emulator only (§10.5 layer 4)** | a §10.5 layer-4 run against the managed service |\n"

// overheadMd publishes a comparison for "raw" and for nothing else. The
// second table is the shape the check must NOT read as a comparison: it is
// docs/performance/overhead.md's own "Not measured" table, where an arm with
// no denominator is recorded with the reason it has none -- and its middle row
// NAMES a BenchmarkNative slug while publishing no comparison for it, which
// is what the real page does too ("a second `goamqp` session, against the
// RabbitMQ container"). A pattern that dropped the BenchmarkDriver cell would
// read that row as a published comparison, so the Stable case below is only a
// real test while this row is here.
//
// The headings are load-bearing now that the obstacle kinds are read: the
// exemption is one row of one named SECTION, so `obstacles` cuts the page at
// `### Not measured` rather than matching every bolded second cell -- the real
// page's §10.2 tables bold a breach, and a shape reader would take those for
// obstacles the next time one of them moved.
const overheadMd = `# Overhead

### Measured

| Arm | Raw-client denominator | Broker |
|---|---|---|
| ` + "`BenchmarkDriver/good`" + ` | ` + "`BenchmarkNative/raw`" + ` | container |

### Not measured

| Profile | Obstacle | What it would take |
|---|---|---|
| ` + "`unmeasured`" + ` | **not comparable (§10.5)** | one boundary for both arms |
| ` + "`good`" + ` | **no denominator** | a second ` + "`BenchmarkNative/lonely`" + ` session, against this broker |
` + emulatedRow

// evidence is the generated-capability link §2.7's Beta bar and above asks a
// page to cite, for the `emulated` driver.
const evidence = "\n[e](emulated.capabilities.md)\n"

// waiver renders a gate waiver line in the shape waivedGatesRe fixes, with
// the blank line that keeps it a paragraph of its own.
func waiver(gates string) string {
	return "**Stable gates waived:** " + gates + " — owner decision 2026-06-12 (assumption C46.6).\n\n"
}

// driverPage renders a driver page: the maturity line where
// docs/drivers/_template.md puts it, anything the case wants ABOVE the first
// section -- which is where a gate waiver belongs and the only place
// checkStableGates accepts one -- then §7.2's sections, then anything after.
func driverPage(level, tier, front, extra string) string {
	return "# P\n\n<!-- c -->\n\n**Maturity:** " + level + " · **Tier:** " + tier +
		" · **Owner:** o\n\n" + front + sections(1) + extra
}

// suitesJSON renders the suite manifest, with the `emulated` entry's driver
// and run kinds given per case. The `extra` entry has no reports and exists
// for checkDriverPagesExist's quiet count; `good` declares every gate so that
// a case promoting IT to Stable fails on the one thing it is breaking.
func suitesJSON(driver string, runs ...string) string {
	q := make([]string, len(runs))
	for i, r := range runs {
		q[i] = strconv.Quote(r)
	}
	return `{"suites":[` +
		`{"name":"good","driver":"good","runs":["integration","chaos","matrix","soak"],"reports":["good.json"]},` +
		`{"name":"emulated","driver":` + strconv.Quote(driver) + `,"runs":[` + strings.Join(q, ",") + `],"reports":["emulated.json"]},` +
		`{"name":"extra","reports":[]}]}`
}

// maturityTree writes a tree that checkMaturity and checkDriverPagesExist
// both pass over, and returns the root. Each case below breaks exactly one
// thing in it.
func maturityTree(t *testing.T, mutate func(write func(rel, body string))) *checker {
	t.Helper()
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	page := func(level, tier, extra string) string {
		return driverPage(level, tier, "", extra)
	}
	write("docs/drivers/_template.md", page("Experimental", "N", ""))
	write("docs/drivers/good.md", page("Beta", "2", "\n[e](good.capabilities.md)\n"))
	write("docs/drivers/good.capabilities.md", "| a | b |\n")
	// `emulated` is the second driver, and the only one the §2.7 emulator
	// exemption can reach: a baseline registered, no published comparison,
	// and an **emulator only (§10.5 layer 4)** row that says why. It is Beta
	// in the control, so the cases below promote it with ONE write.
	write("docs/drivers/emulated.md", page("Beta", "1", evidence))
	write("docs/drivers/emulated.capabilities.md", "| a | b |\n")
	write("docs/performance/overhead.md", overheadMd)
	write("docs/drivers/index.md", "# Guide\n\n| Driver | Page | Maturity · Tier | Choose it when |\n|---|---|---|---|\n| Good | [good.md](good.md) | Beta · 2 | always |\n")
	write(ClientsPath, clientsGo)
	write(SuitesPath, suitesJSON("emulated", "integration", "chaos", "matrix", "soak"))
	mutate(write)
	c := &checker{root: root}
	c.load()
	c.checkMaturity()
	c.checkDriverPagesExist()
	return c
}

// TestMaturityPassesOnAGoodTree is the control. Without it the cases below
// cannot tell "fails on the defect" from "fails always".
func TestMaturityPassesOnAGoodTree(t *testing.T) {
	c := maturityTree(t, func(func(string, string)) {})
	if len(c.problems) != 0 {
		t.Errorf("a correct tree reported problems:\n%s", strings.Join(c.problems, "\n"))
	}
	if len(c.report) != 3 {
		t.Errorf("report = %q, want one line per check", c.report)
	}
}

// TestMaturityFailsOnEachDefect: one case per branch that can fail, each
// breaking exactly one thing in the tree the control passes over.
func TestMaturityFailsOnEachDefect(t *testing.T) {
	page := func(level, tier, extra string) string {
		return driverPage(level, tier, "", extra)
	}
	for name, tc := range map[string]struct {
		mutate func(write func(rel, body string))
		want   string
	}{
		"no maturity line at all": {
			func(w func(string, string)) { w("docs/drivers/good.md", "# P\n\n"+sections(1)) },
			"no `**Maturity:**` line",
		},
		"the line is not on line 5": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", "# P\n\n**Maturity:** Beta · **Tier:** 2\n\n"+sections(1)+"\n[e](good.capabilities.md)\n")
			},
			"is line 3, not line 5",
		},
		"two maturity lines": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", page("Beta", "2", "\n**Maturity:** Stable · **Tier:** 1\n\n[e](good.capabilities.md)\n"))
			},
			"2 `**Maturity:**` lines",
		},
		"a level §2.7 does not list": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", page("Production", "2", "\n[e](good.capabilities.md)\n"))
			},
			`maturity "Production" is not one of §2.7's levels`,
		},
		"a tier §2.7 does not list": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", page("Beta", "4", "\n[e](good.capabilities.md)\n"))
			},
			`tier "4" is not one of §2.7's tiers`,
		},
		"no tier at all": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", "# P\n\n<!-- c -->\n\n**Maturity:** Beta\n\n"+sections(1)+"\n[e](good.capabilities.md)\n")
			},
			"carries no `**Tier:**`",
		},
		"Beta with no generated evidence cited": {
			func(w func(string, string)) { w("docs/drivers/good.md", page("Beta", "2", "")) },
			`declares Beta and links no generated capability evidence`,
		},
		"Stable with no native baseline registered": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", page("Stable", "2", "\n[e](good.capabilities.md)\n"))
				w(ClientsPath, "package nativebench\n\nfunc Clients() []Client {\n\treturn []Client{{Slug: \"raw\", Driver: \"other\"}}\n}\n")
			},
			`registers no native baseline with Driver: "good"`,
		},
		"Stable whose baseline no published comparison pairs": {
			func(w func(string, string)) {
				w("docs/drivers/good.md", page("Stable", "2", "\n[e](good.capabilities.md)\n"))
				w(ClientsPath, "package nativebench\n\nfunc Clients() []Client {\n\treturn []Client{{Slug: \"lonely\", Driver: \"good\"}}\n}\n")
			},
			"pairs no driver arm with BenchmarkNative/lonely",
		},
		// ------------------------------------- §2.7's emulator exemption
		//
		// The exemption's whole risk is being WIDER than the row that
		// grants it, so each case below is one way of not having that row.
		"not comparable does not exempt a Stable page": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence))
				w("docs/performance/overhead.md", strings.Replace(overheadMd, emulatedRow,
					"| `emulated` | **not comparable (§10.5)** | one boundary for both arms |\n", 1))
			},
			"which is not §2.7's emulator exemption",
		},
		"a Stable page with no published obstacle at all": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence))
				w("docs/performance/overhead.md", strings.Replace(overheadMd, emulatedRow, "", 1))
			},
			`records no obstacle for "emulated"`,
		},
		"the section the exemption is read out of is gone": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence))
				w("docs/performance/overhead.md", strings.Replace(overheadMd, "### Not measured\n", "", 1))
			},
			`has no "### Not measured" section`,
		},
		// ------------------------------------- §2.7's chaos/matrix/soak gates
		"a Stable page whose chaos gate is neither declared nor waived": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence))
				w(SuitesPath, suitesJSON("emulated", "integration", "matrix", "soak"))
			},
			`declares no [chaos] run for "emulated"`,
		},
		"a gate waiver that outlived its reason": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", driverPage("Stable", "1", waiver("chaos"), evidence))
			},
			"waiver still names [chaos]",
		},
		"a gate waiver naming a gate §2.7 does not list": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", driverPage("Stable", "1", waiver("race"), evidence))
			},
			"names [race], which [chaos matrix soak] does not list",
		},
		"a gate waiver below the first section heading": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence+"\n"+waiver("chaos")))
				w(SuitesPath, suitesJSON("emulated", "integration", "matrix", "soak"))
			},
			"below the first section heading",
		},
		"a gate waiver citing no assumption": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", driverPage("Stable", "1",
					"**Stable gates waived:** chaos — owner decision 2026-06-12.\n\n", evidence))
				w(SuitesPath, suitesJSON("emulated", "integration", "matrix", "soak"))
			},
			"cites no assumption",
		},
		"two gate waiver lines": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", driverPage("Stable", "1", waiver("chaos")+waiver("soak"), evidence))
			},
			"2 `**Stable gates waived:**` lines",
		},
		"a Stable page whose driver names no suite": {
			func(w func(string, string)) {
				w("docs/drivers/emulated.md", page("Stable", "1", evidence))
				w(SuitesPath, suitesJSON("ghost", "integration", "chaos", "matrix", "soak"))
			},
			`holds no suite with "emulated" as its driver`,
		},
		"a named driver's capability page does not exist": {
			func(w func(string, string)) {
				w(SuitesPath, `{"suites":[{"name":"good","reports":["ghost.json"]}]}`)
			},
			"docs/drivers/ghost.capabilities.md does not exist",
		},
		"the selection guide's level disagrees with the page": {
			func(w func(string, string)) {
				w("docs/drivers/index.md", "# Guide\n\n| Driver | Page | Maturity · Tier | Choose it when |\n|---|---|---|---|\n| Good | [good.md](good.md) | Experimental · 2 | always |\n")
			},
			`the row for good.md says "Experimental · 2" where the page declares Beta`,
		},
		"the selection guide has no Maturity column": {
			func(w func(string, string)) {
				w("docs/drivers/index.md", "# Guide\n\n| Driver | Page |\n|---|---|\n| Good | [good.md](good.md) |\n")
			},
			"no table column whose header names Maturity",
		},
		"the selection guide has no Page column": {
			func(w func(string, string)) {
				w("docs/drivers/index.md", "# Guide\n\n| Driver | Maturity · Tier |\n|---|---|\n| [good.md](good.md) | Beta · 2 |\n")
			},
			"has a Maturity column and no Page column",
		},
		"the selection guide links no driver page": {
			func(w func(string, string)) {
				w("docs/drivers/index.md", "# Guide\n\n| Driver | Page | Maturity · Tier |\n|---|---|---|\n| Good | good.md | Beta · 2 |\n")
			},
			"links no driver page, so its Maturity column was compared with nothing",
		},
		"a named driver's evidence no page links": {
			func(w func(string, string)) { w("docs/drivers/good.md", page("Experimental", "2", "")) },
			"is linked from no driver page",
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := maturityTree(t, tc.mutate)
			all := strings.Join(c.problems, "\n")
			if !strings.Contains(all, tc.want) {
				t.Errorf("no problem contains %q; got:\n%s", tc.want, all)
			}
		})
	}
}

// TestPairedNativeRowsAreTheMeasuredOnes pins the one piece of reading this
// tool does of a hand-written table. The "not comparable" row is the case
// that matters: a check that read it as a comparison would have passed the
// page that prompted this whole check.
func TestPairedNativeRowsAreTheMeasuredOnes(t *testing.T) {
	for _, tc := range []struct{ row, want string }{
		{"| `BenchmarkDriver/redis/streams` | `BenchmarkNative/rueidis` | `redis` |", "rueidis"},
		{"| `BenchmarkDriver/memory` | *none, by construction* | in-process |", ""},
		{"| `sqs` | **not comparable (§10.5)** | one boundary for both arms |", ""},
		{"| `activemq/stomp` | **no denominator** | a `BenchmarkNative/gostomp` package |", ""},
	} {
		got := ""
		if m := pairedNativeRe.FindStringSubmatch(tc.row); m != nil {
			got = m[1]
		}
		if got != tc.want {
			t.Errorf("pairedNativeRe on %q = %q, want %q", tc.row, got, tc.want)
		}
	}
}

// TestGuideProseCellIsNotReadAsTheRowsLevel is a must-PASS case, and it is the
// only thing that distinguishes reading the Page CELL from reading the whole
// row.
//
// The guide's last column is prose and it links siblings: the real
// docs/drivers/index.md ends a row with "see [Choosing between the four
// combinations](activemq.md#…)". A check that scanned the row would compare
// THAT page's level against THIS row's Maturity cell, and report a driver as
// mislabelled for a cross-reference. Both rows here are correct and the prose
// links the other driver, whose level differs, so a row-wide reader fails and
// a cell reader does not.
func TestGuideProseCellIsNotReadAsTheRowsLevel(t *testing.T) {
	c := maturityTree(t, func(w func(string, string)) {
		w("docs/drivers/other.md", "# O\n\n<!-- c -->\n\n**Maturity:** Experimental · **Tier:** 3 · **Owner:** o\n\n"+sections(1))
		w("docs/drivers/index.md", "# Guide\n\n| Driver | Page | Maturity · Tier | Choose it when |\n"+
			"|---|---|---|---|\n"+
			"| Good | [good.md](good.md) | Beta · 2 | unlike [other.md](other.md), which is not | \n"+
			"| Other | [other.md](other.md) | Experimental · 3 | see [good.md](good.md) |\n")
	})
	for _, p := range c.problems {
		if strings.Contains(p, "index.md") {
			t.Errorf("a correct guide was reported: %s", p)
		}
	}
}

// TestEmulatorExemptionPassesAStablePage is the exemption's control, and the
// only thing that distinguishes "the row exempts the page" from "Stable can
// no longer pass at all".
//
// It also asserts the exemption is REPORTED. A check that let the page
// through in silence would read, in `make docs-check`'s output, exactly like
// one that had found a published comparison -- and the difference between
// those two is the whole subject of §2.7's paragraph.
func TestEmulatorExemptionPassesAStablePage(t *testing.T) {
	c := maturityTree(t, func(w func(string, string)) {
		w("docs/drivers/emulated.md", driverPage("Stable", "1", "", evidence))
	})
	if len(c.problems) != 0 {
		t.Errorf("a Stable page with the emulator exemption reported problems:\n%s", strings.Join(c.problems, "\n"))
	}
	if all := strings.Join(c.report, "\n"); !strings.Contains(all, "under §2.7's emulator exemption") {
		t.Errorf("the exemption was taken and not reported; got:\n%s", all)
	}
}

// TestStableGatesWaivedOnThePagePasses is the other control: a gate that is
// genuinely absent and said to be absent, on the page, in the fixed shape.
func TestStableGatesWaivedOnThePagePasses(t *testing.T) {
	c := maturityTree(t, func(w func(string, string)) {
		w("docs/drivers/emulated.md", driverPage("Stable", "1", waiver("chaos, soak"), evidence))
		w(SuitesPath, suitesJSON("emulated", "integration", "matrix"))
	})
	if len(c.problems) != 0 {
		t.Errorf("a Stable page waiving two absent gates reported problems:\n%s", strings.Join(c.problems, "\n"))
	}
	if all := strings.Join(c.report, "\n"); !strings.Contains(all, "waived by owner decision 2026-06-12") {
		t.Errorf("the waiver was accepted and not reported; got:\n%s", all)
	}
}
