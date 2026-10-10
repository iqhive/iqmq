package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testRegistry is the prefix registry a fixture ledger needs (assumption
// C14.4). A27 is registered rather than historical, so the fixtures exercise
// the branch that actually gates a new prefix.
const testRegistry = "<!-- prefix registry: assumption C14.4\n" +
	"     historical: A19 B01 B1 C3 D12b\n" +
	"     A27  fix/fixture  the throwaway rows these tests build\n" +
	"     A27b fix/fixture  the second branch off one increment\n" +
	"     A27c fix/fixture  a third\n" +
	"     C02  fix/fixture  a plan-part letter case\n" +
	"-->\n\n"

// writeFile is a test helper for building a throwaway adr/ and hack/bakeoff/.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// adrFile writes a minimal ADR.
func adrFile(t *testing.T, dir, name, variants, auto string) string {
	t.Helper()
	front := "status: Accepted\nkind: policy\ndate: 2026-09-10\nvariants: " + variants + "\n"
	if auto != "" {
		front += auto
	}
	path := filepath.Join(dir, name)
	writeFile(t, path, "---\n"+front+"---\n\n# "+strings.TrimSuffix(name, ".md")+": Title\n")
	return path
}

// envFile writes a family's bake-off env file.
func envFile(t *testing.T, dir, family, adr, autoKey string) {
	t.Helper()
	writeFile(t, filepath.Join(dir, family+".env"),
		"PKG=./internal/x\nBENCH=BenchmarkX\nREF=ref\nAUTO_KEY="+autoKey+"\nADR="+adr+"\nTITLE=\"t\"\n")
}

// loadOne reads one ADR with the real parser, so that the checks are
// exercised against what a file says rather than a hand-built struct.
func loadOne(t *testing.T, path string) *adr {
	t.Helper()
	a, problems := load(path)
	if a == nil {
		t.Fatalf("%s: %v", path, problems)
	}
	return a
}

// TestOneNumberOneFile: two files claiming one ADR number is the failure a
// per-family ADR render produced, and `make adr-check` was silent about it.
func TestOneNumberOneFile(t *testing.T) {
	dir := t.TempDir()
	a := loadOne(t, adrFile(t, dir, "ADR-011-failure-policy.md", "[C5]", ""))
	b := loadOne(t, adrFile(t, dir, "ADR-011-error-construction.md", "[C5]", ""))
	problems := checkBakeoffMap([]*adr{a, b}, nil)
	if len(problems) != 1 || !strings.Contains(problems[0], "ADR-011 is claimed by 2 files") {
		t.Fatalf("problems = %v", problems)
	}
	if got := checkBakeoffMap([]*adr{a}, nil); len(got) != 0 {
		t.Errorf("a single file reported %v", got)
	}
}

// TestAutoKeyNamesAFamilyOfThisADR: an auto: key binds a variant family, so
// it must name one that exists and one this ADR collects.
func TestAutoKeyNamesAFamilyOfThisADR(t *testing.T) {
	dir := t.TempDir()
	envs := t.TempDir()
	envFile(t, envs, "B10", "11", "b10")
	envFile(t, envs, "A1", "14", "queue")
	fams, problems := loadFamilies(envs)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if len(fams) != 2 {
		t.Fatalf("families = %v", fams)
	}
	for _, tc := range []struct {
		name, auto, want string
	}{
		{"unknown key", "auto:\n  oplog: ring\n", "auto.oplog names no variant family"},
		{"other ADR's family", "auto:\n  queue: mpmc\n", "auto.queue binds a family this ADR does not collect: A1 (ADR-014)"},
		{"own family", "auto:\n  b10: prealloccodes\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := loadOne(t, adrFile(t, dir, "ADR-011-failure-policy.md", "[B10]", tc.auto))
			got := checkBakeoffMap([]*adr{a}, fams)
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("problems = %v", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("problems = %v, want %q", got, tc.want)
			}
		})
	}
}

// TestFamilyAppearsInItsADRsVariants: the map and the record must agree, and
// the check is scoped to ADR numbers that already have a file -- seven of the
// twelve are rendered only after the campaign.
func TestFamilyAppearsInItsADRsVariants(t *testing.T) {
	dir := t.TempDir()
	envs := t.TempDir()
	envFile(t, envs, "B10", "11", "b10")
	envFile(t, envs, "A1", "14", "queue") // ADR-014 has no file yet
	fams, _ := loadFamilies(envs)

	missing := loadOne(t, adrFile(t, dir, "ADR-011-failure-policy.md", "[C5, C6]", ""))
	got := checkBakeoffMap([]*adr{missing}, fams)
	if len(got) != 1 || !strings.Contains(got[0], "says ADR=11 but the ADR's variants are [C5, C6]: add B10") {
		t.Fatalf("problems = %v", got)
	}
	listed := loadOne(t, adrFile(t, dir, "ADR-011-failure-policy.md", "[B10, C5, C6]", ""))
	if got := checkBakeoffMap([]*adr{listed}, fams); len(got) != 0 {
		t.Fatalf("problems = %v", got)
	}
}

// TestEveryFamilyHasAnADRNumber: fifteen env files had ADR= empty, and an
// empty number renders no ADR at all.
func TestEveryFamilyHasAnADRNumber(t *testing.T) {
	envs := t.TempDir()
	envFile(t, envs, "B10", "", "b10")
	fams, _ := loadFamilies(envs)
	got := checkBakeoffMap(nil, fams)
	if len(got) != 1 || !strings.Contains(got[0], "hack/bakeoff/B10.env: ADR= is empty") {
		t.Fatalf("problems = %v", got)
	}
}

// TestRepositoryMapIsConsistent runs the checks over the repository itself,
// so that the gate is not only exercised against fixtures.
func TestRepositoryMapIsConsistent(t *testing.T) {
	fams, problems := loadFamilies("../../hack/bakeoff")
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if len(fams) < 30 {
		t.Fatalf("only %d bake-off famil(y/ies) found; the map was not read", len(fams))
	}
	files, err := filepath.Glob("../../adr/ADR-*.md")
	if err != nil {
		t.Fatal(err)
	}
	var adrs []*adr
	for _, f := range files {
		a, ps := load(f)
		if len(ps) != 0 {
			t.Errorf("%s: %v", f, ps)
		}
		if a != nil {
			adrs = append(adrs, a)
		}
	}
	if len(adrs) > 0 {
		if got := checkBakeoffMap(adrs, fams); len(got) != 0 {
			t.Errorf("repository map: %v", got)
		}
	}
}

// ledgerFixture writes a throwaway ADR-017 with the rows and Alternatives
// entries it is given, so that the identifier rule is exercised against a
// file rather than against a hand-built map.
func ledgerFixture(t *testing.T, rows [][2]string, alts ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("# ADR-017\n\n")
	// Every fixture ledger carries a prefix registry, because assumption
	// C14.4 refuses a table whose prefixes nothing accounts for.
	b.WriteString(testRegistry)
	b.WriteString(tableHeader + "\n|---|---|---|---|\n")
	for _, r := range rows {
		b.WriteString("| " + r[0] + " | an assumption | somewhere | " + r[1] + " |\n")
	}
	b.WriteString("\n## Alternatives\n\n")
	for _, a := range alts {
		b.WriteString("- **Assumption " + a + "**: the alternative.\n")
	}
	path := filepath.Join(t.TempDir(), ledgerFile)
	writeFile(t, path, b.String())
	return path
}

// TestLedgerIdentifierTakesABranchSuffix pins the identifier shape a second
// branch off one increment needs.
//
// Two agents implemented one plan row in sequence, the second on a branch
// named after the first (inc/A27b), and the ledger had no identifier it
// could take: A27.14 belongs to the increment that landed, and the next
// plan row's number belongs to another agent. So the increment part of an
// identifier may carry one trailing lower-case letter, and the row's own
// Increment column must spell it the same way — a suffix on one side only
// is the collision the identifier rule exists to prevent.
func TestLedgerIdentifierTakesABranchSuffix(t *testing.T) {
	path := ledgerFixture(t,
		[][2]string{{"A27.1", "A27"}, {"A27b.1", "A27b"}, {"A27b.2", "A27b"}},
		"A27.1", "A27b.1", "A27b.2")
	rows, problems := loadLedger(path)
	if len(problems) != 0 {
		t.Fatalf("a ledger with suffixed identifiers reported %v", problems)
	}
	for _, want := range []string{"A27.1", "A27b.1", "A27b.2"} {
		if rows[want].id != want {
			t.Errorf("row %s was not read (rows = %v)", want, rows)
		}
	}
	// A suffixed identifier and an unsuffixed increment name two different
	// branches of one row and must not be read as one.
	mismatched := ledgerFixture(t, [][2]string{{"A27b.1", "A27"}}, "A27b.1")
	_, problems = loadLedger(mismatched)
	if len(problems) == 0 {
		t.Fatal("identifier A27b.1 on a row whose Increment column says A27 was accepted")
	}
	// An upper-case suffix is not the same identifier as a lower-case one,
	// and neither is a two-letter one: the rule is exactly one letter.
	for _, bad := range []string{"A27B.1", "A27bb.1", "A27b.0"} {
		if _, ps := loadLedger(ledgerFixture(t, [][2]string{{bad, "A27b"}}, bad)); len(ps) == 0 {
			t.Errorf("identifier %q was accepted", bad)
		}
	}
}

// TestLedgerCitationTakesABranchSuffix is the other half: a citation of a
// suffixed row has to be recognised as a citation, or every "assumption
// A27b.1" in the repository is invisible to the check that citations name a
// row that exists.
func TestLedgerCitationTakesABranchSuffix(t *testing.T) {
	rows, problems := loadLedger(ledgerFixture(t, [][2]string{{"A27b.1", "A27b"}}, "A27b.1"))
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good.go")
	writeFile(t, good, "package p\n\n// The rule of assumption A27b.1 applies here.\n")
	n, ps := scanCitations(good, "good.go", rows)
	if n != 1 || len(ps) != 0 {
		t.Fatalf("scanning a citation of A27b.1 found %d citation(s), problems %v; want 1 and none", n, ps)
	}
	bad := filepath.Join(dir, "bad.go")
	// The undefined identifier is spliced in rather than written out: this
	// file is itself scanned by the repository-wide citation check, and a
	// literal citation of a row no ledger defines would fail it.
	undefined := "A27z" + ".9"
	writeFile(t, bad, "package p\n\n// The rule of assumption "+undefined+" applies here.\n")
	if n, ps := scanCitations(bad, "bad.go", rows); n != 1 || len(ps) != 1 {
		t.Fatalf("scanning a citation of the undefined A27z.9 found %d citation(s), problems %v; want 1 and one problem", n, ps)
	}
}

// TestLedgerIdentifierTakesAPlanPartLetter pins the other half of an
// identifier's increment: the letter is the plan PART, not a fixed "A".
//
// Every regular expression in this tool was written while only part A of the
// plan existed, so the first increment of part B could not take a row at all —
// `B01.1` was rejected as "not <increment>.<ordinal>", which is the message a
// reader would take to mean they had typed it wrong. Parts A to D are the
// plan's four.
func TestLedgerIdentifierTakesAPlanPartLetter(t *testing.T) {
	path := ledgerFixture(t,
		[][2]string{{"B01.1", "B01"}, {"B1.2", "B1"}, {"C3.1", "C3"}, {"D12b.4", "D12b"}},
		"B01.1", "B1.2", "C3.1", "D12b.4")
	rows, problems := loadLedger(path)
	if len(problems) != 0 {
		t.Fatalf("a ledger with part-B, part-C and part-D identifiers reported %v", problems)
	}
	// B01.1 and B1.2 normalise into one increment, the way A09.1 and A9.1
	// always have; the part letter is part of the key, so B1.1 is not A1.1.
	for _, want := range []string{"B1.1", "B1.2", "C3.1", "D12b.4"} {
		if rows[want].id == "" {
			t.Errorf("row %s was not read (rows = %v)", want, rows)
		}
	}
	// The part letter has to agree with the row's own Increment column, and
	// a letter outside the plan's four parts is not an identifier.
	if _, ps := loadLedger(ledgerFixture(t, [][2]string{{"B01.1", "A01"}}, "B01.1")); len(ps) == 0 {
		t.Error("identifier B01.1 on a row whose Increment column says A01 was accepted")
	}
	for _, bad := range []string{"E1.1", "AB1.1", "1.1"} {
		if _, ps := loadLedger(ledgerFixture(t, [][2]string{{bad, "B1"}}, bad)); len(ps) == 0 {
			t.Errorf("identifier %q was accepted", bad)
		}
	}
}

// TestLedgerCitationTakesAPlanPartLetter is the citation half: a citation of a
// part-B row has to be recognised, or every "assumption B01.8" in the
// repository is invisible to the check that citations name a row that exists.
func TestLedgerCitationTakesAPlanPartLetter(t *testing.T) {
	rows, problems := loadLedger(ledgerFixture(t, [][2]string{{"B01.8", "B01"}}, "B01.8"))
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good.go")
	writeFile(t, good, "package p\n\n// The rule of assumption B01.8 applies here.\n")
	if n, ps := scanCitations(good, "good.go", rows); n != 1 || len(ps) != 0 {
		t.Fatalf("scanning a citation of B01.8 found %d citation(s), problems %v; want 1 and none", n, ps)
	}
	bad := filepath.Join(dir, "bad.go")
	undefined := "D77" + ".9"
	writeFile(t, bad, "package p\n\n// The rule of assumption "+undefined+" applies here.\n")
	if n, ps := scanCitations(bad, "bad.go", rows); n != 1 || len(ps) != 1 {
		t.Fatalf("scanning a citation of the undefined D77.9 found %d citation(s), problems %v; want 1 and one problem", n, ps)
	}
}

// TestMeasuredADRMayNotBeAcceptedOnAnUnpinnedHost is the plan's C01 rule
// : a measured record that declares `authoritative: no` may
// not carry the status `Accepted`, because a reader would then have no way to
// tell a decision taken on this shared host from a ratified one.
//
// The `no` spelling is load-bearing and is why this test exists. YAML 1.1
// reads a plain `no` as the BOOLEAN false, so the first version of the check --
// `yamlmini.String(auth) == "no"` -- rendered it "false", never matched, and
// was a check that could not fail on any file in the repository. The table
// therefore carries `no`, `false` and `yes` rather than one of them.
func TestMeasuredADRMayNotBeAcceptedOnAnUnpinnedHost(t *testing.T) {
	// A measured ADR needs its sections and its sysinfo line, or the other
	// checks in the same block report and this one's finding is lost in them.
	body := "# ADR-099: Title\n\n## Decision\n\nd\n\n## Disposition\n\nd\n\n" +
		"## Raw data\n\nsysinfo: `x.json`\n\n## Alternatives\n\na\n"
	write := func(status, auth string) []string {
		dir := t.TempDir()
		path := filepath.Join(dir, "ADR-099-x.md")
		writeFile(t, path, "---\nstatus: "+status+"\nkind: measured\ndate: 2026-09-18\n"+
			"variants: [A99]\nauthoritative: "+auth+"\n---\n\n"+body)
		_, problems := load(path)
		return problems
	}
	offending := func(ps []string) string {
		for _, p := range ps {
			if strings.Contains(p, "authoritative: no and status: Accepted") {
				return p
			}
		}
		return ""
	}
	for _, auth := range []string{"no", "false"} {
		if got := offending(write("Accepted", auth)); got == "" {
			t.Errorf("authoritative: %s with status Accepted was not reported; problems = %v",
				auth, write("Accepted", auth))
		}
		if got := offending(write("Accepted (provisional)", auth)); got != "" {
			t.Errorf("authoritative: %s with status Accepted (provisional) was reported: %s", auth, got)
		}
	}
	// A pinned host is exactly what lifts the rule, so a `yes` with a plain
	// Accepted must NOT be reported -- otherwise the check would make
	// ratification impossible, which is the opposite of what it is for.
	for _, auth := range []string{"yes", "true"} {
		if got := offending(write("Accepted", auth)); got != "" {
			t.Errorf("authoritative: %s with status Accepted was reported: %s", auth, got)
		}
	}
}

// C01.6's rule is scoped to `kind: measured`, and four records sat just
// outside it: ADR-003 and ADR-007 (structural) and ADR-009 and ADR-011
// (policy) each say plainly `Accepted`, declare `authoritative: no`, and
// carry a generated bake-off block -- three of them binding a family. Their
// primary decision is one no measurement can unsettle and their status is
// right; what was missing is that nothing said WHERE the line falls, so a
// reader had a status about one half and a front-matter key about the other.
//
// The answer taken here is that a record CAN be Accepted about one thing and
// provisional about another, and that the split is declared rather than
// inferred. `authoritative` describes the record's measured CONTENT -- three
// values, each checked against whether a measurement is actually in the file
// -- and `provisional_scope` names the half the status does not cover.
func TestAuthoritativeDescribesTheMeasurementAndProvisionalScopeNamesTheHalf(t *testing.T) {
	const block = "\n<!-- BEGIN bakeoff-generated (bakeoff adr) -->\n## Bake-off data (B6)\n\nx\n"
	write := func(kind, auth, scope, body string) []string {
		dir := t.TempDir()
		path := filepath.Join(dir, "ADR-099-x.md")
		front := "---\nstatus: Accepted\nkind: " + kind + "\ndate: 2026-09-18\n"
		if auth != "" {
			front += "authoritative: " + auth + "\n"
		}
		if scope != "" {
			front += "provisional_scope: \"" + scope + "\"\n"
		}
		writeFile(t, path, front+"---\n\n# ADR-099: Title\n\n## Decision\n\nd\n"+body)
		_, problems := load(path)
		return problems
	}
	has := func(ps []string, want string) bool {
		for _, p := range ps {
			if strings.Contains(p, want) {
				return true
			}
		}
		return false
	}
	cases := []struct {
		name              string
		kind, auth, scope string
		body              string
		want, mustNotWant string
	}{
		{
			name: "a block with no authoritative key", kind: "policy", body: block,
			scope: "the block only", want: "declares no authoritative: key",
		},
		{
			name: "a block declared n/a", kind: "policy", auth: "n/a", scope: "the block only",
			body: block, want: "n/a is for a record that contains none",
		},
		{
			name: "no measurement but authoritative: yes", kind: "policy", auth: "yes",
			want: "carries no measurement to qualify",
		},
		{
			name: "a structural record with a block and no scope", kind: "structural", auth: "no",
			body: block, want: "declare provisional_scope:",
		},
		{
			name: "a policy record with a block and a scope", kind: "policy", auth: "no",
			scope: "the b6 binding only", body: block, mustNotWant: "provisional_scope",
		},
		{
			name: "a scope with no measurement to scope", kind: "policy", auth: "n/a",
			scope: "nothing", want: "carries no measurement to scope",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps := write(c.kind, c.auth, c.scope, c.body)
			if c.want != "" && !has(ps, c.want) {
				t.Errorf("problems = %v; want one containing %q", ps, c.want)
			}
			if c.mustNotWant != "" && has(ps, c.mustNotWant) {
				t.Errorf("problems = %v; none may mention %q", ps, c.mustNotWant)
			}
		})
	}
	// And a measured record's whole decision is provisional: a scope key
	// there would be a second, narrower claim about the same thing.
	measured := "\n## Disposition\n\nd\n\n## Raw data\n\nsysinfo: `x.json`\n\n## Alternatives\n\na\n" + block
	dir := t.TempDir()
	path := filepath.Join(dir, "ADR-098-x.md")
	writeFile(t, path, "---\nstatus: Accepted (provisional)\nkind: measured\ndate: 2026-09-18\n"+
		"variants: [A99]\nauthoritative: no\nprovisional_scope: \"half of it\"\n---\n\n# ADR-098: T\n\n## Decision\n\nd\n"+measured)
	if _, ps := load(path); !has(ps, "the whole record is provisional") {
		t.Errorf("a measured record with provisional_scope was not reported; problems = %v", ps)
	}
}

// TestLedgerRowEndsAtItsFinalPipe is the damage a merge does to this file and
// that every other check here was blind to: `## Alternatives` glued onto the
// end of a row.
//
// Splitting such a row on the pipe still yields six cells and its fourth cell
// still reads the increment, so the identifier rule, the increment rule and
// the column count all passed — while the file had no Alternatives heading at
// all and the reader saw the heading rendered as a fifth column. One instance
// was found by hand; this check found a second, 160 lines earlier, that
// nobody had noticed.
func TestLedgerRowEndsAtItsFinalPipe(t *testing.T) {
	good := ledgerFixture(t, [][2]string{{"A27.1", "A27"}}, "A27.1")
	if _, ps := loadLedger(good); len(ps) != 0 {
		t.Fatalf("a well-formed ledger reported %d problem(s): %v", len(ps), ps)
	}

	for name, tail := range map[string]string{
		"a heading":   "## Alternatives",
		"a paragraph": "and then some prose",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), ledgerFile)
			writeFile(t, path, testRegistry+tableHeader+"\n|---|---|---|---|\n"+
				"| A27.1 | an assumption | somewhere | A27 |"+tail+"\n"+
				"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
			_, ps := loadLedger(path)
			if len(ps) == 0 {
				t.Fatalf("%q glued onto the end of a row was accepted: the row still has four columns and "+
					"the fourth still reads the increment, which is exactly why this needs its own check", tail)
			}
			var named bool
			for _, p := range ps {
				if strings.Contains(p, "final |") {
					named = true
				}
			}
			if !named {
				t.Errorf("the problems do not name the trailing text: %v", ps)
			}
		})
	}
}

// TestAnUnregisteredIncrementPrefixIsRefused is the rule that stopped three
// branches taking one prefix.
//
// the contributor notes forbids taking an identifier's ORDINAL from the length of
// the table -- the mistake whose merge deleted sixteen rows -- and said nothing
// about where the PREFIX comes from. `main` topped out at C10, so C11 looked
// derived rather than guessed, and fix/ci-hardening, fix/cover-core and
// fix/followups each claimed C11.1 onward with different content: seventeen
// rows about to collide into seven, each agent following a reasonable-looking
// local rule to the same wrong answer.
func TestAnUnregisteredIncrementPrefixIsRefused(t *testing.T) {
	// C11 is in neither the registry nor its historical line.
	path := filepath.Join(t.TempDir(), ledgerFile)
	writeFile(t, path, testRegistry+tableHeader+"\n|---|---|---|---|\n"+
		"| A27.1 | a registered prefix | somewhere | A27 |\n"+
		"| C11.1 | a prefix nobody assigned | somewhere | C11 |\n"+
		// No Alternatives entry for the unregistered row: loadLedger does not
		// require one, and writing the citation form here would make this test
		// file cite an assumption the real ledger does not define -- which
		// scanRepo reports, correctly.
		"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
	_, ps := loadLedger(path)
	if !hasProblem(ps, "C11") {
		t.Fatalf("an unregistered prefix was accepted: %v", ps)
	}
	for _, want := range []string{"C11.1", "prefix registry", "C14.4"} {
		if !hasProblem(ps, want) {
			t.Errorf("the message does not name %q, which a reader needs to act on it: %v", want, ps)
		}
	}
	// And the registered and grandfathered halves must stay clean, or the rule
	// is "no new rows" rather than "register your prefix".
	if hasProblem(ps, "A27.1") {
		t.Errorf("a REGISTERED prefix was refused: %v", ps)
	}
	good := ledgerFixture(t, [][2]string{{"A19.1", "A19"}, {"A27.1", "A27"}}, "A19.1", "A27.1")
	if _, ps := loadLedger(good); len(ps) != 0 {
		t.Errorf("a historical prefix (A19) beside a registered one (A27) reported %d problem(s): %v", len(ps), ps)
	}
}

// TestARegisteredPrefixNeedsNoRows is the asymmetry with assumption C14.1's
// marker, and it is deliberate.
//
// A marker for a name nobody cites is refused, because it outlives its reason.
// A registry line for a prefix with no rows is a RESERVATION, and refusing it
// would mean four branches could not register in one place and land in any
// order: C11, C12 and C13 are registered on the branch that added this check
// while their rows are on three others, so the symmetric rule would have made
// this check's own landing red on `main`.
func TestARegisteredPrefixNeedsNoRows(t *testing.T) {
	path := ledgerFixture(t, [][2]string{{"A27.1", "A27"}}, "A27.1")
	// testRegistry registers A27b, A27c and C02 as well, and no row uses them.
	if _, ps := loadLedger(path); len(ps) != 0 {
		t.Fatalf("a reserved prefix with no rows was refused: %v", ps)
	}
	reg, ps := loadPrefixRegistry(strings.Split(readFile(t, path), "\n"))
	if len(ps) != 0 {
		t.Fatalf("the fixture registry does not parse: %v", ps)
	}
	if reg.registered["A27c"] == "" {
		t.Error("a registry line for a prefix with no rows was not read at all")
	}
	if !reg.historical["A19"] {
		t.Error("the historical line was not read")
	}
}

// TestAMissingPrefixRegistryIsRefused keeps the check from passing by not
// finding anything to check, which is how six gates in this repository turned
// out not to work.
func TestAMissingPrefixRegistryIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), ledgerFile)
	writeFile(t, path, tableHeader+"\n|---|---|---|---|\n"+
		"| A27.1 | no registry above this table | somewhere | A27 |\n"+
		"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
	if _, ps := loadLedger(path); !hasProblem(ps, "no prefix registry") {
		t.Errorf("a ledger with no registry at all was accepted: %v", ps)
	}
}

// TestAPrefixRegisteredTwiceIsRefused: a prefix has one owner.
func TestAPrefixRegisteredTwiceIsRefused(t *testing.T) {
	dup := "<!-- prefix registry: assumption C14.4\n" +
		"     A27  fix/one  the first claim\n" +
		"     A27  fix/two  the second claim\n-->\n\n"
	path := filepath.Join(t.TempDir(), ledgerFile)
	writeFile(t, path, dup+tableHeader+"\n|---|---|---|---|\n"+
		"| A27.1 | a row | somewhere | A27 |\n"+
		"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
	if _, ps := loadLedger(path); !hasProblem(ps, "twice") {
		t.Errorf("one prefix with two owners was accepted: %v", ps)
	}
}

// readFile is a test helper.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
