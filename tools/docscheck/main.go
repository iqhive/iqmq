// Command docscheck validates documentation structure, local links,
// anchors, navigation and generated capability claims when reports are present.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/iqhive/iqmq/tools/internal/mdcheck"
)

func main() {
	root := flag.String("root", ".", "repository root")
	verbose := flag.Bool("v", false, "print what passed as well as what failed")
	flag.Parse()

	c := &checker{root: *root, verbose: *verbose}
	c.load()
	c.checkSections()
	if generated, _ := filepath.Glob(filepath.Join(c.root, "docs/drivers/*.capabilities.md")); len(generated) > 0 {
		c.checkMaturity()
		c.checkDriverPagesExist()
	}
	c.checkLinks()
	c.checkNav()
	c.checkREADME()
	c.checkMarkdownStructure()

	for _, p := range c.problems {
		fmt.Fprintln(os.Stderr, "docscheck: "+p)
	}
	if len(c.problems) > 0 {
		fmt.Fprintf(os.Stderr, "docscheck: %d problem(s)\n", len(c.problems))
		os.Exit(1)
	}
	for _, line := range c.report {
		fmt.Println("docscheck: ok, " + line)
	}
}

type page struct {
	rel      string   // slash path relative to the repository root
	body     string   // the file, verbatim
	headings []string // heading text in document order, "## " stripped
	levels   []int    // the heading's level, index-aligned with headings
	anchors  map[string]bool
}

type checker struct {
	root     string
	verbose  bool
	problems []string
	report   []string

	pages map[string]*page // by slash path relative to the root
}

// structureDirs are the directories checkMarkdownStructure walks, beyond the
// pages docscheck already holds.
//
// `hack/` is on the list because nothing read it: `tools/adrcheck` reads
// `adr/`, this reads `docs/`, and the maintenance notes -- the longest prose
// document in the repository and the one every increment appends to -- had a
// code fence whose body and closer a merge had dropped, so 1,400 lines of it
// including a `## C07` heading were inside a code block.
var structureDirs = []string{"docs", "adr", "hack", "conformance", "benchmarks"}

// checkMarkdownStructure looks for the damage a bad merge leaves in a markdown
// file: a heading run onto the end of the line before it, and a code fence
// with no closer.
//
// It is deliberately wider than the rest of this tool. The other checks are
// about the docs SITE -- its nav, its links, its §7.2 sections -- and this one
// is about any markdown file in the repository, because the defect has been
// found in `adr/`, in `hack/` and in `mkdocs.yml`, and each time the check
// that would have caught it looked somewhere else.
func (c *checker) checkMarkdownStructure() {
	seen := map[string]bool{}
	var files []string
	for _, dir := range structureDirs {
		root := filepath.Join(c.root, dir)
		if _, err := os.Stat(root); err != nil {
			continue
		}
		err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(p, ".md") {
				return nil
			}
			rel, err := filepath.Rel(c.root, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if !seen[rel] {
				seen[rel] = true
				files = append(files, rel)
			}
			return nil
		})
		if err != nil {
			c.failf("walking %s/ for markdown structure: %v", dir, err)
		}
	}
	for rel := range c.pages {
		if !seen[rel] {
			seen[rel] = true
			files = append(files, rel)
		}
	}
	sort.Strings(files)

	found := 0
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(c.root, filepath.FromSlash(rel)))
		if err != nil {
			c.failf("%s: %v", rel, err)
			continue
		}
		for _, p := range mdcheck.Check(rel, 0, string(b)) {
			c.failf("%s", p)
			found++
		}
	}
	c.okf(found, "%d markdown file(s) have no glued heading and no unclosed code fence", len(files))
}

func (c *checker) failf(format string, a ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, a...))
}

// okf records what a check proved. It is deliberately conditional on the
// check having found nothing: a tool that prints "ok, every driver page
// carries §7.2's sections" beside the problem that says one does not is how a
// reader learns to skim past the summary.
func (c *checker) okf(from int, format string, a ...any) {
	if len(c.problems) != from {
		return
	}
	c.report = append(c.report, fmt.Sprintf(format, a...))
}

// load reads every markdown page this tool has an opinion about: everything
// under docs/, the root's own markdown, and llms.txt (which is markdown in
// all but name, and whose links are the reason it is here).
func (c *checker) load() {
	c.pages = map[string]*page{}
	add := func(rel string) {
		b, err := os.ReadFile(filepath.Join(c.root, filepath.FromSlash(rel)))
		if err != nil {
			c.failf("%s: %v", rel, err)
			return
		}
		c.pages[rel] = parse(rel, string(b))
	}
	err := filepath.WalkDir(filepath.Join(c.root, "docs"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".md") {
			return nil
		}
		rel, err := filepath.Rel(c.root, p)
		if err != nil {
			return err
		}
		add(filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		c.failf("walking docs/: %v", err)
	}
	for _, rel := range []string{
		"README.md", "CONTRIBUTING.md", "SECURITY.md", "llms.txt",
	} {
		if _, err := os.Stat(filepath.Join(c.root, rel)); err == nil {
			add(rel)
		}
	}
}

var headingRe = regexp.MustCompile(`(?m)^(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$`)

// parse records a page's headings and the anchors they mint.
func parse(rel, body string) *page {
	p := &page{rel: rel, body: body, anchors: map[string]bool{}}
	seen := map[string]int{}
	for _, line := range headingLines(body) {
		m := headingRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		text := m[2]
		p.headings = append(p.headings, text)
		p.levels = append(p.levels, len(m[1]))
		a := slug(text)
		if a == "" {
			continue
		}
		// python-markdown's toc extension disambiguates a repeated slug with
		// _1, _2, …; a link may name either form, so both are anchors.
		if n := seen[a]; n > 0 {
			p.anchors[fmt.Sprintf("%s_%d", a, n)] = true
		} else {
			p.anchors[a] = true
		}
		seen[a]++
	}
	// An explicit id= or name= attribute is an anchor too: the capability
	// tables and the generated matrix use HTML comments and raw anchors.
	for _, m := range explicitAnchorRe.FindAllStringSubmatch(body, -1) {
		p.anchors[m[2]] = true
	}
	return p
}

var explicitAnchors = `(?:id|name)`

var explicitAnchorRe = regexp.MustCompile(`<a[^>]*\b` + explicitAnchors + `="([^"]*)"|\{#([A-Za-z0-9_.:-]+)\}`)

// headingLines returns the lines of body that are outside a fenced code
// block. A `# comment` inside a shell block is not a heading, and a driver
// page's DSN examples are full of them.
func headingLines(body string) []string {
	var out []string
	fence := ""
	s := bufio.NewScanner(strings.NewReader(body))
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		line := s.Text()
		trimmed := strings.TrimLeft(line, " \t")
		switch {
		case fence != "":
			if strings.HasPrefix(trimmed, fence) {
				fence = ""
			}
			continue
		case strings.HasPrefix(trimmed, "```"):
			fence = "```"
			continue
		case strings.HasPrefix(trimmed, "~~~"):
			fence = "~~~"
			continue
		}
		out = append(out, line)
	}
	return out
}

// slug is python-markdown's toc slugify with the default separator, which is
// what mkdocs mints heading ids with: strip inline markup, drop every
// character that is not a word character, whitespace or a dash, lower-case,
// and collapse runs of dash-or-space into one dash.
//
// It is NOT GitHub's slugify, and the difference is a whole class of link
// that renders as text on the site: GitHub keeps each space, so
// "Ack / nack / extend mapping" is `ack--nack--extend-mapping` there and
// `ack-nack-extend-mapping` here. Four driver pages had the GitHub form.
func slug(text string) string {
	text = stripInline(text)
	var b strings.Builder
	for _, r := range text {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_':
			b.WriteRune(unicode.ToLower(r))
		case r == '-' || unicode.IsSpace(r):
			b.WriteRune('-')
		}
	}
	out := regexp.MustCompile(`-+`).ReplaceAllString(b.String(), "-")
	return strings.Trim(out, "-")
}

var (
	inlineLinkRe = regexp.MustCompile(`\[([^\]]*)\]\(([^)]*)\)`)
	inlineCodeRe = regexp.MustCompile("`+([^`]*)`+")
	emphasisRe   = regexp.MustCompile(`\*+|_{2,}`)
)

// stripInline removes the inline markup a heading may carry before the slug
// is taken, because the rendered text is what python-markdown slugifies.
func stripInline(text string) string {
	text = inlineLinkRe.ReplaceAllString(text, "$1")
	text = inlineCodeRe.ReplaceAllString(text, "$1")
	text = emphasisRe.ReplaceAllString(text, "")
	return text
}

// ---------------------------------------------------------------- §7.2

// driverPages are the pages §7.2's "each driver has docs/drivers/<name>.md
// with fixed sections" is about: everything under docs/drivers/ that is not
// the index, the matrix, the author guide, the template, or one of confgen's
// generated capability partials.
func (c *checker) driverPages() []string {
	var out []string
	for rel := range c.pages {
		if !strings.HasPrefix(rel, "docs/drivers/") {
			continue
		}
		base := path.Base(rel)
		switch base {
		case "index.md", "matrix.md", "author-guide.md", "_template.md":
			continue
		}
		if strings.HasSuffix(base, ".capabilities.md") {
			continue
		}
		out = append(out, rel)
	}
	sort.Strings(out)
	return out
}

// templateSections reads §7.2's fixed section list out of
// docs/drivers/_template.md, so the list has one home and a driver page and
// the template cannot disagree about it.
func (c *checker) templateSections() []string {
	tmpl := c.pages["docs/drivers/_template.md"]
	if tmpl == nil {
		c.failf("docs/drivers/_template.md is missing; it is where §7.2's fixed section list lives")
		return nil
	}
	var want []string
	for i, h := range tmpl.headings {
		if tmpl.levels[i] == 2 {
			want = append(want, h)
		}
	}
	if len(want) != 15 {
		c.failf("docs/drivers/_template.md declares %d level-2 sections; §7.2 fixes fifteen", len(want))
	}
	return want
}

func (c *checker) checkSections() {
	from := len(c.problems)
	want := c.templateSections()
	if len(want) == 0 {
		return
	}
	pages := c.driverPages()
	if len(pages) == 0 {
		c.failf("no driver page found under docs/drivers/; §7.2 requires one per driver")
		return
	}
	for _, rel := range pages {
		p := c.pages[rel]
		// The page's own level-2 headings, in order. A page may add its own
		// sections (a conformance diff, a bake-off note) after or between
		// §7.2's, so this is a SUBSEQUENCE test rather than an equality one:
		// every fixed section is present, and their relative order is §7.2's.
		have := map[string]int{}
		for i, h := range p.headings {
			if p.levels[i] == 2 {
				if _, dup := have[h]; !dup {
					have[h] = i
				}
			}
		}
		var missing []string
		last, lastName := -1, ""
		var outOfOrder []string
		for _, w := range want {
			at, ok := have[w]
			if !ok {
				missing = append(missing, w)
				continue
			}
			if at < last {
				outOfOrder = append(outOfOrder, fmt.Sprintf("%q comes before %q", w, lastName))
			}
			last, lastName = at, w
		}
		if len(missing) > 0 {
			c.failf("%s: §7.2 section(s) missing: %s", rel, strings.Join(missing, "; "))
		}
		if len(outOfOrder) > 0 {
			c.failf("%s: §7.2 section(s) out of order: %s", rel, strings.Join(outOfOrder, "; "))
		}
	}
	c.okf(from, "%d driver page(s) carry §7.2's %d fixed sections in order", len(pages), len(want))
}

// ---------------------------------------------------------------- links

func (c *checker) checkLinks() {
	from := len(c.problems)
	rels := make([]string, 0, len(c.pages))
	for rel := range c.pages {
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	checked := 0
	for _, rel := range rels {
		for _, target := range c.pages[rel].links() {
			checked++
			c.resolve(rel, target)
		}
	}
	c.okf(from, "%d relative link(s) across %d page(s) resolve, anchors included", checked, len(rels))
}

func isExternal(target string) bool {
	switch {
	case strings.HasPrefix(target, "#"):
		return false
	case strings.Contains(target, "://"):
		return true
	case strings.HasPrefix(target, "mailto:"):
		return true
	case strings.HasPrefix(target, "//"):
		return true
	}
	return false
}

// resolve checks one relative link from page rel.
func (c *checker) resolve(rel, target string) {
	file, anchor, _ := strings.Cut(target, "#")
	dest := rel
	if file != "" {
		if strings.HasPrefix(file, "/") {
			dest = strings.TrimPrefix(file, "/")
		} else {
			dest = path.Join(path.Dir(rel), file)
		}
		if _, err := os.Stat(filepath.Join(c.root, filepath.FromSlash(dest))); err != nil {
			c.failf("%s: link %q: %s does not exist", rel, target, dest)
			return
		}
	}
	if anchor == "" {
		return
	}
	// An anchor is only checkable in a page this tool parsed. A link into a
	// directory, a non-markdown file or a page outside docs/ and the root set
	// carries an anchor nothing here can verify, and saying so would be a
	// check that fails for being unable to look rather than for a defect.
	p := c.pages[dest]
	if p == nil {
		if strings.HasSuffix(dest, ".md") {
			c.failf("%s: link %q names an anchor in %s, which is not a page docscheck reads", rel, target, dest)
		}
		return
	}
	if !p.anchors[anchor] {
		near := nearest(anchor, p.anchors)
		c.failf("%s: link %q: %s has no anchor %q%s", rel, target, dest, anchor, near)
	}
}

// nearest names the anchor a broken link most likely meant, which is what
// turns "no such anchor" into a fix. The GitHub-versus-python-markdown dash
// collapse is the common case, so a candidate that differs only in dash runs
// is reported first.
func nearest(anchor string, anchors map[string]bool) string {
	collapsed := regexp.MustCompile(`-+`).ReplaceAllString(anchor, "-")
	if anchors[collapsed] {
		return fmt.Sprintf(" (did you mean %q? python-markdown collapses a dash run, GitHub does not)", collapsed)
	}
	var names []string
	for a := range anchors {
		names = append(names, a)
	}
	sort.Strings(names)
	for _, a := range names {
		if strings.Contains(a, anchor) || strings.Contains(anchor, a) {
			return fmt.Sprintf(" (did you mean %q?)", a)
		}
	}
	return ""
}

// ---------------------------------------------------------------- nav

var (
	navEntryRe   = regexp.MustCompile(`^\s*(?:-\s*)?(?:[^:]*:\s*)?([A-Za-z0-9_./*-]+\.md)\s*$`)
	notInNavRe   = regexp.MustCompile(`(?m)^not_in_nav:\s*\|?\s*$`)
	docsDirRe    = regexp.MustCompile(`(?m)^docs_dir:\s*(\S+)\s*$`)
	validationRe = regexp.MustCompile(`(?m)^\s*anchors:\s*(\S+)\s*$`)
	// A nav line carrying a SECOND list entry after its value: two entries
	// joined onto one line, which is what a merge does when it resolves a
	// hunk boundary badly. mkdocs refuses the file outright ("mapping values
	// are not allowed here") while this tool read its 96 nav entries and
	// passed, because it scans lines rather than parsing YAML -- ADR-002
	// keeps a YAML library out of the root module, so the check is for the
	// damage rather than for well-formedness in general.
	// [ \t]+ and not \s+ for the gap: in Go's regexp \s matches a NEWLINE, so
	// the first version of this spanned two perfectly good consecutive lines
	// and reported 54 problems in a file that had one.
	gluedNavRe = regexp.MustCompile(`(?m)^([ \t]*-[ \t]+[^:\n]+:[ \t]*[^ \t\n]+\.(?:md|html))[ \t]+(-[ \t]+[^\n]*)$`)
)

// checkNav holds the site's shape: mkdocs.yml names files that exist, its
// anchor validation is strict enough to fail a broken in-page link, and every
// page under docs/ is REACHABLE.
//
// Reachable is the property that matters and "listed in nav" is not it. Two
// kinds of page are deliberately out of the nav and must still be findable:
// `bakeoff decide` writes one report per §9 variant row (49 of them), and
// `confgen` writes one capability table per driver profile (22). Before this
// check, 48 of the variant reports and 19 of the capability tables were
// linked from nothing at all: mkdocs shipped the HTML and no route on the site
// reached it, which it reports as INFO under `--strict`. So the check is the
// transitive closure of relative links from the nav, and a generated report
// nothing links is a failure rather than a note.
func (c *checker) checkNav() {
	from := len(c.problems)
	b, err := os.ReadFile(filepath.Join(c.root, "mkdocs.yml"))
	if err != nil {
		c.failf("mkdocs.yml: %v", err)
		return
	}
	cfg := string(b)

	// mkdocs' own anchor validation defaults to `info`, which `--strict` does
	// not promote, so a site with broken in-page links builds green. This
	// tool checks anchors itself; the config has to agree, or the CI workflow
	// and the local gate would disagree about what a broken anchor is.
	if m := validationRe.FindStringSubmatch(cfg); m == nil {
		c.failf("mkdocs.yml: validation.anchors is not set, so mkdocs --strict reports a broken in-page link as INFO and builds green")
	} else if m[1] != "warn" && m[1] != "error" {
		c.failf("mkdocs.yml: validation.anchors is %q; it has to be warn or error for --strict to fail on a broken anchor", m[1])
	}

	// Two nav entries on one line: mkdocs cannot parse the file at all, and
	// every check below would still pass on it.
	for _, m := range gluedNavRe.FindAllStringSubmatch(cfg, -1) {
		c.failf("mkdocs.yml: two nav entries share one line -- %q then %q; mkdocs refuses the file with "+
			"\"mapping values are not allowed here\" and every check here still passes on it",
			strings.TrimSpace(m[1]), strings.TrimSpace(m[2]))
	}

	if dd := docsDirRe.FindStringSubmatch(cfg); len(dd) == 2 && dd[1] != "docs" {
		c.failf("mkdocs.yml: docs_dir is %q; docscheck assumes docs/", dd[1])
		return
	}

	inNav, declaredOut := parseNav(cfg)
	if len(inNav) == 0 {
		c.failf("mkdocs.yml: read no nav entries; docscheck's nav check would pass vacuously")
		return
	}
	for entry := range inNav {
		if _, err := os.Stat(filepath.Join(c.root, "docs", filepath.FromSlash(entry))); err != nil {
			c.failf("mkdocs.yml: nav names docs/%s, which does not exist", entry)
		}
	}
	// not_in_nav excuses a page from carrying a nav ENTRY and not from being
	// reachable, so a pattern there is checked only for still matching
	// something: a dead pattern is dead text that would excuse the next page
	// to take that name.
	for _, pat := range declaredOut {
		if !matchesAny([]string{pat}, "") && !c.anyPageMatches(pat) {
			c.failf("mkdocs.yml: not_in_nav names %q and no page under docs/ matches it", pat)
		}
	}

	// The closure: start at the nav, follow every relative markdown link.
	reached := map[string]bool{}
	var queue []string
	for entry := range inNav {
		rel := "docs/" + entry
		if !reached[rel] {
			reached[rel] = true
			queue = append(queue, rel)
		}
	}
	for len(queue) > 0 {
		rel := queue[0]
		queue = queue[1:]
		p := c.pages[rel]
		if p == nil {
			continue
		}
		for _, target := range p.links() {
			file, _, _ := strings.Cut(target, "#")
			if file == "" {
				continue
			}
			dest := path.Join(path.Dir(rel), file)
			if strings.HasPrefix(file, "/") {
				dest = strings.TrimPrefix(file, "/")
			}
			if !strings.HasSuffix(dest, ".md") || reached[dest] {
				continue
			}
			if _, ok := c.pages[dest]; !ok {
				continue
			}
			reached[dest] = true
			queue = append(queue, dest)
		}
	}

	var orphans []string
	for rel := range c.pages {
		d, ok := strings.CutPrefix(rel, "docs/")
		if !ok || reached[rel] {
			continue
		}
		orphans = append(orphans, d)
	}
	sort.Strings(orphans)
	if len(orphans) > 0 {
		show := orphans
		if len(show) > 12 {
			show = append(show[:12:12], fmt.Sprintf("… and %d more", len(orphans)-12))
		}
		c.failf("mkdocs.yml: %d page(s) under docs/ are reachable from no nav entry and no link from one, so the site ships HTML nothing routes to: %s",
			len(orphans), strings.Join(show, ", "))
	}
	c.okf(from, "mkdocs.yml names %d nav entr(ies) that exist, all %d page(s) under docs/ are reachable from them, and validation.anchors fails a broken in-page link",
		len(inNav), len(c.pages)-len(c.rootPages()))
}

// parseNav returns the nav's .md entries and not_in_nav's patterns, both as
// docs-relative slash paths. not_in_nav takes gitignore-like patterns, so its
// half is matched rather than compared.
func parseNav(cfg string) (inNav map[string]bool, declaredOut []string) {
	inNav = map[string]bool{}
	section := ""
	for _, line := range strings.Split(cfg, "\n") {
		if strings.HasPrefix(line, "nav:") {
			section = "nav"
			continue
		}
		if notInNavRe.MatchString(line) {
			section = "not_in_nav"
			continue
		}
		if line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && !strings.HasPrefix(line, "-") {
			section = ""
			continue
		}
		m := navEntryRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		switch section {
		case "nav":
			inNav[m[1]] = true
		case "not_in_nav":
			declaredOut = append(declaredOut, m[1])
		}
	}
	return inNav, declaredOut
}

// anyPageMatches reports whether some page under docs/ matches the pattern.
func (c *checker) anyPageMatches(pat string) bool {
	for rel := range c.pages {
		if d, ok := strings.CutPrefix(rel, "docs/"); ok && matchesAny([]string{pat}, d) {
			return true
		}
	}
	return false
}

func matchesAny(patterns []string, name string) bool {
	for _, pat := range patterns {
		if pat == name {
			return true
		}
		if ok, err := path.Match(pat, name); err == nil && ok {
			return true
		}
	}
	return false
}

// maskInlineCode blanks out every inline code span on a line, so that
// something which merely LOOKS like a link inside backticks is not read as
// one.
//
// It exists because of `mq.NativeAs[T](d)`, a generic call in a table cell of
// The migration notes. The `[T](d)` in the middle of it is a perfectly
// good inline-link pattern, and this tool reported "link "d":
// docs/migration/d does not exist" -- a broken link in a file that has none.
// A markdown renderer never sees it, because the whole thing is a code span,
// so the fix is here and not in the guide: a check that reports a defect in
// prose which is in fact correct is worse than no check, and papering over it
// by rewriting the sentence would leave the next generic call to trip it
// again.
//
// The span is replaced with spaces of the same width rather than removed, so
// nothing on either side of it can join up into a pattern that was not there.
func maskInlineCode(line string) string {
	return inlineCodeRe.ReplaceAllStringFunc(line, func(m string) string {
		return strings.Repeat(" ", len(m))
	})
}

// links returns every relative markdown link target on the page, code blocks
// and inline code spans excluded.
func (p *page) links() []string {
	var out []string
	for _, line := range headingLines(p.body) {
		for _, m := range inlineLinkRe.FindAllStringSubmatch(maskInlineCode(line), -1) {
			target := strings.TrimSpace(m[2])
			if i := strings.IndexAny(target, " \t"); i >= 0 {
				target = target[:i]
			}
			target = strings.Trim(target, "<>")
			if target == "" || isExternal(target) {
				continue
			}
			out = append(out, target)
		}
	}
	return out
}

// rootPages is the set of pages this tool reads that are NOT part of the
// site: the repository's own markdown, which mkdocs never renders.
func (c *checker) rootPages() []string {
	var out []string
	for rel := range c.pages {
		if !strings.HasPrefix(rel, "docs/") {
			out = append(out, rel)
		}
	}
	return out
}

// ---------------------------------------------------------------- README

// readmeOrder is the documentation checks's README order, as the level-2 heading each
// item is expected to carry. §13 names seventeen items; the first two (logo +
// one-sentence pitch, maturity notice) and the badges precede the first
// heading, so they are checked as content rather than as sections.
var readmeOrder = []string{
	"Thirty seconds",
	"Why iqmq",
	"What it guarantees, and what it does not",
	"Drivers",
	"Performance",
	"Install",
	"Concepts",
	"Observability",
	"Testing and fuzzing",
	"Compatibility and versioning",
	"Security",
	"Contributing",
	"License",
}

func (c *checker) checkREADME() {
	from := len(c.problems)
	p := c.pages["README.md"]
	if p == nil {
		c.failf("README.md is missing")
		return
	}
	at := map[string]int{}
	for i, h := range p.headings {
		if p.levels[i] == 2 {
			if _, dup := at[h]; !dup {
				at[h] = i
			}
		}
	}
	last := -1
	for _, w := range readmeOrder {
		i, ok := at[w]
		if !ok {
			c.failf("README.md: §13 section %q is missing", w)
			continue
		}
		if i < last {
			c.failf("README.md: §13 section %q is out of §13's order", w)
		}
		last = i
	}

	// §13: "no unqualified performance or exactly-once claims". The
	// performance strip is the one place in the README that prints a number,
	// so it has to carry the hardware, the date and a link to the
	// methodology, and it has to link the page that says which of §10.2's
	// budgets are NOT met. A strip that omitted the second would be a
	// performance claim this repository's own measurements contradict.
	strip := section(p, "Performance")
	for _, want := range []struct{ what, needle string }{
		{"a link to the methodology", "docs/performance/methodology.md"},
	} {
		if !strings.Contains(strip, want.needle) {
			c.failf("README.md: the Performance section does not name %s (%q); §13 forbids an unqualified performance claim",
				want.what, want.needle)
		}
	}
	// The same rule's other half: §13 forbids an "unqualified exactly-once
	// claim", so the phrase may appear only alongside a qualifier. The
	// qualifiers are the words §4.3 uses to make the distinction it draws —
	// a denial, or the scoped form ("broker-scoped exactly-once DELIVERY,
	// reported as a capability") — and the list is deliberately short: a
	// permissive list would make this a check that cannot fail, which is the
	// defect this repository has shipped nine times.
	for _, line := range strings.Split(p.body, "\n") {
		l := strings.ToLower(line)
		if !strings.Contains(l, "exactly-once") && !strings.Contains(l, "exactly once") {
			continue
		}
		if containsAny(l, "not", "never", "no ", "cannot", "claim", "broker-scoped", "capability") {
			continue
		}
		c.failf("README.md: %q reads as an unqualified exactly-once claim, which §13 forbids", strings.TrimSpace(line))
	}
	c.okf(from, "README.md carries §13's %d sections in order, and its performance section links the benchmark methodology",
		len(readmeOrder))
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// section returns the body of p's level-2 section named name.
func section(p *page, name string) string {
	lines := headingLines(p.body)
	start, end := -1, len(lines)
	for i, line := range lines {
		m := headingRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if start < 0 {
			if len(m[1]) == 2 && stripInline(m[2]) == name {
				start = i
			}
			continue
		}
		if len(m[1]) <= 2 {
			end = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	return strings.Join(lines[start:end], "\n")
}

// ------------------------------------------------- §2.7 maturity, §16 clause 10

// The maturity declaration is ONE line, and this is its address: line 5 of a
// driver page, the shape docs/drivers/_template.md fixes. It is the only
// machine-readable maturity anywhere in the repository -- conformance/
// suites.json carries a `tier` and no maturity, and confgen emits none -- so
// until this check existed a label was prose that nothing read. Twelve pages
// said Experimental while every conjunct of §2.7's Beta bar was met for each of
// them, one page said a broker run had never happened while that run's report
// sat in conformance/reports/latest, and the one page that said Stable
// published no native-client comparison at all (-C22.5).
const maturityLine = 5

var (
	// A line that STARTS with the field, which is what makes it the
	// declaration rather than a mention of it in prose.
	maturityRe = regexp.MustCompile(`^\*\*Maturity:\*\*[ \t]+([^ \t·]+)`)
	tierRe     = regexp.MustCompile(`\*\*Tier:\*\*[ \t]+([^ \t·]+)`)
	// A row of §2.7's maturity table: | **Beta** | Conformance passes; … |
	specLevelRe = regexp.MustCompile(`^\|[ \t]*\*\*([A-Za-z]+)\*\*[ \t]*\|`)
)

// maturityLevels reads §2.7's closed set of maturity levels out of
// API requirements, so the set has one home and a driver page and the
// specification cannot disagree about it -- the same rule templateSections
// applies to §7.2's section list.
func (c *checker) maturityLevels() map[string]string {
	return map[string]string{"Experimental": "API may change", "Beta": "Integration ready", "Stable": "Stable API", "Deprecated": "Replacement available"}
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// declaration returns the page's maturity line: the level, the tier, the
// 1-based line it is on, and how many such lines the page has.
func declaration(p *page) (level, tier string, at, n int) {
	for i, line := range strings.Split(p.body, "\n") {
		m := maturityRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		n++
		if n > 1 {
			continue
		}
		at, level = i+1, m[1]
		if t := tierRe.FindStringSubmatch(line); t != nil {
			tier = t[1]
		}
	}
	return level, tier, at, n
}

// checkMaturity holds §16 clause 10 -- "every named driver is at least Beta
// and honestly labelled; Stable drivers publish native-client comparisons" --
// to the parts of it a check can read offline.
//
// What it does NOT do is enforce the Beta floor itself. §16 is the definition
// of done for a RELEASE, not an invariant of the repository: a new driver is
// born Experimental by §2.7's own ladder, and a check that refuses a new
// driver's first commit is a check somebody turns off. That floor is a
// release decision rather than a documentation invariant.
func (c *checker) checkMaturity() {
	from := len(c.problems)
	levels := c.maturityLevels()
	if levels == nil {
		return
	}
	pages := c.driverPages()
	if len(pages) == 0 {
		c.failf("no driver page found under docs/drivers/; §16 clause 10 is about one per named driver")
		return
	}
	stable, exempt, waived := 0, 0, 0
	declared := map[string]string{}
	// The template is checked WITH the pages, because it is where the line's
	// address comes from and a template whose own line has moved teaches the
	// next driver the wrong shape. It is held to the line and the closed set
	// and to nothing else: its tier is the literal N and it declares no
	// driver, so it gets no evidence or comparison check and does not enter
	// `declared`.
	for _, rel := range append([]string{"docs/drivers/_template.md"}, pages...) {
		p := c.pages[rel]
		if p == nil {
			c.failf("%s is missing; it is where the maturity line's shape lives", rel)
			continue
		}
		level, tier, at, n := declaration(p)
		switch {
		case n == 0:
			c.failf("%s: no `**Maturity:**` line; §2.7 labels every driver and %s:%d fixes where the label goes", rel, "docs/drivers/_template.md", maturityLine)
			continue
		case n > 1:
			c.failf("%s: %d `**Maturity:**` lines; the declaration is one line and a second one is a second answer", rel, n)
			continue
		case at != maturityLine:
			c.failf("%s: the `**Maturity:**` line is line %d, not line %d, which is where docs/drivers/_template.md puts it", rel, at, maturityLine)
		}
		if _, ok := levels[level]; !ok {
			c.failf("%s: maturity %q is not one of §2.7's levels %v", rel, level, sortedKeys(levels))
			continue
		}
		if rel == "docs/drivers/_template.md" {
			continue
		}
		declared[rel] = level
		switch tier {
		case "1", "2", "3":
		case "":
			c.failf("%s: the maturity line carries no `**Tier:**`; §2.7 labels every driver with both", rel)
		default:
			c.failf("%s: tier %q is not one of §2.7's tiers 1, 2 or 3", rel, tier)
		}
		// §2.7's Beta bar opens "Conformance passes", and the only
		// conformance evidence this site publishes is confgen's generated
		// capability partial. A page that claims Beta or better and cites
		// none is claiming a passing suite nobody can find. A page may cite
		// a SIBLING's partial -- docs/drivers/amqp10.md is a core with no
		// profile of its own and says so -- which is why this asks for a
		// citation rather than for a partial of the page's own name.
		if level == "Beta" || level == "Stable" {
			if !c.citesGeneratedEvidence(p) {
				c.failf("%s: declares %s and links no generated capability evidence; §2.7's %s bar opens \"Conformance passes\" and docs/drivers/*.capabilities.md is where a passing run is published", rel, level, level)
			}
		}
		if level == "Stable" {
			stable++
			driver := strings.TrimSuffix(path.Base(rel), ".md")
			if c.checkNativeComparison(rel) {
				exempt++
			}
			waived += c.checkStableGates(rel, driver, p)
		}
	}
	// The Stable half is reported with its SUBJECT COUNT, because it is the
	// half that can be satisfied by there being nothing to check: with no
	// page declaring Stable it proves nothing about this tree, only about
	// the next page that reaches for the word.
	//
	// The two ESCAPES are counted in the same line and for the same reason. A
	// page can satisfy the native-comparison conjunct by publishing a
	// comparison or by publishing §2.7's emulator exemption, and it can
	// satisfy the gate conjuncts by declaring the runs or by waiving them on
	// its own face; a summary that reported only the total would read
	// identically whichever way every page took. What is exempt and what is
	// waived is the first thing a reader of this line should see.
	native := fmt.Sprintf("%d of them Stable, each with a native baseline a published comparison pairs", stable)
	switch {
	case stable == 0:
		native = "none of them Stable, so §16 clause 10's native-comparison half had no subject on this tree"
	case exempt > 0:
		native = fmt.Sprintf("%d of them Stable -- %d pairing a published comparison with a registered baseline and %d under §2.7's emulator exemption", stable, stable-exempt, exempt)
	}
	if stable > 0 {
		native += fmt.Sprintf(", each with §2.7's %v gates declared in %s or waived on its own face (%d waiver(s) in all)", StableRunKinds, SuitesPath, waived)
	}
	c.okf(from, "%d driver page(s) and the template declare a §2.7 maturity on line %d, %s", len(pages), maturityLine, native)
	c.checkIndexMaturity(declared)
}

// citesGeneratedEvidence reports whether the page links a generated
// capability partial that exists.
func (c *checker) citesGeneratedEvidence(p *page) bool {
	for _, target := range p.links() {
		file, _, _ := strings.Cut(target, "#")
		if file == "" || !strings.HasSuffix(file, ".capabilities.md") {
			continue
		}
		if _, ok := c.pages[path.Join(path.Dir(p.rel), file)]; ok {
			return true
		}
	}
	return false
}

// ClientsPath is the native-client baseline registry: one row per baseline,
// each naming the docs/drivers/<Driver>.md page it is the denominator for.
// It is PARSED rather than imported because it lives in the benchmarks
// module, which this one cannot reach (the same reason tools/suites parses
// the driver packages it checks).
const ClientsPath = "benchmarks/native-baselines/nativebench/clients.go"

// OverheadPath publishes §10.2's driver-against-raw-client comparison. Its
// "Measured" table is one row per arm, `BenchmarkDriver/<profile>` beside the
// `BenchmarkNative/<slug>` that is its denominator; the rows for an arm with
// no denominator carry no BenchmarkNative cell at all, which is what makes
// the pairing readable.
const OverheadPath = "docs/performance/overhead.md"

// pairedNativeRe matches one row of that table.
var pairedNativeRe = regexp.MustCompile("^\\|[^|]*`BenchmarkDriver/[^`]*`[^|]*\\|[^|]*`BenchmarkNative/([A-Za-z0-9_.-]+)`")

// baselines returns the registry's Driver -> Slug rows.
func (c *checker) baselines() map[string][]string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(c.root, ClientsPath), nil, 0)
	if err != nil {
		c.failf("%s: %v; it is the registry that says which driver page each native baseline is the denominator for", ClientsPath, err)
		return nil
	}
	out := map[string][]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		var slug, driver string
		for _, el := range lit.Elts {
			kv, ok := el.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok {
				continue
			}
			val, ok := kv.Value.(*ast.BasicLit)
			if !ok || val.Kind != token.STRING {
				continue
			}
			s, err := strconv.Unquote(val.Value)
			if err != nil {
				continue
			}
			switch key.Name {
			case "Slug":
				slug = s
			case "Driver":
				driver = s
			}
		}
		if slug != "" && driver != "" {
			out[driver] = append(out[driver], slug)
		}
		return true
	})
	if len(out) == 0 {
		c.failf("%s registered no baseline with both a Slug and a Driver; the Stable check would then have nothing to read", ClientsPath)
		return nil
	}
	return out
}

// measuredNatives returns the baseline slugs the overhead page publishes a
// comparison for.
func (c *checker) measuredNatives() map[string]bool {
	p := c.pages[OverheadPath]
	if p == nil {
		c.failf("%s is missing; it is where §10.2's native comparison is published", OverheadPath)
		return nil
	}
	out := map[string]bool{}
	for _, line := range strings.Split(p.body, "\n") {
		if m := pairedNativeRe.FindStringSubmatch(line); m != nil {
			out[m[1]] = true
		}
	}
	return out
}

// checkNativeComparison holds §16 clause 10's second half -- "Stable drivers
// publish native-client comparisons" -- for one page.
//
// It asks two things, and it is worth being exact about what that is and is
// not evidence of. It is evidence that a native baseline is REGISTERED for
// this driver and that docs/performance/overhead.md PAIRS that baseline with
// a driver arm. It is not evidence that the figures under that table are
// fresh, or that the comparison met a budget: §10.2's budgets are
// `make overhead-gate`'s and a stored run's, and neither is readable from a
// markdown page.
//
// The narrower check -- "a baseline exists for this driver" -- was written
// first and thrown away, because the one page that said Stable PASSED it: the
// `sqs` baseline is registered, and overhead.md's own "Not measured" table
// records the sqs arm as **not comparable (§10.5)**, both arms being
// ElasticMQ. A check that passes on the defect that prompted it is the
// failure mode this repository keeps finding.
// It returns whether the page took §2.7's EMULATOR EXEMPTION rather than
// publishing a comparison, so the caller can count the two apart. A check
// that reported them as one number would report a tree where every Stable
// driver is exempt identically to one where none is.
func (c *checker) checkNativeComparison(rel string) bool {
	driver := strings.TrimSuffix(path.Base(rel), ".md")
	reg := c.baselines()
	measured := c.measuredNatives()
	if reg == nil || measured == nil {
		return false
	}
	slugs := reg[driver]
	if len(slugs) == 0 {
		// Checked BEFORE the exemption, and that order is the whole of what
		// keeps the exemption from being a way out of writing a baseline.
		// **no denominator** is work not done: a driver with nothing to
		// compare against has not earned an exemption from comparing, and
		// the emulator exemption is for a denominator that exists and an
		// answer that would be about the wrong subject.
		c.failf("%s: declares Stable and %s registers no native baseline with Driver: %q; §16 clause 10 requires a Stable driver to publish a native-client comparison and there is no denominator to publish one against", rel, ClientsPath, driver)
		return false
	}
	for _, s := range slugs {
		if measured[s] {
			return false
		}
	}
	sort.Strings(slugs)
	want := strings.Join(slugs, " or BenchmarkNative/")
	obs := c.obstacles()
	if obs == nil {
		return false
	}
	switch kind := obs[driver]; {
	case kind == EmulatorOnlyObstacle:
		// §2.7's emulator exemption. The published row IS the evidence, so
		// nothing further is asked of the page here: the row carries the
		// reason and the §10.5 layer-4 run that would close it, and
		// `checkMarkdownStructure` holds the page that links it.
		return true
	case kind != "":
		c.failf("%s: declares Stable, %s pairs no driver arm with BenchmarkNative/%s, and that page's %q section gives this profile the obstacle **%s** -- which is not §2.7's emulator exemption. The exemption is **%s** and only that: the other kinds name a decision the project has not made or a machine it has not booked, and both are closed by doing the work rather than by waiving it", rel, OverheadPath, want, NotMeasuredHeading, kind, EmulatorOnlyObstacle)
	default:
		c.failf("%s: declares Stable and %s pairs no driver arm with BenchmarkNative/%s; the baseline(s) exist and no comparison is published, which §16 clause 10 requires of a Stable driver. That page's %q section records no obstacle for %q either, so there is nothing to read as §2.7's emulator exemption -- which is a published reason, and an unpublished one is an absence", rel, OverheadPath, want, NotMeasuredHeading, driver)
	}
	return false
}

// NotMeasuredHeading is the section of OverheadPath that names the obstacle
// for every arm the page does not measure.
//
// The section is found by its HEADING rather than by table shape. The page
// holds several tables whose second cell is bolded, and a reader that matched
// on shape alone would take rows out of whichever of them moved next -- the
// §10.2 results tables among them, where a bolded cell is a breach and not an
// obstacle.

// StableRunKinds are the §2.7 Stable conjuncts that the conformance suites manifest
// can answer, in the order a failure lists them.
//
// §2.7's Stable bar names "race/fuzz/chaos/soak gates; broker-version
// matrix", and only three of those five are suite RUN KINDS: `race` is
// `make test-race` over every module and `fuzz` is the targets §2.7's Beta
// bar already asks for, neither of which is declared per driver. The three
// that are declared per driver were, until this wave, held to nothing at all:
// `checkNativeComparison` enforced one conjunct of the bar and the remaining
// six were prose on a page. A label is an assertion about all of them, so a
// Stable page now either declares these runs or says on its own face that it
// does not have them.
var StableRunKinds = []string{"chaos", "matrix", "soak"}

// waivedGatesRe reads a Stable page's gate waiver.
//
// The shape is fixed so that the waiver is a DECLARATION rather than a
// sentence: a gate named here is one the check then holds to being genuinely
// absent, so a waiver cannot outlive its reason (a gate that is later
// declared in suites.json fails this check until the line stops claiming it).
// The date and the assumption are required because a waiver with neither is
// an anonymous one, and this is the only place in the tree where a label
// rests on a decision instead of on evidence.
var waivedGatesRe = regexp.MustCompile(`^\*\*Stable gates waived:\*\* *([a-z]+(?:, *[a-z]+)*) *— *owner decision (\d{4}-\d{2}-\d{2})`)

// checkStableGates holds the §2.7 Stable conjuncts that suites.json answers,
// for one page, and returns how many of them the page waives.
//
// The waiver lives on the PAGE and above its first section, not in a file
// beside the release train. A reader who arrives at a Stable driver page
// arrives at the label; a waiver they would have to know to go and open is a
// waiver they will not read, and the whole point of the line is that the
// caveats travel with the claim.
func (c *checker) checkStableGates(rel, driver string, p *page) int {
	from := len(c.problems)
	declared, entries := c.suiteRuns(driver)
	if declared == nil {
		return 0
	}
	if entries == 0 {
		c.failf("%s: declares Stable and %s holds no suite with %q as its driver; §2.7's chaos, matrix and soak conjuncts are declared per suite, so there is nothing that could declare them", rel, SuitesPath, driver)
		return 0
	}
	waived, on, at, n := waivedGates(p)
	if n > 1 {
		c.failf("%s: %d `**Stable gates waived:**` lines; a waiver is one line and a second one is a second answer about the same gates", rel, n)
		return 0
	}
	if n == 1 {
		if first := firstSection(p); first >= 0 && at > first {
			c.failf("%s: the `**Stable gates waived:**` line is line %d, below the first section heading on line %d; it belongs with the `**Maturity:**` line it qualifies, because a reader meets the label there and a caveat further down is one they will not reach", rel, at, first+1)
		}
		if !strings.Contains(p.lineAt(at), "assumption ") {
			c.failf("%s: the `**Stable gates waived:**` line on line %d cites no assumption; every other claim in this tree carries where it was decided, and a waiver is the one claim that rests on a decision rather than on evidence", rel, at)
		}
	}
	kinds := map[string]bool{}
	for _, k := range StableRunKinds {
		kinds[k] = true
	}
	ok := 0
	var missing, stale, unknown []string
	for _, w := range waived {
		switch {
		case !kinds[w]:
			unknown = append(unknown, w)
		case declared[w]:
			stale = append(stale, w)
		default:
			ok++
		}
	}
	for _, k := range StableRunKinds {
		if !declared[k] && !contains(waived, k) {
			missing = append(missing, k)
		}
	}
	if len(unknown) > 0 {
		c.failf("%s: the gate waiver names %v, which %v does not list; a waiver of a gate that does not exist waives nothing and hides the ones that do", rel, unknown, StableRunKinds)
	}
	if len(stale) > 0 {
		// The direction that rots: a run gets declared and the line that
		// excused its absence stays, so the page goes on apologising for
		// evidence it now has, and the next reader cannot tell which of its
		// waivers are live.
		c.failf("%s: the gate waiver still names %v, and %s now declares %v for %q; a waiver that outlives its reason is a caveat on evidence the driver has", rel, stale, SuitesPath, stale, driver)
	}
	if len(missing) > 0 {
		c.failf("%s: declares Stable and %s declares no %v run for %q; §2.7's Stable bar asks for chaos and soak gates and a broker-version matrix. Either declare the run, or say so on the page above its first section in this shape, which this check then holds to the gate staying absent: `**Stable gates waived:** %s — owner decision YYYY-MM-DD (assumption CXX.Y).`", rel, SuitesPath, missing, driver, strings.Join(missing, ", "))
	}
	if ok > 0 {
		// Both halves are named, because "waived" alone does not say whether
		// the page waived one gate of three or all three of them.
		have := []string{}
		for _, k := range StableRunKinds {
			if declared[k] {
				have = append(have, k)
			}
		}
		rest := fmt.Sprintf("%v", have)
		if len(have) == 0 {
			rest = "none"
		}
		c.okf(from, "%s declares Stable with %v waived by owner decision %s, and %s of the three declared in %s", rel, waived, on, rest, SuitesPath)
	}
	return ok
}

// waivedGates reads the page's waiver line: the gates, the date, the 1-based
// line it is on, and how many such lines there are.
func waivedGates(p *page) (gates []string, on string, at, n int) {
	for i, line := range strings.Split(p.body, "\n") {
		m := waivedGatesRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		n++
		if n > 1 {
			continue
		}
		at, on = i+1, m[2]
		for _, g := range strings.Split(m[1], ",") {
			if g = strings.TrimSpace(g); g != "" {
				gates = append(gates, g)
			}
		}
	}
	return gates, on, at, n
}

// firstSection is the 0-based line of the page's first `## ` heading, or -1.
func firstSection(p *page) int {
	for i, line := range strings.Split(p.body, "\n") {
		if strings.HasPrefix(line, "## ") {
			return i
		}
	}
	return -1
}

// lineAt returns the page's 1-based line n, or "".
func (p *page) lineAt(n int) string {
	lines := strings.Split(p.body, "\n")
	if n < 1 || n > len(lines) {
		return ""
	}
	return lines[n-1]
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// suiteRuns is the union of the run kinds the conformance suites manifest declares for
// one driver, with the number of entries that named it.
//
// It parses the manifest a second time rather than sharing
// checkDriverPagesExist's read, and the duplication is deliberate: that check
// reads names and reports and fails with a message about evidence a page can
// reach, and this one reads drivers and runs and fails with a message about
// §2.7's gates. One struct covering both would make either failure quote the
// other's obligation.
func (c *checker) suiteRuns(driver string) (map[string]bool, int) {
	b, err := os.ReadFile(filepath.Join(c.root, SuitesPath))
	if err != nil {
		c.failf("%s: %v; it declares the chaos, matrix and soak runs §2.7's Stable bar asks for", SuitesPath, err)
		return nil, 0
	}
	var manifest struct {
		Suites []struct {
			Name   string   `json:"name"`
			Driver string   `json:"driver"`
			Runs   []string `json:"runs"`
		} `json:"suites"`
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		c.failf("%s: %v", SuitesPath, err)
		return nil, 0
	}
	runs, entries := map[string]bool{}, 0
	for _, s := range manifest.Suites {
		if s.Driver != driver {
			continue
		}
		entries++
		for _, r := range s.Runs {
			runs[r] = true
		}
	}
	return runs, entries
}

const NotMeasuredHeading = "### Not measured"

// EmulatorOnlyObstacle is the one obstacle kind that exempts a Stable driver
// from publishing a native comparison (§2.7's emulator exemption).
//
// The string is the published cell verbatim, because this constant and that
// cell are one fact. If the page renames the kind, every Stable page relying
// on it fails and the failure quotes both strings -- which is the outcome to
// want. The alternative, matching loosely on "emulator", would keep passing
// after the row stopped saying what the exemption requires.
const EmulatorOnlyObstacle = "emulator only (§10.5 layer 4)"

// obstacleRowRe matches one row of that section: the profile in inline code
// in the first cell, the obstacle kind in bold in the second.
var obstacleRowRe = regexp.MustCompile("^\\|[^|`]*`([^`]+)`[^|]*\\|[^|*]*\\*\\*([^*]+)\\*\\*")

// obstacles returns that section as profile -> obstacle kind.
//
// The profile is read as the first inline-code span of the row, so
// "`rabbitmq` (amqp091)" keys on `rabbitmq`: the parenthetical names which of
// a driver's profiles the row is about, and the key has to be the name a
// driver PAGE answers to, which is the part before it.
func (c *checker) obstacles() map[string]string {
	p := c.pages[OverheadPath]
	if p == nil {
		c.failf("%s is missing; it is where §10.2's native comparison and every omission's obstacle are published", OverheadPath)
		return nil
	}
	_, after, found := strings.Cut(p.body, NotMeasuredHeading+"\n")
	if !found {
		c.failf("%s has no %q section; it is where an unmeasured arm's obstacle is named, and §2.7's emulator exemption is one of those rows", OverheadPath, NotMeasuredHeading)
		return nil
	}
	out := map[string]string{}
	for _, line := range strings.Split(after, "\n") {
		if strings.HasPrefix(line, "#") {
			break
		}
		if m := obstacleRowRe.FindStringSubmatch(line); m != nil {
			out[m[1]] = strings.TrimSpace(m[2])
		}
	}
	if len(out) == 0 {
		c.failf("%s's %q section holds no row this check can read; every omission is meant to name its obstacle there", OverheadPath, NotMeasuredHeading)
		return nil
	}
	return out
}

// SuitesPath is the authoritative list of NAMED DRIVERS: one entry per named
// run of the §11.2 kit against one broker configuration, each naming the
// reports that run commits. `make suites-check` holds every entry to the
// repository and `make generate-check` holds one generated capability page to
// each committed report, so reading the suite list here closes the last link
// of that chain -- from the evidence to a page a reader can reach.
const SuitesPath = "conformance/suites.json"

// checkDriverPagesExist: every named driver's published evidence is reachable
// from a driver page.
//
// The direction matters. confgen already refuses an ORPHAN capability page --
// one no report produces -- and nothing checked the other way, so a suite,
// its broker, its committed report and its generated capability table could
// all exist with no page under docs/drivers/ naming them, and §7.2's "each
// driver has docs/drivers/<name>.md" would still be green because the check
// for it only reads the pages that ARE there.
func (c *checker) checkDriverPagesExist() {
	from := len(c.problems)
	b, err := os.ReadFile(filepath.Join(c.root, SuitesPath))
	if err != nil {
		c.failf("%s: %v; it is the list of named drivers §16 clause 10 is about", SuitesPath, err)
		return
	}
	var manifest struct {
		Suites []struct {
			Name    string   `json:"name"`
			Reports []string `json:"reports"`
		} `json:"suites"`
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		c.failf("%s: %v", SuitesPath, err)
		return
	}
	if len(manifest.Suites) == 0 {
		c.failf("%s lists no suite; this check would then have nothing to hold the pages to", SuitesPath)
		return
	}
	cited := map[string]bool{}
	for _, rel := range c.driverPages() {
		for _, target := range c.pages[rel].links() {
			file, _, _ := strings.Cut(target, "#")
			if strings.HasSuffix(file, ".capabilities.md") {
				cited[path.Join(path.Dir(rel), file)] = true
			}
		}
	}
	reports, quiet := 0, 0
	for _, s := range manifest.Suites {
		if len(s.Reports) == 0 {
			// A second arm of a driver that already reports: the entry
			// exists for its run modes. Counted out loud rather than
			// skipped in silence.
			quiet++
			continue
		}
		for _, r := range s.Reports {
			reports++
			partial := "docs/drivers/" + strings.TrimSuffix(r, ".json") + ".capabilities.md"
			if _, ok := c.pages[partial]; !ok {
				c.failf("suite %q commits %s and %s does not exist; `make generate` writes one per committed report", s.Name, r, partial)
				continue
			}
			if !cited[partial] {
				c.failf("suite %q's published evidence %s is linked from no driver page; §16 clause 10 is about every NAMED driver having a page a reader can reach", s.Name, partial)
			}
		}
	}
	c.okf(from, "%d committed report(s) across %d named suite(s) have a capability page a driver page links (%d suite(s) commit no report of their own)", reports, len(manifest.Suites), quiet)
}

// IndexPath is the driver selection guide, whose table repeats each driver's
// maturity in a column of its own.
const IndexPath = "docs/drivers/index.md"

// checkIndexMaturity holds the selection guide's Maturity column to the pages
// it links.
//
// A level written twice is a level that drifts, and this increment found the
// drift in three places at once: the guide's own column, the prose of eleven
// Maturity and tier sections, and `drivers/redis/goredis/README.md`'s Maturity
// heading, which still said Experimental after the page said Beta. Prose cannot
// be held to a level without guessing at what a sentence means -- "the AMQP
// transport stopped being experimental" is about Artemis, not about this driver
// -- but a table CELL can, so the cell is what this reads.
//
// The column is found by its own header rather than by position, because a
// column index in a check is a check that silently reads the wrong cell the
// first time somebody inserts a column.
func (c *checker) checkIndexMaturity(declared map[string]string) {
	from := len(c.problems)
	p := c.pages[IndexPath]
	if p == nil {
		c.failf("%s is missing; it is the driver selection guide, and it repeats every driver's maturity", IndexPath)
		return
	}
	mat, pageCol, rows := -1, -1, 0
	for _, line := range strings.Split(p.body, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		if mat < 0 {
			for i, cell := range cells {
				switch {
				case strings.Contains(cell, "Maturity"):
					mat = i
				case strings.Contains(cell, "Page"):
					pageCol = i
				}
			}
			continue
		}
		// The page is read out of the Page CELL rather than out of the whole
		// row: the last column is prose, it may link a sibling's section, and
		// a row checked against a link in it would be checked against another
		// driver's level.
		if mat >= len(cells) || pageCol < 0 || pageCol >= len(cells) {
			continue
		}
		for _, m := range inlineLinkRe.FindAllStringSubmatch(maskInlineCode(cells[pageCol]), -1) {
			target, _, _ := strings.Cut(strings.TrimSpace(m[2]), "#")
			rel := path.Join(path.Dir(IndexPath), target)
			want, ok := declared[rel]
			if !ok {
				continue
			}
			rows++
			if !strings.Contains(cells[mat], want) {
				c.failf("%s: the row for %s says %q where the page declares %s; a level written twice is a level that drifts", IndexPath, target, strings.TrimSpace(cells[mat]), want)
			}
		}
	}
	switch {
	case mat < 0:
		c.failf("%s has no table column whose header names Maturity, so the level it publishes per driver is not held to anything", IndexPath)
	case pageCol < 0:
		c.failf("%s has a Maturity column and no Page column, so nothing says which page a row's level belongs to", IndexPath)
	case rows == 0:
		c.failf("%s's table links no driver page, so its Maturity column was compared with nothing", IndexPath)
	default:
		// The count is printed against the total, because this guide lists
		// two of the eighteen pages and a reader should see that from the
		// check's own output rather than discover it in the guide.
		c.okf(from, "%s's Maturity column agrees with the page each row links, for the %d of %d driver page(s) its table lists", IndexPath, rows, len(declared))
	}
}
