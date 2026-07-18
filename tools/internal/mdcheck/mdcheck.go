// Package mdcheck holds the markdown structural checks that more than one
// tool in this repository needs.
//
// It exists because the same defect has now been found four times, in three
// files, by three different means, and each time the check that would have
// caught it lived somewhere that did not look at that file. `tools/adrcheck`
// reads `adr/`, `tools/docscheck` reads `docs/`, and nothing at all read
// `hack/`, which is where the worst instance was: a code fence whose body and
// closing fence a merge had dropped, so 1,400 lines of the followups backlog
// -- including a `## C07` heading -- were inside a code block (assumption
// C02b.8).
//
// The checks are for the DAMAGE and not for well-formedness in general. ADR-002
// keeps a markdown parser out of the root module for the same reason
// `tools/docscheck` scans lines rather than parsing YAML, and a parser would
// not have caught either of these anyway: both documents parse, and both parse
// into the wrong thing.
package mdcheck

import (
	"fmt"
	"regexp"
	"strings"
)

// gluedHeadingRE matches a heading that begins in the middle of a line: two or
// more hashes with text after them, run onto something that is not whitespace.
//
// Two hashes and not one, because a single hash fires on prose that mentions
// an issue number or a language called C-sharp.
var gluedHeadingRE = regexp.MustCompile(`[^\s#](#{2,6}) \S`)

// codeSpanRE is an inline code span, which is where every legitimate mid-line
// `## Something` in these records lives: a document that DISCUSSES a heading
// writes it in backticks, and four rows of ADR-017's assumption table do.
var codeSpanRE = regexp.MustCompile("`[^`]*`")

// headingRE is a markdown heading at the start of a line.
var headingRE = regexp.MustCompile(`^#{1,6} \S`)

// firstHeading is the index of the first heading line strictly between two
// fences, or -1.
func firstHeading(lines []string, from, to int) int {
	for i := from; i < to && i < len(lines); i++ {
		if headingRE.MatchString(lines[i]) {
			return i
		}
	}
	return -1
}

// Check reports the structural damage in one markdown document.
//
// offset is how many lines precede body in the file, so that a caller which
// strips front matter still reports the line a reader can find. Problems are
// returned in document order, each prefixed with "path:line: ".
func Check(path string, offset int, body string) []string {
	var out []string
	lines := strings.Split(body, "\n")

	// The fences are found FIRST and separately, because the glued-heading
	// scan has to skip fenced blocks and a scan that tracks the fences as it
	// goes inherits their damage: an unterminated fence makes the rest of
	// the file look like code, so the scan silently stops checking. That is
	// the same shape as the defects it is looking for, and it is how the
	// first sweep for the glued heading in the maintenance notes reported one
	// where there were two.
	var fences []int
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fences = append(fences, i)
		}
	}
	fenced := map[int]bool{}
	if len(fences)%2 == 0 {
		for i := 0; i+1 < len(fences); i += 2 {
			for j := fences[i]; j <= fences[i+1]; j++ {
				fenced[j] = true
			}
		}
	}
	if len(fences)%2 == 1 {
		// An odd count says the document is damaged and says nothing about
		// WHERE: from the stray fence onwards every pair is shifted by one,
		// so the last unmatched fence is usually an innocent closer far
		// past the break. Naming it sends a reader to the wrong place, and
		// the wrong place is 1,400 lines from the right one.
		//
		// The evidence that locates it is a markdown HEADING at column zero
		// inside what the shifted pairing calls a block. A code block in
		// these documents holds Go, shell, YAML or a test log, and none of
		// those begins a line with "## "; a heading there means the block
		// is not a block, and the fence that opened it is the one whose
		// closer went missing. That is exactly the the maintenance notes
		// case, where it points at the fence and not at the far end.
		at := fences[len(fences)-1]
		why := "and nothing in the document says where it was broken"
		for i := 0; i+1 < len(fences); i += 2 {
			if h := firstHeading(lines, fences[i]+1, fences[i+1]); h >= 0 {
				at = fences[i]
				why = fmt.Sprintf("and %q on line %d is inside it, which is how far the damage reaches",
					strings.TrimSpace(lines[h]), offset+h+1)
				break
			}
		}
		out = append(out, fmt.Sprintf("%s:%d: this code fence is never closed, so everything after it "+
			"renders as code -- a heading inside a block is not a heading, and a merge that drops a "+
			"closing fence takes the rest of the document with it, %s", path, offset+at+1, why))
		// `fenced` was deliberately left EMPTY above, so the glued-heading
		// scan below reads every line of a document whose fences are known
		// not to pair. It is the one thing known about such a document:
		// which regions are code is exactly what cannot be trusted, and a
		// scan that trusted the shifted pairing would go blind precisely
		// where the damage is. the maintenance notes had two glued headings
		// and the first sweep for them reported one, because the second
		// was inside a nominal block that the break had invented. A code
		// line that now reads as a glued heading is noise in a document
		// already being reported as damaged, and the alternative is the
		// silence that hid the defect for a month.
	}

	for i, line := range lines {
		if fenced[i] {
			continue
		}
		bare := codeSpanRE.ReplaceAllString(line, " ")
		m := gluedHeadingRE.FindStringIndex(bare)
		if m == nil {
			continue
		}
		out = append(out, fmt.Sprintf("%s:%d: %q begins in the middle of a line; a heading run onto the "+
			"text before it is not a heading -- the renderer shows it as words and the document loses the "+
			"whole section it was opening", path, offset+i+1, strings.TrimSpace(bare[m[0]+1:])))
	}
	return out
}
