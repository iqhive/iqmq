package mdcheck

import (
	"strings"
	"testing"
)

// TestGluedHeadingsAreCaught pins the general form of the defect assumption
// C02b.6's trailing-cell check caught only inside a table row.
//
// A heading run onto the line before it is not a heading: the renderer shows
// it as words and the document silently loses the section. The table case is
// covered by `adrcheck`'s own TestLedgerRowEndsAtItsFinalPipe; this is the
// paragraph and list cases, which a row check cannot see because neither has
// cells, and which is how `## Consequences` came to be missing from ADR-017
// and `## B14b` from the followups backlog.
//
// The false-positive arm is what makes the check usable: four rows of the
// assumption table quote a heading in backticks while discussing one, and a
// check that reported those would have been switched off the day it landed.
func TestGluedHeadingsAreCaught(t *testing.T) {
	for name, line := range map[string]string{
		"onto a paragraph": "a sentence that ends here.## Consequences",
		"onto a table row": "| A27.1 | an assumption | somewhere | A27 |## Alternatives",
		"onto a list item": "- the last alternative, rejected.### Notes",
		"onto a bold run":  "Owner: closed by **D02**.## B14b -- the next section",
	} {
		t.Run(name, func(t *testing.T) {
			ps := Check("x.md", 0, "# Title\n\n"+line+"\n")
			if len(ps) != 1 {
				t.Fatalf("%q reported %d problem(s), want 1: %v", line, len(ps), ps)
			}
		})
	}

	// Everything a record legitimately contains.
	clean := "# Title\n\n" +
		"## A real heading\n\n" +
		"the driver page's `## Ordering` section is an empty heading\n" +
		"it emits Keep a Changelog's `## [X.Y.Z] - YYYY-MM-DD` shape\n" +
		"the same damage that had glued `## Alternatives` onto a row\n" +
		"an issue number like #12 and a language called C# are not headings\n" +
		"```\nsomething.## not a heading, this is a fenced block\n```\n" +
		"| A27.1 | an assumption | somewhere | A27 |\n"
	if ps := Check("x.md", 0, clean); len(ps) != 0 {
		t.Fatalf("a clean document reported %d problem(s), and a check that fires on correct prose is one "+
			"nobody leaves switched on: %v", len(ps), ps)
	}

	// The line number is the one in the FILE, not in the body: a reader given
	// an offset into the text after the front matter cannot find the line.
	ps := Check("x.md", 7, "# Title\n\nglued.## Heading\n")
	if len(ps) != 1 || !strings.Contains(ps[0], "x.md:10:") {
		t.Fatalf("the problem is reported as %v, want it at x.md:10 -- three lines into a body that starts "+
			"after seven lines of front matter", ps)
	}
}

// TestAnUnclosedFenceIsReportedAndDoesNotBlindTheScan pins the half of this
// package that exists because the FIRST sweep for a glued heading was wrong.
//
// A fence with no closer makes everything after it look like code. A scanner
// that tracks fences as it goes then stops checking at that point and says
// nothing, which is the same defect it is looking for: the maintenance notes had
// two glued headings and a sweep reported one, because a merge had dropped a
// code block's body and its closing fence 1,400 lines earlier and the scan had
// been inside that block ever since.
//
// So the fence itself is a problem, and the region after it is scanned as
// prose rather than skipped -- the one thing that is known about it is that it
// is NOT reliably code.
func TestAnUnclosedFenceIsReportedAndDoesNotBlindTheScan(t *testing.T) {
	// Three fences, so they cannot pair. The glued heading sits between the
	// first two -- inside a block the BREAK invented, which is the only
	// place a scan that trusted the shifted pairing would not look.
	body := "# Title\n\n" +
		"```\n" +
		"some output\n" +
		"and a heading glued on.## C07\n" +
		"```\n" +
		"prose a reader now sees as code\n" +
		"```\n"

	ps := Check("x.md", 0, body)
	if len(ps) != 2 {
		t.Fatalf("a document whose fences do not pair reported %d problem(s), want 2 -- the fence and the "+
			"heading after it: %v", len(ps), ps)
	}
	if !strings.Contains(ps[0], "never closed") {
		t.Errorf("the first problem is %q, want the unpaired fence", ps[0])
	}
	if !strings.Contains(ps[1], "## C07") {
		t.Errorf("the second problem is %q. A scan that believed the shifted pairing would have called "+
			"line 5 fenced and said nothing, which is exactly how the real sweep of the maintenance notes "+
			"found one glued heading where there were two", ps[1])
	}

	// When a heading at column zero is inside one of those invented blocks,
	// it names WHERE the document was broken -- the last unpaired fence is
	// usually an innocent closer a long way past it.
	located := Check("x.md", 0, "# Title\n\n"+
		"```\n"+
		"--- FAIL: something (0.09s)\n"+
		"## C07 the next section\n"+
		"\n"+
		"more prose\n"+
		"```go\n"+
		"code\n"+
		"```\n")
	if len(located) == 0 || !strings.Contains(located[0], "x.md:3:") {
		t.Fatalf("the break is reported as %v, want it at the fence on line 3 that swallowed the heading, "+
			"and not at the innocent fence at the far end", located)
	}
	if !strings.Contains(located[0], "## C07 the next section") {
		t.Errorf("the report is %q and does not name the heading inside the block, which is the evidence "+
			"that locates the break", located[0])
	}

	// A balanced document still skips what is genuinely inside a block.
	ok := "# Title\n\n```\nsomething.## not a heading\n```\n"
	if ps := Check("x.md", 0, ok); len(ps) != 0 {
		t.Fatalf("a balanced document reported %d problem(s): %v", len(ps), ps)
	}
}
