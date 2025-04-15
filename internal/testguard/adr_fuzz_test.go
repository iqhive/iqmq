package testguard

import (
	"strings"
	"testing"
)

// FuzzParseADRAutoBinding checks the front-matter parser never panics and
// only binds when the document has front matter naming the family.
func FuzzParseADRAutoBinding(f *testing.F) {
	f.Add("---\nauto: {queue: seqmpmc}\n---\n", "queue")
	f.Add("---\nauto:\n  queue: chan\n---\n", "queue")
	f.Add("---\nauto: {}\n---\n", "queue")
	f.Add("junk", "wait")
	f.Fuzz(func(t *testing.T, doc, family string) {
		slug, ok := ParseADRAutoBinding(doc, family)
		if !ok {
			if slug != "" {
				t.Fatalf("slug %q without binding", slug)
			}
			return
		}
		if !strings.HasPrefix(doc, "---\n") || !strings.Contains(doc, family) || !strings.Contains(doc, "auto:") {
			t.Fatalf("bound %q from a document without front matter/family/auto: %q", slug, doc)
		}
	})
}
