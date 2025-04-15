package testguard

import "testing"

func TestParseADRAutoBinding(t *testing.T) {
	flow := "---\nstatus: Accepted\nauto: {queue: seqmpmc, wait: hybrid}\n---\n# x"
	if s, ok := ParseADRAutoBinding(flow, "queue"); !ok || s != "seqmpmc" {
		t.Fatalf("flow: %q %v", s, ok)
	}
	if s, ok := ParseADRAutoBinding(flow, "wait"); !ok || s != "hybrid" {
		t.Fatalf("flow wait: %q %v", s, ok)
	}
	block := "---\nauto:\n  wait: hybrid\n  queue: chan\nsupersedes: \"\"\n---\n"
	if s, ok := ParseADRAutoBinding(block, "queue"); !ok || s != "chan" {
		t.Fatalf("block: %q %v", s, ok)
	}
	for _, doc := range []string{"---\nauto: {}\n---\n", "no front matter", "---\nstatus: x\n---\n", "---\nauto: {queue: x}"} {
		if _, ok := ParseADRAutoBinding(doc, "queue"); ok {
			t.Fatalf("%q should not bind", doc)
		}
	}
	if _, _, err := ADRAutoBinding("../..", "queue"); err != nil {
		t.Fatal(err)
	}
}
