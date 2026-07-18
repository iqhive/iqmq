package main

import (
	"strings"
	"unicode"
)

// filler are the words a doc comment can be made of without saying anything
// the identifier does not already say: articles, copulas, the handful of verbs
// GoDoc convention puts in front of every declaration, and the words for the
// declaration's own kind. A comment whose first sentence contains nothing but
// filler and the identifier's own words is counted as a restatement.
//
// The list is deliberately narrow. Every word in it makes the check MORE
// likely to call a comment a restatement, so a word added by taste inflates
// the number; the table in main_test.go is what holds the list honest, and it
// carries the comments this repository actually has on both sides of the line.
var filler = map[string]bool{
	"a": true, "an": true, "the": true, "this": true, "that": true, "these": true,
	"it": true, "its": true, "itself": true, "one": true, "own": true, "s": true,
	"is": true, "are": true, "was": true, "be": true, "being": true, "been": true,
	"of": true, "for": true, "to": true, "in": true, "on": true, "and": true,
	"or": true, "with": true, "as": true, "by": true, "from": true, "at": true,
	"returns": true, "return": true, "reports": true, "holds": true, "gives": true,
	"contains": true, "represents": true, "describes": true, "denotes": true,
	"specifies": true, "says": true, "provides": true, "sets": true, "gets": true,
	"used": true, "uses": true, "use": true,
	"count": true, "counts": true, "number": true, "amount": true, "how": true,
	"many": true, "much": true,
	"func": true, "function": true, "method": true, "type": true, "struct": true,
	"interface": true, "constant": true, "const": true, "var": true, "variable": true,
	"field": true, "option": true, "value": true, "values": true,
}

// synonyms fold the short forms an identifier uses into the words prose uses
// for them, in both directions, so "MaxMessages is the maximum number of
// messages" is recognised for what it is. Only abbreviations are listed: a
// genuine synonym pair would let the check swallow a sentence that says
// something new.
var synonyms = map[string]string{
	"maximum": "max", "minimum": "min", "identifier": "id", "configuration": "config",
	"cfg": "config", "ctx": "context", "err": "error", "msg": "message",
	"num": "number", "opts": "options", "opt": "option", "len": "length",
}

func canonical(w string) string {
	if c, ok := synonyms[w]; ok {
		return c
	}
	return w
}

// restatesName says whether a doc comment's first sentence adds nothing to the
// identifier's own name, and returns that sentence so a reader can judge the
// verdict. "Topic is the topic." restates; "Topic is the destination the
// broker resolves the publication against." does not.
//
// The rule: split the identifier into words (camelCase, and the receiver from
// the method), take the comment's first sentence, and drop every word that is
// one of the identifier's own or is filler. If nothing is left, the sentence
// said only what the name said.
//
// It is a heuristic with two known blind spots, both in the safe direction --
// they UNDER-count, so the proportion it reports is a floor:
//
//   - a sentence that names one unrelated noun passes ("Valid reports whether
//     the mode is valid" keeps "mode"), and
//   - a restatement followed by a real second sentence is not counted at all,
//     because only the first sentence is read.
//
// An empty or whitespace-only comment is not a restatement: it is an
// undocumented declaration and is counted as one by the caller.
func restatesName(name, doc string) (string, bool) {
	first := firstSentence(doc)
	if first == "" {
		return "", false
	}
	own := map[string]bool{}
	for _, part := range strings.Split(name, ".") {
		for _, wrd := range splitIdent(part) {
			own[canonical(wrd)] = true
		}
	}
	// "Deprecated:" and "Hot path:" carry real information about the
	// declaration that its name does not.
	if strings.HasPrefix(first, "Deprecated:") || strings.Contains(first, "Hot path:") {
		return first, false
	}
	for _, wrd := range words(first) {
		wrd = canonical(wrd)
		if own[wrd] || filler[wrd] {
			continue
		}
		// A plural or possessive of one of the identifier's own words is the
		// same word: "Headers is the headers." must not escape by the "s".
		if trimmed := strings.TrimSuffix(wrd, "s"); trimmed != wrd && own[trimmed] {
			continue
		}
		return first, false
	}
	return first, true
}

// firstSentence returns the comment's first sentence, without the trailing
// full stop. A sentence ends at ". " or at the end of the first paragraph, so
// a one-line comment with no full stop is still a sentence.
func firstSentence(doc string) string {
	doc = strings.TrimSpace(doc)
	if doc == "" {
		return ""
	}
	if i := strings.Index(doc, "\n\n"); i >= 0 {
		doc = doc[:i]
	}
	doc = strings.Join(strings.Fields(doc), " ")
	for i := range len(doc) - 1 {
		if doc[i] == '.' && doc[i+1] == ' ' {
			return strings.TrimSpace(doc[:i])
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(doc, "."))
}

// words lowercases a sentence into comparable words, dropping punctuation and
// anything that is not a letter or a digit. A camelCase identifier inside the
// prose is split the same way the identifier itself is, so "ErrClosed" in a
// sentence matches "Err" and "Closed".
func words(s string) []string {
	toks := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(toks))
	for _, tok := range toks {
		out = append(out, splitIdent(tok)...)
	}
	return out
}

// splitIdent splits an identifier into lower-cased words on camelCase
// boundaries and on runs of capitals: "MaxBatchBytes" -> max, batch, bytes;
// "DSNFor" -> dsn, for; "publishV2" -> publish, v2.
func splitIdent(s string) []string {
	if s == "" {
		return nil
	}
	rs := []rune(s)
	var out []string
	start := 0
	for i := 1; i < len(rs); i++ {
		switch {
		case unicode.IsUpper(rs[i]) && !unicode.IsUpper(rs[i-1]):
		case unicode.IsUpper(rs[i-1]) && unicode.IsUpper(rs[i]) && i+1 < len(rs) && unicode.IsLower(rs[i+1]):
		default:
			continue
		}
		out = append(out, strings.ToLower(string(rs[start:i])))
		start = i
	}
	out = append(out, strings.ToLower(string(rs[start:])))
	// Drop the empties a leading separator can leave behind.
	kept := out[:0]
	for _, w := range out {
		if w != "" {
			kept = append(kept, w)
		}
	}
	return kept
}
