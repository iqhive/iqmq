// Package yamlmini parses the small YAML subset used by iqmq's own
// configuration files (budgets.yaml, fuzz-ratchet.yaml, ADR front matter).
//
// Supported: a document whose top level is a block mapping or a block
// sequence, and inside it block mappings and sequences, flow mappings and
// sequences on a single line, plain and quoted scalars, comments, and blank
// lines. Not supported: a top level that is a scalar or a flow collection,
// anchors, tags, multi-line scalars, multi-document streams. A document that
// uses one, or that is malformed (a key written twice, an unbalanced bracket, a
// quote that does not close, ": " inside a plain scalar), is refused with the
// line it is on. Scalars are returned as string, int64, float64, bool or nil;
// callers convert as needed. The parser never panics on malformed input.
package yamlmini

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Map is an ordered mapping.
type Map struct {
	Keys   []string
	Values map[string]any
}

// Get returns the value for key and whether it exists.
func (m *Map) Get(key string) (any, bool) {
	if m == nil {
		return nil, false
	}
	v, ok := m.Values[key]
	return v, ok
}

// set adds key to m, or reports false when m already holds it: a key written
// twice is a typo, and keeping the last one silently set a gate from a value
// its author meant to replace, or never meant at all.
func (m *Map) set(key string, v any) bool {
	if m.Values == nil {
		m.Values = map[string]any{}
	}
	if _, exists := m.Values[key]; exists {
		return false
	}
	m.Keys = append(m.Keys, key)
	m.Values[key] = v
	return true
}

// duplicateKey is the error for a key written twice.
func duplicateKey(key string, line int) error {
	return fmt.Errorf("yamlmini: duplicate key %q at line %d", key, line)
}

// Parse parses a document whose top level is a block mapping or a block
// sequence into a *Map or a []any. An empty document, or one that holds only
// comments, is nil.
func Parse(data []byte) (any, error) {
	p := &parser{}
	for i, raw := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		text := stripComment(raw)
		if strings.TrimSpace(text) == "" {
			continue
		}
		indent := len(text) - len(strings.TrimLeft(text, " "))
		if strings.HasPrefix(strings.TrimLeft(text, " "), "\t") {
			return nil, errors.New("yamlmini: tabs are not allowed for indentation")
		}
		p.lines = append(p.lines, line{indent: indent, text: strings.TrimSpace(text), num: i + 1})
	}
	if len(p.lines) == 0 {
		return nil, nil //nolint:nilnil // an empty document decodes to a nil value by definition
	}
	v, err := p.block(p.lines[0].indent)
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.lines) {
		return nil, fmt.Errorf("yamlmini: unexpected content at line %d", p.lines[p.pos].num)
	}
	return v, nil
}

// line is one line that holds something. num is its line number in the
// document, counting the comments and blank lines Parse drops, and is the
// number an error names: the position among the kept lines sent an operator
// to the wrong line of a file with a comment above the fault.
type line struct {
	indent int
	text   string
	num    int
}

type parser struct {
	lines []line
	pos   int
}

func stripComment(s string) string {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c == '"' || c == '\'') && opensQuote(s, i):
			i = closingQuote(s, i)
		case c == '#' && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t'):
			return s[:i]
		}
	}
	return s
}

// closingQuote returns the index of the quote that closes the quoted scalar
// opening at i, or len(s) when nothing does. A single-quoted scalar writes its
// quote doubled and a double-quoted one escapes it with a backslash, and
// neither ends the scalar: reading the first quote of a doubled pair as the
// closing one put the rest of the scalar outside it, and a " #" in that rest
// was taken for a comment.
func closingQuote(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case q == '"' && s[j] == '\\':
			j++
		case s[j] == q && q == '\'' && j+1 < len(s) && s[j+1] == '\'':
			j++
		case s[j] == q:
			return j
		}
	}
	return len(s)
}

// opensQuote reports whether the quote at i starts a quoted scalar, which it
// does only where a scalar starts: at the line's start, after the "- " of a
// sequence item, after the ": " of a key, or after the "[", "{" or "," of a flow
// collection. Anywhere else it is a letter of a plain scalar, and reading the
// apostrophe of "it's" as an opening quote hid the comment after it inside a
// quotation that never closed.
func opensQuote(s string, i int) bool {
	j := i
	for j > 0 && s[j-1] == ' ' {
		j--
	}
	if strings.TrimLeft(s[:j], " ") == "" {
		return true
	}
	switch s[j-1] {
	case '[', '{', ',':
		return true
	case ':':
		return j < i
	case '-':
		return j < i && strings.TrimLeft(s[:j-1], " ") == ""
	}
	return false
}

// block parses a mapping or sequence whose items sit at the given indent.
func (p *parser) block(indent int) (any, error) {
	if p.pos >= len(p.lines) {
		return nil, nil //nolint:nilnil // no more lines means an absent value
	}
	if strings.HasPrefix(p.lines[p.pos].text, "- ") || p.lines[p.pos].text == "-" {
		return p.sequence(indent)
	}
	return p.mapping(indent)
}

func (p *parser) mapping(indent int) (any, error) {
	m := &Map{}
	if err := p.mappingInto(m, indent); err != nil {
		return nil, err
	}
	return m, nil
}

// mappingInto reads the keys at the given indent into m, which already holds
// the first key when the mapping started after a dash.
func (p *parser) mappingInto(m *Map, indent int) error {
	for p.pos < len(p.lines) {
		ln := p.lines[p.pos]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return fmt.Errorf("yamlmini: unexpected indentation at line %d", ln.num)
		}
		if strings.HasPrefix(ln.text, "- ") || ln.text == "-" {
			return fmt.Errorf("yamlmini: sequence item inside mapping at line %d", ln.num)
		}
		key, rest, ok := splitKey(ln.text)
		if !ok {
			return fmt.Errorf("yamlmini: expected key: value at line %d", ln.num)
		}
		p.pos++
		var v any
		var err error
		switch {
		case rest != "":
			if v, err = parseInline(rest); err != nil {
				return fmt.Errorf("yamlmini: line %d: %w", ln.num, err)
			}
		case p.pos < len(p.lines) && p.lines[p.pos].indent > indent:
			if v, err = p.block(p.lines[p.pos].indent); err != nil {
				return err
			}
		case p.pos < len(p.lines) && p.lines[p.pos].indent == indent && (strings.HasPrefix(p.lines[p.pos].text, "- ") || p.lines[p.pos].text == "-"):
			// A sequence may sit at the same indent as its key.
			if v, err = p.sequence(indent); err != nil {
				return err
			}
		default:
		}
		if !m.set(key, v) {
			return duplicateKey(key, ln.num)
		}
	}
	return nil
}

func (p *parser) sequence(indent int) (any, error) {
	var out []any
	for p.pos < len(p.lines) {
		ln := p.lines[p.pos]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return nil, fmt.Errorf("yamlmini: unexpected indentation at line %d", ln.num)
		}
		if !strings.HasPrefix(ln.text, "- ") && ln.text != "-" {
			break
		}
		item := strings.TrimSpace(strings.TrimPrefix(ln.text, "-"))
		p.pos++
		if item == "" {
			if p.pos < len(p.lines) && p.lines[p.pos].indent > indent {
				v, err := p.block(p.lines[p.pos].indent)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else {
				out = append(out, nil)
			}
			continue
		}
		// "- key: value" starts an inline mapping whose further keys line up
		// with the first, wherever it starts: "-   key" puts them four
		// columns past the dash, not the two of "- key".
		if key, rest, ok := splitKey(item); ok && !isFlow(item) && !isQuoted(item) {
			m := &Map{}
			childIndent := indent + len(ln.text) - len(item)
			if rest == "" {
				switch {
				case p.pos < len(p.lines) && p.lines[p.pos].indent > childIndent:
					v, err := p.block(p.lines[p.pos].indent)
					if err != nil {
						return nil, err
					}
					m.set(key, v)
				case p.pos < len(p.lines) && p.lines[p.pos].indent == childIndent && isItem(p.lines[p.pos].text):
					// A sequence may sit at the same indent as its key, which
					// mapping allows for every key after this one.
					v, err := p.sequence(childIndent)
					if err != nil {
						return nil, err
					}
					m.set(key, v)
				default:
					m.set(key, nil)
				}
			} else {
				v, err := parseInline(rest)
				if err != nil {
					return nil, fmt.Errorf("yamlmini: line %d: %w", ln.num, err)
				}
				m.set(key, v)
			}
			// The keys after the first are read into the same mapping, so
			// one that repeats the first is refused with its line.
			if p.pos < len(p.lines) && p.lines[p.pos].indent == childIndent && !strings.HasPrefix(p.lines[p.pos].text, "- ") {
				if err := p.mappingInto(m, childIndent); err != nil {
					return nil, err
				}
			}
			out = append(out, m)
			continue
		}
		v, err := parseInline(item)
		if err != nil {
			return nil, fmt.Errorf("yamlmini: line %d: %w", ln.num, err)
		}
		out = append(out, v)
	}
	if out == nil {
		out = []any{}
	}
	return out, nil
}

func isFlow(s string) bool   { return strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") }
func isQuoted(s string) bool { return strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "'") }
func isItem(s string) bool   { return strings.HasPrefix(s, "- ") || s == "-" }

// splitKey splits "key: value" at the first ": " or trailing ":" outside
// quotes and flow brackets.
func splitKey(s string) (key, rest string, ok bool) {
	if isQuoted(s) || isFlow(s) {
		return "", "", false
	}
	depth := 0
	for i := range len(s) {
		switch s[i] {
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case ':':
			if depth == 0 && (i == len(s)-1 || s[i+1] == ' ') {
				return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:]), true
			}
		}
	}
	return "", "", false
}

// parseInline parses a scalar or a single-line flow collection.
func parseInline(s string) (any, error) {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "{"):
		if !strings.HasSuffix(s, "}") {
			return nil, errors.New("unterminated flow mapping")
		}
		parts, err := splitFlow(s[1 : len(s)-1])
		if err != nil {
			return nil, err
		}
		m := &Map{}
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			k, v, ok := splitKey(strings.TrimSpace(part))
			if !ok {
				return nil, fmt.Errorf("bad flow mapping entry %q", part)
			}
			val, err := parseInline(v)
			if err != nil {
				return nil, err
			}
			if !m.set(k, val) {
				return nil, fmt.Errorf("duplicate key %q in flow mapping", k)
			}
		}
		return m, nil
	case strings.HasPrefix(s, "["):
		if !strings.HasSuffix(s, "]") {
			return nil, errors.New("unterminated flow sequence")
		}
		parts, err := splitFlow(s[1 : len(s)-1])
		if err != nil {
			return nil, err
		}
		out := []any{}
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			v, err := parseInline(part)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	if err := checkScalar(s); err != nil {
		return nil, err
	}
	return scalar(s), nil
}

// checkScalar refuses a scalar that scalar would read as something its author
// did not write. A quote that does not close, or closes before the end, was
// read as a plain string with the quotes in it, and "a: b" or "a:" as that
// plain string, where YAML reads a mapping that cannot sit there.
func checkScalar(s string) error {
	if isQuoted(s) {
		switch end := closingQuote(s, 0); {
		case end == len(s):
			return errors.New("unterminated quoted scalar")
		case end != len(s)-1:
			return fmt.Errorf("text after a quoted scalar in %q", s)
		}
		return nil
	}
	if _, _, ok := splitKey(s); ok {
		return fmt.Errorf(`a plain scalar cannot hold ": " or end in ":" (%q); quote it`, s)
	}
	return nil
}

// splitFlow splits on top-level commas, honouring quotes and nesting.
//
// A quote opens a quoted scalar only where one starts, as in stripComment: the
// apostrophe of "it's" used to open one here, and every comma after it was
// read as part of the scalar.
//
// Every closing bracket has to close the one it matches. Counting depth alone
// let the depth go below zero, so "[a, b], [c]" was read as [a, "b], [c"] and
// "[a, b]]" as [a, "b]"].
func splitFlow(s string) ([]string, error) {
	var parts []string
	var open []byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c == '"' || c == '\'') && opensQuote(s, i):
			i = closingQuote(s, i)
		case c == '{' || c == '[':
			open = append(open, c)
		case c == '}' || c == ']':
			if len(open) == 0 || open[len(open)-1] != opener(c) {
				return nil, fmt.Errorf("unexpected %q in flow collection", c)
			}
			open = open[:len(open)-1]
		case c == ',' && len(open) == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	switch {
	case len(open) == 0:
	case open[len(open)-1] == '[':
		return nil, errors.New("unterminated flow sequence")
	default:
		return nil, errors.New("unterminated flow mapping")
	}
	parts = append(parts, s[start:])
	return parts, nil
}

// opener returns the bracket that the closing bracket c closes.
func opener(c byte) byte {
	if c == '}' {
		return '{'
	}
	return '['
}

func scalar(s string) any {
	if len(s) >= 2 && (s[0] == '"' && s[len(s)-1] == '"') {
		if u, err := strconv.Unquote(s); err == nil {
			return u
		}
		return s[1 : len(s)-1]
	}
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'")
	}
	switch s {
	case "", "~", "null", "Null", "NULL":
		return nil
	case "true", "True", "TRUE", "yes", "Yes", "on", "On":
		return true
	case "false", "False", "FALSE", "no", "No", "off", "Off":
		return false
	}
	if i, ok := integer(s); ok {
		return i
	}
	if f, ok := float(s); ok {
		return f
	}
	return s
}

// float parses the floats of YAML's core schema: a decimal mantissa with an
// optional exponent, .inf with an optional sign, and .nan, each of the last two
// in three spellings. It is not strconv's ParseFloat, which is Go's syntax:
// that read "inf", "nan" and "infinity" as numbers and 0x1p-2 as a quarter, and
// left ".inf" a string.
func float(s string) (float64, bool) {
	body := strings.TrimLeft(s, "+-")
	if len(s)-len(body) > 1 {
		return 0, false
	}
	switch body {
	case ".inf", ".Inf", ".INF":
		if strings.HasPrefix(s, "-") {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	case ".nan", ".NaN", ".NAN":
		return math.NaN(), body == s
	}
	if !decimalFloat(body) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// decimalFloat reports whether s is [0-9]+(.[0-9]*)? or .[0-9]+, followed by
// an optional [eE][-+]?[0-9]+.
func decimalFloat(s string) bool {
	i, digits := 0, 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		exp := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == exp {
			return false
		}
	}
	return i == len(s)
}

// integer parses the integers of YAML's core schema: signed decimal, 0o octal
// and 0x hexadecimal. It is not strconv's base 0, which is Go's literal syntax:
// that read a leading zero as octal, so 010 was 8 and 08 fell through to a
// float, and it took 0b101 for a number.
func integer(s string) (int64, bool) {
	base, digits := 10, s
	switch {
	case strings.HasPrefix(s, "0x"):
		base, digits = 16, s[2:]
	case strings.HasPrefix(s, "0o"):
		base, digits = 8, s[2:]
	}
	if base != 10 && (digits == "" || digits[0] == '+' || digits[0] == '-') {
		return 0, false
	}
	i, err := strconv.ParseInt(digits, base, 64)
	return i, err == nil
}

// String returns v as a string; numbers and bools are formatted, nil is "".
func String(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return fmt.Sprint(v)
}

// Float returns v as a float64 and whether it was numeric. A string, such as
// a quoted "3", is read by the rules the parser reads a plain scalar with:
// strconv's ParseFloat read "nan", "inf" and 0x1p-2 as numbers, so a threshold
// the parser kept as a string still reached a gate as NaN or infinity.
func Float(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case string:
		if i, ok := integer(x); ok {
			return float64(i), true
		}
		return float(x)
	}
	return 0, false
}

// Int returns v as an int and whether it was an integer. A string is read by
// the rules the parser reads a plain scalar with, as in Float.
func Int(v any) (int, bool) {
	switch x := v.(type) {
	case int64:
		return int(x), true
	case float64:
		if x == float64(int64(x)) {
			return int(x), true
		}
	case string:
		i, ok := integer(x)
		return int(i), ok
	}
	return 0, false
}

// Bool returns v as a bool and whether it was boolean.
func Bool(v any) (bool, bool) {
	b, ok := v.(bool)
	return b, ok
}

// Strings returns a sequence of scalars as strings; a lone scalar becomes a
// one-element slice.
func Strings(v any) []string {
	switch x := v.(type) {
	case nil:
		return nil
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, String(e))
		}
		return out
	default:
		return []string{String(v)}
	}
}

// Maps returns a sequence of mappings, skipping non-mapping items.
func Maps(v any) []*Map {
	seq, ok := v.([]any)
	if !ok {
		if m, ok := v.(*Map); ok {
			return []*Map{m}
		}
		return nil
	}
	var out []*Map
	for _, e := range seq {
		if m, ok := e.(*Map); ok {
			out = append(out, m)
		}
	}
	return out
}
