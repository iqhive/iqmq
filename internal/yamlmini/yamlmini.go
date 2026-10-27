// Package yamlmini parses the small YAML subset used by iqmq's own
// configuration files (budgets.yaml, fuzz-ratchet.yaml, ADR front matter).
//
// Supported: block mappings and sequences, flow mappings and sequences on a
// single line, plain and quoted scalars, comments, and blank lines. Not
// supported: anchors, tags, multi-line scalars, multi-document streams.
// Scalars are returned as string, int64, float64, bool or nil; callers
// convert as needed. The parser never panics on malformed input.
package yamlmini

import (
	"errors"
	"fmt"
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

func (m *Map) set(key string, v any) {
	if m.Values == nil {
		m.Values = map[string]any{}
	}
	if _, exists := m.Values[key]; !exists {
		m.Keys = append(m.Keys, key)
	}
	m.Values[key] = v
}

// Parse parses a document into a *Map, a []any or a scalar.
func Parse(data []byte) (any, error) {
	p := &parser{}
	for _, raw := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		text := stripComment(raw)
		if strings.TrimSpace(text) == "" {
			continue
		}
		indent := len(text) - len(strings.TrimLeft(text, " "))
		if strings.HasPrefix(strings.TrimLeft(text, " "), "\t") {
			return nil, errors.New("yamlmini: tabs are not allowed for indentation")
		}
		p.lines = append(p.lines, line{indent: indent, text: strings.TrimSpace(text)})
	}
	if len(p.lines) == 0 {
		return nil, nil //nolint:nilnil // an empty document decodes to a nil value by definition
	}
	v, err := p.block(p.lines[0].indent)
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.lines) {
		return nil, fmt.Errorf("yamlmini: unexpected content at line %d", p.pos+1)
	}
	return v, nil
}

type line struct {
	indent int
	text   string
}

type parser struct {
	lines []line
	pos   int
}

func stripComment(s string) string {
	inQ := byte(0)
	for i := range len(s) {
		c := s[i]
		switch {
		case inQ != 0:
			if c == inQ {
				inQ = 0
			}
		case (c == '"' || c == '\'') && opensQuote(s, i):
			inQ = c
		case c == '#' && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t'):
			return s[:i]
		}
	}
	return s
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
	for p.pos < len(p.lines) {
		ln := p.lines[p.pos]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return nil, fmt.Errorf("yamlmini: unexpected indentation at line %d", p.pos+1)
		}
		if strings.HasPrefix(ln.text, "- ") || ln.text == "-" {
			return nil, fmt.Errorf("yamlmini: sequence item inside mapping at line %d", p.pos+1)
		}
		key, rest, ok := splitKey(ln.text)
		if !ok {
			return nil, fmt.Errorf("yamlmini: expected key: value at line %d", p.pos+1)
		}
		p.pos++
		if rest == "" {
			switch {
			case p.pos < len(p.lines) && p.lines[p.pos].indent > indent:
				v, err := p.block(p.lines[p.pos].indent)
				if err != nil {
					return nil, err
				}
				m.set(key, v)
			case p.pos < len(p.lines) && p.lines[p.pos].indent == indent && (strings.HasPrefix(p.lines[p.pos].text, "- ") || p.lines[p.pos].text == "-"):
				// A sequence may sit at the same indent as its key.
				v, err := p.sequence(indent)
				if err != nil {
					return nil, err
				}
				m.set(key, v)
			default:
				m.set(key, nil)
			}
			continue
		}
		v, err := parseInline(rest)
		if err != nil {
			return nil, fmt.Errorf("yamlmini: line %d: %w", p.pos, err)
		}
		m.set(key, v)
	}
	return m, nil
}

func (p *parser) sequence(indent int) (any, error) {
	var out []any
	for p.pos < len(p.lines) {
		ln := p.lines[p.pos]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return nil, fmt.Errorf("yamlmini: unexpected indentation at line %d", p.pos+1)
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
		// "- key: value" starts an inline mapping whose further keys are
		// indented past the dash.
		if key, rest, ok := splitKey(item); ok && !isFlow(item) && !isQuoted(item) {
			m := &Map{}
			childIndent := indent + 2
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
					return nil, fmt.Errorf("yamlmini: line %d: %w", p.pos, err)
				}
				m.set(key, v)
			}
			if p.pos < len(p.lines) && p.lines[p.pos].indent == childIndent && !strings.HasPrefix(p.lines[p.pos].text, "- ") {
				more, err := p.mapping(childIndent)
				if err != nil {
					return nil, err
				}
				if mm, ok := more.(*Map); ok {
					for _, k := range mm.Keys {
						m.set(k, mm.Values[k])
					}
				}
			}
			out = append(out, m)
			continue
		}
		v, err := parseInline(item)
		if err != nil {
			return nil, fmt.Errorf("yamlmini: line %d: %w", p.pos, err)
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
		m := &Map{}
		for _, part := range splitFlow(s[1 : len(s)-1]) {
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
			m.set(k, val)
		}
		return m, nil
	case strings.HasPrefix(s, "["):
		if !strings.HasSuffix(s, "]") {
			return nil, errors.New("unterminated flow sequence")
		}
		out := []any{}
		for _, part := range splitFlow(s[1 : len(s)-1]) {
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
	return scalar(s), nil
}

// splitFlow splits on top-level commas, honouring quotes and nesting.
func splitFlow(s string) []string {
	var parts []string
	depth := 0
	inQ := byte(0)
	start := 0
	for i := range len(s) {
		c := s[i]
		switch {
		case inQ != 0:
			if c == inQ {
				inQ = 0
			}
		case c == '"' || c == '\'':
			inQ = c
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
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
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
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

// Float returns v as a float64 and whether it was numeric.
func Float(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

// Int returns v as an int and whether it was an integer.
func Int(v any) (int, bool) {
	switch x := v.(type) {
	case int64:
		return int(x), true
	case float64:
		if x == float64(int64(x)) {
			return int(x), true
		}
	case string:
		i, err := strconv.Atoi(x)
		return i, err == nil
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
