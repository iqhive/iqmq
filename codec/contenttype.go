package codec

import (
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// ContentTypeError reports a body whose content type is not the one a codec
// speaks, or a content type that cannot be parsed at all.
//
// It carries errcode.Unsupported, so errors.Is(err, mq.ErrUnsupported) holds
// and drivers need no translation.
type ContentTypeError struct {
	// Want is the content type the codec produces, empty when unknown.
	Want string
	// Got is the content type carried by the message.
	Got string
	// Reason, when non-empty, says why Got could not be used: it names the
	// syntax error for a malformed value and is empty for a plain mismatch.
	Reason string
}

// Error implements error.
func (e *ContentTypeError) Error() string {
	var b strings.Builder
	b.WriteString("codec: content type ")
	b.WriteString(quote(e.Got))
	switch {
	case e.Reason != "":
		b.WriteString(" is invalid: ")
		b.WriteString(e.Reason)
	case e.Want != "":
		b.WriteString(" is not ")
		b.WriteString(quote(e.Want))
	default:
		b.WriteString(" has no codec")
	}
	return b.String()
}

// Code implements errcode.Coder: a content type this build cannot decode is
// an unsupported feature, not a corrupt message.
func (e *ContentTypeError) Code() errcode.Code { return errcode.Unsupported }

// Is reports whether target carries the same Code, the matching rule of
// internal/errcode.
func (e *ContentTypeError) Is(target error) bool {
	c, ok := target.(errcode.Coder)
	return ok && c.Code() == e.Code()
}

// quote wraps s in double quotes for an error message.
func quote(s string) string { return `"` + s + `"` }

// Param is one content-type parameter, for example charset=utf-8. Names are
// lower-case; values keep their case and are already unquoted.
type Param struct {
	// Name is the lower-case parameter name.
	Name string
	// Value is the parameter value with quoting removed.
	Value string
}

// MediaType is a parsed content type.
//
// The zero value is not a valid media type; ParseContentType produces every
// one. Media is the canonical lower-case "type/subtype" and is what Registry
// keys on.
type MediaType struct {
	// Media is the canonical lower-case type/subtype without parameters.
	Media string
	// Type is the lower-case type, the part before the slash.
	Type string
	// Subtype is the lower-case subtype, the part after the slash.
	Subtype string
	// Suffix is the structured syntax suffix: the part of Subtype after the
	// last plus sign ("json" in "application/vnd.acme.order+json"), empty
	// when the subtype has none.
	Suffix string
	// Params are the parameters in the order they appeared.
	Params []Param
}

// Param returns the value of the named parameter. The name is matched
// case-insensitively only when it is already lower-case, which is what
// ParseContentType produces.
func (m MediaType) Param(name string) (string, bool) {
	for _, p := range m.Params {
		if p.Name == name {
			return p.Value, true
		}
	}
	return "", false
}

// String returns the canonical form: the lower-case media type followed by
// the parameters, quoting a value that is not a bare token. Parsing the
// result yields an equal MediaType.
func (m MediaType) String() string {
	if len(m.Params) == 0 {
		return m.Media
	}
	var b strings.Builder
	b.WriteString(m.Media)
	for _, p := range m.Params {
		b.WriteString("; ")
		b.WriteString(p.Name)
		b.WriteByte('=')
		if isToken(p.Value) {
			b.WriteString(p.Value)
			continue
		}
		b.WriteByte('"')
		for i := range len(p.Value) {
			if c := p.Value[i]; c == '"' || c == '\\' {
				b.WriteByte('\\')
			}
			b.WriteByte(p.Value[i])
		}
		b.WriteByte('"')
	}
	return b.String()
}

// ParseContentType parses a content type as defined by RFC 9110 §8.3:
// type/subtype followed by optional parameters. Type, subtype and parameter
// names are lower-cased; parameter values are unquoted.
//
// It never panics. A malformed value yields a *ContentTypeError whose Reason
// names the problem.
func ParseContentType(s string) (MediaType, error) {
	m, cte := parseContentType(s)
	if cte != nil {
		return MediaType{}, cte
	}
	return m, nil
}

// parseContentType is ParseContentType with a concrete error type, so callers
// inside the package read Reason without a type assertion.
func parseContentType(s string) (MediaType, *ContentTypeError) {
	rest := trimOWS(s)
	if rest == "" {
		return MediaType{}, &ContentTypeError{Got: s, Reason: "empty"}
	}
	if i := strings.IndexByte(rest, ';'); i >= 0 {
		rest = rest[i+1:]
	} else {
		rest = ""
	}
	media, typ, subtype, suffix, ok := mediaOf(s)
	if !ok {
		return MediaType{}, &ContentTypeError{Got: s, Reason: "expected type/subtype"}
	}
	m := MediaType{Media: media, Type: typ, Subtype: subtype, Suffix: suffix}
	for trimOWS(rest) != "" {
		var p Param
		var err error
		p, rest, err = parseParam(rest)
		if err != nil {
			return MediaType{}, &ContentTypeError{Got: s, Reason: err.Error()}
		}
		m.Params = append(m.Params, p)
	}
	return m, nil
}

// parseParam decodes one "name=value" parameter and returns the unparsed
// remainder, which is empty or starts after the separating semicolon.
func parseParam(in string) (p Param, rest string, err error) {
	in = trimOWS(in)
	eq := strings.IndexByte(in, '=')
	if eq <= 0 {
		return Param{}, "", errParamSyntax
	}
	name := trimOWS(in[:eq])
	if !isToken(name) {
		return Param{}, "", errParamName
	}
	p.Name = lower(name)
	in = in[eq+1:]
	if in == "" {
		return Param{}, "", errParamValue
	}
	if in[0] != '"' {
		value := in
		if i := strings.IndexByte(in, ';'); i >= 0 {
			value, rest = in[:i], in[i+1:]
		}
		value = trimOWS(value)
		if !isToken(value) {
			return Param{}, "", errParamValue
		}
		p.Value = value
		return p, rest, nil
	}
	var b strings.Builder
	for i := 1; i < len(in); i++ {
		switch c := in[i]; c {
		case '\\':
			if i+1 >= len(in) {
				return Param{}, "", errParamQuote
			}
			i++
			b.WriteByte(in[i])
		case '"':
			p.Value = b.String()
			rest = trimOWS(in[i+1:])
			if rest == "" {
				return p, "", nil
			}
			if rest[0] != ';' {
				return Param{}, "", errParamSyntax
			}
			return p, rest[1:], nil
		default:
			b.WriteByte(c)
		}
	}
	return Param{}, "", errParamQuote
}

// Parameter syntax errors. They never escape the package: ParseContentType
// wraps their message into the *ContentTypeError it returns.
var (
	errParamSyntax = errcode.New(errcode.InvalidArgument, "malformed parameter")
	errParamName   = errcode.New(errcode.InvalidArgument, "invalid parameter name")
	errParamValue  = errcode.New(errcode.InvalidArgument, "invalid parameter value")
	errParamQuote  = errcode.New(errcode.InvalidArgument, "unterminated quoted value")
)

// Match reports whether a body whose content type is got can be decoded by a
// codec that produces want.
//
// An empty got matches: header-less backends drop the content type and the
// receiver falls back to the codec it was configured with. Beyond an exact
// match, a structured syntax suffix matches its base type, so a codec
// producing application/json accepts application/vnd.acme.order+json.
// Anything else is a *ContentTypeError.
//
// Parameters (charset and friends) are ignored and not validated: only the
// media type decides. ParseContentType is the strict entry point.
func Match(want, got string) error {
	if got == "" || want == "" || want == got {
		return nil
	}
	wm, wtyp, wsub, _, ok := mediaOf(want)
	if !ok {
		return &ContentTypeError{Want: want, Got: got, Reason: "codec content type is invalid"}
	}
	gm, gtyp, _, gsuffix, ok := mediaOf(got)
	if !ok {
		return &ContentTypeError{Want: want, Got: got, Reason: "expected type/subtype"}
	}
	if gm == wm || (gsuffix != "" && gtyp == wtyp && gsuffix == wsub) {
		return nil
	}
	return &ContentTypeError{Want: want, Got: got}
}

// mediaOf returns the canonical media type of a content type together with
// its parts, ignoring everything after the first semicolon, and reports
// whether the type/subtype is well formed.
//
// Hot path: it allocates nothing for an already lower-case value, which is
// what a canonical content-type header carries.
func mediaOf(s string) (media, typ, subtype, suffix string, ok bool) {
	head := trimOWS(s)
	if i := strings.IndexByte(head, ';'); i >= 0 {
		head = trimOWS(head[:i])
	}
	slash := strings.IndexByte(head, '/')
	if slash <= 0 || slash == len(head)-1 {
		return "", "", "", "", false
	}
	if !isToken(head[:slash]) || !isToken(head[slash+1:]) {
		return "", "", "", "", false
	}
	media = lower(head)
	typ, subtype = media[:slash], media[slash+1:]
	if plus := strings.LastIndexByte(subtype, '+'); plus >= 0 && plus < len(subtype)-1 {
		suffix = subtype[plus+1:]
	}
	return media, typ, subtype, suffix, true
}

// tokenChars marks the characters RFC 9110 allows in a token.
var tokenChars [256]bool

func init() {
	const specials = "!#$%&'*+-.^_`|~"
	for c := 'a'; c <= 'z'; c++ {
		tokenChars[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		tokenChars[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		tokenChars[c] = true
	}
	for i := range len(specials) {
		tokenChars[specials[i]] = true
	}
}

// isToken reports whether s is a non-empty RFC 9110 token.
func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if !tokenChars[s[i]] {
			return false
		}
	}
	return true
}

// lower returns s in lower case, returning s itself when it already is, so
// the common canonical content type is parsed without allocating.
func lower(s string) string {
	for i := range len(s) {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

// trimOWS trims the optional whitespace RFC 9110 allows around tokens.
func trimOWS(s string) string { return strings.Trim(s, " \t") }
