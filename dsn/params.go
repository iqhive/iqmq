package dsn

import (
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Params are a DSN's query parameters: ordered, at most one value per name, and
// strict about the names nobody read.
//
// The strictness is the point. A driver reads the parameters it knows with the
// typed accessors, each of which marks the name as read, and then calls
// [Params.Strict], which fails when anything is left. That turns a typo'd
// parameter into a construction error naming it, where the alternative — the
// one every URL-based configuration in the wild ships — is a silently ignored
// setting nobody finds until production behaves unlike the DSN says it should
// (ADR-010).
//
// A Params value shares its storage with every copy, so the read marks a driver
// leaves are visible through the Config it was copied from: a driver that reads
// a parameter from one copy and calls Strict on another still gets one answer.
type Params struct {
	keys []string
	vals []string
	read []bool
}

// Len returns the number of parameters.
func (p *Params) Len() int { return len(p.keys) }

// All iterates the parameters in the order the DSN wrote them. It does not mark
// anything as read: iterating is inspection, not consumption.
func (p *Params) All() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for i, k := range p.keys {
			if !yield(k, p.vals[i]) {
				return
			}
		}
	}
}

// Format renders the parameters as [Config.String] renders its query, the
// value of every credential-named one redacted, under every verb fmt hands to
// a method, as [Config.Format] does.
//
// A Config renders itself, but its Params field is exported and gets printed
// on its own, and Params has no String to answer %v, because [Params.String]
// is the typed accessor. fmt therefore printed the unexported fields as three
// bare slices, a credential's value among them: {[token] [s3cr3t] [false]}.
func (p Params) Format(f fmt.State, verb rune) {
	var b strings.Builder
	p.render(&b)
	if verb == 'v' && f.Flag('#') {
		_, _ = io.WriteString(f, b.String())
		return
	}
	_, _ = fmt.Fprintf(f, fmt.FormatString(f, verb), b.String())
}

// render writes the parameters as a query, "k=v&k=v" with no leading "?", in
// the order the DSN wrote them and with every credential's value redacted.
func (p *Params) render(b *strings.Builder) {
	for i, k := range p.keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(escape(k))
		b.WriteByte('=')
		if IsCredentialName(k) {
			b.WriteString(Redacted)
			continue
		}
		b.WriteString(escape(p.vals[i]))
	}
}

// find returns the index of key, or -1.
func (p *Params) find(key string) int {
	for i, k := range p.keys {
		if k == key {
			return i
		}
	}
	return -1
}

// Has reports whether the parameter is present, without marking it read. A
// driver that branches on presence still has to read the value, so inspecting
// one here does not satisfy [Params.Strict].
func (p *Params) Has(key string) bool { return p.find(key) >= 0 }

// Lookup returns the parameter's value and marks it read.
func (p *Params) Lookup(key string) (string, bool) {
	i := p.find(key)
	if i < 0 {
		return "", false
	}
	p.read[i] = true
	return p.vals[i], true
}

// MarkRead marks a parameter read without giving it a value, for a driver that
// handles one specially. Marking a name the DSN does not carry does nothing.
func (p *Params) MarkRead(key string) {
	if i := p.find(key); i >= 0 {
		p.read[i] = true
	}
}

// String reads a string parameter and calls set with it, and does not call set
// when the DSN did not name it. An absent parameter is never an error: the
// default is the caller's, and a DSN that omits a parameter is asking for it.
func (p *Params) String(key string, set func(string)) error {
	if v, ok := p.Lookup(key); ok {
		set(v)
	}
	return nil
}

// Bool reads a boolean parameter. Only the two spellings a DSN is written with
// are accepted: "1" and "t" are strconv's notion of a boolean, not a connection
// string's, and accepting them invites "tls=0" to mean something it does not.
func (p *Params) Bool(key string, set func(bool)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	switch v {
	case "true":
		set(true)
	case "false":
		set(false)
	default:
		return valueErr(key)
	}
	return nil
}

// Int reads an integer parameter.
func (p *Params) Int(key string, set func(int)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return valueErr(key)
	}
	set(n)
	return nil
}

// PositiveInt reads an integer parameter that must be greater than zero,
// refusing zero and every negative value.
func (p *Params) PositiveInt(key string, set func(int)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return valueErr(key)
	}
	set(n)
	return nil
}

// Int64 reads a 64-bit integer parameter.
func (p *Params) Int64(key string, set func(int64)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return valueErr(key)
	}
	set(n)
	return nil
}

// Duration reads a duration parameter in Go's own notation ("30s", "1m30s").
func (p *Params) Duration(key string, set func(time.Duration)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return valueErr(key)
	}
	set(d)
	return nil
}

// Enum reads a parameter whose values are the names of an enumeration and calls
// set with the index of the name it matched.
func (p *Params) Enum(key string, names []string, set func(int)) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	for i, n := range names {
		if n == v {
			set(i)
			return nil
		}
	}
	return valueErr(key)
}

// Unread returns the names of the parameters nothing has read, in the order the
// DSN wrote them.
func (p *Params) Unread() []string {
	var out []string
	for i, k := range p.keys {
		if !p.read[i] {
			out = append(out, k)
		}
	}
	return out
}

// Strict fails when any parameter was never read, naming every one of them. It
// is the call that makes a typo'd DSN a construction error.
//
// The names are QUOTED, which is not cosmetic. A parameter name comes off the
// wire after percent decoding, so it can carry any byte at all, and an error
// that interpolated one raw produced a message that was not valid UTF-8 — a
// failure no log pipeline can carry and no test can compare. `natsjs://0?\x93`
// was the input that found it.
func (p *Params) Strict() error {
	left := p.Unread()
	if len(left) == 0 {
		return nil
	}
	quoted := make([]string, len(left))
	for i, k := range left {
		quoted[i] = quoteName(k)
	}
	return errcode.New(errcode.InvalidConfig,
		"iqmq/dsn: the DSN names parameters this driver does not have: "+strings.Join(quoted, ", "))
}

// valueErr names the parameter whose value was refused. A failure that does not
// say which parameter is a failure a caller has to guess at.
func valueErr(key string) error {
	return errcode.New(errcode.InvalidConfig,
		"iqmq/dsn: the DSN gives a parameter a value it cannot take: "+quoteName(key))
}

// quoteName renders a parameter name so that the result is always valid UTF-8
// and always printable, whatever bytes the DSN carried.
//
// A name that is already plain text is left alone rather than wrapped in
// quotation marks: the common case is a typo, and `stream` reads better in a
// failure than `"stream"` does.
func quoteName(k string) string {
	if isPlainName(k) {
		return k
	}
	return strconv.Quote(k)
}

// isPlainName reports whether a name is printable ASCII with no space or
// comma — the alphabet a parameter name is written in, and the one a message can
// interpolate unescaped.
func isPlainName(k string) bool {
	if k == "" {
		return false
	}
	for i := range len(k) {
		if c := k[i]; c <= ' ' || c >= 0x7f || c == ',' || c == '"' {
			return false
		}
	}
	return true
}
