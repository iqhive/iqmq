package codec

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Registry maps content types to codecs so that a receiver can decode a body
// it did not encode. It is safe for concurrent use; registration is expected
// at start-up and lookups on the receive path.
//
// The zero value is not usable; NewRegistry returns an empty registry and
// NewDefaultRegistry one pre-populated with the JSON codec of kind Auto and
// with Raw.
type Registry struct {
	mu       sync.RWMutex
	byType   map[string]Codec
	fallback Codec
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{byType: make(map[string]Codec, 4)} }

// NewDefaultRegistry returns a registry holding the JSON codec of kind Auto
// under application/json and Raw under application/octet-stream. Its fallback
// (the codec used for a body that carries no content type) is Raw, because a
// body without a declared type is just bytes.
//
// Each call builds a new registry, so a codec one caller registers into it is
// never seen by another.
func NewDefaultRegistry() *Registry {
	r := NewRegistry()
	_ = r.Register(autoJSON())
	raw := Raw()
	_ = r.Register(raw)
	r.SetFallback(raw)
	return r
}

// Register adds c under the content type it produces. It reports
// ErrDuplicateCodec when that content type is taken; use Replace to override.
func (r *Registry) Register(c Codec) error {
	if c == nil {
		return errNilCodec
	}
	return r.register(c.ContentType(), c, false)
}

// RegisterAs adds c under an additional content type, for example a
// vendor-specific alias of application/json.
func (r *Registry) RegisterAs(contentType string, c Codec) error {
	if c == nil {
		return errNilCodec
	}
	return r.register(contentType, c, false)
}

// Replace adds c under contentType, overriding any codec already registered
// there. It is how an application swaps the JSON implementation.
func (r *Registry) Replace(contentType string, c Codec) error {
	if c == nil {
		return errNilCodec
	}
	return r.register(contentType, c, true)
}

func (r *Registry) register(contentType string, c Codec, replace bool) error {
	m, cte := parseContentType(contentType)
	if cte != nil {
		return cte
	}
	if len(m.Params) != 0 {
		return &ContentTypeError{Got: contentType, Reason: "registration takes no parameters"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byType == nil {
		r.byType = make(map[string]Codec, 4)
	}
	if _, taken := r.byType[m.Media]; taken && !replace {
		return ErrDuplicateCodec
	}
	r.byType[m.Media] = c
	return nil
}

// SetFallback sets the codec used for a body that carries no content type at
// all. A nil argument clears it, and For then reports ErrNoCodec.
func (r *Registry) SetFallback(c Codec) {
	r.mu.Lock()
	r.fallback = c
	r.mu.Unlock()
}

// Fallback returns the codec set by SetFallback, or nil.
func (r *Registry) Fallback() Codec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fallback
}

// ContentTypes returns the registered content types in sorted order.
func (r *Registry) ContentTypes() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.byType))
	for ct := range r.byType {
		out = append(out, ct)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

// For returns the codec for a body whose content type header is contentType.
//
// An empty contentType selects the fallback. Parameters (charset and friends)
// are ignored and not validated, and a structured syntax suffix falls back to
// its base type, so
// application/vnd.acme.order+json resolves to the codec registered for
// application/json. An unknown or malformed type is a *ContentTypeError; an
// empty type with no fallback is ErrNoCodec.
//
// Hot path: a canonical, parameterless content type is resolved with one map
// lookup and no allocation.
func (r *Registry) For(contentType string) (Codec, error) {
	if contentType == "" {
		if c := r.Fallback(); c != nil {
			return c, nil
		}
		return nil, ErrNoCodec
	}
	media, typ, _, suffix, valid := mediaOf(contentType)
	if !valid {
		return nil, &ContentTypeError{Got: contentType, Reason: "expected type/subtype"}
	}
	r.mu.RLock()
	c, ok := r.byType[media]
	if !ok && suffix != "" {
		c, ok = r.byType[typ+"/"+suffix]
	}
	r.mu.RUnlock()
	if !ok {
		return nil, &ContentTypeError{Got: contentType}
	}
	return c, nil
}

// Negotiate returns the best codec for an Accept-style list of media ranges,
// for example "application/json;q=0.9, application/octet-stream".
//
// Ranges are ordered by their q value (default 1, a q of 0 rejects the range)
// and, at equal q, by specificity: an exact type beats type/*, which beats
// */*. A wildcard resolves to the fallback when one is set and otherwise to
// the first matching registered content type in sorted order, so the choice is
// deterministic. An empty list selects the fallback.
func (r *Registry) Negotiate(accept string) (Codec, error) {
	if trimOWS(accept) == "" {
		if c := r.Fallback(); c != nil {
			return c, nil
		}
		return nil, ErrNoCodec
	}
	ranges, cte := parseAccept(accept)
	if cte != nil {
		return nil, cte
	}
	sort.SliceStable(ranges, func(i, j int) bool {
		if ranges[i].q != ranges[j].q {
			return ranges[i].q > ranges[j].q
		}
		return ranges[i].rank > ranges[j].rank
	})
	for _, mr := range ranges {
		if c := r.match(mr); c != nil {
			return c, nil
		}
	}
	return nil, &ContentTypeError{Got: accept}
}

// mediaRange is one entry of an Accept header.
type mediaRange struct {
	m    MediaType
	q    int // parts per thousand, so ordering needs no float comparison
	rank int // 2 exact, 1 type/*, 0 */*
}

// match returns the codec satisfying one media range, or nil.
func (r *Registry) match(mr mediaRange) Codec {
	if mr.q == 0 {
		return nil
	}
	if mr.rank == 2 {
		c, err := r.For(mr.m.Media)
		if err != nil {
			return nil
		}
		return c
	}
	if c := r.Fallback(); c != nil && (mr.rank == 0 || strings.HasPrefix(c.ContentType(), mr.m.Type+"/")) {
		return c
	}
	for _, ct := range r.ContentTypes() {
		if mr.rank == 0 || strings.HasPrefix(ct, mr.m.Type+"/") {
			r.mu.RLock()
			c := r.byType[ct]
			r.mu.RUnlock()
			if c != nil {
				return c
			}
		}
	}
	return nil
}

// parseAccept splits an Accept header into media ranges with their q values.
func parseAccept(accept string) ([]mediaRange, *ContentTypeError) {
	parts := splitList(accept)
	out := make([]mediaRange, 0, len(parts))
	for _, part := range parts {
		mr, cte := parseRange(part, accept)
		if cte != nil {
			return nil, cte
		}
		out = append(out, mr)
	}
	if len(out) == 0 {
		return nil, &ContentTypeError{Got: accept, Reason: "empty accept list"}
	}
	return out, nil
}

// parseRange parses one media range; whole is the header it came from and is
// used only for error reporting.
func parseRange(part, whole string) (mediaRange, *ContentTypeError) {
	head, params := part, ""
	if i := strings.IndexByte(part, ';'); i >= 0 {
		head, params = part[:i], part[i:]
	}
	head = trimOWS(head)
	mr := mediaRange{q: 1000, rank: 2}
	switch {
	case head == "*/*":
		mr.m = MediaType{Media: "*/*", Type: "*", Subtype: "*"}
		mr.rank = 0
	case strings.HasSuffix(head, "/*"):
		t := head[:len(head)-2]
		if !isToken(t) {
			return mediaRange{}, &ContentTypeError{Got: whole, Reason: "invalid type in media range"}
		}
		t = lower(t)
		mr.m = MediaType{Media: t + "/*", Type: t, Subtype: "*"}
		mr.rank = 1
	default:
		m, cte := parseContentType(part)
		if cte != nil {
			return mediaRange{}, &ContentTypeError{Got: whole, Reason: cte.Reason}
		}
		mr.m = m
	}
	if mr.rank != 2 && params != "" {
		// A wildcard range is not a media type, so its parameters are parsed
		// against a placeholder type.
		m, cte := parseContentType("x/y" + params)
		if cte != nil {
			return mediaRange{}, &ContentTypeError{Got: whole, Reason: cte.Reason}
		}
		mr.m.Params = m.Params
	}
	if q, ok := mr.m.Param("q"); ok {
		v, err := strconv.ParseFloat(q, 64)
		if err != nil || v < 0 || v > 1 {
			return mediaRange{}, &ContentTypeError{Got: whole, Reason: "invalid q value"}
		}
		mr.q = int(v*1000 + 0.5)
	}
	return mr, nil
}

// splitList splits a comma-separated header value, ignoring commas inside
// quoted strings.
func splitList(s string) []string {
	var out []string
	quoted, escaped, start := false, false, 0
	for i := range len(s) {
		switch c := s[i]; {
		case escaped:
			escaped = false
		case c == '\\' && quoted:
			escaped = true
		case c == '"':
			quoted = !quoted
		case c == ',' && !quoted:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// errNilCodec reports a nil codec passed to a registration method.
var errNilCodec = errcode.New(errcode.InvalidArgument, "codec: nil codec")
