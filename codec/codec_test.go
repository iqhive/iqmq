package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(AllKinds()) != 2 {
		t.Fatalf("AllKinds() = %v", AllKinds())
	}
	seen := map[string]bool{}
	for _, k := range AllKinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		got, ok := ParseKind(k.String())
		if !ok || got != k {
			t.Fatalf("ParseKind(%q) = %v, %v", k, got, ok)
		}
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted an unknown slug")
	}
	if k, ok := ParseKind("auto"); !ok || k != Auto {
		t.Fatalf("ParseKind(auto) = %v, %v", k, ok)
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Available() {
		t.Fatalf("Auto resolves to %v (available=%v)", AutoKind, AutoKind.Available())
	}
	if Kind(200).Valid() || Kind(200).String() != "kind(?)" || Kind(200).Ident() != "" {
		t.Fatal("out-of-range kind")
	}
	// Kinds() is exactly the available subset of AllKinds().
	avail := 0
	for _, k := range AllKinds() {
		if k.Available() {
			avail++
		}
	}
	if len(Kinds()) != avail {
		t.Fatalf("Kinds() = %v, %d kinds are available", Kinds(), avail)
	}
	if JSONv1.Available() != true {
		t.Fatal("JSONv1 must be available in every build")
	}
	if JSONv2.Available() != jsonv2Available {
		t.Fatal("JSONv2 availability disagrees with the build")
	}
	t.Logf("jsonv2 available: %v", jsonv2Available)
}

func TestNewRejectsAnInvalidKind(t *testing.T) {
	if _, err := New(Kind(200)); err == nil {
		t.Fatal("New(invalid) did not fail")
	}
}

func TestAutoJSONIsAutoKind(t *testing.T) {
	if got, want := reflect.TypeOf(autoJSON()), reflect.TypeOf(mustNew(t, Auto)); got != want {
		t.Fatalf("autoJSON is %v, New(Auto) is %v", got, want)
	}
}

func mustNew(tb testing.TB, k Kind) Codec {
	tb.Helper()
	c, err := New(k)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return c
}

// TestAutoBindingMatchesADR keeps auto.go and the ADR that decides in
// agreement once the bake-off has run.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("..", "codec")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds codec yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds codec to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestNoRootImport enforces the Phase-0 layering: codec sits below the root
// package and may never depend on it (specification layering, plan).
func TestNoRootImport(t *testing.T) {
	testguard.NoRootImport(t, "github.com/iqhive/iqmq/codec")
	testguard.StdlibOnly(t, "github.com/iqhive/iqmq/codec")
}

// hookValue implements every hook Raw honours.
type hookValue struct {
	n uint32
}

func (h hookValue) MarshalMessage() ([]byte, error) { return h.AppendMessage(nil) }

func (h hookValue) AppendMessage(dst []byte) ([]byte, error) {
	return binary.LittleEndian.AppendUint32(dst, h.n), nil
}

func (h *hookValue) UnmarshalMessage(data []byte) error {
	if len(data) != 4 {
		return errcode.New(errcode.CorruptMessage, "hookValue: want 4 bytes")
	}
	h.n = binary.LittleEndian.Uint32(data)
	return nil
}

func TestRawPassThrough(t *testing.T) {
	c := Raw()
	if c.ContentType() != ContentTypeOctetStream {
		t.Fatalf("ContentType = %q", c.ContentType())
	}
	body := []byte("hello")
	b, err := c.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if &b[0] != &body[0] {
		t.Fatal("Marshal([]byte) copied; the documented contract is pass-through")
	}
	s := "hello"
	if b, err = c.Marshal(&s); err != nil || string(b) != s {
		t.Fatalf("Marshal(*string) = %q, %v", b, err)
	}
	if b, err = c.Marshal(s); err != nil || string(b) != s {
		t.Fatalf("Marshal(string) = %q, %v", b, err)
	}
	if b, err = c.Marshal(&body); err != nil || string(b) != "hello" {
		t.Fatalf("Marshal(*[]byte) = %q, %v", b, err)
	}
	if b, err = c.Marshal(nil); err != nil || b != nil {
		t.Fatalf("Marshal(nil) = %q, %v", b, err)
	}
	if _, err = c.Marshal(42); !errors.Is(err, ErrNotRawValue) {
		t.Fatalf("Marshal(int) = %v", err)
	}

	// Hooks.
	hv := hookValue{n: 0x01020304}
	if b, err = c.Marshal(hv); err != nil || len(b) != 4 || binary.LittleEndian.Uint32(b) != hv.n {
		t.Fatalf("Marshal(hook) = %x, %v", b, err)
	}
	got := hookValue{}
	if err = c.Unmarshal(b, &got); err != nil || got != hv {
		t.Fatalf("Unmarshal(hook) = %v, %v", got, err)
	}
	if err = c.Unmarshal([]byte{1}, &got); errcode.CodeOf(err) != errcode.CorruptMessage {
		t.Fatalf("hook error not preserved: %v", err)
	}

	// Append.
	a := c.(Appender)
	dst := []byte("pre-")
	if b, err = a.AppendMarshal(dst, body); err != nil || string(b) != "pre-hello" {
		t.Fatalf("AppendMarshal([]byte) = %q, %v", b, err)
	}
	if b, err = a.AppendMarshal(dst, hv); err != nil || string(b[:4]) != "pre-" || len(b) != 8 {
		t.Fatalf("AppendMarshal(hook) = %q, %v", b, err)
	}
	if b, err = a.AppendMarshal(dst, s); err != nil || string(b) != "pre-hello" {
		t.Fatalf("AppendMarshal(string) = %q, %v", b, err)
	}
	b, err = a.AppendMarshal(dst, 42)
	if !errors.Is(err, ErrNotRawValue) {
		t.Fatalf("AppendMarshal(int) = %v", err)
	}
	// A value Raw cannot pass through leaves the caller's buffer alone.
	if !bytes.Equal(b, dst) {
		t.Fatalf("AppendMarshal(int) = %q, want the caller's buffer %q", b, dst)
	}

	// Unmarshal copies into the target's capacity and never aliases data.
	data := []byte("payload")
	target := make([]byte, 0, 16)
	if err = c.Unmarshal(data, &target); err != nil || string(target) != "payload" {
		t.Fatalf("Unmarshal(*[]byte) = %q, %v", target, err)
	}
	data[0] = 'X'
	if string(target) != "payload" {
		t.Fatal("Unmarshal aliased the source buffer")
	}
	var str string
	if err = c.Unmarshal(data, &str); err != nil || str != "Xayload" {
		t.Fatalf("Unmarshal(*string) = %q, %v", str, err)
	}
	for _, target := range []any{nil, (*[]byte)(nil), (*string)(nil)} {
		if err = c.Unmarshal(data, target); !errors.Is(err, ErrNilTarget) {
			t.Fatalf("Unmarshal into %T = %v", target, err)
		}
	}
	if err = c.Unmarshal(data, 42); !errors.Is(err, ErrNotRawValue) {
		t.Fatalf("Unmarshal into int = %v", err)
	}
}

func TestRawTyped(t *testing.T) {
	tb := Adapt[[]byte](Raw())
	b, err := tb.Encode([]byte("body"))
	if err != nil || string(b) != "body" {
		t.Fatalf("Encode = %q, %v", b, err)
	}
	got, err := tb.Decode(b)
	if err != nil || string(got) != "body" {
		t.Fatalf("Decode = %q, %v", got, err)
	}
	ts := Adapt[string](Raw())
	if s, err := ts.Decode([]byte("text")); err != nil || s != "text" {
		t.Fatalf("Decode = %q, %v", s, err)
	}
	if ts.ContentType() != ContentTypeOctetStream {
		t.Fatalf("ContentType = %q", ts.ContentType())
	}
	if _, ok := tb.(AppendingTyped[[]byte]); !ok {
		t.Fatal("Raw is an Appender; Adapt must carry the append path")
	}
}

// nonAppender is a Codec that deliberately does not implement Appender.
type nonAppender struct{}

func (nonAppender) ContentType() string             { return "application/x-test" }
func (nonAppender) Marshal(any) ([]byte, error)     { return []byte("x"), nil }
func (nonAppender) Unmarshal(_ []byte, _ any) error { return nil }

func TestAdaptWithoutAppender(t *testing.T) {
	if _, ok := Adapt[string](nonAppender{}).(AppendingTyped[string]); ok {
		t.Fatal("Adapt invented an append path")
	}
}

func TestParseContentType(t *testing.T) {
	cases := []struct {
		in      string
		media   string
		suffix  string
		params  []Param
		invalid bool
	}{
		{in: "application/json", media: "application/json"},
		{in: "Application/JSON", media: "application/json"},
		{in: "  application/json  ", media: "application/json"},
		{in: "application/json; charset=utf-8", media: "application/json", params: []Param{{"charset", "utf-8"}}},
		{in: "application/json;charset=UTF-8;q=0.5", media: "application/json", params: []Param{{"charset", "UTF-8"}, {"q", "0.5"}}},
		{in: `application/json; note="a;b\"c"`, media: "application/json", params: []Param{{"note", `a;b"c`}}},
		{in: "application/vnd.acme.order+json", media: "application/vnd.acme.order+json", suffix: "json"},
		{in: "text/plain", media: "text/plain"},
		{in: "", invalid: true},
		{in: "application", invalid: true},
		{in: "application/", invalid: true},
		{in: "/json", invalid: true},
		{in: "application/json extra", invalid: true},
		{in: "application/json;", media: "application/json"},
		{in: "application/json; ", media: "application/json"},
		{in: "application/json; charset", invalid: true},
		{in: "application/json; =utf-8", invalid: true},
		{in: `application/json; note="unterminated`, invalid: true},
		{in: "app\x00lication/json", invalid: true},
	}
	for _, tc := range cases {
		m, err := ParseContentType(tc.in)
		if tc.invalid {
			var cte *ContentTypeError
			if !errors.As(err, &cte) {
				t.Fatalf("ParseContentType(%q) = %+v, %v; want *ContentTypeError", tc.in, m, err)
			}
			if cte.Reason == "" {
				t.Fatalf("ParseContentType(%q): error without a reason", tc.in)
			}
			if errcode.CodeOf(err) != errcode.Unsupported {
				t.Fatalf("ParseContentType(%q) code = %v", tc.in, errcode.CodeOf(err))
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseContentType(%q): %v", tc.in, err)
		}
		if m.Media != tc.media || m.Suffix != tc.suffix {
			t.Fatalf("ParseContentType(%q) = %q suffix %q", tc.in, m.Media, m.Suffix)
		}
		if len(m.Params) != len(tc.params) {
			t.Fatalf("ParseContentType(%q) params = %v", tc.in, m.Params)
		}
		for i, p := range tc.params {
			if m.Params[i] != p {
				t.Fatalf("ParseContentType(%q) param %d = %v, want %v", tc.in, i, m.Params[i], p)
			}
			if v, ok := m.Param(p.Name); !ok || v != p.Value {
				t.Fatalf("Param(%q) = %q, %v", p.Name, v, ok)
			}
		}
		if _, ok := m.Param("absent"); ok {
			t.Fatal("Param invented a parameter")
		}
		// String is canonical: parsing it again yields the same value.
		again, err := ParseContentType(m.String())
		if err != nil {
			t.Fatalf("ParseContentType(%q): %v", m.String(), err)
		}
		if again.String() != m.String() {
			t.Fatalf("String is not canonical: %q vs %q", again.String(), m.String())
		}
	}
}

func TestMatch(t *testing.T) {
	ok := []struct{ want, got string }{
		{ContentTypeJSON, ContentTypeJSON},
		{ContentTypeJSON, ""},
		{ContentTypeJSON, "application/json; charset=utf-8"},
		{ContentTypeJSON, "Application/JSON"},
		{ContentTypeJSON, "application/vnd.acme.order+json"},
		{"", ContentTypeJSON},
	}
	for _, tc := range ok {
		if err := Match(tc.want, tc.got); err != nil {
			t.Fatalf("Match(%q, %q) = %v", tc.want, tc.got, err)
		}
	}
	bad := []struct{ want, got string }{
		{ContentTypeJSON, ContentTypeOctetStream},
		{ContentTypeJSON, "text/json"},
		{ContentTypeJSON, "not a content type"},
		{"application/vnd.acme.order+json", ContentTypeJSON},
	}
	for _, tc := range bad {
		err := Match(tc.want, tc.got)
		var cte *ContentTypeError
		if !errors.As(err, &cte) {
			t.Fatalf("Match(%q, %q) = %v", tc.want, tc.got, err)
		}
		if cte.Want != tc.want || cte.Got != tc.got {
			t.Fatalf("Match(%q, %q) = %+v", tc.want, tc.got, cte)
		}
		if !errors.Is(err, errcode.ErrUnsupported) {
			t.Fatalf("Match(%q, %q) does not match ErrUnsupported", tc.want, tc.got)
		}
		if !strings.Contains(cte.Error(), tc.got) {
			t.Fatalf("error text %q does not name the content type", cte.Error())
		}
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if _, err := r.For(""); !errors.Is(err, ErrNoCodec) {
		t.Fatalf("empty registry For(\"\") = %v", err)
	}
	if err := r.Register(mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(mustNew(t, Auto)); !errors.Is(err, ErrDuplicateCodec) {
		t.Fatalf("duplicate registration = %v", err)
	}
	if err := r.Register(nil); errcode.CodeOf(err) != errcode.InvalidArgument {
		t.Fatalf("Register(nil) = %v", err)
	}
	if err := r.RegisterAs("application/vnd.acme.order+json", mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterAs("application/json; charset=utf-8", mustNew(t, Auto)); err == nil {
		t.Fatal("RegisterAs accepted parameters")
	}
	if err := r.RegisterAs("nonsense", mustNew(t, Auto)); err == nil {
		t.Fatal("RegisterAs accepted a malformed content type")
	}
	raw := Raw()
	if err := r.Register(raw); err != nil {
		t.Fatal(err)
	}
	if got := r.ContentTypes(); len(got) != 3 || got[0] != ContentTypeJSON {
		t.Fatalf("ContentTypes() = %v", got)
	}

	// Lookup.
	for _, ct := range []string{ContentTypeJSON, "application/json; charset=utf-8", "APPLICATION/JSON", "application/vnd.acme.other+json"} {
		c, err := r.For(ct)
		if err != nil {
			t.Fatalf("For(%q): %v", ct, err)
		}
		if c.ContentType() != ContentTypeJSON {
			t.Fatalf("For(%q) = %q", ct, c.ContentType())
		}
	}
	if _, err := r.For("text/plain"); err == nil {
		t.Fatal("For(text/plain) succeeded")
	}
	if _, err := r.For("bogus"); err == nil {
		t.Fatal("For(bogus) succeeded")
	}
	if _, err := r.For(""); !errors.Is(err, ErrNoCodec) {
		t.Fatalf("For(\"\") without a fallback = %v", err)
	}
	r.SetFallback(raw)
	if c, err := r.For(""); err != nil || c.ContentType() != ContentTypeOctetStream {
		t.Fatalf("For(\"\") with a fallback = %v, %v", c, err)
	}
	if r.Fallback() == nil {
		t.Fatal("Fallback() = nil")
	}

	// Replace swaps the implementation behind a content type.
	if err := r.Replace(ContentTypeJSON, raw); err != nil {
		t.Fatal(err)
	}
	if c, _ := r.For(ContentTypeJSON); c.ContentType() != ContentTypeOctetStream {
		t.Fatal("Replace did not take effect")
	}
	if err := r.Replace(ContentTypeJSON, mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	if err := r.Replace(ContentTypeJSON, nil); errcode.CodeOf(err) != errcode.InvalidArgument {
		t.Fatalf("Replace(nil) = %v", err)
	}
}

func TestRegistryNegotiate(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	raw := Raw()
	if err := r.Register(raw); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		accept string
		want   string
		err    bool
	}{
		{accept: "", want: "", err: true},
		{accept: "application/json", want: ContentTypeJSON},
		{accept: "application/octet-stream", want: ContentTypeOctetStream},
		{accept: "text/plain, application/json", want: ContentTypeJSON},
		{accept: "application/json;q=0.2, application/octet-stream;q=0.8", want: ContentTypeOctetStream},
		{accept: "application/json;q=0, application/octet-stream", want: ContentTypeOctetStream},
		{accept: "*/*", want: ContentTypeJSON},
		{accept: "application/*", want: ContentTypeJSON},
		{accept: "text/*", err: true},
		{accept: "text/plain", err: true},
		{accept: "application/json;q=2", err: true},
		{accept: "application/json;q=x", err: true},
		{accept: "nonsense", err: true},
		{accept: "application/vnd.acme.order+json", want: ContentTypeJSON},
		{accept: `application/json;note="a,b"`, want: ContentTypeJSON},
		{accept: "*/*;q=0.1, application/octet-stream;q=0.05", want: ContentTypeJSON},
	}
	for _, tc := range cases {
		c, err := r.Negotiate(tc.accept)
		if tc.err {
			if err == nil {
				t.Fatalf("Negotiate(%q) = %q, want an error", tc.accept, c.ContentType())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Negotiate(%q): %v", tc.accept, err)
		}
		if c.ContentType() != tc.want {
			t.Fatalf("Negotiate(%q) = %q, want %q", tc.accept, c.ContentType(), tc.want)
		}
	}
	// With a fallback a wildcard resolves to it.
	r.SetFallback(raw)
	if c, err := r.Negotiate("*/*"); err != nil || c.ContentType() != ContentTypeOctetStream {
		t.Fatalf("Negotiate(*/*) with a fallback = %v, %v", c, err)
	}
	if c, err := r.Negotiate(""); err != nil || c.ContentType() != ContentTypeOctetStream {
		t.Fatalf("Negotiate(\"\") = %v, %v", c, err)
	}
}

func TestDefaultRegistry(t *testing.T) {
	r := NewDefaultRegistry()
	if r == NewDefaultRegistry() {
		t.Fatal("NewDefaultRegistry returned a shared registry")
	}
	c, err := r.For(ContentTypeJSON)
	if err != nil || c.ContentType() != ContentTypeJSON {
		t.Fatalf("For(json) = %v, %v", c, err)
	}
	if c, err = r.For(""); err != nil || c.ContentType() != ContentTypeOctetStream {
		t.Fatalf("fallback = %v, %v", c, err)
	}
}

func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	for i := range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := range 200 {
				if j%7 == 0 {
					_ = r.RegisterAs("application/x-"+string(rune('a'+i)), Raw())
				}
				if _, err := r.For(ContentTypeJSON); err != nil {
					t.Error(err)
					return
				}
				_ = r.ContentTypes()
				if _, err := r.Negotiate("application/*"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 8 {
		<-done
	}
}

var allocSink []byte

// TestAllocs pins the allocation-free paths: kind construction, the canonical
// registry lookup, content-type matching and the raw pass-through.
func TestAllocs(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(mustNew(t, Auto)); err != nil {
		t.Fatal(err)
	}
	body := []byte("payload")
	target := make([]byte, 0, 32)
	cases := []struct {
		name string
		fn   func()
	}{
		{"New", func() { _, _ = New(JSONv1) }},
		{"ParseContentType", func() { _, _ = ParseContentType(ContentTypeJSON) }},
		{"Match/equal", func() { _ = Match(ContentTypeJSON, ContentTypeJSON) }},
		{"Match/parameters", func() { _ = Match(ContentTypeJSON, "application/json; charset=utf-8") }},
		{"Registry/For", func() { _, _ = r.For(ContentTypeJSON) }},
		{"Registry/For/parameters", func() { _, _ = r.For("application/json;charset=utf-8") }},
		{"Raw/Marshal", func() { allocSink, _ = Raw().Marshal(&body) }},
		{"Raw/Unmarshal", func() { _ = Raw().Unmarshal(body, &target) }},
		{"Raw/AppendMarshal", func() { allocSink, _ = Raw().(Appender).AppendMarshal(target[:0], &body) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if a := testing.AllocsPerRun(200, tc.fn); a != 0 {
				t.Fatalf("%s: %.1f allocs/op", tc.name, a)
			}
		})
	}
}
