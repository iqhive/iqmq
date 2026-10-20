package codec

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// fuzzAlphabet is what generated strings are built from: ASCII, the runes
// encoding/json escapes, multi-byte runes and the line separators the two
// kinds treat differently. Every piece is valid UTF-8, so a round trip is
// exact for both kinds (encoding/json replaces invalid UTF-8 with U+FFFD,
// encoding/json/v2 rejects it; neither behaviour belongs in a round-trip
// oracle, and FuzzJSONCodec covers both from the decode side).
var fuzzAlphabet = []string{
	"a", "b", "Z", "0", "9", "_", " ", "\t", "\n", "\r",
	"<", ">", "&", `"`, `\`, "/", "{", "}", "[", "]",
	"é", "☃", "\u2028", "\u2029", "\x7f", "\u00a0", "𝄞",
}

// genString builds a valid-UTF-8 string of at most max pieces.
func genString(r *fuzzutil.Reader, max int) string {
	n := r.Int(0, max)
	var b strings.Builder
	for range n {
		b.WriteString(fuzzAlphabet[r.Choice(len(fuzzAlphabet))])
	}
	return b.String()
}

// genValue builds a modelValue from fuzz input. Empty containers are made nil
// so that the omitempty round trip is exact.
func genValue(r *fuzzutil.Reader, depth int) modelValue {
	v := modelValue{
		ID:  genString(r, 6),
		Seq: int64(r.Uint64()),
		// A binary fraction of a bounded integer: every value survives the
		// shortest-representation round trip exactly and is never NaN or Inf.
		Amount: float64(int64(r.Uint32())-1<<31) / 8,
		OK:     r.Bool(),
	}
	for n := r.Int(0, 3); n > 0; n-- {
		v.Tags = append(v.Tags, genString(r, 4))
	}
	if n := r.Int(0, 3); n > 0 {
		v.Attrs = make(map[string]string, n)
		for range n {
			v.Attrs[genString(r, 4)] = genString(r, 4)
		}
	}
	if b := r.Bytes(16); len(b) > 0 {
		v.Body = append([]byte(nil), b...)
	}
	if depth > 0 && r.Bool() {
		next := genValue(r, depth-1)
		v.Next = &next
	}
	return v
}

// codecFuzzState is the op-script state: the kind currently bound and a
// registry to exercise the negotiation paths.
type codecFuzzState struct {
	kind Kind
	c    Codec
	reg  *Registry
}

var codecScript = fuzzutil.Script[*codecFuzzState]{
	Steps: []fuzzutil.Step[*codecFuzzState]{
		{Name: "kind", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *codecFuzzState) {
			s.kind = Kinds()[r.Choice(len(Kinds()))]
			s.c = mustNew(t, s.kind)
		}},
		{Name: "value", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, s *codecFuzzState) {
			v := genValue(r, 2)
			b, err := s.c.Marshal(&v)
			if err != nil {
				t.Fatalf("%v: Marshal: %v", s.kind, err)
			}
			again, err := s.c.Marshal(&v)
			if err != nil || !bytes.Equal(again, b) {
				t.Fatalf("%v: Marshal is not deterministic: %s vs %s (%v)", s.kind, b, again, err)
			}
			var got modelValue
			if err := s.c.Unmarshal(b, &got); err != nil {
				t.Fatalf("%v: Unmarshal(%s): %v", s.kind, b, err)
			}
			if !reflect.DeepEqual(got, v) {
				t.Fatalf("%v: round trip changed the value\n got %+v\nwant %+v\nbody %s", s.kind, got, v, b)
			}
			// The append path produces exactly what Marshal produces.
			const prefix = "P"
			ap, err := s.c.(Appender).AppendMarshal([]byte(prefix), &v)
			if err != nil || string(ap) != prefix+string(b) {
				t.Fatalf("%v: AppendMarshal = %s, want %s (%v)", s.kind, ap, prefix+string(b), err)
			}
			// The generic shape agrees with the non-generic one.
			td := Adapt[modelValue](s.c)
			tb, err := td.Encode(v)
			if err != nil || !bytes.Equal(tb, b) {
				t.Fatalf("%v: typed Encode = %s, want %s (%v)", s.kind, tb, b, err)
			}
			tv, err := td.Decode(b)
			if err != nil || !reflect.DeepEqual(tv, v) {
				t.Fatalf("%v: typed Decode = %+v (%v)", s.kind, tv, err)
			}
			// Every kind decodes what any kind encodes.
			for _, other := range Kinds() {
				var cross modelValue
				if err := mustNew(t, other).Unmarshal(b, &cross); err != nil {
					t.Fatalf("%v cannot decode %v output %s: %v", other, s.kind, b, err)
				}
				if !reflect.DeepEqual(cross, v) {
					t.Fatalf("%v decoded %v output differently:\n got %+v\nwant %+v", other, s.kind, cross, v)
				}
			}
		}},
		{Name: "raw", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *codecFuzzState) {
			body := r.Bytes(64)
			raw := Raw()
			b, err := raw.Marshal(body)
			if err != nil || !bytes.Equal(b, body) {
				t.Fatalf("raw Marshal = %q (%v)", b, err)
			}
			target := make([]byte, 0, 8)
			if err := raw.Unmarshal(body, &target); err != nil || !bytes.Equal(target, body) {
				t.Fatalf("raw Unmarshal = %q (%v)", target, err)
			}
			var str string
			if err := raw.Unmarshal(body, &str); err != nil || str != string(body) {
				t.Fatalf("raw Unmarshal into string = %q (%v)", str, err)
			}
			ap, err := raw.(Appender).AppendMarshal([]byte("P"), body)
			if err != nil || string(ap) != "P"+string(body) {
				t.Fatalf("raw AppendMarshal = %q (%v)", ap, err)
			}
		}},
		{Name: "registry", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *codecFuzzState) {
			ct := genContentType(r)
			c, err := s.reg.For(ct)
			if err == nil {
				if merr := Match(c.ContentType(), ct); merr != nil {
					t.Fatalf("For(%q) returned %q, which does not Match: %v", ct, c.ContentType(), merr)
				}
			} else {
				var cte *ContentTypeError
				if !errors.As(err, &cte) && !errors.Is(err, ErrNoCodec) {
					t.Fatalf("For(%q) = %v", ct, err)
				}
			}
			if c, err := s.reg.Negotiate(ct); err == nil {
				found := false
				for _, have := range s.reg.ContentTypes() {
					if have == c.ContentType() {
						found = true
					}
				}
				if !found {
					t.Fatalf("Negotiate(%q) returned an unregistered codec %q", ct, c.ContentType())
				}
			}
		}},
	},
	MaxSteps: 48,
}

// genContentType builds a content type that is sometimes canonical, sometimes
// close to it and sometimes nonsense.
func genContentType(r *fuzzutil.Reader) string {
	switch r.Choice(6) {
	case 0:
		return ContentTypeJSON
	case 1:
		return ContentTypeOctetStream
	case 2:
		return "application/vnd." + genString(r, 3) + "+json"
	case 3:
		return ContentTypeJSON + "; charset=" + genString(r, 3)
	case 4:
		return genString(r, 4) + "/" + genString(r, 4)
	default:
		return genString(r, 8)
	}
}

// FuzzCodecRoundTrip drives values, raw bodies and content types through every
// kind: encoding is deterministic, the round trip is exact, the append and
// generic shapes agree with the non-generic one, every kind decodes what any
// other kind encodes, and registry lookups agree with Match.
func FuzzCodecRoundTrip(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		codecScript.WriteStep(f, w, "kind")
		w.Choice(len(Kinds()), ki)
		for _, s := range []string{"", "a", "<&>", "é☃"} {
			codecScript.WriteStep(f, w, "value")
			w.Int(0, 6, len(s))
			for range len(s) {
				w.Choice(len(fuzzAlphabet), 0)
			}
			w.Uint64(uint64(ki))
			w.Uint32(1 << 31)
			w.Bool(true)
			w.Int(0, 3, 1) // one tag
			w.Int(0, 4, 1) // of one piece
			w.Choice(len(fuzzAlphabet), 20)
			w.Int(0, 3, 1) // one attribute
			w.Int(0, 4, 1)
			w.Choice(len(fuzzAlphabet), 0)
			w.Int(0, 4, 1)
			w.Choice(len(fuzzAlphabet), 1)
			w.Bytes(16, []byte("body"))
			w.Bool(false) // no nesting
		}
		codecScript.WriteStep(f, w, "raw")
		w.Bytes(64, []byte("raw-body"))
		codecScript.WriteStep(f, w, "registry")
		w.Choice(6, 0)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		reg := NewRegistry()
		if err := reg.Register(mustNew(t, Auto)); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(Raw()); err != nil {
			t.Fatal(err)
		}
		codecScript.Run(t, data, &codecFuzzState{kind: JSONv1, c: mustNew(t, JSONv1), reg: reg})
	})
}

// FuzzJSONCodec feeds arbitrary bodies to every JSON kind: decoding never
// panics, a rejected body is always a *DecodeError carrying CorruptMessage, an
// accepted body re-marshals stably, and the kinds agree on every body they
// both accept (specification).
func FuzzJSONCodec(f *testing.F) {
	seeds := []string{
		"", " ", "null", "true", "tru", "0", "-0", "1e400", "1e-400", "9007199254740993",
		`{}`, `{"id":"x"}`, `{"id":"x","id":"y"}`, `{"ID":"x"}`, `{"id":1}`, `{"seq":"1"}`,
		`{"amount":1e309}`, `{"body":"AAH/"}`, `{"body":"not base64"}`, `{"tags":["a",null]}`,
		`{"attrs":{"a":"b","a":"c"}}`, `{"next":{"next":{"id":"deep"}}}`,
		"{\"id\":\"\xff\"}", `{"id":"\ud800"}`, `{"id":"\u0000"}`, "[1,2,3]", "[",
		`{"id":"x"} trailing`, "\ufeff{}", strings.Repeat("[", 64) + strings.Repeat("]", 64),
		strings.Repeat("[", 20000), `{"id":"` + strings.Repeat("a", 4096) + `"}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		type decoded struct {
			kind Kind
			v    any
		}
		var accepted []decoded
		for _, k := range Kinds() {
			c := mustNew(t, k)
			// A typed target must not panic and must classify its failures.
			var mv modelValue
			if err := c.Unmarshal(data, &mv); err != nil {
				assertDecodeError(t, k, err)
			}
			var v any
			err := c.Unmarshal(data, &v)
			if err != nil {
				assertDecodeError(t, k, err)
				continue
			}
			// Stable re-marshal: what decoded must encode, and encoding it
			// again must give the same bytes and decode to the same value.
			b, err := c.Marshal(v)
			if err != nil {
				t.Fatalf("%v: re-Marshal of a decoded body failed: %v (body %q)", k, err, data)
			}
			b2, err := c.Marshal(v)
			if err != nil || !bytes.Equal(b, b2) {
				t.Fatalf("%v: re-Marshal is not deterministic: %s vs %s (%v)", k, b, b2, err)
			}
			var again any
			if err := c.Unmarshal(b, &again); err != nil {
				t.Fatalf("%v: cannot decode its own output %s: %v", k, b, err)
			}
			if b3, err := c.Marshal(again); err != nil || !bytes.Equal(b3, b) {
				t.Fatalf("%v: second round trip differs: %s vs %s (%v)", k, b3, b, err)
			}
			accepted = append(accepted, decoded{kind: k, v: v})
		}
		// Kinds that both accept a body must have understood it the same way.
		// The comparison goes through one encoder so that map ordering and
		// number formatting cannot make equal values look different.
		ref := mustNew(t, JSONv1)
		for i := 1; i < len(accepted); i++ {
			a, err1 := ref.Marshal(accepted[0].v)
			b, err2 := ref.Marshal(accepted[i].v)
			if err1 != nil || err2 != nil {
				t.Fatalf("re-encoding decoded values failed: %v %v", err1, err2)
			}
			if !bytes.Equal(a, b) {
				t.Fatalf("%v and %v disagree on %q:\n%s\n%s", accepted[0].kind, accepted[i].kind, data, a, b)
			}
		}
	})
}

// assertDecodeError checks the classification of a rejected body.
func assertDecodeError(t *testing.T, k Kind, err error) {
	t.Helper()
	var de *DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("%v: %v is not a *DecodeError", k, err)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%v: %v carries %v, want CorruptMessage", k, err, code)
	}
	if de.Err == nil || de.ContentType != ContentTypeJSON {
		t.Fatalf("%v: incomplete *DecodeError %+v", k, de)
	}
}

// FuzzContentType drives the content-type parser, the matcher and the
// registry from one string: nothing panics, the canonical form round-trips,
// and lookup, negotiation and matching stay consistent with each other.
func FuzzContentType(f *testing.F) {
	seeds := []string{
		"", " ", "application/json", "Application/JSON ", "application/json;charset=utf-8",
		"application/json; charset=\"utf-8\"", `application/json; a="b\"c;d"`, "application/vnd.acme.order+json",
		"*/*", "application/*", "application/json;q=0.5, */*;q=0.1", "text/plain",
		"application/json;", ";", "/", "a/", "/b", "a/b/c", "a b/c", "application/json extra",
		"application/json; =v", "application/json; k", "application/json; k=\"", strings.Repeat("a", 300) + "/b",
		"\x00/\x01", "application/json;q=", "application/json;q=1.0000001", "APPLICATION/JSON;Q=0.5",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		reg := NewRegistry()
		if err := reg.Register(mustNew(t, Auto)); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(Raw()); err != nil {
			t.Fatal(err)
		}
		m, err := ParseContentType(s)
		if err != nil {
			var cte *ContentTypeError
			if !errors.As(err, &cte) || cte.Reason == "" {
				t.Fatalf("ParseContentType(%q) = %v", s, err)
			}
			if m.Media != "" {
				t.Fatalf("ParseContentType(%q) returned %+v with an error", s, m)
			}
		} else {
			checkMediaType(t, s, m)
			canonical := m.String()
			again, err := ParseContentType(canonical)
			if err != nil {
				t.Fatalf("canonical form %q of %q does not parse: %v", canonical, s, err)
			}
			if again.Media != m.Media || again.Suffix != m.Suffix || !reflect.DeepEqual(again.Params, m.Params) {
				t.Fatalf("canonical form of %q is not stable:\n%+v\n%+v", s, m, again)
			}
			if again.String() != canonical {
				t.Fatalf("String is not idempotent: %q vs %q", again.String(), canonical)
			}
			// A parsed content type matches itself and its own media type.
			if err := Match(m.Media, s); err != nil {
				t.Fatalf("Match(%q, %q) = %v", m.Media, s, err)
			}
		}
		// Lookup, matching and negotiation never panic and stay consistent.
		c, ferr := reg.For(s)
		if ferr == nil {
			if err := Match(c.ContentType(), s); err != nil {
				t.Fatalf("For(%q) = %q but Match rejects it: %v", s, c.ContentType(), err)
			}
			if _, perr := ParseContentType(c.ContentType()); perr != nil {
				t.Fatalf("registered content type %q does not parse: %v", c.ContentType(), perr)
			}
		} else if s != "" {
			var cte *ContentTypeError
			if !errors.As(ferr, &cte) {
				t.Fatalf("For(%q) = %v", s, ferr)
			}
		}
		if _, nerr := reg.Negotiate(s); nerr != nil {
			var cte *ContentTypeError
			if !errors.As(nerr, &cte) && !errors.Is(nerr, ErrNoCodec) {
				t.Fatalf("Negotiate(%q) = %v", s, nerr)
			}
		}
		_ = Match(ContentTypeJSON, s)
		_ = Match(s, ContentTypeJSON)
		_ = Match(s, s)
	})
}

// checkMediaType asserts the invariants of a successfully parsed content type.
func checkMediaType(t *testing.T, in string, m MediaType) {
	t.Helper()
	if m.Media != strings.ToLower(m.Media) {
		t.Fatalf("ParseContentType(%q): media %q is not lower case", in, m.Media)
	}
	typ, sub, ok := strings.Cut(m.Media, "/")
	if !ok || typ != m.Type || sub != m.Subtype || !isToken(typ) || !isToken(sub) {
		t.Fatalf("ParseContentType(%q) = %+v", in, m)
	}
	if m.Suffix != "" && !strings.HasSuffix(m.Subtype, "+"+m.Suffix) {
		t.Fatalf("ParseContentType(%q): suffix %q is not part of %q", in, m.Suffix, m.Subtype)
	}
	if m.Suffix == "" {
		if plus := strings.LastIndexByte(m.Subtype, '+'); plus >= 0 && plus < len(m.Subtype)-1 {
			t.Fatalf("ParseContentType(%q): suffix %q not reported", in, m.Subtype[plus+1:])
		}
	}
	media, typ2, sub2, suffix2, valid := mediaOf(in)
	if !valid || media != m.Media || typ2 != m.Type || sub2 != m.Subtype || suffix2 != m.Suffix {
		t.Fatalf("mediaOf(%q) disagrees with ParseContentType: %q %q %q %q %v", in, media, typ2, sub2, suffix2, valid)
	}
	for _, p := range m.Params {
		if p.Name != strings.ToLower(p.Name) || !isToken(p.Name) {
			t.Fatalf("ParseContentType(%q): parameter name %q", in, p.Name)
		}
		// Param reports the first occurrence of a repeated name.
		first := ""
		for _, q := range m.Params {
			if q.Name == p.Name {
				first = q.Value
				break
			}
		}
		if v, ok := m.Param(p.Name); !ok || v != first {
			t.Fatalf("ParseContentType(%q): Param(%q) = %q, %v; want %q", in, p.Name, v, ok, first)
		}
	}
}
