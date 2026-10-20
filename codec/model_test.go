package codec

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
)

// modelValue is the message shape the model, the fuzz targets and the
// benchmarks encode. Its tags exercise the options both JSON kinds agree on:
// omitempty on a slice, a map and a pointer means "omit when empty" in v1 and
// in v2 alike.
type modelValue struct {
	ID     string            `json:"id"`
	Seq    int64             `json:"seq"`
	Amount float64           `json:"amount"`
	OK     bool              `json:"ok"`
	Tags   []string          `json:"tags,omitempty"`
	Attrs  map[string]string `json:"attrs,omitempty"`
	Body   []byte            `json:"body,omitempty"`
	Next   *modelValue       `json:"next,omitempty"`
}

// modelCase is one value with the bytes each kind must produce for it. The
// closures carry the static type of the value, so the round trip decodes into
// the same type the case was written with.
type modelCase struct {
	// name identifies the case in subtest names.
	name string
	// v1 is the encoding/json output; v2 is the encoding/json/v2 output, empty
	// when the two agree (the usual case: they differ only where v1 escapes
	// HTML-sensitive runes and the line separators U+2028 and U+2029).
	v1, v2 string
	// marshal encodes the case's value with c.
	marshal func(c Codec) ([]byte, error)
	// appendMarshal appends the case's value to dst.
	appendMarshal func(c Codec, dst []byte) ([]byte, error)
	// decodeEqual decodes data with c and reports whether the result equals
	// the case's value.
	decodeEqual func(c Codec, data []byte) (bool, error)
	// typedRoundTrip encodes and decodes through Adapt and reports equality.
	typedRoundTrip func(c Codec) (bool, error)
	// typedAppend appends through AppendingTyped when c supports it.
	typedAppend func(c Codec, dst []byte) ([]byte, bool, error)
}

// modelOf builds a case for a value of a concrete type T.
func modelOf[T any](name string, v T, v1, v2 string) modelCase {
	return modelCase{
		name: name, v1: v1, v2: v2,
		marshal: func(c Codec) ([]byte, error) { return c.Marshal(&v) },
		appendMarshal: func(c Codec, dst []byte) ([]byte, error) {
			return c.(Appender).AppendMarshal(dst, &v)
		},
		decodeEqual: func(c Codec, data []byte) (bool, error) {
			var got T
			if err := c.Unmarshal(data, &got); err != nil {
				return false, err
			}
			return reflect.DeepEqual(got, v), nil
		},
		typedRoundTrip: func(c Codec) (bool, error) {
			t := Adapt[T](c)
			b, err := t.Encode(v)
			if err != nil {
				return false, err
			}
			got, err := t.Decode(b)
			if err != nil {
				return false, err
			}
			return reflect.DeepEqual(got, v), nil
		},
		typedAppend: func(c Codec, dst []byte) ([]byte, bool, error) {
			a, ok := Adapt[T](c).(AppendingTyped[T])
			if !ok {
				return dst, false, nil
			}
			b, err := a.AppendEncode(dst, v)
			return b, true, err
		},
	}
}

// modelCases is the corpus every kind is checked against. The expected bytes
// are the oracle: they were derived from the specification of each package
// (sorted object members, base64 bodies, omitted empty containers) and pin the
// exact wire form a driver will see.
func modelCases() []modelCase {
	return []modelCase{
		modelOf("scalars", modelValue{ID: "order-1", Seq: 42, Amount: 1.5, OK: true},
			`{"id":"order-1","seq":42,"amount":1.5,"ok":true}`, ""),
		modelOf("zero", modelValue{}, `{"id":"","seq":0,"amount":0,"ok":false}`, ""),
		modelOf("html", modelValue{ID: `<b>&"a"`},
			`{"id":"\u003cb\u003e\u0026\"a\"","seq":0,"amount":0,"ok":false}`,
			`{"id":"<b>&\"a\"","seq":0,"amount":0,"ok":false}`),
		modelOf("unicode", modelValue{ID: "héllo ☃"}, `{"id":"héllo ☃","seq":0,"amount":0,"ok":false}`, ""),
		modelOf("map-sorted", modelValue{ID: "m", Attrs: map[string]string{"z": "3", "a": "1", "m": "2"}},
			`{"id":"m","seq":0,"amount":0,"ok":false,"attrs":{"a":"1","m":"2","z":"3"}}`, ""),
		modelOf("bytes", modelValue{ID: "b", Body: []byte{0x00, 0x01, 0xff}},
			`{"id":"b","seq":0,"amount":0,"ok":false,"body":"AAH/"}`, ""),
		modelOf("tags", modelValue{ID: "t", Tags: []string{"x", "y"}},
			`{"id":"t","seq":0,"amount":0,"ok":false,"tags":["x","y"]}`, ""),
		modelOf("nested", modelValue{ID: "a", Next: &modelValue{ID: "b", Seq: 2}},
			`{"id":"a","seq":0,"amount":0,"ok":false,"next":{"id":"b","seq":2,"amount":0,"ok":false}}`, ""),
		modelOf("negative", modelValue{Seq: -9007199254740993, Amount: -0.125},
			`{"id":"","seq":-9007199254740993,"amount":-0.125,"ok":false}`, ""),
		modelOf("slice", []modelValue{{ID: "a"}, {ID: "b"}},
			`[{"id":"a","seq":0,"amount":0,"ok":false},{"id":"b","seq":0,"amount":0,"ok":false}]`, ""),
		modelOf("map-any", map[string]any{"b": float64(1), "a": "x"}, `{"a":"x","b":1}`, ""),
		modelOf("string", "plain", `"plain"`, ""),
		modelOf("nil-pointer", (*modelValue)(nil), `null`, ""),
	}
}

// wantBytes returns the encoding kind k must produce for the case.
func (mc modelCase) wantBytes(k Kind) string {
	if k.Resolve() == JSONv2 && mc.v2 != "" {
		return mc.v2
	}
	return mc.v1
}

// TestModel checks every kind against the model corpus: the
// exact bytes it must produce, the round trip through Codec and through the
// generic Typed shape, the append path, cross-kind interoperability and the
// classification of decode failures.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			c := mustNew(t, k)
			if c.ContentType() != ContentTypeJSON {
				t.Fatalf("ContentType = %q", c.ContentType())
			}
			t.Run("bytes", func(t *testing.T) { modelBytes(t, k, c) })
			t.Run("roundtrip", func(t *testing.T) { modelRoundTrip(t, k, c) })
			t.Run("append", func(t *testing.T) { modelAppend(t, k, c) })
			t.Run("typed", func(t *testing.T) { modelTyped(t, c) })
			t.Run("interop", func(t *testing.T) { modelInterop(t, k, c) })
			t.Run("errors", func(t *testing.T) { modelErrors(t, c) })
			t.Run("append-errors", func(t *testing.T) { modelAppendErrors(t, c) })
			t.Run("stability", func(t *testing.T) { modelStability(t, k, c) })
		})
	}
}

// modelBytes pins the exact encoding of every case.
func modelBytes(t *testing.T, k Kind, c Codec) {
	for _, mc := range modelCases() {
		b, err := mc.marshal(c)
		if err != nil {
			t.Fatalf("%s: Marshal: %v", mc.name, err)
		}
		if want := mc.wantBytes(k); string(b) != want {
			t.Fatalf("%s: Marshal = %s, want %s", mc.name, b, want)
		}
	}
}

// modelRoundTrip decodes the encoding of every case back into its own type.
func modelRoundTrip(t *testing.T, k Kind, c Codec) {
	for _, mc := range modelCases() {
		b, err := mc.marshal(c)
		if err != nil {
			t.Fatalf("%s: Marshal: %v", mc.name, err)
		}
		eq, err := mc.decodeEqual(c, b)
		if err != nil {
			t.Fatalf("%s: Unmarshal: %v", mc.name, err)
		}
		if !eq {
			t.Fatalf("%s: round trip through %v changed the value", mc.name, k)
		}
	}
}

// modelAppend checks that appending preserves the prefix and produces exactly
// what Marshal produces.
func modelAppend(t *testing.T, k Kind, c Codec) {
	a, ok := c.(Appender)
	if !ok {
		t.Fatalf("%v does not implement Appender", k)
	}
	_ = a
	const prefix = "prefix-"
	for _, mc := range modelCases() {
		dst := make([]byte, 0, 8)
		dst = append(dst, prefix...)
		got, err := mc.appendMarshal(c, dst)
		if err != nil {
			t.Fatalf("%s: AppendMarshal: %v", mc.name, err)
		}
		if !strings.HasPrefix(string(got), prefix) {
			t.Fatalf("%s: AppendMarshal dropped the prefix: %s", mc.name, got)
		}
		if want := prefix + mc.wantBytes(k); string(got) != want {
			t.Fatalf("%s: AppendMarshal = %s, want %s", mc.name, got, want)
		}
	}
}

// modelTyped checks the generic shape against the non-generic one.
func modelTyped(t *testing.T, c Codec) {
	for _, mc := range modelCases() {
		eq, err := mc.typedRoundTrip(c)
		if err != nil {
			t.Fatalf("%s: typed round trip: %v", mc.name, err)
		}
		if !eq {
			t.Fatalf("%s: typed round trip changed the value", mc.name)
		}
		want, err := mc.marshal(c)
		if err != nil {
			t.Fatalf("%s: Marshal: %v", mc.name, err)
		}
		got, ok, err := mc.typedAppend(c, nil)
		if err != nil {
			t.Fatalf("%s: AppendEncode: %v", mc.name, err)
		}
		if !ok {
			t.Fatalf("%s: Adapt did not carry the append path", mc.name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: AppendEncode = %s, want %s", mc.name, got, want)
		}
	}
}

// modelInterop checks that every other kind decodes what this kind encodes.
func modelInterop(t *testing.T, k Kind, c Codec) {
	for _, other := range Kinds() {
		if other == k {
			continue
		}
		oc := mustNew(t, other)
		for _, mc := range modelCases() {
			b, err := mc.marshal(c)
			if err != nil {
				t.Fatalf("%s: Marshal: %v", mc.name, err)
			}
			eq, err := mc.decodeEqual(oc, b)
			if err != nil {
				t.Fatalf("%s: %v cannot decode %v output %s: %v", mc.name, other, k, b, err)
			}
			if !eq {
				t.Fatalf("%s: %v decoded %v output into a different value", mc.name, other, k)
			}
		}
	}
}

// modelStability checks that re-encoding a decoded value reproduces the same
// bytes: a codec's output is a function of the value alone, which is what
// dedup keys and content hashes depend on.
func modelStability(t *testing.T, k Kind, c Codec) {
	for _, mc := range modelCases() {
		first, err := mc.marshal(c)
		if err != nil {
			t.Fatalf("%s: Marshal: %v", mc.name, err)
		}
		var v any
		if err := c.Unmarshal(first, &v); err != nil {
			t.Fatalf("%s: Unmarshal into any: %v", mc.name, err)
		}
		second, err := c.Marshal(v)
		if err != nil {
			t.Fatalf("%s: re-Marshal: %v", mc.name, err)
		}
		third, err := c.Marshal(v)
		if err != nil {
			t.Fatalf("%s: re-Marshal: %v", mc.name, err)
		}
		if !bytes.Equal(second, third) {
			t.Fatalf("%s: %v is not deterministic: %s vs %s", mc.name, k, second, third)
		}
	}
}

// modelAppendErrors checks the failure contract of the append path: a value
// the codec cannot encode is reported as an error and the caller's buffer
// comes back untouched, with no partial value appended to it. A publisher
// encodes into a pooled buffer it keeps using, so a codec that returned a
// half-written slice would corrupt the next message rather than the failed
// one.
func modelAppendErrors(t *testing.T, c Codec) {
	a := c.(Appender)
	const prefix = "keep-me"
	dst := make([]byte, 0, 64)
	dst = append(dst, prefix...)

	got, err := a.AppendMarshal(dst, make(chan int))
	if err == nil {
		t.Fatal("AppendMarshal(chan) succeeded")
	}
	var de *DecodeError
	if errors.As(err, &de) {
		t.Fatalf("AppendMarshal(chan) = %v; an encode failure is not a *DecodeError", err)
	}
	if string(got) != prefix {
		t.Fatalf("AppendMarshal(chan) = %q, want the caller's buffer %q", got, prefix)
	}

	// The generic shape must not corrupt the buffer either.
	ta, ok := Adapt[chan int](c).(AppendingTyped[chan int])
	if !ok {
		t.Fatal("Adapt did not carry the append path")
	}
	if got, err = ta.AppendEncode(dst, make(chan int)); err == nil || string(got) != prefix {
		t.Fatalf("AppendEncode(chan) = %q, %v", got, err)
	}

	// The next value appends at the caller's own length, so the failure left
	// nothing behind.
	v := modelValue{ID: "after"}
	want, err := c.Marshal(&v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got, err = a.AppendMarshal(dst, &v); err != nil || string(got) != prefix+string(want) {
		t.Fatalf("AppendMarshal after a failure = %q, want %q (%v)", got, prefix+string(want), err)
	}
}

// modelErrors checks how each kind classifies bad input and bad targets.
func modelErrors(t *testing.T, c Codec) {
	for _, body := range []string{"", "{", `{"id":}`, `{"seq":"not a number"}`, "[1,2", "tru", `{"id":"x"}}`} {
		var v modelValue
		err := c.Unmarshal([]byte(body), &v)
		if err == nil {
			t.Fatalf("Unmarshal(%q) succeeded", body)
		}
		var de *DecodeError
		if !errors.As(err, &de) {
			t.Fatalf("Unmarshal(%q) = %v, want *DecodeError", body, err)
		}
		if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
			t.Fatalf("Unmarshal(%q) code = %v, want CorruptMessage", body, code)
		}
		if de.Err == nil {
			t.Fatalf("Unmarshal(%q): *DecodeError without the decoder's error", body)
		}
	}
	var v modelValue
	for _, target := range []any{nil, v, (*modelValue)(nil), 42} {
		if err := c.Unmarshal([]byte(`{}`), target); !errors.Is(err, ErrNilTarget) {
			t.Fatalf("Unmarshal into %T = %v, want ErrNilTarget", target, err)
		}
	}
	if _, err := c.Marshal(make(chan int)); err == nil {
		t.Fatal("Marshal(chan) succeeded")
	}
}
