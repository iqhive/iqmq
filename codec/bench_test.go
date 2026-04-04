package codec

import (
	"strconv"
	"strings"
	"testing"
)

// benchSizes are the encoded body sizes of specification, with the
// label each one carries in a benchmark name.
var benchSizes = []struct {
	name string
	size int
}{
	{"64B", 64},
	{"1KiB", 1024},
	{"64KiB", 64 << 10},
}

// benchValue returns a message whose JSON encoding is size bytes long, made
// of a realistic mixture: scalar fields, a few tags and attributes that grow
// with the payload, and a binary body that fills the rest. Encoding a single
// huge string would measure the copy alone; this measures the structure walk
// as well.
func benchValue(size int) modelValue {
	v := modelValue{ID: "bench-" + strconv.Itoa(size), Seq: 1 << 40, Amount: 12.5, OK: true}
	extras := 0
	switch {
	case size >= 64<<10:
		extras = 64
	case size >= 1024:
		extras = 8
	}
	if extras > 0 {
		v.Attrs = make(map[string]string, extras)
		for i := range extras {
			v.Tags = append(v.Tags, "tag-"+strconv.Itoa(i))
			v.Attrs["key-"+strconv.Itoa(i)] = "value-" + strconv.Itoa(i)
		}
	}
	head, err := New(JSONv1).Marshal(&v)
	if err != nil {
		panic(err)
	}
	// base64 costs four bytes per three, plus the "body":"" member itself.
	if deficit := size - len(head) - len(`,"body":""`); deficit > 0 {
		v.Body = make([]byte, deficit*3/4)
		for i := range v.Body {
			v.Body[i] = byte(i)
		}
	}
	return v
}

var (
	benchBytes []byte
	benchErr   error
)

// BenchmarkCodec/JSON/<kind>/{Marshal,Unmarshal,AppendMarshal}/<size> is the bake-off benchmark; BenchmarkCodec/raw/… measures the pass-through
// codec, and BenchmarkCodec/{Registry,ContentType}/… the receive-side
// negotiation that every consumer pays per message.
func BenchmarkCodec(b *testing.B) {
	b.Run("JSON", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				benchCodec(b, New(k))
			})
		}
	})
	b.Run("raw", func(b *testing.B) { benchRaw(b) })
	b.Run("Registry", func(b *testing.B) { benchRegistry(b) })
	b.Run("ContentType", func(b *testing.B) { benchContentType(b) })
}

// benchCodec runs the three operations over the three payload sizes.
func benchCodec(b *testing.B, c Codec) {
	a := c.(Appender)
	for _, size := range benchSizes {
		v := benchValue(size.size)
		body, err := c.Marshal(&v)
		if err != nil {
			b.Fatal(err)
		}
		b.Run("Marshal/"+size.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				benchBytes, benchErr = c.Marshal(&v)
			}
			if benchErr != nil {
				b.Fatal(benchErr)
			}
		})
		b.Run("AppendMarshal/"+size.name, func(b *testing.B) {
			buf := make([]byte, 0, len(body)+64)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				buf, benchErr = a.AppendMarshal(buf[:0], &v)
			}
			benchBytes = buf
			if benchErr != nil {
				b.Fatal(benchErr)
			}
		})
		b.Run("Unmarshal/"+size.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			var out modelValue
			for b.Loop() {
				out = modelValue{}
				benchErr = c.Unmarshal(body, &out)
			}
			benchBytes = out.Body
			if benchErr != nil {
				b.Fatal(benchErr)
			}
		})
	}
}

// benchRaw measures the pass-through codec at the same sizes.
func benchRaw(b *testing.B) {
	c := Raw()
	a := c.(Appender)
	for _, size := range benchSizes {
		body := make([]byte, size.size)
		for i := range body {
			body[i] = byte(i)
		}
		b.Run("Marshal/"+size.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				benchBytes, benchErr = c.Marshal(&body)
			}
		})
		b.Run("AppendMarshal/"+size.name, func(b *testing.B) {
			buf := make([]byte, 0, len(body))
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				buf, benchErr = a.AppendMarshal(buf[:0], &body)
			}
			benchBytes = buf
		})
		b.Run("Unmarshal/"+size.name, func(b *testing.B) {
			out := make([]byte, 0, len(body))
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				benchErr = c.Unmarshal(body, &out)
			}
			benchBytes = out
		})
	}
}

var benchCodecSink Codec

// benchRegistry measures the receive-side lookup: one map probe for a
// canonical content type, the parameter path, and Accept negotiation.
func benchRegistry(b *testing.B) {
	r := NewRegistry()
	if err := r.Register(New(Auto)); err != nil {
		b.Fatal(err)
	}
	if err := r.Register(Raw()); err != nil {
		b.Fatal(err)
	}
	cases := []struct{ name, in string }{
		{"For/canonical", ContentTypeJSON},
		{"For/parameters", "application/json; charset=utf-8"},
		{"For/suffix", "application/vnd.acme.order+json"},
		{"Negotiate/exact", "application/json"},
		{"Negotiate/list", "text/plain;q=0.3, application/json;q=0.9, */*;q=0.1"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			if strings.HasPrefix(tc.name, "For") {
				for b.Loop() {
					benchCodecSink, benchErr = r.For(tc.in)
				}
			} else {
				for b.Loop() {
					benchCodecSink, benchErr = r.Negotiate(tc.in)
				}
			}
			if benchErr != nil {
				b.Fatal(benchErr)
			}
		})
	}
}

var (
	benchMedia MediaType
	benchMatch error
)

// benchContentType measures the parser and the matcher on their own.
func benchContentType(b *testing.B) {
	b.Run("Parse/canonical", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchMedia, benchErr = ParseContentType(ContentTypeJSON)
		}
	})
	b.Run("Parse/parameters", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchMedia, benchErr = ParseContentType("application/json; charset=utf-8")
		}
	})
	b.Run("Match/equal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchMatch = Match(ContentTypeJSON, ContentTypeJSON)
		}
	})
	b.Run("Match/suffix", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchMatch = Match(ContentTypeJSON, "application/vnd.acme.order+json")
		}
	})
	_ = benchMatch
}

// TestBenchPayloadSizes keeps the size labels in benchmark names honest.
func TestBenchPayloadSizes(t *testing.T) {
	for _, size := range benchSizes {
		v := benchValue(size.size)
		b, err := New(JSONv1).Marshal(&v)
		if err != nil {
			t.Fatal(err)
		}
		if delta := len(b) - size.size; delta < -size.size/20 || delta > size.size/20 {
			t.Fatalf("%s payload encodes to %d bytes", size.name, len(b))
		}
	}
}
