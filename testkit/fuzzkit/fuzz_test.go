package fuzzkit

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/topicname"
)

// FuzzGenerators drives every generator with arbitrary input and checks the
// guarantees the package documents: GenTopic is always valid and within its
// limits, generated payloads respect Limits, never alias the input, are
// deterministic, and survive a Put/Gen round trip.
func FuzzGenerators(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3})
	w := fuzzutil.NewWriter()
	PutPayload(w, Limits{}, &payload.Payload{Key: []byte("k"), Body: []byte("b"), Headers: []payload.KV{{Key: "a", Value: []byte("1")}}})
	if err := PutTopic(w, Limits{}, "orders/created"); err != nil {
		f.Fatal(err)
	}
	f.Add(w.Data())
	f.Fuzz(func(t *testing.T, data []byte) {
		l := Limits{MaxKey: 8, MaxBody: 64, MaxHeaders: 4, MaxHeaderKey: 6, MaxHeaderValue: 8, MaxID: 8, MaxSegments: 3, MaxSegment: 6}
		orig := append([]byte{}, data...)

		p := GenPayload(fuzzutil.NewReader(data), l)
		name := GenTopic(fuzzutil.NewReader(data), l)
		if err := topicname.Validate(name); err != nil || !fitsLimits(name, l.normalized()) {
			t.Fatalf("GenTopic = %q: %v", name, err)
		}
		if len(p.Key) > 8 || len(p.Body) > 64 || len(p.Headers) > 4 || len(p.ID) > 8 {
			t.Fatalf("limits exceeded: %+v", *p)
		}
		for _, kv := range p.Headers {
			if len(kv.Key) > 6 || len(kv.Value) > 8 {
				t.Fatalf("header limits exceeded: %+v", kv)
			}
		}

		// Deterministic.
		if q := GenPayload(fuzzutil.NewReader(data), l); !reflect.DeepEqual(p, q) {
			t.Fatal("GenPayload not deterministic")
		}
		_ = GenTopicUnchecked(fuzzutil.NewReader(data), l)

		// Round trip through the mirrors.
		w := fuzzutil.NewWriter()
		PutPayload(w, l, p)
		if err := PutTopic(w, l, name); err != nil {
			t.Fatal(err)
		}
		r := fuzzutil.NewReader(w.Data())
		if q := GenPayload(r, l); !reflect.DeepEqual(p, q) {
			t.Fatalf("PutPayload/GenPayload round trip differs:\n%+v\n%+v", *p, *q)
		}
		if n := GenTopic(r, l); n != name {
			t.Fatalf("PutTopic/GenTopic round trip: %q vs %q", name, n)
		}

		// No aliasing of the input.
		for i := range p.Body {
			p.Body[i]++
		}
		for i := range p.Key {
			p.Key[i]++
		}
		for i := range p.Headers {
			for j := range p.Headers[i].Value {
				p.Headers[i].Value[j]++
			}
		}
		if !bytes.Equal(orig, data) {
			t.Fatal("generated payload aliases the fuzz input")
		}
	})
}
