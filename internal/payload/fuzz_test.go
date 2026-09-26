package payload_test

import (
	"reflect"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/testkit/fuzzkit"
)

// fuzzLimits keeps generated payloads small enough for many iterations while
// still exercising every shape class.
var fuzzLimits = fuzzkit.Limits{MaxKey: 16, MaxBody: 256, MaxHeaders: 6, MaxHeaderKey: 8, MaxHeaderValue: 32, MaxID: 16}

// naiveClone is the oracle deep copy: field by field with append, which
// preserves nil versus empty by construction.
func naiveClone(p *payload.Payload) *payload.Payload {
	cp := func(b []byte) []byte {
		if b == nil {
			return nil
		}
		return append([]byte{}, b...)
	}
	c := &payload.Payload{Key: cp(p.Key), Body: cp(p.Body), ID: p.ID, Props: p.Props}
	if p.Headers != nil {
		c.Headers = make([]payload.KV, len(p.Headers))
		for i, kv := range p.Headers {
			c.Headers[i] = payload.KV{Key: kv.Key, Value: cp(kv.Value)}
		}
	}
	return c
}

// scribble flips every byte reachable through p and appends to every slice,
// so that any storage shared with another payload becomes visible there.
func scribble(p *payload.Payload) {
	for i := range p.Key {
		p.Key[i] ^= 0xff
	}
	for i := range p.Body {
		p.Body[i] ^= 0xff
	}
	for i := range p.Headers {
		for j := range p.Headers[i].Value {
			p.Headers[i].Value[j] ^= 0xff
		}
		p.Headers[i].Value = append(p.Headers[i].Value, 'z')
		p.Headers[i].Key += "!"
	}
	p.Key = append(p.Key, 'k')
	p.Body = append(p.Body, 'b')
	p.Headers = append(p.Headers, payload.KV{Key: "scribble", Value: []byte{1}})
	p.ID += "!"
	p.Props.ReplyTo += "!"
}

// FuzzPayloadClone checks that Clone and CloneInto produce equal,
// storage-independent copies for every payload shape (nil versus empty,
// duplicate headers, binary values), that Alias shares storage exactly when
// there is storage to share, that Equal agrees with reflect.DeepEqual, and
// that Reset and Zero leave the source untouched.
func FuzzPayloadClone(f *testing.F) {
	seedPayloads := []*payload.Payload{
		{},
		{Key: []byte{}, Body: []byte{}, Headers: []payload.KV{}},
		{Key: []byte("k"), Body: []byte("body"), Headers: []payload.KV{{Key: "a", Value: []byte("1")}, {Key: "a", Value: []byte("2")}, {Key: "n", Value: nil}, {Key: "e", Value: []byte{}}}, ID: "id-1"},
		{Body: []byte{0, 255, 0, 255}, Props: payload.Props{ContentType: "application/json", ReplyTo: "replies/inbox"}},
	}
	for _, p := range seedPayloads {
		w := fuzzutil.NewWriter()
		fuzzkit.PutPayload(w, fuzzLimits, p)
		fuzzkit.PutPayload(w, fuzzLimits, p) // destination for CloneInto
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		p := fuzzkit.GenPayload(r, fuzzLimits)
		dst := fuzzkit.GenPayload(r, fuzzLimits)
		snapshot := naiveClone(p)
		if !payload.Equal(p, snapshot) || !reflect.DeepEqual(p, snapshot) {
			t.Fatalf("naive clone differs from source:\n%+v\n%+v", *p, *snapshot)
		}

		// Clone: equal, independent, nil-ness preserved, same accounting.
		c := p.Clone()
		if !payload.Equal(p, c) || !reflect.DeepEqual(p, c) {
			t.Fatalf("Clone differs:\n%+v\n%+v", *p, *c)
		}
		if payload.Aliases(p, c) {
			t.Fatal("Clone shares storage with its source")
		}
		if (p.Key == nil) != (c.Key == nil) || (p.Body == nil) != (c.Body == nil) || (p.Headers == nil) != (c.Headers == nil) {
			t.Fatal("Clone changed nil-ness")
		}
		if c.Size() != p.Size() {
			t.Fatalf("Clone Size %d, source %d", c.Size(), p.Size())
		}
		scribble(c)
		if !payload.Equal(p, snapshot) {
			t.Fatalf("writing through the clone changed the source:\n%+v\n%+v", *p, *snapshot)
		}

		// CloneInto a destination with arbitrary prior content and capacity.
		before := dst.Alias()
		p.CloneInto(dst)
		if !payload.Equal(p, dst) || !reflect.DeepEqual(p, dst) {
			t.Fatalf("CloneInto differs:\n%+v\n%+v", *p, *dst)
		}
		if payload.Aliases(p, dst) {
			t.Fatal("CloneInto shares storage with its source")
		}
		// Reuse is allowed but never grows beyond what was there or needed.
		if cap(dst.Key) < len(p.Key) || cap(dst.Body) < len(p.Body) || cap(dst.Headers) < len(p.Headers) {
			t.Fatal("CloneInto capacity too small for its content")
		}
		_ = before
		scribble(dst)
		if !payload.Equal(p, snapshot) {
			t.Fatal("writing through the CloneInto destination changed the source")
		}

		// Writes to the source do not reach an earlier clone.
		c2 := p.Clone()
		c2snap := naiveClone(c2)
		scribble(p)
		if !payload.Equal(c2, c2snap) {
			t.Fatal("writing through the source changed a clone")
		}
		p = naiveClone(snapshot)

		// CloneInto(self) is a no-op.
		p.CloneInto(p)
		if !payload.Equal(p, snapshot) {
			t.Fatal("CloneInto(self) changed the payload")
		}

		// Alias shares exactly when there is storage; Equal agrees with
		// DeepEqual either way.
		a := p.Alias()
		if !payload.Equal(p, a) || !reflect.DeepEqual(p, a) {
			t.Fatal("Alias differs from its source")
		}
		if payload.Aliases(p, a) != p.HasStorage() {
			t.Fatalf("Aliases(p, Alias) = %v, HasStorage %v", payload.Aliases(p, a), p.HasStorage())
		}
		var into payload.Payload
		p.AliasInto(&into)
		if !reflect.DeepEqual(p, &into) || payload.Aliases(p, &into) != p.HasStorage() {
			t.Fatal("AliasInto differs from Alias")
		}

		// Equal is symmetric and detects every single-field change.
		if !payload.Equal(c2snap, c2) || !payload.Equal(c2, c2snap) {
			t.Fatal("Equal not symmetric")
		}
		mutants := []func(q *payload.Payload){
			func(q *payload.Payload) { q.ID += "x" },
			func(q *payload.Payload) { q.Props.CausationID += "x" },
			func(q *payload.Payload) { q.Body = append(q.Body, 0) },
			func(q *payload.Payload) { q.Key = append(q.Key, 0) },
			func(q *payload.Payload) { q.Headers = append(q.Headers, payload.KV{}) },
			func(q *payload.Payload) {
				if q.Body == nil {
					q.Body = []byte{}
				} else {
					q.Body = nil
				}
			},
		}
		for i, mutate := range mutants {
			q := naiveClone(p)
			mutate(q)
			if payload.Equal(p, q) != reflect.DeepEqual(p, q) {
				t.Fatalf("mutant %d: Equal = %v, DeepEqual = %v", i, payload.Equal(p, q), reflect.DeepEqual(p, q))
			}
			if payload.Equal(p, q) {
				t.Fatalf("mutant %d not detected by Equal", i)
			}
		}

		// Reset and Zero on a clone leave the source untouched and clear
		// the clone; Zero also wipes the bytes.
		rc := p.Clone()
		rc.Reset()
		if rc.Size() != 0 || len(rc.Headers) != 0 || !payload.Equal(p, snapshot) {
			t.Fatal("Reset misbehaved")
		}
		zc := p.Clone()
		zc.Zero()
		if zc.Size() != 0 || zc.ID != "" || !zc.Props.IsZero() || !payload.Equal(p, snapshot) {
			t.Fatal("Zero misbehaved")
		}
		for _, b := range [][]byte{zc.Key[:cap(zc.Key)], zc.Body[:cap(zc.Body)]} {
			for _, x := range b {
				if x != 0 {
					t.Fatal("Zero left a non-zero byte")
				}
			}
		}
		for _, kv := range zc.Headers[:cap(zc.Headers)] {
			if kv.Key != "" {
				t.Fatal("Zero left a header key")
			}
			for _, x := range kv.Value[:cap(kv.Value)] {
				if x != 0 {
					t.Fatal("Zero left a non-zero header byte")
				}
			}
		}
	})
}
