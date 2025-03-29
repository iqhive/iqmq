package payload

import (
	"bytes"
	"testing"
)

func TestOverlaps(t *testing.T) {
	arr := make([]byte, 16)
	other := make([]byte, 16)
	cases := []struct {
		name string
		a, b []byte
		want bool
	}{
		{"same slice", arr, arr, true},
		{"distinct arrays", arr, other, false},
		{"nil vs nil", nil, nil, false},
		{"nil vs slice", nil, arr, false},
		{"empty vs itself", []byte{}, []byte{}, false},
		{"zero-len window with capacity", arr[:0], arr[8:], true},
		{"disjoint windows, first capped", arr[0:4:4], arr[4:8], false},
		{"disjoint windows, first not capped", arr[0:4], arr[4:8], true},
		{"nested", arr[2:6], arr[4:5], true},
		{"adjacent capped", arr[0:8:8], arr[8:16], false},
		{"second before first", arr[8:], arr[0:8:8], false},
		{"second before first reaching", arr[8:], arr[0:4], true},
	}
	for _, c := range cases {
		if got := Overlaps(c.a, c.b); got != c.want {
			t.Errorf("%s: Overlaps = %v, want %v", c.name, got, c.want)
		}
		if got := Overlaps(c.b, c.a); got != c.want {
			t.Errorf("%s: Overlaps (swapped) = %v, want %v", c.name, got, c.want)
		}
	}
	// Element types wider than a byte.
	kv := make([]KV, 4)
	if !overlaps(kv[:2], kv[1:]) || overlaps(kv[:2:2], kv[2:]) {
		t.Error("overlaps on []KV mis-scaled by element size")
	}
	if overlaps(make([]struct{}, 4), make([]struct{}, 4)) {
		t.Error("zero-size elements reported as overlapping")
	}
}

func TestAliasesAndClone(t *testing.T) {
	p := sample()
	c := p.Clone()
	if Aliases(p, c) || Aliases(c, p) {
		t.Fatal("Clone aliases its source")
	}
	a := p.Alias()
	if !Aliases(p, a) || !Aliases(a, p) || !Equal(p, a) {
		t.Fatal("Alias does not alias its source")
	}
	var into Payload
	p.AliasInto(&into)
	if !Aliases(p, &into) || !Equal(p, &into) {
		t.Fatal("AliasInto does not alias its source")
	}
	// Writes through the alias are visible through the original.
	a.Body[0] = 'X'
	if p.Body[0] != 'X' {
		t.Error("alias write not visible")
	}
	// Individual field sharing is detected.
	cases := map[string]*Payload{
		"key":          {Key: p.Key},
		"body":         {Body: p.Body},
		"key as body":  {Body: p.Key},
		"header value": {Headers: []KV{{Key: "z", Value: p.Headers[1].Value}}},
		"value as key": {Key: p.Headers[2].Value},
		"header table": {Headers: p.Headers[:1]},
	}
	for name, q := range cases {
		if !Aliases(p, q) || !Aliases(q, p) {
			t.Errorf("%s: sharing not detected", name)
		}
	}
	// Equal content in separate storage is not aliasing.
	if q := sample(); Aliases(p, q) {
		t.Error("equal payloads reported as aliasing")
	}
	// Storage-less payloads alias nothing, including themselves.
	empty := &Payload{Key: []byte{}, Body: nil, Headers: []KV{}, ID: "x"}
	if empty.HasStorage() || Aliases(empty, empty) || Aliases(empty, empty.Alias()) {
		t.Error("storage-less payload reported as aliasing")
	}
	if !p.HasStorage() || !Aliases(p, p) {
		t.Error("payload with storage not reported as self-aliasing")
	}
	if Aliases(nil, p) || Aliases(p, nil) || Aliases(nil, nil) {
		t.Error("nil payload reported as aliasing")
	}
}

func TestZero(t *testing.T) {
	p := sample()
	p.Key = append(make([]byte, 0, 8), p.Key...)
	p.Body = append(make([]byte, 0, 32), p.Body...)
	p.Headers = append(make([]KV, 0, 6), p.Headers...)
	p.Headers[0].Value = append(make([]byte, 0, 16), p.Headers[0].Value...)
	p.Headers = p.Headers[:2] // a stale entry beyond len keeps its storage
	stale := p.Headers[:3][2].Value
	keyCap, bodyCap, hdrCap, valCap := cap(p.Key), cap(p.Body), cap(p.Headers), cap(p.Headers[0].Value)
	keyFull, bodyFull, valFull := p.Key[:keyCap], p.Body[:bodyCap], p.Headers[0].Value[:valCap]
	copy(keyFull[len(p.Key):], "secret")
	p.Zero()
	if p.Size() != 0 || len(p.Key) != 0 || len(p.Body) != 0 || len(p.Headers) != 0 || p.ID != "" || !p.Props.IsZero() {
		t.Errorf("Zero left content: %+v", *p)
	}
	if cap(p.Key) != keyCap || cap(p.Body) != bodyCap || cap(p.Headers) != hdrCap || cap(p.Headers[:1][0].Value) != valCap {
		t.Error("Zero dropped capacity")
	}
	for name, b := range map[string][]byte{"key": keyFull, "body": bodyFull, "value": valFull, "stale value": stale} {
		if !bytes.Equal(b, make([]byte, len(b))) {
			t.Errorf("%s not zeroed over full capacity: %q", name, b)
		}
	}
	if h := p.Headers[:hdrCap]; h[2].Key != "" || len(h[2].Value) != 0 {
		t.Errorf("stale header entry not cleared: %+v", h[2])
	}
	// Nil-ness is preserved and a storage-less payload zeroes cleanly.
	var empty Payload
	empty.Zero()
	if empty.Key != nil || empty.Body != nil || empty.Headers != nil {
		t.Error("Zero materialised storage")
	}
}

func TestAllocsAlias(t *testing.T) {
	p := sample()
	q := p.Clone()
	var into Payload
	var sink int
	if n := testing.AllocsPerRun(200, func() { p.AliasInto(&into) }); n != 0 {
		t.Errorf("AliasInto allocated %v times", n)
	}
	if n := testing.AllocsPerRun(200, func() {
		if Aliases(p, q) {
			sink++
		}
	}); n != 0 {
		t.Errorf("Aliases allocated %v times", n)
	}
	if n := testing.AllocsPerRun(200, func() { q.Zero(); p.CloneInto(q) }); n != 0 {
		t.Errorf("Zero+CloneInto allocated %v times", n)
	}
	_ = sink
}
