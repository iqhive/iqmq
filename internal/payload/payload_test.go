package payload

import (
	"testing"
	"time"
)

func sample() *Payload {
	return &Payload{
		Key:  []byte("k"),
		Body: []byte("body"),
		Headers: []KV{
			{Key: "a", Value: []byte("1")},
			{Key: "a", Value: []byte("2")},
			{Key: "bin", Value: []byte{0, 1, 2}},
		},
		ID: "id-1",
		Props: Props{
			ContentType: "application/json",
			CreatedAt:   time.Unix(1, 0),
		},
	}
}

func TestCloneIndependence(t *testing.T) {
	p := sample()
	c := p.Clone()
	if !Equal(p, c) {
		t.Fatal("clone not equal")
	}
	c.Key[0] = 'x'
	c.Body[0] = 'x'
	c.Headers[0].Value[0] = 'x'
	c.Headers = append(c.Headers, KV{Key: "z"})
	if string(p.Key) != "k" || string(p.Body) != "body" || string(p.Headers[0].Value) != "1" || len(p.Headers) != 3 {
		t.Fatal("mutating the clone changed the original")
	}
}

func TestClonePreservesNilVersusEmpty(t *testing.T) {
	cases := []Payload{
		{},
		{Body: []byte{}},
		{Key: []byte{}, Body: nil},
		{Headers: []KV{}},
		{Headers: []KV{{Key: "e", Value: []byte{}}}},
		{Headers: []KV{{Key: "n", Value: nil}}},
	}
	for i, p := range cases {
		c := p.Clone()
		if !Equal(&p, c) {
			t.Errorf("case %d: clone differs: %+v vs %+v", i, p, *c)
		}
		if (p.Body == nil) != (c.Body == nil) || (p.Key == nil) != (c.Key == nil) || (p.Headers == nil) != (c.Headers == nil) {
			t.Errorf("case %d: nil-ness not preserved", i)
		}
	}
}

func TestCloneIntoReusesCapacity(t *testing.T) {
	dst := &Payload{
		Key:     make([]byte, 0, 16),
		Body:    make([]byte, 0, 16),
		Headers: make([]KV, 0, 4),
	}
	dst.Headers = dst.Headers[:1]
	dst.Headers[0].Value = make([]byte, 0, 8)
	keyPtr, bodyPtr, hdrPtr, valPtr := &dst.Key[:1][0], &dst.Body[:1][0], &dst.Headers[:1][0], &dst.Headers[0].Value[:1][0]
	p := sample()
	p.CloneInto(dst)
	if !Equal(p, dst) {
		t.Fatal("CloneInto result differs")
	}
	if &dst.Key[0] != keyPtr || &dst.Body[0] != bodyPtr || &dst.Headers[0] != hdrPtr || &dst.Headers[0].Value[0] != valPtr {
		t.Error("CloneInto did not reuse destination storage")
	}
	// Self-clone is a no-op.
	before := p.Clone()
	p.CloneInto(p)
	if !Equal(p, before) {
		t.Error("CloneInto(self) changed the payload")
	}
}

func TestCloneIntoGrows(t *testing.T) {
	dst := &Payload{Key: []byte("x"), Headers: []KV{{Key: "q", Value: []byte("q")}}}
	p := sample()
	p.CloneInto(dst)
	if !Equal(p, dst) {
		t.Fatal("CloneInto with insufficient capacity differs")
	}
}

func TestSizeAndReset(t *testing.T) {
	p := sample()
	want := 1 + 4 + 4 + (1 + 1) + (1 + 1) + (3 + 3) + len("application/json")
	if got := p.Size(); got != want {
		t.Errorf("Size = %d, want %d", got, want)
	}
	keyCap, bodyCap, hdrCap, valCap := cap(p.Key), cap(p.Body), cap(p.Headers), cap(p.Headers[0].Value)
	p.Reset()
	if p.Size() != 0 || p.ID != "" || !p.Props.IsZero() || len(p.Headers) != 0 || len(p.Key) != 0 || len(p.Body) != 0 {
		t.Errorf("Reset left content: %+v", *p)
	}
	if cap(p.Key) != keyCap || cap(p.Body) != bodyCap || cap(p.Headers) != hdrCap || cap(p.Headers[:1][0].Value) != valCap {
		t.Error("Reset dropped capacity")
	}
}

func TestEqual(t *testing.T) {
	a, b := sample(), sample()
	if !Equal(a, b) || !Equal(nil, nil) || Equal(a, nil) || Equal(nil, b) {
		t.Fatal("basic Equal cases failed")
	}
	b.Props.ReplyTo = "r"
	if Equal(a, b) {
		t.Error("Equal ignored props")
	}
	b = sample()
	b.Headers[1].Key = "b"
	if Equal(a, b) {
		t.Error("Equal ignored header key")
	}
	b = sample()
	b.Body = nil
	if Equal(a, b) {
		t.Error("Equal ignored nil body")
	}
}

func TestAllocsCloneInto(t *testing.T) {
	p := sample()
	dst := p.Clone()
	if n := testing.AllocsPerRun(1000, func() { p.CloneInto(dst) }); n != 0 {
		t.Errorf("CloneInto into a sufficiently sized destination allocated %v times", n)
	}
}
