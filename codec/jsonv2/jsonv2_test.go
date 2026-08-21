package jsonv2

import (
	"bytes"
	"errors"
	"testing"

	"bitbucket.org/iqhive/iqmq/codec"
)

type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

func TestNew(t *testing.T) {
	c, err := New()
	if !Available() {
		if !errors.Is(err, codec.ErrUnavailable) {
			t.Fatalf("New() = %v, %v on a build without json/v2", c, err)
		}
		t.Skip("build has no encoding/json/v2")
	}
	if err != nil {
		t.Fatal(err)
	}
	if c.ContentType() != codec.ContentTypeJSON {
		t.Fatalf("ContentType = %q", c.ContentType())
	}
	want := order{ID: "a", Qty: 2}
	b, err := c.Marshal(&want)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"id":"a","qty":2}` {
		t.Fatalf("Marshal = %s", b)
	}
	var got order
	if err := c.Unmarshal(b, &got); err != nil || got != want {
		t.Fatalf("Unmarshal = %+v, %v", got, err)
	}
	if MustNew().ContentType() != codec.ContentTypeJSON {
		t.Fatal("MustNew")
	}
	tc, err := Typed[order]()
	if err != nil {
		t.Fatal(err)
	}
	tb, err := tc.Encode(want)
	if err != nil || !bytes.Equal(tb, b) {
		t.Fatalf("typed Encode = %s, %v", tb, err)
	}
	if v, err := tc.Decode(tb); err != nil || v != want {
		t.Fatalf("typed Decode = %+v, %v", v, err)
	}
}

func TestRegister(t *testing.T) {
	r := codec.NewRegistry()
	if err := r.Register(codec.New(codec.JSONv1)); err != nil {
		t.Fatal(err)
	}
	err := Register(r)
	if !Available() {
		if !errors.Is(err, codec.ErrUnavailable) {
			t.Fatalf("Register = %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.For(codec.ContentTypeJSON)
	if err != nil {
		t.Fatal(err)
	}
	// v2 does not escape HTML-sensitive runes; v1 does. That difference is
	// how the test tells which implementation is registered.
	b, err := c.Marshal(&order{ID: "<a>"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"id":"<a>","qty":0}` {
		t.Fatalf("registered codec is not v2: %s", b)
	}
}
