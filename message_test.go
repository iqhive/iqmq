package iqmq_test

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestMessageClone holds the specification's "independent deep copy" and the specification's
// nil-versus-empty distinction.
func TestMessageClone(t *testing.T) {
	t.Run("NoAliasSurvives", func(t *testing.T) {
		m := &mq.Message{
			Topic: "orders",
			Key:   []byte("cust-42"),
			Body:  []byte(`{"id":1}`),
			ID:    "msg-1",
			Properties: mq.Properties{
				ContentType:   "application/json",
				CorrelationID: "corr-1",
				CreatedAt:     time.Unix(1, 0).UTC(),
			},
		}
		m.Headers.Add("trace", []byte("one"))
		m.Headers.Add("trace", []byte("two"))
		c := m.Clone()

		// Every mutable byte of the original is rewritten in place. A clone
		// that shared any of that storage shows it.
		for i := range m.Key {
			m.Key[i] = 'X'
		}
		for i := range m.Body {
			m.Body[i] = 'Y'
		}
		for _, v := range m.Headers.Values("trace") {
			for i := range v {
				v[i] = 'Z'
			}
		}
		if string(c.Key) != "cust-42" {
			t.Errorf("the clone's Key followed the original's mutation: %q", c.Key)
		}
		if string(c.Body) != `{"id":1}` {
			t.Errorf("the clone's Body followed the original's mutation: %q", c.Body)
		}
		vals := c.Headers.Values("trace")
		got := make([]string, 0, len(vals))
		for _, v := range vals {
			got = append(got, string(v))
		}
		if len(got) != 2 || got[0] != "one" || got[1] != "two" {
			t.Errorf("the clone's headers followed the original's mutation: %q", got)
		}

		// And the reverse: a write to the clone is invisible to the original.
		c.Headers.Add("only-in-clone", nil)
		if _, ok := m.Headers.Get("only-in-clone"); ok {
			t.Error("the original saw a header added to the clone")
		}
		if c.Topic != "orders" || c.ID != "msg-1" || c.Properties.ContentType != "application/json" {
			t.Errorf("the clone dropped an immutable field: %+v", c)
		}
		if !c.Properties.CreatedAt.Equal(time.Unix(1, 0).UTC()) {
			t.Errorf("the clone dropped CreatedAt: %v", c.Properties.CreatedAt)
		}
	})

	t.Run("NilAndEmptyStayDifferent", func(t *testing.T) {
		// The specification: "nil and empty both valid". A Clone that turned a nil Body
		// into an empty one would change what the driver publishes.
		cases := []struct {
			name string
			m    *mq.Message
		}{
			{"nil", &mq.Message{}},
			{"empty", &mq.Message{Key: []byte{}, Body: []byte{}}},
		}
		for _, c := range cases {
			got := c.m.Clone()
			if (got.Key == nil) != (c.m.Key == nil) {
				t.Errorf("%s: Key nil-ness changed: %v -> %v", c.name, c.m.Key == nil, got.Key == nil)
			}
			if (got.Body == nil) != (c.m.Body == nil) {
				t.Errorf("%s: Body nil-ness changed: %v -> %v", c.name, c.m.Body == nil, got.Body == nil)
			}
		}
	})

	t.Run("ResetClearsEverything", func(t *testing.T) {
		m := &mq.Message{
			Topic: "t", Key: []byte("k"), Body: []byte("b"), ID: "i",
			Properties: mq.Properties{ContentType: "c", CreatedAt: time.Unix(1, 0)},
		}
		m.Headers.Add("h", []byte("v"))
		m.Reset()
		if m.Topic != "" || m.Key != nil || m.Body != nil || m.ID != "" {
			t.Errorf("Reset left a field set: %+v", m)
		}
		if m.Headers.Len() != 0 {
			t.Errorf("Reset left %d headers", m.Headers.Len())
		}
		if !m.Properties.IsZero() {
			t.Errorf("Reset left properties set: %+v", m.Properties)
		}
		var zero mq.Message
		if got, want := snapshotMessage(m), snapshotMessage(&zero); got != want {
			t.Errorf("a Reset message is not the zero Message: a pooled message would carry state forward\n  got  %s\n  want %s", got, want)
		}
	})

	t.Run("PropertiesIsZero", func(t *testing.T) {
		var p mq.Properties
		if !p.IsZero() {
			t.Fatal("the zero Properties is not IsZero")
		}
		for _, set := range []func(*mq.Properties){
			func(p *mq.Properties) { p.ContentType = "x" },
			func(p *mq.Properties) { p.ContentEncoding = "x" },
			func(p *mq.Properties) { p.CorrelationID = "x" },
			func(p *mq.Properties) { p.CausationID = "x" },
			func(p *mq.Properties) { p.ReplyTo = "x" },
			func(p *mq.Properties) { p.CreatedAt = time.Unix(1, 0) },
		} {
			var q mq.Properties
			set(&q)
			if q.IsZero() {
				t.Errorf("IsZero reported true for %+v: a field is not being read", q)
			}
		}
	})
}

// FuzzMessageClone is target 5: alias independence over arbitrary
// messages.
//
// The specification raced the key representations and rows / the delivery and header
// shapes, but no prototype owned Message.Clone, so the target is written here
// against the frozen type. The invariant is the one the specification states and the one
// a caller relies on when it keeps a received message past settlement: after
// Clone, no mutation of either message is observable in the other.
func FuzzMessageClone(f *testing.F) {
	seed := func(nKeys, keyLen, bodyLen int) []byte {
		w := fuzzutil.NewWriter()
		w.Bytes(32, bytes.Repeat([]byte("k"), keyLen))
		w.Bool(keyLen > 0)
		w.Bytes(64, bytes.Repeat([]byte("b"), bodyLen))
		w.Bool(bodyLen > 0)
		w.String(16, "topic")
		w.String(16, "id")
		w.Int(0, 8, nKeys)
		for i := range nKeys {
			w.String(8, "h")
			w.Bytes(16, bytes.Repeat([]byte("v"), i))
			w.Bool(i%2 == 0)
		}
		w.String(16, "application/json")
		w.String(16, "gzip")
		w.String(16, "corr")
		w.String(16, "cause")
		w.String(16, "reply")
		w.Int(0, 1<<20, 1234)
		return w.Data()
	}
	f.Add(seed(0, 0, 0))
	f.Add(seed(1, 3, 8))
	f.Add(seed(8, 0, 0))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		m := decodeMessage(r)
		before := snapshotMessage(m)
		c := m.Clone()
		if got := snapshotMessage(c); got != before {
			t.Fatalf("Clone is not a copy:\n  clone    %s\n  original %s", got, before)
		}
		// Rewrite every mutable byte of the original, then of the clone.
		scribble(m, 'X')
		if got := snapshotMessage(c); got != before {
			t.Fatalf("the clone followed a mutation of the original:\n  clone %s\n  want  %s", got, before)
		}
		scribble(m, 'X') // back to the original bytes
		if got := snapshotMessage(m); got != before {
			t.Fatalf("the fixture is wrong: scribbling twice did not restore the original")
		}
		scribble(c, 'Y')
		if got := snapshotMessage(m); got != before {
			t.Fatalf("the original followed a mutation of the clone:\n  original %s\n  want     %s", got, before)
		}
		// A structural change to one must not reach the other.
		c.Headers.Add("clone-only", []byte("z"))
		if _, ok := m.Headers.Get("clone-only"); ok {
			t.Fatal("the original saw a header added to the clone")
		}
		m.Headers.Add("original-only", []byte("z"))
		if _, ok := c.Headers.Get("original-only"); ok {
			t.Fatal("the clone saw a header added to the original")
		}
		// Reset must leave the zero Message whatever the message held.
		m.Reset()
		var zero mq.Message
		if got, want := snapshotMessage(m), snapshotMessage(&zero); got != want {
			t.Fatalf("Reset left %s, want the zero Message %s", got, want)
		}
	})
}

// decodeMessage builds a message from arbitrary bytes. The nil-versus-empty
// distinction of Key and Body is reachable: a length-zero slice is stored as
// nil or as an empty non-nil slice according to a bit of the input.
func decodeMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	m.Key = maybeNil(r.Bytes(32), r.Bool())
	m.Body = maybeNil(r.Bytes(64), r.Bool())
	m.Topic = r.String(16)
	m.ID = r.String(16)
	n := r.Int(0, 8)
	for range n {
		key := r.String(8)
		m.Headers.Add(key, maybeNil(r.Bytes(16), r.Bool()))
	}
	m.Properties = mq.Properties{
		ContentType:     r.String(16),
		ContentEncoding: r.String(16),
		CorrelationID:   r.String(16),
		CausationID:     r.String(16),
		ReplyTo:         r.String(16),
		CreatedAt:       time.Unix(0, int64(r.Int(0, 1<<20))).UTC(),
	}
	return m
}

// maybeNil returns b, or nil when b is empty and nonNil is false, so that
// both of the empty forms are generated.
func maybeNil(b []byte, nonNil bool) []byte {
	b = bytes.Clone(b)
	if len(b) == 0 {
		if nonNil {
			return []byte{}
		}
		return nil
	}
	return b
}

// snapshotMessage renders everything about a message that a copy must
// preserve, including whether each slice is nil, as one comparable string.
func snapshotMessage(m *mq.Message) string {
	var b []byte
	b = append(b, "topic="...)
	b = append(b, m.Topic...)
	b = append(b, " id="...)
	b = append(b, m.ID...)
	b = appendSlice(b, " key=", m.Key)
	b = appendSlice(b, " body=", m.Body)
	b = append(b, " headers=["...)
	for k, v := range m.Headers.All() {
		b = append(b, k...)
		b = appendSlice(b, "=", v)
		b = append(b, ';')
	}
	b = append(b, "] props="...)
	p := m.Properties
	for _, s := range []string{p.ContentType, p.ContentEncoding, p.CorrelationID, p.CausationID, p.ReplyTo} {
		b = append(b, s...)
		b = append(b, '|')
	}
	b = append(b, p.CreatedAt.Format(time.RFC3339Nano)...)
	return string(b)
}

// appendSlice renders a byte slice, distinguishing nil from empty.
func appendSlice(dst []byte, label string, v []byte) []byte {
	dst = append(dst, label...)
	if v == nil {
		return append(dst, "<nil>"...)
	}
	dst = append(dst, '<')
	dst = append(dst, v...)
	return append(dst, '>')
}

// scribble exclusive-ors every mutable byte of the message with c, so that
// applying it twice restores the original bytes.
func scribble(m *mq.Message, c byte) {
	for i := range m.Key {
		m.Key[i] ^= c
	}
	for i := range m.Body {
		m.Body[i] ^= c
	}
	// All yields the stored slice, and each pair exactly once, so a
	// duplicated key is scribbled once per pair and not once per lookup.
	for _, v := range m.Headers.All() {
		for i := range v {
			v[i] ^= c
		}
	}
}
