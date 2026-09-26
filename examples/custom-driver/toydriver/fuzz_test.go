package toydriver_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/examples/custom-driver/toydriver"
)

// FuzzMetadataModes holds the toy driver to the one property the specification gives a
// driver whose broker cannot carry every kind of metadata: under
// MetadataNativeStrict it either round-trips a message exactly or REFUSES it
// with ErrUnsupported, and under MetadataPortableEnvelope it always
// round-trips it exactly.
//
// The forbidden third outcome is the one this target exists to find: a
// publication that succeeds and arrives with something missing. That is what a
// driver does when it wraps under strict mode, or drops a header it cannot
// represent without saying so, and it is invisible to a test that only checks
// the happy path.
//
// It is also the fuzz target a driver author outside this repository has to
// write for themselves. `testkit/fuzzkit` generates exactly this shape of data
// from a fuzz input — and imports `internal/payload` and `internal/fuzzutil`,
// so it is unreachable from here. What is left is Go's own
// multi-argument fuzzing, which is enough.
func FuzzMetadataModes(f *testing.F) {
	f.Add([]byte("cust-1"), []byte(`{"id":1}`), "content-kind", []byte("order"), "m-1")
	f.Add([]byte(nil), []byte(nil), "", []byte(nil), "")
	f.Add([]byte{}, []byte{}, "x", []byte{}, "")
	f.Add([]byte{0x00, 0xff}, []byte{0x80, 0x00, 0x7f}, "trace-state", []byte{0x00, 0xff}, "id-9")
	f.Add([]byte("k"), bytes.Repeat([]byte{0xa5}, 4096), "long", bytes.Repeat([]byte("v"), 300), "id")

	f.Fuzz(func(t *testing.T, key, body []byte, hkey string, hval []byte, id string) {
		// A header key the portable model rejects is not this driver's
		// business: the grammar is the core's to enforce, and a fuzzer
		// that spent its budget on invalid keys would never reach the
		// representability question this target is about.
		if hkey != "" && (!utf8.ValidString(hkey) || strings.HasPrefix(hkey, mq.ReservedHeaderPrefix)) {
			t.Skip("the portable header grammar rejects this key")
		}
		if !utf8.ValidString(id) {
			t.Skip("a message identifier is a string and this one is not valid UTF-8")
		}
		for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope} {
			roundTrip(t, mode, key, body, hkey, hval, id)
		}
	})
}

// roundTrip publishes one message under one metadata mode and checks what came
// back, or that nothing did and the driver said why.
func roundTrip(t *testing.T, mode mq.MetadataMode, key, body []byte, hkey string, hval []byte, id string) {
	t.Helper()
	ctx := t.Context()
	client, err := toydriver.Open(toydriver.Config{
		Topics: []toydriver.TopicSpec{{Name: "fuzz", Subscriptions: []string{"s"}}},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = client.Close() }()

	// The consumer is opened first: this broker's subscriptions start at the
	// end of their queue, so one created after the publication would see
	// nothing and the check would pass over an empty topic.
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "fuzz", Subscription: "s", Metadata: mode})
	if err != nil {
		t.Fatalf("Consumer(%v): %v", mode, err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "fuzz", Metadata: mode})
	if err != nil {
		t.Fatalf("Publisher(%v): %v", mode, err)
	}

	m := &mq.Message{Key: key, Body: body, ID: id}
	if hkey != "" {
		m.Headers.Set(hkey, hval)
	}
	switch err := p.Publish(ctx, m); {
	case err == nil:
	case errors.Is(err, mq.ErrUnsupported):
		if mode == mq.MetadataPortableEnvelope {
			t.Fatalf("the envelope mode refused a message it exists to carry: %v", err)
		}
		// The refusal the specification asks for. Nothing was published, so nothing is
		// expected on the other side.
		return
	default:
		// Any other refusal has to be a classified one. A driver that
		// failed with CodeUnknown has told the caller nothing they can act
		// on.
		if code := mq.CodeOf(err); code == mq.CodeUnknown {
			t.Fatalf("Publish(%v) failed with no portable code: %v", mode, err)
		}
		return
	}

	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 1 || err != nil {
		t.Fatalf("Receive after an accepted publication under %v: %d, %v", mode, n, err)
	}
	d := &dst[0]
	// Key and Body distinguish nil from empty, and a driver may not turn one
	// into the other.
	if len(d.Key) != len(key) || !bytes.Equal(d.Key, key) {
		t.Errorf("%v: the key came back as %q, want %q", mode, d.Key, key)
	}
	if len(d.Body) != len(body) || !bytes.Equal(d.Body, body) {
		t.Errorf("%v: the body came back as %d bytes, want %d", mode, len(d.Body), len(body))
	}
	if hkey != "" {
		vals := d.Headers.Values(hkey)
		if len(vals) != 1 || !bytes.Equal(vals[0], hval) {
			t.Errorf("%v: header %q came back as %d value(s) %q, want exactly [%q]", mode, hkey, len(vals), vals, hval)
		}
	}
	if mode == mq.MetadataPortableEnvelope && d.ID != id {
		// The envelope carries the identifier; the native mode has nowhere
		// to put it on this broker and says so on its page.
		t.Errorf("%v: the identifier came back as %q, want %q", mode, d.ID, id)
	}
	if err := d.Ack(); err != nil {
		t.Errorf("%v: Ack: %v", mode, err)
	}
}
