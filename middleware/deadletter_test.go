package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"
)

// These tests are the dead-letter copy of a message that Encrypt or Compress
// opened for the handler. The runner dead-letters the message it received,
// and that only works when the middleware REPLACES what it rewrites: both used
// to delete their marker header from the received header storage in place,
// which on the memory driver is the broker's own record, so the copy lost the
// marker and gained an empty pair where it had been, and a redelivery of the
// record arrived without it.

// deadLettered runs chain over the one message m with a handler that always
// fails and a dead-letter topic, and returns the copy that reached it.
func deadLettered(t *testing.T, chain mq.Middleware, m *mq.Message) *mq.Message {
	t.Helper()
	h := newHarness(t, memory.TestTopics("orders.dlq"))
	ctx := context.Background()
	dlq, err := h.cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	t.Cleanup(func() { _ = dlq.Close() })
	if err := h.cl.Publish(ctx, h.topic, m); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	r := h.run(chain(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return errBoom })),
		mq.WithMaxAttempts(1), mq.WithDeadLetter("orders.dlq"))
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	got, err := memory.Drain(drainCtx, dlq, 1)
	if err != nil {
		t.Fatalf("nothing reached the dead-letter topic: %v", err)
	}
	if err := r.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	return got[0]
}

// noEmptyKeys fails the test when a header set holds a pair with no key,
// which is what an in-place delete leaves behind in a copy of the set.
func noEmptyKeys(t *testing.T, h *mq.Headers) {
	t.Helper()
	for k := range h.All() {
		if k == "" {
			t.Fatal("the dead letter carries a header with no key")
		}
	}
}

func TestEncryptDeadLettersTheSealedMessage(t *testing.T) {
	k := testKey(t)
	rec := &publishRecorder{}
	p := mustWrapPublisher(t, rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	m := &mq.Message{Topic: "orders", Body: []byte("card=4111111111111111")}
	m.Headers.Set("trace", []byte("t-1"))
	if err := p.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	sealed := rec.sent[0]
	dl := deadLettered(t, middleware.Encrypt(middleware.EncryptConfig{Keys: k, RequireSealed: true}), sealed)
	if !bytes.Equal(dl.Body, sealed.Body) {
		t.Fatalf("the dead letter's body is %q, want the sealed body that was published", dl.Body)
	}
	if id, ok := dl.Headers.Get(middleware.HeaderKeyID); !ok || string(id) != "k1" {
		t.Fatalf("the dead letter's key id = %q (present=%v), want k1: without it nothing can open the copy", id, ok)
	}
	if v, ok := dl.Headers.Get("trace"); !ok || string(v) != "t-1" {
		t.Fatalf("the dead letter's trace header = %q (present=%v), want the publisher's", v, ok)
	}
	noEmptyKeys(t, &dl.Headers)
	var opened []byte
	h := middleware.Encrypt(middleware.EncryptConfig{Keys: k, RequireSealed: true})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			opened = bytes.Clone(d.Body)
			return nil
		}),
	)
	d := delivery("")
	d.Body, d.Headers = dl.Body, dl.Headers
	if err := h.Handle(context.Background(), d); err != nil || string(opened) != "card=4111111111111111" {
		t.Fatalf("opening the dead letter = %v, %q; want the plaintext", err, opened)
	}
}

func TestCompressDeadLettersTheCompressedMessage(t *testing.T) {
	compressed := gzipOf(t, bytes.Repeat([]byte("the quick brown fox "), 64))
	m := &mq.Message{Body: compressed}
	m.Headers.Set(middleware.HeaderContentEncoding, []byte(middleware.ContentEncodingGzip))
	m.Headers.Set("trace", []byte("t-1"))
	dl := deadLettered(t, mustCompress(t, middleware.CompressConfig{}), m)
	if !bytes.Equal(dl.Body, compressed) {
		t.Fatalf("the dead letter's body is %d bytes, want the %d compressed bytes that were published", len(dl.Body), len(compressed))
	}
	if v, ok := dl.Headers.Get(middleware.HeaderContentEncoding); !ok || string(v) != middleware.ContentEncodingGzip {
		t.Fatalf("the dead letter's content-encoding = %q (present=%v), want gzip: without it the body reads as garbage", v, ok)
	}
	if v, ok := dl.Headers.Get("trace"); !ok || string(v) != "t-1" {
		t.Fatalf("the dead letter's trace header = %q (present=%v), want the publisher's", v, ok)
	}
	noEmptyKeys(t, &dl.Headers)
}

func TestEncryptAndCompressLeaveTheReceivedHeadersAlone(t *testing.T) {
	// The received header set is borrowed storage: on the memory driver it
	// is the broker's record, which every redelivery and every other
	// subscription reads. Opening a message replaces the delivery's headers
	// rather than deleting from them.
	k := testKey(t)
	rec := &publishRecorder{}
	p := mustWrapPublisher(t, rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	m := &mq.Message{Topic: "orders", Body: []byte("payload")}
	m.Headers.Set("trace", []byte("t-1"))
	if err := p.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	var gzipped mq.Headers
	gzipped.Set(middleware.HeaderContentEncoding, []byte(middleware.ContentEncodingGzip))
	gzipped.Set("trace", []byte("t-1"))
	for _, c := range []struct {
		name    string
		mw      mq.Middleware
		body    []byte
		headers mq.Headers
	}{
		{"Encrypt", middleware.Encrypt(middleware.EncryptConfig{Keys: k}), rec.sent[0].Body, rec.sent[0].Headers},
		{"Compress", mustCompress(t, middleware.CompressConfig{}), gzipOf(t, []byte("payload")), gzipped},
	} {
		t.Run(c.name, func(t *testing.T) {
			received := c.headers.Clone()
			want := pairsOf(&received)
			d := delivery("")
			d.Body = c.body
			d.Headers = received
			h := c.mw(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
			if err := h.Handle(context.Background(), d); err != nil {
				t.Fatalf("Handle = %v, want nil", err)
			}
			if got := pairsOf(&received); !slices.Equal(got, want) {
				t.Fatalf("the received headers are %q after the open, want %q", got, want)
			}
		})
	}
}

// pairsOf renders a header set as key=value strings, in order.
func pairsOf(h *mq.Headers) []string {
	var out []string
	for k, v := range h.All() {
		out = append(out, k+"="+string(v))
	}
	return out
}
