package goredis_test

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// blobBody is large enough that a per-byte copy is the dominant cost and small
// enough to publish a hundred of in a unit test.
const blobBody = 64 << 10

// TestTheTwoClientsDisagreeAboutAliasingAndBothAreRight is the cross-client
// half.
//
// This module exists so that anything that behaves differently between the two
// clients behaves differently BECAUSE of the client library, and this is a case
// where the right answers differ. It is asserted on the two SHIPPED adapters --
// not on a copy of either written for the test -- because the answer is a claim
// about the library each one wraps.
func TestTheTwoClientsDisagreeAboutAliasingAndBothAreRight(t *testing.T) {
	_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	if got := streams.AliasesArgs(goredisCommander(t, srv.Addr())); !got {
		t.Errorf("the go-redis adapter does not accept aliased arguments, so every publication "+
			"still copies its body: go-redis writes and reads in the calling goroutine, so there "+
			"is no path on which a token is read after Do returns (got %v)", got)
	}
	if got := streams.AliasesArgs(rueidisCommander(t, srv.Addr())); got {
		t.Errorf("the rueidis adapter accepts aliased arguments, and it must not: `pipe.Do` queues "+
			"a command for a background writer and returns `ctx.Err()` on a cancelled context "+
			"while the command is still queued, so `_backgroundWrite` would write the caller's "+
			"LATER bytes and store a corrupt entry (got %v)", got)
	}
}

// TestPublishReadsTheBodyBeforeItReturns is the message contract on the arm
// that aliases, end to end over a real go-redis client and a real RESP link.
//
// The check is the contract verbatim: the caller reuses its buffer the instant
// Publish returns, and the entry the server stored still holds what was
// published. Nothing else in this repository can ask it of the aliasing path,
// because the answer depends on when go-redis reads the argument — which is
// why this test goes over the wire rather than over a fake commander.
func TestPublishReadsTheBodyBeforeItReturns(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	body := bytes.Repeat([]byte{'a'}, blobBody)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: body}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	scribble(body, 'z')
	got := fetchOne(t, r.ctx, src)
	if n := bytes.Count(got.Message.Body, []byte{'a'}); n != blobBody {
		t.Errorf("the stored entry holds %d of the %d published bytes: the body was still being "+
			"written after Publish returned, and the specification gave the buffer back to the caller",
			n, blobBody)
	}
}

// TestPublishBatchReadsEveryBodyBeforeItReturns is the same contract for the
// path the batched budget is about.
//
// A batch builds every command BEFORE it sends any of them, so each message's
// blob has to stay valid for the whole pipeline rather than for one round
// trip. That is the case a per-message argument would get wrong and a batch
// test is the only thing that asks it: each message carries its own buffer,
// every buffer is overwritten the instant PublishBatch returns, and all of
// them must have reached the server intact.
func TestPublishBatchReadsEveryBodyBeforeItReturns(t *testing.T) {
	const n = 16
	r := newRig(t)
	sink, src := r.openPair(t)
	batch, ok := sink.(mq.BatchSink)
	if !ok {
		t.Fatal("the sink is not a BatchSink, so the batched path this budget is about is unreachable")
	}
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Topic: testTopic, Body: bytes.Repeat([]byte{byte('A' + i)}, blobBody)}
	}
	if err := batch.PublishBatch(r.ctx, ms, nil); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for i := range ms {
		scribble(ms[i].Body, 'z')
	}
	dst := make([]mq.DriverDelivery, n)
	read := 0
	for read < n {
		got, err := src.Fetch(r.ctx, dst[read:])
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if got == 0 {
			t.Fatalf("Fetch returned nothing with %d of %d deliveries still to come", n-read, n)
		}
		read += got
	}
	for i := range dst {
		want := byte('A' + i)
		if c := bytes.Count(dst[i].Message.Body, []byte{want}); c != blobBody {
			t.Errorf("delivery %d holds %d of the %d bytes published as %q: a batched body was read "+
				"after PublishBatch returned", i, c, blobBody, want)
		}
	}
}

// TestPublishReadsTheBodyBeforeItReturnsUnderTheEnvelope is the same contract
// for the metadata mode that rebuilds the body.
//
// Under MetadataPortableEnvelope the aliased bytes are the ENVELOPE's, not the
// caller's — `driverkit.Envelope.Wrap` allocates a body for the call — so the
// alias is safe for a different reason and the round trip has to prove the
// envelope still decodes. A driver that aliased a scratch buffer the envelope
// reused would pass every test above and corrupt this one.
func TestPublishReadsTheBodyBeforeItReturnsUnderTheEnvelope(t *testing.T) {
	const mode = mq.MetadataPortableEnvelope
	r := newRig(t)
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic:        testTopic,
		Subscription: testSub,
		Prefetch:     4,
		AckDeadline:  testLease,
		Start:        mq.StartEarliest(),
		Metadata:     mode,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Metadata: mode})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	body := bytes.Repeat([]byte{'a'}, blobBody)
	m := &mq.Message{Topic: testTopic, Body: body}
	m.Headers.Set("content-kind", []byte("order"))
	if err := sink.Publish(r.ctx, m, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	scribble(body, 'z')
	got := fetchOne(t, r.ctx, src)
	if n := bytes.Count(got.Message.Body, []byte{'a'}); n != blobBody {
		t.Errorf("the enveloped entry holds %d of the %d published bytes", n, blobBody)
	}
	if v, ok := got.Message.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("the enveloped entry lost its header: got %q, %v", v, ok)
	}
}

// scribble overwrites b, which is what the specification says a caller may do the instant
// a publish call returns.
func scribble(b []byte, with byte) {
	for i := range b {
		b[i] = with
	}
}

var _ = goredis.ClientModule
