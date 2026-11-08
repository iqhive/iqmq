package goredis_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// TestARefusedDialIsADefiniteFailure is drivers/redis's test of the same name
// over go-redis: once the server has gone, go-redis answers a command with
// the error of the pooled connection it could not dial. Nothing was written,
// so a publication is a definite failure a caller may repeat, and an
// acknowledgement leaves the delivery settleable, where both used to come
// back as possibly applied.
//
// The classification is drivers/redis's, which reads a dial that connected
// nowhere as a command that never left after v1.1.2, so the test skips
// against v1.1.2 and runs from the release whose go.mod requires the
// drivers/redis release that has it.
func TestARefusedDialIsADefiniteFailure(t *testing.T) {
	if v, old := driversRedisBefore(t, "v1.1.3"); old {
		t.Skipf("this build links drivers/redis %s, which reads a refused dial as possibly applied", v)
	}
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("stored")}, nil); err != nil {
		t.Fatalf("Publish before the server went: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	if err := r.srv.Close(); err != nil { // the listener is gone, so every dial is refused
		t.Fatalf("respsrv.Close: %v", err)
	}
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	untilDialRefused := func(what string, call func(context.Context) error) error {
		t.Helper()
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(r.ctx, time.Second)
			err := call(ctx)
			cancel()
			if oe, ok := errors.AsType[*net.OpError](err); ok && oe.Op == "dial" {
				return err
			}
			if err != nil && !mq.IsAmbiguous(err) {
				t.Errorf("%s that failed on the dying connection is %v, which reads as definite; it may have been written", what, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("go-redis never answered %s with its refused dial", what)
		return nil
	}

	perr := untilDialRefused("a publication", func(ctx context.Context) error {
		return sink.Publish(ctx, &mq.Message{Topic: testTopic, Body: []byte("never written")}, nil)
	})
	if n := len(r.fake.Entries(keys.Stream(0))); n != 1 {
		t.Fatalf("the stream holds %d entries, want only the one stored before the server went", n)
	}
	if mq.IsAmbiguous(perr) || !mq.IsRetryable(perr) {
		t.Errorf("a publication whose dial the server refused is %v: ambiguous %v, retryable %v; "+
			"nothing was written, so it is a definite failure a caller may repeat",
			perr, mq.IsAmbiguous(perr), mq.IsRetryable(perr))
	}

	aerr := untilDialRefused("an acknowledgement", func(ctx context.Context) error {
		return src.Settler().Ack(ctx, &d)
	})
	if pending := len(r.fake.Pending(keys.Stream(0), testSub)); pending != 1 {
		t.Fatalf("%d entries are pending, want the one the refused acknowledgement left", pending)
	}
	if mq.IsAmbiguous(aerr) || errors.Is(aerr, mq.ErrSettlementUnknown) {
		t.Errorf("an acknowledgement whose dial the server refused is %v: ambiguous %v; "+
			"its outcome is known, and an unknown one is kept for good", aerr, mq.IsAmbiguous(aerr))
	}
}
