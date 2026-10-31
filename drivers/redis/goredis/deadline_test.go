package goredis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
)

// TestPublishReturnsAtItsDeadline is a caller's timeout on the publish path.
//
// go-redis applies a context's deadline to the socket only when the client has
// ContextTimeoutEnabled, and the client this driver dialled did not, so a
// Publish whose deadline had passed waited for the reply up to the read
// timeout (Block plus DialTimeout, ten seconds by default) and then reported
// success. The deadline now bounds the read, and a publication whose reply
// did not arrive in time is reported as ambiguous.
func TestPublishReturnsAtItsDeadline(t *testing.T) {
	slow, addr := serveSlowFirstXAdd(t, 1500*time.Millisecond)
	cfg := baseConfig(slow.Server)
	cfg.Block = 0 // the default, so the read timeout is the driver's own
	cfg.Addrs = []string{addr}
	b, err := goredis.OpenBackend(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	perr := sink.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
	took := time.Since(start)
	t.Logf("Publish = %v after %v (deadline 100ms)", perr, took)
	if took > 500*time.Millisecond {
		t.Errorf("a Publish with a 100ms deadline returned after %v", took)
	}
	if perr == nil || !mq.IsAmbiguous(perr) {
		t.Errorf("a Publish whose reply missed its deadline returned %v, want an ambiguous failure", perr)
	}
	<-slow.landed
}

// TestFetchReturnsAtItsDeadline is the same bound on the consume path: a Fetch
// parked in XREADGROUP BLOCK returns at its context's deadline rather than
// when the block ends.
func TestFetchReturnsAtItsDeadline(t *testing.T) {
	const block = 2 * time.Second
	r := newRig(t, func(c *rd.Config) { c.Block = block })
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 4,
		AckDeadline: testLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	n, ferr := src.Fetch(ctx, make([]mq.DriverDelivery, 4))
	took := time.Since(start)
	t.Logf("Fetch = %d, %v after %v (deadline 100ms, block %v)", n, ferr, took, block)
	if took > 500*time.Millisecond {
		t.Errorf("a Fetch with a 100ms deadline returned after %v: it waited out the %v block", took, block)
	}
	if !errors.Is(ferr, context.DeadlineExceeded) {
		t.Errorf("Fetch = %v, want the context's deadline", ferr)
	}
}
