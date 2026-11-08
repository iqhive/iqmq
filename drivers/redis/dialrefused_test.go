package redis_test

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// TestARefusedDialIsADefiniteFailure: once the server has gone, rueidis
// answers a command with the error of the dial it tried and the server
// refused. Nothing was written, as the stream and the pending list show, so
// a publication is a definite failure a caller may repeat, and a settlement
// is one the delivery stays settleable after.
//
// The driver mapped every failure with sentAlready true, and
// driverkit.ClassifyNet calls a dial error possibly applied under it, so
// every publication while the server was down came back ambiguous and not
// retryable, and every acknowledgement went to an unknown outcome, which the
// core keeps for good. The driver now reads a dial that connected nowhere as
// a command that never left (written), which needs nothing new from the root
// module.
func TestARefusedDialIsADefiniteFailure(t *testing.T) {
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{srv.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	cmder, err := rd.NewCommander(client)
	if err != nil {
		client.Close()
		t.Fatalf("NewCommander: %v", err)
	}
	b, err := rd.BackendWithCommander(t.Context(), cmder, driverkit.ClientOwned, rd.Config{AutoCreate: true})
	if err != nil {
		_ = cmder.Close()
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 4, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Topic: testTopic, Body: []byte("stored")}, nil); err != nil {
		t.Fatalf("Publish before the server went: %v", err)
	}
	fctx, fcancel := context.WithTimeout(t.Context(), 5*time.Second)
	dst := make([]mq.DriverDelivery, 4)
	got, err := src.Fetch(fctx, dst)
	fcancel()
	if err != nil || got != 1 {
		t.Fatalf("Fetch before the server went = %d, %v", got, err)
	}
	_ = srv.Close() // the listener is gone, so every dial is refused

	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	// The command in flight when the connection died fails with that
	// connection's error, which is rightly ambiguous; the ones after it are
	// answered with the redial's.
	untilDialRefused := func(what string, call func(context.Context) error) error {
		t.Helper()
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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
		t.Fatalf("rueidis never answered %s with its refused dial", what)
		return nil
	}

	perr := untilDialRefused("a publication", func(ctx context.Context) error {
		return sink.Publish(ctx, &mq.Message{Topic: testTopic, Body: []byte("never written")}, nil)
	})
	v, err := fake.Do(t.Context(), streams.XLen(keys.Stream(0)))
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	if n, _ := v.Int64(); n != 1 {
		t.Fatalf("the stream holds %d entries, want only the one stored before the server went", n)
	}
	if mq.IsAmbiguous(perr) || !mq.IsRetryable(perr) {
		t.Errorf("a publication whose dial the server refused is %v: ambiguous %v, retryable %v; "+
			"nothing was written, so it is a definite failure a caller may repeat",
			perr, mq.IsAmbiguous(perr), mq.IsRetryable(perr))
	}

	aerr := untilDialRefused("an acknowledgement", func(ctx context.Context) error {
		return src.Settler().Ack(ctx, &dst[0])
	})
	if pending := len(fake.Pending(keys.Stream(0), testSub)); pending != 1 {
		t.Fatalf("%d entries are pending, want the one the refused acknowledgement left", pending)
	}
	if mq.IsAmbiguous(aerr) || errors.Is(aerr, mq.ErrSettlementUnknown) {
		t.Errorf("an acknowledgement whose dial the server refused is %v: ambiguous %v; "+
			"its outcome is known, and an unknown one is kept for good", aerr, mq.IsAmbiguous(aerr))
	}
}

// refusedDial is the error net.Dialer gives a dial the server refused.
var refusedDial = &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

// dlqTopic is a dead-letter topic on another hash slot than testTopic's, so a
// dead-letter nack is the two-command move rather than the script.
const dlqTopic = "acme.orders-dlq"

// dlqEntries is how many entries the dead-letter topic holds.
func dlqEntries(t *testing.T, r *rig) int {
	t.Helper()
	keys, err := streams.NewKeys(dlqTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	return len(r.srv.Entries(keys.Stream(0)))
}

// TestADeadLetterMoveWhoseCopyWasRefusedADialIsDefinite: a cross-slot
// dead-letter move whose XADD's dial the server refused moved nothing, so the
// Reject is a definite failure the caller may repeat, and the repeat moves
// the message once.
func TestADeadLetterMoveWhoseCopyWasRefusedADialIsDefinite(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.DeadLetterTopic = dlqTopic })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("poison")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	r.srv.Fail("XADD", refusedDial)
	err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if n := dlqEntries(t, r); n != 0 {
		t.Fatalf("setup: the dead-letter topic holds %d entries after a refused XADD", n)
	}
	if mq.IsAmbiguous(err) || !mq.IsRetryable(err) {
		t.Fatalf("Nack = %v: ambiguous %v, retryable %v; nothing moved, so it is a definite failure a caller may repeat",
			err, mq.IsAmbiguous(err), mq.IsRetryable(err))
	}
	if err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("the repeated Reject: %v", err)
	}
	if n := dlqEntries(t, r); n != 1 {
		t.Fatalf("the dead-letter topic holds %d entries after the repeat, want 1", n)
	}
}

// TestADeadLetterMoveWhoseAckWasRefusedADialIsUnknown is the other step: the
// copy is in the dead-letter topic and the acknowledgement's dial was then
// refused, so the entry is still pending. The move is half done and a
// repeated Reject would write a second copy, so its outcome is unknown,
// although the dial that failed wrote nothing.
//
// It is internal/streams' consumer that says so, which it does after v1.1.2,
// so the test skips against v1.1.2 and runs from the release whose go.mod
// requires the root release that has it. Against v1.1.2 the driver reads the
// refused dial as a definite failure, which is why this driver's release has
// to require that root release.
func TestADeadLetterMoveWhoseAckWasRefusedADialIsUnknown(t *testing.T) {
	if v, old := rd.RootModuleBeforeForTest(t, "v1.1.3"); old {
		t.Skipf("this build uses root module %s, whose streams consumer returns the acknowledgement's error as it came", v)
	}
	r := newRig(t, func(c *rd.Config) { c.DeadLetterTopic = dlqTopic })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("poison")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	r.srv.Fail("EVALSHA", refusedDial)
	err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	keys, kerr := streams.NewKeys(testTopic, 1)
	if kerr != nil {
		t.Fatal(kerr)
	}
	if n, pending := dlqEntries(t, r), len(r.srv.Pending(keys.Stream(0), testSub)); n != 1 || pending != 1 {
		t.Fatalf("setup: %d dead-letter entries and %d pending, want the copy and the entry the failed acknowledgement left", n, pending)
	}
	if !mq.IsAmbiguous(err) || !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("Nack = %v (ambiguous %v): the copy is in the dead-letter topic, so a repeated Reject writes a second one",
			err, mq.IsAmbiguous(err))
	}
}
