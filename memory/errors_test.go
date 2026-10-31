package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestEngineErrorsCarryTheirRetryability holds mq.IsRetryable and
// mq.IsAmbiguous to the class each engine error is documented with. The
// driver used to pass the engine's errors on bare, and only an *mq.OpError
// carries a judgement of retryability, so mq.IsRetryable answered false for
// all of them: a publication refused because another publication of its dedup
// key was still being admitted, which the engine documents as retryable, read
// as one not worth repeating.
func TestEngineErrorsCarryTheirRetryability(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, what string, err error, code mq.ErrorCode, retryable bool) {
		t.Helper()
		if mq.CodeOf(err) != code {
			t.Fatalf("%s = %v (code %v), want code %v", what, err, mq.CodeOf(err), code)
		}
		if got := mq.IsRetryable(err); got != retryable {
			t.Fatalf("%s: mq.IsRetryable = %v, want %v (%v)", what, got, retryable, err)
		}
		if mq.IsAmbiguous(err) {
			t.Fatalf("%s: mq.IsAmbiguous = true for a refusal that admitted nothing (%v)", what, err)
		}
	}
	consumer := func(t *testing.T, client *mq.Client) *mq.ConsumerHandle {
		t.Helper()
		c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("Consumer: %v", err)
		}
		return c
	}

	t.Run("DedupKeyPending", func(t *testing.T) {
		client, _ := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
			Partitions: 1, CapacityMessages: 1, Overflow: memory.OverflowBlock, DedupWindow: time.Hour,
		}))
		consumer(t, client)
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("filler")}); err != nil {
			t.Fatalf("filling the lane: %v", err)
		}
		// A keyed publication parks on the full lane, holding its key
		// pending until its context ends. It retries while the probe below
		// holds the key, so one of the two always ends up parked on it.
		parkedCtx, stopParked := context.WithCancel(ctx)
		parked := make(chan error, 1)
		go func() {
			for {
				err := client.Publish(parkedCtx, "orders", &mq.Message{Body: []byte("first")}, mq.DedupKey("k"))
				if mq.CodeOf(err) != mq.CodeUnavailable {
					parked <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		var refused error
		for deadline := time.Now().Add(10 * time.Second); refused == nil && time.Now().Before(deadline); {
			probeCtx, stop := context.WithTimeout(ctx, 5*time.Millisecond)
			err := client.Publish(probeCtx, "orders", &mq.Message{Body: []byte("second")}, mq.DedupKey("k"))
			stop()
			if mq.CodeOf(err) == mq.CodeUnavailable {
				refused = err
			}
		}
		stopParked()
		if err := <-parked; mq.CodeOf(err) != mq.CodeCanceled {
			t.Fatalf("the parked publication = %v, want its context's cancellation", err)
		}
		if refused == nil {
			t.Fatal("no publication of the key was refused as pending within 10s")
		}
		check(t, "a publication of a pending dedup key", refused, mq.CodeUnavailable, true)
	})
	t.Run("RejectPublish", func(t *testing.T) {
		client, b := memory.NewForTest(t)
		consumer(t, client)
		b.Faults().RejectPublish(1)
		err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")})
		check(t, "a publication the fault refused", err, mq.CodeUnavailable, true)
	})
	t.Run("QueueFull", func(t *testing.T) {
		client, _ := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
			Partitions: 1, CapacityMessages: 1, Overflow: memory.OverflowReject,
		}))
		consumer(t, client)
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("filling the lane: %v", err)
		}
		err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("y")})
		check(t, "a publication to a full lane", err, mq.CodeResourceExhausted, true)
	})
	// A publication no settlement can ever make room for is refused with
	// ErrQueueFull too, and was mapped as retryable with it: every retry got
	// the same answer, and a caller that trusts mq.IsRetryable, such as an
	// ordered outbox relay, retried it for ever.
	neverFits := []struct {
		name string
		cfg  memory.Config
		subs []string
		body int
	}{
		{
			name: "NeverFitsTheLane",
			cfg:  memory.Config{DefaultTopology: memory.TopologyConfig{CapacityBytes: 8}},
			subs: []string{"billing"},
			body: 64,
		},
		{
			name: "NeverFitsTheLaneUnderReject",
			cfg: memory.Config{DefaultTopology: memory.TopologyConfig{
				CapacityBytes: 8, Overflow: memory.OverflowReject,
			}},
			subs: []string{"billing"},
			body: 64,
		},
		{
			name: "NeverFitsTheEngineLimit",
			cfg:  memory.Config{MaxTotalMessages: 1},
			subs: []string{"billing", "audit"},
			body: 1,
		},
	}
	for _, tc := range neverFits {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := memory.NewForTest(t, memory.TestConfig(tc.cfg))
			for _, s := range tc.subs {
				c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: s})
				if err != nil {
					t.Fatalf("Consumer: %v", err)
				}
				t.Cleanup(func() { _ = c.Close() })
			}
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			err := client.Publish(pctx, "orders", &mq.Message{Body: make([]byte, tc.body)})
			if !errors.Is(err, mq.ErrQueueFull) {
				t.Fatalf("a publication that can never fit = %v, want ErrQueueFull", err)
			}
			check(t, "a publication that can never fit", err, mq.CodeResourceExhausted, false)
		})
	}
	t.Run("FailSettlement", func(t *testing.T) {
		client, b := memory.NewForTest(t)
		c := consumer(t, client)
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		ds := receiveN(t, ctx, c, 1)
		b.Faults().FailSettlement(1)
		check(t, "a settlement the fault refused", ds[0].Ack(), mq.CodeUnavailable, true)
	})
	t.Run("LeaseLost", func(t *testing.T) {
		client, b := memory.NewForTest(t)
		c := consumer(t, client)
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		ds := receiveN(t, ctx, c, 1)
		b.Faults().DelaySettlement(1)
		check(t, "a settlement that lost its lease", ds[0].Ack(), mq.CodeLeaseLost, false)
	})
}
