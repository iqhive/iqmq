package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestEntryPointsHonourTheirContextAtChannelOpen is every caller-facing entry
// point that opens a channel of its own, under a broker slow to answer
// `channel.open`.
//
// The library's `Channel()` is synchronous with no deadline, which is why the
// handles' attaches and repairs open theirs with a bound. Ping, every Admin
// call, a consumer's open and the delay-plugin probe opened theirs with no
// bound, so each of them waited for the broker past its caller's context: a
// liveness probe with a 200 ms deadline did not return, and then reported the
// connection alive once the open was answered.
func TestEntryPointsHonourTheirContextAtChannelOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(ctx context.Context, cl *mq.Client, b *rq.Backend) error
	}{
		{"Ping", func(ctx context.Context, _ *mq.Client, b *rq.Backend) error { return b.Ping(ctx) }},
		{"Admin.Stats", func(ctx context.Context, cl *mq.Client, _ *rq.Backend) error {
			a, err := cl.Admin()
			if err != nil {
				return err
			}
			_, err = a.Stats(ctx, "orders", "workers")
			return err
		}},
		{"Consumer open", func(ctx context.Context, cl *mq.Client, _ *rq.Backend) error {
			c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "other"})
			if err == nil {
				_ = c.Close()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := rabbitfake.New(rabbitfake.Config{})
			t.Cleanup(func() { _ = broker.Close() })
			dialer := newChannelGateDialer(t, broker.Dialer())
			cfg := base()
			cfg.Declare = true
			cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
			if err != nil {
				t.Fatalf("opening a client over the fake broker: %v", err)
			}
			closeBounded(t, cl)
			consumeOn(t, cl, "orders", "workers")
			b := backendOf(t, cl)

			dialer.arm()
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- tc.call(ctx, cl, b) }()
			dialer.waitReached(t)
			select {
			case err := <-done:
				t.Logf("%s returned %v after %v", tc.name, err, time.Since(start))
				if err == nil {
					t.Errorf("%s succeeded although its channel was never opened", tc.name)
				}
			case <-time.After(time.Second):
				dialer.release()
				err := <-done
				t.Fatalf("%s with a 200ms context was still waiting 1s later, and returned %v only once the "+
					"channel open was answered: its channel open has no bound and does not select on ctx", tc.name, err)
			}
		})
	}
}

// TestTheDelayPluginProbeHonoursItsContextAtChannelOpen is the probe Open runs
// with Config.DetectDelayPlugin, which opened its channel with no bound as
// well: an Open whose broker was slow to answer `channel.open` waited past its
// context.
func TestTheDelayPluginProbeHonoursItsContextAtChannelOpen(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{DelayedMessagePlugin: true})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	defer dialer.release()
	cfg := base()
	cfg.Declare, cfg.DetectDelayPlugin = true, true
	dialer.arm()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		cl, err := rq.OpenWithDialer(ctx, dialer, cfg)
		if err == nil {
			_ = cl.Close()
		}
		done <- err
	}()
	dialer.waitReached(t)
	select {
	case err := <-done:
		t.Logf("Open returned %v while the probe's channel open was parked", err)
		if err == nil {
			t.Error("Open succeeded although the probe's channel was never opened")
		}
	case <-time.After(1500 * time.Millisecond):
		dialer.release()
		err := <-done
		t.Fatalf("Open with a 300ms context was still waiting 1.5s later, and returned %v once the probe's "+
			"channel open was answered", err)
	}
}
