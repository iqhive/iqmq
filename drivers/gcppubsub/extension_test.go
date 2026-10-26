package gcppubsub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// recordingAPI is psfake with the subscription settings the driver asks the
// client library for kept, so a test can hold them against what the driver
// reports.
type recordingAPI struct {
	*psfake.Server
	mu   sync.Mutex
	subs []psapi.SubscriptionConfig
}

func (r *recordingAPI) Subscription(cfg psapi.SubscriptionConfig) (psapi.Subscription, error) {
	r.mu.Lock()
	r.subs = append(r.subs, cfg)
	r.mu.Unlock()
	return r.Server.Subscription(cfg)
}

func (r *recordingAPI) maxExtension(tb testing.TB) time.Duration {
	tb.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.subs) != 1 {
		tb.Fatalf("the driver opened %d subscription handles, want 1", len(r.subs))
	}
	return r.subs[0].MaxExtension
}

// TestTheLibraryExtendsForTheLeaseTheDriverReports: the client library is what
// keeps a held message from another consumer, so the lease a delivery reports
// must not outlast the extension the library was told to perform. A consumer
// asking for a two-minute deadline under a ten-second MaxExtension was told it
// held its message for two minutes, while the library let go at ten seconds
// and the service redelivered it elsewhere.
func TestTheLibraryExtendsForTheLeaseTheDriverReports(t *testing.T) {
	for name, tc := range map[string]struct {
		deadline, maxExtension, want time.Duration
	}{
		"the deadline is longer":  {deadline: 2 * time.Minute, maxExtension: 10 * time.Second, want: 2 * time.Minute},
		"the extension is longer": {deadline: 10 * time.Second, maxExtension: 5 * time.Minute, want: 5 * time.Minute},
		"past the ceiling":        {deadline: 2 * time.Hour, maxExtension: 10 * time.Second, want: gps.MaxExtensionCeiling},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true, MaxExtension: tc.maxExtension}
			api := &recordingAPI{Server: psfake.New(cfg.ProjectID)}
			b, err := gps.BackendWithAPI(context.Background(), api, driverkit.ClientOwned, cfg)
			if err != nil {
				t.Fatalf("BackendWithAPI: %v", err)
			}
			c := gps.ClientFor(b, cfg)
			t.Cleanup(func() { _ = c.Close() })
			topic := newTopic("extension")
			provision(t, c, topic, "s")
			ctx := context.Background()
			if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s", AckDeadline: tc.deadline})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			d := receiveOne(t, cons, 5*time.Second)
			info := d.Info()
			if lease := info.LeaseExpires.Sub(info.ReceivedAt); lease != tc.want {
				t.Errorf("the delivery reports a %v lease, want %v", lease, tc.want)
			}
			if ext := api.maxExtension(t); ext != tc.want {
				t.Errorf("the client library was told to extend for %v, want %v: the lease the delivery "+
					"reports is not one the library keeps", ext, tc.want)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}
