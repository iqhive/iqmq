//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/testkit"
)

// The chaos runs layer-3: the scenarios of scenario.go against a real
// broker whose traffic goes through Toxiproxy.
//
// ONE broker and ONE Toxiproxy at a time. `make chaos DRIVER=<suite>` selects
// one target, and the two targets here never run together: this host carries
// about fifty other people's containers and starting several brokers at once
// is how a chaos run becomes a measurement of the host.
//
// EXTENDED=1 adds the two weekly scenarios (the specification's "nightly subset / weekly
// extended"): a container stop-start and a topology deletion, which between
// them cost about a minute of broker time.

// chaosLease is the lease every chaos topology is created with AND the value
// the drain is told, in one place so the two cannot drift. It is short, so
// that a redelivery after an ambiguous settlement arrives inside the drain.
const chaosLease = 10 * time.Second

// toxiInjector is the real [Injector]: Toxiproxy for the network faults, the
// container for the restart, and the portable Admin surface for the topology
// deletion.
type toxiInjector struct {
	t     *testing.T
	toxi  *harness.Toxi
	proxy string
	// broker is the container the restart scenario stops. Removing it is
	// the harness's own cleanup; nothing here removes a container.
	broker *harness.Broker
	// admin deletes the topology, and topic is what it deletes.
	admin mq.Admin
	topic string
}

func (x *toxiInjector) Latency(ctx context.Context, ms, jitter int) (func(context.Context) error, error) {
	return x.toxi.Latency(ctx, x.proxy, ms, jitter)
}

func (x *toxiInjector) Reset(ctx context.Context, ms int) (func(context.Context) error, error) {
	return x.toxi.ResetPeer(ctx, x.proxy, ms)
}

func (x *toxiInjector) Bandwidth(ctx context.Context, kbps int) (func(context.Context) error, error) {
	name, err := x.toxi.AddToxic(ctx, x.proxy, harness.Toxic{
		Type: "bandwidth", Stream: "upstream", Toxicity: 1,
		Attributes: map[string]any{"rate": kbps},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return x.toxi.RemoveToxic(ctx, x.proxy, name) }, nil
}

func (x *toxiInjector) Disable(ctx context.Context) (func(context.Context) error, error) {
	if err := x.toxi.SetEnabled(ctx, x.proxy, false); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return x.toxi.SetEnabled(ctx, x.proxy, true) }, nil
}

func (x *toxiInjector) StopBroker(ctx context.Context) (func(context.Context) error, error) {
	ctr := x.broker.Container()
	if ctr == nil {
		return nil, errors.New("the broker is external, so this run cannot stop it")
	}
	d := 5 * time.Second
	if err := ctr.Stop(ctx, &d); err != nil {
		return nil, fmt.Errorf("stopping the broker container: %w", err)
	}
	return func(ctx context.Context) error {
		if err := ctr.Start(ctx); err != nil {
			return fmt.Errorf("restarting the broker container: %w", err)
		}
		return nil
	}, nil
}

func (x *toxiInjector) DeleteTopology(ctx context.Context) (func(context.Context) error, error) {
	if x.admin == nil {
		return nil, errors.New("this driver has no portable admin surface, so the topology cannot be deleted")
	}
	if err := x.admin.DeleteTopic(ctx, x.topic); err != nil {
		return nil, fmt.Errorf("deleting %s: %w", x.topic, err)
	}
	return func(ctx context.Context) error {
		return x.admin.EnsureTopic(ctx, mq.TopicSpec{Name: x.topic})
	}, nil
}

// chaosArm is what one driver contributes: a harness that opens a client
// through the PROXIED endpoint, and the injector's broker and proxy.
type chaosArm struct {
	name    string
	harness testkit.Harness
	broker  *harness.Broker
	toxi    *harness.Toxi
	proxy   string
}

// runChaos is the body of every TestChaos_<suite>.
func runChaos(t *testing.T, arm chaosArm) {
	extended := extendedSet()
	scenarios := Select(extended)
	t.Logf("chaos %s: %d scenario(s), extended=%v", arm.name, len(scenarios), extended)
	for _, s := range scenarios {
		t.Run(s.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			// Every scenario gets its own client and its own topic. A
			// scenario that left a proxy disabled or a container stopped
			// must not be the reason the next one fails, and the
			// deferred Reset below is what guarantees it.
			topic := arm.harness.TopicName("chaos-" + s.Name)
			client, err := arm.harness.Open(ctx, testkit.Need{
				Topic:         topic,
				Subscriptions: []string{"chaos"},
				Partitions:    1,
				AckDeadline:   chaosLease,
			})
			if err != nil {
				t.Fatalf("opening a client: %v", err)
			}
			defer func() { _ = client.Close() }()
			admin, _ := client.Admin()

			defer func() {
				rctx, rcancel := context.WithTimeout(context.Background(), time.Minute)
				defer rcancel()
				if err := arm.toxi.Reset(rctx); err != nil {
					t.Errorf("resetting toxiproxy after %s: %v", s.Name, err)
				}
			}()

			run, err := RunScenario(ctx, s, Env{
				Client:         client,
				Topic:          topic,
				Subscription:   "chaos",
				Injector:       &toxiInjector{t: t, toxi: arm.toxi, proxy: arm.proxy, broker: arm.broker, admin: admin, topic: topic},
				SettleTimeout:  1500 * time.Millisecond,
				PublishTimeout: 5 * time.Second,
				ReceiveTimeout: 3 * time.Second,
				// The SAME lease the topology was created with. The two
				// were 30 s and a defaulted 5 s, so the drain waited ten
				// seconds for entries that were leased for thirty.
				AckDeadline: chaosLease,
				Log:         t.Logf,
			})
			if err != nil {
				t.Fatalf("running %s: %v", s.Name, err)
			}
			if err := s.Verdict(run); err != nil {
				t.Fatalf("%v\n  %s", err, run)
			}
			t.Logf("passed: %s", run)
		})
	}
}

// extendedSet reads EXTENDED, which `make chaos EXTENDED=1` passes.
func extendedSet() bool {
	v := envOr("EXTENDED", "")
	return v != "" && v != "0" && v != "false"
}

// TestChaos_jetstream runs the L3 scenarios against NATS with JetStream, with
// every client connection routed through Toxiproxy.
func TestChaos_jetstream(t *testing.T) {
	nw := harness.NewNetwork(t)
	b := harness.NATS(t, harness.OnNetwork("nats", nw))
	tp, toxi := harness.Toxiproxy(t, nw, harness.Proxy{Name: "nats", Upstream: "nats:4222"})
	_ = tp
	addr := tp.Addrs["nats"]
	t.Logf("jetstream %s at %s, proxied at %s", b.Version, b.Addr, addr)
	runChaos(t, chaosArm{
		name: "jetstream",
		harness: jq.ConformanceHarness(
			jq.Config{
				Addrs: []string{"nats://" + addr},
				// DoubleAck makes the ack a REQUEST rather than a
				// publish, so it waits for the server to record it.
				// Without it jetstream's Ack returns as soon as the
				// frame is in the write buffer, and a latency toxic
				// therefore cannot make a settlement ambiguous — the
				// latency-over-timeout-during-ack scenario reported
				// "no settlement returned ErrSettlementUnknown ...
				// so the ambiguous-acknowledgement path was never
				// taken" against a live 4-second toxic, because there
				// was no round trip for the toxic to straddle.
				DoubleAck: true,
			},
			testkit.EvidenceContainer,
			testkit.Broker{Image: b.Image, Version: b.Version},
		),
		broker: b, toxi: toxi, proxy: "nats",
	})
}

// TestChaos_redis runs the L3 scenarios against Redis Streams through
// Toxiproxy.
func TestChaos_redis(t *testing.T) {
	nw := harness.NewNetwork(t)
	b := harness.Redis(t, harness.OnNetwork("redis", nw))
	tp, toxi := harness.Toxiproxy(t, nw, harness.Proxy{Name: "redis", Upstream: "redis:6379"})
	addr := tp.Addrs["redis"]
	t.Logf("redis %s at %s, proxied at %s", b.Version, b.Addr, addr)
	runChaos(t, chaosArm{
		name: "redis",
		harness: rd.ConformanceHarness(
			rd.Config{Addrs: []string{addr}},
			testkit.EvidenceContainer,
			testkit.Broker{Image: b.Image, Version: b.Version},
		),
		broker: b, toxi: toxi, proxy: "redis",
	})
}

var _ testcontainers.Container = (*testcontainers.DockerContainer)(nil)
