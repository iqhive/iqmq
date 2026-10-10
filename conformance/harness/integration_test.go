//go:build integration

package harness

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// pingOrFail pings b and dumps container logs on failure.
func pingOrFail(t *testing.T, b *Broker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()
	if err := Ping(ctx, b); err != nil {
		t.Fatalf("%s at %s: ping: %v\n--- container logs ---\n%s", b.Name, b.Addr, err, b.Logs(ctx))
	}
	t.Logf("%s %s ready at %s %v (external=%v)", b.Name, b.Version, b.Addr, b.Addrs, b.External)
}

func TestHarnessRedpanda(t *testing.T)        { t.Parallel(); pingOrFail(t, Redpanda(t)) }
func TestHarnessKafka(t *testing.T)           { t.Parallel(); pingOrFail(t, Kafka(t)) }
func TestHarnessNATS(t *testing.T)            { t.Parallel(); pingOrFail(t, NATS(t)) }
func TestHarnessNSQ(t *testing.T)             { t.Parallel(); pingOrFail(t, NSQ(t)) }
func TestHarnessPubSub(t *testing.T)          { t.Parallel(); pingOrFail(t, PubSub(t)) }
func TestHarnessElasticMQ(t *testing.T)       { t.Parallel(); pingOrFail(t, ElasticMQ(t)) }
func TestHarnessLocalStack(t *testing.T)      { t.Parallel(); pingOrFail(t, LocalStack(t)) }
func TestHarnessRabbitMQ(t *testing.T)        { t.Parallel(); pingOrFail(t, RabbitMQ(t)) }
func TestHarnessRabbitMQDelayed(t *testing.T) { t.Parallel(); pingOrFail(t, RabbitMQDelayed(t)) }
func TestHarnessArtemis(t *testing.T)         { t.Parallel(); pingOrFail(t, Artemis(t)) }
func TestHarnessActiveMQClassic(t *testing.T) { t.Parallel(); pingOrFail(t, ActiveMQClassic(t)) }
func TestHarnessRedis(t *testing.T)           { t.Parallel(); pingOrFail(t, Redis(t)) }
func TestHarnessValkey(t *testing.T)          { t.Parallel(); pingOrFail(t, Valkey(t)) }
func TestHarnessPulsar(t *testing.T)          { t.Parallel(); pingOrFail(t, Pulsar(t)) }
func TestHarnessPostgres(t *testing.T)        { t.Parallel(); pingOrFail(t, Postgres(t)) }

// TestHarnessToxiproxy routes RESP through Toxiproxy, verifies a latency
// toxic is observable, and that disabling the proxy severs the path.
func TestHarnessToxiproxy(t *testing.T) {
	t.Parallel()
	nw := NewNetwork(t)
	redis := Redis(t, OnNetwork("redis", nw))
	pingOrFail(t, redis)
	tp, toxi := Toxiproxy(t, nw, Proxy{Name: "redis", Upstream: "redis:6379"})
	pingOrFail(t, tp)
	proxied := &Broker{Name: "redis", Addr: tp.Addrs["redis"]}
	pingOrFail(t, proxied)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rm, err := toxi.Latency(ctx, "redis", 700, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := PingRESP(ctx, proxied.Addr); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Fatalf("latency toxic not observed: ping took %v", d)
	}
	if err := rm(ctx); err != nil {
		t.Fatal(err)
	}
	if err := toxi.SetEnabled(ctx, "redis", false); err != nil {
		t.Fatal(err)
	}
	if err := PingRESP(ctx, proxied.Addr); err == nil {
		t.Fatal("ping through disabled proxy should fail")
	}
	if err := toxi.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	pingOrFail(t, proxied)
}

var _ testcontainers.Container = (*testcontainers.DockerContainer)(nil)
