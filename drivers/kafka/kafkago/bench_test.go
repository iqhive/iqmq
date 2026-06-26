package kafkago_test

import (
	"os"
	"strings"
	"testing"

	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// BenchmarkDriver runs the driver-layer benchmarks against a Kafka cluster
// (the layer-3 benchmark: a driver against a real broker).
//
// It is env-gated for the same reason the conformance test is: a benchmark run
// with no broker would measure nothing, and a `go test -bench .` on a
// developer's machine should not start a cluster. With IQMQ_KAFKA_ADDR set it
// measures that cluster; with IQMQ_KAFKAGO_BENCH set and no address it
// measures an in-process kfake cluster, which is useful for a relative
// comparison and is NOT comparable with a container run.
//
// The comparison against the franz-go driver is a different
// benchmark in a different place — `BenchmarkKafkaClient` in
// benchmarks/kafkaclient — because it has to import BOTH clients and this
// module imports only one.
//
//	BenchmarkDriver/kafka/Publish
//	BenchmarkDriver/kafka/PubSubAck
//	BenchmarkDriver/kafka/PublishBatch/size=N
func BenchmarkDriver(b *testing.B) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		if os.Getenv("IQMQ_KAFKAGO_BENCH") == "" {
			b.Skip("set IQMQ_KAFKA_ADDR to measure a cluster, or IQMQ_KAFKAGO_BENCH=1 to measure an in-process kfake cluster")
		}
		c := cluster(b, 1, nil)
		addr = strings.Join(c.ListenAddrs(), ",")
	}
	cfg := kq.Config{Brokers: strings.Split(addr, ",")}
	testkit.RunBenchmarks(b, kq.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: addr}))
}
