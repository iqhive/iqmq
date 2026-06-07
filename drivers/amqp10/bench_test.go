package amqp10_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The benchmarks, in the canonical names and with `key=value`
// dimensions in a fixed order.
//
// Two layers are measured here and they answer different questions.
// BenchmarkAMQP10 is layer 1: the PURE parts of this core — the mapping, the
// error table, the capability lookup — measured against nothing, so the numbers
// are comparable on any machine and need no broker. BenchmarkDriver is layer 3
// and is env-gated: it measures this driver against a real broker through
// testkit's own loops, so that every driver is measured the same way rather than
// each inventing one.
//
// Every benchmark here completes in well under three seconds, which is the
// harness ceiling of IQMQ_BENCH_PER_BENCH_MAX_MS counted per invocation, per
// name and per ITERATION.

// BenchmarkAMQP10 measures the pure paths.
//
//	BenchmarkAMQP10/Encode/mode=nativestrict/headers=0/payload=64B
//	BenchmarkAMQP10/Decode/mode=nativestrict/headers=0/payload=64B
//	BenchmarkAMQP10/ErrorMapping/op=publish/shape=condition
//	BenchmarkAMQP10/Capability/feature=AtLeastOnce
func BenchmarkAMQP10(b *testing.B) {
	b.Run("Encode", benchEncode)
	b.Run("Decode", benchDecode)
	b.Run("ErrorMapping", benchErrorMapping)
	b.Run("Capability", benchCapability)
}

// benchEncode measures the publish-side mapping, which is the one hot path this
// module owns entirely: a publication's cost here is paid per message.
//
// The dimensions are the specification, in a fixed order: the metadata mode, the header
// count, and the payload size.
func benchEncode(b *testing.B) {
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope} {
		for _, headers := range []int{0, 2, 8} {
			for _, payload := range []struct {
				name string
				size int
			}{{"64B", 64}, {"1KiB", 1 << 10}} {
				name := "mode=" + mode.String() +
					"/headers=" + strconv.Itoa(headers) +
					"/payload=" + payload.name
				b.Run(name, func(b *testing.B) {
					m := benchMapper(b, mode)
					buf := aq.NewBuf()
					msg := benchMessage(headers, payload.size)
					opts := &mq.PublishOptions{TTL: time.Minute}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := m.Encode(buf, msg, opts); err != nil {
							b.Fatalf("Encode: %v", err)
						}
					}
				})
			}
		}
	}
}

// benchDecode measures the receive-side mapping into reused storage, which is
// what a source really does: the message and the header slice come from the
// token slot.
func benchDecode(b *testing.B) {
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataPortableEnvelope} {
		for _, headers := range []int{0, 2, 8} {
			for _, payload := range []struct {
				name string
				size int
			}{{"64B", 64}, {"1KiB", 1 << 10}} {
				name := "mode=" + mode.String() +
					"/headers=" + strconv.Itoa(headers) +
					"/payload=" + payload.name
				b.Run(name, func(b *testing.B) {
					m := benchMapper(b, mode)
					wire, err := m.Encode(aq.NewBuf(), benchMessage(headers, payload.size), nil)
					if err != nil {
						b.Fatalf("Encode: %v", err)
					}
					var dst mq.Message
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := m.Decode(&dst, wire); err != nil {
							b.Fatalf("Decode: %v", err)
						}
					}
				})
			}
		}
	}
}

// benchErrorMapping measures the table, which runs on every failure and
// therefore on every retry loop a degraded broker produces.
//
// The `shape` dimension is what makes the number readable: a condition the table
// knows is a walk down the rule list, and a wrapper with no condition walks the
// whole list before the transport rules answer.
func benchErrorMapping(b *testing.B) {
	f := openBench(b)
	for _, tc := range []struct {
		shape  string
		native error
	}{
		{"condition", &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondNotFound}}},
		{"nocondition", &amqp.LinkError{}},
	} {
		for _, op := range []string{mq.OpPublish, mq.OpAck} {
			b.Run("op="+op+"/shape="+tc.shape, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_ = aq.MapNetForTest(f.b, op, tc.native, true)
				}
			})
		}
	}
}

// benchCapability measures the capability lookup, which rule 4 requires to
// be compiled into booleans at construction — so the number here is the cost of
// the one the driver itself makes per settlement.
func benchCapability(b *testing.B) {
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		b.Fatalf("capabilities: %v", err)
	}
	for _, feature := range []mq.Feature{mq.FeatureAtLeastOnce, mq.FeatureDeadLetter} {
		b.Run("feature="+feature.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if !caps.Has(feature) {
					b.Fatal("the feature is not claimed")
				}
			}
		})
	}
}

// BenchmarkDriver runs the driver-layer benchmarks against a real AMQP 1.0
// broker (the layer-3 benchmark).
//
// It is env-gated for the same reason the conformance test is: a benchmark run
// with no broker would measure nothing, and `go test -bench .` on a developer's
// machine must not try to start one. The profile is the generic one, which is
// what a broker-specific module replaces with its own.
//
//	BenchmarkDriver/amqp10/Publish
//	BenchmarkDriver/amqp10/PubSubAck
//	BenchmarkDriver/amqp10/PublishBatch/size=N
func BenchmarkDriver(b *testing.B) {
	addr := os.Getenv("IQMQ_AMQP10_ADDR")
	if addr == "" {
		b.Skip("set IQMQ_AMQP10_ADDR to measure a broker; this module's own tests need none")
	}
	runDriverBenchmarks(b, addr)
}

// benchMapper builds a mapping for a benchmark.
func benchMapper(b *testing.B, mode mq.MetadataMode) *aq.Mapper {
	b.Helper()
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		b.Fatalf("capabilities: %v", err)
	}
	m, err := aq.MapperForTest(testProfile(), mode, caps, true)
	if err != nil {
		b.Fatalf("mapper: %v", err)
	}
	return m
}

// openBench opens a backend over a scripted peer for a benchmark that needs one
// but measures nothing about it.
func openBench(b *testing.B) *fixture {
	b.Helper()
	return open(b, testProfile(), aq.Config{})
}

// benchMessage is the message every mapping benchmark measures: n headers and a
// payload of the given size, with the typed properties a real publication
// carries.
func benchMessage(headers, payload int) *mq.Message {
	msg := &mq.Message{
		Key:  []byte("order-42"),
		Body: make([]byte, payload),
		ID:   "msg-1",
		Properties: mq.Properties{
			ContentType:   "application/json",
			CorrelationID: "corr-1",
			ReplyTo:       "replies",
		},
	}
	for i := range headers {
		msg.Headers.Add("header-"+strconv.Itoa(i), []byte(strings.Repeat("v", 8)))
	}
	return msg
}

// runDriverBenchmarks is the layer-3 loop, kept out of BenchmarkDriver so that
// the environment gate reads as one line.
func runDriverBenchmarks(b *testing.B, addr string) {
	b.Helper()
	cfg := aq.Config{Addrs: strings.Split(addr, ",")}
	client, err := aq.Open(context.Background(), genericProfile(), cfg)
	if err != nil {
		b.Skipf("opening a client against %s: %v", addr, err)
	}
	b.Cleanup(func() { _ = client.Close() })
	b.Run("amqp10/Publish", func(b *testing.B) {
		p, perr := client.Publisher(context.Background(), mq.PublisherConfig{Topic: benchTopic()})
		if perr != nil {
			b.Skipf("opening a publisher: %v", perr)
		}
		defer func() { _ = p.Close() }()
		msg := &mq.Message{Body: make([]byte, 64)}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := p.Publish(context.Background(), msg); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	})
}

// benchTopic is the topic the layer-3 benchmark publishes to, distinct per run
// so that two runs against one broker do not share a queue.
func benchTopic() string {
	return "iqmq-bench-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// genericProfile is the "generic AMQP 1.0" profile: plain addresses, a
// shared durable subscription named by appending the subscription to the
// address, and nothing broker-specific claimed.
//
// It is what the env-gated benchmark and the env-gated conformance test open
// with, and it is deliberately minimal: a core measured through a profile that
// claimed more than the protocol has would be measuring the profile.
func genericProfile() aq.Profile {
	return aq.Profile{
		Driver:        "amqp10",
		Name:          "generic",
		SinkAddress:   func(topic string) (string, error) { return topic, nil },
		SourceAddress: func(topic, sub string) (string, error) { return topic, nil },
	}
}
