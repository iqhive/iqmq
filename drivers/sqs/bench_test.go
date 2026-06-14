package sqs_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// It reports the number the family is decided on beside ns/op, because ns/op
// against an in-process server is a measurement of a Go map and the criterion
// is "cost":
//
//	requests/settlement  the API calls the service charges for, per
//	                     settlement. Immediate is 1.0 and the batching
//	                     policies approach 1/MaxItems.
//
//	BenchmarkSQSSettlement/<policy>/batch=<1|10|100>
//
// It runs against the in-process server by default, so it needs no container
// and its numbers describe the DRIVER's request pattern rather than the
// service's latency — which is the only thing an emulator can honestly
// measure (layer 4). IQMQ_SQS_ADDR points it at a real endpoint, and the
// same request counts then come with a real round trip in them.
func BenchmarkSQSSettlement(b *testing.B) {
	for _, kind := range flush.Kinds() {
		b.Run(kind.String(), func(b *testing.B) {
			for _, batch := range []int{1, 10, 100} {
				b.Run("batch="+strconv.Itoa(batch), func(b *testing.B) {
					benchSettlement(b, kind, batch)
				})
			}
		})
	}
}

// benchSettlement drives one policy at one batch size.
func benchSettlement(b *testing.B, kind flush.Kind, batch int) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	cfg := sq.Config{
		Region:          "fake-region-1",
		AccessKeyID:     "AKIAFAKE",
		SecretAccessKey: "not-a-real-secret",
		AutoCreate:      true,
		WaitTime:        time.Second,
		AckDeadline:     time.Hour,
		Settle:          kind,
		SettleMaxItems:  10,
		SettleInterval:  5 * time.Millisecond,
		Clock:           benchClock{server},
	}
	back, err := sq.BackendWithAPI(context.Background(), server, server, driverkit.ClientBorrowed, cfg)
	if err != nil {
		b.Fatalf("BackendWithAPI: %v", err)
	}
	client := mq.NewClient(back)
	b.Cleanup(func() {
		if err := client.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	name := "bench-settle-" + kind.String() + "-" + strconv.Itoa(batch)
	cons, err := client.Consumer(context.Background(), mq.ConsumerConfig{
		Topic: name, Subscription: "s", Prefetch: batch,
	})
	if err != nil {
		b.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()

	dst := make([]mq.Delivery, batch)
	settled, requests := 0, 0
	body := []byte("payload")
	b.ReportAllocs()
	for b.Loop() {
		for range batch {
			if err := client.Publish(context.Background(), name, &mq.Message{Body: body}); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
		got := 0
		for got < batch {
			n, err := cons.Receive(context.Background(), dst[got:])
			got += n
			if err != nil {
				b.Fatalf("Receive: %v", err)
			}
		}
		before := server.Calls("DeleteMessageBatch") + server.Calls("DeleteMessage")
		errs := settleConcurrently(dst[:got])
		for _, err := range errs {
			if err != nil {
				b.Fatalf("Ack: %v", err)
			}
		}
		requests += server.Calls("DeleteMessageBatch") + server.Calls("DeleteMessage") - before
		settled += got
	}
	if settled == 0 {
		return
	}
	b.ReportMetric(float64(requests)/float64(settled), "requests/settlement")
}

// settleConcurrently acknowledges every delivery from its own goroutine, which
// is what lets a batching policy coalesce them: a sequential loop would make
// one request per settlement whatever the policy.
func settleConcurrently(ds []mq.Delivery) []error {
	errs := make([]error, len(ds))
	done := make(chan struct{}, len(ds))
	for i := range ds {
		go func(i int) {
			errs[i] = ds[i].Ack()
			done <- struct{}{}
		}(i)
	}
	for range ds {
		<-done
	}
	return errs
}

// benchClock is the server's clock, so that nothing in the benchmark waits for
// a visibility window.
type benchClock struct{ s *sqsfake.Server }

func (c benchClock) Now() time.Time { return c.s.Now() }

func (c benchClock) NewTimer(d time.Duration) mq.Timer { return &realTimer{t: time.NewTimer(d)} }

// BenchmarkDriver is the conformance kit's own benchmark set against a real
// endpoint, which is what makes this driver's numbers comparable with
// benchmarks/native-baselines/sqs.
//
// It is env-gated: without IQMQ_SQS_ADDR there is no endpoint, and an
// in-process server would produce numbers that look like a driver benchmark
// and measure a Go map.
func BenchmarkDriver(b *testing.B) {
	addr := os.Getenv("IQMQ_SQS_ADDR")
	if addr == "" {
		b.Skip("IQMQ_SQS_ADDR is not set; this benchmark needs a real endpoint (layer 4)")
	}
	testkit.RunBenchmarks(b, sq.ConformanceHarness(
		endpointConfig(addr),
		testkit.EvidenceEmulator,
		testkit.Broker{Image: os.Getenv("IQMQ_SQS_IMAGE")},
	))
}

// endpointConfig is the configuration for an endpoint named by an environment
// variable, which both the env-gated benchmark and the env-gated conformance
// test use.
func endpointConfig(addr string) sq.Config {
	return sq.Config{
		Region:            envOr("IQMQ_SQS_REGION", "elasticmq"),
		Endpoint:          "http://" + addr,
		InsecureTransport: true,
		AccessKeyID:       envOr("IQMQ_SQS_KEY", "x"),
		SecretAccessKey:   dsnSecret(envOr("IQMQ_SQS_SECRET", "x")),
		AutoCreate:        true,
		WaitTime:          time.Second,
		AckDeadline:       30 * time.Second,
	}
}

// envOr is an environment variable with a default.
func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
