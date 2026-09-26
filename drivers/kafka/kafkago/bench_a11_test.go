package kafkago_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	franz "github.com/iqhive/iqmq/drivers/kafka"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// The Kafka client-library bake-off.
//
// The specification names the benchmark outright —
// `BenchmarkKafkaClient/{franz,kafkago}/{publish,batch,consume,endtoend}/{64B,1KiB,64KiB}`
// — and the shape below is that name and nothing else.
//
// # Why it lives here
//
// the arms are two CLIENT LIBRARIES in two modules, which is the one shape
// could not express: it runs one `go test` in one package,
// and a bake-off needs both arms in one benchmark so that the campaign's
// per-count interleaving measures them adjacently (step 2). The two arms
// can only be in one package if that package imports both drivers, and the
// only package in this repository that already does is THIS one's test
// binary — the kafka-go driver's own tests import drivers/kafka for the
// cross-driver equivalence suite and for kfake.
//
// # What the broker is, and what that costs the result
//
// An in-process kfake cluster, not the container the specification asks for, and that is a
// declared gap rather than a substitution (`CRITERION_GAP` in.env). Three
// reasons it is the right broker for THIS measurement and one reason it is not
// the whole of the evidence:
//
//   - the question is what the CLIENT costs. A container's broker — its
//     fsync, its replication, its own CPU — is common-mode cost that both arms
//     pay identically, so it adds variance to the difference without adding
//     information about it.
//   - The campaign runs inside `make ci`'s ceilings and on a shared host:
//     `make bench-lint` lists every family's rows at `-benchtime=1x` on every
//     CI run, and a family whose rows need Docker would either start
//     containers in every agent's `make ci` or select no benchmark at all and
//     fail the lint.
//   - kfake is a real Kafka protocol server on a real socket, and both clients
//     speak to the same instance of it.
//   - What it does NOT measure is what the specification also asks for: the rebalance
//     storm, the reconnect chaos, and the p99 against a container. Those are
//     declared as the criterion's gap and named with the command that produces
//     them (`BenchmarkDriver` with IQMQ_KAFKA_ADDR, and this package's own
//     critical tests for rebalance correctness).
//
// # How the two arms are configured
//
// The specification says "acks=all, same batch/linger". Both arms run acks=all, which is
// franz-go's default and is NOT kafka-go's. "Same linger" means NEITHER arm
// lingers on a synchronous publication: franz-go's linger is zero and
// kafka-go's batch size is one, because kafka-go has no immediate-flush path
// and a batch above one makes every synchronous publication wait out the batch
// timer. "Same batch" means the batch arm batches a hundred
// records on both: franz-go batches whatever is buffered, kafka-go needs its
// batch size raised to a hundred to do the same. Configuring the arms
// identically field-for-field would measure the configuration rather than the
// client.

// a11Arm is one arm of A client library behind the driver
// contract.
type a11Arm struct {
	slug  string
	ident string
	open  func(tb testing.TB, addrs []string, batch int) *mq.Client
}

// a11Arms are the family's kinds, in declaration order. franz is the
// reference: the specification expects it to win.
func a11Arms() []a11Arm {
	return []a11Arm{
		{slug: "franz", ident: "Franz", open: openFranzArm},
		{slug: "kafkago", ident: "KafkaGo", open: openKafkaGoArm},
	}
}

// openFranzArm opens the franz-go driver with acks=all and no linger.
func openFranzArm(tb testing.TB, addrs []string, _ int) *mq.Client {
	tb.Helper()
	c, err := franz.Open(context.Background(), franz.Config{
		Brokers:        addrs,
		ClientID:       "iqmq-a11",
		CommitInterval: 50 * time.Millisecond,
		FetchMaxWait:   10 * time.Millisecond,
		RequestTimeout: 10 * time.Second,
		// Zero is franz-go's own default and this driver's: a synchronous
		// publisher waits for the promise, so a linger is latency it pays and
		// cannot use. The batch size is not a field here — franz-go batches
		// whatever is buffered when a produce goes out.
		Linger: 0,
	})
	if err != nil {
		tb.Fatalf("opening the franz-go arm: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// openKafkaGoArm opens the kafka-go driver with acks=all and the batch size
// the operation needs.
func openKafkaGoArm(tb testing.TB, addrs []string, batch int) *mq.Client {
	tb.Helper()
	c, err := kq.Open(context.Background(), kq.Config{
		Brokers:         addrs,
		ClientID:        "iqmq-a11",
		CommitInterval:  50 * time.Millisecond,
		FetchMaxWait:    10 * time.Millisecond,
		RequestTimeout:  10 * time.Second,
		BatchMaxRecords: batch,
		BatchTimeout:    time.Millisecond,
	})
	if err != nil {
		tb.Fatalf("opening the kafka-go arm: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// TestKafkaClientKindsList announces the kinds, which is what
// and `bakeoff lint` read to know what to measure
func TestKafkaClientKindsList(t *testing.T) {
	arms := a11Arms()
	parts := make([]string, 0, len(arms))
	for _, a := range arms {
		parts = append(parts, a.slug+"="+a.ident)
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
	if len(arms) < 2 {
		t.Fatalf("The specification has %d arm(s); a bake-off needs two", len(arms))
	}
}

// a11Size is one payload size of sweep.
type a11Size struct {
	name  string
	bytes int
}

// a11Sizes are the three payload sizes the specification names.
func a11Sizes() []a11Size {
	return []a11Size{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 * 1024}}
}

// a11BatchSize is how many records the batch operation publishes at once. It
// is a hundred because that is what benchmarks/native-baselines measures its
// raw-client batch at, so `BenchmarkKafkaClient/*/batch/*` and
// `BenchmarkNative/{franzgo,kafkago}/PublishBatch/*` are comparable.
const a11BatchSize = 100

// BenchmarkKafkaClient is.
//
//	BenchmarkKafkaClient/{franz,kafkago}/{publish,batch,consume,endtoend}/{64B,1KiB,64KiB}
func BenchmarkKafkaClient(b *testing.B) {
	cl := cluster(b, 4, nil)
	addrs := cl.ListenAddrs()
	for _, arm := range a11Arms() {
		b.Run(arm.slug, func(b *testing.B) {
			// Two clients per arm and not one per leaf: opening a client is
			// two round trips, and 24 rows times the campaign's ten counts
			// would pay for 240 of them. The batch size is the only
			// configuration that differs between the operations.
			single := arm.open(b, addrs, 1)
			batched := arm.open(b, addrs, a11BatchSize)
			for _, op := range []string{"publish", "batch", "consume", "endtoend"} {
				b.Run(op, func(b *testing.B) {
					for _, size := range a11Sizes() {
						b.Run(size.name, func(b *testing.B) {
							c := single
							if op == "batch" {
								c = batched
							}
							a11Leaf(b, c, op, size)
						})
					}
				})
			}
		})
	}
}

// a11Leaf measures one operation at one payload size.
func a11Leaf(b *testing.B, c *mq.Client, op string, size a11Size) {
	b.Helper()
	topic := newTopic("a11")
	admin, err := c.Admin()
	if err != nil {
		b.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(context.Background(), mq.TopicSpec{Name: topic}); err != nil {
		b.Fatalf("EnsureTopic: %v", err)
	}
	pub, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		b.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	body := make([]byte, size.bytes)
	for i := range body {
		body[i] = byte(i)
	}
	msg := func(i int) *mq.Message {
		return &mq.Message{Key: []byte("k" + strconv.Itoa(i&63)), Body: body}
	}
	ctx := context.Background()

	switch op {
	case "publish":
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			if err := pub.Publish(ctx, msg(i)); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	case "batch":
		batch := make([]*mq.Message, a11BatchSize)
		for i := range batch {
			batch[i] = msg(i)
		}
		b.SetBytes(int64(size.bytes) * a11BatchSize)
		b.ResetTimer()
		for b.Loop() {
			if err := pub.PublishBatch(ctx, batch); err != nil {
				b.Fatalf("PublishBatch: %v", err)
			}
		}
	case "consume":
		cons, err := c.Consumer(ctx, mq.ConsumerConfig{
			Topic: topic, Subscription: "a11-" + topic, Start: mq.StartEarliest(),
		})
		if err != nil {
			b.Fatalf("Consumer: %v", err)
		}
		defer func() { _ = cons.Close() }()
		const prefill = 128
		dst := make([]mq.Delivery, 1)
		fill := func(n int) {
			for i := range n {
				if err := pub.Publish(ctx, msg(i)); err != nil {
					b.Fatalf("prefill: %v", err)
				}
			}
		}
		fill(prefill)
		left := prefill
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for b.Loop() {
			if left == 0 {
				b.StopTimer()
				fill(prefill)
				left = prefill
				b.StartTimer()
			}
			n, err := cons.Receive(ctx, dst)
			if n == 0 {
				b.Fatalf("Receive returned nothing: %v", err)
			}
			if err := dst[0].Ack(); err != nil {
				b.Fatalf("Ack: %v", err)
			}
			left--
		}
		b.StopTimer()
	case "endtoend":
		cons, err := c.Consumer(ctx, mq.ConsumerConfig{
			Topic: topic, Subscription: "a11-" + topic, Start: mq.StartEarliest(),
		})
		if err != nil {
			b.Fatalf("Consumer: %v", err)
		}
		defer func() { _ = cons.Close() }()
		dst := make([]mq.Delivery, 1)
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			if err := pub.Publish(ctx, msg(i)); err != nil {
				b.Fatalf("Publish: %v", err)
			}
			n, err := cons.Receive(ctx, dst)
			if n == 0 {
				b.Fatalf("Receive returned nothing: %v", err)
			}
			if err := dst[0].Ack(); err != nil {
				b.Fatalf("Ack: %v", err)
			}
		}
		b.StopTimer()
	default:
		b.Fatalf("unknown operation %q", op)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}
