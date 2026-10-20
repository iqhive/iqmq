// Package opbench is the shared shape of the layer-3
// benchmarks: the canonical payload sizes, the four canonical operations, the
// per-run resource names, the loop that times them and the metrics every row
// reports.
//
// It exists so that the two arms of the overhead comparison are one
// experiment. The budget is "core overhead vs raw client", which is a
// DIFFERENCE -- and a difference computed from two loops, two warm-up rules,
// two payload generators and two refill policies is not a difference, it is a
// comparison of two experiments. So `BenchmarkNative/<client>/...` (the
// raw-client baselines, in benchmarks/native-baselines) and
// `BenchmarkDriver/<driver>/...` (the same work through this library, in
// benchmarks/overhead) both run THIS loop, and the only thing that differs
// between them is the Session implementation the benchmark opens.
//
// This package imports nothing from iqmq -- not the root module, not a driver.
// That is what lets the raw-client baselines keep using it while keeping their
// own TestNoRootImport, which is their reason to exist.
package opbench

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

// BatchSize is the number of messages one PublishBatch operation sends. It
// is the same for every client so the batch column compares like with like;
// clients whose native batch limit is smaller (SQS: 10) split internally and
// say so in their package documentation.
const BatchSize = 100

// prefetch is how many messages Receive and EndToEnd top up in one go when
// the supply of published messages runs out. Refills happen with the
// benchmark timer stopped.
const prefetch = 256

// The Publish and PublishBatch leaves publish without waiting for anybody to
// read, so what they leave behind grows for as long as they run. These two
// bound it, and the drain that enforces them happens with the timer stopped,
// exactly as benchReceive's refills do.
//
// It is not a tidiness measure. An in-process broker's lane is BOUNDED and its
// overflow policy is to block (the default), so an unbounded publish leaf
// deadlocks against its own backpressure: watched, at the default 65,536-record
// lane capacity, `BenchmarkDriver/memory/PublishBatch/64B` published about
// eighty thousand hundred-message batches into a lane nothing was reading and
// stopped for ever with the guard holding the host lock. At a longer benchtime
// the single-message leaf reaches the same wall. Against a network broker the
// same leaf instead leaves tens of megabytes of server-side backlog behind for
// the length of the run.
//
// So both leaves keep at most maxOutstanding messages, and at most
// drainBudgetBytes of payload, unread at any moment -- whichever is smaller at
// the leaf's payload size. At 64 B that is 8,192 messages, at 1 KiB 8,192, and
// at 64 KiB 512, so every size is comfortably under an in-process lane's
// capacity and under 32 MiB of broker backlog.
const (
	maxOutstanding   = 8192
	drainBudgetBytes = 32 << 20
)

// outstandingBound is how many published-but-unread messages a publish leaf
// may hold at the given payload size.
func outstandingBound(size Size) int {
	n := maxOutstanding
	if size.Bytes > 0 {
		if byBytes := drainBudgetBytes / size.Bytes; byBytes < n {
			n = byBytes
		}
	}
	// A bound below one batch would drain inside every iteration, which would
	// measure the drain rather than the publication.
	return max(n, BatchSize)
}

// Size is one canonical payload size.
type Size struct {
	// Name is the benchmark path segment ("64B", "1KiB", "64KiB").
	Name string
	// Bytes is the payload length.
	Bytes int
}

// Sizes returns the three canonical payload sizes, in benchmark
// order.
func Sizes() []Size {
	return []Size{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 << 10}}
}

// Ops returns the four canonical operations, in benchmark order.
func Ops() []string { return []string{"Publish", "PublishBatch", "Receive", "EndToEnd"} }

// Names returns every canonical benchmark name for one arm, in the order Run
// produces them. prefix is the enclosing benchmark function's name --
// "BenchmarkNative" for a raw client, "BenchmarkDriver" for the same work
// through this library.
func Names(prefix, arm string) []string {
	out := make([]string, 0, len(Ops())*len(Sizes()))
	for _, op := range Ops() {
		for _, s := range Sizes() {
			out = append(out, strings.Join([]string{prefix, arm, op, s.Name}, "/"))
		}
	}
	return out
}

// Session is one native client bound to one broker and one topic, queue or
// stream. Every method is called from a single goroutine; both arms measure a
// single producer and a single consumer (P=1).
type Session interface {
	// Publish sends one message and returns when the confirmation boundary
	// has been reached (the boundary is documented per package and recorded
	// in docs/drivers/<driver>.md). The two arms of a comparison MUST agree
	// on it: the native-comparison rules say an unconfirmed publication
	// is never compared as equivalent to a broker-confirmed one.
	Publish(ctx context.Context, payload []byte) error
	// PublishBatch sends len(payloads) messages as one client-level batch
	// and returns when all of them are confirmed.
	PublishBatch(ctx context.Context, payloads [][]byte) error
	// Receive consumes and settles exactly n messages that were published
	// earlier on this session.
	Receive(ctx context.Context, n int) error
	// Close releases the client and any server-side resources the session
	// created.
	Close() error
}

// Params describes the benchmark leaf a session is opened for. Clients whose
// batching is configured on the connection (kafka-go's Writer.BatchSize,
// Pulsar's producer batching) use Op to pick the configuration a real
// deployment would use for that call pattern; the choice is recorded in the
// package documentation.
type Params struct {
	// Op is one of Ops(): "Publish", "PublishBatch", "Receive", "EndToEnd".
	Op string
	// Size is the payload size of the leaf.
	Size Size
}

// Batched reports whether the leaf publishes in client-level batches
// (PublishBatch, and the Receive leaf's untimed refills).
func (p Params) Batched() bool { return p.Op == "PublishBatch" || p.Op == "Receive" }

// Open opens a session for one benchmark leaf. It is called once per leaf,
// before the timer starts, and must fail the test rather than return a
// broken session.
type Open func(tb testing.TB, p Params) Session

// Run runs the four canonical operations over the three canonical payload
// sizes for one arm, producing exactly the names of Names(<enclosing
// benchmark>, arm).
//
// The prefix comes from the benchmark function Run is called from, which is
// what lets the raw-client arm and the driver arm share this loop: a
// `func BenchmarkNative` produces BenchmarkNative/... and a
// `func BenchmarkDriver` produces BenchmarkDriver/..., over identical work.
func Run(b *testing.B, arm string, open Open) {
	b.Helper()
	b.Run(arm, func(b *testing.B) {
		for _, op := range Ops() {
			b.Run(op, func(b *testing.B) {
				for _, size := range Sizes() {
					b.Run(size.Name, func(b *testing.B) { leaf(b, op, size, open) })
				}
			})
		}
	})
}

func leaf(b *testing.B, op string, size Size, open Open) {
	b.Helper()
	ctx := Context(b)
	s := open(b, Params{Op: op, Size: size})
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("close: %v", err)
		}
	})
	warmup(ctx, b, s, size)
	switch op {
	case "Publish":
		benchPublish(ctx, b, s, size)
	case "PublishBatch":
		benchPublishBatch(ctx, b, s, size)
	case "Receive":
		benchReceive(ctx, b, s, size)
	case "EndToEnd":
		benchEndToEnd(ctx, b, s, size)
	default:
		b.Fatalf("unknown op %q", op)
	}
}

// warmup round-trips one message before the timer starts. It forces the
// lazy work every client hides behind the first call — TCP connect, metadata
// fetch, consumer-group join, topic auto-creation — out of the measurement,
// and fails the benchmark immediately when a session cannot round-trip.
func warmup(ctx context.Context, b *testing.B, s Session, size Size) {
	b.Helper()
	if err := s.Publish(ctx, Payload(size.Bytes)); err != nil {
		b.Fatalf("warm-up publish: %v", err)
	}
	if err := s.Receive(ctx, 1); err != nil {
		b.Fatalf("warm-up receive: %v", err)
	}
}

func benchPublish(ctx context.Context, b *testing.B, s Session, size Size) {
	p := Payload(size.Bytes)
	bound := outstandingBound(size)
	b.SetBytes(int64(size.Bytes))
	b.ResetTimer()
	n, outstanding := 0, 0
	for b.Loop() {
		if outstanding >= bound {
			b.StopTimer()
			if err := s.Receive(ctx, outstanding); err != nil {
				b.Fatalf("draining %d published message(s): %v", outstanding, err)
			}
			outstanding = 0
			b.StartTimer()
		}
		if err := s.Publish(ctx, p); err != nil {
			b.Fatalf("publish: %v", err)
		}
		outstanding++
		n++
	}
	b.StopTimer()
	report(b, n)
	drain(ctx, b, s, outstanding)
}

func benchPublishBatch(ctx context.Context, b *testing.B, s Session, size Size) {
	batch := Payloads(BatchSize, size.Bytes)
	bound := outstandingBound(size)
	b.SetBytes(int64(size.Bytes) * BatchSize)
	b.ResetTimer()
	n, outstanding := 0, 0
	for b.Loop() {
		if outstanding+BatchSize > bound {
			b.StopTimer()
			if err := s.Receive(ctx, outstanding); err != nil {
				b.Fatalf("draining %d published message(s): %v", outstanding, err)
			}
			outstanding = 0
			b.StartTimer()
		}
		if err := s.PublishBatch(ctx, batch); err != nil {
			b.Fatalf("publish batch: %v", err)
		}
		outstanding += BatchSize
		n += BatchSize
	}
	b.StopTimer()
	report(b, n)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/msg")
	drain(ctx, b, s, outstanding)
}

func benchReceive(ctx context.Context, b *testing.B, s Session, size Size) {
	batch := Payloads(prefetch, size.Bytes)
	if err := s.PublishBatch(ctx, batch); err != nil {
		b.Fatalf("prefill: %v", err)
	}
	supply := prefetch
	b.SetBytes(int64(size.Bytes))
	b.ResetTimer()
	n := 0
	for b.Loop() {
		if supply == 0 {
			b.StopTimer()
			if err := s.PublishBatch(ctx, batch); err != nil {
				b.Fatalf("refill: %v", err)
			}
			supply = prefetch
			b.StartTimer()
		}
		if err := s.Receive(ctx, 1); err != nil {
			b.Fatalf("receive: %v", err)
		}
		supply--
		n++
	}
	b.StopTimer()
	report(b, n)
	drain(ctx, b, s, supply)
}

func benchEndToEnd(ctx context.Context, b *testing.B, s Session, size Size) {
	p := Payload(size.Bytes)
	b.SetBytes(int64(size.Bytes))
	b.ResetTimer()
	n := 0
	for b.Loop() {
		if err := s.Publish(ctx, p); err != nil {
			b.Fatalf("publish: %v", err)
		}
		if err := s.Receive(ctx, 1); err != nil {
			b.Fatalf("receive: %v", err)
		}
		n++
	}
	report(b, n)
}

// drain consumes whatever a leaf left unread, so the session's queue is empty
// before Close: a leftover backlog would otherwise be charged to the next leaf
// that reuses the same broker, and on an in-process broker it is memory held
// until the engine is closed.
func drain(ctx context.Context, b *testing.B, s Session, supply int) {
	b.Helper()
	for supply > 0 {
		n := min(supply, 64)
		if err := s.Receive(ctx, n); err != nil {
			b.Logf("drain (%d left): %v", supply, err)
			return
		}
		supply -= n
	}
}

// report adds the throughput metric the specification asks every layer-3 benchmark to
// carry. Bytes per second comes from b.SetBytes as the standard MB/s column.
func report(b *testing.B, msgs int) {
	b.Helper()
	secs := b.Elapsed().Seconds()
	if secs <= 0 || msgs == 0 {
		return
	}
	b.ReportMetric(float64(msgs)/secs, "msgs/s")
}

// Payload returns a deterministic n-byte payload. The bytes are not
// compressible-in-a-trivial-way (a repeating 251-byte cycle), so codecs that
// compress by default do not read as free.
func Payload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}
	return p
}

// Payloads returns count distinct payloads of n bytes each.
func Payloads(count, n int) [][]byte {
	out := make([][]byte, count)
	for i := range out {
		p := Payload(n)
		if n > 0 {
			p[0] = byte(i)
		}
		out[i] = p
	}
	return out
}

// ResourceStem is the prefix BOTH arms of the overhead comparison give the
// broker resource they publish to. Every `BenchmarkNative/<client>/…` baseline
// names its stream, subject, topic or queue `Name(ResourceStem)`, and the
// `BenchmarkDriver/…` arms whose driver charges for the name's LENGTH name
// their topic the same way.
//
// It is a constant here, and not a literal at each call site, because the
// length is a measured cost and the two arms were once measured at different
// lengths. On the Redis driver every key is hash-tagged (`{topic}`,
// internal/streams.Keys), rueidis computes the cluster slot with a
// table-driven CRC16 over the tag's contents ONCE PER COMMAND
// (internal/cmds.slot), and the server charges the key's bytes again on top.
// So a driver arm publishing to `{iqmqconf.<nonce>.<n>.bench-PublishBatch}` —
// 42 hashed bytes, 150 RESP bytes per XADD — against a native arm publishing
// to `iqmqbench-<runid>-<n>` — 20 hashed bytes, 126 RESP bytes — was charging
// the library for the conformance harness's nonce. The native-comparison
// rules ask for the same topology on both arms, and a key of a different shape
// and more than twice the length is not the same topology.
//
// Uniqueness comes from [Name]'s random suffix, exactly as it does on the
// native arm, so no part of the name has to carry the benchmark leaf's purpose.
const ResourceStem = "iqmqbench"

// Name returns a unique broker-safe resource name with the given prefix,
// for example "iqmqbench-4f2a19c3d5e6". Only [a-z0-9-] is used, which every
// broker in the harness accepts (Pub/Sub additionally requires a letter
// first, which the prefix provides).
//
// The suffix is 48 random bits at a fixed width, which keeps two names of one
// run, and two concurrent runs against the same external broker
// (IQMQ_<BROKER>_ADDR), apart without a counter shared by the process, and
// keeps every name the same length so that no arm pays for a longer key.
func Name(prefix string) string {
	return fmt.Sprintf("%s-%012x", prefix, rand.Uint64()>>16) //nolint:gosec // uniqueness, not secrecy
}

// Context returns a context cancelled when the benchmark ends.
func Context(tb testing.TB) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	return ctx
}
