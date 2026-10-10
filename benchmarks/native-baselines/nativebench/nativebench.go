// Package nativebench is the shared shape of the native-client baselines:
// the canonical payload sizes, the performance budgets benchmark names, the
// per-run resource names and the metrics every baseline reports.
//
// The baselines answer one question: what does the native Go client for a
// broker cost, on this host, against this container, with the durability
// settings the corresponding iqmq driver will use? They are the denominator
// of the overhead budget, so they must not import iqmq —
// TestNoRootImport enforces that for the whole module.
//
// The loop itself lives in github.com/iqhive/iqmq/benchmarks/opbench and is
// re-exported here. That is not tidying: the budget is "core overhead vs
// raw client", which is a DIFFERENCE, and `BenchmarkDriver/<driver>/…` has to
// be the same experiment as `BenchmarkNative/<client>/…` rather than a second
// one that resembles it. Sharing the loop is what makes "with identical
// settings" a property of the code instead of a claim in a report. `opbench`
// imports nothing from iqmq either, which is why this module can depend on it
// without weakening TestNoRootImport.
package nativebench

import (
	"context"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// BatchSize is the number of messages one PublishBatch operation sends. It
// is the same for every client so the batch column compares like with like;
// clients whose native batch limit is smaller (SQS: 10) split internally and
// say so in their package documentation.
const BatchSize = opbench.BatchSize

type (
	// Size is one canonical payload size.
	Size = opbench.Size
	// Session is one native client bound to one broker and one topic, queue
	// or stream.
	Session = opbench.Session
	// Params describes the benchmark leaf a session is opened for.
	Params = opbench.Params
	// Open opens a session for one benchmark leaf.
	Open = opbench.Open
)

// Sizes returns the three canonical payload sizes, in benchmark
// order.
func Sizes() []Size { return opbench.Sizes() }

// Ops returns the four canonical operations, in benchmark order.
func Ops() []string { return opbench.Ops() }

// Names returns every canonical benchmark name for one client, in the order
// Run produces them.
func Names(client string) []string { return opbench.Names("BenchmarkNative", client) }

// Run runs the four canonical operations over the three canonical payload
// sizes for one client, producing exactly the names of Names(client).
func Run(b *testing.B, client string, open Open) {
	b.Helper()
	opbench.Run(b, client, open)
}

// Payload returns a deterministic n-byte payload.
func Payload(n int) []byte { return opbench.Payload(n) }

// Payloads returns count distinct payloads of n bytes each.
func Payloads(count, n int) [][]byte { return opbench.Payloads(count, n) }

// ResourceStem is the prefix BOTH arms of the overhead comparison give the
// broker resource they publish to, so that the resource NAME is not one of the
// differences the ratio is measuring. A baseline passes it to [Name] rather
// than a literal of its own; see [opbench.ResourceStem] for why the length
// matters.
const ResourceStem = opbench.ResourceStem

// Name returns a unique broker-safe resource name with the given prefix.
func Name(prefix string) string { return opbench.Name(prefix) }

// Context returns a context cancelled when the benchmark ends.
func Context(tb testing.TB) context.Context { return opbench.Context(tb) }
