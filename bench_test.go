package iqmq_test

import (
	"strconv"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// benchHeaderCounts are the header dimension: an empty set, two headers
// (what iqmq's own properties cost), eight (a typical application set and the
// count the budget is stated at), and the thirty-two and
// hundred-and-twenty-eight a header-heavy caller or an envelope reaches.
var benchHeaderCounts = []int{0, 2, 8, 32, 128}

// registryKeys are the well-known header names, plus the
// hyphenated names a real caller adds. They vary in length and in first byte,
// which is what makes a linear scan cheap: a string comparison that fails on
// the length never reaches runtime.memequal.
//
// This is the corpus the budget is held to, because the budget is a
// statement about what the API costs in use.
var registryKeys = []string{
	"content-type", "content-encoding", "correlation-id", "causation-id",
	"reply-to", "traceparent", "tracestate", "baggage",
	"schema-id", "x-request-id", "user-agent", "accept",
	"idempotency-key", "tenant", "region", "priority-class",
}

// keyShapes are the two key corpora BenchmarkHeaders measures over.
//
// "uniform" is the corpus x/apibench/b4 raced the four representations on:
// eight three-byte keys of identical length sharing their first byte. For a
// race between four representations that is the right corpus — every
// representation does the same work and none is flattered — but it is the
// worst possible input for a linear scan, because every comparison has to
// call runtime.memequal, and it makes a header lookup look 3.7 times more
// expensive than it is. It is kept so that the prototype's number stays
// comparable, and reported as the worst case.
var keyShapes = []struct {
	name string
	key  func(i int) string
}{
	{"registry", func(i int) string {
		if i < len(registryKeys) {
			return registryKeys[i]
		}
		// Past the registry, keep the varying shape rather than falling back
		// to a uniform one: "h-12" and "h-7" differ in length.
		return "h-" + strconv.Itoa(i)
	}},
	{"uniform", func(i int) string {
		const letters = "abcdefghijklmnopqrstuvwxyz"
		return "h" + string(letters[i%26]) + string(letters[(i/26)%26])
	}},
}

// benchHeaderValue is the i-th value of the benchmark corpus.
func benchHeaderValue(i int) []byte {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	val := make([]byte, 0, 16)
	val = append(val, 'v', letters[i%26])
	for range i % 7 {
		val = append(val, '.')
	}
	return val
}

// benchHeaderPair is the i-th pair of the uniform corpus. It is what the
// allocation tests fill a set from: an allocation count does not depend on
// the key shape, so they need one corpus and not two.
func benchHeaderPair(i int) (string, []byte) {
	return keyShapes[1].key(i), benchHeaderValue(i)
}

// headerRig is a filled header set plus the destinations every operation
// writes into, sized to the corpus so that no operation is charged for a
// destination having to grow.
type headerRig struct {
	h    mq.Headers
	key  string
	val  []byte
	sink mq.Headers
	vals [][]byte
}

func newHeaderRig(n int, key func(int) string) *headerRig {
	r := &headerRig{val: []byte("benchmark-value")}
	for i := range n {
		r.h.Add(key(i), benchHeaderValue(i))
	}
	// The key every operation names is the LAST one, which is the worst case
	// for a linear scan. Naming the first would measure how quickly the scan
	// can stop rather than what a lookup costs.
	if n > 0 {
		r.key = key(n - 1)
	} else {
		r.key = "absent"
	}
	return r
}

// BenchmarkHeaders is the name for the header micro-benchmarks, with the n= dimension. The specification budgets Get and Set at eight headers or fewer
// to 15 ns and zero allocations, with Set allowed to grow once.
//
// Every operation is size-stable — an Add is taken straight back out and a
// Delete is put back — so what is measured is one operation at size n and not
// an ever-growing header set.
func BenchmarkHeaders(b *testing.B) {
	ops := []struct {
		name string
		run  func(r *headerRig)
	}{
		{"Get", func(r *headerRig) { r.h.Get(r.key) }},
		{"Set", func(r *headerRig) { r.h.Set(r.key, r.val) }},
		{"Add", func(r *headerRig) {
			r.h.Add("bench-add", r.val)
			r.h.Delete("bench-add")
		}},
		{"Delete", func(r *headerRig) {
			r.h.Delete(r.key)
			r.h.Add(r.key, r.val)
		}},
		{"Values", func(r *headerRig) { r.vals = r.h.Values(r.key) }},
		{"Clone", func(r *headerRig) { r.sink = r.h.Clone() }},
		{"All", func(r *headerRig) {
			n := 0
			for range r.h.All() {
				n++
			}
			_ = n
		}},
	}
	for _, op := range ops {
		b.Run(op.name, func(b *testing.B) {
			for _, shape := range keyShapes {
				b.Run("keys="+shape.name, func(b *testing.B) {
					benchHeaderOp(b, op.name, op.run, shape.key)
				})
			}
		})
	}
}

// benchHeaderOp measures one operation over every header count of one key
// corpus.
func benchHeaderOp(b *testing.B, name string, run func(*headerRig), key func(int) string) {
	for _, n := range benchHeaderCounts {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			r := newHeaderRig(n, key)
			for range 16 {
				run(r)
			}
			steady := r.h.Len()
			if steady > n+1 {
				b.Fatalf("%s/n=%d: the warm-up grew the set to %d headers", name, n, steady)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run(r)
			}
			b.StopTimer()
			if got := r.h.Len(); got != steady {
				b.Fatalf("%s/n=%d: the benchmark left %d headers, the steady state is %d — it is accumulating state",
					name, n, got, steady)
			}
		})
	}
}

// BenchmarkAPI is the layer-1 name for the option and capability
// micro-benchmarks. The option call sites are the ones the specification's
// "0 allocs at call sites" is a claim about, so they are written out as
// literal variadic calls rather than fed a pre-built slice: handing them a
// slice would measure the slice, and hoisting the construction out of the
// loop would measure the fold alone.
func BenchmarkAPI(b *testing.B) {
	b.Run("Options/value", func(b *testing.B) {
		b.Run("n=1", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var o mq.PublishOptions
				if err := o.Apply(mq.Delay(benchDelay)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("n=5", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var o mq.PublishOptions
				if err := o.Apply(mq.Delay(benchDelay), mq.TTL(benchTTL),
					mq.Priority(7), mq.Partition(3), mq.DedupKey("order-42")); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	caps := benchCapabilities()
	b.Run("Capabilities/Has", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if !caps.Has(mq.FeatureExplicitAck) {
				b.Fatal("missing feature")
			}
		}
	})
	b.Run("Capabilities/Require", func(b *testing.B) {
		req := mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)
		b.ReportAllocs()
		for b.Loop() {
			if err := caps.Require(req); err != nil {
				b.Fatal(err)
			}
		}
	})
}
