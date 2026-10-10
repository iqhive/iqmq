package competitors_test

import (
	"testing"

	"github.com/iqhive/iqmq/benchmarks/competitors"
	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// BenchmarkCompare is the layer-5 row:
//
//	BenchmarkCompare/{iqmq,watermill,gocloud,raw}/<family>/{Publish,PublishBatch,Receive,EndToEnd}/{64B,1KiB,64KiB}
//
// One family at a time, every arm of a family against the same broker, through
// the same opbench loop. A family whose address is unset is skipped BY NAME,
// because a row that was not measured has to be visible in the log the report
// is written from.
//
// Every leaf is measured, including the ones competitors.Exclusions() refuses
// to compare: a measured-and-withheld row is evidence and an unmeasured one is
// a gap. `benchmarks/cmd/compare` decides what appears as a ratio.
//
// It starts no container. hack/bench/run-compare.sh does, one at a time.
func BenchmarkCompare(b *testing.B) {
	for _, f := range competitors.Families() {
		addr, ok := f.Addr()
		if !ok {
			b.Run(f.Name, func(b *testing.B) {
				b.Skipf("competitors: %s is unset, so the %s family was not measured", f.AddrEnv, f.Name)
			})
			continue
		}
		for _, arm := range competitors.ArmsFor(f.Name) {
			opbench.Run(b, arm.Name(), func(tb testing.TB, p opbench.Params) opbench.Session {
				return arm.Open(tb, addr, p)
			})
		}
	}
}
