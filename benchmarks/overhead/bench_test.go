package overhead_test

import (
	"testing"

	"github.com/iqhive/iqmq/benchmarks/opbench"
	"github.com/iqhive/iqmq/benchmarks/overhead"
)

// BenchmarkDriver is the layer-3 driver row:
//
//	BenchmarkDriver/<driver>/{Publish,PublishBatch,Receive,EndToEnd}/{64B,1KiB,64KiB}
//
// One profile at a time, each against the broker its AddrEnv names, with a
// profile whose address is unset skipped by name. The loop is
// benchmarks/opbench -- the same one BenchmarkNative runs -- so the difference
// between a row here and the row of the same shape under
// `BenchmarkNative/<client>` is the library's overhead and not a difference of
// method.
//
// It starts no container. `hack/bench/run-local.sh` does, one broker at a
// time, and passes the addresses in.
func BenchmarkDriver(b *testing.B) {
	for _, p := range overhead.Profiles() {
		addr, ok := p.Addr()
		if !ok {
			// A named skip, not silence: a profile that was not measured
			// has to be visible in the log the report is written from.
			b.Run(p.Arm, func(b *testing.B) {
				b.Skipf("overhead: %s is unset, so %s was not measured", p.AddrEnv, p.Arm)
			})
			continue
		}
		h := p.Harness(addr)
		opbench.Run(b, p.Arm, func(tb testing.TB, params opbench.Params) opbench.Session {
			return overhead.Open(tb, h, params)
		})
	}
}
