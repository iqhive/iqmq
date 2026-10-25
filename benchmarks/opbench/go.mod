// Module github.com/iqhive/iqmq/benchmarks/opbench is the layer-3
// timing loop both arms of the overhead comparison run, and it is a module of
// its own for one reason: it must depend on NOTHING.
//
// It started as a package inside the `benchmarks` module, which the raw-client
// baselines then required in order to share the loop. That worked and it moved
// version selection: `benchmarks` requires the two observability integrations
// (for BenchmarkHooks), so `go mod tidy` in `benchmarks/native-baselines`
// raised prometheus/client_golang, prometheus/common, prometheus/procfs,
// go.opentelemetry.io/otel, go-logr and golang-jwt in the baselines' own
// graph -- a measurement artefact's dependencies moving because of an
// unrelated integration. A baseline "may import no part of iqmq at all" is the
// rule its TestNoRootImport enforces; having its indirect graph pulled about
// by iqmq's observability modules is the same coupling by another route.
//
// A module with an empty require block cannot do that to anybody, and it makes
// the claim -- that the shared loop imports nothing -- a
// structural fact rather than a convention.
module github.com/iqhive/iqmq/benchmarks/opbench

go 1.27.3
