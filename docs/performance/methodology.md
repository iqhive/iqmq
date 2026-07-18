# Benchmark methodology

No benchmark results are tracked in this checkout. The commands below generate
new measurements; their presence does not establish a performance result.

Use a quiet host and the same Go version, CPU allocation, payloads, broker configuration and client settings for each comparison. Repeat measurements and compare their distributions.

Run core benchmarks with `go test -run=^$ -bench=. -benchmem ./...`. Use `make bench-overhead` for driver comparisons and `make bench-compare` for comparisons with other libraries. These targets write local output under `benchmarks/reports/`.

Use `make benchguard-test` and `make benchgate-test` to check the measurement tools. Results from emulators describe those emulators; use the intended managed service when assessing production behaviour.
