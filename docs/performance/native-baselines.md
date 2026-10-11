# Native client comparisons

The `benchmarks/native-baselines` module runs broker clients without the iqmq API. Match its payload, batch size, acknowledgements and concurrency to the driver workload when comparing overhead.

Run `make native-baselines` on a quiet host with the required brokers available. Keep results local and compare repeated samples under the same configuration.
