# Driver overhead

A driver that adds a millisecond to every publish costs you more than the broker does, so each measured driver is timed against the client library it wraps. Both arms send the same payloads to the same broker container, on the same pinned CPUs, and the gap between them is the driver's overhead.

The figures here are a fresh baseline taken on 19 October 2026 from the v1.0.0 release tree. Earlier overhead figures, and some of the benchmarks behind them, stayed in the private Bitbucket repository when the project moved to GitHub, because they held internal, project-specific information. Compare future runs against this page, not against anything older.

## The run

| | |
|---|---|
| Run | `benchmarks/reports/results/20261019T024052Z-overhead` (kept locally, not committed) |
| Host | AMD EPYC 9474F, 16 CPUs held by the benchmark (GOMAXPROCS 16), performance governor |
| Go | go1.27.2 |
| Samples | 10 rounds of 50ms per benchmark, both arms interleaved |
| Brokers | Redis and NATS containers pinned in `conformance/harness/images.lock` |
| Guard | clean for every broker |

The budgets are on throughput lost, not time added. An arm that is 10% slower per operation moves 9.09% fewer messages a second. A single-message publish may lose up to 10%, and a batched publish up to 5%.

### Measured

| Arm | Raw-client denominator | Broker |
|---|---|---|
| `BenchmarkDriver/memory` | none, in-process | none |
| `BenchmarkDriver/redis/streams` | `BenchmarkNative/rueidis` | Redis |
| `BenchmarkDriver/redis/goredis` | `BenchmarkNative/goredis` | Redis |
| `BenchmarkDriver/nats/core` | `BenchmarkNative/nats` | NATS |
| `BenchmarkDriver/jetstream` | `BenchmarkNative/jetstream` | NATS with JetStream |

### Not measured

| Profile | Obstacle | What it would take |
|---|---|---|
| `memory` | **no denominator** | nothing; there is no client underneath it to compare with |
| `kafka` (franzgo, kafkago, sarama) | **no driver arm** | a Kafka profile in `benchmarks/overhead`, against the Redpanda container the baselines already use |
| `nsq` | **no driver arm** | an NSQ profile in `benchmarks/overhead` |
| `rabbitmq` (amqp091) | **no driver arm** | a RabbitMQ profile in `benchmarks/overhead` |
| `activemq` (goamqp) | **no driver arm** | an ActiveMQ profile in `benchmarks/overhead`, against the Artemis container |
| `pulsar` | **no driver arm** | a Pulsar profile in `benchmarks/overhead` |
| `gcppubsub` | **emulator only (§10.5 layer 4)** | a §10.5 layer-4 run against the managed service |
| `sqs` | **emulator only (§10.5 layer 4)** | a §10.5 layer-4 run against the managed service; the baseline runs against ElasticMQ |

The native baselines for the drivers in this section are still measured on their own. See [Native client comparisons](native-baselines.md).

## Results

| Arm | Op | Size | driver ns/op | native ns/op | throughput loss | budget | verdict |
|---|---|---|---|---|---|---|---|
| `memory` | Publish | 64B | 740 ±3.6% (n=10) | — | — | — | not compared |
| `memory` | Publish | 1KiB | 1000 ±7.9% (n=10) | — | — | — | not compared |
| `memory` | Publish | 64KiB | 16400 ±18.4% (n=10) | — | — | — | not compared |
| `memory` | PublishBatch | 64B | 63811 ±19.1% (n=10) | — | — | — | not compared |
| `memory` | PublishBatch | 1KiB | 94168 ±13.8% (n=10) | — | — | — | not compared |
| `memory` | PublishBatch | 64KiB | 1604149 ±16.9% (n=10) | — | — | — | not compared |
| `memory` | Receive | 64B | 720 ±3.7% (n=10) | — | — | — | not compared |
| `memory` | Receive | 1KiB | 771 ±5.4% (n=10) | — | — | — | not compared |
| `memory` | Receive | 64KiB | 871 ±10.2% (n=10) | — | — | — | not compared |
| `memory` | EndToEnd | 64B | 1278 ±5.6% (n=10) | — | — | — | not compared |
| `memory` | EndToEnd | 1KiB | 1598 ±6.0% (n=10) | — | — | — | not compared |
| `memory` | EndToEnd | 64KiB | 19006 ±9.3% (n=10) | — | — | — | not compared |
| `redis/streams` | Publish | 64B | 85462 ±7.6% (n=10) | 83811 ±7.7% (n=10) | +1.9% | ≤ 10% | met |
| `redis/streams` | Publish | 1KiB | 88182 ±11.9% (n=10) | 81604 ±7.0% (n=10) | +7.5% | ≤ 10% | met |
| `redis/streams` | Publish | 64KiB | 221914 ±8.5% (n=10) | 209484 ±42.3% (n=10) | +5.6% | ≤ 10% | met |
| `redis/streams` | PublishBatch | 64B | 490488 ±3.4% (n=10) | 437832 ±4.0% (n=10) | +10.7% | ≤ 5% | **BREACH** |
| `redis/streams` | PublishBatch | 1KiB | 808578 ±4.3% (n=10) | 715463 ±2.7% (n=10) | +11.5% | ≤ 5% | **BREACH** |
| `redis/streams` | PublishBatch | 64KiB | 15596011 ±8.4% (n=10) | 13019642 ±7.8% (n=10) | +16.5% | ≤ 5% | **BREACH** |
| `redis/streams` | Receive | 64B | 101125 ±6.5% (n=10) | 175229 ±10.7% (n=10) | — | — | not compared |
| `redis/streams` | Receive | 1KiB | 100864 ±5.1% (n=10) | 171027 ±8.1% (n=10) | — | — | not compared |
| `redis/streams` | Receive | 64KiB | 232076 ±16.8% (n=10) | 256033 ±11.2% (n=10) | — | — | not compared |
| `redis/streams` | EndToEnd | 64B | 275118 ±7.6% (n=10) | 269686 ±10.6% (n=10) | — | — | not compared |
| `redis/streams` | EndToEnd | 1KiB | 276731 ±4.4% (n=10) | 264622 ±7.4% (n=10) | — | — | not compared |
| `redis/streams` | EndToEnd | 64KiB | 522989 ±5.0% (n=10) | 423329 ±12.9% (n=10) | — | — | not compared |
| `redis/goredis` | Publish | 64B | 72484 ±2.2% (n=10) | 76854 ±9.2% (n=10) | -6.0% | ≤ 10% | met |
| `redis/goredis` | Publish | 1KiB | 75598 ±6.4% (n=10) | 82468 ±7.8% (n=10) | -9.1% | ≤ 10% | met |
| `redis/goredis` | Publish | 64KiB | 200691 ±5.1% (n=10) | 208918 ±11.8% (n=10) | -4.1% | ≤ 10% | met |
| `redis/goredis` | PublishBatch | 64B | 462062 ±3.4% (n=10) | 436231 ±3.1% (n=10) | +5.6% | ≤ 5% | **BREACH** |
| `redis/goredis` | PublishBatch | 1KiB | 723761 ±6.7% (n=10) | 701306 ±5.4% (n=10) | +3.1% | ≤ 5% | met |
| `redis/goredis` | PublishBatch | 64KiB | 12826304 ±8.8% (n=10) | 12035135 ±7.6% (n=10) | +6.2% | ≤ 5% | **BREACH** |
| `redis/goredis` | Receive | 64B | 93916 ±5.8% (n=10) | 151594 ±9.4% (n=10) | — | — | not compared |
| `redis/goredis` | Receive | 1KiB | 97252 ±9.1% (n=10) | 149681 ±6.4% (n=10) | — | — | not compared |
| `redis/goredis` | Receive | 64KiB | 255626 ±10.0% (n=10) | 254554 ±25.6% (n=10) | — | — | not compared |
| `redis/goredis` | EndToEnd | 64B | 249848 ±6.4% (n=10) | 231370 ±6.9% (n=10) | — | — | not compared |
| `redis/goredis` | EndToEnd | 1KiB | 272910 ±8.7% (n=10) | 224558 ±11.0% (n=10) | — | — | not compared |
| `redis/goredis` | EndToEnd | 64KiB | 464234 ±7.2% (n=10) | 432748 ±10.5% (n=10) | — | — | not compared |
| `nats/core` | Publish | 64B | 90564 ±6.1% (n=10) | 83249 ±11.2% (n=10) | +8.1% | ≤ 10% | met |
| `nats/core` | Publish | 1KiB | 92132 ±7.7% (n=10) | 84894 ±13.6% (n=10) | +7.9% | ≤ 10% | met |
| `nats/core` | Publish | 64KiB | 250560 ±12.6% (n=10) | 260395 ±15.1% (n=10) | -3.9% | ≤ 10% | met |
| `nats/core` | PublishBatch | 64B | 216910 ±11.0% (n=10) | 181111 ±7.4% (n=10) | +16.5% | ≤ 5% | **BREACH** |
| `nats/core` | PublishBatch | 1KiB | 362922 ±10.0% (n=10) | 324368 ±9.0% (n=10) | +10.6% | ≤ 5% | **BREACH** |
| `nats/core` | PublishBatch | 64KiB | 9847361 ±10.0% (n=10) | 9334266 ±14.0% (n=10) | +5.2% | ≤ 5% | **BREACH** |
| `nats/core` | Receive | 64B | 329 ±8.9% (n=10) | 40 ±4.4% (n=10) | — | — | not compared |
| `nats/core` | Receive | 1KiB | 394 ±5.6% (n=10) | 46 ±3.9% (n=10) | — | — | not compared |
| `nats/core` | Receive | 64KiB | 380 ±7.9% (n=10) | 64 ±3.4% (n=10) | — | — | not compared |
| `nats/core` | EndToEnd | 64B | 87790 ±10.5% (n=10) | 77062 ±7.8% (n=10) | — | — | not compared |
| `nats/core` | EndToEnd | 1KiB | 90034 ±11.1% (n=10) | 80502 ±10.2% (n=10) | — | — | not compared |
| `nats/core` | EndToEnd | 64KiB | 241904 ±6.4% (n=10) | 226798 ±13.8% (n=10) | — | — | not compared |
| `jetstream` | Publish | 64B | 145072 ±8.3% (n=10) | 128446 ±8.4% (n=10) | +11.5% | ≤ 10% | **BREACH** |
| `jetstream` | Publish | 1KiB | 146026 ±11.0% (n=10) | 131592 ±8.2% (n=10) | +9.9% | ≤ 10% | met |
| `jetstream` | Publish | 64KiB | 375854 ±10.6% (n=10) | 284000 ±9.4% (n=10) | +24.4% | ≤ 10% | **BREACH** |
| `jetstream` | PublishBatch | 64B | 1915357 ±4.0% (n=10) | 1807280 ±6.0% (n=10) | +5.6% | ≤ 5% | **BREACH** |
| `jetstream` | PublishBatch | 1KiB | 2165510 ±2.9% (n=10) | 1824928 ±6.4% (n=10) | +15.7% | ≤ 5% | **BREACH** |
| `jetstream` | PublishBatch | 64KiB | 12983430 ±3.7% (n=10) | 9385238 ±6.7% (n=10) | +27.7% | ≤ 5% | **BREACH** |
| `jetstream` | Receive | 64B | 5026 ±8.5% (n=10) | 177056 ±12.3% (n=10) | — | — | not compared |
| `jetstream` | Receive | 1KiB | 5068 ±4.5% (n=10) | 184146 ±10.4% (n=10) | — | — | not compared |
| `jetstream` | Receive | 64KiB | 28081 ±6.4% (n=10) | 392560 ±12.8% (n=10) | — | — | not compared |
| `jetstream` | EndToEnd | 64B | 165824 ±6.9% (n=10) | 414808 ±5.4% (n=10) | — | — | not compared |
| `jetstream` | EndToEnd | 1KiB | 164976 ±6.7% (n=10) | 354730 ±27.0% (n=10) | — | — | not compared |
| `jetstream` | EndToEnd | 64KiB | 346506 ±18.0% (n=10) | 707850 ±7.0% (n=10) | — | — | not compared |

60 row(s); 13 breach(es) of the overhead budgets.

`Receive` and `EndToEnd` are measured on both arms and deliberately not
turned into a ratio: the native-comparison rules require the same
prefetch on both sides and these arms do not have it, in both directions and
at two orders of magnitude (nats/core Receive: the driver 9x slower;
jetstream Receive: the driver 34x faster). EndToEnd contains a receive, so it
carries the same asymmetry. Both medians are in the table above; what is
compared is a batched publication and a single-message publication, which is
what the two budgets are written about.

## What this baseline shows

Single-message publishing is within its budget, or too close to call, on every arm. Batched publishing is where the overhead sits. The table marks 13 rows as breaches on their medians alone. `make overhead-gate`, which allows for the noise in both arms, fails this run on two rows, JetStream `PublishBatch` at 1KiB (+15.7%) and 64KiB (+27.7%), whose confidence intervals sit wholly above the 5% budget. The other 20 compared rows are undecided, because the noise on a shared host is wider than the gap being judged. Redis Streams batching (+10.7% to +16.5%) is the likeliest to become a confirmed breach on a quieter run.

`make overhead-gate` runs locally only. Neither CI nor the release workflow runs it, so these breaches are open work, not a release blocker.

## Reproduce it

```sh
make bench-overhead
make overhead-report RESULTS=benchmarks/reports/results/<run>/raw.txt
make overhead-gate RESULTS=benchmarks/reports/results/<run>/raw.txt.gz
```

A container benchmark on a shared host is evidence, not a verdict. Before you quote a figure, check that the run's guard verdict is clean, and repeat any run marked contended.
