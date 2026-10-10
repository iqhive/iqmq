<p align="center">
  <img src="docs/hero.svg" width="836" alt="Animated iqmq terminal: hexadecimal wordmark, a JSON order published and received by the memory driver, Nack redelivery followed by Ack, the pre-alpha quick-start card, and the IQ Hive logo.">
</p>

<p align="center">
One small, capability-honest, bytes-first Go API for publishing and consuming
messages across Kafka, NATS/JetStream, NSQ, Google Cloud Pub/Sub, Amazon SQS,
SNS+SQS, RabbitMQ, ActiveMQ, Redis Streams and Pulsar — plus a deterministic
in-process memory engine that is also the canonical test double.
</p>

<p align="center">
<a href="https://github.com/iqhive/iqmq/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/iqhive/iqmq/ci.yml?branch=main&label=ci" alt="CI"></a>
<a href="https://github.com/iqhive/iqmq/releases"><img src="https://img.shields.io/github/v/release/iqhive/iqmq?label=release&include_prereleases" alt="Release"></a>
<a href="https://pkg.go.dev/github.com/iqhive/iqmq"><img src="https://pkg.go.dev/badge/github.com/iqhive/iqmq.svg" alt="Go Reference"></a>
<a href="https://github.com/iqhive/iqmq/actions/workflows/ci.yml"><img src="https://img.shields.io/badge/coverage-unpublished-inactive" alt="Coverage"></a>
<a href="https://github.com/iqhive/iqmq/actions/workflows/fuzz.yml"><img src="https://img.shields.io/github/actions/workflow/status/iqhive/iqmq/fuzz.yml?branch=main&label=fuzz" alt="Fuzzing"></a>
<a href="docs/performance/index.md"><img src="https://img.shields.io/badge/benchmarks-provisional-orange" alt="Benchmarks"></a>
<a href="https://goreportcard.com/report/github.com/iqhive/iqmq"><img src="https://goreportcard.com/badge/github.com/iqhive/iqmq" alt="Go Report Card"></a>
<a href="https://scorecard.dev/viewer/?uri=github.com/iqhive/iqmq"><img src="https://img.shields.io/ossf-scorecard/github.com/iqhive/iqmq?label=scorecard" alt="OpenSSF Scorecard"></a>

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

</p>

## Thirty seconds

No broker, no container. The in-process engine is a real driver — the same
`Backend`, the same runner, the same settlement state machine as Kafka — so
this is the actual code path.

```go
ctx := context.Background()
client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
if err != nil {
    panic(err)
}
defer func() { _ = client.Shutdown(ctx) }()

consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
if err != nil {
    panic(err)
}
handled := make(chan struct{})
go func() {
    _ = consumer.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
        fmt.Printf("order %s: %s\n", d.Key, d.Body)
        close(handled)
        return nil // ack
    }), mq.WithConcurrency(8), mq.WithOrderingByKey())
}()

if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
    panic(err)
}
<-handled
// Output: order cust-42: {"id":1}
```

That is `Example` in `example_test.go`, verbatim — a **godoc example with a
checked output**, so it is compiled and run by `go test` during the Go test run. A
quick start that does not compile is the first documentation defect anybody
meets.

`memory.Open` is the only line that names a driver. Swapping it for
`dsn.Open(ctx, "kafka://…")` is the whole of what the specification promises about changing
brokers — with the honest caveat that the *capabilities* change too, which is
what [capability negotiation](docs/concepts/capabilities.md) is for.

## Why iqmq

**Against writing to a raw client** — you get one API instead of eleven, with
retries, backoff, attempt counting, dead-lettering, lease extension, ordering
and graceful drain implemented once and the same way everywhere, instead of
once per broker with a different bug in each. The measured price of the
abstraction is published, including where it is over budget.

**Against Watermill and gocloud.dev** — capabilities are a **five-level
typed model that is generated from conformance evidence**, not prose. Asking a
broker for something it cannot do fails at construction or before any I/O with
`ErrUnsupported`; it never silently becomes an emulation you did not choose.
A capability nothing probed renders differently from one a probe verified.

**Bytes-first, not `interface{}`-first.** `Message.Body` is `[]byte`, headers
are ordered and binary-capable, and codecs are an optional layer on top. No
reflection anywhere on a hot path, and no per-message allocation in steady
state — every hot path has an `AllocsPerRun` test asserting the **exact**
count.

**One Go module per driver.** Depending on this library costs you the one
broker SDK you chose, not eleven. The root module and the memory driver depend
on the standard library only, and a check enforces it — a build-time property
and a security one.

**The test double is the real thing.** `memory.NewForTest(t)` gives you a
fake clock, a fault injector and a goroutine-leak check over the *same* engine
your production driver's runner uses — and under `testing/synctest` you can
assert the **schedule** of a retry, not just that it eventually succeeded.

## What it guarantees, and what it does not

**Guaranteed** (and probed, per driver, in both directions):

* **at-least-once delivery** by default, with explicit settlement, generation
  tokens that stop a stale handle settling a newer delivery, and exactly one
  concurrent terminal attempt winning.
* **the dead-letter order**: publish to the destination, confirm it, *then* ack
  the original. A refused or ambiguous dead-letter publication requeues the
  original and is counted; it never acks.
* **bounded everything.** Every buffer has a default bound and there is no
  value that turns it off. Fixed limits imply a memory plateau, asserted by a
  model fuzzer after every step.
* **an unknown stays unknown.** An ambiguous publish, an ambiguous settlement
  and an unadvertised limit each have a representation of their own.
* **errors carry no payload, key, header value, credential or address**, so a
  log line is safe to ship anywhere.

**Not guaranteed, and deliberately so:**

* **exactly-once processing.** Not provided, and not providable at this layer.
  Broker-scoped exactly-once *delivery* is reported as a capability; your side
  effects want an idempotent handler or an inbox table.
* **global ordering.** Never implied. Order is a capability with a scope, and
  four conditions have to hold.
* **no duplicates.** At-least-once means the window between your handler
  returning and the ack landing is real, and a process that dies in it
  produces a duplicate.
* **durability from an emulation.** `EmulatedVolatile` never satisfies a
  durability requirement, and `ConfirmDurable` **fails at construction** on a
  profile with no meaningful durable boundary — NATS Core and the memory
  engine among them.
* **that the specification's own budgets are met.** Ten of the twelve
  measured batched overhead rows are missed. See the strip below.

## How it fits together

<p align="center">
  <img src="docs/assets/architecture.svg" alt="iqmq architecture: your code, the core, the SPI, driverkit, the drivers, the brokers" width="960">
</p>

Every policy the specification names lives in the core and none of it lives in
a driver — the specification puts that as a prohibition, because two owners of redelivery
accounting produce a message retried 25 times when you asked for 5. A driver
implements three interfaces; everything a broker does *differently* is an
optional interface the core type-asserts once, at open, and never on a
message's path.

The ack/nack/retry/dead-letter sequence, read out of the runner rather than
drawn from the obvious shape:

<p align="center">
  <img src="docs/assets/settlement-sequence.svg" alt="The settlement sequence: fetch, dispatch, ack, and what a failed delivery becomes" width="980">
</p>

Four details in it are easy to get wrong and are why the picture is here:
`Halt` stops the fetch **before** it requeues; a dead-letter directive with no
destination **halts** rather than dropping; the dead-letter publication is
confirmed before the ack; and the attempt bound is **unreachable** under
relaxed ordering on a broker that keeps no delivery count, so such a message
is retried for as long as it fails.

## Drivers

Every driver carries a maturity level and a maintenance tier; the
[selection guide](docs/drivers/index.md) compares them and each
[driver page](docs/drivers/) has the fifteen fixed sections.

Driver capabilities are available through the client API. Run fresh conformance
tests against your broker and configuration before relying on a capability.

<!-- matrix:start -->
<!-- matrix:end -->

## Performance

Run `go test -run=^$ -bench=. -benchmem ./...` for core benchmarks. See the
[benchmark methodology](docs/performance/methodology.md) for driver and native-client comparisons.

## Install

```sh
go get github.com/iqhive/iqmq                    # the core, standard library only
go get github.com/iqhive/iqmq/drivers/kafka      # and one module per broker
```

**One module per driver**, so importing the core never pulls in a broker SDK.
The root module and `dsn/global` depend on the standard library and nothing
else; each driver brings its own client library and only its own, so a program
that talks to Kafka never compiles the AWS SDK.

Core minors ship as a **release train** — every official module is tagged
together, so `iqmq v1.4.0` and `iqmq/drivers/kafka v1.4.0` are a matched pair.
Patches are independent.

The full table of modules and DSN schemes:
[docs/getting-started/installation.md](docs/getting-started/installation.md).

## Concepts

Eleven pages for the semantic model, each answering the question a reader
arrives with:

| | |
|---|---|
| [Delivery guarantees](docs/concepts/delivery-guarantees.md) | what an ack means, and what it does not |
| [Duplicates and idempotency](docs/concepts/idempotency.md) | where every duplicate comes from |
| [Acks and leases](docs/concepts/acks-and-leases.md) | what happens between received and settled |
| [Groups and subscriptions](docs/concepts/subscriptions.md) | who gets which message |
| [Backpressure](docs/concepts/backpressure.md) | what happens when a producer outruns a broker |
| [Confirmation boundaries](docs/concepts/confirmation.md) | what `nil` from `Publish` actually means |
| [Ambiguous publishes](docs/concepts/ambiguous-publishes.md) | what if nobody knows |
| [Capability negotiation](docs/concepts/capabilities.md) | how to find out what this broker can do |

## Observability

the observation points, opt-in and costing nothing when unset. There is no
package-level logger and no `init()`-time side effect: what you get is hooks,
stats snapshots and errors carrying one of the twenty-six codes.

```go
client := mq.NewClient(backend, mq.WithHooks(m.Hooks()))
consumer.Run(ctx, handler, mq.WithMiddleware(middleware.Metrics(m.Recorder(), nil)))
```

  the canonical names, which are the API a dashboard is written against.
  the histograms and gauges, and W3C trace context carried in a message's
  headers.
  **Capacity**,
  **Troubleshooting** — and the gaps,
  which are stated: the specification has no shutdown hook, so a drain that exceeded its
  timeout is not a metric.

## Testing and fuzzing

**The double is the driver.** `memory.NewForTest(t)` turns on `AutoCreate`,
installs a fake clock and registers a goroutine-leak check on `t.Cleanup` —
over the same engine, runner and settlement machine as production. Its
capability table is generated from a real conformance run like every other
driver's.

**Never sleep.** `broker.Advance(d)` drives the scheduler, so delayed
delivery, TTL, lease expiry and retry backoff are all deterministic. Under
`testing/synctest` you can assert the *schedule*:

```go
func TestChargeWorkflowInABubble(t *testing.T) {
    const base = time.Hour
    synctest.Test(t, func(t *testing.T) {
        client, broker := memory.NewForTest(t, memory.TestRealClock())
        if err := broker.Advance(time.Second); err == nil {
            t.Fatal("Advance succeeded on a broker left on the wall clock")
        }
        start := time.Now()
        runWorkflow(t, client, base)
        // Two failures at a doubling backoff based at an hour: exactly three
        // hours of bubble time. A runner that stopped waiting, capped the
        // delay, or drew it from a jitter envelope would change this number
        // and nothing else in the test.
        if got, want := time.Since(start), base+2*base; got != want {
            t.Errorf("took %v of bubble time, want exactly %v", got, want)
        }
    })
}
```

**Inject rather than arrange.** `broker.Faults()` is the injector — reject
the *n*th publication, fail a settlement, force a duplicate, expire leases,
fail a dead-letter publication — and every fault carries a counter, so a fault
that fired **zero** times tells you the test never reached the path it was
written for.

**130 fuzz targets**, with a ratchet that stops the count falling and requires
one in every package that parses or holds state. `make fuzz-seeds` runs every
committed input under `-race` in **every** CI run, so a crash found once is
checked for ever. The interesting targets are differential: a fuzzer-written
op script run against the implementation and a reference oracle, compared after
**every** step.

Testing · synctest ·
testkit ·
Conformance ·
Fuzzing

## Compatibility and versioning

SemVer. **`v0.x` while the API settles**, with breaking changes announced and
migration notes. From **v1.0** the root package and the SPI are stable, and
growth happens through optional interfaces and concrete handles — adding a
method to a public interface breaks everyone who implemented it.

`make apidiff` holds the public surface against a committed baseline, so a
change to it is deliberate rather than discovered by a user. Deprecations are
kept for at least two minor releases with `Deprecated:` markers; Go version
bumps happen only on minors, and are announced.

API freeze ·

## Security

Broker payloads are untrusted: limits are enforced **before allocation**, the
wire envelope is never auto-detected unless you configured it, and every
asynchronous and retry buffer is bounded. Credentials never render — each
driver's `Config` redacts through `String`, `GoString`, `LogValue` and `DSN`,
with a test that pushes it through a real `log/slog` handler — and 
forbids an error from carrying a body, key, header value, credential or
address.

TLS is on by default for remote profiles; any option that disables it has
`Insecure` in its name; contradictory TLS settings are rejected at
construction rather than resolved by guessing.

Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/iqhive/iqmq/security/advisories/new).
See the security guide for the reporting process and
the design.

## Contributing

`make ci` runs the local checks. `make ci-parity` checks their coverage
in the GitHub workflow. Include a negative case when adding a check
so its failure path is exercised.

[`CONTRIBUTING.md`](CONTRIBUTING.md) ·
Contributing guide ·
Performance rules ·
[Design decisions](docs/contributing/decisions.md)

Questions go to Discussions. Report bugs through Issues with the `iqmq doctor`
output requested by the issue form.

## Layout

* The root module and memory driver use the Go standard library.
* Broker drivers, integrations and codecs have separate modules.
* `internal/` contains implementation variants and benchmark tooling.
* [`llms.txt`](llms.txt) links the main documentation pages.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

## Licence and trademarks

The first-party software in this repository is licensed under the
[Apache License, Version 2.0](LICENSE). Third-party components and any
files with separate licence notices remain subject to their respective
terms.

The Apache licence does not grant general permission to use IQ Hive
trademarks. See [Trademarks](TRADEMARKS.md).

See [Contributing](CONTRIBUTING.md) for contribution terms and guidance.

## Release verification

Local release tags span v0.5.0 through v0.9.0 and are lightweight references to commits. They carry no tag messages, tagger identities, dates or signatures. The commits are unsigned. GitHub publication and release-artefact signatures have not been verified.

Tags before v0.9.0 use the `bitbucket.org/iqhive/iqmq` module path. The v0.9.0 release train uses `github.com/iqhive/iqmq`.
