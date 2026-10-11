# Performance rules

the ten hot-path rules, each with how it is enforced. A rule with no
enforcement is a preference, and this page says which is which.

## The ten

**1. No per-message allocation in steady state.** Every core hot path has a
`testing.AllocsPerRun` test asserting the **exact** count — not "at most", which
is a budget that ratchets upward invisibly.
*Enforced: `make allocs`, in `make ci`. Load-insensitive, so always trustworthy.*

**2. No `map`, `fmt`, `reflect`, `map[string]any` option lookups, or non-pointer
`any` boxing on a hot path.**
*Enforced: `forbidigo` in `.golangci.yml` forbids `fmt.Errorf`, `fmt.Sprintf`,
`time.Now` and `context.WithTimeout` in `internal/*` (tests exempt). Use
`internal/errcode` sentinels and the injected `internal/clock`.*

**3. Batch-fill APIs everywhere.** No goroutine or timer per message; fixed
worker pools; no unbounded channels.
*Enforced: review, plus the shape of the SPI — `Source.Fetch` fills a caller's
slice, so there is no per-message call to make.*

**4. No per-message `context.WithTimeout`; no `time.Now` unless an observer is
active; no destination re-parsing; capabilities compiled into booleans at
construction.**
*Enforced: `forbidigo` for the first two. The third is why `PublisherHandle`
ignores `Message.Topic`, and the fourth is why the optional SPI interfaces are
type-asserted once at open.*

**5. Copy-on-write subscriber lists, atomic pointers, cache-line padding on
contended counters, per-lane sharding.**
*Enforced: review and the bake-off — several of these were the candidates
raced in families, and.*

**6. Options are value types; middleware chains composed once; topic names
interned.**
*Enforced: `RunOption` and `PublishOption` are value tokens. Review option
changes for allocation and lifetime costs.*

**7. Escape-analysis review (`-gcflags=-m`) for hot files; leaf functions
inlinable.**
*Enforced: review.*

**8. `unsafe` only in `internal/queue` and `wire`**, behind build tags with a
pure-Go fallback, fuzzing and security review. Any assembly proposal must
include measurements that justify it.
*Enforced: review, plus the scoring — a tie in a bake-off is broken by
complexity, and `unsafe` costs 1000 notional lines.*

**9. Benchmarks are mandatory for a PR touching a `perf-critical` path**
(`hack/perf-critical.txt`).
*Enforced: the PR gate, and `make gate-if-touched BASE=main` before landing.*

**10. Every driver documents its tuning knobs** — linger, prefetch, pollers,
pipelining depth — **with the measured effect.**
*Enforced: review of the driver page's Tuning section.*

## the disqualifiers

Before any of the above: a benchmark must not select an implementation that
violates declared delivery semantics, has a data race, is not linearizable
where claimed, can lose an accepted message outside a documented failure
boundary, uses unbounded memory under bounded configuration, can double-release
owned memory, lets a stale ack settle a later redelivery, misclassifies an
ambiguous publish, or silently drops unsupported metadata or options.

**A faster implementation that does any of those is not a candidate.** Run
correctness checks for every candidate before measuring it.

## Running the gate

```sh
make allocs                        # always trustworthy
make bench-quick                   # smoke, seconds, never a decision
make gate-if-touched BASE=main     # intersects your diff with the gated packages
make gate BASE=main GATE=pr        # the full gate, two interleaved runs
make gate-selftest                 # prove the gate can fail
make budgets-lint                  # the budget file's own arithmetic; in make ci
```

**A green `make ci` does not mean the budgets are met.** It cannot: the gate is
two interleaved measurement runs on a quiet host and `make ci` runs several
times per change. Reading "green `make ci`" as "gates passed" is how a family
that breached a ceiling *by construction* once landed.

`make` flattens every failure to exit status 2, so when you need the gate's own
verdict run `hack/benchgate.sh` directly: 0 pass, 1 regression or breach, 2
setup error, 3 inconclusive, 4 contended, 5 the guard refused to start. A retry
loop keyed on `make gate` cannot tell a busy host from a regression.

Until October 2026 that advice was **wrong, and the direct call was no better
than the `make` one**: `hack/benchgate.sh` reached `benchmarks/cmd/benchgate`
through `go run`, which prints `exit status N` on stderr and then exits 1
itself, whatever N was. 2 and 3 were unreachable from anywhere, and 1 is the
status that means regression. The script builds the command now;
`make benchgate-test` keeps it built and fails if any `hack/*.sh` captures a
`go run` status into a variable.

## On a shared machine

Everything that measures time goes through two things with different jobs.
**`cpu-policy run --bench`** decides whether a run may START: on the shared hosts
it waits until the machine has room, takes a heavy-job lock shared with every
other repository and session there, and pins the run to the quietest whole cores,
recording which ones in `CPU_POLICY_CPUS`. **`hack/benchguard.sh`** then decides
whether the numbers may be USED: it records the load either side and refuses to
produce a verdict from a contended run, which is the question the policy cannot
answer afterwards — a neighbour who does not take the lock can still spoil a
measurement. Every `make bench-*`, `make bakeoff` and `make gate` target wires
both up; where the tool is not installed, only the second applies and nothing
else changes.

Exit **75** from the policy means the machine stayed busy for the whole timeout.
That is an answer: wait and retry, or measure later. There is no force flag,
because a forced run records numbers taken under contention.

The ceilings — 3 s per benchmark, 600 s per family, 2400 s per campaign — are
refusals the harness applies before it measures anything.

And `make bakeoff-dryrun` before a campaign: five minutes, and it answers the
one question a campaign cannot recover from, which is whether a family can be
decided at all.

[Methodology](../performance/methodology.md) is the long version.
