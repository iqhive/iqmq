# Contributing

`CONTRIBUTING.md` in the repository root is the normative version — developer
setup, the seven rules, and the review checklist. This page is the map, plus
the things that are easier to learn from a page than from a failed CI run.

| Page | |
|---|---|
| [ADRs and RFCs](decisions.md) | where a decision goes, and the assumptions ledger |
| API freeze | what v1.0 freezes and how the baseline is held |

## The one command

```sh
make ci
```

It runs exactly what the CI workflow runs — `make ci-parity` is a check that
holds those two lists together, because they drifted once and seven of
twenty-four checks an increment passed locally were never run on a branch. Run
it before every commit.

It is CPU-heavy (race tests, fuzz seeds, TLC, and now the docs check), so run
it once at the end of a change rather than in a loop.

One figure it reports is lower than the truth, on purpose. `make ci` runs no
containers, so a driver's management API, its conformance harness and its
`Open` family are measured as uncovered while being exercised on every
`make integration` run — and those are most of what is left in the drivers
area. `make cover-integration` runs both arms over one tree and merges them,
which is the honest figure and takes about fifty minutes. Do not raise a floor
in `hack/coverage-floors.yaml` from it: the ratchet is on what a pull request
can reproduce, which is `make ci`'s profile alone.

## The rules, in one screen

1. **Correctness first.** A change to delivery, ordering, durability or
   capability semantics needs a specification reference and, where a state
   machine is involved, an update to its reference model and its TLA+ spec.
2. **Measure, don't guess.** A `perf-critical` path change ships with
   benchmarks. Every variant of a design decision is implemented and raced;
   defaults come from `make bakeoff` and are recorded in an ADR with the raw
   data.
3. **Bounded everything.** No goroutine or timer per message; every buffer has
   a bound; dropping is never a default.
4. **Standard library only** in the root module and `memory`.
5. **Fuzz what parses or holds state.** `make ratchet` fails when such a
   package has no `Fuzz*` target, and the target count only rises.
6. **Docs are product.** Every exported identifier documents its concurrency,
   ownership, blocking and error behaviour.
7. **Capability honesty.** A driver claims a capability only when the
   conformance suite proves it **positively and negatively**.

## The thing this repository will teach you either way

**Watch every new check fail before you make it pass, and quote what you saw.**

The single most-repeated defect here has been a check that could not fail —
nine instances, every one of them green. A budget matching no benchmark. A
swamped ceiling unreachable on the host it ran on. A load guard that called
its own caller foreign. `go test -bench` exiting 0 on a pattern that selected
nothing. An empty conformance manifest that made every suite skip for six
increments. A `-check` mode comparing a generated page against nothing.

So: write the check, run it against the broken tree, read the failure, *then*
fix the tree. And **your instrument is part of the code under test** — four
increments in a row found defects in their own harness, and the rule that
catches it is to suspect the harness as hard when a number is implausibly bad
as when it is implausibly good.

## Where a change goes

| Change | Home |
|---|---|
| a bug fix with a test | the package, plus a regression test that fails first |
| a new capability claim | the driver, plus a conformance scenario in **both** directions |
| a performance claim | a benchmark, a guarded run, and the run id |
| a documentation page | `docs/`, and `make docs-check` holds its links and its shape |

## Review checklist

From `CONTRIBUTING.md`, because it is the list a reviewer will actually use:

- [ ] `make ci` green
- [ ] allocation tests for new hot paths
- [ ] context cancellation and `Close` behaviour tested
- [ ] no payloads, credentials or bulk headers in logs or errors
- [ ] capabilities declared honestly; conformance run for touched drivers
- [ ] benchmarks attached for `perf-critical` changes, with the `make gate`
      verdict
- [ ] fuzz seeds added for new parsers or state machines
- [ ] an ADR written or updated for design decisions
- [ ] `apidiff` reviewed; a breaking change carries the `BREAKING:` label

## Community

Discussions are for usage questions and design proposals. Security reports go through [private vulnerability reporting](../security/index.md) and
never a public issue.
