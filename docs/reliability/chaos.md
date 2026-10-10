# Chaos, soak and differential testing

Proving the reliability claims by breaking things on purpose, and by running
the same operation script against two implementations and comparing what they
settled.

Everything on this page is a `make` target, and `make suites-modes` prints the
state of every run mode, which is the generated answer to "what actually
runs?":

```
chaos         implemented  conformance/chaos TestChaos_*
soak          implemented  conformance/soak TestSoak_*
differential  implemented  conformance/differential FuzzDifferential_*
matrix        implemented  conformance/matrix TestMatrix_*
```

A suite may only declare a run mode this repository implements, so
`the conformance suites manifest` and the mode registry are checked against each other
in `make ci` (`make suites-check`). A mode declared and not implemented used to
turn a skipped job into one that failed at a step that could not pass.

## What each mode is for

| Mode | Question | Cost | When |
|---|---|---|---|
| `make chaos DRIVER=x` | does the driver keep and the promises while the network is broken? | a broker and a Toxiproxy | nightly subset, weekly `EXTENDED=1` |
| `make soak DRIVER=x TIME=4h` | does anything grow without bound? | one broker for TIME | weekly |
| `make differential DRIVER=x` | is what the broker did a legal reading of what the memory engine did? | a broker per seed | nightly |
| `make matrix DRIVER=x` | do the claims that depend on the broker's VERSION actually change with it? | one broker per image | nightly |

## Chaos: the L3 fault scenarios

`conformance/chaos` holds six scenarios, each naming the specification it is
evidence for:

| Scenario | Fault | What must hold |
|---|---|---|
| `reset-mid-burst` | `reset_peer` 200 ms into every connection | no loss, and the client recovers within 30 s |
| `latency-over-timeout-during-ack` | 4 s downstream latency during settlement | at least one `ErrSettlementUnknown`, then duplicates and no loss |
| `session-timeout-invalidates-the-generation` | the proxy is disabled: the socket answers nothing | no loss across the rebalance |
| `bandwidth-bounds-backpressure` | 8 kbps upstream | the publisher buffer stays inside its bound |
| `container-stop-start-reconnects` (weekly) | the broker container is stopped | the workload resumes within 90 s |
| `topology-deletion-is-refused-or-degrades` (weekly) | the topic is deleted through `Client.Admin` | an operation is refused, or `Health` reports degraded |

Three things about the design are worth copying if you write your own.

**A chaos run in which nothing failed has tested nothing.** Every scenario's
first check is `FaultBit`, which requires the injected fault to have made at
least one operation fail. A Toxiproxy toxic silently does nothing more often
than it does anything — the wrong stream, a proxy name that does not match, a
workload whose own timeouts outlast the toxic — and each of those produces a
run in which every property holds trivially. Two of the four nightly scenarios
reported exactly that against a live broker with a live toxic in place, because
the fault was being injected after the consumer's prefetch buffer was already
full: the remaining work was local and could not fail. The workload now runs in
**two halves with the fault injected between them**.

**A message that was never fetched is not a message that was lost.** The
ledger's "missing" column counts accepted publishes with no delivery, and it
only means loss once the final drain has *converged* — three consecutive empty
receives that did not error. A run whose client never reconnected reported
eight messages as lost that the drain had never had a chance to fetch. `Run`
records `Drained`, `NoLoss` declines to claim loss without it, and
`DrainConverged` is the check that says so.

**A settlement you cannot fail is a settlement whose loss you cannot detect.**
`latency-over-timeout-during-ack` needs an acknowledgement whose fate is
genuinely unknown, and on JetStream an `Ack` without `Config.DoubleAck` returns
as soon as the frame is in the write buffer — there is no round trip for a
latency toxic to straddle. The chaos arm therefore sets `DoubleAck`, and the
scenario then reports 23 ambiguous settlements, 9 duplicates and no loss, which
is the contract exactly.

The verdicts are pure functions of a `Run` struct, and `verdict_test.go` hands
every one of them a run it must accept and a run it must reject. That half runs
in `make ci`, with no container.

## Soak: three grades, never two

`make soak DRIVER=x TIME=4h` runs the L4 workload — publish, consume, settle,
a reconnect storm every TIME/20 and a lease left to expire every TIME/40 — and
samples the live heap, the live object count and the goroutine count
throughout.

The verdict has **three** grades, and that is the whole point:

* **`fail`** — a fitted growth rate above a ceiling *and* a material absolute
  increase over the fitted window.
* **`pass`** — no such growth, over a run long enough to say so.
* **`inconclusive`** — the run was shorter than ten minutes, took fewer than
  eight samples, or its workload made no progress.

A ten-minute soak that reports "no leak" has not tested a leak slower than ten
minutes, so a short run cannot grade `pass` however flat its samples are. The
grade is printed beside the duration every time, and a run's JSON artefact
carries both.

Two details that were wrong on the first attempt and are worth knowing:

* the slope is fitted to the **tail** of the series, not to the first and last
  samples. A process that allocates its buffers in the first thirty seconds and
  then holds them steady has a large first-to-last difference and no leak.
* a slope alone cannot fail a run. A per-hour rate extrapolated from a
  two-second window multiplies by eighteen hundred, and a healthy two-second
  run graded `fail` on "+51946 objects/h" of ordinary allocation noise. A
  failure needs the fitted window to span at least a minute **and** a material
  absolute increase — four megabytes of live heap, ten thousand live objects,
  two goroutines.

## Differential: the relation, written down first

`make differential DRIVER=x` runs one operation script against the in-process
memory engine and against the broker, and compares.

It cannot compare the two for equality. A broker may interleave two keys
differently, redeliver a message whose settlement it did not see, and answer a
publish ambiguously — so an equality check either fails on every run or is
weakened until it passes, and after that it is a check that cannot fail. What
is compared instead is a relation:

* every id **both arms accepted** is delivered at least once by both;
* no id the subject **refused** is delivered, and no delivery carries an id
  nobody published;
* the **settled multisets** agree: an id the oracle acked is acked by the
  subject too, and an id the oracle dropped is not;
* per key, the order in which ids are **first delivered** is the same on both
  arms — a redelivery cannot move a first delivery, which is what makes the
  ordering half insensitive to exactly the behaviour brokers differ on;
* a message the script **nacked** comes back and one it **dropped** does not.

Every one of those is conditional on what the driver claims.
`PermissionsFor(caps)` reads the permissions off the capability table, so a
driver that advertises `FeatureOrderedByKey` is held to per-key order and one
that advertises nothing is held to loss and settlement only. The only way to
make a failing ordering run pass is to change the claim — at which point the
conformance suite's negative probes require the driver to refuse what it no
longer claims. Each run logs which half of the relation actually bound it
(`held to [...]`, `not checked [...]`) and how many ids it compared, because
"no divergence" over zero compared ids is not a result: `KindVacuous` is a
divergence.

The relation is a pure function of two observations, so it is held to a table
of nineteen transformations — six a conforming broker may make, which it must
accept, and thirteen injected defects, which it must reject **under their own
kind**. The same table is then applied to the observation a real broker
produced, because a relation that only ever saw a hand-written fixture could
depend on something the fixture has and a run does not.

## Recording a run

Record the source revision, broker image, command and outcome when
running a campaign. A scenario being implemented does not establish
that it passes for a particular driver or deployment.

## Doing it yourself

`make broker-up NAME=toxiproxy` starts the proxy, and the compose files under
`hack/compose` start each broker from the digests
`conformance/harness/images.lock` pins.

Three rules if you do, all learned here. Start **one broker at a time** and
remove your own containers **by id** — a filtered `docker rm` has taken out
other people's work on a shared machine. Read a fault-injection result with the
same suspicion as a benchmark: a number that is implausibly bad is as likely to
be the instrument as the code, and three of the defects on this page were in
the instrument. And when a chaos run is green, read what it says it *did* —
`faulted=`, `unknown=`, `drained=`, `compared=` — rather than the word "PASS".
