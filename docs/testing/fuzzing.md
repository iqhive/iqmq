# Fuzzing

130 fuzz-target functions across the repository, with a ratchet that stops the
count going down. Specification.

## What is fuzzed, and in what shape

**Parsers and encoders, for crashes and round-trips.** `wire`'s envelope
encode/decode, header packing, DSN parsing, the drivers' own native mappings
(record mapping, attribute encoding, tag settlement, error mapping). The
property is usually "decode(encode(x)) == x, and decode never panics on
anything".

**State machines, differentially.** This is the shape that finds the
interesting bugs: the fuzzer writes an **op script** — publish, receive, ack,
nack, expire, close, advance the clock — and the script is run against both the
implementation and a reference oracle, step by step, with the states compared
after every step. `internal/fuzzutil` is the script machinery, and
`Script.WriteStep` is how a seed corpus is authored in code so it stays
readable.

`FuzzDispatcher` is the runner's: it asserts that acks equal successes and that
there is exactly one settlement per delivery.

**Message-shaped data, steered.** `testkit/fuzzkit` generates the awkward
shapes from fuzz input — nil versus empty bodies, duplicate header keys, hot
keys, hierarchical topic names, maximum-length values — while guaranteeing the
structural properties the system under test relies on: a generated topic is
always a valid portable name, a generated payload never aliases the fuzz
input, sizes respect the limits. The generators have exact mirrors (`Put…`) so
a seed can be written by hand.

It depends on the standard library and `internal/` only, never on the root
package, so the lowest-layer targets can use it.

## Running it

```sh
make fuzz-seeds                    # every committed input, under -race. In every make ci.
make fuzz-smoke                    # a few seconds per target
make fuzz FUZZTIME=30s             # a real campaign, locally
make fuzz-nightly                  # the 10 minutes per target, sharded eight ways
make fuzz-targets                  # what the campaign would run (SHARDS=8 SHARD=3 for one shard)
make ratchet                       # the count only rises; parsing/stateful packages need a target
make ratchet-update                # after adding targets
```

Do not set `-parallel` by hand. A fuzz worker is a separate PROCESS with its
own `GOMAXPROCS`, and the shell starts several of those at once (`xargs -P` for
`make fuzz`, one child per shard for the nightly), so the real cost is
`jobs x workers x GOMAXPROCS`. All three come from `hack/cpucap.sh fuzz` via
`JOBS`, `SHARDS` and `FUZZ_WORKERS`, so the product stays inside
`IQMQ_MAX_CPUS` — 2 x 2 x 4 = 16 on this host. `Keep -parallel 8` is what this
page used to say, and at the current width that would have been 128 threads
against a 16-core ceiling. The scheduled campaign job takes
`FUZZTIME_NIGHTLY` (10 minutes), `SHARDS` and `SHARD`, and ends with
`fuzzmanifest -require-runs` — so **a shard that died is a failure rather than
a manifest with a hole in it**.

The campaign also records the crashers and exports the corpus before the
manifest step.

**Cadence, and why it is not nightly.** The full campaign runs **weekly**
(Wednesday 01:00 UTC), not nightly. Eight shards at ten minutes a target is about
2,000 runner-minutes per campaign, which as a nightly job was 87 % of this
repository's entire CI bill — for a library that is not under active fuzz-driven
development, that is a cost without a matching signal. Weekly is also *better*
fuzzing for the same money: `go test -fuzz` builds its corpus over the length of
a single run, so one ten-minute run a week finds more than seven ninety-second
runs would, and the corpus cache carries between them either way.

What still runs on every pull request is `seeds-and-changed`, which fuzzes the
targets your change actually touches — that is where a new crasher comes from.
The race-instrumented campaign stays weekly on Sunday, and
`workflow_dispatch` runs the full campaign on demand, which is what to use while
preparing a release.

`.github/workflows/fuzz.yml` does the same across eight runners instead of
eight background shells, and has to spell the campaign out because the pieces
land on different machines:

| step | where | why there |
|---|---|---|
| `hack/fuzz.sh shard 8 N 10m` | each shard job | one eighth of the targets |
| `make fuzz-corpus` | each shard job | the fuzz cache it reads is that runner's |
| `make fuzz-manifest-complete` | the `nightly-manifest` job | `-require-runs` is a statement about the whole target set, so only the job that has collected every shard's results can make it |

`-require-runs` asks for a run of every target the **default build** includes.
`conformance/differential`'s three `FuzzDifferential_*` targets are
`//go:build integration` and need a live broker, so they are in the manifest
with their corpus hashes and a `build_constraint` field rather than counted as
holes: the campaign enumerates 192 targets where the manifest walk finds 195,
and those three are the difference.

The weekly race campaign is sharded the same way (`hack/fuzz.sh race-shard`),
because one job over 195 targets at two minutes each never finished: it was
cancelled at its 340-minute timeout every week and reported nothing.

## Replaying a failing input

Fuzzing writes failing inputs under `testdata/fuzz/<target>/`. CI uploads
these files when a shard fails. Replay an input from its module directory:

```sh
go test -run='FuzzTarget/input-hash' ./path/to/package
```

Use `-race` when replaying an input found by a race shard. Inspect the failure
output before treating an input as a code defect: build failures and worker
termination can also interrupt a fuzz run. File issues manually with the input,
replay command and output.

## The race shards keep the race report

`hack/fuzz.sh` sets `GORACE=log_path=…` for any `-race` run. Without it a race
found by a shard reaches the log as

```
testing.go:1712: race detected during execution of test
```

Not `halt_on_error=1`: it also saves a crasher, but it kills the worker
mid-report and the report text is lost again, which is the one thing this is
for.

## `make fuzz-seeds` is the one that matters most

It runs every committed corpus input under `-race`, in every CI run. So a
crash that was found once is a crash that is checked for ever after, at the
cost of a few seconds.

Which is why committing an input is the important step after a crash, and why
`tools/fuzzcorpus export` **adds inputs and deletes nothing**: deciding which
of the thousands of interesting inputs earn a commit is a person's judgement,
and throwing one away is not a tool's decision to make.

Two committed inputs in this repository are worth knowing about because they
pin real, fixed defects:

* a native body that **begins with the envelope's magic prefix**, which
  `MetadataAdaptive` once mis-decoded as an envelope. See
  [metadata modes](../concepts/metadata-modes.md).
* a RabbitMQ delivery-tag settlement ordering that raced. It made `main`
  intermittently red *from its own committed seed*, which is exactly what a
  seed corpus is for.

## The ratchet

`hack/fuzz-ratchet.yaml` holds a `baseline_count` that only ever increases, and
a list of packages that **must** have a fuzz target. `make ratchet` (in
`make ci`) fails when the count drops or when a parsing or stateful package has
none.

It is a blunt instrument and deliberately so: the failure mode it prevents is a
refactor that quietly deletes a target, which is invisible in a diff and
removes coverage that took a machine-week to earn.

## Writing one

```go
func FuzzMyMapping(f *testing.F) {
    // A seed corpus authored in code, so it is readable in review: the Put
    // functions are the exact mirrors of the Gen functions.
    var w fuzzutil.Writer
    fuzzkit.PutTopic(&w, fuzzkit.Default, "orders.eu")
    fuzzkit.PutPayload(&w, fuzzkit.Default, somePayload)
    f.Add(w.Bytes())

    f.Fuzz(func(t *testing.T, in []byte) {
        r := fuzzutil.NewReader(in)
        topic := fuzzkit.GenTopic(r, fuzzkit.Default)   // always a valid portable name
        p := fuzzkit.GenPayload(r, fuzzkit.Default)     // never aliases `in`
        // …encode, decode, compare. Never accept a partial round trip.
        _ = topic
        _ = p
    })
}
```

`fuzzkit.GenTopicUnchecked` is the deliberate exception, for a target whose
subject *is* the validator.

For a state machine, `fuzzutil.Script[S]` is the op-script interpreter: `Steps`
are the available operations over your state `S`, `MaxSteps` bounds the run,
and **`Check` runs after every step** — which is where a differential
comparison or an invariant assertion belongs, because a violation found three
steps later is a violation you have to bisect by hand.

One caveat: `internal/fuzzutil` is internal to this repository, so a driver
written outside it has `testkit/fuzzkit` (which is published) and its own
script machinery. That asymmetry is recorded rather than hidden.

Three rules from this repository's own corpus:

**Assert the property, not the absence of a panic.** A fuzz target that only
checks for crashes finds crashes. One that checks `decode(encode(x)) == x`
finds *wrong answers*, which are more common and worse.

**Compare against an oracle where one exists.** For the engine that is the
reference model; for a codec it is the standard library; for a driver's mapping
it is the client library's own round trip. A differential target is the
strongest shape available.

**Never let the generated data alias the fuzz input.** `fuzzkit` guarantees
this for you. A target that hands the fuzzer's own slice to something that
retains it produces failures that do not reproduce.

## Where a crash goes

A crash writes an input under `testdata/fuzz/<Target>/`. Commit it with a
`testdata/fuzz/<Target>.README` line saying what it reaches — the existing ones
do, and a corpus input whose purpose nobody recorded is an input the next
person deletes during a cleanup.
