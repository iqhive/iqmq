# Formal models

TLA+/PlusCal specifications of the highest-risk state machines
(the test suite), each mirrored by an executable Go reference model
under `internal/model/<name>`:

| Spec | Go model | Properties |
|---|---|---|
| `Settlement` | `internal/model/settle` | one winner per delivery, stale generation rejected, no double terminal, ambiguous stays unknown |
| `MemoryLease` | `internal/model/memq` | no loss of accepted records, capacity conservation, no early delivery, redelivery after expiry, stale generation rejected |
| `FanoutReserve` | `internal/model/fanout` | atomic all-or-nothing reservation, no capacity leak, deadlock freedom |
| `KafkaCommit` | `internal/model/commit` | committed offset never crosses a gap, stale generation rejected, monotonic |
| `Shutdown` | `internal/model/shutdown` | phase monotone, DLQ publish before ack, no lost in-flight work, idempotent close |

`TRACE.md` maps every TLA+ action and invariant to the Go function and model
invariant that implements it, and lists what has no counterpart on the other
side. `go run ./tools/modelcheck` holds that mapping to the code: every
invariant a configuration checks must be defined by its module, documented by
its Go model, given a row in `TRACE.md` that names a model check and an
implementation test that exists, given a case in the model package's
`TestInvariantsHaveTeeth`, and given a mutation in `mutations.txt`.

## Watching a specification fail

A model checker that has never reported a violation is not evidence.
`mutations.txt` carries one deliberate defect per checked invariant -- a
weakened guard or a broken next-state relation -- and

```
make tla-mutate                                  # ~42 s for all 50
go run ./tools/tlatools mutate -only Shutdown    # one specification
go run ./tools/tlatools mutate -list             # what each defect is
```

applies each to a throwaway copy of this directory under a configuration
reduced to the single invariant it targets, and fails when TLC does *not*
report that invariant violated. `MUTATIONS.md` is the recorded output: the
defect, and the first two states of the counterexample. Three invariants could
not fail at all before this existed.

## Running TLC

```
go run ./tools/tlatools -pin          # first run: download the pinned jar and record its digest
make tla                              # model-check every spec (27.8 s for all eleven)
make tla SPEC=Settlement              # one spec
make tla TLA_WORKERS=16 TLA_XMX=8g    # on a host of your own
```

`make tla` runs TLC with two worker threads and a 2 GB heap, which is a
footprint decision rather than a speed one: the configurations are exhaustive
in seconds and the development host is shared with about fifty containers and
several agents.

Configurations are deliberately small (two racers, two generations, capacity
one or two) so that the state space is exhaustive within minutes; they explore
interleavings rather than scale. Model-checker output goes to the ignored
`states/` directory.
