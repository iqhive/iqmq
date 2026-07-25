# TLA+ ↔ Go trace table

Maps each specification to its Go reference model, and each action or
invariant to the code that implements it. Updated whenever a spec or a model
changes.

`tools/modelcheck` (in `make ci`) reads this file and fails when the
cross-reference is not real. It checks all eleven configurations, resolving
each sibling to the module it extends, and requires of every invariant a
`.cfg` checks that

1. the module defines it and the Go model's `Invariants` documents it by name;
2. it has a row in this file naming a Go model check and an implementation
   test, and that every `Test`/`Fuzz` name this file cites exists in the
   repository (one did not: the `StrictBlocksLane` row cited a
   TestStrictBlocksLane, spelled without the backticks here so that this
   sentence is not itself a citation, and no such test has ever existed);
3. it has a case in the model package's `TestInvariantsHaveTeeth`, which
   forces the fault and asserts the invariant of that name reports it — a
   documented bullet with no evaluation behind it passes every other check;
4. it has a deliberate defect in `models/tla/mutations.txt` under which TLC
   reports *that* invariant violated. `make tla-mutate` runs all fifty of
   them in about 40 s and `models/tla/MUTATIONS.md` records each
   counterexample's first two states.

"What has no counterpart on the other side" is the section below; read it
before trusting a row.

| Spec | Go model |
|---|---|
| Settlement | internal/model/settle |
| MemoryLease | internal/model/memq |
| FanoutReserve | internal/model/fanout |
| KafkaCommit | internal/model/commit |
| Shutdown | internal/model/shutdown |

## What has no counterpart on the other side

A specification invariant nothing in Go can violate is decoration; a Go model
invariant no specification states is unverified. Both lists are kept here, and
both are checked mechanically as far as a name can be: `tools/modelcheck`
fails on a configured invariant the model does not document, on one with no
mutation, and on one with no case in `TestInvariantsHaveTeeth`.

### Names with no counterpart

| Direction | Names | Note |
|---|---|---|
| A `.cfg` invariant with no Go counterpart | none | every name in all eleven configurations is documented and evaluated by its model's `Invariants`, and has a case in that package's `TestInvariantsHaveTeeth` |

Two of the three names that used to sit in the second list were fixed rather
than listed, because the invariant was real and only the evaluation was
missing: `fanout`'s `NoUnreservedAdmission` now reports the ledger check that
records-held equals reservations-enqueued, and `memq`'s
`NoUnadmittedSettlement` now reports the inequality half of `Conservation`'s
sum.

### Invariants that only a mutation can falsify

These are negative checks: in every reachable state of the *correct*
specification the flag they read is false, so the exhaustive run says nothing
about them and `make tla-mutate` is the only evidence they work. That is by
design — they exist to catch a change to the next-state relation — but a run
that reports "no error" is not evidence for them, which is why the mutation
harness is in the gate.

| Invariant | Spec | Why it cannot fail in a correct run | Mutation |
|---|---|---|---|
| `StaleGenRejected` | KafkaCommit | `staleAccepted` is set only inside a branch guarded by `h.g = gen`, so the flag is constant FALSE | `KafkaCommit/StaleGenRejected` |
| `DropSlowAccounted` | FanoutReserve | `everDropped` is set only in the DropSlow branch, so the implication's antecedent and its consequent cannot both be true | `FanoutReserve/DropSlowAccounted` |
| `StaleRejected` (second conjunct) | Settlement | generation 0 is never leased, so `settledCount[0]` cannot rise | `Settlement/StaleRejected` |
| `NoUnadmittedSettlement`, `NoUnreservedAdmission` | MemoryLease, FanoutReserve | both are action properties over transitions the correct module never takes | `MemoryLease/NoUnadmittedSettlement`, `FanoutReserve/NoUnreservedAdmission` |

### Invariants one configuration cannot falsify

An invariant guarded by a constant is vacuous in the configurations that turn
its antecedent off. That is the reason for the six sibling configurations, and
the reason the mutation for each such invariant names the configuration that
*can* falsify it.

| Invariant | Vacuous in | Falsified under |
|---|---|---|
| `AtomicAllOrNothing` | `FanoutReserve{Independent,BestEffort,DropSlow}` (`Policy # "atomic"`) | `FanoutReserve` |
| `Isolation` | `FanoutReserve` (`Policy = "atomic"`), and under `Cap = 1` it is implied by `NoOverflow` everywhere | `FanoutReserveIndependent` |
| `DropSlowAccounted` | `FanoutReserveDropSlow` (the antecedent is `Policy # "dropslow"`) | `FanoutReserve` |
| `StrictBlocksLane` | `MemoryLease{Relaxed,DropNewest}` (`Sealing = FALSE`) | `MemoryLease` |
| `DeadLetterBeforeSettle` | `MemoryLeaseRelaxed` (`HasDLQ = FALSE`, so no record reaches `dlqpending` or `dead`) | `MemoryLease` |
| `OverflowAccounted` (each arm) | every configuration but the one whose `Overflow` the arm names | `MemoryLease` (reject: both arms are negative) |

### Conjuncts that carry nothing

Found while writing the mutations; none is a defect that hides a bug, and each
is recorded rather than quietly reworded, because a reader who trusts the name
is entitled to know what the formula says.

| Where | What |
|---|---|
| `MemoryLease.OneState`, second conjunct | `state[r] \in Terminal => state[r] \notin Held` is a tautology: `Terminal` and `Held` are disjoint by construction. The invariant's content is its first conjunct, `(owner[r] # None) <=> (state[r] = "inflight")` |
| `MemoryLease.Conservation`, first conjunct | `\A r : state[r] \in States` repeats `TypeOK`'s first conjunct; the second conjunct is the one about refused records |
| `MemoryLease.AttemptsMonotone` | states that a record with attempts is not "new" — not monotonicity. The Go model's bullet of that name says the same thing, so the pair is sound and only the name is ambitious |
| `FanoutReserve.ReservationBalance` | was `Idle(p) => (res[p] = {} /\ refused[p] = {})`, and `Idle(p)` is *defined* as those sets being empty: `(A /\ B /\ C) => (B /\ C)`. Restated in as the partition of `todo`, `res` and `refused`, which the harness falsifies |
| `internal/model/commit.SafeWatermark` | sat behind a `TypeOK` conjunct that rejected any delivered offset below the watermark, so no state could reach it. `TypeOK` now checks that settled history is dropped and `SafeWatermark` reports the watermark passing unfinished work |
| `internal/model/memq.NoEarlyDelivery`, first half | "a delayed record is not past its due time" is reachable only when a retry entry's due time and the record's own disagree — `OneState` catches every other route to it. `TestInvariantsHaveTeeth/NoEarlyDelivery/delayed` forces exactly that |

## Actions and invariants

### Settlement

`models/tla/Settlement.tla` (constants: `Racers = {r1, r2}`, `MaxGen = 2`;
native outcomes commit / reject / ambiguous) ↔ `internal/settlement.Cell`
↔ `internal/model/settle.Model`.

| TLA+ action | Go implementation | Go model op |
|---|---|---|
| `Lease` | `(*Cell).Lease` | `settle.Lease` |
| `Receive(r)` | the engine hands a `Gen` (or `Token`) to a consumer | harness: racer reads `Cell.Load` |
| `Forge(r)` | a stale or foreign handle | harness: racer holds an arbitrary `Gen` |
| `Begin(r)` | `(*Cell).Begin` | `settle.Begin` |
| `Retry(r)` | `(*Cell).Retry` | `settle.Retry` |
| `Commit(r)` | `(*Cell).Commit` | `settle.Commit` |
| `Reject(r)` | `(*Cell).Reject` | `settle.Reject` |
| `Ambiguous(r)` | `(*Cell).Ambiguous` | `settle.Ambiguous` |
| `Revoke` | `(*Cell).Revoke` | `settle.Revoke` |
| `Close` | `(*Cell).Close` | `settle.Close` |

| TLA+ variable | Go |
|---|---|
| `state`, `gen` | bits 0–2 and 32–63 of the cell word; `settle.Delivery.State/Gen` |
| `pending` | bits 8–9 (revoke / close pending); `settle.Delivery.Pending` |
| `owner` | not stored by the cell (the winner is whoever holds the nil from `Begin`); `settle.Delivery.Owner` |
| `settledCount`, `lost` | `settle.Delivery.Settled`, `settle.Delivery.Lost` |

| TLA+ invariant / property | Go model check (`(*Model).Invariants`) | Implementation test |
|---|---|---|
| `TypeOK` | `TypeOK` | `TestPackUnpack`, `TestTransitionMatrix` |
| `OneWinner` | `OneWinner` | `TestOneWinnerRace`, `FuzzSettlementConcurrent` (in-flight counter) |
| `NoDoubleTerminal` | `NoDoubleTerminal` | `TestLifecycle` (second Commit fails), `FuzzSettlementConcurrent` (commits per generation ≤ 1) |
| `StaleRejected` | `StaleRejected` | `TestTransitionMatrix` (every stale gen → `ErrLeaseLost`), `FuzzSettlementConcurrent` |
| `LeaseLostNeverSettles` | `LeaseLostNeverSettles` | `TestRevokeRace`, `FuzzSettlementConcurrent` (lost ∩ committed = ∅) |
| `UnknownSticky` | `UnknownSticky` | `TestLifecycle` (Begin on unknown → `ErrSettlementUnknown`; Revoke/Close no-op) |
| `SettledSticky` | `SettledSticky` | `TestLifecycle` (`ErrAlreadySettled`; only Lease leaves) |
| `ClosedSticky` | `ClosedSticky` | `TestLifecycle` (`ErrClosed`; only Lease leaves) |
| `GenMonotone` | `GenMonotone` | `TestLeaseSkipsZeroOnWrap`, `TestLifecycle` |

The errcode result of a losing `Begin` (which sentinel) is not modelled in
TLA+; it is defined by `settle.Model.Step` and checked against the
implementation by `FuzzSettlement` (sequential differential) and `TestModel`.

### KafkaCommit

`models/tla/KafkaCommit.tla` (constants: `MaxOffset = 4`, `Window = 3`,
`MaxGen = 2`, so offsets 0..4 and one revocation) ↔ `driverkit/offset.Tracker`
(six kinds) ↔ `internal/model/commit.Model`.

| TLA+ action | Go implementation | Go model op |
|---|---|---|
| `Deliver(o)` | `Tracker.Track` (`o > high` is the gap the broker leaves) | `commit.Track` |
| `Complete(h)` | `Tracker.Complete` | `commit.Complete` |
| `Commit` | the driver's offset commit; the tracker only supplies `Watermark` | `commit.Commit` |
| `Revoke` | `Tracker.Reset` after the synchronous boundary commit in `OnPartitionsRevoked` | `commit.Reset` |

| TLA+ variable | Go |
|---|---|
| `wm` | `Tracker.Watermark`; `commit.Snapshot.Watermark` |
| `high`, `delivered`, `done` | the kind's storage (map, bitset, ranges, heap, tree, segments); `commit.Snapshot.High/Delivered` |
| `gen` | `Tracker.Gen`; `commit.Snapshot.Gen` |
| `committed` | the driver's committed offset; `commit` records it for `CommitSafe` |
| `handles` | the delivery handles handlers hold: `(gen, offset)` pairs |
| `staleAccepted` | never set: the Go model records refused handles instead |

| TLA+ invariant / property | Go model check (`(*Model).Invariants`) | Implementation test |
|---|---|---|
| `TypeOK` | `TypeOK` | `TestScenarios`, `TestKindsAgree` |
| `SafeWatermark` | `SafeWatermark` | `FuzzOffsetTracker` (no in-flight offset below the watermark) |
| `WatermarkProgress` | `WatermarkProgress` | `TestModel/<kind>/{sequential,random,gaps,widewindow}` |
| `BoundedWindow` | `BoundedWindow` | `TestPauseSignal` (`ErrWindowFull`, `Paused`) |
| `StaleGenRejected` | `StaleGenRejected` | `TestModel/<kind>/rebalance`, `FuzzOffsetTracker` (`trackStale`, `completeStale`) |
| `CommitSafe` | `CommitSafe` | `FuzzOffsetTracker` (`commit` step compares the published offset with the watermark) |
| `WatermarkMonotone` | `WatermarkMonotone` | `TestKindsAgree` (every kind advances identically) |
| `CommitMonotone` | `CommitMonotone` | `TestModel/<kind>/rebalance` |

The TLA+ specification defines the watermark by its closed form (the lowest
incomplete offset, or the offset after the newest record); the Go model and
the six trackers compute it incrementally, and `FuzzOffsetTracker` is what
checks that the incremental computation equals the closed form after every
operation.

### MemoryLease

Records are numbered, so the lane's publication order is the order on `1..N`.
`Sealing` covers both ways a redelivery blocks its lane: the `BlockLane` kind
, which always seals, and any kind under `OrderingStrict`. The
settlement race between two holders of one delivery is `Settlement`'s, so a
settlement here names its record rather than a generation token.

| TLA+ action | Go implementation | Go model op |
|---|---|---|
| `Publish(r)` | `Topic.publishPerSub` → `lane.reserve` → `lane.admit` | `memq.Publish` |
| `DropOldest(r)` | `Topic.reserveLane` → `makeRoom` → `partitionStore.DropOldest` | `Model.dropOldest` |
| `Refuse(r)` | the `ErrQueueFull` / `OverflowDropNewest` arm of `publishPerSub` | `Model.overflow` |
| `Fetch(r, c)` | `Consumer.Fetch` → `lane.claim` → `retryQueue.claim` → `lane.lease` | `memq.Fetch` |
| `Ack(r, c)` | `Consumer.Ack` → `lane.discard` | `memq.Ack` |
| `Nack(r, c)` | `Consumer.Nack` → `lane.requeue` | `memq.Nack` |
| `Expire(r)` | `lane.expireLeases` (scheduler or `Faults.ExpireLeases`) | `memq.ExpireLeases` |
| `CommitDeadLetter(r)` | `lane.deadLetter` after a committed `dlq.Publish` | `Model.deadLetter` |
| `FailDeadLetter(r)` | `lane.deadLetter` after a failed one (`Faults.FailDeadLetter`) | `Model.deadLetter` with `FailDeadLetter(true)` |
| `Tick` | the engine scheduler promoting due records | `memq.Advance` |
| `Close` | `Engine.Close` / `Subscription.drain` | `memq.CloseEngine` |

| TLA+ variable | Go |
|---|---|
| `state` | the lane structure the envelope sits in; `memq.rec.state` |
| `owner`, `attempts` | `Envelope.owner`, `Envelope.attempts`; `memq.rec.owner/attempts` |
| `due` | `Envelope.deadline` while delayed, `retryEntry.at`; `memq.rec.dueAt` |
| `seal` | `retryQueue.out` and its queue head; `memq.lane.out` |
| `released` | the publisher's `Release` callback; `memq.Model.Released` |
| `overflowed`, `displaced` | `Snapshot.Overflowed`, `Snapshot.Dropped`; `memq.counters.overflowed/displaced` |
| `dlqFailed` | `Snapshot.DeadLetterFailures`; `memq.counters.dlqFailed` |

| TLA+ invariant / property | Go model check (`(*Model).Invariants`) | Implementation test |
|---|---|---|
| `TypeOK` | `TypeOK` | `TestModel/<kind>`, `FuzzEngineModel` |
| `OneState` | `OneState` | `FuzzEngineModel` (gauges after every step) |
| `LeaseUnique` | `LeaseUnique` | `TestLeaseExpiryRedelivers`, `FuzzEngineSettle` |
| `AttemptsMonotone` | `AttemptsMonotone` | `TestRetryAttempts`, `FuzzEngineOrdering` |
| `MaxAttemptsRespected` | `MaxAttemptsRespected` | `TestDeadLetterAfterMaxAttempts` |
| `CapacityNonNegative` | `CapacityNonNegative` | `TestOverflowPolicies`, `FuzzEngineOverflow` |
| `Conservation` | `Conservation` | `Snapshot.Conserved` after every fuzz step |
| `ReleaseOnce` | `ReleaseOnce` | `FuzzEngineModel` (release ledger), `TestDeadLetterReleasesOnce` |
| `NoEarlyDelivery` | `NoEarlyDelivery` (both halves: a delayed record is not past its due time, and an in-flight one is not before it) | `TestSchedulerNeverEarly`, `TestDeliverAt` |
| `ClosedTerminal` | `ClosedTerminal` | `TestCloseReleasesEverything` |
| `DeadLetterBeforeSettle` | `DeadLetterBeforeSettle` | `TestDeadLetterFailureNeverSettles` |
| `OverflowAccounted` | `OverflowAccounted` | `TestOverflowPolicies`, `FuzzEngineOverflow` |
| `StrictBlocksLane` (property) | `StrictBlocksLane` | `TestStrictOrderingHoldsOnEverySchedulerKind`, `TestDelayedRetryHoldsSealedLane`, `FuzzEngineOrdering` |
| `NoUnadmittedSettlement` (property) | folded into `Conservation` and `OneState` | `TestAllocs` path: every settlement follows an admission |

`OverflowBlock` is this specification's stutter: a parked publisher is a
publication that has not happened, and `Publish` becomes enabled again when a
record leaves the lane. The Go side checks it directly instead, in
`TestOverflowBlockWaitsForRoom`, because a blocked publisher is a goroutine
rather than a state.

### FanoutReserve

`models/tla/FanoutReserve.tla` (constants: `Pubs = {p1, p2}`,
`Subs = {s1, s2, s3}`, `Cap = 1`, `Policy = "atomic"`; the three sibling
configurations `FanoutReserve{Independent,BestEffort,DropSlow}` re-check the
same model under the other policies) ↔ `internal/engine`
`Topic.publishPerSub` / `Topic.commit` ↔ `internal/model/fanout.Model`.

| TLA+ action | Go implementation | Go model op |
|---|---|---|
| `Begin(p)` | `Topic.Publish` loads the copy-on-write subscription snapshot | `fanout.Publish` (the live set) |
| `Reserve(p)` | `lane.reserve`, in topic subscription order | `Model.room` per subscription |
| `Reserve(p)` drop branch | `makeRoom` → `partitionStore.DropOldest` | `Model.dropOldest` |
| `Commit(p)` | `Topic.commit` (or the Atomic rollback that precedes it) | `Model.admit` / the Atomic release loop |
| `Settle(s)` | `Consumer.Ack` → `lane.unreserve` | `fanout.Settle` |

| TLA+ variable | Go |
|---|---|
| `held` | `lane.depth`; `fanout.sub.recs` |
| `res` | the reservations `publishPerSub` holds in its `admission` mask |
| `todo`, `refused` | the loop position and `admission.refused` |
| `lastTake`, `lastRefused` | `Result.Reached`; `Snapshot.Rejected` |
| `everDropped` | `Snapshot.Dropped`; `fanout.sub.dropped` |
| `starved` | `fanout.Model.starved` |

| TLA+ invariant / property | Go model check (`(*Model).Invariants`) | Implementation test |
|---|---|---|
| `TypeOK` | `TypeOK` | `TestModel`, `FuzzEngineFanout` |
| `NoOverflow` | `NoOverflow` | `TestFanoutPartialDelivery`, `FuzzEngineFanout` |
| `ReservationBalance` | `ReservationBalance` | `FuzzEngineFanout` (ledger after every step) |
| `AtomicAllOrNothing` | `AtomicAllOrNothing` | `TestFanoutPartialDelivery/atomic` |
| `PartialAccounted` | `PartialAccounted` | `TestFanoutPartialDelivery` (rejection counters) |
| `DropSlowAccounted` | `DropSlowAccounted` | `TestFanoutPartialDelivery/dropslow` |
| `Isolation` | `Isolation` | `TestSlowSubscriberIsolation`, `FuzzEngineGroupIsolation` |
| `NoStarvation` | `NoStarvation` | `FuzzEngineFanout`, `FuzzEngineGroupIsolation` |
| `NoUnreservedAdmission` | folded into `ReservationBalance` and `NoOverflow` | `TestAllocs` path: every admit follows a reserve |

The specification models one partition and one record per publication, which
is what admission decides; ordering, leases, redelivery and delay are the
business of `MemoryLease` and `internal/model/memq`. The shared-log
architecture (`Arch: LogCursor`) is admission through one reservation for
every subscription at once, which the specification covers as the degenerate
case where every publisher reserves the same slot — the executable model
carries it as `fanout.Config.Shared` and `FuzzEngineFanout` checks it.

### Shutdown

`models/tla/Shutdown.tla` (constants: `Records = {m1, m2}`,
`Closers = {c1, c2}`, `MaxTime = 5`, `Deadline = 2` — two records are the
fewest that can have one half-placed while another is in flight, which is the
interleaving `NoLostInFlight` is about; two closers are the fewest that can
race for the one run `CloseIdempotent` bounds; and a deadline strictly inside
the clock is what makes `DrainBounded`'s antecedent reachable rather than
vacuous) ↔ `internal/lifecycle` (the phases, the admission gates and the
dead-letter hook) and `internal/engine` `Engine.Close` / `Engine.Shutdown` ↔
`internal/model/shutdown.Model`.

Admitting a publication and placing it are separate actions, and so are
claiming a delivery and taking it. That interval is what the specification is
for: it is where the engine's recurring close-race defect class lives
(the contributor notes) — a record placed into a lane whose drain has already run
is a payload nothing releases and a reservation nothing returns. `Place` and
`Take` are deliberately unguarded by the phase; what keeps them from landing
late is that `LeaveStopAccept` and `LeaveStopFetch` do not pass while a record
is `admitting` or `claiming`, which is `lifecycle.enter` waiting on the gate.

| TLA+ action | Go implementation | Go model op |
|---|---|---|
| `Admit(r)` | `Lifecycle.Enter(OpPublish)` in `Topic.publish` | `shutdown.Admit` |
| `Place(r)` | `Topic.commit` → `lane.admit`, then `Lifecycle.Leave` | `shutdown.Place` |
| `Claim(r)` | `Lifecycle.Enter(OpFetch)` in `Consumer.Fetch` | `shutdown.Claim` |
| `Take(r)` | `lane.claim` → `lane.lease`, then `Lifecycle.Leave` | `shutdown.Take` |
| `Ack(r)` | `Consumer.Ack` → `lane.discard` | `shutdown.Ack` |
| `DeadLetter(r)` | `lane.deadLetter` → `Lifecycle.BeginDeadLetter` | `shutdown.DeadLetter` |
| `CommitDeadLetter(r)` | `Pending.Commit` then `lane.discard` | `shutdown.CommitDeadLetter` |
| `FailDeadLetter(r)` | `Pending.Abort` then `lane.requeueOrDrop` | `shutdown.FailDeadLetter` |
| `Begin(c)` | `Engine.Close` / `Engine.Shutdown` → `Lifecycle.run`'s `sync.Once` | `shutdown.Close` / `shutdown.Shutdown` |
| `LeaveStopAccept` | `lifecycle.enter(StopAccept)`: bar the publish gate, wait for it | the `Admitting` guard in `Model.next` |
| `LeaveStopFetch` | `lifecycle.enter(StopFetch)` + `scheduler.close` | the `Claiming` guard in `Model.next` |
| `LeaveFlushing` | `Steps.Flush` (the engine kernel admits nothing asynchronously) | `Model.next` |
| `LeaveDraining` | `Steps.Drain` = `Engine.WaitIdle(IdleSettled)`, bounded | the `InFlight` guard in `Model.next` |
| `LeaveSettling` | `Lifecycle.waitDeadLetters` then `Steps.Settle` = `Subscription.closeAll` + `Topic.dropAllRetained` | `Model.dropAll` |
| `LeaveCommitting` | `Steps.Commit` then `Steps.CloseNative` | `Model.to(Closed)` |
| `Tick` | the injected `clock.Clock` the drain deadline is measured on | `shutdown.Tick` → `Model.Advance` |
| `AtRest` | nothing: it exists so TLC's deadlock check does not report a closed handle as a fault | — |

| TLA+ variable | Go |
|---|---|
| `phase` | `Lifecycle.phase`; `shutdown.Model.phase` |
| `rstate` | the lane structure the envelope sits in, plus the gate ticket a placement holds; `shutdown.rec.state` |
| `dlqOpen`, `dlqDone` | `Lifecycle.dlq` and `Pending.state`; `shutdown.rec.dlqOpen/dlqCommitted` |
| `runs`, `asked`, `done` | `Lifecycle.once`, `Lifecycle.done`; `shutdown.Model.runs/waiting/done` |
| `time`, `deadline` | the injected clock and `Config.DrainTimeout`; `shutdown.Model.now/deadline` |
| `released` | the publisher's `Release` callback via `Engine.releasePayload`; `shutdown.rec.releases` |

| TLA+ invariant / property | Go model check (`(*Model).Invariants`) | Implementation test |
|---|---|---|
| `TypeOK` | `TypeOK` | `TestNames`, `FuzzShutdownModel` |
| `CloseIdempotent` | `CloseIdempotent` | `TestCloseIsIdempotentUnderConcurrency`, `TestSeveralClosersRunTheSequenceOnce`, `TestEngineCloseIsIdempotent` |
| `NoPublishAfterStopAccept` | `NoPublishAfterStopAccept` | `TestPhaseWaitsForAdmittedOperation`, `TestPlacementNeverEscapesTheDrain`, `TestPublishRacesEngineCloseUnderTheGate` |
| `NoFetchAfterStopFetch` | `NoFetchAfterStopFetch` | `TestPlacementNeverEscapesTheDrain` (the fetch gate), `TestFetchRacesEngineClose` |
| `DLQBeforeAck` | `DLQBeforeAck` | `TestDeadLetterOrdering`, `TestDeadLetterFailureNeverSettles`, `TestSettlingWaitsForDeadLetters` |
| `NoLostInFlight` | `NoLostInFlight` | `TestCloseReleasesEverything`, `FuzzShutdown` (release ledger) |
| `ReleaseOnce` | `ReleaseOnce` | `TestCloseReleasesEverything`, `FuzzShutdown` |
| `DrainBounded` | `DrainBounded` | `TestDrainDeadline`, `TestDrainDeadlineEndsTheWait`, `TestEngineShutdownDrainDeadline` |
| `PhaseMonotone` (property) | `PhaseMonotone` | `TestPhaseMonotone` |

Two asymmetries in the invariants are deliberate. `DLQBeforeAck` here is only the half that says
a record reaches the dead-lettered state through a committed publication: the
other half — that nothing is acknowledged while a publication is in flight —
cannot fail in this specification, because a dead-letter-pending record is out
of the in-flight list and `Ack` cannot name it, and `TypeOK`'s last conjunct is
what a specification breaking that trips. The executable model carries the
stronger form and `TestInvariantsHaveTeeth` fires it. `DrainBounded` is stated
as an enabledness condition because `Spec` carries no fairness: an untimed
specification can say the escape is available, not that it is taken; the
executable model advances eagerly and checks the stronger statement, that it
is never still parked past the deadline.

A third asymmetry is on the caller's side rather than the run's. `Begin(c)`
records that a closer asked, and `LeaveCommitting` sets `done' = asked`: in the
specification a closer's wait ends exactly when the run does. In Go a graceful
`Shutdown` whose context or drain deadline expires while a phase barrier is
still waiting returns to its caller first, with `ErrDrainTimeout` or the
caller's own error, while the run carries on through every remaining phase. No
action of this specification is skipped or reordered by that — the barrier
never abandons the placement it is waiting for, which is what
`NoPublishAfterStopAccept` and `NoLostInFlight` depend on — so what the model
does not describe is when a caller stops looking, not what the handle does.
`TestShutdownBarrierHonoursTheDrainDeadline` and
`TestShutdownBarrierHonoursTheCallerContext` check both halves: the caller
returns on its bound, and the run still reaches `Closed`.

`OverflowBlock`'s parked publisher is not modelled here for the reason
`MemoryLease` gives, and the settlement race between two holders of one
delivery is `Settlement`'s: a settlement here names its record.
