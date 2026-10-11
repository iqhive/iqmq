----------------------------- MODULE MemoryLease -----------------------------
(***************************************************************************)
(* The record lifecycle of one subscription lane in the memory engine       *)
(* (the memory engine): admission under an overflow policy, delivery under *)
(* a lease, redelivery through the retry placement, *)
(* dead-lettering once a record has used up its attempts, time to live and  *)
(* close.                                                                   *)
(*                                                                          *)
(* Implementation: internal/engine (lane, Consumer.Ack/Nack, lane.deadLetter*)
(* and Topic.reserveLane). Executable model: internal/model/memq.           *)
(* models/tla/TRACE.md maps every action and invariant below onto both.     *)
(*                                                                          *)
(* Records are numbered so that the lane's publication order is the natural  *)
(* order on 1..N, which is what strict ordering is a promise about. The      *)
(* settlement race between two holders of the same delivery is not modelled  *)
(* here — models/tla/Settlement.tla owns it — so a settlement names its      *)
(* record rather than a generation token.                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS N,           \* records offered to the lane; Recs = 1..N in publication order
          Consumers,   \* the consumers competing for the lane, e.g. {c1, c2}
          Cap,         \* records the lane may hold at once
          MaxTime,     \* the clock bound
          MaxAttempts, \* deliveries a record may have before it is routed away
          Sealing,     \* TRUE when a redelivery blocks its lane (BlockLane, or strict)
          HasDLQ,      \* TRUE when the subscription has a dead-letter topic
          Overflow     \* "block" | "reject" | "dropnew" | "dropold"

ASSUME N \in Nat /\ N > 0
ASSUME Cap \in Nat /\ Cap > 0
ASSUME MaxTime \in Nat
ASSUME MaxAttempts \in Nat
ASSUME Sealing \in BOOLEAN /\ HasDLQ \in BOOLEAN
ASSUME Overflow \in {"block", "reject", "dropnew", "dropold"}

Recs == 1..N
None == 0

\* "new" has been offered and not yet admitted; "refused" was never admitted.
\* "dlqpending" is the dead-letter-pending state of the lifecycle: the
\* delivery is settled nowhere until the dead-letter publication commits.
Waiting == {"ready", "delayed"}
Held == Waiting \cup {"inflight", "dlqpending"}
Terminal == {"acked", "dead", "dropped"}
States == {"new", "refused"} \cup Held \cup Terminal

VARIABLES
  state,      \* record -> state
  owner,      \* record -> the consumer holding the lease, or None
  attempts,   \* record -> deliveries so far
  due,        \* record -> the time the record becomes deliverable
  seal,       \* the redelivery the lane is blocked on, or None
  released,   \* record -> payload releases (0 or 1)
  now,        \* the clock
  closed,     \* the engine has closed
  overflowed, \* a publication was discarded before admission
  displaced,  \* an admitted record was discarded to make room for a newer one
  dlqFailed   \* a dead-letter publication failed, so a record outlived its budget

vars == <<state, owner, attempts, due, seal, released, now, closed,
          overflowed, displaced, dlqFailed>>

-----------------------------------------------------------------------------

HeldRecs == {r \in Recs : state[r] \in Held}
Room == Cardinality(HeldRecs) < Cap
\* A record a full lane may displace: one that is waiting, never one that is
\* leased or whose dead-letter publication is in flight.
Displaceable == {r \in Recs : state[r] \in Waiting}
Deliverable(r) == state[r] = "ready" /\ due[r] <= now

\* Strict ordering: a lane hands out its records in publication order, so a
\* record may be delivered only when every earlier one is settled, in flight
\* or still waiting for a due time its publisher asked for.
InOrder(r) == ~Sealing \/ \A o \in Recs : (o < r) => state[o] # "ready"

\* While the lane is sealed on a redelivery nothing else leaves it.
Unsealed(r) == seal = None \/ seal = r

Init ==
  /\ state = [r \in Recs |-> "new"]
  /\ owner = [r \in Recs |-> None]
  /\ attempts = [r \in Recs |-> 0]
  /\ due = [r \in Recs |-> 0]
  /\ seal = None
  /\ released = [r \in Recs |-> 0]
  /\ now = 0
  /\ closed = FALSE
  /\ overflowed = FALSE
  /\ displaced = FALSE
  /\ dlqFailed = FALSE

-----------------------------------------------------------------------------
(* Admission (engine: Topic.publishPerSub, Topic.reserveLane) *)

\* Admit a record the lane has room for, ready or held back until a due time.
Publish(r) ==
  /\ ~closed
  /\ state[r] = "new"
  /\ Room
  /\ \E d \in {0, 1} :
       /\ state' = [state EXCEPT ![r] = IF d = 0 THEN "ready" ELSE "delayed"]
       /\ due' = [due EXCEPT ![r] = now + d]
  /\ UNCHANGED <<owner, attempts, seal, released, now, closed, overflowed, displaced, dlqFailed>>

\* Make room for a record by discarding the one the lane has held longest,
\* which only the DropOldest policy may do.
DropOldest(r) ==
  /\ ~closed
  /\ state[r] = "new"
  /\ ~Room
  /\ Overflow = "dropold"
  /\ \E v \in Displaceable :
       /\ v # r
       /\ state' = [state EXCEPT ![v] = "dropped", ![r] = "ready"]
       /\ released' = [released EXCEPT ![v] = 1]
       /\ seal' = IF seal = v THEN None ELSE seal
  /\ due' = [due EXCEPT ![r] = now]
  /\ displaced' = TRUE
  /\ UNCHANGED <<owner, attempts, now, closed, overflowed, dlqFailed>>

\* Refuse a publication the lane had no room for. Reject tells the publisher
\* and leaves the payload with it; DropNewest discards the record, counts it
\* and releases the payload, because the publisher was told it was taken.
Refuse(r) ==
  /\ ~closed
  /\ state[r] = "new"
  /\ ~Room
  /\ Overflow \in {"reject", "dropnew"}
  /\ state' = [state EXCEPT ![r] = "refused"]
  /\ released' = [released EXCEPT ![r] = IF Overflow = "dropnew" THEN 1 ELSE 0]
  \* Parenthesised for the reason models/tla/FanoutReserve.tla gives at
  \* starved': = binds tighter than \/, so the unbracketed form parses as
  \* "(overflowed' = (Overflow = "dropnew")) \/ overflowed" and leaves
  \* overflowed unassigned once it is TRUE.
  /\ overflowed' = ((Overflow = "dropnew") \/ overflowed)
  /\ UNCHANGED <<owner, attempts, due, seal, now, closed, displaced, dlqFailed>>

\* OverflowBlock parks the publisher, which is this specification's stutter:
\* the publication has simply not happened yet, and Publish becomes enabled
\* when a record leaves the lane.

-----------------------------------------------------------------------------
(* Delivery and settlement (engine: lane.claim, Consumer.Ack/Nack) *)

Fetch(r, c) ==
  /\ ~closed
  /\ Deliverable(r)
  /\ Unsealed(r)
  /\ InOrder(r)
  /\ attempts[r] < MaxAttempts + 1
  /\ state' = [state EXCEPT ![r] = "inflight"]
  /\ owner' = [owner EXCEPT ![r] = c]
  /\ attempts' = [attempts EXCEPT ![r] = attempts[r] + 1]
  /\ UNCHANGED <<due, seal, released, now, closed, overflowed, displaced, dlqFailed>>

Ack(r, c) ==
  /\ state[r] = "inflight"
  /\ owner[r] = c
  /\ state' = [state EXCEPT ![r] = "acked"]
  /\ owner' = [owner EXCEPT ![r] = None]
  /\ released' = [released EXCEPT ![r] = 1]
  /\ seal' = IF seal = r THEN None ELSE seal
  /\ UNCHANGED <<attempts, due, now, closed, overflowed, displaced, dlqFailed>>

\* What a failed delivery does: the record is routed away once it has used up
\* its attempts, and goes back to its lane otherwise. A sealing placement
\* makes the lane wait for it.
Fail(r, d) ==
  IF MaxAttempts > 0 /\ attempts[r] >= MaxAttempts
    THEN /\ state' = [state EXCEPT ![r] = IF HasDLQ THEN "dlqpending" ELSE "dropped"]
         /\ released' = [released EXCEPT ![r] = IF HasDLQ THEN 0 ELSE 1]
         /\ seal' = IF seal = r THEN None ELSE seal
         /\ due' = due
    ELSE /\ state' = [state EXCEPT ![r] = IF d = 0 THEN "ready" ELSE "delayed"]
         /\ due' = [due EXCEPT ![r] = now + d]
         /\ released' = released
         /\ seal' = IF Sealing THEN r ELSE seal

Nack(r, c) ==
  /\ state[r] = "inflight"
  /\ owner[r] = c
  /\ \E d \in {0, 1} : Fail(r, d)
  /\ owner' = [owner EXCEPT ![r] = None]
  /\ UNCHANGED <<attempts, now, closed, overflowed, displaced, dlqFailed>>

\* The scheduler reclaims a lease whose deadline has passed. The deadline
\* itself is not modelled: an expiry may happen at any time, which covers
\* every deadline the engine could have set.
Expire(r) ==
  /\ state[r] = "inflight"
  /\ Fail(r, 0)
  /\ owner' = [owner EXCEPT ![r] = None]
  /\ UNCHANGED <<attempts, now, closed, overflowed, displaced, dlqFailed>>

-----------------------------------------------------------------------------
(* Dead-lettering (engine: lane.deadLetter, adr/ADR-011-failure-policy.md) *)

\* Only a committed dead-letter publication settles the original.
CommitDeadLetter(r) ==
  /\ state[r] = "dlqpending"
  /\ state' = [state EXCEPT ![r] = "dead"]
  /\ released' = [released EXCEPT ![r] = 1]
  /\ seal' = IF seal = r THEN None ELSE seal
  /\ UNCHANGED <<owner, attempts, due, now, closed, overflowed, displaced, dlqFailed>>

\* A failed or ambiguous one never does: the record goes back to its lane.
FailDeadLetter(r) ==
  /\ state[r] = "dlqpending"
  /\ IF closed
       \* A lane that closed under the publication admits nothing again, so
       \* the record is dropped rather than put back (lane.requeueOrDrop).
       THEN /\ state' = [state EXCEPT ![r] = "dropped"]
            /\ released' = [released EXCEPT ![r] = 1]
            /\ due' = due
            /\ seal' = IF seal = r THEN None ELSE seal
       ELSE /\ state' = [state EXCEPT ![r] = "ready"]
            /\ released' = released
            /\ due' = [due EXCEPT ![r] = now]
            /\ seal' = IF Sealing THEN r ELSE seal
  /\ dlqFailed' = TRUE
  /\ UNCHANGED <<owner, attempts, now, closed, overflowed, displaced>>

-----------------------------------------------------------------------------
(* Time and close *)

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ state' = [r \in Recs |->
                 IF state[r] = "delayed" /\ due[r] <= now + 1 THEN "ready" ELSE state[r]]
  /\ UNCHANGED <<owner, attempts, due, seal, released, closed, overflowed, displaced, dlqFailed>>

\* Close drops every record the lane still holds, releasing each payload once.
\* A dead-letter publication that is already in flight keeps its outcome, so a
\* pending record is not dropped from under it.
Close ==
  /\ ~closed
  /\ closed' = TRUE
  /\ state' = [r \in Recs |->
                 IF state[r] \in Held /\ state[r] # "dlqpending" THEN "dropped" ELSE state[r]]
  /\ released' = [r \in Recs |->
                    IF state[r] \in Held /\ state[r] # "dlqpending" THEN 1 ELSE released[r]]
  /\ owner' = [r \in Recs |-> None]
  /\ seal' = None
  /\ UNCHANGED <<attempts, due, now, overflowed, displaced, dlqFailed>>

\* Nothing is left to do once the engine has closed and the clock has run out:
\* every record it held is terminal and no publication is admitted again. The
\* self-loop is what keeps TLC from reporting that terminal state as a
\* deadlock.
Done == closed /\ now = MaxTime /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
  \/ \E r \in Recs : Publish(r) \/ DropOldest(r) \/ Refuse(r) \/ Expire(r)
                       \/ CommitDeadLetter(r) \/ FailDeadLetter(r)
  \/ \E r \in Recs, c \in Consumers : Fetch(r, c) \/ Ack(r, c) \/ Nack(r, c)
  \/ Tick
  \/ Close
  \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. Every name below is one the Go reference model checks too:    *)
(* the Invariants method of internal/model/memq documents them by name.      *)

TypeOK ==
  /\ \A r \in Recs : state[r] \in States
  /\ \A r \in Recs : owner[r] \in Consumers \cup {None}
  /\ \A r \in Recs : attempts[r] \in 0..(MaxAttempts + 1)
  /\ \A r \in Recs : due[r] \in 0..(MaxTime + 1)
  /\ \A r \in Recs : released[r] \in 0..1
  /\ seal \in Recs \cup {None}
  /\ now \in 0..MaxTime
  /\ closed \in BOOLEAN /\ overflowed \in BOOLEAN /\ displaced \in BOOLEAN
  /\ dlqFailed \in BOOLEAN

\* A record is in exactly one logical state, and the fields that belong to a
\* state hold only in it: only an in-flight record has an owner, and a
\* terminal one is held nowhere.
OneState ==
  /\ \A r \in Recs : (owner[r] # None) <=> (state[r] = "inflight")
  /\ \A r \in Recs : state[r] \in Terminal => state[r] \notin Held

\* A lease belongs to one consumer, and a record that has never been
\* delivered holds none.
LeaseUnique ==
  \A r \in Recs : (state[r] = "inflight" => attempts[r] > 0)
                    /\ (attempts[r] = 0 => owner[r] = None)

\* Attempts are counted per delivery and never exceed the budget: a record
\* that has used up its attempts is routed away rather than delivered again.
AttemptsMonotone == \A r \in Recs : attempts[r] > 0 => state[r] # "new"

\* A record is delivered no more often than its budget allows. The one way
\* past it is a dead-letter publication that failed: the record goes back to
\* its lane rather than being acked, and is tried again — which is the rule
\* ADR-011 puts above the budget.
MaxAttemptsRespected ==
  MaxAttempts > 0 =>
    \A r \in Recs : attempts[r] <= MaxAttempts \/ dlqFailed

\* The lane never holds more than its capacity.
CapacityNonNegative == Cardinality(HeldRecs) <= Cap

\* Every record the lane admitted is either held or terminal, and one it
\* refused is neither.
Conservation ==
  /\ \A r \in Recs : state[r] \in States
  /\ \A r \in Recs : state[r] = "refused" => attempts[r] = 0

\* A payload is released exactly once, and never while the record may still
\* come back.
ReleaseOnce ==
  /\ \A r \in Recs : released[r] <= 1
  /\ \A r \in Recs : state[r] \in Terminal => released[r] = 1
  /\ \A r \in Recs : state[r] \in Held => released[r] = 0

\* A record is never delivered before its due time. Both halves are needed:
\* the first says a record still waiting for a due time has one in the future,
\* and the second says a record that left the waiting states did so on time.
\* Without it the invariant could not observe the very thing it is named for
\* — a Fetch that ignores the due time takes the record out of "delayed", and
\* the record it delivered early is then no longer constrained by anything.
\* due[r] only ever moves to now, or to a time in the future together with
\* state "delayed" (Publish, Fail, FailDeadLetter, DropOldest), so a delivered
\* or deliverable record whose due time is still ahead is exactly an early
\* delivery or an early promotion.
NoEarlyDelivery ==
  /\ \A r \in Recs : state[r] = "delayed" => due[r] > now
  /\ \A r \in Recs : state[r] \in {"ready", "inflight"} => due[r] <= now

\* A closed lane holds nothing but the dead-letter publications already in
\* flight, whose outcome stands.
ClosedTerminal ==
  closed => \A r \in Recs : state[r] \notin Waiting /\ state[r] # "inflight"

\* Only a committed dead-letter publication settles the original: while one is
\* in flight the record is released nowhere, and a record reaches the
\* dead-letter topic only when there is one.
DeadLetterBeforeSettle ==
  /\ \A r \in Recs : state[r] = "dlqpending" => released[r] = 0
  /\ \A r \in Recs : state[r] = "dead" => HasDLQ /\ MaxAttempts > 0

\* A record is only discarded by an overflow policy that says it may be.
OverflowAccounted ==
  /\ overflowed => Overflow = "dropnew"
  /\ displaced => Overflow = "dropold"

(* Action properties *)

\* Nothing overtakes a redelivery a sealing placement is holding the lane on:
\* while the lane is sealed, no record but that one is handed to a consumer.
\* A record leased before the seal was armed keeps its lease — the seal stops
\* new deliveries, it does not revoke the ones already out — which is why this
\* is a property of the transitions and not of one state.
StrictBlocksLane ==
  [][\A r \in Recs : (Sealing /\ seal # None /\ r # seal /\ state[r] # "inflight")
                       => state'[r] # "inflight"]_vars

\* Every record that leaves the lane was admitted to it first: nothing is
\* settled, dead-lettered or dropped that the capacity check never let in.
NoUnadmittedSettlement ==
  [][\A r \in Recs : (state'[r] \in Terminal /\ state[r] \notin Terminal)
                       => state[r] \in Held]_vars

=============================================================================
