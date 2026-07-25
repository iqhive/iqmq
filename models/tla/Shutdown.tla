------------------------------- MODULE Shutdown -----------------------------
(***************************************************************************)
(* Graceful shutdown (the message contract): a handle stops accepting *)
(* publications, stops fetching, flushes, waits for the in-flight handlers  *)
(* within a drain deadline, settles or requeues what is left, commits and   *)
(* closes — in that order, once, however many goroutines ask.               *)
(*                                                                         *)
(* Implementation: internal/lifecycle (the phases and the admission gates)  *)
(* and internal/engine (Engine.Close / Engine.Shutdown). Executable model:  *)
(* internal/model/shutdown. models/tla/TRACE.md maps every action and       *)
(* invariant below onto both.                                              *)
(*                                                                         *)
(* Admitting a publication and placing it are separate actions, and so are  *)
(* claiming a delivery and taking it. That interval is the point of the     *)
(* specification: it is where the engine's recurring close-race defect      *)
(* class lives (hack/PLAYBOOK.md) — a record placed into a lane whose drain *)
(* has already run is a payload nothing releases. StopAccept and StopFetch  *)
(* may not be left while an operation they bar is inside that interval, and *)
(* NoLostInFlight is what fails when they are.                             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Records,   \* the records the handle may hold, e.g. {m1, m2}
          Closers,   \* the goroutines that call Close, e.g. {c1, c2}
          MaxTime,   \* the clock bound: time runs 0..MaxTime
          Deadline   \* the drain deadline, in the same units

ASSUME MaxTime \in Nat /\ Deadline \in Nat /\ Deadline <= MaxTime

\* The phases of the message contract, ranked so that "forward" has a meaning.
Phases == {"running", "stopaccept", "stopfetch", "flushing",
           "draining", "settling", "committing", "closed"}

Rank(p) == CASE p = "running"    -> 0
             [] p = "stopaccept" -> 1
             [] p = "stopfetch"  -> 2
             [] p = "flushing"   -> 3
             [] p = "draining"   -> 4
             [] p = "settling"   -> 5
             [] p = "committing" -> 6
             [] p = "closed"     -> 7

\* The record states, reduced to what a shutdown distinguishes.
\* "admitting" and "claiming" are the two half-finished operations.
States == {"unpublished", "admitting", "held", "claiming", "inflight",
           "dlqpending", "acked", "deadlettered", "dropped"}

Terminal == {"acked", "deadlettered", "dropped"}

VARIABLES
  phase,     \* the shutdown phase
  rstate,    \* record -> its state
  dlqOpen,   \* records whose dead-letter publication is in flight
  dlqDone,   \* records whose dead-letter publication committed
  runs,      \* how many shutdown sequences have been started
  asked,     \* closers that have called Close and not yet returned
  done,      \* closers that have returned
  time,      \* the clock
  deadline,  \* the instant the graceful waits stop at, once a run started
  released   \* record -> number of payload releases

vars == <<phase, rstate, dlqOpen, dlqDone, runs, asked, done, time,
          deadline, released>>

-----------------------------------------------------------------------------

Count(S) == Cardinality(S)
InState(s) == {r \in Records : rstate[r] = s}
Expired == runs > 0 /\ time >= deadline

Init ==
  /\ phase = "running"
  /\ rstate = [r \in Records |-> "unpublished"]
  /\ dlqOpen = {}
  /\ dlqDone = {}
  /\ runs = 0
  /\ asked = {}
  /\ done = {}
  /\ time = 0
  /\ deadline = MaxTime + 1
  /\ released = [r \in Records |-> 0]

-----------------------------------------------------------------------------
(* Record actions *)

\* Accept a publication (engine: Topic.publish under lifecycle.Enter of
\* OpPublish). Admitted only while running.
Admit(r) ==
  /\ phase = "running"
  /\ rstate[r] = "unpublished"
  /\ rstate' = [rstate EXCEPT ![r] = "admitting"]
  /\ UNCHANGED <<phase, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* Complete the publication by putting the record in a lane (engine:
\* Topic.commit -> lane.admit, then lifecycle.Leave). The action is
\* deliberately not guarded by the phase: what keeps it from landing late is
\* that StopAccept does not pass while a record is "admitting", and removing
\* that guard is what makes NoLostInFlight fail.
Place(r) ==
  /\ rstate[r] = "admitting"
  /\ rstate' = [rstate EXCEPT ![r] = "held"]
  /\ UNCHANGED <<phase, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* Accept a fetch (engine: Consumer.Fetch under OpFetch).
Claim(r) ==
  /\ Rank(phase) < Rank("stopfetch")
  /\ rstate[r] = "held"
  /\ rstate' = [rstate EXCEPT ![r] = "claiming"]
  /\ UNCHANGED <<phase, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* Hand the delivery to the consumer (engine: lane.lease).
Take(r) ==
  /\ rstate[r] = "claiming"
  /\ rstate' = [rstate EXCEPT ![r] = "inflight"]
  /\ UNCHANGED <<phase, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* Settle an in-flight delivery (engine: Consumer.Ack -> lane.discard). A
\* record whose dead-letter publication is in flight is out of the in-flight
\* list — that is what "dead-letter-pending" means — so a settlement
\* cannot overtake a publication here by construction, and TypeOK's last
\* conjunct is what fails in a specification that breaks it.
Ack(r) ==
  /\ Rank(phase) < Rank("committing")
  /\ rstate[r] = "inflight"
  /\ rstate' = [rstate EXCEPT ![r] = "acked"]
  /\ released' = [released EXCEPT ![r] = released[r] + 1]
  /\ UNCHANGED <<phase, dlqOpen, dlqDone, runs, asked, done, time, deadline>>

\* Take an in-flight record out of its lane and publish it to the dead-letter
\* topic (engine: lane.deadLetter, under lifecycle.BeginDeadLetter).
DeadLetter(r) ==
  /\ Rank(phase) < Rank("committing")
  /\ rstate[r] = "inflight"
  /\ rstate' = [rstate EXCEPT ![r] = "dlqpending"]
  /\ dlqOpen' = dlqOpen \cup {r}
  /\ UNCHANGED <<phase, dlqDone, runs, asked, done, time, deadline, released>>

\* The publication committed: only now may the original be settled
\* (adr/ADR-011: the one rule stated twice).
CommitDeadLetter(r) ==
  /\ r \in dlqOpen
  /\ dlqOpen' = dlqOpen \ {r}
  /\ dlqDone' = dlqDone \cup {r}
  /\ rstate' = [rstate EXCEPT ![r] = "deadlettered"]
  /\ released' = [released EXCEPT ![r] = released[r] + 1]
  /\ UNCHANGED <<phase, runs, asked, done, time, deadline>>

\* The publication failed or was ambiguous: the record goes back to its lane,
\* or is released when the settling phase has already drained the lanes.
FailDeadLetter(r) ==
  /\ r \in dlqOpen
  /\ dlqOpen' = dlqOpen \ {r}
  /\ IF Rank(phase) >= Rank("settling")
       THEN /\ rstate' = [rstate EXCEPT ![r] = "dropped"]
            /\ released' = [released EXCEPT ![r] = released[r] + 1]
       ELSE /\ rstate' = [rstate EXCEPT ![r] = "held"]
            /\ UNCHANGED released
  /\ UNCHANGED <<phase, dlqDone, runs, asked, done, time, deadline>>

-----------------------------------------------------------------------------
(* Shutdown actions *)

\* A closer arrives. The first one starts the sequence; every later one joins
\* the run under way rather than starting a second (lifecycle.run's Once).
Begin(c) ==
  /\ c \notin asked
  /\ asked' = asked \cup {c}
  /\ IF runs = 0
       THEN /\ runs' = 1
            /\ phase' = "stopaccept"
            /\ deadline' = time + Deadline
       ELSE UNCHANGED <<runs, phase, deadline>>
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, done, time, released>>

\* The barrier: StopAccept is left only once no publication is between its
\* admission and its placement. This is the wait that makes the settling
\* drain safe, and it is the guard a reviewer should remove to see
\* NoLostInFlight fail.
LeaveStopAccept ==
  /\ phase = "stopaccept"
  /\ InState("admitting") = {}
  /\ phase' = "stopfetch"
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* The same barrier on the fetch side.
LeaveStopFetch ==
  /\ phase = "stopfetch"
  /\ InState("claiming") = {}
  /\ phase' = "flushing"
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

LeaveFlushing ==
  /\ phase = "flushing"
  /\ phase' = "draining"
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* The drain waits for the in-flight handlers, and stops waiting at the
\* deadline: the shutdown never waits indefinitely.
LeaveDraining ==
  /\ phase = "draining"
  /\ (InState("inflight") = {} \/ Expired)
  /\ phase' = "settling"
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, runs, asked, done, time, deadline,
                 released>>

\* Settling waits for the dead-letter publications — a record whose
\* publication is in flight is neither settled nor in a lane — and then drops
\* everything the lanes still hold, releasing each payload once.
LeaveSettling ==
  /\ phase = "settling"
  /\ (dlqOpen = {} \/ Expired)
  /\ LET drop == InState("held") \cup InState("inflight") IN
     /\ rstate' = [r \in Records |->
                     IF r \in drop THEN "dropped" ELSE rstate[r]]
     /\ released' = [r \in Records |->
                     IF r \in drop THEN released[r] + 1 ELSE released[r]]
  /\ phase' = "committing"
  /\ UNCHANGED <<dlqOpen, dlqDone, runs, asked, done, time, deadline>>

\* Commit the safe boundaries, close the natives, and let every closer return.
LeaveCommitting ==
  /\ phase = "committing"
  /\ phase' = "closed"
  /\ done' = asked
  /\ UNCHANGED <<rstate, dlqOpen, dlqDone, runs, asked, time, deadline,
                 released>>

\* The clock. It stops at MaxTime, which bounds the state space; a shutdown
\* parked on a wait it will never be released from is what the deadline is
\* for, and DrainBounded is what says so.
Tick ==
  /\ time < MaxTime
  /\ time' = time + 1
  /\ UNCHANGED <<phase, rstate, dlqOpen, dlqDone, runs, asked, done, deadline,
                 released>>

\* The closed handle at rest. It keeps TLC's deadlock check from reporting the
\* end of the run as a fault: a handle that has closed and released everything
\* has nothing left to do, which is the point.
AtRest ==
  /\ phase = "closed"
  /\ time = MaxTime
  /\ dlqOpen = {}
  /\ \A r \in Records : rstate[r] \in Terminal \cup {"unpublished"}
  /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
  \/ \E r \in Records :
       \/ Admit(r) \/ Place(r) \/ Claim(r) \/ Take(r) \/ Ack(r)
       \/ DeadLetter(r) \/ CommitDeadLetter(r) \/ FailDeadLetter(r)
  \/ \E c \in Closers : Begin(c)
  \/ LeaveStopAccept \/ LeaveStopFetch \/ LeaveFlushing
  \/ LeaveDraining \/ LeaveSettling \/ LeaveCommitting
  \/ Tick
  \/ AtRest

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
  /\ phase \in Phases
  /\ \A r \in Records : rstate[r] \in States
  /\ dlqOpen \subseteq Records /\ dlqDone \subseteq Records
  /\ asked \subseteq Closers /\ done \subseteq Closers
  /\ runs \in 0..1
  /\ time \in 0..MaxTime
  /\ \A r \in Records : released[r] \in 0..1
  \* A publication is in flight only for a record that is waiting for it.
  /\ \A r \in dlqOpen : rstate[r] = "dlqpending"

\* However many goroutines ask, the sequence runs at most once, and a caller
\* returns only once the handle is closed. That is the idempotent,
\* concurrency-safe Close.
CloseIdempotent ==
  /\ runs <= 1
  /\ done \subseteq asked
  /\ (done # {} => phase = "closed")

\* No publication is placed once the phase has passed StopAccept: the record
\* would sit in a lane the settling drain has already walked.
NoPublishAfterStopAccept ==
  Rank(phase) > Rank("stopaccept") => InState("admitting") = {}

\* The same on the fetch side.
NoFetchAfterStopFetch ==
  Rank(phase) > Rank("stopfetch") => InState("claiming") = {}

\* A record reaches the dead-lettered state only through a committed
\* publication: a failed or ambiguous one never settles the original, which
\* is the one rule ADR-011 states twice. The other half of the ordering —
\* that nothing is acknowledged while a publication is in flight — is
\* TypeOK's last conjunct here, because a dead-letter-pending record is out
\* of the in-flight list and Ack cannot name it. The executable model carries
\* the stronger form: internal/model/shutdown records such an acknowledgement
\* and its own DLQBeforeAck fails on it.
DLQBeforeAck ==
  \A r \in Records : rstate[r] = "deadlettered" => r \in dlqDone

\* Once the handle is closed nothing is queued, in flight or half-placed:
\* every record is terminal, never published, or still owned by a dead-letter
\* publication that has not resolved.
NoLostInFlight ==
  phase = "closed" =>
    \A r \in Records :
      rstate[r] \in Terminal \cup {"unpublished", "dlqpending"}

\* A payload is released exactly once, and only by the transition into a
\* terminal state.
ReleaseOnce ==
  \A r \in Records :
    released[r] = (IF rstate[r] \in Terminal THEN 1 ELSE 0)

\* Once the drain deadline has passed, nothing the handle is waiting for can
\* hold it any longer: whatever the handlers and the dead-letter publications
\* are doing, the shutdown can leave the phase it is in, in one step.
\*
\* It is stated as an enabledness condition because Spec carries no fairness:
\* an untimed specification cannot say that the step is taken, only that it is
\* available. Removing the "\/ Expired" escape from LeaveDraining or
\* LeaveSettling makes TLC report a counterexample here. The executable model
\* checks the stronger statement — internal/model/shutdown advances eagerly,
\* so its DrainBounded says the model is never still parked past the deadline.
DrainBounded ==
  /\ (phase = "draining" /\ Expired) => ENABLED LeaveDraining
  /\ (phase = "settling" /\ Expired) => ENABLED LeaveSettling

(* Action properties *)

\* The phase only ever moves forward.
PhaseMonotone == [][Rank(phase') >= Rank(phase)]_vars

=============================================================================
