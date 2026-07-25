---------------------------- MODULE KafkaCommit ----------------------------
(***************************************************************************)
(* Cumulative offset acknowledgement (the SPI kafka, *)
(* AckCumulative): one partition whose records are delivered in increasing  *)
(* order and completed in any order, a watermark that is safe to commit,    *)
(* and a rebalance that revokes the assignment while completions are still  *)
(* in flight.                                                              *)
(*                                                                         *)
(* Implementation: driverkit/offset (Tracker, six kinds). Executable model: *)
(* internal/model/commit. models/tla/TRACE.md maps every action and         *)
(* invariant below onto both.                                              *)
(*                                                                         *)
(* The configuration bounds the run: offsets 0..MaxOffset, a tracking       *)
(* window of Window offsets and one revocation (MaxGen = 2).               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS MaxOffset,  \* the highest offset the partition holds
          Window,     \* the tracking window: the span the driver may keep open
          MaxGen      \* bound on the number of assignments (generations 1..MaxGen)

Offsets == 0..MaxOffset
Gens    == 1..MaxGen

\* A handle is what a handler holds while it processes a record: the offset
\* and the generation of the assignment it was delivered under.
Handles == [off: Offsets, g: Gens]

VARIABLES
  wm,            \* watermark: the offset that is safe to commit
  high,          \* the offset after the newest delivered record
  delivered,     \* offsets delivered under the current assignment
  done,          \* delivered offsets whose processing completed
  gen,           \* assignment generation
  committed,     \* the last watermark published to the broker
  handles,       \* completion handles not yet used
  staleAccepted  \* set when a handle from a superseded generation was applied

vars == <<wm, high, delivered, done, gen, committed, handles, staleAccepted>>

-----------------------------------------------------------------------------

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* The records that are delivered but not finished: they and nothing else
\* hold the watermark back.
Incomplete == delivered \ done

\* The watermark implied by a set of incomplete records: the lowest of them,
\* or the offset after the newest record when there are none. Offsets the
\* broker never delivered are not in delivered and so never hold it back.
WM(inc, h) == IF inc = {} THEN h ELSE Min(inc)

Init ==
  /\ wm = 0
  /\ high = 0
  /\ delivered = {}
  /\ done = {}
  /\ gen = 1
  /\ committed = 0
  /\ handles = {}
  /\ staleAccepted = FALSE

-----------------------------------------------------------------------------
(* Fetch-loop actions *)

\* Deliver the record at offset o to the application (Tracker.Track). Offsets
\* increase strictly; o > high models the offsets the broker never delivers
\* (compacted records, transaction markers). The window bounds the span only
\* while records are in flight: with nothing outstanding the watermark jumps
\* to the new record, since every offset below it is resolved.
Deliver(o) ==
  /\ o >= high
  /\ \/ Incomplete = {}
     \/ o - wm < Window
  /\ delivered' = delivered \cup {o}
  /\ high' = o + 1
  /\ wm' = IF Incomplete = {} THEN o ELSE wm
  /\ handles' = handles \cup {[off |-> o, g |-> gen]}
  /\ UNCHANGED <<done, gen, committed, staleAccepted>>

\* A handler finishes with the record its handle names (Tracker.Complete). A
\* handle from a superseded generation, for a record that was never delivered
\* under this assignment, or for one already completed, changes nothing.
Complete(h) ==
  /\ h \in handles
  /\ handles' = handles \ {h}
  /\ IF h.g = gen /\ h.off \in Incomplete
       THEN /\ done' = done \cup {h.off}
            /\ wm' = WM(Incomplete \ {h.off}, high)
            /\ staleAccepted' = (staleAccepted \/ h.g # gen)
       ELSE UNCHANGED <<done, wm, staleAccepted>>
  /\ UNCHANGED <<delivered, high, gen, committed>>

\* Publish the watermark to the broker.
Commit ==
  /\ committed' = wm
  /\ UNCHANGED <<wm, high, delivered, done, gen, handles, staleAccepted>>

\* The group rebalances: the driver stops dispatch, commits the safe boundary
\* synchronously, bumps the generation and drops every in-flight record. The
\* handles handlers still hold stay behind and are now stale; the next
\* assignment resumes from the committed boundary.
Revoke ==
  /\ gen < MaxGen
  /\ committed' = wm
  /\ gen' = gen + 1
  /\ delivered' = {}
  /\ done' = {}
  /\ high' = wm
  /\ UNCHANGED <<wm, handles, staleAccepted>>

-----------------------------------------------------------------------------

Next ==
  \/ \E o \in Offsets : Deliver(o)
  \/ \E h \in handles : Complete(h)
  \/ Commit
  \/ Revoke

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
  /\ wm \in 0..(MaxOffset + 1)
  /\ high \in 0..(MaxOffset + 1)
  /\ committed \in 0..(MaxOffset + 1)
  /\ delivered \subseteq Offsets
  /\ done \subseteq delivered
  /\ gen \in Gens
  /\ handles \subseteq Handles
  /\ staleAccepted \in BOOLEAN
  /\ wm <= high

\* The watermark never passes an unfinished record, so committing it can
\* never acknowledge work that is still in flight.
SafeWatermark == \A o \in Incomplete : wm <= o

\* The watermark is not merely safe but as far advanced as it may be: the
\* lowest unfinished record, or the offset after the newest record when none
\* is left. A gap of offsets the broker never delivered does not stall it.
WatermarkProgress == wm = WM(Incomplete, high)

\* The tracking window is bounded, so memory is bounded however the
\* completions interleave.
BoundedWindow == high - wm <= Window

\* A handle from a superseded generation is never applied: a late completion
\* after a rebalance cannot acknowledge a record of the new assignment.
StaleGenRejected == ~staleAccepted

\* The offset published to the broker is never above the watermark, and never
\* above an unfinished record.
CommitSafe ==
  /\ committed <= wm
  /\ \A o \in Incomplete : committed <= o

(* Action properties *)

\* Within one assignment the watermark never goes backwards.
WatermarkMonotone == [][(gen' = gen) => wm' >= wm]_vars

\* The offset published to the broker never goes backwards.
CommitMonotone == [][committed' >= committed]_vars

=============================================================================
