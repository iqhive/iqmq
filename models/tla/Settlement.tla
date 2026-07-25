---------------------------- MODULE Settlement ----------------------------
(***************************************************************************)
(* Delivery settlement (the message contract): one delivery record guarded by *)
(* a compare-and-swap word, a set of racers holding settlement handles, and *)
(* the engine that leases, revokes and closes.                             *)
(*                                                                         *)
(* Implementation: internal/settlement (Cell). Executable model:           *)
(* internal/model/settle. models/tla/TRACE.md maps every action and         *)
(* invariant below onto both.                                              *)
(*                                                                         *)
(* The configuration bounds the run: two racers, two generations, and the  *)
(* three native outcomes (commit, definite rejection, ambiguous).          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Racers,  \* settlement handles racing on the delivery
          MaxGen,  \* bound on the number of leases (generations 1..MaxGen)
          None     \* model value: no owner

States   == {"idle", "active", "settling", "settled", "unknown", "closed"}
Pendings == {"active", "idle", "closed"}  \* where a definite rejection lands
Gens     == 0..MaxGen

VARIABLES
  state,        \* settlement state of the delivery
  pending,      \* revocation recorded while settling: "active" = none
  gen,          \* current generation; 0 = never leased
  owner,        \* racer whose attempt is in flight, or None
  pc,           \* racer program counter: "idle" | "holding" | "settling"
  token,        \* generation each racer presents
  settledCount, \* how many times each generation was terminally settled
  lost          \* generations whose lease was revoked or closed while active

vars == <<state, pending, gen, owner, pc, token, settledCount, lost>>

-----------------------------------------------------------------------------

TypeOK ==
  /\ state \in States
  /\ pending \in Pendings
  /\ gen \in Gens
  /\ owner \in Racers \cup {None}
  /\ pc \in [Racers -> {"idle", "holding", "settling"}]
  /\ token \in [Racers -> Gens]
  /\ settledCount \in [Gens -> 0..2]
  /\ lost \subseteq Gens
  /\ (state # "settling") => (pending = "active" /\ owner = None)
  /\ (state = "settling") => owner \in Racers

Init ==
  /\ state = "idle"
  /\ pending = "active"
  /\ gen = 0
  /\ owner = None
  /\ pc = [r \in Racers |-> "idle"]
  /\ token = [r \in Racers |-> 0]
  /\ settledCount = [g \in Gens |-> 0]
  /\ lost = {}

-----------------------------------------------------------------------------
(* Engine actions *)

\* Deliver or redeliver the record under a fresh generation (Cell.Lease).
Lease ==
  /\ state \notin {"active", "settling"}
  /\ gen < MaxGen
  /\ state' = "active"
  /\ gen' = gen + 1
  /\ pending' = "active"
  /\ UNCHANGED <<owner, pc, token, settledCount, lost>>

\* The lease expired or was rebalanced away (Cell.Revoke).
Revoke ==
  \/ /\ state = "active"
     /\ state' = "idle"
     /\ lost' = lost \cup {gen}
     /\ UNCHANGED <<pending, gen, owner, pc, token, settledCount>>
  \/ /\ state = "settling"
     /\ pending = "active"
     /\ pending' = "idle"
     /\ UNCHANGED <<state, gen, owner, pc, token, settledCount, lost>>

\* The owning consumer closed (Cell.Close); close outranks a pending revoke.
Close ==
  \/ /\ state = "active"
     /\ state' = "closed"
     /\ lost' = lost \cup {gen}
     /\ UNCHANGED <<pending, gen, owner, pc, token, settledCount>>
  \/ /\ state = "settling"
     /\ pending # "closed"
     /\ pending' = "closed"
     /\ UNCHANGED <<state, gen, owner, pc, token, settledCount, lost>>

-----------------------------------------------------------------------------
(* Racer actions *)

\* Racer r receives the live delivery: its handle carries the current gen.
Receive(r) ==
  /\ pc[r] = "idle"
  /\ state = "active"
  /\ token' = [token EXCEPT ![r] = gen]
  /\ pc' = [pc EXCEPT ![r] = "holding"]
  /\ UNCHANGED <<state, pending, gen, owner, settledCount, lost>>

\* Racer r holds an arbitrary handle: stale, forged, or from the future.
Forge(r) ==
  /\ pc[r] = "idle"
  /\ \E g \in Gens : token' = [token EXCEPT ![r] = g]
  /\ pc' = [pc EXCEPT ![r] = "holding"]
  /\ UNCHANGED <<state, pending, gen, owner, settledCount, lost>>

\* The CAS active(gen) -> settling(gen) (Cell.Begin). A losing attempt
\* returns an errcode sentinel and the racer's handle is spent.
Begin(r) ==
  /\ pc[r] = "holding"
  /\ IF state = "active" /\ token[r] = gen
       THEN /\ state' = "settling"
            /\ owner' = r
            /\ pc' = [pc EXCEPT ![r] = "settling"]
       ELSE /\ pc' = [pc EXCEPT ![r] = "idle"]
            /\ UNCHANGED <<state, owner>>
  /\ UNCHANGED <<pending, gen, token, settledCount, lost>>

\* A protocol-safe retry may also leave the unknown state (Cell.Retry).
Retry(r) ==
  /\ pc[r] = "holding"
  /\ IF state \in {"active", "unknown"} /\ token[r] = gen
       THEN /\ state' = "settling"
            /\ owner' = r
            /\ pc' = [pc EXCEPT ![r] = "settling"]
       ELSE /\ pc' = [pc EXCEPT ![r] = "idle"]
            /\ UNCHANGED <<state, owner>>
  /\ UNCHANGED <<pending, gen, token, settledCount, lost>>

\* Native outcome: success (Cell.Commit).
Commit(r) ==
  /\ pc[r] = "settling"
  /\ owner = r
  /\ state' = "settled"
  /\ pending' = "active"
  /\ owner' = None
  /\ settledCount' = [settledCount EXCEPT ![gen] = @ + 1]
  /\ pc' = [pc EXCEPT ![r] = "idle"]
  /\ UNCHANGED <<gen, token, lost>>

\* Native outcome: definite rejection (Cell.Reject) lands where the pending
\* revocation says: back to active, or idle / closed if the lease was lost.
Reject(r) ==
  /\ pc[r] = "settling"
  /\ owner = r
  /\ state' = pending
  /\ pending' = "active"
  /\ owner' = None
  /\ pc' = [pc EXCEPT ![r] = "idle"]
  /\ UNCHANGED <<gen, token, settledCount, lost>>

\* Native outcome: ambiguous (Cell.Ambiguous); the delivery stays unknown.
Ambiguous(r) ==
  /\ pc[r] = "settling"
  /\ owner = r
  /\ state' = "unknown"
  /\ pending' = "active"
  /\ owner' = None
  /\ pc' = [pc EXCEPT ![r] = "idle"]
  /\ UNCHANGED <<gen, token, settledCount, lost>>

-----------------------------------------------------------------------------

Next ==
  \/ Lease \/ Revoke \/ Close
  \/ \E r \in Racers :
       Receive(r) \/ Forge(r) \/ Begin(r) \/ Retry(r) \/ Commit(r) \/ Reject(r) \/ Ambiguous(r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

\* Exactly one concurrent terminal attempt wins: at most one racer is
\* settling, and it is the recorded owner of a settling delivery.
OneWinner ==
  /\ Cardinality({r \in Racers : pc[r] = "settling"}) <= 1
  /\ \A r \in Racers : pc[r] = "settling" => (owner = r /\ state = "settling")

\* No generation is terminally settled twice.
NoDoubleTerminal == \A g \in Gens : settledCount[g] <= 1

\* A stale handle never settles: the attempt in flight presents the current
\* generation, and generation 0 (never leased) is never settled.
StaleRejected ==
  /\ \A r \in Racers : pc[r] = "settling" => token[r] = gen
  /\ settledCount[0] = 0

\* A generation whose lease was revoked or closed while active never settles.
LeaseLostNeverSettles == \A g \in lost : settledCount[g] = 0

(* Action properties *)

\* Unknown is left only by Retry (to settling) or Lease (to active).
UnknownSticky == [][state = "unknown" => state' \in {"unknown", "settling", "active"}]_vars

\* Settled is left only by Lease.
SettledSticky == [][state = "settled" => state' \in {"settled", "active"}]_vars

\* Closed is left only by Lease.
ClosedSticky == [][state = "closed" => state' \in {"closed", "active"}]_vars

\* The generation is unchanged or advanced by exactly one lease.
GenMonotone == [][gen' \in {gen, gen + 1}]_vars

=============================================================================
