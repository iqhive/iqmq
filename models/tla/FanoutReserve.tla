---------------------------- MODULE FanoutReserve ---------------------------
(***************************************************************************)
(* Fan-out admission (the memory engine): several publishers fan *)
(* one record out to several subscriptions, each with its own bounded      *)
(* capacity, under one of four admission policies. A publication reserves   *)
(* capacity in the subscriptions it targets, in a stable order, and either  *)
(* turns every reservation it holds into a record or gives them all back.   *)
(*                                                                         *)
(* Implementation: internal/engine (Topic.publishPerSub and the            *)
(* FanoutPolicy family). Executable model: internal/model/fanout.          *)
(* models/tla/TRACE.md maps every action and invariant below onto both.    *)
(*                                                                         *)
(* The state is deliberately counter-free — what the last publication took  *)
(* and what it was refused, rather than how many each subscription has      *)
(* taken — so that the model is finite and TLC exhausts it in seconds.      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Pubs,    \* the publishers, e.g. {p1, p2}
          Subs,    \* the subscriptions, e.g. {s1, s2, s3}
          Cap,     \* records one subscription may hold
          Policy   \* "atomic" | "independent" | "besteffort" | "dropslow"

ASSUME Cap \in Nat /\ Cap > 0
ASSUME Policy \in {"atomic", "independent", "besteffort", "dropslow"}

VARIABLES
  held,        \* subscription -> number of records it holds
  res,         \* publisher -> subscriptions it holds a reservation in
  todo,        \* publisher -> subscriptions it has still to visit
  refused,     \* publisher -> subscriptions that had no room for it
  lastTake,    \* the subscriptions the last commit admitted to
  lastRefused, \* the subscriptions that refused that publication
  everDropped, \* set when a record was discarded to make room
  starved      \* set when a publication every subscription had room for was lost

vars == <<held, res, todo, refused, lastTake, lastRefused, everDropped, starved>>

-----------------------------------------------------------------------------

\* Every publisher visits the subscriptions in the same order, which is what
\* makes concurrent publications unable to wait on each other: nobody holds a
\* reservation in a later subscription while asking for one in an earlier one.
Order == CHOOSE f \in [Subs -> 1..Cardinality(Subs)] :
           \A a, b \in Subs : a # b => f[a] # f[b]

FirstOf(S) == CHOOSE s \in S : \A t \in S : Order[s] <= Order[t]

\* A publisher's phase is read from what it is carrying rather than stored.
Idle(p)       == todo[p] = {} /\ res[p] = {} /\ refused[p] = {}
Reserving(p)  == todo[p] # {}
Committing(p) == todo[p] = {} /\ (res[p] # {} \/ refused[p] # {})

\* The reservations outstanding against a subscription: what publishers have
\* promised themselves room for and not yet used or given back.
Reserved(s) == Cardinality({p \in Pubs : s \in res[p]})

Init ==
  /\ held = [s \in Subs |-> 0]
  /\ res = [p \in Pubs |-> {}]
  /\ todo = [p \in Pubs |-> {}]
  /\ refused = [p \in Pubs |-> {}]
  \* Before any publication nothing has been taken and nothing has been
  \* reached, which is the empty publication: taken {} and refused all.
  /\ lastTake = {}
  /\ lastRefused = Subs
  /\ everDropped = FALSE
  /\ starved = FALSE

-----------------------------------------------------------------------------
(* Publication actions *)

\* Begin a publication: the publisher takes the subscription set and starts
\* visiting it in the fixed order (engine: Topic.Publish loads the
\* copy-on-write subscription snapshot).
Begin(p) ==
  /\ Idle(p)
  /\ todo' = [todo EXCEPT ![p] = Subs]
  /\ UNCHANGED <<held, res, refused, lastTake, lastRefused, everDropped, starved>>

\* Reserve capacity in the next subscription (engine: lane.reserve). The
\* reservation is taken against what the subscription holds plus what every
\* other publisher has already promised itself, which is what makes a
\* reservation a promise rather than a guess.
Reserve(p) ==
  /\ Reserving(p)
  /\ LET s == FirstOf(todo[p]) IN
     /\ todo' = [todo EXCEPT ![p] = todo[p] \ {s}]
     /\ IF held[s] + Reserved(s) < Cap
          THEN /\ res' = [res EXCEPT ![p] = res[p] \cup {s}]
               /\ UNCHANGED <<held, refused, everDropped>>
          ELSE IF Policy = "dropslow" /\ held[s] > 0
                 THEN /\ held' = [held EXCEPT ![s] = held[s] - 1]
                      /\ res' = [res EXCEPT ![p] = res[p] \cup {s}]
                      /\ everDropped' = TRUE
                      /\ UNCHANGED refused
                 ELSE /\ refused' = [refused EXCEPT ![p] = refused[p] \cup {s}]
                      /\ UNCHANGED <<res, held, everDropped>>
  /\ UNCHANGED <<lastTake, lastRefused, starved>>

\* Turn the reservations into records (engine: Topic.commit). Atomic commits
\* only when it was refused nowhere; the other policies commit what they hold.
Commit(p) ==
  /\ Committing(p)
  /\ LET take == IF Policy = "atomic" /\ refused[p] # {} THEN {} ELSE res[p] IN
     /\ held' = [s \in Subs |-> IF s \in take THEN held[s] + 1 ELSE held[s]]
     /\ lastTake' = take
     /\ lastRefused' = Subs \ take
     \* The parentheses are load-bearing: = binds tighter than \/, so
     \* "starved' = starved \/ P" parses as "(starved' = starved) \/ P" --
     \* an action that leaves starved unassigned whenever P holds, which is
     \* to say starved could never become TRUE and NoStarvation could never
     \* fail. The mutation harness is what found that out.
     /\ starved' = (starved \/ (refused[p] = {} /\ take # Subs))
  /\ res' = [res EXCEPT ![p] = {}]
  /\ refused' = [refused EXCEPT ![p] = {}]
  /\ UNCHANGED <<todo, everDropped>>

\* A consumer settles a record, which returns its capacity (engine:
\* Consumer.Ack).
Settle(s) ==
  /\ held[s] > 0
  /\ held' = [held EXCEPT ![s] = held[s] - 1]
  /\ UNCHANGED <<res, todo, refused, lastTake, lastRefused, everDropped, starved>>

-----------------------------------------------------------------------------

Next ==
  \/ \E p \in Pubs : Begin(p) \/ Reserve(p) \/ Commit(p)
  \/ \E s \in Subs : Settle(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
  /\ \A s \in Subs : held[s] \in 0..Cap
  /\ \A p \in Pubs : res[p] \subseteq Subs /\ refused[p] \subseteq Subs
                     /\ todo[p] \subseteq Subs
  /\ lastTake \subseteq Subs /\ lastRefused \subseteq Subs
  /\ everDropped \in BOOLEAN /\ starved \in BOOLEAN

\* No subscription ever holds more than its capacity, and the reservations
\* outstanding against it never promise more than it can still take.
NoOverflow == \A s \in Subs : held[s] + Reserved(s) <= Cap

\* Every subscription a publication has to visit is accounted for exactly
\* once: it is still to be visited, it holds a reservation, or it refused
\* the publication -- never two of those, and never none of them. So a
\* publisher that is not in the middle of a publication holds no
\* reservation, and every reservation it took was turned into a record or
\* given back.
\*
\* Stating it of the three sets rather than of Idle(p) is what gives it
\* teeth. The form this replaces read
\*
\*     \A p \in Pubs : Idle(p) => (res[p] = {} /\ refused[p] = {})
\*
\* and Idle(p) is *defined* as those two sets and todo[p] being empty, so it
\* was the tautology (A /\ B /\ C) => (B /\ C): no defect in this module
\* could falsify it, and the mutation harness is what found that out
\* (models/tla/mutations.txt).
ReservationBalance ==
  \A p \in Pubs :
    /\ res[p] \cap todo[p] = {}
    /\ res[p] \cap refused[p] = {}
    /\ todo[p] \cap refused[p] = {}
    /\ (todo[p] \cup res[p] \cup refused[p]) \in {{}, Subs}

\* Under Atomic a publication reaches every subscription or none of them.
AtomicAllOrNothing ==
  Policy = "atomic" => lastTake \in {{}, Subs}

\* Every subscription a publication did not reach is accounted for as one it
\* was refused by, so a partial delivery is never silent.
PartialAccounted == lastTake \cup lastRefused = Subs /\ lastTake \cap lastRefused = {}

\* Only DropSlow discards a record to make room.
DropSlowAccounted == Policy # "dropslow" => ~everDropped

\* A subscription that never settles cannot take capacity from another: a
\* full subscription never bounds what another may still be promised.
Isolation ==
  Policy \in {"independent", "besteffort", "dropslow"} =>
    \A a, b \in Subs : (held[a] = Cap /\ a # b) => held[b] + Reserved(b) <= Cap

\* A publication every subscription had room for reaches all of them.
NoStarvation == ~starved

(* Action properties *)

\* Every record a subscription gains was reserved for it first: nothing is
\* admitted past the capacity check.
NoUnreservedAdmission ==
  [][\A s \in Subs : held'[s] > held[s] => Reserved(s) > 0]_vars

=============================================================================
