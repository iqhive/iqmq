------------------------- MODULE MemoryLeaseRelaxed -------------------------
(***************************************************************************)
(* MemoryLease with a retry placement that does not seal its lane — the     *)
(* PushFront and SideQueue kinds under relaxed ordering. The *)
(* module adds nothing: it exists so that TLC checks the placement that     *)
(* lets records overtake a redelivery, since a configuration binds one      *)
(* constant value per module.                                              *)
(***************************************************************************)
EXTENDS MemoryLease

=============================================================================
