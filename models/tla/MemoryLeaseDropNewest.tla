----------------------- MODULE MemoryLeaseDropNewest -----------------------
(***************************************************************************)
(* MemoryLease with a lane too small for the records offered to it, under   *)
(* the DropNewest overflow policy: the publication a full lane had no room  *)
(* for is discarded, counted and reported as taken, so its payload is       *)
(* released by the engine. The module adds nothing; a configuration binds   *)
(* one constant value per module.                                          *)
(***************************************************************************)
EXTENDS MemoryLease

=============================================================================
