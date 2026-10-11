------------------------ MODULE MemoryLeaseOverflow ------------------------
(***************************************************************************)
(* MemoryLease with a lane too small for the records offered to it, under   *)
(* the DropOldest overflow policy: a full lane displaces the record it has  *)
(* held longest so that a newer one can take its place. The module adds     *)
(* nothing; a configuration binds one constant value per module.           *)
(***************************************************************************)
EXTENDS MemoryLease

=============================================================================
