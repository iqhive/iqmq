---------------------------- MODULE FanoutReserveIndependent ----------------------------
(***************************************************************************)
(* FanoutReserve under the independent admission policy. The module adds          *)
(* nothing: it exists so that TLC checks every policy, since *)
(* a configuration binds one constant value per module.                     *)
(***************************************************************************)
EXTENDS FanoutReserve

=============================================================================
