# RFC 0001: Semantic contract

- **Status:** Open for review during v0
- **Scope:** the current [semantic concepts](../docs/concepts/index.md) and public API

## Summary

This RFC tracks review of the core semantic model: publication outcomes
(accepted / rejected / ambiguous), confirmation modes, delivery guarantees and
ack models, the settlement state machine with generation tokens, ordering
scopes, retry/backoff/failure policies, per-message controls, headers,
capability model, metadata and emulation modes, the error model, context and
lifecycle behaviour, and the memory-ownership contract.

## Review checklist

- [ ] Every outcome of a publish is one of exactly three; ambiguity is never
      misclassified.
- [ ] Every settlement error is reachable and tested (`ErrAlreadySettled`,
      `ErrSettlementInProgress`, `ErrLeaseLost`, `ErrNotSettleable`,
      `ErrSettlementUnknown`, `ErrClosed`).
- [ ] Capability levels cannot promote volatile emulation to a durable
      requirement.
- [ ] Strict defaults are documented with their lenient opt-ins.
- [ ] The memory driver is never described as persistent.

## Implementation choices to review

These describe the current source, rather than decisions established by a
separate design archive:

1. DSN opening uses [`dsn.Registry.Open`](../dsn/registry.go) on a registry
   the program builds; there is no process-wide registry, and the root package
   does not re-export it.
2. The memory engine defaults to one partition; see
   [`DefaultPartitions`](../internal/engine/engine.go).
3. NATS Core has no settlement interface; see
   [`source.Settler`](../drivers/nats/source.go). Its deliveries cannot be
   acknowledged, negatively acknowledged or rejected.
4. [`Level.Rank`](../capabilities.go) ranks `EmulatedDurable` above `Conditional`.
5. [`drivers/snssqs/go.mod`](../drivers/snssqs/go.mod) defines a separate module
   that depends on `drivers/sqs`.
