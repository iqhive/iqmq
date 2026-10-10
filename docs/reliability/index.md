# Reliability

The failure modes this library has an opinion about, and what that opinion is.

| Page | The failure |
|---|---|
| [Inbox and outbox](inbox-outbox.md) | your database and the broker disagree about whether something happened |
| [Poison messages](poison-messages.md) | a message that will never succeed, blocking one that would |
| [Graceful shutdown](graceful-shutdown.md) | a deploy in the middle of a handler |
| [Rebalance](rebalance.md) | the broker moves your partition to somebody else |
| [Chaos testing](chaos.md) | proving any of the above, deliberately |

## The three rules underneath them

**Never lose a message to make an operation succeed.** Every branch in the
runner that could drop a message instead requeues it and stops. The
dead-letter route confirms before it acks; an exhausted message halts rather
than being skipped; `mq.FailureSkip` exists and is never a default. The one
place the library chooses a duplicate is the ambiguous dead-letter
publication, and it counts the choice.

**Never report an unknown as a known.** An ambiguous publish outcome, an
ambiguous settlement and an unadvertised limit are three different unknowns,
and each has a representation of its own — `mq.IsAmbiguous`,
`mq.CodeSettlementUnknown`, `Limit[T].Known == false`. The specification calls
misclassifying ambiguity *a disqualifying correctness defect*.

**Exactly one layer owns redelivery accounting.** The specification. Two retry loops
produce a message tried 25 times when you asked for 5, and a message the
driver dead-lettered and the runner requeued. Where the broker has native
retry and dead-letter policy the driver maps it and the runner stands down.

## What is proved, and how

| Property | Held by |
|---|---|
| the record lifecycle's invariants, after **every** step | the memory engine's model fuzzer: one logical state per record, terminal release exactly once, stale tokens cannot settle a newer delivery, conservation per subscription, memory plateau under fixed limits |
| acks equal successes, one settlement per delivery | `FuzzDispatcher` |
| the shutdown ordering, including DLQ-publish-before-ack | `models/tla/Shutdown` and `internal/model/shutdown` — a TLA+ spec and a Go reference model, checked in `make ci` |
| a capability claim, in both directions | the conformance kit, per driver, against a containerised broker |
| linearizability of the memory engine's history | `testkit/linear`, over the histories `internal/engine`'s `TestEngineLinearizable`, `TestEngineLinearizableKeys` and `FuzzEngineLinearizable` record — as tests in `make ci`, and weekly as `make linearize-campaign`, which gives the fuzzer hours instead of seconds and whose own negative control is `make linearize-selftest` |
| an allocation budget on a path that must not allocate | `make allocs` and the gated budgets |

## Checking a deployment

Chaos, soak and differential test packages are available.
`make suites-modes` lists their commands; `conformance/suites.json`
declares which modes each driver supports. Run the relevant modes
and retain their source revision and results before making a claim
about your deployment.
