# Testing

Testing code that uses a message broker, and testing this library itself.

| Page | |
|---|---|
| [Memory driver](memory-driver.md) | the in-process engine as a test double that is also a real driver |
| [synctest](synctest.md) | deterministic tests of backoff, leases and scheduled delivery, with no sleeping |
| [testkit](testkit.md) | the conformance kit: prove a driver's capability claims in both directions |
| [Conformance](conformance.md) | running the suite, its run modes and generating fresh reports |
| [Fuzzing](fuzzing.md) | 130 fuzz targets, the differential shape, and the ratchet |

## The short version

**Use `memory.NewForTest(t)`.** It is not a mock. It is the same `Backend`, the
same runner and the same settlement state machine your production driver uses,
with `AutoCreate` on, a fake clock installed, and a goroutine-leak check
registered on `t.Cleanup`.

```go
func TestBilling(t *testing.T) {
    client, broker := memory.NewForTest(t)
    // …publish, run a handler, assert…
    _ = broker.Advance(30 * time.Second)   // drive the scheduler; do not sleep
}
```

**Never sleep in a test of a backoff.** `Broker.Advance(d)` moves the fake
clock, so a test of a 30-second retry takes microseconds and gives the same
answer every time. Under `testing/synctest` you can go further and assert the
*schedule* — see [synctest](synctest.md).

**Inject faults rather than arranging them.** `broker.Faults()` is the specification's
injector: reject the *n*th publication, delay or fail a settlement, force a
duplicate delivery, expire leases, close consumers, simulate reconnect
generations, fail a dead-letter publication. Every counted fault carries a
counter, and **a fault that fired zero times is how you discover that your test
did not reach the path it was written for**.

**Assert on error codes, not on messages.** `mq.CodeOf(err)` and
`errors.Is(err, mq.ErrLeaseLost)`. The codes are the stable interface.

## What testing this library looks like

Because it is worth knowing which of these an ordinary contribution has to
satisfy:

| Check | What it is |
|---|---|
| `make test-race` | every module, under `-race` |
| `make allocs` | `testing.AllocsPerRun` with **exact** counts on the hot paths the specification names |
| `make fuzz-seeds` | every committed corpus input, under `-race`, in every CI run |
| `make ratchet` | the fuzz-target count only rises, and parsing or stateful packages must have one |
| `make modelcheck` | the Go reference models, including the record lifecycle's invariants after every step |
| `make tla` | the TLA+ specifications, `models/tla/Shutdown` among them |
| `make conformance-memory` | the kit against the memory driver, no container |
| `make critical` | every driver critical tests, in-process against fakes |
| `make docs-check` | this documentation's own shape: the specification sections, links, anchors, nav reachability |

The rule underneath all of them, from the contributor notes and earned the hard
way: **watch a new check fail before you make it pass.** The single
most-repeated defect in this repository has been a check that could not fail —
nine instances — and every one of them was green.
