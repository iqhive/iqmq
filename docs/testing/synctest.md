# Testing with `testing/synctest`

`testing/synctest` runs a test inside a **bubble**: a goroutine group with its
own fake clock, where time advances only when every goroutine in the group is
blocked. A test of an hour's backoff finishes in microseconds and — the part
that matters — you can assert *how much time passed*.

This library is built to work in one. `internal/clock` is injected everywhere,
nothing in a hot path calls `time.Now` unless an observer is active, and the
memory engine's scheduler is a single goroutine that sleeps until the next
non-empty slot rather than ticking.

## The two clocks, and which one you want

| | `memory.NewForTest(t)` | `memory.NewForTest(t, memory.TestRealClock())` |
|---|---|---|
| clock | the engine's own fake clock | whatever `time` does — the **bubble's** clock inside `synctest` |
| advance | `broker.Advance(d)` | the bubble advances it when everything blocks |
| use it for | a plain unit test with no bubble | a `synctest` test |

`TestRealClock` is the option that hands time to the bubble. Inside a bubble
the "real" clock *is* fake, so this is not a contradiction: the bubble owns
time, and the engine must not own a second clock that the bubble cannot move.

`broker.Advance` on such a broker returns an error, because there is no clock
of its own to move, and the error says exactly that rather than silently
succeeding.

## The shape

```go
func TestChargeWorkflowInABubble(t *testing.T) {
    const base = time.Hour
    synctest.Test(t, func(t *testing.T) {
        client, broker := memory.NewForTest(t, memory.TestRealClock())
        if err := broker.Advance(time.Second); err == nil {
            t.Fatal("Advance succeeded on a broker left on the wall clock")
        }

        start := time.Now()
        if err := chargeWorkflow(t.Context(), &out, client, base); err != nil {
            t.Fatal(err)
        }
        // Two failures and a doubling backoff based at an hour is one hour
        // then two: exactly three hours of bubble time.
        if got, want := time.Since(start), base+2*base; got != want {
            t.Errorf("took %v of bubble time, want exactly %v", got, want)
        }
    })
}
```

That is `examples/testing-with-synctest`, which is compiled and run by
`make examples` in every `make ci`.

## Why the assertion is the point

Anybody can run a retrying consumer under `synctest` and watch it finish
quickly. What a deterministic test buys is that **the schedule is checkable**:
if the runner stopped waiting between attempts, or capped the delay, or drew it
out of a jitter envelope when the test asked for none, that `time.Since` would
change and nothing else in the test would.

A test that only asserts "it eventually succeeded" passes against a runner with
no backoff at all. This one does not.

## What to assert, by feature

| Feature | The assertion |
|---|---|
| retry backoff | elapsed bubble time equals the sum of the intervals your policy defines. Use `mq.NoJitter` — `mq.FullJitter` is the default and is deliberately not deterministic. |
| a lease expiring mid-handler | the settlement returns `mq.ErrLeaseLost`, and `ConsumerStats.Redelivered` advanced |
| delayed publication | nothing is delivered before the due time, and exactly one thing is at it |
| TTL | the record is gone at the boundary and present just before it |
| a drain timeout | `Shutdown` returns non-nil after exactly `mq.DefaultDrainTimeout` |

## Two rules

**Do not `time.Sleep` in a bubble to "let things settle".** The bubble advances
time when everything is blocked, so a sleep is an assertion about ordering
dressed up as a delay. Block on the thing you are waiting for.

**Use `mq.NoJitter` when you assert a schedule.** The default is
`mq.FullJitter` — it draws uniformly from `[0, interval]`, because the
default is *bounded exponential jittered* backoff and a caller who names a base
and a cap should get the jitter rather than a retry storm. A schedule
assertion against a jittered backoff is a flaky test, and the fix is to name
the jitter rather than to widen the assertion.

## Outside a bubble

`broker.Advance(d)` is the whole mechanism and it is enough for most tests. The
bubble adds two things: your *own* goroutines are also on fake time, and the
"everything is blocked" condition gives you a precise notion of quiescence. If
your code under test has no timers of its own, `Advance` alone is simpler.
