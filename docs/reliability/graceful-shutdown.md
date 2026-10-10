# Graceful shutdown

What happens between SIGTERM and the process exiting, and what the library
guarantees about it. Specification the ordering is modelled in
`models/tla/Shutdown` and `internal/model/shutdown`, both checked in every
`make ci`.

## The two calls

```go
client.Shutdown(ctx)   // graceful, bounded by ctx: flush, drain, close
client.Close()         // prompt: stop now
```

`Shutdown` flushes publishers, drains consumers and then closes the backend.
It is idempotent, and closing a child handle afterwards is also idempotent —
which matters, because a `defer consumer.Close()` inside a function whose
caller already called `Shutdown` is the normal shape of Go code and must not be
an error.

`Close` is the prompt version, for when the deadline has passed and you would
rather lose the in-flight work than the process.

The API provides a prompt `Close` **and** a
graceful `Shutdown`, rather than one method with a flag.

## The order, and why each step is where it is

1. **stop accepting.** New publications and new fetches are refused with
   `mq.ErrClosed`. Nothing new enters the system, so the set to drain is
   finite.
2. **flush publishers.** Asynchronous and batched publications that were
   admitted are sent and confirmed. A publication the publisher *accepted* is
   a publication it owes you, so this is not optional.
3. **drain consumers.** Handlers already in flight run to completion and
   settle. The wait is `mq.DefaultDrainTimeout` — 30 seconds — or
   `mq.WithDrainTimeout(d)`; past it their contexts are cancelled.
4. **requeue what was not settled.** Whatever the runner held and did not
   settle goes back to the broker, so it is redeliverable rather than sitting
   on a lease nobody will settle. `ConsumerStats.InFlight` returns to zero.
5. **close the backend.** The client library's connections go last, because
   steps 2 and 4 need them.

Step 4 is the one that is easy to get wrong and is the reason this ordering is
in a TLA+ spec rather than in a comment. A runner that closed its source
before requeueing would strand every unsettled delivery on an expiring lease:
no loss, but a redelivery storm one lease-timeout later, on a broker that has
just lost a consumer.

## What a handler sees

Its context is cancelled when the drain timeout expires — or earlier, if you
passed `Shutdown` a context with a nearer deadline. A handler that respects
its context stops; a handler that does not is abandoned, its delivery is
unsettled, and the message comes back.

So the handler contract during shutdown is exactly the handler contract at any
other time: **return promptly when your context is done, and do not settle
after you have returned.** A handler that ignores its context turns a graceful
shutdown into a hard one.

`mq.WithLeaseDeadline()` helps here for a different reason: it ends the
handler's context with the *lease*, so a handler cannot keep working past the
point at which its work no longer counts — which during a shutdown is usually
sooner than the drain timeout.

## Ordering during a drain

Under `mq.OrderingStrict`, a delivery in a backoff wait when the shutdown
starts is **requeued rather than retried**. The runner does not sit out the
remaining backoff during a drain and it does not skip the message either: it
gives it back, and the next process gets it. That is the branch that makes a
shutdown in the middle of a retry loop lose nothing.

## The recurring defect this is the fix for

The engine's most-repeated bug in this repository has been *a record put into a
lane, a store or a retained log while its subscription or the engine is
closing*: the placing goroutine checks the closed flag, `Close` drains, and the
placement lands after the drain — so the payload's release callback never runs
and the engine-wide reservation is consumed for ever, breaking the specification's
terminal-release invariant.

It has been publish-versus-close, nack-versus-close twice, and
Seek-versus-close. The established fix is to re-read the closed flags after
placing and re-drain, or to place under the same mutex the drain takes; and the
established *test* is thousands of rounds under `-race`, because the known
instances appeared at five to twenty failures per two or three thousand rounds.
A hundred rounds proves nothing, which is why this paragraph is in the
documentation and not only in the playbook.

## A caveat about goroutines

`Shutdown` returning does not always mean every goroutine is gone, and the
reason is not in this library: some client libraries hold a goroutine past a
close — up to one of their own timeouts — and there is nothing the driver can
do about it. The driver pages record the known instances with the upstream
cause, and the drivers' own tests use `goleak` so that a *new* one is a test
failure rather than a slow leak in production.
