# `x/claimcheck`

Park a payload the broker will not carry, publish its address, fetch it on
the way in.

```go
store, err := claimcheck.NewFileStore(claimcheck.FileOptions{
    Name: "orders-payloads", Dir: "/var/lib/orders/payloads", TTL: 14 * 24 * time.Hour,
})                              // or s3store.New(…), or claimcheck.NewMemoryStore(…) in tests
cfg := claimcheck.Config{Store: store, Threshold: 64 << 10, RequireDigest: true}
pm, err := claimcheck.PublishCheckIn(cfg)
mw, err := claimcheck.CheckOut(cfg)
```

Every broker has a maximum message size — 256 KiB or 1 MiB on SQS, 5 MiB on
Pulsar, 1 MiB by default on Kafka — and `mq.ErrMessageTooLarge` is refused
against the driver's advertised `MaxMessageBytes` *before* any I/O, so
discovering the limit costs no round trip.

## The reference

```
magic     4 bytes  "IQK1"
flags     1 byte   reserved; a non-zero value is REFUSED
digest    1 byte   0 = none, 1 = SHA-256
size      8 bytes  big-endian uint64
storeLen  2 bytes
keyLen    2 bytes
store, key, digest
```

`ParseRef` reads it without allocating. A body under `Threshold` is published
as it is, so a topic carries a mixture and the consumer tells them apart by
the magic.

## What happens when the store is unreachable

The specification requires this to be stated rather than discovered, and the three answers
are deliberately **asymmetric**.

| Situation | What happens | Why |
|---|---|---|
| Unreachable while **publishing** | the publication fails; nothing reaches the broker | a reference to an object that was never uploaded is a message that can never be handled |
| Unreachable while **consuming** | `ErrStoreUnavailable`, `CodeUnavailable`, retried, **not acked** | acking a message whose payload could not be fetched is silent loss |
| Payload **gone** (the store answered: no such object) | `mq.DeadLetter(ErrPayloadGone)`, `CodePoisonMessage`, **permanent**, decided on the **first** attempt | no retry can bring it back; the failure policy decides at once (`WithDeadLetter` else Halt) |

The two consume-side cases are the most consequential thing a `Store`
implementation gets right or wrong. A timeout, a refused connection or a 5xx
is `ErrStoreUnavailable`. A definite 404 is `ErrPayloadGone`. Reporting the
wrong one either dead-letters good messages for a transient outage, or
retries a lane for ever.

## What a consumer sees when the payload is gone

The store answered, and the object does not exist — most often because the
store's TTL passed before a redelivery arrived. `CheckOut` returns
`ErrPayloadGone` wrapped in `mq.DeadLetter`, so:

- the handler **does not run**;
- with `WithDeadLetter`, the message is dead-lettered on its **first**
  attempt — no backoff, no `MaxAttempts` spent on an answer that cannot
  change — with the **reference as its body** (`ParseRef` reads it, and it
  names the store and key that went missing) and `ErrPayloadGone`'s text in
  its `iqmq-failure` header;
- without one, the runner **halts** with a `*mq.PoisonError` wrapping
  `ErrPayloadGone`, and the message goes back to the broker rather than being
  dropped.

`errors.Is(err, claimcheck.ErrPayloadGone)` and `mq.CodeOf(err) ==
mq.CodePoisonMessage` both still hold. Both arms are proved through a real
runner over the memory driver, with a `FileStore` whose TTL has passed.
Every other `CheckOut` failure is returned as it is, so
the runner's configured backoff and bound decide.

## Can a claim be settled while its payload is gone?

Yes — and the rule that makes that survivable is:

> **The consumer never deletes the payload.**

`Store` has no `Delete` method, and a test asserts that it does not. Deletion
is the store's own lifecycle policy — an S3 lifecycle rule, a GCS TTL — set
longer than the broker's retention plus its longest redelivery window.

Delete-on-ack looks obviously right and breaks at-least-once delivery. The
substrate redelivers: a lease that expires after the handler succeeded, a
rebalance, a consumer restart, an operator replaying a day. Each of those is
a second delivery of a message whose payload the first delivery has just
deleted, and that second delivery is then dead-lettered as poison — a message
that *was processed successfully*, reported as broken.
Delete-on-dead-letter is worse still: the dead-letter consumer needs the
payload most.

So a settled claim may outlive its payload, and a redelivery after the TTL is
`ErrPayloadGone`. That is a bounded, visible failure with a knob — the TTL —
rather than a silent one, and it is the cost of the pattern rather than a
defect in this implementation.

## The digest is not decoration

It is on by default, and reaching `DigestNone` requires writing
`claimcheck.WithoutDigest(cfg)` — it cannot be selected by leaving a field at
its zero value.

The payload arrives from an external store, so it is **remote input**, and
[the security page](../security/index.md) applies to it exactly as it applies
to a broker-delivered body. A key collision, a stale replica, an operator
restoring a backup, or somebody with write access to the bucket hands your
handler a body the publisher never sent. The digest is recorded at publish
time, travels in the reference, and is checked against what arrives.

It is a digest and not a MAC, so it does not authenticate the *publisher*: an
attacker who can write both the bucket and the topic can produce a consistent
pair. [`x/encrypt`](encrypt.md) is what closes that.

Without a digest the only check left is the size, which still catches a
truncated object.

## The size cap

`Config.MaxPayload` bounds one fetched payload, and the recorded size is a
claim:

1. a reference declaring more than the limit is `ErrTooLarge` **before the
   fetch** — an oversized payload costs no transfer;
2. the fetch is bounded again by the limit, because the *store* decides how
   many bytes exist;
3. the fetched payload's length and digest are checked against the reference.

## Errors

| Error | Code |
|---|---|
| `ErrNotRef`, `ErrBadFlags`, `ErrDigestMismatch`, `ErrSizeMismatch` | `CodeCorruptMessage` |
| `ErrUnknownDigest` | `CodeUnsupported` |
| `ErrTooLarge` | `CodeMessageTooLarge` |
| `ErrPayloadGone` | `CodePoisonMessage` |
| `ErrStoreUnavailable` | `CodeUnavailable` |
| `ErrWrongStore`, `ErrDigestRequired`, `ErrInvalidConfig` | `CodeInvalidConfig` |

## The file store

`NewFileStore` keeps each payload in a file of its own under one directory.

- **Atomic.** `Put` writes a temporary file, syncs it, renames it to its key
  and syncs the directory, so a reader finds either nothing
  (`ErrPayloadGone`) or the whole payload — never a prefix. A test polls the
  directory during a 32 MiB `Put` to prove it.
- **Bounded.** `MaxBytes` (default 1 GiB) bounds the payloads the store holds,
  and a `Put` past it is refused, never evicted, for the same reason as the
  memory store: `ErrStoreUnavailable` (retryable; the TTL sweep frees room)
  while the store is full, and `ErrTooLarge` only for a payload bigger than
  `MaxBytes` itself. The count is read from the directory at open and
  at every sweep and is exact for the process's own writes; several processes
  sharing a directory each lag the others' writes by at most one sweep.
- **A TTL, which is its lifecycle policy.** A payload older than `TTL`
  (default fourteen days) is gone: `Get` says so whether or not the sweep has
  deleted it yet, so what a consumer sees does not depend on the sweep's
  timing. The store's own goroutine sweeps every `SweepEvery` (default TTL/16,
  between a minute and an hour); a negative `SweepEvery` leaves the sweep to
  you through `FileStore.Sweep`. Set `TTL` to the broker's retention plus its
  longest redelivery window.
- **A key cannot leave the directory.** Keys are 32 hex characters the store
  generated. Anything else — `../secret`, an absolute path, a temporary
  file's name — is `ErrPayloadGone` without touching the filesystem, and every
  access goes through an `os.Root`, so a symbolic link planted in the directory
  reaches nothing outside it.
- **The three failures stay distinct.** A missing or expired file is
  `ErrPayloadGone`; any other filesystem error is `ErrStoreUnavailable`,
  joined with its cause; a file over the caller's limit is `ErrTooLarge`,
  decided from its size before it is read.

`Close` stops the sweep, waits for the operations in flight and deletes
nothing: the payloads are the durable half of messages still in the broker.

## The S3-compatible store

`github.com/iqhive/iqmq/x/claimcheck/s3store` is its own module, because the
AWS SDK is a dependency the root module may not have. It works against Amazon
S3 and anything that speaks its API — MinIO, Ceph RGW, R2.

```go
store, err := s3store.New(s3store.Options{
    Client: s3.NewFromConfig(cfg),   // lent: the store never closes it
    Bucket: "orders-payloads",
    Prefix: "claims/",
})
```

What S3 says decides the answer, and only one thing S3 says is a definite
absence:

| S3 answered | The store reports | Why |
|---|---|---|
| `NoSuchKey` | `ErrPayloadGone` | the bucket answered and the object is not there |
| `AccessDenied` (403) | `ErrStoreUnavailable` | a permission is configuration; and S3 answers a missing key with 403 whenever `s3:ListBucket` is not granted |
| `NoSuchBucket` (404) | `ErrStoreUnavailable` | a missing bucket is configuration: dead-lettering on it would empty the topic into its dead-letter queue for a typo |
| a 5xx, a throttle, a timeout, a refused connection, a cancelled context | `ErrStoreUnavailable` | might succeed later |

**Grant `s3:ListBucket`** on the payload bucket, or on its prefix. Without it
S3 cannot tell a consumer that a payload is gone rather than forbidden, and
this store will retry those payloads instead of dead-lettering them.

Keys are the prefix followed by 32 random hex characters, and a reference
naming any other key is `ErrPayloadGone` **without a request**: a forged
reference cannot read another object in the bucket into a handler. The limit is
decided from the response's `Content-Length` before the body is read, and a
body with no length is read no further than the limit — a test fetches a
chunked 64 MiB body with a 64 KiB limit and holds the allocation under 8 MiB.

Nothing deletes: set a lifecycle rule on the prefix, longer than the broker's
retention plus its longest redelivery window. If the server rejects the SDK's
default request checksums, set the client's `RequestChecksumCalculation` to
`aws.RequestChecksumCalculationWhenRequired`.

It is tested against an in-process S3 in every `make ci` — the SDK's own
signing and error parsing over real HTTP — and against MinIO when
`IQMQ_MINIO_ADDR` names one. The pinned image is
`quay.io/minio/minio@sha256:14cea493…` in `conformance/harness/images.lock`:
Docker Hub no longer serves `minio/minio`.

## One contract for every store

`x/claimcheck/storetest` is the contract the memory store, the file store and
the S3 store all run — round trip, copy semantics, distinct keys, gone is
permanent, an outage is retryable, the limit, forged keys, close, a stable
name — and the one to run against a store of your own:

```go
storetest.Run(t, storetest.Harness{
    New:    func(tb testing.TB) claimcheck.Store { return myStore(tb) },
    Remove: func(tb testing.TB, s claimcheck.Store, key string) { deleteBehindItsBack(key) },
    Break:  func(tb testing.TB, s claimcheck.Store) func() { takeItDown(); return bringItBack },
})
```

The contract has been watched failing: `x/claimcheck` runs it against a store
carrying each of nine defects in turn and requires the right case to fail.

## The memory store

`NewMemoryStore` is for tests and examples. It refuses at its bound, with the
retryable `ErrStoreUnavailable`, rather than evicting, because an evicted payload is a message that will be
dead-lettered as poison, and silently converting a memory bound into poison
messages is worse than refusing the publication.

`SetUnavailable`, `SetGone` and `Overwrite` are exported rather than
test-only hooks: the three failure modes above are the interesting part of
this package, and somebody writing a real S3 store needs to test their own
handler against them.
