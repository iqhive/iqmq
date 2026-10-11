# Extensions (`x/`)

The `x/` packages are patterns that sit **above** the messaging contract
rather than inside it. Nothing in the core depends on them, and each one is
opt-in: a middleware you add to a chain, or a store you hand a relay.

| Package | Problem it solves | Status |
|---|---|---|
| [`x/inbox`](inbox.md) | make processing effectively-once, in your database's transaction | shipped |
| [`x/outbox`](outbox.md) | publish what your transaction committed, and nothing else | shipped |
| [`x/claimcheck`](claimcheck.md) | a payload larger than the broker will carry | shipped |
| `x/kafkatx`, `x/pulsartx` | broker transactions, where the broker has them | shipped |

They are `x/` and not core for one reason each, and the reasons are worth
knowing before you reach for one:

- **They are not portable.** An outbox needs your database. A claim check
  needs object storage. A broker transaction exists on two of the twelve
  drivers. The core is the part that behaves the same everywhere.
- **They have policy in them.** What to do when the idempotency cache is
  down, whether an unkeyed message is refused, whether a missing payload is
  poison — these are decisions about *your* system's failure modes, and the
  core does not get to make them for you.
- **Their API may move.** `x/` is outside the API freeze.

## Which duplicate-suppression do I want?

This is the question people get wrong most often, and the two packages that
answer it look almost identical.

|---|---|---|
| Where the record goes | a separate store (Redis, or in-process) | **the same database as your side effect** |
| What it gives you | a *reduced* duplicate rate | effectively-once, for effects in that database |
| When the store is down | fails **open** by default (process) | fails **closed** by default (retry) |
| An unkeyed message | processed, unguarded | refused |
| Needs a transaction? | no | yes — that is the whole mechanism |

Use `x/inbox` when your handler writes to a database you control. Use
`x/idempotency` when it does not — when the effect is an HTTP call, another
broker, or an email — and accept that it reduces duplicates rather than
removing them. [The duplicates page](../concepts/idempotency.md) makes the
argument at more length.

## The three that is binding on

`x/compress`, `x/encrypt` and
[`x/claimcheck`](claimcheck.md) all take bytes from somewhere that is not the
broker, or hand bytes to somewhere that is not the handler, so
[the security page](../security/index.md) applies to all three. Each one's
page has a section saying what it does **not** protect, and those sections are
the most useful part of this documentation.

The shared rule, which comes out of a real defect: **a declared size is a
claim and never an allocation.** All three formats carry a length field, and
in all three it is compared with a configured limit before anything is
allocated, checked again against what actually arrived, and never used to
size a buffer on its own. Two codecs elsewhere in this repository once let a
five-byte input ask for 64 GiB.
