# The `iqmq` command

`iqmq` is the command-line tool: publish, consume, watch, generate load, and
ask a broker what it can actually do.

```
go install github.com/iqhive/iqmq/cmd/iqmq@latest
```

## What it is for

The library is the product. The CLI exists for the things you cannot do from a
library:

- **`iqmq doctor`** — point it at a DSN and it tells you which driver answered,
  what that driver advertises about itself, what limits it declares, and which
  variant kinds are in force. It is the first thing to run when a deployment
  behaves unlike the documentation, and its output is what a driver bug report
  should carry.
- **`iqmq pub` / `iqmq sub`** — put a message in, take a message out, with
  settlement and confirmation stated rather than implied.
- **`iqmq tail`** — watch a topic, and be told plainly when watching it costs
  somebody else their messages.
- **`iqmq bench`** — see roughly how fast this machine talks to that broker.
- **`iqmq topology plan` / `apply`** — declare the topics and subscriptions a
  deployment needs, see what would change, and then make it so. `Open` never
  creates production topology ((../concepts/index.md)), so something has
  to, explicitly; this is it.
- **`iqmq bridge`** — move messages from one broker to another, with what the
  seam guarantees stated on every run rather than left to be assumed.

## Exit codes

A script branches on these, so they are a contract.

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | the operation ran and did not succeed — the broker refused, a publication was not confirmed, a deadline passed with the work unfinished |
| 2 | the invocation could not be acted on — bad flags, a DSN that does not parse, a scheme no driver claims |
| 3 | the driver does not have a capability the command needs ((../concepts/capabilities.md)) |

Exit 3 is separate from exit 1 on purpose: nothing a caller retries will change
it. `iqmq tail` against a work queue exits 3, and the message names the feature
and the level it read.

## Flags may follow positional arguments

```console
$ iqmq pub memory://?autocreate=true orders --header trace=abc 'hello'
```

Go's `flag` package stops parsing at the first non-flag argument, which would
make that invocation report "a DSN and a topic are required" — true of nothing
you typed. The CLI moves flags ahead of positionals before parsing, so a flag
works wherever you put it.

The one consequence: a positional that begins with `-` has to come after a `--`
terminator. In practice that is a `pub` body:

```console
$ iqmq pub memory://?autocreate=true orders -- '-starts-with-a-dash'
```

## Credentials never reach the terminal

This is a security property, not a convenience, and it is built in two layers
because neither is enough alone.

**Nothing in the CLI renders a DSN parameter or a configuration field itself.**
A target is rendered as scheme, user, endpoints and path — the password becomes
`redacted` and *every* query parameter is dropped, because the CLI cannot know
which of a driver's parameters is a credential and a list it maintained would be
wrong the moment a driver added one. A resolved configuration is rendered by the
driver's own `Config.String`, which (../security/index.md) makes the
driver's obligation and which each driver proves in its own tests.

**Everything the CLI prints then passes through a scrub** holding each credential
it can identify — the DSN's password, the value of every credential-named
parameter, the value of every credential-named environment variable. That layer
exists because the output also carries driver error strings, native client
messages and broker replies, none of which the CLI wrote.

So:

```console
$ iqmq doctor 'sqs://AKIAEXAMPLE:wJalrXUtnFEMI@127.0.0.1:9324?region=elasticmq&sessiontoken=FQoGZ'
target
  dsn                   sqs://AKIAEXAMPLE:redacted@127.0.0.1:9324?<2 parameters, not shown>
  parameters            region sessiontoken=<redacted>
```

The environment is reported by **name and set/unset only**. There is nowhere in
the report for a variable's value to go.

A credential that is also an ordinary word is replaced *wherever it appears*,
which can over-redact: a broker whose password is `artemis` and whose profile is
also called `artemis` produces `profile=redacted`. That is the right trade — a
scrub that skipped short or dictionary-word secrets would be a hole with a
threshold on it — so the CLI **says** when it happened, counting the occurrences
it replaced and printing a note under the report.

Message bodies and headers are a separate question. `sub` and `tail` print the
**body** by default, because showing you the message is what you invoked them
for, and they do **not** print **headers** unless you pass `--headers` — a
bearer token in an `authorization` header is exactly what the specification is about.

## `doctor`

```console
$ iqmq doctor memory://?autocreate=true
```

`doctor` reports four different kinds of fact, and keeps them apart:

1. **The build** — this binary's version, platform, registered drivers and
   schemes, and the driver modules it *cannot* register with the reason. "The
   CLI cannot open it" and "iqmq has no such driver" are different facts.
2. **The target** — the redacted DSN, the driver that claimed the scheme, the
   configuration the driver parsed it into, and whether a connection and a ping
   succeeded.
3. **What the driver advertises** — every feature of the inventory with the
   level and scope *the driver claims*. This is **not probe evidence**. A
   conformance run verifying a claim is a separate fact, and
   the capability matrix renders six distinct states to
   keep advertised apart from verified. Nine features have no behavioural probe
   in any conformance run at all, so a driver advertising one cannot be held to
   it; `doctor` marks those rows `[never probed]` and names all nine in its own
   output rather than leaving them to be looked up.
4. **The variant kinds** — two lists, because they are two facts. The *build*
   list is every family's `Auto` binding, a generated constant chosen by a
   bake-off, with the **provisional** flag that says the campaign was not
   authoritative. The *target* list is the kind actually in force for this DSN,
   and says whether it came from the DSN or from the `Auto` binding.

`--no-connect` reports everything that needs no I/O. `-o json` is the form a
bug report should attach. `--drivers` reports the driver table alone.

## `pub`

```console
$ iqmq pub memory://?autocreate=true orders '{"id":1}'
$ iqmq pub kafka://localhost:9092 orders -H trace=abc -k customer-7 --file events.ndjson
$ iqmq pub sqs://key:secret@aws?region=eu-west-1 orders --confirm durable --receipt
```

Bodies come from positional arguments, or one per line of `--file` (`--file -`
for stdin), or as one whole message with `--no-lines`. `--batch` sends the lot
through `PublishBatch`, and a partly failed batch is reported **per message**:
(../drivers/author-guide.md)'s index alignment is what makes that possible,
and a batch result aligned with anything else is how a batch publish loses a
message silently.

**Confirmation is explicit and the exit status honours it.** `--confirm durable`
asks for a durable boundary and fails at construction where the profile cannot
define one. A publication whose acceptance the broker never reported is **not** a
success: it is counted as `NOT CONFIRMED (acceptance unknown)` and the command
exits 1.

`--delay` requires `FeatureScheduledPublish` and `--receipt` requires
`FeatureReceipts`; both are checked before the publisher is constructed, so the
refusal names the feature rather than arriving at the first message. The
publication controls are folded and validated before a connection is opened
too, so `--priority 999` tells you about the priority and not about the dial.

## `sub`

```console
$ iqmq sub memory://?autocreate=true orders billing --max 10
$ iqmq sub kafka://localhost:9092 orders workers --settle nack --nack-action dead-letter
$ iqmq sub nats://localhost:4222 orders workers --idle 5s --concurrency 4 -o json
```

Settlement is never implicit:

- `--settle ack|nack|reject|none` is what a handled delivery is settled with.
- `--on-stop ack|nack|reject|none` is what a delivery **still in flight when the
  run ends** is settled with, and it defaults to `nack`. `--max 1` against a
  batch of ten gives one delivery back to you and returns nine, rather than
  holding them until their leases expire with nothing in the output saying so.
- Every disposition is counted and printed, and `abandoned` is not spelled the
  same as `acked`.

The settlement flags are capability requirements, checked before the consumer is
opened: `--settle nack` needs `FeatureNack`, `--settle reject` needs
`FeatureDeadLetter` (it is `Nack(NackActionDeadLetter)` — the specification makes "destroy
this" explicit, so Reject is the dead-letter route and not a drop),
`--nack-delay` needs `FeatureDelayedRedelivery`, and `--start earliest` needs
`FeatureReplay`. A settlement the driver refuses as unsupported at run time
exits **3**, not 1, and says that no retry will change it.

Note that naming a dead-letter destination is not the same as having one: a
driver may refuse `reject` because the destination does not exist, and the
reference driver deliberately will not create it for you.

`--settle none` leaves everything unsettled, which makes `sub` a read-only probe
on a driver whose unsettled deliveries are redelivered. It is not free: an
unsettled delivery holds a prefetch slot, so such a run stops when it has filled
the window, and says so.

## `tail`

```console
$ iqmq tail nats://localhost:4222 'orders.>'
```

`tail` starts at the latest message, takes a subscription of its own, and settles
nothing where the driver permits it.

Whether a topic can be tailed at all is **a fact about the broker**. `tail` reads
two capabilities before it opens anything:

| Capability | What it decides |
|---|---|
| `FeatureFanOut` | whether a subscription of tail's own receives a **copy**. Without it, every message tail takes is one a competing consumer does not get — so `tail` refuses with exit 3 unless `--consume` says that is wanted. |
| `FeatureAtLeastOnce` | whether an **unsettled** delivery comes back. Without it, and with explicit acknowledgement, a delivery tail does not settle is lost; `tail` says so and carries on. |

Several drivers cannot be tailed without consuming. That is a capability fact and
not a limitation of this tool, and the refusal quotes the feature and the level it
read so you can check it against the driver's page.

## `bench`

```console
$ iqmq bench memory://?autocreate=true orders --messages 2000
$ iqmq bench memory://?autocreate=true orders --variants
$ iqmq bench kafka://localhost:9092 orders --trace /tmp/iqmq.trace
```

!!! warning "These figures are operational, not iqmq's budgets"

    `iqmq bench` measures *this* machine talking to *that* broker at the moment
    you ran it, alongside whatever else the host is doing. It prints that notice
    every time, and the JSON form carries `"source": "cli-operational"` and
    `"authoritative": false`.

    (../performance/methodology.md)'s budgets come from the `benchmarks`
    module, under `hack/benchguard.sh` on a quiet host, with contention recorded
    and a run discarded when another benchmark touched the machine. `make gate`
    is what holds a change to them. Do not quote a `bench` number as iqmq's
    performance, and do not read two runs of it as a regression gate.

The output carries `GOMAXPROCS`, the CPU count and the host load average for
exactly that reason: a reader who cannot see the load cannot judge the number.

**`--trace PATH`** turns on `runtime/trace`'s flight recorder for the whole run
and writes the window it still holds when the run ends. Read it with
`go tool trace`. It is the tool for "the first two hundred publications were fast
and then something stalled", which a summary line cannot show you.

**`--variants`** runs one arm per kind of every variant family the target selects
through its DSN, and prints a row per arm; **`-- `** sweeps one family
alone. `iqmq doctor` lists which families a target selects; a target that selects
none says so rather than running one arm and calling it a sweep.

A kind of a runtime-selectable family is not automatically reachable from a
connection string, and a sweep says which ones it could not measure. Family
the specification's `custom` nack strategy needs a Go callback (`Config.Decide`), so
`nack=custom` is refused by both Kafka drivers: the family has four kinds and a
DSN can select three. `bench` prints the excluded kind with its reason before
the first arm runs, because three arms presented as a family comparison would be
a lie about what was measured. The memory driver is the special case: its DSN selects
kinds only *in bulk* — `engine=safe` pins every family at once to its
mutex-backed reference kind — so its sweep has two arms that each move nine
families, and it is labelled `engine` rather than a family id, because calling it
The specification would be a lie about what was measured.

## `topology`

```console
$ iqmq topology plan  memory://?autocreate=true --topic orders --subscription orders=billing
$ iqmq topology apply kafka://localhost:9092 -f topology.json
```

`Open` never creates production topology, which is why every other command in
this tool fails against a topic that does not exist. `topology apply` is the
explicit route (../concepts/index.md) keeps for that, over the portable
`Admin` surface — **Ensure operations only**. `DeleteTopic`,
`DeleteSubscription` and `Purge` are the destructive operations and no spec
this tool reads can reach them; `TestApplyNeverReachesADestructiveOperation`
counts the calls through a fake to prove it.

**`plan` performs no mutation.** It reads what exists and prints what apply
would do. `--no-connect` plans without opening anything, and then says so on
every row rather than presenting a guess as a reading.

The desired topology comes from flags, from a JSON file, or from both:

```json
{"version": 1,
 "topics": [{"name": "orders"}, {"name": "orders/dead"}],
 "subscriptions": [{"topic": "orders", "name": "billing"}]}
```

`"version"` is **required** — a format that accepts an unversioned file can
never introduce a version later without guessing which grammar an old file was
written in — and an **unknown field is refused**, applying the same strictness as
DSN parsing: a misspelled key that is silently ignored is a
deployment that quietly does not provision what its author wrote down.

A subscription on the command line is written `TOPIC=SUB`, because `=` is the
one byte a portable topic name cannot contain (the grammar is segments of
`ALPHA`/`DIGIT`/`.`/`_`/`-` joined by `/`), so the split is unambiguous for
every name this tool accepts.

### Three things it reports that nothing else does

**`apply` ensures and then RE-READS.** `EnsureTopic` returns nil whether it
created the topic or found one, so the call's own success is not evidence that
anything is there. Asking again is. An object that was ensured and is still
absent is reported as `ENSURED BUT NOT VISIBLE` and fails the run, because that
is a defect in the driver and not in the spec.

**A state nothing could read is `unknown`, not `absent`.** the specification's `Admin` has
no `ListSubscriptions`, so the only read-only question there is about a
subscription is `Stats`. A `CodeNotFound` answer is evidence of absence;
`ErrUnsupported` is evidence of nothing, and a plan that rendered the second as
the first would promise a creation it had no reason to believe was needed.

**The two `Admin` facts are kept apart**, the way `doctor` keeps advertised
apart from verified: the level the driver claims for `FeatureAdmin`, and
whether `Client.Admin()` actually answered one. `doctor` can only ever report
the first; this command produces the second, so it can compare them — and a
disagreement either way round is a defect in the driver, printed as one.

An **unregistrable** target is reported with its reason rather than as absent:

```console
$ iqmq topology plan amqp10://host --topic orders
iqmq topology plan: amqp10://host: the scheme "amqp10" is claimed by
  github.com/iqhive/iqmq/drivers/amqp10, which this binary cannot register:
    the generic AMQP 1.0 core ships no Profile value, and its Register takes one; …
```

"No driver claims that scheme" and "a shipped module claims it and this binary
cannot register it" read the same to a user and mean entirely different things.
A scheme two modules claim — `nats://` and `natsjs://`, which
`drivers/natsunified` claims as well — reports which driver answered.

## `bridge`

```console
$ iqmq bridge natsjs://localhost:4222 orders bridge kafka://localhost:9092 mirror
$ iqmq bridge --dry-run memory://?autocreate=true orders bridge memory:// mirror
```

A bridge settles on two brokers that know nothing about each other, so nothing
can make the destination's acceptance and the source's acknowledgement one
atomic step. The order is fixed and it is the whole design:

    receive -> publish -> flush (CONFIRMED, not merely admitted) -> settle the source

so the guarantee is **at-least-once, with duplicates possible on the
destination**, and the command prints that sentence on every run. A crash, a
cancelled context or a broker that stops answering between the confirmation and
the source settlement leaves the source delivery unsettled: the source
redelivers a message the destination already has. The reverse order — settle,
then publish — loses messages instead, and that is the trade: a duplicate is
something a destination can be built to tolerate, a lost message is not. There
is no third order without a transaction spanning both brokers, which the specification does
not offer.

Two details of that order are easy to get wrong and both are load-bearing:

- **`Flush`, not just `Publish`.** `Publish` returning nil means the message
  was *admitted* to the driver, which for a buffering driver is not the same as
  accepted by the broker ((../concepts/confirmation.md)). Settling the source
  against an admission is the same lost message as settling before publishing.
- **Nack on every failure path.** A message the destination refused is a
  message the source still owes. `bridge` refuses to start against a source
  that cannot nack (`FeatureNack`, exit 3), because there the refusal of a
  publication would become a lost message.

The run counts what the guarantee cost, and the counters are the point:

| Counter | What it means |
|---|---|
| `moved` | confirmed on the destination **and** settled on the source: nothing outstanding |
| `publish-failed` | the destination did not take it; the source delivery was returned, so nothing is lost |
| `ambiguous` | the publication failed with the destination's acceptance **unknown** — every `Flush` failure is one, and so is a `Publish` failure the driver marked ambiguous. The destination may hold a message the source is about to redeliver |
| `duplicate-risk` | the destination confirmed and the source would **not** settle. That message moved *and* will come again |
| `returned` | handled and handed back with `--on-stop` (every delivery of a `--dry-run`) |

`--max N` receives at most N deliveries, so the run takes nothing from the
source that it would only hand back. `--dry-run` takes `--on-stop nack` or
`none` and refuses `ack` and `reject`, because a dry run must leave the source
as it was.

`--dedup-from id` asks the destination to suppress the duplicates by carrying
each source message's id as the publication's de-duplication key
((../concepts/idempotency.md)). It requires `FeatureDeduplication` and refuses
without it — and it does not change the guarantee: the de-duplication *window*
belongs to the destination broker, so a redelivery that arrives after the window
has closed is a duplicate the destination accepts. A source delivery with no
message id gets no key, and those are counted as `unkeyed` rather than passed
over in silence.

The bridge is **single-flight by design**: one message is confirmed before the
next is received, so the destination sees the source's order wherever the source
delivers in order. A concurrent bridge would be faster and would reorder, and a
flag that silently reordered a stream somebody bridged *for* its order is not a
default worth offering.

[`examples/bridge`](../examples/index.md) is the same pattern as a program, and
it produces the duplicate rather than describing it.

## `doctor` against a harness

`hack/doctor-harness.sh` runs `doctor` against the brokers
`conformance/suites.json` names, one container at a time, from the digests
`conformance/harness/images.lock` pins:

```console
$ hack/doctor-harness.sh list          # what can be reached, and what cannot
$ hack/doctor-harness.sh run nats      # start the broker, run doctor, remove it
```

The suite list is the single source of truth for which harnesses exist, so the
script has no list of its own. A suite entry carries a `doctor` recipe only once
somebody has run `doctor` against that broker and recorded what worked --
`make suites-check` refuses a recipe with no verification date -- and `list`
reports a suite without one as unreachable rather than guessing, so the absence
is visible instead of silent.

Seventeen of the twenty-two suites have a verified recipe and all seventeen pass.
The five that do not each have a different reason, and none of them is the CLI's:

| Suite | Why `doctor` has no recipe |
|---|---|
| `memory` | in-process. There is no container and `memory://` needs no address; `iqmq doctor memory://` works and is covered by the unit tests. |
| `natsunified` | its schemes are `nats://` and `natsjs://`, which the two profile drivers already claim, so this binary does not register it at all. `doctor --drivers` says so with the reason. |
| `goredis` | exports no `Register` by design: it is the second client library behind one capability table, and a connection string cannot choose between two client libraries. |
| `rabbitmq-delayed` | its image is **built** from `hack/images/rabbitmq-delayed/`, not pulled, so `conformance/harness/images.lock` pins no digest for it and the script will not start an image it cannot pin. |

The seventeen that pass cover **sixteen distinct driver profiles**: NATS Core,
Kafka (franz-go and kafka-go), NSQ, Pub/Sub (emulator), SQS standard, SQS FIFO,
SQS on LocalStack, SNS+SQS, Redis Streams, RabbitMQ over AMQP 0-9-1 and over
AMQP 1.0, ActiveMQ Artemis and Classic over AMQP 1.0, and both of them over
STOMP, plus Pulsar. Each one reports its own profile, its client library's
version and, where the broker tells it, the broker version.

## What the CLI could not express

The CLI is the first consumer of the DSN layer, the driver registry and the
capability model from outside the library's own tests, and a few things it
wanted are not there. They are recorded rather than worked around silently:

- **Not every shipped driver can be registered at once.**
  `drivers/natsunified` claims `nats://` and `natsjs://`, which `drivers/nats`
  and `drivers/jetstream` already claim, and `dsn.Registry.Register` treats a
  second claim as a conflict rather than a replacement — deliberately, since
  resolving it by import order is a decision nobody reading the program can see.
  The CLI registers the two profile drivers, so that `doctor` can name the
  profile that answered.
- **`drivers/amqp10`'s generic `amqp10://` scheme is not registrable from
  outside.** Its `Register` takes a `Profile`, and the module ships no generic
  `Profile` value, so a caller must author one in Go.
- **`drivers/gcppubsub/v1compat` and `drivers/redis/goredis` export no
  `Register`**, by design: each is the second client-library arm of a variant
  family behind one capability table, and one scheme cannot select between two
  client libraries.
- **`memory.Config` implements neither `String` nor `LogValue`**, the two
  renderings the specification asks a configuration for. It carries no credential, so nothing
  leaks; it does mean the CLI cannot render a memory configuration the uniform
  way it renders every other driver's, and reports the resolved engine kinds
  instead.

`doctor` reports the first three as unregistered **with the reason**, so the gap
is visible in the tool rather than only in this page.
