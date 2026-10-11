# testkit — the conformance kit

`github.com/iqhive/iqmq/testkit` is the suite every driver has to pass, and
the machine-readable report it produces. Specification.

## What it is for

A driver's capability table is a set of claims, and the specification says they have to be
true. The kit is what makes them checkable: **every scenario is gated by a
capability, and every claim is proved in both directions** — the feature works
when the driver says it does, and the unsupported path is *refused* when it
says it cannot.

A claim proved in only one direction is not proved, and that is not a nicety.
Two drivers are invisible to a happy-path suite: one that advertises delayed
redelivery and silently redelivers immediately, and one that says it cannot
dead-letter while its reject destroys the message.

## It uses only the portable API

Every scenario goes through `mq.Client` and the handles it opens. Nothing in
the kit reaches for the SPI, for a driver's own types, or for `Native`. A
conformance kit that used a driver's internals would pass a driver whose
*public* behaviour was wrong, which is the one thing it exists to prevent.

That also makes it usable on your own driver, from outside this repository.

## Running it

```go
func TestConformance(t *testing.T) {
    testkit.RunConformance(t, testkit.Harness{
        Driver:    "mydriver",
        Open:      open,
        TopicName: func(p string) string { return "conformance." + p },
        Timeout:   30 * time.Second,
        Evidence:  testkit.EvidenceInProcess,
    })
}
```

`RunConformance` is the `*testing.T` entry point. `Run` is the same suite
*without* one, and it exists for a specific reason: it is what lets the kit's
own tests check that it **rejects a driver it should reject**. A conformance
kit nobody has watched fail is assumed not to work.

Write the test in `package <driver>_test`, not in the driver's own package.
`drivers/nats/conformance_test.go` is the model. A harness that reaches
unexported helpers works for its own driver and is useless as an example for
anybody else's — which is exactly the state `memory/conformance_test.go` is in,
and it is recorded as such.

## `Evidence` is part of the report

| Value | Means |
|---|---|
| `testkit.EvidenceInProcess` | no broker; the memory driver |
| `testkit.EvidenceContainer` | a real broker in a container |
| `testkit.EvidenceEmulator` | a vendor emulator — Pub/Sub's, LocalStack, ElasticMQ |
| `testkit.EvidenceManaged` | a real managed service |

This is not bookkeeping. An emulator's evidence is **never** presented as the
managed service's: the capability matrix renders such a cell with `◐`, and
The specification layer 4 is where a managed-service claim belongs. A report that claimed
`EvidenceContainer` for ElasticMQ would be the specific dishonesty the whole
generated-matrix machinery exists to prevent — and no run has ever carried
`EvidenceManaged`.

## What a driver has to supply

Beyond `Open`: the things the kit cannot know.

* **`TopicName`** — the driver's own naming, so the kit's topics do not collide
  with anything.
* **`Wildcard`** — the subject syntax, where the driver has wildcards. The kit
  cannot guess `orders.*` versus `orders.#`.
* **`Secrets`** — for the redaction scenario.
* **`Faults`** — the injection hook, where the driver has a fake backend to
  inject into. `testkit.Faults.LeakSecret` already exists and is wired into
  the kit's own fake, and so do the three the negative probes are held to:
  `ShareSubscriptions` (two subscription names are one subscription, which is
  a queue broker's shape and what the SPI cannot declare), `RefuseOpenSource`
  (a refusal from a call that is not the feature's) and `NackError` (a driver
  that answers the feature's own call with something that is not a refusal).

## Guarantees, behaviours, and the driver that does something else

Every claim is checked in both directions, and the negative direction — *the
driver says it cannot do this; prove it refuses* — has a question behind it
that only sometimes has an answer.

A **behaviour** is a call the driver can refuse: a delay, a lease extension, a
seek, a dead-letter route. One run settles it in both directions.

A **guarantee** is a promise about every run: an ordering, a delivery
multiplicity, a durability. There is no call to refuse, so one run can observe
it *broken* and no number of runs can observe it *absent*. `testkit.Kind` says
which a result is evidence about, and the report carries it.

The third case is the one that took longest to name: a driver that **accepts
the feature's own call and does something else**. A queue broker takes two
subscription names and makes them one subscription; a broker that deletes a
settled message takes a second reader at the earliest position and has nothing
to give it; a driver with settlement and no at-least-once takes a `Nack` and
correctly never redelivers. Nothing is refused, the call succeeds, and the
difference shows up only in *what arrives*. A probe says so with
`testkit.Diverged`, and the two directions read it oppositely:

| the driver | negative direction (`.../refused`) | positive direction (`.../works`) |
|---|---|---|
| refuses the feature's own call | **pass** — the denial is proven, and the note names the call | **fail** — it advertises the feature and refuses it |
| does the feature | **fail** — an unverified negative claim is how a caller comes to rely on behaviour the driver may remove | **pass** |
| accepts the call and does something else | **unrefuted** — a skip that says what happened; the result's kind becomes `guarantee`, so the matrix renders `unrefuted` rather than `unverified` | **fail**, naming what happened instead |
| answers the call with something that is neither | **fail** — the specification allows exactly one refusal, `ErrUnsupported` (or the specification's `ErrNotSettleable`) | **fail** |
| never reaches the feature's own call | **skip**, naming the call that was not reached | **fail** |

The last row is why each probe names the one call that matters. A refusal from
somewhere else in the probe — opening a consumer, publishing on the way past —
is evidence about *that* call, and reading it as the refusal made a driver that
could not open a consumer at all look like one whose every negative claim had
been refuted by observation.

## Reading the report

It also fails on a generated page no report produces, which is the gap
`-check` used to have: remove a driver's report and its capability page stayed
on disk, stating capabilities from evidence that was gone.

## Its own known weaknesses

Documented rather than glossed, because a kit's blind spots are the reader's
business:

* **A driver that accepts a header and loses it cannot be told from a broker
  that has no such header.** The three header claims are the one place where
  "the call was accepted and the value did not survive" is both a
  violation (a driver that cannot carry a header must refuse the publication)
  and the honest shape of a broker without native headers. The probe reports a
  divergence, so the negative direction stays unrefuted rather than failing a
  truthful denial — and an *enveloping* driver, whose headers do survive,
  therefore fails the negative direction because the header comes back.
  Recorded with an owner.
* **Nine of the features have no portable probe at all** — a probe would
  need something the kit cannot do portably, such as restarting a broker for
  durability or knowing a filter expression's syntax. Fifteen more are probed
  by some runs and not others. Those claims render in parentheses, and the
  matrix derives the list from the reports rather than counting by hand.
* **There is no provisioning hook**, so a fan-out driver must provision inside
  `Harness.Open` — SNS delivers only to subscriptions that exist when a message
  is published, and several scenarios publish before they open a consumer.
* **`harness.Image` returns the tag rather than the digest `images.lock`
  pins.** Four drivers have recorded it. The report therefore names a tag where
  it should name a digest.
