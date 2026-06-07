# `drivers/gcppubsub`

The iqmq driver for Google Cloud Pub/Sub, on `cloud.google.com/go/pubsub/v2`.

```go
client, err := gcppubsub.Open(ctx, gcppubsub.Config{
    ProjectID: "my-project",
    Ordering:  true,
})
```

`docs/drivers/gcppubsub.md` is the driver's specification: the fixed 
sections, the full table of what maps to what, the service limits read out of
the pinned client, and the capability matrix.

What is worth knowing before reading the code:

- **`Subscriber.Receive` delivers by callback** and the SPI asks to be called.
  `driverkit.PushAdapter` is the join, and the callback BLOCKING is the
  backpressure: the library releases a flow-control slot when the callback
  returns.
- **Ordering keys are conditional on `Config.Ordering`**, because the library's
  `EnableMessageOrdering` is a construction flag. A failed ordered publication
  pauses its key for the life of the publisher, so the driver resumes it — a
  publisher that is not resumed has silently stopped carrying that key.
- **A delayed nack is an in-process hold**, because Pub/Sub has none. It is
  advertised as `EmulatedVolatile` and does not survive a process death.
- **`Extend` sends nothing.** The library extends automatically; the
  acknowledgement identifier a `ModifyAckDeadline` needs is private to its
  message type.
- **`Close` settles before it waits**, because the library's shutdown waits for
  those settlements. The other order deadlocks.
- **Exactly-once delivery is not claimed.** Pub/Sub's setting of that name is a
  guarantee about a settlement; the feature is about delivery multiplicity.

`drivers/gcppubsub/v1compat` is the same driver over the deprecated v1 client
library: the second arm, and a migration path for a deployment that
already holds a v1 `*pubsub.Client`.
