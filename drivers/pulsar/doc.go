// Package pulsar is the Apache Pulsar driver.
//
// # Topics and subscriptions
//
// A portable topic is the LOCAL name of a Pulsar topic:
// `persistent://<tenant>/<namespace>/<prefix><topic>`, with the domain, tenant,
// namespace and prefix coming from [Config]. A topic that already carries a
// scheme is taken as fully qualified and validated as one, which is how a
// caller reaches another tenant or a non-persistent topic; a topic that
// contains a slash but no scheme is REFUSED rather than read as
// `tenant/namespace/topic`, so a name cannot silently escape the namespace the
// deployment configured. A portable subscription is a Pulsar subscription: a
// named, durable cursor every consumer of that name shares.
//
// A topic carrying a regular-expression metacharacter is a PATTERN and opens a
// regex subscription. `.` is not such a metacharacter here, because it is also
// an ordinary character in a Pulsar topic name.
//
// # Confirmation boundary
//
//   - ConfirmNone: the client has admitted the publication (SendAsync
//     returned). The outcome still reaches the driver's health state and
//     counters, because a publication whose failure nobody can see is a lost
//     message with no evidence.
//   - ConfirmAccepted and ConfirmDurable: Producer.Send has returned the
//     broker's send receipt. On a persistent topic that receipt is given after
//     the entry has been written to the configured write quorum of bookies, so
//     the two modes are the SAME round trip here and both are durable. On a
//     non-persistent topic there is no durable boundary at all and
//     ConfirmDurable fails at construction.
//
// # Settlement boundary
//
// With [Config.AckWithResponse] off — the default — an acknowledgement is
// handed to the client's acknowledgement-grouping tracker and sent within
// AckGroupingMaxTime; Ack returns once the client has taken it. With it on,
// every acknowledgement is a round trip the broker answers and Ack returns when
// the answer arrives.
//
// # The at-least-once window
//
// Four windows, and each of them is a duplicate rather than a loss:
//
//   - between a delivery and its acknowledgement. A consumer that disconnects
//     makes every unacknowledged message of its subscription available again at
//     once, which is also how this driver's Close gives work back.
//   - between an acknowledgement being grouped and being sent, when
//     AckWithResponse is off. A process that dies inside AckGroupingMaxTime
//     loses the acknowledgement and not the message.
//   - a delayed negative settlement held in this process, when
//     [Config.RetryEnable] is off. The hold is lost with the process and the
//     message comes back at the next disconnection rather than at the time
//     asked for; the capability table calls it EmulatedVolatile.
//   - the emulated lease. [Config.AckTimeout] is the DRIVER's deadline, not the
//     broker's: pulsar-client-go v0.21 has no client-side acknowledgement
//     timeout and Pulsar's broker redelivers on disconnection alone, so the
//     driver tracks each delivery's deadline, hands the message back one
//     LeaseGuard before it, and refuses a settlement that arrives after that.
//     [Config.DisableAckTimeout] turns it off and gives Pulsar's own behaviour.
//
// # Ordering
//
// [Config.SubscriptionType] decides every ordering claim, and the four types
// give four different answers. The capability table states them and the driver
// page quotes them; in short:
//
//   - Shared: no ordering of any kind. Messages go to whichever consumer has
//     permits, and a negative settlement puts one back behind the messages that
//     followed it.
//   - Key_Shared with AllowOutOfOrderDelivery false: per-KEY order, conditional
//     on a stable consumer set and a stable partition count.
//   - Exclusive and Failover: per-PARTITION order natively, per-key order
//     conditional on the partition count, and global order conditional on the
//     topic being non-partitioned.
//
// Adding partitions to a topic rehashes every key onto a different partition,
// after which a key's older and newer messages are in two partitions with no
// order between them. That is the broker's, not this driver's, and
// TestCriticalPartitionExpansion is what holds the claim to it.
//
// # Batched entries and
//
// Pulsar's producer batches by default, so one broker ENTRY usually holds
// several messages, and an acknowledgement names an entry. pulsar-client-go
// keeps a per-entry bitmap inside the identifier it hands out and only
// acknowledges the entry once every index is clear — unless the identifier has
// lost that bookkeeping, in which case the acknowledgement settles the entry
// and every sibling in it, or unless an acknowledgement SET is sent to a broker
// whose `acknowledgmentAtBatchIndexLevelEnabled` is false, which has the same
// effect. Both are the specification's "can lose an accepted message outside a documented
// failure boundary". This driver keeps the identifier the consumer gave it for
// the delivery's whole life, refuses a settlement it cannot vouch for, and
// refuses [Config.EnableBatchIndexAck] at open against a broker that would
// ignore it. See [AckRules].
//
// # Maturity
//
// Experimental, tier 2. The conformance evidence in
// conformance/reports/latest is two profiles — `shared` and `keyshared` — of
// one run against the pinned apachepulsar/pulsar image.
package pulsar
