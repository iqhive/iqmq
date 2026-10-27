package stomp

import (
	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
)

// capabilities is this configuration table.
//
// It starts from the shared protocol's answer for this broker — which is the
// PROTOCOL's half, measured against the pinned images and held by family
// the specification's own tests — and then NARROWS it for what this driver's code does not
// implement. It never widens a row: a table that claimed more than the
// protocol supports would be a promise the wire cannot keep, and
// `TestDriverNarrowsAndNeverWidens` holds this function to it.
//
// Three rows are the deployment's rather than the protocol's and are the only
// ones this function decides on its own: scheduled publication (Classic's
// broker scheduler may be off), dead-lettering (which route a rejection
// takes), and the server-side filter (which this driver refuses to configure
// on Artemis).
func (c Config) capabilities() (mq.Capabilities, error) {
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		return mq.Capabilities{}, err
	}
	b := driverkit.NewCapabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		b.Support(f, c.narrow(f, p.Support(c.Broker, f)))
	}
	b.Limits(c.limits(p))
	if missing := b.Missing(); len(missing) > 0 {
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// narrow is the driver's own answer for one row, given the protocol's.
func (c Config) narrow(f mq.Feature, s mq.Support) mq.Support {
	switch f {
	case mq.FeatureNack:
		// The protocol's answer is Unsupported, because a NACK frame does
		// not return a message on either broker -- on Artemis it is an
		// acknowledgement and on Classic a poison-ack. This driver WIDENS
		// nothing: what it adds is a mechanism of its own, and the level it
		// claims is the emulated one the specification has for exactly this.
		//
		// The mechanism is the protocol's own recovery: an unacknowledged
		// `client-individual` delivery is redelivered when its subscription
		// goes away, measured on both brokers and marked `redelivered:
		// true`. So a requeue is an UNSUBSCRIBE and a SUBSCRIBE, and the
		// broker gives the message back from its own durable storage --
		// which is why the level is EmulatedDurable and not
		// EmulatedVolatile: nothing is held in this process.
		return mq.Support{
			Level: mq.EmulatedDurable,
			Scope: mq.ScopeGroup,
			Note: "a STOMP NACK frame returns nothing on either broker -- measured, Artemis treats it as an ACK " +
				"and Classic as a poison-ack -- so a requeue is a subscription RESET: the driver unsubscribes and " +
				"re-subscribes and the broker redelivers every unacknowledged delivery, marked `redelivered`. The " +
				"scope is the subscription and not the delivery: nacking one returns them all, and every other " +
				"delivery the caller was holding reports ErrLeaseLost when it is settled",
		}
	case mq.FeatureDeadLetter:
		if c.DeadLetterTopic != "" {
			return mq.Support{
				Level: mq.EmulatedDurable,
				Scope: mq.ScopeTopic,
				Note: "a dead-lettered delivery is PUBLISHED to " + c.DeadLetterTopic +
					" and the original is acknowledged only once that publication's receipt has arrived ",
			}
		}
		if c.Broker == activemq.Classic {
			return mq.Support{
				Level: mq.Conditional,
				Scope: mq.ScopeTopic,
				Note: "a NACK frame is a POISON_ACK on ActiveMQ Classic and reaches the broker's own dead-letter " +
					"strategy (ActiveMQ.DLQ by default), which the driver cannot read over STOMP; set " +
					"Config.DeadLetterTopic for a route this driver owns",
			}
		}
		return mq.Support{
			Level: mq.Unsupported,
			Note: "measured: a NACK frame on Artemis ACKNOWLEDGES the delivery rather than dead-lettering it, so " +
				"there is no frame that reaches a dead-letter address; set Config.DeadLetterTopic for a route this " +
				"driver publishes to",
		}
	case mq.FeaturePriority:
		// On Classic the ordering is the DEPLOYMENT's: the broker carries
		// the `priority` header and dispatches in FIFO order unless the
		// destination's policy sets `prioritizedMessages="true"`, which
		// the pinned image does not do for queues at all. Measured with it
		// off, a high-priority publication arrives after a low-priority
		// one, so the claim is withdrawn unless Config.Prioritised says
		// the policy is there.
		if c.Broker == activemq.Classic && !c.Prioritised {
			return mq.Support{
				Level: mq.Unsupported,
				Note: "ActiveMQ Classic carries the `priority` frame header and dispatches in FIFO order unless the " +
					"destination's policy sets prioritizedMessages=\"true\", which the pinned image does not do for " +
					"queues; measured with it off, a high-priority publication arrives after a low-priority one. Set " +
					"Config.Prioritised once the broker's policy has it",
			}
		}
		return s
	case mq.FeatureScheduledPublish:
		if c.Broker == activemq.Classic && !c.Scheduler {
			return mq.Support{
				Level: mq.Unsupported,
				Note: "ActiveMQ Classic's message scheduler is off unless the broker is configured with " +
					"schedulerSupport=\"true\", and measured, with it off the broker delivers a message carrying " +
					"AMQ_SCHEDULED_DELAY immediately and passes the header through to the consumer; set " +
					"Config.Scheduler once the broker has it",
			}
		}
		return s
	case mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders:
		// A STOMP frame header is text and cannot carry either, which is
		// what the protocol's table says -- and under the enveloping
		// modes this driver carries BOTH, in iqmq's own envelope inside the
		// body. The specification has a level for exactly that, and the level is DURABLE
		// rather than volatile because the envelope is in the message the
		// broker stores: nothing is held in this process.
		//
		// The claim moves with the configuration, which is the specification's own rule
		// that a table is a property of the CLIENT -- so a deployment that
		// chose native-strict mode gets the honest refusal and one that
		// chose the envelope gets the honest emulation.
		if c.Metadata == mq.MetadataPortableEnvelope || c.Metadata == mq.MetadataAdaptive {
			return mq.Support{
				Level: mq.EmulatedDurable,
				Scope: mq.ScopeTopic,
				Note: "a STOMP frame header is text, so this is iqmq's own envelope inside the body under the specification's " +
					c.Metadata.String() + " mode; the bytes on the wire are not what a non-iqmq " +
					"consumer of this destination would read",
			}
		}
		return s
	case mq.FeatureServerFilter:
		if c.Broker == activemq.Artemis {
			// The protocol's answer, unchanged. It is Unsupported on
			// Artemis -- measured: a `selector` header is IGNORED on a
			// fully qualified queue name, which is the only subscription
			// shape this driver uses there -- and `Config.Validate` refuses
			// `Config.Selector` on Artemis for that reason. Conditional
			// here was a claim no configuration could redeem: a caller
			// doing `RequireAtLeast(FeatureServerFilter, Conditional)`
			// passed and then could not configure the filter.
			return s
		}
		if c.Selector == "" {
			// The protocol has it on Classic; this client did not ask for
			// it. The evidence rule is that a claim which cannot be
			// exercised does not render, and a selector nobody configured
			// cannot be.
			return mq.Support{
				Level: mq.Conditional,
				Scope: mq.ScopeGroup,
				Note: "a JMS message selector is applied per subscription through Config.Selector, which this " +
					"client does not set. Measured: it filters on ActiveMQ Classic and is ignored on Artemis when " +
					"the subscription is a fully qualified queue name, so Config.Selector is refused there",
			}
		}
		return s
	case mq.FeatureAdmin:
		// STOMP has no topology operation at all: there is no frame that
		// creates a destination, deletes one, purges one or reports its
		// depth. Claiming Admin would make `Client.Admin()` available and
		// answer ErrUnsupported from it, which the specification forbids.
		return mq.Support{
			Level: mq.Unsupported,
			Note: "STOMP has no topology frame: on Classic a destination comes into existence when a subscription " +
				"attaches to it, and on Artemis `drivers/activemq`'s Admin (the broker's management address over " +
				"AMQP 1.0) or the broker's own console is what creates one",
		}
	case mq.FeatureFanOut:
		// A JMS queue delivers each message to one consumer, and that is the
		// point of the mode: claiming fan-out here would tell a caller that two
		// subscriptions each receive a message the broker gives to one.
		if c.Addressing == AddressQueue {
			return mq.Support{
				Level: mq.Unsupported,
				Note: "this client is configured with AddressQueue: a topic names one JMS queue and every consumer " +
					"of it competes, so a message reaches one consumer whatever subscription each one names",
			}
		}
		return s
	case mq.FeatureConsumerGroups:
		if c.Addressing == AddressQueue {
			return mq.Support{
				Level: mq.Native,
				Scope: mq.ScopeGroup,
				Note: "every consumer subscribes to the one JMS queue the topic names and they compete for its " +
					"messages, whatever subscription each one gives",
			}
		}
		return s
	case mq.FeatureReceipts:
		if !c.Receipts {
			return mq.Support{
				Level: mq.Unsupported,
				Note:  "this client is configured NoReceipts, so a publication is resolved when its frame is written and the broker says nothing back",
			}
		}
		return s
	case mq.FeaturePublisherConfirm, mq.FeatureDurableConfirm:
		// Both boundaries ARE the receipt, so they go with it. They stayed
		// advertised under NoReceipts while OpenSink refused every mode but
		// ConfirmNone -- the default, ConfirmAccepted, included -- so a
		// caller that checked the table before opening a sink was promised
		// the one thing this client is configured not to do.
		if !c.Receipts {
			return mq.Support{
				Level: mq.Unsupported,
				Note:  "this client is configured NoReceipts: a confirmation is the broker's RECEIPT frame, so a sink opens with ConfirmNone only",
			}
		}
		return s
	case mq.FeatureBatchPublish:
		// One SEND frame carries one message and the library has no batched
		// write, so a batch is a loop. The specification calls that an emulation and it
		// is a volatile one: the loop's failure leaves whatever it had
		// already written published, which is not atomic.
		return mq.Support{
			Level: mq.EmulatedVolatile,
			Scope: mq.ScopeTopic,
			Note:  "one STOMP SEND frame carries one message, so PublishBatch is a loop over Publish and a failure part-way through leaves what it had already sent published",
		}
	default:
		return s
	}
}

// limits is the limit table.
//
// ONE of the seven is advertised and six are left UNKNOWN, which is the specification's
// rule that an unknown limit is never an overloaded zero:
//
//   - MaxHeaderBytes is the longest header name or value either broker
//     carries, which is `internal/activemq`'s own name ceiling. It is
//     advertised because the mapping ENFORCES it, before any I/O.
//   - MaxMessageBytes is left unknown, and the shared protocol's
//     `MaxMessageBytes()` returns zero for the reason a driver cannot do
//     better: a STOMP frame's real bound is the acceptor's
//     `wireFormat.maxFrameSize`, which is a deployment's configuration and
//     which no frame reports. Advertising the pinned images' 100 MiB would
//     be this driver's assertion presented as the broker's answer.
//
// MaxDelay is not advertised because neither broker documents a ceiling on a
// scheduled delivery and neither refused one in testing; MaxLease is not
// advertised because STOMP grants no lease; the batch limits are not
// advertised because a batch here is a loop with no protocol-level bound.
func (c Config) limits(p protocol.Protocol) mq.Limits {
	return mq.Limits{
		MaxMessageBytes: mq.Limit[int64]{Value: p.MaxMessageBytes(), Known: p.MaxMessageBytes() > 0},
		MaxHeaderBytes:  mq.Limit[int64]{Value: activemq.MaxNameLen, Known: true},
	}
}

// info is what the specification asks a driver to report about itself.
func (c Config) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       c.profileName(),
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
	}
}
