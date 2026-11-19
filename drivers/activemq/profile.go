package activemq

import (
	"errors"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/internal/activemq"
)

// Artemis's and Classic's per-message publication controls, as the brokers
// spell them. Every one of these was measured against the pinned image; the
// package documentation says what each measurement was.
const (
	// AnnotationDeliveryDelay is Artemis's relative scheduled-delivery
	// message annotation, in whole milliseconds.
	AnnotationDeliveryDelay = "x-opt-delivery-delay"
	// AnnotationDeliveryTime is Artemis's absolute scheduled-delivery
	// message annotation, as epoch milliseconds.
	AnnotationDeliveryTime = "x-opt-delivery-time"
	// PropertyScheduledDelay is Classic's relative scheduled-delivery
	// application property, in whole milliseconds. Measured: on Classic the
	// MESSAGE ANNOTATION form is ignored and this one is honoured, and on
	// Artemis it is the other way round.
	PropertyScheduledDelay = "AMQ_SCHEDULED_DELAY"
	// PropertyScheduledTime is Classic's absolute scheduled-delivery
	// application property, as epoch milliseconds.
	PropertyScheduledTime = "AMQ_SCHEDULED_TIME"
	// PropertyDuplicateID is Artemis's duplicate-detection application
	// property. Artemis discards a second message carrying a value it has
	// already seen, within its own `id-cache-size` window.
	PropertyDuplicateID = "_AMQ_DUPL_ID"
	// AnnotationPrefix is the prefix of Artemis's own message annotations,
	// which is what tells this driver that a caller's header belongs in the
	// annotation section rather than in application-properties.
	AnnotationPrefix = "x-opt-"
)

// Profile builds the shared core's profile for this configuration.
//
// It is a method on [Config] and not a package-level value, because three of
// its answers depend on the DEPLOYMENT rather than on the product: the
// addressing depends on [Config.Prefix], the scheduled-publication claim
// depends on [Config.Scheduler] (Classic's scheduler is off by default), and
// the dead-letter claim depends on [Config.DeadLetter]. A constant profile
// would have to claim the union of every deployment's facts, which is the
// capability table exists to prevent.
func (c Config) Profile() (core.Profile, error) {
	if err := c.Validate(); err != nil {
		return core.Profile{}, err
	}
	r := c.resolve()
	p := core.Profile{
		Driver:  DriverName,
		Name:    r.profileName(),
		Version: Version,
		SinkAddress: func(topic string) (string, error) {
			n, err := r.Names(topic, "")
			if err != nil {
				return "", err
			}
			return n.Publish, nil
		},
		SourceAddress: func(topic, subscription string) (string, error) {
			n, err := r.Names(topic, subscription)
			if err != nil {
				return "", err
			}
			if n.Consume == "" {
				return "", activemq.ErrNoSubscription
			}
			return n.Consume, nil
		},
		Capabilities: r.claims,
		// Limits is nil, so the core's own limits stand, and they are none.
		//
		// The core builds the table when the client opens, before any link,
		// and AMQP 1.0 carries a message-size ceiling only in a link's attach,
		// so it never fills Facts.MaxMessageBytes; a size ceiling here would
		// be this driver's assertion rather than the broker's answer. Neither
		// product documents a delay ceiling, and AMQP 1.0 grants no lease.
		//
		// MaxHeaderBytes is not advertised either. It was, as 255, which is
		// internal/activemq's name ceiling and not a header bound of either
		// product, and the core holds MaxHeaderBytes as the whole header set,
		// keys and values counted, so every message with more than 255 bytes
		// of headers was refused (TestAHeaderSetOver255BytesIsCarried).
		// Measured on Artemis 2.44.0 and Classic 6.2.0, both carried a
		// 70000-byte key, a 1 MiB value, 10000 application properties and
		// 5 MB of them in one non-durable message. A durable message meets the
		// broker's store, whose bounds are the deployment's journal on Artemis
		// and one key on Classic, and is refused by it (see errorRules), so
		// neither is a number this table could advertise for the set.
		Limits:           nil,
		Delay:            r.delay(),
		NackDelay:        core.NackDelayRule{},
		Attempt:          core.AttemptRule{Source: core.AttemptFromHeader},
		Dedup:            r.dedup(),
		Durable:          r.durable(),
		AnnotationPrefix: AnnotationPrefix,
		AutoCreates:      r.autoCreates(),
		CheckBroker:      r.checkBroker,
		ErrorRules:       errorRules,
		Admin:            r.admin(),
	}
	if r.DeadLetterTopic != "" {
		p.DeadLetterAddress = func(topic, subscription string) (string, error) {
			n, err := r.Names(topic, subscription)
			if err != nil {
				return "", err
			}
			return n.DeadLetter, nil
		}
	}
	if err := p.Validate(); err != nil {
		return core.Profile{}, err
	}
	return p, nil
}

// profileName is the profile the report and DriverInfo carry.
func (c Config) profileName() string {
	if c.Broker == activemq.Classic {
		return ProfileClassic
	}
	return ProfileArtemis
}

// The delay rules advertise no `Max`: neither product documents a ceiling on
// a scheduled delivery and neither refused one in testing, so MaxDelay
// stays unknown rather than carrying a number this driver made up.
//
// delay is the profile's scheduled-publication rule.
//
// The SLOT differs between the two products as well as the key, which is why
// this is a rule and not a constant. Measured against the pinned images: on
// Artemis a message annotation `x-opt-delivery-delay: 2000` delayed delivery
// by 2.0 s (with a plain string key and with an `amqp.Symbol` key alike) and
// `AMQ_SCHEDULED_DELAY` as an annotation delayed nothing; on Classic an
// application property `AMQ_SCHEDULED_DELAY: 2000` delayed delivery by 2.2 s
// and the annotation form delayed nothing.
//
// Both rules are the RELATIVE form, and the core restates a DeliverAt for
// them: it goes out as the delay left until the instant, measured on
// [Config.Clock], and one that has already passed goes out with no delay at
// all.
//
// A Classic deployment whose scheduler is off gets the ZERO rule, which the
// core reads as "this broker has no way to express that" and answers with
// ErrUnsupported before any I/O. That is the honest answer and it is not the
// obvious one: with `schedulerSupport` unset the broker accepts the property,
// delivers the message immediately and passes the property through to the
// consumer, so a driver that wrote the key anyway would report success for a
// delay that did not happen.
func (c Config) delay() core.DelayRule {
	if c.Broker == activemq.Classic {
		if !c.Scheduler {
			return core.DelayRule{}
		}
		return core.DelayRule{Slot: core.SlotApplicationProperty, Key: PropertyScheduledDelay}
	}
	return core.DelayRule{Slot: core.SlotMessageAnnotation, Key: AnnotationDeliveryDelay}
}

// dedup is the profile's duplicate-detection rule.
//
// Artemis has `_AMQ_DUPL_ID` and a cache of identifiers it has seen; Classic
// has nothing of the kind, so the rule is the zero value and both
// [mq.FeatureDeduplication] and [mq.FeatureIdempotentPublish] are refused
// there rather than emulated.
func (c Config) dedup() core.SlotRule {
	if c.Broker == activemq.Classic {
		return core.SlotRule{}
	}
	return core.SlotRule{Slot: core.SlotApplicationProperty, Key: PropertyDuplicateID}
}

// durable is the profile's answer to `ConfirmDurable`.
//
// AMQP 1.0 has no durable disposition, so the question is whether the peer's
// `accepted` implies the message is on disk. On both products it does, for a
// message published with the AMQP header's `durable` flag set and a
// destination whose address settings persist: Artemis writes the journal
// entry before it accepts, and Classic writes the store before it acknowledges
// a persistent send. So the answer is yes with the condition stated — and it
// is withdrawn for a client configured with [Config.NonDurable], which clears
// the flag the whole argument rests on.
func (c Config) durable() core.DurableRule {
	if c.NonDurable {
		return core.DurableRule{}
	}
	return core.DurableRule{
		Supported: true,
		Note: "the peer's `accepted` disposition for a message published with AMQP's `durable` flag set: " +
			"Artemis writes its journal entry before it accepts and Classic writes its store before it " +
			"acknowledges a persistent send, so the acceptance IS the durability. It needs a destination the " +
			"broker's address settings persist, and it is withdrawn for a client configured NonDurable",
	}
}

// autoCreates reports whether the broker creates a destination from an attach.
//
// It is a fact about the DEPLOYMENT on Artemis and about the PRODUCT on
// Classic, and the difference is worth stating because it is the difference
// between a claim this driver can prove and one it can only make.
//
// Artemis auto-creates by default and can be told not to
// (`artemis create --no-autocreate`, or `<auto-create-addresses>false`), and
// with auto-creation off an attach to a destination that does not exist is
// refused with `amqp:not-found` — which the core's table turns into
// CodeNotFound and which is what proves the driver creates nothing.
// [Config.BrokerAutoCreates] is how a deployment says its broker still does.
//
// Classic creates a destination on attach and cannot be told not to without an
// authorization plugin, so this reports true there and the "creates no
// topology" property is the driver's own code rather than an observable
// refusal.
func (c Config) autoCreates() bool {
	return c.Broker == activemq.Classic || c.BrokerAutoCreates
}

// checkBroker refuses a broker this configuration was not written for.
//
// The PRODUCT is checked before the version and the reason is the hazard the
// package documentation opens with: the two products' topic mappings name
// nothing in common, and Artemis does not fail an attach to a name it does not
// have — it creates it. A client configured for Classic and pointed at Artemis
// would therefore create an address literally called
// `topic://VirtualTopic.orders`, publish into it for ever, and deliver nothing.
// This is `drivers/rabbitmq/amqp10`'s own reasoning one broker along, and that
// module's gate is what refuses THIS driver pointed at a RabbitMQ.
//
// A broker that identified itself as nothing is accepted, which is the
// opposite of the RabbitMQ profile's rule and is measured: Classic sends no
// `product` and no `version` in its AMQP 1.0 open performative at all, so
// refusing silence would refuse one of the four combinations the specification names.
func (c Config) checkBroker(f core.Facts) error {
	if err := activemq.CheckProduct(c.Broker, f.Product); err != nil {
		return err
	}
	got, _ := activemq.ParseVersion(f.Version)
	if c.Broker == activemq.Classic {
		return activemq.CheckAtLeast(got, activemq.MinClassicAMQP, "ActiveMQ Classic's AMQP 1.0 transport")
	}
	return activemq.CheckAtLeast(got, activemq.MinArtemisFQQN,
		"Artemis's fully qualified queue name addressing, which this driver's shared durable subscription needs")
}

// errorRules are the rules this driver contributes to the core's table.
//
// They are tried BEFORE the core's for the same operation, so each of them is
// a case where an ActiveMQ answer means something more specific than the
// condition alone says.
func errorRules() map[string][]driverkit.Rule {
	// A `released` disposition on a PUBLICATION means the broker took the
	// transfer and routed it nowhere. On Artemis that is a multicast address
	// with no queues bound to it, and on Classic a virtual topic nobody has
	// a consumer queue for — which is the specification's "the topology is not there"
	// rather than a retryable failure, and reporting it as Unavailable would
	// have a publisher retry for ever into a destination that will never
	// take the message.
	//
	// The rule is registered for OpPublish only: a `released` on the CONSUME
	// side is this driver's own requeue and never reaches the mapper as a
	// failure.
	publish := []driverkit.Rule{
		{Match: isReleased, Code: mq.CodeNotFound},
		// A transfer the broker refused for its size. Neither bound is one a
		// client can know in advance, which is why the profile advertises no
		// limit and leaves the refusal to the broker. All three arrive as
		// `rejected{failed}`, measured on Artemis 2.44.0 and Classic 6.2.0,
		// which the core reads as Unknown. The same message meets the same
		// bound, so none is retryable.
		//
		// Artemis refuses a durable message whose encoded record is over its
		// journal's buffer, `journal-buffer-size`, 490 KiB unless configured:
		// "AMQ149005: Message of 1048818 bytes is bigger than the max record
		// size of 501760 bytes". Classic writes a persistent message's strings
		// to its store as Java modified UTF-8, which holds 65535 octets, and
		// refuses a longer one in two places. An application-property key, an
		// `x-opt-` key stored as `JMS_AMQP_MA_<key>` among them, is refused as
		// "encoded string (kkkkkkkk...kkkkkkkk) too long: 65536 bytes" and the
		// message goes nowhere. These two are definite.
		{Match: rejectedSaying("AMQ149005"), Code: mq.CodeMessageTooLarge},
		{Match: rejectedSaying("encoded string (", ") too long: "), Code: mq.CodeMessageTooLarge},
		// The message key's `group-id`, the correlation ID, the message ID
		// and the subject are refused as "java.io.UTFDataFormatException:
		// encoded string too long: 65536 bytes", and that refusal comes from
		// the store write Classic makes while it dispatches: measured, the
		// rejected message was delivered every time, to a JMS queue's consumer,
		// and to one of a virtual topic's two subscriptions. So it is
		// ambiguous: the broker said no and a consumer has it anyway.
		{Match: rejectedSaying("encoded string too long: "), Code: mq.CodeMessageTooLarge, Ambiguous: true},
	}
	// Artemis names its refusals, and two of them are worth reading before
	// the condition. AMQ119010 and AMQ119002 are "source address does not
	// exist" and "target address does not exist" and arrive as
	// `amqp:not-found`, which the core already maps to CodeNotFound — so
	// they need no rule. AMQ229019 is "Queue X already exists on address Y",
	// which arrives from the MANAGEMENT path as a failed operation rather
	// than as a link condition, and which Admin has to tell from a genuine
	// conflict because an Artemis queue name is unique across the whole
	// broker. AMQ229204 is its twin for an address, "Address already
	// exists". It was mapped to CodeNotFound, which told a caller the address
	// the broker had refused to create twice was missing.
	//
	// AMQ229067 is "Cannot find resource with name queue.X": the queue a
	// Purge or a Stats names is not there, so the subscription is missing. It
	// reached the caller as CodeUnknown.
	admin := []driverkit.Rule{
		{Match: driverkit.MatchContains("AMQ229019"), Code: mq.CodeConflict},
		{Match: driverkit.MatchContains("AMQ229204"), Code: mq.CodeConflict},
		{Match: driverkit.MatchContains("AMQ229067"), Code: mq.CodeNotFound},
	}
	return map[string][]driverkit.Rule{
		mq.OpPublish: publish,
		mq.OpAdmin:   admin,
	}
}

// isReleased reports whether err carries a `released` outcome.
func isReleased(err error) bool {
	var oerr *core.OutcomeError
	if !errors.As(err, &oerr) || oerr == nil {
		return false
	}
	return oerr.Outcome == core.OutcomeReleased
}

// rejectedSaying matches a `rejected` outcome whose error says every one of
// parts.
func rejectedSaying(parts ...string) func(error) bool {
	return func(err error) bool {
		oerr, ok := errors.AsType[*core.OutcomeError](err)
		if !ok || oerr == nil || oerr.Outcome != core.OutcomeRejected {
			return false
		}
		msg := err.Error()
		for _, p := range parts {
			if !strings.Contains(msg, p) {
				return false
			}
		}
		return true
	}
}

// GroupIDProperty is where the specification's `JMSXGroupID` travels over AMQP 1.0: the
// message's own `group-id` property, which both products read as the JMS
// message group.
//
// It is a constant rather than a mapping rule because the core already carries
// it — `mq.Message.Key` reaches `group-id` through the core's mapper when the
// capability table claims ordering by key — and this is the name the driver
// page and the tests refer to.
const GroupIDProperty = "group-id"
