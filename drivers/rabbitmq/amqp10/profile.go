package amqp10

import (
	"errors"
	"strconv"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
)

// Profile is the [bitbucket.org/iqhive/iqmq/drivers/amqp10.Profile] this module
// exists to supply: everything AMQP 1.0 leaves open, answered for RabbitMQ 4.
//
// It is a FUNCTION of the configuration and not a package-level value, because
// four of its answers depend on this deployment rather than on the broker: the
// addressing depends on [Config.Prefix], and the capability claims depend on
// the queue type, the dead-letter route and the priority ceiling. A constant
// profile would have to claim the union of every deployment's facts, which is
// the capability table exists to prevent.
//
// # What the profile decides
//
// Reading it top to bottom is the shortest description of what this driver is:
//
//   - **Addressing** is RabbitMQ 4's own path form,
//     `/exchanges/<exchange>/<routing-key>` for a publication and
//     `/queues/<queue>` for a consumer, built from `internal/rabbit`'s names
//     by `internal/rabbit/protocol`. Both are shared with the AMQP 0-9-1
//     driver, so a caller who moves between the profiles finds the same
//     exchange, the same quorum queue and the same dead-letter route.
//   - **The version gate** refuses anything below RabbitMQ 4.0 and never falls
//     back to AMQP 0-9-1 (the specification's "never silently substituted"). It also
//     refuses a peer that names a product that is not RabbitMQ, because this
//     profile's addressing means something else on every other AMQP 1.0
//     broker.
//   - **The absences** are the whole of the specification's "feature subset"
//     criterion, and every one of them is a zero value here rather than a
//     guess: no delay rule, no nack-delay rule, no de-duplication slot, no
//     annotation prefix, no topology surface and no auto-creation.
//   - **The one error rule** RabbitMQ needs: a `released` disposition for a
//     publication means the exchange routed the message NOWHERE, which is a
//     definite CodeNotFound and not the core's generic retryable Unavailable.
//
// # Why there is no annotation prefix
//
// RabbitMQ's own AMQP 1.0 controls live under `x-` message annotations, so the
// obvious value for [bitbucket.org/iqhive/iqmq/drivers/amqp10.Profile.AnnotationPrefix]
// is `x-`. It is deliberately empty, and the reason is interoperability
// between the two RabbitMQ profiles: RabbitMQ converts between AMQP 1.0 and
// AMQP 0-9-1 by mapping APPLICATION PROPERTIES to and from the 0-9-1 field
// table, so a header this driver put in the annotation section would be in a
// different place on the other protocol — and a message published through one
// RabbitMQ driver would read back without that header through the other. With
// no prefix every portable header is an application property and the round
// trip holds. `internal/rabbit/protocol`'s own mapping records the same
// decision for the same reason.
func (c Config) Profile() (core.Profile, error) {
	r := c.resolve()
	proto, err := protocol.New(protocol.AMQP10)
	if err != nil {
		return core.Profile{}, err
	}
	p := core.Profile{
		Driver:  DriverName,
		Name:    ProfileName,
		Version: Version,
		SinkAddress: func(topic string) (string, error) {
			return addressOf(proto, r, topic, "", protocol.Publish)
		},
		SourceAddress: func(topic, subscription string) (string, error) {
			return addressOf(proto, r, topic, subscription, protocol.Consume)
		},
		// The dead-letter route is the QUEUE's own `x-dead-letter-exchange`
		// policy and not something this driver publishes to, which is why
		// this is nil: a `rejected` disposition reaches `<queue>.dlx` because
		// whoever declared the queue said so. Supplying a route here would
		// make the core re-publish the message itself and then reject the
		// original — one more copy, one more window, and a second dead-letter
		// mechanism for a broker that already has one.
		DeadLetterAddress: nil,
		Capabilities:      r.claims,
		// Limits adds the one ceiling this profile knows and the core cannot:
		// the 255 bytes an AMQP 0-9-1 field-table key has.
		//
		// It is a CROSS-PROFILE limit and that is why it is advertised, which
		// is a reversal of the first version's decision. An AMQP 1.0
		// application-property key is a string with no such bound, so on the
		// 1.0 endpoint alone the number is conservative — and no RabbitMQ
		// deployment has only the 1.0 endpoint: both are always on, the same
		// queue serves both, and a key longer than 255 bytes published here
		// cannot be represented to a 0-9-1 consumer of the same queue. The specification
		// makes an advertised limit something a caller may RELY on, and what
		// a caller relies on is "stay under this and both profiles can read
		// it", which is true. Advertising nothing at all is what the specification's
		// `limits/advertised` refuses against container evidence, and rightly:
		// a table with no limit in it is indistinguishable from a lookup that
		// silently failed.
		//
		// Everything else is left as the core found it. The message ceiling is
		// the peer's own declared `max-message-size`, which the core reads off
		// the first attach; there is no delay, so no MaxDelay; and AMQP 1.0
		// grants no lease, so no MaxLease.
		Limits: func(_ core.Facts, base mq.Limits) mq.Limits {
			out := base
			out.MaxHeaderBytes = mq.Limit[int64]{Value: rabbit.MaxNameLen, Known: true}
			return out
		},
		// Delay, NackDelay and Dedup are the zero value, which the core reads
		// as "this broker has no way to express that" and answers with
		// ErrUnsupported before any I/O. See capabilities.go for what each
		// absence costs and why none of them may be guessed at.
		Durable:          r.durable(),
		AnnotationPrefix: "",
		// RabbitMQ 4's AMQP 1.0 endpoint creates nothing from an address: an
		// attach to a queue that does not exist closes the link with
		// `amqp:not-found`, and a target address naming an exchange that does
		// not exist does the same. So the specification's "no hidden creation" is a
		// property of the broker here rather than a choice this driver makes,
		// and the core's error table turns the refusal into CodeNotFound —
		// which is what the conformance kit's `topology/no-hidden-creation`
		// scenario looks for.
		AutoCreates: false,
		CheckBroker: checkBroker,
		ErrorRules:  errorRules,
		// Admin is nil, so Client.Admin answers ErrUnsupported and
		// FeatureAdmin is not claimed. RabbitMQ 4 does have an AMQP 1.0
		// management extension over a `/management` node; implementing it is
		// a separate piece of work and claiming it without one would publish
		// a surface that answers ErrUnsupported.
		Admin: nil,
	}
	if err := p.Validate(); err != nil {
		return core.Profile{}, err
	}
	return p, nil
}

// addressOf derives one AMQP 1.0 address, in one place for both roles.
//
// The two halves are deliberately somebody else's: `internal/rabbit` owns the
// NAMES, which both RabbitMQ drivers must derive identically, and
// `internal/rabbit/protocol` owns the ADDRESS FORM, including the
// percent-encoding that keeps a routing key with a "/" in it from addressing a
// different node. This function is the join, and it is the only place in this
// module that spells either.
func addressOf(proto protocol.Protocol, cfg Config, topic, subscription string, role protocol.Role) (string, error) {
	if role == protocol.Consume && strings.ContainsAny(topic, "*#") {
		// A wildcard SUBSCRIPTION is refused with ErrUnsupported and not with
		// the invalid-name error `internal/rabbit` would give it, and the
		// difference is what makes the capability claim provable.
		//
		// This profile does not claim FeatureWildcardSubscribe: a wildcard is
		// a binding pattern on a topic exchange, and a receiving link
		// addresses a queue rather than a pattern. The negative probe for
		// an unclaimed feature runs the feature and requires ErrUnsupported,
		// so a refusal carrying CodeInvalidArgument would leave the claim
		// unverified in both directions — which is the shape drivers/sqs
		// recorded for the same feature.
		return "", &WildcardError{Pattern: topic}
	}
	names, err := rabbit.Derive(rabbit.Config{
		Prefix:       cfg.Prefix,
		Topic:        topic,
		Subscription: subscription,
	})
	if err != nil {
		return "", err
	}
	addr, err := proto.Address(names, role)
	if err != nil {
		return "", err
	}
	return addr.Node, nil
}

// checkBroker refuses a broker this profile does not support, from what the
// peer said in its OPEN frame.
//
// It is this module's headline behaviour, and both halves of it matter.
//
// **The version.** RabbitMQ 4.0 is the first release that speaks AMQP 1.0
// natively; on 3.13 it is a plugin whose feature set, address syntax and
// settlement behaviour differ. The specification says this profile is "never silently
// substituted", so an older broker is refused with
// [bitbucket.org/iqhive/iqmq.ErrUnsupported] naming both versions, and the
// caller reaches for `drivers/rabbitmq` deliberately rather than being moved
// there. A peer that did not say what version it is satisfies no gate, which
// is what keeps the gate from being open by omission (`rabbit.CheckAtLeast`).
//
// **The product.** A broker that is not RabbitMQ is refused BEFORE the
// version, because the version gate alone would pass every other AMQP 1.0
// broker in existence: Apache Artemis reports version 2.44, which is happily
// "at least 4.0" of nothing. That matters more than it sounds, because
// Artemis AUTO-CREATES an address from an attach — so a client pointed at the
// wrong broker would not fail, it would create an address literally called
// `/exchanges/orders/orders` and publish into it for ever. Refusing a named
// product that is not RabbitMQ's is the same "never silently substituted" rule
// read in the other direction.
//
// A peer that names NO product is not refused on that ground: an AMQP 1.0
// proxy in front of RabbitMQ need not forward the connection properties, and a
// refusal would then depend on a hop rather than on the broker. Such a peer
// still has to satisfy the version gate, which it cannot do silently — a peer
// that sent no version at all is refused by it.
func checkBroker(f core.Facts) error {
	if f.Product != "" && !isRabbitProduct(f.Product) {
		return &ProductError{Product: f.Product}
	}
	got, _ := rabbit.ParseVersion(f.Version)
	return rabbit.CheckAtLeast(got, rabbit.MinBrokerAMQP10, "the AMQP 1.0 profile for RabbitMQ")
}

// isRabbitProduct reports whether a peer's "product" connection property names
// RabbitMQ.
//
// The comparison is a whole-string, ASCII-case-insensitive match against the
// one name the broker sends ("RabbitMQ"), and not a substring search: a
// substring test would accept a product called "not-rabbitmq" and, more to the
// point, a proxy that names itself after the broker it fronts. ASCII folding
// rather than [strings.EqualFold] because what is being compared is a protocol
// constant in ASCII and the other side came off the wire — Unicode case
// folding is a different question than the one being asked.
func isRabbitProduct(product string) bool { return equalASCIIFold(product, "rabbitmq") }

// equalASCIIFold reports whether s equals lower, ignoring ASCII case. lower
// must already be lower-case.
func equalASCIIFold(s, lower string) bool {
	if len(s) != len(lower) {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// ProductError is a peer that is not RabbitMQ.
//
// It is a type rather than a sentinel so that the refusal can name what the
// peer said it was, which is the one piece of information that makes the
// mistake obvious: a caller who pointed this module at an Artemis endpoint
// sees "apache-activemq-artemis" in the message.
type ProductError struct {
	// Product is the peer's own "product" connection property, as it sent
	// it.
	Product string
}

// Error renders the refusal. The product comes off the wire, so it is rendered
// through the core's own quoting rather than interpolated raw: a peer that sent
// bytes that are not valid UTF-8 would otherwise produce an error string no log
// pipeline could carry.
func (e *ProductError) Error() string {
	return "iqmq/rabbitmq: this is the AMQP 1.0 profile for RabbitMQ 4 and the peer says it is " +
		quoteProduct(e.Product) + "; the driver documentation forbids substituting one broker's profile for another's"
}

// Unwrap makes the refusal ErrUnsupported: the broker is real and reachable
// and is not the one this profile addresses, which is Unsupported
// rather than a configuration error.
func (e *ProductError) Unwrap() error { return mq.ErrUnsupported }

// quoteProduct renders a peer-supplied product name printably.
func quoteProduct(s string) string {
	if s == "" {
		return `""`
	}
	for i := range len(s) {
		if c := s[i]; c <= ' ' || c >= 0x7f {
			return strconv.Quote(s)
		}
	}
	return s
}

// WildcardError is a subscription pattern on a profile that has no way to
// subscribe to one.
//
// It is a type rather than a sentinel so that the refusal can name the pattern
// the caller asked for: a caller who moved a wildcard subscription across from
// `drivers/rabbitmq` sees the pattern that no longer works, which is the one
// piece of information that makes the change obvious.
type WildcardError struct {
	// Pattern is the topic the caller named.
	Pattern string
}

// Error renders the refusal without interpolating the pattern raw: a topic
// reaches this from a caller and may hold any byte at all.
func (e *WildcardError) Error() string {
	return "iqmq/rabbitmq: the AMQP 1.0 profile has no wildcard subscription: a wildcard is a binding pattern on a " +
		"topic exchange and a receiving link addresses a queue (`/queues/<queue>`), so the pattern " +
		quoteProduct(e.Pattern) + " cannot be an address; bind a queue with the pattern and subscribe to the queue, " +
		"or use drivers/rabbitmq"
}

// Unwrap makes the refusal ErrUnsupported, which is what the negative probe
// for an unclaimed feature reads.
func (e *WildcardError) Unwrap() error { return mq.ErrUnsupported }

// errorRules are the rules RabbitMQ adds to the core's table.
//
// There is exactly one, and it is the publication outcome the specification names. The
// core's generic table reads a `released` disposition as the peer declining to
// act on the transfer — CodeUnavailable and retryable, because another peer
// might take it. On RabbitMQ that reading is wrong in the one way that costs a
// caller the most: an exchange that routes a message NOWHERE releases it, so
// the message is gone, the topology is what is wrong, and retrying throws the
// message away again.
//
// AMQP 0-9-1 expresses the same thing as `mandatory` plus `basic.return`, which
// The specification maps to Rejected — CodeNotFound, not retryable. This rule is what makes
// the two profiles agree about it, and it is why the rule belongs to the
// profile: the core cannot know that this broker's `released` means unroutable
// rather than "try another node".
//
// The rule is registered for OpPublish only. A `released` disposition on the
// CONSUME side is the driver's own requeue and never reaches the mapper as a
// failure, and a released transfer on a dead-letter publication goes through
// OpNack, where the core's retryable reading is the right one: the copy was
// not stored, nothing was settled, and the delivery is still the broker's.
func errorRules() map[string][]driverkit.Rule {
	unroutable := []driverkit.Rule{
		{Match: isReleased, Code: mq.CodeNotFound},
	}
	return map[string][]driverkit.Rule{
		mq.OpPublish: unroutable,
	}
}

// isReleased reports whether a publication failed because the peer answered it
// with the `released` outcome.
//
// It matches the core's own [bitbucket.org/iqhive/iqmq/drivers/amqp10.OutcomeError]
// by type and by outcome name rather than through errors.Is against a
// sentinel, for the reason the core's own predicate does: a sentinel built with
// `internal/errcode` matches every error carrying the same code, so
// errors.Is against a CodeUnavailable sentinel would fire for any transport
// failure at all.
func isReleased(err error) bool {
	var oerr *core.OutcomeError
	if !errors.As(err, &oerr) || oerr == nil {
		return false
	}
	return oerr.Outcome == core.OutcomeReleased
}
