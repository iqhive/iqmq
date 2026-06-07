package gcppubsub

import "strings"

// MaxIDLen is the longest topic or subscription identifier the service
// accepts.
const MaxIDLen = 255

// TopicID is the topic identifier a portable topic name maps to.
//
// The mapping is a pure function and it is deliberately simple: the prefix,
// then the name with "/" replaced by "~". "~" is in the service's identifier
// grammar and is not in [bitbucket.org/iqhive/iqmq.ValidateTopic]'s, so the
// mapping is INJECTIVE — "orders/eu" and "orders~eu" cannot both be written by
// a portable caller, so they cannot collide, which is a property the SQS
// driver's "/" → "-" mapping does not have and documents as a gotcha.
//
// A topic that is already a fully qualified resource name
// ("projects/p/topics/t") is passed through verbatim, which is reachable only
// through the SPI: mq.ValidateTopic refuses a name with a "/" and a scheme in
// it, so a portable caller uses [Config.TopicPrefix] instead.
func (c Config) TopicID(topic string) (string, error) {
	if topic == "" {
		return "", errUnknownTopic
	}
	if strings.HasPrefix(topic, "projects/") {
		return topic, nil
	}
	if wildcardish(topic) {
		// A name carrying a subscription-pattern metacharacter is refused as
		// UNSUPPORTED rather than as a bad character, and the distinction is
		// the one the specification asks for: a caller that wrote a pattern is asking for
		// a feature this broker does not have -- a Pub/Sub subscription names
		// exactly one topic -- and "the character is not allowed" would send
		// them looking for an escaping rule instead of for
		// FeatureServerFilter.
		//
		// None of these characters is in mq.ValidateTopic's alphabet, so a
		// portable caller cannot reach this by accident with a literal name.
		return "", errWildcard
	}
	id := c.TopicPrefix + strings.ReplaceAll(topic, "/", "~")
	if len(id) > MaxIDLen {
		return "", errTopicTooLong
	}
	if err := checkID("topic identifier", id); err != nil {
		return "", err
	}
	return id, nil
}

// SubscriptionID is the subscription identifier a portable (topic,
// subscription) pair maps to.
//
// It carries BOTH halves, and it has to: a Pub/Sub subscription names exactly
// one topic and has its own acknowledgement state, so two topics that shared
// one subscription identifier would be one resource attached to whichever
// topic created it first — and a consumer of the second topic would silently
// read the first topic's messages. The portable model is a subscription PER
// TOPIC, and this is where that becomes a resource name.
//
// The shape is `<prefix><topic>~<subscription>`. [Config.SubscriptionIDs] is
// the escape hatch for a deployment whose subscriptions are named by something
// else — Terraform, a naming policy, a subscription that already exists —
// keyed by "<topic>/<subscription>" so that the override is unambiguous.
func (c Config) SubscriptionID(topic, sub string) (string, error) {
	if sub == "" {
		return "", errNoSubscription
	}
	if id, ok := c.SubscriptionIDs[topic+"/"+sub]; ok {
		return id, nil
	}
	if strings.HasPrefix(sub, "projects/") {
		return sub, nil
	}
	topicID, err := c.TopicID(topic)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(topicID, "projects/") {
		// A fully qualified topic was passed through, so there is no short
		// identifier to build a subscription name out of.
		topicID = lastSegment(topicID)
	}
	id := c.SubscriptionPrefix + topicID + "~" + strings.ReplaceAll(sub, "/", "~")
	if len(id) > MaxIDLen {
		return "", errSubTooLong
	}
	if err := checkID("subscription identifier", id); err != nil {
		return "", err
	}
	return id, nil
}

// wildcardish reports whether a name carries a metacharacter that means
// "pattern" on some other broker: NATS's and MQTT's, AMQP's and Redis's.
//
// It is not an attempt to parse anybody's pattern grammar. It is the set of
// characters that only ever appear in a name because the caller meant a
// pattern, so that the refusal can say what is actually missing.
func wildcardish(name string) bool {
	for i := range len(name) {
		switch name[i] {
		case '*', '>', '#', '?':
			return true
		}
	}
	return false
}

// lastSegment is the identifier at the end of a qualified resource name.
func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}
