package snssqs

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The name grammars, read out of the pinned SDKs' generated documentation and
// recorded here rather than in the portable API, which is the specification's "service
// limits discovered/documented from the tested API version".
const (
	// MaxTopicNameLen is the longest SNS topic name the service accepts.
	// Note that it is more than THREE TIMES the eighty characters a queue
	// name allows, which is the whole reason [QueueName] can hash: a legal
	// topic and a legal subscription compose into a name the queue side
	// cannot hold.
	MaxTopicNameLen = 256
	// MaxQueueNameLen is the longest SQS queue name the service accepts. It
	// is the SQS driver's constant, restated here because it is the budget
	// the composed name is measured against.
	MaxQueueNameLen = sqs.MaxQueueNameLen
	// hashLen is how many hex characters of SHA-256 a composed name that
	// does not fit ends in. Sixty-four bits is enough that two queue names
	// in one account will not collide, and short enough to leave sixty-three
	// readable characters.
	hashLen = 16
)

// The escape the default mapping writes names with.
//
// A topic and a subscription are made of alphanumerics, hyphens, underscores
// and slashes, and a name takes the first three only, so something has to give.
// What gives is the underscore: it begins every two-character escape, and
// everything but the slash and the underscore stands for itself. That is what
// makes the mapping INJECTIVE. Read left to right, an underscore and the
// character after it are always one escape, so a name decodes one way only,
// and the alphanumerics and hyphens most names are made of come through
// unchanged.
//
// The join is an escape too, and it has to be: an escape that no topic can
// produce is the only separator that cannot also sit inside one of the halves
// it separates. No escaped half holds "__" at all, because no escape ends in
// an underscore.
const (
	// escapedSlash is a "/" in a topic or a subscription.
	escapedSlash = "_-"
	// escapedUnderscore is an underscore of the caller's own.
	escapedUnderscore = "_u"
	// queueNameJoin separates the topic from the subscription in a queue
	// name.
	queueNameJoin = "__"
)

// escapeNamePart writes one half of an identity, a portable topic or a
// subscription, with the escapes above.
//
// A character outside the four is passed through as it stands rather than
// refused here, so that the refusal is still the grammar's: CheckTopicName
// reports a ".fifo" topic as a FIFO topic and a dot anywhere else as a bad
// character, which an escaper that refused first would turn into one message
// for both.
func escapeNamePart(s string) string {
	var b strings.Builder
	b.Grow(len(s) + strings.Count(s, "/") + strings.Count(s, "_"))
	for i := range len(s) {
		switch s[i] {
		case '/':
			b.WriteString(escapedSlash)
		case '_':
			b.WriteString(escapedUnderscore)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// TopicName is the default portable-topic-to-SNS-topic mapping (the specification's
// "topic→queue name/URL (resolver option)", one level up).
//
// A portable topic is a slash-separated path and an SNS topic name is not, so
// the topic is written with the mapping's escapes: "/" becomes "_-" and an
// underscore becomes "_u", and alphanumerics and hyphens stand for themselves.
// "orders/eu", "orders-eu" and "orders_eu" are therefore three topics,
// "orders_-eu", "orders-eu" and "orders_ueu", and the mapping is injective for
// a given prefix. Two portable topics that shared an SNS topic would each
// receive the other's publications in every one of their subscriptions.
//
// The prefix is prepended as it stands. It is a namespace, so two deployments
// sharing an account choose prefixes neither of which begins the other.
// [Config.Resolver] and [Config.TopicARNs] remain for a deployment whose topics
// are named by something else.
func TopicName(topic, prefix string) (string, error) {
	if sqs.HasWildcard(topic) {
		// SNS has no pattern subscription: a subscription names ONE topic
		// ARN. So a topic carrying a wildcard metacharacter is refused with
		// ErrUnsupported rather than as a name with a bad character in
		// it, which is what makes the negative direction for
		// FeatureWildcardSubscribe provable — the conformance kit hands the
		// driver a pattern and reads the refusal.
		return "", errNoWildcard
	}
	if err := mq.ValidateTopic(topic); err != nil {
		return "", err
	}
	name := prefix + escapeNamePart(topic)
	if err := CheckTopicName(name); err != nil {
		return "", err
	}
	return name, nil
}

// CheckTopicName reports whether name is an SNS topic name the service
// accepts. It is pure and runs before any I/O.
func CheckTopicName(name string) error {
	switch {
	case name == "":
		return errNoTopicName
	case len(name) > MaxTopicNameLen:
		return errTopicNameLong
	case strings.HasSuffix(name, ".fifo"):
		// A ".fifo" topic is an SNS FIFO topic, whose ordering,
		// deduplication and subscription rules are a different capability
		// table and whose subscribers must be FIFO queues. This driver
		// implements the standard topic, so the name is refused rather than
		// accepted into a profile that does not exist.
		return errFIFOTopic
	case !validNamePart(name):
		return errTopicNameChars
	}
	return nil
}

// QueueName is the default (topic, subscription)-to-queue-name mapping, which
// is where the fan-out lives: one SNS topic, one queue per subscription, and a
// consumer of a subscription consumes that subscription's own queue.
//
// The composition is `<queuePrefix><topic name>__<subscription>`, where the
// topic name is [TopicName]'s and the subscription is written with the same
// escapes. It is injective for given prefixes, and that is the property a
// fan-out stands on: two subscriptions that composed one queue would be one
// SNS subscription, so their consumers would split one copy of each message
// between them with no error anywhere. The join is "__", which no escaped
// topic contains, so ("orders-eu", "billing") and ("orders", "eu-billing")
// are "orders-eu__billing" and "orders__eu-billing", and the subscriptions
// "eu/billing" and "eu-billing" of one topic are two queues as well.
//
// A composed name of eighty characters or more, which is what a queue name
// allows, keeps a readable head and ends in a hash of the whole identity,
// rather than being refused: an SNS topic name may be 256 characters and a
// subscription is a name of the caller's choosing, so the two compose past the
// queue side's budget for inputs that are each entirely legal. A hashed name
// is therefore always exactly eighty characters and a readable one at most
// seventy-nine, which is what keeps the two apart. The head, the hyphen and
// the hex digits are all characters a subscription may hold, so a readable
// name allowed the full eighty could spell another pair's hashed name
// character for character.
func QueueName(topic, subscription, topicPrefix, queuePrefix string) (string, error) {
	name, err := TopicName(topic, topicPrefix)
	if err != nil {
		return "", err
	}
	if subscription == "" {
		// A fan-out subscription IS a queue, so it has to have a name. The
		// core resolves the default before opening a source, so an empty
		// one here is a caller composing the SPI directly.
		return "", errNoSubscription
	}
	sub := escapeNamePart(subscription)
	full := queuePrefix + name + queueNameJoin + sub
	if err := checkQueueNameParts(queuePrefix, sub); err != nil {
		return "", err
	}
	if len(full) < MaxQueueNameLen {
		return full, nil
	}
	// The hash is over the caller's own four strings, NUL-separated, none of
	// which may hold a NUL: that is injective whatever the prefixes are, where
	// the readable name is injective only for given ones.
	sum := sha256.Sum256([]byte(queuePrefix + "\x00" + topicPrefix + "\x00" + topic + "\x00" + subscription))
	head := full[:MaxQueueNameLen-hashLen-1]
	return head + "-" + hex.EncodeToString(sum[:])[:hashLen], nil
}

// checkQueueNameParts refuses a prefix or a subscription made of characters a
// queue name cannot hold, before the hash can turn them into a legal name.
//
// It matters because the hash makes every long name legal: without this check
// a subscription called "a b" would compose into an illegal short name and a
// legal long one, and which of the two a deployment got would depend on how
// long its topic was.
func checkQueueNameParts(queuePrefix, sub string) error {
	if queuePrefix != "" && !validNamePart(queuePrefix) {
		return errQueuePrefix
	}
	if !validNamePart(sub) {
		return errSubscriptionChars
	}
	return nil
}

// validNamePart reports whether s is made only of the characters an SNS topic
// name and an SQS queue name both accept: alphanumerics, hyphens and
// underscores.
func validNamePart(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// arnPrefix is the ARN scheme this driver builds and recognises.
const arnPrefix = "arn:"

// TopicARN builds a topic's ARN from its parts.
//
// It is exported because the mapping is a deployment's business: an operator
// creating topics by hand, or a Terraform module naming them, needs to know
// what a portable topic becomes before anything runs.
func TopicARN(partition, region, accountID, name string) string {
	if partition == "" {
		partition = DefaultPartition
	}
	return arnPrefix + partition + ":sns:" + region + ":" + accountID + ":" + name
}

// DefaultPartition is the ARN partition a configuration that names none is
// built with. It is not a guess at the caller's partition: an ARN needs one,
// and a driver pointed at an emulator has no real partition at all.
const DefaultPartition = "aws"

// IsTopicARN reports whether s is an SNS topic ARN rather than a topic name.
//
// A topic that IS an ARN is used verbatim, which is what a program that
// already has the ARN from its infrastructure does. The test is the service's
// shape and not merely the "arn:" prefix: an SQS queue ARN also starts with
// "arn:", and publishing to one would be publishing to nothing.
func IsTopicARN(s string) bool {
	return strings.HasPrefix(s, arnPrefix) && strings.Contains(s, ":sns:") &&
		!strings.HasPrefix(s, arnPrefix+"aws:sqs:")
}

// TopicNameOfARN is the topic name an ARN names.
//
// A SUBSCRIPTION ARN is a topic ARN with one more colon-separated element, so
// the last element of a six-element ARN is the topic and the last element of a
// seven-element one is the subscription identifier; this returns the topic
// either way.
func TopicNameOfARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return arn
	}
	return parts[5]
}

// The name refusals.
var (
	errNoTopicName       = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: the topic name is empty")
	errTopicNameLong     = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: the topic name is longer than the service's 256 characters")
	errTopicNameChars    = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: an SNS topic name takes alphanumerics, hyphens and underscores only")
	errFIFOTopic         = errcode.New(errcode.Unsupported, "iqmq/snssqs: a .fifo name is an SNS FIFO topic, whose ordering and deduplication rules are a capability table this driver does not implement; drivers/sqs has the FIFO queue profile")
	errNoSubscription    = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: a fan-out subscription IS a queue and has to be named; SourceConfig.Subscription is empty")
	errSubscriptionChars = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: a subscription becomes part of a queue name, which takes alphanumerics, hyphens, underscores and the slashes this mapping escapes")
	errQueuePrefix       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: QueuePrefix is not made of the alphanumerics, hyphens and underscores a queue name accepts")
	errNoWildcard        = errcode.New(errcode.Unsupported, "iqmq/snssqs: SNS has no pattern subscription; a subscription names one topic ARN, so a topic carrying a wildcard metacharacter names no topic")
)
