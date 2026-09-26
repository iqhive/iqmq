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

// TopicName is the default portable-topic-to-SNS-topic mapping (the specification's
// "topic→queue name/URL (resolver option)", one level up).
//
// A portable topic is a slash-separated path and an SNS topic name is not, so
// "/" becomes "-". The mapping is therefore NOT injective — "orders/eu" and
// "orders-eu" are one topic — which is stated rather than hidden, and is the
// same trade the specification records for the queue side. A deployment that
// cannot live with it sets [Config.Resolver] or [Config.TopicARNs].
func TopicName(topic, prefix string) (string, error) {
	if sqs.HasWildcard(topic) {
		// SNS has no pattern subscription: a subscription names ONE topic
		// ARN. So a topic carrying a wildcard metacharacter is refused with
		// ErrUnsupported rather than flattened into a topic called
		// "orders-", which is what makes the negative direction for
		// FeatureWildcardSubscribe provable — the conformance kit hands the
		// driver a pattern and reads the refusal.
		return "", errNoWildcard
	}
	if err := mq.ValidateTopic(topic); err != nil {
		return "", err
	}
	name := prefix + strings.ReplaceAll(topic, "/", "-")
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
// The composition is `<prefix><topic>-<subscription>` with "/" flattened to
// "-" in both halves. A composed name longer than the eighty characters a
// queue name allows keeps a readable head and ends in a hash of the whole
// identity, rather than being refused: an SNS topic name may be 256 characters
// and a subscription is a name of the caller's choosing, so the two compose
// past the queue side's budget for inputs that are each entirely legal.
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
	sub := strings.ReplaceAll(subscription, "/", "-")
	full := queuePrefix + name + "-" + sub
	if err := checkQueueNameParts(queuePrefix, sub); err != nil {
		return "", err
	}
	if len(full) <= MaxQueueNameLen {
		return full, nil
	}
	sum := sha256.Sum256([]byte(queuePrefix + "\x00" + name + "\x00" + sub))
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
	errSubscriptionChars = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: a subscription becomes part of a queue name, which takes alphanumerics, hyphens, underscores and the slashes this mapping flattens")
	errQueuePrefix       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: QueuePrefix is not made of the alphanumerics, hyphens and underscores a queue name accepts")
	errNoWildcard        = errcode.New(errcode.Unsupported, "iqmq/snssqs: SNS has no pattern subscription; a subscription names one topic ARN, so a topic carrying a wildcard metacharacter names no topic")
)
