package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The queue-name grammar, from the pinned SDK's CreateQueue documentation: a
// name is at most eighty characters of alphanumerics, hyphens and underscores,
// and a FIFO queue's name ends in ".fifo" — which is the only place a period
// is allowed.
const fifoSuffix = ".fifo"

// QueueName is the default topic-to-queue mapping (the specification's "topic→queue
// name/URL (resolver option)").
//
// A portable topic is a slash-separated path and a queue name is not, so "/"
// becomes "-". The mapping is therefore NOT injective — "orders/eu" and
// "orders-eu" are one queue — which is stated here rather than hidden, and is
// the same trade the specification records for NATS subjects. A deployment that
// cannot live with it sets [Config.Resolver] or [Config.QueueURLs].
//
// The prefix is prepended verbatim, and the ".fifo" suffix the service
// requires is appended for the FIFO profile — once: a topic that already ends
// in ".fifo" does not get a second one.
func QueueName(topic, prefix string, fifo bool) (string, error) {
	// The wildcard check comes FIRST, before the portable grammar. A
	// pattern metacharacter is outside the portable segment alphabet, so
	// mq.ValidateTopic refuses it as an invalid topic — and the conformance
	// kit's negative probe for FeatureWildcardSubscribe reads
	// ErrUnsupported and nothing else, so a driver that let the grammar
	// answer first would leave the claim unproved in both directions. The
	// core validates a topic on the publish path and not on the consume
	// path, which is how a pattern reaches a driver at all.
	if HasWildcard(topic) {
		// A queue name is a name and the service has no pattern
		// subscription at all, so a topic carrying a wildcard
		// metacharacter is refused with ErrUnsupported rather than
		// flattened into a queue called "orders-". The negative
		// direction is provable this way: the conformance kit hands the
		// driver a pattern and reads the refusal.
		return "", errNoWildcard
	}
	if err := mq.ValidateTopic(topic); err != nil {
		return "", err
	}
	name := prefix + strings.ReplaceAll(topic, "/", "-")
	if fifo {
		name = strings.TrimSuffix(name, fifoSuffix) + fifoSuffix
	}
	if err := CheckQueueName(name, fifo); err != nil {
		return "", err
	}
	return name, nil
}

// CheckQueueName reports whether name is a queue name the service accepts.
//
// It is pure and runs before any I/O, which is what turns a topic the mapping
// cannot express into an immediate refusal naming the rule rather than a
// signed request the service rejects with a message about a parameter.
func CheckQueueName(name string, fifo bool) error {
	body := name
	switch {
	case name == "":
		return errNoQueueName
	case len(name) > MaxQueueNameLen:
		return errQueueNameLong
	case fifo && !strings.HasSuffix(name, fifoSuffix):
		return errQueueNotFIFO
	case !fifo && strings.HasSuffix(name, fifoSuffix):
		return errQueueIsFIFO
	}
	if fifo {
		body = strings.TrimSuffix(name, fifoSuffix)
		if body == "" {
			return errNoQueueName
		}
	}
	if !validNamePart(body) {
		return errQueueNameChars
	}
	return nil
}

// HasWildcard reports whether a topic carries a subscription-pattern
// metacharacter.
//
// The set is the union of the ones the brokers iqmq speaks to use — "*" and
// "?" (glob), "#" and ">" (AMQP and NATS) — because what this function
// answers is "did the caller mean a pattern?", and a caller who meant one on
// any other broker means one here.
func HasWildcard(topic string) bool {
	for i := range len(topic) {
		switch topic[i] {
		case '*', '?', '#', '>':
			return true
		}
	}
	return false
}

// validNamePart reports whether s is made only of the characters a queue name
// accepts.
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

// The group-identifier grammar: at most 128 characters of alphanumerics and
// punctuation, from the pinned SDK's SendMessage documentation for
// MessageGroupId and MessageDeduplicationId.
const maxGroupIDLen = 128

// GroupID renders a key as a FIFO MessageGroupId.
//
// A key is an arbitrary byte string and a group identifier is at most 128
// characters of printable ASCII, so the two do not fit. The rendering is:
//
//   - a key that is already a short printable-ASCII string is used verbatim,
//     so an ordinary key is readable in the console and interoperable with a
//     non-iqmq producer that uses the same convention;
//   - anything else is "h-" followed by the hex of its SHA-256, truncated to
//     forty characters.
//
// The hash half is not a collision worry in the direction that matters. Two
// keys that collided would share a message group, which ADDS an ordering
// constraint and never removes one: the guarantee "one key's messages are
// delivered in order" still holds, and what a collision costs is FIFO
// throughput, which is per group. A rendering that truncated the key instead
// would have the same collision behaviour and would also make two DIFFERENT
// short keys indistinguishable in the console.
//
// An empty key renders as [DefaultGroupID].
func GroupID(key []byte) string {
	if len(key) == 0 {
		return DefaultGroupID
	}
	if len(key) <= maxGroupIDLen && printableASCII(key) {
		return string(key)
	}
	sum := sha256.Sum256(key)
	return "h-" + hex.EncodeToString(sum[:20])
}

// DefaultGroupID is the message group a FIFO publication with no key is sent
// in.
//
// A FIFO queue REQUIRES a MessageGroupId, so a keyless publication has to name
// one, and the two alternatives are both worse: refusing would make an
// ordinary keyless publish fail on the FIFO profile alone, and inventing a
// unique group per message would silently turn the profile a caller chose for
// its ordering into an unordered one. One shared group is the honest answer —
// keyless messages are globally ordered with respect to each other, which is
// what a FIFO queue with one group means — and the cost is throughput, which
// docs/drivers/sqs.md states.
const DefaultGroupID = "iqmq-default"

// DedupID renders a caller's DedupKey as a MessageDeduplicationId, under the
// same grammar and the same rendering as [GroupID].
func DedupID(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= maxGroupIDLen && printableASCII([]byte(key)) {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return "h-" + hex.EncodeToString(sum[:20])
}

// printableASCII reports whether b is printable ASCII with no space, which is
// the conservative intersection of what a group identifier accepts.
func printableASCII(b []byte) bool {
	for i := range b {
		if b[i] <= ' ' || b[i] >= 0x7f {
			return false
		}
	}
	return true
}

// QueueNameOfURL is the queue name a URL or an ARN names, which is what the
// admin surface reports and what an ARN is built from.
func QueueNameOfURL(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// TopicOfQueueName is [QueueName] backwards: the portable topic a queue name
// came from, and false for a name this configuration did not produce.
//
// It exists because [admin.ListTopics] yielded the raw queue name, prefix and
// all, while every other method on that surface PREPENDS the prefix to what
// it is given. So a caller who listed the topics and passed one back looked
// up `app-app-orders` and was told the queue does not exist -- the listing's
// output could not be used with the surface it came from whenever a
// QueuePrefix was configured.
//
// The "/" that QueueName flattens to "-" is NOT restored: the mapping is
// lossy in that direction and a topic containing a slash and one containing
// a dash produce the same queue. What comes back is therefore the topic a
// caller can address, which is what a listing is for.
func TopicOfQueueName(name, prefix string) (string, bool) {
	if prefix != "" {
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok {
			// A queue of this account that another deployment owns. The
			// service filters by prefix, so it should not be here at all --
			// and reporting it as a topic would invite a caller to delete it.
			return "", false
		}
		name = rest
	}
	name = strings.TrimSuffix(name, fifoSuffix)
	if name == "" {
		return "", false
	}
	return name, true
}

// The name refusals.
var (
	errNoQueueName    = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the queue name is empty")
	errQueueNameLong  = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the queue name is longer than the service's eighty characters")
	errQueueNameChars = errcode.New(errcode.InvalidArgument, "iqmq/sqs: a queue name takes alphanumerics, hyphens and underscores only")
	errQueueNotFIFO   = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the FIFO profile needs a queue name ending in .fifo")
	errQueueIsFIFO    = errcode.New(errcode.InvalidArgument, "iqmq/sqs: a .fifo queue name needs the FIFO profile; Config.FIFO changes the capability table as well as the name")
	errNoWildcard     = errcode.New(errcode.Unsupported, "iqmq/sqs: the service has no pattern subscription; a topic carrying a wildcard metacharacter names no queue")
)
