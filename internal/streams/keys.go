package streams

import (
	"strconv"
	"strings"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Keys is a topic's key naming: which Redis key each of its shards lives
// under, and which auxiliary keys the delay and dead-letter routes use.
//
// Every name is hash-tagged. Redis Cluster hashes the substring between the
// first '{' and the next '}' rather than the whole key, so a topic's shards
// are "{acme.orders}", "{acme.orders}:1", "{acme.orders}:2" and every one of
// them lands on the same slot. That is not a tidiness preference: the Lua
// dead-letter move and the delayed-mover script each touch two keys at once,
// and a Lua script in a cluster is refused outright when its keys are not on
// one slot (CROSSSLOT). With the tag, a topic and its own auxiliary keys are
// one slot by construction; without it, the dead-letter move would work on a
// single node and fail on every cluster, which is the worst way to find out.
//
// The cost is stated rather than hidden: one topic is one slot, so a topic
// is never spread across a cluster's nodes and a single very hot topic does
// not scale out by sharding. Partitions buy ordering lanes and parallel
// consumers, not node fan-out. The driver page says so.
type Keys struct {
	// Topic is the logical topic.
	Topic string
	// Shards is how many shards the topic has, always at least 1.
	Shards int
	// tag is the hash tag, computed once.
	tag string
}

// NewKeys returns the key naming for a topic with shards shards.
//
// A topic name that already contains '{' or '}' is refused: it would make the
// hash tag something other than the topic, so two topics could collide on one
// slot in ways neither operator asked for, and the shard keys of one topic
// could hash differently from each other. Refusing is the only answer that
// keeps the invariant this type exists for.
func NewKeys(topic string, shards int) (Keys, error) {
	if topic == "" {
		return Keys{}, ErrEmptyTopic
	}
	if strings.ContainsAny(topic, "{}") {
		return Keys{}, ErrTopicHashTag
	}
	if strings.ContainsAny(topic, "\r\n") {
		// A key with a newline in it is legal in Redis and illegal in the
		// inline command protocol, and it breaks every operator tool that
		// reads a key list. It is also never a topic anybody meant.
		return Keys{}, ErrTopicControl
	}
	if shards < 1 {
		return Keys{}, ErrShardCount
	}
	return Keys{Topic: topic, Shards: shards, tag: "{" + topic + "}"}, nil
}

// Tag returns the hash tag every one of this topic's keys carries.
func (k Keys) Tag() string { return k.tag }

// Stream returns the key of shard i.
//
// Shard 0 is the bare tag and not "{t}:0", which is deliberate: an
// unpartitioned topic is the overwhelmingly common case, and its key is then
// "{acme.orders}" — readable in redis-cli, and the same key whether the topic
// is later given more shards. A naming that spelled shard 0 as "{t}:0" would
// mean that adding a partition renamed the stream that already held every
// message.
func (k Keys) Stream(i int) string {
	if i <= 0 {
		return k.tag
	}
	return k.tag + ":" + strconv.Itoa(i)
}

// Streams appends every shard key, in shard order, to dst.
func (k Keys) Streams(dst []string) []string {
	for i := range k.Shards {
		dst = append(dst, k.Stream(i))
	}
	return dst
}

// Shard returns the shard a key belongs to, and false when the key is not one
// of this topic's shards.
func (k Keys) Shard(key string) (int, bool) {
	if key == k.tag {
		return 0, true
	}
	rest, ok := strings.CutPrefix(key, k.tag+":")
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(rest)
	if err != nil || i <= 0 || i >= k.Shards {
		return 0, false
	}
	return i, true
}

// Delayed returns the sorted-set key the zset nack strategy parks a delayed
// redelivery in, for shard i.
//
// It carries the topic's tag, so the script that moves an entry from the zset
// back into the stream touches two keys on one slot.
func (k Keys) Delayed(i int) string { return k.Stream(i) + ":delayed" }

// SlotSafe reports whether every key in keys shares one hash slot, judged by
// the hash tag alone.
//
// It is what the Lua routes check before they run: a script whose keys are on
// two slots is refused by a cluster, and the honest driver behaviour is to
// refuse it first, with an error that says which keys, rather than to hand
// the operator a CROSSSLOT from inside a script they did not write.
func SlotSafe(keys ...string) bool {
	if len(keys) < 2 {
		return true
	}
	first, ok := HashTag(keys[0])
	if !ok {
		return false
	}
	for _, key := range keys[1:] {
		tag, ok := HashTag(key)
		if !ok || tag != first {
			return false
		}
	}
	return true
}

// HashTag returns the substring a Redis Cluster hashes, and false when the key
// has no tag at all.
//
// The rule is Redis's own and it has two corners worth keeping: the tag is
// between the FIRST '{' and the FIRST '}' after it, and an empty tag ("{}")
// does not count — Redis falls back to hashing the whole key. A helper that
// got either wrong would report two keys as co-located when they are not,
// which is the one mistake this function exists to prevent.
func HashTag(key string) (string, bool) {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return "", false
	}
	shut := strings.IndexByte(key[open+1:], '}')
	if shut <= 0 {
		return "", false
	}
	return key[open+1 : open+1+shut], true
}

// Group returns the consumer-group name for a subscription.
//
// It is the subscription name unchanged. A group is scoped to a stream key in
// Redis, so two topics' subscriptions of the same name never collide, and
// decorating the name would make the group in redis-cli something other than
// what the operator configured.
func Group(sub string) (string, error) {
	if sub == "" {
		return "", ErrEmptySubscription
	}
	if strings.ContainsAny(sub, "\r\n") {
		return "", ErrSubscriptionControl
	}
	return sub, nil
}

// The key naming's refusals.
var (
	// ErrEmptyTopic is a topic with no name.
	ErrEmptyTopic = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: the topic is empty")
	// ErrTopicHashTag is a topic that would break the hash tag.
	ErrTopicHashTag = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: a topic name may not contain '{' or '}': the key naming hash-tags the topic so that its shards share one cluster slot, and a tag inside a tag is not one")
	// ErrTopicControl is a topic with a carriage return or newline in it.
	ErrTopicControl = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: a topic name may not contain a carriage return or a newline")
	// ErrShardCount is a shard count below one.
	ErrShardCount = errcode.New(errcode.InvalidConfig,
		"iqmq/streams: a topic has at least one shard")
	// ErrEmptySubscription is a subscription with no name.
	ErrEmptySubscription = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: the subscription is empty")
	// ErrSubscriptionControl is a subscription name with a control character.
	ErrSubscriptionControl = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: a subscription name may not contain a carriage return or a newline")
)
