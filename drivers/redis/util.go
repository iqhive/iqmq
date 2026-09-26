package redis

import (
	"hash/fnv"

	"github.com/iqhive/iqmq/internal/errcode"
)

// ShardOf is the key-to-shard mapping: which of a topic's shards a message
// with this key belongs to.
//
// It is FNV-1a over the key bytes, reduced modulo the shard count, and the
// two properties that matter are stability and independence from the process.
// A key that hashed differently in two publishers, or differently after a
// restart, would put one key's messages in two shards — and the per-key
// ordering is exactly "one key, one shard, one order", so it would be broken
// with nothing failing. FNV-1a is in the standard library, is specified, and
// gives the same answer everywhere; a map-iteration hash or maphash would not.
//
// A nil or empty key is shard 0 rather than a random shard. The specification makes a
// message with no key unordered, so any shard is correct, and the cheapest
// one is the one that needs no hash. The alternative — round-robin — would
// spread keyless messages evenly, which is what a driver with per-shard
// consumers wants; it is not chosen here because it makes a publication's
// destination depend on a counter, so two runs of the same program produce
// different streams and a test cannot assert on either.
func ShardOf(key []byte, shards int) int {
	if shards <= 1 || len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(shards))
}

// errDurableShort is a WAITAOF that came back short of the boundary the
// publisher asked for.
//
// It is AMBIGUOUS rather than a failure, and the distinction is the whole
// point: the entry IS in the stream, so the message may well be delivered,
// and it may yet be fsynced. A caller that read this as "the publication
// failed" and republished would duplicate the message.
var errDurableShort = errcode.New(errcode.Ambiguous,
	"iqmq/redis: WAITAOF returned before the write had reached the number of append-only files ConfirmDurable asked for; the entry is in the stream and may yet be persisted, so the outcome is unknown rather than failed")
