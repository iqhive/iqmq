package redis_test

import (
	"strings"
	"testing"

	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/streams"
)

// FuzzRedisKeyNaming holds the key naming to the property the Lua routes
// depend on: every key of one topic shares one cluster hash slot.
//
// It is not a tidiness property. The dead-letter move, the delayed park and
// the delayed release are each one script touching two of a topic's keys, and
// Redis Cluster refuses a script whose keys are on two slots — so a topic
// whose keys did not share a tag would work on a single node and fail on every
// cluster, which is the worst way to find out. The bound is checked here over
// every topic name and prefix the fuzzer can build.
//
// The second property is that a name the driver ACCEPTS produces keys it can
// map back: [streams.Keys.Shard] has to recover the shard from the key, or a
// reply cannot be attributed to the shard it came from and a settlement goes
// to the wrong stream.
func FuzzRedisKeyNaming(f *testing.F) {
	seeds := [][3]string{
		{"", "orders", "1"},
		{"iq.", "orders", "4"},
		{"", "acme.orders", "8"},
		{"", "a", "1"},
		{"", "{braced}", "1"},
		{"pre{", "orders", "2"},
		{"", "orders}", "2"},
		{"", "a\nb", "1"},
		{"", "", "1"},
		{"", strings.Repeat("t", 600), "2"},
		{"", "🙂", "3"},
		{"", "a:b:c", "5"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1], s[2])
	}
	f.Fuzz(func(t *testing.T, prefix, topic, shardsStr string) {
		shards := 1
		if len(shardsStr) > 0 {
			shards = int(shardsStr[0])%8 + 1
		}
		cfg := rd.Config{KeyPrefix: prefix, Partitions: shards}
		if err := cfg.Validate(); err != nil {
			// A configuration the driver refuses proves nothing about the
			// key naming; what matters is that it refused rather than
			// produced keys on two slots.
			return
		}
		keys, err := streams.NewKeys(prefix+topic, shards)
		if err != nil {
			return
		}
		// Property 1: every key of the topic shares one hash slot, including
		// the auxiliary ones the scripts touch.
		all := make([]string, 0, 2*shards+1)
		for i := range shards {
			all = append(all, keys.Stream(i), keys.Delayed(i))
		}
		all = append(all, keys.Stream(0)+rd.DefaultDeadLetterSuffix)
		if !streams.SlotSafe(all...) {
			t.Fatalf("the keys of one topic are not on one slot: %q — a Lua route touching two of "+
				"them is refused by every cluster with CROSSSLOT", all)
		}
		tag, ok := streams.HashTag(keys.Stream(0))
		if !ok {
			t.Fatalf("shard 0's key %q has no hash tag", keys.Stream(0))
		}
		if tag != prefix+topic {
			t.Fatalf("the hash tag is %q, want the prefixed topic %q: a tag that is not the topic "+
				"lets two topics collide on one slot", tag, prefix+topic)
		}
		// Property 2: a key maps back to its shard.
		for i := range shards {
			got, known := keys.Shard(keys.Stream(i))
			if !known || got != i {
				t.Fatalf("Shard(%q) = %d, %v; want shard %d — a reply that cannot be attributed "+
					"to its shard settles against the wrong stream", keys.Stream(i), got, known, i)
			}
		}
		// And a key that is NOT one of this topic's shards is not claimed as
		// one, which is the direction that would send a settlement to a
		// stranger's stream.
		for _, foreign := range []string{
			keys.Delayed(0),
			keys.Stream(0) + rd.DefaultDeadLetterSuffix,
			keys.Stream(shards),
			"{other}",
		} {
			if _, known := keys.Shard(foreign); known {
				t.Fatalf("Shard(%q) claimed a key that is not one of this topic's %d shards",
					foreign, shards)
			}
		}
	})
}

// FuzzRedisDSN holds the connection-string parser to the two properties a DSN
// parser has to have.
//
// It never panics, whatever bytes arrive — a DSN comes from a configuration
// file or an environment variable, so it is untrusted input in the sense that
// matters. And whatever it ACCEPTS renders back to something safe to log: the
// credential is gone and the result is printable UTF-8, because a rendered
// configuration goes into a log line and into every error built from one
func FuzzRedisDSN(f *testing.F) {
	for _, seed := range []string{
		"redis://h:6379",
		"redis://user:pass@h:6379/3?partitions=4",
		"redis://a:6379,b:6379?nack=zsetmover&ackguard=none",
		"redis://h?tls=true&insecureskipverify=true",
		"redis://h?keyprefix=iq.&maxlen=1000",
		"redis://\xc5",
		"redis://h?consumerprefix=\x00",
		"redis://h/99999999999999999999",
		"redis://h?ackdeadline=notaduration",
		"rediss://h",
		"redis://h?partitions=99999999999999999999",
		// A one-character password whose value is also a substring of the
		// host: the input that showed a substring redaction check cannot
		// work.
		"redis://:0@0",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := rd.ParseDSN(s)
		if err != nil {
			return
		}
		rendered := cfg.DSN()
		if !utf8ValidString(rendered) {
			t.Fatalf("ParseDSN accepted %q and renders as invalid UTF-8: a configuration goes into "+
				"a log line and into every error built from one", s)
		}
		for _, r := range rendered {
			if r < 0x20 || r == 0x7f {
				t.Fatalf("ParseDSN accepted %q and renders with a control character (%q): no log "+
					"pipeline can carry it", s, r)
			}
		}
		// The redaction is checked by RE-PARSING the rendering and looking at
		// the credential position, not by searching the whole string for the
		// password.
		//
		// A substring search is what this target had first, and it reported a
		// leak on `redis://:0@0` within fifteen seconds: a one-character
		// password is a substring of almost any rendering, and there the "0"
		// it found was the host name. The false positive is the fuzzer's
		// point — a redaction check that cannot handle a one-byte credential
		// cannot tell a leak from a coincidence, in either direction.
		// The committed input is
		// testdata/fuzz/FuzzRedisDSN/d8391ee21c6aa4a8.
		if cfg.Password != "" {
			back, rerr := rd.ParseDSN(rendered)
			if rerr != nil {
				t.Fatalf("ParseDSN accepted %q and its own rendering does not parse: %v", s, rerr)
			}
			if got := back.Password.Reveal(); got != dsn.Redacted {
				t.Fatalf("the rendered configuration's credential is %q, want the redaction "+
					"marker %q ", got, dsn.Redacted)
			}
		}
		// What it accepted has to be usable: Validate is what every Open
		// calls first, so a configuration ParseDSN accepted and Validate
		// refuses is a DSN that fails at a confusing place.
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN accepted %q and Validate refuses the result: %v", s, verr)
		}
		// And the rendering parses, which is what makes it usable in a
		// diagnostic rather than only readable. The credential is
		// deliberately not restored: what is asserted is the SHAPE.
		if _, rerr := rd.ParseDSN(rendered); rerr != nil {
			t.Fatalf("ParseDSN accepted %q and its own rendering %q does not parse: %v",
				s, rendered, rerr)
		}
	})
}

// utf8ValidString is strings.ToValidUTF8's test, spelled out so that the
// assertion reads as what it checks.
func utf8ValidString(s string) bool { return strings.ToValidUTF8(s, "�") == s }

// FuzzRedisShardRouting holds the key-to-shard mapping to the per-key
// ordering requirement.
//
// The requirement reduces to one property: a key always goes to the same
// shard. A key that hashed differently in two publishers, or differently
// after a restart, would put one key's messages in two shards — and per-key
// ordering would be broken with nothing failing, which is the worst kind of
// broken.
func FuzzRedisShardRouting(f *testing.F) {
	for _, seed := range [][]byte{nil, {}, []byte("a"), []byte("order-4711"), {0, 1, 0xff}, make([]byte, 300)} {
		f.Add(seed, 4)
	}
	f.Fuzz(func(t *testing.T, key []byte, shardsIn int) {
		shards := shardsIn%64 + 1
		if shards < 1 {
			shards = 1
		}
		first := rd.ShardOf(key, shards)
		if first < 0 || first >= shards {
			t.Fatalf("ShardOf(%q, %d) = %d, outside the shard range", key, shards, first)
		}
		for range 8 {
			if got := rd.ShardOf(key, shards); got != first {
				t.Fatalf("ShardOf(%q, %d) returned %d then %d: a key that moves between shards "+
					"breaks per-key ordering with nothing failing", key, shards, first, got)
			}
		}
		// A copy of the key hashes the same, which is what makes the mapping
		// a function of the BYTES rather than of the slice.
		copied := append([]byte(nil), key...)
		if got := rd.ShardOf(copied, shards); got != first {
			t.Fatalf("ShardOf hashed a copy of the key to %d instead of %d", got, first)
		}
	})
}
