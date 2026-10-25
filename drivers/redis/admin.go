package redis

import (
	"context"
	"iter"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// Admin returns the topology surface.
//
// Redis has one, which is why this driver implements
// [github.com/iqhive/iqmq.AdminBackend] where `drivers/nats` does not: a
// stream is a key and a subscription is a consumer group, and both can be
// created, deleted, purged and counted.
//
// What it does NOT do is anything the specification leaves to the driver's own typed
// administration. TopicSpec carries a name and nothing else, so the shard
// count, the trim and the auto-creation come from the [Config] rather than
// from the spec — which is the rule the specification states and the reason the portable
// intersection is as small as it is.
func (b *Backend) Admin() (mq.Admin, error) {
	if err := b.CheckOpen(context.Background()); err != nil {
		return nil, err
	}
	return &admin{b: b}, nil
}

// admin is the topology surface.
type admin struct{ b *Backend }

var _ mq.Admin = (*admin)(nil)

// EnsureTopic creates the topic's streams if they do not exist.
//
// A Redis stream is created by writing to it, and there is no "create an empty
// stream" command — so this creates each shard with a zero-length group
// instead: XGROUP CREATE ... MKSTREAM makes the key without adding an entry.
// The group is named [ensureGroup] and immediately destroyed, which leaves
// exactly the empty stream the caller asked for.
//
// It is idempotent, which the specification requires: a stream that exists is left alone.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	keys, err := a.b.cfg.keysFor(spec.Name)
	if err != nil {
		return err
	}
	cmds := make([]streams.Cmd, 0, 2*keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds,
			streams.XGroupCreate(keys.Stream(i), ensureGroup, streams.IDZero, true),
			streams.XGroupDestroy(keys.Stream(i), ensureGroup),
		)
	}
	results, err := a.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, spec.Name, "", err)
	}
	for i, res := range results {
		if res.Err == nil || streams.IsBusyGroup(res.Err) {
			continue
		}
		// The destroy of a group that was never created reports nothing to
		// destroy, which is not a failure.
		if i%2 == 1 {
			continue
		}
		return a.b.errs.Map(mq.OpAdmin, spec.Name, "", res.Err)
	}
	return nil
}

// ensureGroup is the throwaway group EnsureTopic uses to make an empty
// stream.
//
// The name is fixed and recognisable on purpose: an operator who finds it in
// XINFO GROUPS should be able to tell that something crashed between the
// create and the destroy rather than wonder what subscription it is.
const ensureGroup = "iqmq-ensure-topic"

// DeleteTopic removes a topic and everything in it.
//
// It is DEL on every shard and on every shard's delayed sorted set, which is
// "everything in it" including the redeliveries the zset strategy parked: a
// delete that left those behind would resurrect messages into a topic that no
// longer exists.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	keys, err := a.b.cfg.keysFor(topic)
	if err != nil {
		return err
	}
	tokens := make([]string, 0, 1+2*keys.Shards)
	tokens = append(tokens, "DEL")
	for i := range keys.Shards {
		tokens = append(tokens, keys.Stream(i), keys.Delayed(i))
	}
	// Every key carries the topic's hash tag, so the multi-key DEL is on one
	// slot by construction — which is what makes this one command rather
	// than one per shard.
	cmd := streams.Cmd{Tokens: tokens, KeyStart: 1, KeyCount: len(tokens) - 1}
	if _, err := a.b.cmder.Do(ctx, cmd); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return nil
}

// ListTopics iterates the topics the broker reports.
//
// It is a SCAN over the key space with this deployment's prefix, and it is
// deliberately a SCAN rather than a KEYS: KEYS blocks the server for the
// whole key space, which on a production Redis is an outage. The iteration
// stops at the first error it yields, which is the contract.
//
// It reports a topic once, not once per shard: shard 0's key IS the topic's
// tag, so the pattern matches the bare tag and the shard suffixes are
// filtered out.
//
// On a cluster the SCAN runs on each node in turn. SCAN names no key, so a
// cluster client sends it to whichever node it likes, and a cursor is only
// meaningful to the node that issued it: one iteration spread across nodes
// listed some of the topics, or restarted at random. A node list can include
// replicas, which hold the same keys as their masters, so a topic is reported
// once however many nodes hold it.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		nodes := []streams.Commander{a.b.cmder}
		if nl, ok := a.b.cmder.(nodeLister); ok {
			ns, err := nl.Nodes(ctx)
			if err != nil {
				yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
				return
			}
			if len(ns) > 0 {
				nodes = ns
			}
		}
		var seen map[string]struct{}
		if len(nodes) > 1 {
			seen = make(map[string]struct{})
		}
		for _, node := range nodes {
			if !a.scanTopics(ctx, node, seen, yield) {
				return
			}
		}
	}
}

// nodeLister is the optional surface of a commander over a cluster: one
// commander per node, each sending to that node only. A commander over one
// server does not implement it, or answers nil. The commanders it returns are
// borrowed from the cluster client and are not closed by the caller.
type nodeLister interface {
	Nodes(ctx context.Context) ([]streams.Commander, error)
}

// scanTopics is one node's SCAN, yielding each topic that seen does not already
// hold. It reports false when the iteration has ended, by the caller or by an
// error it yielded.
func (a *admin) scanTopics(ctx context.Context, node streams.Commander, seen map[string]struct{}, yield func(string, error) bool) bool {
	cursor := "0"
	// The pattern deliberately does NOT close the hash tag. A pattern of
	// "{prefix*}" would match shard 0's key and nothing else, which looks
	// like a saving and is a trap: it makes the SERVER responsible for
	// excluding the shard keys, so the client-side check below becomes
	// unreachable and a later change to either one silently reports a
	// four-shard topic four times. With the open pattern both halves are
	// exercised by TestListTopicsReportsATopicOnce.
	pattern := "{" + a.b.cfg.KeyPrefix + "*"
	for {
		v, err := node.Do(ctx, streams.Cmd{Tokens: []string{
			"SCAN", cursor, "MATCH", pattern, "COUNT", "256", "TYPE", "stream",
		}})
		if err != nil {
			yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
			return false
		}
		pair, ok := v.Arr()
		if !ok || len(pair) != 2 {
			yield("", a.b.errs.Map(mq.OpAdmin, "", "", streams.ErrReplyShape))
			return false
		}
		next, ok := pair[0].Str()
		if !ok {
			yield("", a.b.errs.Map(mq.OpAdmin, "", "", streams.ErrReplyShape))
			return false
		}
		items, ok := pair[1].Arr()
		if !ok {
			yield("", a.b.errs.Map(mq.OpAdmin, "", "", streams.ErrReplyShape))
			return false
		}
		for _, item := range items {
			key, ok := item.Str()
			if !ok {
				continue
			}
			topic, ok := a.topicOf(key)
			if !ok {
				continue
			}
			if seen != nil {
				if _, dup := seen[topic]; dup {
					continue
				}
				seen[topic] = struct{}{}
			}
			if !yield(topic, nil) {
				return false
			}
		}
		if next == "0" {
			return true
		}
		cursor = next
	}
}

// topicOf recovers a topic name from shard 0's key, and reports false for
// anything else — a shard suffix, a delayed sorted set, a key that is not
// ours.
func (a *admin) topicOf(key string) (string, bool) {
	tag, ok := streams.HashTag(key)
	if !ok {
		return "", false
	}
	if key != "{"+tag+"}" {
		// A shard key or an auxiliary key: the topic is reported once, from
		// shard 0.
		return "", false
	}
	if a.b.cfg.KeyPrefix != "" {
		if len(tag) <= len(a.b.cfg.KeyPrefix) || tag[:len(a.b.cfg.KeyPrefix)] != a.b.cfg.KeyPrefix {
			return "", false
		}
		return tag[len(a.b.cfg.KeyPrefix):], true
	}
	return tag, true
}

// EnsureSubscription creates the consumer group if it does not exist.
//
// It is idempotent: BUSYGROUP is what a second caller sees and is not a
// failure. The start position is the end of the stream, which is the specification's
// default for a subscription with no committed position — a subscription
// created by an administrator should not begin by replaying everything the
// stream still holds.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	keys, err := a.b.cfg.keysFor(spec.Topic)
	if err != nil {
		return err
	}
	group, err := streams.Group(spec.Name)
	if err != nil {
		return err
	}
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.XGroupCreate(keys.Stream(i), group, streams.IDLast, true))
	}
	results, err := a.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, spec.Topic, spec.Name, err)
	}
	for _, res := range results {
		if res.Err != nil && !streams.IsBusyGroup(res.Err) {
			return a.b.errs.Map(mq.OpAdmin, spec.Topic, spec.Name, res.Err)
		}
	}
	return nil
}

// DeleteSubscription removes the consumer group.
//
// It is destructive in a way worth stating: XGROUP DESTROY removes the
// group's pending-entries list with the group, so every message this
// subscription had been handed and not settled is forgotten. The messages
// themselves stay in the stream — another subscription still sees them — but
// this one's in-flight work is gone. The specification makes the destructive operations
// explicit and never implied, which is exactly why this is the only place it
// happens.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	keys, err := a.b.cfg.keysFor(topic)
	if err != nil {
		return err
	}
	group, err := streams.Group(sub)
	if err != nil {
		return err
	}
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.XGroupDestroy(keys.Stream(i), group))
	}
	results, err := a.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	for _, res := range results {
		if res.Err != nil && !streams.IsNoGroup(res.Err) {
			return a.b.errs.Map(mq.OpAdmin, topic, sub, res.Err)
		}
	}
	return nil
}

// Purge discards everything a subscription has not consumed.
//
// It is XGROUP SETID to the end of the stream, which moves the group past
// every entry it has not delivered. It does NOT touch the pending-entries
// list, and that is not an omission: a purge is about what has not been
// delivered, and an entry in the pending list HAS been delivered and is
// somebody's in-flight work. Deleting those would be a purge that threw away
// messages a handler is running.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	keys, err := a.b.cfg.keysFor(topic)
	if err != nil {
		return err
	}
	group, err := streams.Group(sub)
	if err != nil {
		return err
	}
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.XGroupSetID(keys.Stream(i), group, streams.IDLast))
	}
	results, err := a.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	for _, res := range results {
		if res.Err != nil {
			return a.b.errs.Map(mq.OpAdmin, topic, sub, res.Err)
		}
	}
	return nil
}

// Stats reports a subscription's depth, in-flight count and lag.
//
// All three come from XINFO GROUPS, summed over the topic's shards. `lag` is
// Redis's own — the entries the group has not delivered — and it is the one
// field that can be unknown: Redis reports it as nil after an XDEL or an
// XTRIM made the count unreliable, and Limit[T] is how "the broker did
// not say" is reported rather than as a zero.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	keys, err := a.b.cfg.keysFor(topic)
	if err != nil {
		return mq.QueueStats{}, err
	}
	group, err := streams.Group(sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.Cmd{
			Tokens:   []string{"XINFO", "GROUPS", keys.Stream(i)},
			KeyStart: 2, KeyCount: 1,
		})
	}
	results, err := a.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return mq.QueueStats{}, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	var (
		pending, lag int64
		lagKnown     = true
		found        bool
	)
	for _, res := range results {
		if res.Err != nil {
			if streams.IsNoGroup(res.Err) {
				continue
			}
			return mq.QueueStats{}, a.b.errs.Map(mq.OpAdmin, topic, sub, res.Err)
		}
		rows, ok := res.Value.Arr()
		if !ok {
			continue
		}
		for _, row := range rows {
			name, p, l, lk, ok := groupRow(row)
			if !ok || name != group {
				continue
			}
			found = true
			pending += p
			lag += l
			if !lk {
				lagKnown = false
			}
		}
	}
	if !found {
		return mq.QueueStats{}, a.b.errs.Map(mq.OpAdmin, topic, sub, streams.ErrNoStream)
	}
	stats := mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: lag, Known: lagKnown},
		InFlight: mq.Limit[int64]{Value: pending, Known: true},
		Lag:      mq.Limit[int64]{Value: lag, Known: lagKnown},
	}
	return stats, nil
}

// groupRow reads one XINFO GROUPS row: its name, its pending count, its lag,
// and whether the lag is known.
func groupRow(row streams.Value) (name string, pending, lag int64, lagKnown, ok bool) {
	fields, arrOK := row.Arr()
	if !arrOK {
		return "", 0, 0, false, false
	}
	lagKnown = true
	for i := 0; i+1 < len(fields); i += 2 {
		key, keyOK := fields[i].Str()
		if !keyOK {
			continue
		}
		switch key {
		case "name":
			name, _ = fields[i+1].Str()
		case "pending":
			pending, _ = fields[i+1].Int64()
		case "lag":
			if fields[i+1].IsNil() {
				// Redis reports a nil lag when an XDEL or an XTRIM made the
				// count unreliable. The specification forbids reading that as zero.
				lagKnown = false
				continue
			}
			lag, _ = fields[i+1].Int64()
		}
	}
	return name, pending, lag, lagKnown, name != ""
}
