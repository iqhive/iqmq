package kafkago

import (
	"context"
	"errors"
	"iter"
	"strings"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"

	mq "github.com/iqhive/iqmq"
)

// admin is the typed topology surface.
var _ mq.Admin = (*admin)(nil)

type admin struct{ b *Backend }

// EnsureTopic creates the topic if it does not exist.
//
// It is idempotent, which is what "ensure" means: TOPIC_ALREADY_EXISTS is a
// success and not a failure. The partition count and replication factor come
// from [Config.Partitions] and [Config.ReplicationFactor], because
// [github.com/iqhive/iqmq.TopicSpec] carries only the portable intersection —
// the name — and everything else a broker wants is the driver's own typed
// administration.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	name, err := Topic(a.b.cfg.TopicPrefix, spec.Name)
	if err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	resp, err := a.b.cl.CreateTopics(rctx, &kgo.CreateTopicsRequest{
		Topics: []kgo.TopicConfig{{
			Topic:             name,
			NumPartitions:     int(a.b.cfg.Partitions),
			ReplicationFactor: int(a.b.cfg.ReplicationFactor),
		}},
	})
	if err != nil {
		return a.fail(spec.Name, err)
	}
	if terr := resp.Errors[name]; terr != nil && !errors.Is(terr, kgo.TopicAlreadyExists) {
		return a.fail(spec.Name, terr)
	}
	return nil
}

// DeleteTopic removes a topic and everything in it.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	name, err := Topic(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	resp, err := a.b.cl.DeleteTopics(rctx, &kgo.DeleteTopicsRequest{Topics: []string{name}})
	if err != nil {
		return a.fail(topic, err)
	}
	if terr := resp.Errors[name]; terr != nil && !errors.Is(terr, kgo.UnknownTopicOrPartition) {
		return a.fail(topic, terr)
	}
	return nil
}

// ListTopics iterates the topics the cluster reports.
//
// Internal topics — __consumer_offsets and the transaction state log — are
// left out: they are the broker's own bookkeeping and a caller listing "the
// topics" does not mean them.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		rctx, cancel := a.b.bound(ctx)
		defer cancel()
		resp, err := a.b.cl.Metadata(rctx, &kgo.MetadataRequest{})
		if err != nil {
			yield("", a.fail("", err))
			return
		}
		prefix := a.b.cfg.TopicPrefix
		for _, t := range resp.Topics {
			if t.Internal {
				continue
			}
			if t.Error != nil {
				if !yield("", a.fail(TopicOf(prefix, t.Name), t.Error)) {
					return
				}
				continue
			}
			if prefix != "" && !strings.HasPrefix(t.Name, prefix) {
				continue
			}
			if !yield(TopicOf(prefix, t.Name), nil) {
				return
			}
		}
	}
}

// EnsureSubscription creates the subscription if it does not exist.
//
// A Kafka consumer group is not a thing that is created: it exists because it
// has a committed position. So "ensure" here means exactly what
// [Backend.ensureGroupPosition] does — commit the default start position for
// every partition that has none — and that is what makes a group that has
// never consumed anything still remember where it was asked to start.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	name, err := Topic(a.b.cfg.TopicPrefix, spec.Topic)
	if err != nil {
		return err
	}
	if err := CheckGroup(spec.Name); err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	_, err = a.b.ensureGroupPosition(rctx, name, spec.Name, mq.StartLatest())
	return err
}

// DeleteSubscription removes a subscription.
//
// It deletes the consumer group, which is the only thing a Kafka subscription
// is. The group has to be empty: a coordinator refuses to delete a group with
// live members, and that refusal is surfaced rather than worked around —
// deleting the position of a running consumer would make it re-read the whole
// topic.
//
// A group spans topics, so a subscription name used on two topics is one
// group with a position on each, and deleting the group deleted the other
// topic's position too: that subscription restarted at its reset offset. A
// group with a position on another topic loses only this topic's offsets
// (OffsetDelete, Kafka 2.4 and later). The coordinator refuses that while a
// live member consumes this topic, and allows it while members consume only
// the others. A broker without OffsetDelete is refused as unsupported.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	name, err := Topic(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	if err := CheckGroup(sub); err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	positions, err := a.b.groupPositions(rctx, sub)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	if positionedElsewhere(positions, name) {
		return a.deleteTopicOffsets(rctx, topic, sub, name, positions[name])
	}
	resp, err := a.b.cl.DeleteGroups(rctx, &kgo.DeleteGroupsRequest{GroupIDs: []string{sub}})
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	if gerr := resp.Errors[sub]; gerr != nil && !errors.Is(gerr, kgo.GroupIdNotFound) {
		return a.failSub(topic, sub, gerr)
	}
	return nil
}

// positionedElsewhere reports whether the group has a position on a topic
// other than name.
func positionedElsewhere(positions map[string][]int, name string) bool {
	for t := range positions {
		if t != name {
			return true
		}
	}
	return false
}

// deleteTopicOffsets deletes the group's committed offsets on the partitions
// of name it has them for, and nothing else of the group.
func (a *admin) deleteTopicOffsets(ctx context.Context, topic, sub, name string, parts []int) error {
	if len(parts) == 0 {
		// The group has no position on this topic, so the subscription is
		// already gone.
		return nil
	}
	supported, err := a.b.hasOffsetDelete(ctx)
	switch {
	case err != nil:
		return a.failSub(topic, sub, err)
	case !supported:
		return errNoOffsetDelete
	}
	_, err = retryCoordinator(ctx, func() (*kgo.OffsetDeleteResponse, error) {
		resp, err := a.b.cl.OffsetDelete(ctx, &kgo.OffsetDeleteRequest{
			GroupID: sub,
			Topics:  map[string][]int{name: parts},
		})
		if err != nil {
			return resp, err
		}
		if resp.Error != nil {
			return resp, resp.Error
		}
		for _, p := range resp.Topics[name] {
			if p.Error != nil {
				return resp, p.Error
			}
		}
		return resp, nil
	})
	if err != nil && !errors.Is(err, kgo.GroupIdNotFound) {
		return a.failSub(topic, sub, err)
	}
	return nil
}

// hasOffsetDelete reports whether the cluster's broker lists OffsetDelete,
// which Kafka added in 2.4.
//
// It is asked because kafka-go does not check a request against the versions
// a broker advertised: it sends version zero of a key the broker does not
// know, and such a broker closes the connection, which reads as a link
// failure worth retrying rather than as a feature the cluster lacks.
//
// It asks whichever broker kafka-go sends ApiVersions to, not the group's
// coordinator, so in a cluster part-way through an upgrade from a version
// before 2.4 the answer can be yes while the coordinator lacks OffsetDelete.
// The deletion then fails as that dropped connection, CodeUnavailable and
// retryable, and still deletes nothing.
func (b *Backend) hasOffsetDelete(ctx context.Context) (bool, error) {
	resp, err := b.cl.ApiVersions(ctx, &kgo.ApiVersionsRequest{})
	if err != nil {
		return false, err
	}
	if resp.Error != nil {
		return false, resp.Error
	}
	for _, k := range resp.ApiKeys {
		if k.ApiKey == int(protocol.OffsetDelete) {
			return true, nil
		}
	}
	return false, nil
}

// groupPositions lists, by topic, the partitions a group has a committed
// offset for. A group that does not exist has none.
func (b *Backend) groupPositions(ctx context.Context, group string) (map[string][]int, error) {
	// No topics asks for every topic the group has committed to.
	resp, err := retryCoordinator(ctx, func() (*kgo.OffsetFetchResponse, error) {
		r, err := b.cl.OffsetFetch(ctx, &kgo.OffsetFetchRequest{GroupID: group})
		if err == nil && r != nil && r.Error != nil {
			err = r.Error
		}
		return r, err
	})
	switch {
	case errors.Is(err, kgo.GroupIdNotFound):
		return map[string][]int{}, nil
	case err != nil:
		return nil, err
	}
	out := make(map[string][]int, len(resp.Topics))
	for t, parts := range resp.Topics {
		for _, p := range parts {
			if p.Error == nil && p.CommittedOffset >= 0 {
				out[t] = append(out[t], p.Partition)
			}
		}
	}
	return out, nil
}

// Purge discards everything a subscription has not consumed.
//
// On Kafka that is not a deletion: the records stay in the log for whoever
// else is reading it, and what moves is this group's position. Purge commits
// the end offsets for the group, so the subscription resumes at the end and
// the backlog is skipped. The driver page says so, because a caller coming
// from a queue broker expects Purge to destroy messages.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	name, err := Topic(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return err
	}
	if err := CheckGroup(sub); err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	parts, err := a.b.partitionsOf(rctx, name)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	if len(parts) == 0 {
		return nil
	}
	bounds, err := a.b.logBounds(rctx, name, parts)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	commits := make([]kgo.OffsetCommit, 0, len(bounds))
	for _, p := range parts {
		lb, ok := bounds[p]
		if !ok {
			continue
		}
		commits = append(commits, kgo.OffsetCommit{Partition: p, Offset: lb.last})
	}
	if len(commits) == 0 {
		return nil
	}
	if err := a.b.commitForGroup(rctx, sub, name, commits); err != nil {
		return a.failSub(topic, sub, err)
	}
	return nil
}

// Stats reports a subscription's depth, in-flight count and lag.
//
// Two of the three are the same number on Kafka and the third does not exist,
// which is why each is a Limit: the lag IS the depth — how many records the
// group has not consumed — and there is no in-flight count at the broker,
// because Kafka does not know which delivered records a consumer is still
// working on. Saying "unknown" is what the specification requires instead of a zero.
//
// A consumer group spans topics, so a subscription name can have a position
// on one topic and none on another, and for a topic it has no position on
// every figure is unknown.
//
// It is two round trips where franz-go's kadm offers one `Lag` call: kafka-go
// has no lag helper, so the end offsets and the committed offsets are fetched
// and subtracted here.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	name, err := Topic(a.b.cfg.TopicPrefix, topic)
	if err != nil {
		return mq.QueueStats{}, err
	}
	if err := CheckGroup(sub); err != nil {
		return mq.QueueStats{}, err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	parts, err := a.b.partitionsOf(rctx, name)
	if err != nil {
		return mq.QueueStats{}, a.failSub(topic, sub, err)
	}
	if len(parts) == 0 {
		return mq.QueueStats{}, nil
	}
	bounds, err := a.b.logBounds(rctx, name, parts)
	if err != nil {
		return mq.QueueStats{}, a.failSub(topic, sub, err)
	}
	committed, err := a.b.committedOffsets(rctx, sub, name, parts)
	if err != nil {
		return mq.QueueStats{}, a.failSub(topic, sub, err)
	}
	var total int64
	seen, positioned := false, false
	for _, p := range parts {
		lb, ok := bounds[p]
		if !ok {
			continue
		}
		at, has := committed[p]
		switch {
		case !has || at < 0:
			// The group has no position for this partition, so it has
			// consumed nothing of it: the whole log is its backlog.
			at = lb.first
		default:
			positioned = true
		}
		seen = true
		if lag := lb.last - at; lag > 0 {
			total += lag
		}
	}
	if !seen || !positioned {
		// A group with no position on any partition of the topic has never
		// consumed it, and the log is not what it would consume either: one
		// opened at the default start begins at the end. Every figure is
		// unknown, as on the franz-go driver.
		return mq.QueueStats{}, nil
	}
	return mq.QueueStats{
		Depth: mq.Limit[int64]{Value: total, Known: true},
		Lag:   mq.Limit[int64]{Value: total, Known: true},
		// InFlight stays unknown: the broker does not know how many delivered
		// records a consumer has not settled.
	}, nil
}

// fail maps an administrative failure.
func (a *admin) fail(topic string, err error) error {
	return a.b.errs.Map(mq.OpAdmin, topic, "", err)
}

// failSub maps an administrative failure that names a subscription.
func (a *admin) failSub(topic, sub string, err error) error {
	return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
}

// logBound is one partition's log range.
type logBound struct{ first, last int64 }

// partitionsOf lists a topic's partition ids, or nothing when the topic does
// not exist.
func (b *Backend) partitionsOf(ctx context.Context, name string) ([]int, error) {
	resp, err := b.cl.Metadata(ctx, &kgo.MetadataRequest{Topics: []string{name}})
	if err != nil {
		return nil, err
	}
	for _, t := range resp.Topics {
		if t.Name != name {
			continue
		}
		if t.Error != nil {
			if errors.Is(t.Error, kgo.UnknownTopicOrPartition) {
				return nil, nil
			}
			return nil, t.Error
		}
		parts := make([]int, 0, len(t.Partitions))
		// Indexed rather than ranged by value: a kafka-go Partition carries
		// its leader, its replicas and its isr, which is 160 bytes a
		// partition and nothing this loop reads.
		for i := range t.Partitions {
			parts = append(parts, t.Partitions[i].ID)
		}
		return parts, nil
	}
	return nil, nil
}

// logBounds reads the first and last offset of every named partition.
//
// The two are asked for in ONE call and the timestamp lookup in another, which
// is not fussiness: kafka-go merges a ListOffsets response by matching the
// RESPONSE's timestamp against the FirstOffset and LastOffset sentinels, and a
// timestamp query that matches no record answers timestamp -1 — which is the
// LastOffset sentinel. Asking for both in one request therefore lets a missing
// timestamp overwrite the partition's real end offset with -1.
func (b *Backend) logBounds(ctx context.Context, name string, parts []int) (map[int]logBound, error) {
	reqs := make([]kgo.OffsetRequest, 0, 2*len(parts))
	for _, p := range parts {
		reqs = append(reqs, kgo.FirstOffsetOf(p), kgo.LastOffsetOf(p))
	}
	resp, err := b.cl.ListOffsets(ctx, &kgo.ListOffsetsRequest{
		Topics: map[string][]kgo.OffsetRequest{name: reqs},
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int]logBound, len(parts))
	for _, po := range resp.Topics[name] {
		if po.Error != nil {
			// A partition whose bounds the broker would not give is a
			// failure and not an absence: dropping it quietly hands the
			// caller a partial answer it cannot tell from a whole one.
			return nil, po.Error
		}
		out[po.Partition] = logBound{first: po.FirstOffset, last: po.LastOffset}
	}
	return out, nil
}

// offsetsAt reads the first offset at or after t for every named partition.
func (b *Backend) offsetsAt(ctx context.Context, name string, parts []int, t time.Time) (map[int]int64, error) {
	reqs := make([]kgo.OffsetRequest, 0, len(parts))
	for _, p := range parts {
		reqs = append(reqs, kgo.TimeOffsetOf(p, t))
	}
	resp, err := b.cl.ListOffsets(ctx, &kgo.ListOffsetsRequest{
		Topics: map[string][]kgo.OffsetRequest{name: reqs},
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int]int64, len(parts))
	for _, po := range resp.Topics[name] {
		if po.Error != nil {
			continue
		}
		// kafka-go keys the answer by offset, so the offset is the map's key
		// and there is at most one: a ListOffsets by timestamp returns the
		// first record at or after it.
		best := int64(-1)
		for off := range po.Offsets {
			if off >= 0 && (best < 0 || off < best) {
				best = off
			}
		}
		if best >= 0 {
			out[po.Partition] = best
		}
	}
	return out, nil
}

// committedOffsets reads a group's committed position for every named
// partition.
func (b *Backend) committedOffsets(ctx context.Context, group, name string, parts []int) (map[int]int64, error) {
	resp, err := retryCoordinator(ctx, func() (*kgo.OffsetFetchResponse, error) {
		r, err := b.cl.OffsetFetch(ctx, &kgo.OffsetFetchRequest{
			GroupID: group,
			Topics:  map[string][]int{name: parts},
		})
		if err == nil && r != nil && r.Error != nil {
			err = r.Error
		}
		return r, err
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int]int64, len(parts))
	for _, p := range resp.Topics[name] {
		if p.Error != nil {
			continue
		}
		out[p.Partition] = p.CommittedOffset
	}
	return out, nil
}

// commitForGroup commits offsets on behalf of a group with no live member.
//
// GenerationID -1 with an empty member id is the protocol's "simple consumer"
// commit, which is how `kafka-consumer-groups --reset-offsets` moves a group's
// position and the only way to give a group a position before it has ever had
// a member. A coordinator refuses it for a group that DOES have live members,
// which is the correct answer and the caller's to see.
func (b *Backend) commitForGroup(ctx context.Context, group, name string, commits []kgo.OffsetCommit) error {
	_, err := retryCoordinator(ctx, func() (*kgo.OffsetCommitResponse, error) {
		resp, err := b.cl.OffsetCommit(ctx, &kgo.OffsetCommitRequest{
			GroupID:      group,
			GenerationID: -1,
			MemberID:     "",
			Topics:       map[string][]kgo.OffsetCommit{name: commits},
		})
		if err != nil {
			return resp, err
		}
		for _, p := range resp.Topics[name] {
			if p.Error != nil {
				return resp, p.Error
			}
		}
		return resp, nil
	})
	return err
}

// ensureGroupPosition commits the resolved start position for every partition
// of the topic that the group has no committed offset for.
//
// It is what makes a Kafka subscription durable from the moment it exists
// rather than from the moment it first consumes something. Without it, a
// consumer that was opened, published to and closed without consuming anything
// would resume at the END of the topic and skip everything published while it
// was gone — which is what FeatureDurableSubscriptions promises does not
// happen.
//
// On THIS client it does a second job the franz-go driver does not need it
// for: kafka-go's group start offset is FirstOffset or LastOffset and nothing
// else, so a start position by TIME or by SEQUENCE has no expression in the
// group configuration at all and this commit is the only place it can be
// honoured.
//
// It is deliberately soft about failure. Committing on behalf of a group is
// only possible while the group has no live member, and the coordinator
// refuses it otherwise. A cluster whose ACLs forbid the commit is reported
// through health and still works.
//
// What it returns is the position it RESOLVED for each partition the group had
// no committed offset for, whether or not the commit landed, and the source
// starts there when the commit did not (see [source.readPartition]). A refused
// commit used to be taken to mean "the group is live, so it already has a
// position", which does not follow: a Kafka group spans topics and a portable
// subscription does not, so the same subscription name on a second topic joins
// a live group that has never committed anything for that topic. The partition
// then started at LastOffset resolved at ASSIGNMENT, skipping everything
// published while the member joined -- and a start by time or by sequence,
// which this commit is the only expression of, was lost outright.
func (b *Backend) ensureGroupPosition(ctx context.Context, name, group string, start mq.StartPosition) ([]kgo.OffsetCommit, error) {
	parts, err := b.partitionsOf(ctx, name)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, name), group, err)
	}
	if len(parts) == 0 {
		// The topic does not exist yet. There is nothing to resolve a start
		// position against, and the group's own StartOffset covers the case
		// when it appears.
		return nil, nil
	}
	committed, err := b.committedOffsets(ctx, group, name, parts)
	if err != nil && !errors.Is(err, kgo.GroupIdNotFound) {
		b.health.Degraded(b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, name), group, err))
		return nil, nil
	}
	want := make([]int, 0, len(parts))
	for _, p := range parts {
		if at, has := committed[p]; has && at >= 0 {
			continue
		}
		want = append(want, p)
	}
	if len(want) == 0 {
		return nil, nil
	}
	commits, err := b.startOffsets(ctx, name, want, start)
	if err != nil {
		b.health.Degraded(err)
		return nil, nil
	}
	if len(commits) == 0 {
		return nil, nil
	}
	if cerr := b.commitForGroup(ctx, group, name, commits); cerr != nil {
		// A live group refuses the commit. The subscription still starts
		// where it was asked to, because the caller keeps the resolved
		// position; what is lost is the DURABILITY of that position until
		// the member first commits.
		b.health.Degraded(b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, name), group, cerr))
	}
	return commits, nil
}

// startOffsets resolves the start position into one offset per partition.
//
// Every answer is CLAMPED into the log's own range, which is not a
// convenience: an offset outside the range is a fetch error the broker answers
// with OFFSET_OUT_OF_RANGE, and Kafka's own auto.offset.reset exists precisely
// because an out-of-range position has to resolve to something. A sequence
// beyond the end resolves to the end, one below the start resolves to the
// start, and a time no record is at or after resolves to the end — which is
// what makes StartAtSequence and StartAt mean something on a topic that is
// empty or has been compacted.
func (b *Backend) startOffsets(ctx context.Context, name string, parts []int, start mq.StartPosition) ([]kgo.OffsetCommit, error) {
	bounds, err := b.logBounds(ctx, name, parts)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, name), "", err)
	}
	var timed map[int]int64
	if start.Kind() == mq.StartKindTime {
		if timed, err = b.offsetsAt(ctx, name, parts, start.Time()); err != nil {
			return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, name), "", err)
		}
	}
	out := make([]kgo.OffsetCommit, 0, len(parts))
	for _, p := range parts {
		lb, ok := bounds[p]
		if !ok || lb.first < 0 || lb.last < 0 {
			continue
		}
		at := lb.last
		switch start.Kind() {
		case mq.StartKindEarliest:
			at = lb.first
		case mq.StartKindSequence:
			// The specification bounds a sequence to the log, and clamp below enforces it.
			at = int64(start.Sequence())
		case mq.StartKindTime:
			if t, has := timed[p]; has {
				at = t
			}
		case mq.StartKindLatest, mq.StartKindUnset:
		default:
		}
		out = append(out, kgo.OffsetCommit{Partition: p, Offset: clamp(at, lb.first, lb.last)})
	}
	return out, nil
}

// clamp holds an offset inside the log's own range.
func clamp(at, lo, hi int64) int64 {
	switch {
	case at < lo:
		return lo
	case at > hi:
		return hi
	default:
		return at
	}
}

// retryCoordinator runs a group-coordinator round trip, retrying while the
// coordinator is not ready yet.
//
// A brand-new group's first OffsetFetch or OffsetCommit routinely arrives
// before the coordinator has loaded its partition of __consumer_offsets, and a
// real broker answers COORDINATOR_LOAD_IN_PROGRESS or
// COORDINATOR_NOT_AVAILABLE rather than doing the work. Treating that as a
// failure means the resolved start position is never committed, and the group
// then falls back to its StartOffset — which resolves "latest" at ASSIGNMENT
// time and therefore skips anything published while the member was joining.
// That is a message the subscription was asked to receive and did not, and it
// is what made the franz-go driver envelope scenario flake under a
// full suite's load while passing on its own.
//
// The bound is the context's, so a caller's own deadline ends it.
func retryCoordinator[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	const backoff = 100 * time.Millisecond
	var (
		resp T
		err  error
	)
	for {
		resp, err = fn()
		if err == nil || !coordinatorNotReady(err) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return resp, err
		case <-time.After(backoff):
		}
	}
}

// coordinatorNotReady reports whether err is the group coordinator saying "not
// yet", which is a condition to wait through rather than a failure.
func coordinatorNotReady(err error) bool {
	return errors.Is(err, kgo.GroupLoadInProgress) ||
		errors.Is(err, kgo.GroupCoordinatorNotAvailable) ||
		errors.Is(err, kgo.NotCoordinatorForGroup) ||
		errors.Is(err, kgo.RebalanceInProgress)
}
