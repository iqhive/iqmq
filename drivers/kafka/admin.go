package kafka

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

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
	// kadm's CreateTopic returns the per-topic error as the function's own
	// error as well, so the idempotence has to be applied to both: a
	// TOPIC_ALREADY_EXISTS reaching the caller is the difference between
	// "ensure" and "create".
	resp, err := a.b.adm.CreateTopic(rctx, a.b.cfg.Partitions, a.b.cfg.ReplicationFactor, nil, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return a.fail(spec.Name, err)
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
	resps, err := a.b.adm.DeleteTopics(rctx, name)
	if err != nil {
		return a.fail(topic, err)
	}
	for _, r := range resps {
		if r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			return a.fail(topic, r.Err)
		}
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
		details, err := a.b.adm.ListTopics(rctx)
		if err != nil {
			yield("", a.fail("", err))
			return
		}
		for name, d := range details {
			if d.IsInternal {
				continue
			}
			if d.Err != nil {
				if !yield("", a.fail(TopicOf(a.b.cfg.TopicPrefix, name), d.Err)) {
					return
				}
				continue
			}
			if a.b.cfg.TopicPrefix != "" && len(name) <= len(a.b.cfg.TopicPrefix) {
				continue
			}
			if !yield(TopicOf(a.b.cfg.TopicPrefix, name), nil) {
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
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	if err := CheckGroup(sub); err != nil {
		return err
	}
	rctx, cancel := a.b.bound(ctx)
	defer cancel()
	resps, err := a.b.adm.DeleteGroups(rctx, sub)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	for _, r := range resps {
		if r.Err != nil && !errors.Is(r.Err, kerr.GroupIDNotFound) {
			return a.failSub(topic, sub, r.Err)
		}
	}
	return nil
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
	ends, err := a.b.adm.ListEndOffsets(rctx, name)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	offsets := make(kadm.Offsets)
	for _, o := range ends[name] {
		if o.Err != nil {
			return a.failSub(topic, sub, o.Err)
		}
		offsets.Add(kadm.Offset{
			Topic: name, Partition: o.Partition,
			At: o.Offset, LeaderEpoch: o.LeaderEpoch,
		})
	}
	if len(offsets) == 0 {
		return nil
	}
	resp, err := a.b.adm.CommitOffsets(rctx, sub, offsets)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	if err := resp.Error(); err != nil {
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
// The figures are the group's on this topic alone, and all three are unknown
// for a topic the group has no position on.
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
	lags, err := a.b.adm.Lag(rctx, sub)
	if err != nil {
		return mq.QueueStats{}, a.failSub(topic, sub, err)
	}
	gl, ok := lags[sub]
	if !ok {
		return mq.QueueStats{}, a.failSub(topic, sub, kerr.GroupIDNotFound)
	}
	var total int64
	seen := false
	// The lag map is keyed by topic and then by partition, and holds every
	// topic the group has a position on: a group spans topics, and a
	// subscription name used on two of them is one group. Only this
	// topic's partitions are its backlog. Each value is a large struct;
	// iterating by key and reading only the two fields this needs keeps the
	// loop from copying 256 bytes per partition.
	parts := gl.Lag[name]
	for p := range parts {
		lag, lerr := parts[p].Lag, parts[p].Err
		if lerr != nil {
			continue
		}
		if lag > 0 {
			total += lag
		}
		seen = true
	}
	if !seen {
		return mq.QueueStats{}, nil
	}
	return mq.QueueStats{
		Depth: mq.Limit[int64]{Value: total, Known: true},
		Lag:   mq.Limit[int64]{Value: total, Known: true},
		// InFlight stays unknown: the broker does not know how many
		// delivered records a consumer has not settled.
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

// ensureGroupPosition commits the resolved start position for every partition
// of the topic that the group has no committed offset for.
//
// It is what makes a Kafka subscription durable from the moment it exists
// rather than from the moment it first consumes something. Without it, a
// consumer that was opened, published to and closed without consuming
// anything would resume at the END of the topic and skip everything published
// while it was gone — which is what FeatureDurableSubscriptions promises does
// not happen.
//
// It is deliberately soft about failure. Committing on behalf of a group is
// only possible while the group has no live member, and the coordinator
// refuses it otherwise. A cluster whose ACLs forbid the commit is reported
// through health and still works.
//
// What it returns is the position it RESOLVED for each partition the group had
// no committed offset for, whether or not the commit landed, and the source
// starts there when the commit did not (see [source.adjustStart]). That half
// used to be missing because a refused commit was taken to mean "the group is
// live, so it already has a position". It does not follow: a Kafka group spans
// topics and a portable subscription does not, so the SAME subscription name
// on a second topic joins a live group that has never committed anything for
// that topic. The group then started at the end of the log as of its
// ASSIGNMENT, and everything published between Consumer returning and the
// rebalance completing was skipped -- the loss the specification fixed for a
// coordinator that was not ready, reached again through a live group. It is
// the conformance(kafka) failure of nightly run 35678720257, where every
// scenario's subscription is called "conformance".
func (b *Backend) ensureGroupPosition(ctx context.Context, topic, group string, start mq.StartPosition) ([]kadm.Offset, error) {
	details, err := b.adm.ListTopics(ctx, topic)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), group, err)
	}
	td, ok := details[topic]
	if !ok || td.Err != nil || len(td.Partitions) == 0 {
		// The topic does not exist yet. There is nothing to resolve a
		// start position against, and kgo.ConsumeResetOffset covers the
		// case when it appears.
		return nil, nil
	}
	committed, err := b.retryCoordinator(ctx, func() (kadm.OffsetResponses, error) {
		return b.adm.FetchOffsets(ctx, group)
	})
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		b.health.Degraded(b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), group, err))
		return nil, nil
	}
	want := make([]int32, 0, len(td.Partitions))
	for _, pd := range td.Partitions {
		if o, has := committed.Lookup(topic, pd.Partition); has && o.Err == nil && o.At >= 0 {
			continue
		}
		want = append(want, pd.Partition)
	}
	if len(want) == 0 {
		return nil, nil
	}
	offsets, err := b.startOffsets(ctx, topic, want, start)
	if err != nil {
		b.health.Degraded(err)
		return nil, nil
	}
	if len(offsets) == 0 {
		return nil, nil
	}
	_, cerr := b.retryCoordinator(ctx, func() (kadm.OffsetResponses, error) {
		resp, err := b.adm.CommitOffsets(ctx, group, offsets)
		if err == nil {
			err = resp.Error()
		}
		return resp, err
	})
	if cerr != nil {
		// A live group refuses the commit. The subscription still
		// starts where it was asked to, because the caller keeps the
		// resolved position; what is lost is the DURABILITY of that
		// position until the member first commits.
		b.health.Degraded(b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), group, cerr))
	}
	return offsets.Sorted(), nil
}

// retryCoordinator runs a group-coordinator round trip, retrying while the
// coordinator is not ready yet.
//
// A brand-new group's first OffsetFetch or OffsetCommit routinely arrives
// before the coordinator has loaded its partition of __consumer_offsets, and a
// real broker answers COORDINATOR_LOAD_IN_PROGRESS or COORDINATOR_NOT_AVAILABLE
// rather than doing the work. Treating that as a failure means the resolved
// start position is never committed, and the group then falls back to
// ConsumeResetOffset — which resolves "latest" at ASSIGNMENT time and therefore
// skips anything published while the member was joining. That is a message the
// subscription was asked to receive and did not, and it is what made the
// envelope scenario flake under a full suite's load while passing on its own.
//
// The bound is the context's, so a caller's own deadline ends it.
func (b *Backend) retryCoordinator(ctx context.Context, fn func() (kadm.OffsetResponses, error)) (kadm.OffsetResponses, error) {
	const backoff = 100 * time.Millisecond
	var (
		resp kadm.OffsetResponses
		err  error
	)
	for attempt := 0; ; attempt++ {
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
	return errors.Is(err, kerr.CoordinatorLoadInProgress) ||
		errors.Is(err, kerr.CoordinatorNotAvailable) ||
		errors.Is(err, kerr.NotCoordinator) ||
		errors.Is(err, kerr.RebalanceInProgress)
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
func (b *Backend) startOffsets(ctx context.Context, topic string, parts []int32, start mq.StartPosition) (kadm.Offsets, error) {
	starts, err := b.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), "", err)
	}
	ends, err := b.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), "", err)
	}
	var timed kadm.ListedOffsets
	if start.Kind() == mq.StartKindTime {
		if timed, err = b.adm.ListOffsetsAfterMilli(ctx, start.Time().UnixMilli(), topic); err != nil {
			return nil, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), "", err)
		}
	}
	out := make(kadm.Offsets)
	for _, p := range parts {
		lo, okLo := starts.Lookup(topic, p)
		hi, okHi := ends.Lookup(topic, p)
		if !okLo || !okHi || lo.Err != nil || hi.Err != nil {
			continue
		}
		at := hi.Offset
		epoch := hi.LeaderEpoch
		switch start.Kind() {
		case mq.StartKindEarliest:
			at, epoch = lo.Offset, lo.LeaderEpoch
		case mq.StartKindSequence:
			at = int64(start.Sequence())
		case mq.StartKindTime:
			if t, ok := timed.Lookup(topic, p); ok && t.Err == nil && t.Offset >= 0 {
				at, epoch = t.Offset, t.LeaderEpoch
			}
		case mq.StartKindLatest, mq.StartKindUnset:
		default:
		}
		at = clamp(at, lo.Offset, hi.Offset)
		out.Add(kadm.Offset{Topic: topic, Partition: p, At: at, LeaderEpoch: epoch})
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
