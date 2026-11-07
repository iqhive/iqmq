package kafka

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
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
// topics" does not mean them. So, under [Config.TopicPrefix], is every topic
// outside the prefix, and so is a failure reported for one: the prefix is how
// two deployments share a cluster, and the other deployment's topics are not
// this one's. Stripping the prefix alone left such a name as it was, and the
// listing reported it as this deployment's own topic.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		rctx, cancel := a.b.bound(ctx)
		defer cancel()
		details, err := a.b.adm.ListTopics(rctx)
		if err != nil {
			yield("", a.fail("", err))
			return
		}
		prefix := a.b.cfg.TopicPrefix
		for name, d := range details {
			if d.IsInternal {
				continue
			}
			if prefix != "" && (len(name) <= len(prefix) || !strings.HasPrefix(name, prefix)) {
				continue
			}
			if d.Err != nil {
				if !yield("", a.fail(TopicOf(prefix, name), d.Err)) {
					return
				}
				continue
			}
			if !yield(TopicOf(prefix, name), nil) {
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
	path := &healthPath{}
	_, err = a.b.ensureGroupPosition(rctx, path, name, spec.Name, mq.StartLatest())
	a.b.subscriptionFailed(spec.Name, path)
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
// topic's position too: that subscription restarted at its reset offset. So
// the group is deleted only when its committed offsets name no topic but this
// one. A group with a position on another topic loses this topic's offsets
// alone (OffsetDelete, Kafka 2.4 and later). The coordinator refuses that, as
// a conflict, while a live member consumes this topic, and allows it while
// the live members consume only other topics; a broker without OffsetDelete
// refuses it as unsupported. The topic is validated as every other call's is,
// because it now names what is deleted.
//
// The offsets are read and then deleted in two round trips. A consumer on
// another topic that joins the group in between makes it non-empty, and the
// coordinator then refuses to delete it.
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
	offs, err := a.b.adm.FetchOffsets(rctx, sub)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return a.failSub(topic, sub, err)
	}
	if elsewhere(offs, name) {
		if err := a.deleteTopicOffsets(rctx, topic, sub, name, offs); err != nil {
			return err
		}
		a.b.subscriptionWorks(sub) // deleted: its start position is nobody's concern
		return nil
	}
	resps, err := a.b.adm.DeleteGroups(rctx, sub)
	if err != nil {
		return a.failSub(topic, sub, err)
	}
	for _, r := range resps {
		if r.Err != nil && !errors.Is(r.Err, kerr.GroupIDNotFound) {
			return a.failSub(topic, sub, r.Err)
		}
	}
	a.b.subscriptionWorks(sub) // deleted: its start position is nobody's concern
	return nil
}

// elsewhere reports whether the group's offsets name a topic other than name.
func elsewhere(offs kadm.OffsetResponses, name string) bool {
	for t := range offs {
		if t != name {
			return true
		}
	}
	return false
}

// deleteTopicOffsets deletes the group's committed offsets on name alone.
func (a *admin) deleteTopicOffsets(ctx context.Context, topic, sub, name string, offs kadm.OffsetResponses) error {
	parts := offs[name]
	if len(parts) == 0 {
		return nil
	}
	set := make(kadm.TopicsSet)
	for p := range parts {
		set.Add(name, p)
	}
	resps, err := a.b.adm.DeleteOffsets(ctx, sub, set)
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			return nil
		}
		return a.failSub(topic, sub, err)
	}
	if err := resps.Error(); err != nil {
		return a.failSub(topic, sub, err)
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
// for a topic the group has no position on, or for a group that does not
// exist. A round trip that failed is an error and never unknown figures:
// kadm's Lag reports a failed describe or offset fetch in the group's own
// entry and a failed offset listing in the partition's, and returns no error
// of its own, so a Stats that read neither answered (unknown, nil) for a
// coordinator that could not answer, or left a partition out of a total it
// still reported as known.
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
	if err := gl.Error(); err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			return mq.QueueStats{}, nil
		}
		return mq.QueueStats{}, a.failSub(topic, sub, err)
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
			return mq.QueueStats{}, a.failSub(topic, sub, lerr)
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
// through health and still works. The report is on path ([paths]): the
// opening source's commits, whose next commit round clears it, or the
// subscription's own for EnsureSubscription ([Backend.subscriptionFailed]),
// which a clean run of this for the group, by EnsureSubscription or a
// consumer opening, clears, as does a commit by any consumer of the group.
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
//
// A position it could not read is the same loss by another road, so a read
// that fails is not taken for an answer. The group's committed positions
// that could not be read leave the start position to be read for every
// partition, and kept but not committed: a commit could move a position the
// group already has, and the source applies a kept position only to a
// partition that turns out to have none. A partition whose start could not
// be read is reported on path rather than left out without a word, and
// starts at the reset offset when the group assigns it (see
// [Backend.startOffsets] for what is asked again first).
func (b *Backend) ensureGroupPosition(ctx context.Context, path *healthPath, topic, group string, start mq.StartPosition) ([]kadm.Offset, error) {
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
	// One read of every partition's start comes first, asked once and
	// reported nowhere. The group's read can wait out the whole bound for a
	// coordinator that is still loading, and a start read made after it ran
	// on an ended context and kept nothing. It is not asked again here, so
	// that a group with a position for every partition opens at once even
	// while a partition cannot be read.
	all := slices.Sorted(maps.Keys(td.Partitions))
	early := make(kadm.Offsets)
	_, _ = b.readStartOffsets(ctx, topic, all, start, early)
	committed, err := b.retryCoordinator(ctx, func() (kadm.OffsetResponses, error) {
		return b.adm.FetchOffsets(ctx, group)
	})
	var unread error
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		unread = b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), group, err)
		committed = nil
	}
	want := make([]int32, 0, len(all))
	for _, p := range all {
		if o, has := committed.Lookup(topic, p); has && o.Err == nil && o.At >= 0 {
			continue
		}
		want = append(want, p)
	}
	if len(want) == 0 {
		b.recovered(path)
		b.subscriptionWorks(group)
		return nil, nil
	}
	offsets, rerr := b.startOffsets(ctx, topic, want, start, early)
	if unread != nil {
		b.failed(path, unread)
	}
	if rerr != nil {
		b.failed(path, rerr)
	}
	if unread != nil || len(offsets) == 0 {
		return offsets.Sorted(), nil
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
		b.failed(path, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), group, cerr))
		return offsets.Sorted(), nil
	}
	if rerr == nil {
		b.recovered(path)
		b.subscriptionWorks(group)
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
// The bound is the context's, so a caller's own deadline ends it, and every
// caller hands it a context with the driver's own bound ([Backend.bound]):
// under a context with no deadline it would wait for ever for a coordinator
// that never finishes loading.
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
//
// What it returns is every partition it resolved, and the failure that left
// the rest unresolved. A partition the broker answered with an error is not
// resolved: it used to be left out without a word, which read as a start that
// needed nothing, and for a start by time a failed lookup resolved to the end
// of the log. A partition early already holds is taken from it, and the rest
// are read. A read that failed in a way asking again can cure ([readAgain])
// is asked again for the partitions still unresolved, every 100 ms until ctx
// ends: right after a topic is created, or while a partition elects a new
// leader, the broker answers LEADER_NOT_AVAILABLE or OFFSET_NOT_AVAILABLE for
// a moment, and a consumer opened then started at its assignment instead.
func (b *Backend) startOffsets(ctx context.Context, topic string, parts []int32, start mq.StartPosition, early kadm.Offsets) (kadm.Offsets, error) {
	const backoff = 100 * time.Millisecond
	out := make(kadm.Offsets)
	unread := make([]int32, 0, len(parts))
	for _, p := range parts {
		if o, ok := early.Lookup(topic, p); ok {
			out.Add(o)
			continue
		}
		unread = append(unread, p)
	}
	parts = unread
	for len(parts) > 0 {
		left, err := b.readStartOffsets(ctx, topic, parts, start, out)
		if err == nil {
			return out, nil
		}
		if !readAgain(err) {
			return out, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), "", err)
		}
		select {
		case <-ctx.Done():
			return out, b.errs.Map(mq.OpOpen, TopicOf(b.cfg.TopicPrefix, topic), "", err)
		case <-time.After(backoff):
		}
		parts = left
	}
	return out, nil
}

// readStartOffsets is one read of the start position of parts. It adds each
// partition it resolves to out, and returns the partitions it did not resolve
// and why: the first failure asking again can cure, or else the first
// failure, so that one partition refused for good does not stop another being
// asked again.
func (b *Backend) readStartOffsets(ctx context.Context, topic string, parts []int32, start mq.StartPosition, out kadm.Offsets) ([]int32, error) {
	starts, err := b.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return parts, err
	}
	ends, err := b.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return parts, err
	}
	var timed kadm.ListedOffsets
	if start.Kind() == mq.StartKindTime {
		if timed, err = b.adm.ListOffsetsAfterMilli(ctx, start.Time().UnixMilli(), topic); err != nil {
			return parts, err
		}
	}
	var (
		left        []int32
		first, cure error
	)
	for _, p := range parts {
		lo, hi := listed(starts, topic, p), listed(ends, topic, p)
		var t kadm.ListedOffset
		if start.Kind() == mq.StartKindTime {
			t = listed(timed, topic, p)
		}
		if perr := cmp.Or(lo.Err, hi.Err, t.Err); perr != nil {
			left = append(left, p)
			first = cmp.Or(first, perr)
			if readAgain(perr) {
				cure = cmp.Or(cure, perr)
			}
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
			if t.Offset >= 0 {
				at, epoch = t.Offset, t.LeaderEpoch
			}
		case mq.StartKindLatest, mq.StartKindUnset:
		default:
		}
		at = clamp(at, lo.Offset, hi.Offset)
		out.Add(kadm.Offset{Topic: topic, Partition: p, At: at, LeaderEpoch: epoch})
	}
	return left, cmp.Or(cure, first)
}

// listed is one partition's answer in an offset listing, with errNotListed
// for a partition the listing does not hold.
func listed(l kadm.ListedOffsets, topic string, p int32) kadm.ListedOffset {
	o, ok := l.Lookup(topic, p)
	if !ok {
		o.Err = errNotListed
	}
	return o
}

// readAgain reports whether a read of the start position that failed may
// succeed if it is asked again: the broker answered with an error Kafka marks
// retriable, such as a partition with no leader yet, it listed nothing for a
// partition yet, or the request failed on its way.
//
// UNKNOWN_SERVER_ERROR is asked again as well, within the open's bound: it is
// the broker's answer for any failure it did not expect, often one that
// passes, and a read can be asked again at no cost but the wait. What the
// caller is told if it lasts the whole bound is unchanged, CodeUnknown and
// not retryable.
func readAgain(err error) bool {
	if kerr.IsRetriable(err) || errors.Is(err, errNotListed) || errors.Is(err, kerr.UnknownServerError) {
		return true
	}
	_, retryable, _ := driverkit.ClassifyNet(err, false)
	return retryable
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
