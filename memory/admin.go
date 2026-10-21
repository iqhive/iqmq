package memory

import (
	"context"
	"iter"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
)

// admin is the topology surface of the API.
//
// Four of the seven operations are the engine's own and three are not, and
// the three say so rather than pretending: the engine has no way to remove a
// topic or to discard a subscription's backlog, so DeleteTopic answers
// ErrUnsupported and Purge is an emulation that names what it cannot reach.
type admin struct{ b *Broker }

var _ mq.Admin = (*admin)(nil)

// EnsureTopic creates the topic if it does not exist.
//
// It is idempotent, which is what Ensure means: a second call with the same
// name returns nil and changes nothing, including the topology the topic was
// created with. The topology is the broker's DefaultTopology — TopicSpec
// carries only the portable intersection, which is the name, and everything
// else a broker wants is named by the driver's own typed configuration.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.b.eng.Closed() {
		return mq.ErrClosed
	}
	if err := mq.ValidateTopic(spec.Name); err != nil {
		return err
	}
	_, err := a.b.eng.CreateTopic(spec.Name, a.b.topo)
	return err
}

// DeleteTopic would remove a topic and everything in it.
//
// The engine's registry has no removal: a topic exists for the life of the
// broker, and a caller that wants a fresh one builds a fresh broker, which in
// process costs nothing. It is ErrUnsupported rather than a partial delete —
// closing every subscription and leaving the topic in the registry would be
// neither a delete nor a no-op.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.b.eng.Closed() {
		return mq.ErrClosed
	}
	if _, err := a.b.eng.Topic(topic); err != nil {
		return err
	}
	return mq.ErrUnsupported
}

// ListTopics iterates the topics the broker holds. The iteration
// stops at the first error it yields, and this one yields none.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := ctx.Err(); err != nil {
			yield("", err)
			return
		}
		if a.b.eng.Closed() {
			yield("", mq.ErrClosed)
			return
		}
		stopped := false
		a.b.eng.Topics(func(t *engine.Topic[*mq.Message]) bool {
			if !yield(t.Name(), nil) {
				stopped = true
				return false
			}
			return true
		})
		_ = stopped
	}
}

// EnsureSubscription creates the subscription if it does not exist.
//
// It is idempotent, and it is what makes a subscription accumulate records
// before any consumer attaches: the engine's queue and topic topologies
// retain nothing, so a subscription that does not exist when a publication is
// made does not receive it.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.b.eng.Closed() {
		return mq.ErrClosed
	}
	if spec.Name == "" {
		return errNoSubscriptionName
	}
	t, err := a.b.topicFor(spec.Topic)
	if err != nil {
		return err
	}
	_, err = t.Subscribe(spec.Name, engine.SubscriptionConfig{})
	return err
}

// DeleteSubscription removes a subscription.
//
// It is destructive and explicit: the subscription is detached from its topic
// and every record it still holds is dropped, releasing each payload exactly
// once. A source still open on it answers ErrClosed afterwards.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.b.eng.Closed() {
		return mq.ErrClosed
	}
	t, err := a.b.eng.Topic(topic)
	if err != nil {
		return err
	}
	s, err := t.Subscription(sub)
	if err != nil {
		return err
	}
	return s.Close()
}

// Purge discards what a subscription has not consumed.
//
// The engine discards it directly (engine.Subscription.Purge), across every
// lane: no consumer is attached for it, so a sticky subscription is not
// rebalanced and no live consumer loses a lease. What it therefore does NOT
// discard is a record a consumer holds in flight — that record is not the
// subscription's to throw away while a lease is outstanding — nor one still
// waiting for its due time, which is not yet deliverable.
//
// The records it discards are counted in Snapshot.Acked, for the same reason
// a dropped delivery is: the engine's terminal settlement is the only one it
// has.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.b.eng.Closed() {
		return mq.ErrClosed
	}
	t, err := a.b.eng.Topic(topic)
	if err != nil {
		return err
	}
	s, err := t.Subscription(sub)
	if err != nil {
		return err
	}
	s.Purge()
	return nil
}

// errNoSubscriptionName is what an unnamed subscription is refused with.
var errNoSubscriptionName = errcode.New(errcode.InvalidConfig,
	"iqmq/memory: SubscriptionSpec.Name is required")

// Stats reports a subscription's depth, in-flight count and lag.
//
// Each figure is a Limit, so a broker that cannot report one says so instead
// of reporting zero. This one can report all three, and what it means by them
// is written down: depth is what is waiting, in-flight is what has been
// handed out and not settled, and lag is the backlog a consumer has not
// reached — which for this broker is the depth, because a subscription's
// records are the whole of what it has not consumed.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return mq.QueueStats{}, err
	}
	snap, err := a.b.eng.Inspect(topic, sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	depth := int64(snap.Ready + snap.Delayed)
	return mq.QueueStats{
		Depth:    mq.Limit[int64]{Value: depth, Known: true},
		InFlight: mq.Limit[int64]{Value: int64(snap.InFlight), Known: true},
		Lag:      mq.Limit[int64]{Value: depth, Known: true},
	}, nil
}
