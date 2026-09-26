package snssqs

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
)

// The source implements the mandatory consumption surface; its settler is the
// inner SQS source's, unchanged.
var _ mq.Source = (*source)(nil)

// source is one open subscription: one SQS queue, consumed by the SQS driver,
// with the portable topic put back.
//
// # What this type deliberately does not do
//
// It does not settle, it does not hold a delivery, it does not keep a lease,
// and it shares nothing with any other source. Those are all one decision:
// **settlement on a fan-out is per queue.** One publication becomes N messages
// in N queues; each has its own receipt handle, its own visibility window and
// its own ApproximateReceiveCount, and nothing about SNS relates them. A
// driver that cached a source per TOPIC, or that keyed settlement bookkeeping
// on the publication, would make one subscriber's acknowledgement settle
// another subscriber's copy — which is a message lost for every subscription
// but the first, and is invisible until somebody counts.
//
// So [Backend.OpenSource] opens a new inner source per (topic, subscription)
// pair with no cache in front of it, and [source.Settler] hands back the inner
// source's own settler rather than anything of this driver's. The fan-out
// isolation that the specification's `fanout/isolation` scenario checks is therefore a
// property of the composition rather than of code here, and
// TestOneSubscriberSettlementLeavesTheOtherCopyAlone is what holds it.
type source struct {
	b     *Backend
	inner mq.Source
	topic string
	sub   string
	queue resolvedSub
	once  sync.Once
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The one thing it does to a delivery is put the portable topic back. The
// inner driver was opened with the QUEUE NAME as its topic — on that driver a
// topic is a queue — so every delivery arrives naming the queue, and an
// application that read [github.com/iqhive/iqmq.DeliveryInfo.Topic] would get
// `orders-eu-billing` where it published to `orders/eu`. The specification makes the
// delivery's topic "the topic the message was published to", which for a
// fan-out is the SNS topic and not the queue it landed in.
//
// DeliveryInfo.Source is left as the inner driver set it — the queue's URL —
// because that is the one field that should say which COPY this is, and on a
// fan-out that is the useful half of the answer. DeliveryInfo.Subscription is
// already right: the inner source was opened with the same subscription name,
// and rewriting a field that is already correct would hide a bug rather than
// fix one.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n, err := s.inner.Fetch(ctx, dst)
	for i := range n {
		dst[i].Info.Topic = s.topic
		if dst[i].Message != nil {
			dst[i].Message.Topic = s.topic
		}
	}
	return n, err
}

// Settler settles this source's deliveries.
//
// It is the INNER source's settler, handed straight back. Wrapping it would
// buy nothing and would cost the optional surfaces: the inner settler
// implements the batched settlement and the lease extension the specification defines as
// optional, and a facade that forwarded only mq.Settler would silently retire
// both (the omission the specification records and driverkit.WrapSettler now
// prevents). Handing back the real value is also what keeps the settlement
// per queue: it can only ever name deliveries from this one source.
func (s *source) Settler() mq.Settler { return s.inner.Settler() }

// Close releases the source. It is idempotent.
func (s *source) Close() error {
	var err error
	s.once.Do(func() {
		err = s.inner.Close()
		s.b.forget(s, nil)
	})
	return err
}

// Queue is the queue this subscription's copies arrive in, for a caller that
// needs to reach it with the SQS driver's own typed administration.
func (s *source) Queue() resolvedSub { return s.queue }
