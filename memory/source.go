package memory

import (
	"context"
	"sync"
	"time"

	"github.com/iqhive/iqmq/wire"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
)

// The source implements the mandatory consumption surface and the optional
// lease extension. It does NOT implement mq.BatchSettler: the engine settles
// one delivery at a time under one lane mutex, so a batch method would be the
// same loop the core already writes, and a driver that offers one makes the
// core skip its own adapter for no saving (the lesson, and the reason this is asserted here rather than left to a reader).
var (
	_ mq.Source           = (*source)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
	_ mq.Settler          = (*settler)(nil)
)

// source is one open subscription: the engine consumer, the claim buffer a
// fetch fills, and the settler that settles what the fetch handed over.
type source struct {
	b     *Broker
	topic *engine.Topic[*mq.Message]
	sub   *engine.Subscription[*mq.Message]
	cons  *engine.Consumer[*mq.Message]
	st    *settler

	// name and subscription are what a delivery reports when the engine has
	// no resolved subject of its own.
	name         string
	subscription string
	// lease is how long a delivery's lease lasts, resolved at open, so that
	// filling DeliveryInfo.LeaseExpires costs an addition rather than a
	// lookup. It is zero when leases never expire.
	lease time.Duration
	meta  metadataMode

	// claims is the buffer the engine's fetch fills, allocated once at open
	// and reused for ever: Fetch fills a caller slice precisely so that
	// a fetch allocates nothing, and this is the other half of that promise.
	// Only one Fetch runs at a time, which is what makes a single
	// buffer safe.
	claims []engine.Claim[*mq.Message]

	once   sync.Once
	closed chan struct{}
}

// newSource builds the source over an attached consumer.
func newSource(
	b *Broker,
	t *engine.Topic[*mq.Message],
	sub *engine.Subscription[*mq.Message],
	cons *engine.Consumer[*mq.Message],
	cfg mq.SourceConfig,
	meta metadataMode,
) *source {
	// The subscription's deadline, not cfg's: a source opened on a
	// subscription that already exists gets the lease the subscription was
	// created with, whatever cfg asks for, and a LeaseExpires computed from
	// cfg told the core's lease scheduler a later expiry than the engine
	// applies, so it extended too late and the delivery was redelivered
	// while its handler still ran.
	lease := sub.AckDeadline()
	if lease < 0 {
		// A negative deadline means leases never expire, which is the
		// absence of a lease deadline rather than one in the past. The specification
		// makes the zero time "the broker grants no lease".
		lease = 0
	}
	src := &source{
		b:            b,
		topic:        t,
		sub:          sub,
		cons:         cons,
		name:         cfg.Topic,
		subscription: cfg.Subscription,
		lease:        lease,
		meta:         meta,
		claims:       make([]engine.Claim[*mq.Message], cfg.Prefetch),
		closed:       make(chan struct{}),
	}
	src.st = &settler{src: src}
	return src
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// It allocates nothing per fetch, and nothing per message: the claims come
// out of the source's own buffer, every DriverDelivery field below is a
// scalar or a slice header, and the driver handle a settlement needs later is
// a pointer stored directly in the interface word of Ref rather than boxed.
// That is what the budget is stated against — a receive that costs no
// allocation per message — and TestAllocs is what holds it.
//
// A Close in another goroutine unblocks a parked Fetch, which is the specification's
// obligation and the specification's "Close unblocks blocked operations" reaching a
// driver. It is the engine's own consumer close that does it, so nothing here
// merges the caller's context with a cancellation of its own: a merge costs
// two or three allocations per receive, on the one path that may not have
// them.
//
// A record the source cannot decode is never handed over and never fails the
// fetch (refuse). Fetch's error ends the root runner's Run, so returning the
// decode error stopped every Run, restarted ones included, on the first such
// record, and the records behind it were never handled. When every record a
// fetch claimed was refused, Fetch takes the next ones, unless ctx is done:
// an empty answer with no error is not one the SPI allows.
func (src *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n := min(len(dst), len(src.claims))
	if n == 0 {
		return 0, nil
	}
	for {
		got, err := src.cons.Fetch(ctx, src.claims[:n])
		err = engineErr(mq.OpFetch, src.name, src.subscription, err)
		if got == 0 {
			return 0, err
		}
		if kept := src.deliver(dst, got); kept > 0 || err != nil {
			return kept, err
		}
		if err := ctx.Err(); err != nil {
			return 0, engineErr(mq.OpFetch, src.name, src.subscription, err)
		}
	}
}

// deliver fills dst with the first got claims the engine handed over, leaving
// out the ones refuse sends back, and returns how many it filled.
func (src *source) deliver(dst []mq.DriverDelivery, got int) int {
	now := src.b.eng.Clock().Now()
	var expires time.Time
	if src.lease > 0 {
		expires = now.Add(src.lease)
	}
	kept := 0
	for i := range got {
		cl := &src.claims[i]
		m := cl.Payload
		if src.meta == metaEnvelope {
			decoded, derr := unwrap(src.b.envelope, m)
			if derr != nil {
				src.refuse(cl)
				continue
			}
			m = decoded
		}
		topic := src.name
		if cl.Info.Subject != "" {
			// The resolved topic of a wildcard publication, which the specification
			// requires rather than the pattern a subscription named.
			topic = cl.Info.Subject
		}
		dst[kept] = mq.DriverDelivery{
			Message: m,
			Info: mq.DeliveryInfo{
				Topic:        topic,
				Subscription: src.subscription,
				ReceivedAt:   now,
				LeaseExpires: expires,
				Attempt:      cl.Info.Attempts,
				Redelivered:  cl.Info.Attempts > 1,
				Partition:    cl.Info.Partition,
				HasPartition: true,
				Sequence:     cl.Info.Seq,
				HasSequence:  true,
			},
			// The envelope and the generation are what a settlement
			// presents, and the core hands this struct back untouched. A
			// pointer in an interface value is stored directly, so the
			// handle costs no allocation.
			Ref: cl.Env,
			Gen: cl.Gen,
		}
		kept++
	}
	return kept
}

// refuse sends back a claimed record whose envelope the source cannot
// decode, which for an in-process broker means its publisher was not
// enveloping. It is a failed delivery like a lease that ran out: the attempt
// it spent counts, so the topic's attempt limit and dead-letter topic decide
// what becomes of it at once, and otherwise it comes back after one lease
// (DefaultAckDeadline when leases never expire), wherever the retry
// placement puts it. Back at once, it was the first record the next fetch
// claimed, and a fetch that refused everything it claimed took it again
// straight away.
func (src *source) refuse(cl *engine.Claim[*mq.Message]) {
	delay := src.lease
	if delay <= 0 {
		delay = DefaultAckDeadline
	}
	_ = src.cons.Nack(cl.Env, cl.Gen, delay)
}

// Settler settles this source's deliveries. There is one per source
// and it may be called concurrently with a fetch.
func (src *source) Settler() mq.Settler { return src.st }

// Close releases the source, returns every delivery it handed out and never
// had settled to its lane for another consumer, and unblocks a parked Fetch.
// It is idempotent.
//
// The subscription itself is left alone: a subscription outlives its
// consumers, which is what makes a consumer group a group and what
// FeatureDurableSubscriptions advertises. Deleting it is the admin surface's
// job and is explicit.
func (src *source) Close() error {
	src.once.Do(func() {
		close(src.closed)
		_ = src.cons.Close()
		src.b.forget(src, nil)
	})
	return nil
}

// unwrap decodes an enveloped message (MetadataPortableEnvelope).
//
// It allocates: the decoded key, body and header table outlive the call and
// cannot come from a pool the next delivery reuses. That is the cost of the
// mode, and it is why nothing reaches it by accident.
func unwrap(layout wire.Layout, m *mq.Message) (*mq.Message, error) {
	out := &mq.Message{Topic: m.Topic}
	if err := layout.Decode(m.Body, out, nil); err != nil {
		return nil, err
	}
	return out, nil
}
