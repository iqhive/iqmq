package testkit

import (
	"context"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// Faults is the fault injection the specification asks a conformance run to use "where
// possible": a wrapper around a driver's [bitbucket.org/iqhive/iqmq.Backend] that
// makes it misbehave in the specific ways the specification forbids.
//
// It has two uses, and the second is the important one.
//
// A driver author uses it to reach the paths a healthy broker will not produce:
// a rejected publication, an ambiguous one, a settlement that fails.
//
// The kit uses it on itself. A conformance kit that has never rejected anything
// is assumed not to work, and six checks in this repository turned out not to.
// The way to find out is to take a driver that passes, break one thing it claims,
// and watch the corresponding scenario fail — which is what testkit's own tests
// do with this type. Every field below is therefore a defect somebody could ship
// by accident, written as a switch.
type Faults struct {
	// Capabilities rewrites the driver's capability table. It is how a
	// claim is made false without touching the driver: claim a feature the
	// broker does not have, or withdraw one it does.
	Capabilities func(mq.Capabilities) mq.Capabilities
	// Publish replaces the sink's own answer when it returns a non-nil
	// error, which is how a rejected or ambiguous publication is reached.
	Publish func(m *mq.Message) error
	// SwallowAck makes every acknowledgement report success and do nothing,
	// which is the defect that makes an acked message come back.
	SwallowAck bool
	// DropNackDelay makes a delayed negative settlement redeliver
	// immediately, which is the defect that makes a backoff invisible.
	DropNackDelay bool
	// HideExtender makes the source's settler stop being a
	// DeadlineExtender, which is what a source-side adapter that forgot to
	// delegate Extend does to every driver underneath it.
	HideExtender bool
	// FailExtend makes every lease extension fail with the error it returns.
	FailExtend func() error
	// LeakSecret renders this string into every error the driver builds,
	// which is what a driver that interpolates its configuration into its
	// own error messages does.
	//
	// It is the defect forbid and the one `secrets/redaction`
	// could not see: two of that scenario's original four probes were
	// answered by the CORE's validator before the driver was reached, so a
	// driver with this defect passed. It is applied only where the wrapped
	// backend ALREADY failed -- a redaction fault that invented failures
	// would break every other scenario instead of this one.
	LeakSecret string
	// ReorderFetch hands out each batch of deliveries backwards, which is
	// what a driver that fetched from several partitions and merged them
	// carelessly does to an ordering claim.
	//
	// It is how the guarantee machinery is held to both its directions: a
	// driver that CLAIMS an ordering has to fail when this is on, and a
	// driver that DENIES one has to have its denial confirmed by it rather
	// than merely unrefuted.
	ReorderFetch bool
	// ShareSubscriptions makes every subscription name open the SAME
	// subscription, which is the shape a queue broker has and the SPI has
	// no way to declare.
	//
	// It is the driver that does something DIFFERENT rather than refusing:
	// SourceConfig.Subscription is accepted, nothing is refused, and the
	// difference shows up only in what arrives -- the second reader
	// competes with the first instead of getting its own copy. SQS is this
	// driver, and `capability/FanOut/refused` and
	// `capability/Replay/refused` FAILED it on "no delivery arrived" for
	// denials that were true, which is why three scenarios are documented
	// away in drivers/sqs.
	ShareSubscriptions bool
	// RefuseOpenSource makes every source open answer this error, which is
	// a refusal from a step that is NOT the feature's own call.
	//
	// It is the other half of the same defect: a negative capability probe
	// used to pass on an ErrUnsupported from anywhere in the probe, so a
	// driver that cannot open a consumer at all had every one of its
	// negative claims recorded as refuted by observation without the
	// feature's own call ever being made.
	RefuseOpenSource error
	// NackError replaces a negative settlement's answer when it returns
	// non-nil, which is how a driver that answers the feature's own call
	// with something that is not a refusal is reached: the specification requires
	// ErrUnsupported from a driver that does not support a call, and one
	// that reports an internal failure instead has done something other
	// than refuse.
	NackError func(o mq.NackOptions) error
}

// Wrap returns a backend that behaves like b except where the faults say
// otherwise. A zero Faults changes nothing, which is what makes it safe to wrap
// unconditionally in a harness and switch the faults on per test.
func Wrap(b mq.Backend, f Faults) mq.Backend {
	return &faultBackend{Backend: b, faults: f}
}

// faultBackend is the wrapped backend.
type faultBackend struct {
	mq.Backend
	faults Faults
}

// Capabilities returns the rewritten table.
func (b *faultBackend) Capabilities() mq.Capabilities {
	caps := b.Backend.Capabilities()
	if b.faults.Capabilities != nil {
		return b.faults.Capabilities(caps)
	}
	return caps
}

// Admin delegates to the wrapped backend when it has one, wrapped so that its
// errors carry the injected credential too.
func (b *faultBackend) Admin() (mq.Admin, error) {
	a, ok := b.Backend.(mq.AdminBackend)
	if !ok {
		return nil, mq.ErrUnsupported
	}
	admin, err := a.Admin()
	if err != nil || b.faults.LeakSecret == "" {
		return admin, b.leak(err)
	}
	return &faultAdmin{Admin: admin, leak: b.leak}, nil
}

// faultAdmin renders the injected credential into the administrative
// surface's errors, which is where a driver that logs its configuration is
// most likely to do it.
type faultAdmin struct {
	mq.Admin
	leak func(error) error
}

// EnsureTopic delegates and leaks.
func (a *faultAdmin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	return a.leak(a.Admin.EnsureTopic(ctx, spec))
}

// Stats delegates and leaks.
func (a *faultAdmin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	st, err := a.Admin.Stats(ctx, topic, sub)
	return st, a.leak(err)
}

// Health delegates to the wrapped backend when it reports one.
func (b *faultBackend) Health() mq.HealthSnapshot {
	if h, ok := b.Backend.(mq.HealthReporter); ok {
		return h.Health()
	}
	if b.Backend == nil {
		return mq.HealthSnapshot{State: mq.HealthClosed}
	}
	return mq.HealthSnapshot{State: mq.HealthReady}
}

// OpenSink opens a sink that answers the injected publication faults.
func (b *faultBackend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	s, err := b.Backend.OpenSink(ctx, cfg)
	if err != nil {
		return s, b.leak(err)
	}
	if b.faults.Publish == nil && b.faults.LeakSecret == "" {
		return s, nil
	}
	return &faultSink{Sink: s, publish: b.faults.Publish, leak: b.leak}, nil
}

// OpenSource opens a source whose settler answers the injected settlement
// faults.
//
// ShareSubscriptions is applied HERE rather than in Fetch because that is
// where a queue broker's topology lives: the two names are one subscription
// from the moment they are opened, so the first reader's settlement really
// does remove the message from the second reader's reach, which a fault that
// merely hid deliveries could not reproduce.
func (b *faultBackend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if b.faults.RefuseOpenSource != nil {
		return nil, b.leak(b.faults.RefuseOpenSource)
	}
	if b.faults.ShareSubscriptions {
		cfg.Subscription = sharedSubscription
	}
	s, err := b.Backend.OpenSource(ctx, cfg)
	if err != nil {
		return s, b.leak(err)
	}
	return &faultSource{src: s, faults: b.faults, leak: b.leak}, nil
}

// sharedSubscription is the one subscription ShareSubscriptions collapses
// every name onto.
const sharedSubscription = "faults-one-queue"

// leak renders the injected credential into an error the driver produced,
// preserving its portable code and its sentinel so that nothing else in the
// suite changes behaviour.
func (b *faultBackend) leak(err error) error {
	if err == nil || b.faults.LeakSecret == "" {
		return err
	}
	return &leakedError{inner: err, secret: b.faults.LeakSecret}
}

// leakedError is an error that renders a credential, wrapping the real one so
// that errors.Is, the portable code and every other check still see through it.
type leakedError struct {
	inner  error
	secret string
}

// Error implements error, with the credential in it, which is the defect.
func (e *leakedError) Error() string {
	return e.inner.Error() + " [dsn=broker://user:" + e.secret + "@host]"
}

// Unwrap keeps the sentinel and the code reachable.
func (e *leakedError) Unwrap() error { return e.inner }

// faultSink refuses the publications the fault names.
type faultSink struct {
	mq.Sink
	publish func(*mq.Message) error
	leak    func(error) error
}

// Publish applies the fault before the driver sees the message.
func (s *faultSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.publish != nil {
		if err := s.publish(m); err != nil {
			return s.leak(err)
		}
	}
	return s.leak(s.Sink.Publish(ctx, m, o))
}

// faultSource wraps a source's settler.
type faultSource struct {
	src    mq.Source
	faults Faults
	leak   func(error) error
}

// Fetch delegates, reversing the batch when the ordering fault is on.
func (s *faultSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n, err := s.src.Fetch(ctx, dst)
	if s.faults.ReorderFetch {
		for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
			dst[i], dst[j] = dst[j], dst[i]
		}
	}
	return n, s.leak(err)
}

// Close delegates.
func (s *faultSource) Close() error { return s.src.Close() }

// Settler returns the wrapped settler, keeping lease extension unless the fault
// takes it away on purpose.
//
// The two shapes here are the whole of the rule: a wrapper either
// preserves DeadlineExtender or deliberately does not, and there is no third
// case where it loses one by accident.
func (s *faultSource) Settler() mq.Settler {
	inner := s.src.Settler()
	if inner == nil {
		return nil
	}
	w := &faultSettler{inner: inner, faults: s.faults}
	ex, ok := inner.(mq.DeadlineExtender)
	if !ok || s.faults.HideExtender {
		return w
	}
	return &faultExtendingSettler{faultSettler: w, extender: ex}
}

// faultSettler applies the settlement faults.
type faultSettler struct {
	inner  mq.Settler
	faults Faults
}

// Ack swallows the settlement when the fault says to.
func (s *faultSettler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	if s.faults.SwallowAck {
		return nil
	}
	return s.inner.Ack(ctx, d)
}

// Nack drops the delay when the fault says to, and answers the injected error
// when one is named.
func (s *faultSettler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if s.faults.NackError != nil {
		if err := s.faults.NackError(o); err != nil {
			return err
		}
	}
	if s.faults.DropNackDelay {
		o.Delay = 0
	}
	return s.inner.Nack(ctx, d, o)
}

// faultExtendingSettler keeps the driver's lease extension.
type faultExtendingSettler struct {
	*faultSettler
	extender mq.DeadlineExtender
}

// Extend delegates, or fails when the fault says to.
func (s *faultExtendingSettler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if s.faults.FailExtend != nil {
		return s.faults.FailExtend()
	}
	return s.extender.Extend(ctx, d, by)
}

// ClaimFeature returns a Capabilities rewriter that advertises a feature at a
// level, which is how a test makes a driver claim something it cannot do.
func ClaimFeature(f mq.Feature, s mq.Support) func(mq.Capabilities) mq.Capabilities {
	return func(caps mq.Capabilities) mq.Capabilities {
		out := copyCapabilities(caps)
		_ = out.Set(f, s)
		return out
	}
}

// WithdrawFeature returns a Capabilities rewriter that stops advertising a
// feature the driver has, which is how a test makes a negative claim false.
func WithdrawFeature(f mq.Feature) func(mq.Capabilities) mq.Capabilities {
	return ClaimFeature(f, mq.Support{Level: mq.Unsupported})
}

// WithdrawLimits returns a Capabilities rewriter that advertises none of the limits, which is what a driver whose limit lookup silently returned
// nothing looks like.
//
// the did, against a real Kafka broker, for four months. Nothing noticed:
// publish/oversized-payload skips when the limit is unknown, and a skip is
// indistinguishable from a pass. `limits/advertised` is the scenario that
// notices, and this is what makes it fail.
func WithdrawLimits() func(mq.Capabilities) mq.Capabilities {
	return func(caps mq.Capabilities) mq.Capabilities {
		out := copyCapabilities(caps)
		out.SetLimits(mq.Limits{})
		return out
	}
}

// AdvertiseMaxLease makes the table claim d as MaxLease.
//
// It is how the lease ceiling is made a CLAIM for a driver that advertises
// none, so that `capability/ExtendLease/works` has a limit to hold the driver
// to. A driver whose Extend then grants more than it advertises is the defect
// the probe is for: `DeadlineExtender.Extend` returns only an error, so a
// shorter grant than the caller asked for is invisible and the lease scheduler
// goes on assuming the documented minimum.
func AdvertiseMaxLease(d time.Duration) func(mq.Capabilities) mq.Capabilities {
	return func(caps mq.Capabilities) mq.Capabilities {
		out := copyCapabilities(caps)
		lim := caps.Limits()
		lim.MaxLease = mq.Limit[time.Duration]{Value: d, Known: true}
		out.SetLimits(lim)
		return out
	}
}

// copyCapabilities returns an independent table with the same claims.
//
// Capabilities shares its storage between copies by design — the specification makes the
// table constant per client, so nothing was ever meant to change one — which is
// why a rewriter has to rebuild rather than mutate: mutating would reach the
// driver's own table and change what every other client of it advertises.
func copyCapabilities(caps mq.Capabilities) mq.Capabilities {
	var out mq.Capabilities
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		if s := caps.Support(f); s.Level != mq.Unsupported {
			_ = out.Set(f, s)
		}
	}
	out.SetLimits(caps.Limits())
	return out
}

// counter is a small concurrency-safe tally the fakes and the faults share.
type counter struct {
	mu sync.Mutex
	n  int64
}

// add increments the tally.
func (c *counter) add(n int64) {
	c.mu.Lock()
	c.n += n
	c.mu.Unlock()
}

// load reads the tally.
func (c *counter) load() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
