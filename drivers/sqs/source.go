package sqs

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The source implements the mandatory consumption surface; its settler
// implements the optional batched settlement and the optional lease extension.
var _ mq.Source = (*source)(nil)

// source is one open subscription: N long-polling receivers feeding one
// bounded ring, and the bookkeeping that makes a stale settlement detectable.
//
// The shape is the specification: "N pollers with long poll 20 s × 10 messages". Each
// poller is one ReceiveMessage call in flight, so N pollers is N concurrent
// requests and up to N×MaxMessages messages held; the ring in between is
// [driverkit.PushAdapter], and its bound is what turns a slow consumer into
// backpressure on the pollers rather than into unbounded memory. A poller
// blocked in Push stops asking the service for messages, which is the only
// flow control a long poll has.
type source struct {
	b    *Backend
	cfg  mq.SourceConfig
	enc  *encoder
	url  string
	ring *driverkit.PushAdapter
	set  *settler

	// deadline is the visibility timeout every ReceiveMessage asks for and
	// every delivery's lease is measured against.
	deadline time.Duration

	// gen is the delivery generation, one per delivery and redelivery
	// It is what makes a settlement carrying a delivery the driver
	// no longer vouches for detectable.
	gen atomic.Uint32

	// mu guards inflight, which maps a message identifier to the delivery
	// this source currently vouches for.
	//
	// It is the other half of the stale-receipt defence, and it is the half
	// the visibility window cannot provide. A message this source receives
	// TWICE — because its window expired, or because somebody made it
	// visible again early — has two receipt handles, and the service will
	// accept a delete for either of them. So the older delivery is marked
	// superseded the moment the newer one arrives, and a settlement for it
	// reports ErrLeaseLost rather than deleting a message this process is
	// processing under a different delivery.
	mu       sync.Mutex
	inflight map[string]*delivery

	// stale counts the superseded deliveries Fetch has dropped.
	stale atomic.Uint64

	wg     sync.WaitGroup
	stop   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

// newSource builds the source and its settler, without starting the pollers.
func (b *Backend) newSource(cfg mq.SourceConfig, enc *encoder, url string) (*source, error) {
	resolved := b.cfg.resolve()
	deadline := cfg.AckDeadline
	if deadline <= 0 {
		deadline = resolved.AckDeadline
	}
	if deadline > MaxVisibility {
		return nil, errSourceDeadline
	}
	// The service's visibility timeout is a whole number of SECONDS, so the
	// deadline is rounded UP to what the service will actually grant and the
	// source then measures its local lease against that rather than against
	// what was asked for.
	//
	// Rounding is not a detail here. A half-second deadline truncated to zero
	// is a visibility timeout of zero, which means "immediately visible" --
	// every consumer of the queue sees every message at once and nothing has
	// a lease at all. The lease scenarios ask for 500 ms, and against the
	// pinned container they got exactly that: the driver granted no lease and
	// then refused every settlement, because its local view said the window
	// had closed the instant it opened.
	deadline = time.Duration(visibilitySeconds(deadline)) * time.Second
	// The ring holds a poller's whole answer without blocking it mid-batch,
	// and never less than the prefetch the core resolved: a ring smaller
	// than one ReceiveMessage's answer would park a poller holding messages
	// whose visibility window is already running.
	capacity := max(cfg.Prefetch, resolved.Pollers*resolved.MaxMessages)
	ring, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: capacity})
	if err != nil {
		return nil, err
	}
	src := &source{
		b:        b,
		cfg:      cfg,
		enc:      enc,
		url:      url,
		ring:     ring,
		deadline: deadline,
		inflight: make(map[string]*delivery, capacity),
		stop:     make(chan struct{}),
	}
	src.gen.Store(1)
	set, err := newSettler(src)
	if err != nil {
		return nil, err
	}
	src.set = set
	return src, nil
}

// start launches the pollers.
//
// They deliberately do NOT take the opening context. They outlive the call
// that started them — a source lives until Close — so a poller bound to the
// open's context would stop the moment a caller who used a bounded context for
// construction let it expire, and a subscription whose pollers have stopped is
// one that has silently stopped consuming.
func (s *source) start() {
	n := s.b.cfg.resolve().Pollers
	s.wg.Add(n)
	for range n {
		go s.poll()
	}
}

// poll is one receiver: long-poll, push, repeat.
func (s *source) poll() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		if err := s.round(); err != nil {
			select {
			case <-s.stop:
				return
			default:
			}
			// A failed poll is a background failure with nowhere to surface
			// but Health. It is not fatal: the next round retries,
			// which is what a request/response broker's transient failure
			// needs.
			s.b.degraded(err)
			if !s.wait(pollBackoff) {
				return
			}
			continue
		}
		s.b.ready()
	}
}

// pollBackoff is how long a poller waits after a failed round.
//
// It exists so that a queue that has been deleted, or a credential that has
// expired, costs one request per interval rather than a spin. The long poll
// itself is the pacing for the successful path, so there is nothing to back
// off from there.
const pollBackoff = 500 * time.Millisecond

// wait sleeps for d unless the source is closing, and reports whether the
// poller should continue.
func (s *source) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.stop:
		return false
	}
}

// round is one ReceiveMessage and the pushes it produces.
func (s *source) round() error {
	cfg := s.b.cfg.resolve()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The long poll is bounded by the service's WaitTimeSeconds, and the
	// context is cancelled when the source closes so that a parked request
	// does not outlive it.
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	out, err := s.b.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(s.url),
		MaxNumberOfMessages:   int32(cfg.MaxMessages),
		WaitTimeSeconds:       int32(cfg.WaitTime / time.Second),
		VisibilityTimeout:     visibilitySeconds(s.deadline),
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameSentTimestamp,
			types.MessageSystemAttributeNameMessageGroupId,
			types.MessageSystemAttributeNameSequenceNumber,
		},
	})
	if err != nil {
		if s.closed.Load() || ctx.Err() != nil {
			return nil
		}
		return s.b.errs.Map(mq.OpFetch, s.cfg.Topic, s.cfg.Subscription, err)
	}
	// undecodable collects the messages of this answer that cannot be
	// handed out at all; they are released with the answer's one release
	// rather than one call each, so however many there are they cost Close
	// one APITimeout.
	var undecodable []string
	for i := range out.Messages {
		ok, err := s.admit(ctx, &out.Messages[i])
		if err != nil {
			// The source is closing: admit fails only when the ring refuses
			// a push. The message it was pushing and the rest of this answer
			// were received -- the service started their visibility windows
			// when it answered -- and none of them will ever reach the ring
			// or Close's drain, so they are made visible again here, before
			// the poller returns and Close's wg.Wait lets Close return.
			// The messages BEFORE i are in the ring and
			// are the drain's, except the undecodable ones, which are this
			// answer's; releasing the ring's here too would release them
			// twice.
			//
			// Returning without this is how a consumer closed while its
			// poller was part-way through an answer left the rest of that
			// answer invisible for the whole AckDeadline:
			// TestCloseReleasesUnfetchedDeliveries on GitHub, "took 2 of 4"
			// with a one-hour deadline. It is one batched call, not one per
			// message: an answer is at most MaxBatchEntries messages.
			s.release(ctx, append(undecodable, receiptHandles(out.Messages[i:])...)...)
			return err
		}
		if !ok {
			undecodable = append(undecodable, aws.ToString(out.Messages[i].ReceiptHandle))
		}
	}
	s.release(ctx, undecodable...)
	return nil
}

// receiptHandles is the receipt handles of ms, in order.
func receiptHandles(ms []types.Message) []string {
	out := make([]string, len(ms))
	for i := range ms {
		out[i] = aws.ToString(ms[i].ReceiptHandle)
	}
	return out
}

// admit turns one received message into a delivery and pushes it into the
// ring, blocking while the ring is full. It reports false for a message it
// cannot decode, which round releases, and an error when the ring refuses the
// push because the source is closing.
//
// A message this source is already holding supersedes its earlier delivery,
// which is the stale-receipt defence: the earlier handle is still valid at the
// service, so nothing but this bookkeeping stops a settlement for it deleting
// a message the newer delivery owns.
func (s *source) admit(ctx context.Context, m *types.Message) (bool, error) {
	now := s.b.now()
	ref := &delivery{
		handle: aws.ToString(m.ReceiptHandle),
		id:     aws.ToString(m.MessageId),
		url:    s.url,
		group:  groupOf(m.Attributes),
		native: m,
		gen:    s.gen.Add(1),
		src:    s,
	}
	ref.extend(now.Add(s.deadline))
	msg := &mq.Message{}
	if err := s.enc.decode(m, s.cfg.Topic, msg); err != nil {
		// A message this driver cannot decode is not something to retry for
		// ever and not something to delete: making it visible again lets the
		// queue's own redrive policy take it to the dead-letter queue after
		// MaxReceiveCount attempts, which is where a corrupt record belongs
		// The failure is reported through Health, because there is
		// no delivery to attach it to. round makes the release, batched
		// with the rest of the answer's.
		s.b.degraded(err)
		return false, nil
	}
	seq, hasSeq := sequenceOf(m.Attributes)
	d := mq.DriverDelivery{
		Message: msg,
		Info: mq.DeliveryInfo{
			Topic:        s.cfg.Topic,
			Source:       s.url,
			Subscription: s.cfg.Subscription,
			ReceivedAt:   now,
			LeaseExpires: ref.expiresAt(),
			Attempt:      attemptOf(m.Attributes),
			Sequence:     seq,
			HasSequence:  hasSeq,
		},
		Ref:    ref,
		Native: m,
		Gen:    ref.gen,
	}
	d.Info.Redelivered = d.Info.Attempt > 1
	s.supersede(ref)
	if err := s.ring.Push(ctx, d); err != nil {
		// The ring is closed, so the source is closing. The message is NOT
		// released here: round releases it together with the rest of its
		// answer, in one call, because a delivery nobody will ever fetch is
		// one whose visibility window expires for no reason.
		s.forget(ref)
		return false, err
	}
	return true, nil
}

// supersede records this delivery as the one the source vouches for, marking
// any earlier delivery of the same message lost.
func (s *source) supersede(ref *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.inflight[ref.id]; ok && prev != ref {
		prev.lose()
	}
	s.inflight[ref.id] = ref
}

// forget drops a delivery from the in-flight map, if it is still the current
// one.
func (s *source) forget(ref *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.inflight[ref.id]; ok && cur == ref {
		delete(s.inflight, ref.id)
	}
}

// release makes messages visible again without settling them, which is what a
// source does with deliveries it cannot hand over.
//
// It is ChangeMessageVisibilityBatch, MaxBatchEntries handles a call, and every
// call of one release shares ONE deadline: the caller's, when its context has
// one, and Config.APITimeout from now otherwise. The caller's CANCELLATION is
// not inherited, because the callers that matter are closing -- a poller whose
// context Close has just cancelled -- and a release that gave up the moment it
// started would strand exactly what it exists to hand back. The deadline is
// kept, because it is what makes Close's bound true: this used to strip it
// along with the cancellation (context.WithoutCancel removes both) and give
// every handle a fresh APITimeout, so Close's drain, which asked for one
// APITimeout, took one per delivery against a service that had stopped
// answering. A failed release is not reported: the message
// is still in flight at the service and its visibility window is the backstop.
func (s *source) release(ctx context.Context, handles ...string) {
	if len(handles) == 0 {
		return
	}
	callCtx, cancel := s.releaseContext(ctx)
	defer cancel()
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, min(len(handles), MaxBatchEntries))
	flush := func() {
		if len(entries) == 0 {
			return
		}
		_, _ = s.b.api.ChangeMessageVisibilityBatch(callCtx, &sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(s.url),
			Entries:  entries,
		})
		entries = entries[:0]
	}
	for _, h := range handles {
		if h == "" {
			continue
		}
		entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(entryID(len(entries))),
			ReceiptHandle:     aws.String(h),
			VisibilityTimeout: 0,
		})
		if len(entries) == MaxBatchEntries {
			flush()
		}
	}
	flush()
}

// releaseContext is the context one release runs under: ctx's deadline if it
// has one, otherwise Config.APITimeout, and never ctx's cancellation.
func (s *source) releaseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(ctx)
	if dl, ok := ctx.Deadline(); ok {
		return context.WithDeadline(base, dl)
	}
	return context.WithTimeout(base, s.b.cfg.resolve().APITimeout)
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The route here is the visibility window. A poller keeps polling while the
// application is doing something else, so a message whose window passes is
// received AGAIN and pushed into the ring behind the delivery nobody has
// fetched yet. A Fetch that handed out the older one would hand out a delivery
// whose settlement is refused with ErrLeaseLost — while the message sits in
// the ring one slot along, perfectly settleable. The specification's
// lease/expiry-redelivery scenario is exactly that shape: it holds a delivery,
// waits three seconds past a one-second deadline, and then acknowledges the
// redelivery it is handed.
//
// Nothing is released for a dropped delivery: the message is already in flight
// under the newer one, which is in the ring behind it.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if s.closed.Load() {
			return 0, mq.ErrClosed
		}
		n, err := s.ring.Fetch(ctx, dst)
		if n == 0 {
			return 0, err
		}
		live, now := 0, s.b.now()
		for i := range n {
			if ref, ok := dst[i].Ref.(*delivery); ok && s.dead(ref, now) {
				s.stale.Add(1)
				continue
			}
			if live != i {
				dst[live] = dst[i]
			}
			live++
		}
		if live > 0 || err != nil {
			return live, err
		}
	}
}

// dead reports whether a delivery in the ring is one nobody could settle: one
// a later delivery has superseded, or one whose visibility window has closed
// while it waited to be fetched.
//
// The second half is the one an emulator's speed decides. A poller keeps
// receiving a message the application is not fetching, so the ring can hold a
// delivery whose window has already passed by the time a Fetch reaches it —
// and handing that out gives the caller a delivery whose settlement is refused
// through no fault of its own. Dropping it costs nothing: the message is
// visible again at the service, so the next poll brings it back with a full
// window.
func (s *source) dead(ref *delivery, now time.Time) bool {
	return ref.state.Load() == stateLost || ref.expired(now, s.b.cfg.LeaseGuard)
}

// Stale is how many superseded deliveries Fetch has dropped, which is the
// number a test asserts on: a source that never drops one under a lease that
// keeps expiring is a source handing out deliveries nobody can settle.
func (s *source) Stale() uint64 { return s.stale.Load() }

// Settler settles this source's deliveries. There is one per source.
func (s *source) Settler() mq.Settler { return s.set.settler() }

// Close releases the source, makes everything it has handed out and never
// settled visible again, and unblocks a parked Fetch. It is idempotent.
func (s *source) Close() error {
	var err error
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		// The ring is closed BEFORE the pollers are waited for, so a poller
		// parked in Push is woken rather than waited on for the length of a
		// long poll.
		_ = s.ring.Close()
		s.wg.Wait()
		// Everything the ring still held is made visible again: a delivery
		// the application never saw must not wait out its visibility window
		// (the specification's "Close requeues or abandons everything it has handed out").
		// It is one release, batched, under one APITimeout for the lot.
		ctx, cancel := context.WithTimeout(context.Background(), s.b.cfg.resolve().APITimeout)
		defer cancel()
		var held []string
		s.ring.Drain(func(d *mq.DriverDelivery) {
			if ref, ok := d.Ref.(*delivery); ok && ref.claimLost() {
				held = append(held, ref.handle)
			}
		})
		s.release(ctx, held...)
		err = s.set.close()
		s.b.forget(s, nil)
	})
	return err
}

// The delivery settlement states, which are the as one source sees them.
const (
	// stateActive is a delivery that has been handed out and not settled.
	stateActive uint32 = iota
	// stateSettling is a settlement in flight at the service.
	stateSettling
	// stateSettled is a delivery whose delete the service took.
	stateSettled
	// stateUnknown is a delivery whose settlement was ambiguous. It is
	// sticky: nothing later turns it into a success.
	stateUnknown
	// stateLost is a delivery this source no longer vouches for, because
	// the message has been received again or the source is closing.
	stateLost
)

// delivery is everything a settlement needs, and the only thing this driver
// boxes per delivery.
//
// The receipt handle is why. It is a string the service mints per RECEIVE, it
// is the only thing a delete can name, and no integer can carry it — which is
// the case [github.com/iqhive/iqmq.DriverDelivery.Ref] exists for.
type delivery struct {
	handle string
	id     string
	url    string
	group  string
	native *types.Message
	gen    uint32
	src    *source

	// expires is the local view of the visibility window, as Unix
	// nanoseconds. It is atomic because the local lease check reads it from
	// whichever goroutine is settling while an Extend writes it from
	// another, and because a time.Time is two words and cannot be read
	// atomically at all.
	expires atomic.Int64
	state   atomic.Uint32
}

// expiresAt is the local view of the visibility window.
func (d *delivery) expiresAt() time.Time { return time.Unix(0, d.expires.Load()) }

// extend moves the local view of the visibility window, which an Extend has
// to do or the next settlement would be refused against a deadline that is no
// longer the service's.
func (d *delivery) extend(at time.Time) { d.expires.Store(at.UnixNano()) }

// lose marks the delivery one this source no longer vouches for.
func (d *delivery) lose() { d.state.Store(stateLost) }

// claimLost takes an active delivery out of circulation, reporting whether it
// was still active. It is what Close uses so that a delivery being settled
// concurrently is not released underneath its own settlement.
func (d *delivery) claimLost() bool {
	return d.state.CompareAndSwap(stateActive, stateLost)
}

// expired reports whether the visibility window has closed, or is within
// guard of closing.
//
// This is the local lease check, and it is the reason it exists: the service
// will accept a DeleteMessage for a receipt handle whose message another
// consumer now holds, so a delete sent after the window closed does not fail —
// it deletes somebody else's work. There is no API that asks "do I still hold
// this?", so the only place the refusal can happen is here, before the call.
func (d *delivery) expired(now time.Time, guard time.Duration) bool {
	return !now.Add(guard).Before(d.expiresAt())
}

// stateError maps a delivery's current state to the answer the specification gives a second
// settlement.
func stateError(state uint32) error {
	switch state {
	case stateSettling:
		return mq.ErrSettlementInProgress
	case stateSettled:
		return mq.ErrAlreadySettled
	case stateUnknown:
		return mq.ErrSettlementUnknown
	case stateLost:
		return mq.ErrLeaseLost
	case stateActive:
		return nil
	default:
		return mq.ErrLeaseLost
	}
}

// nextNonce is a random number, used for the generated deduplication
// identifiers a FIFO publication needs. Sixty-four random bits keep two sinks
// apart without a counter shared by every backend in the process, and keep
// two processes publishing to one queue apart as well.
func nextNonce() uint64 { return rand.Uint64() } //nolint:gosec // uniqueness, not secrecy

// visibilitySeconds converts a duration to the whole seconds the service
// takes, rounding UP and clamping to the service's twelve hours.
//
// Rounding up is the safe direction for a lease: a half-second extension that
// rounded to zero would make the message immediately visible, which is the
// opposite of what an extension asks for.
func visibilitySeconds(d time.Duration) int32 {
	switch {
	case d <= 0:
		return 0
	case d > MaxVisibility:
		return int32(MaxVisibility / time.Second)
	default:
		return int32((d + time.Second - 1) / time.Second)
	}
}

// entryID is the batch entry identifier for the item at index i.
func entryID(i int) string { return strconv.Itoa(i) }

// The source's refusals.
var (
	errNackAction = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: the negative settlement names no action of the message contract")
	errDelayTooLongForLease = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: the redelivery delay is longer than the service's twelve-hour visibility timeout")
	errExtendZero = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: an extension of zero or less would make the message immediately visible, which is what a Nack means")
	errExtendTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: the extension is longer than the service's twelve-hour visibility timeout")
	errExtendPastCeiling = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: the extension plus the remaining visibility window is longer than the service's twelve-hour visibility timeout, and no timeout grants that deadline")
	errSourceDeadline = errcode.New(errcode.InvalidConfig,
		"iqmq/sqs: the source's acknowledgement deadline is longer than the service's twelve-hour visibility timeout")
	errNoSeek = errcode.New(errcode.Unsupported,
		"iqmq/sqs: a queue has no history to seek in; a settled message is deleted, so StartLatest and StartEarliest are the same position and a time or a sequence is refused")
	errNoEntryResult = errcode.New(errcode.Unknown,
		"iqmq/sqs: the service named this batch entry in neither its successful nor its failed list")
)
