package sqs

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// TestAnExpiredDeliveryLeavesTheInflightMap holds that a delivery the local
// lease check finds lost is forgotten as well: a lost delivery is terminal, so
// the in-flight map keeping it is a leak of one entry per expired message.
func TestAnExpiredDeliveryLeavesTheInflightMap(t *testing.T) {
	src := &source{b: &Backend{}, inflight: map[string]*delivery{}}
	set := &settler{src: src}
	ref := &delivery{id: "m", src: src}
	ref.expires.Store(1)
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}
	if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}

// unansweredDelete is an API whose DeleteMessageBatch is sent and never
// answered, so the caller's wait ends first.
type unansweredDelete struct {
	sqsapi.API
	calls int
}

func (u *unansweredDelete) DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	u.calls++
	return nil, context.DeadlineExceeded
}

// TestAnAmbiguousSettlementLeavesTheInflightMap holds that a delivery whose
// delete may have landed is forgotten as well. The unknown state is terminal,
// so if the delete landed the message never comes back, and the in-flight map
// kept the delivery, with the whole message, for the life of the source. Leaving
// the map does not make it settleable again: a second settlement still reports
// ErrSettlementUnknown and sends nothing, and a redelivery of the message does
// not change that.
func TestAnAmbiguousSettlementLeavesTheInflightMap(t *testing.T) {
	api := &unansweredDelete{}
	src := &source{b: &Backend{api: api, errs: newErrorMapper()}, inflight: map[string]*delivery{}, url: "q"}
	set := &settler{src: src}
	ref := &delivery{id: "m", handle: "h", gen: 1, src: src, url: "q", native: &types.Message{Body: aws.String("payload")}}
	ref.extend(time.Now().Add(time.Minute))
	src.supersede(ref)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	d := mq.DriverDelivery{Ref: ref, Gen: 1}
	if err := set.Ack(ctx, &d); !mq.IsAmbiguous(err) {
		t.Fatalf("an Ack whose delete went unanswered reported %v, want an ambiguous answer", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Errorf("the in-flight map holds %d delivery(ies) after an ambiguous Ack, want 0: it is terminal, and "+
			"the map kept the whole message for the life of the source", n)
	}
	redelivered := &delivery{id: "m", handle: "h2", gen: 2, src: src, url: "q"}
	redelivered.extend(time.Now().Add(time.Minute))
	src.supersede(redelivered)
	if err := set.Ack(ctx, &d); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Errorf("a second Ack of the ambiguous delivery reported %v, want ErrSettlementUnknown", err)
	}
	if api.calls != 1 {
		t.Errorf("DeleteMessageBatch was called %d times, want once: a second settlement of an unknown one sends nothing", api.calls)
	}
}

// stepClock is an [mq.Clock] whose time moves only when a test moves it.
type stepClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func (*stepClock) NewTimer(time.Duration) mq.Timer { panic("not used") }

// slowService is an API whose extensions, deletes and dead-letter sends each
// take a while to answer, as a throttled call the SDK retries does, and which
// records when a request carrying each receipt handle or body was sent. The
// time they take is the clock's: each answer moves it on.
type slowService struct {
	sqsapi.API
	clock             *stepClock
	extend, del, send time.Duration
	// extending is closed when an extension reaches the service, which then
	// answers once the batch has claimed settling, or refuses the extension
	// after errNeverClaimed's wall-clock bound, so that a batch that never
	// claims it fails the test rather than hanging it.
	extending chan struct{}
	settling  *delivery
	mu        sync.Mutex
	sent      map[string]time.Time
}

func (s *slowService) record(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sent[key]; !ok {
		s.sent[key] = s.clock.Now()
	}
}

func (s *slowService) sentAt(key string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.sent[key]
	return at, ok
}

// errNeverClaimed is the extension's refusal when the batch has not claimed
// the delivery it waits for within ten seconds of the wall clock.
var errNeverClaimed = errors.New("the batch never claimed the delivery the extension waits for")

func (s *slowService) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	close(s.extending)
	deadline := time.Now().Add(10 * time.Second)
	for s.settling.state.Load() != stateSettling {
		if time.Now().After(deadline) {
			return nil, errNeverClaimed
		}
		runtime.Gosched()
	}
	s.clock.advance(s.extend)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (s *slowService) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		s.record(aws.ToString(e.ReceiptHandle))
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
	}
	s.clock.advance(s.del)
	return out, nil
}

func (s *slowService) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	s.record(aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (s *slowService) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	s.record(aws.ToString(in.MessageBody))
	s.clock.advance(s.send)
	return &sqs.SendMessageOutput{}, nil
}

// TestABatchedSettlementSendsNothingForADeliveryWhoseWindowClosedWhileItWaited
// holds the local lease check to the moment a settlement's request is sent,
// not the moment its delivery was claimed. A batched settlement claims every
// delivery and then sends a request per chunk of ten, or per dead-letter route,
// one after another, so a delivery can wait behind another delivery's
// extension, which holds the claim until the service answers, behind an
// earlier chunk, or behind an earlier route, and a dead-letter route's delete
// waits for its own send. The delete then went out after the delivery's window
// had closed and was reported as a success, and a delete for an expired
// receipt handle can take the copy another consumer now holds. It is refused
// with ErrLeaseLost instead, and the message comes back.
func TestABatchedSettlementSendsNothingForADeliveryWhoseWindowClosedWhileItWaited(t *testing.T) {
	const (
		slow  = 400 * time.Millisecond
		short = 100 * time.Millisecond
		dlq   = "https://sqs.ap-southeast-2.amazonaws.com/000000000000/dlq"
	)
	dead := mq.NackOptions{Action: mq.NackActionDeadLetter}
	for _, tc := range []struct {
		name string
		api  *slowService
		// n deliveries are settled, in order, and the one at index short
		// has a window of 100 ms; the rest have a minute.
		n, short int
		settle   func(t *testing.T, set *settler, ds []*mq.DriverDelivery) []error
	}{
		{
			"behind another delivery's extension", &slowService{extend: slow}, 2, 0,
			func(t *testing.T, set *settler, ds []*mq.DriverDelivery) []error {
				api := set.src.b.api.(*slowService)
				api.settling = ds[0].Ref.(*delivery)
				extended := make(chan error, 1)
				go func() { extended <- set.Extend(t.Context(), ds[1], time.Minute) }()
				<-api.extending
				errs := set.ackResults(t.Context(), ds)
				if err := <-extended; err != nil {
					t.Errorf("Extend: %v", err)
				}
				return errs
			},
		},
		{
			"behind an earlier chunk", &slowService{del: slow}, MaxBatchEntries + 1, MaxBatchEntries,
			func(t *testing.T, set *settler, ds []*mq.DriverDelivery) []error {
				return set.ackResults(t.Context(), ds)
			},
		},
		{
			"behind an earlier dead-letter route", &slowService{send: slow}, 2, 1,
			func(t *testing.T, set *settler, ds []*mq.DriverDelivery) []error {
				return set.nackResults(t.Context(), ds, dead)
			},
		},
		{
			"behind its own dead-letter send", &slowService{send: slow}, 1, 0,
			func(t *testing.T, set *settler, ds []*mq.DriverDelivery) []error {
				return set.nackResults(t.Context(), ds, dead)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := &stepClock{at: time.Unix(1_000_000, 0)}
			api := tc.api
			api.clock, api.extending, api.sent = clk, make(chan struct{}), map[string]time.Time{}
			cfg := Config{DeadLetterQueue: dlq, APITimeout: time.Minute}
			src := &source{
				b: &Backend{
					api: api, cfg: cfg, clock: clk, errs: newErrorMapper(),
					res: newResolver(api, cfg),
				},
				inflight: map[string]*delivery{},
				url:      "https://sqs.ap-southeast-2.amazonaws.com/000000000000/q",
			}
			set := &settler{src: src}
			ds := make([]*mq.DriverDelivery, tc.n)
			for i := range ds {
				id := "m" + strconv.Itoa(i)
				ref := &delivery{
					id: id, handle: "h" + id, gen: uint32(i + 1), src: src, url: src.url,
					native: &types.Message{Body: aws.String(id)},
				}
				ref.extend(clk.Now().Add(time.Minute))
				src.supersede(ref)
				ds[i] = &mq.DriverDelivery{Ref: ref, Gen: ref.gen}
			}
			lapsing := ds[tc.short].Ref.(*delivery)
			lapsing.extend(clk.Now().Add(short))
			errs := tc.settle(t, set, ds)
			for _, key := range []string{lapsing.handle, lapsing.id} {
				if at, ok := api.sentAt(key); ok && !at.Before(lapsing.expiresAt()) {
					t.Errorf("a request carrying %s was sent %v after its delivery's window closed", key,
						at.Sub(lapsing.expiresAt()))
				}
			}
			for i, err := range errs {
				want := error(nil)
				if i == tc.short {
					want = mq.ErrLeaseLost
				}
				if !errors.Is(err, want) {
					t.Errorf("delivery %d of %d reported %v, want %v", i, tc.n, err, want)
				}
			}
		})
	}
}

// visibilityRecorder is an API that counts ChangeMessageVisibility calls and
// answers nothing else, which is all an extension may reach.
type visibilityRecorder struct {
	sqsapi.API
	calls int
}

func (r *visibilityRecorder) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	r.calls++
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// TestExtendRefusesADeliveryBeingSettled holds that an extension does not
// reach the service while a settlement owns the visibility window: a requeue
// Nack's ChangeMessageVisibilityBatch sets the window to the redelivery delay,
// and an extension sent beside it would undo that delay.
func TestExtendRefusesADeliveryBeingSettled(t *testing.T) {
	for _, tc := range []struct {
		state uint32
		want  error
	}{
		{stateSettling, mq.ErrSettlementInProgress},
		{stateSettled, mq.ErrAlreadySettled},
		{stateUnknown, mq.ErrSettlementUnknown},
	} {
		api := &visibilityRecorder{}
		src := &source{b: &Backend{api: api}, inflight: map[string]*delivery{}}
		set := &settler{src: src}
		ref := &delivery{id: "m", src: src}
		ref.extend(time.Now().Add(time.Minute))
		ref.state.Store(tc.state)
		d := mq.DriverDelivery{Ref: ref}
		if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, tc.want) {
			t.Errorf("Extend of a delivery in state %d returned %v, want %v", tc.state, err, tc.want)
		}
		if api.calls != 0 {
			t.Errorf("Extend of a delivery in state %d sent %d ChangeMessageVisibility calls, want 0: the settlement owns the window", tc.state, api.calls)
		}
	}
}

// refusedDuringRedelivery is an API whose DeleteMessageBatch records the
// receipt handles it is sent, runs a hook while the delete is in flight, and
// then refuses the delete with a throttle.
type refusedDuringRedelivery struct {
	sqsapi.API
	during  func()
	handles []string
}

func (r *refusedDuringRedelivery) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	for _, e := range in.Entries {
		r.handles = append(r.handles, aws.ToString(e.ReceiptHandle))
	}
	if r.during != nil {
		r.during()
		r.during = nil
	}
	return nil, &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}
}

// TestARefusedSettlementLeavesASupersededDeliveryLost holds that a delivery
// the source marked lost while its delete was in flight stays lost when the
// service refuses the delete. Here the message is received again during the
// delete, so the source now vouches for the new receipt. The refusal used to
// put the old delivery back in the active state over that loss, and a retried
// Ack sent DeleteMessageBatch again with the stale receipt handle, which the
// service can accept and use to delete the copy the new receipt holds.
func TestARefusedSettlementLeavesASupersededDeliveryLost(t *testing.T) {
	api := &refusedDuringRedelivery{}
	src := &source{b: &Backend{api: api, errs: newErrorMapper()}, inflight: map[string]*delivery{}, url: "q"}
	set := &settler{src: src}
	first := &delivery{id: "m", handle: "h-old", gen: 1, src: src}
	first.extend(time.Now().Add(time.Minute))
	src.supersede(first)
	api.during = func() {
		second := &delivery{id: "m", handle: "h-new", gen: 2, src: src}
		second.extend(time.Now().Add(time.Minute))
		src.supersede(second)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	d := mq.DriverDelivery{Ref: first, Gen: 1}
	if err := set.Ack(ctx, &d); mq.CodeOf(err) != mq.CodeThrottled {
		t.Fatalf("the first Ack returned %v, want the throttle the service answered", err)
	}
	if got := first.state.Load(); got != stateLost {
		t.Errorf("after a redelivery superseded it, the refused delivery is in state %d, want stateLost (%d)", got, stateLost)
	}
	if err := set.Ack(ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the retried Ack of the superseded delivery returned %v, want ErrLeaseLost", err)
	}
	if len(api.handles) != 1 {
		t.Errorf("DeleteMessageBatch was sent the receipt handles %q; the retry must not send the stale %q again",
			api.handles, first.handle)
	}
}
