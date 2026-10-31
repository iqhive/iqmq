package sqs

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
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

// TestASettlementsAnswerOutlivesItsWindow holds a second settlement of a
// delivery whose settlement has finished to that settlement's answer after the
// delivery's window has closed as well. On SQS a delete that did not land
// brings the message back only once its window has closed, so the redelivery
// an ambiguous Ack is followed by comes after the window. The lease check came
// before the state: a second settlement then reported ErrLeaseLost and stored
// the lost state over the sticky unknown one, and over a settled one too.
func TestASettlementsAnswerOutlivesItsWindow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		api   sqsapi.API
		state uint32
		want  error
	}{
		{"unknown", &unansweredDelete{}, stateUnknown, mq.ErrSettlementUnknown},
		{"settled", &recordingService{}, stateSettled, mq.ErrAlreadySettled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, d := settleable(tc.api)
			ref := d.Ref.(*delivery)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			_ = set.Ack(ctx, d)
			if got := ref.state.Load(); got != tc.state {
				t.Fatalf("the first Ack left the delivery in state %d, want %d", got, tc.state)
			}
			// The window closes and the message comes back, or not.
			ref.extend(time.Now().Add(-time.Second))
			redelivered := &delivery{id: ref.id, handle: "h2", gen: 2, src: ref.src, url: ref.url}
			redelivered.extend(time.Now().Add(time.Minute))
			ref.src.supersede(redelivered)
			if err := set.Ack(ctx, d); !errors.Is(err, tc.want) {
				t.Errorf("a second Ack after the window reported %v, want %v", err, tc.want)
			}
			if err := set.Extend(ctx, d, time.Second); !errors.Is(err, tc.want) {
				t.Errorf("an Extend after the window reported %v, want %v", err, tc.want)
			}
			if got := ref.state.Load(); got != tc.state {
				t.Errorf("the delivery is in state %d after the second settlement, want %d: its answer was overwritten",
					got, tc.state)
			}
		})
	}
}

// recordingService answers settlements the way the SDK and the service do: a
// request whose context is already done fails with the context's error before
// anything is sent, and every other one succeeds. sent counts the requests that
// reached the service. When extending is set, an extension closes it on
// arrival and is answered once hold is closed.
type recordingService struct {
	sqsapi.API
	sent      atomic.Int32
	extending chan struct{}
	hold      chan struct{}
	// copied, when set, runs once a dead-letter copy has been sent.
	copied func()
}

func (r *recordingService) GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL(aws.ToString(in.QueueName)))}, nil
}

func (r *recordingService) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sent.Add(1)
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
	}
	return out, nil
}

func (r *recordingService) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sent.Add(1)
	out := &sqs.ChangeMessageVisibilityBatchOutput{}
	for _, e := range in.Entries {
		out.Successful = append(out.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: e.Id})
	}
	return out, nil
}

func (r *recordingService) ChangeMessageVisibility(ctx context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sent.Add(1)
	if r.extending != nil {
		close(r.extending)
		<-r.hold
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (r *recordingService) SendMessage(ctx context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sent.Add(1)
	if r.copied != nil {
		r.copied()
	}
	return &sqs.SendMessageOutput{MessageId: aws.String("copy")}, nil
}

func (r *recordingService) DeleteMessage(ctx context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sent.Add(1)
	return &sqs.DeleteMessageOutput{}, nil
}

// queueURL is the URL of a queue of the account and region these tests use.
func queueURL(name string) string {
	return "https://sqs.ap-southeast-2.amazonaws.com/000000000000/" + name
}

// settleable is a settler over api and one active delivery of it, with a
// dead-letter queue configured by name, so a Reject has to look its URL up.
func settleable(api sqsapi.API) (*settler, *mq.DriverDelivery) {
	cfg := Config{DeadLetterQueue: "dlq", APITimeout: time.Minute}
	src := &source{
		b:        &Backend{api: api, cfg: cfg, errs: newErrorMapper(), res: newResolver(api, cfg)},
		inflight: map[string]*delivery{},
		url:      queueURL("q"),
	}
	ref := &delivery{id: "m", handle: "h", gen: 1, src: src, url: src.url, native: &types.Message{Body: aws.String("m")}}
	ref.extend(time.Now().Add(time.Hour))
	src.supersede(ref)
	return &settler{src: src}, &mq.DriverDelivery{Ref: ref, Gen: 1}
}

// TestASettlementWhoseContextEndedBeforeItsRequestIsDefinite holds a
// settlement whose context is done before its request is made to a definite
// failure. The SDK refuses such a request before anything is sent, and the
// settler mapped every failure as one sent already, so the settlement was
// reported ambiguous and the delivery was stuck unknown: a settlement that
// never happened could never be retried, and the message came back after its
// window to be processed again. An extension is held to the same rule. The
// consumer's settlement context ends this way when a shutdown interrupts it.
func TestASettlementWhoseContextEndedBeforeItsRequestIsDefinite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(context.Context, *settler, *mq.DriverDelivery) error
	}{
		{"Ack", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error { return s.Ack(ctx, d) }},
		{"requeue", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionRequeue})
		}},
		{"drop", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDrop})
		}},
		{"dead-letter", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error {
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		}},
		{"dead-letter, queue URL known", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error {
			if _, err := s.src.b.res.URLOfName(context.Background(), "dlq"); err != nil {
				return err
			}
			return s.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		}},
		{"Extend", func(ctx context.Context, s *settler, d *mq.DriverDelivery) error {
			return s.Extend(ctx, d, time.Minute)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &recordingService{}
			set, d := settleable(api)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := tc.settle(ctx, set, d)
			if n := api.sent.Load(); n != 0 {
				t.Fatalf("%d request(s) reached the service with a done context", n)
			}
			if err == nil || mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
				t.Errorf("a settlement that sent nothing reported %v (ambiguous %v), want a definite failure",
					err, mq.IsAmbiguous(err))
			}
			if err := tc.settle(t.Context(), set, d); err != nil {
				t.Errorf("the settlement retried with a live context reported %v, want success: the first sent nothing", err)
			}
		})
	}
}

// TestASettlementWaitingForAnExtensionHonoursItsContext: a settlement claims
// its delivery only once an extension in flight has been answered, so that its
// own request reaches the service last. That wait was a mutex, so a settlement
// whose context ended meanwhile waited for the extension's whole round trip,
// up to Config.APITimeout with the SDK's retries inside, and every delivery
// after it in the same batch waited too. It ends with the context now, as a
// definite failure that leaves the delivery settleable.
func TestASettlementWaitingForAnExtensionHonoursItsContext(t *testing.T) {
	api := &recordingService{extending: make(chan struct{}), hold: make(chan struct{})}
	set, d := settleable(api)
	extended := make(chan error, 1)
	go func() { extended <- set.Extend(context.Background(), d, time.Minute) }()
	<-api.extending
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	acked := make(chan error, 1)
	go func() { acked <- set.Ack(ctx, d) }()
	var err error
	select {
	case err = <-acked:
	case <-time.After(5 * time.Second):
		close(api.hold)
		t.Logf("the extension returned %v", <-extended)
		t.Fatalf("an Ack whose context ended after 100ms was still waiting 5s later behind an extension in "+
			"flight; once the extension was answered it returned %v", <-acked)
	}
	close(api.hold)
	if err := <-extended; err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err == nil || mq.IsAmbiguous(err) {
		t.Errorf("the Ack whose context ended while it waited reported %v, want a definite failure", err)
	}
	if n := api.sent.Load(); n != 1 {
		t.Errorf("%d request(s) reached the service, want the extension alone", n)
	}
	if err := set.Ack(t.Context(), d); err != nil {
		t.Errorf("the Ack retried once the extension was answered reported %v, want success", err)
	}
}

// TestADeadLetterDeleteWhoseContextEndedAfterTheCopyIsDefinite: a Reject
// whose context ends once the copy is on the dead-letter queue sends no
// delete. Nothing it could not take back reached the service, so the failure
// is definite, as a refused delete's is: the message is on both queues, which
// is a duplicate and not a loss, and the delivery stays settleable.
func TestADeadLetterDeleteWhoseContextEndedAfterTheCopyIsDefinite(t *testing.T) {
	api := &recordingService{}
	set, d := settleable(api)
	ctx, cancel := context.WithCancel(t.Context())
	api.copied = cancel
	err := set.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if n := api.sent.Load(); n != 1 {
		t.Fatalf("%d request(s) reached the service, want the copy alone", n)
	}
	if err == nil || mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
		t.Errorf("a Reject whose delete was never sent reported %v (ambiguous %v), want a definite failure",
			err, mq.IsAmbiguous(err))
	}
	api.copied = nil
	if err := set.Nack(t.Context(), d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Errorf("the Reject retried with a live context reported %v, want success", err)
	}
}

// hookClock is the wall clock, which runs hook the first time it is read.
type hookClock struct {
	ran  atomic.Bool
	hook func()
}

func (c *hookClock) Now() time.Time {
	if c.ran.CompareAndSwap(false, true) {
		c.hook()
	}
	return time.Now()
}

func (*hookClock) NewTimer(time.Duration) mq.Timer { panic("not used") }

// TestASettlementWaitsForAnExtensionThatBeganDuringItsClaim: a claim waits for
// an extension in flight before it reads the lease, and an extension that
// begins after that wait and before the claim finds the delivery still active
// and goes to the service. The claim waits for that one too once it holds the
// delivery, so its delete reaches the service after the extension. The clock
// the claim reads its lease from is where the extension is started.
func TestASettlementWaitsForAnExtensionThatBeganDuringItsClaim(t *testing.T) {
	api := &recordingService{extending: make(chan struct{}), hold: make(chan struct{})}
	set, d := settleable(api)
	extended := make(chan error, 1)
	set.src.b.clock = &hookClock{hook: func() {
		go func() { extended <- set.Extend(context.Background(), d, time.Minute) }()
		// Bounded, so that a claim the extension has to wait for fails
		// the test rather than hanging it.
		select {
		case <-api.extending:
		case <-time.After(5 * time.Second):
		}
	}}
	acked := make(chan error, 1)
	go func() { acked <- set.Ack(t.Context(), d) }()
	select {
	case err := <-acked:
		t.Errorf("the Ack returned (%v) while an extension that found the delivery active was still on its way, "+
			"so its delete went first (%d request(s) sent)", err, api.sent.Load())
		acked <- err
	case <-time.After(100 * time.Millisecond):
	}
	if n := api.sent.Load(); n != 1 {
		t.Errorf("%d request(s) reached the service before the extension was answered, want the extension alone", n)
	}
	close(api.hold)
	if err := <-extended; err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := <-acked; err != nil {
		t.Fatalf("Ack: %v", err)
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
