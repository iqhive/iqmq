package future_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/future"
)

// asyncStub is an mq.AsyncPublisher whose completions the test drives by
// hand: PublishAsync records the callback instead of running it, so a test
// can decide when — and whether — a publication resolves.
type asyncStub struct {
	mu      sync.Mutex
	pending []func(mq.PublishResult)
	msgs    []*mq.Message
	// refuse is what PublishAsync returns instead of admitting the
	// publication. A refused publication never runs its callback.
	refuse error
}

func (s *asyncStub) Publish(context.Context, *mq.Message, ...mq.PublishOption) error { return nil }

func (s *asyncStub) PublishBatch(context.Context, []*mq.Message, ...mq.PublishOption) error {
	return nil
}

func (s *asyncStub) Flush(context.Context) error { return nil }

func (s *asyncStub) Close() error { return nil }

func (s *asyncStub) PublishAsync(_ context.Context, m *mq.Message, done func(mq.PublishResult), _ ...mq.PublishOption) error {
	if s.refuse != nil {
		return s.refuse
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, done)
	s.msgs = append(s.msgs, m)
	return nil
}

// complete runs the i-th recorded callback with res.
func (s *asyncStub) complete(i int, res mq.PublishResult) {
	s.mu.Lock()
	done := s.pending[i]
	s.mu.Unlock()
	done(res)
}

var _ mq.AsyncPublisher = (*asyncStub)(nil)

func TestPublishResolvesWithTheBrokersOutcome(t *testing.T) {
	s := &asyncStub{}
	m := &mq.Message{Topic: "orders", Body: []byte("x")}
	f, err := future.Publish(context.Background(), s, m)
	if err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	if f.Resolved() {
		t.Fatal("the future resolved before the publication completed")
	}
	if _, ok := f.Result(); ok {
		t.Fatal("Result reported an outcome before the publication completed")
	}
	s.complete(0, mq.PublishResult{Message: m, Receipt: mq.PublishReceipt{MessageID: "srv-1", Offset: 7}})
	if err := f.Wait(context.Background()); err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
	if !f.Resolved() {
		t.Fatal("Resolved is false after the publication completed")
	}
	r, err := f.Receipt(context.Background())
	if err != nil {
		t.Fatalf("Receipt = %v, want nil", err)
	}
	if r.MessageID != "srv-1" || r.Offset != 7 {
		t.Fatalf("receipt = %+v, want the broker's own (srv-1, 7)", r)
	}
}

func TestWaitReportsThePublicationsFailure(t *testing.T) {
	s := &asyncStub{}
	f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
	if err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	want := errors.New("broker refused it")
	s.complete(0, mq.PublishResult{Err: want})
	if err := f.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Wait = %v, want the publication's own error", err)
	}
	if _, err := f.Receipt(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Receipt = %v, want the publication's own error", err)
	}
}

func TestARefusedPublicationReturnsNoFuture(t *testing.T) {
	s := &asyncStub{refuse: mq.ErrClosed}
	f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
	if !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish = %v, want ErrClosed", err)
	}
	if f != nil {
		t.Fatalf("Publish returned a future (%p) for a publication that was never admitted; nothing would ever resolve it", f)
	}
}

func TestWaitEndsOnItsContextAndLeavesThePublicationOutstanding(t *testing.T) {
	s := &asyncStub{}
	f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
	if err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait on a cancelled context = %v, want context.Canceled", err)
	}
	if f.Resolved() {
		t.Fatal("a cancelled wait resolved the future; the publication is still outstanding")
	}
	// The publication still completes, and a later wait still sees it.
	s.complete(0, mq.PublishResult{Receipt: mq.PublishReceipt{MessageID: "late"}})
	r, err := f.Receipt(context.Background())
	if err != nil {
		t.Fatalf("Receipt after the cancelled wait = %v, want nil", err)
	}
	if r.MessageID != "late" {
		t.Fatalf("receipt = %+v, want the outcome that arrived after the cancelled wait", r)
	}
}

func TestResolutionIsIdempotentAndSeenByEveryWaiter(t *testing.T) {
	s := &asyncStub{}
	f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
	if err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	const waiters = 8
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = f.Wait(context.Background())
		}()
	}
	want := errors.New("one answer for all of them")
	s.complete(0, mq.PublishResult{Err: want})
	// A second completion must not panic on a closed channel: the core
	// promises exactly one callback, and this is the belt to that brace.
	s.complete(0, mq.PublishResult{Err: errors.New("a second, contradictory outcome")})
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, want) {
			t.Fatalf("waiter %d saw %v, want the one resolution %v", i, err, want)
		}
	}
}

func TestResolvedFollowsTheCompletion(t *testing.T) {
	s := &asyncStub{}
	f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
	if err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	if f.Resolved() {
		t.Fatal("Resolved reported true before the publication completed")
	}
	s.complete(0, mq.PublishResult{})
	if !f.Resolved() {
		t.Fatal("Resolved reported false after the completion")
	}
}

func TestWaitAllReportsTheBatchOutcome(t *testing.T) {
	ctx := context.Background()
	t.Run("all succeed", func(t *testing.T) {
		s := &asyncStub{}
		fs := publishN(t, s, 3)
		for i := range 3 {
			s.complete(i, mq.PublishResult{})
		}
		if err := future.WaitAll(ctx, fs...); err != nil {
			t.Fatalf("WaitAll = %v, want nil", err)
		}
	})
	t.Run("the only publication fails", func(t *testing.T) {
		// This subtest asserted that THREE publications, two of them
		// accepted, answer with the one failure unwrapped and never a
		// *mq.BatchError -- citing as forbidding it. That is a
		// misreading, which forbids a BatchError on the path where
		// every message was ACCEPTED, and of [mq.BatchError]'s own contract,
		// which says the single error is the answer for "one in which
		// nothing was [accepted], and every item has the same outcome". Two
		// acceptances beside one refusal is the partial batch a BatchError
		// is for, and it is now asserted as such by
		// TestWaitAllDoesNotReadAPartialFailureAsATotalOne. What survives here is the collapse that stayed: a batch
		// nothing of which was accepted.
		s := &asyncStub{}
		fs := publishN(t, s, 1)
		want := errors.New("the only one, and it failed")
		s.complete(0, mq.PublishResult{Err: want})
		err := future.WaitAll(ctx, fs...)
		if !errors.Is(err, want) {
			t.Fatalf("WaitAll = %v, want the single failure unwrapped", err)
		}
		var be *mq.BatchError
		if errors.As(err, &be) {
			t.Fatalf("WaitAll built a *mq.BatchError for a wholly failed batch of one: %v", err)
		}
	})
	t.Run("two fail, index-aligned", func(t *testing.T) {
		s := &asyncStub{}
		fs := publishN(t, s, 4)
		first, third := errors.New("message 1"), errors.New("message 3")
		s.complete(0, mq.PublishResult{})
		s.complete(1, mq.PublishResult{Err: first})
		s.complete(2, mq.PublishResult{})
		s.complete(3, mq.PublishResult{Err: third})
		err := future.WaitAll(ctx, fs...)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Fatalf("WaitAll = %v, want a *mq.BatchError", err)
		}
		if len(be.Errs) != 4 {
			t.Fatalf("BatchError.Errs has %d entries, want one per future (4)", len(be.Errs))
		}
		if be.Errs[0] != nil || be.Errs[2] != nil {
			t.Fatalf("BatchError.Errs = %v, want nil at the indices that succeeded", be.Errs)
		}
		if !errors.Is(be.Errs[1], first) || !errors.Is(be.Errs[3], third) {
			t.Fatalf("BatchError.Errs = %v, want each failure at its own index", be.Errs)
		}
	})
	t.Run("nil entries are skipped", func(t *testing.T) {
		s := &asyncStub{}
		fs := publishN(t, s, 1)
		s.complete(0, mq.PublishResult{})
		if err := future.WaitAll(ctx, nil, fs[0], nil); err != nil {
			t.Fatalf("WaitAll with nil entries = %v, want nil", err)
		}
	})
	t.Run("a cancelled context is not a batch outcome", func(t *testing.T) {
		// Two of the three publications have resolved -- one of them with a
		// real failure -- and the third is still outstanding when the
		// context ends. The batch's outcome is therefore not known, and
		// reporting the two that did resolve would be a claim about the one
		// that did not.
		s := &asyncStub{}
		fs := publishN(t, s, 3)
		s.complete(0, mq.PublishResult{Err: errors.New("this one really failed")})
		s.complete(1, mq.PublishResult{})
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		err := future.WaitAll(cctx, fs...)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitAll on a cancelled context = %v, want context.Canceled", err)
		}
		var be *mq.BatchError
		if errors.As(err, &be) {
			t.Fatalf("WaitAll reported a partial batch outcome (%v) for publications still outstanding", err)
		}
	})
	t.Run("a resolved future answers over a dead context", func(t *testing.T) {
		// Both channels are ready, and a plain select over two ready
		// channels picks at random: without the ready-first check this
		// fails about half the time, which is worse than failing.
		s := &asyncStub{}
		fs := publishN(t, s, 1)
		s.complete(0, mq.PublishResult{Receipt: mq.PublishReceipt{MessageID: "resolved"}})
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		for i := range 200 {
			r, err := fs[0].Receipt(cctx)
			if err != nil {
				t.Fatalf("round %d: Receipt on a resolved future = %v, want the publication's own outcome", i, err)
			}
			if r.MessageID != "resolved" {
				t.Fatalf("round %d: receipt = %+v, want the broker's own", i, r)
			}
		}
	})
}

// TestWaitAllDoesNotReadAPartialFailureAsATotalOne is the rule for a batch
// that was not uniformly refused.
//
// WaitAll collapsed to the single error whenever exactly ONE future failed,
// however many succeeded beside it, so a batch of a thousand publications
// with one refusal answered with that refusal and nothing else — the same
// value a batch of one whose only publication was refused answers with. A
// caller cannot tell those apart, and the specification forbids blindly retrying a
// partially successful batch: republishing "the batch" duplicates the 999 the
// broker accepted. The collapse is only sound when NOTHING was accepted,
// which is what [mq.BatchError]'s own contract says.
func TestWaitAllDoesNotReadAPartialFailureAsATotalOne(t *testing.T) {
	ctx := context.Background()
	t.Run("one of three failed", func(t *testing.T) {
		s := &asyncStub{}
		fs := publishN(t, s, 3)
		want := errors.New("only this one failed")
		s.complete(0, mq.PublishResult{})
		s.complete(1, mq.PublishResult{Err: want})
		s.complete(2, mq.PublishResult{})
		err := future.WaitAll(ctx, fs...)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Fatalf("WaitAll(accepted, refused, accepted) = %v, want an index-aligned *mq.BatchError: "+
				"two of the three publications were ACCEPTED and this answer cannot say so", err)
		}
		if len(be.Errs) != 3 || be.Errs[0] != nil || be.Errs[2] != nil || !errors.Is(be.Errs[1], want) {
			t.Fatalf("BatchError.Errs = %v, want [nil, %v, nil]", be.Errs, want)
		}
		if !errors.Is(err, want) || !errors.Is(err, mq.ErrPartial) {
			t.Fatalf("WaitAll = %v: want errors.Is to reach both the item's cause and ErrPartial", err)
		}
	})
	t.Run("a nil entry is an outcome that succeeded", func(t *testing.T) {
		// A nil entry is documented as a publication that already succeeded,
		// so a batch holding one is not wholly failed either.
		s := &asyncStub{}
		fs := publishN(t, s, 1)
		want := errors.New("the only publication attempted")
		s.complete(0, mq.PublishResult{Err: want})
		err := future.WaitAll(ctx, nil, fs[0])
		var be *mq.BatchError
		if !errors.As(err, &be) || len(be.Errs) != 2 || be.Errs[0] != nil || !errors.Is(be.Errs[1], want) {
			t.Fatalf("WaitAll(nil, refused) = %v, want an index-aligned *mq.BatchError [nil, %v]", err, want)
		}
	})
	t.Run("the whole batch failed and it is one message", func(t *testing.T) {
		// The collapse that is still right: nothing was accepted, so the
		// single error misattributes nothing. This is the mutation guard —
		// a fix that always built a *BatchError would fail here.
		s := &asyncStub{}
		fs := publishN(t, s, 1)
		want := errors.New("refused")
		s.complete(0, mq.PublishResult{Err: want})
		err := future.WaitAll(ctx, fs...)
		if err != want { //nolint:errorlint // identity: the publication's own error, unwrapped
			t.Fatalf("WaitAll(refused) = %v, want the publication's own error unwrapped", err)
		}
	})
}

// publishN publishes n messages through s and returns their futures.
func publishN(t *testing.T, s *asyncStub, n int) []*future.Future {
	t.Helper()
	fs := make([]*future.Future, n)
	for i := range n {
		f, err := future.Publish(context.Background(), s, &mq.Message{Body: []byte("x")})
		if err != nil {
			t.Fatalf("Publish %d = %v, want nil", i, err)
		}
		fs[i] = f
	}
	return fs
}

func TestNewHandsBackTheCallbackThatResolvesIt(t *testing.T) {
	f, done := future.New()
	if f.Resolved() {
		t.Fatal("a fresh future is already resolved")
	}
	done(mq.PublishResult{Receipt: mq.PublishReceipt{MessageID: "m"}})
	res, ok := f.Result()
	if !ok {
		t.Fatal("Result reported no outcome after the callback ran")
	}
	if res.Receipt.MessageID != "m" {
		t.Fatalf("result = %+v, want the callback's own", res)
	}
}
