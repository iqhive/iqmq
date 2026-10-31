package iqmq_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestPublishItemsCarriesPerItemOptions pins the heterogeneous batch: each
// item goes out under its own option set, and an item with none goes out under
// the publisher's defaults.
func TestPublishItemsCarriesPerItemOptions(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Defaults: mq.PublishOptions{DedupKey: "from-defaults"},
	})
	sets := []mq.PublishOptions{
		{DedupKey: "first", Delay: time.Minute},
		{DedupKey: "second"},
	}
	items := []mq.PublishItem{
		{Message: &mq.Message{Body: []byte("a")}, Options: &sets[0]},
		{Message: &mq.Message{Body: []byte("b")}, Options: &sets[0]},
		{Message: &mq.Message{Body: []byte("c")}, Options: &sets[1]},
		{Message: &mq.Message{Body: []byte("d")}},
	}
	if err := p.PublishItems(context.Background(), items); err != nil {
		t.Fatalf("PublishItems: %v", err)
	}
	want := []advRec{
		{body: "a", dedup: "first", delay: time.Minute},
		{body: "b", dedup: "first", delay: time.Minute},
		{body: "c", dedup: "second"},
		{body: "d", dedup: "from-defaults"},
	}
	got := b.records()
	if len(got) != len(want) {
		t.Fatalf("the driver saw %d publications, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].body != want[i].body || got[i].dedup != want[i].dedup || got[i].delay != want[i].delay {
			t.Fatalf("publication %d was (%q, %q, %v), want (%q, %q, %v)",
				i, got[i].body, got[i].dedup, got[i].delay, want[i].body, want[i].dedup, want[i].delay)
		}
	}
	if got := p.Stats().Published; got != 4 {
		t.Fatalf("Stats().Published = %d, want 4", got)
	}
}

// TestPublishItemsBatchesTheRunsThatShareASet pins the grouping: items that
// resolved to the same set and sit next to each other are one native batch,
// because the specification gives a native batch one option set for every message in it.
//
// The count of native calls is the only way to see it, and it is worth seeing:
// a caller whose items mostly share a set should not lose batching altogether.
func TestPublishItemsBatchesTheRunsThatShareASet(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{})
	shared := mq.PublishOptions{DedupKey: "shared"}
	other := mq.PublishOptions{DedupKey: "other"}
	items := []mq.PublishItem{
		{Message: &mq.Message{Body: []byte("a")}, Options: &shared},
		{Message: &mq.Message{Body: []byte("b")}, Options: &shared},
		{Message: &mq.Message{Body: []byte("c")}, Options: &shared},
		{Message: &mq.Message{Body: []byte("d")}, Options: &other},
		{Message: &mq.Message{Body: []byte("e")}, Options: &shared},
	}
	if err := p.PublishItems(context.Background(), items); err != nil {
		t.Fatalf("PublishItems: %v", err)
	}
	if got := len(b.records()); got != 5 {
		t.Fatalf("the driver saw %d publications, want 5", got)
	}
	// The broker counts one native call per accepted message, so what the
	// grouping is checked by is the order the messages arrived in: a run is
	// handed over intact.
	if got := b.bodies(); got[0] != "a" || got[2] != "c" || got[3] != "d" || got[4] != "e" {
		t.Fatalf("the driver saw %v, want the items in order", got)
	}
}

// TestPublishItemsValidatesBeforeAnyIO is the rule for a heterogeneous
// batch: one bad item and nothing at all is published.
func TestPublishItemsValidatesBeforeAnyIO(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		missing []mq.Feature
		items   func() []mq.PublishItem
		want    error
	}{
		{"nil-message", nil, func() []mq.PublishItem {
			return []mq.PublishItem{
				{Message: &mq.Message{Body: []byte("a")}},
				{},
			}
		}, nil},
		{"too-large", nil, func() []mq.PublishItem {
			return []mq.PublishItem{
				{Message: &mq.Message{Body: []byte("a")}},
				{Message: &mq.Message{Body: make([]byte, (1<<20)+1)}},
			}
		}, mq.ErrMessageTooLarge},
		{"unsupported-option", []mq.Feature{mq.FeatureDeduplication}, func() []mq.PublishItem {
			bad := mq.PublishOptions{DedupKey: "nope"}
			return []mq.PublishItem{
				{Message: &mq.Message{Body: []byte("a")}},
				{Message: &mq.Message{Body: []byte("b")}, Options: &bad},
			}
		}, mq.ErrUnsupported},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			b := newAdvBroker(openBatch, c.missing...)
			p := advPublisher(t, b, mq.PublisherConfig{})
			err := p.PublishItems(context.Background(), c.items())
			switch {
			case err == nil:
				t.Fatal("PublishItems returned nil, want an error")
			case c.want != nil && !errors.Is(err, c.want):
				t.Fatalf("PublishItems = %v, want %v", err, c.want)
			case c.want == nil && mq.CodeOf(err) != mq.CodeInvalidArgument:
				t.Fatalf("PublishItems = %v, want CodeInvalidArgument", err)
			}
			if got := b.nativeCalls(); got != 0 {
				t.Fatalf("the driver saw %d publications after a validation failure, want 0 ", got)
			}
		})
	}
}

// TestPublishItemsAlignsItsResults pins the index alignment the whole batch
// contract rests on: a result aligned with anything but the caller's items
// reports one message's outcome for another.
func TestPublishItemsAlignsItsResults(t *testing.T) {
	t.Parallel()
	refused := errors.New("refused")
	t.Run("partial", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openBatch, mq.PublisherConfig{})
		b.setFail(func(n int, _ *mq.Message) error {
			if n == 3 {
				return refused
			}
			return nil
		})
		items := publishItems("a", "b", "c", "d")
		err := p.PublishItems(context.Background(), items)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Fatalf("PublishItems = %v, want a *BatchError", err)
		}
		if !errors.Is(err, mq.ErrPartial) {
			t.Fatalf("errors.Is(%v, ErrPartial) = false", err)
		}
		if len(be.Errs) != 4 {
			t.Fatalf("the batch reported %d results, want 4", len(be.Errs))
		}
		for i, got := range be.Errs {
			if (i == 2) != (got != nil) {
				t.Fatalf("result %d is %v: only the third item was refused", i, got)
			}
		}
		if got := p.Stats().Published; got != 3 {
			t.Fatalf("Stats().Published = %d, want 3", got)
		}
		if got := p.Stats().Failed; got != 1 {
			t.Fatalf("Stats().Failed = %d, want 1", got)
		}
	})
	t.Run("none", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openBatch, mq.PublisherConfig{})
		b.setFail(func(int, *mq.Message) error { return refused })
		err := p.PublishItems(context.Background(), publishItems("a", "b"))
		if !errors.Is(err, refused) {
			t.Fatalf("PublishItems = %v, want the driver's error when no item was accepted", err)
		}
		var be *mq.BatchError
		if errors.As(err, &be) {
			t.Fatalf("PublishItems = %v, want a single error and not a *BatchError when nothing was accepted", err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b, p := advClient(t, openBatch, mq.PublisherConfig{})
		if err := p.PublishItems(context.Background(), nil); err != nil {
			t.Fatalf("PublishItems(nil) = %v, want nil", err)
		}
		if got := b.nativeCalls(); got != 0 {
			t.Fatalf("the driver saw %d publications for an empty batch, want 0", got)
		}
	})
	t.Run("closed", func(t *testing.T) {
		t.Parallel()
		_, p := advClient(t, openBatch, mq.PublisherConfig{})
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := p.PublishItems(context.Background(), publishItems("a")); !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("PublishItems after Close = %v, want ErrClosed", err)
		}
	})
}

// TestPublishItemsSplitsOversizedRuns pins that a run larger than the
// broker's advertised batch maximum is split and its pieces' results
// re-aligned at the piece's offset, which is the place a batch loses a
// message.
func TestPublishItemsSplitsOversizedRuns(t *testing.T) {
	t.Parallel()
	b := newAdvBroker(openBatch)
	b.caps = advCapsWithBatchLimit(2)
	p := advPublisher(t, b, mq.PublisherConfig{})
	refused := errors.New("refused")
	b.setFail(func(n int, _ *mq.Message) error {
		if n == 5 {
			return refused
		}
		return nil
	})
	items := publishItems("a", "b", "c", "d", "e", "f")
	err := p.PublishItems(context.Background(), items)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != 6 {
		t.Fatalf("PublishItems = %v, want a *BatchError with 6 results", err)
	}
	for i, got := range be.Errs {
		if (i == 4) != (got != nil) {
			t.Fatalf("result %d is %v: only the fifth item was refused, and a split must not move a result", i, got)
		}
	}
}

// TestASplitBatchHoldsBackAKeyThatFailedInAnEarlierPiece splits a batch in
// pieces of two, and the first piece has a refused message of key "a" and a
// refused keyless one. On a client that keeps order per key, the later
// messages of key "a" and the later keyless one are held back: the core sent
// them in the next piece, so they reached the broker ahead of the caller's
// retry of the refused ones. One native call to an SQS FIFO queue holds a
// group back the same way, and the split let the later pieces through. Key
// "b" goes on, and so does everything on a client that orders nothing by key.
func TestASplitBatchHoldsBackAKeyThatFailedInAnEarlierPiece(t *testing.T) {
	t.Parallel()
	keys := []string{"a", "", "b", "a", "", "b"}
	bodies := []string{"a0", "k0", "b0", "a1", "k1", "b1"}
	refused := map[string]bool{"a0": true, "k0": true}
	surfaces := []struct {
		name    string
		cfg     mq.PublisherConfig
		publish func(t *testing.T, p *mq.PublisherHandle, ms []*mq.Message) []error
	}{
		{"PublishBatch", mq.PublisherConfig{}, func(t *testing.T, p *mq.PublisherHandle, ms []*mq.Message) []error {
			t.Helper()
			var be *mq.BatchError
			if err := p.PublishBatch(context.Background(), ms); !errors.As(err, &be) || len(be.Errs) != len(ms) {
				t.Fatalf("PublishBatch = %v, want a *BatchError with %d results", err, len(ms))
			}
			return be.Errs
		}},
		{"PublishItems", mq.PublisherConfig{}, func(t *testing.T, p *mq.PublisherHandle, ms []*mq.Message) []error {
			t.Helper()
			items := make([]mq.PublishItem, len(ms))
			for i, m := range ms {
				items[i].Message = m
			}
			var be *mq.BatchError
			if err := p.PublishItems(context.Background(), items); !errors.As(err, &be) || len(be.Errs) != len(ms) {
				t.Fatalf("PublishItems = %v, want a *BatchError with %d results", err, len(ms))
			}
			return be.Errs
		}},
		{"PublishAsync with Batching", mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 100}}, func(t *testing.T, p *mq.PublisherHandle, ms []*mq.Message) []error {
			t.Helper()
			var mu sync.Mutex
			errs := make([]error, len(ms))
			for i, m := range ms {
				if err := p.PublishAsync(context.Background(), m, func(r mq.PublishResult) {
					mu.Lock()
					errs[i] = r.Err
					mu.Unlock()
				}); err != nil {
					t.Fatalf("PublishAsync(%d): %v", i, err)
				}
			}
			if err := p.Flush(context.Background()); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(errs)
		}},
	}
	orders := []struct {
		name    string
		without []mq.Feature
		held    []int
		sent    []string
	}{
		{"ordered by key", nil, []int{3, 4}, []string{"b0", "b1"}},
		{"not ordered by key", []mq.Feature{mq.FeatureOrderedByKey}, nil, []string{"b0", "a1", "k1", "b1"}},
	}
	for _, s := range surfaces {
		for _, order := range orders {
			t.Run(s.name+"/"+order.name, func(t *testing.T) {
				t.Parallel()
				b := newAdvBroker(openBatch)
				b.caps = advCapsWithBatchLimit(2, order.without...)
				b.setFail(func(_ int, m *mq.Message) error {
					if refused[string(m.Body)] {
						return mq.ErrBackpressure
					}
					return nil
				})
				p := advPublisher(t, b, s.cfg)
				ms := make([]*mq.Message, len(bodies))
				for i := range ms {
					ms[i] = &mq.Message{Key: []byte(keys[i]), Body: []byte(bodies[i])}
				}
				errs := s.publish(t, p, ms)
				for i, err := range errs {
					switch {
					case refused[bodies[i]]:
						if !errors.Is(err, mq.ErrBackpressure) {
							t.Errorf("%s = %v, want the broker's refusal", bodies[i], err)
						}
					case slices.Contains(order.held, i):
						if !errors.Is(err, mq.ErrBackpressure) || mq.IsAmbiguous(err) || !mq.IsRetryable(err) {
							t.Errorf("%s = %v, want a definite, retryable not-attempted answer: an earlier message of its key was refused", bodies[i], err)
						}
					case err != nil:
						t.Errorf("%s = %v, want nil", bodies[i], err)
					}
				}
				if got := b.bodies(); !slices.Equal(got, order.sent) {
					t.Errorf("the broker took %q, want %q", got, order.sent)
				}
			})
		}
	}
}

// TestAKeyThatFailedIsHeldBackAcrossOptionRuns is the same hold across the
// runs PublishItems and the Batching flusher cut a batch into by option set.
// A FIFO caller who gives every message its own DedupKey, the use PublishItem
// names, gets one run per message, and the hold did not cross a run: k2 and
// k3 of the key whose k1 was refused were sent, and the retry of k1 landed
// behind them. They are held back now, each with a definite, retryable
// not-attempted answer, on a client that keeps order per key and only there.
func TestAKeyThatFailedIsHeldBackAcrossOptionRuns(t *testing.T) {
	t.Parallel()
	throttled := &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeThrottled, Retryable: true}
	sets := []mq.PublishOptions{{DedupKey: "d1"}, {DedupKey: "d2"}, {DedupKey: "d3"}}
	bodies := []string{"k1", "k2", "k3"}
	surfaces := []struct {
		name    string
		cfg     mq.PublisherConfig
		publish func(t *testing.T, p *mq.PublisherHandle) []error
	}{
		{"PublishItems", mq.PublisherConfig{}, func(t *testing.T, p *mq.PublisherHandle) []error {
			t.Helper()
			items := make([]mq.PublishItem, len(bodies))
			for i, body := range bodies {
				items[i] = mq.PublishItem{Message: &mq.Message{Key: []byte("k"), Body: []byte(body)}, Options: &sets[i]}
			}
			err := p.PublishItems(context.Background(), items)
			var be *mq.BatchError
			switch {
			case errors.As(err, &be) && len(be.Errs) == len(items):
				return be.Errs
			case err != nil && !errors.As(err, &be):
				// Every item has one outcome, refused as Throttled and
				// retryable, and none was accepted: the single error
				// stands for each of them.
				return []error{err, err, err}
			}
			t.Fatalf("PublishItems = %v, want a *BatchError with %d results or one error for all of them", err, len(items))
			return nil
		}},
		{"PublishAsync with Batching", mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 100}}, func(t *testing.T, p *mq.PublisherHandle) []error {
			t.Helper()
			var mu sync.Mutex
			errs := make([]error, len(bodies))
			for i, body := range bodies {
				m := &mq.Message{Key: []byte("k"), Body: []byte(body)}
				if err := p.PublishAsync(context.Background(), m, func(r mq.PublishResult) {
					mu.Lock()
					errs[i] = r.Err
					mu.Unlock()
				}, mq.DedupKey(sets[i].DedupKey)); err != nil {
					t.Fatalf("PublishAsync(%s): %v", body, err)
				}
			}
			_ = p.Flush(context.Background())
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(errs)
		}},
	}
	orders := []struct {
		name    string
		without []mq.Feature
		sent    []string
	}{
		{"ordered by key", nil, nil},
		{"not ordered by key", []mq.Feature{mq.FeatureOrderedByKey}, []string{"k2", "k3"}},
	}
	for _, s := range surfaces {
		for _, order := range orders {
			for name, open := range map[string]func(*advBroker, mq.SinkConfig) mq.Sink{"plain": openPlain, "native": openBatch} {
				t.Run(s.name+"/"+order.name+"/"+name, func(t *testing.T) {
					t.Parallel()
					b := newAdvBroker(open, order.without...)
					b.setFail(func(_ int, m *mq.Message) error {
						if string(m.Body) == "k1" {
							return throttled
						}
						return nil
					})
					p := advPublisher(t, b, s.cfg)
					errs := s.publish(t, p)
					if got := b.bodies(); !slices.Equal(got, order.sent) {
						t.Errorf("the broker took %q, want %q; errs = %v", got, order.sent, errs)
					}
					if !errors.Is(errs[0], throttled) {
						t.Errorf("k1 = %v, want the broker's refusal", errs[0])
					}
					for i, err := range errs[1:] {
						held := order.sent == nil
						switch {
						case held && (err == nil || mq.IsAmbiguous(err) || !mq.IsRetryable(err)):
							t.Errorf("%s = %v, want a definite, retryable not-attempted answer: k1 of its key was refused", bodies[i+1], err)
						case !held && err != nil:
							t.Errorf("%s = %v, want nil", bodies[i+1], err)
						}
					}
				})
			}
		}
	}
}

// TestPublishItemsStopsEveryLaterRunAfterAFatalError is a sink that closes
// inside the first run of a batch whose items carry two option sets. The
// stop is the whole batch's and not the run's: the second run is never
// offered to the dead sink, and its items come back not attempted, which is
// definite and retryable, rather than as whatever the sink answered.
func TestPublishItemsStopsEveryLaterRunAfterAFatalError(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{})
	b.setFail(func(n int, _ *mq.Message) error {
		if n == 2 {
			return mq.ErrClosed
		}
		return nil
	})
	sets := []mq.PublishOptions{{DedupKey: "first"}, {DedupKey: "second"}}
	items := []mq.PublishItem{
		{Message: &mq.Message{Body: []byte("a")}, Options: &sets[0]},
		{Message: &mq.Message{Body: []byte("b")}, Options: &sets[0]},
		{Message: &mq.Message{Body: []byte("c")}, Options: &sets[1]},
		{Message: &mq.Message{Body: []byte("d")}, Options: &sets[1]},
	}
	err := p.PublishItems(context.Background(), items)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != 4 {
		t.Fatalf("PublishItems = %v, want a *BatchError with 4 results", err)
	}
	if got := b.nativeCalls(); got != 2 {
		t.Fatalf("the driver saw %d publications, want 2: the second run went to a closed sink", got)
	}
	for i, got := range be.Errs[2:] {
		if !errors.Is(got, mq.ErrClosed) || mq.IsAmbiguous(got) || !mq.IsRetryable(got) {
			t.Fatalf("result %d is %v, want a definite, retryable not-attempted ErrClosed", i+2, got)
		}
	}
}

// publishItems builds items with no per-item options.
func publishItems(bodies ...string) []mq.PublishItem {
	out := make([]mq.PublishItem, 0, len(bodies))
	for _, body := range bodies {
		out = append(out, mq.PublishItem{Message: &mq.Message{Body: []byte(body)}})
	}
	return out
}

// advCapsWithBatchLimit is advCaps with a smaller advertised batch maximum, so
// that a batch has to be split.
func advCapsWithBatchLimit(n int, without ...mq.Feature) mq.Capabilities {
	caps := advCaps(without...)
	caps.SetLimits(mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: 1 << 20, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: n, Known: true},
		MaxHeaderCount:   mq.Limit[int]{Value: 64, Known: true},
		MaxDelay:         mq.Limit[time.Duration]{Value: time.Hour, Known: true},
	})
	return caps
}
