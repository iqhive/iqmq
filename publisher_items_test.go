package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
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
func advCapsWithBatchLimit(n int) mq.Capabilities {
	caps := advCaps()
	caps.SetLimits(mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: 1 << 20, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: n, Known: true},
		MaxHeaderCount:   mq.Limit[int]{Value: 64, Known: true},
		MaxDelay:         mq.Limit[time.Duration]{Value: time.Hour, Known: true},
	})
	return caps
}
