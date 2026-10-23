package iqmq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestBatchingSendsWhatTheBoundCannotGrow pins the size-triggered batch whose
// threshold is out of reach of the publisher's own bound. Such a batch never
// fills, so it must leave when the buffer is full of it: before, a publisher
// under BackpressureBlock parked for ever, the other modes refused or
// discarded every later publication, and nothing was published until Flush.
func TestBatchingSendsWhatTheBoundCannotGrow(t *testing.T) {
	t.Parallel()
	body := bytes.Repeat([]byte("x"), 100)
	for _, tc := range []struct {
		name string
		cfg  mq.PublisherConfig
	}{
		{"count", mq.PublisherConfig{
			MaxPendingMessages: 4,
			Batching:           mq.BatchingConfig{MaxMessages: 100},
		}},
		{"bytes", mq.PublisherConfig{
			MaxPendingBytes: 4 * int64(len(body)),
			Batching:        mq.BatchingConfig{MaxBytes: 1 << 20},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, p := advClient(t, openBatch, tc.cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			const n = 20
			outcomes := make(chan error, n)
			for i := range n {
				if err := p.PublishAsync(ctx, &mq.Message{Body: body}, func(r mq.PublishResult) {
					outcomes <- r.Err
				}); err != nil {
					t.Fatalf("PublishAsync(%d) = %v, want nil: a full buffer of queued publications must send them rather than park", i, err)
				}
			}
			// The last buffer's worth has no later arrival to find it full,
			// so it waits for the Flush like any SizeOnly batch; everything
			// before it was sent because the buffer filled.
			const unforced = 4
			for i := range n - unforced {
				select {
				case err := <-outcomes:
					if err != nil {
						t.Fatalf("callback %d reported %v, want nil", i, err)
					}
				case <-ctx.Done():
					t.Fatalf("only %d of %d callbacks ran without a Flush", i, n-unforced)
				}
			}
			if err := p.Flush(ctx); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			for i := range unforced {
				if err := <-outcomes; err != nil {
					t.Fatalf("flushed callback %d reported %v, want nil", i, err)
				}
			}
			if got := len(b.bodies()); got != n {
				t.Fatalf("the driver saw %d publications, want %d", got, n)
			}
		})
	}
}

// TestBatchingUnstallsTheRefusingModes is the same stall under the modes that
// do not park: the refusal that meets the full buffer is the publication that
// sends it, so the publications already admitted complete without a Flush.
func TestBatchingUnstallsTheRefusingModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []mq.BackpressureMode{mq.BackpressureReject, mq.BackpressureDropNewest, mq.BackpressureDropOldest} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			_, p := advClient(t, openBatch, mq.PublisherConfig{
				Backpressure:       mode,
				MaxPendingMessages: 2,
				Batching:           mq.BatchingConfig{MaxMessages: 100},
			})
			ctx := context.Background()
			outcomes := make(chan string, 3)
			for _, body := range []string{"a", "b", "c"} {
				err := p.PublishAsync(ctx, &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
					outcomes <- body + ":" + errText(r.Err)
				})
				if err != nil && !errors.Is(err, mq.ErrBackpressure) {
					t.Fatalf("PublishAsync(%s) = %v, want nil or ErrBackpressure", body, err)
				}
			}
			// The drop mode's victim may answer first; what matters is that a
			// survivor is sent with nobody calling Flush.
			deadline := time.After(10 * time.Second)
			for {
				select {
				case got := <-outcomes:
					if got == "a:ok" || got == "b:ok" || got == "c:ok" {
						return
					}
				case <-deadline:
					t.Fatal("no queued publication was sent: a buffer full of a batch that cannot fill waited for a Flush")
				}
			}
		})
	}
}

// TestBackpressureValidatesBeforeTakingAPlace pins the order of the two
// refusals a publication can meet: a publication that is invalid on its own
// is refused for that, and taking a place in a full buffer never happens for
// it. Before, BackpressureDropOldest discarded a valid queued publication to
// make room for one that was then refused anyway, and the other modes
// reported ErrBackpressure for a message the buffer was never going to take.
func TestBackpressureValidatesBeforeTakingAPlace(t *testing.T) {
	t.Parallel()
	tooLarge := &mq.Message{Body: make([]byte, 2<<20)}
	for _, tc := range []struct {
		name string
		mode mq.BackpressureMode
		m    *mq.Message
		opts []mq.PublishOption
		want error
	}{
		{"dropoldest/size", mq.BackpressureDropOldest, tooLarge, nil, mq.ErrMessageTooLarge},
		{"dropoldest/option", mq.BackpressureDropOldest, &mq.Message{Body: []byte("bad")}, []mq.PublishOption{mq.Priority(300)}, nil},
		{"reject/size", mq.BackpressureReject, tooLarge, nil, mq.ErrMessageTooLarge},
		{"dropnewest/size", mq.BackpressureDropNewest, tooLarge, nil, mq.ErrMessageTooLarge},
		{"block/size", mq.BackpressureBlock, tooLarge, nil, mq.ErrMessageTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The place is held by a publication the driver has not
			// answered, except under the drop mode: only a queued
			// publication can be discarded, so that one is held in the
			// batching buffer.
			open, batching := openAsync, mq.BatchingConfig{}
			if tc.mode == mq.BackpressureDropOldest {
				open, batching = openBatch, mq.BatchingConfig{MaxMessages: 4}
			}
			b, p := advClient(t, open, mq.PublisherConfig{
				Backpressure:       tc.mode,
				MaxPendingMessages: 1,
				Batching:           batching,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			kept := make(chan string, 1)
			if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("kept")}, func(r mq.PublishResult) {
				kept <- errText(r.Err)
			}); err != nil {
				t.Fatalf("the first PublishAsync: %v", err)
			}
			err := p.PublishAsync(ctx, tc.m, nil, tc.opts...)
			switch {
			case err == nil:
				t.Fatal("the invalid PublishAsync = nil, want its own refusal")
			case errors.Is(err, mq.ErrBackpressure):
				t.Fatalf("the invalid PublishAsync = %v, want its validation error and not ErrBackpressure", err)
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("the invalid PublishAsync = %v, want %v", err, tc.want)
			}
			if got := p.Stats().Drops; got != 0 {
				t.Fatalf("Stats().Drops = %d, want 0: an invalid publication discarded a valid one", got)
			}
			if got := p.Stats().BackpressureEvents; got != 0 {
				t.Fatalf("Stats().BackpressureEvents = %d, want 0: an invalid publication never reached the bound", got)
			}
			if tc.mode != mq.BackpressureDropOldest {
				sink := advSinkOf[*advAsyncSink](t, b)
				sink.complete(sink.heldCount(), nil)
			}
			if err := p.Flush(ctx); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			select {
			case got := <-kept:
				if got != "ok" {
					t.Fatalf("the valid publication reported %q, want ok", got)
				}
			case <-ctx.Done():
				t.Fatal("the valid publication never completed")
			}
		})
	}
}
