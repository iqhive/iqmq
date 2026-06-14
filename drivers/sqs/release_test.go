package sqs

import (
	"context"
	"testing"
	"time"
)

// TestReleaseKeepsTheCallersDeadlineAndDropsItsCancellation pins the context
// every release runs under.
//
// Both halves matter and they pull in opposite directions. The callers that
// release are closing -- a poller whose context Close has just cancelled -- so
// inheriting the cancellation would give up on the very messages the release
// exists to hand back. But the caller's DEADLINE is its bound, and this used
// to be lost with the cancellation (context.WithoutCancel removes both), which
// gave every release a fresh Config.APITimeout whatever the caller had asked
// for.
func TestReleaseKeepsTheCallersDeadlineAndDropsItsCancellation(t *testing.T) {
	s := &source{b: &Backend{cfg: Config{APITimeout: time.Hour}}}

	want := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.Background(), want)
	ctx, done := s.releaseContext(parent)
	defer done()
	cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("a release context ended with its caller's cancellation (%v); a closing poller's release "+
			"would give up before it reached the service", err)
	}
	if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
		t.Fatalf("a release context has deadline %v (set %t), want its caller's %v: the caller's bound is lost",
			got, ok, want)
	}

	free, doneFree := s.releaseContext(context.Background())
	defer doneFree()
	got, ok := free.Deadline()
	if !ok {
		t.Fatal("a release for a caller with no deadline has none either; it is Config.APITimeout's to bound")
	}
	if left := time.Until(got); left < 59*time.Minute || left > time.Hour {
		t.Fatalf("a release for a caller with no deadline has %v left, want Config.APITimeout (1h)", left)
	}
}
