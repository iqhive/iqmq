package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestCompileOptionsValidatesOnce pins the specification's "fail before any I/O", met once
// for a whole loop instead of once per publication: a control the backend
// cannot honour is refused by CompileOptions, where there is no message yet.
func TestCompileOptionsValidatesOnce(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		missing mq.Feature
		opts    mq.PublishOptions
	}{
		{"delay", mq.FeatureScheduledPublish, mq.PublishOptions{Delay: time.Minute}},
		{"deliverat", mq.FeatureScheduledPublish, mq.PublishOptions{DeliverAt: time.Now()}},
		{"ttl", mq.FeatureMessageTTL, mq.PublishOptions{TTL: time.Hour}},
		{"priority", mq.FeaturePriority, mq.PublishOptions{Priority: 3, PrioritySet: true}},
		{"dedup", mq.FeatureDeduplication, mq.PublishOptions{DedupKey: "order-42"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			b := newAdvBroker(openPlain, c.missing)
			p := advPublisher(t, b, mq.PublisherConfig{})
			set, err := p.CompileOptions(c.opts)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("CompileOptions = (%v, %v), want ErrUnsupported", set, err)
			}
			if set != nil {
				t.Fatal("CompileOptions returned a set alongside an error")
			}
			if got := b.nativeCalls(); got != 0 {
				t.Fatalf("the driver saw %d publications while compiling options, want 0", got)
			}
		})
	}
}

// TestCompileOptionsHoldsDeliverAtToTheMaxDelay pins that the advertised
// delay ceiling binds a DeliverAt as it binds a Delay: the two are the same
// hold, one stated as an instant.
func TestCompileOptionsHoldsDeliverAtToTheMaxDelay(t *testing.T) {
	t.Parallel()
	_, p := advClient(t, openPlain, mq.PublisherConfig{})
	// advCaps advertises an hour.
	if _, err := p.CompileOptions(mq.PublishOptions{DeliverAt: time.Now().Add(2 * time.Hour)}); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("CompileOptions with a DeliverAt past the ceiling = %v, want ErrUnsupported", err)
	}
	if _, err := p.CompileOptions(mq.PublishOptions{DeliverAt: time.Now().Add(30 * time.Minute)}); err != nil {
		t.Fatalf("CompileOptions with a DeliverAt inside the ceiling = %v, want nil", err)
	}
}

// TestPublishCompiledCarriesTheCompiledControls pins that the compiled set is
// what the driver is handed — the whole set, and nothing per call.
func TestPublishCompiledCarriesTheCompiledControls(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openPlain, mq.PublisherConfig{})
	set, err := p.CompileOptions(mq.PublishOptions{
		Delay:     5 * time.Minute,
		DedupKey:  "order-42",
		Partition: 3, PartitionSet: true,
	})
	if err != nil {
		t.Fatalf("CompileOptions: %v", err)
	}
	if got := set.Options().DedupKey; got != "order-42" {
		t.Fatalf("the compiled set reports DedupKey %q, want order-42", got)
	}
	for i := range 3 {
		if err := p.PublishCompiled(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, set); err != nil {
			t.Fatalf("PublishCompiled(%d): %v", i, err)
		}
	}
	for i, rec := range b.records() {
		if rec.dedup != "order-42" || rec.delay != 5*time.Minute || rec.part != 3 {
			t.Fatalf("publication %d carried (%q, %v, %d), want (order-42, 5m0s, 3)", i, rec.dedup, rec.delay, rec.part)
		}
	}
	if got := len(b.records()); got != 3 {
		t.Fatalf("the driver saw %d publications, want 3", got)
	}
}

// TestPublishCompiledRefusesASetFromAnotherPublisher pins.
//
// A compiled set carries the capability answers *and the defaults* of the
// publisher that compiled it, so using one on another publisher would publish
// under a configuration the caller never asked for — silently, and even when
// the two publishers are identical. It is a publish error, because a compiled
// set is a value a caller may pass anywhere and the misuse is only visible
// where it is used.
func TestPublishCompiledRefusesASetFromAnotherPublisher(t *testing.T) {
	t.Parallel()
	b := newAdvBroker(openPlain)
	cl := mq.NewClient(b)
	t.Cleanup(func() { _ = cl.Close() })
	first, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening the first publisher: %v", err)
	}
	second, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening the second publisher: %v", err)
	}
	set, err := first.CompileOptions(mq.PublishOptions{DedupKey: "order-42"})
	if err != nil {
		t.Fatalf("CompileOptions: %v", err)
	}
	err = second.PublishCompiled(context.Background(), &mq.Message{Body: []byte("x")}, set)
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("PublishCompiled with another publisher's set = %v (code %v), want CodeInvalidArgument",
			err, mq.CodeOf(err))
	}
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications, want 0: the refusal must come before any I/O", got)
	}
	// The set still works on the publisher that compiled it.
	if err := first.PublishCompiled(context.Background(), &mq.Message{Body: []byte("x")}, set); err != nil {
		t.Fatalf("PublishCompiled on its own publisher: %v", err)
	}
}

// TestPublishCompiledRejectsANilSetAndMessage pins the two degenerate calls.
func TestPublishCompiledRejectsANilSetAndMessage(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openPlain, mq.PublisherConfig{})
	if err := p.PublishCompiled(context.Background(), &mq.Message{Body: []byte("x")}, nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("PublishCompiled(nil set) = %v, want CodeInvalidArgument", err)
	}
	set, err := p.CompileOptions(p.Defaults())
	if err != nil {
		t.Fatalf("CompileOptions: %v", err)
	}
	if err := p.PublishCompiled(context.Background(), nil, set); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("PublishCompiled(nil message) = %v, want CodeInvalidArgument", err)
	}
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications, want 0", got)
	}
}

// TestPublishCompiledStillEnforcesTheMessageLimits pins the half that
// compiling cannot answer: a limit that depends on the message.
func TestPublishCompiledStillEnforcesTheMessageLimits(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openPlain, mq.PublisherConfig{})
	set, err := p.CompileOptions(p.Defaults())
	if err != nil {
		t.Fatalf("CompileOptions: %v", err)
	}
	err = p.PublishCompiled(context.Background(), &mq.Message{Body: make([]byte, (1<<20)+1)}, set)
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("PublishCompiled of an oversized message = %v, want ErrMessageTooLarge", err)
	}
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications, want 0", got)
	}
}

// TestPublishCompiledOnAClosedPublisher pins the specification's "after close all
// operations return ErrClosed", for this path too.
func TestPublishCompiledOnAClosedPublisher(t *testing.T) {
	t.Parallel()
	_, p := advClient(t, openPlain, mq.PublisherConfig{})
	set, err := p.CompileOptions(p.Defaults())
	if err != nil {
		t.Fatalf("CompileOptions: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.PublishCompiled(context.Background(), &mq.Message{Body: []byte("x")}, set); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("PublishCompiled after Close = %v, want ErrClosed", err)
	}
	if _, err := p.CompileOptions(p.Defaults()); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("CompileOptions after Close = %v, want ErrClosed", err)
	}
}
