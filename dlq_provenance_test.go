package iqmq_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/memory"
)

// the dead-letter route publishes a copy stamped with five reserved-prefix
// headers through the ORDINARY publish path, so every driver has to carry
// those five — and for six shipped drivers it did not, because their
// Representable applied the rule about a CALLER's header to the core's own.
// driverkit.ReservedHeaderAllowed is the one
// predicate that says which keys are the core's; this file is what keeps it
// honest, by reading the keys off a message the route really published rather
// than off a list somebody typed twice.

// deadLetteredCopy runs the core dead-letter route over the memory driver and
// returns the copy it published.
func deadLetteredCopy(t *testing.T, cause error) *mq.Message {
	t.Helper()
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	dlq, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	publishToMemory(t, cl, "orders", 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return cause }),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	waitFor(t, "the original to be acked", func() bool {
		return inspect(t, b, "orders", "billing").Acked == 1
	})
	got, derr := memory.Drain(context.Background(), dlq, 1)
	if derr != nil {
		t.Fatalf("draining the dead-letter topic: %v", derr)
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(got) != 1 {
		t.Fatalf("the dead-letter topic holds %d messages, want 1", len(got))
	}
	return got[0]
}

// TestProvenanceHeadersAreExactlyWhatTheRouteWrites is the instrument that
// makes driverkit.ReservedHeaderAllowed maintainable: it reads the reserved
// keys off a copy the route really published, so a sixth provenance header
// added to dlq.go fails HERE — beside the twelve drivers that would otherwise
// refuse it one at a time and count a DeadLetterFailures for ever.
func TestProvenanceHeadersAreExactlyWhatTheRouteWrites(t *testing.T) {
	m := deadLetteredCopy(t, errors.New("undecodable"))

	var wrote []string
	for k := range m.Headers.All() {
		if strings.HasPrefix(k, mq.ReservedHeaderPrefix) {
			wrote = append(wrote, k)
		}
	}
	// The route writes iqmq-first-delivered-at only when this delivery IS the
	// first one, which it is here , so all five are present.
	want := driverkit.ProvenanceHeaders()
	if len(wrote) != len(want) {
		t.Fatalf("the route wrote %d reserved headers %q; driverkit.ProvenanceHeaders names %d %q",
			len(wrote), wrote, len(want), want)
	}
	for _, k := range wrote {
		if !driverkit.ReservedHeaderAllowed(k) {
			t.Errorf("the dead-letter route wrote %q and driverkit.ReservedHeaderAllowed refuses it: "+
				"every driver will refuse the publication, requeue the message and count a DeadLetterFailures", k)
		}
	}
	for _, k := range want {
		if _, ok := m.Headers.Get(k); !ok {
			t.Errorf("driverkit.ProvenanceHeaders names %q and the route does not write it: "+
				"a driver would be widened for a header nothing sends", k)
		}
	}
}

// TestReservedHeaderAllowedAdmitsNothingElse is the other direction: the
// widening is exactly five keys, so the protection against a caller
// colliding with the library survives it.
func TestReservedHeaderAllowedAdmitsNothingElse(t *testing.T) {
	for _, k := range []string{
		"", "iqmq", "iqmq-", driverkit.EnvelopeMarker,
		mq.ReservedHeaderPrefix + "key",
		mq.ReservedHeaderPrefix + "id",
		mq.ReservedHeaderPrefix + "attempt",
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "origin-partition",
		mq.ReservedHeaderPrefix + "origin-offset",
		mq.HeaderOriginTopic + "x",
		strings.ToUpper(mq.HeaderFailure),
		"origin-topic",
	} {
		if driverkit.ReservedHeaderAllowed(k) {
			t.Errorf("ReservedHeaderAllowed(%q) = true, want false", k)
		}
	}
	if n := driverkit.ProvenanceHeaderCount; n != 5 {
		t.Errorf("ProvenanceHeaderCount = %d, want 5: the specification names five provenance headers", n)
	}
}

// TestTheFailureHeaderIsFramableEverywhere pins a handler's
// error text is arbitrary, and several brokers cannot carry a control
// character in a header value at all — NATS and JetStream frame headers as
// text lines, so a newline in the reason would put the route back where the specification
// found it, refused before any I/O on a driver the fix was meant to repair.
//
// The route therefore renders the reason with its control characters replaced
// by spaces, and says so on mq.HeaderFailure. The transformation is the
// core's own diagnostic string and not a caller's metadata, which is why the specification's
// no-silent-loss rule is not what governs it.
func TestTheFailureHeaderIsFramableEverywhere(t *testing.T) {
	cause := errors.New("line one\r\nInjected: yes\tand a tab\x00and a NUL")
	m := deadLetteredCopy(t, cause)
	v, ok := m.Headers.Get(mq.HeaderFailure)
	if !ok {
		t.Fatalf("the dead-lettered copy carries no %s", mq.HeaderFailure)
	}
	for i := range v {
		if c := v[i]; c < 0x20 || c == 0x7f {
			t.Fatalf("%s = %q: byte %d is the control character %#x, which NATS, JetStream and "+
				"every text-framed broker refuse — the route is refused before any I/O", mq.HeaderFailure, v, i, c)
		}
	}
	if len(v) == 0 || v[0] == ' ' || v[0] == '\t' || v[len(v)-1] == ' ' || v[len(v)-1] == '\t' {
		t.Fatalf("%s = %q: a leading or trailing space is unframable on NATS", mq.HeaderFailure, v)
	}
	if want := "line one  Injected: yes and a tab and a NUL"; string(v) != want {
		t.Fatalf("%s = %q, want %q", mq.HeaderFailure, v, want)
	}
}

// TestTheAttemptsHeaderCountsTheRunnersOwnRetries pins.
//
// Under OrderingStrict a retry never leaves the process, so the BROKER's
// attempt counter does not move — and on a broker that counts no attempts at
// all it never moves. The route used to stamp d.Attempt, which is the broker's
// number, so `iqmq-attempts` said "1" however many attempts had been made and
// was absent altogether on drivers/nats, drivers/nsq and every other broker
// with no counter. It now stamps attemptOf, which is the count the runner
// bounded the retries with, so the header says what the policy acted on.
func TestTheAttemptsHeaderCountsTheRunnersOwnRetries(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	dlq, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	publishToMemory(t, cl, "orders", 1)
	var calls atomic.Int64
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		calls.Add(1)
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(3),
		mq.WithDeadLetter("orders.dlq"),
		mq.WithOrderingByKey(mq.OrderingStrict),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
	)
	waitFor(t, "the original to be acked", func() bool {
		return inspect(t, b, "orders", "billing").Acked == 1
	})
	got, derr := memory.Drain(context.Background(), dlq, 1)
	if derr != nil {
		t.Fatalf("draining the dead-letter topic: %v", derr)
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(got) != 1 {
		t.Fatalf("the dead-letter topic holds %d messages, want 1", len(got))
	}
	// The handler ran three times and the broker's own counter never moved,
	// because the message was never given back.
	if n := calls.Load(); n != 3 {
		t.Fatalf("the handler ran %d times, want 3", n)
	}
	v, ok := got[0].Headers.Get(mq.HeaderAttempts)
	if !ok {
		t.Fatalf("the dead-lettered copy carries no %s", mq.HeaderAttempts)
	}
	if string(v) != "3" {
		t.Fatalf("%s = %q, want %q: the header is the broker's counter rather than the count the "+
			"attempt bound was applied to", mq.HeaderAttempts, v, "3")
	}
}
