package redis

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// stopLease is the acknowledgement deadline of the tests in this file.
const stopLease = 5 * time.Second

// openStopSource is a Stop source and a sink on one topic over a fake server.
// The backend's sweep interval is an hour, so under Stop the source sweeps
// every third of stopLease of wall-clock time, longer than these tests run.
func openStopSource(t *testing.T) (*fakestreams.Server, *Backend, *source, mq.Sink) {
	t.Helper()
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	ctx := context.Background()
	b, err := BackendWithCommander(ctx, srv, driverkit.ClientOwned, Config{
		AckDeadline:   stopLease,
		ClaimInterval: time.Hour,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Nack:          streams.Stop,
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "acme.orders", Subscription: "billing", Prefetch: 16, AckDeadline: stopLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "acme.orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	return srv, b, src.(*source), sink
}

// publishTo publishes one message per body, in order.
func publishTo(t *testing.T, sink mq.Sink, bodies ...string) {
	t.Helper()
	for _, body := range bodies {
		if err := sink.Publish(context.Background(), &mq.Message{Topic: "acme.orders", Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish %q: %v", body, err)
		}
	}
}

// fetchFrom fetches exactly one delivery, failing if none arrives.
func fetchFrom(t *testing.T, s *source) mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := s.Fetch(ctx, dst); n != 1 || err != nil {
		t.Fatalf("Fetch = %d, %v; want one delivery", n, err)
	}
	return dst[0]
}

// TestAStopNackAfterAReadReturnedKeepsWhatItBrought is a handler's Stop nack
// landing after Fetch's read has returned and before the source has buffered
// what it brought. The read was no longer in flight, so nothing told it
// about the nack, and the nack found nothing behind it in the buffer: the
// next fetch handed out the entry behind the failed message before its
// retry. No command passes in that window, so the test runs the read the way
// Fetch does and nacks between the two halves.
func TestAStopNackAfterAReadReturnedKeepsWhatItBrought(t *testing.T) {
	srv, b, s, sink := openStopSource(t)
	ctx := context.Background()
	publishTo(t, sink, "a")
	first := fetchFrom(t, s)
	publishTo(t, sink, "b")
	if _, err := s.collect(ctx, func(ctx context.Context) ([]streams.Delivery, error) {
		ds, err := s.read(ctx)
		if len(ds) != 1 {
			t.Errorf("the read brought %d entries, want b", len(ds))
		}
		if nerr := s.Settler().Nack(ctx, &first, mq.NackOptions{}); nerr != nil {
			t.Errorf("Nack: %v", nerr)
		}
		return ds, err
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	fctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, _ := s.Fetch(fctx, dst); n != 0 {
		t.Fatalf("the fetch after the nack handed out %q, which is behind the failed message", dst[0].Message.Body)
	}
	srv.Advance(2 * stopLease)
	if _, _, err := SweepForTest(b, s); err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	for i, want := range []string{"a", "b"} {
		d := fetchFrom(t, s)
		if string(d.Message.Body) != want {
			t.Fatalf("delivery %d after the sweep is %q, want %q: the shard's order did not survive the retry",
				i, d.Message.Body, want)
		}
		if err := s.Settler().Ack(ctx, &d); err != nil {
			t.Fatalf("Ack of %q: %v", want, err)
		}
	}
}

// TestAHandBackLeavesTheLiveDeliveryOfAnUndecodableEntryAlone is a buffered
// delivery that a sweep has superseded by the time a Stop nack hands it back.
// The sweep can run between the nack's hold and the hand-back, and the newer
// delivery is then the entry's live one. Poisoning the old one called Lost
// on the entry, which forgot the newer delivery too, and the source skipped
// it when it came round.
func TestAHandBackLeavesTheLiveDeliveryOfAnUndecodableEntryAlone(t *testing.T) {
	srv, _, s, sink := openStopSource(t)
	ctx := context.Background()
	keys, _ := streams.NewKeys("acme.orders", 1)
	publishTo(t, sink, "a")
	// A header index that skips one, which no correct encoder writes.
	if _, err := srv.Do(ctx, streams.XAdd(streams.XAddArgs{
		Key:    keys.Stream(0),
		ID:     streams.IDAuto,
		Fields: []string{"b", "body", "h:0", "1:kv", "h:2", "1:kv"},
	})); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	first := fetchFrom(t, s)
	s.mu.Lock()
	if len(s.pending) != 1 {
		s.mu.Unlock()
		t.Fatalf("the source buffered %d entries behind a, want the undecodable one", len(s.pending))
	}
	buffered := s.pending[0].Key
	s.mu.Unlock()
	// What the sweep does when it claims the entry again.
	live, _, ok := s.cons.Reclaimer().Hand(buffered)
	if !ok {
		t.Fatal("the reclaimer refused the newer delivery")
	}

	if err := s.Settler().Nack(ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if err := s.cons.Reclaimer().Check(buffered, live); err != nil {
		t.Fatalf("the hand-back of the superseded delivery forgot the live one: %v", err)
	}
}

// TestSweepIntervalFollowsAStopSourcesDeadline is how often a source sweeps.
func TestSweepIntervalFollowsAStopSourcesDeadline(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		deadline time.Duration
		nack     streams.Kind
		want     time.Duration
	}{
		{"another strategy keeps the configured interval", time.Hour, 300 * time.Millisecond, streams.LeavePending, time.Hour},
		{"zero is the default", 0, time.Hour, streams.LeavePending, DefaultClaimInterval},
		{"stop sweeps every third of a shorter deadline", time.Hour, 300 * time.Millisecond, streams.Stop, 100 * time.Millisecond},
		{"stop keeps a shorter configured interval", 50 * time.Millisecond, 300 * time.Millisecond, streams.Stop, 50 * time.Millisecond},
		{"stop derives nothing below the floor", time.Hour, 3 * time.Millisecond, streams.Stop, minStopSweep},
		{"stop keeps a configured interval below the floor", time.Millisecond, 3 * time.Millisecond, streams.Stop, time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sweepInterval(tt.interval, tt.deadline, tt.nack); got != tt.want {
				t.Fatalf("sweepInterval(%v, %v, %v) = %v, want %v", tt.interval, tt.deadline, tt.nack, got, tt.want)
			}
		})
	}
}
