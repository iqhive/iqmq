package kafkago

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	mq "github.com/iqhive/iqmq"
)

// TestAPausedPartitionsBacklogIsBounded: a partition whose tracking window is
// full keeps the records it cannot track in the source's backlog, and poll
// drained the shared queue into that backlog for as long as the fetch loop
// kept fetching, so one handler holding its deliveries pulled the whole rest
// of the partition into memory. The fetch loop now waits while the backlog is
// full, and resumes once the window has room.
func TestAPausedPartitionsBacklogIsBounded(t *testing.T) {
	const records = 300
	topic := "backlog-bound"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic),
		kfake.GroupMinSessionTimeout(time.Second))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	b, err := OpenBackend(t.Context(), Config{
		Brokers:           c.ListenAddrs(),
		ClientID:          "iqmq-test",
		CommitInterval:    50 * time.Millisecond,
		SessionTimeout:    2 * time.Second,
		RebalanceTimeout:  4 * time.Second,
		HeartbeatInterval: 300 * time.Millisecond,
		JoinBackoff:       200 * time.Millisecond,
		RevokeWait:        2 * time.Second,
		FetchMaxWait:      50 * time.Millisecond,
		RequestTimeout:    3 * time.Second,
		WriteTimeout:      2 * time.Second,
		// A small fetch keeps one response to a few records, so the bound
		// the test sees is the backlog's and not one response's.
		FetchMaxBytes:     1024,
		QueueCapacity:     4,
		TrackerMaxPending: 2,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	client := mq.NewClient(b)
	for range records {
		if err := client.Publish(t.Context(), topic, &mq.Message{Body: []byte("a record of the partition")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	opened, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-backlog", Prefetch: 2,
		Start: mq.StartEarliest(), Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	src, _ := opened.(*source)
	st := src.Settler()

	// Two deliveries fill the window, and nothing settles them yet.
	held := fetchN(t, src, 2)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 2))
	cancel()
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch with a full window = (%d, %v), want (0, DeadlineExceeded)", n, err)
	}
	src.mu.Lock()
	backlog := len(src.parts[0].backlog)
	src.mu.Unlock()
	if backlog > 32 {
		t.Fatalf("the paused partition's backlog holds %d of %d records: its fetch loop kept reading", backlog, records)
	}

	// Settling makes room, and the rest of the partition arrives.
	for i := range held {
		if err := st.Ack(t.Context(), &held[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	got := len(held)
	for got < records {
		for _, d := range fetchN(t, src, 1) {
			if err := st.Ack(t.Context(), &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			got++
		}
	}
}

// fetchN fetches until it holds n deliveries, copied out of the fetch buffer.
func fetchN(t *testing.T, src *source, n int) []mq.DriverDelivery {
	t.Helper()
	out := make([]mq.DriverDelivery, 0, n)
	dst := make([]mq.DriverDelivery, n)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for len(out) < n {
		m, err := src.Fetch(ctx, dst[:n-len(out)])
		if err != nil {
			t.Fatalf("Fetch after %d of %d: %v", len(out), n, err)
		}
		out = append(out, dst[:m]...)
	}
	return out
}
