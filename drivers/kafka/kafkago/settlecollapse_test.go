package kafkago_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// fetchN fetches exactly n deliveries, retrying until they have all arrived.
func fetchN(t *testing.T, ctx context.Context, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	got := make([]mq.DriverDelivery, 0, n)
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		dst := make([]mq.DriverDelivery, n-len(got))
		fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		k, err := src.Fetch(fctx, dst)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Fetch: %v", err)
		}
		got = append(got, dst[:k]...)
	}
	if len(got) != n {
		t.Fatalf("fetched %d of %d published messages", len(got), n)
	}
	return got
}
