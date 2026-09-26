package rabbitmq_test

import (
	"context"
	"sort"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
)

// offlineBackend is a backend with no connection, for the tests that exercise
// the pure surfaces: the error table, the capability table, the naming and the
// message mapping.
func offlineBackend(t *testing.T) *rq.Backend {
	t.Helper()
	b, stop, err := rq.OfflineBackendForTest(base())
	if err != nil {
		t.Fatalf("building an offline backend: %v", err)
	}
	t.Cleanup(stop)
	return b
}

// decodeMessage maps one AMQP delivery through the driver's own mapping.
func decodeMessage(t *testing.T, d *amqp.Delivery, quorum bool) (*mq.Message, mq.DeliveryInfo) {
	t.Helper()
	msg, info, err := rq.DecodeForTest(offlineBackend(t), d, quorum)
	if err != nil {
		t.Fatalf("decoding a delivery: %v", err)
	}
	return msg, info
}

// decodeInfo is [decodeMessage] for a test that only reads the metadata.
func decodeInfo(t *testing.T, d *amqp.Delivery, quorum bool) mq.DeliveryInfo {
	t.Helper()
	_, info := decodeMessage(t, d, quorum)
	return info
}

// contextWithTimeout bounds one test operation.
func contextWithTimeout(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), d)
}

// contextWithTimeoutTB is [contextWithTimeout] for a fuzz target, whose
// testing.TB is not a *testing.T.
func contextWithTimeoutTB(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.Context(), d)
}

// sortStrings sorts in place, so that a fuzz target's own choice does not
// depend on a map's iteration order and a corpus entry reproduces.
func sortStrings(xs []string) { sort.Strings(xs) }

// benchContext bounds one drain inside a benchmark.
func benchContext(b *testing.B) (context.Context, context.CancelFunc) {
	return context.WithTimeout(b.Context(), 5*time.Second)
}
