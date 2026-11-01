package rabbitmq

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestADelayedNackThatWroteNothingIsAnsweredForWhatItIs is [settler.declareError]:
// the answer to a delayed nack that wrote nothing of its own, because the
// broker had blocked the connection or the delay queue could not be declared.
//
// Every answer is definite, and only one whose retry can succeed is
// retryable. While the delivery is still the caller's, a refusal of the
// declaration is the declaration's, read as an open's (a 406 is a Conflict,
// not LeaseLost), and a declaration channel that went on its own is retryable.
// A connection that went took the delivery with it: the consumer's channel is
// on it, so that is ErrLeaseLost, as a settlement on that channel reports it,
// and a retry could only meet the same answer. It was Unavailable and
// retryable.
func TestADelayedNackThatWroteNothingIsAnsweredForWhatItIs(t *testing.T) {
	refusal := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - inequivalent arg", Server: true}
	for _, tc := range []struct {
		name      string
		state     func(b *Backend, conn *rabbitfake.ConnHandle)
		err       error
		code      mq.ErrorCode
		retryable bool
	}{
		{"the alarm", nil, &blockedError{reason: "low on disk"}, mq.CodeBackpressure, true},
		{"the broker's refusal", nil, refusal, mq.CodeConflict, false},
		{"the declaration channel gone on its own", nil, amqp.ErrClosed, mq.CodeUnavailable, true},
		{"the caller's deadline", nil, context.DeadlineExceeded, mq.CodeDeadlineExceeded, true},
		{"the connection gone with the delivery", func(_ *Backend, conn *rabbitfake.ConnHandle) {
			conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		}, io.ErrUnexpectedEOF, mq.CodeLeaseLost, false},
		{"the connection gone as the client closes", func(b *Backend, _ *rabbitfake.ConnHandle) {
			b.BeginClose()
		}, io.ErrUnexpectedEOF, mq.CodeClosed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, src, d, conn := deliveredOverAFake(t)
			if tc.state != nil {
				tc.state(b, conn)
			}
			got := (*settler)(src).declareError(d, tc.err)
			if mq.CodeOf(got) != tc.code || mq.IsRetryable(got) != tc.retryable || mq.IsAmbiguous(got) {
				t.Fatalf("declareError(%v) = %v (code %v, retryable %v, ambiguous %v), want code %v, retryable %v, "+
					"definite", tc.err, got, mq.CodeOf(got), mq.IsRetryable(got), mq.IsAmbiguous(got), tc.code,
					tc.retryable)
			}
		})
	}
}

// deliveredOverAFake is a backend over a fake broker, its source of
// orders/workers, the one delivery that source has handed out, and the
// connection it came on.
func deliveredOverAFake(t *testing.T) (*Backend, *source, *mq.DriverDelivery, *rabbitfake.ConnHandle) {
	t.Helper()
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{Addrs: []string{"localhost:5672"}, Declare: true})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientOwned); err != nil {
		t.Fatalf("opening: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	s, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish("", "orders.workers", amqp.Publishing{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := s.Fetch(ctx, dst); n != 1 {
		t.Fatalf("fetching: %v", err)
	}
	src, ok := s.(*source)
	if !ok {
		t.Fatalf("OpenSource returned %T", s)
	}
	if errors.Is((*settler)(src).gone(&dst[0]), mq.ErrLeaseLost) {
		t.Fatal("the delivery is gone before the test began, so this test proves nothing")
	}
	return b, src, &dst[0], broker.LastConnection()
}
