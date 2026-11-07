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
	"github.com/iqhive/iqmq/internal/rabbit/delay"
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
// retryable. That holds whatever the declaration met once the connection had
// gone: a nack that waited for the declaration turn behind one the loss ended
// found no connection at all, or no answer to its `channel.open`, and was
// answered Unavailable and retryable as though the delivery were still the
// caller's. The alarm too: a nack whose delivery went while it waited for the
// turn, and that then gave up on hearing the alarm, was answered Backpressure
// and retryable.
func TestADelayedNackThatWroteNothingIsAnsweredForWhatItIs(t *testing.T) {
	refusal := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - inequivalent arg", Server: true}
	lose := func(_ *Backend, conn *rabbitfake.ConnHandle) {
		conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	}
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
		{"the connection gone with the delivery", lose, io.ErrUnexpectedEOF, mq.CodeLeaseLost, false},
		{"no connection, the delivery gone with the last", lose, errReconnecting, mq.CodeLeaseLost, false},
		{"no answer to channel.open, the delivery gone", lose, errChannelOpenTimeout, mq.CodeLeaseLost, false},
		{"the caller's deadline, the delivery gone", lose, context.DeadlineExceeded, mq.CodeLeaseLost, false},
		{"the alarm, the delivery gone", lose, &blockedError{reason: "low on memory"}, mq.CodeLeaseLost, false},
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

// TestADelayedNackLooksAtItsDeliveryAgainOnceItHasTheDeclarationTurn is
// [settler.ensureDelay]'s second look: a nack whose delivery went while it
// waited for the declaration turn, in a way the source has not acted on yet,
// declares nothing once it has the turn and reports ErrLeaseLost.
//
// The source hears a channel or a connection go a little after the client
// library marks it closed, and the dial loop redials at once. A nack that got
// the turn in between had nothing to stop it, declared its delay queue on
// whatever connection was current, the replacement included, and waited for
// the broker's answer before its settlement found the delivery gone. Here the
// tokens are invalidated with the channel still held, as they are for a
// moment in the source's Close, so nothing ends the wait for the turn and the
// look after it is all that can answer.
func TestADelayedNackLooksAtItsDeliveryAgainOnceItHasTheDeclarationTurn(t *testing.T) {
	b, src, d, _ := deliveredOverAFake(t)
	plan := delay.Plan{Declare: &delay.QueueDecl{
		Name: "orders.workers.delay.1600", TTLMillis: 1600, DeadLetterRoutingKey: "orders.workers",
	}}
	q := decl{kind: declQueue, name: plan.Declare.Name, args: delayQueueArgs(plan.Declare)}
	b.declTurn <- struct{}{} // another nack's declaration
	done := make(chan error, 1)
	go func() { done <- (*settler)(src).ensureDelay(t.Context(), d, &plan) }()
	time.Sleep(100 * time.Millisecond) // the nack is waiting for the turn
	src.tokens.InvalidateAll()
	<-b.declTurn
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the delayed nack had not returned 5s after it got the turn")
	}
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the delayed nack whose delivery went while it waited for the turn reported %v, want ErrLeaseLost",
			err)
	}
	if b.topo.known(q) {
		t.Fatalf("the delayed nack whose delivery went while it waited for the turn declared %s", q.name)
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
