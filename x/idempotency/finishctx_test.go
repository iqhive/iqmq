package idempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/idempotency"
)

// ctxStore is a store that, like a network one, refuses a call on a context
// that is already done.
type ctxStore struct{ *idempotency.MemoryStore }

func (s ctxStore) Commit(ctx context.Context, c idempotency.Claim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.Commit(ctx, c)
}

func (s ctxStore) Abort(ctx context.Context, c idempotency.Claim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.Abort(ctx, c)
}

// cancelling is a handler whose delivery context ends as it returns: the
// deadline passing, or the runner shutting down, at the last moment.
type cancelling struct {
	cancel context.CancelFunc
	err    error
	calls  int
}

func (h *cancelling) Handle(context.Context, *mq.Delivery) error {
	h.calls++
	h.cancel()
	return h.err
}

// TestACancelledContextStillFinishesTheClaim: Commit and Abort used to run on
// the delivery's context, so a handler that finished as that context ended
// lost its commit -- the redelivery then ran the work a second time -- or its
// abort, which kept the redelivery deferred for the whole lease.
func TestACancelledContextStillFinishesTheClaim(t *testing.T) {
	for name, tc := range map[string]struct {
		handlerErr error
		want       idempotency.Status
	}{
		"commit": {nil, idempotency.StatusDuplicate},
		"abort":  {errors.New("the handler failed"), idempotency.StatusFirst},
	} {
		t.Run(name, func(t *testing.T) {
			_, mem := newGuard(t)
			in, err := idempotency.New(idempotency.Config{
				Store:  ctxStore{mem},
				Key:    idempotency.KeyFromHeader(keyHeader),
				Window: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			h := &cancelling{cancel: cancel, err: tc.handlerErr}
			_ = in.Middleware()(h).Handle(ctx, delivery("k1", "payload"))
			if c := in.Counts(); c.CommitLost != 0 {
				t.Errorf("CommitLost = %d: the commit ran on the delivery's cancelled context", c.CommitLost)
			}
			claim, err := mem.Claim(t.Context(), "k1", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if claim.Status != tc.want {
				t.Errorf("the next claim of the key is %v, want %v", claim.Status, tc.want)
			}
		})
	}
}
