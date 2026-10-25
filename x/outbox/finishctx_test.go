package outbox_test

import (
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/outbox"
)

// ctxStore is a MemoryStore that honours its context the way a SQL store
// does: a call on a context that has ended fails before it touches a row.
type ctxStore struct{ *outbox.MemoryStore }

func (s ctxStore) MarkSent(ctx context.Context, ids []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.MarkSent(ctx, ids)
}

func (s ctxStore) Release(ctx context.Context, ids []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.Release(ctx, ids)
}

func (s ctxStore) Fail(ctx context.Context, id string, reason error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.Fail(ctx, id, reason)
}

// TestAShutdownMidPassStillMarksThePublishedRows: MarkSent, Release and Fail
// used to run on Run's context, so a shutdown that landed while a batch was
// publishing failed the mark of rows already on the broker, Run returned nil
// because its context had ended, and every one of them was published again
// once its lock lapsed.
func TestAShutdownMidPassStillMarksThePublishedRows(t *testing.T) {
	mem := newStore(t, outbox.MemoryOptions{})
	ids := append3(t, mem)
	bad, err := mem.Append(outbox.Row{Topic: "orders", Body: []byte("bad")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sk := &sink{fail: func(m *mq.Message) error {
		cancel()
		if string(m.Body) == "bad" {
			return &mq.OpError{Operation: "publish", Code: mq.CodeMessageTooLarge, Err: errPublish}
		}
		return nil
	}}
	r, err := outbox.New(outbox.Config{Store: ctxStore{mem}, Publish: sk.publish})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil for a shutdown", err)
	}
	if got := mem.Sent(); len(got) != len(ids) {
		t.Fatalf("marked %v sent after a shutdown mid-pass, want %v: published rows will be published again", got, ids)
	}
	if err := mem.FailureOf(bad); err == nil {
		t.Error("the failed row's failure was not recorded after a shutdown mid-pass")
	}
}

// TestAMarkFailureDuringShutdownIsReported: Run treats an error as a clean stop
// when its context has ended, which hid a MarkSent failure behind the
// shutdown. The mark no longer runs on that context, so its failure is the
// store's and is reported.
func TestAMarkFailureDuringShutdownIsReported(t *testing.T) {
	mem := newStore(t, outbox.MemoryOptions{})
	append3(t, mem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sk := &sink{fail: func(*mq.Message) error {
		cancel()
		mem.SetUnavailable(true)
		return nil
	}}
	r, err := outbox.New(outbox.Config{Store: ctxStore{mem}, Publish: sk.publish})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(ctx)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want the store's MarkSent failure", err)
	}
	if mq.CodeOf(err) != mq.CodeUnavailable {
		t.Errorf("Run = %v, want the store's own error unwrapped", err)
	}
}
